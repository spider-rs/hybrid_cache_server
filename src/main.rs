//! hybrid_cache_server: a remote HTTP response cache for the crawl fleet.
//! RocksDB holds the data, a bounded in-memory cache holds hot resources,
//! and Meilisearch indexing is optional.

mod config;
mod meili;
mod model;
mod netio;
mod server;
mod store;
mod telemetry;
#[cfg(test)]
mod tests;

use std::{net::SocketAddr, sync::Arc, time::Duration};

use hyper::{body::Incoming, Request};
use hyper_util::{
    rt::{TokioExecutor, TokioIo, TokioTimer},
    server::conn::auto::Builder as AutoBuilder,
    service::TowerToHyperService,
};
use tokio::net::TcpListener;
use tracing::{debug, error, info, warn};
use tracing_subscriber::EnvFilter;

use crate::{
    config::Config,
    meili::Meili,
    server::{compression_layer, handle, AppState},
    store::Store,
    telemetry as tm,
};

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// Return freed pages to the OS at once. The server frees multi-MB buffers
/// at a high rate under load; with a 1 s decay the loadgen held 2.4 GB of
/// freed pages at c=32 (RSS peak 4.9 GB, against 2.5 GB with 0). The 1 s
/// decay was about 10% faster there. Overridable with _RJEM_MALLOC_CONF.
#[cfg(target_os = "linux")]
#[allow(non_upper_case_globals)]
#[export_name = "_rjem_malloc_conf"]
pub static _rjem_malloc_conf: &[u8] = b"background_thread:true,dirty_decay_ms:0,muzzy_decay_ms:0\0";

/// macOS jemalloc has no background threads; purging happens on the
/// allocating threads instead.
#[cfg(all(not(target_env = "msvc"), not(target_os = "linux")))]
#[allow(non_upper_case_globals)]
#[export_name = "_rjem_malloc_conf"]
pub static _rjem_malloc_conf: &[u8] = b"dirty_decay_ms:0,muzzy_decay_ms:0\0";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let cfg = Config::from_env();
    let prometheus = tm::install();
    info!("{}", tm::allocator_decay());

    let store = Store::open(&cfg)?;
    info!(
        "rocksdb open at {} (block cache {} MiB, blob files {}), mem cache {} MiB",
        cfg.rocksdb_path,
        cfg.rocksdb_block_cache_bytes >> 20,
        cfg.rocksdb_blob_files,
        cfg.mem_cache_bytes >> 20
    );

    let meili = match &cfg.meili {
        Some(m) => Meili::start(m).await,
        None => {
            info!("meilisearch indexing off (set MEILI_ENABLE=1 to turn it on)");
            None
        }
    };

    let state = Arc::new(AppState::new(cfg.clone(), store, meili, prometheus));
    tokio::spawn(run_cleanup_worker(state.clone()));

    let addr: SocketAddr = ([0, 0, 0, 0], cfg.port).into();
    let listener = TcpListener::bind(addr).await?;
    info!("listening on http://{addr}");

    let conns = Arc::new(tokio::sync::Semaphore::new(cfg.max_connections));
    loop {
        let (stream, _peer) = match listener.accept().await {
            Ok(v) => v,
            Err(e) => {
                // EMFILE and friends are transient. Back off and keep serving.
                metrics::counter!(tm::HTTP_ACCEPT_ERRORS).increment(1);
                warn!("accept failed: {e}");
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
        };
        netio::tune_socket(&stream, cfg.tcp_timeout);
        let Ok(permit) = conns.clone().acquire_owned().await else {
            continue;
        };
        let state = state.clone();

        tokio::spawn(async move {
            metrics::gauge!(tm::HTTP_CONNS).increment(1.0);
            let svc_state = state.clone();
            let svc = tower::service_fn(move |req: Request<Incoming>| {
                let state = svc_state.clone();
                async move { handle(req, state).await }
            });
            let svc = tower::ServiceBuilder::new()
                .layer(compression_layer(&state.cfg))
                .service(svc);

            let mut builder = AutoBuilder::new(TokioExecutor::new());
            builder
                .http1()
                .timer(TokioTimer::new())
                .header_read_timeout(state.cfg.header_read_timeout);
            builder.http2().timer(TokioTimer::new());

            if let Err(err) = builder
                .serve_connection(
                    TokioIo::new(netio::WriteIdleTimeout::new(
                        stream,
                        state.cfg.write_idle_timeout,
                    )),
                    TowerToHyperService::new(svc),
                )
                .await
            {
                if is_benign(&*err) {
                    debug!("connection closed: {err}");
                } else {
                    metrics::counter!(tm::HTTP_CONN_ERRORS).increment(1);
                    warn!("connection error: {err}");
                }
            }
            metrics::gauge!(tm::HTTP_CONNS).decrement(1.0);
            drop(permit);
        });
    }
}

/// Resets, hang-ups and idle timeouts are normal for a server behind a
/// load balancer and are not worth an error line.
fn is_benign(err: &(dyn std::error::Error + 'static)) -> bool {
    let mut cur: Option<&(dyn std::error::Error + 'static)> = Some(err);
    while let Some(e) = cur {
        if let Some(h) = e.downcast_ref::<hyper::Error>() {
            if h.is_incomplete_message() || h.is_canceled() || h.is_closed() || h.is_timeout() {
                return true;
            }
        }
        if let Some(io) = e.downcast_ref::<std::io::Error>() {
            use std::io::ErrorKind::*;
            if matches!(
                io.kind(),
                ConnectionReset | ConnectionAborted | BrokenPipe | UnexpectedEof | TimedOut
            ) {
                return true;
            }
        }
        cur = e.source();
    }
    false
}

async fn run_cleanup_worker(state: Arc<AppState>) {
    let mut pass: u64 = 0;
    loop {
        tokio::time::sleep(state.cfg.cleanup_interval).await;
        // The first pass and every Nth after it also sweep all stored bodies,
        // which clears bodies orphaned before v0.3 tracked overwrites.
        let full = pass.is_multiple_of(state.cfg.full_sweep_every);
        pass += 1;
        let ttl = state.cfg.cache_ttl_secs;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        let st = state.clone();
        match tokio::task::spawn_blocking(move || st.store.cleanup(ttl, now, full)).await {
            Ok(Ok(r)) => {
                if !r.expired_resource_keys.is_empty() || r.removed_files > 0 {
                    info!(
                        "cleanup: scanned {} resources, removed {} expired, {} bodies{}",
                        r.scanned,
                        r.expired_resource_keys.len(),
                        r.removed_files,
                        if full { " (full sweep)" } else { "" }
                    );
                }
                if let Some(m) = &state.meili {
                    m.delete(&r.expired_resource_keys).await;
                }
            }
            Ok(Err(e)) => error!("cleanup failed: {e}"),
            Err(e) => error!("cleanup task failed: {e}"),
        }
    }
}
