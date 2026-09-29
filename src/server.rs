//! HTTP routing and handlers.

use std::{
    collections::VecDeque,
    convert::Infallible,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Instant,
};

use bytes::{Bytes, BytesMut};
use http::{header, HeaderMap, Request, Response, StatusCode};
use http_body::{Body, Frame, SizeHint};
use http_body_util::{BodyExt, LengthLimitError, Limited};
use metrics_exporter_prometheus::PrometheusHandle;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tower_http::compression::{predicate::Predicate, CompressionLayer, CompressionLevel};
use tracing::{error, warn};

use crate::{
    config::{Compression, Config},
    meili::{CacheIndexDoc, Meili},
    model::{encode_payload, IncomingPayload},
    store::{prepare_entry, resolve_website_key, SiteKeyPrecedence, Store},
    telemetry as tm,
};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

pub struct AppState {
    pub cfg: Config,
    pub store: Arc<Store>,
    /// Bytes of request and response buffers that may be held at once.
    pub budget: Arc<Semaphore>,
    pub meili: Option<Meili>,
    pub prometheus: Option<PrometheusHandle>,
}

impl AppState {
    pub fn new(
        cfg: Config,
        store: Store,
        meili: Option<Meili>,
        prometheus: Option<PrometheusHandle>,
    ) -> Self {
        let budget = Arc::new(Semaphore::new(cfg.max_inflight_bytes as usize));
        AppState {
            cfg,
            store: Arc::new(store),
            budget,
            meili,
            prometheus,
        }
    }
}

// ---------------------------------------------------------------- response body

/// Response body made of ready chunks. Holds its share of the in-flight
/// budget until hyper has written it out and dropped it.
pub struct ChunkedBody {
    chunks: VecDeque<Bytes>,
    remaining: u64,
    _permit: Option<OwnedSemaphorePermit>,
}

impl ChunkedBody {
    pub fn new(chunks: Vec<Bytes>, permit: Option<OwnedSemaphorePermit>) -> Self {
        let remaining = chunks.iter().map(|c| c.len() as u64).sum();
        ChunkedBody {
            chunks: chunks.into_iter().filter(|c| !c.is_empty()).collect(),
            remaining,
            _permit: permit,
        }
    }

    pub fn full(b: impl Into<Bytes>) -> Self {
        Self::new(vec![b.into()], None)
    }
}

impl Body for ChunkedBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        match self.chunks.pop_front() {
            Some(b) => {
                self.remaining -= b.len() as u64;
                Poll::Ready(Some(Ok(Frame::data(b))))
            }
            None => Poll::Ready(None),
        }
    }

    fn is_end_stream(&self) -> bool {
        self.chunks.is_empty()
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(self.remaining)
    }
}

pub fn text_response(status: StatusCode, text: impl Into<String>) -> Response<ChunkedBody> {
    let mut r = Response::new(ChunkedBody::full(text.into()));
    *r.status_mut() = status;
    r.headers_mut().insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    r
}

fn json_response(status: StatusCode, body: ChunkedBody) -> Response<ChunkedBody> {
    let mut r = Response::new(body);
    *r.status_mut() = status;
    r.headers_mut().insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static("application/json"),
    );
    r
}

// ---------------------------------------------------------------- compression

/// Compress only text-like responses of at least 1 KiB. Stored bodies of
/// images, fonts, video, archives and octet-streams are already compressed
/// or not worth the CPU.
#[derive(Clone, Copy)]
pub struct CompressPredicate {
    pub enabled: bool,
}

pub fn compressible_content_type(ct: &str) -> bool {
    let mime = ct
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();
    if mime.is_empty() {
        return false;
    }
    if mime == "text/event-stream" {
        return false;
    }
    mime.starts_with("text/")
        || mime == "application/json"
        || mime.ends_with("+json")
        || mime.contains("javascript")
        || mime.contains("ecmascript")
        || mime.ends_with("/css")
        || mime.ends_with("/xml")
        || mime.ends_with("+xml")
}

impl Predicate for CompressPredicate {
    fn should_compress<B>(&self, response: &http::Response<B>) -> bool
    where
        B: Body,
    {
        if !self.enabled {
            return false;
        }
        if let Some(n) = response.body().size_hint().exact() {
            if n < 1024 {
                return false;
            }
        }
        response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(compressible_content_type)
            .unwrap_or(false)
    }
}

pub fn compression_layer(cfg: &Config) -> CompressionLayer<CompressPredicate> {
    // gzip stays on as the fallback for clients that do not accept the
    // chosen codec. tower-http picks the highest q-value, and on a tie
    // prefers zstd, then br, then gzip.
    let layer = CompressionLayer::new()
        .no_deflate()
        .no_br()
        .no_zstd()
        .gzip(true);
    let layer = match cfg.compression {
        Compression::Gzip | Compression::Off => layer,
        Compression::Br => layer.br(true),
        Compression::Zstd => layer.zstd(true),
    };
    layer
        .quality(CompressionLevel::Precise(cfg.compression_level))
        .compress_when(CompressPredicate {
            enabled: cfg.compression != Compression::Off,
        })
}

// ---------------------------------------------------------------- routing

fn route_of(method: &http::Method, path: &str) -> &'static str {
    let segments: Vec<&str> = path.trim_start_matches('/').split('/').collect();
    match (method.as_str(), segments.as_slice()) {
        ("POST", ["cache", "index"]) => "index",
        ("POST", ["cache", "index", "batch"]) => "index_batch",
        ("GET", ["cache", "resource", _]) => "resource",
        ("GET", ["cache", "site", _]) => "site",
        ("GET", ["cache", "size"]) => "size",
        ("POST", ["cache", "purge", "empty"]) => "purge_empty",
        ("GET", ["health"]) => "health",
        ("GET", ["metrics"]) => "metrics",
        _ => "not_found",
    }
}

/// Entry point for every request: routes, applies the request timeout,
/// records metrics.
pub async fn handle<B>(
    req: Request<B>,
    state: Arc<AppState>,
) -> Result<Response<ChunkedBody>, Infallible>
where
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: Into<BoxError>,
{
    let started = Instant::now();
    let route = route_of(req.method(), req.uri().path());
    let timeout = state.cfg.request_timeout;
    let resp = match tokio::time::timeout(timeout, dispatch(req, state, route)).await {
        Ok(r) => r,
        Err(_) => text_response(StatusCode::SERVICE_UNAVAILABLE, "Request timed out"),
    };
    let status = resp.status().as_u16().to_string();
    let elapsed = started.elapsed().as_secs_f64();
    metrics::counter!(tm::HTTP_REQUESTS, "route" => route, "status" => status.clone()).increment(1);
    metrics::histogram!(tm::HTTP_DURATION, "route" => route, "status" => status).record(elapsed);
    if let Some(n) = resp.body().size_hint().exact() {
        metrics::counter!(tm::HTTP_RESP_BYTES, "route" => route).increment(n);
    }
    Ok(resp)
}

async fn dispatch<B>(
    req: Request<B>,
    state: Arc<AppState>,
    route: &'static str,
) -> Response<ChunkedBody>
where
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: Into<BoxError>,
{
    match route {
        "index" => handle_put(req, state, false).await,
        "index_batch" => handle_put(req, state, true).await,
        "resource" => {
            let key = last_segment(req.uri().path());
            let raw = wants_raw(req.uri().query().unwrap_or(""));
            handle_resource(state, key, raw).await
        }
        "site" => handle_site(state, last_segment(req.uri().path())).await,
        "size" => handle_size(state).await,
        "purge_empty" => handle_purge_empty(state),
        "health" => handle_health(&state),
        "metrics" => handle_metrics(&state),
        _ => text_response(StatusCode::NOT_FOUND, "Not Found"),
    }
}

fn last_segment(path: &str) -> String {
    path.rsplit('/').next().unwrap_or("").to_string()
}

fn wants_raw(query: &str) -> bool {
    query.split('&').any(|pair| {
        let mut parts = pair.splitn(2, '=');
        match (parts.next(), parts.next()) {
            (Some("raw"), Some(v)) => v == "1" || v.eq_ignore_ascii_case("true"),
            (Some("format"), Some(v)) => {
                v.eq_ignore_ascii_case("bytes") || v.eq_ignore_ascii_case("raw")
            }
            _ => false,
        }
    })
}

// ---------------------------------------------------------------- writes

/// Peak memory of a write is about 2.5x its JSON size: the request buffer,
/// the decoded bodies, and RocksDB's copy in the WriteBatch.
fn write_reservation(len: u64, cfg: &Config) -> u32 {
    let r = len.saturating_mul(5) / 2;
    r.min(cfg.max_inflight_bytes).min(u32::MAX as u64) as u32
}

fn content_length(headers: &HeaderMap) -> Option<u64> {
    headers
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.trim().parse().ok())
}

/// Read a request body within MAX_BODY_BYTES, holding budget for it.
async fn read_body<B>(
    headers: &HeaderMap,
    body: B,
    state: &AppState,
) -> Result<(Bytes, OwnedSemaphorePermit), Response<ChunkedBody>>
where
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: Into<BoxError>,
{
    let max = state.cfg.max_body_bytes;
    let declared = content_length(headers);
    if declared.is_some_and(|n| n > max) {
        return Err(text_response(
            StatusCode::PAYLOAD_TOO_LARGE,
            "Body too large",
        ));
    }
    let reserve = write_reservation(declared.unwrap_or(max), &state.cfg);
    let wait = Instant::now();
    let permit = match state.budget.clone().acquire_many_owned(reserve).await {
        Ok(p) => p,
        Err(_) => {
            return Err(text_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "Shutting down",
            ))
        }
    };
    metrics::histogram!(tm::INFLIGHT_WAIT).record(wait.elapsed().as_secs_f64());

    // Copy frames into one buffer sized from Content-Length as they
    // arrive. Collecting frames and then concatenating them would hold the
    // body twice at the peak.
    let mut limited = std::pin::pin!(Limited::new(body, max as usize));
    let mut buf = BytesMut::with_capacity(declared.unwrap_or(0).min(max) as usize);
    while let Some(frame) = limited.as_mut().frame().await {
        match frame {
            Ok(f) => {
                if let Ok(d) = f.into_data() {
                    buf.extend_from_slice(&d);
                }
            }
            Err(e) => {
                return if e.downcast_ref::<LengthLimitError>().is_some() {
                    Err(text_response(
                        StatusCode::PAYLOAD_TOO_LARGE,
                        "Body too large",
                    ))
                } else {
                    tracing::debug!("failed to read request body: {e}");
                    Err(text_response(StatusCode::BAD_REQUEST, "Invalid body"))
                };
            }
        }
    }
    metrics::counter!(tm::HTTP_REQ_BYTES).increment(buf.len() as u64);
    Ok((buf.freeze(), permit))
}

enum PutOutcome {
    BadJson(String),
    NothingIndexed,
    CommitFailed(String),
    Indexed { ok: usize, failed: usize },
}

async fn handle_put<B>(req: Request<B>, state: Arc<AppState>, batch: bool) -> Response<ChunkedBody>
where
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: Into<BoxError>,
{
    let (parts, body) = req.into_parts();
    let (bytes, permit) = match read_body(&parts.headers, body, &state).await {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let header_site = parts
        .headers
        .get("x-cache-site")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);

    let st = state.clone();
    // Parse, decode, hash and write off the async workers. The permit moves
    // in too, so the budget is held until the work is really done even if
    // the request times out first.
    let joined = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let payloads: Vec<IncomingPayload<'_>> = if batch {
            match serde_json::from_slice(&bytes) {
                Ok(p) => p,
                Err(e) => return (PutOutcome::BadJson(e.to_string()), Vec::new()),
            }
        } else {
            match serde_json::from_slice(&bytes) {
                Ok(p) => vec![p],
                Err(e) => return (PutOutcome::BadJson(e.to_string()), Vec::new()),
            }
        };
        let precedence = if batch {
            SiteKeyPrecedence::ItemFirst
        } else {
            SiteKeyPrecedence::HeaderFirst
        };
        let mut failed = 0usize;
        let mut prepared = Vec::with_capacity(payloads.len());
        for p in payloads {
            let site = resolve_website_key(
                precedence,
                header_site.as_deref(),
                p.website_key.as_deref(),
                &p.url,
            );
            match prepare_entry(p, site) {
                Ok(e) => prepared.push(e),
                Err(e) => {
                    failed += 1;
                    warn!("rejected cache item: {e}");
                }
            }
        }
        drop(bytes);
        if prepared.is_empty() {
            return (PutOutcome::NothingIndexed, Vec::new());
        }
        let docs: Vec<CacheIndexDoc> = if st.meili.is_some() {
            prepared
                .iter()
                .map(|e| CacheIndexDoc::from_resource(&e.resource))
                .collect()
        } else {
            Vec::new()
        };
        match st.store.commit(prepared) {
            Ok(ok) => (PutOutcome::Indexed { ok, failed }, docs),
            Err(e) => (PutOutcome::CommitFailed(e), Vec::new()),
        }
    })
    .await;

    let (outcome, docs) = match joined {
        Ok(v) => v,
        Err(e) => {
            error!("index task failed: {e}");
            return text_response(StatusCode::INTERNAL_SERVER_ERROR, "Index error");
        }
    };
    if let Some(m) = &state.meili {
        for d in docs {
            m.enqueue(d);
        }
    }
    match outcome {
        PutOutcome::BadJson(e) => {
            tracing::debug!("invalid JSON payload: {e}");
            text_response(StatusCode::BAD_REQUEST, "Invalid JSON")
        }
        PutOutcome::NothingIndexed => {
            metrics::counter!(tm::BATCH_ITEM_FAILURES).increment(1);
            if batch {
                text_response(StatusCode::INTERNAL_SERVER_ERROR, "No entries indexed")
            } else {
                text_response(StatusCode::INTERNAL_SERVER_ERROR, "Index error")
            }
        }
        PutOutcome::CommitFailed(e) => {
            error!("commit failed: {e}");
            text_response(StatusCode::INTERNAL_SERVER_ERROR, "Index error")
        }
        PutOutcome::Indexed { ok, failed } => {
            if failed > 0 {
                metrics::counter!(tm::BATCH_ITEM_FAILURES).increment(failed as u64);
            }
            if !batch {
                text_response(StatusCode::CREATED, "Indexed")
            } else if failed == 0 {
                text_response(StatusCode::CREATED, format!("Indexed {ok} entries"))
            } else {
                // Still 201: clients only check for 2xx, and the items that
                // did land should not be retried.
                text_response(
                    StatusCode::CREATED,
                    format!("Indexed {ok} entries, {failed} failed"),
                )
            }
        }
    }
}

// ---------------------------------------------------------------- reads

/// Bodies at most this big are encoded on the async worker on a mem hit.
const INLINE_ENCODE_MAX: usize = 64 * 1024;

async fn handle_resource(state: Arc<AppState>, key: String, raw: bool) -> Response<ChunkedBody> {
    // A mem cache hit needs no blocking thread; a miss reads RocksDB on one.
    let r = match state.store.get_cached(&key) {
        Some(hit) => hit,
        None => {
            let st = state.clone();
            match tokio::task::spawn_blocking(move || {
                st.store.get_resource(&key, true).map_err(|e| (key, e))
            })
            .await
            {
                Ok(Ok(Some(r))) => r,
                Ok(Ok(None)) => return text_response(StatusCode::NOT_FOUND, "Resource not found"),
                Ok(Err((key, e))) => {
                    error!("lookup of {key} failed: {e}");
                    return text_response(StatusCode::INTERNAL_SERVER_ERROR, "Lookup error");
                }
                Err(e) => {
                    error!("lookup task failed: {e}");
                    return text_response(StatusCode::INTERNAL_SERVER_ERROR, "Lookup error");
                }
            }
        }
    };

    if raw {
        // A stored Content-Type that is not a valid header value falls back
        // to octet-stream instead of failing the request.
        let ct = r
            .resource
            .content_type()
            .and_then(|v| header::HeaderValue::from_str(v).ok())
            .unwrap_or_else(|| header::HeaderValue::from_static("application/octet-stream"));
        let mut resp = Response::new(ChunkedBody::full(r.body.clone()));
        resp.headers_mut().insert(header::CONTENT_TYPE, ct);
        return resp;
    }

    // Wait for room for the JSON copy. The request timeout bounds the wait.
    let size = crate::model::base64_len(r.body.len()) + 1024;
    let reserve = (size as u64)
        .min(state.cfg.max_inflight_bytes)
        .min(u32::MAX as u64) as u32;
    let Ok(permit) = state.budget.clone().acquire_many_owned(reserve).await else {
        return text_response(StatusCode::SERVICE_UNAVAILABLE, "Shutting down");
    };
    let encoded = if r.body.len() <= INLINE_ENCODE_MAX {
        encode_payload(&r.resource, &r.body)
    } else {
        let r2 = r.clone();
        tokio::task::spawn_blocking(move || encode_payload(&r2.resource, &r2.body))
            .await
            .unwrap_or_else(|e| Err(format!("encode task failed: {e}")))
    };
    match encoded {
        Ok(json) => json_response(
            StatusCode::OK,
            ChunkedBody::new(vec![Bytes::from(json)], Some(permit)),
        ),
        Err(e) => {
            error!("serialize payload: {e}");
            text_response(StatusCode::INTERNAL_SERVER_ERROR, "Serialization error")
        }
    }
}

async fn handle_site(state: Arc<AppState>, website_key: String) -> Response<ChunkedBody> {
    let st = state.clone();
    let joined = tokio::task::spawn_blocking(move || {
        st.store.build_site_response(
            &website_key,
            st.cfg.max_site_response_bytes as usize,
            &st.budget,
        )
    })
    .await;
    match joined {
        Ok(Ok(site)) => {
            let mut resp =
                json_response(StatusCode::OK, ChunkedBody::new(site.chunks, site.permit));
            if let Some(reason) = site.truncated {
                resp.headers_mut().insert(
                    "x-cache-truncated",
                    header::HeaderValue::from_static(reason),
                );
            }
            resp
        }
        Ok(Err(e)) => {
            error!("site lookup failed: {e}");
            text_response(StatusCode::INTERNAL_SERVER_ERROR, "Lookup error")
        }
        Err(e) => {
            error!("site task failed: {e}");
            text_response(StatusCode::INTERNAL_SERVER_ERROR, "Lookup error")
        }
    }
}

async fn handle_size(state: Arc<AppState>) -> Response<ChunkedBody> {
    let st = state.clone();
    match tokio::task::spawn_blocking(move || st.store.size_report()).await {
        Ok(Ok(report)) => {
            let json = serde_json::to_vec(&report).unwrap_or_else(|_| b"{}".to_vec());
            json_response(StatusCode::OK, ChunkedBody::full(json))
        }
        Ok(Err(e)) => {
            error!("cache size failed: {e}");
            text_response(StatusCode::INTERNAL_SERVER_ERROR, "Cache size error")
        }
        Err(e) => {
            error!("cache size task failed: {e}");
            text_response(StatusCode::INTERNAL_SERVER_ERROR, "Cache size error")
        }
    }
}

fn handle_purge_empty(state: Arc<AppState>) -> Response<ChunkedBody> {
    // Runs detached so it survives the client hanging up.
    tokio::spawn(async move {
        let st = state.clone();
        match tokio::task::spawn_blocking(move || st.store.purge_empty()).await {
            Ok(Ok(r)) => {
                tracing::info!(
                    "purge_empty done: removed {} resources, {} bodies",
                    r.purged_resources,
                    r.purged_files
                );
                if let Some(m) = &state.meili {
                    m.delete(&r.resource_keys).await;
                }
            }
            Ok(Err(e)) => error!("purge_empty failed: {e}"),
            Err(e) => error!("purge_empty task failed: {e}"),
        }
    });
    json_response(
        StatusCode::ACCEPTED,
        ChunkedBody::full(
            &br#"{"status":"started","message":"Purge running in background. Check logs for results."}"#[..],
        ),
    )
}

fn handle_health(state: &AppState) -> Response<ChunkedBody> {
    match state.store.health() {
        Ok(_) => text_response(StatusCode::OK, "ok"),
        Err(e) => {
            error!("health check failed: {e}");
            text_response(StatusCode::SERVICE_UNAVAILABLE, "rocksdb unavailable")
        }
    }
}

fn handle_metrics(state: &AppState) -> Response<ChunkedBody> {
    let Some(handle) = &state.prometheus else {
        return text_response(StatusCode::NOT_FOUND, "metrics disabled");
    };
    let mem = &state.store.mem;
    metrics::gauge!(tm::MEM_BYTES).set(mem.weighted_size() as f64);
    metrics::gauge!(tm::MEM_ENTRIES).set(mem.entry_count() as f64);
    metrics::gauge!(tm::INFLIGHT_AVAILABLE).set(state.budget.available_permits() as f64);
    tm::record_allocator();
    let mut r = Response::new(ChunkedBody::full(handle.render()));
    r.headers_mut().insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static("text/plain; version=0.0.4"),
    );
    r
}
