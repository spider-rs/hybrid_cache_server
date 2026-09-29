//! Prometheus metrics. Every metric the server emits is named here.

use std::time::{Duration, Instant};

use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};

pub const HTTP_REQUESTS: &str = "http_requests_total";
pub const HTTP_DURATION: &str = "http_request_duration_seconds";
pub const HTTP_REQ_BYTES: &str = "http_request_body_bytes_total";
pub const HTTP_RESP_BYTES: &str = "http_response_body_bytes_total";
pub const HTTP_CONNS: &str = "http_connections_in_flight";
pub const HTTP_ACCEPT_ERRORS: &str = "http_accept_errors_total";
pub const HTTP_CONN_ERRORS: &str = "http_connection_errors_total";
pub const BATCH_ITEM_FAILURES: &str = "batch_item_failures_total";

pub const MEM_HITS: &str = "mem_cache_hits_total";
pub const MEM_MISSES: &str = "mem_cache_misses_total";
pub const MEM_BYTES: &str = "mem_cache_bytes";
pub const MEM_ENTRIES: &str = "mem_cache_entries";

pub const INFLIGHT_AVAILABLE: &str = "inflight_budget_available_bytes";
pub const INFLIGHT_WAIT: &str = "inflight_budget_wait_seconds";
pub const SITE_TRUNCATED: &str = "site_response_truncated_total";

pub const DB_READ: &str = "rocksdb_read_seconds";
pub const DB_WRITE: &str = "rocksdb_write_seconds";
pub const DB_FILE_PUT_SKIPPED: &str = "rocksdb_file_put_skipped_total";
pub const DB_FILE_PUT: &str = "rocksdb_file_put_total";

pub const MEILI_DROPPED: &str = "meili_dropped_total";

pub const CLEANUP_DURATION: &str = "cleanup_duration_seconds";
pub const CLEANUP_REMOVED_RESOURCES: &str = "cleanup_removed_resources_total";
pub const CLEANUP_REMOVED_FILES: &str = "cleanup_removed_files_total";

pub const JEMALLOC_ALLOCATED: &str = "jemalloc_allocated_bytes";
pub const JEMALLOC_ACTIVE: &str = "jemalloc_active_bytes";
pub const JEMALLOC_RESIDENT: &str = "jemalloc_resident_bytes";
pub const JEMALLOC_RETAINED: &str = "jemalloc_retained_bytes";

/// Refresh allocator gauges. allocated is what the program holds; resident
/// minus allocated is what the allocator keeps cached.
#[cfg(not(target_env = "msvc"))]
pub fn record_allocator() {
    use tikv_jemalloc_ctl::{epoch, stats};
    if epoch::advance().is_err() {
        return;
    }
    let set = |name: &'static str, v: Result<usize, tikv_jemalloc_ctl::Error>| {
        if let Ok(v) = v {
            metrics::gauge!(name).set(v as f64);
        }
    };
    set(JEMALLOC_ALLOCATED, stats::allocated::read());
    set(JEMALLOC_ACTIVE, stats::active::read());
    set(JEMALLOC_RESIDENT, stats::resident::read());
    set(JEMALLOC_RETAINED, stats::retained::read());
}

#[cfg(target_env = "msvc")]
pub fn record_allocator() {}

/// The allocator's page decay settings, for the startup log.
#[cfg(not(target_env = "msvc"))]
pub fn allocator_decay() -> String {
    let read = |k: &[u8]| unsafe { tikv_jemalloc_ctl::raw::read::<isize>(k) };
    match (read(b"opt.dirty_decay_ms\0"), read(b"opt.muzzy_decay_ms\0")) {
        (Ok(d), Ok(m)) => format!("jemalloc dirty_decay_ms={d} muzzy_decay_ms={m}"),
        _ => "jemalloc decay unknown".to_string(),
    }
}

#[cfg(target_env = "msvc")]
pub fn allocator_decay() -> String {
    "system allocator".to_string()
}

/// Install the global recorder. Returns None if one is already installed
/// (tests), in which case metrics calls are no-ops.
pub fn install() -> Option<PrometheusHandle> {
    let buckets = [
        0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
    ];
    let builder = PrometheusBuilder::new().set_buckets(&buckets).ok()?;
    let handle = builder.install_recorder().ok()?;
    let upkeep = handle.clone();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(10));
        loop {
            tick.tick().await;
            upkeep.run_upkeep();
        }
    });
    Some(handle)
}

/// Record how long a closure took into a histogram.
#[inline]
pub fn timed<T>(name: &'static str, f: impl FnOnce() -> T) -> T {
    let t = Instant::now();
    let out = f();
    metrics::histogram!(name).record(t.elapsed().as_secs_f64());
    out
}
