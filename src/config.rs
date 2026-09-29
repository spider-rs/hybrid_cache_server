//! Runtime configuration, read once from the environment at startup.

use std::time::Duration;

const MIB: u64 = 1024 * 1024;

#[derive(Debug, Clone)]
pub struct Config {
    pub port: u16,
    pub max_connections: usize,
    /// Largest accepted request body. Bigger bodies get 413.
    pub max_body_bytes: u64,
    /// Budget for request and response buffers held at once, in bytes.
    pub max_inflight_bytes: u64,
    /// Largest /cache/site response. Items past it are left out.
    pub max_site_response_bytes: u64,
    /// Deadline for GET and purge requests.
    pub request_timeout: Duration,
    /// Deadline for POST /cache/index*, which includes the upload.
    pub upload_timeout: Duration,
    pub header_read_timeout: Duration,
    /// A connection whose peer accepts no bytes for this long is closed, so
    /// a stalled reader cannot hold response buffers.
    pub write_idle_timeout: Duration,
    /// TCP keepalive idle time and TCP_USER_TIMEOUT (Linux) on accepted sockets.
    pub tcp_timeout: Duration,
    /// Reservations up to this size come from the small pool.
    pub small_request_bytes: u64,
    /// Separate budget for small reservations, so they never queue behind a
    /// large write waiting on the main budget.
    pub max_inflight_small_bytes: u64,
    /// Blocking-pool tasks for reads (mem cache misses, site scans, large
    /// encodes) at once.
    pub max_blocking_reads: usize,
    /// Byte budget of the in-memory resource cache.
    pub mem_cache_bytes: u64,
    pub rocksdb_path: String,
    pub rocksdb_block_cache_bytes: u64,
    pub rocksdb_max_open_files: i32,
    pub rocksdb_blob_files: bool,
    pub rocksdb_rate_limit_bytes: i64,
    pub compression: Compression,
    pub compression_level: i32,
    pub meili: Option<MeiliConfig>,
    pub cache_ttl_secs: i64,
    pub cleanup_interval: Duration,
    /// Run a full scan of stored file bodies every N cleanup passes.
    pub full_sweep_every: u64,
}

#[derive(Debug, Clone)]
pub struct MeiliConfig {
    pub host: String,
    pub key: String,
    pub index: String,
    pub queue_cap: usize,
    pub batch_max: usize,
    pub flush_every: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    Off,
    Gzip,
    Br,
    Zstd,
}

pub fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(default)
}

pub fn env_bool(name: &str, default: bool) -> bool {
    std::env::var(name)
        .ok()
        .map(|v| {
            let v = v.trim();
            v == "1" || v.eq_ignore_ascii_case("true") || v.eq_ignore_ascii_case("yes")
        })
        .unwrap_or(default)
}

fn env_str(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

impl Config {
    pub fn from_env() -> Self {
        // Meilisearch is off unless MEILI_ENABLE=1. MEILI_DISABLE=1 still wins,
        // so an old unit file that sets it keeps working.
        let meili = if env_bool("MEILI_ENABLE", false) && !env_bool("MEILI_DISABLE", false) {
            Some(MeiliConfig {
                host: env_str("MEILI_HOST", "http://127.0.0.1:7700"),
                key: env_str("MEILI_MASTER_KEY", "masterKey"),
                index: env_str("MEILI_INDEX", "hybrid_cache"),
                queue_cap: env_u64("MEILI_QUEUE_CAP", 10_000).max(1) as usize,
                batch_max: env_u64("MEILI_BATCH_MAX", 256).max(1) as usize,
                flush_every: Duration::from_millis(env_u64("MEILI_FLUSH_MS", 200).max(10)),
            })
        } else {
            None
        };

        // zstd level 1 had the lowest CPU per byte saved on base64 JSON in
        // the loadgen (see BENCHMARKS.md); gzip and brotli cost 2x to 6x more.
        let compression = match env_str("COMPRESSION", "zstd").to_ascii_lowercase().as_str() {
            "off" | "none" | "0" => Compression::Off,
            "br" | "brotli" => Compression::Br,
            "gzip" => Compression::Gzip,
            _ => Compression::Zstd,
        };

        Config {
            port: std::env::var("CACHE_PORT")
                .ok()
                .and_then(|p| p.trim().parse().ok())
                .unwrap_or(8080),
            max_connections: env_u64("MAX_CONNECTIONS", 10_000).max(1) as usize,
            // The fleet's largest request is 16 bodies of 5 MiB as base64
            // JSON, about 107 MiB, which v0.2 accepted.
            max_body_bytes: env_u64("MAX_BODY_BYTES", 160 * MIB).max(1024),
            max_inflight_bytes: env_u64("MAX_INFLIGHT_BYTES", 1024 * MIB).max(MIB),
            max_site_response_bytes: env_u64("MAX_SITE_RESPONSE_BYTES", 64 * MIB).max(1024),
            request_timeout: Duration::from_secs(env_u64("REQUEST_TIMEOUT_SECS", 30).max(1)),
            upload_timeout: Duration::from_secs(env_u64("UPLOAD_TIMEOUT_SECS", 120).max(1)),
            write_idle_timeout: Duration::from_secs(env_u64("WRITE_IDLE_TIMEOUT_SECS", 30).max(1)),
            tcp_timeout: Duration::from_secs(env_u64("TCP_TIMEOUT_SECS", 30).max(1)),
            small_request_bytes: env_u64("SMALL_REQUEST_BYTES", 8 * MIB),
            max_inflight_small_bytes: env_u64("MAX_INFLIGHT_SMALL_BYTES", 128 * MIB).max(MIB),
            max_blocking_reads: env_u64("MAX_BLOCKING_READS", 64).max(1) as usize,
            header_read_timeout: Duration::from_secs(
                env_u64("HEADER_READ_TIMEOUT_SECS", 120).max(1),
            ),
            mem_cache_bytes: env_u64("MEM_CACHE_BYTES", 512 * MIB),
            rocksdb_path: env_str("ROCKSDB_PATH", "cache_db"),
            rocksdb_block_cache_bytes: env_u64("ROCKSDB_BLOCK_CACHE_BYTES", 512 * MIB).max(MIB),
            rocksdb_max_open_files: env_u64("ROCKSDB_MAX_OPEN_FILES", 4096).min(i32::MAX as u64)
                as i32,
            rocksdb_blob_files: env_bool("ROCKSDB_BLOB_FILES", true),
            rocksdb_rate_limit_bytes: env_u64("ROCKSDB_RATE_LIMIT_BYTES", 256 * MIB)
                .min(i64::MAX as u64) as i64,
            compression,
            compression_level: std::env::var("COMPRESSION_LEVEL")
                .ok()
                .and_then(|v| v.trim().parse().ok())
                .unwrap_or(1),
            meili,
            cache_ttl_secs: std::env::var("CACHE_TTL_SECS")
                .ok()
                .and_then(|s| s.trim().parse().ok())
                .unwrap_or(60 * 60 * 24),
            cleanup_interval: Duration::from_secs(
                env_u64("CACHE_CLEANUP_INTERVAL_SECS", 600).max(1),
            ),
            full_sweep_every: env_u64("CACHE_FULL_SWEEP_EVERY", 144).max(1),
        }
    }

    /// Defaults with no environment, for tests.
    #[cfg(test)]
    pub fn for_tests(path: &str) -> Self {
        Config {
            port: 0,
            max_connections: 100,
            max_body_bytes: 160 * MIB,
            max_inflight_bytes: 1024 * MIB,
            max_site_response_bytes: 64 * MIB,
            request_timeout: Duration::from_secs(30),
            upload_timeout: Duration::from_secs(120),
            write_idle_timeout: Duration::from_secs(30),
            tcp_timeout: Duration::from_secs(30),
            small_request_bytes: 8 * MIB,
            max_inflight_small_bytes: 128 * MIB,
            max_blocking_reads: 64,
            header_read_timeout: Duration::from_secs(120),
            mem_cache_bytes: 64 * MIB,
            rocksdb_path: path.to_string(),
            rocksdb_block_cache_bytes: 8 * MIB,
            rocksdb_max_open_files: 256,
            rocksdb_blob_files: true,
            rocksdb_rate_limit_bytes: 0,
            compression: Compression::Zstd,
            compression_level: 1,
            meili: None,
            cache_ttl_secs: 60 * 60 * 24,
            cleanup_interval: Duration::from_secs(600),
            full_sweep_every: 144,
        }
    }
}
