use std::{
    collections::{HashMap, HashSet},
    convert::Infallible,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use base64::engine::general_purpose;
use base64::Engine;
use bytes::Bytes;
use dashmap::DashMap;
use http_body_util::{combinators::BoxBody, BodyExt, Full};
use hyper::{body::Incoming, service::service_fn, Request, Response, StatusCode};
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto::Builder as AutoBuilder,
};
use meilisearch_sdk::client::Client as MeiliClient;
use rocksdb::{Direction, IteratorMode, DB};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

type ResponseBody = BoxBody<Bytes, hyper::Error>;

#[derive(Clone)]
struct AppState {
    db: Arc<DB>,
    meili_client: MeiliClient,
    index_name: String,
    /// In-memory resource cache keyed by resource_key for fast lookups.
    mem_resources: Arc<DashMap<String, ResourceWithBody>>,
}

/// Resource metadata, without the actual body.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ResourceEntry {
    website_key: String,
    resource_key: String,
    url: String,
    method: String,
    status: u16,
    request_headers: HashMap<String, String>,
    response_headers: HashMap<String, String>,
    /// ID of the file payload stored separately, deduped by content.
    file_id: String,
    /// When this resource was cached (unix timestamp, seconds).
    #[serde(default)]
    created_at: Option<i64>,
}

/// Stored file payload (deduped by file_id).
#[derive(Debug, Clone, Serialize, Deserialize)]
struct FileEntry {
    file_id: String,
    body: Vec<u8>,
}

/// In-memory combined view (resource + body).
#[derive(Debug, Clone)]
struct ResourceWithBody {
    resource: ResourceEntry,
    body: Vec<u8>,
}

/// Shape your HTTP API uses over the wire.
///
/// - `website_key` – which website this belongs to
///   - can also come from the `X-Cache-Site` header
///   - example: `"example.com"` or `"https://example.com"`
/// - `resource_key` – your unique cache key per resource (from put_hybrid_cache)
///   - example:
///       "GET::https://example.com/style.css"
///       "GET::https://cdn.example.com/jquery.js::Accept:text/javascript"
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CachedEntryPayload {
    /// Website-level key (optional in payload; we can also accept header or derive from URL).
    #[serde(default)]
    website_key: Option<String>,
    /// Unique per-resource cache key (matches your `cache_key` from put_hybrid_cache).
    resource_key: String,
    url: String,
    method: String,
    status: u16,
    request_headers: HashMap<String, String>,
    response_headers: HashMap<String, String>,
    /// HTTP response body as base64-encoded bytes.
    body_base64: String,
}

/// Minimal document indexed in Meilisearch for search/lookup.
#[derive(Debug, Serialize, Deserialize)]
struct CacheIndexDoc {
    website_key: String,
    resource_key: String,
    url: String,
    status: u16,
    content_type: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    // ---- RocksDB ----
    let db = Arc::new(DB::open_default("cache_db")?);

    // ---- Meilisearch ----
    let meili_host = std::env::var("MEILI_HOST").unwrap_or_else(|_| "http://127.0.0.1:7700".into());
    let meili_key = std::env::var("MEILI_MASTER_KEY").unwrap_or_else(|_| "masterKey".into());
    let index_name = std::env::var("MEILI_INDEX").unwrap_or_else(|_| "hybrid_cache".into());
    let port = std::env::var("CACHE_PORT")
        .unwrap_or_else(|_| "8080".into())
        .parse::<u16>()
        .unwrap_or_default();

    let meili_client = MeiliClient::new(
        meili_host,
        if meili_key.is_empty() {
            None
        } else {
            Some(meili_key)
        },
    );

    // Ensure index exists with primary key = resource_key.
    {
        let mut idx = meili_client.index(&index_name);
        if idx.fetch_info().await.is_err() {
            let _ = meili_client
                .create_index(&index_name, Some("resource_key"))
                .await;
        }
    }

    let state = Arc::new(AppState {
        db,
        meili_client,
        index_name,
        mem_resources: Arc::new(DashMap::new()),
    });

    {
        let cleanup_state = state.clone();
        tokio::spawn(async move {
            run_cleanup_worker(cleanup_state).await;
        });
    }

    let addr: SocketAddr = ([0, 0, 0, 0], port).into();
    info!("Listening on http://{}", addr);

    let listener = TcpListener::bind(addr).await?;
    loop {
        let (stream, peer) = listener.accept().await?;
        info!("Accepted connection from {}", peer);

        let io = TokioIo::new(stream);
        let state = state.clone();

        tokio::spawn(async move {
            let svc = service_fn(move |req: Request<Incoming>| {
                let state = state.clone();
                async move { handle(req, state).await }
            });

            if let Err(err) = AutoBuilder::new(TokioExecutor::new())
                .serve_connection(io, svc)
                .await
            {
                error!("Error serving connection: {:?}", err);
            }
        });
    }
}

/// Top-level router.
async fn handle(
    req: Request<Incoming>,
    state: Arc<AppState>,
) -> Result<Response<ResponseBody>, hyper::Error> {
    let path = req.uri().path().to_owned();
    let segments: Vec<&str> = path.trim_start_matches('/').split('/').collect();
    match (req.method().as_str(), segments.as_slice()) {
        // POST /cache/index  (single item)
        ("POST", ["cache", "index"]) => handle_put_index(req, state).await,
        // POST /cache/index/batch  (batch)
        ("POST", ["cache", "index", "batch"]) => handle_put_index_batch(req, state).await,
        // GET /cache/resource/{resource_key}
        // Optional query: ?raw=1 or ?format=bytes/raw -> raw bytes instead of JSON/base64.
        ("GET", ["cache", "resource", key]) => {
            handle_resource_lookup(req, state, key.to_string()).await
        }
        // GET /cache/site/{website_key}
        ("GET", ["cache", "site", website_key]) => handle_site_lookup(website_key, state).await,
        _ => Ok(text_response(StatusCode::NOT_FOUND, "Not Found")),
    }
}

/// POST /cache/index – index a single resource.
async fn handle_put_index(
    req: Request<Incoming>,
    state: Arc<AppState>,
) -> Result<Response<ResponseBody>, hyper::Error> {
    let (parts, body) = req.into_parts();
    let headers = parts.headers;

    let whole_body = match body.collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(e) => {
            error!("Failed to read request body: {e}");
            return Ok(text_response(StatusCode::BAD_REQUEST, "Invalid body"));
        }
    };

    let payload: CachedEntryPayload = match serde_json::from_slice(&whole_body) {
        Ok(p) => p,
        Err(e) => {
            error!("Failed to parse JSON payload: {e}");
            return Ok(text_response(StatusCode::BAD_REQUEST, "Invalid JSON"));
        }
    };

    // Optional special header to attach website to this resource.
    // Example: X-Cache-Site: example.com
    let header_site_key = headers
        .get("x-cache-site")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    match index_single_entry(payload, header_site_key, state.clone()).await {
        Ok(_) => Ok(text_response(StatusCode::CREATED, "Indexed")),
        Err(e) => {
            error!("Failed to index single entry: {e}");
            Ok(text_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Index error",
            ))
        }
    }
}

/// POST /cache/index/batch – index a batch of resources.
async fn handle_put_index_batch(
    req: Request<Incoming>,
    state: Arc<AppState>,
) -> Result<Response<ResponseBody>, hyper::Error> {
    let (parts, body) = req.into_parts();
    let headers = parts.headers;

    let whole_body = match body.collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(e) => {
            error!("Failed to read request body: {e}");
            return Ok(text_response(StatusCode::BAD_REQUEST, "Invalid body"));
        }
    };

    let payloads: Vec<CachedEntryPayload> = match serde_json::from_slice(&whole_body) {
        Ok(p) => p,
        Err(e) => {
            error!("Failed to parse batch JSON payload: {e}");
            return Ok(text_response(StatusCode::BAD_REQUEST, "Invalid JSON"));
        }
    };

    let header_site_key = headers
        .get("x-cache-site")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    let mut success = 0usize;
    for payload in payloads {
        match index_single_entry(payload, header_site_key.clone(), state.clone()).await {
            Ok(_) => success += 1,
            Err(e) => {
                error!("Failed to index entry in batch: {e}");
            }
        }
    }

    if success == 0 {
        Ok(text_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "No entries indexed",
        ))
    } else {
        Ok(text_response(
            StatusCode::CREATED,
            format!("Indexed {} entries", success),
        ))
    }
}

/// GET /cache/resource/{resource_key}
///
/// - Default: JSON payload with base64 body (for Chrome, etc).
/// - If `?raw=1` or `?format=bytes|raw`: raw bytes with original Content-Type.
async fn handle_resource_lookup(
    req: Request<Incoming>,
    state: Arc<AppState>,
    resource_key: String,
) -> Result<Response<ResponseBody>, hyper::Error> {
    let wants_raw = {
        let query = req.uri().query().unwrap_or("");
        query.split('&').any(|pair| {
            let mut parts = pair.splitn(2, '=');
            match (parts.next(), parts.next()) {
                (Some("raw"), Some(v)) if v == "1" || v.eq_ignore_ascii_case("true") => true,
                (Some("format"), Some(v))
                    if v.eq_ignore_ascii_case("bytes") || v.eq_ignore_ascii_case("raw") =>
                {
                    true
                }
                _ => false,
            }
        })
    };

    let res = match get_resource_with_body(&state, &resource_key) {
        Ok(Some(res)) => res,
        Ok(None) => {
            return Ok(text_response(StatusCode::NOT_FOUND, "Resource not found"));
        }
        Err(e) => {
            error!("Error loading resource {}: {}", resource_key, e);
            return Ok(text_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Lookup error",
            ));
        }
    };

    if wants_raw {
        // Raw bytes response, use cached Content-Type if present.
        let content_type = res
            .resource
            .response_headers
            .get("Content-Type")
            .or_else(|| res.resource.response_headers.get("content-type"))
            .map(|s| s.as_str())
            .unwrap_or("application/octet-stream");

        let body = Full::from(Bytes::from(res.body))
            .map_err(|never: Infallible| match never {})
            .boxed();

        return Ok(Response::builder()
            .status(StatusCode::OK)
            .header("content-type", content_type)
            .body(body)
            .unwrap());
    }

    // Default JSON + base64 mode
    let payload = resource_with_body_to_payload(&res);
    let json = match serde_json::to_vec(&payload) {
        Ok(v) => v,
        Err(e) => {
            error!("Failed to serialize CachedEntryPayload: {e}");
            return Ok(text_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Serialization error",
            ));
        }
    };

    Ok(json_response(StatusCode::OK, json))
}

/// GET /cache/site/{website_key}
///
/// Returns all resources attached to this website as JSON with base64 bodies.
async fn handle_site_lookup(
    website_key: &str,
    state: Arc<AppState>,
) -> Result<Response<ResponseBody>, hyper::Error> {
    let prefix = format!("site:{}::", website_key);
    let mut resources = Vec::new();

    let iter = state
        .db
        .iterator(IteratorMode::From(prefix.as_bytes(), Direction::Forward));

    for item in iter {
        match item {
            Ok((key_bytes, _value)) => {
                if !key_bytes.starts_with(prefix.as_bytes()) {
                    break;
                }

                // Key is "site:{website_key}::{resource_key}"
                let suffix = &key_bytes[prefix.len()..];
                let resource_key = match std::str::from_utf8(suffix) {
                    Ok(s) => s.to_string(),
                    Err(e) => {
                        error!("Invalid UTF-8 in site index key: {e}");
                        continue;
                    }
                };

                match get_resource_with_body(&state, &resource_key) {
                    Ok(Some(res)) => {
                        let payload = resource_with_body_to_payload(&res);
                        resources.push(payload);
                    }
                    Ok(None) => {
                        error!("Site index refers to missing resource_key {}", resource_key);
                    }
                    Err(e) => {
                        error!("Failed to load resource {}: {}", resource_key, e);
                    }
                }
            }
            Err(e) => {
                error!("RocksDB iterator error: {}", e);
                break;
            }
        }
    }

    let json = match serde_json::to_vec(&resources) {
        Ok(v) => v,
        Err(e) => {
            error!("Failed to serialize site resource list: {e}");
            return Ok(text_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Serialization error",
            ));
        }
    };

    Ok(json_response(StatusCode::OK, json))
}

/// Core logic to index a single entry:
/// - figure out website_key (header, payload, or derived from URL)
/// - compute file_id = hash(body)
/// - store body once per file_id
/// - store ResourceEntry per (website_key, resource_key)
/// - add site index entry for fast per-website lookups
/// - index in Meilisearch
async fn index_single_entry(
    payload: CachedEntryPayload,
    header_site_key: Option<String>,
    state: Arc<AppState>,
) -> Result<(), String> {
    let body_bytes = general_purpose::STANDARD
        .decode(&payload.body_base64)
        .map_err(|e| format!("Invalid base64 body: {e}"))?;

    // Determine website_key:
    // 1) X-Cache-Site header (if provided)
    // 2) payload.website_key
    // 3) derive from URL (host)
    let website_key = header_site_key
        .or(payload.website_key.clone())
        .unwrap_or_else(|| derive_website_key_from_url(&payload.url));
    let website_key = if website_key.is_empty() {
        derive_website_key_from_url(&payload.url)
    } else {
        website_key
    };
    let file_id = compute_file_id(&body_bytes);

    let resource = ResourceEntry {
        website_key: website_key.clone(),
        resource_key: payload.resource_key.clone(),
        url: payload.url.clone(),
        method: payload.method.clone(),
        status: payload.status,
        request_headers: payload.request_headers.clone(),
        response_headers: payload.response_headers.clone(),
        file_id: file_id.clone(),
        created_at: Some(now_unix_timestamp()),
    };

    // --- In-memory cache (resource + body) ---
    state.mem_resources.insert(
        resource.resource_key.clone(),
        ResourceWithBody {
            resource: resource.clone(),
            body: body_bytes.clone(),
        },
    );

    // --- RocksDB: file payload (dedup by file_id) ---
    let file_key = format!("file:{}", file_id);

    if state
        .db
        .get(file_key.as_bytes())
        .map_err(|e| format!("RocksDB get file error: {e}"))?
        .is_none()
    {
        let file_entry = FileEntry {
            file_id: file_id.clone(),
            body: body_bytes,
        };
        let file_bytes = serde_json::to_vec(&file_entry)
            .map_err(|e| format!("Serialize FileEntry error: {e}"))?;

        state
            .db
            .put(file_key.as_bytes(), file_bytes)
            .map_err(|e| format!("RocksDB put file error: {e}"))?;
    }

    // --- RocksDB: resource metadata ---
    let res_key = format!("res:{}", resource.resource_key);
    let res_bytes =
        serde_json::to_vec(&resource).map_err(|e| format!("Serialize ResourceEntry error: {e}"))?;

    state
        .db
        .put(res_key.as_bytes(), res_bytes)
        .map_err(|e| format!("RocksDB put resource error: {e}"))?;

    // --- RocksDB: site index entry site:{website_key}::{resource_key} ---
    let site_key = format!("site:{}::{}", website_key, resource.resource_key);

    state
        .db
        .put(site_key.as_bytes(), b"")
        .map_err(|e| format!("RocksDB put site index error: {e}"))?;

    // --- Meilisearch index ---
    let content_type = resource
        .response_headers
        .get("content-type")
        .cloned()
        .or_else(|| resource.response_headers.get("Content-Type").cloned());

    let doc = CacheIndexDoc {
        website_key,
        resource_key: resource.resource_key.clone(),
        url: resource.url.clone(),
        status: resource.status,
        content_type,
    };

    let index = state.meili_client.index(&state.index_name);

    if let Err(e) = index.add_documents(&[doc], Some("resource_key")).await {
        // treat Meili failures as non-fatal for the cache
        error!("Failed to index document in Meilisearch: {}", e);
    }

    Ok(())
}

/// Get a ResourceWithBody by resource_key:
/// - check in-memory first
/// - then load ResourceEntry + FileEntry from RocksDB
fn get_resource_with_body(
    state: &AppState,
    resource_key: &str,
) -> Result<Option<ResourceWithBody>, String> {
    if let Some(res) = state.mem_resources.get(resource_key) {
        return Ok(Some(res.clone()));
    }

    let res_key = format!("res:{}", resource_key);
    let res_bytes_opt = state
        .db
        .get(res_key.as_bytes())
        .map_err(|e| format!("RocksDB get resource error: {e}"))?;

    let Some(res_bytes) = res_bytes_opt else {
        return Ok(None);
    };

    let resource: ResourceEntry = serde_json::from_slice(&res_bytes)
        .map_err(|e| format!("Deserialize ResourceEntry error: {e}"))?;

    let file_key = format!("file:{}", resource.file_id);
    let file_bytes_opt = state
        .db
        .get(file_key.as_bytes())
        .map_err(|e| format!("RocksDB get file error: {e}"))?;

    let Some(file_bytes) = file_bytes_opt else {
        return Err(format!(
            "FileEntry missing for file_id {}",
            resource.file_id
        ));
    };

    let file_entry: FileEntry = serde_json::from_slice(&file_bytes)
        .map_err(|e| format!("Deserialize FileEntry error: {e}"))?;

    let combined = ResourceWithBody {
        resource: resource.clone(),
        body: file_entry.body.clone(),
    };

    // back-fill in-memory cache
    state
        .mem_resources
        .insert(resource_key.to_string(), combined.clone());

    Ok(Some(combined))
}

/// Convert internal `ResourceWithBody` into API payload (JSON with base64 body).
fn resource_with_body_to_payload(res: &ResourceWithBody) -> CachedEntryPayload {
    CachedEntryPayload {
        website_key: Some(res.resource.website_key.clone()),
        resource_key: res.resource.resource_key.clone(),
        url: res.resource.url.clone(),
        method: res.resource.method.clone(),
        status: res.resource.status,
        request_headers: res.resource.request_headers.clone(),
        response_headers: res.resource.response_headers.clone(),
        body_base64: general_purpose::STANDARD.encode(&res.body),
    }
}

/// Compute a stable file_id hash for deduplication of shared assets (e.g. CDNs).
fn compute_file_id(body: &[u8]) -> String {
    let hash = blake3::hash(body);
    hex::encode(hash.as_bytes())
}

/// Derive a website_key from URL if none is provided.
///
/// For example: "https://example.com/path" -> "example.com"
fn derive_website_key_from_url(url_str: &str) -> String {
    match url::Url::parse(url_str) {
        Ok(url) => url.host_str().unwrap_or("unknown").to_string(),
        Err(_) => "unknown".to_string(),
    }
}

/// Helper to build a plain text response.
fn text_response(status: StatusCode, text: impl Into<String>) -> Response<ResponseBody> {
    let body = Full::from(Bytes::from(text.into()))
        .map_err(|never: Infallible| match never {})
        .boxed();

    Response::builder()
        .status(status)
        .header("content-type", "text/plain; charset=utf-8")
        .body(body)
        .unwrap()
}

/// Helper to build a JSON response from pre-serialized bytes.
fn json_response(status: StatusCode, json: Vec<u8>) -> Response<ResponseBody> {
    let body = Full::from(Bytes::from(json))
        .map_err(|never: Infallible| match never {})
        .boxed();

    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(body)
        .unwrap()
}

fn now_unix_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn cache_ttl_secs() -> i64 {
    std::env::var("CACHE_TTL_SECS")
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(60 * 60 * 24) // default: 24 hours
}

fn cleanup_interval() -> Duration {
    let secs = std::env::var("CACHE_CLEANUP_INTERVAL_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(60 * 10); // default: 10 minutes
    Duration::from_secs(secs)
}

fn do_rocksdb_cleanup(
    state: &AppState,
    ttl_secs: i64,
    now: i64,
) -> Result<(Vec<String>, Vec<String>), String> {
    let mut expired_resource_keys = Vec::new();
    let mut expired_site_keys: Vec<Vec<u8>> = Vec::new();
    let mut expired_file_ids = Vec::new();
    let mut live_file_ids = HashSet::new();

    let iter = state
        .db
        .iterator(IteratorMode::From(b"res:", Direction::Forward));

    for item in iter {
        let (key, value) = item.map_err(|e| format!("RocksDB iterator error: {e}"))?;
        if !key.starts_with(b"res:") {
            break;
        }

        let resource: ResourceEntry = match serde_json::from_slice(&value) {
            Ok(r) => r,
            Err(e) => {
                error!("Failed to deserialize ResourceEntry during cleanup: {e}");
                continue;
            }
        };

        let created_at = resource.created_at.unwrap_or(now);
        if now - created_at > ttl_secs {
            let resource_key = resource.resource_key.clone();
            expired_resource_keys.push(resource_key.clone());
            expired_file_ids.push(resource.file_id.clone());

            let site_key = format!("site:{}::{}", resource.website_key, resource_key);
            expired_site_keys.push(site_key.into_bytes());
        } else {
            live_file_ids.insert(resource.file_id.clone());
        }
    }

    // Delete expired resource metadata
    for res_key in &expired_resource_keys {
        let key = format!("res:{}", res_key);
        if let Err(e) = state.db.delete(key.as_bytes()) {
            error!("Failed to delete resource key {}: {}", res_key, e);
        }
    }

    // Delete site index entries
    for site_key in &expired_site_keys {
        if let Err(e) = state.db.delete(site_key) {
            error!("Failed to delete site key during cleanup: {}", e);
        }
    }

    // Compute orphaned file IDs: those used only by expired resources
    let mut orphaned_file_ids = Vec::new();
    for file_id in expired_file_ids {
        if !live_file_ids.contains(&file_id) {
            orphaned_file_ids.push(file_id);
        }
    }

    // Delete orphaned file:* entries
    for file_id in &orphaned_file_ids {
        let file_key = format!("file:{}", file_id);
        if let Err(e) = state.db.delete(file_key.as_bytes()) {
            error!("Failed to delete file entry {}: {}", file_id, e);
        }
    }

    Ok((expired_resource_keys, orphaned_file_ids))
}

/// A cleanup worker script
async fn run_cleanup_worker(state: Arc<AppState>) {
    loop {
        let interval = cleanup_interval();
        tokio::time::sleep(interval).await;

        let ttl = cache_ttl_secs();
        let now = now_unix_timestamp();
        let state_blocking = state.clone();

        let result =
            tokio::task::spawn_blocking(move || do_rocksdb_cleanup(&state_blocking, ttl, now))
                .await;

        match result {
            Ok(Ok((expired_resource_keys, orphaned_file_ids))) => {
                for key in &expired_resource_keys {
                    state.mem_resources.remove(key);
                }

                if !expired_resource_keys.is_empty() {
                    info!(
                        "Cache cleanup removed {} resources and {} file bodies",
                        expired_resource_keys.len(),
                        orphaned_file_ids.len()
                    );
                }

                // Remove docs from Meilisearch (best-effort)
                if !expired_resource_keys.is_empty() {
                    let index = state.meili_client.index(&state.index_name);
                    if let Err(e) = index.delete_documents(&expired_resource_keys).await {
                        error!("Failed to delete documents from Meilisearch: {}", e);
                    }
                }
            }
            Ok(Err(e)) => {
                error!("Cache cleanup error: {}", e);
            }
            Err(e) => {
                error!("Cache cleanup join error: {}", e);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{collections::HashMap, sync::Arc};
    use tempfile::tempdir;

    /// Create an AppState that uses a temporary RocksDB directory.
    fn create_test_state() -> Arc<AppState> {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().to_path_buf();
        std::mem::forget(dir);

        let db = Arc::new(DB::open_default(path).expect("open rocksdb"));

        let meili_client = MeiliClient::new(
            "http://127.0.0.1:7700".to_string(),
            Some("testMasterKey".to_string()),
        );

        Arc::new(AppState {
            db,
            meili_client,
            index_name: "test_index".to_string(),
            mem_resources: Arc::new(DashMap::new()),
        })
    }

    #[tokio::test]
    async fn index_and_retrieve_single_resource() {
        let state = create_test_state();

        let body_bytes = b"console.log('hi');";

        let mut resp_headers = HashMap::new();
        resp_headers.insert(
            "Content-Type".to_string(),
            "application/javascript".to_string(),
        );

        let payload = CachedEntryPayload {
            website_key: Some("example.com".to_string()),
            resource_key: "GET::https://example.com/script.js".to_string(),
            url: "https://example.com/script.js".to_string(),
            method: "GET".to_string(),
            status: 200,
            request_headers: HashMap::new(),
            response_headers: resp_headers,
            body_base64: general_purpose::STANDARD.encode(body_bytes),
        };

        index_single_entry(payload, None, state.clone())
            .await
            .expect("index_single_entry");

        let resource_key = "GET::https://example.com/script.js";
        let res = get_resource_with_body(&state, resource_key)
            .expect("get_resource_with_body")
            .expect("resource exists");

        assert_eq!(res.resource.website_key, "example.com");
        assert_eq!(res.resource.resource_key, resource_key);
        assert_eq!(res.body, body_bytes);

        let site_key = format!("site:{}::{}", "example.com", resource_key);
        assert!(
            state.db.get(site_key.as_bytes()).unwrap().is_some(),
            "site index entry should exist"
        );
    }

    #[tokio::test]
    async fn deduplicates_shared_body_across_resources() {
        let state = create_test_state();

        let body_bytes = b"/* shared body across sites */";

        let payload1 = CachedEntryPayload {
            website_key: Some("example.com".to_string()),
            resource_key: "GET::https://example.com/shared.js".to_string(),
            url: "https://example.com/shared.js".to_string(),
            method: "GET".to_string(),
            status: 200,
            request_headers: HashMap::new(),
            response_headers: HashMap::new(),
            body_base64: general_purpose::STANDARD.encode(body_bytes),
        };

        let payload2 = CachedEntryPayload {
            website_key: Some("other.com".to_string()),
            resource_key: "GET::https://cdn.example.com/shared.js".to_string(),
            url: "https://cdn.example.com/shared.js".to_string(),
            method: "GET".to_string(),
            status: 200,
            request_headers: HashMap::new(),
            response_headers: HashMap::new(),
            body_base64: general_purpose::STANDARD.encode(body_bytes),
        };

        index_single_entry(payload1, None, state.clone())
            .await
            .expect("index_single_entry 1");
        index_single_entry(payload2, None, state.clone())
            .await
            .expect("index_single_entry 2");

        let res1 = get_resource_with_body(&state, "GET::https://example.com/shared.js")
            .expect("get_resource_with_body 1")
            .expect("resource 1 exists");
        let res2 = get_resource_with_body(&state, "GET::https://cdn.example.com/shared.js")
            .expect("get_resource_with_body 2")
            .expect("resource 2 exists");

        assert_eq!(
            res1.resource.file_id, res2.resource.file_id,
            "resources with same body should share file_id"
        );

        let mut file_keys = 0usize;
        for item in state.db.iterator(rocksdb::IteratorMode::Start) {
            let (key, _val) = item.expect("iterator item");
            if key.starts_with(b"file:") {
                file_keys += 1;
            }
        }

        assert_eq!(
            file_keys, 1,
            "only one file:* entry should exist for the shared body"
        );
    }
}
