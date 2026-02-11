use std::{
    collections::{HashMap, HashSet},
    convert::Infallible,
    fs, io,
    net::SocketAddr,
    path::{Path, PathBuf},
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
use tokio::{net::TcpListener, sync::mpsc};
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

type ResponseBody = BoxBody<Bytes, hyper::Error>;

#[derive(Debug, Clone, Serialize)]
pub struct RocksDbSize {
    pub estimate_live_data_size_bytes: Option<u64>,
    pub total_sst_files_size_bytes: Option<u64>,
    pub live_sst_files_size_bytes: Option<u64>,
    pub estimate_num_keys: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct MemCacheSize {
    pub entries: usize,
    pub body_bytes: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct CacheSizeReport {
    pub rocksdb: RocksDbSize,
    pub rocksdb_dir_bytes: u64,
    pub mem_cache: MemCacheSize,
}

/// Fast: ask RocksDB for estimates (doesn't scan disk).
pub fn rocksdb_size(db: &DB) -> RocksDbSize {
    fn prop(db: &DB, name: &str) -> Option<u64> {
        db.property_int_value(name).ok().flatten()
    }

    RocksDbSize {
        estimate_live_data_size_bytes: prop(db, "rocksdb.estimate-live-data-size"),
        total_sst_files_size_bytes: prop(db, "rocksdb.total-sst-files-size"),
        live_sst_files_size_bytes: prop(db, "rocksdb.live-sst-files-size"),
        estimate_num_keys: prop(db, "rocksdb.estimate-num-keys"),
    }
}

/// Exact-ish: scans the RocksDB directory and sums file sizes.
/// Skips symlinks and uses an explicit stack (no recursion).
pub fn dir_size_bytes(root: impl AsRef<Path>) -> io::Result<u64> {
    let root = root.as_ref();
    let mut total: u64 = 0;
    let mut stack: Vec<PathBuf> = vec![root.to_path_buf()];

    while let Some(p) = stack.pop() {
        let md = fs::symlink_metadata(&p)?;
        let ft = md.file_type();

        if ft.is_symlink() {
            continue;
        }
        if ft.is_dir() {
            for entry in fs::read_dir(&p)? {
                stack.push(entry?.path());
            }
        } else if ft.is_file() {
            total = total.saturating_add(md.len());
        }
    }

    Ok(total)
}

/// Sums in-memory cached bodies.
fn mem_cache_size(state: &AppState) -> MemCacheSize {
    let mut body_bytes: u64 = 0;
    let entries = state.mem_resources.len();

    for item in state.mem_resources.iter() {
        body_bytes = body_bytes.saturating_add(item.value().body.len() as u64);
    }

    MemCacheSize {
        entries,
        body_bytes,
    }
}

/// Full report.
fn cache_size_report(
    state: &AppState,
    rocksdb_dir: impl AsRef<Path>,
) -> io::Result<CacheSizeReport> {
    Ok(CacheSizeReport {
        rocksdb: rocksdb_size(&state.db),
        rocksdb_dir_bytes: dir_size_bytes(rocksdb_dir)?,
        mem_cache: mem_cache_size(state),
    })
}

#[derive(Clone)]
struct AppState {
    db: Arc<DB>,
    meili_client: MeiliClient,
    index_name: String,
    /// In-memory resource cache keyed by resource_key for fast lookups.
    mem_resources: Arc<DashMap<String, ResourceWithBody>>,
    /// Meili indexing queue (batched by a background worker). None disables Meili indexing.
    meili_tx: Option<mpsc::Sender<CacheIndexDoc>>,
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
    #[serde(default)]
    http_version: HttpVersion,
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

/// Represents an HTTP version
#[derive(Default, Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum HttpVersion {
    Http09,
    Http10,
    #[default]
    Http11,
    H2,
    H3,
}

/// Shape your HTTP API uses over the wire.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CachedEntryPayload {
    #[serde(default)]
    website_key: Option<String>,
    resource_key: String,
    url: String,
    method: String,
    status: u16,
    request_headers: HashMap<String, String>,
    response_headers: HashMap<String, String>,
    body_base64: String,
    #[serde(default)]
    http_version: HttpVersion,
}

/// Minimal document indexed in Meilisearch for search/lookup.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CacheIndexDoc {
    /// Safe Meili primary key (only [0-9a-f] here), derived from resource_key.
    doc_id: String,
    website_key: String,
    resource_key: String,
    url: String,
    status: u16,
    content_type: Option<String>,
}

fn meili_doc_id(resource_key: &str) -> String {
    let h = blake3::hash(resource_key.as_bytes());
    hex::encode(h.as_bytes())
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_bool(name: &str, default: bool) -> bool {
    std::env::var(name)
        .ok()
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true") || v.eq_ignore_ascii_case("yes"))
        .unwrap_or(default)
}

fn enqueue_meili_doc(state: &AppState, doc: CacheIndexDoc) {
    let Some(tx) = &state.meili_tx else {
        return;
    };

    match tx.try_send(doc) {
        Ok(_) => {}
        Err(mpsc::error::TrySendError::Full(doc)) => {
            // best-effort: don't block request path
            let tx = tx.clone();
            tokio::spawn(async move {
                let _ = tx.send(doc).await;
            });
        }
        Err(mpsc::error::TrySendError::Closed(_doc)) => {
            warn!("meili queue is closed; skipping indexing");
        }
    }
}

async fn flush_meili_buf(
    index: &meilisearch_sdk::indexes::Index,
    buf: &mut HashMap<String, CacheIndexDoc>,
    max_batch: usize,
) {
    // Drain in chunks of <= max_batch without borrowing across await.
    while !buf.is_empty() {
        let keys: Vec<String> = buf.keys().take(max_batch).cloned().collect();
        if keys.is_empty() {
            break;
        }

        let mut docs = Vec::with_capacity(keys.len());
        for k in keys {
            if let Some(doc) = buf.remove(&k) {
                docs.push(doc);
            }
        }

        if docs.is_empty() {
            break;
        }

        if let Err(e) = index.add_documents(&docs, Some("doc_id")).await {
            error!(
                "meili add_documents batch failed ({} docs): {}",
                docs.len(),
                e
            );
        }
    }
}

async fn meili_indexer_worker(
    mut rx: mpsc::Receiver<CacheIndexDoc>,
    client: MeiliClient,
    index_name: String,
    flush_every: Duration,
    max_batch: usize,
) {
    let index = client.index(&index_name);

    // Keep only latest doc per doc_id.
    let mut buf: HashMap<String, CacheIndexDoc> = HashMap::new();

    let mut tick = tokio::time::interval(flush_every);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = tick.tick() => {
                flush_meili_buf(&index, &mut buf, max_batch).await;
            }
            maybe = rx.recv() => {
                match maybe {
                    Some(doc) => {
                        buf.insert(doc.doc_id.clone(), doc);

                        if buf.len() >= max_batch {
                            flush_meili_buf(&index, &mut buf, max_batch).await;
                        }
                    }
                    None => {
                        // channel closed
                        flush_meili_buf(&index, &mut buf, max_batch).await;
                        break;
                    }
                }
            }
        }
    }
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
        .unwrap_or(8080);

    let meili_disabled = env_bool("MEILI_DISABLE", false);
    let meili_queue_cap = env_u64("MEILI_QUEUE_CAP", 50_000) as usize;
    let meili_batch_max = env_u64("MEILI_BATCH_MAX", 256).max(1) as usize;
    let meili_flush_ms = env_u64("MEILI_FLUSH_MS", 200).max(10);

    let meili_client = MeiliClient::new(
        meili_host,
        if meili_key.is_empty() {
            None
        } else {
            Some(meili_key)
        },
    )
    .expect("valid meili client");

    // Ensure index exists with primary key = doc_id.
    {
        let mut idx = meili_client.index(&index_name);

        if let Err(_) = idx.fetch_info().await {
            let _ = meili_client.create_index(&index_name, Some("doc_id")).await;
        }
    }

    // Spawn batching Meili indexer worker (optional)
    let meili_tx = if meili_disabled {
        warn!("MEILI_DISABLE=1 set; skipping Meilisearch indexing");
        None
    } else {
        let (tx, rx) = mpsc::channel::<CacheIndexDoc>(meili_queue_cap);
        tokio::spawn(meili_indexer_worker(
            rx,
            meili_client.clone(),
            index_name.clone(),
            Duration::from_millis(meili_flush_ms),
            meili_batch_max,
        ));
        Some(tx)
    };

    let state = Arc::new(AppState {
        db,
        meili_client,
        index_name,
        mem_resources: Arc::new(DashMap::new()),
        meili_tx,
    });

    // cleanup worker
    tokio::spawn(run_cleanup_worker(state.clone()));

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
        // GET /cache/size
        ("GET", ["cache", "size"]) => handle_cache_size(state).await,
        // POST /cache/purge/empty – remove resources with empty HTML bodies
        ("POST", ["cache", "purge", "empty"]) => handle_purge_empty(state).await,
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

    let payload: Box<CachedEntryPayload> = match serde_json::from_slice(&whole_body) {
        Ok(p) => p,
        Err(e) => {
            error!("Failed to parse JSON payload: {e}");
            return Ok(text_response(StatusCode::BAD_REQUEST, "Invalid JSON"));
        }
    };

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

    let payloads: Vec<Box<CachedEntryPayload>> = match serde_json::from_slice(&whole_body) {
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
            Err(e) => error!("Failed to index entry in batch: {e}"),
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
        Ok(None) => return Ok(text_response(StatusCode::NOT_FOUND, "Resource not found")),
        Err(e) => {
            error!("Error loading resource {}: {}", resource_key, e);
            return Ok(text_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Lookup error",
            ));
        }
    };

    if wants_raw {
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
                let suffix = &key_bytes[prefix.len()..];
                let resource_key = match std::str::from_utf8(suffix) {
                    Ok(s) => s.to_string(),
                    Err(e) => {
                        error!("Invalid UTF-8 in site index key: {e}");
                        continue;
                    }
                };

                match get_resource_with_body(&state, &resource_key) {
                    Ok(Some(res)) => resources.push(resource_with_body_to_payload(&res)),
                    Ok(None) => {
                        error!("Site index refers to missing resource_key {}", resource_key);
                        if let Err(e) = state.db.delete(&key_bytes) {
                            error!("Failed to delete dangling site index key: {}", e);
                        }
                    }
                    Err(e) => error!("Failed to load resource {}: {}", resource_key, e),
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

/// Core logic to index a single entry (RocksDB + mem), then enqueue Meili doc (batched).
async fn index_single_entry(
    payload: Box<CachedEntryPayload>,
    header_site_key: Option<String>,
    state: Arc<AppState>,
) -> Result<(), String> {
    let body_bytes = general_purpose::STANDARD
        .decode(&payload.body_base64)
        .map_err(|e| format!("Invalid base64 body: {e}"))?;

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
        http_version: payload.http_version,
        created_at: Some(now_unix_timestamp()),
    };

    // --- In-memory cache ---
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

    // --- RocksDB: site index entry ---
    let site_key = format!("site:{}::{}", website_key, resource.resource_key);
    state
        .db
        .put(site_key.as_bytes(), b"")
        .map_err(|e| format!("RocksDB put site index error: {e}"))?;

    // --- Meili: enqueue doc (batched) ---
    let content_type = resource
        .response_headers
        .get("content-type")
        .cloned()
        .or_else(|| resource.response_headers.get("Content-Type").cloned());

    let doc = CacheIndexDoc {
        doc_id: meili_doc_id(&resource.resource_key),
        website_key,
        resource_key: resource.resource_key.clone(),
        url: resource.url.clone(),
        status: resource.status,
        content_type,
    };

    enqueue_meili_doc(&state, doc);

    Ok(())
}

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

    state
        .mem_resources
        .insert(resource_key.to_string(), combined.clone());

    Ok(Some(combined))
}

fn resource_with_body_to_payload(res: &ResourceWithBody) -> CachedEntryPayload {
    CachedEntryPayload {
        website_key: Some(res.resource.website_key.clone()),
        resource_key: res.resource.resource_key.clone(),
        url: res.resource.url.clone(),
        method: res.resource.method.clone(),
        status: res.resource.status,
        request_headers: res.resource.request_headers.clone(),
        response_headers: res.resource.response_headers.clone(),
        http_version: res.resource.http_version,
        body_base64: general_purpose::STANDARD.encode(&res.body),
    }
}

fn compute_file_id(body: &[u8]) -> String {
    let hash = blake3::hash(body);
    hex::encode(hash.as_bytes())
}

fn derive_website_key_from_url(url_str: &str) -> String {
    match url::Url::parse(url_str) {
        Ok(url) => url.host_str().unwrap_or("unknown").to_string(),
        Err(_) => "unknown".to_string(),
    }
}

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
        .unwrap_or(60 * 60 * 24)
}

fn cleanup_interval() -> Duration {
    let secs = std::env::var("CACHE_CLEANUP_INTERVAL_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(60 * 10);
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

    let mut batch = rocksdb::WriteBatch::default();

    // delete resources
    for rk in &expired_resource_keys {
        let key = format!("res:{}", rk);
        batch.delete(key.as_bytes());
    }

    // delete site index entries
    for site_key in &expired_site_keys {
        batch.delete(site_key);
    }

    state
        .db
        .write(batch)
        .map_err(|e| format!("RocksDB batch delete (res+site) error: {e}"))?;

    let mut orphaned_file_ids = Vec::new();
    for file_id in expired_file_ids {
        if !live_file_ids.contains(&file_id) {
            orphaned_file_ids.push(file_id);
        }
    }

    for file_id in &orphaned_file_ids {
        let file_key = format!("file:{}", file_id);
        if let Err(e) = state.db.delete(file_key.as_bytes()) {
            error!("Failed to delete file entry {}: {}", file_id, e);
        }
    }

    Ok((expired_resource_keys, orphaned_file_ids))
}

/// Returns true if `body` looks like an empty HTML page (no meaningful text content).
/// Also handles base64-encoded bodies transparently.
fn is_empty_html(body: &[u8]) -> bool {
    if body.is_empty() {
        return true;
    }

    // Try raw bytes first.
    if let Ok(text) = std::str::from_utf8(body) {
        if is_empty_html_str(text) {
            return true;
        }

        // Body might be base64-encoded; try decoding it.
        let trimmed = text.trim();
        if !trimmed.is_empty() && !trimmed.starts_with('<') {
            if let Ok(decoded) = general_purpose::STANDARD.decode(trimmed.as_bytes()) {
                if let Ok(decoded_text) = std::str::from_utf8(&decoded) {
                    if is_empty_html_str(decoded_text) {
                        return true;
                    }
                }
            }
        }
    }

    false
}

/// Core check: is this text string an empty HTML page?
fn is_empty_html_str(text: &str) -> bool {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return true;
    }

    let lower = trimmed.to_ascii_lowercase();

    // Remove optional doctype
    let html = lower
        .strip_prefix("<!doctype html>")
        .unwrap_or(&lower)
        .trim();

    // Must start with <html
    if !html.starts_with("<html") {
        return false;
    }

    // Strip all tags, then check if any non-whitespace text remains.
    let mut inside_tag = false;
    let mut has_text = false;

    for ch in html.chars() {
        if ch == '<' {
            inside_tag = true;
        } else if ch == '>' {
            inside_tag = false;
        } else if !inside_tag && !ch.is_whitespace() {
            has_text = true;
            break;
        }
    }

    !has_text
}

#[derive(Debug, Clone, Serialize)]
struct PurgeResult {
    purged_resources: usize,
    purged_files: usize,
    resource_keys: Vec<String>,
}

/// Known empty HTML page bodies. Pre-computing their BLAKE3 file_ids lets us
/// scan only the lightweight `res:` entries instead of deserializing every
/// multi-KB `file:` body (which is 10x+ slower on large caches).
fn known_empty_page_file_ids() -> HashSet<String> {
    let patterns: &[&[u8]] = &[
        b"<html><head></head><body></body></html>",
        b"<!DOCTYPE html><html><head></head><body></body></html>",
        b"<!doctype html><html><head></head><body></body></html>",
        b"<html>\n<head></head>\n<body></body>\n</html>",
        b"<html>\n  <head></head>\n  <body></body>\n</html>",
        b"<html><head></head><body>\n</body></html>",
        b"<html><head>\n</head><body>\n</body></html>",
        b"",
    ];

    patterns
        .iter()
        .map(|p| compute_file_id(p))
        .collect()
}

/// Scan file entries for empty HTML bodies, returning their file_ids.
/// Only deserializes entries whose raw JSON value is small (< max_value_bytes),
/// since empty HTML bodies produce tiny file entries.
fn scan_empty_file_ids(state: &AppState, max_value_bytes: usize) -> Result<HashSet<String>, String> {
    let mut empty_file_ids: HashSet<String> = HashSet::new();
    let mut scanned = 0u64;
    let mut skipped_large = 0u64;

    let iter = state
        .db
        .iterator(IteratorMode::From(b"file:", Direction::Forward));

    for item in iter {
        let (key, value) = item.map_err(|e| format!("RocksDB iterator error: {e}"))?;
        if !key.starts_with(b"file:") {
            break;
        }

        // Skip large values — they can't be empty HTML pages.
        if value.len() > max_value_bytes {
            skipped_large += 1;
            continue;
        }

        scanned += 1;

        let file_entry: FileEntry = match serde_json::from_slice(&value) {
            Ok(f) => f,
            Err(e) => {
                error!("Failed to deserialize FileEntry during purge scan: {e}");
                continue;
            }
        };

        if is_empty_html(&file_entry.body) {
            info!("purge_empty: found empty file_id={} body_len={}", file_entry.file_id, file_entry.body.len());
            empty_file_ids.insert(file_entry.file_id);
        }
    }

    info!(
        "purge_empty: scanned {} small file entries, skipped {} large, found {} empty",
        scanned, skipped_large, empty_file_ids.len()
    );

    Ok(empty_file_ids)
}

/// Purge resources with empty HTML bodies.
/// Strategy: fast-path with pre-computed hashes, then fallback to scanning small file entries.
fn do_purge_empty(state: &AppState) -> Result<PurgeResult, String> {
    // Phase 1a: try known patterns first (instant).
    let mut empty_file_ids = known_empty_page_file_ids();

    // Phase 1b: also scan small file entries to catch unknown variations.
    // Empty HTML body ~38 bytes → serde JSON array ~230 bytes, but a base64-
    // encoded body or extra whitespace variants could be larger.
    // Threshold of 2048 is generous and skips >99% of real content files.
    let scanned = scan_empty_file_ids(state, 2048)?;
    empty_file_ids.extend(scanned);

    info!(
        "purge_empty: total {} empty-page file_ids to match",
        empty_file_ids.len()
    );

    // Phase 2: scan all res: entries and collect those referencing empty file_ids.
    let mut purge_resource_keys = Vec::new();
    let mut purge_site_keys: Vec<Vec<u8>> = Vec::new();
    let mut referenced_file_ids: HashSet<String> = HashSet::new();
    let mut live_file_ids: HashSet<String> = HashSet::new();

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
                error!("Failed to deserialize ResourceEntry during purge: {e}");
                continue;
            }
        };

        if empty_file_ids.contains(&resource.file_id) {
            purge_resource_keys.push(resource.resource_key.clone());
            referenced_file_ids.insert(resource.file_id.clone());

            let site_key = format!("site:{}::{}", resource.website_key, resource.resource_key);
            purge_site_keys.push(site_key.into_bytes());
        } else {
            live_file_ids.insert(resource.file_id.clone());
        }
    }

    // Phase 3: batch delete resources + site index entries.
    let mut batch = rocksdb::WriteBatch::default();

    for rk in &purge_resource_keys {
        batch.delete(format!("res:{}", rk).as_bytes());
    }
    for site_key in &purge_site_keys {
        batch.delete(site_key);
    }

    state
        .db
        .write(batch)
        .map_err(|e| format!("RocksDB batch delete error: {e}"))?;

    // Phase 4: delete orphaned file entries (not referenced by any remaining resource).
    let mut purged_files = 0usize;
    for file_id in &referenced_file_ids {
        if !live_file_ids.contains(file_id) {
            let file_key = format!("file:{}", file_id);
            if let Err(e) = state.db.delete(file_key.as_bytes()) {
                error!("Failed to delete file entry {}: {}", file_id, e);
            } else {
                purged_files += 1;
            }
        }
    }

    // Phase 5: evict from in-memory cache.
    for rk in &purge_resource_keys {
        state.mem_resources.remove(rk);
    }

    Ok(PurgeResult {
        purged_resources: purge_resource_keys.len(),
        purged_files,
        resource_keys: purge_resource_keys,
    })
}

/// POST /cache/purge/empty – fire-and-forget: spawns background purge, returns immediately.
async fn handle_purge_empty(
    state: Arc<AppState>,
) -> Result<Response<ResponseBody>, hyper::Error> {
    let meili_client = state.meili_client.clone();
    let index_name = state.index_name.clone();

    // Spawn the heavy work as a detached background task so it survives
    // the HTTP connection being closed by the client.
    tokio::spawn(async move {
        let state_blocking = state.clone();
        let result =
            tokio::task::spawn_blocking(move || do_purge_empty(&state_blocking)).await;

        match result {
            Ok(Ok(purge_result)) => {
                info!(
                    "purge_empty DONE: removed {} resources, {} file bodies",
                    purge_result.purged_resources, purge_result.purged_files
                );

                // Best-effort Meilisearch cleanup.
                if !purge_result.resource_keys.is_empty() {
                    let doc_ids: Vec<String> = purge_result
                        .resource_keys
                        .iter()
                        .map(|rk| meili_doc_id(rk))
                        .collect();
                    let index = meili_client.index(&index_name);
                    if let Err(e) = index.delete_documents(&doc_ids).await {
                        error!("purge_empty: meili delete error: {}", e);
                    }
                }
            }
            Ok(Err(e)) => error!("purge_empty error: {}", e),
            Err(e) => error!("purge_empty join error: {}", e),
        }
    });

    Ok(json_response(
        StatusCode::ACCEPTED,
        serde_json::to_vec(&serde_json::json!({
            "status": "started",
            "message": "Purge running in background. Check logs for results."
        }))
        .unwrap(),
    ))
}

async fn handle_cache_size(state: Arc<AppState>) -> Result<Response<ResponseBody>, hyper::Error> {
    let report = match cache_size_report(&state, "cache_db") {
        Ok(r) => r,
        Err(e) => {
            error!("Failed to compute cache size: {e}");
            return Ok(text_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Cache size error",
            ));
        }
    };

    let json = serde_json::to_vec(&report).unwrap_or_else(|_| b"{}".to_vec());
    Ok(json_response(StatusCode::OK, json))
}

/// A cleanup worker script
async fn run_cleanup_worker(state: Arc<AppState>) {
    loop {
        tokio::time::sleep(cleanup_interval()).await;

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

                // Remove docs from Meilisearch (best-effort) — delete by doc_id now.
                if !expired_resource_keys.is_empty() {
                    let doc_ids: Vec<String> = expired_resource_keys
                        .iter()
                        .map(|rk| meili_doc_id(rk))
                        .collect();
                    let index = state.meili_client.index(&state.index_name);
                    if let Err(e) = index.delete_documents(&doc_ids).await {
                        error!("Failed to delete documents from Meilisearch: {}", e);
                    }
                }
            }
            Ok(Err(e)) => error!("Cache cleanup error: {}", e),
            Err(e) => error!("Cache cleanup join error: {}", e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn create_test_state() -> Arc<AppState> {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().to_path_buf();
        std::mem::forget(dir);

        let db = Arc::new(DB::open_default(path).expect("open rocksdb"));

        // No Meili worker in tests.
        let meili_client = MeiliClient::new(
            "http://127.0.0.1:7700".to_string(),
            Some("testMasterKey".to_string()),
        )
        .expect("valid client");

        Arc::new(AppState {
            db,
            meili_client,
            index_name: "test_index".to_string(),
            mem_resources: Arc::new(DashMap::new()),
            meili_tx: None,
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

        let payload = Box::new(CachedEntryPayload {
            website_key: Some("example.com".to_string()),
            resource_key: "GET:https://example.com/script.js".to_string(),
            url: "https://example.com/script.js".to_string(),
            method: "GET".to_string(),
            status: 200,
            request_headers: HashMap::new(),
            response_headers: resp_headers,
            body_base64: general_purpose::STANDARD.encode(body_bytes),
            http_version: HttpVersion::Http11,
        });

        index_single_entry(payload, None, state.clone())
            .await
            .expect("index_single_entry");

        let resource_key = "GET:https://example.com/script.js";
        let res = get_resource_with_body(&state, resource_key)
            .expect("get_resource_with_body")
            .expect("resource exists");

        assert_eq!(res.resource.website_key, "example.com");
        assert_eq!(res.resource.resource_key, resource_key);
        assert_eq!(res.body, body_bytes);

        let site_key = format!("site:{}::{}", "example.com", resource_key);
        assert!(state.db.get(site_key.as_bytes()).unwrap().is_some());
    }

    #[tokio::test]
    async fn deduplicates_shared_body_across_resources() {
        let state = create_test_state();

        let body_bytes = b"/* shared body across sites */";

        let payload1 = Box::new(CachedEntryPayload {
            website_key: Some("example.com".to_string()),
            resource_key: "GET:https://example.com/shared.js".to_string(),
            url: "https://example.com/shared.js".to_string(),
            method: "GET".to_string(),
            status: 200,
            request_headers: HashMap::new(),
            response_headers: HashMap::new(),
            body_base64: general_purpose::STANDARD.encode(body_bytes),
            http_version: HttpVersion::Http11,
        });

        let payload2 = Box::new(CachedEntryPayload {
            website_key: Some("other.com".to_string()),
            resource_key: "GET:https://cdn.example.com/shared.js".to_string(),
            url: "https://cdn.example.com/shared.js".to_string(),
            method: "GET".to_string(),
            status: 200,
            request_headers: HashMap::new(),
            response_headers: HashMap::new(),
            body_base64: general_purpose::STANDARD.encode(body_bytes),
            http_version: HttpVersion::Http11,
        });

        index_single_entry(payload1, None, state.clone())
            .await
            .expect("index_single_entry 1");
        index_single_entry(payload2, None, state.clone())
            .await
            .expect("index_single_entry 2");

        let res1 = get_resource_with_body(&state, "GET:https://example.com/shared.js")
            .expect("get_resource_with_body 1")
            .expect("resource 1 exists");
        let res2 = get_resource_with_body(&state, "GET:https://cdn.example.com/shared.js")
            .expect("get_resource_with_body 2")
            .expect("resource 2 exists");

        assert_eq!(res1.resource.file_id, res2.resource.file_id);

        let mut file_keys = 0usize;
        for item in state.db.iterator(rocksdb::IteratorMode::Start) {
            let (key, _val) = item.expect("iterator item");
            if key.starts_with(b"file:") {
                file_keys += 1;
            }
        }

        assert_eq!(file_keys, 1);
    }

    #[test]
    fn is_empty_html_detects_empty_pages() {
        // Exact match from user report
        assert!(is_empty_html(b"<html><head></head><body></body></html>"));
        // With doctype
        assert!(is_empty_html(
            b"<!DOCTYPE html><html><head></head><body></body></html>"
        ));
        // With whitespace / newlines
        assert!(is_empty_html(
            b"<html>\n  <head></head>\n  <body>  </body>\n</html>"
        ));
        // With attributes on tags
        assert!(is_empty_html(
            b"<html lang=\"en\"><head></head><body class=\"x\"></body></html>"
        ));
        // Empty string
        assert!(is_empty_html(b""));
        // Just whitespace
        assert!(is_empty_html(b"   \n\t  "));
    }

    #[test]
    fn is_empty_html_rejects_non_empty_pages() {
        // Has text content
        assert!(!is_empty_html(
            b"<html><head></head><body>Hello</body></html>"
        ));
        // Not HTML at all
        assert!(!is_empty_html(b"console.log('hi');"));
        // Binary content
        assert!(!is_empty_html(&[0xFF, 0xD8, 0xFF, 0xE0]));
        // Has nested element with text
        assert!(!is_empty_html(
            b"<html><head><title>Test</title></head><body></body></html>"
        ));
    }

    #[tokio::test]
    async fn purge_empty_removes_empty_html_resources() {
        let state = create_test_state();

        // Index an empty HTML page
        let empty_body = b"<html><head></head><body></body></html>";
        let payload_empty = Box::new(CachedEntryPayload {
            website_key: Some("example.com".to_string()),
            resource_key: "GET:https://example.com/".to_string(),
            url: "https://example.com/".to_string(),
            method: "GET".to_string(),
            status: 200,
            request_headers: HashMap::new(),
            response_headers: HashMap::new(),
            body_base64: general_purpose::STANDARD.encode(empty_body),
            http_version: HttpVersion::Http11,
        });

        // Index a real page
        let real_body = b"<html><head></head><body><h1>Hello World</h1></body></html>";
        let payload_real = Box::new(CachedEntryPayload {
            website_key: Some("example.com".to_string()),
            resource_key: "GET:https://example.com/about".to_string(),
            url: "https://example.com/about".to_string(),
            method: "GET".to_string(),
            status: 200,
            request_headers: HashMap::new(),
            response_headers: HashMap::new(),
            body_base64: general_purpose::STANDARD.encode(real_body),
            http_version: HttpVersion::Http11,
        });

        index_single_entry(payload_empty, None, state.clone())
            .await
            .expect("index empty");
        index_single_entry(payload_real, None, state.clone())
            .await
            .expect("index real");

        // Both should exist before purge
        assert!(get_resource_with_body(&state, "GET:https://example.com/")
            .unwrap()
            .is_some());
        assert!(get_resource_with_body(&state, "GET:https://example.com/about")
            .unwrap()
            .is_some());

        // Clear mem cache so purge eviction is testable
        state.mem_resources.clear();

        let result = do_purge_empty(&state).expect("purge_empty");
        assert_eq!(result.purged_resources, 1);
        assert_eq!(result.purged_files, 1);
        assert_eq!(result.resource_keys, vec!["GET:https://example.com/"]);

        // Empty page should be gone
        assert!(get_resource_with_body(&state, "GET:https://example.com/")
            .unwrap()
            .is_none());

        // Real page should still exist
        assert!(get_resource_with_body(&state, "GET:https://example.com/about")
            .unwrap()
            .is_some());
    }
}
