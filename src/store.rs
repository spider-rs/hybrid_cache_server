//! RocksDB storage, the in-memory resource cache, and garbage collection.
//!
//! Key layout (unchanged since v0.2):
//!   file:{file_id}                     body bytes, deduped by blake3 of the body
//!   res:{resource_key}                 JSON ResourceEntry
//!   site:{website_key}::{resource_key} empty, the per-site index
//! Added in v0.3:
//!   orph:{file_id}                     empty, "this body may have lost its last reference"
//!
//! Every function here blocks. Callers on the async runtime go through
//! `spawn_blocking`.

use std::{
    collections::{HashMap, HashSet},
    fmt, fs,
    future::Future,
    io,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        mpsc::{sync_channel, SyncSender},
        Arc,
    },
    task::{Context, Poll, Wake, Waker},
    time::{Duration, Instant},
};

use base64::{engine::general_purpose::STANDARD, Engine};
use bytes::Bytes;
use moka::sync::Cache;
use rocksdb::{BlockBasedOptions, DBCompressionType, Options, ReadOptions, WriteBatch, DB};
use serde::Serialize;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, SemaphorePermit};

#[cfg(test)]
use std::sync::{atomic::AtomicBool, Mutex};
use tracing::{error, info, warn};

use crate::{
    config::Config,
    model::{
        CachedResource, FileEntry, IncomingPayload, PayloadEncoder, ResourceEntry, ResourceMeta,
    },
    telemetry::{self as tm, timed},
};

pub type MemCache = Cache<String, Arc<CachedResource>>;

#[cfg(test)]
pub(crate) type TestHook = Box<dyn FnOnce(&Store) + Send>;

pub struct Store {
    pub db: Arc<DB>,
    pub mem: MemCache,
    pub path: PathBuf,
    gc: Gc,
    /// Test hook run by either collector between its scan and delete phase.
    #[cfg(test)]
    pub after_scan: Mutex<Option<TestHook>>,
    #[cfg(test)]
    pub hooks: TestHooks,
}

/// Writers and competing maintenance requests never queue. A collector
/// queues for all writer slots, closing admission until existing writes drain.
/// Writers hold one slot through the existence check, batch and mem update;
/// snapshot creation and deletion require every slot. The maintenance slot
/// spans the entire scan so cleanup and purge cannot reset each other's epoch.
/// Slots coordinate exclusion, not upload capacity: the request byte budgets
/// and blocking pool already bound uploads. Do not add a small commit limit.
const WRITE_SLOTS: u32 = if Semaphore::MAX_PERMITS < u32::MAX as usize {
    Semaphore::MAX_PERMITS as u32
} else {
    u32::MAX
};
const GC_ADMISSION_TIMEOUT: Duration = Duration::from_secs(2);

/// Wake a blocking collector when its queued semaphore acquisition is ready.
/// A single coalesced notification suffices; waking never waits for the reader.
struct AdmissionWake(SyncSender<()>);

impl Wake for AdmissionWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        let _ = self.0.try_send(());
    }
}

/// A bounded, conservative set of bodies referenced or orphaned during an
/// epoch. Hash collisions only retain extra bodies until a later pass. Unlike
/// an evicting set, saturation cannot forget a writer's protection. At 128 KiB
/// per Store this does not grow with write volume or a slow scan.
const TOUCH_WORDS: usize = 16_384;

// Every reference is either committed before the snapshot (and scanned),
// or marked before its file existence check. Taking every writer slot for
// deletion excludes in-flight reuse decisions; maintenance admission excludes
// a second collector resetting these marks. No map guards or busy waits.
struct Gc {
    writers: Semaphore,
    maintenance: Arc<Semaphore>,
    touched: Box<[AtomicU64]>,
}

impl Gc {
    fn new() -> Self {
        Self {
            writers: Semaphore::new(WRITE_SLOTS as usize),
            maintenance: Arc::new(Semaphore::new(1)),
            touched: (0..TOUCH_WORDS).map(|_| AtomicU64::new(0)).collect(),
        }
    }

    fn maintenance(&self) -> Result<OwnedSemaphorePermit, String> {
        self.maintenance
            .clone()
            .try_acquire_owned()
            .map_err(|_| "storage busy: maintenance active".to_string())
    }

    fn touch_bit(file_id: &[u8]) -> (usize, u64) {
        let hash = blake3::hash(file_id);
        let h = hash.as_bytes();
        let bit = u32::from_le_bytes([h[0], h[1], h[2], h[3]]) as usize;
        ((bit / 64) % TOUCH_WORDS, 1u64 << (bit % 64))
    }

    fn touch(&self, file_id: &[u8]) {
        let (word, bit) = Self::touch_bit(file_id);
        self.touched[word].fetch_or(bit, Ordering::Relaxed);
    }

    fn touched(&self, file_id: &[u8]) -> bool {
        let (word, bit) = Self::touch_bit(file_id);
        self.touched[word].load(Ordering::Relaxed) & bit != 0
    }

    // Only called with every writer slot and the maintenance slot held.
    // Semaphore release/acquire orders the relaxed bitmap operations.
    fn reset(&self) {
        for word in &self.touched {
            word.store(0, Ordering::Relaxed);
        }
    }
}

/// Deterministic barriers and I/O faults; absent from production builds.
#[cfg(test)]
#[derive(Default)]
pub struct TestHooks {
    pub before_commit: Mutex<Option<TestHook>>,
    pub before_delete: Mutex<Option<TestHook>>,
    pub before_gc_wait: Mutex<Option<TestHook>>,
    pub fail_write: AtomicBool,
    pub fail_gc_read: AtomicBool,
}

#[cfg(test)]
fn run_hook(hook: &Mutex<Option<TestHook>>, store: &Store) {
    // Drop the test mutex before calling code that can reenter the store.
    let hook = hook.lock().unwrap().take();
    if let Some(hook) = hook {
        hook(store);
    }
}

/// One resource ready to be committed.
pub struct PreparedEntry {
    pub resource: ResourceEntry,
    pub body: Bytes,
}

#[derive(Debug)]
pub enum CommitError {
    Busy,
    Storage(String),
}

impl From<String> for CommitError {
    fn from(error: String) -> Self {
        Self::Storage(error)
    }
}

impl fmt::Display for CommitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Busy => {
                f.write_str("storage busy: collector pending or write capacity exhausted")
            }
            Self::Storage(error) => f.write_str(error),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct RocksDbSize {
    pub estimate_live_data_size_bytes: Option<u64>,
    pub total_sst_files_size_bytes: Option<u64>,
    pub live_sst_files_size_bytes: Option<u64>,
    pub estimate_num_keys: Option<u64>,
    pub total_blob_file_size_bytes: Option<u64>,
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

#[derive(Debug, Clone, Default, Serialize)]
pub struct PurgeResult {
    pub purged_resources: usize,
    pub purged_files: usize,
    pub resource_keys: Vec<String>,
}

#[derive(Debug, Default)]
pub struct CleanupResult {
    pub expired_resource_keys: Vec<String>,
    pub removed_files: usize,
    pub scanned: usize,
}

/// A /cache/site response: JSON array chunks plus the budget they hold.
pub struct SiteResponse {
    pub chunks: Vec<Bytes>,
    pub total_bytes: usize,
    pub items: usize,
    pub truncated: Option<&'static str>,
    pub permit: Option<OwnedSemaphorePermit>,
}

pub fn db_options(cfg: &Config) -> Options {
    let mut o = Options::default();
    o.create_if_missing(true);

    let cache = rocksdb::Cache::new_lru_cache(cfg.rocksdb_block_cache_bytes as usize);
    let mut block = BlockBasedOptions::default();
    block.set_bloom_filter(10.0, false);
    block.set_block_cache(&cache);
    block.set_cache_index_and_filter_blocks(true);
    block.set_pin_l0_filter_and_index_blocks_in_cache(true);
    o.set_block_based_table_factory(&block);

    o.set_write_buffer_size(64 * 1024 * 1024);
    o.set_max_write_buffer_number(3);
    o.set_min_write_buffer_number_to_merge(1);

    let cpus = std::thread::available_parallelism()
        .map(|n| n.get() as i32)
        .unwrap_or(4);
    o.increase_parallelism(cpus);
    o.set_max_background_jobs(cpus.max(2));
    o.set_enable_pipelined_write(true);
    o.set_level_compaction_dynamic_level_bytes(true);
    o.set_max_bytes_for_level_base(256 * 1024 * 1024);
    o.set_max_open_files(cfg.rocksdb_max_open_files);
    o.set_keep_log_file_num(5);
    o.set_max_log_file_size(16 * 1024 * 1024);

    o.set_compression_type(DBCompressionType::Lz4);

    if cfg.rocksdb_rate_limit_bytes > 0 {
        // Caps flush and compaction IO so a compaction burst cannot starve
        // foreground reads. A fixed rate: the auto-tuned limiter drops to
        // rate/20 when idle and then stalls writes during a burst.
        o.set_ratelimiter(cfg.rocksdb_rate_limit_bytes, 100_000, 10);
    }

    if cfg.rocksdb_blob_files {
        // Integrated BlobDB: bodies of 64 KiB and up live in blob files, so
        // compaction rewrites a small reference instead of the whole body.
        // Existing SSTs keep their inline values and are read as before;
        // bodies move to blob files as compaction touches them.
        o.set_enable_blob_files(true);
        o.set_min_blob_size(64 * 1024);
        o.set_blob_file_size(256 * 1024 * 1024);
        o.set_blob_compression_type(DBCompressionType::Lz4);
        o.set_enable_blob_gc(true);
        o.set_blob_gc_age_cutoff(0.25);
        o.set_blob_compaction_readahead_size(2 * 1024 * 1024);
        o.set_blob_cache(&cache);
        // With bodies in blob files the SSTs hold only small metadata, so
        // zstd on the bottom level is cheap. Without blob files the bottom
        // level holds the bodies too, and zstd there slowed a write burst
        // 4x in the loadgen (compaction fell behind and stalled writes).
        o.set_bottommost_compression_type(DBCompressionType::Zstd);
    }
    o
}

fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

pub fn compute_file_id(body: &[u8]) -> String {
    hex::encode(blake3::hash(body).as_bytes())
}

pub fn derive_website_key_from_url(url_str: &str) -> String {
    match url::Url::parse(url_str) {
        Ok(url) => url.host_str().unwrap_or("unknown").to_string(),
        Err(_) => "unknown".to_string(),
    }
}

fn non_empty(s: Option<&str>) -> Option<&str> {
    s.filter(|s| !s.is_empty())
}

/// Which site key wins when both the request header and the item name one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SiteKeyPrecedence {
    /// POST /cache/index: `x-cache-site` header, then the payload's
    /// `website_key`, then the URL host. spider_remote_cache sends the
    /// crawl's cache_site in the header and the URL host in the payload,
    /// so the header is the better key here. Same as v0.2.
    HeaderFirst,
    /// POST /cache/index/batch: the item's own `website_key`, then the
    /// header, then the URL host. A batch can mix sites, and
    /// spider_remote_cache fills the header from the first item only, so
    /// the header must not override items that name their own site.
    ItemFirst,
}

pub fn resolve_website_key(
    precedence: SiteKeyPrecedence,
    header: Option<&str>,
    item: Option<&str>,
    url: &str,
) -> String {
    let (first, second) = match precedence {
        SiteKeyPrecedence::HeaderFirst => (non_empty(header), non_empty(item)),
        SiteKeyPrecedence::ItemFirst => (non_empty(item), non_empty(header)),
    };
    first
        .or(second)
        .map(str::to_string)
        .unwrap_or_else(|| derive_website_key_from_url(url))
}

/// Decode and validate one incoming payload. Pure computation.
pub fn prepare_entry(
    payload: IncomingPayload<'_>,
    website_key: String,
) -> Result<PreparedEntry, String> {
    let body = STANDARD
        .decode(payload.body_base64.as_bytes())
        .map_err(|e| format!("invalid base64 body: {e}"))?;
    let file_id = compute_file_id(&body);
    Ok(PreparedEntry {
        resource: ResourceEntry {
            website_key,
            resource_key: payload.resource_key,
            url: payload.url,
            method: payload.method,
            status: payload.status,
            request_headers: payload.request_headers,
            response_headers: payload.response_headers,
            file_id,
            http_version: payload.http_version,
            created_at: Some(now_unix()),
        },
        body: Bytes::from(body),
    })
}

impl Store {
    pub fn open(cfg: &Config) -> Result<Self, String> {
        let opts = db_options(cfg);
        let db = DB::open(&opts, &cfg.rocksdb_path)
            .map_err(|e| format!("open rocksdb at {}: {e}", cfg.rocksdb_path))?;
        Ok(Self::with_db(Arc::new(db), cfg))
    }

    pub fn with_db(db: Arc<DB>, cfg: &Config) -> Self {
        let ttl = Duration::from_secs(cfg.cache_ttl_secs.max(1) as u64);
        let mem = Cache::builder()
            .max_capacity(cfg.mem_cache_bytes)
            .weigher(|k: &String, v: &Arc<CachedResource>| -> u32 {
                let w = k.len() + v.body.len() + v.resource.heap_estimate() + 128;
                w.min(u32::MAX as usize) as u32
            })
            .time_to_live(ttl)
            .build();
        Store {
            db,
            mem,
            path: PathBuf::from(&cfg.rocksdb_path),
            gc: Gc::new(),
            #[cfg(test)]
            after_scan: Mutex::new(None),
            #[cfg(test)]
            hooks: TestHooks::default(),
        }
    }

    pub fn health(&self) -> Result<u64, String> {
        self.db
            .property_int_value("rocksdb.estimate-num-keys")
            .map_err(|e| e.to_string())
            .map(|v| v.unwrap_or(0))
    }

    // ------------------------------------------------------------ reads

    fn read_body(&self, file_id: &str) -> Result<Option<Bytes>, String> {
        let key = format!("file:{file_id}");
        let pinned = timed(tm::DB_READ, || self.db.get_pinned(key.as_bytes()))
            .map_err(|e| format!("rocksdb get file: {e}"))?;
        let Some(raw) = pinned else { return Ok(None) };
        decode_file_value(&raw).map(|b| Some(b.into_owned_bytes()))
    }

    fn read_resource_entry(&self, resource_key: &str) -> Result<Option<ResourceEntry>, String> {
        let key = format!("res:{resource_key}");
        let pinned = timed(tm::DB_READ, || self.db.get_pinned(key.as_bytes()))
            .map_err(|e| format!("rocksdb get resource: {e}"))?;
        let Some(raw) = pinned else { return Ok(None) };
        serde_json::from_slice(&raw)
            .map(Some)
            .map_err(|e| format!("deserialize ResourceEntry: {e}"))
    }

    fn load_from_db(&self, resource_key: &str) -> Result<Option<Arc<CachedResource>>, String> {
        let Some(resource) = self.read_resource_entry(resource_key)? else {
            return Ok(None);
        };
        // The body can vanish between the two reads if cleanup removes the
        // resource meanwhile. That is a miss, not a server error.
        let Some(body) = self.read_body(&resource.file_id)? else {
            warn!(
                "resource {resource_key}: body {} missing, treating as a miss",
                resource.file_id
            );
            return Ok(None);
        };
        Ok(Some(Arc::new(CachedResource { resource, body })))
    }

    /// Mem cache lookup only.
    pub fn get_cached(&self, resource_key: &str) -> Option<Arc<CachedResource>> {
        let hit = self.mem.get(resource_key);
        if hit.is_some() {
            metrics::counter!(tm::MEM_HITS).increment(1);
        }
        hit
    }

    /// Look up one resource. A miss is read from RocksDB and, when `fill`
    /// is set, admitted to the mem cache.
    pub fn get_resource(
        &self,
        resource_key: &str,
        fill: bool,
    ) -> Result<Option<Arc<CachedResource>>, String> {
        if let Some(hit) = self.get_cached(resource_key) {
            return Ok(Some(hit));
        }
        metrics::counter!(tm::MEM_MISSES).increment(1);
        let loaded = self.load_from_db(resource_key)?;
        if fill {
            if let Some(r) = loaded {
                // Insert only if absent: a write that committed after our
                // read has already put the newer version here, and it wins.
                let entry = self.mem.entry(resource_key.to_string()).or_insert(r);
                return Ok(Some(entry.into_value()));
            }
        }
        Ok(loaded)
    }

    /// Build the JSON array for GET /cache/site/{website_key}.
    ///
    /// Each item is encoded straight from the mem cache entry or the pinned
    /// RocksDB value into its own exactly sized buffer, so the response is
    /// never copied or grown. It stops adding items once `max_bytes` would
    /// be exceeded or the shared in-flight budget runs out, so one request
    /// can hold at most `max_bytes`. Items read from RocksDB are not added
    /// to the mem cache: a site load is a scan, not a sign that any one
    /// resource is hot, and admitting it would flush the hot set.
    pub fn build_site_response(
        &self,
        website_key: &str,
        max_bytes: usize,
        budget: &Arc<Semaphore>,
    ) -> Result<SiteResponse, String> {
        let prefix = format!("site:{website_key}::");
        let prefix = prefix.as_bytes();
        let mut out = SiteResponse {
            chunks: Vec::new(),
            total_bytes: 2, // "[" and "]"
            items: 0,
            truncated: None,
            permit: None,
        };
        let mut dangling: Vec<(Vec<u8>, String)> = Vec::new();

        let mut it = self.db.raw_iterator();
        it.seek(prefix);
        out.chunks.push(Bytes::from_static(b"["));

        while it.valid() {
            let Some(key) = it.key() else { break };
            if !key.starts_with(prefix) {
                break;
            }
            let resource_key = match std::str::from_utf8(&key[prefix.len()..]) {
                Ok(s) => s.to_string(),
                Err(e) => {
                    warn!("invalid UTF-8 in site index key: {e}");
                    it.next();
                    continue;
                }
            };

            // Emit one item; returns false when the response is full.
            let mut emit = |resource: &ResourceEntry, body: &[u8]| -> Result<bool, String> {
                let enc = PayloadEncoder::new(resource, body.len())?;
                let sep = usize::from(out.items > 0);
                let item_len = enc.len() + sep;
                if out.total_bytes + item_len > max_bytes {
                    out.truncated = Some("size");
                    return Ok(false);
                }
                let Ok(p) = budget
                    .clone()
                    .try_acquire_many_owned(item_len.min(u32::MAX as usize) as u32)
                else {
                    out.truncated = Some("budget");
                    return Ok(false);
                };
                match out.permit.as_mut() {
                    Some(all) => all.merge(p),
                    None => out.permit = Some(p),
                }
                let mut buf = Vec::with_capacity(item_len);
                if sep == 1 {
                    buf.push(b',');
                }
                enc.write(body, &mut buf);
                out.total_bytes += buf.len();
                out.items += 1;
                out.chunks.push(Bytes::from(buf));
                Ok(true)
            };

            let keep_going = if let Some(hit) = self.get_cached(&resource_key) {
                emit(&hit.resource, &hit.body)?
            } else {
                metrics::counter!(tm::MEM_MISSES).increment(1);
                match self.read_resource_entry(&resource_key) {
                    Ok(Some(resource)) => {
                        let fkey = format!("file:{}", resource.file_id);
                        match timed(tm::DB_READ, || self.db.get_pinned(fkey.as_bytes())) {
                            Ok(Some(raw)) => match decode_file_value(&raw) {
                                Ok(body) => emit(&resource, body.as_slice())?,
                                Err(e) => {
                                    error!("site {website_key}: bad body for {resource_key}: {e}");
                                    true
                                }
                            },
                            Ok(None) => {
                                error!("site {website_key}: body missing for {resource_key}");
                                true
                            }
                            Err(e) => {
                                error!("site {website_key}: read body for {resource_key}: {e}");
                                true
                            }
                        }
                    }
                    Ok(None) => {
                        dangling.push((key.to_vec(), resource_key));
                        true
                    }
                    Err(e) => {
                        error!("site {website_key}: load {resource_key}: {e}");
                        true
                    }
                }
            };
            if !keep_going {
                break;
            }
            it.next();
        }
        if let Err(e) = it.status() {
            error!("rocksdb iterator error in site scan: {e}");
        }
        drop(it);
        out.chunks.push(Bytes::from_static(b"]"));

        if let Some(reason) = out.truncated {
            metrics::counter!(tm::SITE_TRUNCATED, "reason" => reason).increment(1);
        }

        // Drop site index keys whose resource is gone. Re-check first so a
        // resource written since the scan keeps its index key.
        for (site_key, resource_key) in dangling {
            let res_key = format!("res:{resource_key}");
            if matches!(self.db.get_pinned(res_key.as_bytes()), Ok(None)) {
                if let Err(e) = self.db.delete(&site_key) {
                    warn!("delete dangling site key: {e}");
                }
            }
        }
        Ok(out)
    }

    // ------------------------------------------------------------ writes

    fn file_exists(&self, file_key: &[u8]) -> Result<bool, String> {
        // Bloom filter first: a new body (the common case) costs no read.
        if !self.db.key_may_exist(file_key) {
            return Ok(false);
        }
        timed(tm::DB_READ, || self.db.get_pinned(file_key))
            .map(|v| v.is_some())
            .map_err(|e| format!("rocksdb get file: {e}"))
    }

    /// Commit entries in one WriteBatch, then admit them to the mem cache.
    pub fn commit(&self, entries: Vec<PreparedEntry>) -> Result<usize, CommitError> {
        if entries.is_empty() {
            return Ok(0);
        }
        let _writer = self
            .gc
            .writers
            .try_acquire()
            .map_err(|_| CommitError::Busy)?;
        let count = entries.len();
        let mut batch = WriteBatch::default();
        {
            for e in &entries {
                self.gc.touch(e.resource.file_id.as_bytes());
            }

            let mut files_in_batch: HashSet<&str> = HashSet::new();
            // Resource key -> file_id of its previous version, including
            // earlier items of this same batch.
            let mut previous: HashMap<&str, String> = HashMap::new();

            for e in &entries {
                let r = &e.resource;
                if files_in_batch.insert(&r.file_id) {
                    let file_key = format!("file:{}", r.file_id);
                    if self.file_exists(file_key.as_bytes())? {
                        metrics::counter!(tm::DB_FILE_PUT_SKIPPED).increment(1);
                    } else {
                        metrics::counter!(tm::DB_FILE_PUT).increment(1);
                        batch.put(file_key.as_bytes(), e.body.as_ref());
                    }
                }

                let res_key = format!("res:{}", r.resource_key);
                let prev = match previous.remove(r.resource_key.as_str()) {
                    Some(p) => Some(p),
                    None => match timed(tm::DB_READ, || self.db.get_pinned(res_key.as_bytes())) {
                        Ok(Some(raw)) => serde_json::from_slice::<ResourceMeta>(&raw)
                            .ok()
                            .map(|m| m.file_id.into_owned()),
                        Ok(None) => None,
                        Err(e) => return Err(format!("rocksdb get resource: {e}").into()),
                    },
                };
                if let Some(old_file) = prev {
                    if old_file != r.file_id {
                        // Protect the new orphan marker from a collector
                        // whose snapshot still saw this body as live.
                        self.gc.touch(old_file.as_bytes());
                        batch.put(format!("orph:{old_file}").as_bytes(), b"");
                    }
                }
                // Site index keys are add-only. When an overwrite files the
                // resource under a different site, the old site keeps its
                // key too. Current clients depend on this: a single dump
                // files a resource under the crawl's site key (header), and
                // a later batch dump of the same resource carries the URL
                // host, so deleting the old key would drop the resource from
                // the site the crawler reads. A site key whose resource is
                // gone is removed lazily by the next site lookup.
                previous.insert(&r.resource_key, r.file_id.clone());

                let res_bytes =
                    serde_json::to_vec(r).map_err(|e| format!("serialize ResourceEntry: {e}"))?;
                batch.put(res_key.as_bytes(), &res_bytes);
                batch.put(
                    format!("site:{}::{}", r.website_key, r.resource_key).as_bytes(),
                    b"",
                );
            }

            #[cfg(test)]
            run_hook(&self.hooks.before_commit, self);
            timed(tm::DB_WRITE, || self.write_batch(batch, "batch"))?;
        }

        for e in entries {
            let key = e.resource.resource_key.clone();
            self.mem.insert(
                key,
                Arc::new(CachedResource {
                    resource: e.resource,
                    body: e.body,
                }),
            );
        }
        Ok(count)
    }

    // ------------------------------------------------------------ maintenance

    /// Drive one fair semaphore acquisition on this blocking storage thread.
    /// Once polled, Tokio reserves slots for this collector as writers finish;
    /// new try_acquire writers cannot pass it. Sleep on wake notifications,
    /// never spin or repeatedly race writers for an unqueued acquisition.
    /// The deadline bounds a stuck writer. Dropping the future on timeout
    /// returns any partial reservation and reopens writer admission.
    fn gc_exclusive(&self) -> Result<SemaphorePermit<'_>, String> {
        let deadline = Instant::now() + GC_ADMISSION_TIMEOUT;
        let (wake, ready) = sync_channel(1);
        let waker = Waker::from(Arc::new(AdmissionWake(wake)));
        let mut cx = Context::from_waker(&waker);
        // This future is driven by a blocking thread, not an async executor.
        // Disable cooperative task-budget yields so every Pending means an
        // actual semaphore wait, including tests called from an async task.
        let mut acquire = std::pin::pin!(tokio::task::unconstrained(
            self.gc.writers.acquire_many(WRITE_SLOTS)
        ));
        let timeout_error =
            || "storage busy: writers active after GC admission deadline".to_string();
        loop {
            if Instant::now() >= deadline {
                return Err(timeout_error());
            }
            match acquire.as_mut().poll(&mut cx) {
                Poll::Ready(result) => {
                    return result.map_err(|_| "storage busy: writer admission closed".to_string());
                }
                Poll::Pending => {}
            }
            #[cfg(test)]
            run_hook(&self.hooks.before_gc_wait, self);
            ready
                .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                .map_err(|_| timeout_error())?;
        }
    }

    fn write_batch(&self, batch: WriteBatch, operation: &str) -> Result<(), String> {
        #[cfg(test)]
        if self.hooks.fail_write.swap(false, Ordering::Relaxed) {
            return Err(format!("rocksdb {operation} write: injected I/O failure"));
        }
        self.db
            .write(batch)
            .map_err(|e| format!("rocksdb {operation} write: {e}"))
    }

    /// A storage read failure must abort the entire delete batch: its body
    /// may still be referenced by a resource we could not recheck. Malformed
    /// metadata is skipped by the collectors, as it is during their scans.
    fn gc_resource(&self, key: &[u8]) -> Result<Option<rocksdb::DBPinnableSlice<'_>>, String> {
        #[cfg(test)]
        if self.hooks.fail_gc_read.swap(false, Ordering::Relaxed) {
            return Err("rocksdb GC read: injected I/O failure".into());
        }
        self.db
            .get_pinned(key)
            .map_err(|e| format!("rocksdb GC read: {e}"))
    }

    // The caller holds maintenance admission until deletion and cache
    // invalidation finish, including all early-error and cancellation paths.
    fn begin_gc_epoch(&self) -> Result<rocksdb::SnapshotWithThreadMode<'_, DB>, String> {
        let _exclusive = self.gc_exclusive()?;
        self.gc.reset();
        Ok(self.db.snapshot())
    }

    /// TTL cleanup. Deletes expired resources, then every body that no live
    /// resource references. Bodies are only candidates if an expired
    /// resource or an overwrite pointed at them (or on a full sweep, if no
    /// resource does); each candidate is checked against every live
    /// resource and every write since the scan began before it is deleted.
    pub fn cleanup(
        &self,
        ttl_secs: i64,
        now: i64,
        full_sweep: bool,
    ) -> Result<CleanupResult, String> {
        let _maintenance = self.gc.maintenance()?;
        let started = Instant::now();
        let mut result = CleanupResult::default();

        let snap = self.begin_gc_epoch()?;
        let mut live: HashSet<Vec<u8>> = HashSet::new();
        let mut expired: Vec<String> = Vec::new();
        let mut candidates: HashSet<Vec<u8>> = HashSet::new();
        let mut markers: Vec<Vec<u8>> = Vec::new();

        {
            let mut it = snap.raw_iterator();
            it.seek(b"res:");
            while it.valid() {
                let (Some(k), Some(v)) = (it.key(), it.value()) else {
                    break;
                };
                if !k.starts_with(b"res:") {
                    break;
                }
                result.scanned += 1;
                match serde_json::from_slice::<ResourceMeta>(v) {
                    Ok(m) => {
                        let created = m.created_at.unwrap_or(now);
                        if now - created > ttl_secs {
                            expired.push(m.resource_key.into_owned());
                            candidates.insert(m.file_id.as_bytes().to_vec());
                        } else {
                            live.insert(m.file_id.as_bytes().to_vec());
                        }
                    }
                    Err(e) => warn!("cleanup: unreadable resource entry: {e}"),
                }
                it.next();
            }
            it.status().map_err(|e| format!("rocksdb iterator: {e}"))?;
        }
        {
            let mut it = snap.raw_iterator();
            it.seek(b"orph:");
            while it.valid() {
                let Some(k) = it.key() else { break };
                if !k.starts_with(b"orph:") {
                    break;
                }
                markers.push(k[5..].to_vec());
                candidates.insert(k[5..].to_vec());
                it.next();
            }
            it.status().map_err(|e| format!("rocksdb iterator: {e}"))?;
        }
        if full_sweep {
            let mut ro = ReadOptions::default();
            ro.fill_cache(false);
            let mut it = snap.raw_iterator_opt(ro);
            it.seek(b"file:");
            while it.valid() {
                let Some(k) = it.key() else { break };
                if !k.starts_with(b"file:") {
                    break;
                }
                if !live.contains(&k[5..]) {
                    candidates.insert(k[5..].to_vec());
                }
                it.next();
            }
            it.status().map_err(|e| format!("rocksdb iterator: {e}"))?;
        }
        drop(snap);
        candidates.retain(|f| !live.contains(f));
        #[cfg(test)]
        run_hook(&self.after_scan, self);

        let mut batch = WriteBatch::default();
        {
            let _exclusive = self.gc_exclusive()?;
            #[cfg(test)]
            run_hook(&self.hooks.before_delete, self);
            // Re-check each expired resource: a writer may have refreshed it
            // since the scan.
            for rk in &expired {
                let key = format!("res:{rk}");
                let Some(raw) = self.gc_resource(key.as_bytes())? else {
                    continue;
                };
                let m = match serde_json::from_slice::<ResourceMeta>(&raw) {
                    Ok(m) => m,
                    Err(e) => {
                        warn!("cleanup: unreadable resource entry during recheck: {e}");
                        continue;
                    }
                };
                if now - m.created_at.unwrap_or(now) > ttl_secs {
                    batch.delete(key.as_bytes());
                    batch.delete(format!("site:{}::{}", m.website_key, rk).as_bytes());
                    // If this body is protected this pass, keep durable
                    // work for the next incremental cleanup.
                    batch.put(format!("orph:{}", m.file_id).as_bytes(), b"");
                    result.expired_resource_keys.push(rk.clone());
                }
            }
            for f in &candidates {
                let Ok(fid) = std::str::from_utf8(f) else {
                    continue;
                };
                if self.gc.touched(f) {
                    // Full sweeps also find legacy orphans without markers.
                    // Retained candidates must remain incremental GC work.
                    batch.put(format!("orph:{fid}").as_bytes(), b"");
                    continue;
                }
                let fkey = format!("file:{fid}");
                if self.db.key_may_exist(fkey.as_bytes()) {
                    batch.delete(fkey.as_bytes());
                    result.removed_files += 1;
                }
                batch.delete(format!("orph:{fid}").as_bytes());
            }
            // Markers whose body turned out to be live are settled.
            for m in &markers {
                if live.contains(m) && !self.gc.touched(m) {
                    batch.delete([b"orph:".as_slice(), m].concat());
                }
            }
            self.write_batch(batch, "cleanup")?;
            for rk in &result.expired_resource_keys {
                self.mem.invalidate(rk);
            }
        }
        metrics::histogram!(tm::CLEANUP_DURATION).record(started.elapsed().as_secs_f64());
        metrics::counter!(tm::CLEANUP_REMOVED_RESOURCES)
            .increment(result.expired_resource_keys.len() as u64);
        metrics::counter!(tm::CLEANUP_REMOVED_FILES).increment(result.removed_files as u64);
        Ok(result)
    }

    /// Reserve before spawning so a flood of purge requests cannot queue
    /// scans. Dropping an unstarted job releases admission; a started job
    /// owns it through completion even when its requester disconnects.
    pub fn reserve_purge(
        self: &Arc<Self>,
    ) -> Result<impl FnOnce() -> Result<PurgeResult, String> + Send + 'static, String> {
        let permit = self.gc.maintenance()?;
        let store = self.clone();
        Ok(move || store.purge_empty_admitted(permit))
    }

    #[cfg(test)]
    pub fn purge_empty(&self) -> Result<PurgeResult, String> {
        self.purge_empty_admitted(self.gc.maintenance()?)
    }

    /// Remove resources whose body is an empty HTML page.
    fn purge_empty_admitted(
        &self,
        _maintenance: OwnedSemaphorePermit,
    ) -> Result<PurgeResult, String> {
        let mut empty: HashSet<String> = known_empty_page_file_ids();
        let snap = self.begin_gc_epoch()?;

        // Phase 1: scan small bodies for empty pages. Values are read in
        // place; nothing over 2 KiB is copied or parsed.
        {
            let mut ro = ReadOptions::default();
            ro.fill_cache(false);
            let mut it = snap.raw_iterator_opt(ro);
            it.seek(b"file:");
            let (mut scanned, mut skipped) = (0u64, 0u64);
            while it.valid() {
                let (Some(k), Some(v)) = (it.key(), it.value()) else {
                    break;
                };
                if !k.starts_with(b"file:") {
                    break;
                }
                if v.len() > 2048 {
                    skipped += 1;
                } else {
                    scanned += 1;
                    let is_empty = match decode_file_value(v) {
                        Ok(b) => is_empty_html(b.as_slice()),
                        Err(_) => false,
                    };
                    if is_empty {
                        if let Ok(fid) = std::str::from_utf8(&k[5..]) {
                            empty.insert(fid.to_string());
                        }
                    }
                }
                it.next();
            }
            it.status().map_err(|e| format!("rocksdb iterator: {e}"))?;
            info!("purge_empty: scanned {scanned} small bodies, skipped {skipped} large");
        }

        // Phase 2: find resources that point at an empty body.
        let mut purge: Vec<String> = Vec::new();
        let mut live: HashSet<String> = HashSet::new();
        {
            let mut it = snap.raw_iterator();
            it.seek(b"res:");
            while it.valid() {
                let (Some(k), Some(v)) = (it.key(), it.value()) else {
                    break;
                };
                if !k.starts_with(b"res:") {
                    break;
                }
                match serde_json::from_slice::<ResourceMeta>(v) {
                    Ok(m) => {
                        if empty.contains(m.file_id.as_ref()) {
                            purge.push(m.resource_key.into_owned());
                        } else {
                            live.insert(m.file_id.into_owned());
                        }
                    }
                    Err(e) => warn!("purge: unreadable resource entry: {e}"),
                }
                it.next();
            }
            it.status().map_err(|e| format!("rocksdb iterator: {e}"))?;
        }
        drop(snap);

        #[cfg(test)]
        run_hook(&self.after_scan, self);

        // Phase 3: drain existing writers with a deadline, then recheck writes.
        let mut result = PurgeResult::default();
        let mut batch = WriteBatch::default();
        {
            let _exclusive = self.gc_exclusive()?;
            #[cfg(test)]
            run_hook(&self.hooks.before_delete, self);
            for rk in &purge {
                let key = format!("res:{rk}");
                let Some(raw) = self.gc_resource(key.as_bytes())? else {
                    continue;
                };
                let m = match serde_json::from_slice::<ResourceMeta>(&raw) {
                    Ok(m) => m,
                    Err(e) => {
                        warn!("purge: unreadable resource entry during recheck: {e}");
                        continue;
                    }
                };
                if empty.contains(m.file_id.as_ref()) {
                    batch.delete(key.as_bytes());
                    batch.delete(format!("site:{}::{}", m.website_key, rk).as_bytes());
                    // If this body is protected this pass, keep durable
                    // work for the next incremental cleanup.
                    batch.put(format!("orph:{}", m.file_id).as_bytes(), b"");
                    result.resource_keys.push(rk.clone());
                }
            }
            for fid in &empty {
                if live.contains(fid) {
                    continue;
                }
                if self.gc.touched(fid.as_bytes()) {
                    batch.put(format!("orph:{fid}").as_bytes(), b"");
                    continue;
                }
                let fkey = format!("file:{fid}");
                if self
                    .db
                    .get_pinned(fkey.as_bytes())
                    .map_err(|e| format!("rocksdb purge read: {e}"))?
                    .is_some()
                {
                    batch.delete(fkey.as_bytes());
                    result.purged_files += 1;
                }
                batch.delete(format!("orph:{fid}").as_bytes());
            }
            self.write_batch(batch, "purge")?;
            for rk in &result.resource_keys {
                self.mem.invalidate(rk);
            }
        }
        result.purged_resources = result.resource_keys.len();
        Ok(result)
    }

    // ------------------------------------------------------------ sizes

    pub fn rocksdb_size(&self) -> RocksDbSize {
        let prop = |name: &str| self.db.property_int_value(name).ok().flatten();
        RocksDbSize {
            estimate_live_data_size_bytes: prop("rocksdb.estimate-live-data-size"),
            total_sst_files_size_bytes: prop("rocksdb.total-sst-files-size"),
            live_sst_files_size_bytes: prop("rocksdb.live-sst-files-size"),
            estimate_num_keys: prop("rocksdb.estimate-num-keys"),
            total_blob_file_size_bytes: prop("rocksdb.total-blob-file-size"),
        }
    }

    pub fn mem_cache_size(&self) -> MemCacheSize {
        self.mem.run_pending_tasks();
        let mut body_bytes = 0u64;
        for (_, v) in self.mem.iter() {
            body_bytes += v.body.len() as u64;
        }
        MemCacheSize {
            entries: self.mem.entry_count() as usize,
            body_bytes,
        }
    }

    pub fn size_report(&self) -> io::Result<CacheSizeReport> {
        Ok(CacheSizeReport {
            rocksdb: self.rocksdb_size(),
            rocksdb_dir_bytes: dir_size_bytes(&self.path)?,
            mem_cache: self.mem_cache_size(),
        })
    }
}

/// A body read from RocksDB: borrowed for the current raw format, owned for
/// the legacy JSON format.
pub enum BodyRef<'a> {
    Borrowed(&'a [u8]),
    Owned(Vec<u8>),
}

impl BodyRef<'_> {
    pub fn as_slice(&self) -> &[u8] {
        match self {
            BodyRef::Borrowed(b) => b,
            BodyRef::Owned(v) => v,
        }
    }
    fn into_owned_bytes(self) -> Bytes {
        match self {
            BodyRef::Borrowed(b) => Bytes::copy_from_slice(b),
            BodyRef::Owned(v) => Bytes::from(v),
        }
    }
}

/// Stored bodies are raw bytes. Entries written before v0.2 are JSON
/// `FileEntry` objects; those start with `{"file_id"`. A raw body that
/// happens to start with `{` (a JSON API response) is returned as is.
fn decode_file_value(raw: &[u8]) -> Result<BodyRef<'_>, String> {
    if raw.starts_with(b"{\"file_id\"") {
        if let Ok(f) = serde_json::from_slice::<FileEntry>(raw) {
            return Ok(BodyRef::Owned(f.body));
        }
    }
    Ok(BodyRef::Borrowed(raw))
}

/// Sum of file sizes under `root`, skipping symlinks.
pub fn dir_size_bytes(root: impl AsRef<Path>) -> io::Result<u64> {
    let mut total: u64 = 0;
    let mut stack: Vec<PathBuf> = vec![root.as_ref().to_path_buf()];
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

/// Known empty HTML page bodies, so the purge can match them by hash.
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
    patterns.iter().map(|p| compute_file_id(p)).collect()
}

/// True if `body` is an empty HTML page, raw or base64 encoded.
pub fn is_empty_html(body: &[u8]) -> bool {
    if body.is_empty() {
        return true;
    }
    if let Ok(text) = std::str::from_utf8(body) {
        if is_empty_html_str(text) {
            return true;
        }
        let trimmed = text.trim();
        if !trimmed.is_empty() && !trimmed.starts_with('<') {
            if let Ok(decoded) = STANDARD.decode(trimmed.as_bytes()) {
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

fn is_empty_html_str(text: &str) -> bool {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return true;
    }
    let lower = trimmed.to_ascii_lowercase();
    let html = lower
        .strip_prefix("<!doctype html>")
        .unwrap_or(&lower)
        .trim();
    if !html.starts_with("<html") {
        return false;
    }
    let mut inside_tag = false;
    for ch in html.chars() {
        if ch == '<' {
            inside_tag = true;
        } else if ch == '>' {
            inside_tag = false;
        } else if !inside_tag && !ch.is_whitespace() {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod gc_tests {
    use super::*;

    #[test]
    fn gc_write_admission_has_no_64_commit_ceiling() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = Config::for_tests(dir.path().join("db").to_str().unwrap());
        let store = Store::open(&cfg).unwrap();
        // Hold the same slots as 64 in-flight commits, then perform real I/O
        // with a 65th. Upload byte budgets remain the capacity bound.
        let _writers = store.gc.writers.try_acquire_many(64).unwrap();
        assert_eq!(
            store
                .commit(vec![crate::tests::gc_entry("extra", b"body", 100)])
                .unwrap(),
            1
        );
        store.mem.invalidate_all();
        assert_eq!(
            &store.get_resource("extra", false).unwrap().unwrap().body[..],
            b"body"
        );
    }

    #[test]
    fn gc_admission_timeout_returns_partial_reservations_and_reopens_writes() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = Config::for_tests(dir.path().join("db").to_str().unwrap());
        let store = Store::open(&cfg).unwrap();
        let writer = store.gc.writers.try_acquire().unwrap();
        let started = Instant::now();
        assert!(store
            .gc_exclusive()
            .unwrap_err()
            .contains("admission deadline"));
        assert!(started.elapsed() >= GC_ADMISSION_TIMEOUT);
        assert!(started.elapsed() < GC_ADMISSION_TIMEOUT + Duration::from_secs(2));
        assert_eq!(
            store.gc.writers.available_permits(),
            WRITE_SLOTS as usize - 1
        );
        // A failed collector must not keep its gate closed even though the
        // original writer is still running.
        assert_eq!(
            store
                .commit(vec![crate::tests::gc_entry("retry", b"body", 100)])
                .unwrap(),
            1
        );
        drop(writer);
        assert!(store.gc_exclusive().is_ok());
    }

    #[test]
    fn gc_saturated_filter_defers_bodies_without_losing_incremental_work() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = Config::for_tests(dir.path().join("db").to_str().unwrap());
        let store = Store::open(&cfg).unwrap();
        let live = b"referenced body";
        let live_id = compute_file_id(live);
        store
            .commit(vec![PreparedEntry {
                resource: ResourceEntry {
                    website_key: "gc".into(),
                    resource_key: "live".into(),
                    url: "https://gc/".into(),
                    method: "GET".into(),
                    status: 200,
                    request_headers: HashMap::new(),
                    response_headers: HashMap::new(),
                    file_id: live_id.clone(),
                    http_version: Default::default(),
                    created_at: Some(100),
                },
                body: Bytes::from_static(live),
            }])
            .unwrap();
        // A legacy orphan with no marker: even a false positive must not
        // require another full sweep to make progress after saturation ends.
        store.db.put(b"file:legacy", b"unreferenced").unwrap();
        *store.after_scan.lock().unwrap() = Some(Box::new(|store| {
            for word in &store.gc.touched {
                word.store(u64::MAX, Ordering::Relaxed);
            }
        }));
        assert_eq!(store.cleanup(10, 100, true).unwrap().removed_files, 0);
        assert!(store.db.get(b"file:legacy").unwrap().is_some());
        assert!(store.db.get(b"orph:legacy").unwrap().is_some());
        assert_eq!(store.cleanup(10, 100, false).unwrap().removed_files, 1);
        assert!(store.db.get(b"file:legacy").unwrap().is_none());
        assert!(store.db.get(b"orph:legacy").unwrap().is_none());
        store.mem.invalidate_all();
        assert_eq!(
            &store.get_resource("live", false).unwrap().unwrap().body[..],
            live
        );
        assert!(store.db.get(format!("file:{live_id}")).unwrap().is_some());
    }
}
