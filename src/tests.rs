use std::{collections::HashMap, sync::Arc};

use base64::{engine::general_purpose::STANDARD, Engine};
use bytes::Bytes;
use http::{Request, StatusCode};
use http_body_util::{BodyExt, Full};
use rocksdb::{Options, DB};
use tempfile::TempDir;

use crate::{
    config::Config,
    model::{CachedEntryPayload, FileEntry, HttpVersion, IncomingPayload, ResourceEntry},
    server::{compressible_content_type, handle, AppState},
    store::{compute_file_id, is_empty_html, prepare_entry, Store},
};

struct TestEnv {
    _dir: TempDir,
    state: Arc<AppState>,
}

fn env_with(tweak: impl FnOnce(&mut Config)) -> TestEnv {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("db");
    let mut cfg = Config::for_tests(path.to_str().unwrap());
    tweak(&mut cfg);
    let store = Store::open(&cfg).expect("open store");
    TestEnv {
        _dir: dir,
        state: Arc::new(AppState::new(cfg, store, None, None)),
    }
}

fn env() -> TestEnv {
    env_with(|_| {})
}

fn payload(site: Option<&str>, key: &str, url: &str, body: &[u8]) -> CachedEntryPayload {
    let mut resp_headers = HashMap::new();
    resp_headers.insert("Content-Type".to_string(), "text/html".to_string());
    CachedEntryPayload {
        website_key: site.map(str::to_string),
        resource_key: key.to_string(),
        url: url.to_string(),
        method: "GET".to_string(),
        status: 200,
        request_headers: HashMap::new(),
        response_headers: resp_headers,
        body_base64: STANDARD.encode(body),
        http_version: HttpVersion::Http11,
        created_at: None,
    }
}

async fn call(
    state: &Arc<AppState>,
    req: Request<Full<Bytes>>,
) -> (StatusCode, http::HeaderMap, Bytes) {
    let resp = handle(req, state.clone()).await.unwrap();
    let status = resp.status();
    let headers = resp.headers().clone();
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    (status, headers, body)
}

fn post(path: &str, site_header: Option<&str>, json: Vec<u8>) -> Request<Full<Bytes>> {
    let mut b = Request::post(path).header("content-type", "application/json");
    if let Some(s) = site_header {
        b = b.header("x-cache-site", s);
    }
    b.header("content-length", json.len())
        .body(Full::new(Bytes::from(json)))
        .unwrap()
}

fn get(path: &str) -> Request<Full<Bytes>> {
    Request::get(path).body(Full::new(Bytes::new())).unwrap()
}

async fn index_one(state: &Arc<AppState>, p: &CachedEntryPayload, header: Option<&str>) {
    let (s, _, b) = call(
        state,
        post("/cache/index", header, serde_json::to_vec(p).unwrap()),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "{}", String::from_utf8_lossy(&b));
}

async fn site_items(state: &Arc<AppState>, site: &str) -> Vec<CachedEntryPayload> {
    let (s, _, b) = call(state, get(&format!("/cache/site/{site}"))).await;
    assert_eq!(s, StatusCode::OK);
    serde_json::from_slice(&b).expect("site json")
}

// ---------------------------------------------------------------- basics

#[tokio::test]
async fn index_and_retrieve_single_resource() {
    let t = env();
    let body = b"console.log('hi');";
    let p = payload(
        Some("example.com"),
        "k1",
        "https://example.com/script.js",
        body,
    );
    index_one(&t.state, &p, None).await;

    let (s, _, b) = call(&t.state, get("/cache/resource/k1")).await;
    assert_eq!(s, StatusCode::OK);
    let got: CachedEntryPayload = serde_json::from_slice(&b).unwrap();
    assert_eq!(got.website_key.as_deref(), Some("example.com"));
    assert_eq!(got.resource_key, "k1");
    assert_eq!(STANDARD.decode(got.body_base64).unwrap(), body);
    assert_eq!(
        got.response_headers.get("Content-Type").unwrap(),
        "text/html"
    );

    let (s, _, raw) = call(&t.state, get("/cache/resource/k1?raw=1")).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(&raw[..], body);

    let (s, _, _) = call(&t.state, get("/cache/resource/missing")).await;
    assert_eq!(s, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn resource_read_from_rocksdb_after_mem_eviction() {
    let t = env();
    let body = vec![b'x'; 200_000];
    index_one(
        &t.state,
        &payload(Some("s"), "big", "https://s/big", &body),
        None,
    )
    .await;
    t.state.store.mem.invalidate_all();
    t.state.store.mem.run_pending_tasks();
    let (s, _, b) = call(&t.state, get("/cache/resource/big")).await;
    assert_eq!(s, StatusCode::OK);
    let got: CachedEntryPayload = serde_json::from_slice(&b).unwrap();
    assert_eq!(STANDARD.decode(got.body_base64).unwrap(), body);
}

#[tokio::test]
async fn deduplicates_shared_body_across_resources() {
    let t = env();
    let body = b"/* shared body across sites */";
    index_one(
        &t.state,
        &payload(Some("a.com"), "r1", "https://a.com/x.js", body),
        None,
    )
    .await;
    index_one(
        &t.state,
        &payload(Some("b.com"), "r2", "https://cdn.b.com/x.js", body),
        None,
    )
    .await;

    let r1 = t.state.store.get_resource("r1", false).unwrap().unwrap();
    let r2 = t.state.store.get_resource("r2", false).unwrap().unwrap();
    assert_eq!(r1.resource.file_id, r2.resource.file_id);

    let files = t
        .state
        .store
        .db
        .prefix_iterator(b"file:")
        .take_while(|kv| {
            kv.as_ref()
                .map(|(k, _)| k.starts_with(b"file:"))
                .unwrap_or(false)
        })
        .count();
    assert_eq!(files, 1);
}

#[test]
fn payload_encoder_matches_serde() {
    let mut req = HashMap::new();
    req.insert("Accept".to_string(), "text/\"html\"".to_string());
    let r = ResourceEntry {
        website_key: "w\u{e9}b".into(),
        resource_key: "k\n1".into(),
        url: "https://x/\\y".into(),
        method: "GET".into(),
        status: 404,
        request_headers: req,
        response_headers: HashMap::new(),
        file_id: "f".into(),
        http_version: HttpVersion::H2,
        created_at: Some(1),
    };
    for body in [
        &b""[..],
        b"a",
        b"ab",
        b"abc",
        b"abcd",
        &[0xff, 0x00, 0x10][..],
    ] {
        let enc = crate::model::encode_payload(&r, body).unwrap();
        let got: CachedEntryPayload = serde_json::from_slice(&enc).unwrap();
        assert_eq!(got.resource_key, r.resource_key);
        assert_eq!(got.url, r.url);
        assert_eq!(got.status, 404);
        assert_eq!(got.http_version, HttpVersion::H2);
        assert_eq!(got.request_headers, r.request_headers);
        assert_eq!(got.website_key.as_deref(), Some("w\u{e9}b"));
        assert_eq!(STANDARD.decode(got.body_base64).unwrap(), body);
        let enc2 = crate::model::PayloadEncoder::new(&r, body.len()).unwrap();
        assert_eq!(enc2.len(), enc.len());
    }
}

// ---------------------------------------------------------------- site keys

#[tokio::test]
async fn batch_item_site_key_wins_over_header() {
    let t = env();
    let items = vec![
        payload(Some("a.com"), "a1", "https://a.com/1", b"one"),
        payload(Some("b.com"), "b1", "https://b.com/1", b"two"),
        // No site key of its own: falls back to the header.
        payload(None, "n1", "https://n.com/1", b"three"),
        // Empty site key counts as none.
        payload(Some(""), "e1", "https://e.com/1", b"four"),
    ];
    let (s, _, b) = call(
        &t.state,
        post(
            "/cache/index/batch",
            Some("a.com"),
            serde_json::to_vec(&items).unwrap(),
        ),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED);
    assert_eq!(&b[..], b"Indexed 4 entries");

    let mut a: Vec<_> = site_items(&t.state, "a.com")
        .await
        .into_iter()
        .map(|p| p.resource_key)
        .collect();
    a.sort();
    assert_eq!(a, vec!["a1", "e1", "n1"]);
    let b: Vec<_> = site_items(&t.state, "b.com")
        .await
        .into_iter()
        .map(|p| p.resource_key)
        .collect();
    assert_eq!(b, vec!["b1"]);
}

#[tokio::test]
async fn batch_without_header_uses_item_then_url_host() {
    let t = env();
    let items = vec![
        payload(Some("a.com"), "a1", "https://a.com/1", b"one"),
        payload(None, "h1", "https://host.example/1", b"two"),
    ];
    let (s, _, _) = call(
        &t.state,
        post(
            "/cache/index/batch",
            None,
            serde_json::to_vec(&items).unwrap(),
        ),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED);
    assert_eq!(site_items(&t.state, "a.com").await.len(), 1);
    assert_eq!(site_items(&t.state, "host.example").await.len(), 1);
}

#[tokio::test]
async fn single_index_header_still_wins() {
    let t = env();
    let p = payload(
        Some("cdn.example.com"),
        "s1",
        "https://cdn.example.com/x",
        b"x",
    );
    index_one(&t.state, &p, Some("crawl-site-hash")).await;
    assert_eq!(site_items(&t.state, "crawl-site-hash").await.len(), 1);
    assert!(site_items(&t.state, "cdn.example.com").await.is_empty());
}

#[tokio::test]
async fn overwrite_with_new_site_keeps_old_index_key() {
    let t = env();
    index_one(
        &t.state,
        &payload(Some("old.com"), "k", "https://x/k", b"v1"),
        None,
    )
    .await;
    index_one(
        &t.state,
        &payload(Some("new.com"), "k", "https://x/k", b"v2"),
        None,
    )
    .await;
    // Add-only: both sites list the resource, with its latest body.
    for site in ["old.com", "new.com"] {
        let items = site_items(&t.state, site).await;
        assert_eq!(items.len(), 1, "{site}");
        assert_eq!(STANDARD.decode(&items[0].body_base64).unwrap(), b"v2");
    }
}

/// The sequence the fleet produces with spider_remote_cache 0.3/0.4: a single
/// dump files the resource under the crawl's site key from the header, then
/// a batch re-dump of the same resource carries website_key = URL host. The
/// resource must stay listed under the header's site key.
#[tokio::test]
async fn batch_redump_keeps_resource_under_single_dump_site() {
    let t = env();
    let site_hash = "a1b2c3site";
    index_one(
        &t.state,
        &payload(Some("example.com"), "rk", "https://example.com/p", b"first"),
        Some(site_hash),
    )
    .await;
    assert_eq!(site_items(&t.state, site_hash).await.len(), 1);

    let batch = vec![payload(
        Some("example.com"),
        "rk",
        "https://example.com/p",
        b"second",
    )];
    let (s, _, _) = call(
        &t.state,
        post(
            "/cache/index/batch",
            Some("example.com"),
            serde_json::to_vec(&batch).unwrap(),
        ),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED);

    let items = site_items(&t.state, site_hash).await;
    assert_eq!(
        items.len(),
        1,
        "batch re-dump dropped the resource from the crawl's site"
    );
    assert_eq!(STANDARD.decode(&items[0].body_base64).unwrap(), b"second");
    assert_eq!(site_items(&t.state, "example.com").await.len(), 1);
}

#[tokio::test]
async fn missing_body_is_a_miss_not_an_error() {
    let t = env();
    index_one(
        &t.state,
        &payload(Some("a"), "gone", "https://a/gone", b"body"),
        None,
    )
    .await;
    t.state.store.mem.invalidate_all();
    t.state.store.mem.run_pending_tasks();
    let fid = compute_file_id(b"body");
    t.state.store.db.delete(format!("file:{fid}")).unwrap();
    let (s, _, _) = call(&t.state, get("/cache/resource/gone")).await;
    assert_eq!(s, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn read_fill_does_not_replace_a_newer_cached_entry() {
    let t = env();
    let store = &t.state.store;
    index_one(
        &t.state,
        &payload(Some("a"), "k", "https://a/k", b"old"),
        None,
    )
    .await;
    store.mem.invalidate_all();
    store.mem.run_pending_tasks();
    // A newer version lands in the mem cache (as commit() does) while the
    // disk still holds the old one; the read must not overwrite it.
    let newer = Arc::new(crate::model::CachedResource {
        resource: store
            .get_resource("k", false)
            .unwrap()
            .unwrap()
            .resource
            .clone(),
        body: Bytes::from_static(b"newer"),
    });
    store.mem.insert("k".to_string(), newer);
    store.mem.invalidate("k"); // force the miss path below...
    store.mem.run_pending_tasks();
    let loaded = store.get_resource("k", true).unwrap().unwrap();
    assert_eq!(&loaded.body[..], b"old");
    // ...and with the newer entry present, a fill keeps it.
    let newer = Arc::new(crate::model::CachedResource {
        resource: loaded.resource.clone(),
        body: Bytes::from_static(b"newer"),
    });
    store.mem.insert("k".to_string(), newer);
    let got = store
        .mem
        .entry("k".to_string())
        .or_insert(loaded)
        .into_value();
    assert_eq!(&got.body[..], b"newer");
}

// ---------------------------------------------------------------- path decoding

/// Percent-encode everything but unreserved characters, like the
/// `urlencoding` crate browser_server uses.
fn urlencode(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}

#[tokio::test]
async fn encoded_resource_key_resolves_to_raw_key() {
    let t = env();
    let key = "GET:https://example.com/a/b?q=1 2";
    index_one(
        &t.state,
        &payload(
            Some("example.com"),
            key,
            "https://example.com/a/b",
            b"hello",
        ),
        None,
    )
    .await;
    let path = format!("/cache/resource/{}", urlencode(key));
    let (s, _, b) = call(&t.state, get(&path)).await;
    assert_eq!(s, StatusCode::OK, "{path}");
    let got: CachedEntryPayload = serde_json::from_slice(&b).unwrap();
    assert_eq!(got.resource_key, key);
    let (s, _, raw) = call(&t.state, get(&format!("{path}?raw=1"))).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(&raw[..], b"hello");
}

#[tokio::test]
async fn raw_keys_still_resolve() {
    let t = env();
    // No percent at all.
    index_one(
        &t.state,
        &payload(Some("a"), "plainkey123", "https://a/", b"x"),
        None,
    )
    .await;
    let (s, _, _) = call(&t.state, get("/cache/resource/plainkey123")).await;
    assert_eq!(s, StatusCode::OK);
    // A stored key that itself contains '%' is still found as sent.
    index_one(
        &t.state,
        &payload(Some("a"), "k%41", "https://a/", b"y"),
        None,
    )
    .await;
    let (s, _, b) = call(&t.state, get("/cache/resource/k%41")).await;
    assert_eq!(s, StatusCode::OK);
    let got: CachedEntryPayload = serde_json::from_slice(&b).unwrap();
    assert_eq!(got.resource_key, "k%41");
    // Invalid encodings fall back to the raw segment.
    index_one(
        &t.state,
        &payload(Some("a"), "bad%ZZ%FF", "https://a/", b"z"),
        None,
    )
    .await;
    let (s, _, _) = call(&t.state, get("/cache/resource/bad%ZZ%FF")).await;
    assert_eq!(s, StatusCode::OK);
}

#[tokio::test]
async fn site_key_decoding_is_a_no_op_for_hex() {
    let t = env();
    let hex = "a".repeat(64);
    index_one(
        &t.state,
        &payload(None, "r1", "https://x/", b"x"),
        Some(&hex),
    )
    .await;
    assert_eq!(site_items(&t.state, &hex).await.len(), 1);
    assert!(crate::server::decoded_segment(&hex).is_none());
    // An encoded site key resolves too.
    index_one(
        &t.state,
        &payload(None, "r2", "https://x/", b"y"),
        Some("my site"),
    )
    .await;
    assert_eq!(site_items(&t.state, "my%20site").await.len(), 1);
}

#[tokio::test]
async fn payload_carries_created_at() {
    let t = env();
    index_one(
        &t.state,
        &payload(Some("a"), "c1", "https://a/", b"x"),
        None,
    )
    .await;
    let (_, _, b) = call(&t.state, get("/cache/resource/c1")).await;
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let ts = v["created_at"].as_i64().expect("created_at present");
    assert!((now - ts).abs() < 60);
    let items = site_items(&t.state, "a").await;
    assert!(items[0].created_at.is_some());

    // A legacy entry without created_at omits the field.
    let mut e: ResourceEntry =
        serde_json::from_slice(&t.state.store.db.get(b"res:c1").unwrap().unwrap()).unwrap();
    e.created_at = None;
    let enc = crate::model::encode_payload(&e, b"x").unwrap();
    let v: serde_json::Value = serde_json::from_slice(&enc).unwrap();
    assert!(v.get("created_at").is_none());
}

#[tokio::test]
async fn small_requests_do_not_queue_behind_a_large_reservation() {
    let t = env_with(|c| c.request_timeout = std::time::Duration::from_secs(2));
    index_one(
        &t.state,
        &payload(Some("a"), "small", "https://a/", b"tiny"),
        None,
    )
    .await;
    // A large write holds, and another waits for, the whole main budget.
    let all = t.state.cfg.max_inflight_bytes as u32;
    let held = t
        .state
        .budget
        .clone()
        .acquire_many_owned(all)
        .await
        .unwrap();
    let waiter = {
        let b = t.state.budget.clone();
        tokio::spawn(async move { b.acquire_many_owned(all).await.map(drop) })
    };
    tokio::task::yield_now().await;
    // Small reads and writes still go through.
    let (s, _, _) = call(&t.state, get("/cache/resource/small")).await;
    assert_eq!(s, StatusCode::OK);
    index_one(
        &t.state,
        &payload(Some("a"), "small2", "https://a/", b"tiny2"),
        None,
    )
    .await;
    drop(held);
    waiter.await.unwrap().unwrap();
}

#[test]
fn default_body_limit_fits_the_largest_fleet_batch() {
    // 16 bodies of 5 MiB as base64 JSON is about 107 MiB.
    let cfg = Config::from_env();
    assert!(cfg.max_body_bytes >= 107 * 1024 * 1024);
    assert!(cfg.upload_timeout >= std::time::Duration::from_secs(120));
}

// ---------------------------------------------------------------- batch failures

#[tokio::test]
async fn batch_partial_failure_is_201_with_count() {
    let t = env();
    let mut bad = payload(Some("a.com"), "bad", "https://a.com/bad", b"x");
    bad.body_base64 = "!!!not base64!!!".into();
    let items = vec![
        payload(Some("a.com"), "good", "https://a.com/good", b"ok"),
        bad,
    ];
    let (s, _, b) = call(
        &t.state,
        post(
            "/cache/index/batch",
            None,
            serde_json::to_vec(&items).unwrap(),
        ),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED);
    assert_eq!(&b[..], b"Indexed 1 entries, 1 failed");

    let mut only_bad = payload(Some("a.com"), "bad", "https://a.com/bad", b"x");
    only_bad.body_base64 = "!!!".into();
    let (s, _, _) = call(
        &t.state,
        post(
            "/cache/index/batch",
            None,
            serde_json::to_vec(&vec![only_bad]).unwrap(),
        ),
    )
    .await;
    assert_eq!(s, StatusCode::INTERNAL_SERVER_ERROR);

    let (s, _, _) = call(&t.state, post("/cache/index", None, b"{not json".to_vec())).await;
    assert_eq!(s, StatusCode::BAD_REQUEST);
}

// ---------------------------------------------------------------- limits

#[tokio::test]
async fn body_over_limit_is_413() {
    let t = env_with(|c| c.max_body_bytes = 4096);
    let p = payload(Some("a"), "k", "https://a/k", &vec![b'z'; 8192]);
    let json = serde_json::to_vec(&p).unwrap();

    // Declared length over the limit.
    let (s, _, _) = call(&t.state, post("/cache/index", None, json.clone())).await;
    assert_eq!(s, StatusCode::PAYLOAD_TOO_LARGE);

    // No declared length: the streaming limit catches it.
    let req = Request::post("/cache/index/batch")
        .body(Full::new(Bytes::from(json)))
        .unwrap();
    let (s, _, _) = call(&t.state, req).await;
    assert_eq!(s, StatusCode::PAYLOAD_TOO_LARGE);

    // Under the limit still works.
    let small = payload(Some("a"), "k2", "https://a/k2", b"small");
    let (s, _, _) = call(
        &t.state,
        post("/cache/index", None, serde_json::to_vec(&small).unwrap()),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED);
}

#[tokio::test]
async fn site_response_is_capped() {
    let t = env_with(|c| c.max_site_response_bytes = 64 * 1024);
    for i in 0..10 {
        let body = vec![b'a' + i as u8; 20_000];
        index_one(
            &t.state,
            &payload(Some("big"), &format!("k{i}"), "https://big/", &body),
            None,
        )
        .await;
    }
    let (s, h, b) = call(&t.state, get("/cache/site/big")).await;
    assert_eq!(s, StatusCode::OK);
    assert!(b.len() <= 64 * 1024, "{} bytes", b.len());
    assert_eq!(h.get("x-cache-truncated").unwrap(), "size");
    let items: Vec<CachedEntryPayload> = serde_json::from_slice(&b).unwrap();
    assert!(!items.is_empty() && items.len() < 10);

    // All budget released once the response is gone.
    assert_eq!(
        t.state.budget.available_permits() as u64,
        t.state.cfg.max_inflight_bytes
    );
}

#[tokio::test]
async fn site_response_respects_inflight_budget() {
    let t = env_with(|c| c.max_inflight_bytes = 2 * 1024 * 1024);
    for i in 0..4 {
        let body = vec![b'a' + i as u8; 20_000];
        index_one(
            &t.state,
            &payload(Some("s"), &format!("k{i}"), "https://s/", &body),
            None,
        )
        .await;
    }
    // Hold almost all of the budget.
    let held = t
        .state
        .budget
        .clone()
        .try_acquire_many_owned(2 * 1024 * 1024 - 30_000)
        .unwrap();
    let (s, h, b) = call(&t.state, get("/cache/site/s")).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(h.get("x-cache-truncated").unwrap(), "budget");
    let items: Vec<CachedEntryPayload> = serde_json::from_slice(&b).unwrap();
    assert_eq!(items.len(), 1);
    drop(held);
    assert_eq!(site_items(&t.state, "s").await.len(), 4);
}

#[tokio::test]
async fn mem_cache_bound_is_respected() {
    let cap = 1024 * 1024;
    let t = env_with(|c| c.mem_cache_bytes = cap);
    for i in 0..40 {
        let body = vec![i as u8; 100_000];
        index_one(
            &t.state,
            &payload(Some("m"), &format!("k{i}"), "https://m/", &body),
            None,
        )
        .await;
    }
    t.state.store.mem.run_pending_tasks();
    assert!(
        t.state.store.mem.weighted_size() <= cap,
        "{}",
        t.state.store.mem.weighted_size()
    );
    // Everything is still readable from RocksDB.
    for i in 0..40 {
        let r = t
            .state
            .store
            .get_resource(&format!("k{i}"), true)
            .unwrap()
            .unwrap();
        assert_eq!(r.body.len(), 100_000);
    }
    t.state.store.mem.run_pending_tasks();
    assert!(t.state.store.mem.weighted_size() <= cap);
}

#[tokio::test]
async fn bad_stored_content_type_does_not_panic() {
    let t = env();
    let mut p = payload(Some("a"), "ct", "https://a/ct", b"body");
    p.response_headers
        .insert("Content-Type".into(), "text/html\u{7f}\r\nx-evil: 1".into());
    index_one(&t.state, &p, None).await;
    let (s, h, b) = call(&t.state, get("/cache/resource/ct?raw=1")).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(h.get("content-type").unwrap(), "application/octet-stream");
    assert_eq!(&b[..], b"body");
}

#[tokio::test]
async fn health_and_not_found() {
    let t = env();
    let (s, _, b) = call(&t.state, get("/health")).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(&b[..], b"ok");
    let (s, _, _) = call(&t.state, get("/nope")).await;
    assert_eq!(s, StatusCode::NOT_FOUND);
    let (s, _, b) = call(&t.state, get("/cache/size")).await;
    assert_eq!(s, StatusCode::OK);
    let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert!(v["mem_cache"]["entries"].is_number());
}

#[test]
fn compression_predicate_content_types() {
    for ct in [
        "text/html; charset=utf-8",
        "application/json",
        "application/ld+json",
        "application/javascript",
        "text/css",
        "application/xml",
        "image/svg+xml",
    ] {
        assert!(compressible_content_type(ct), "{ct}");
    }
    for ct in [
        "application/octet-stream",
        "font/woff2",
        "image/png",
        "video/mp4",
        "application/zip",
        "",
        "text/event-stream",
    ] {
        assert!(!compressible_content_type(ct), "{ct}");
    }
}

// ---------------------------------------------------------------- empty page purge

#[test]
fn is_empty_html_detects_empty_pages() {
    assert!(is_empty_html(b"<html><head></head><body></body></html>"));
    assert!(is_empty_html(
        b"<!DOCTYPE html><html><head></head><body></body></html>"
    ));
    assert!(is_empty_html(
        b"<html>\n  <head></head>\n  <body>  </body>\n</html>"
    ));
    assert!(is_empty_html(
        b"<html lang=\"en\"><head></head><body class=\"x\"></body></html>"
    ));
    assert!(is_empty_html(b""));
    assert!(is_empty_html(b"   \n\t  "));
}

#[test]
fn is_empty_html_rejects_non_empty_pages() {
    assert!(!is_empty_html(
        b"<html><head></head><body>Hello</body></html>"
    ));
    assert!(!is_empty_html(b"console.log('hi');"));
    assert!(!is_empty_html(&[0xFF, 0xD8, 0xFF, 0xE0]));
    assert!(!is_empty_html(
        b"<html><head><title>Test</title></head><body></body></html>"
    ));
}

#[tokio::test]
async fn purge_empty_removes_empty_html_resources() {
    let t = env();
    let empty = b"<html><head></head><body></body></html>";
    let real = b"<html><head></head><body><h1>Hello World</h1></body></html>";
    index_one(
        &t.state,
        &payload(
            Some("example.com"),
            "GET:https://example.com/",
            "https://example.com/",
            empty,
        ),
        None,
    )
    .await;
    index_one(
        &t.state,
        &payload(
            Some("example.com"),
            "GET:https://example.com/about",
            "https://example.com/about",
            real,
        ),
        None,
    )
    .await;

    let store = &t.state.store;
    let r = store.purge_empty().expect("purge");
    assert_eq!(r.purged_resources, 1);
    assert_eq!(r.purged_files, 1);
    assert_eq!(r.resource_keys, vec!["GET:https://example.com/"]);
    assert!(store
        .get_resource("GET:https://example.com/", false)
        .unwrap()
        .is_none());
    assert!(store
        .get_resource("GET:https://example.com/about", false)
        .unwrap()
        .is_some());
}

// ---------------------------------------------------------------- cleanup

fn file_exists(store: &Store, fid: &str) -> bool {
    store
        .db
        .get_pinned(format!("file:{fid}"))
        .unwrap()
        .is_some()
}

#[tokio::test]
async fn cleanup_keeps_shared_body_and_collects_orphans() {
    let t = env();
    let store = &t.state.store;
    let shared = b"shared body".to_vec();
    index_one(
        &t.state,
        &payload(Some("a"), "old", "https://a/old", &shared),
        None,
    )
    .await;
    // Backdate "old" so it expires.
    let mut e: ResourceEntry =
        serde_json::from_slice(&store.db.get(b"res:old").unwrap().unwrap()).unwrap();
    e.created_at = Some(0);
    store
        .db
        .put(b"res:old", serde_json::to_vec(&e).unwrap())
        .unwrap();
    index_one(
        &t.state,
        &payload(Some("b"), "fresh", "https://b/fresh", &shared),
        None,
    )
    .await;

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let r = store.cleanup(86_400, now, false).unwrap();
    assert_eq!(r.expired_resource_keys, vec!["old"]);
    let fid = compute_file_id(&shared);
    assert!(file_exists(store, &fid), "shared body was deleted");
    assert!(store.get_resource("fresh", false).unwrap().is_some());
    assert!(store.get_resource("old", false).unwrap().is_none());

    // Overwrite "fresh" with a new body: the old one becomes an orphan.
    index_one(
        &t.state,
        &payload(Some("b"), "fresh", "https://b/fresh", b"v2"),
        None,
    )
    .await;
    let r = store.cleanup(86_400, now, false).unwrap();
    assert_eq!(r.removed_files, 1);
    assert!(!file_exists(store, &fid));
    assert!(file_exists(store, &compute_file_id(b"v2")));
    assert!(store
        .db
        .prefix_iterator(b"orph:")
        .next()
        .map(|kv| !kv.unwrap().0.starts_with(b"orph:"))
        .unwrap_or(true));
}

#[tokio::test]
async fn cleanup_never_deletes_body_reused_during_the_scan() {
    let t = env();
    let store = &t.state.store;
    let body = b"reused".to_vec();
    let fid = compute_file_id(&body);
    // An orphaned body: on disk with a marker, nothing points at it.
    store.db.put(format!("file:{fid}"), &body).unwrap();
    store.db.put(format!("orph:{fid}"), b"").unwrap();

    // While cleanup sits between its scan and its deletes, a writer indexes
    // a resource with that same body. The writer sees the body exists and
    // skips the put, so deleting it now would leave a dangling resource.
    let body2 = body.clone();
    *store.after_scan.lock().unwrap() = Some(Box::new(move |s: &Store| {
        let json = serde_json::to_vec(&payload(Some("a"), "k", "https://a/k", &body2)).unwrap();
        let p: IncomingPayload = serde_json::from_slice(&json).unwrap();
        s.commit(vec![prepare_entry(p, "a".into()).unwrap()])
            .unwrap();
    }));
    let r = store.cleanup(86_400, 1, true).unwrap();
    assert_eq!(r.removed_files, 0);
    assert!(file_exists(store, &fid), "body reused mid-scan was deleted");
    assert_eq!(
        &store.get_resource("k", false).unwrap().unwrap().body[..],
        &body[..]
    );

    // Next pass: it is referenced, so it stays and its marker is settled.
    let r = store.cleanup(86_400, 1, false).unwrap();
    assert_eq!(r.removed_files, 0);
    assert!(file_exists(store, &fid));
    assert!(store.db.get(format!("orph:{fid}")).unwrap().is_none());

    // Control: without the concurrent writer the orphan is collected.
    store.db.put(b"file:cafe", b"x").unwrap();
    store.db.put(b"orph:cafe", b"").unwrap();
    let r = store.cleanup(86_400, 1, false).unwrap();
    assert_eq!(r.removed_files, 1);
    assert!(!file_exists(store, "cafe"));
}

#[tokio::test]
async fn full_sweep_collects_pre_v03_orphans() {
    let t = env();
    let store = &t.state.store;
    // A v0.2-era orphan: a body nobody references and no marker.
    store.db.put(b"file:deadbeef", b"orphan").unwrap();
    index_one(
        &t.state,
        &payload(Some("a"), "k", "https://a/k", b"live"),
        None,
    )
    .await;
    let r = store.cleanup(86_400, 1, false).unwrap();
    assert_eq!(r.removed_files, 0);
    let r = store.cleanup(86_400, 1, true).unwrap();
    assert_eq!(r.removed_files, 1);
    assert!(!file_exists(store, "deadbeef"));
    assert!(file_exists(store, &compute_file_id(b"live")));
}

// ---------------------------------------------------------------- GC admission races

pub(crate) fn gc_entry(key: &str, body: &[u8], created_at: i64) -> crate::store::PreparedEntry {
    let json = serde_json::to_vec(&payload(Some("gc"), key, "https://gc/", body)).unwrap();
    let p: IncomingPayload = serde_json::from_slice(&json).unwrap();
    let mut e = prepare_entry(p, "gc".into()).unwrap();
    e.resource.created_at = Some(created_at);
    e
}

fn gc_orphan(store: &Store, body: &[u8]) -> String {
    let fid = compute_file_id(body);
    store.db.put(format!("file:{fid}"), body).unwrap();
    store.db.put(format!("orph:{fid}"), b"").unwrap();
    fid
}

fn gc_collect(store: &Store, purge: bool) -> Result<(usize, usize), String> {
    if purge {
        store
            .purge_empty()
            .map(|r| (r.purged_resources, r.purged_files))
    } else {
        store
            .cleanup(10, 100, true)
            .map(|r| (r.expired_resource_keys.len(), r.removed_files))
    }
}

const GC_EMPTY: &[u8] = b"<html><head></head><body></body></html>";
const GC_WAIT: std::time::Duration = std::time::Duration::from_secs(10);

/// Every barrier has a deadline, and an assertion failure releases its peer.
/// Production code never waits on these test-only hooks.
struct GcPause {
    entered: std::sync::mpsc::Receiver<()>,
    release: std::sync::mpsc::SyncSender<()>,
}

impl GcPause {
    fn at(hook: &std::sync::Mutex<Option<crate::store::TestHook>>) -> Self {
        let (entered_tx, entered) = std::sync::mpsc::sync_channel(1);
        let (release, release_rx) = std::sync::mpsc::sync_channel(1);
        *hook.lock().unwrap() = Some(Box::new(move |_| {
            entered_tx.send(()).unwrap();
            release_rx
                .recv_timeout(GC_WAIT)
                .expect("test did not release GC barrier");
        }));
        Self { entered, release }
    }

    fn wait(&self) {
        self.entered
            .recv_timeout(GC_WAIT)
            .expect("worker did not reach GC barrier");
    }
}

impl Drop for GcPause {
    fn drop(&mut self) {
        let _ = self.release.try_send(());
    }
}

fn gc_assert_body(store: &Store, key: &str, expected: &[u8]) {
    // Bypass memory: a cached body must not hide a dangling on-disk reference.
    store.mem.invalidate_all();
    assert_eq!(
        &store.get_resource(key, false).unwrap().unwrap().body[..],
        expected
    );
}

#[test]
fn gc_scan_preserves_reuse_when_competing_maintenance_is_rejected() {
    for purge in [false, true] {
        let t = env();
        let store = &t.state.store;
        let fid = gc_orphan(store, GC_EMPTY);
        let control = gc_orphan(store, b"<html lang=\"control\"><body></body></html>");
        let pause = GcPause::at(&store.after_scan);
        std::thread::scope(|scope| {
            let gc = scope.spawn(|| gc_collect(store, purge));
            pause.wait();
            store
                .commit(vec![gc_entry("reused", GC_EMPTY, 100)])
                .unwrap();
            // Both the other collector and a duplicate request must fail
            // without resetting the first collector's protection bitmap.
            assert!(store
                .cleanup(10, 100, true)
                .unwrap_err()
                .contains("maintenance active"));
            assert!(store
                .purge_empty()
                .unwrap_err()
                .contains("maintenance active"));
            drop(pause);
            assert_eq!(gc.join().unwrap().unwrap(), (0, 1));
        });
        assert!(file_exists(store, &fid));
        assert!(!file_exists(store, &control));
        gc_assert_body(store, "reused", GC_EMPTY);
        assert_eq!(store.cleanup(10, 100, false).unwrap().removed_files, 0);
        assert!(store.db.get(format!("orph:{fid}")).unwrap().is_none());
    }
}

#[test]
fn gc_snapshot_cannot_pass_an_inflight_body_existence_check() {
    let t = env();
    let store = &t.state.store;
    let fid = gc_orphan(store, GC_EMPTY);
    let pause = GcPause::at(&store.hooks.before_commit);
    std::thread::scope(|scope| {
        let writer = scope.spawn(|| store.commit(vec![gc_entry("reused", GC_EMPTY, 100)]));
        pause.wait(); // The writer has decided to skip the existing file put.
        for purge in [false, true] {
            assert!(gc_collect(store, purge)
                .unwrap_err()
                .contains("writers active"));
        }
        assert!(file_exists(store, &fid));
        drop(pause);
        assert_eq!(writer.join().unwrap().unwrap(), 1);
    });
    assert_eq!(store.cleanup(10, 100, false).unwrap().removed_files, 0);
    gc_assert_body(store, "reused", GC_EMPTY);
}

#[test]
fn gc_delete_cannot_pass_a_writer_that_started_after_the_snapshot() {
    for purge in [false, true] {
        let t = env();
        let store = &t.state.store;
        let fid = gc_orphan(store, GC_EMPTY);
        let scan = GcPause::at(&store.after_scan);
        let write = GcPause::at(&store.hooks.before_commit);
        std::thread::scope(|scope| {
            let gc = scope.spawn(|| gc_collect(store, purge));
            scan.wait();
            let writer = scope.spawn(|| store.commit(vec![gc_entry("reused", GC_EMPTY, 100)]));
            write.wait();
            drop(scan);
            assert!(gc.join().unwrap().unwrap_err().contains("writers active"));
            assert!(file_exists(store, &fid));
            drop(write);
            writer.join().unwrap().unwrap();
        });
        assert_eq!(store.cleanup(10, 100, false).unwrap().removed_files, 0);
        gc_assert_body(store, "reused", GC_EMPTY);
    }
}

#[test]
fn gc_delete_sheds_writers_and_retry_recreates_the_collected_body() {
    for purge in [false, true] {
        let t = env();
        let store = &t.state.store;
        let fid = gc_orphan(store, GC_EMPTY);
        let pause = GcPause::at(&store.hooks.before_delete);
        std::thread::scope(|scope| {
            let gc = scope.spawn(|| gc_collect(store, purge));
            pause.wait();
            assert!(store
                .commit(vec![gc_entry("retry", GC_EMPTY, 100)])
                .unwrap_err()
                .to_string()
                .contains("storage busy"));
            assert!(store.db.get(b"res:retry").unwrap().is_none());
            assert!(store.get_cached("retry").is_none());
            drop(pause);
            assert_eq!(gc.join().unwrap().unwrap(), (0, 1));
        });
        assert!(!file_exists(store, &fid));
        store
            .commit(vec![gc_entry("retry", GC_EMPTY, 100)])
            .unwrap();
        gc_assert_body(store, "retry", GC_EMPTY);
    }
}

#[test]
fn gc_collectors_finish_under_continuous_writes_at_both_admission_phases() {
    use crate::store::CommitError;
    use std::sync::mpsc::{self, RecvTimeoutError};
    use std::time::Duration;

    for purge in [false, true] {
        for after_snapshot in [false, true] {
            let t = env();
            let store = &t.state.store;
            store
                .commit(vec![
                    gc_entry("expired", GC_EMPTY, 0),
                    gc_entry("survivor", b"referenced", 100),
                ])
                .unwrap();
            let control = gc_orphan(store, b"<html lang=\"control\"><body></body></html>");
            let scan = after_snapshot.then(|| GcPause::at(&store.after_scan));
            let queued = GcPause::at(&store.hooks.before_gc_wait);
            std::thread::scope(|scope| {
                let (start, start_rx) = mpsc::sync_channel(1);
                let (done, done_rx) = mpsc::sync_channel(1);
                let collector = scope.spawn(move || {
                    start_rx.recv_timeout(GC_WAIT).unwrap();
                    let result = gc_collect(store, purge);
                    done.send(()).unwrap();
                    result
                });
                if let Some(scan) = &scan {
                    start.send(()).unwrap();
                    scan.wait();
                }
                let write = GcPause::at(&store.hooks.before_commit);
                let reused = if after_snapshot {
                    GC_EMPTY
                } else {
                    b"live write"
                };
                let writer =
                    scope.spawn(move || store.commit(vec![gc_entry("reused", reused, 100)]));
                write.wait(); // No zero-writer gap is possible before admission queues.

                let (stop, stop_rx) = mpsc::channel::<()>();
                let (active, active_rx) = mpsc::sync_channel(1);
                let (shed, shed_rx) = mpsc::sync_channel(1);
                let traffic = scope.spawn(move || {
                    let (mut committed, mut rejected) = (0, 0);
                    loop {
                        match stop_rx.recv_timeout(Duration::from_millis(1)) {
                            Ok(()) | Err(RecvTimeoutError::Disconnected) => break,
                            Err(RecvTimeoutError::Timeout) => {}
                        }
                        match store.commit(vec![gc_entry("traffic", b"ongoing traffic", 100)]) {
                            Ok(1) => {
                                committed += 1;
                                let _ = active.try_send(());
                            }
                            Err(CommitError::Busy) => {
                                rejected += 1;
                                let _ = shed.try_send(());
                            }
                            result => panic!("unexpected traffic result: {result:?}"),
                        }
                    }
                    (committed, rejected)
                });
                active_rx.recv_timeout(GC_WAIT).unwrap();
                if after_snapshot {
                    drop(scan);
                } else {
                    start.send(()).unwrap();
                }
                queued.wait();
                shed_rx.recv_timeout(GC_WAIT).unwrap();
                // Fair admission must close the gate before the writer drains.
                assert!(matches!(
                    store.commit(vec![gc_entry("must_retry", b"later", 100)]),
                    Err(CommitError::Busy)
                ));
                drop(queued);
                drop(write);
                assert_eq!(writer.join().unwrap().unwrap(), 1);
                done_rx
                    .recv_timeout(GC_WAIT)
                    .expect("collector starved under writes");
                // Traffic is still running when collection finishes.
                let result = collector.join().unwrap().unwrap();
                drop(stop);
                let (committed, rejected) = traffic.join().unwrap();
                assert!(committed > 0 && rejected > 0);
                assert_eq!(result, (1, if after_snapshot { 1 } else { 2 }));
                gc_assert_body(store, "reused", reused);
            });
            assert!(!file_exists(store, &control));
            assert!(store.db.get(b"res:expired").unwrap().is_none());
            gc_assert_body(store, "survivor", b"referenced");
            gc_assert_body(store, "traffic", b"ongoing traffic");
        }
    }
}

#[tokio::test]
async fn gc_upload_shedding_is_retryable_during_pending_and_deleting_collection() {
    for purge in [false, true] {
        for pending in [false, true] {
            let t = env();
            let store = &t.state.store;
            gc_orphan(store, GC_EMPTY);
            let writer = if pending {
                let pause = GcPause::at(&store.hooks.before_commit);
                let st = store.clone();
                let job = tokio::task::spawn_blocking(move || {
                    st.commit(vec![gc_entry("inflight", b"live write", 100)])
                });
                pause.wait();
                Some((pause, job))
            } else {
                None
            };
            let pause = GcPause::at(if pending {
                &store.hooks.before_gc_wait
            } else {
                &store.hooks.before_delete
            });
            let st = store.clone();
            let collector = tokio::task::spawn_blocking(move || gc_collect(&st, purge));
            pause.wait();
            for batch in [false, true] {
                let p = payload(Some("gc"), "retry", "https://gc/retry", GC_EMPTY);
                let (path, json) = if batch {
                    ("/cache/index/batch", serde_json::to_vec(&vec![p]).unwrap())
                } else {
                    ("/cache/index", serde_json::to_vec(&p).unwrap())
                };
                let (status, headers, _) = tokio::time::timeout(
                    std::time::Duration::from_secs(1),
                    call(&t.state, post(path, None, json)),
                )
                .await
                .unwrap();
                assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
                assert_eq!(headers["retry-after"], "1");
                assert!(store.db.get(b"res:retry").unwrap().is_none());
                assert!(store.db.get(b"site:gc::retry").unwrap().is_none());
                assert!(store.get_cached("retry").is_none());
            }
            drop(pause);
            if let Some((pause, job)) = writer {
                drop(pause);
                assert_eq!(job.await.unwrap().unwrap(), 1);
            }
            assert_eq!(collector.await.unwrap().unwrap(), (0, 1));
            index_one(
                &t.state,
                &payload(Some("gc"), "retry", "https://gc/retry", GC_EMPTY),
                None,
            )
            .await;
            gc_assert_body(store, "retry", GC_EMPTY);

            // Genuine storage failures keep their error classification.
            store
                .hooks
                .fail_write
                .store(true, std::sync::atomic::Ordering::Relaxed);
            let (status, headers, _) = call(
                &t.state,
                post(
                    "/cache/index",
                    None,
                    serde_json::to_vec(&payload(
                        Some("gc"),
                        "failed",
                        "https://gc/failed",
                        b"failure",
                    ))
                    .unwrap(),
                ),
            )
            .await;
            assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
            assert!(!headers.contains_key("retry-after"));
            assert!(store.db.get(b"res:failed").unwrap().is_none());
        }
    }
}

#[tokio::test]
async fn cleanup_worker_retries_a_failed_full_sweep_on_its_next_pass() {
    use std::time::Duration;
    let t = env_with(|cfg| {
        cfg.cleanup_interval = Duration::from_millis(20);
        cfg.full_sweep_every = 10_000;
    });
    t.state
        .store
        .db
        .put(b"file:legacy", b"unmarked orphan")
        .unwrap();
    t.state
        .store
        .hooks
        .fail_write
        .store(true, std::sync::atomic::Ordering::Relaxed);
    let pause = GcPause::at(&t.state.store.hooks.before_delete);
    let worker = tokio::spawn(crate::run_cleanup_worker(t.state.clone()));
    let pause = tokio::task::spawn_blocking(move || {
        pause.wait();
        pause
    })
    .await
    .unwrap();
    assert!(t.state.store.db.get(b"file:legacy").unwrap().is_some());
    drop(pause); // First full sweep fails its batch; the next must still be full.
    let result = tokio::time::timeout(GC_WAIT, async {
        loop {
            let store = t.state.store.clone();
            if tokio::task::spawn_blocking(move || store.db.get(b"file:legacy").unwrap())
                .await
                .unwrap()
                .is_none()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    worker.abort();
    assert!(worker.await.unwrap_err().is_cancelled());
    result.expect("failed full sweep was skipped until the next full-sweep cycle");
}

#[test]
fn gc_refresh_during_scan_preserves_resource_and_durable_orphan_work() {
    for purge in [false, true] {
        let t = env();
        let store = &t.state.store;
        store
            .commit(vec![gc_entry("refresh", GC_EMPTY, 0)])
            .unwrap();
        let old = compute_file_id(GC_EMPTY);
        let pause = GcPause::at(&store.after_scan);
        std::thread::scope(|scope| {
            let gc = scope.spawn(|| gc_collect(store, purge));
            pause.wait();
            store
                .commit(vec![gc_entry("refresh", b"fresh body", 100)])
                .unwrap();
            drop(pause);
            assert_eq!(gc.join().unwrap().unwrap(), (0, 0));
        });
        gc_assert_body(store, "refresh", b"fresh body");
        assert!(store.db.get(format!("orph:{old}")).unwrap().is_some());
        assert_eq!(store.cleanup(10, 100, false).unwrap().removed_files, 1);
        assert!(!file_exists(store, &old));
    }
}

#[test]
fn gc_scan_does_not_settle_a_marker_recreated_by_an_overwrite() {
    let t = env();
    let store = &t.state.store;
    store
        .commit(vec![gc_entry("live", b"old live body", 100)])
        .unwrap();
    let old = compute_file_id(b"old live body");
    store.db.put(format!("orph:{old}"), b"").unwrap();
    let pause = GcPause::at(&store.after_scan);
    std::thread::scope(|scope| {
        let gc = scope.spawn(|| store.cleanup(10, 100, false));
        pause.wait();
        store
            .commit(vec![gc_entry("live", b"new live body", 100)])
            .unwrap();
        drop(pause);
        assert_eq!(gc.join().unwrap().unwrap().removed_files, 0);
    });
    assert!(store.db.get(format!("orph:{old}")).unwrap().is_some());
    assert_eq!(store.cleanup(10, 100, false).unwrap().removed_files, 1);
    gc_assert_body(store, "live", b"new live body");
}

#[test]
fn gc_io_failure_aborts_deletion_and_releases_all_admission() {
    use std::sync::atomic::Ordering;
    for purge in [false, true] {
        for read_failure in [false, true] {
            let t = env();
            let store = &t.state.store;
            store
                .commit(vec![
                    gc_entry("expired", GC_EMPTY, 0),
                    gc_entry("survivor", b"referenced", 100),
                ])
                .unwrap();
            let fail = if read_failure {
                &store.hooks.fail_gc_read
            } else {
                &store.hooks.fail_write
            };
            fail.store(true, Ordering::Relaxed);
            assert!(gc_collect(store, purge)
                .unwrap_err()
                .contains("injected I/O failure"));
            assert!(store.db.get(b"site:gc::expired").unwrap().is_some());
            assert!(store.get_cached("expired").is_some());
            gc_assert_body(store, "expired", GC_EMPTY);
            gc_assert_body(store, "survivor", b"referenced");
            assert_eq!(gc_collect(store, purge).unwrap(), (1, 1));
            assert!(store.db.get(b"res:expired").unwrap().is_none());
            gc_assert_body(store, "survivor", b"referenced");
        }
    }
}

#[test]
fn gc_failed_writer_neither_publishes_memory_nor_leaks_epoch_admission() {
    for purge in [false, true] {
        let t = env();
        let store = &t.state.store;
        let fid = gc_orphan(store, GC_EMPTY);
        let pause = GcPause::at(&store.after_scan);
        std::thread::scope(|scope| {
            let gc = scope.spawn(|| gc_collect(store, purge));
            pause.wait();
            store
                .hooks
                .fail_write
                .store(true, std::sync::atomic::Ordering::Relaxed);
            assert!(store
                .commit(vec![gc_entry("failed", GC_EMPTY, 100)])
                .unwrap_err()
                .to_string()
                .contains("injected I/O failure"));
            drop(pause);
            assert_eq!(gc.join().unwrap().unwrap(), (0, 0));
        });
        assert!(store.get_cached("failed").is_none());
        assert!(store.db.get(b"res:failed").unwrap().is_none());
        assert!(file_exists(store, &fid));
        assert_eq!(store.cleanup(10, 100, false).unwrap().removed_files, 1);
        assert!(!file_exists(store, &fid));
    }
}

#[tokio::test]
async fn gc_cancelled_waiter_cannot_release_a_running_scan_epoch() {
    for purge in [false, true] {
        let t = env();
        let store = &t.state.store;
        gc_orphan(store, GC_EMPTY);
        let pause = GcPause::at(&store.after_scan);
        let st = store.clone();
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        let job = tokio::task::spawn_blocking(move || {
            let result = gc_collect(&st, purge);
            let _ = done_tx.send(result);
        });
        pause.wait();
        let waiter = tokio::spawn(job);
        waiter.abort();
        assert!(waiter.await.unwrap_err().is_cancelled());
        assert!(gc_collect(store, !purge)
            .unwrap_err()
            .contains("maintenance active"));
        store
            .commit(vec![gc_entry("reused", GC_EMPTY, 100)])
            .unwrap();
        drop(pause);
        assert_eq!(
            tokio::time::timeout(GC_WAIT, done_rx)
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
            (0, 0)
        );
        gc_assert_body(store, "reused", GC_EMPTY);
        assert_eq!(store.cleanup(10, 100, false).unwrap().removed_files, 0);
    }
}

#[tokio::test]
async fn gc_cancelled_writer_keeps_its_slot_through_the_database_commit() {
    let t = env();
    let store = &t.state.store;
    gc_orphan(store, GC_EMPTY);
    let pause = GcPause::at(&store.hooks.before_commit);
    let st = store.clone();
    let (done_tx, done_rx) = tokio::sync::oneshot::channel();
    let job = tokio::task::spawn_blocking(move || {
        let result = st.commit(vec![gc_entry("reused", GC_EMPTY, 100)]);
        let _ = done_tx.send(result);
    });
    pause.wait();
    let waiter = tokio::spawn(job);
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    for purge in [false, true] {
        assert!(gc_collect(store, purge)
            .unwrap_err()
            .contains("writers active"));
    }
    drop(pause);
    assert_eq!(
        tokio::time::timeout(GC_WAIT, done_rx)
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        1
    );
    assert_eq!(store.cleanup(10, 100, false).unwrap().removed_files, 0);
    gc_assert_body(store, "reused", GC_EMPTY);
}

#[tokio::test]
async fn gc_cancelled_requester_cannot_reopen_a_pending_collectors_gate() {
    for purge in [false, true] {
        let t = env();
        let store = &t.state.store;
        let orphan = gc_orphan(store, GC_EMPTY);
        let write = GcPause::at(&store.hooks.before_commit);
        let st = store.clone();
        let writer = tokio::task::spawn_blocking(move || {
            st.commit(vec![gc_entry("inflight", b"referenced", 100)])
        });
        write.wait();
        let queued = GcPause::at(&store.hooks.before_gc_wait);
        let st = store.clone();
        let (done, done_rx) = tokio::sync::oneshot::channel();
        let collector = tokio::task::spawn_blocking(move || {
            let _ = done.send(gc_collect(&st, purge));
        });
        queued.wait();
        let requester = tokio::spawn(collector);
        requester.abort();
        assert!(requester.await.unwrap_err().is_cancelled());
        assert!(gc_collect(store, !purge)
            .unwrap_err()
            .contains("maintenance active"));
        assert!(matches!(
            store.commit(vec![gc_entry("retry", GC_EMPTY, 100)]),
            Err(crate::store::CommitError::Busy)
        ));
        assert!(file_exists(store, &orphan));
        drop(queued);
        drop(write);
        assert_eq!(writer.await.unwrap().unwrap(), 1);
        assert_eq!(
            tokio::time::timeout(GC_WAIT, done_rx)
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
            (0, 1)
        );
        gc_assert_body(store, "inflight", b"referenced");
        store
            .commit(vec![gc_entry("retry", GC_EMPTY, 100)])
            .unwrap();
        gc_assert_body(store, "retry", GC_EMPTY);
    }
}

#[test]
fn gc_dropping_an_unstarted_purge_releases_its_reservation() {
    let t = env();
    let store = &t.state.store;
    let job = store.reserve_purge().unwrap();
    assert!(store.reserve_purge().is_err());
    assert!(store.cleanup(10, 100, true).is_err());
    drop(job);
    store.cleanup(10, 100, true).unwrap();
    store.reserve_purge().unwrap()().unwrap();
}

#[test]
fn gc_real_database_write_errors_preserve_references_on_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = Config::for_tests(dir.path().join("db").to_str().unwrap());
    {
        let store = Store::open(&cfg).unwrap();
        store
            .commit(vec![
                gc_entry("expired", GC_EMPTY, 0),
                gc_entry("survivor", b"referenced", 100),
            ])
            .unwrap();
        store.db.flush().unwrap();
    }
    {
        let db = DB::open_for_read_only(&crate::store::db_options(&cfg), &cfg.rocksdb_path, false)
            .unwrap();
        let store = Store::with_db(Arc::new(db), &cfg);
        // Real RocksDB failures, not only fault injection. Repeated calls
        // must get an I/O error rather than an admission slot leaked by one.
        for _ in 0..2 {
            assert!(store
                .commit(vec![gc_entry("failed", b"new", 100)])
                .unwrap_err()
                .to_string()
                .contains("rocksdb batch write"));
            for purge in [false, true] {
                let error = gc_collect(&store, purge).unwrap_err();
                assert!(error.contains("write"), "{error}");
                assert!(!error.contains("busy"), "{error}");
                gc_assert_body(&store, "expired", GC_EMPTY);
                gc_assert_body(&store, "survivor", b"referenced");
                assert!(store.db.get(b"site:gc::expired").unwrap().is_some());
            }
            assert!(store.get_cached("failed").is_none());
            assert!(store.db.get(b"res:failed").unwrap().is_none());
        }
    }
    {
        let store = Store::open(&cfg).unwrap();
        assert_eq!(gc_collect(&store, false).unwrap(), (1, 1));
        store.db.flush().unwrap();
    }
    let store = Store::open(&cfg).unwrap();
    assert!(store.db.get(b"res:expired").unwrap().is_none());
    assert!(!file_exists(&store, &compute_file_id(GC_EMPTY)));
    gc_assert_body(&store, "survivor", b"referenced");
}

#[test]
fn gc_unreadable_rows_do_not_block_collection() {
    for purge in [false, true] {
        let t = env();
        let store = &t.state.store;
        store
            .commit(vec![
                gc_entry("expired", GC_EMPTY, 0),
                gc_entry("survivor", b"referenced", 100),
            ])
            .unwrap();
        let orphan = gc_orphan(store, b"<html lang=\"orphan\"><body></body></html>");
        let bad_rows: &[(&[u8], &[u8])] = &[
            (b"res:broken", b"{unreadable reference"),
            (b"res:broken-schema", br#"{"file_id":123}"#),
        ];
        for &(key, value) in bad_rows {
            store.db.put(key, value).unwrap();
        }

        assert_eq!(gc_collect(store, purge).unwrap(), (1, 2));
        assert!(store.db.get(b"res:expired").unwrap().is_none());
        assert!(store.db.get(b"site:gc::expired").unwrap().is_none());
        assert!(!file_exists(store, &compute_file_id(GC_EMPTY)));
        assert!(!file_exists(store, &orphan));
        assert!(store.db.get(format!("orph:{orphan}")).unwrap().is_none());
        gc_assert_body(store, "survivor", b"referenced");
        for &(key, value) in bad_rows {
            assert_eq!(store.db.get(key).unwrap().as_deref(), Some(value));
        }

        // The same unreadable rows must not stop subsequent passes either.
        store
            .commit(vec![gc_entry("expired_again", GC_EMPTY, 0)])
            .unwrap();
        assert_eq!(gc_collect(store, purge).unwrap(), (1, 1));
        gc_assert_body(store, "survivor", b"referenced");
    }
}

#[test]
fn gc_unreadable_recheck_does_not_block_collection_or_lose_reused_body() {
    for purge in [false, true] {
        let t = env();
        let store = &t.state.store;
        store
            .commit(vec![
                gc_entry("a-broken", GC_EMPTY, 0),
                gc_entry("z-expired", GC_EMPTY, 0),
                gc_entry("survivor", b"referenced", 100),
            ])
            .unwrap();
        let orphan = gc_orphan(store, b"<html lang=\"orphan\"><body></body></html>");
        let scan = GcPause::at(&store.after_scan);
        std::thread::scope(|scope| {
            let gc = scope.spawn(|| gc_collect(store, purge));
            scan.wait();
            // Inject corruption after the snapshot selected this resource.
            store
                .db
                .put(b"res:a-broken", b"{unreadable reference")
                .unwrap();
            store
                .commit(vec![gc_entry("reused", GC_EMPTY, 100)])
                .unwrap();
            drop(scan);
            assert_eq!(gc.join().unwrap().unwrap(), (1, 1));
        });
        assert_eq!(
            store.db.get(b"res:a-broken").unwrap().as_deref(),
            Some(b"{unreadable reference".as_slice())
        );
        assert!(store.db.get(b"res:z-expired").unwrap().is_none());
        assert!(store.db.get(b"site:gc::z-expired").unwrap().is_none());
        assert!(!file_exists(store, &orphan));
        assert!(store.db.get(format!("orph:{orphan}")).unwrap().is_none());
        gc_assert_body(store, "reused", GC_EMPTY);
        gc_assert_body(store, "survivor", b"referenced");
        assert!(store
            .db
            .get(format!("orph:{}", compute_file_id(GC_EMPTY)))
            .unwrap()
            .is_some());
    }
}

#[tokio::test]
async fn gc_purge_http_admission_is_bounded_before_spawning() {
    let t = env();
    let job = t.state.store.reserve_purge().unwrap();
    let (status, headers, _) = call(&t.state, post("/cache/purge/empty", None, Vec::new())).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(headers["retry-after"], "1");
    drop(job);
    t.state.store.reserve_purge().unwrap()().unwrap();
}

#[tokio::test]
async fn size_report_sheds_when_blocking_read_capacity_is_full() {
    let t = env_with(|cfg| cfg.max_blocking_reads = 1);
    let _slot = t.state.blocking_reads.clone().try_acquire_owned().unwrap();
    let (status, headers, _) = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        call(&t.state, get("/cache/size")),
    )
    .await
    .unwrap();
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(headers["retry-after"], "1");
}

#[test]
fn startup_capacities_reject_zero_and_oversized_values_without_constructing_channels() {
    use crate::config::MeiliConfig;
    let good = Config::for_tests("unused");
    good.validate().unwrap();
    for value in [0, 1_000_001, usize::MAX] {
        let mut cfg = good.clone();
        cfg.max_connections = value;
        assert!(cfg.validate().unwrap_err().contains("MAX_CONNECTIONS"));
        cfg = good.clone();
        cfg.max_blocking_reads = value;
        assert!(cfg.validate().unwrap_err().contains("MAX_BLOCKING_READS"));
        cfg = good.clone();
        cfg.meili = Some(MeiliConfig {
            host: "unused".into(),
            key: "".into(),
            index: "unused".into(),
            queue_cap: value,
            batch_max: 256,
            flush_every: std::time::Duration::from_millis(200),
        });
        assert!(cfg.validate().unwrap_err().contains("MEILI_QUEUE_CAP"));
    }
    for value in [0, tokio::sync::Semaphore::MAX_PERMITS as u64 + 1, u64::MAX] {
        let mut cfg = good.clone();
        cfg.max_inflight_bytes = value;
        assert!(cfg.validate().unwrap_err().contains("MAX_INFLIGHT_BYTES"));
        cfg = good.clone();
        cfg.max_inflight_small_bytes = value;
        assert!(cfg
            .validate()
            .unwrap_err()
            .contains("MAX_INFLIGHT_SMALL_BYTES"));
    }
}

#[tokio::test(start_paused = true)]
async fn incomplete_upload_deadline_releases_its_memory_budget() {
    struct PendingBody;
    impl http_body::Body for PendingBody {
        type Data = Bytes;
        type Error = std::convert::Infallible;
        fn poll_frame(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Result<http_body::Frame<Bytes>, Self::Error>>> {
            std::task::Poll::Pending
        }
    }
    let t = env_with(|cfg| cfg.upload_timeout = std::time::Duration::from_secs(2));
    let before = t.state.budget.available_permits();
    let small_before = t.state.small_budget.available_permits();
    let response = handle(
        Request::post("/cache/index").body(PendingBody).unwrap(),
        t.state.clone(),
    )
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(t.state.budget.available_permits(), before);
    assert_eq!(t.state.small_budget.available_permits(), small_before);
}

// ---------------------------------------------------------------- v0.2.3 compatibility

/// Options exactly as v0.2.3 set them (main.rs 334-367 at bdf83f6).
fn v023_options() -> Options {
    let mut o = Options::default();
    o.create_if_missing(true);
    let mut block = rocksdb::BlockBasedOptions::default();
    block.set_bloom_filter(10.0, false);
    block.set_block_cache(&rocksdb::Cache::new_lru_cache(8 * 1024 * 1024));
    block.set_cache_index_and_filter_blocks(true);
    block.set_pin_l0_filter_and_index_blocks_in_cache(true);
    o.set_block_based_table_factory(&block);
    o.set_write_buffer_size(64 * 1024 * 1024);
    o.set_max_write_buffer_number(3);
    o.set_min_write_buffer_number_to_merge(1);
    o.set_enable_pipelined_write(true);
    o.set_level_compaction_dynamic_level_bytes(true);
    o.set_max_bytes_for_level_base(256 * 1024 * 1024);
    o.set_compression_type(rocksdb::DBCompressionType::Lz4);
    o
}

/// Write records the way v0.2.3 did: raw bodies, one legacy JSON body,
/// ResourceEntry JSON, site index keys.
fn write_v023_records(db: &DB, n: usize) -> Vec<(String, Vec<u8>)> {
    let mut out = Vec::new();
    for i in 0..n {
        let body: Vec<u8> = format!("<html>{}</html>", "y".repeat(i * 3000)).into_bytes();
        let fid = compute_file_id(&body);
        let key = format!("GET:https://old.com/{i}");
        if i == 0 {
            let legacy = FileEntry {
                file_id: fid.clone(),
                body: body.clone(),
            };
            db.put(format!("file:{fid}"), serde_json::to_vec(&legacy).unwrap())
                .unwrap();
        } else {
            db.put(format!("file:{fid}"), &body).unwrap();
        }
        let e = ResourceEntry {
            website_key: "old.com".into(),
            resource_key: key.clone(),
            url: format!("https://old.com/{i}"),
            method: "GET".into(),
            status: 200,
            request_headers: HashMap::new(),
            response_headers: HashMap::new(),
            file_id: fid,
            http_version: HttpVersion::Http11,
            created_at: Some(1_700_000_000 + i as i64),
        };
        db.put(format!("res:{key}"), serde_json::to_vec(&e).unwrap())
            .unwrap();
        db.put(format!("site:old.com::{key}"), b"").unwrap();
        out.push((key, body));
    }
    db.flush().unwrap();
    out
}

#[test]
fn gc_collectors_preserve_v023_references_and_persist_deletions() {
    for purge in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cache_db");
        let orphan_body = b"<html lang=\"legacy-orphan\"><body></body></html>";
        let orphan = compute_file_id(orphan_body);
        let expected = {
            let db = DB::open(&v023_options(), &path).unwrap();
            let expected = write_v023_records(&db, 6);

            // An older row can omit both optional fields. Its shared body
            // must survive TTL expiry of the original reference.
            let raw = db.get(format!("res:{}", expected[1].0)).unwrap().unwrap();
            let mut alias: serde_json::Value = serde_json::from_slice(&raw).unwrap();
            let fields = alias.as_object_mut().unwrap();
            fields.insert("resource_key".into(), "legacy-alias".into());
            fields.remove("created_at");
            fields.remove("http_version");
            db.put(b"res:legacy-alias", serde_json::to_vec(&alias).unwrap())
                .unwrap();
            db.put(b"site:old.com::legacy-alias", b"").unwrap();
            // v0.2.3 did not write orphan markers.
            db.put(format!("file:{orphan}"), orphan_body).unwrap();
            db.put(b"res:broken", b"{unreadable reference").unwrap();
            db.flush().unwrap();
            expected
        };

        let mut cfg = Config::for_tests(path.to_str().unwrap());
        cfg.rocksdb_blob_files = true;
        let collect = |store: &Store| {
            if purge {
                let result = store.purge_empty().unwrap();
                (result.purged_resources, result.purged_files)
            } else {
                let result = store.cleanup(10, 1_700_000_012, true).unwrap();
                (result.expired_resource_keys.len(), result.removed_files)
            }
        };
        let removed = if purge { 1 } else { 2 };
        {
            let store = Store::open(&cfg).unwrap();
            assert_eq!(collect(&store), (removed, 2));
            gc_assert_body(&store, "legacy-alias", &expected[1].1);
            store.db.flush().unwrap();
        }

        let store = Store::open(&cfg).unwrap();
        for (i, (key, body)) in expected.iter().enumerate() {
            if i < removed {
                assert!(store.db.get(format!("res:{key}")).unwrap().is_none());
                assert!(store
                    .db
                    .get(format!("site:old.com::{key}"))
                    .unwrap()
                    .is_none());
            } else {
                gc_assert_body(&store, key, body);
            }
        }
        gc_assert_body(&store, "legacy-alias", &expected[1].1);
        assert!(store
            .db
            .get(b"site:old.com::legacy-alias")
            .unwrap()
            .is_some());
        assert!(!file_exists(&store, &compute_file_id(&expected[0].1)));
        assert!(!file_exists(&store, &orphan));
        assert!(store.db.get(format!("orph:{orphan}")).unwrap().is_none());
        assert_eq!(
            store.db.get(b"res:broken").unwrap().as_deref(),
            Some(b"{unreadable reference".as_slice())
        );
        assert_eq!(collect(&store), (0, 0));
    }
}

#[tokio::test]
async fn opens_v023_db_and_reads_everything_back() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cache_db");
    let expected = {
        let db = DB::open(&v023_options(), &path).unwrap();
        write_v023_records(&db, 60)
    };

    let mut cfg = Config::for_tests(path.to_str().unwrap());
    cfg.rocksdb_blob_files = true;
    let store = Store::open(&cfg).expect("v0.3 opens a v0.2.3 db");
    let state = Arc::new(AppState::new(cfg.clone(), store, None, None));

    for (key, body) in &expected {
        let r = state
            .store
            .get_resource(key, false)
            .unwrap()
            .expect("resource");
        assert_eq!(&r.body[..], &body[..], "{key}");
    }
    let items = site_items(&state, "old.com").await;
    assert_eq!(items.len(), expected.len());

    // Write new data (big bodies go to blob files), compact so old values
    // move too, and read everything again.
    for i in 0..20 {
        let body = vec![b'0' + (i % 10) as u8; 150_000 + i];
        index_one(
            &state,
            &payload(Some("new.com"), &format!("n{i}"), "https://new.com/", &body),
            None,
        )
        .await;
    }
    state.store.db.flush().unwrap();
    state.store.db.compact_range::<&[u8], &[u8]>(None, None);
    assert!(
        state
            .store
            .rocksdb_size()
            .total_blob_file_size_bytes
            .unwrap_or(0)
            > 0,
        "blob files in use"
    );
    state.store.mem.invalidate_all();
    for (key, body) in &expected {
        let r = state.store.get_resource(key, false).unwrap().unwrap();
        assert_eq!(&r.body[..], &body[..]);
    }
    drop(state);

    // Rollback: v0.2.3's options (no blob settings) must still read it all.
    let db = DB::open(&v023_options(), &path).expect("v0.2.3 reopens a v0.3 db");
    for (key, body) in &expected {
        let raw = db.get(format!("res:{key}")).unwrap().unwrap();
        let e: ResourceEntry = serde_json::from_slice(&raw).unwrap();
        let f = db.get(format!("file:{}", e.file_id)).unwrap().unwrap();
        if f.first() == Some(&b'{') {
            let fe: FileEntry = serde_json::from_slice(&f).unwrap();
            assert_eq!(&fe.body, body);
        } else {
            assert_eq!(&f, body);
        }
    }
    for i in 0..20 {
        let raw = db.get(format!("res:n{i}")).unwrap().unwrap();
        let e: ResourceEntry = serde_json::from_slice(&raw).unwrap();
        let f = db.get(format!("file:{}", e.file_id)).unwrap().unwrap();
        assert_eq!(f.len(), 150_000 + i);
    }
}
