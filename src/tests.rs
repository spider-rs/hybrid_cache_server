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
async fn overwrite_with_new_site_moves_index_key() {
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
    assert!(site_items(&t.state, "old.com").await.is_empty());
    assert_eq!(site_items(&t.state, "new.com").await.len(), 1);
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
