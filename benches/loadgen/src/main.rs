//! Load generator for hybrid_cache_server.
//!
//! Drives the same wire format the crawl fleet uses (spider_remote_cache
//! writes, chromey reads) and samples the server's RSS with `ps` while it runs.
//!
//! Usage: loadgen [key=value ...]
//!
//!   url=http://127.0.0.1:8080   server base URL
//!   pid=<n>                     server pid to sample RSS from (optional)
//!   scenario=mixed              seed | mixed | site-storm | batch-storm | write-churn | steps
//!   secs=60                     run length for the timed scenarios
//!   conc=16                     concurrency; "a:b" ramps linearly from a to b over the run
//!   steps=1,2,4,8,16,32         concurrency steps for scenario=steps
//!   step_secs=15                seconds per step
//!   sites=40                    number of sites to seed
//!   max_site=200                resource count of the largest site
//!   min_body=51200              smallest body in bytes
//!   max_body=2097152            largest body in bytes (log-uniform between min and max)
//!   huge_pct=2                  percent of bodies that are 5 MiB
//!   encoding=gzip,br,zstd       Accept-Encoding the client sends ("identity" to disable)
//!   idle_after=10               seconds to keep sampling RSS after the load stops
//!   csv=<path>                  write the RSS time series here
//!   seed=1                      RNG seed
//!   rss_limit_mib=0             SIGKILL the server when its RSS passes this (0 = off),
//!                               standing in for the kernel OOM killer on a small box
//!
//! scenario takes a comma list, run in order against the same server, e.g.
//! scenario=seed,site-storm.
//! The seeded sites are named site-0 .. site-{sites-1}. Site 0 is always the
//! largest (max_site resources); the rest follow a skewed distribution.

use std::{
    collections::HashMap,
    io::Write,
    process::Command,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

use base64::{engine::general_purpose::STANDARD, Engine};

/// Set when the server is gone; workers stop instead of spinning on errors.
static SERVER_DEAD: AtomicBool = AtomicBool::new(false);
use serde::Serialize;

#[derive(Clone)]
struct Cfg {
    url: String,
    pid: Option<u32>,
    scenario: String,
    secs: u64,
    conc: (usize, usize),
    steps: Vec<usize>,
    step_secs: u64,
    sites: usize,
    max_site: usize,
    min_body: usize,
    max_body: usize,
    huge_pct: u32,
    encoding: String,
    idle_after: u64,
    csv: Option<String>,
    seed: u64,
    rss_limit_mib: u64,
}

fn parse_cfg() -> Cfg {
    let mut kv: HashMap<String, String> = HashMap::new();
    for a in std::env::args().skip(1) {
        if let Some((k, v)) = a.split_once('=') {
            kv.insert(k.to_string(), v.to_string());
        } else {
            eprintln!("ignoring argument {a:?} (expected key=value)");
        }
    }
    let get = |k: &str, d: &str| kv.get(k).cloned().unwrap_or_else(|| d.to_string());
    let conc_s = get("conc", "16");
    let conc = match conc_s.split_once(':') {
        Some((a, b)) => (a.parse().unwrap_or(1), b.parse().unwrap_or(16)),
        None => {
            let n = conc_s.parse().unwrap_or(16);
            (n, n)
        }
    };
    Cfg {
        url: get("url", "http://127.0.0.1:8080"),
        pid: kv.get("pid").and_then(|v| v.parse().ok()),
        scenario: get("scenario", "mixed"),
        secs: get("secs", "60").parse().unwrap_or(60),
        conc,
        steps: get("steps", "1,2,4,8,16,32")
            .split(',')
            .filter_map(|s| s.parse().ok())
            .collect(),
        step_secs: get("step_secs", "15").parse().unwrap_or(15),
        sites: get("sites", "40").parse().unwrap_or(40),
        max_site: get("max_site", "200").parse().unwrap_or(200),
        min_body: get("min_body", "51200").parse().unwrap_or(51_200),
        max_body: get("max_body", "2097152").parse().unwrap_or(2_097_152),
        huge_pct: get("huge_pct", "2").parse().unwrap_or(2),
        encoding: get("encoding", "gzip, br, zstd"),
        idle_after: get("idle_after", "10").parse().unwrap_or(10),
        csv: kv.get("csv").cloned(),
        seed: get("seed", "1").parse().unwrap_or(1),
        rss_limit_mib: get("rss_limit_mib", "0").parse().unwrap_or(0),
    }
}

// ---------------------------------------------------------------- bodies

/// A few MiB of HTML-shaped text. Bodies are windows into it with a unique
/// prefix, so every body hashes differently but compresses like real HTML.
fn corpus() -> Arc<Vec<u8>> {
    const WORDS: &[&str] = &[
        "the",
        "product",
        "price",
        "shipping",
        "account",
        "search",
        "results",
        "page",
        "cart",
        "checkout",
        "review",
        "rating",
        "category",
        "news",
        "article",
        "author",
        "published",
        "update",
        "privacy",
        "terms",
        "contact",
        "about",
        "login",
        "menu",
        "footer",
        "header",
        "section",
        "container",
        "button",
        "primary",
        "secondary",
        "item",
        "list",
        "grid",
        "image",
        "description",
        "details",
        "specification",
    ];
    const TAGS: &[&str] = &["div", "span", "p", "a", "li", "section", "article", "td"];
    let mut rng = fastrand::Rng::with_seed(42);
    let target = 12 * 1024 * 1024;
    let mut out = Vec::with_capacity(target + 4096);
    out.extend_from_slice(b"<!DOCTYPE html><html><head><title>bench</title></head><body>");
    while out.len() < target {
        let tag = TAGS[rng.usize(..TAGS.len())];
        let _ = write!(
            out,
            "<{tag} class=\"c{} {}\" data-id=\"{}\">",
            rng.u32(..500),
            WORDS[rng.usize(..WORDS.len())],
            rng.u64(..1_000_000_000)
        );
        for _ in 0..rng.usize(3..14) {
            out.extend_from_slice(WORDS[rng.usize(..WORDS.len())].as_bytes());
            out.push(b' ');
        }
        let _ = write!(out, "</{tag}>\n");
    }
    Arc::new(out)
}

fn body_size(cfg: &Cfg, rng: &mut fastrand::Rng) -> usize {
    if rng.u32(..100) < cfg.huge_pct {
        return 5 * 1024 * 1024;
    }
    let lo = (cfg.min_body as f64).ln();
    let hi = (cfg.max_body as f64).ln();
    (lo + rng.f64() * (hi - lo)).exp() as usize
}

fn make_body(corpus: &[u8], size: usize, uniq: u64, rng: &mut fastrand::Rng) -> Vec<u8> {
    let size = size.min(corpus.len() - 1);
    let start = rng.usize(..corpus.len() - size);
    let mut v = Vec::with_capacity(size + 64);
    let _ = write!(v, "<!-- uniq {uniq} {} -->", rng.u64(..));
    v.extend_from_slice(&corpus[start..start + size]);
    v
}

// ---------------------------------------------------------------- wire format

#[derive(Serialize)]
struct Payload {
    website_key: Option<String>,
    resource_key: String,
    url: String,
    method: String,
    status: u16,
    request_headers: HashMap<String, String>,
    response_headers: HashMap<String, String>,
    http_version: &'static str,
    body_base64: String,
}

fn payload(site: usize, page: usize, body: &[u8]) -> Payload {
    let url = format!("https://site-{site}.example/page-{page}");
    let mut req = HashMap::new();
    req.insert("Accept".into(), "text/html".into());
    let mut resp = HashMap::new();
    resp.insert("Content-Type".into(), "text/html; charset=utf-8".into());
    resp.insert("Cache-Control".into(), "max-age=3600".into());
    Payload {
        website_key: Some(site_key(site)),
        resource_key: resource_key(site, page),
        url,
        method: "GET".into(),
        status: 200,
        request_headers: req,
        response_headers: resp,
        http_version: "Http11",
        body_base64: STANDARD.encode(body),
    }
}

fn site_key(site: usize) -> String {
    format!("site-{site}")
}

fn resource_key(site: usize, page: usize) -> String {
    // The server routes on raw path segments, so real clients use
    // slash-free keys (hashes). Keep ours slash-free too.
    format!("res-{site}-{page}")
}

// ---------------------------------------------------------------- stats

#[derive(Default)]
struct RouteStats {
    lat_us: Vec<u32>,
    status: HashMap<u16, u64>,
    errors: u64,
    bytes: u64,
}

#[derive(Default)]
struct Stats {
    routes: Mutex<HashMap<&'static str, RouteStats>>,
}

impl Stats {
    fn record(&self, route: &'static str, dur: Duration, status: Option<u16>, bytes: u64) {
        let mut g = self.routes.lock().unwrap();
        let r = g.entry(route).or_default();
        r.lat_us.push(dur.as_micros().min(u32::MAX as u128) as u32);
        r.bytes += bytes;
        match status {
            Some(s) => *r.status.entry(s).or_default() += 1,
            None => r.errors += 1,
        }
    }

    fn report(&self, label: &str, elapsed: Duration) -> Vec<(String, f64, f64, f64, f64, f64)> {
        let mut g = self.routes.lock().unwrap();
        let mut names: Vec<_> = g.keys().cloned().collect();
        names.sort();
        let mut out = Vec::new();
        println!(
            "[{label}] {:<14} {:>8} {:>9} {:>9} {:>9} {:>9} {:>10}  status",
            "route", "count", "req/s", "p50 ms", "p99 ms", "p999 ms", "MiB/s"
        );
        for n in names {
            let r = g.get_mut(n).unwrap();
            r.lat_us.sort_unstable();
            let pct = |p: f64| -> f64 {
                if r.lat_us.is_empty() {
                    return 0.0;
                }
                let i = ((r.lat_us.len() as f64 - 1.0) * p).round() as usize;
                r.lat_us[i] as f64 / 1000.0
            };
            let count = r.lat_us.len() as f64;
            let rps = count / elapsed.as_secs_f64();
            let mibs = r.bytes as f64 / 1_048_576.0 / elapsed.as_secs_f64();
            let mut st: Vec<_> = r.status.iter().map(|(k, v)| format!("{k}:{v}")).collect();
            st.sort();
            if r.errors > 0 {
                st.push(format!("err:{}", r.errors));
            }
            println!(
                "[{label}] {:<14} {:>8} {:>9.1} {:>9.1} {:>9.1} {:>9.1} {:>10.1}  {}",
                n,
                r.lat_us.len(),
                rps,
                pct(0.5),
                pct(0.99),
                pct(0.999),
                mibs,
                st.join(" ")
            );
            out.push((n.to_string(), rps, pct(0.5), pct(0.99), pct(0.999), mibs));
        }
        out
    }
}

// ---------------------------------------------------------------- rss sampler

struct Rss {
    samples: Mutex<Vec<(f64, u64)>>,
    peak_kb: AtomicU64,
    phase_peak_kb: AtomicU64,
    killed: AtomicBool,
}

fn rss_kb(pid: u32) -> Option<u64> {
    let out = Command::new("ps")
        .args(["-o", "rss=", "-p", &pid.to_string()])
        .output()
        .ok()?;
    String::from_utf8_lossy(&out.stdout).trim().parse().ok()
}

fn spawn_rss_sampler(
    pid: Option<u32>,
    stop: Arc<AtomicBool>,
    t0: Instant,
    limit_mib: u64,
) -> Arc<Rss> {
    let rss = Arc::new(Rss {
        samples: Mutex::new(Vec::new()),
        peak_kb: AtomicU64::new(0),
        phase_peak_kb: AtomicU64::new(0),
        killed: AtomicBool::new(false),
    });
    if let Some(pid) = pid {
        let r = rss.clone();
        std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                match rss_kb(pid) {
                    Some(kb) => {
                        r.peak_kb.fetch_max(kb, Ordering::Relaxed);
                        r.phase_peak_kb.fetch_max(kb, Ordering::Relaxed);
                        let t = t0.elapsed().as_secs_f64();
                        let n = {
                            let mut g = r.samples.lock().unwrap();
                            g.push((t, kb));
                            g.len()
                        };
                        if n % 20 == 0 {
                            println!("[rss] t={t:.0}s {} MiB", kb / 1024);
                        }
                        if limit_mib > 0 && kb / 1024 > limit_mib {
                            println!(
                                "[rss] t={t:.1}s {} MiB passed the {limit_mib} MiB limit: killing the server (OOM stand-in)",
                                kb / 1024
                            );
                            let _ = Command::new("kill").args(["-9", &pid.to_string()]).status();
                            r.killed.store(true, Ordering::Relaxed);
                            SERVER_DEAD.store(true, Ordering::Relaxed);
                            break;
                        }
                    }
                    None => {
                        r.samples
                            .lock()
                            .unwrap()
                            .push((t0.elapsed().as_secs_f64(), 0));
                        eprintln!("rss sampler: pid {pid} is gone (server died?)");
                        SERVER_DEAD.store(true, Ordering::Relaxed);
                        break;
                    }
                }
                std::thread::sleep(Duration::from_millis(250));
            }
        });
    }
    rss
}

// ---------------------------------------------------------------- workload

struct World {
    cfg: Cfg,
    client: reqwest::Client,
    corpus: Arc<Vec<u8>>,
    /// Resource count per site.
    site_sizes: Vec<usize>,
    uniq: AtomicU64,
    stats: Stats,
}

impl World {
    fn pick_site(&self, rng: &mut fastrand::Rng) -> usize {
        // Skewed: half of all picks go to the 5 largest sites.
        if rng.bool() {
            rng.usize(..self.site_sizes.len().min(5))
        } else {
            rng.usize(..self.site_sizes.len())
        }
    }

    async fn get(&self, route: &'static str, path: String) {
        let t = Instant::now();
        let res = self
            .client
            .get(format!("{}{}", self.cfg.url, path))
            .send()
            .await;
        match res {
            Ok(resp) => {
                let status = resp.status().as_u16();
                // Read the whole body: decompression cost is part of the client latency.
                let n = resp.bytes().await.map(|b| b.len() as u64).unwrap_or(0);
                self.stats.record(route, t.elapsed(), Some(status), n);
            }
            Err(_) => self.stats.record(route, t.elapsed(), None, 0),
        }
    }

    async fn post(&self, route: &'static str, path: &str, site: usize, json: Vec<u8>) {
        let n = json.len() as u64;
        let t = Instant::now();
        let res = self
            .client
            .post(format!("{}{}", self.cfg.url, path))
            .header("content-type", "application/json")
            .header("x-cache-site", site_key(site))
            .body(json)
            .send()
            .await;
        match res {
            Ok(resp) => {
                let status = resp.status().as_u16();
                let _ = resp.bytes().await;
                self.stats.record(route, t.elapsed(), Some(status), n);
            }
            Err(_) => self.stats.record(route, t.elapsed(), None, n),
        }
    }

    fn build_batch(&self, site: usize, pages: &[usize], rng: &mut fastrand::Rng) -> Vec<u8> {
        let items: Vec<Payload> = pages
            .iter()
            .map(|&p| {
                let sz = body_size(&self.cfg, rng);
                let body = make_body(
                    &self.corpus,
                    sz,
                    self.uniq.fetch_add(1, Ordering::Relaxed),
                    rng,
                );
                payload(site, p, &body)
            })
            .collect();
        serde_json::to_vec(&items).unwrap()
    }

    async fn write_batch(&self, site: usize, pages: Vec<usize>, rng: &mut fastrand::Rng) {
        let json = self.build_batch(site, &pages, rng);
        self.post("batch", "/cache/index/batch", site, json).await;
    }

    async fn write_single(&self, site: usize, page: usize, rng: &mut fastrand::Rng) {
        let sz = body_size(&self.cfg, rng);
        let body = make_body(
            &self.corpus,
            sz,
            self.uniq.fetch_add(1, Ordering::Relaxed),
            rng,
        );
        let json = serde_json::to_vec(&payload(site, page, &body)).unwrap();
        self.post("index", "/cache/index", site, json).await;
    }

    /// One operation of the 80/20 mix.
    async fn mixed_op(&self, rng: &mut fastrand::Rng) {
        let site = self.pick_site(rng);
        let n = self.site_sizes[site];
        let roll = rng.u32(..100);
        if roll < 24 {
            // 30% of reads are whole-site loads.
            self.get("site", format!("/cache/site/{}", site_key(site)))
                .await;
        } else if roll < 80 {
            let page = rng.usize(..n);
            self.get(
                "resource",
                format!("/cache/resource/{}", resource_key(site, page)),
            )
            .await;
        } else if roll < 94 {
            // 70% of writes are batches of 16 (overwrites of existing pages).
            let pages: Vec<usize> = (0..16).map(|_| rng.usize(..n.max(1))).collect();
            self.write_batch(site, pages, rng).await;
        } else {
            let page = rng.usize(..n);
            self.write_single(site, page, rng).await;
        }
    }
}

async fn seed(world: Arc<World>) {
    let t = Instant::now();
    let mut jobs = Vec::new();
    for (site, &n) in world.site_sizes.iter().enumerate() {
        let pages: Vec<usize> = (0..n).collect();
        for chunk in pages.chunks(16) {
            jobs.push((site, chunk.to_vec()));
        }
    }
    let total = jobs.len();
    let jobs = Arc::new(Mutex::new(jobs));
    let mut handles = Vec::new();
    for w in 0..8 {
        let world = world.clone();
        let jobs = jobs.clone();
        handles.push(tokio::spawn(async move {
            let mut rng = fastrand::Rng::with_seed(world.cfg.seed * 1000 + w);
            loop {
                let job = jobs.lock().unwrap().pop();
                let Some((site, pages)) = job else { break };
                world.write_batch(site, pages, &mut rng).await;
            }
        }));
    }
    for h in handles {
        let _ = h.await;
    }
    let res: usize = world.site_sizes.iter().sum();
    println!(
        "[seed] {} sites, {} resources, {} batches in {:.1}s",
        world.site_sizes.len(),
        res,
        total,
        t.elapsed().as_secs_f64()
    );
}

/// Run `op` with a concurrency that ramps from conc.0 to conc.1 over `secs`.
async fn run_timed<F, Fut>(world: Arc<World>, secs: u64, conc: (usize, usize), op: F)
where
    F: Fn(Arc<World>, u64) -> Fut + Send + Sync + 'static + Clone,
    Fut: std::future::Future<Output = ()> + Send,
{
    let t0 = Instant::now();
    let dur = Duration::from_secs(secs);
    let maxc = conc.0.max(conc.1);
    let mut handles = Vec::new();
    for w in 0..maxc {
        let world = world.clone();
        let op = op.clone();
        handles.push(tokio::spawn(async move {
            let mut i = 0u64;
            while t0.elapsed() < dur && !SERVER_DEAD.load(Ordering::Relaxed) {
                // Worker w is active once the ramp reaches it.
                let frac = t0.elapsed().as_secs_f64() / dur.as_secs_f64();
                let active = conc.0 as f64 + (conc.1 as f64 - conc.0 as f64) * frac;
                if (w as f64) >= active.max(1.0) {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
                op(world.clone(), (w as u64) << 32 | i).await;
                i += 1;
            }
        }));
    }
    for h in handles {
        let _ = h.await;
    }
}

#[tokio::main]
async fn main() {
    let cfg = parse_cfg();
    let mut rng = fastrand::Rng::with_seed(cfg.seed);

    // Site sizes: site 0 is the largest, others skew small (1..max_site).
    let mut site_sizes = vec![cfg.max_site];
    for i in 1..cfg.sites {
        let x = rng.f64();
        let n = if i < 5 {
            cfg.max_site / 2 + rng.usize(..cfg.max_site / 2 + 1)
        } else {
            1 + (x * x * x * cfg.max_site as f64) as usize
        };
        site_sizes.push(n.clamp(1, cfg.max_site));
    }

    let mut headers = reqwest::header::HeaderMap::new();
    if cfg.encoding != "identity" {
        headers.insert(
            reqwest::header::ACCEPT_ENCODING,
            reqwest::header::HeaderValue::from_str(&cfg.encoding).unwrap(),
        );
    }
    let mut builder = reqwest::Client::builder()
        .pool_idle_timeout(Duration::from_secs(90))
        .timeout(Duration::from_secs(120))
        .default_headers(headers);
    if cfg.encoding == "identity" {
        builder = builder.no_gzip().no_brotli().no_zstd();
    }
    let client = builder.build().unwrap();

    let world = Arc::new(World {
        cfg: cfg.clone(),
        client,
        corpus: corpus(),
        site_sizes,
        uniq: AtomicU64::new(cfg.seed << 40),
        stats: Stats::default(),
    });

    let stop = Arc::new(AtomicBool::new(false));
    let t0 = Instant::now();
    let rss = spawn_rss_sampler(cfg.pid, stop.clone(), t0, cfg.rss_limit_mib);
    if let Some(pid) = cfg.pid {
        println!("[rss] start {} MiB", rss_kb(pid).unwrap_or(0) / 1024);
    }

    let scenarios: Vec<String> = cfg.scenario.split(',').map(str::to_string).collect();
    for scenario in scenarios {
        *world.stats.routes.lock().unwrap() = HashMap::new();
        rss.phase_peak_kb.store(0, Ordering::Relaxed);
        let run_start = Instant::now();
        match scenario.as_str() {
            "seed" => seed(world.clone()).await,
            "mixed" => {
                run_timed(world.clone(), cfg.secs, cfg.conc, |w, i| async move {
                    let mut rng = fastrand::Rng::with_seed(i ^ 0x9e37_79b9);
                    w.mixed_op(&mut rng).await;
                })
                .await
            }
            "site-storm" => {
                // Everyone loads the big sites at once: a fleet starting crawls
                // of the same large sites.
                run_timed(world.clone(), cfg.secs, cfg.conc, |w, i| async move {
                    let site = (i as usize) % w.site_sizes.len().min(5);
                    w.get("site", format!("/cache/site/{}", site_key(site)))
                        .await;
                })
                .await
            }
            "batch-storm" => {
                // Large batches: 16 bodies of 1-5 MiB each.
                run_timed(world.clone(), cfg.secs, cfg.conc, |w, i| async move {
                    let mut rng = fastrand::Rng::with_seed(i);
                    let site = w.pick_site(&mut rng);
                    let n = w.site_sizes[site];
                    let pages: Vec<usize> = (0..16).map(|_| rng.usize(..n)).collect();
                    let items: Vec<Payload> = pages
                        .iter()
                        .map(|&p| {
                            let sz = 1_048_576 + rng.usize(..4 * 1_048_576);
                            let body = make_body(
                                &w.corpus,
                                sz,
                                w.uniq.fetch_add(1, Ordering::Relaxed),
                                &mut rng,
                            );
                            payload(site, p, &body)
                        })
                        .collect();
                    let json = serde_json::to_vec(&items).unwrap();
                    w.post("batch", "/cache/index/batch", site, json).await;
                })
                .await
            }
            "write-churn" => {
                // Fresh keys only: every write is a new resource.
                run_timed(world.clone(), cfg.secs, cfg.conc, |w, i| async move {
                    let mut rng = fastrand::Rng::with_seed(i);
                    let site = 1000 + rng.usize(..500);
                    let base = w.uniq.fetch_add(16, Ordering::Relaxed) as usize;
                    let pages: Vec<usize> = (0..16).map(|k| base + k).collect();
                    w.write_batch(site, pages, &mut rng).await;
                })
                .await
            }
            "steps" => {
                let mut summary = Vec::new();
                for &c in &cfg.steps.clone() {
                    *world.stats.routes.lock().unwrap() = HashMap::new();
                    let t = Instant::now();
                    run_timed(world.clone(), cfg.step_secs, (c, c), |w, i| async move {
                        let mut rng = fastrand::Rng::with_seed(i ^ 0x51ed);
                        w.mixed_op(&mut rng).await;
                    })
                    .await;
                    let rows = world.stats.report(&format!("c={c}"), t.elapsed());
                    let rss_now = cfg.pid.and_then(rss_kb).unwrap_or(0) / 1024;
                    summary.push((c, rows, rss_now));
                }
                println!("\n[steps] summary (req/s, p99 ms per route)");
                for (c, rows, rss_mib) in summary {
                    let cells: Vec<String> = rows
                        .iter()
                        .map(|(n, rps, _, p99, _, _)| format!("{n}={rps:.0}/s p99={p99:.1}"))
                        .collect();
                    println!("c={c:<4} rss={rss_mib}MiB  {}", cells.join("  "));
                }
            }
            other => {
                eprintln!("unknown scenario {other}");
                std::process::exit(2);
            }
        }
        let elapsed = run_start.elapsed();
        if scenario != "steps" {
            world.stats.report(&scenario, elapsed);
        }
        if let Some(pid) = cfg.pid {
            println!(
                "[rss] after {scenario}: now {} MiB, phase peak {} MiB",
                rss_kb(pid).unwrap_or(0) / 1024,
                rss.phase_peak_kb.load(Ordering::Relaxed) / 1024
            );
        }
        if rss.killed.load(Ordering::Relaxed) {
            println!("[rss] server was killed at the RSS limit; stopping");
            break;
        }
    }

    if let Some(pid) = cfg.pid {
        let end_kb = rss_kb(pid).unwrap_or(0);
        println!("[rss] at load end {} MiB", end_kb / 1024);
        tokio::time::sleep(Duration::from_secs(cfg.idle_after)).await;
        let idle_kb = rss_kb(pid).unwrap_or(0);
        stop.store(true, Ordering::Relaxed);
        println!(
            "[rss] peak {} MiB, after {}s idle {} MiB",
            rss.peak_kb.load(Ordering::Relaxed) / 1024,
            cfg.idle_after,
            idle_kb / 1024
        );
        if let Some(path) = &cfg.csv {
            let mut f = std::fs::File::create(path).unwrap();
            let _ = writeln!(f, "t_secs,rss_kb");
            for (t, kb) in rss.samples.lock().unwrap().iter() {
                let _ = writeln!(f, "{t:.2},{kb}");
            }
        }
    }
}
