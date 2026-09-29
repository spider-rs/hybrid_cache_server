# Benchmarks: v0.2.3 against v0.3.0

All numbers come from `benches/loadgen` driving each server on the same machine: an Apple M1 Max, 10 cores, 64 GB, macOS. The loadgen and the server share the machine. Each row is one run of a minute or two, and repeat runs of the same scenario varied by about 15%, so treat small differences as noise.

v0.2.3 is `bdf83f6`, the commit prod runs. v0.3.0 is this branch with default settings.

## Workload

- 20 sites, 1515 resources, about 1 GB of HTML-shaped bodies (1.3 GB as base64 JSON). Site 0 has 200 resources, four more have 100 to 200, the rest are small.
- Bodies are 50 KB to 2 MB, log-uniform, and 2% are 5 MiB. Each body is unique, so dedup never hides a write.
- Writes are batches of 16 (70%) and single items (30%). Reads are `/cache/resource` (70%) and `/cache/site` (30%). The mix is 80% reads.
- The client is reqwest and sends `Accept-Encoding: gzip, br, zstd`, like the fleet.
- The loadgen samples the server's RSS every 250 ms with `ps`. `rss_limit_mib=7300` kills the server with SIGKILL when RSS passes 7300 MiB, standing in for the kernel OOM killer on the 7.7 GiB c7g.xlarge.

## The OOM

v0.2.3 dies in every heavy scenario. v0.3.0 finishes all of them under 2.7 GB.

| Scenario | v0.2.3 | v0.3.0 |
| --- | --- | --- |
| Seed 1515 resources, RSS after | 3153 MiB | 1165 MiB |
| Site storm, concurrency 1 to 32 over 60 s | killed at 39.6 s (7572 MiB) after 119 site loads | peak 1714 MiB, 2377 site loads, 39.3/s |
| Batch storm, 16 bodies of 1 to 5 MiB, concurrency 1 to 16 over 45 s | killed at 8.8 s (7596 MiB) after 64 batches | peak 2650 MiB, 411 batches, all 201 |
| Write churn, fresh keys, concurrency 8 for 60 s | killed at 3.3 s (7771 MiB) | peak 1709 MiB, 2551 batches |
| Mixed 80/20, concurrency 16 for 60 s | peak 7009 MiB, still 5842 MiB after 15 s idle | peak 2387 MiB, 1782 MiB after 15 s idle |
| Concurrency steps 1 to 32 | killed during the c=32 step (7385 MiB) | peak 2381 MiB |

### Root cause

Three things in v0.2.3 add up. Each one alone is enough to kill it in the loadgen.

1. A `/cache/site` load holds the whole site several times. For each item it clones the cached base64 String into a payload (one copy), serializes the whole list with `serde_json::to_vec` into a Vec that grows by doubling (one to two more), and inserts every item it had to read from RocksDB into the mem cache as body plus base64 (2.33x the body, kept until the TTL). Site 0 here is 124 MB of bodies, so one load needs about 500 MB, and brotli then compresses the result on one of the four async workers. Sixteen concurrent loads of big sites is several GB, and while brotli holds the workers the queue behind them grows.
2. The mem cache has no bound. Every write and every read miss inserts body plus base64, and only the 24 h TTL cleanup removes anything. Seeding 1 GB of bodies left v0.2.3 at 3.1 GB RSS.
3. Nothing limits bytes in flight. There is no body limit, so a batch is buffered whole, parsed into owned Strings, decoded, and base64-encoded again for the cache. Memory grows linearly with the number of concurrent heavy requests until the kernel steps in.

Prod shows 28 MB of bodies in the mem cache but 2.65 GB RSS with a 2.67 GB high-water mark. So the cache is not most of prod's memory. What is left is the transient buffers from (1) and (3), kept by glibc malloc after they were freed. The two OOMs coincided with CPU spikes and NLB resets, which fits a burst of site loads or large batches: base64 and brotli work on the async workers, requests queueing behind them, and each queued request holding its buffers. The loadgen cannot say which of the three patterns hit prod on 9/25 and 9/27. v0.3.0's `/metrics` can: watch `inflight_budget_available_bytes`, `http_requests_total{route="site"}` and `jemalloc_allocated_bytes`.

A caveat: these runs use macOS. v0.2.3 used the macOS system allocator here and glibc malloc in prod, so its retention after a burst will differ. Peak usage is live allocation and does not depend on the allocator.

### What bounds v0.3.0

- The mem cache is capped by bytes at 512 MiB. At the end of every run `mem_cache_bytes` was 534 to 537 MB and `jemalloc_allocated_bytes` 600 to 651 MB, so the cache is nearly all of the Rust heap at rest.
- A 1 GiB in-flight budget covers request bodies and response buffers. Under load it ran 80 to 95% used, and writes waited on it (batch storm p50 959 ms) instead of allocating.
- A site response is capped at 64 MiB and built from exactly sized buffers.
- jemalloc returns freed pages at once. With a 1 s decay, at c=32 jemalloc kept 3.4 GB resident while the program held 0.6 to 1.2 GB. With decay 0 that run peaked at 2.5 GB instead of 4.9 GB and was about 10% slower.

The rest of RSS is RocksDB: a 512 MiB block and blob cache and up to three 64 MiB memtables.

## Latency and throughput, mixed load at concurrency 16

| Route | v0.2.3 req/s | p50 ms | p99 ms | p999 ms | v0.3.0 req/s | p50 ms | p99 ms | p999 ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| batch | 2.2 | 284 | 1200 | 1625 | 13.1 | 113 | 370 | 457 |
| index | 0.8 | 114 | 348 | 613 | 5.8 | 30 | 230 | 278 |
| resource | 8.5 | 84 | 457 | 530 | 52.8 | 27 | 193 | 332 |
| site | 3.8 | 3916 | 9499 | 9768 | 23.2 | 519 | 1181 | 1446 |

Total operations went from 15.3/s to 95/s. In v0.2.3 the site loads hold the async workers, so every other route waits behind them.

## Concurrency steps, 10 s each

p99 in ms, with each route's rate at that step.

| c | v0.2.3 resource | v0.2.3 batch | v0.2.3 site | v0.3.0 resource | v0.3.0 batch | v0.3.0 site |
| ---: | --- | --- | --- | --- | --- | --- |
| 1 | 2/s, 36 | 1/s, 29 | 1/s, 2156 | 15/s, 20 | 4/s, 48 | 6/s, 210 |
| 2 | 2/s, 685 | 1/s, 60 | 1/s, 2804 | 28/s, 22 | 8/s, 67 | 11/s, 217 |
| 4 | 3/s, 503 | 1/s, 71 | 2/s, 3392 | 40/s, 38 | 11/s, 88 | 17/s, 425 |
| 8 | 5/s, 131 | 2/s, 79 | 3/s, 4932 | 42/s, 112 | 12/s, 263 | 18/s, 777 |
| 16 | 5/s, 631 | 2/s, 1878 | 3/s, 9217 | 51/s, 217 | 14/s, 379 | 23/s, 1402 |
| 32 | killed | killed | killed | 67/s, 343 | 16/s, 693 | 28/s, 1858 |

Highest load with p99 at or under 50 ms:

- `/cache/resource`: v0.2.3 manages 2/s at c=1. v0.3.0 manages 40/s at c=4.
- Batch writes: 1/s at c=1 for v0.2.3, 4/s at c=1 for v0.3.0.
- `/cache/site`: neither version gets under 50 ms. The large sites here are 50 to 64 MiB of JSON per response, and the time goes to moving and decompressing that.

RSS in the v0.3.0 step run went from 1264 MiB at c=1 to 1756 MiB at c=32, with a 2381 MiB peak.

## Where v0.3.0 is slower

Raw write throughput on an empty database:

| Run | v0.2.3 | v0.3.0 |
| --- | --- | --- |
| Seed, 8 writers, batches/s | 63 to 105 | 54 to 65 |
| Write churn, 8 writers, batches/s | 85 until it died at 3.3 s | 42 |
| Write churn, `ROCKSDB_RATE_LIMIT_BYTES=0` | | 64 |

Most of the gap is the 256 MiB/s flush and compaction rate limiter. The churn run pushes 550 MiB/s of JSON, over 2000 times prod's peak write rate of 800 MB an hour. The rest is probably the two point reads per item that v0.3.0 adds (not measured separately): the existence check that lets it skip a duplicate body, and the read of the previous version that lets GC find overwritten bodies. At prod's rate neither shows. If a backfill ever needs the speed, set `ROCKSDB_RATE_LIMIT_BYTES=0`.

## Compression

`benches/compression.sh` loads the three largest sites three times per setting. `cpu_s` is server CPU for the whole run, and the `off` row is the cost of serving without compression.

| Codec | Level | Wire MiB | JSON MiB | Ratio | Server CPU s | CPU ms per MiB saved |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| off | | 1431 | 1431 | 1.000 | 2.26 | |
| gzip | 1 | 499 | 1398 | 0.357 | 8.71 | 8.4 |
| gzip | 6 | 295 | 1378 | 0.214 | 25.51 | 22.5 |
| br | 1 | 345 | 1363 | 0.253 | 8.55 | 7.3 |
| br | 4 | 303 | 1367 | 0.222 | 15.22 | 13.2 |
| zstd | 1 | 339 | 1420 | 0.239 | 5.08 | 3.7 |
| zstd | 3 | 300 | 1301 | 0.231 | 6.20 | 5.1 |

zstd level 1 costs the least CPU per byte saved: half of brotli 1, and under a third of brotli 4, the level v0.2.3 compressed with. v0.3.0 uses it, with gzip as the fallback for clients that do not accept zstd. Stored images, fonts, video, archives and octet-streams are not compressed at all, and neither is anything under 1 KiB.

## Commands

```bash
# v0.2.3 baseline, built outside the worktree
mkdir -p /tmp/v023src && git archive bdf83f6 | tar -x -C /tmp/v023src
(cd /tmp/v023src && CARGO_TARGET_DIR="$PWD/target" cargo build --release)

# v0.3.0 and the loadgen
cargo build --release
(cd benches/loadgen && cargo build --release)

V023=/tmp/v023src/target/release/hybrid_cache_server
V030=target/release/hybrid_cache_server

for B in $V023 $V030; do
  benches/run.sh $B scenario=seed,site-storm sites=20 secs=60 conc=1:32 rss_limit_mib=7300 idle_after=15
  benches/run.sh $B scenario=seed,batch-storm sites=20 secs=45 conc=1:16 rss_limit_mib=7300 idle_after=15
  benches/run.sh $B scenario=write-churn sites=20 secs=60 conc=8 rss_limit_mib=7300 idle_after=15
  benches/run.sh $B scenario=seed,mixed sites=20 secs=60 conc=16 rss_limit_mib=7300 idle_after=15
  benches/run.sh $B scenario=seed,steps sites=20 steps=1,2,4,8,16,32 step_secs=10 rss_limit_mib=7300 idle_after=5
done

N=3 benches/compression.sh $V030

# jemalloc decay comparison
_RJEM_MALLOC_CONF=dirty_decay_ms:1000,muzzy_decay_ms:1000 \
  benches/run.sh $V030 scenario=seed,mixed sites=20 conc=32 secs=30 idle_after=5
```
