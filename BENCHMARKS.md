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

## Graviton c7g.xlarge (Linux, glibc)

The same loadgen scenarios, run on 2026-09-29 on a temporary spot c7g.xlarge in us-east-2c: 4 vCPU Graviton3, 7761 MiB, no swap, Amazon Linux 2023 with kernel 6.18 and glibc 2.34. The box was terminated afterwards. Server and loadgen shared it, so the loadgen's own memory, 0.4 to 2.5 GB during site loads, counts against the same 7.7 GiB.

Binaries:

- v0.2.3 is `bdf83f6`, built on the box with plain `cargo build --release` (rustc 1.98.1, gcc 11.5), the way prod installs it. It uses glibc malloc.
- v0.3.0 is `f73804b`, cross-built on macOS with `scripts/build-aarch64.sh` (zigbuild, glibc 2.26 target, `target-cpu=neoverse-v1`).
- The loadgen is cross-built with zigbuild for the same target.

Each run starts the server as an unprivileged user with `ulimit -n 65535`, in an empty directory with no cache_db, with `MEILI_DISABLE=1` and no meilisearch running. v0.2.3 reads `MEILI_DISABLE` through `env_bool`, which accepts `1`, `true` or `yes`. It still builds a meili client and probes it once at startup; the probe fails fast and it logs "skipping Meilisearch indexing". Every run keeps sampling RSS for 60 s after the load stops, 120 s for the retention runs.

### Which OOM fired

The loadgen's 7300 MiB SIGKILL never fired. The kernel OOM killer always got there first, because the loadgen's RSS and the page cache also need the 7.7 GiB. It killed v0.2.3 at 6.1 to 7.3 GB of anonymous memory.

One thing makes these runs gentler on v0.2.3 than prod would be. The server inherited `oom_score_adj=-900` from the SSM agent that launched it, so the kernel first killed systemd-logind, systemd-networkd, chronyd, agetty and other small daemons. In the site storm that was enough, and v0.2.3 survived. Prod's service does not have -900, so there it would be the first thing killed. While the kernel was reclaiming, the box stalled. The 250 ms `ps` sampler got no samples for 170 s in the site storm and 340 s in the mixed run, so the true peaks there are higher than the table shows.

| Scenario | v0.2.3 | v0.3.0 |
| --- | --- | --- |
| Seed 1515 resources, then 60 s idle | peak 3120 MiB, 2905 MiB idle | peak 1224 MiB, 1133 MiB idle |
| Site storm, c 1 to 32 over 60 s | peak 5874 MiB or more, 3 kernel OOM kills of other daemons, 41 site loads with 10 errors, p50 126 s | peak 1965 MiB, 867 site loads, 14.3/s, no errors |
| Batch storm, 16 bodies of 1 to 5 MiB, c 1 to 16 over 45 s | kernel OOM killed the server at 6.3 GB anon, 38 batches with 12 errors | peak 2045 MiB, 94 batches, all 201 |
| Write churn, c=8 for 60 s | kernel OOM killed the server at 41 s, 7.3 GB anon, 315 batches with 49 errors | peak 1414 MiB, 467 batches, all 201 |
| Mixed 80/20, c=16 for 60 s | stalled 340 s in reclaim, then kernel OOM killed the server | peak 2692 MiB, 2560 requests, no errors |
| Concurrency steps 1 to 32 | survived, peak 5644 MiB | peak 2868 MiB |

### Does v0.2.3 keep its RSS after a burst on glibc

Yes. RSS 60 s after each burst ended, for the runs where the server lived:

| Run | v0.2.3 RSS after idle | v0.2.3 cached bodies | v0.3.0 RSS after idle | v0.3.0 cached bodies |
| --- | ---: | ---: | ---: | ---: |
| Seed only | 2905 MiB | 1029 MB | 1133 MiB | 536 MB |
| Site storm | 3668 MiB | 1021 MB | 1258 MiB | 535 MB |
| Steps to c=32 | 3638 MiB | 1006 MB | 2273 MiB | 533 MB |
| Mixed c=8, 120 s idle | | | 1864 MiB | 536 MB |

The runs above mix retention with v0.2.3's unbounded cache, so two more runs empty the cache to match prod, which holds 28 MB of bodies. They set `CACHE_TTL_SECS=30 CACHE_CLEANUP_INTERVAL_SECS=10`, seed, run the mixed load at c=8 for 60 s, and idle for 120 s. The TTL sweep also deletes the RocksDB rows, which is why 122 resource reads returned 404.

| v0.2.3 run | Peak | RSS after 120 s idle | Cached bodies left |
| --- | ---: | ---: | ---: |
| Default glibc | 4666 MiB | 3313 MiB | 2.4 MB, 2 entries |
| `MALLOC_ARENA_MAX=2` | 4346 MiB | 2988 MiB | 0 |

With the cache empty, v0.2.3 still holds 3.3 GB, all anonymous memory. RocksDB accounts for at most about 320 MiB of it: a 128 MiB block cache and three 64 MiB memtables. The other 3 GB is heap that glibc keeps after the request buffers are freed. That is prod's 2.65 GB RSS with 28 MB of bodies, reproduced. Two arenas instead of the default eight per core save only 10%, so an arena setting will not fix v0.2.3.

v0.3.0 also keeps some memory on Linux, just not in the Rust heap. At the end of every run jemalloc reported 590 to 620 MB resident, nearly all of it the 512 MiB mem cache. RSS was 1.1 to 2.3 GB. The difference is RocksDB's C++ allocations, which go through glibc malloc and not jemalloc: the 512 MiB block and blob cache, the memtables, and whatever glibc keeps after compactions and large reads. It grew with load, from 0.5 GB after a seed to 1.7 GB after the c=32 steps, and it stayed flat through the idle period. It is bounded by what RocksDB holds, but if it matters, the next step is to build RocksDB against jemalloc as well.

### Latency and throughput, mixed load at c=16

| Route | v0.2.3 req/s | p50 ms | p99 ms | p999 ms | v0.3.0 req/s | p50 ms | p99 ms | p999 ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| batch | 0.1 | 2857 | 276622 | 276622 | 5.9 | 416 | 2443 | 3310 |
| index | 0.0 | 1033 | 4870 | 4870 | 2.5 | 158 | 1998 | 2222 |
| resource | 0.3 | 828 | 2140 | 237565 | 23.5 | 84 | 512 | 676 |
| site | 0.1 | 12847 | 345197 | 345197 | 10.6 | 872 | 2119 | 2635 |

v0.2.3 completed 175 requests, with 5 errors, before the kernel killed it. v0.3.0 completed 2560 in 60 s with none. v0.2.3's rates are over the whole 440 s the phase took, including the stall.

At c=8, where v0.3.0 is not saturated: batch 6.4/s p99 1394 ms, index 2.8/s p99 899 ms, resource 24.2/s p50 15 ms p99 205 ms, site 10.4/s p99 1564 ms.

### Concurrency steps, 10 s each

p99 in ms, with each route's rate at that step.

| c | v0.2.3 resource | v0.2.3 batch | v0.2.3 site | v0.3.0 resource | v0.3.0 batch | v0.3.0 site |
| ---: | --- | --- | --- | --- | --- | --- |
| 1 | 0.6/s, 49 | 0.2/s, 1975 | 0.3/s, 3029 | 9/s, 26 | 3/s, 73 | 4/s, 313 |
| 2 | 1.2/s, 46 | 0.4/s, 46 | 0.8/s, 3290 | 18/s, 27 | 5/s, 80 | 7/s, 317 |
| 4 | 2.1/s, 238 | 0.7/s, 71 | 1.2/s, 4234 | 21/s, 59 | 6/s, 104 | 9/s, 1361 |
| 8 | 2.6/s, 1019 | 0.8/s, 1287 | 1.4/s, 9122 | 23/s, 336 | 7/s, 448 | 10/s, 1160 |
| 16 | 2.8/s, 2807 | 1.2/s, 3601 | 1.4/s, 11335 | 23/s, 507 | 6/s, 817 | 10/s, 2315 |
| 32 | 2.9/s, 3965 | 1.3/s, 12507 | 1.4/s, 16074 | 25/s, 899 | 6/s, 2378 | 11/s, 2814 |

v0.3.0 reaches its ceiling at c=8 on 4 vCPUs, where server and loadgen together use all four. Its RSS went from 1292 MiB at c=1 to 2275 MiB at c=32. v0.2.3's went from 2958 to 3638 MiB.

### Server CPU

Seconds of server CPU over each run, taken from `/proc/<pid>/stat`.

| Run | v0.2.3 | v0.3.0 |
| --- | ---: | ---: |
| Seed | 9.8 | 7.4 |
| Site storm | 99.3, 41 site loads | 174.2, 867 site loads |
| Batch storm | 22.1 until killed | 43.8 |
| Write churn | 23.8 until killed | 44.3 |
| Mixed c=16 | 112.7 until killed | 183.1 |
| Steps | 218.4 | 163.3 |

Per request v0.3.0 is far cheaper: in the site storm, after taking out the seed, it spent 0.2 CPU s per site load against about 2.2 for v0.2.3.

### Regressions on Linux

The macOS write-throughput gap does not show up here. Seeding took 8.1 to 9.7 s on v0.3.0 and 9.6 to 11.5 s on v0.2.3. Write churn ran at 7.4 batches/s on v0.3.0 and 7.6/s on v0.2.3 before it died. The batch storm's p50 of 5.0 s on v0.3.0 is writes waiting on the 1 GiB in-flight budget, not slower writes. It moved 110 MiB/s against v0.2.3's 85 MiB/s and returned no errors.

The one Linux-specific finding is the post-idle RSS above: v0.3.0 sits at 1.9 to 2.3 GB after heavy mixed load while jemalloc holds 0.6 GB, because RocksDB still allocates through glibc.

### zigbuild and RocksDB

v0.2.3 cross-built with zigbuild crashed with SIGSEGV on the first concurrent batch writes, in `rocksdb::WriteThread::EnterAsBatchGroupLeader` (`db/write_thread.cc:560`) under `DBImpl::PipelinedWriteImpl`, called from the `spawn_blocking` write in `commit_entries`. The same source built natively on the box ran every scenario without a crash, so the v0.2.3 numbers above use the native build. v0.3.0 uses the same librocksdb-sys 0.17.3 (RocksDB 10.4.2) and also enables pipelined writes. Its zigbuild binary served every run here, about 9,500 requests, without a crash. The cause is not known. Until it is, treat the zigbuild binary as unproven for prod: build v0.3.0 natively on the box, or soak the zigbuild binary under concurrent writes first.

### Commands

```bash
# v0.3.0 and the loadgen, cross-built on macOS
scripts/build-aarch64.sh
(cd benches/loadgen && PATH="<dir with zig ar and ranlib shims>:$PATH" \
  cargo zigbuild --release --target aarch64-unknown-linux-gnu.2.26)

# v0.2.3, built natively on the c7g
git archive bdf83f6 | tar -x -C v023src
(cd v023src && cargo build --release)

# Each run, as root on the box: fresh dir, unprivileged user, nofile 65535
cd "$(mktemp -d)" && chown bench .
bash -c "ulimit -n 65535; ulimit -c 0; exec setpriv --reuid=bench --regid=bench --init-groups \
  env CACHE_PORT=18080 MEILI_DISABLE=1 RUST_LOG=warn $SERVER" &
/usr/bin/time -v loadgen url=http://127.0.0.1:18080 pid=$! csv=run.rss.csv <args>

# <args> per scenario, for SERVER in v0.2.3 and v0.3.0
C="sites=20 rss_limit_mib=7300 idle_after=60"
scenario=seed $C
scenario=seed,site-storm secs=60 conc=1:32 $C
scenario=seed,batch-storm secs=45 conc=1:16 $C
scenario=write-churn secs=60 conc=8 $C
scenario=seed,mixed secs=60 conc=16 $C
scenario=seed,steps steps=1,2,4,8,16,32 step_secs=10 $C

# retention with the mem cache emptied, v0.2.3, with and without MALLOC_ARENA_MAX=2
CACHE_TTL_SECS=30 CACHE_CLEANUP_INTERVAL_SECS=10 \
  scenario=seed,mixed secs=60 conc=8 sites=20 rss_limit_mib=7300 idle_after=120
# the same load on v0.3.0 with default settings
scenario=seed,mixed secs=60 conc=8 sites=20 rss_limit_mib=7300 idle_after=120
```

After each run the script read `/cache/size`, `/metrics` and `/proc/<pid>/smaps_rollup`, and counted new `Out of memory` lines in `dmesg`.
