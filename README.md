# hybrid_cache_server

A small Rust service that acts as a **Chrome-aware cache indexing server**:

- **RocksDB** for persistent storage
- a byte-bounded in-memory cache ([moka](https://crates.io/crates/moka)) for hot resources
- optional **Meilisearch** indexing, off unless `MEILI_ENABLE=1`
- **Deduped file bodies** so shared assets (e.g. CDNs like jQuery) are stored once and reused across websites

You send it HTTP responses (with your own `resource_key` / `website_key`) and it:

- Stores the metadata + body
- Deduplicates the body via a content hash
- Indexes metadata in Meilisearch, when enabled
- Lets you quickly retrieve:
  - a **single resource** by `resource_key`
  - **all resources for a given website** by `website_key`

---

## Quick start

You need Rust. Meilisearch is only needed with `MEILI_ENABLE=1`.

1. `cargo install hybrid_cache_server`
2. `./start.sh`

### Configuration

Every setting is an environment variable. `start.sh` passes the environment through to the server.

| Variable | Default | What it does |
| --- | --- | --- |
| `CACHE_PORT` | `8080` | Listen port. |
| `ROCKSDB_PATH` | `cache_db` | Database directory, relative to the working directory. |
| `MEM_CACHE_BYTES` | 512 MiB | Byte budget of the in-memory cache. `0` turns it off. |
| `MAX_BODY_BYTES` | 160 MiB | Largest request body. Bigger ones get 413. The fleet's largest batch, 16 bodies of 5 MiB, is about 107 MiB. |
| `MAX_INFLIGHT_BYTES` | 1 GiB | Request and response buffers held at once. Writes and lookups wait for room; site loads stop adding items. |
| `SMALL_REQUEST_BYTES` | 8 MiB | Reservations up to this size use a separate pool, so they never wait behind a large write. |
| `MAX_INFLIGHT_SMALL_BYTES` | 128 MiB | Size of that pool. |
| `MAX_BLOCKING_READS` | `64` | RocksDB reads, site scans and large encodes running at once. |
| `MAX_SITE_RESPONSE_BYTES` | 64 MiB | Largest `/cache/site` response. Items past it are left out and the response carries `x-cache-truncated: size`. |
| `REQUEST_TIMEOUT_SECS` | `30` | Deadline for GET and purge requests. Past it the server answers 503. |
| `UPLOAD_TIMEOUT_SECS` | `120` | Deadline for `POST /cache/index` and `/cache/index/batch`, upload included. |
| `WRITE_IDLE_TIMEOUT_SECS` | `30` | Closes a connection whose peer accepts no response bytes for this long, which frees the buffers it held. |
| `TCP_TIMEOUT_SECS` | `30` | TCP keepalive idle time, and on Linux `TCP_USER_TIMEOUT`, on accepted sockets. |
| `HEADER_READ_TIMEOUT_SECS` | `120` | Closes a connection that sends no complete request head for this long, idle keep-alive included. Keep it above the clients' pool idle timeout (90 s in spider_remote_cache). |
| `MAX_CONNECTIONS` | `10000` | Concurrent connections. |
| `COMPRESSION` | `zstd` | Response codec: `zstd`, `br`, `gzip` or `off`. gzip is always offered as a fallback. |
| `COMPRESSION_LEVEL` | `1` | Codec level. |
| `ROCKSDB_BLOCK_CACHE_BYTES` | 512 MiB | Shared block and blob cache. |
| `ROCKSDB_MAX_OPEN_FILES` | `4096` | Needs a matching `LimitNOFILE`. |
| `ROCKSDB_BLOB_FILES` | `1` | Store bodies of 64 KiB and up in blob files. |
| `ROCKSDB_RATE_LIMIT_BYTES` | 256 MiB/s | Flush and compaction IO cap. `0` turns it off. |
| `CACHE_TTL_SECS` | `86400` | Resource lifetime. |
| `CACHE_CLEANUP_INTERVAL_SECS` | `600` | TTL cleanup interval. |
| `CACHE_FULL_SWEEP_EVERY` | `144` | The first cleanup and every Nth after it also scan every stored body for orphans. |
| `MEILI_ENABLE` | `0` | `1` turns Meilisearch indexing on. `MEILI_DISABLE=1` still wins. |
| `MEILI_HOST`, `MEILI_MASTER_KEY`, `MEILI_INDEX`, `MEILI_QUEUE_CAP`, `MEILI_BATCH_MAX`, `MEILI_FLUSH_MS` | | Used only when Meilisearch is on. A full queue drops documents and counts them in `meili_dropped_total`. |

### Building for production (aarch64 Graviton3)

On the Graviton box:

```bash
RUSTFLAGS="-C target-cpu=neoverse-v1" cargo build --release
```

From a Mac, `scripts/build-aarch64.sh` cross-builds with cargo-zigbuild. The binary needs glibc 2.25 or newer and no libstdc++.

`target-cpu=neoverse-v1` is for Graviton3 only. Leave `RUSTFLAGS` unset for any other machine.

## Data Model

### Keys

- **`website_key`**  
  Represents a _site-level_ identifier. Examples:

  - `"example.com"`
  - `"https://example.com"`

  This is used to group resources so you can ask: “give me everything for this website”.

- **`resource_key`**  
  A _unique cache key per resource_ (you generate this on the producer side, typically from your `put_hybrid_cache` logic).

  Examples:

  - `GET:https://example.com/`
  - `GET:https://example.com/style.css`
  - `GET:https://cdn.example.com/jquery.js::Accept:text/javascript`

  Whatever you use here must match the key you pass to `put_hybrid_cache(cache_key, ...)`.

- **`file_id`**  
  Internally computed as `blake3(body_bytes)` and hex-encoded.  
  All bodies with the same content share the same `file_id` and are stored **once** in RocksDB.

### RocksDB Key Layout

Internally we use these key prefixes:

- `file:{file_id}` → JSON-encoded `FileEntry` (the raw body bytes)
- `res:{resource_key}` → JSON-encoded `ResourceEntry` (metadata, including `file_id`)
- `site:{website_key}::{resource_key}` → empty value used as an index to quickly scan all resources for a site

This layout lets us:

- Deduplicate file content (`file:{file_id}` reused across many resources)
- Quickly find all `resource_key`s for a given `website_key` via prefix iteration

---

## HTTP API

All endpoints are under `/cache/*`.

### `POST /cache/index`

Index a **single resource** (one HTTP response).

**Request**

- Headers:

  - Optional: `X-Cache-Site: example.com`  
    Sets `website_key`. On this route it wins over the payload's own `website_key`.

- Body: JSON `CachedEntryPayload`:

```jsonc
{
  "website_key": "example.com", // optional; can come from header or derived from URL
  "resource_key": "GET:https://example.com/style.css",
  "url": "https://example.com/style.css",
  "method": "GET",
  "status": 200,
  "request_headers": {
    "Accept": "text/css"
  },
  "response_headers": {
    "Content-Type": "text/css; charset=utf-8"
  },
  "body_base64": "LyogY3NzIGJvZHkgKi8K"
}
```

### `POST /cache/index/batch`: index a batch of resources

A batch where some items fail still returns 201, with `Indexed N entries, M failed` as the body. It returns 500 only when no item was indexed.

Index many HTTP responses at once.

#### Request

- Method: `POST`
- Path: `/cache/index/batch`
- Headers:
  - `Content-Type: application/json`
  - Optional: `X-Cache-Site: example.com`. Each item's own `website_key` wins; the header only applies to items that have none. An item with neither is filed under its URL host.
- Body: JSON array of the same payload objects used in `/cache/index`

```jsonc
[
  {
    "website_key": "example.com",
    "resource_key": "GET:https://example.com/",
    "url": "https://example.com/",
    "method": "GET",
    "status": 200,
    "request_headers": { "Accept": "text/html" },
    "response_headers": { "Content-Type": "text/html" },
    "body_base64": "PGh0bWw+Li4uPC9odG1sPg=="
  },
  {
    "website_key": "example.com",
    "resource_key": "GET:https://example.com/app.js",
    "url": "https://example.com/app.js",
    "method": "GET",
    "status": 200,
    "request_headers": { "Accept": "*/*" },
    "response_headers": { "Content-Type": "application/javascript" },
    "body_base64": "Y29uc29sZS5sb2coImhpIik7"
  }
]
```

### `GET /cache/resource/{resource_key}`: fetch a cached resource

Lookup a cached resource by its `resource_key`.

#### Request

- Method: `GET`
- Path: `/cache/resource/{resource_key}`
- Query params (optional):
  - `raw=1` → return raw bytes (instead of JSON/base64)
  - `format=bytes` or `format=raw` → same as `raw=1`

The key may be percent-encoded as one path segment. The server tries it decoded first, then exactly as sent.

#### Response

- Default: JSON containing metadata + `body_base64`, plus `created_at` (unix seconds) when the entry has one
- With `raw=1` (or `format=raw|bytes`): returns the raw body bytes (content-type may be inferred from stored headers)

#### Examples

Fetch JSON (default):

```bash
curl -sS "http://127.0.0.1:8080/cache/resource/GET:https%3A%2F%2Fexample.com%2Fapp.js"
```

### `GET /cache/site/{website_key}`: list resources for a site

Lookup cached resources by `website_key` (ex: a domain / site key).

#### Request

- Method: `GET`
- Path: `/cache/site/{website_key}`

#### Response

Returns a JSON array of the same payload objects `/cache/resource` returns. Site index keys are add-only: when a resource is written again under a different site key, both sites list it. One response holds at most `MAX_SITE_RESPONSE_BYTES`; when items were left out, the `x-cache-truncated` header is `size` or `budget`.

#### Example

```bash
curl -sS "http://127.0.0.1:8080/cache/site/example.com"
```

### `GET /cache/size`: cache size and stats

Returns current cache statistics for memory + RocksDB.

#### Request

- Method: `GET`
- Path: `/cache/size`

#### Response

JSON with stats (example fields):

- `rocksdb.*`: RocksDB estimates and sizes
- `rocksdb_dir_bytes`: on-disk directory usage
- `mem_cache.entries`: in-memory entry count
- `mem_cache.body_bytes`: in-memory body byte total

#### Example

```bash
curl -sS "http://127.0.0.1:8080/cache/size"
```

### `GET /health`

Returns 200 `ok` when RocksDB answers a property read, 503 otherwise.

### `GET /metrics`

Prometheus text format: requests and latency by route and status, mem cache hits, misses, bytes and entries, RocksDB read and write latency, open connections, body bytes in and out, in-flight budget, Meilisearch drops, cleanup duration and removals.

## Benchmarks

See [BENCHMARKS.md](BENCHMARKS.md) and `benches/`.

## Docker

```
docker build -f docker/Dockerfile.ubuntu -t hybrid-cache:ubuntu --build-arg BIN_NAME=hybrid_cache_server .
docker run -p 8080:8080 -p 7700:7700 -e MEILI_MASTER_KEY=masterKey hybrid-cache:ubuntu
```
