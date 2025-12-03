# hybrid_cache_server

A small Rust service that acts as a **Chrome-aware cache indexing server**:

- **RocksDB** for persistent storage
- **DashMap** as an in-memory cache
- **Meilisearch** for index lookups
- **Deduped file bodies** so shared assets (e.g. CDNs like jQuery) are stored once and reused across websites

You send it HTTP responses (with your own `resource_key` / `website_key`) and it:

- Stores the metadata + body
- Deduplicates the body via a content hash
- Indexes metadata in Meilisearch
- Lets you quickly retrieve:
  - a **single resource** by `resource_key`
  - **all resources for a given website** by `website_key`

---

## Quick start

Make sure to have Rust. Rocksdb, and Meilisearch installed.

1. `cargo install hybrid_cache_server`
2. `./start.sh`

Use the env variable `CACHE_PORT` to change the startup port.

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
    Overrides/sets `website_key` if present.

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

### `POST /cache/index/batch` — Index a batch of resources

Index many HTTP responses at once.

#### Request

- Method: `POST`
- Path: `/cache/index/batch`
- Headers:
  - `Content-Type: application/json`
  - Optional: `X-Cache-Site: example.com` (applies as a default/override depending on your server logic)
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

### `GET /cache/resource/{resource_key}` — Fetch a cached resource

Lookup a cached resource by its `resource_key`.

#### Request

- Method: `GET`
- Path: `/cache/resource/{resource_key}`
- Query params (optional):
  - `raw=1` → return raw bytes (instead of JSON/base64)
  - `format=bytes` or `format=raw` → same as `raw=1`

#### Response

- Default: JSON containing metadata + `body_base64`
- With `raw=1` (or `format=raw|bytes`): returns the raw body bytes (content-type may be inferred from stored headers)

#### Examples

Fetch JSON (default):

```bash
curl -sS "http://127.0.0.1:8080/cache/resource/GET:https%3A%2F%2Fexample.com%2Fapp.js"
```

### `GET /cache/site/{website_key}` — List resources for a site

Lookup cached resources by `website_key` (ex: a domain / site key).

#### Request

- Method: `GET`
- Path: `/cache/site/{website_key}`

#### Response

Returns JSON for the site index (typically includes a list of resource keys and/or metadata, depending on your server’s index schema).

#### Example

```bash
curl -sS "http://127.0.0.1:8080/cache/site/example.com"
```

### `GET /cache/size` — Cache size & stats

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

## Docker

```
docker build -f docker/Dockerfile.ubuntu -t hybrid-cache:ubuntu --build-arg BIN_NAME=hybrid_cache_server .
docker run -p 8080:8080 -p 7700:7700 -e MEILI_MASTER_KEY=masterKey hybrid-cache:ubuntu
```
