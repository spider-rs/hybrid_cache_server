# Hybrid Cache Server

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

## Data Model

### Keys

- **`website_key`**  
  Represents a *site-level* identifier. Examples:
  - `"example.com"`
  - `"https://example.com"`

  This is used to group resources so you can ask: “give me everything for this website”.

- **`resource_key`**  
  A *unique cache key per resource* (you generate this on the producer side, typically from your `put_hybrid_cache` logic).

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

### 1. `POST /cache/index`

Index a **single resource** (one HTTP response).

**Request**

- Headers:
  - Optional: `X-Cache-Site: example.com`  
    Overrides/sets `website_key` if present.

- Body: JSON `CachedEntryPayload`:

```jsonc
{
  "website_key": "example.com",          // optional; can come from header or derived from URL
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

## Docker

```
docker build -f docker/Dockerfile.ubuntu -t hybrid-cache:ubuntu --build-arg BIN_NAME=hybrid_cache_server .
docker run -p 8080:8080 -p 7700:7700 -e MEILI_MASTER_KEY=masterKey hybrid-cache:ubuntu
```