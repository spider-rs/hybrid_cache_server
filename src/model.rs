//! Stored records and the JSON wire format.

use std::{borrow::Cow, collections::HashMap};

use base64::{engine::general_purpose::STANDARD, Engine};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Resource metadata, without the body. Stored as JSON at `res:{resource_key}`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceEntry {
    pub website_key: String,
    pub resource_key: String,
    pub url: String,
    pub method: String,
    pub status: u16,
    pub request_headers: HashMap<String, String>,
    pub response_headers: HashMap<String, String>,
    /// ID of the body stored at `file:{file_id}`, deduped by content.
    pub file_id: String,
    #[serde(default)]
    pub http_version: HttpVersion,
    /// When this resource was cached (unix seconds).
    #[serde(default)]
    pub created_at: Option<i64>,
}

impl ResourceEntry {
    pub fn content_type(&self) -> Option<&str> {
        self.response_headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("content-type"))
            .map(|(_, v)| v.as_str())
    }

    /// Rough heap size, for the mem cache weigher.
    pub fn heap_estimate(&self) -> usize {
        let headers = |m: &HashMap<String, String>| -> usize {
            m.iter().map(|(k, v)| k.len() + v.len() + 48).sum::<usize>()
        };
        self.website_key.len()
            + self.resource_key.len()
            + self.url.len()
            + self.method.len()
            + self.file_id.len()
            + headers(&self.request_headers)
            + headers(&self.response_headers)
            + 160
    }
}

/// The few fields the TTL cleanup needs. serde skips the header maps
/// without allocating them.
#[derive(Debug, Deserialize)]
pub struct ResourceMeta<'a> {
    #[serde(borrow)]
    pub website_key: Cow<'a, str>,
    #[serde(borrow)]
    pub resource_key: Cow<'a, str>,
    #[serde(borrow)]
    pub file_id: Cow<'a, str>,
    #[serde(default)]
    pub created_at: Option<i64>,
}

/// Legacy stored body format: `{"file_id":"...","body":[...]}`. Current
/// versions store raw bytes, but old entries can still be read.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileEntry {
    pub file_id: String,
    pub body: Vec<u8>,
}

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

/// Wire shape of one resource, as sent by clients and returned by the
/// lookup routes. Field names are part of the API. The server itself reads
/// [`IncomingPayload`] and writes with [`PayloadEncoder`]; this type is the
/// reference shape the tests check both against.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(not(test), allow(dead_code))]
pub struct CachedEntryPayload {
    #[serde(default)]
    pub website_key: Option<String>,
    pub resource_key: String,
    pub url: String,
    pub method: String,
    pub status: u16,
    pub request_headers: HashMap<String, String>,
    pub response_headers: HashMap<String, String>,
    pub body_base64: String,
    #[serde(default)]
    pub http_version: HttpVersion,
    /// Returned by the lookup routes; ignored on writes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_at: Option<i64>,
}

/// Same shape as [`CachedEntryPayload`], parsed from the request buffer.
/// `body_base64` borrows from that buffer (base64 has no JSON escapes), so
/// the only body copy made is the decoded bytes.
#[derive(Debug, Deserialize)]
pub struct IncomingPayload<'a> {
    #[serde(default)]
    pub website_key: Option<String>,
    pub resource_key: String,
    pub url: String,
    pub method: String,
    pub status: u16,
    #[serde(default)]
    pub request_headers: HashMap<String, String>,
    #[serde(default)]
    pub response_headers: HashMap<String, String>,
    #[serde(borrow)]
    pub body_base64: Cow<'a, str>,
    #[serde(default)]
    pub http_version: HttpVersion,
}

/// A resource plus its body, as held in the mem cache. The body is
/// reference counted; nothing here is copied when a response is built.
#[derive(Debug)]
pub struct CachedResource {
    pub resource: ResourceEntry,
    pub body: Bytes,
}

/// Borrowed view of the metadata fields of a payload, serialized without
/// `body_base64` (which is written separately).
#[derive(Serialize)]
struct PayloadMeta<'a> {
    website_key: Option<&'a str>,
    resource_key: &'a str,
    url: &'a str,
    method: &'a str,
    status: u16,
    request_headers: &'a HashMap<String, String>,
    response_headers: &'a HashMap<String, String>,
    http_version: HttpVersion,
    /// Unix seconds when the resource was stored. Absent for entries
    /// written before created_at existed.
    #[serde(skip_serializing_if = "Option::is_none")]
    created_at: Option<i64>,
}

const B64_PREFIX: &[u8] = b"{\"body_base64\":\"";

/// Serialized size of the metadata part and a closure-free way to write the
/// whole payload into one exactly sized buffer.
pub struct PayloadEncoder {
    meta: Vec<u8>,
    body_len: usize,
}

impl PayloadEncoder {
    pub fn new(resource: &ResourceEntry, body_len: usize) -> Result<Self, String> {
        let meta = serde_json::to_vec(&PayloadMeta {
            website_key: Some(&resource.website_key),
            resource_key: &resource.resource_key,
            url: &resource.url,
            method: &resource.method,
            status: resource.status,
            request_headers: &resource.request_headers,
            response_headers: &resource.response_headers,
            http_version: resource.http_version,
            created_at: resource.created_at,
        })
        .map_err(|e| format!("serialize payload meta: {e}"))?;
        Ok(Self { meta, body_len })
    }

    /// Exact number of bytes [`Self::write`] appends.
    pub fn len(&self) -> usize {
        // {"body_base64":"<b64>",<meta without its leading '{'>
        B64_PREFIX.len() + base64_len(self.body_len) + 2 + (self.meta.len() - 1)
    }

    /// Append the payload JSON to `out`. The base64 is encoded straight into
    /// `out`; there is no intermediate String.
    pub fn write(&self, body: &[u8], out: &mut Vec<u8>) {
        debug_assert_eq!(body.len(), self.body_len);
        out.extend_from_slice(B64_PREFIX);
        let start = out.len();
        let n = base64_len(body.len());
        out.resize(start + n, 0);
        let written = STANDARD.encode_slice(body, &mut out[start..]).unwrap_or(0);
        out.truncate(start + written);
        out.extend_from_slice(b"\",");
        out.extend_from_slice(&self.meta[1..]);
    }
}

pub fn base64_len(n: usize) -> usize {
    n.div_ceil(3) * 4
}

/// Encode one payload into a fresh, exactly sized buffer.
pub fn encode_payload(resource: &ResourceEntry, body: &[u8]) -> Result<Vec<u8>, String> {
    let enc = PayloadEncoder::new(resource, body.len())?;
    let mut out = Vec::with_capacity(enc.len());
    enc.write(body, &mut out);
    Ok(out)
}
