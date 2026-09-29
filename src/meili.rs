//! Optional Meilisearch indexing. Off by default: nothing in the server
//! reads the index back. MEILI_ENABLE=1 turns it on.

use std::{collections::HashMap, time::Duration};

use meilisearch_sdk::client::Client as MeiliClient;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tracing::{error, info, warn};

use crate::{config::MeiliConfig, model::ResourceEntry, telemetry as tm};

/// Minimal document indexed in Meilisearch.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheIndexDoc {
    /// Meili primary key (hex only), derived from resource_key.
    pub doc_id: String,
    pub website_key: String,
    pub resource_key: String,
    pub url: String,
    pub status: u16,
    pub content_type: Option<String>,
}

impl CacheIndexDoc {
    pub fn from_resource(r: &ResourceEntry) -> Self {
        CacheIndexDoc {
            doc_id: doc_id(&r.resource_key),
            website_key: r.website_key.clone(),
            resource_key: r.resource_key.clone(),
            url: r.url.clone(),
            status: r.status,
            content_type: r.content_type().map(str::to_string),
        }
    }
}

pub fn doc_id(resource_key: &str) -> String {
    hex::encode(blake3::hash(resource_key.as_bytes()).as_bytes())
}

pub struct Meili {
    client: MeiliClient,
    index: String,
    tx: mpsc::Sender<CacheIndexDoc>,
}

impl Meili {
    pub async fn start(cfg: &MeiliConfig) -> Option<Meili> {
        let key = (!cfg.key.is_empty()).then(|| cfg.key.clone());
        let client = match MeiliClient::new(cfg.host.clone(), key) {
            Ok(c) => c,
            Err(e) => {
                error!("meilisearch client: {e}; indexing disabled");
                return None;
            }
        };
        let mut idx = client.index(&cfg.index);
        let probe = tokio::time::timeout(Duration::from_secs(5), idx.fetch_info()).await;
        if !matches!(probe, Ok(Ok(_))) {
            let _ = tokio::time::timeout(
                Duration::from_secs(5),
                client.create_index(&cfg.index, Some("doc_id")),
            )
            .await;
        }
        let (tx, rx) = mpsc::channel(cfg.queue_cap);
        tokio::spawn(worker(
            rx,
            client.clone(),
            cfg.index.clone(),
            cfg.flush_every,
            cfg.batch_max,
        ));
        info!(
            "meilisearch indexing on ({}, index {})",
            cfg.host, cfg.index
        );
        Some(Meili {
            client,
            index: cfg.index.clone(),
            tx,
        })
    }

    /// Queue a document without waiting. A full queue drops the document
    /// and counts it; the request path never blocks on Meilisearch.
    pub fn enqueue(&self, doc: CacheIndexDoc) {
        match self.tx.try_send(doc) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                metrics::counter!(tm::MEILI_DROPPED, "reason" => "full").increment(1);
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                metrics::counter!(tm::MEILI_DROPPED, "reason" => "closed").increment(1);
            }
        }
    }

    pub async fn delete(&self, resource_keys: &[String]) {
        if resource_keys.is_empty() {
            return;
        }
        let ids: Vec<String> = resource_keys.iter().map(|k| doc_id(k)).collect();
        if let Err(e) = self.client.index(&self.index).delete_documents(&ids).await {
            warn!("meilisearch delete of {} docs failed: {e}", ids.len());
        }
    }
}

async fn flush(
    index: &meilisearch_sdk::indexes::Index,
    buf: &mut HashMap<String, CacheIndexDoc>,
    max_batch: usize,
) {
    while !buf.is_empty() {
        let keys: Vec<String> = buf.keys().take(max_batch).cloned().collect();
        let docs: Vec<CacheIndexDoc> = keys.iter().filter_map(|k| buf.remove(k)).collect();
        if docs.is_empty() {
            break;
        }
        if let Err(e) = index.add_documents(&docs, Some("doc_id")).await {
            warn!(
                "meilisearch add_documents ({} docs) failed: {e}",
                docs.len()
            );
        }
    }
}

async fn worker(
    mut rx: mpsc::Receiver<CacheIndexDoc>,
    client: MeiliClient,
    index_name: String,
    flush_every: Duration,
    max_batch: usize,
) {
    let index = client.index(&index_name);
    // Latest doc per doc_id. Bounded by max_batch: it flushes when full.
    let mut buf: HashMap<String, CacheIndexDoc> = HashMap::new();
    let mut tick = tokio::time::interval(flush_every);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = tick.tick() => flush(&index, &mut buf, max_batch).await,
            maybe = rx.recv() => match maybe {
                Some(doc) => {
                    buf.insert(doc.doc_id.clone(), doc);
                    if buf.len() >= max_batch {
                        flush(&index, &mut buf, max_batch).await;
                    }
                }
                None => {
                    flush(&index, &mut buf, max_batch).await;
                    break;
                }
            }
        }
    }
}
