use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use futures_util::stream::Stream;
use std::pin::Pin;
use tokio::sync::{broadcast, RwLock};

use crate::models::Record;
use super::{MapCache, StorageBackend, WriteError};

type RecordMap = BTreeMap<u64, (String, Vec<u8>, i64)>;

pub struct MemoryStorage {
    records: Arc<RwLock<RecordMap>>,
    cache: MapCache,
    next_ordinal: AtomicU64,
    tx: broadcast::Sender<Record>,
}

impl MemoryStorage {
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(1024);
        Self {
            records: Arc::new(RwLock::new(BTreeMap::new())),
            cache: MapCache::new(),
            next_ordinal: AtomicU64::new(1),
            tx,
        }
    }
}

impl Default for MemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl StorageBackend for MemoryStorage {
    async fn append(&self, key: String, value: Vec<u8>) -> Result<u64, sqlx::Error> {
        let mut records = self.records.write().await;
        let timestamp = chrono::Utc::now().timestamp_millis();
        let ordinal = self.next_ordinal.fetch_add(1, Ordering::SeqCst);

        records.insert(ordinal, (key.clone(), value.clone(), timestamp));
        let _ = self.tx.send(Record {
            ordinal,
            key,
            value,
            timestamp,
        });

        Ok(ordinal)
    }

    async fn write(
        &self,
        _ordinal: u64,
        key: String,
        value: Vec<u8>,
        latest_known: u64,
    ) -> Result<u64, WriteError> {
        let mut records = self.records.write().await;
        let timestamp = chrono::Utc::now().timestamp_millis();
        let ordinal = self.next_ordinal.fetch_add(1, Ordering::SeqCst);

        self.cache.update(key.clone(), latest_known as i64, ordinal as i64).await.map_err(|_| {
            let latest = records.keys().next_back().copied().unwrap_or(0);
            WriteError::Conflict(latest)
        })?;

        records.insert(ordinal, (key.clone(), value.clone(), timestamp));
        let value_maybe_str = String::from_utf8_lossy(&value);
        println!("update #{ordinal}: {key} -> {value_maybe_str:?}");
        let _ = self.tx.send(Record {
            ordinal,
            key,
            value,
            timestamp,
        });

        Ok(ordinal)
    }

    fn subscribe_from(&self, ordinal: u64) -> Pin<Box<dyn Stream<Item = Record> + Send>> {
        let records = self.records.clone();
        let tx = self.tx.clone();

        Box::pin(async_stream::stream! {
            let mut rx = {
                let r = records.read().await;
                for (&ord, (key, value, timestamp)) in r.range(ordinal..) {
                    yield Record {
                        ordinal: ord,
                        key: key.clone(),
                        value: value.clone(),
                        timestamp: *timestamp,
                    };
                }
                tx.subscribe()
            };

            loop {
                match rx.recv().await {
                    Ok(record) => {
                        if record.ordinal >= ordinal {
                            yield record;
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!("Subscriber lagged by {} messages", n);
                    }
                }
            }
        })
    }

    async fn get_latest_snapshot(&self) -> Result<Option<(u64, Vec<u8>)>, WriteError> {
        Ok(None)
    }
}
