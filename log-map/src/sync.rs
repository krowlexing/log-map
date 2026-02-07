//! Background synchronization task for keeping the cache updated.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use futures_util::StreamExt;
use log_server_types::kv::kv_server_client::KvServerClient;
use log_server_types::kv::{Record, SubscribeRequest};
use tonic::transport::Channel;

use crate::cache::Cache;
use crate::Error;

const MAP_PREFIX: &str = "map:";

pub struct SyncTask {
    client: KvServerClient<Channel>,
    cache: Arc<Cache>,
    last_sync: Arc<AtomicU64>,
    latest_known: Arc<AtomicU64>,
}

impl SyncTask {
    pub fn new(
        client: KvServerClient<Channel>,
        cache: Arc<Cache>,
        last_sync: Arc<AtomicU64>,
        latest_known: Arc<AtomicU64>,
    ) -> Self {
        Self {
            client,
            cache,
            last_sync,
            latest_known,
        }
    }

    pub async fn run(mut self) -> Result<(), Error> {
        loop {
            let from = self.last_sync.load(Ordering::SeqCst);

            let mut request = SubscribeRequest::default();
            request.start_ordinal = from;

            let mut stream = self.client.subscribe(request).await?.into_inner();

            while let Some(result) = stream.next().await {
                match result {
                    Ok(record) => {
                        self.process_record(record);
                    }
                    Err(e) => {
                        return Err(Error::from(e));
                    }
                }
            }
        }
    }

    fn process_record(&self, record: Record) {
        if let Some(key) = record.key.strip_prefix(MAP_PREFIX) {
            if let Ok(parsed_key) = key.parse::<i64>() {
                self.last_sync.fetch_max(record.ordinal, Ordering::SeqCst);
                self.latest_known.fetch_max(record.ordinal, Ordering::SeqCst);

                if record.value.is_empty() {
                    self.cache.remove(&parsed_key);
                } else {
                    let value = String::from_utf8_lossy(&record.value).to_string();
                    self.cache.insert(parsed_key, value);
                }
            }
        }
    }
}
