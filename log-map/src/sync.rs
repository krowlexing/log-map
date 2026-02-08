//! Background synchronization task for keeping the cache updated.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use futures_util::StreamExt;
use log_server_types::kv::kv_server_client::KvServerClient;
use log_server_types::kv::{GetSnapshotRequest, Record, SubscribeRequest};
use tonic::transport::Channel;

use crate::Error;
use crate::cache::Cache;

const MAP_PREFIX: &str = "map:";
const BMAP_MAGIC: &[u8; 4] = b"BMAP";

pub struct SnapshotLoader;

impl SnapshotLoader {
    pub fn load_from_bytes(data: &[u8]) -> Result<Vec<(String, Vec<u8>)>, String> {
        if data.is_empty() {
            return Ok(Vec::new());
        }

        if data.len() < 12 {
            return Err("Data too short".to_string());
        }

        if &data[0..4] != BMAP_MAGIC {
            return Err("Invalid magic".to_string());
        }

        let version = u32::from_le_bytes([data[4], data[5], data[6], data[7]]);
        if version != 1 {
            return Err(format!("Invalid version: {}", version));
        }

        let count = u32::from_le_bytes([data[8], data[9], data[10], data[11]]) as usize;
        let mut result = Vec::with_capacity(count);
        let mut offset = 12;

        for _ in 0..count {
            if offset + 2 > data.len() {
                return Err("Data truncated (key length)".to_string());
            }
            let key_len = u16::from_le_bytes([data[offset], data[offset + 1]]) as usize;
            offset += 2;

            if offset + key_len > data.len() {
                return Err("Data truncated (key)".to_string());
            }
            let key = String::from_utf8_lossy(&data[offset..offset + key_len]).to_string();
            offset += key_len;

            if offset + 4 > data.len() {
                return Err("Data truncated (value length)".to_string());
            }
            let value_len = u32::from_le_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
            ]) as usize;
            offset += 4;

            if offset + value_len > data.len() {
                return Err("Data truncated (value)".to_string());
            }
            let value = data[offset..offset + value_len].to_vec();
            offset += value_len;

            result.push((key, value));
        }

        Ok(result)
    }
}

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

    pub async fn initialize_with_snapshot(
        client: &KvServerClient<Channel>,
        cache: &Arc<Cache>,
    ) -> Result<u64, Error> {
        let mut client_clone = client.clone();
        let response = client_clone
            .get_snapshot(GetSnapshotRequest::default())
            .await?
            .into_inner();

        println!("latest snapshot ordinal: {}", response.snapshot_ordinal);
        if response.snapshot_ordinal > 0 && !response.snapshot_data.is_empty() {
            println!("log-map: loading from snapshot...");
            let records = SnapshotLoader::load_from_bytes(&response.snapshot_data)
                .map_err(|e| Error::Internal(e.to_string()))?;
            println!("log-map: received {} records", records.len());

            let parsed: Vec<(i64, String)> = records
                .into_iter()
                .filter_map(|(key, value)| {
                    key.strip_prefix(MAP_PREFIX)
                        .and_then(|k| k.parse::<i64>().ok())
                        .map(|k| (k, String::from_utf8_lossy(&value).to_string()))
                        .filter(|(_, v)| !v.is_empty())
                })
                .collect();

            cache.insert_all(parsed);
        }

        Ok(response.snapshot_ordinal)
    }

    pub async fn run(mut self) -> Result<(), Error> {
        println!("starting syncing...");
        loop {
            println!("initializing with snapshot...");
            let from = Self::initialize_with_snapshot(&self.client, &self.cache).await?;
            self.last_sync.store(from, Ordering::SeqCst);

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
                self.latest_known
                    .fetch_max(record.ordinal, Ordering::SeqCst);

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
