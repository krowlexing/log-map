use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures_util::{stream, StreamExt};
use log_server_types::kv::kv_server_client::KvServerClient;
use log_server_types::kv::WriteRequest;
use tonic::transport::{Channel, Endpoint};
use tokio::task::JoinHandle;

use crate::cache::Cache;
use crate::error::Error;
use crate::sync::SyncTask;

const MAP_PREFIX: &str = "map:";
const MAX_RETRIES: usize = 5;

pub struct LogMap {
    inner: Arc<LogMapInner>,
}

struct LogMapInner {
    cache: Arc<Cache>,
    client: tokio::sync::Mutex<KvServerClient<Channel>>,
    next_ordinal: AtomicU64,
    latest_known: Arc<AtomicU64>,
    last_sync: Arc<AtomicU64>,
    _sync_handle: Arc<tokio::sync::Mutex<Option<JoinHandle<()>>>>,
}

impl LogMap {
    pub async fn connect(addr: impl Into<ServerAddr>) -> Result<Self, Error> {
        let server_addr = addr.into();
        let endpoint = Endpoint::from_shared(format!("http://{}", server_addr.0))?;
        let channel = endpoint.connect().await?;
        let client = KvServerClient::new(channel);

        let cache = Arc::new(Cache::new());
        let next_ordinal = AtomicU64::new(1);
        let latest_known = Arc::new(AtomicU64::new(0));
        let last_sync = Arc::new(AtomicU64::new(0));

        let inner = Arc::new(LogMapInner {
            cache: Arc::clone(&cache),
            client: tokio::sync::Mutex::new(client),
            next_ordinal,
            latest_known: Arc::clone(&latest_known),
            last_sync: Arc::clone(&last_sync),
            _sync_handle: Arc::new(tokio::sync::Mutex::new(None)),
        });

        let sync_task = SyncTask::new(
            inner.client.lock().await.clone(),
            cache,
            last_sync,
            latest_known,
        );

        let sync_handle = tokio::spawn(async move {
            if let Err(e) = sync_task.run().await {
                eprintln!("Sync task error: {:?}", e);
            }
        });

        *inner._sync_handle.lock().await = Some(sync_handle);

        Ok(Self { inner })
    }

    pub async fn get(&self, key: i64) -> Result<Option<String>, Error> {
        Ok(self.inner.cache.get(&key))
    }

    pub async fn insert(&self, key: i64, value: String) -> Result<(), Error> {
        let mut retries = 0;
        let mut delay = Duration::from_millis(100);

        loop {
            let ordinal = self.inner.next_ordinal.fetch_add(1, Ordering::SeqCst);
            let latest_known = self.inner.latest_known.load(Ordering::SeqCst);

            let mut request = WriteRequest::default();
            request.ordinal = ordinal;
            request.key = format!("{}{}", MAP_PREFIX, key);
            request.value = value.clone().into_bytes();
            request.latest_known = latest_known;

            let mut client = self.inner.client.lock().await;
            let request_stream = stream::once(async { request });
            let mut response_stream = client.write(request_stream).await?.into_inner();
            let response = response_stream.next().await.ok_or(Error::ConnectionClosed)??;
            drop(client);

            if response.accepted {
                return Ok(());
            }

            retries += 1;
            if retries >= MAX_RETRIES {
                return Err(Error::Conflict(retries));
            }

            self.sync_now().await?;
            tokio::time::sleep(delay).await;
            delay *= 2;
        }
    }

    pub async fn remove(&self, key: i64) -> Result<(), Error> {
        let mut retries = 0;
        let mut delay = Duration::from_millis(100);

        loop {
            let ordinal = self.inner.next_ordinal.fetch_add(1, Ordering::SeqCst);
            let latest_known = self.inner.latest_known.load(Ordering::SeqCst);

            let mut request = WriteRequest::default();
            request.ordinal = ordinal;
            request.key = format!("{}{}", MAP_PREFIX, key);
            request.value = Vec::new();
            request.latest_known = latest_known;

            let mut client = self.inner.client.lock().await;
            let request_stream = stream::once(async { request });
            let mut response_stream = client.write(request_stream).await?.into_inner();
            let response = response_stream.next().await.ok_or(Error::ConnectionClosed)??;
            drop(client);

            if response.accepted {
                return Ok(());
            }

            retries += 1;
            if retries >= MAX_RETRIES {
                return Err(Error::Conflict(retries));
            }

            self.sync_now().await?;
            tokio::time::sleep(delay).await;
            delay *= 2;
        }
    }

    pub fn contains_key(&self, key: i64) -> bool {
        self.inner.cache.contains_key(&key)
    }

    pub fn len(&self) -> usize {
        self.inner.cache.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.cache.is_empty()
    }

    pub async fn sync_now(&self) -> Result<(), Error> {
        Ok(())
    }
}

#[derive(Clone)]
pub struct ServerAddr(pub String);

impl From<String> for ServerAddr {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for ServerAddr {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}
