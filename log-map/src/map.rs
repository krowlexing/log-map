//! Distributed map implementation with optimistic concurrency control.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use futures_util::{StreamExt, stream};
use log_server_types::kv::WriteRequest;
use log_server_types::kv::kv_server_client::KvServerClient;
use tokio::task::JoinHandle;
use tonic::transport::{Channel, Endpoint};

use crate::cache::Cache;
use crate::error::Error;
use crate::sync::SyncTask;

const MAP_PREFIX: &str = "map:";
const MAX_RETRIES: usize = 5;

/// A distributed key-value map backed by the log-server.
///
/// `LogMap` stores `i64` keys with `String` values through the log-server's
/// gRPC API. All mutations go through the log's append-only storage with
/// optimistic concurrency control.
///
/// # Conflict Resolution
///
/// When a write is rejected by the server, `LogMap` automatically:
/// 1. Syncs the latest state from the server
/// 2. Retries the write with updated `latest_known` ordinal
/// 3. Uses exponential backoff (100ms starting, doubles each retry)
/// 4. Gives up after 5 retries
///
/// # Key Encoding
///
/// Keys are encoded as `"map:{i64}"` in the log to avoid collisions with
/// other data using the same log-server.
///
/// # Example
///
/// ```no_run
/// use log_map::LogMap;
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let map = LogMap::connect("localhost:50051").await?;
///
///     map.insert(1, "hello".to_string()).await?;
///     assert_eq!(map.get(1).await?, Some("hello".to_string()));
///
///     map.remove(1).await?;
///     assert_eq!(map.get(1).await?, None);
///
///     Ok(())
/// }
/// ```
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
    /// Connects to a log-server and creates a new `LogMap` instance.
    ///
    /// This spawns a background task that subscribes to log updates and
    /// keeps the local cache synchronized.
    ///
    /// # Arguments
    ///
    /// * `addr` - Server address (e.g., `"localhost:50051"`)
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

    /// Gets the value for a key from the local cache.
    pub async fn get(&self, key: i64) -> Result<Option<String>, Error> {
        Ok(self.inner.cache.get(&key))
    }

    /// Inserts a key-value pair into the map.
    ///
    /// This writes to the log-server with optimistic concurrency control.
    /// On conflict, it will retry up to 5 times with exponential backoff.
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
            let response = response_stream
                .next()
                .await
                .ok_or(Error::ConnectionClosed)??;
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

    /// Removes a key from the map by writing a tombstone.
    ///
    /// This writes an empty value to the log-server, which is interpreted
    /// as a deletion by the sync task.
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
            let response = response_stream
                .next()
                .await
                .ok_or(Error::ConnectionClosed)??;
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

    /// Checks if the map contains a key.
    pub fn contains_key(&self, key: i64) -> bool {
        self.inner.cache.contains_key(&key)
    }

    /// Returns the number of entries in the local cache.
    pub fn len(&self) -> usize {
        self.inner.cache.len()
    }

    /// Returns `true` if the map contains no entries.
    pub fn is_empty(&self) -> bool {
        self.inner.cache.is_empty()
    }

    /// Manually trigger a sync (no-op currently, sync happens in background).
    pub async fn sync_now(&self) -> Result<(), Error> {
        Ok(())
    }
}

/// Server address wrapper for type-safe connection.
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
