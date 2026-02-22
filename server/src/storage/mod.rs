use std::pin::Pin;

use async_trait::async_trait;
use futures_util::stream::Stream;
use sqlx::{Row, SqlitePool};
use crate::{models::Record, snapshot};
pub use cache::MapCache;

mod cache;
mod memory;
mod storage;

pub use memory::MemoryStorage;
pub use storage::StorageBackend;

pub struct SqliteStorage {
    pool: SqlitePool,
    snapshot: Option<snapshot::Snapshot>,
    cache: MapCache,
}

impl SqliteStorage {
    pub fn new(pool: SqlitePool) -> Self {
        Self {
            pool,
            cache: MapCache::new(),
            snapshot: None,
        }
    }

    pub fn with_snapshot(
        pool: SqlitePool,
        snapshot_dir: &str,
        snapshot_interval: u64,
    ) -> Result<Self, snapshot::Error> {
        Ok(Self {
            pool,
            cache: MapCache::new(),
            snapshot: Some(snapshot::Snapshot::new(snapshot_dir, snapshot_interval)?),
        })
    }

    pub async fn append(&self, key: String, value: Vec<u8>) -> Result<u64, sqlx::Error> {
        let now = chrono::Utc::now().timestamp_millis();
        let result = sqlx::query(
            "INSERT INTO records (key, value, timestamp) VALUES (?, ?, ?) RETURNING ordinal",
        )
        .bind(&key)
        .bind(&value)
        .bind(now)
        .fetch_one(&self.pool)
        .await?;

        Ok(result.get("ordinal"))
    }

    pub async fn write(
        &self,
        _ordinal: u64,
        key: String,
        value: Vec<u8>,
        latest_known: u64,
    ) -> Result<u64, WriteError> {
        let now = chrono::Utc::now().timestamp_millis();

        let latest_ordinal: Option<i64> =
            sqlx::query("SELECT MAX(ordinal) as max_ord FROM records")
                .fetch_one(&self.pool)
                .await?
                .get("max_ord");

        let latest_ordinal = latest_ordinal.unwrap_or(0) as u64;
        let new_ordinal = latest_ordinal + 1;

        let update_result = self.cache.update(key.clone(), latest_known as i64, new_ordinal as i64).await;
        if let Err(_e) = update_result {
            println!("conflict!: latest persisted - {latest_ordinal}, latest_known by client - {latest_known}");
            return Err(WriteError::Conflict(latest_ordinal));
        }

        let result = sqlx::query(
            "INSERT INTO records (ordinal, key, value, timestamp) VALUES (?, ?, ?, ?)
             ON CONFLICT(ordinal) DO UPDATE SET key = excluded.key, value = excluded.value, timestamp = excluded.timestamp
             RETURNING ordinal",
        )
        .bind(new_ordinal as i64)
        .bind(&key)
        .bind(&value)
        .bind(now)
        .fetch_one(&self.pool)
        .await?;

        let written_ordinal = result.get("ordinal");

        if let Some(ref snapshot) = self.snapshot {
            if snapshot.should_snapshot(written_ordinal) {
                self.create_snapshot().await?;
            }
        }

        Ok(written_ordinal)
    }

    async fn create_snapshot(&self) -> Result<(), snapshot::Error> {
        if let Some(ref snapshot) = self.snapshot {
            let records = sqlx::query_as::<_, (String, Vec<u8>)>(
                "SELECT key, value FROM records WHERE key LIKE 'map:%'",
            )
            .fetch_all(&self.pool)
            .await
            .map_err(|e| snapshot::Error::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))?;

            snapshot.save_text(&records).await?;
            snapshot.save_binary(&records).await?;
        }
        Ok(())
    }

    pub fn subscribe_from(&self, ordinal: u64) -> Pin<Box<dyn Stream<Item = Record> + Send>> {
        let pool = self.pool.clone();
        Box::pin(async_stream::stream! {
            let mut conn = pool.acquire().await.unwrap();
            let mut ordinal = ordinal as i64;

            loop {
                let rows = sqlx::query_as::<_, (i64, String, Vec<u8>, i64)>(
                    "SELECT ordinal, key, value, timestamp FROM records WHERE ordinal > ? ORDER BY ordinal LIMIT 100"
                )
                .bind(ordinal)
                .fetch_all(&mut *conn)
                .await
                .unwrap();

                if rows.is_empty() {
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    continue;
                }

                for (ord, key, value, timestamp) in rows {
                    ordinal = ord;
                    yield Record {
                        ordinal: ord as u64,
                        key,
                        value,
                        timestamp,
                    };
                }
            }
        })
    }

    pub async fn get_latest_snapshot(&self) -> Result<Option<(u64, Vec<u8>)>, WriteError> {
        if let Some(ref snapshot) = self.snapshot {
            let (ordinal, data) = snapshot.get_latest_snapshot().await?;
            if let Some(data) = data {
                return Ok(Some((ordinal, data)));
            }
        }
        Ok(None)
    }
}

#[async_trait]
impl StorageBackend for SqliteStorage {
    async fn write(
        &self,
        ordinal: u64,
        key: String,
        value: Vec<u8>,
        latest_known: u64,
    ) -> Result<u64, WriteError> {
        self.write(ordinal, key, value, latest_known).await
    }

    fn subscribe_from(&self, ordinal: u64) -> Pin<Box<dyn Stream<Item = Record> + Send>> {
        self.subscribe_from(ordinal)
    }

    async fn get_latest_snapshot(&self) -> Result<Option<(u64, Vec<u8>)>, WriteError> {
        self.get_latest_snapshot().await
    }
}

#[derive(Debug)]
pub enum WriteError {
    Conflict(u64),
    Sql(sqlx::Error),
    Snapshot(snapshot::Error),
}

impl From<sqlx::Error> for WriteError {
    fn from(err: sqlx::Error) -> Self {
        WriteError::Sql(err)
    }
}

impl From<snapshot::Error> for WriteError {
    fn from(err: snapshot::Error) -> Self {
        WriteError::Snapshot(err)
    }
}

impl std::fmt::Display for WriteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WriteError::Conflict(ord) => write!(f, "Conflict: latest ordinal is {}", ord),
            WriteError::Sql(e) => write!(f, "Database error: {}", e),
            WriteError::Snapshot(e) => write!(f, "Snapshot error: {}", e),
        }
    }
}

impl std::error::Error for WriteError {}
