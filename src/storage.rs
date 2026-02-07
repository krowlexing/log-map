use crate::models::Record;
use futures_util::stream::Stream;
use sqlx::{Row, SqlitePool};
use std::pin::Pin;

pub struct Storage {
    pool: SqlitePool,
}

impl Storage {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
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
        ordinal: u64,
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

        if latest_known < latest_ordinal {
            return Err(WriteError::Conflict(latest_ordinal));
        }

        let result = sqlx::query(
            "INSERT INTO records (ordinal, key, value, timestamp) VALUES (?, ?, ?, ?)
             ON CONFLICT(ordinal) DO UPDATE SET key = excluded.key, value = excluded.value, timestamp = excluded.timestamp
             RETURNING ordinal",
        )
        .bind(ordinal as i64)
        .bind(&key)
        .bind(&value)
        .bind(now)
        .fetch_one(&self.pool)
        .await?;

        Ok(result.get("ordinal"))
    }

    pub fn subscribe_from(
        &self,
        ordinal: u64,
    ) -> Pin<Box<dyn Stream<Item = Record> + Send>> {
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
}

#[derive(Debug)]
pub enum WriteError {
    Conflict(u64),
    Sql(sqlx::Error),
}

impl From<sqlx::Error> for WriteError {
    fn from(err: sqlx::Error) -> Self {
        WriteError::Sql(err)
    }
}

impl std::fmt::Display for WriteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WriteError::Conflict(ord) => write!(f, "Conflict: latest ordinal is {}", ord),
            WriteError::Sql(e) => write!(f, "Database error: {}", e),
        }
    }
}

impl std::error::Error for WriteError {}
