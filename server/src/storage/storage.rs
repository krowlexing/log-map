use crate::models::Record;
use async_trait::async_trait;
use futures_util::stream::Stream;
use std::pin::Pin;

use super::WriteError;

#[async_trait]
pub trait StorageBackend: Send + Sync {
    async fn append(&self, key: String, value: Vec<u8>) -> Result<u64, sqlx::Error>;
    async fn write(
        &self,
        ordinal: u64,
        key: String,
        value: Vec<u8>,
        latest_known: u64,
    ) -> Result<u64, WriteError>;
    fn subscribe_from(&self, ordinal: u64) -> Pin<Box<dyn Stream<Item = Record> + Send>>;
    async fn get_latest_snapshot(&self) -> Result<Option<(u64, Vec<u8>)>, WriteError>;
}
