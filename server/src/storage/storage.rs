use crate::models::Record;
use futures_util::stream::Stream;
use std::pin::Pin;

use super::WriteError;

pub trait StorageBackend: Send + Sync {
    fn append(&self, key: String, value: Vec<u8>) -> impl std::future::Future<Output = Result<u64, sqlx::Error>> + Send;
    fn write(
        &self,
        ordinal: u64,
        key: String,
        value: Vec<u8>,
        latest_known: u64,
    ) -> impl std::future::Future<Output = Result<u64, WriteError>> + Send;
    fn subscribe_from(&self, ordinal: u64) -> Pin<Box<dyn Stream<Item = Record> + Send>>;
    fn get_latest_snapshot(&self) -> impl std::future::Future<Output = Result<Option<(u64, Vec<u8>)>, WriteError>> + Send;
}
