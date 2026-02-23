use std::error::Error;

use crate::snapshot;

mod cache;
mod memory;
#[cfg(feature = "sqlite")]
mod sqlite;
mod storage;

pub use cache::MapCache;
pub use memory::MemoryStorage;
#[cfg(feature = "sqlite")]
pub use sqlite::SqliteStorage;
pub use storage::StorageBackend;

#[derive(Debug)]
pub enum WriteError {
    Conflict(u64),
    Other(Box<dyn Error + Send>),
    Snapshot(snapshot::Error),
}

#[cfg(feature = "sqlite")]
impl From<sqlx::Error> for WriteError {
    fn from(err: sqlx::Error) -> Self {
        WriteError::Other(Box::new(err))
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
            WriteError::Other(e) => write!(f, "Database error: {}", e),
            WriteError::Snapshot(e) => write!(f, "Snapshot error: {}", e),
        }
    }
}

impl std::error::Error for WriteError {}
