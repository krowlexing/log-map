//! A distributed key-value map backed by the log-server.
//!
//! `log-map` provides a `Map<i64, String>` implementation that stores all data
//! through the log-server's gRPC API. It uses optimistic concurrency control
//! with automatic retry on conflicts.
//!
//! # Features
//!
//! - Distributed key-value storage with automatic sync
//! - Optimistic concurrency control with exponential backoff
//! - Background subscription to keep local cache updated
//! - Key prefix isolation (`map:`) to avoid collisions
//!
//! # Example
//!
//! ```no_run
//! use log_map::LogMap;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let map = LogMap::connect("localhost:50051").await?;
//!
//!     map.insert(1, "hello".to_string()).await?;
//!     let value = map.get(1).await?;
//!
//!     map.remove(1).await?;
//!     Ok(())
//! }
//! ```

mod cache;
mod error;
mod map;
mod sync;

pub use error::Error;
pub use map::{LogMap, ServerAddr};
