//! Error types for log-map operations.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("gRPC transport error: {0}")]
    Transport(#[from] tonic::transport::Error),

    #[error("gRPC status error: {0}")]
    Status(#[from] tonic::Status),

    #[error("write conflict after {0} retries")]
    Conflict(usize),

    #[error("connection closed")]
    ConnectionClosed,
}
