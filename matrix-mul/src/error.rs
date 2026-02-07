//! Error types for matrix-mul operations.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("log-map error: {0}")]
    LogMap(#[from] log_map::Error),

    #[error("matrix dimension mismatch: A is {0}x{1}, B is {2}x{3}")]
    DimensionMismatch(usize, usize, usize, usize),

    #[error("parse error: {0}")]
    Parse(#[from] std::num::ParseFloatError),

    #[error("missing matrix data at key {0}")]
    MissingMatrixData(i64),

    #[error("timeout waiting for completion")]
    Timeout,
}
