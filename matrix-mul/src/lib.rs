//! Distributed matrix multiplication using log-map.
//!
//! `matrix-mul` provides distributed matrix multiplication where multiple
//! clients collaborate to compute C = A × B. Matrices are stored in the
//! log-map, and computation is triggered by writing "start" to key 0.
//!
//! # Storage Layout
//!
//! - **Matrix A rows**: keys -1, -2, -3, ... (row i at key -(i+1))
//! - **Matrix B rows**: keys -(m+1), -(m+2), ... (row j at key -(m+j+1))
//! - **Start signal**: key 0 (write "start" to begin computation)
//! - **Results**: keys 1, 2, 3, ... (element C[i][j] at key i*p+j+1)
//!
//! # Example
//!
//! ```no_run
//! use matrix_mul::MatrixMul;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let mut mm = MatrixMul::connect("localhost:50051").await?;
//!
//!     let a = vec![vec![1.0, 2.0], vec![3.0, 4.0]];
//!     let b = vec![vec![5.0, 6.0], vec![7.0, 8.0]];
//!
//!     mm.load_matrices(a, b).await?;
//!     mm.start().await?;
//!     mm.wait_for_completion(2, 2).await?;
//!
//!     let result = mm.get_result(2, 2).await?;
//!     assert_eq!(result, vec![vec![19.0, 22.0], vec![43.0, 50.0]]);
//!
//!     Ok(())
//! }
//! ```

mod error;
mod matrix_mul;

pub use error::Error;
pub use matrix_mul::MatrixMul;
