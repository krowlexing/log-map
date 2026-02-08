use std::sync::Arc;
use tonic::transport::Server;

use log_server::{db, grpc, storage};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let pool = db::init_pool("sqlite::memory:").await?;
    let storage = Arc::new(storage::Storage::new(pool));
    let server = grpc::create_server(storage);

    let addr = "127.0.0.1:50051".parse()?;
    Server::builder().add_service(server).serve(addr).await?;

    Ok(())
}
