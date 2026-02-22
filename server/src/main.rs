use std::sync::Arc;
use tonic::transport::Server;

use log_server::{grpc, storage};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let storage = Arc::new(storage::MemoryStorage::new());
    let server = grpc::create_server(storage);

    let addr = "127.0.0.1:50051".parse()?;
    Server::builder().add_service(server).serve(addr).await?;

    Ok(())
}
