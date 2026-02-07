use futures_util::StreamExt;
use kv::kv_server_client::KvServerClient;
use kv::{SubscribeRequest, WriteRequest};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::time::sleep;

pub mod kv {
    tonic::include_proto!("kv");
}

async fn start_test_server() -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("[::1]:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let pool = log_server::db::init_pool("sqlite::memory:").await.unwrap();
    let storage = Arc::new(log_server::storage::Storage::new(pool));
    let server = log_server::grpc::create_server(storage);

    let handle = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(server)
            .serve_with_incoming(
                tokio_stream::wrappers::TcpListenerStream::new(listener).map(|r| r.map_err(|e| {
                    println!("Error accepting connection: {}", e);
                    std::io::Error::new(std::io::ErrorKind::Other, e)
                })),
            )
            .await
            .unwrap();
    });

    sleep(Duration::from_millis(100)).await;
    (addr, handle)
}

#[tokio::test]
async fn test_subscribe() {
    let (addr, _handle) = start_test_server().await;
    let url = format!("http://{}", addr);

    let mut client = KvServerClient::connect(url).await.unwrap();

    let response = client
        .subscribe(SubscribeRequest { start_ordinal: 0 })
        .await;

    assert!(response.is_ok());
}

#[tokio::test]
async fn test_write() {
    let (addr, _handle) = start_test_server().await;
    let url = format!("http://{}", addr);

    let mut client = KvServerClient::connect(url).await.unwrap();

    let request = WriteRequest {
        ordinal: 1,
        key: "test_key".to_string(),
        value: b"test_value".to_vec(),
        latest_known: 0,
    };

    let mut stream = client
        .write(tokio_stream::once(request))
        .await
        .unwrap()
        .into_inner();

    if let Some(response) = stream.next().await {
        let resp = response.unwrap();
        assert!(resp.accepted);
    }
}
