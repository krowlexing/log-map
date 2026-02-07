use crate::storage::{Storage, WriteError};
use futures_util::stream::{Stream, StreamExt};
use kv::{kv_server_server::{KvServer, KvServerServer}, Record, SubscribeRequest, WriteRequest, WriteResponse};
use std::pin::Pin;
use std::sync::Arc;
use tonic::{Request, Response, Status};

pub mod kv {
    tonic::include_proto!("kv");
}

#[derive(Clone)]
pub struct KvServiceImpl {
    storage: Arc<Storage>,
}

impl KvServiceImpl {
    pub fn new(storage: Arc<Storage>) -> Self {
        Self { storage }
    }
}

type SubscribeStream = Pin<Box<dyn Stream<Item = Result<Record, Status>> + Send>>;
type WriteStream = Pin<Box<dyn Stream<Item = Result<WriteResponse, Status>> + Send>>;

#[tonic::async_trait]
impl KvServer for KvServiceImpl {
    type SubscribeStream = SubscribeStream;
    type WriteStream = WriteStream;

    async fn subscribe(
        &self,
        request: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let req = request.into_inner();
        let stream = self.storage.subscribe_from(req.start_ordinal);

        let output = async_stream::stream! {
            let mut db_stream = stream;
            while let Some(record) = db_stream.next().await {
                let proto_record = Record {
                    ordinal: record.ordinal,
                    key: record.key,
                    value: record.value,
                    timestamp: record.timestamp,
                };
                yield Ok(proto_record);
            }
        };

        Ok(Response::new(Box::pin(output)))
    }

    async fn write(
        &self,
        request: Request<tonic::Streaming<WriteRequest>>,
    ) -> Result<Response<Self::WriteStream>, Status> {
        let mut stream = request.into_inner();

        let storage = self.storage.clone();
        let output = async_stream::stream! {
            while let Some(result) = stream.next().await {
                match result {
                    Ok(req) => {
                        match storage.write(req.ordinal, req.key, req.value, req.latest_known).await {
                            Ok(ordinal) => {
                                yield Ok(WriteResponse {
                                    accepted: true,
                                    error: String::new(),
                                    assigned_ordinal: ordinal,
                                });
                            }
                            Err(WriteError::Conflict(latest)) => {
                                yield Ok(WriteResponse {
                                    accepted: false,
                                    error: format!("Conflict: latest ordinal is {}", latest),
                                    assigned_ordinal: latest,
                                });
                            }
                            Err(WriteError::Sql(e)) => {
                                yield Ok(WriteResponse {
                                    accepted: false,
                                    error: format!("Database error: {}", e),
                                    assigned_ordinal: 0,
                                });
                            }
                        }
                    }
                    Err(e) => {
                        yield Err(Status::internal(format!("Stream error: {}", e)));
                        break;
                    }
                }
            }
        };

        Ok(Response::new(Box::pin(output)))
    }
}

pub fn create_server(storage: Arc<Storage>) -> KvServerServer<KvServiceImpl> {
    KvServerServer::new(KvServiceImpl::new(storage))
}
