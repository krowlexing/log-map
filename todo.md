benchmark how different transport protocols influence performance

toggleble transport backends 
    - http + plain json
    - http + bson
    - grpc
    - protobuf
    - custom tcp protocol

toggleable map implementations 
    - distributed log based (default) 
    - single-pc multithreaded
    - single-pc multithreaded without persistent log

database backend: 
    - in-memory
    - sqlite
    - postgres
    - clickhouse

snapshot backend:
    - filesystem
    - minio (s3)
    - garage (s3)

