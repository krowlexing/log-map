# log-map

A distributed log server with gRPC API for subscribing to and writing records.

## Project Structure

```
log-server/
├── types/                  # Proto definitions crate
├── server/                 # Server implementation
├── log-map/                # Rust KV map client
├── log-map-ffi/            # C FFI bindings
├── include/                # C++ headers
├── sync/                   # C++ templet framework + sample application
└── snapshots/              # Database snapshots
```

## Dependencies

- Rust toolchain
- Protobuf compiler
- GCC compiler


## Running

Build project for C bindings

```
cargo build --release
```

Start log-map server. this will create log.db sqlite database at the root of the project.

```bash
cargo run --release -p log-server
```

Compile client using compiled map library

```bash
cd ./sync && ./build.sh
```

Run client

```bash
./run.sh
```

## Architecture

- **types crate** contains generated gRPC message types and service definitions
- **server crate** implements the gRPC service with SQLite storage
- **SQLite database** stores records with auto-assigned ordinals
- **Streaming** support for both Subscribe and Write RPCs

## Proto Definition

```protobuf
service KVServer {
    rpc Subscribe(SubscribeRequest) returns (stream Record);
    rpc Write(stream WriteRequest) returns (stream WriteResponse);
}
```

