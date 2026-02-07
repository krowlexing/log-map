# log-map

A distributed log server with gRPC API for subscribing to and writing records.

## Project Structure

```
log-server/
├── Cargo.toml        # Workspace configuration
└── server/           # Main crate
    ├── Cargo.toml
    ├── build.rs      # Proto compilation
    ├── proto/        # gRPC definitions
    ├── src/
    └── tests/
```

## Architecture

- **SQLite database** stores records with auto-assigned ordinals
- **gRPC server** exposes `Subscribe` and `Write` RPCs
- **Streaming** support for both operations

## Proto Definition

```protobuf
service KVServer {
    rpc Subscribe(SubscribeRequest) returns (stream Record);
    rpc Write(stream WriteRequest) returns (stream WriteResponse);
}
```

## Running

```bash
cargo run
```

Server listens on `[::1]:50051`.

## Testing

```bash
cargo test
```

## API

### Subscribe

Request with `start_ordinal` to receive all records from that point onward as a stream.

### Write

Stream write requests with:
- `ordinal`: proposed ordinal
- `key`: record key
- `value`: record value
- `latest_known`: client's latest seen ordinal (for conflict detection)

Returns `accepted: true` if written, `false` on conflict.
