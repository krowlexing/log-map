# log-map

A distributed key-value map backed by the log-server.

## Overview

`log-map` provides a `Map<i64, String>` implementation that stores all data through the log-server's gRPC API. It uses optimistic concurrency control with automatic retry on conflicts.

## Features

- Distributed key-value storage with automatic sync
- Optimistic concurrency control with exponential backoff
- Background subscription to keep local cache updated
- Key prefix isolation (`map:`) to avoid collisions

## Usage

```rust
use log_map::LogMap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let map = LogMap::connect("localhost:50051").await?;

    map.insert(1, "hello".to_string()).await?;
    let value = map.get(1).await?;

    map.remove(1).await?;
    Ok(())
}
```

## Conflict Resolution

When a write is rejected by the server, `LogMap` automatically:

1. Syncs the latest state from the server
2. Retries the write with updated `latest_known` ordinal
3. Uses exponential backoff (100ms starting, doubles each retry)
4. Gives up after 5 retries

## Key Encoding

Keys are encoded as `map:{i64}` in the log to avoid collisions with other data using the same log-server.
