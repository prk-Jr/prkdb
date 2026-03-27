# Getting Started with PrkDB

This guide uses the binaries and APIs that currently ship in this repository.

## Prerequisites

- Rust 1.75+
- `protoc`
- `cargo`

## Build

```bash
git clone https://github.com/prk-Jr/prkdb.git
cd prkdb
cargo build --release
```

## Start a Local Server

For a single-node development setup with the HTTP API, start `prkdb-cli serve` on port `8080` and its companion gRPC endpoint on `50051`:

```bash
cargo run -p prkdb-cli -- serve --host 127.0.0.1 --port 8080 --grpc-port 50051
```

For a production-style single-node server, run the canonical `prkdb-server` binary on one gRPC port:

```bash
NODE_ID=1 \
CLUSTER_NODES=1@127.0.0.1:8080 \
STORAGE_PATH=/tmp/prkdb-node1 \
cargo run -p prkdb --bin prkdb-server
```

You can also start the example 3-node Raft cluster with:

```bash
./scripts/start_cluster.sh
```

That script starts `prkdb-server` nodes on `127.0.0.1:8080`, `:8081`, and `:8082`.

## Basic Operations

### HTTP health

```bash
curl http://127.0.0.1:8080/health
```

### gRPC key-value operations

```bash
# Write a value
cargo run -p prkdb-cli -- put my-key "Hello PrkDB" --server http://127.0.0.1:8080

# Read it back
cargo run -p prkdb-cli -- get my-key --server http://127.0.0.1:8080

# Delete it
cargo run -p prkdb-cli -- delete my-key --server http://127.0.0.1:8080
```

### HTTP collection operations

```bash
# Insert a JSON document
curl -X PUT http://127.0.0.1:8080/collections/users/data \
  -H 'Content-Type: application/json' \
  -d '{"id":"1001","name":"Alice","age":30}'

# Fetch the document by id
curl http://127.0.0.1:8080/collections/users/data/1001

# Browse the collection
curl 'http://127.0.0.1:8080/collections/users/data?limit=20&offset=0'

# Count records
curl http://127.0.0.1:8080/collections/users/count
```

## Rust Client Example

Use `prkdb-client` for the remote smart client:

```toml
[dependencies]
prkdb-client = { git = "https://github.com/prk-Jr/prkdb" }
serde_json = "1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

```rust
use prkdb_client::{PrkDbClient, ReadConsistency};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = PrkDbClient::new(vec![
        "http://127.0.0.1:8080".to_string(),
    ]).await?;

    let payload = serde_json::json!({
        "id": "1001",
        "name": "Alice",
        "age": 30
    });

    client
        .put(b"users:1001", payload.to_string().as_bytes())
        .await?;

    let value = client
        .get_with_consistency(b"users:1001", ReadConsistency::Linearizable)
        .await?
        .expect("record should exist");

    println!("{}", String::from_utf8(value)?);
    Ok(())
}
```

## Schema Registry

Schema registration and schema listing are admin operations. Set `PRKDB_ADMIN_TOKEN` before using.

Use `http://127.0.0.1:8080` when talking to `prkdb-server`, or `http://127.0.0.1:50051` when talking to the gRPC endpoint that `prkdb-cli serve` exposes locally.

```bash
export PRKDB_ADMIN_TOKEN=change-me
prkdb schema register --server http://127.0.0.1:8080 --collection users --proto ./schemas/users.binpb
prkdb schema list --server http://127.0.0.1:8080
```

## Next Steps

- [Deployment Guide](./deployment.md)
- [Replication Guide](./replication.md)
- [Schema Registry](./schema-registry.md)
- [Cross-Language SDK](./codegen.md)
- [Metrics Reference](./metrics.md)
