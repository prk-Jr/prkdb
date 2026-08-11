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

PrkDB refuses to serve without authorization. You must either create an admin
credential or opt out explicitly — starting with neither is an error, not a default:

```
No principals are configured. Set PRKDB_BOOTSTRAP_TOKEN to create an admin
principal, or pass --allow-anonymous to serve without authorization
(development only).
```

That is deliberate. Every `/collections` route reads and writes your data, so a server
that starts unauthenticated by default is one that ships that way by accident.

### Development: no authorization

```bash
cargo run -p prkdb-cli -- serve --host 127.0.0.1 --port 8080 --grpc-port 50051 \
  --allow-anonymous
```

`--allow-anonymous` makes every collection readable and writable by anyone who can reach
the port, and the server says so loudly at startup. Use it on a loopback interface, never
on a routable one.

### With a credential

```bash
PRKDB_BOOTSTRAP_TOKEN=choose-a-strong-secret \
  cargo run -p prkdb-cli -- serve --host 127.0.0.1 --port 8080 --grpc-port 50051
```

That mints a single admin principal on first start. It is ignored once any principal
exists, so a restart cannot quietly create a second way in. Pass the same value as
`--credential` to the CLI, or `Authorization: Bearer <token>` over HTTP.

See [Authorization](./security.md) for principals, roles, and per-collection grants.

For a production-style single-node server, run the canonical `prkdb-server` binary:

```bash
NODE_ID=1 \
CLUSTER_NODES=1@127.0.0.1:8080 \
STORAGE_PATH=/tmp/prkdb-node1 \
PRKDB_BOOTSTRAP_TOKEN=choose-a-strong-secret \
cargo run -p prkdb --bin prkdb-server
```

`prkdb-server` applies the same rule, with `PRKDB_ALLOW_ANONYMOUS=1` as its opt-out.

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
# `prkdb-cli serve` exposes gRPC on --grpc-port (50051 by default)
#
# Against a server started with a bootstrap token, pass the credential. Without it the
# call fails while fetching cluster metadata, which needs Read like any other request:
#     PRKDB_CREDENTIAL=choose-a-strong-secret

# Write a value
cargo run -p prkdb-cli -- put my-key "Hello PrkDB" --server http://127.0.0.1:50051

# Read it back
cargo run -p prkdb-cli -- get my-key --server http://127.0.0.1:50051

# Delete it
cargo run -p prkdb-cli -- delete my-key --server http://127.0.0.1:50051
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
        "http://127.0.0.1:50051".to_string(),
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

Schema registration and listing require `Admin`. Set `PRKDB_CREDENTIAL` (or the deprecated
`PRKDB_ADMIN_TOKEN`) before using — see [Security & Operations](./security.md).

Use `http://127.0.0.1:8080` when talking to `prkdb-server`, or `http://127.0.0.1:50051` when talking to the gRPC endpoint that `prkdb-cli serve` exposes locally.

```bash
export PRKDB_ADMIN_TOKEN=change-me
prkdb schema register --server http://127.0.0.1:50051 --collection users --proto ./schemas/users.binpb
prkdb schema list --server http://127.0.0.1:50051
```

## Next Steps

- [Deployment Guide](./deployment.md)
- [Replication Guide](./replication.md)
- [Schema Registry](./schema-registry.md)
- [Cross-Language SDK](./codegen.md)
- [Metrics Reference](./metrics.md)
