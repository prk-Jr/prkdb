# Getting Started with PrkDB

PrkDB v2 is a distributed key-value store built on Raft consensus. This guide will help you set up a local cluster and perform basic operations.

## Prerequisites

- **Rust 1.75+**
- **protoc** (Protocol Buffers compiler)
- **cargo** (Rust package manager)

## Installation

Clone the repository and build from source:

```bash
git clone https://github.com/prk-Jr/prkdb.git
cd prkdb
cargo build --release
```

## Running a Local Cluster

For development, you can start a 3-node local cluster using the provided script:

```bash
./scripts/start_cluster.sh
```

This will start:

- **Node 1** (Leader): http://127.0.0.1:8081
- **Node 2** (Follower): http://127.0.0.1:8082
- **Node 3** (Follower): http://127.0.0.1:8083

## Basic Operations (CLI)

Use the `prkdb-cli` tool to interact with the cluster.

### 1. Check Cluster Health

```bash
cargo run -p prkdb-cli -- health
```

Expected output:

```
Cluster Status: HEALTHY
Leader: Node 1 (127.0.0.1:8081)
Active Nodes: 3/3
```

### 2. Put & Get Data

```bash
# Write a value
cargo run -p prkdb-cli -- put my-key "Hello PrkDB"

# Read it back (Linearizable read)
cargo run -p prkdb-cli -- get my-key
```

### 3. Create a Collection

PrkDB organizes data into collections (tables).

```bash
cargo run -p prkdb-cli -- schema create users
```

## Client Usage (Rust)

Add `prkdb` to your `Cargo.toml`:

```toml
[dependencies]
prkdb = { git = "https://github.com/prk-Jr/prkdb" }
tokio = { version = "1.0", features = ["full"] }
```

### Example Code

```rust
use prkdb::client::PrkDbClient;
use prkdb_macros::Collection;
use serde::{Deserialize, Serialize};

#[derive(Collection, Serialize, Deserialize, Clone, Debug)]
pub struct User {
    #[key]
    pub id: String,
    pub name: String,
    pub age: u32,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to the cluster
    let client = PrkDbClient::new(vec![
        "http://127.0.0.1:8081".to_string(),
    ]).await?;

    // Create a new user
    let user = User {
        id: "1001".to_string(),
        name: "Alice".to_string(),
        age: 30,
    };

    // Put data using the typed client interface
    client.put(&user).await?;

    // Linearizable Get (guaranteed latest data)
    let fetched_user: User = client.get("User", "1001").await?.unwrap();
    println!("Fetched: {:?}", fetched_user);

    Ok(())
}
```

### 4. Cross-Language SDKs

PrkDB supports dynamic code generation for TypeScript, Python, and Go based on your Rust schema. See the [Cross-Language SDK](/guide/codegen) guide for details.

## Next Steps

- [Deployment Guide](./deployment.md) - Deploying to production
- [Replication Guide](./replication.md) - Understanding Multi-Raft
- [Metrics Reference](./metrics.md) - Monitoring your cluster

## Features (v2.0)

✅ **Multi-Raft Consensus** - Strong consistency & easy failover.
✅ **gRPC Transport** - High performance internal communication.
✅ **Linearizable Reads** - Read-your-writes guarantees.
✅ **Log Compaction** - Efficient storage management.
