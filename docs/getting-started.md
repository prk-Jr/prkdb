# Getting Started with PrkDB

## Quick Start (5 minutes)

### Installation

**Prerequisites**:
- Rust 1.70+
- Docker (for cluster deployment)

**Build from source**:
```bash
git clone https://github.com/yourusername/prkdb.git
cd prkdb
cargo build --release
```

### Running Your First Instance

**1. Start a single node**:
```bash
cargo run --bin prkdb-server --release
```

**2. Using the CLI**:
```bash
cargo run --bin prkdb -- --help
```

**3. Start a 3-node cluster**:
```bash
docker-compose up -d
```

### Basic Operations

**Using the Rust client**:
```rust
use prkdb::client::PrkDbClient;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Connect to cluster
    let client = PrkDbClient::new(vec![
        "http://127.0.0.1:8081".to_string(),
    ]).await?;
    
    // Write data
    client.put(b"my_key", b"my_value").await?;
    
    // Read data
    let value = client.get(b"my_key").await?;
    println!("Value: {:?}", value);
    
    Ok(())
}
```

## Next Steps

- [Architecture Overview](../ARCHITECTURE.md)
- [CLI Reference](../crates/prkdb-cli/README.md)
- [Performance Tuning](guides/performance.md)
- [Deployment Guide](guides/deployment.md)

## Features

✅ **4.2M writes/sec** - High-performance WAL  
✅ **Multi-Raft** - Distributed consensus  
✅ **Smart Client** - Automatic routing and failover  
✅ **Strong Consistency** - Linearizable reads  
✅ **Partitioning** - Horizontal scaling  

## Support

- Documentation: [docs/](.)
- Examples: [examples/](../crates/prkdb/examples/)
- Issues: GitHub Issues

## Performance

See [benchmarks](benchmarks/) for detailed performance data.

**Highlights**:
- WAL: 4.2M msgs/sec (local)
- Cluster writes: 16.5K ops/sec
- Read latency: 0.28ms avg
