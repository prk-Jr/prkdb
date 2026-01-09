# prkdb-metrics

Prometheus metrics export and HTTP server for PrkDB.

## Features

- 📊 Prometheus metrics with rich labels
- 🌐 HTTP server with `/metrics` and `/health`
- 📈 Consumer lag tracking across partitions
- 🎯 Partition-level metrics (records, size)

## Installation

```toml
[dependencies]
prkdb-metrics = { path = "../../prkdb-metrics" }
```

If consuming from crates.io, use the published version instead of the path.

## Quick Start

```rust
use prkdb_metrics::{MetricsServer, ConsumerLagTracker};
use std::net::SocketAddr;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Start metrics server
    let addr: SocketAddr = "127.0.0.1:9090".parse()?;
    let server = MetricsServer::new(addr);

    tokio::spawn(async move {
        server.start().await.unwrap();
    });

    // Track consumer lag
    let lag = ConsumerLagTracker::new();
    lag.update_latest_offset("orders", 0, 100);
    lag.update_consumer_offset("group1", "orders", 0, 70);

    println!("Lag: {:?}", lag.calculate_lag("group1", "orders", 0));
    Ok(())
}
```

## Endpoints

- GET `http://localhost:9090/metrics` — Prometheus metrics in text format
- GET `http://localhost:9090/health` — Health check

## Exported Metrics

- `prkdb_events_produced_total{collection}`
- `prkdb_events_consumed_total{collection,consumer_group}`
- `prkdb_partition_records{collection,partition}`
- `prkdb_partition_size_bytes{collection,partition}`
- `prkdb_consumer_lag{consumer_group,collection,partition}`
- `prkdb_consumer_offset{consumer_group,collection,partition}`
- `prkdb_consumer_group_members{consumer_group}`
- `prkdb_operation_duration_seconds_bucket|count|sum{operation,collection}`
- `prkdb_storage_operations_total{operation}`

See the full guide for examples and queries.

## Example

Run the full example from the main repo:

```bash
cargo run -p prkdb --example metrics_server
```

Then open http://localhost:9090/metrics

## Documentation

See the comprehensive guide at `docs/METRICS.md` in the root of the repository.

## License

Apache-2.0 (same as PrkDB)
