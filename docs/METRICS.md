# PrkDB Metrics Guide

This guide explains how to use the `prkdb-metrics` crate to monitor your PrkDB deployment with Prometheus.

## Overview

The `prkdb-metrics` crate provides:
- **Prometheus metrics export** - Standard metrics in Prometheus format
- **HTTP metrics server** - Built-in `/metrics` endpoint
- **Consumer lag tracking** - Monitor consumer group lag across partitions
- **Partition-level metrics** - Track records and size per partition

## Quick Start

### 1. Add Dependency

```toml
[dependencies]
prkdb = "0.1"
prkdb-metrics = "0.1"
```

### 2. Start Metrics Server
```bash
cargo run -p prkdb --example metrics_server
```

```rust
use prkdb_metrics::MetricsServer;
use std::net::SocketAddr;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr: SocketAddr = "127.0.0.1:9090".parse()?;
    let server = MetricsServer::new(addr);
    
    tokio::spawn(async move {
        server.start().await.unwrap();
    });
    
    // Your application code...
    
    Ok(())
}
```

### 3. Access Metrics
- Metrics endpoint: http://localhost:9090/metrics
- Health check: http://localhost:9090/health

## Available Metrics

### Event Metrics
| Metric | Type | Labels | Description |
|---|---|---|---|
| `prkdb_events_produced_total` | Counter | `collection` | Total events produced |
| `prkdb_events_consumed_total` | Counter | `collection`, `consumer_group` | Total events consumed |

### Partition Metrics
| Metric | Type | Labels | Description |
|---|---|---|---|
| `prkdb_partition_records` | Gauge | `collection`, `partition` | Number of records in partition |
| `prkdb_partition_size_bytes` | Gauge | `collection`, `partition` | Size of partition in bytes |

### Consumer Metrics
| Metric | Type | Labels | Description |
|---|---|---|---|
| `prkdb_consumer_lag` | Gauge | `consumer_group`, `collection`, `partition` | Consumer lag (messages behind) |
| `prkdb_consumer_offset` | Gauge | `consumer_group`, `collection`, `partition` | Current consumer offset |
| `prkdb_consumer_group_members` | Gauge | `consumer_group` | Number of active consumers in group |

### Operation Metrics
| Metric | Type | Labels | Description |
|---|---|---|---|
| `prkdb_operation_duration_seconds` | Histogram | `operation`, `collection` | Operation latency distribution |
| `prkdb_storage_operations_total` | Counter | `operation` | Total storage operations (put/get/delete/scan) |

## Consumer Lag Tracking
```rust 
use prkdb_metrics::ConsumerLagTracker;
use std::sync::Arc;

let lag_tracker = Arc::new(ConsumerLagTracker::new());

// Update latest offset when producing
lag_tracker.update_latest_offset("orders", partition_id, offset);

// Update consumer offset when consuming
lag_tracker.update_consumer_offset(
    "my-consumer-group",
    "orders",
    partition_id,
    consumer_offset
);

// Get lag for a consumer group
let lags = lag_tracker.get_group_lags("my-consumer-group");
for (collection, partition, lag) in lags {
    println!("{}/partition-{}: {} behind", collection, partition, lag);
}
```

## Recording Custom Metrics
### Event Production
```rust 
use prkdb_metrics::exporter::EVENTS_PRODUCED;

// Increment when producing events
EVENTS_PRODUCED
    .with_label_values(&["Order"])
    .inc();
```

### Event Consumption
```rust
use prkdb_metrics::exporter::EVENTS_CONSUMED;

// Increment when consuming events
EVENTS_CONSUMED
    .with_label_values(&["Order", "order-processor"])
    .inc();
```

### Operation Latency
```rust
use prkdb_metrics::exporter::OPERATION_LATENCY;
use std::time::Instant;

let start = Instant::now();
// ... perform operation ...
let duration = start.elapsed();

OPERATION_LATENCY
    .with_label_values(&["put", "Order"])
    .observe(duration.as_secs_f64());
```

### Prometheus Configuration
Add this to your prometheus.yml:

```yaml
scrape_configs:
  - job_name: 'prkdb'
    static_configs:
      - targets: ['localhost:9090']
    scrape_interval: 15s
```

## Grafana Queries

- Events produced rate:
  - `rate(prkdb_events_produced_total[5m])`
- Consumer lag by group:
  - `prkdb_consumer_lag{consumer_group="order-processor"}`
- p99 operation latency:
  - `histogram_quantile(0.99, rate(prkdb_operation_duration_seconds_bucket[5m]))`
- Partition distribution for a collection:
  - `prkdb_partition_records{collection="Order"}`

## Alerting Rules (Prometheus)

```yaml
groups:
  - name: prkdb
    rules:
      - alert: HighConsumerLag
        expr: prkdb_consumer_lag > 1000
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High consumer lag detected"
          description: "Consumer {{ $labels.consumer_group }} is {{ $value }} messages behind"

      - alert: NoConsumersInGroup
        expr: prkdb_consumer_group_members == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "No active consumers"
          description: "Consumer group {{ $labels.consumer_group }} has no active members"
```
