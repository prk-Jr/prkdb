# PrkDB Replication Guide

This guide explains how to set up and use leader-follower replication in PrkDB.

## Overview

PrkDB supports **leader-follower replication** for high availability and read scalability:

- **Leader** - Accepts writes, broadcasts changes to followers
- **Followers** - Sync from leader, serve read requests
- **Automatic failover** - (Future: automatic leader election)

## Architecture

```
┌─────────────┐
│   Leader    │ ← Writes
│  (Node 1)   │
└──────┬──────┘
       │ Replication
       ├────────────┐
       ▼            ▼
┌──────────┐  ┌──────────┐
│Follower 1│  │Follower 2│ ← Reads
└──────────┘  └──────────┘
```

### How It Works

1. **Leader** receives writes and adds them to the outbox
2. **Followers** poll the leader for new changes (1 second interval)
3. **Changes** are applied to follower storage
4. **Offset tracking** ensures no changes are missed
5. **Metrics** track lag, throughput, and health

## Quick Start

### 1. Create Leader Node

```rust
use prkdb::replication::{ReplicaNode, ReplicationConfig, ReplicationManager};
use std::sync::Arc;

let leader_db = Arc::new(
    PrkDb::builder()
        .with_storage(InMemoryAdapter::new())
        .register_collection::<Order>()
        .build()?
);

let config = ReplicationConfig {
    self_node: ReplicaNode {
        id: "leader-1".to_string(),
        address: "localhost:8080".to_string(),
    },
    leader_address: None,  // None = this is the leader
    followers: vec![
        ReplicaNode {
            id: "follower-1".to_string(),
            address: "localhost:8081".to_string(),
        },
    ],
    timing: ReplicationTiming::default(),
};

let manager = Arc::new(ReplicationManager::new(leader_db.clone(), config));
manager.clone().start().await?;
```

### 2. Create Follower Node

```rust
let follower_db = Arc::new(
    PrkDb::builder()
        .with_storage(InMemoryAdapter::new())
        .register_collection::<Order>()
        .build()?
);

let config = ReplicationConfig {
    self_node: ReplicaNode {
        id: "follower-1".to_string(),
        address: "localhost:8081".to_string(),
    },
    leader_address: Some("localhost:8080".to_string()),  // Connect to leader
    followers: vec![],
    timing: ReplicationTiming::default(),
};

let manager = Arc::new(ReplicationManager::new(follower_db.clone(), config));
manager.clone().start().await?;
```

### 3. Write to Leader

```rust
// Writes go to the leader
let orders = leader_db.collection::<Order>();
orders.put(order).await?;

// Changes are automatically replicated to followers
```

## Monitoring & Health Checks

### Health Status

Check replication health:

```rust
let health = manager.get_health().await;

println!("Healthy: {}", health.healthy);
println!("Lag: {:.2}s", health.lag_seconds);
println!("Last sync: {}", health.last_sync);

if let Some(error) = health.error {
    println!("Error: {}", error);
}
```

### Replication State

Get detailed state:

```rust
let state = manager.get_state().await;

println!("Changes applied: {}", state.changes_applied);
println!("Last change ID: {:?}", state.last_change_id);
println!("Connected: {}", state.connected);
println!("Error count: {}", state.error_count);
```

## Prometheus Metrics

### Available Metrics

#### Follower Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `prkdb_replication_lag_seconds` | Gauge | Replication lag in seconds |
| `prkdb_replication_changes_applied_total` | Counter | Total changes applied |
| `prkdb_replication_follower_connected` | Gauge | Connection status (1=connected) |
| `prkdb_replication_sync_errors_total` | Counter | Total sync errors |
| `prkdb_replication_last_sync_timestamp_seconds` | Gauge | Last successful sync |
| `prkdb_replication_sync_duration_seconds` | Histogram | Sync operation duration |
| `prkdb_replication_batch_size` | Histogram | Changes per sync batch |

#### Leader Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `prkdb_replication_changes_pending` | Gauge | Changes pending replication |
| `prkdb_replication_active_followers` | Gauge | Number of active followers |

### Example Queries

```promql
# Replication lag by follower
prkdb_replication_lag_seconds{node_id="follower-1"}

# Changes applied per second
rate(prkdb_replication_changes_applied_total[1m])

# Sync error rate
rate(prkdb_replication_sync_errors_total[5m])

# Average sync duration
rate(prkdb_replication_sync_duration_seconds_sum[1m]) / 
rate(prkdb_replication_sync_duration_seconds_count[1m])

# 95th percentile batch size
histogram_quantile(0.95, prkdb_replication_batch_size_bucket)
```

### Grafana Dashboard

Create a dashboard with:

1. **Replication Lag** - Line chart of lag over time
2. **Throughput** - Changes per second
3. **Connection Status** - Binary status indicator
4. **Error Rate** - Errors per minute
5. **Batch Size Distribution** - Histogram

## Retry Logic & Error Handling

### Exponential Backoff

Followers use exponential backoff on errors:

- **Initial retry**: 2 seconds
- **Subsequent retries**: 2^n seconds (4s, 8s, 16s, 32s, 60s)
- **Max delay**: 60 seconds
- **Reset**: On successful sync

### Error Types

| Error Type | Description | Action |
|------------|-------------|--------|
| `sync_failed` | Failed to fetch changes | Retry with backoff |
| `apply_failed` | Failed to apply change | Retry with backoff |
| `connection_error` | Network issue | Retry with backoff |

### State Tracking

```rust
pub struct ReplicationState {
    pub last_change_id: Option<String>,
    pub changes_applied: u64,
    pub last_sync_ts: i64,
    pub connected: bool,
    pub error_count: u64,
    pub last_error: Option<String>,
}
```

## Configuration

### ReplicationConfig

```rust
pub struct ReplicationConfig {
    /// This node's identity
    pub self_node: ReplicaNode,
    
    /// Leader address (None if this is the leader)
    pub leader_address: Option<String>,
    
    /// List of follower nodes (leader only)
    pub followers: Vec<ReplicaNode>,

    /// Timing/backoff configuration
    pub timing: ReplicationTiming,
}
```

### ReplicationTiming

```rust
pub struct ReplicationTiming {
    pub follower_poll_interval: Duration,
    pub backoff_base: Duration,
    pub backoff_max: Duration,
    pub request_timeout: Duration,
}
```

- **Sync interval**: default 1s (`follower_poll_interval`)
- **Backoff**: exponential, seeded by `backoff_base`, capped by `backoff_max`
- **Request timeout**: HTTP fetch timeout to the leader
- **Batch size**: limit stays at 100 per request today

## Production Deployment

### Best Practices

1. **Monitor lag & drift** - Health exposes `lag_seconds`; `ReplicationHealth` also includes `outbox_drift` (saved - drained) to watch backlog.
2. **Track errors** - Alert on consecutive failures.
3. **Use persistent storage** - Not InMemoryAdapter.
4. **Enable metrics** - Always run metrics server.
5. **Log aggregation** - Collect logs from all nodes

### Example Production Setup

```rust
// Leader
let leader = PrkDb::builder()
    .with_storage(SledAdapter::new("/data/leader"))
    .register_collection::<Order>()
    .build()?;

// Follower with monitoring
let follower = PrkDb::builder()
    .with_storage(SledAdapter::new("/data/follower"))
    .register_collection::<Order>()
    .build()?;

// Start metrics server
let metrics = MetricsServer::new("0.0.0.0:9090".parse()?);
tokio::spawn(async move { metrics.start().await });

// Health check loop
tokio::spawn(async move {
    loop {
        let health = manager.get_health().await;
        if !health.healthy {
            error!("Replication unhealthy: {:?}", health.error);
        }
        sleep(Duration::from_secs(30)).await;
    }
});
```

### Alerting Rules

```yaml
groups:
  - name: replication
    rules:
      - alert: ReplicationLagHigh
        expr: prkdb_replication_lag_seconds > 10
        for: 5m
        annotations:
          summary: "Replication lag is high"
          
      - alert: ReplicationDisconnected
        expr: prkdb_replication_follower_connected == 0
        for: 2m
        annotations:
          summary: "Follower disconnected from leader"
          
      - alert: ReplicationErrorRate
        expr: rate(prkdb_replication_sync_errors_total[5m]) > 0.1
        for: 5m
        annotations:
          summary: "High replication error rate"
```

## Limitations (Current Implementation)

1. **Pull-based** - Followers poll the leader (not push)
2. **No HTTP/gRPC** - Change fetching is stubbed
3. **No type registry** - Change application is placeholder
4. **No automatic failover** - Manual leader election required
5. **Single leader** - No multi-leader support

## Roadmap

### Phase 1: Network Transport (Next)
- [ ] HTTP/gRPC endpoints for change streaming
- [ ] Authentication and TLS
- [ ] Compression for large batches

### Phase 2: Advanced Features
- [ ] Type registry for dynamic collection handling
- [ ] Conflict resolution strategies
- [ ] Read-your-writes consistency
- [ ] Multi-region replication

### Phase 3: High Availability
- [ ] Automatic leader election (Raft/Paxos)
- [ ] Quorum-based writes
- [ ] Split-brain detection
- [ ] Automatic failover

## Troubleshooting

### Follower not syncing

1. Check leader is running: `manager.is_leader()`
2. Check follower state: `manager.get_state().await`
3. Check logs for connection errors
4. Verify network connectivity

### High replication lag

1. Check metrics: `prkdb_replication_lag_seconds`
2. Reduce sync interval (requires code change)
3. Increase network bandwidth
4. Optimize storage adapter performance

### Changes not applying

1. Verify collection is registered on follower
2. Check for deserialization errors in logs
3. Ensure storage adapter is working
4. Check `prkdb_replication_sync_errors_total` metric

### Connection issues

1. Check `prkdb_replication_follower_connected` metric
2. Verify leader address is correct
3. Check network firewall rules
4. Review error logs for details

## Example

See `examples/replication.rs` for a complete working example:

```bash
cargo run -p prkdb --example replication
```

This example:
- Sets up 1 leader and 2 followers
- Writes 10 orders to the leader
- Shows replication state and health
- Exposes metrics on port 9091
- Demonstrates monitoring

## See Also

- [Metrics Guide](METRICS.md)
- [Partitioning Guide](PARTITIONING.md)
- [Examples](../examples/)
