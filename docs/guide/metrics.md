# Metrics Reference

PrkDB exposes Prometheus metrics at `/metrics` (default port 8081).

## Core Metrics

| Metric                 | type  | Description          |
| ---------------------- | ----- | -------------------- |
| `prkdb_uptime_seconds` | Gauge | Process uptime       |
| `prkdb_memory_bytes`   | Gauge | Current memory usage |

## Raft Consensus (Crucial)

Monitor these to ensure cluster health. high churn in `current_term` means instability.

| Metric                      | Type  | Description                                      |
| --------------------------- | ----- | ------------------------------------------------ |
| `prkdb_raft_current_term`   | Gauge | Current election term. Monotonically increasing. |
| `prkdb_raft_commit_index`   | Gauge | Index of the last committed log entry.           |
| `prkdb_raft_last_log_index` | Gauge | Index of the last entry in the log.              |
| `prkdb_raft_state`          | Gauge | 0=Follower, 1=Candidate, 2=Leader                |
| `prkdb_raft_voted_for`      | Gauge | Node ID this node voted for in current term.     |

### Alert Rules

**1. Leader Flapping**
Alert if term changes frequently (e.g. > 5 times in 5 minutes).

```yaml
rate(prkdb_raft_current_term[5m]) > 0.016
```

**2. Replication Lag**
Alert if `last_log_index` is far ahead of `commit_index` on Leader, or `commit_index` is behind Leader's.

```yaml
(prkdb_raft_last_log_index - prkdb_raft_commit_index) > 1000
```

## gRPC Transport

| Metric                       | Type      | Description                       |
| ---------------------------- | --------- | --------------------------------- |
| `prkdb_grpc_requests_total`  | Counter   | Total RPC requests served         |
| `prkdb_grpc_errors_total`    | Counter   | Total failed RPC requests         |
| `prkdb_grpc_latency_seconds` | Histogram | Latency distribution of RPC calls |

## Storage (WAL)

| Metric                             | Type      | Description                                      |
| ---------------------------------- | --------- | ------------------------------------------------ |
| `prkdb_wal_segments_total`         | Gauge     | Number of active WAL segments                    |
| `prkdb_wal_bytes_written_total`    | Counter   | Total bytes written to disk                      |
| `prkdb_wal_fsync_duration_seconds` | Histogram | Time taken for `fsync` calls (critical for perf) |

## Example Grafana Dashboard

Basic panel setup:

1. **Cluster Status**: Table showing `prkdb_raft_state` for all nodes.
2. **Commit Velocity**: Rate of `prkdb_raft_commit_index` increase.
3. **RPC Latency**: 99th percentile of `prkdb_grpc_latency_seconds`.
4. **Disk I/O**: Rate of `prkdb_wal_bytes_written_total`.
