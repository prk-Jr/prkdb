# Metrics Reference

PrkDB exports Prometheus metrics from the shared registry in `crates/prkdb/src/prometheus_metrics.rs`.

## Where Metrics Are Exposed

- `prkdb-server`: `http://<host>:9090 + NODE_ID/metrics`
- `prkdb-cli serve --prometheus`: `http://<host>:<port>/metrics`

The HTTP server now re-exports the real Prometheus registry instead of placeholder counters.

## Core Health Metrics

| Metric | Type | Description |
| --- | --- | --- |
| `prkdb_up` | Gauge | Server liveness by `node_id` |
| `prkdb_collections_active` | Gauge | Number of active collections |
| `prkdb_collection_size_bytes` | Gauge | Collection size by `node_id` and `collection` |

## Raft Metrics

| Metric | Type | Description |
| --- | --- | --- |
| `prkdb_raft_state` | Gauge | Current Raft state by `node_id` and `partition` |
| `prkdb_raft_term` | Gauge | Current term |
| `prkdb_raft_commit_index` | Gauge | Commit index |
| `prkdb_raft_leader_elections_total` | Counter | Total leader elections |
| `prkdb_raft_heartbeats_sent_total` | Counter | Heartbeats sent to peers |
| `prkdb_raft_heartbeats_failed_total` | Counter | Failed heartbeats |
| `prkdb_raft_append_entries_total` | Counter | AppendEntries RPCs by result |
| `prkdb_raft_snapshot_index` | Gauge | Last snapshot index |

## Snapshot Metrics

| Metric | Type | Description |
| --- | --- | --- |
| `prkdb_snapshots_created_total` | Counter | Snapshots created |
| `prkdb_snapshot_creation_duration_seconds` | Histogram | Snapshot creation latency |

## Operation Metrics

| Metric | Type | Description |
| --- | --- | --- |
| `prkdb_ops_total` | Counter | Total operations |
| `prkdb_reads_total` | Counter | Total reads |
| `prkdb_writes_total` | Counter | Total writes |
| `prkdb_deletes_total` | Counter | Total deletes |
| `prkdb_delete_batches_total` | Counter | Delete batch operations |
| `prkdb_operation_duration_seconds` | Histogram | Operation latency |
| `prkdb_read_duration_seconds` | Histogram | Read latency |
| `prkdb_write_duration_seconds` | Histogram | Write latency |
| `prkdb_batch_duration_seconds` | Histogram | Batch write latency |

## Cache Metrics

| Metric | Type | Description |
| --- | --- | --- |
| `prkdb_cache_hits_total` | Counter | Cache hits |
| `prkdb_cache_misses_total` | Counter | Cache misses |
| `prkdb_cache_hit_ratio` | Gauge | Cache hit ratio |

## Example Checks

```bash
curl http://127.0.0.1:9091/metrics | grep prkdb_up
curl http://127.0.0.1:9091/metrics | grep prkdb_raft_state
curl http://127.0.0.1:9091/metrics | grep prkdb_operation_duration_seconds
```

## Alert Ideas

### Node down

```promql
prkdb_up == 0
```

### Frequent leader elections

```promql
rate(prkdb_raft_leader_elections_total[5m]) > 0
```

### Heartbeat failures

```promql
rate(prkdb_raft_heartbeats_failed_total[5m]) > 0
```

### High write latency

```promql
histogram_quantile(0.99, sum by (le) (rate(prkdb_write_duration_seconds_bucket[5m])))
```

### Write path not confirming writes

There is no PromQL for this one, and that is worth saying plainly rather than leaving you
to search for a series that does not exist. Storage write-path health is exposed on the
probe endpoints, not on `/metrics`.

```bash
curl -fsS http://node-1:8080/health | jq .write_path
```

```json
{
  "healthy": true,
  "reason": null,
  "queue_depth": 0,
  "oldest_unpublished_age_ms": 0,
  "last_publish_age_ms": 12
}
```

Alert on `/health` returning `503`, or on `healthy` being `false`.

The failure this catches: the WAL writer accepts writes into a queue and publishes them in
the background. If it exits or stops making progress, the process stays up and answers HTTP
normally while every client write hangs unconfirmed. Latency histograms do not catch it,
because a write that never completes never records a duration — the p99 stays flat while
nothing is being written at all.

Read the three counters together:

| pattern | meaning |
|---|---|
| `queue_depth` rising, `oldest_unpublished_age_ms` rising | writer falling behind — a capacity problem |
| `queue_depth` non-zero, `last_publish_age_ms` growing | writer has stopped publishing — trips `healthy: false` |
| `queue_depth` 0, `last_publish_age_ms` growing | idle, not stalled. Normal. |

The third row is why the check requires a non-empty queue: a database nobody is writing to
also has an old last-publish time, and flagging that would make the alert useless.
