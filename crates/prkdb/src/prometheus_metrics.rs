/// Prometheus metrics for Raft operations and system health
///
/// This module provides observability for:
/// - Raft consensus metrics (state, term, log size, elections)
/// - Operation throughput and latency
/// - Snapshot creation and installation
/// - System health indicators
///
/// Metrics are exposed via HTTP endpoint for Prometheus scraping.
use lazy_static::lazy_static;
use prometheus::{
    histogram_opts, opts, CounterVec, Encoder, GaugeVec, HistogramVec, Registry, TextEncoder,
};

lazy_static! {
    // ===== Raft Consensus Metrics =====

    /// Current Raft state (1=Leader, 2=Follower, 3=Candidate)
    pub static ref RAFT_STATE: GaugeVec = GaugeVec::new(
        opts!("prkdb_raft_state", "Current Raft state (1=Leader, 2=Follower, 3=Candidate)"),
        &["node_id", "partition"]
    ).unwrap();

    /// Current Raft term number
    pub static ref RAFT_TERM: GaugeVec = GaugeVec::new(
        opts!("prkdb_raft_term", "Current Raft term"),
        &["node_id", "partition"]
    ).unwrap();

    /// Raft committed log index
    pub static ref RAFT_COMMIT_INDEX: GaugeVec = GaugeVec::new(
        opts!("prkdb_raft_commit_index", "Raft committed log index"),
        &["node_id", "partition"]
    ).unwrap();

    /// Total number of leader elections
    pub static ref RAFT_LEADER_ELECTIONS_TOTAL: CounterVec = CounterVec::new(
        opts!("prkdb_raft_leader_elections_total", "Total number of leader elections"),
        &["node_id", "partition"]
    ).unwrap();

    /// Total heartbeats sent to peers
    pub static ref RAFT_HEARTBEATS_SENT_TOTAL: CounterVec = CounterVec::new(
        opts!("prkdb_raft_heartbeats_sent_total", "Total heartbeats sent to peers"),
        &["node_id", "peer", "partition"]
    ).unwrap();

    /// Total failed heartbeats to peers
    pub static ref RAFT_HEARTBEATS_FAILED_TOTAL: CounterVec = CounterVec::new(
        opts!("prkdb_raft_heartbeats_failed_total", "Total failed heartbeats to peers"),
        &["node_id", "peer", "partition"]
    ).unwrap();

    /// Total AppendEntries RPCs by result (success/failure)
    pub static ref RAFT_APPEND_ENTRIES_TOTAL: CounterVec = CounterVec::new(
        opts!("prkdb_raft_append_entries_total", "Total AppendEntries RPCs by result"),
        &["node_id", "partition", "result"]
    ).unwrap();

    // ===== Snapshot Metrics =====

    /// Last snapshot index
    pub static ref RAFT_SNAPSHOT_INDEX: GaugeVec = GaugeVec::new(
        opts!("prkdb_raft_snapshot_index", "Last snapshot index"),
        &["node_id", "partition"]
    ).unwrap();

    /// Total snapshots created
    pub static ref SNAPSHOTS_CREATED_TOTAL: CounterVec = CounterVec::new(
        opts!("prkdb_snapshots_created_total", "Total snapshots created"),
        &["node_id", "partition"]
    ).unwrap();

    /// Snapshot creation duration in seconds
    pub static ref SNAPSHOT_CREATION_DURATION: HistogramVec = HistogramVec::new(
        histogram_opts!(
            "prkdb_snapshot_creation_duration_seconds",
            "Time taken to create snapshot",
            vec![0.01, 0.05, 0.1, 0.5, 1.0, 2.5, 5.0, 10.0]
        ),
        &["node_id", "partition"]
    ).unwrap();

    // ===== System Health Metrics =====

    /// Server up status (1=up, 0=down)
    pub static ref SERVER_UP: GaugeVec = GaugeVec::new(
        opts!("prkdb_up", "Server up status (1=up)"),
        &["node_id"]
    ).unwrap();

    // ===== Performance Metrics =====

    /// Total operations (reads + writes)
    pub static ref OPS_TOTAL: CounterVec = CounterVec::new(
        opts!("prkdb_ops_total", "Total operations (reads + writes)"),
        &["node_id"]
    ).unwrap();

    /// Current operations per second
    pub static ref OPS_PER_SECOND: GaugeVec = GaugeVec::new(
        opts!("prkdb_ops_per_second", "Current operations per second"),
        &["node_id"]
    ).unwrap();

    /// Total read operations
    pub static ref READS_TOTAL: CounterVec = CounterVec::new(
        opts!("prkdb_reads_total", "Total read operations"),
        &["node_id"]
    ).unwrap();

    /// Total write operations
    pub static ref WRITES_TOTAL: CounterVec = CounterVec::new(
        opts!("prkdb_writes_total", "Total write operations"),
        &["node_id"]
    ).unwrap();

    /// Current read operations per second
    pub static ref READ_OPS_PER_SECOND: GaugeVec = GaugeVec::new(
        opts!("prkdb_read_ops_per_second", "Current read ops/sec"),
        &["node_id"]
    ).unwrap();

    /// Current write operations per second
    pub static ref WRITE_OPS_PER_SECOND: GaugeVec = GaugeVec::new(
        opts!("prkdb_write_ops_per_second", "Current write ops/sec"),
        &["node_id"]
    ).unwrap();

    // ===== Delete Metrics =====

    /// Total delete operations
    pub static ref DELETES_TOTAL: CounterVec = CounterVec::new(
        opts!("prkdb_deletes_total", "Total delete operations"),
        &["node_id"]
    ).unwrap();

    /// Total delete batch operations
    pub static ref DELETE_BATCHES_TOTAL: CounterVec = CounterVec::new(
        opts!("prkdb_delete_batches_total", "Total delete batch operations"),
        &["node_id"]
    ).unwrap();

    // ===== Index Metrics =====

    /// Total index query operations
    pub static ref INDEX_QUERIES_TOTAL: CounterVec = CounterVec::new(
        opts!("prkdb_index_queries_total", "Total index query operations"),
        &["node_id"]
    ).unwrap();

    /// Total index update operations
    pub static ref INDEX_UPDATES_TOTAL: CounterVec = CounterVec::new(
        opts!("prkdb_index_updates_total", "Total index update operations"),
        &["node_id"]
    ).unwrap();

    // ===== Latency Metrics =====

    /// Operation duration in seconds
    pub static ref OPERATION_DURATION: HistogramVec = HistogramVec::new(
        histogram_opts!(
            "prkdb_operation_duration_seconds",
            "Operation latency in seconds",
            vec![0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0]
        ),
        &["node_id", "operation"]
    ).unwrap();

    /// Read operation duration in seconds
    pub static ref READ_DURATION: HistogramVec = HistogramVec::new(
        histogram_opts!(
            "prkdb_read_duration_seconds",
            "Read operation latency in seconds",
            vec![0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0]
        ),
        &["node_id"]
    ).unwrap();

    /// Write operation duration in seconds
    pub static ref WRITE_DURATION: HistogramVec = HistogramVec::new(
        histogram_opts!(
            "prkdb_write_duration_seconds",
            "Write operation latency in seconds",
            vec![0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0]
        ),
        &["node_id"]
    ).unwrap();

    /// Batch write duration in seconds
    pub static ref BATCH_DURATION: HistogramVec = HistogramVec::new(
        histogram_opts!(
            "prkdb_batch_duration_seconds",
            "Batch write latency in seconds",
            vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 2.0, 5.0]
        ),
        &["node_id"]
    ).unwrap();

    // ===== Collection Metrics =====

    /// Operations per collection
    pub static ref COLLECTION_OPS_TOTAL: CounterVec = CounterVec::new(
        opts!("prkdb_collection_ops_total", "Total operations per collection"),
        &["node_id", "collection"]
    ).unwrap();

    /// Collection size in bytes
    pub static ref COLLECTION_SIZE_BYTES: GaugeVec = GaugeVec::new(
        opts!("prkdb_collection_size_bytes", "Collection size in bytes"),
        &["node_id", "collection"]
    ).unwrap();

    /// Number of active collections
    pub static ref COLLECTIONS_ACTIVE: GaugeVec = GaugeVec::new(
        opts!("prkdb_collections_active", "Number of active collections"),
        &["node_id"]
    ).unwrap();

    // ===== Cache Metrics =====

    /// Cache hits
    pub static ref CACHE_HITS_TOTAL: CounterVec = CounterVec::new(
        opts!("prkdb_cache_hits_total", "Total cache hits"),
        &["node_id"]
    ).unwrap();

    /// Cache misses
    pub static ref CACHE_MISSES_TOTAL: CounterVec = CounterVec::new(
        opts!("prkdb_cache_misses_total", "Total cache misses"),
        &["node_id"]
    ).unwrap();

    /// Cache hit ratio (0.0 - 1.0)
    pub static ref CACHE_HIT_RATIO: GaugeVec = GaugeVec::new(
        opts!("prkdb_cache_hit_ratio", "Cache hit ratio (0.0 - 1.0)"),
        &["node_id"]
    ).unwrap();

    /// Global Prometheus registry
    pub static ref PROMETHEUS_REGISTRY: Registry = {
        let r = Registry::new();

        // Register all metrics
        r.register(Box::new(RAFT_STATE.clone())).unwrap();
        r.register(Box::new(RAFT_TERM.clone())).unwrap();
        r.register(Box::new(RAFT_COMMIT_INDEX.clone())).unwrap();
        r.register(Box::new(RAFT_LEADER_ELECTIONS_TOTAL.clone())).unwrap();
        r.register(Box::new(RAFT_HEARTBEATS_SENT_TOTAL.clone())).unwrap();
        r.register(Box::new(RAFT_HEARTBEATS_FAILED_TOTAL.clone())).unwrap();
        r.register(Box::new(RAFT_APPEND_ENTRIES_TOTAL.clone())).unwrap();
        r.register(Box::new(RAFT_SNAPSHOT_INDEX.clone())).unwrap();
        r.register(Box::new(SNAPSHOTS_CREATED_TOTAL.clone())).unwrap();
        r.register(Box::new(SNAPSHOT_CREATION_DURATION.clone())).unwrap();
        r.register(Box::new(SERVER_UP.clone())).unwrap();

        // Register performance metrics
        r.register(Box::new(OPS_TOTAL.clone())).unwrap();
        r.register(Box::new(OPS_PER_SECOND.clone())).unwrap();
        r.register(Box::new(READS_TOTAL.clone())).unwrap();
        r.register(Box::new(WRITES_TOTAL.clone())).unwrap();
        r.register(Box::new(READ_OPS_PER_SECOND.clone())).unwrap();
        r.register(Box::new(WRITE_OPS_PER_SECOND.clone())).unwrap();

        // Register latency metrics
        r.register(Box::new(OPERATION_DURATION.clone())).unwrap();
        r.register(Box::new(READ_DURATION.clone())).unwrap();
        r.register(Box::new(WRITE_DURATION.clone())).unwrap();
        r.register(Box::new(BATCH_DURATION.clone())).unwrap();

        // Register collection metrics
        r.register(Box::new(COLLECTION_OPS_TOTAL.clone())).unwrap();
        r.register(Box::new(COLLECTION_SIZE_BYTES.clone())).unwrap();
        r.register(Box::new(COLLECTIONS_ACTIVE.clone())).unwrap();

        // Register cache metrics
        r.register(Box::new(CACHE_HITS_TOTAL.clone())).unwrap();
        r.register(Box::new(CACHE_MISSES_TOTAL.clone())).unwrap();
        r.register(Box::new(CACHE_HIT_RATIO.clone())).unwrap();

        r
    };
}

/// Initialize Prometheus metrics (called once at startup)
pub fn init_prometheus_metrics() {
    // Metrics are automatically registered via lazy_static
    tracing::info!("Prometheus metrics initialized");
}

/// Export metrics in Prometheus text format
pub fn export_metrics() -> String {
    let encoder = TextEncoder::new();
    let metric_families = PROMETHEUS_REGISTRY.gather();
    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer).unwrap();
    String::from_utf8(buffer).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_metrics_initialization() {
        init_prometheus_metrics();

        // Set a metric value to ensure it appears in export
        RAFT_STATE.with_label_values(&["test_node", "0"]).set(1.0);
        RAFT_COMMIT_INDEX
            .with_label_values(&["test_node", "0"])
            .set(100.0);

        let output = export_metrics();

        // Check for metrics that should exist after initialization
        assert!(
            output.contains("prkdb_raft_commit_index") || output.contains("prkdb_raft_state"),
            "Metrics output should contain Raft metrics after initialization. Output: {}",
            output
        );
    }

    #[test]
    fn test_raft_state_metric() {
        RAFT_STATE.with_label_values(&["1", "0"]).set(1.0); // Leader
        let output = export_metrics();
        assert!(output.contains("prkdb_raft_state"));
    }
}
