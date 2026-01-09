//! Prometheus metrics exporter

use prometheus::{
    register_counter_vec, register_gauge_vec, register_histogram_vec, register_int_gauge_vec,
    CounterVec, Encoder, GaugeVec, HistogramVec, IntGaugeVec, TextEncoder,
};

lazy_static::lazy_static! {
    /// Events produced counter by collection
    pub static ref EVENTS_PRODUCED: CounterVec = register_counter_vec!(
        "prkdb_events_produced_total",
        "Total number of events produced",
        &["collection"]
    ).unwrap();

    /// Events consumed counter by collection and consumer group
    pub static ref EVENTS_CONSUMED: CounterVec = register_counter_vec!(
        "prkdb_events_consumed_total",
        "Total number of events consumed",
        &["collection", "consumer_group"]
    ).unwrap();

    /// Partition-level metrics
    pub static ref PARTITION_RECORDS: GaugeVec = register_gauge_vec!(
        "prkdb_partition_records",
        "Number of records in partition",
        &["collection", "partition"]
    ).unwrap();

    pub static ref PARTITION_SIZE_BYTES: GaugeVec = register_gauge_vec!(
        "prkdb_partition_size_bytes",
        "Size of partition in bytes",
        &["collection", "partition"]
    ).unwrap();

    /// Consumer lag by group and partition
    pub static ref CONSUMER_LAG: GaugeVec = register_gauge_vec!(
        "prkdb_consumer_lag",
        "Consumer lag (difference between latest offset and consumer offset)",
        &["consumer_group", "collection", "partition"]
    ).unwrap();

    /// Consumer offset position
    pub static ref CONSUMER_OFFSET: GaugeVec = register_gauge_vec!(
        "prkdb_consumer_offset",
        "Current consumer offset position",
        &["consumer_group", "collection", "partition"]
    ).unwrap();

    /// Operation latency histogram
    pub static ref OPERATION_LATENCY: HistogramVec = register_histogram_vec!(
        "prkdb_operation_duration_seconds",
        "Operation latency in seconds",
        &["operation", "collection"],
        vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
    ).unwrap();

    /// Storage operations
    pub static ref STORAGE_OPS: CounterVec = register_counter_vec!(
        "prkdb_storage_operations_total",
        "Total storage operations",
        &["operation"] // put, get, delete, scan
    ).unwrap();

    /// Consumer group members
    pub static ref CONSUMER_GROUP_MEMBERS: IntGaugeVec = register_int_gauge_vec!(
        "prkdb_consumer_group_members",
        "Number of active members in a consumer group",
        &["consumer_group"]
    )
    .unwrap();

    // === Replication Metrics ===

    /// Replication lag in seconds (follower perspective)
    pub static ref REPLICATION_LAG_SECONDS: GaugeVec = register_gauge_vec!(
        "prkdb_replication_lag_seconds",
        "Replication lag in seconds between follower and leader",
        &["node_id", "leader_address"]
    )
    .unwrap();

    /// Number of changes emitted by leader into the outbox (leader perspective)
    pub static ref REPLICATION_CHANGES_SENT: CounterVec = register_counter_vec!(
        "prkdb_replication_changes_sent_total",
        "Total number of changes written to the outbox for replication",
        &["node_id", "collection"]
    )
    .unwrap();

    /// Number of changes replicated (follower perspective)
    pub static ref REPLICATION_CHANGES_APPLIED: CounterVec = register_counter_vec!(
        "prkdb_replication_changes_applied_total",
        "Total number of changes applied by follower",
        &["node_id", "collection"]
    )
    .unwrap();

    /// Number of changes available for replication (leader perspective)
    pub static ref REPLICATION_CHANGES_PENDING: GaugeVec = register_gauge_vec!(
        "prkdb_replication_changes_pending",
        "Number of changes pending replication on leader",
        &["node_id", "follower_id"]
    )
    .unwrap();

    /// Replication sync errors
    pub static ref REPLICATION_SYNC_ERRORS: CounterVec = register_counter_vec!(
        "prkdb_replication_sync_errors_total",
        "Total number of replication sync errors",
        &["node_id", "error_type"]
    )
    .unwrap();

    /// Last successful sync timestamp
    pub static ref REPLICATION_LAST_SYNC_TIMESTAMP: GaugeVec = register_gauge_vec!(
        "prkdb_replication_last_sync_timestamp_seconds",
        "Timestamp of last successful sync (Unix epoch)",
        &["node_id"]
    )
    .unwrap();

    /// Replication throughput (changes per second)
    pub static ref REPLICATION_THROUGHPUT: GaugeVec = register_gauge_vec!(
        "prkdb_replication_throughput_changes_per_second",
        "Replication throughput in changes per second",
        &["node_id"]
    )
    .unwrap();

    /// Follower connection status (1 = connected, 0 = disconnected)
    pub static ref REPLICATION_FOLLOWER_CONNECTED: GaugeVec = register_gauge_vec!(
        "prkdb_replication_follower_connected",
        "Follower connection status (1 = connected, 0 = disconnected)",
        &["node_id", "leader_address"]
    )
    .unwrap();

    /// Number of active followers (leader perspective)
    pub static ref REPLICATION_ACTIVE_FOLLOWERS: GaugeVec = register_gauge_vec!(
        "prkdb_replication_active_followers",
        "Number of active followers connected to leader",
        &["node_id"]
    )
    .unwrap();

    /// Replication batch size (number of changes per sync)
    pub static ref REPLICATION_BATCH_SIZE: HistogramVec = register_histogram_vec!(
        "prkdb_replication_batch_size",
        "Distribution of replication batch sizes",
        &["node_id"],
        vec![1.0, 5.0, 10.0, 50.0, 100.0, 500.0, 1000.0]
    )
    .unwrap();

    /// Replication sync duration in seconds
    pub static ref REPLICATION_SYNC_DURATION: HistogramVec = register_histogram_vec!(
        "prkdb_replication_sync_duration_seconds",
        "Duration of replication sync operations",
        &["node_id"],
        vec![0.001, 0.01, 0.1, 0.5, 1.0, 5.0, 10.0]
    )
    .unwrap();
}

/// Export metrics in Prometheus text format
pub fn export_metrics() -> Result<String, Box<dyn std::error::Error>> {
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer)?;
    Ok(String::from_utf8(buffer)?)
}
