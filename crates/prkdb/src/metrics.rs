use crate::partitioning::PartitionId;
use dashmap::DashMap;
use serde::Serialize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Partition-specific metrics for operational visibility
#[derive(Clone)]
pub struct PartitionMetrics {
    partition_id: PartitionId,
    // Event processing metrics
    events_produced: Arc<AtomicU64>,
    events_consumed: Arc<AtomicU64>,
    bytes_produced: Arc<AtomicU64>,
    bytes_consumed: Arc<AtomicU64>,

    // Performance metrics
    last_produce_timestamp: Arc<AtomicU64>,
    last_consume_timestamp: Arc<AtomicU64>,
    max_consume_latency_ms: Arc<AtomicU64>,
    total_consume_latency_micros: Arc<AtomicU64>,
    consume_count: Arc<AtomicU64>,

    // Consumer group metrics
    assigned_consumers: Arc<AtomicU64>,
    last_rebalance_timestamp: Arc<AtomicU64>,
    rebalance_count: Arc<AtomicU64>,
}

impl PartitionMetrics {
    pub fn new(partition_id: PartitionId) -> Self {
        Self {
            partition_id,
            events_produced: Arc::new(AtomicU64::new(0)),
            events_consumed: Arc::new(AtomicU64::new(0)),
            bytes_produced: Arc::new(AtomicU64::new(0)),
            bytes_consumed: Arc::new(AtomicU64::new(0)),
            last_produce_timestamp: Arc::new(AtomicU64::new(0)),
            last_consume_timestamp: Arc::new(AtomicU64::new(0)),
            max_consume_latency_ms: Arc::new(AtomicU64::new(0)),
            total_consume_latency_micros: Arc::new(AtomicU64::new(0)),
            consume_count: Arc::new(AtomicU64::new(0)),
            assigned_consumers: Arc::new(AtomicU64::new(0)),
            last_rebalance_timestamp: Arc::new(AtomicU64::new(0)),
            rebalance_count: Arc::new(AtomicU64::new(0)),
        }
    }

    // Event tracking
    pub fn record_produce(&self, bytes: usize) {
        self.events_produced.fetch_add(1, Ordering::Relaxed);
        self.bytes_produced
            .fetch_add(bytes as u64, Ordering::Relaxed);
        self.last_produce_timestamp
            .store(current_timestamp_ms(), Ordering::Relaxed);
    }

    pub fn record_consume(&self, bytes: usize, latency_ms: u64) {
        self.events_consumed.fetch_add(1, Ordering::Relaxed);
        self.bytes_consumed
            .fetch_add(bytes as u64, Ordering::Relaxed);
        self.last_consume_timestamp
            .store(current_timestamp_ms(), Ordering::Relaxed);

        // Update latency metrics
        // Note: latency_ms input is u64 ms, but we want micros for better precision if available.
        // For now, we'll convert ms to micros, but ideally the input should be micros.
        // We'll assume the input might be changed to micros later or we just store ms * 1000.
        self.total_consume_latency_micros
            .fetch_add(latency_ms * 1000, Ordering::Relaxed);
        self.consume_count.fetch_add(1, Ordering::Relaxed);

        // Update max latency
        let mut prev = self.max_consume_latency_ms.load(Ordering::Relaxed);
        while latency_ms > prev {
            match self.max_consume_latency_ms.compare_exchange(
                prev,
                latency_ms,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(p) => prev = p,
            }
        }
    }

    // Consumer group tracking
    pub fn update_consumer_assignment(&self, consumer_count: usize) {
        self.assigned_consumers
            .store(consumer_count as u64, Ordering::Relaxed);
    }

    pub fn record_rebalance(&self) {
        self.rebalance_count.fetch_add(1, Ordering::Relaxed);
        self.last_rebalance_timestamp
            .store(current_timestamp_ms(), Ordering::Relaxed);
    }

    // Getters
    pub fn partition_id(&self) -> PartitionId {
        self.partition_id
    }
    pub fn events_produced(&self) -> u64 {
        self.events_produced.load(Ordering::Relaxed)
    }
    pub fn events_consumed(&self) -> u64 {
        self.events_consumed.load(Ordering::Relaxed)
    }
    pub fn bytes_produced(&self) -> u64 {
        self.bytes_produced.load(Ordering::Relaxed)
    }
    pub fn bytes_consumed(&self) -> u64 {
        self.bytes_consumed.load(Ordering::Relaxed)
    }
    pub fn last_produce_timestamp(&self) -> u64 {
        self.last_produce_timestamp.load(Ordering::Relaxed)
    }
    pub fn last_consume_timestamp(&self) -> u64 {
        self.last_consume_timestamp.load(Ordering::Relaxed)
    }
    pub fn max_consume_latency_ms(&self) -> u64 {
        self.max_consume_latency_ms.load(Ordering::Relaxed)
    }
    pub fn total_consume_latency_micros(&self) -> u64 {
        self.total_consume_latency_micros.load(Ordering::Relaxed)
    }
    pub fn consume_count(&self) -> u64 {
        self.consume_count.load(Ordering::Relaxed)
    }
    pub fn assigned_consumers(&self) -> u64 {
        self.assigned_consumers.load(Ordering::Relaxed)
    }
    pub fn rebalance_count(&self) -> u64 {
        self.rebalance_count.load(Ordering::Relaxed)
    }

    /// Calculate consumer lag (events produced - consumed)
    pub fn consumer_lag(&self) -> u64 {
        let produced = self.events_produced();
        let consumed = self.events_consumed();
        produced.saturating_sub(consumed)
    }

    /// Calculate throughput in events per second (last minute estimate)
    pub fn throughput_eps(&self) -> f64 {
        let now = current_timestamp_ms();
        let last_consume = self.last_consume_timestamp();

        if last_consume == 0 || now <= last_consume {
            return 0.0;
        }

        let time_diff_sec = (now - last_consume) as f64 / 1000.0;
        if time_diff_sec > 60.0 {
            return 0.0; // No recent activity
        }

        // Rough estimate based on recent activity
        let events = self.events_consumed() as f64;
        events / time_diff_sec.max(1.0)
    }
}

/// Global partition metrics registry
#[derive(Clone)]
pub struct PartitionMetricsRegistry {
    metrics: Arc<DashMap<PartitionId, PartitionMetrics>>,
}

impl Default for PartitionMetricsRegistry {
    fn default() -> Self {
        Self {
            metrics: Arc::new(DashMap::new()),
        }
    }
}

impl PartitionMetricsRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Get or create metrics for a partition
    pub fn get_or_create(&self, partition_id: PartitionId) -> PartitionMetrics {
        if let Some(metrics) = self.metrics.get(&partition_id) {
            metrics.clone()
        } else {
            let metrics = PartitionMetrics::new(partition_id);
            self.metrics.insert(partition_id, metrics.clone());
            metrics
        }
    }

    /// Get metrics for a specific partition
    pub fn get(&self, partition_id: PartitionId) -> Option<PartitionMetrics> {
        self.metrics.get(&partition_id).map(|m| m.clone())
    }

    /// Get all partition metrics
    pub fn all_metrics(&self) -> Vec<PartitionMetrics> {
        self.metrics
            .iter()
            .map(|entry| entry.value().clone())
            .collect()
    }

    /// Get aggregated statistics across all partitions
    pub fn aggregate_stats(&self) -> PartitionAggregateStats {
        let all_metrics = self.all_metrics();

        let mut stats = PartitionAggregateStats::default();

        for metrics in &all_metrics {
            stats.total_partitions += 1;
            stats.total_events_produced += metrics.events_produced();
            stats.total_events_consumed += metrics.events_consumed();
            stats.total_bytes_produced += metrics.bytes_produced();
            stats.total_bytes_consumed += metrics.bytes_consumed();
            stats.total_assigned_consumers += metrics.assigned_consumers();
            stats.total_consumer_lag += metrics.consumer_lag();

            let throughput = metrics.throughput_eps();
            if throughput > stats.max_partition_throughput {
                stats.max_partition_throughput = throughput;
            }

            let latency = metrics.max_consume_latency_ms();
            if latency > stats.max_consume_latency_ms {
                stats.max_consume_latency_ms = latency;
            }
        }

        if stats.total_partitions > 0 {
            stats.avg_events_per_partition =
                stats.total_events_produced as f64 / stats.total_partitions as f64;
            stats.avg_consumers_per_partition =
                stats.total_assigned_consumers as f64 / stats.total_partitions as f64;
        }

        let mut total_latency_micros = 0;
        let mut total_count = 0;
        for metrics in &all_metrics {
            total_latency_micros += metrics.total_consume_latency_micros();
            total_count += metrics.consume_count();
        }
        if total_count > 0 {
            stats.avg_consume_latency_ms =
                (total_latency_micros as f64 / total_count as f64) / 1000.0;
        }

        stats
    }
}

#[derive(Debug, Default, Serialize, Clone)]
pub struct PartitionAggregateStats {
    pub total_partitions: u64,
    pub total_events_produced: u64,
    pub total_events_consumed: u64,
    pub total_bytes_produced: u64,
    pub total_bytes_consumed: u64,
    pub total_assigned_consumers: u64,
    pub total_consumer_lag: u64,
    pub avg_events_per_partition: f64,
    pub avg_consumers_per_partition: f64,
    pub max_partition_throughput: f64,
    pub max_consume_latency_ms: u64,
    pub avg_consume_latency_ms: f64,
}

fn current_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[derive(Clone)]
pub struct DbMetrics {
    puts: std::sync::Arc<AtomicU64>,
    deletes: std::sync::Arc<AtomicU64>,
    outbox_saved: std::sync::Arc<AtomicU64>,
    outbox_drained: std::sync::Arc<AtomicU64>,
    dlq_saved: std::sync::Arc<AtomicU64>,
    dlq_drained: std::sync::Arc<AtomicU64>,
    replay_events: std::sync::Arc<AtomicU64>,
    replay_since_calls: std::sync::Arc<AtomicU64>,
    replay_lag_millis_last: std::sync::Arc<AtomicU64>,
    replay_lag_millis_max: std::sync::Arc<AtomicU64>,

    // Partition metrics registry
    partition_metrics: PartitionMetricsRegistry,
}

impl Default for DbMetrics {
    fn default() -> Self {
        Self {
            puts: std::sync::Arc::new(AtomicU64::new(0)),
            deletes: std::sync::Arc::new(AtomicU64::new(0)),
            outbox_saved: std::sync::Arc::new(AtomicU64::new(0)),
            outbox_drained: std::sync::Arc::new(AtomicU64::new(0)),
            dlq_saved: std::sync::Arc::new(AtomicU64::new(0)),
            dlq_drained: std::sync::Arc::new(AtomicU64::new(0)),
            replay_events: std::sync::Arc::new(AtomicU64::new(0)),
            replay_since_calls: std::sync::Arc::new(AtomicU64::new(0)),
            replay_lag_millis_last: std::sync::Arc::new(AtomicU64::new(0)),
            replay_lag_millis_max: std::sync::Arc::new(AtomicU64::new(0)),
            partition_metrics: PartitionMetricsRegistry::new(),
        }
    }
}

impl DbMetrics {
    pub fn inc_puts(&self) {
        self.puts.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add_puts(&self, count: u64) {
        self.puts.fetch_add(count, Ordering::Relaxed);
    }

    pub fn inc_deletes(&self) {
        self.deletes.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add_deletes(&self, count: u64) {
        self.deletes.fetch_add(count, Ordering::Relaxed);
    }

    pub fn inc_outbox_saved(&self) {
        self.outbox_saved.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add_outbox_saved(&self, count: u64) {
        self.outbox_saved.fetch_add(count, Ordering::Relaxed);
    }
    pub fn inc_outbox_drained(&self) {
        self.outbox_drained.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_dlq_saved(&self) {
        self.dlq_saved.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_dlq_drained(&self) {
        self.dlq_drained.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_replay_events(&self, n: u64) {
        self.replay_events.fetch_add(n, Ordering::Relaxed);
    }
    pub fn inc_replay_since_calls(&self) {
        self.replay_since_calls.fetch_add(1, Ordering::Relaxed);
    }
    pub fn set_replay_lag_millis(&self, last: u64) {
        self.replay_lag_millis_last.store(last, Ordering::Relaxed);
        // update max
        let mut prev = self.replay_lag_millis_max.load(Ordering::Relaxed);
        while last > prev {
            match self.replay_lag_millis_max.compare_exchange(
                prev,
                last,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(p) => prev = p,
            }
        }
    }

    pub fn puts(&self) -> u64 {
        self.puts.load(Ordering::Relaxed)
    }
    pub fn deletes(&self) -> u64 {
        self.deletes.load(Ordering::Relaxed)
    }
    pub fn outbox_saved(&self) -> u64 {
        self.outbox_saved.load(Ordering::Relaxed)
    }
    pub fn outbox_drained(&self) -> u64 {
        self.outbox_drained.load(Ordering::Relaxed)
    }
    /// Difference between saved and drained outbox events (drift backlog).
    pub fn outbox_drift(&self) -> i64 {
        self.outbox_saved() as i64 - self.outbox_drained() as i64
    }
    pub fn dlq_saved(&self) -> u64 {
        self.dlq_saved.load(Ordering::Relaxed)
    }
    pub fn dlq_drained(&self) -> u64 {
        self.dlq_drained.load(Ordering::Relaxed)
    }
    pub fn replay_events(&self) -> u64 {
        self.replay_events.load(Ordering::Relaxed)
    }
    pub fn replay_since_calls(&self) -> u64 {
        self.replay_since_calls.load(Ordering::Relaxed)
    }
    pub fn replay_lag_millis_last(&self) -> u64 {
        self.replay_lag_millis_last.load(Ordering::Relaxed)
    }
    pub fn replay_lag_millis_max(&self) -> u64 {
        self.replay_lag_millis_max.load(Ordering::Relaxed)
    }

    // Partition metrics methods
    pub fn partition_metrics(&self) -> &PartitionMetricsRegistry {
        &self.partition_metrics
    }

    pub fn get_partition_metrics(&self, partition_id: PartitionId) -> PartitionMetrics {
        self.partition_metrics.get_or_create(partition_id)
    }

    pub fn record_partition_produce(&self, partition_id: PartitionId, bytes: usize) {
        let metrics = self.partition_metrics.get_or_create(partition_id);
        metrics.record_produce(bytes);
    }

    pub fn record_partition_consume(
        &self,
        partition_id: PartitionId,
        bytes: usize,
        latency_ms: u64,
    ) {
        let metrics = self.partition_metrics.get_or_create(partition_id);
        metrics.record_consume(bytes, latency_ms);
    }

    pub fn update_partition_consumer_assignment(
        &self,
        partition_id: PartitionId,
        consumer_count: usize,
    ) {
        let metrics = self.partition_metrics.get_or_create(partition_id);
        metrics.update_consumer_assignment(consumer_count);
    }

    pub fn record_partition_rebalance(&self, partition_id: PartitionId) {
        let metrics = self.partition_metrics.get_or_create(partition_id);
        metrics.record_rebalance();
    }

    pub fn get_partition_aggregate_stats(&self) -> PartitionAggregateStats {
        self.partition_metrics.aggregate_stats()
    }
}
