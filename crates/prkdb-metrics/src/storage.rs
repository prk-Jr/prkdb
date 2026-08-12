use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Storage metrics for observability and monitoring
#[derive(Clone)]
pub struct StorageMetrics {
    inner: Arc<StorageMetricsInner>,
}

struct StorageMetricsInner {
    // Cache metrics
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
    cache_evictions: AtomicU64,
    cache_size_bytes: AtomicU64,

    // Compaction metrics
    compaction_cycles: AtomicU64,
    compaction_bytes_reclaimed: AtomicU64,

    // Write metrics
    writes_total: AtomicU64,
    write_bytes_total: AtomicU64,
    write_batches_total: AtomicU64,

    // Read metrics
    reads_total: AtomicU64,
    read_bytes_total: AtomicU64,

    // Delete metrics
    deletes_total: AtomicU64,
    delete_batches_total: AtomicU64,

    // Index metrics
    index_updates_total: AtomicU64,
    index_queries_total: AtomicU64,

    // Error metrics
    errors_total: AtomicU64,

    // Write-path liveness metrics.
    //
    // A stalled writer used to have no external symptom at all: callers blocked, and
    // nothing counted, gauged or logged it (spec 2026-08-11-wal-writer-liveness). These
    // four exist so the stall is visible on a dashboard before a client feels it. Queue
    // depth alone is not enough — a deep queue that is draining is healthy — so the age of
    // the oldest unpublished write and the time of the last publish are carried alongside
    // it, and those are what distinguish "busy" from "stuck".
    write_queue_depth: AtomicU64,
    write_queue_oldest_age_ms: AtomicU64,
    writer_publishes_total: AtomicU64,
    /// Unix milliseconds of the last successful publish; 0 if nothing has published yet.
    writer_last_publish_unix_ms: AtomicU64,
    writer_stalls_total: AtomicU64,
    /// 1 while the write path is healthy, 0 once the writer has exited or stalled.
    writer_healthy: AtomicU64,
}

impl Default for StorageMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl StorageMetrics {
    /// Create new storage metrics
    pub fn new() -> Self {
        Self {
            inner: Arc::new(StorageMetricsInner {
                cache_hits: AtomicU64::new(0),
                cache_misses: AtomicU64::new(0),
                cache_evictions: AtomicU64::new(0),
                cache_size_bytes: AtomicU64::new(0),
                compaction_cycles: AtomicU64::new(0),
                compaction_bytes_reclaimed: AtomicU64::new(0),
                writes_total: AtomicU64::new(0),
                write_bytes_total: AtomicU64::new(0),
                write_batches_total: AtomicU64::new(0),
                reads_total: AtomicU64::new(0),
                read_bytes_total: AtomicU64::new(0),
                deletes_total: AtomicU64::new(0),
                delete_batches_total: AtomicU64::new(0),
                index_updates_total: AtomicU64::new(0),
                index_queries_total: AtomicU64::new(0),
                errors_total: AtomicU64::new(0),
                write_queue_depth: AtomicU64::new(0),
                write_queue_oldest_age_ms: AtomicU64::new(0),
                writer_publishes_total: AtomicU64::new(0),
                writer_last_publish_unix_ms: AtomicU64::new(0),
                writer_stalls_total: AtomicU64::new(0),
                // Healthy until something says otherwise. Starting at 0 would make every
                // freshly opened adapter report a stalled writer until its first publish.
                writer_healthy: AtomicU64::new(1),
            }),
        }
    }

    // Cache metrics
    pub fn record_cache_hit(&self) {
        self.inner.cache_hits.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_cache_miss(&self) {
        self.inner.cache_misses.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_cache_eviction(&self) {
        self.inner.cache_evictions.fetch_add(1, Ordering::Relaxed);
    }

    pub fn set_cache_size_bytes(&self, size: u64) {
        self.inner.cache_size_bytes.store(size, Ordering::Relaxed);
    }

    // Compaction metrics
    pub fn record_compaction_cycle(&self) {
        self.inner.compaction_cycles.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_compaction_bytes(&self, bytes: u64) {
        self.inner
            .compaction_bytes_reclaimed
            .fetch_add(bytes, Ordering::Relaxed);
    }

    // Write metrics
    pub fn record_write(&self, bytes: u64) {
        self.inner.writes_total.fetch_add(1, Ordering::Relaxed);
        self.inner
            .write_bytes_total
            .fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn record_write_batch(&self, count: u64, bytes: u64) {
        self.inner
            .write_batches_total
            .fetch_add(1, Ordering::Relaxed);
        self.inner.writes_total.fetch_add(count, Ordering::Relaxed);
        self.inner
            .write_bytes_total
            .fetch_add(bytes, Ordering::Relaxed);
    }

    // Read metrics
    pub fn record_read(&self, bytes: u64) {
        self.inner.reads_total.fetch_add(1, Ordering::Relaxed);
        self.inner
            .read_bytes_total
            .fetch_add(bytes, Ordering::Relaxed);
    }

    // Delete metrics
    pub fn record_delete(&self) {
        self.inner.deletes_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_delete_batch(&self, count: u64) {
        self.inner
            .delete_batches_total
            .fetch_add(1, Ordering::Relaxed);
        self.inner.deletes_total.fetch_add(count, Ordering::Relaxed);
    }

    // Index metrics
    pub fn record_index_update(&self) {
        self.inner
            .index_updates_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_index_query(&self) {
        self.inner
            .index_queries_total
            .fetch_add(1, Ordering::Relaxed);
    }

    // Error metrics
    pub fn record_error(&self) {
        self.inner.errors_total.fetch_add(1, Ordering::Relaxed);
    }

    // Getters
    pub fn cache_hits(&self) -> u64 {
        self.inner.cache_hits.load(Ordering::Relaxed)
    }

    pub fn cache_misses(&self) -> u64 {
        self.inner.cache_misses.load(Ordering::Relaxed)
    }

    pub fn cache_hit_ratio(&self) -> f64 {
        let hits = self.cache_hits();
        let misses = self.cache_misses();
        let total = hits + misses;
        if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        }
    }

    pub fn cache_evictions(&self) -> u64 {
        self.inner.cache_evictions.load(Ordering::Relaxed)
    }

    pub fn cache_size_bytes(&self) -> u64 {
        self.inner.cache_size_bytes.load(Ordering::Relaxed)
    }

    pub fn compaction_cycles(&self) -> u64 {
        self.inner.compaction_cycles.load(Ordering::Relaxed)
    }

    pub fn compaction_bytes_reclaimed(&self) -> u64 {
        self.inner
            .compaction_bytes_reclaimed
            .load(Ordering::Relaxed)
    }

    pub fn writes_total(&self) -> u64 {
        self.inner.writes_total.load(Ordering::Relaxed)
    }

    pub fn write_bytes_total(&self) -> u64 {
        self.inner.write_bytes_total.load(Ordering::Relaxed)
    }

    pub fn write_batches_total(&self) -> u64 {
        self.inner.write_batches_total.load(Ordering::Relaxed)
    }

    pub fn reads_total(&self) -> u64 {
        self.inner.reads_total.load(Ordering::Relaxed)
    }

    pub fn read_bytes_total(&self) -> u64 {
        self.inner.read_bytes_total.load(Ordering::Relaxed)
    }

    pub fn deletes_total(&self) -> u64 {
        self.inner.deletes_total.load(Ordering::Relaxed)
    }

    pub fn delete_batches_total(&self) -> u64 {
        self.inner.delete_batches_total.load(Ordering::Relaxed)
    }

    pub fn index_updates_total(&self) -> u64 {
        self.inner.index_updates_total.load(Ordering::Relaxed)
    }

    pub fn index_queries_total(&self) -> u64 {
        self.inner.index_queries_total.load(Ordering::Relaxed)
    }

    pub fn errors_total(&self) -> u64 {
        self.inner.errors_total.load(Ordering::Relaxed)
    }

    // Write-path liveness metrics

    /// Publish the current queue depth and the age of the oldest write still in it.
    ///
    /// Set together because either alone is misleading: depth without age cannot tell a
    /// draining backlog from a frozen one, and age without depth has no scale.
    pub fn set_write_queue(&self, depth: u64, oldest_age_ms: u64) {
        self.inner.write_queue_depth.store(depth, Ordering::Relaxed);
        self.inner
            .write_queue_oldest_age_ms
            .store(oldest_age_ms, Ordering::Relaxed);
    }

    /// Record `count` writes reaching the log, at `unix_ms`.
    pub fn record_writer_publish(&self, count: u64, unix_ms: u64) {
        self.inner
            .writer_publishes_total
            .fetch_add(count, Ordering::Relaxed);
        self.inner
            .writer_last_publish_unix_ms
            .store(unix_ms, Ordering::Relaxed);
    }

    /// Publish rate over the supervisor's last observation window.
    ///
    /// Stored as thousandths so the gauge stays an integer atomic; a lifetime average
    /// would be the easier thing to compute and the useless one — it cannot fall to zero
    /// after a busy period, which is exactly the transition worth alerting on.
    /// Mark the write path healthy or not. `reason` is for the log, not the gauge.
    pub fn set_writer_healthy(&self, healthy: bool) {
        self.inner
            .writer_healthy
            .store(u64::from(healthy), Ordering::Relaxed);
    }

    /// Count one stall detection. Monotonic, so a writer that recovers still leaves a
    /// trace an operator can find after the fact.
    pub fn record_writer_stall(&self) {
        self.inner
            .writer_stalls_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn write_queue_depth(&self) -> u64 {
        self.inner.write_queue_depth.load(Ordering::Relaxed)
    }

    pub fn write_queue_oldest_age_ms(&self) -> u64 {
        self.inner.write_queue_oldest_age_ms.load(Ordering::Relaxed)
    }

    pub fn writer_publishes_total(&self) -> u64 {
        self.inner.writer_publishes_total.load(Ordering::Relaxed)
    }

    /// Unix milliseconds of the last successful publish, or `None` if nothing has ever
    /// published through this adapter.
    pub fn writer_last_publish_unix_ms(&self) -> Option<u64> {
        match self
            .inner
            .writer_last_publish_unix_ms
            .load(Ordering::Relaxed)
        {
            0 => None,
            ms => Some(ms),
        }
    }

    pub fn writer_stalls_total(&self) -> u64 {
        self.inner.writer_stalls_total.load(Ordering::Relaxed)
    }

    pub fn writer_healthy(&self) -> bool {
        self.inner.writer_healthy.load(Ordering::Relaxed) != 0
    }

    /// Get a snapshot of all metrics
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            cache_hits: self.cache_hits(),
            cache_misses: self.cache_misses(),
            cache_evictions: self.cache_evictions(),
            cache_size_bytes: self.cache_size_bytes(),
            compaction_cycles: self.compaction_cycles(),
            compaction_bytes_reclaimed: self.compaction_bytes_reclaimed(),
            writes_total: self.writes_total(),
            write_bytes_total: self.write_bytes_total(),
            write_batches_total: self.write_batches_total(),
            reads_total: self.reads_total(),
            read_bytes_total: self.read_bytes_total(),
            deletes_total: self.deletes_total(),
            delete_batches_total: self.delete_batches_total(),
            index_updates_total: self.index_updates_total(),
            index_queries_total: self.index_queries_total(),
            errors_total: self.errors_total(),
            write_queue_depth: self.write_queue_depth(),
            write_queue_oldest_age_ms: self.write_queue_oldest_age_ms(),
            writer_publishes_total: self.writer_publishes_total(),
            writer_last_publish_unix_ms: self.writer_last_publish_unix_ms(),
            writer_stalls_total: self.writer_stalls_total(),
            writer_healthy: self.writer_healthy(),
        }
    }
}

/// Snapshot of all metrics at a point in time
#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub cache_evictions: u64,
    pub cache_size_bytes: u64,
    pub compaction_cycles: u64,
    pub compaction_bytes_reclaimed: u64,
    pub writes_total: u64,
    pub write_bytes_total: u64,
    pub write_batches_total: u64,
    pub reads_total: u64,
    pub read_bytes_total: u64,
    pub deletes_total: u64,
    pub delete_batches_total: u64,
    pub index_updates_total: u64,
    pub index_queries_total: u64,
    pub errors_total: u64,
    pub write_queue_depth: u64,
    pub write_queue_oldest_age_ms: u64,
    pub writer_publishes_total: u64,
    pub writer_last_publish_unix_ms: Option<u64>,
    pub writer_stalls_total: u64,
    pub writer_healthy: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_metrics() {
        let metrics = StorageMetrics::new();

        metrics.record_cache_hit();
        metrics.record_cache_hit();
        metrics.record_cache_miss();

        assert_eq!(metrics.cache_hits(), 2);
        assert_eq!(metrics.cache_misses(), 1);
        assert!((metrics.cache_hit_ratio() - 0.666).abs() < 0.01);
    }

    #[test]
    fn test_write_metrics() {
        let metrics = StorageMetrics::new();

        metrics.record_write(100);
        metrics.record_write_batch(10, 1000);

        assert_eq!(metrics.writes_total(), 11); // 1 + 10
        assert_eq!(metrics.write_bytes_total(), 1100); // 100 + 1000
        assert_eq!(metrics.write_batches_total(), 1);
    }

    #[test]
    fn test_snapshot() {
        let metrics = StorageMetrics::new();

        metrics.record_cache_hit();
        metrics.record_write(100);
        metrics.record_compaction_cycle();

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.cache_hits, 1);
        assert_eq!(snapshot.writes_total, 1);
        assert_eq!(snapshot.compaction_cycles, 1);
    }

    #[test]
    fn write_path_starts_healthy_and_idle() {
        let metrics = StorageMetrics::new();

        assert!(metrics.writer_healthy());
        assert_eq!(metrics.write_queue_depth(), 0);
        assert_eq!(metrics.writer_stalls_total(), 0);
        assert_eq!(
            metrics.writer_last_publish_unix_ms(),
            None,
            "an adapter that has published nothing must say so rather than report the epoch"
        );
    }

    #[test]
    fn write_path_metrics_round_trip_through_the_snapshot() {
        let metrics = StorageMetrics::new();

        metrics.set_write_queue(42, 1_500);
        metrics.record_writer_publish(7, 1_700_000_000_000);
        metrics.record_writer_stall();
        metrics.set_writer_healthy(false);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.write_queue_depth, 42);
        assert_eq!(snapshot.write_queue_oldest_age_ms, 1_500);
        assert_eq!(snapshot.writer_publishes_total, 7);
        assert_eq!(
            snapshot.writer_last_publish_unix_ms,
            Some(1_700_000_000_000)
        );
        assert_eq!(snapshot.writer_stalls_total, 1);
        assert!(!snapshot.writer_healthy);
    }
}
