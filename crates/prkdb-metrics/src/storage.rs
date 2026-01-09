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
}
