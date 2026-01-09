use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Metrics for the Write-Ahead Log
///
/// Tracks performance and internal state of the WAL.
/// Uses atomic counters for low-overhead instrumentation.
#[derive(Debug, Default)]
pub struct WalMetrics {
    /// Total operations performed
    pub total_ops: AtomicU64,

    /// Total bytes written
    pub total_bytes: AtomicU64,

    /// Total batches processed
    pub total_batches: AtomicU64,

    /// Cumulative latency in microseconds
    pub total_latency_us: AtomicU64,

    /// Current batch size (adaptive)
    pub current_batch_size: AtomicU64,

    /// Current batch timeout in milliseconds (adaptive)
    pub current_batch_timeout_ms: AtomicU64,
}

impl WalMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a completed operation
    pub fn record_op(&self, latency: Duration) {
        self.total_ops.fetch_add(1, Ordering::Relaxed);
        self.total_latency_us
            .fetch_add(latency.as_micros() as u64, Ordering::Relaxed);
    }

    /// Record a completed batch
    pub fn record_batch(&self, count: usize, size_bytes: usize, latency: Duration) {
        self.total_ops.fetch_add(count as u64, Ordering::Relaxed);
        self.total_batches.fetch_add(1, Ordering::Relaxed);
        self.total_bytes
            .fetch_add(size_bytes as u64, Ordering::Relaxed);
        self.total_latency_us
            .fetch_add(latency.as_micros() as u64, Ordering::Relaxed);
    }

    /// Update adaptive state
    pub fn update_adaptive_state(&self, batch_size: usize, timeout_ms: u64) {
        self.current_batch_size
            .store(batch_size as u64, Ordering::Relaxed);
        self.current_batch_timeout_ms
            .store(timeout_ms, Ordering::Relaxed);
    }

    /// Get current operations per second (approximate)
    /// Note: This is a simple counter, for rate calculation you'd need a windowed approach
    /// or external scraper (Prometheus).
    pub fn ops_count(&self) -> u64 {
        self.total_ops.load(Ordering::Relaxed)
    }

    /// Get average latency in microseconds
    pub fn avg_latency_us(&self) -> u64 {
        let ops = self.total_ops.load(Ordering::Relaxed);
        if ops == 0 {
            return 0;
        }
        self.total_latency_us.load(Ordering::Relaxed) / ops
    }
}

/// Shared metrics instance
pub type SharedWalMetrics = Arc<WalMetrics>;
