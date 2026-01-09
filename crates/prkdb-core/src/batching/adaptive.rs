use std::collections::VecDeque;
use std::time::{Duration, Instant};

/// Configuration for adaptive batching
#[derive(Debug, Clone)]
pub struct AdaptiveBatchConfig {
    /// Minimum batch size (default: 10)
    pub min_batch_size: usize,
    /// Maximum batch size (default: 1000)
    pub max_batch_size: usize,
    /// Minimum flush interval in milliseconds (default: 1ms)
    pub min_flush_ms: u64,
    /// Maximum flush interval in milliseconds (default: 100ms)
    pub max_flush_ms: u64,
    /// Target P99 latency in milliseconds (default: 50ms)
    pub target_p99_latency_ms: u64,
    /// Target P50 latency in milliseconds (default: 10ms)
    pub target_p50_latency_ms: u64,
    /// How often to adjust parameters (default: 1s)
    pub adjustment_interval: Duration,
    /// Multiplier for adjustments (default: 1.2)
    pub adjustment_step_size: f64,
    /// Number of latency samples to keep (default: 100)
    pub latency_window_size: usize,
}

impl Default for AdaptiveBatchConfig {
    fn default() -> Self {
        Self {
            min_batch_size: 10,
            max_batch_size: 10000, // Phase 5.3: Increased from 1K to 10K for higher throughput
            min_flush_ms: 1,
            max_flush_ms: 50, // Phase 5.3: Reduced from 100ms to 50ms for optimal linger
            target_p99_latency_ms: 50,
            target_p50_latency_ms: 10,
            adjustment_interval: Duration::from_secs(1),
            adjustment_step_size: 1.2,
            latency_window_size: 100,
        }
    }
}

/// Tracks latencies in a rolling window for adaptive decisions
#[derive(Debug)]
pub struct LatencyTracker {
    samples: VecDeque<Duration>,
    window_size: usize,
    target_p99: Duration,
    target_p50: Duration,
}

impl LatencyTracker {
    /// Create a new latency tracker
    pub fn new(window_size: usize, target_p99_ms: u64, target_p50_ms: u64) -> Self {
        Self {
            samples: VecDeque::with_capacity(window_size),
            window_size,
            target_p99: Duration::from_millis(target_p99_ms),
            target_p50: Duration::from_millis(target_p50_ms),
        }
    }

    /// Record a new latency sample
    pub fn record(&mut self, latency: Duration) {
        if self.samples.len() >= self.window_size {
            self.samples.pop_front();
        }
        self.samples.push_back(latency);
    }

    /// Get the P50 (median) latency
    pub fn p50(&self) -> Duration {
        self.percentile(0.50)
    }

    /// Get the P99 latency
    pub fn p99(&self) -> Duration {
        self.percentile(0.99)
    }

    /// Calculate a percentile
    fn percentile(&self, p: f64) -> Duration {
        if self.samples.is_empty() {
            return Duration::from_millis(0);
        }

        let mut sorted: Vec<_> = self.samples.iter().copied().collect();
        sorted.sort();

        let index = ((sorted.len() as f64) * p).floor() as usize;
        let index = index.min(sorted.len() - 1);
        sorted[index]
    }

    /// Check if we should increase batch size (latency under target)
    pub fn should_increase_batch_size(&self) -> bool {
        if self.samples.len() < 10 {
            return false; // Not enough data
        }

        let p99 = self.p99();
        let p50 = self.p50();

        // Increase if both P50 and P99 are under target
        p99 < self.target_p99 && p50 < self.target_p50
    }

    /// Check if we should decrease batch size (latency over target)
    pub fn should_decrease_batch_size(&self) -> bool {
        if self.samples.len() < 10 {
            return false; // Not enough data
        }

        let p99 = self.p99();

        // Decrease if P99 is over target
        p99 > self.target_p99
    }

    /// Get the number of samples currently tracked
    pub fn sample_count(&self) -> usize {
        self.samples.len()
    }
}

/// Adaptive batch accumulator that adjusts batch size based on latency
#[derive(Debug)]
pub struct AdaptiveBatchAccumulator<T> {
    buffer: Vec<T>,

    // Dynamic parameters
    batch_size: usize,
    flush_interval: Duration,

    // Configuration limits
    config: AdaptiveBatchConfig,

    // Tracking
    latency_tracker: LatencyTracker,
    last_flush: Instant,
    last_adjustment: Instant,

    // Stats
    total_items_flushed: u64,
    total_flushes: u64,
}

impl<T> AdaptiveBatchAccumulator<T> {
    /// Create a new adaptive batch accumulator
    pub fn new(config: AdaptiveBatchConfig) -> Self {
        let latency_tracker = LatencyTracker::new(
            config.latency_window_size,
            config.target_p99_latency_ms,
            config.target_p50_latency_ms,
        );

        let initial_batch_size = (config.min_batch_size + config.max_batch_size) / 2;
        let initial_flush_interval =
            Duration::from_millis((config.min_flush_ms + config.max_flush_ms) / 2);

        Self {
            buffer: Vec::with_capacity(initial_batch_size),
            batch_size: initial_batch_size,
            flush_interval: initial_flush_interval,
            config,
            latency_tracker,
            last_flush: Instant::now(),
            last_adjustment: Instant::now(),
            total_items_flushed: 0,
            total_flushes: 0,
        }
    }

    /// Add an item to the batch
    /// Returns true if the batch should be flushed
    pub fn add(&mut self, item: T) -> bool {
        self.buffer.push(item);
        self.buffer.len() >= self.batch_size
    }

    /// Check if the batch should be flushed based on timeout
    pub fn should_flush(&self) -> bool {
        !self.buffer.is_empty() && self.last_flush.elapsed() >= self.flush_interval
    }

    /// Flush the current batch and return the items
    pub fn flush(&mut self) -> Vec<T> {
        let items = std::mem::take(&mut self.buffer);
        let count = items.len() as u64;

        if count > 0 {
            self.total_items_flushed += count;
            self.total_flushes += 1;

            // Record latency (time since items started accumulating)
            let latency = self.last_flush.elapsed();
            self.latency_tracker.record(latency);

            self.last_flush = Instant::now();

            // Periodically adjust parameters
            self.maybe_adjust_parameters();
        }

        items
    }

    /// Check if the buffer is empty
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    /// Get the current batch size
    pub fn current_batch_size(&self) -> usize {
        self.batch_size
    }

    /// Get the current flush interval
    pub fn current_flush_interval(&self) -> Duration {
        self.flush_interval
    }

    /// Get throughput statistics
    pub fn throughput(&self) -> f64 {
        if self.total_flushes == 0 {
            return 0.0;
        }
        self.total_items_flushed as f64 / self.total_flushes as f64
    }

    /// Adjust parameters based on latency feedback
    fn maybe_adjust_parameters(&mut self) {
        // Only adjust every adjustment_interval
        if self.last_adjustment.elapsed() < self.config.adjustment_interval {
            return;
        }

        self.last_adjustment = Instant::now();

        // Adjust batch size based on latency
        if self.latency_tracker.should_increase_batch_size() {
            let new_size = (self.batch_size as f64 * self.config.adjustment_step_size) as usize;
            self.batch_size = new_size.min(self.config.max_batch_size);
        } else if self.latency_tracker.should_decrease_batch_size() {
            let new_size = (self.batch_size as f64 / self.config.adjustment_step_size) as usize;
            self.batch_size = new_size.max(self.config.min_batch_size);
        }

        // Adjust flush interval based on batch fill rate
        let avg_items_per_flush = self.throughput();
        let fill_rate = avg_items_per_flush / self.batch_size as f64;

        if fill_rate > 0.9 {
            // Batches filling quickly, can reduce timeout
            let new_interval_ms =
                (self.flush_interval.as_millis() as f64 / self.config.adjustment_step_size) as u64;
            self.flush_interval =
                Duration::from_millis(new_interval_ms.max(self.config.min_flush_ms));
        } else if fill_rate < 0.3 {
            // Batches filling slowly, increase timeout
            let new_interval_ms =
                (self.flush_interval.as_millis() as f64 * self.config.adjustment_step_size) as u64;
            self.flush_interval =
                Duration::from_millis(new_interval_ms.min(self.config.max_flush_ms));
        }
    }

    /// Get latency statistics
    pub fn p50_latency(&self) -> Duration {
        self.latency_tracker.p50()
    }

    pub fn p99_latency(&self) -> Duration {
        self.latency_tracker.p99()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_latency_tracker_percentiles() {
        let mut tracker = LatencyTracker::new(100, 50, 10);

        // Add samples
        for i in 1..=100 {
            tracker.record(Duration::from_millis(i));
        }

        // P50 should be around 50ms
        let p50 = tracker.p50();
        assert!(p50.as_millis() >= 45 && p50.as_millis() <= 55);

        // P99 should be around 99ms
        let p99 = tracker.p99();
        assert!(p99.as_millis() >= 95);
    }

    #[test]
    fn test_latency_tracker_should_increase() {
        let mut tracker = LatencyTracker::new(100, 50, 10);

        // Add low latencies
        for _ in 0..20 {
            tracker.record(Duration::from_millis(5));
        }

        assert!(tracker.should_increase_batch_size());
        assert!(!tracker.should_decrease_batch_size());
    }

    #[test]
    fn test_latency_tracker_should_decrease() {
        let mut tracker = LatencyTracker::new(100, 50, 10);

        // Add high latencies
        for _ in 0..20 {
            tracker.record(Duration::from_millis(60));
        }

        assert!(!tracker.should_increase_batch_size());
        assert!(tracker.should_decrease_batch_size());
    }

    #[test]
    fn test_adaptive_accumulator_basic() {
        let config = AdaptiveBatchConfig::default();
        let mut acc = AdaptiveBatchAccumulator::new(config);

        // Add items
        for i in 0..5 {
            acc.add(i);
        }

        assert_eq!(acc.buffer.len(), 5);
        assert!(!acc.is_empty());
    }

    #[test]
    fn test_adaptive_accumulator_flush() {
        let config = AdaptiveBatchConfig {
            min_batch_size: 10,
            max_batch_size: 100,
            ..Default::default()
        };
        let mut acc = AdaptiveBatchAccumulator::new(config);

        // Add and flush
        for i in 0..20 {
            acc.add(i);
        }

        let items = acc.flush();
        assert_eq!(items.len(), 20);
        assert!(acc.is_empty());
        assert_eq!(acc.total_items_flushed, 20);
        assert_eq!(acc.total_flushes, 1);
    }

    #[test]
    fn test_adaptive_accumulator_auto_flush() {
        let config = AdaptiveBatchConfig {
            min_batch_size: 5,
            max_batch_size: 10, // Set max=10 so initial size will be (5+10)/2=7
            ..Default::default()
        };
        let mut acc = AdaptiveBatchAccumulator::new(config);

        // Initial batch size should be around 7
        assert!(acc.current_batch_size() >= 5);

        // Add items until auto-flush triggers
        let mut should_flush = false;
        for i in 0..20 {
            if acc.add(i) {
                should_flush = true;
                break;
            }
        }

        assert!(should_flush);
    }
}
