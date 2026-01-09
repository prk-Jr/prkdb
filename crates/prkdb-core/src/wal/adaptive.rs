use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Configuration for adaptive batching
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdaptiveBatchConfig {
    /// Initial batch size
    pub initial_batch_size: usize,
    /// Minimum batch size
    pub min_batch_size: usize,
    /// Maximum batch size
    pub max_batch_size: usize,
    /// Initial batch timeout
    pub initial_timeout: Duration,
    /// Minimum batch timeout
    pub min_timeout: Duration,
    /// Maximum batch timeout
    pub max_timeout: Duration,
    /// Target latency for writes
    pub target_latency: Duration,
}

impl Default for AdaptiveBatchConfig {
    fn default() -> Self {
        Self {
            initial_batch_size: 100,
            min_batch_size: 10,
            max_batch_size: 1000,
            initial_timeout: Duration::from_millis(5),
            min_timeout: Duration::from_millis(1),
            max_timeout: Duration::from_millis(50),
            target_latency: Duration::from_millis(10),
        }
    }
}

/// Workload profile for simplified configuration
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum WorkloadProfile {
    /// Optimized for low latency (e.g., user-facing apps)
    LatencySensitive,
    /// Optimized for high throughput (e.g., bulk ingestion)
    ThroughputOptimized,
    /// Balanced approach (default)
    Balanced,
    /// Custom configuration
    Custom,
}

impl WorkloadProfile {
    /// Get the adaptive config for this profile
    pub fn to_config(&self) -> AdaptiveBatchConfig {
        match self {
            WorkloadProfile::LatencySensitive => AdaptiveBatchConfig {
                initial_batch_size: 10,
                min_batch_size: 1,
                max_batch_size: 100,
                initial_timeout: Duration::from_millis(1),
                min_timeout: Duration::from_micros(100),
                max_timeout: Duration::from_millis(5),
                target_latency: Duration::from_millis(2),
            },
            WorkloadProfile::ThroughputOptimized => AdaptiveBatchConfig {
                initial_batch_size: 1000,
                min_batch_size: 100,
                max_batch_size: 10000,
                initial_timeout: Duration::from_millis(10),
                min_timeout: Duration::from_millis(5),
                max_timeout: Duration::from_millis(100),
                target_latency: Duration::from_millis(50),
            },
            WorkloadProfile::Balanced => AdaptiveBatchConfig::default(),
            WorkloadProfile::Custom => AdaptiveBatchConfig::default(),
        }
    }
}

/// Accumulator that adjusts batch size and timeout based on latency
pub struct AdaptiveBatchAccumulator {
    config: AdaptiveBatchConfig,
    current_batch_size: usize,
    current_timeout: Duration,
    last_flush: std::time::Instant,
    records: Vec<crate::wal::LogRecord>,
}

impl AdaptiveBatchAccumulator {
    pub fn new(config: AdaptiveBatchConfig) -> Self {
        Self {
            current_batch_size: config.initial_batch_size,
            current_timeout: config.initial_timeout,
            config,
            last_flush: std::time::Instant::now(),
            records: Vec::new(),
        }
    }

    pub fn should_flush(&self) -> bool {
        self.records.len() >= self.current_batch_size
            || self.last_flush.elapsed() >= self.current_timeout
    }

    pub fn add_record(&mut self, record: crate::wal::LogRecord) {
        self.records.push(record);
    }

    pub fn take_records(&mut self) -> Vec<crate::wal::LogRecord> {
        self.last_flush = std::time::Instant::now();
        std::mem::take(&mut self.records)
    }

    pub fn update_stats(&mut self, latency: Duration) {
        // Simple AIMD (Additive Increase, Multiplicative Decrease) logic
        if latency > self.config.target_latency {
            // Latency too high, decrease batch size and timeout
            self.current_batch_size = (self.current_batch_size as f64 * 0.8) as usize;
            self.current_timeout = self.current_timeout.mul_f64(0.8);
        } else {
            // Latency good, increase batch size (up to max)
            self.current_batch_size += 1;
            self.current_timeout += Duration::from_micros(100);
        }

        // Clamp values
        self.current_batch_size = self
            .current_batch_size
            .clamp(self.config.min_batch_size, self.config.max_batch_size);
        self.current_timeout = self
            .current_timeout
            .clamp(self.config.min_timeout, self.config.max_timeout);
    }

    pub fn current_batch_size(&self) -> usize {
        self.current_batch_size
    }

    pub fn current_timeout(&self) -> Duration {
        self.current_timeout
    }

    pub fn set_config(&mut self, config: AdaptiveBatchConfig) {
        self.config = config;
        // Reset current values to new initial? Or keep adapting?
        // Let's keep adapting but clamp to new limits
        self.current_batch_size = self
            .current_batch_size
            .clamp(self.config.min_batch_size, self.config.max_batch_size);
        self.current_timeout = self
            .current_timeout
            .clamp(self.config.min_timeout, self.config.max_timeout);
    }
}
