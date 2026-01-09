use crate::wal::CompressionConfig;
use serde::{Deserialize, Serialize};

/// Configuration for async batching window.
///
/// This enables Kafka-style batching where operations are buffered for a short time
/// before execution, dramatically reducing overhead and improving throughput.
///
/// # Example
///
/// ```rust
/// use prkdb_core::batch_config::BatchConfig;
///
/// let config = BatchConfig::default();
/// assert_eq!(config.linger_ms, 10);
///
/// let custom = BatchConfig {
///     linger_ms: 5,
///     max_batch_size: 500,
///     max_buffer_bytes: 512 * 1024, // 512KB
///     compression: Default::default(),
/// };
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchConfig {
    /// Time to wait before flushing batch (milliseconds).
    ///
    /// Similar to Kafka's `linger.ms`. Operations are buffered for this duration
    /// to allow batching. Higher values increase throughput but add latency.
    ///
    /// Default: 10ms (good balance for most workloads)
    pub linger_ms: u64,

    /// Maximum number of items in a batch.
    ///
    /// When reached, the batch is flushed immediately regardless of linger_ms.
    /// This prevents unbounded memory growth and ensures timely execution.
    ///
    /// Default: 1000 items
    pub max_batch_size: usize,

    /// Maximum memory (in bytes) for pending batches.
    ///
    /// When exceeded, backpressure is applied by blocking new operations
    /// until space is available.
    ///
    /// Default: 1MB
    pub max_buffer_bytes: usize,
    pub compression: CompressionConfig,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            linger_ms: 10,                          // 10ms - Kafka's default
            max_batch_size: 1000,                   // Force flush at 1K items
            max_buffer_bytes: 1024 * 1024,          // 1MB buffer
            compression: CompressionConfig::none(), // Phase 1 Optimization: Disable compression
        }
    }
}

impl BatchConfig {
    /// Create a latency-optimized configuration.
    ///
    /// Lower linger time (1ms) for latency-sensitive applications.
    pub fn latency_optimized() -> Self {
        Self {
            linger_ms: 1,
            max_batch_size: 100,
            max_buffer_bytes: 256 * 1024,
            compression: CompressionConfig::none(), // No compression for lowest latency
        }
    }

    /// Create a throughput-optimized configuration.
    ///
    /// Higher linger time (50ms) and larger batches for maximum throughput.
    pub fn throughput_optimized() -> Self {
        Self {
            linger_ms: 50,
            max_batch_size: 10000,
            max_buffer_bytes: 10 * 1024 * 1024,
            compression: CompressionConfig::throughput_optimized(), // LZ4 for high throughput
        }
    }

    /// Disable batching (immediate execution).
    ///
    /// Useful for debugging or latency-critical single operations.
    pub fn no_batching() -> Self {
        Self {
            linger_ms: 0,
            max_batch_size: 1,
            max_buffer_bytes: 0,
            compression: CompressionConfig::none(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_reasonable() {
        let config = BatchConfig::default();
        assert_eq!(config.linger_ms, 10);
        assert_eq!(config.max_batch_size, 1000);
        assert_eq!(config.max_buffer_bytes, 1024 * 1024);
    }

    #[test]
    fn latency_optimized_has_low_linger() {
        let config = BatchConfig::latency_optimized();
        assert_eq!(config.linger_ms, 1);
        assert!(config.max_batch_size < BatchConfig::default().max_batch_size);
    }

    #[test]
    fn throughput_optimized_has_high_linger() {
        let config = BatchConfig::throughput_optimized();
        assert!(config.linger_ms > BatchConfig::default().linger_ms);
        assert!(config.max_batch_size > BatchConfig::default().max_batch_size);
    }

    #[test]
    fn no_batching_disables_buffering() {
        let config = BatchConfig::no_batching();
        assert_eq!(config.linger_ms, 0);
        assert_eq!(config.max_batch_size, 1);
    }
}
