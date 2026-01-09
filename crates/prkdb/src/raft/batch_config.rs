use serde::{Deserialize, Serialize};

/// Configuration for Raft proposal batching
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftBatchConfig {
    /// Maximum number of proposals to batch together
    ///
    /// Higher values improve throughput but may increase latency.
    /// Default: 1000
    pub max_batch_size: usize,

    /// Maximum time to wait for a batch to fill (milliseconds)
    ///
    /// Lower values reduce latency but may decrease throughput.
    /// Default: 10ms
    pub linger_ms: u64,
}

impl Default for RaftBatchConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 1000,
            linger_ms: 10,
        }
    }
}

impl RaftBatchConfig {
    /// Configuration optimized for low latency
    pub fn low_latency() -> Self {
        Self {
            max_batch_size: 100,
            linger_ms: 1,
        }
    }

    /// Configuration optimized for high throughput
    pub fn high_throughput() -> Self {
        Self {
            max_batch_size: 5000,
            linger_ms: 50,
        }
    }

    /// Disable batching (for debugging)
    pub fn no_batching() -> Self {
        Self {
            max_batch_size: 1,
            linger_ms: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = RaftBatchConfig::default();
        assert_eq!(config.max_batch_size, 1000);
        assert_eq!(config.linger_ms, 10);
    }

    #[test]
    fn test_low_latency_config() {
        let config = RaftBatchConfig::low_latency();
        assert!(config.max_batch_size < RaftBatchConfig::default().max_batch_size);
        assert!(config.linger_ms < RaftBatchConfig::default().linger_ms);
    }

    #[test]
    fn test_high_throughput_config() {
        let config = RaftBatchConfig::high_throughput();
        assert!(config.max_batch_size > RaftBatchConfig::default().max_batch_size);
        assert!(config.linger_ms > RaftBatchConfig::default().linger_ms);
    }
}
