use crate::wal::adaptive::{AdaptiveBatchConfig, WorkloadProfile};
use crate::wal::compression::CompressionConfig;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Compaction policy for log segments
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum CompactionPolicy {
    None,
    TimeWindow(u64), // milliseconds
    SizeWindow(u64), // bytes
}

/// Configuration for Write-Ahead Log
#[derive(Debug, Clone)]
pub struct WalConfig {
    /// Maximum size of a single segment file (default: 1GB)
    /// Directory for log files
    pub log_dir: PathBuf,

    /// Max size per segment (default: 1GB)
    pub segment_bytes: u64,

    /// Interval for sparse index (default: 4KB)
    pub index_interval_bytes: u64,

    /// Retention policy (milliseconds, None = infinite)
    pub retention_ms: Option<u64>,

    /// Compaction policy
    pub compaction: CompactionPolicy,

    /// Compression configuration (NEW)
    pub compression: CompressionConfig,

    /// Max batch size for group commit (default: 100)
    pub batch_size: usize,

    /// Max time to wait for batch flush (milliseconds, default: 10)
    pub flush_interval_ms: u64,

    /// Number of parallel segments (default: 4)
    pub segment_count: usize,

    /// Number of WAL shards for parallel writes (default: 16)
    /// Valid range: 1-32. Higher values = more parallelism but more overhead.
    /// Recommended: 8-16 for most workloads, matching CPU core count.
    /// Set to 1 to disable sharding (single WAL mode).
    pub shard_count: Option<usize>,

    /// Workload profile (default: Balanced)
    pub workload_profile: WorkloadProfile,

    /// Adaptive batching configuration
    pub adaptive_config: AdaptiveBatchConfig,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            log_dir: PathBuf::from("./wal"),
            segment_bytes: 1024 * 1024 * 1024, // 1GB
            index_interval_bytes: 4096,        // 4KB
            retention_ms: None,
            compaction: CompactionPolicy::None,
            compression: CompressionConfig::default(), // LZ4 by default
            batch_size: 100,
            flush_interval_ms: 10,
            segment_count: 4,
            shard_count: Some(16), // 16 WAL shards for maximum parallelism
            workload_profile: WorkloadProfile::Balanced,
            adaptive_config: AdaptiveBatchConfig::default(),
        }
    }
}

impl WalConfig {
    /// Apply a workload profile to this configuration
    pub fn apply_profile(&mut self, profile: WorkloadProfile) {
        self.workload_profile = profile;
        self.adaptive_config = profile.to_config();

        // Update static fields for backward compatibility / initial values
        self.batch_size = self.adaptive_config.initial_batch_size;
        self.flush_interval_ms = self.adaptive_config.initial_timeout.as_millis() as u64;
    }

    /// Config for testing (smaller segments, no compression)
    pub fn test_config() -> Self {
        Self {
            log_dir: PathBuf::from("./test_wal"),
            segment_bytes: 1024 * 1024, // 1MB
            index_interval_bytes: 1024, // 1KB
            retention_ms: None,
            compaction: CompactionPolicy::None,
            compression: CompressionConfig::none(),
            batch_size: 10,
            flush_interval_ms: 10,
            segment_count: 4,
            shard_count: Some(4), // Fewer shards for testing
            workload_profile: WorkloadProfile::Balanced,
            adaptive_config: AdaptiveBatchConfig::default(),
        }
    }

    /// Config for benchmarking (larger segments, no compression)
    pub fn benchmark_config() -> Self {
        Self {
            log_dir: PathBuf::from("./bench_wal"),
            segment_bytes: 1024 * 1024 * 1024, // 1GB
            index_interval_bytes: 4096,        // 4KB
            retention_ms: None,
            compaction: CompactionPolicy::None,
            compression: CompressionConfig::none(),
            batch_size: 1000,
            flush_interval_ms: 10,
            segment_count: 8,
            shard_count: Some(16), // Maximum sharding for benchmarks
            workload_profile: WorkloadProfile::Balanced,
            adaptive_config: AdaptiveBatchConfig::default(),
        }
    }

    /// Config for production (compression enabled)
    pub fn production_config() -> Self {
        Self {
            log_dir: PathBuf::from("./wal"),
            segment_bytes: 1024 * 1024 * 1024, // 1GB
            index_interval_bytes: 4096,        // 4KB
            retention_ms: None,
            compaction: CompactionPolicy::None,
            compression: CompressionConfig::default(),
            batch_size: 100,
            flush_interval_ms: 10,
            segment_count: 4,
            shard_count: Some(16), // 16 shards for production
            workload_profile: WorkloadProfile::Balanced,
            adaptive_config: AdaptiveBatchConfig::default(),
        }
    }

    /// Config optimized for compression ratio
    pub fn compression_optimized(log_dir: PathBuf) -> Self {
        Self {
            log_dir,
            segment_bytes: 1024 * 1024 * 1024,
            index_interval_bytes: 4096,
            retention_ms: None,
            compaction: CompactionPolicy::None,
            compression: CompressionConfig::compression_optimized(),
            batch_size: 100,
            flush_interval_ms: 10,
            segment_count: 4,
            shard_count: Some(16), // 16 shards
            workload_profile: WorkloadProfile::Balanced,
            adaptive_config: AdaptiveBatchConfig::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = WalConfig::default();
        assert_eq!(config.segment_bytes, 1024 * 1024 * 1024);
        assert_eq!(
            config.compression.compression_type,
            crate::wal::compression::CompressionType::Lz4
        );
    }

    #[test]
    fn test_test_config() {
        let config = WalConfig::test_config();
        assert_eq!(config.segment_bytes, 1024 * 1024);
        assert_eq!(
            config.compression.compression_type,
            crate::wal::compression::CompressionType::None
        );
    }
}
