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

/// When a write is acknowledged (spec §6.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SyncMode {
    /// Ack after the group-commit batch containing the write is fsynced.
    #[default]
    Durable,
    /// Ack after the write reaches the OS. A sync starts once `sync_interval_ms` has
    /// passed since the oldest unsynced write, at the writer's next batch boundary or
    /// idle wake-up.
    ///
    /// `sync_interval_ms` is a target, not a bound on what a power cut can take: the sync
    /// starts only when the writer gets to it and then takes as long as the disk takes,
    /// so under load or on a slow device the unsynced window is longer. The guarantee is
    /// `Wal::durable_lsn`: every write at or below it survives a power cut, and any
    /// acknowledged write above it may be lost. `Wal::sync` (the storage adapter's
    /// `flush`) makes every write acknowledged so far durable.
    Fast,
}

/// What may release segments from the front of the log (Task 2.15b.1, streaming log
/// design note §8.3). Fixed when the `Wal` opens.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FrontRelease {
    /// Keyed directories: only segments compaction fully elided may be released, and
    /// `Wal::open` refuses a segment below the log start that still holds a live frame.
    #[default]
    ElidedOnly,
    /// Stream directories: retention releases sealed segments whatever they hold, and
    /// `Wal::open` removes any whole segment left below the log start. The data in them
    /// is deleted on purpose. Only the stream sets this; `WalOptions::from_config` never
    /// does.
    Retention,
}

impl SyncMode {
    /// The old name for [`SyncMode::Fast`].
    #[deprecated(note = "renamed to SyncMode::Fast")]
    #[allow(non_upper_case_globals)]
    pub const Performance: SyncMode = SyncMode::Fast;
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

    /// Workload profile (default: Balanced)
    pub workload_profile: WorkloadProfile,

    /// Adaptive batching configuration
    pub adaptive_config: AdaptiveBatchConfig,

    /// Acknowledgement policy. Default `Durable` everywhere, including `test_config()`
    /// (controller decision: Durable is the default everywhere, spec §6.2).
    pub sync_mode: SyncMode,
    /// Fast mode's sync target in milliseconds (default 10): how long after the oldest
    /// unsynced write a sync is started. Not a bound on what a power cut can lose; see
    /// [`SyncMode::Fast`].
    pub sync_interval_ms: u64,
    /// Largest group-commit write (default 16 MiB).
    pub max_batch_bytes: usize,
    /// Admission bound: bytes queued for the writer before appenders wait (default 64 MiB).
    pub max_queued_bytes: usize,
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
            workload_profile: WorkloadProfile::Balanced,
            adaptive_config: AdaptiveBatchConfig::default(),
            sync_mode: SyncMode::Durable,
            sync_interval_ms: 10,
            max_batch_bytes: 16 * 1024 * 1024,
            max_queued_bytes: 64 * 1024 * 1024,
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
            workload_profile: WorkloadProfile::Balanced,
            adaptive_config: AdaptiveBatchConfig::default(),
            sync_mode: SyncMode::Durable,
            sync_interval_ms: 10,
            max_batch_bytes: 16 * 1024 * 1024,
            max_queued_bytes: 64 * 1024 * 1024,
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
            workload_profile: WorkloadProfile::Balanced,
            adaptive_config: AdaptiveBatchConfig::default(),
            sync_mode: SyncMode::Durable,
            sync_interval_ms: 10,
            max_batch_bytes: 16 * 1024 * 1024,
            max_queued_bytes: 64 * 1024 * 1024,
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
            workload_profile: WorkloadProfile::Balanced,
            adaptive_config: AdaptiveBatchConfig::default(),
            sync_mode: SyncMode::Durable,
            sync_interval_ms: 10,
            max_batch_bytes: 16 * 1024 * 1024,
            max_queued_bytes: 64 * 1024 * 1024,
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
            workload_profile: WorkloadProfile::Balanced,
            adaptive_config: AdaptiveBatchConfig::default(),
            sync_mode: SyncMode::Durable,
            sync_interval_ms: 10,
            max_batch_bytes: 16 * 1024 * 1024,
            max_queued_bytes: 64 * 1024 * 1024,
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
