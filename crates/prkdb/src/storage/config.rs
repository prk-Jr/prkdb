use prkdb_core::batching::adaptive::AdaptiveBatchConfig;
use prkdb_core::wal::WalConfig;
use std::path::PathBuf;
use std::time::Duration;

/// When a write is acknowledged. One knob, on `WalConfig::sync_mode`: the old
/// `StorageConfig::sync_mode` was a second setting nothing read (root cause 4, D12).
pub use prkdb_core::wal::SyncMode;

/// When the background task compacts the write-ahead log (Task 2.15).
///
/// When a tokio runtime exists at open, the adapter starts a task that wakes every
/// `min_interval` and runs a compaction if the log's segments total at least
/// `min_wal_size_bytes` and at least `min_dead_ratio` of the sealed segments' bytes could
/// be reclaimed. `WalStorageAdapter::compact` runs one on demand regardless. A zero
/// `min_interval`, or `min_wal_size_bytes: u64::MAX`, turns the background task off.
///
/// Checking the dead ratio reads every sealed segment once (a dry run of compaction's
/// liveness pass), so it happens only once the size threshold is met.
#[derive(Debug, Clone, PartialEq)]
pub struct CompactionConfig {
    /// Minimum total size of the WAL's segments before compaction is considered (bytes).
    pub min_wal_size_bytes: u64,
    /// Time between background checks (and so between background runs).
    pub min_interval: Duration,
    /// Minimum fraction (0.0..=1.0) of the sealed segments' bytes that a compaction would
    /// reclaim before the background task runs one.
    pub min_dead_ratio: f64,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            min_wal_size_bytes: 100 * 1024 * 1024,  // 100 MB
            min_interval: Duration::from_secs(300), // 5 minutes
            min_dead_ratio: 0.5,
        }
    }
}

/// Configuration for the storage engine
#[derive(Debug, Clone)]
pub struct StorageConfig {
    /// WAL configuration
    pub wal: WalConfig,

    /// Cache capacity (number of items)
    pub cache_capacity: usize,

    /// When the background task compacts the WAL (see [`CompactionConfig`]).
    pub compaction: CompactionConfig,

    /// Batching configuration. Only `max_flush_ms` is still read: the write path's
    /// liveness bounds derive from it.
    pub batching: AdaptiveBatchConfig,
}

impl StorageConfig {
    /// Create a new configuration with defaults for a given directory
    pub fn new(log_dir: PathBuf) -> Self {
        Self {
            wal: WalConfig {
                log_dir,
                ..WalConfig::default()
            },
            cache_capacity: 100_000, // Default 100k items for production workloads
            compaction: CompactionConfig::default(),
            batching: AdaptiveBatchConfig::default(),
        }
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self::new(PathBuf::from("prkdb_data"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The thresholds Task 2.15 documents: 100 MB, every 5 minutes, half reclaimable.
    #[test]
    fn compaction_defaults_are_the_documented_ones() {
        let config = CompactionConfig::default();
        assert_eq!(config.min_wal_size_bytes, 100 * 1024 * 1024);
        assert_eq!(config.min_interval, Duration::from_secs(300));
        assert_eq!(config.min_dead_ratio, 0.5);
        assert_eq!(StorageConfig::default().compaction, config);
    }
}
