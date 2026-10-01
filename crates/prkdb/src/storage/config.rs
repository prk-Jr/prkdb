use prkdb_core::batching::adaptive::AdaptiveBatchConfig;
use prkdb_core::wal::WalConfig;
use std::path::PathBuf;
use std::time::Duration;

/// When a write is acknowledged. One knob, on `WalConfig::sync_mode`: the old
/// `StorageConfig::sync_mode` was a second setting nothing read (root cause 4, D12).
pub use prkdb_core::wal::SyncMode;

/// When to compact the write-ahead log.
///
/// Defined here rather than in `prkdb-core`'s compaction module, which drives the old mmap
/// WAL and goes away in Task 2.9. Not read until Task 2.15 adds compaction for the single
/// WAL (which also adds `min_dead_ratio` and drops `keep_segments`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionConfig {
    /// Minimum size of the WAL before compaction is considered (bytes).
    pub min_wal_size_bytes: u64,
    /// Minimum time between compaction runs.
    pub min_interval: Duration,
    /// Number of segments to keep (history).
    pub keep_segments: usize,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            min_wal_size_bytes: 100 * 1024 * 1024,  // 100 MB
            min_interval: Duration::from_secs(300), // 5 minutes
            keep_segments: 2,
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

    /// Compaction configuration. Not read until Task 2.15 adds compaction for the single
    /// WAL.
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

    /// The move out of `prkdb-core` keeps the defaults the type had there.
    #[test]
    fn compaction_defaults_are_unchanged_by_the_move() {
        let config = CompactionConfig::default();
        assert_eq!(config.min_wal_size_bytes, 100 * 1024 * 1024);
        assert_eq!(config.min_interval, Duration::from_secs(300));
        assert_eq!(config.keep_segments, 2);
        assert_eq!(StorageConfig::default().compaction, config);
    }
}
