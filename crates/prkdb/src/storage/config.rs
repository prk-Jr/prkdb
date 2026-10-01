use prkdb_core::batching::adaptive::AdaptiveBatchConfig;
use prkdb_core::wal::compaction::CompactionConfig;
use prkdb_core::wal::WalConfig;
use std::path::PathBuf;

/// When a write is acknowledged. One knob, on `WalConfig::sync_mode`: the old
/// `StorageConfig::sync_mode` was a second setting nothing read (root cause 4, D12).
pub use prkdb_core::wal::SyncMode;

/// Configuration for the storage engine
#[derive(Debug, Clone)]
pub struct StorageConfig {
    /// WAL configuration
    pub wal: WalConfig,

    /// Cache capacity (number of items)
    pub cache_capacity: usize,

    /// Compaction configuration. Not read until Task 2.15 adds compaction for the single
    /// WAL: the old `Compactor` drove the mmap WAL, which the adapter no longer uses.
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
