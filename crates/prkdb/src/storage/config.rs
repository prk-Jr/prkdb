use prkdb_core::batching::adaptive::AdaptiveBatchConfig;
use prkdb_core::wal::compaction::CompactionConfig;
use prkdb_core::wal::WalConfig;
use std::path::PathBuf;

/// Synchronization mode for WAL writes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncMode {
    /// Durable mode: fsync after every write (safest, slower)
    Durable,
    /// Performance mode: rely on OS page cache and background flush (faster, less safe)
    Performance,
}

/// Configuration for the storage engine
#[derive(Debug, Clone)]
pub struct StorageConfig {
    /// WAL configuration
    pub wal: WalConfig,

    /// Cache capacity (number of items)
    pub cache_capacity: usize,

    /// Compaction configuration
    pub compaction: CompactionConfig,

    /// Batching configuration
    pub batching: AdaptiveBatchConfig,

    /// Synchronization mode
    pub sync_mode: SyncMode,
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
            sync_mode: SyncMode::Performance, // Default to performance for now
        }
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self::new(PathBuf::from("prkdb_data"))
    }
}
