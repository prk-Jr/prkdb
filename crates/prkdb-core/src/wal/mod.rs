pub mod adaptive;
pub mod async_fsync;
pub mod async_log_segment;
pub mod async_parallel_wal;
pub mod buffer_pool;
pub mod compaction;
pub mod compression;
pub mod config;
pub mod log_record;
pub mod log_segment;
pub mod metrics;
pub mod mmap_log_segment;
pub mod mmap_parallel_wal;
pub mod offset_index;
pub mod parallel_wal;
pub mod write_ahead_log;

pub use compression::{compress, decompress, CompressionConfig, CompressionError, CompressionType};
pub use config::{CompactionPolicy, WalConfig};
pub use log_record::{LogOperation, LogRecord};
pub use log_segment::LogSegment;

#[derive(Debug, thiserror::Error)]
pub enum WalError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("No active segment")]
    NoActiveSegment,

    #[error("Empty batch - cannot append empty batch")]
    EmptyBatch,

    #[error("Data corruption: {0}")]
    Corruption(String),

    #[error("Checksum mismatch: expected {expected}, found {found}")]
    ChecksumMismatch { expected: u32, found: u32 },

    #[error("Recovery failed: {0}")]
    Recovery(String),
}
pub use offset_index::OffsetIndex;
pub use parallel_wal::ParallelWal;
pub use write_ahead_log::WriteAheadLog;
