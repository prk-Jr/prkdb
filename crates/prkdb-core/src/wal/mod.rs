pub mod adaptive;
pub mod async_fsync;
pub mod async_log_segment;
pub mod async_parallel_wal;
pub mod batch;
pub mod buffer_pool;
pub mod compaction;
pub mod compression;
pub mod config;
pub mod frame;
pub mod log;
pub mod log_record;
pub mod log_segment;
pub mod metrics;
pub mod mmap_log_segment;
pub mod mmap_parallel_wal;
pub mod offset_index;
pub mod parallel_wal;
pub mod segment;
pub mod write_ahead_log;

pub use compression::{
    compress, decompress, decompress_bounded, CompressionConfig, CompressionError, CompressionType,
};
pub use config::{CompactionPolicy, SyncMode, WalConfig};
pub use frame::Lsn;
pub use log::{CommitHook, PendingAppend, RecoveryReport, Reservation, Wal, WalHealth, WalOptions};
pub use log_record::{LogOperation, LogRecord};
pub use log_segment::LogSegment;
pub use segment::RecordLoc;

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

    #[error("unsupported WAL format {found} in {path}; this build reads format {supported}")]
    UnsupportedFormat {
        path: std::path::PathBuf,
        found: u32,
        supported: u32,
    },

    #[error("corrupt WAL: {path} at byte {offset}: {reason}")]
    CorruptSegment {
        path: std::path::PathBuf,
        offset: u64,
        reason: String,
    },

    /// A caller's `replay` closure (passed to `Wal::open`) failed to decode a frame whose
    /// own CRC/LSN checks passed. Distinct from `CorruptSegment` (a fault the WAL's own
    /// frame/segment scan found) so callers can tell "the WAL bytes are fine, the payload
    /// inside them is not" apart from "the WAL itself is corrupt" (spec §8: refuse to
    /// open, name the file).
    #[error("WAL replay failed for lsn {lsn} in {path} at byte {offset}: {source}")]
    ReplayFailed {
        path: std::path::PathBuf,
        offset: u64,
        lsn: Lsn,
        #[source]
        source: Box<WalError>,
    },

    #[error("record of {len} bytes in {path} exceeds the {max}-byte limit")]
    RecordTooLarge {
        path: std::path::PathBuf,
        len: usize,
        max: usize,
    },

    #[error("WAL is poisoned by an earlier I/O failure and accepts no more writes: {0}")]
    Poisoned(String),

    #[error("WAL is closed")]
    Closed,
}
pub use offset_index::OffsetIndex;
pub use parallel_wal::ParallelWal;
pub use write_ahead_log::WriteAheadLog;
