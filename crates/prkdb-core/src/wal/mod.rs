pub mod adaptive;
pub mod batch;
pub mod compression;
pub mod config;
pub mod frame;
pub mod log;
pub mod log_record;
pub mod segment;

pub use compression::{
    compress, decompress, decompress_bounded, CompressionConfig, CompressionError, CompressionType,
};
pub use config::{CompactionPolicy, SyncMode, WalConfig};
pub use frame::Lsn;
pub use log::{
    CommitHook, PendingAppend, RecoveryReport, Reservation, SealedSegment, Wal, WalHealth,
    WalOptions,
};
pub use log_record::{LogOperation, LogRecord};
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

    /// The frame at a location is not the one asked for, and the segment holding it was
    /// rewritten or removed by compaction since this `Wal` was opened (Task 2.15): the
    /// location is stale, not the log corrupt. The caller re-resolves the location (the
    /// storage adapter re-reads its index) and retries. A mismatch in a segment compaction
    /// never touched stays `CorruptSegment`.
    #[error(
        "stale WAL location: lsn {lsn} at byte {offset} of {path} moved when compaction rewrote the segment"
    )]
    Moved {
        path: std::path::PathBuf,
        offset: u64,
        lsn: Lsn,
    },

    /// A compaction step's precondition did not hold (a segment that is not sealed, a
    /// replacement file whose LSN range differs from the segment it would replace, a
    /// segment to remove that still holds a live frame). Nothing was changed.
    #[error("compaction refused: {0}")]
    CompactionRefused(String),
}
