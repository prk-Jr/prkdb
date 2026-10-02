pub mod adaptive;
pub mod batch;
pub mod compression;
pub mod config;
pub mod frame;
pub mod log;
pub mod log_record;
pub mod log_state;
pub mod segment;

pub use compression::{
    compress, decompress, decompress_bounded, CompressionConfig, CompressionError, CompressionType,
};
pub use config::{CompactionPolicy, FrontRelease, SyncMode, WalConfig};
pub use frame::Lsn;
pub use log::{
    CommitHook, PendingAppend, RecoveryReport, Reservation, SealedSegment, Wal, WalHealth,
    WalOptions,
};
pub use log_record::{LogOperation, LogRecord};
pub use log_state::LogState;
pub use segment::{RecordLoc, ScanFlowVisitor, ScanVisitor};

fn frame_kind_note(frame_kind: &Option<(u64, u8)>) -> String {
    match frame_kind {
        Some((offset, kind)) => {
            format!(": the frame at byte {offset} has kind {kind}, which this build does not know")
        }
        None => String::new(),
    }
}

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

    /// A segment this build cannot read, refused and never modified: its header names
    /// another format version (`found`), or `frame_kind` is set: the frame at that byte
    /// offset has a valid CRC but a kind this build does not know, so a later build wrote
    /// it whole (STO-11). `found` is then the segment's own format number.
    #[error(
        "unsupported WAL format {found} in {path}{}; this build reads format {supported}",
        frame_kind_note(.frame_kind)
    )]
    UnsupportedFormat {
        path: std::path::PathBuf,
        found: u32,
        supported: u32,
        /// `(byte offset, kind)` of a CRC-valid frame of unknown kind.
        frame_kind: Option<(u64, u8)>,
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

    /// An append of an empty payload, refused before admission (STO-16): a frame of
    /// length 0 reads back as a torn tail, so recovery would truncate it and every frame
    /// after it.
    #[error("empty record refused in {path}: a WAL record must hold at least one byte")]
    EmptyRecord { path: std::path::PathBuf },

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
