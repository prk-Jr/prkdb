pub mod adaptive;
pub mod batch;
pub mod compression;
pub mod config;
pub mod frame;
pub mod log;
pub mod log_record;
pub mod log_state;
pub mod records;
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

/// `UnsupportedFormat`'s message: the two format numbers for a segment of another
/// version, or, for a CRC-valid frame of an unknown kind (whose segment format matches),
/// the kind and where it is.
fn unsupported_format_message(
    path: &std::path::Path,
    found: u32,
    supported: u32,
    frame_kind: &Option<(u64, u8)>,
) -> String {
    match frame_kind {
        Some((offset, kind)) => format!(
            "unsupported WAL frame kind {kind} in {} at byte {offset}: the frame is whole \
             (its CRC is valid), so a newer build wrote it; this build does not know kind \
             {kind}",
            path.display()
        ),
        None => format!(
            "unsupported WAL format {found} in {}; this build reads format {supported}",
            path.display()
        ),
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
    #[error("{}", unsupported_format_message(.path, *.found, *.supported, .frame_kind))]
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

    /// A record batch the codec refuses to encode (`records.rs`, Task 2.15b.2): no
    /// records or more than 65,536, a header name or header list too long for its
    /// length prefix, or an uncompressed body over `MAX_PAYLOAD_LEN`. Nothing was
    /// written; the caller fixes or splits the batch.
    #[error("invalid record batch: {0}")]
    InvalidRecords(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A segment of another format version names both versions.
    #[test]
    fn a_format_mismatch_names_both_versions() {
        let err = WalError::UnsupportedFormat {
            path: "/d/seg.wal".into(),
            found: 3,
            supported: 2,
            frame_kind: None,
        };
        assert_eq!(
            err.to_string(),
            "unsupported WAL format 3 in /d/seg.wal; this build reads format 2"
        );
    }

    /// STO-11: a CRC-valid frame of an unknown kind is named by its kind and offset,
    /// without the format numbers, which match (2.15b.1 review).
    #[test]
    fn an_unknown_frame_kind_is_named_by_kind() {
        let err = WalError::UnsupportedFormat {
            path: "/d/seg.wal".into(),
            found: 2,
            supported: 2,
            frame_kind: Some((40, 99)),
        };
        let msg = err.to_string();
        assert!(
            msg.starts_with("unsupported WAL frame kind 99 in /d/seg.wal at byte 40"),
            "{msg}"
        );
        assert!(!msg.contains("format"), "{msg}");
    }
}
