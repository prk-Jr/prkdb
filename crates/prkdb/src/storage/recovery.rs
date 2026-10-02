//! Opening the log and rebuilding the index (Tasks 2.8a, 2.14), plus health checks and
//! backups for the single WAL.
//!
//! Recovery is [`open_and_recover`], which every constructor runs: load the newest valid
//! index checkpoint, then `Wal::open` validating every frame kind and applying only
//! frames above the checkpoint. It scans and CRC-checks every segment, so a torn
//! tail on the last segment is truncated and corruption anywhere earlier refuses the open
//! naming the file (STO-04), exactly as without a checkpoint. A checkpoint that turns out
//! invalid is ignored with a warning naming the file, and recovery falls back to an older
//! one or to a full replay. Nothing repairs mid-log corruption automatically.

use super::checkpoint::{self, Decoded};
use papaya::HashMap as LockFreeHashMap;
use prkdb_core::vfs::{OpenMode, Vfs};
use prkdb_core::wal::batch::{Batch, BatchOp};
use prkdb_core::wal::frame::FrameKind;
use prkdb_core::wal::segment::{parse_segment_file_name, scan_segment, SEGMENT_HEADER_LEN};
use prkdb_core::wal::{Lsn, RecordLoc, Wal, WalError, WalOptions};
use prkdb_types::error::StorageError;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::{info, instrument, warn};

/// What the last open did to rebuild the index
/// ([`WalStorageAdapter::last_recovery`](super::WalStorageAdapter::last_recovery)).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RecoveryStats {
    /// The LSN covered by the checkpoint the index was loaded from; `None` = full replay.
    pub checkpoint_lsn: Option<Lsn>,
    /// Index entries loaded from that checkpoint.
    pub checkpoint_entries: u64,
    /// Frames applied to the index on top of the checkpoint (every frame on a full
    /// replay).
    pub frames_replayed: u64,
    /// Frames `Wal::open` scanned and CRC-checked: every frame in the log, checkpoint or
    /// not.
    pub frames_scanned: u64,
    /// Set when the last segment ended in a torn frame and was truncated: path, offset
    /// and fault.
    pub truncated: Option<String>,
    /// Checkpoints that were ignored, each with the reason (also logged at `warn`).
    pub rejected_checkpoints: Vec<String>,
}

type Index = LockFreeHashMap<Vec<u8>, RecordLoc>;

/// Applies one frame to the index: puts insert the frame's location, deletes remove it.
fn apply_frame(
    index: &Index,
    loc: RecordLoc,
    kind: FrameKind,
    payload: &[u8],
) -> Result<(), WalError> {
    if kind == FrameKind::Records {
        return Err(WalError::Corruption(
            "key/value directory contains a Records frame".into(),
        ));
    }
    if kind != FrameKind::Batch {
        return Ok(());
    }
    let pinned = index.pin();
    for op in Batch::decode(payload)?.ops {
        match op {
            BatchOp::Put { key, .. } => {
                pinned.insert(key, loc);
            }
            BatchOp::Delete { key } => {
                pinned.remove(&key);
            }
        }
    }
    Ok(())
}

fn reject(stats: &mut RecoveryStats, path: &Path, reason: impl std::fmt::Display) {
    warn!(
        path = %path.display(),
        %reason,
        "ignoring index checkpoint; recovery falls back to an older checkpoint or a full replay"
    );
    stats
        .rejected_checkpoints
        .push(format!("{}: {reason}", path.display()));
}

/// Loads a decoded checkpoint into the (empty) index; returns the LSN it covers.
fn load_into(index: &Index, checkpoint: Decoded, stats: &mut RecoveryStats) -> Lsn {
    let (covered, entries) = checkpoint;
    stats.checkpoint_lsn = Some(covered);
    stats.checkpoint_entries = entries.len() as u64;
    let pinned = index.pin();
    for (key, loc) in entries {
        pinned.insert(key, loc);
    }
    covered
}

/// Opens the log at `log_dir` and rebuilds `index` (which must be empty) from the newest
/// valid checkpoint plus the frames after it, or from a full replay.
///
/// 1. Checkpoints are listed newest first; the first one that reads and decodes (CRC,
///    magic, format, entry sanity, name matches contents) is loaded into the index. Ones
///    that do not are rejected.
/// 2. `Wal::open` scans every segment and validates every frame kind, including frames
///    covered by the checkpoint. Only frames after it are applied to the index.
/// 3. The loaded checkpoint's locations are checked against the opened log
///    ([`checkpoint::validate_against_log`]). If that fails, the index is cleared and
///    rebuilt from the next older checkpoint that passes, or from a full replay, by
///    scanning the now-open log.
///
/// A checkpoint is never trusted over the log: every way it can be wrong ends in a
/// rejection and a replay, never in a missing key.
pub(crate) fn open_and_recover(
    vfs: Arc<dyn Vfs>,
    log_dir: &Path,
    opts: WalOptions,
    index: &Index,
) -> Result<(Wal, RecoveryStats), WalError> {
    let mut stats = RecoveryStats::default();
    let mut candidates = match checkpoint::list_checkpoints(vfs.as_ref(), log_dir) {
        Ok(found) => found.into_iter(),
        Err(e) => {
            reject(&mut stats, &checkpoint::checkpoint_dir(log_dir), e);
            Vec::new().into_iter()
        }
    };

    // The entries go into the index before replay, which applies every later put and
    // delete on top of them; only their locations are kept for the check after open.
    let mut replay_from = 1;
    let mut pending_check: Option<(PathBuf, Lsn, Vec<RecordLoc>)> = None;
    for (named, path) in candidates.by_ref() {
        match checkpoint::read_checkpoint(vfs.as_ref(), &path, named) {
            Ok(decoded) => {
                let locations = decoded.1.iter().map(|(_, loc)| *loc).collect();
                replay_from = load_into(index, decoded, &mut stats) + 1;
                pending_check = Some((path, named, locations));
                break;
            }
            Err(e) => reject(&mut stats, &path, e),
        }
    }

    let mut replayed = 0u64;
    let (wal, report) = Wal::open(vfs.clone(), log_dir, opts, 1, &mut |loc, kind, payload| {
        // A checkpoint skips index application, never frame-kind validation.
        // Records in a keyed log must refuse even when covered by the checkpoint.
        if kind == FrameKind::Records || loc.lsn >= replay_from {
            if loc.lsn >= replay_from {
                replayed += 1;
            }
            apply_frame(index, loc, kind, payload)?;
        }
        Ok(())
    })?;
    stats.frames_replayed = replayed;
    stats.frames_scanned = report.frames;
    stats.truncated = report
        .truncated
        .as_ref()
        .map(|(path, offset, fault)| format!("{} at byte {offset}: {fault:?}", path.display()));

    let Some((path, covered, locations)) = pending_check else {
        return Ok((wal, stats));
    };
    let last_lsn = wal.next_lsn().saturating_sub(1);
    let segments = wal.segments();
    let Err(reason) = checkpoint::validate_against_log(covered, &locations, last_lsn, &segments)
    else {
        return Ok((wal, stats));
    };

    // The loaded checkpoint does not fit this log: start over on the open log.
    reject(&mut stats, &path, reason);
    index.pin().clear();
    stats.checkpoint_lsn = None;
    stats.checkpoint_entries = 0;
    let mut from = 1;
    for (named, path) in candidates {
        let older = match checkpoint::read_checkpoint(vfs.as_ref(), &path, named) {
            Ok(older) => older,
            Err(e) => {
                reject(&mut stats, &path, e);
                continue;
            }
        };
        let locations: Vec<RecordLoc> = older.1.iter().map(|(_, loc)| *loc).collect();
        if let Err(reason) =
            checkpoint::validate_against_log(older.0, &locations, last_lsn, &segments)
        {
            reject(&mut stats, &path, reason);
            continue;
        }
        from = load_into(index, older, &mut stats) + 1;
        break;
    }
    let mut replayed = 0u64;
    wal.scan_from(from, &mut |loc, kind, payload| {
        replayed += 1;
        apply_frame(index, loc, kind, payload)
    })?;
    stats.frames_replayed = replayed;
    Ok((wal, stats))
}

/// Manages health checks and backups for the storage engine
pub struct RecoveryManager {
    vfs: Arc<dyn Vfs>,
    log_dir: PathBuf,
}

impl RecoveryManager {
    pub fn new(vfs: Arc<dyn Vfs>, log_dir: PathBuf) -> Self {
        Self { vfs, log_dir }
    }

    /// Re-scan every segment and verify every frame (CRC and LSN).
    ///
    /// Returns `Err(StorageError::Corruption)` naming the segment file for any fault in a
    /// sealed segment. The active (highest-numbered) segment is allowed a torn tail or a
    /// missing header, which a write or roll in flight produces; a bad header there is
    /// still corruption.
    #[instrument(skip(self))]
    pub async fn check_health(&self) -> Result<(), StorageError> {
        info!("Starting WAL health check");
        let result = self.scan_all();
        match &result {
            Ok(()) => info!("WAL health check passed"),
            Err(e) => warn!("WAL health check failed: {}", e),
        }
        result
    }

    fn scan_all(&self) -> Result<(), StorageError> {
        let entries = self
            .vfs
            .read_dir(&self.log_dir)
            .map_err(|e| StorageError::Corruption(format!("{}: {e}", self.log_dir.display())))?;
        let mut segments: Vec<(Lsn, PathBuf)> = entries
            .into_iter()
            .filter_map(|path| {
                let first = path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .and_then(parse_segment_file_name)?;
                Some((first, path))
            })
            .collect();
        segments.sort();

        let last = segments.len().saturating_sub(1);
        for (idx, (first_lsn, path)) in segments.into_iter().enumerate() {
            let active = idx == last;
            let file = self
                .vfs
                .open(&path, OpenMode::Read)
                .map_err(|e| StorageError::Corruption(format!("{}: {e}", path.display())))?;
            // The active segment may be mid-roll (created, header not yet written) or
            // mid-write (a frame partly written at its tail). Neither is corruption on a
            // running database; the open path handles both after a crash.
            let len = file
                .len()
                .map_err(|e| StorageError::Corruption(format!("{}: {e}", path.display())))?;
            if active && len < SEGMENT_HEADER_LEN {
                continue;
            }
            let scan = scan_segment(&*file, &path, first_lsn, &mut |_, _, _| Ok(()))
                .map_err(|e| StorageError::Corruption(e.to_string()))?;
            if let Some((offset, fault)) = scan.stopped.filter(|_| !active) {
                return Err(StorageError::Corruption(format!(
                    "{} at byte {offset}: {fault:?}",
                    path.display()
                )));
            }
        }
        Ok(())
    }

    /// There is no in-place repair.
    ///
    /// The old `repair_segments` truncated each segment at its first bad record, wherever
    /// it was, silently discarding every record after it. The single WAL's rules forbid
    /// that: a torn tail is truncated by the open path, and anything else refuses to open.
    #[instrument(skip(self))]
    pub async fn recover(&self) -> Result<(), StorageError> {
        Err(StorageError::Recovery(
            "run the database open path; torn tails are truncated there and mid-log \
             corruption is not repaired automatically"
                .to_string(),
        ))
    }

    /// Create a backup of the current WAL state
    #[instrument(skip(self), fields(backup_dir = %backup_dir.display()))]
    pub async fn create_backup(&self, backup_dir: PathBuf) -> Result<(), StorageError> {
        info!("Creating WAL backup");

        // Create backup directory if it doesn't exist
        std::fs::create_dir_all(&backup_dir)
            .map_err(|e| StorageError::Internal(format!("Failed to create backup dir: {}", e)))?;

        // Copy all log files to backup directory
        let entries = std::fs::read_dir(&self.log_dir)
            .map_err(|e| StorageError::Internal(format!("Failed to read log dir: {}", e)))?;

        let mut file_count = 0;
        for entry in entries {
            let entry = entry
                .map_err(|e| StorageError::Internal(format!("Failed to read entry: {}", e)))?;
            let path = entry.path();

            // The lock file is not data, and Windows refuses reads of a locked file.
            if path.is_file() && !path.ends_with(super::lock::LOCK_FILE) {
                let filename = path
                    .file_name()
                    .ok_or_else(|| StorageError::Internal("Invalid filename".to_string()))?;
                let backup_path = backup_dir.join(filename);

                std::fs::copy(&path, &backup_path)
                    .map_err(|e| StorageError::Internal(format!("Failed to copy file: {}", e)))?;
                file_count += 1;
            }
        }

        info!(
            "WAL backup created successfully. Copied {} files",
            file_count
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prkdb_core::vfs::StdVfs;
    use prkdb_core::wal::segment::{segment_file_name, write_segment_header};

    fn manager(dir: &std::path::Path) -> RecoveryManager {
        RecoveryManager::new(Arc::new(StdVfs), dir.to_path_buf())
    }

    /// A roll in flight leaves the new, highest-numbered segment without a header; a
    /// write in flight leaves a partial frame at its tail. Neither is corruption.
    #[tokio::test]
    async fn the_active_segment_may_be_headerless_or_torn() {
        let dir = tempfile::tempdir().unwrap();
        let sealed = StdVfs
            .create(&dir.path().join(segment_file_name(1)))
            .unwrap();
        write_segment_header(&*sealed, 1).unwrap();
        let active = StdVfs
            .create(&dir.path().join(segment_file_name(2)))
            .unwrap();
        manager(dir.path())
            .check_health()
            .await
            .expect("a headerless active segment is a roll in flight");

        write_segment_header(&*active, 2).unwrap();
        active.write_at(SEGMENT_HEADER_LEN, &[0xAB; 7]).unwrap();
        manager(dir.path())
            .check_health()
            .await
            .expect("a torn tail on the active segment is a write in flight");
    }

    /// The same faults in a sealed segment are corruption, named by file.
    #[tokio::test]
    async fn a_sealed_segment_must_be_whole() {
        let dir = tempfile::tempdir().unwrap();
        let sealed = StdVfs
            .create(&dir.path().join(segment_file_name(1)))
            .unwrap();
        write_segment_header(&*sealed, 1).unwrap();
        sealed.write_at(SEGMENT_HEADER_LEN, &[0xAB; 7]).unwrap();
        let active = StdVfs
            .create(&dir.path().join(segment_file_name(2)))
            .unwrap();
        write_segment_header(&*active, 2).unwrap();

        let error = manager(dir.path())
            .check_health()
            .await
            .expect_err("a torn sealed segment is corruption");
        assert!(
            matches!(error, StorageError::Corruption(ref m) if m.contains(&segment_file_name(1))),
            "{error}"
        );
    }
}
