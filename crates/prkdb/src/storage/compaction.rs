//! Compaction of the single write-ahead log (Task 2.15, spec 2d): rewrite sealed segments
//! keeping only live records, make each rewrite durable, then drop segments that hold
//! nothing.
//!
//! [`WalStorageAdapter::compact`](super::WalStorageAdapter::compact) is the entry point;
//! this module holds the run itself.
//!
//! # What is live
//!
//! A `Put` op is live iff the index still points at its frame (`index[key].lsn ==
//! frame.lsn`); of several puts of one key in one frame only the last is kept, since
//! replay applies them in order. A `Delete` op is never live: every older put of its key
//! is already dead, and compaction runs as a prefix, oldest segment first (next section),
//! so those puts were rewritten away, durably, before the delete is dropped, and no replay
//! can resurrect them. A frame with no live op becomes an `Elided` frame (17-byte header,
//! empty payload, same LSN), so every segment keeps contiguous LSNs and recovery's
//! continuity check is unchanged. A frame whose every op is live is copied byte for byte;
//! one that loses some ops is re-encoded with the log's compression settings. Either way
//! it keeps its LSN.
//!
//! Trade-off: an elided record still costs its 17-byte header until every frame of its
//! segment is elided and the segment is removed from the front of the log.
//!
//! # A run
//!
//! One run at a time. A run takes the sealed segments (never the active one) and walks
//! them oldest first, always from the oldest: the prefix rule above is what makes dropping
//! a delete safe, so it is not an optimisation to relax. For each segment:
//!
//! 1. Liveness is computed against the live index (a dry pass; a segment with nothing to
//!    drop is left alone, and counts as done).
//! 2. `{first:020}.wal.compact` is written (a segment header, then every frame, live or
//!    elided) and `sync_data`ed. Liveness is computed again while writing; it can only
//!    have shrunk since step 1.
//! 3. **Sync the log** if anything the liveness pass consulted is not yet durable. In
//!    `SyncMode::Fast` the index publishes a frame once it is written, before it is
//!    synced, so an op can look dead because a newer, still unsynced write superseded it;
//!    dropping the old op and then losing power would lose both. After the sync every
//!    write that superseded a dropped op is durable. (In Durable mode the index only
//!    publishes synced frames, and the sync costs nothing.)
//! 4. Before the first rename of the run: **delete every index checkpoint** and sync
//!    `checkpoints/`. A rewrite keeps the segment's first LSN and therefore its file name,
//!    so a checkpoint taken before it would still pass recovery's check against the log
//!    (`checkpoint::validate_against_log`) with stale offsets. Deleting them before any
//!    rewritten segment is visible means a crash anywhere in the run leaves no
//!    checkpoint, which is a full replay, never a stale one.
//! 5. [`Wal::replace_segment`]: rename over `{first:020}.wal`, `sync_dir`, swap the read
//!    handle, and move the index entries of the segment's live puts to their new offsets
//!    with a compare-and-set (only an entry whose LSN is still the frame's moves; a key
//!    overwritten meanwhile keeps its newer location).
//!
//! Then every leading segment that is entirely elided is removed (`remove` + `sync_dir`,
//! oldest first; the log then starts at a later LSN), and, if anything changed, a fresh
//! checkpoint is written from the post-swap index. Never the other way round: a fresh
//! checkpoint written before the old ones are gone would leave a loadable stale one in the
//! window.
//!
//! A crash at any point leaves each segment either old or rewritten (rename is atomic, and
//! a rewrite is synced before it is renamed), rewritten segments always a prefix of the
//! sealed ones, and no checkpoint that predates a rewrite: recovery rebuilds exactly the
//! pre-compaction state.
//!
//! # What change-stream consumers see
//!
//! `get_changes_since` and `Wal::scan_from` read what is on disk. Frames removed by
//! compaction are gone: a rewritten frame yields only its surviving ops, an elided frame
//! yields nothing, and dropped deletes are not reported. Surviving ops keep their LSNs
//! (versions), so cursors stay valid and LSNs are never reused. Replaying the stream from
//! 0 after a compaction still rebuilds the current state exactly, but a consumer whose
//! cursor lies inside a compacted range can miss a delete (and the overwrites it
//! superseded) there, as with a compacted Kafka topic: such a consumer must resynchronise
//! from a snapshot rather than apply the stream incrementally.

use super::checkpoint;
use papaya::HashMap as LockFreeHashMap;
use prkdb_core::vfs::{Vfs, VfsFile};
use prkdb_core::wal::batch::{Batch, BatchOp};
use prkdb_core::wal::frame::{encode_frame, FrameKind};
use prkdb_core::wal::segment::{write_segment_header, SEGMENT_HEADER_LEN};
use prkdb_core::wal::{CompressionConfig, Lsn, RecordLoc, SealedSegment, Wal, WalError};
use prkdb_types::error::StorageError;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use tracing::{info, warn};

/// What one compaction run did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CompactionReport {
    /// Sealed segments replaced by a rewrite.
    pub segments_rewritten: usize,
    /// Fully elided segments removed from the front of the log.
    pub segments_removed: usize,
    /// Bytes of the sealed segments the run considered, before it.
    pub bytes_before: u64,
    /// Bytes of the same segments after it (0 for a removed one).
    pub bytes_after: u64,
}

/// A point in a compaction run, handed to the test hook of
/// [`WalStorageAdapter::compact_with_hook`](super::WalStorageAdapter::compact_with_hook)
/// right after the step completes. Each is a distinct on-disk state a crash can leave.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionStep {
    /// `{segment:020}.wal.compact` is written and synced; nothing visible has changed.
    CompactFileWritten { segment: Lsn },
    /// The log is synced: every write that superseded an op the rewrite drops is durable.
    LogSynced { segment: Lsn },
    /// Every index checkpoint is deleted and `checkpoints/` synced (once per run, before
    /// the first rename).
    CheckpointsDeleted,
    /// The rewrite is renamed over `{segment:020}.wal`, the directory synced, the read
    /// handle swapped and the index moved to the new offsets.
    SegmentReplaced { segment: Lsn },
    /// A leading, fully elided segment is removed and the directory synced.
    SegmentRemoved { segment: Lsn },
    /// Every segment is done; the fresh checkpoint is next.
    SegmentsDone,
    /// The fresh checkpoint is written; the run is complete.
    CheckpointWritten,
}

/// The test hook: called after each step; an `Err` aborts the run there (the step itself
/// has already happened).
pub(crate) type StepHook<'a> = dyn FnMut(CompactionStep) -> Result<(), String> + 'a;

pub(crate) type Index = LockFreeHashMap<Vec<u8>, RecordLoc>;

/// What a run needs from the adapter.
pub(crate) struct Ctx<'a> {
    pub wal: &'a Wal,
    pub index: &'a Index,
    pub vfs: &'a dyn Vfs,
    pub log_dir: &'a Path,
    pub compression: &'a CompressionConfig,
}

/// Suffix of a rewrite before it is renamed into place. `Wal::open` ignores such files
/// (they do not parse as segment names); a run removes leftovers before it starts.
const COMPACT_SUFFIX: &str = ".compact";

/// Rewrites are written in chunks of about this many bytes, so a 1 GiB segment is never
/// held in memory.
const WRITE_CHUNK: usize = 1 << 20;

fn compact_path(wal: &Wal, first_lsn: Lsn) -> PathBuf {
    let mut name = wal.segment_path(first_lsn).into_os_string();
    name.push(COMPACT_SUFFIX);
    PathBuf::from(name)
}

fn wal_storage_err(e: WalError) -> StorageError {
    match e {
        e @ (WalError::CorruptSegment { .. }
        | WalError::ReplayFailed { .. }
        | WalError::UnsupportedFormat { .. }) => StorageError::Corruption(e.to_string()),
        e => StorageError::Internal(format!("compaction: {e}")),
    }
}

fn io_err(path: &Path, e: std::io::Error) -> StorageError {
    StorageError::Internal(format!("compaction: {}: {e}", path.display()))
}

/// An index entry to move when the segment's rewrite is swapped in.
struct Relocation {
    key: Vec<u8>,
    lsn: Lsn,
    to: RecordLoc,
}

/// Liveness for one segment, and the rewrite it implies.
struct Plan {
    /// Whether the rewrite differs from the current file at all.
    changed: bool,
    /// Every frame of the rewrite is `Elided`.
    all_elided: bool,
    /// Length of the rewrite in bytes.
    new_len: u64,
    relocations: Vec<Relocation>,
    /// Every frame whose publication the liveness pass could have observed has an LSN at
    /// or below this (the log's last written LSN, read after the pass).
    consulted_upto: Lsn,
}

/// Where a rewrite goes while it is being computed: nowhere (a dry pass), or a file.
struct Sink<'f> {
    file: Option<&'f dyn VfsFile>,
    path: &'f Path,
    buf: Vec<u8>,
    /// File offset of `buf[0]`.
    flushed: u64,
}

impl Sink<'_> {
    fn offset(&self) -> u64 {
        self.flushed + self.buf.len() as u64
    }

    fn frame(&mut self, lsn: Lsn, kind: FrameKind, payload: &[u8]) -> Result<(), WalError> {
        if self.file.is_some() {
            encode_frame(&mut self.buf, lsn, kind, payload);
            if self.buf.len() >= WRITE_CHUNK {
                self.flush()?;
            }
        } else {
            self.flushed += (prkdb_core::wal::frame::FRAME_HEADER_LEN + payload.len()) as u64;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<(), WalError> {
        if let Some(file) = self.file {
            if !self.buf.is_empty() {
                file.write_at(self.flushed, &self.buf).map_err(|e| {
                    WalError::Io(std::io::Error::new(
                        e.kind(),
                        format!("{}: {e}", self.path.display()),
                    ))
                })?;
                self.flushed += self.buf.len() as u64;
                self.buf.clear();
            }
        }
        Ok(())
    }
}

/// Computes liveness for `seg` against the index and, when `out` is set, writes the
/// rewrite into it (header included; the caller syncs it).
fn plan_segment(
    ctx: &Ctx<'_>,
    seg: &SealedSegment,
    out: Option<(&dyn VfsFile, &Path)>,
) -> Result<Plan, WalError> {
    if let Some((file, _)) = out {
        write_segment_header(file, seg.first_lsn)?;
    }
    let mut sink = Sink {
        file: out.map(|(f, _)| f),
        path: out.map_or(ctx.log_dir, |(_, p)| p),
        buf: Vec::new(),
        flushed: SEGMENT_HEADER_LEN,
    };
    let mut changed = false;
    let mut all_elided = true;
    let mut relocations = Vec::new();
    let index = ctx.index.pin();

    ctx.wal
        .scan_sealed(seg.first_lsn, &mut |loc, kind, payload| {
            if kind == FrameKind::Elided {
                return sink.frame(loc.lsn, FrameKind::Elided, &[]);
            }
            let ops = Batch::decode(payload)?.ops;
            // Walking backwards: the first put of a key seen is the frame's last one for it,
            // the only one replay leaves in effect. A delete marks its key seen without being
            // kept (never live), so an earlier put of that key is not kept either.
            let mut seen: HashSet<&[u8]> = HashSet::new();
            let mut keep = vec![false; ops.len()];
            for (i, op) in ops.iter().enumerate().rev() {
                match op {
                    BatchOp::Put { key, .. } => {
                        if seen.insert(key.as_slice())
                            && index.get(key).is_some_and(|at| at.lsn == loc.lsn)
                        {
                            keep[i] = true;
                        }
                    }
                    BatchOp::Delete { key } => {
                        seen.insert(key.as_slice());
                    }
                }
            }
            let kept = keep.iter().filter(|k| **k).count();
            if kept == 0 {
                changed = true;
                return sink.frame(loc.lsn, FrameKind::Elided, &[]);
            }
            all_elided = false;
            let offset = sink.offset();
            let rewritten;
            let new_payload: &[u8] = if kept == ops.len() {
                payload
            } else {
                changed = true;
                let live: Vec<BatchOp> = ops
                    .iter()
                    .zip(&keep)
                    .filter(|(_, k)| **k)
                    .map(|(op, _)| op.clone())
                    .collect();
                rewritten = Batch { ops: live }.encode(ctx.compression)?;
                &rewritten
            };
            let to = RecordLoc {
                lsn: loc.lsn,
                segment: seg.first_lsn,
                offset,
                payload_len: new_payload.len() as u32,
            };
            for (op, k) in ops.iter().zip(&keep) {
                if let (BatchOp::Put { key, .. }, true) = (op, *k) {
                    relocations.push(Relocation {
                        key: key.clone(),
                        lsn: loc.lsn,
                        to,
                    });
                }
            }
            sink.frame(loc.lsn, FrameKind::Batch, new_payload)
        })?;
    sink.flush()?;
    // Read after the pass: every frame whose index effects the pass saw was written
    // before this load (a hook runs only after `next_lsn` covers its frame).
    let consulted_upto = ctx.wal.next_lsn().saturating_sub(1);
    Ok(Plan {
        changed,
        all_elided,
        new_len: sink.offset(),
        relocations,
        consulted_upto,
    })
}

/// Removes leftover `*.wal.compact` files from an interrupted run.
fn remove_stale_rewrites(ctx: &Ctx<'_>) -> Result<(), StorageError> {
    let listed = ctx
        .vfs
        .read_dir(ctx.log_dir)
        .map_err(|e| io_err(ctx.log_dir, e))?;
    let mut removed = false;
    for path in listed {
        let stale = path
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| n.ends_with(".wal.compact"));
        if stale {
            ctx.vfs.remove(&path).map_err(|e| io_err(&path, e))?;
            removed = true;
        }
    }
    if removed {
        ctx.vfs
            .sync_dir(ctx.log_dir)
            .map_err(|e| io_err(ctx.log_dir, e))?;
    }
    Ok(())
}

/// Bytes a compaction could reclaim now: `(sealed bytes, reclaimable bytes)`. A dry pass
/// over every sealed segment (reads only), for the background trigger's dead ratio.
pub(crate) fn reclaimable(ctx: &Ctx<'_>) -> Result<(u64, u64), StorageError> {
    let sealed = ctx.wal.sealed_segments().map_err(wal_storage_err)?;
    let mut total = 0u64;
    let mut reclaimable = 0u64;
    let mut leading = true;
    for seg in &sealed {
        total += seg.len;
        let plan = plan_segment(ctx, seg, None).map_err(wal_storage_err)?;
        leading &= plan.all_elided;
        reclaimable += if leading {
            seg.len
        } else {
            seg.len.saturating_sub(plan.new_len)
        };
    }
    Ok((total, reclaimable))
}

/// Deletes every checkpoint (and temp file) and syncs `checkpoints/`. Unlike the cleanup
/// after a checkpoint write, failures are errors: the run must not rename a rewrite while
/// a checkpoint that predates it can still be loaded.
fn delete_checkpoints(ctx: &Ctx<'_>) -> Result<(), StorageError> {
    let dir = checkpoint::checkpoint_dir(ctx.log_dir);
    if !ctx.vfs.exists(&dir).map_err(|e| io_err(&dir, e))? {
        return Ok(());
    }
    let listed = ctx.vfs.read_dir(&dir).map_err(|e| io_err(&dir, e))?;
    for path in &listed {
        ctx.vfs.remove(path).map_err(|e| io_err(path, e))?;
    }
    ctx.vfs.sync_dir(&dir).map_err(|e| io_err(&dir, e))
}

fn step(hook: &mut StepHook<'_>, at: CompactionStep) -> Result<(), StorageError> {
    hook(at).map_err(|why| {
        StorageError::Internal(format!(
            "compaction aborted by its test hook at {at:?}: {why}"
        ))
    })
}

/// One compaction run (see the module docs). The caller holds the adapter's compaction
/// lock and its checkpoint lock for the whole call. `write_checkpoint` writes a
/// checkpoint without taking that lock again. `stop` is polled between segments: when it
/// says so, the run ends early (consistently, without its fresh checkpoint).
pub(crate) fn run(
    ctx: &Ctx<'_>,
    hook: &mut StepHook<'_>,
    stop: &dyn Fn() -> bool,
    write_checkpoint: &dyn Fn() -> Result<(), StorageError>,
) -> Result<CompactionReport, StorageError> {
    remove_stale_rewrites(ctx)?;
    let sealed = ctx.wal.sealed_segments().map_err(wal_storage_err)?;
    let mut report = CompactionReport {
        bytes_before: sealed.iter().map(|s| s.len).sum(),
        ..CompactionReport::default()
    };
    let mut lengths: Vec<u64> = sealed.iter().map(|s| s.len).collect();
    let mut elided: Vec<bool> = Vec::with_capacity(sealed.len());
    let mut checkpoints_deleted = false;

    for (i, seg) in sealed.iter().enumerate() {
        if stop() {
            info!("compaction stopped early: the adapter is closing");
            report.bytes_after = lengths.iter().sum();
            return Ok(report);
        }
        let dry = plan_segment(ctx, seg, None).map_err(wal_storage_err)?;
        if !dry.changed {
            elided.push(dry.all_elided);
            continue;
        }

        let path = compact_path(ctx.wal, seg.first_lsn);
        let plan = {
            let file = ctx.vfs.create(&path).map_err(|e| io_err(&path, e))?;
            let plan = plan_segment(ctx, seg, Some((&*file, &path))).map_err(wal_storage_err)?;
            file.sync_data().map_err(|e| io_err(&path, e))?;
            plan
        };
        step(
            hook,
            CompactionStep::CompactFileWritten {
                segment: seg.first_lsn,
            },
        )?;

        if ctx.wal.durable_lsn() < plan.consulted_upto {
            let durable = ctx.wal.sync_blocking().map_err(wal_storage_err)?;
            if durable < plan.consulted_upto {
                return Err(StorageError::Internal(format!(
                    "compaction: the log synced to LSN {durable}, short of LSN {} that \
                     liveness for segment {} consulted",
                    plan.consulted_upto, seg.first_lsn
                )));
            }
        }
        step(
            hook,
            CompactionStep::LogSynced {
                segment: seg.first_lsn,
            },
        )?;

        if !checkpoints_deleted {
            delete_checkpoints(ctx)?;
            checkpoints_deleted = true;
            step(hook, CompactionStep::CheckpointsDeleted)?;
        }

        let relocations = plan.relocations;
        let index = ctx.index;
        ctx.wal
            .replace_segment(seg.first_lsn, &path, move || {
                let pinned = index.pin();
                for r in relocations {
                    pinned.update(r.key, |at| if at.lsn == r.lsn { r.to } else { *at });
                }
            })
            .map_err(wal_storage_err)?;
        report.segments_rewritten += 1;
        lengths[i] = plan.new_len;
        elided.push(plan.all_elided);
        step(
            hook,
            CompactionStep::SegmentReplaced {
                segment: seg.first_lsn,
            },
        )?;
    }

    for (i, seg) in sealed.iter().enumerate() {
        if !elided[i] || stop() {
            break;
        }
        ctx.wal
            .remove_leading_segments(seg.next_lsn)
            .map_err(wal_storage_err)?;
        report.segments_removed += 1;
        lengths[i] = 0;
        step(
            hook,
            CompactionStep::SegmentRemoved {
                segment: seg.first_lsn,
            },
        )?;
    }
    report.bytes_after = lengths.iter().sum();

    if report.segments_rewritten + report.segments_removed > 0 && !stop() {
        step(hook, CompactionStep::SegmentsDone)?;
        // Only after the last swap and index update: the checkpoint reads the post-swap
        // index, so its locations are the rewritten ones.
        if let Err(e) = write_checkpoint() {
            // The compaction itself is complete and durable; without a checkpoint the next
            // open replays the whole log, which is correct, only slower.
            warn!(error = %e, "compaction finished but its fresh checkpoint was not written");
        } else {
            step(hook, CompactionStep::CheckpointWritten)?;
        }
    }
    info!(
        rewritten = report.segments_rewritten,
        removed = report.segments_removed,
        bytes_before = report.bytes_before,
        bytes_after = report.bytes_after,
        "WAL compaction run finished"
    );
    Ok(report)
}
