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
//! replay applies them in order. A `Delete` op is kept while it is within the tombstone
//! retention (`CompactionConfig::tombstone_retention_lsns`: its LSN is within that many
//! LSNs of the log's end when the run starts) and dropped after that. Keeping a delete is
//! always safe. Dropping one is safe because every older put of its key is already dead,
//! and compaction runs as a prefix, oldest segment first (next section), so those puts
//! were rewritten away, durably, before the delete is dropped, and no replay can resurrect
//! them. A frame with no op left becomes an `Elided` frame (17-byte header, empty payload,
//! same LSN), so every segment keeps contiguous LSNs and recovery's continuity check is
//! unchanged. A frame that keeps every op is copied byte for byte; one that loses some is
//! re-encoded with the log's compression settings. Either way it keeps its LSN.
//!
//! Trade-off: an elided record still costs its 17-byte header until every frame of its
//! segment is elided and the segment is removed from the front of the log.
//!
//! # A run
//!
//! One run at a time. A run takes the sealed segments (never the active one) and walks
//! them oldest first, always from the oldest: the prefix rule above is what makes dropping
//! a delete safe, so it is not an optimisation to relax. It works in batches of up to
//! [`BATCH_SEGMENTS`] rewritten segments or [`BATCH_BYTES`] of rewrite output:
//!
//! 1. Per segment, oldest first: liveness against the live index (a dry pass; a segment
//!    with nothing to drop is left alone and counts as done), then
//!    `{first:020}.wal.compact` is written (a segment header, then every frame, live or
//!    elided) and `sync_data`ed. Liveness only shrinks between the passes.
//! 2. **One log sync for the batch, only if needed.** In `SyncMode::Fast` the index
//!    publishes a frame once it is written, before it is synced, so an op can look dead
//!    because a newer, still unsynced write superseded it; dropping the old op and then
//!    losing power would lose both. A put dropped because its key now points elsewhere was
//!    superseded by exactly that newer put (`index[key].lsn`); a put whose key is gone was
//!    superseded by a delete whose LSN is unknown, bounded by the log's end after the
//!    pass. The log is synced only if the highest such LSN is not yet durable. (In Durable
//!    mode the index only publishes synced frames, so no sync is ever needed.)
//! 3. **Raise the compaction floor** (`LOG_STATE.compacted_through`, made durable) to the
//!    highest LSN of a frame the batch changes, before anything is renamed. A change-stream
//!    cursor below it gets [`StorageError::CompactedCursor`] instead of a silent subset.
//! 4. Before the first rename of the run: **delete every index checkpoint** and sync
//!    `checkpoints/`. A rewrite keeps the segment's first LSN and therefore its file name,
//!    so a checkpoint taken before it would still pass recovery's check against the log
//!    (`checkpoint::validate_against_log`) with stale offsets. Deleting them before any
//!    rewritten segment is visible means a crash anywhere in the run leaves no
//!    checkpoint, which is a full replay, never a stale one.
//! 5. Per segment, oldest first, [`Wal::replace_segment`]: rename over `{first:020}.wal`,
//!    `sync_dir`, swap the read handle, and move the index entries of the segment's live
//!    puts to their new offsets with a compare-and-set (only an entry whose LSN is still
//!    the frame's moves; a key overwritten meanwhile keeps its newer location).
//!
//! Then the leading segments that are entirely elided are released: `LOG_STATE.log_start`
//! is moved past them first ([`Wal::set_log_start`]), then they are removed oldest first
//! (`remove` + `sync_dir` each); `Wal::open` removes any a crash left in between. Finally,
//! if anything changed, a fresh checkpoint is written from the post-swap index. Never the
//! other way round: a fresh checkpoint written before the old ones are gone would leave a
//! loadable stale one in the window.
//!
//! A crash at any point leaves each segment either old or rewritten (rename is atomic, and
//! a rewrite is synced before it is renamed), rewritten segments always a prefix of the
//! sealed ones, the floor at or above every dropped frame, and no checkpoint that
//! predates a rewrite: recovery rebuilds exactly the pre-compaction state.
//!
//! A run polls its stop condition on every frame (the background task's: the last handle
//! to the adapter dropped), deletes the rewrites it has not renamed yet, and returns, so
//! closing the adapter never waits for more than a frame's work.
//!
//! # What change-stream consumers see
//!
//! `get_changes_since` reads what is on disk. Surviving ops keep their LSNs (versions),
//! so cursors stay valid and LSNs are never reused; frames removed by compaction are gone.
//! A cursor at or above the compaction floor ([`Wal::compacted_through`]) gets the
//! complete stream after it. A cursor below it (other than 0) gets
//! [`StorageError::CompactedCursor`]: the changes after it are incomplete (a dropped
//! delete would leave the consumer holding a deleted key), so it must resynchronise from a
//! snapshot and resume at or above the floor. Cursor 0 means "from nothing": replaying the
//! compacted stream from an empty state rebuilds the current state exactly, so it is
//! served.

use super::checkpoint;
use papaya::HashMap as LockFreeHashMap;
use prkdb_core::vfs::{Vfs, VfsFile};
use prkdb_core::wal::batch::{Batch, BatchOp};
use prkdb_core::wal::frame::{encode_frame, FrameKind, FRAME_HEADER_LEN};
use prkdb_core::wal::segment::{write_segment_header, SEGMENT_HEADER_LEN};
use prkdb_core::wal::{CompressionConfig, Lsn, RecordLoc, SealedSegment, Wal, WalError};
use prkdb_types::error::StorageError;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
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
    /// Log syncs the run asked the WAL writer for (at most one per batch).
    pub log_syncs: usize,
    /// The longest a segment swap held the WAL's segment table (handle swap plus index
    /// update), during which a segment roll on the writer thread waits.
    pub longest_swap: Duration,
    /// Whether the run stopped early because the adapter was closing.
    pub stopped_early: bool,
}

/// A point in a compaction run, handed to the test hook of
/// [`WalStorageAdapter::compact_with_hook`](super::WalStorageAdapter::compact_with_hook)
/// right after the step completes. Each is a distinct on-disk state a crash can leave.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionStep {
    /// `{segment:020}.wal.compact` is written and synced; nothing visible has changed.
    CompactFileWritten { segment: Lsn },
    /// The batch's log sync is done (or was not needed): every write that superseded an
    /// op the batch drops is durable.
    LogSynced,
    /// `LOG_STATE.compacted_through` covers every frame the batch changes.
    FloorRaised { floor: Lsn },
    /// Every index checkpoint is deleted and `checkpoints/` synced (once per run, before
    /// the first rename).
    CheckpointsDeleted,
    /// The rewrite is renamed over `{segment:020}.wal`, the directory synced, the read
    /// handle swapped and the index moved to the new offsets.
    SegmentReplaced { segment: Lsn },
    /// `LOG_STATE.log_start` is moved past the fully elided leading segments, which still
    /// exist.
    LogStartRaised { log_start: Lsn },
    /// Those segments are removed and the directory synced.
    SegmentsRemoved,
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
    /// Deletes within this many LSNs of the log's end are kept.
    pub tombstone_retention_lsns: u64,
    /// Upper bound on the bytes a run reads and writes per second; `None` = unlimited.
    pub max_bytes_per_sec: Option<u64>,
    /// Polled on every frame; `true` ends the run early.
    pub stop: &'a dyn Fn() -> bool,
}

/// Suffix of a rewrite before it is renamed into place. `Wal::open` ignores such files
/// (they do not parse as segment names); a run removes leftovers before it starts.
const COMPACT_SUFFIX: &str = ".compact";

/// Rewrites are written in chunks of about this many bytes, so a 1 GiB segment is never
/// held in memory.
const WRITE_CHUNK: usize = 1 << 20;

/// A batch (one log sync) holds at most this many rewritten segments...
pub const BATCH_SEGMENTS: usize = 64;
/// ...or this many bytes of rewrites waiting to be renamed (the extra disk space a run
/// needs at most, beyond one segment).
pub const BATCH_BYTES: u64 = 256 << 20;

/// The marker error a stopped pass returns; never escapes this module.
const STOPPED: &str = "compaction stopped: the adapter is closing";

fn compact_path(wal: &Wal, first_lsn: Lsn) -> PathBuf {
    let mut name = wal.segment_path(first_lsn).into_os_string();
    name.push(COMPACT_SUFFIX);
    PathBuf::from(name)
}

fn is_stop(e: &WalError) -> bool {
    matches!(e, WalError::CompactionRefused(why) if why == STOPPED)
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

/// Paces a run to `max_bytes_per_sec`.
struct Throttle {
    started: Instant,
    bytes: u64,
    rate: Option<u64>,
}

impl Throttle {
    fn new(rate: Option<u64>) -> Self {
        Throttle {
            started: Instant::now(),
            bytes: 0,
            rate: rate.filter(|r| *r > 0),
        }
    }

    /// Accounts for `bytes` and sleeps while the run is ahead of its rate, in slices of
    /// at most 10 ms so that a stop request is noticed promptly.
    fn charge(&mut self, bytes: u64, stop: &dyn Fn() -> bool) {
        let Some(rate) = self.rate else { return };
        self.bytes += bytes;
        let due = Duration::from_secs_f64(self.bytes as f64 / rate as f64);
        loop {
            let ahead = due.saturating_sub(self.started.elapsed());
            if ahead < Duration::from_millis(2) || stop() {
                return;
            }
            std::thread::sleep(ahead.min(Duration::from_millis(10)));
        }
    }
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
    /// The log must be durable to this LSN before the rewrite is renamed (0 = nothing).
    durable_needed: Lsn,
    /// Highest LSN of a frame the rewrite changes (0 = none).
    max_changed: Lsn,
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
            self.flushed += (FRAME_HEADER_LEN + payload.len()) as u64;
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
/// rewrite into it (header included; the caller syncs it). `horizon`: deletes at or above
/// this LSN are kept.
fn plan_segment(
    ctx: &Ctx<'_>,
    seg: &SealedSegment,
    out: Option<(&dyn VfsFile, &Path)>,
    horizon: Lsn,
    throttle: &mut Throttle,
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
    let mut durable_needed: Lsn = 0;
    let mut superseded_by_delete = false;
    let mut max_changed: Lsn = 0;
    let index = ctx.index.pin();

    ctx.wal
        .scan_sealed(seg.first_lsn, &mut |loc, kind, payload| {
            if (ctx.stop)() {
                return Err(WalError::CompactionRefused(STOPPED.to_string()));
            }
            throttle.charge((FRAME_HEADER_LEN + payload.len()) as u64, ctx.stop);
            if kind == FrameKind::Elided {
                return sink.frame(loc.lsn, FrameKind::Elided, &[]);
            }
            let ops = Batch::decode(payload)?.ops;
            // Walking backwards: the first put of a key seen is the frame's last one for it,
            // the only one replay leaves in effect. A delete marks its key seen (an earlier
            // put of that key in the frame is superseded inside the frame, durably).
            let mut seen: HashSet<&[u8]> = HashSet::new();
            let mut keep = vec![false; ops.len()];
            for (i, op) in ops.iter().enumerate().rev() {
                match op {
                    BatchOp::Put { key, .. } => {
                        if !seen.insert(key.as_slice()) {
                            continue;
                        }
                        match index.get(key) {
                            Some(at) if at.lsn == loc.lsn => keep[i] = true,
                            // Superseded by exactly this later put.
                            Some(at) => durable_needed = durable_needed.max(at.lsn),
                            // Superseded by a delete whose LSN is not known here.
                            None => superseded_by_delete = true,
                        }
                    }
                    BatchOp::Delete { key } => {
                        seen.insert(key.as_slice());
                        keep[i] = loc.lsn >= horizon;
                    }
                }
            }
            let kept = keep.iter().filter(|k| **k).count();
            if kept == 0 {
                changed = true;
                max_changed = max_changed.max(loc.lsn);
                return sink.frame(loc.lsn, FrameKind::Elided, &[]);
            }
            all_elided = false;
            let offset = sink.offset();
            let rewritten;
            let new_payload: &[u8] = if kept == ops.len() {
                payload
            } else {
                changed = true;
                max_changed = max_changed.max(loc.lsn);
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
            if sink.file.is_some() {
                throttle.charge((FRAME_HEADER_LEN + new_payload.len()) as u64, ctx.stop);
            }
            sink.frame(loc.lsn, FrameKind::Batch, new_payload)
        })?;
    sink.flush()?;
    if superseded_by_delete {
        // The delete was published before this load: a hook runs only after `next_lsn`
        // covers its frame.
        durable_needed = durable_needed.max(ctx.wal.next_lsn().saturating_sub(1));
    }
    Ok(Plan {
        changed,
        all_elided,
        new_len: sink.offset(),
        relocations,
        durable_needed,
        max_changed,
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

/// The deletes a run started now would keep: those at or above this LSN.
fn tombstone_horizon(ctx: &Ctx<'_>) -> Lsn {
    ctx.wal
        .next_lsn()
        .saturating_sub(ctx.tombstone_retention_lsns)
}

/// Bytes a compaction could reclaim now: `(sealed bytes, reclaimable bytes)`. A dry pass
/// over every sealed segment (reads only), for the background trigger's dead ratio.
pub(crate) fn reclaimable(ctx: &Ctx<'_>) -> Result<(u64, u64), StorageError> {
    let sealed = ctx.wal.sealed_segments().map_err(wal_storage_err)?;
    let horizon = tombstone_horizon(ctx);
    let mut throttle = Throttle::new(ctx.max_bytes_per_sec);
    let mut total = 0u64;
    let mut reclaimable = 0u64;
    let mut leading = true;
    for seg in &sealed {
        total += seg.len;
        let plan = match plan_segment(ctx, seg, None, horizon, &mut throttle) {
            Ok(plan) => plan,
            Err(e) if is_stop(&e) => return Ok((total, 0)),
            Err(e) => return Err(wal_storage_err(e)),
        };
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

/// Rewrites written but not yet renamed into place. Whatever is left when it drops (a
/// stopped run, an error) is removed, best effort; the next run removes anything a crash
/// left.
struct Pending<'a> {
    vfs: &'a dyn Vfs,
    rewrites: Vec<(usize, Plan, PathBuf)>,
}

impl Drop for Pending<'_> {
    fn drop(&mut self) {
        for (_, _, path) in self.rewrites.drain(..) {
            if let Err(e) = self.vfs.remove(&path) {
                warn!(path = %path.display(), error = %e, "could not remove an unused compaction rewrite");
            }
        }
    }
}

/// One compaction run (see the module docs). The caller holds the adapter's compaction
/// lock and its checkpoint lock for the whole call. `write_checkpoint` writes a
/// checkpoint without taking that lock again.
pub(crate) fn run(
    ctx: &Ctx<'_>,
    hook: &mut StepHook<'_>,
    write_checkpoint: &dyn Fn() -> Result<(), StorageError>,
) -> Result<CompactionReport, StorageError> {
    remove_stale_rewrites(ctx)?;
    let sealed = ctx.wal.sealed_segments().map_err(wal_storage_err)?;
    let horizon = tombstone_horizon(ctx);
    let mut throttle = Throttle::new(ctx.max_bytes_per_sec);
    let mut report = CompactionReport {
        bytes_before: sealed.iter().map(|s| s.len).sum(),
        ..CompactionReport::default()
    };
    let mut lengths: Vec<u64> = sealed.iter().map(|s| s.len).collect();
    let mut elided = vec![false; sealed.len()];
    let mut checkpoints_deleted = false;
    let mut next = 0;

    let stopped = |mut report: CompactionReport, lengths: &[u64]| {
        info!("compaction stopped early: the adapter is closing");
        report.bytes_after = lengths.iter().sum();
        report.stopped_early = true;
        Ok(report)
    };

    while next < sealed.len() {
        // 1. Plan and write a batch of rewrites.
        let mut pending = Pending {
            vfs: ctx.vfs,
            rewrites: Vec::new(),
        };
        let mut batch_bytes = 0u64;
        while next < sealed.len()
            && pending.rewrites.len() < BATCH_SEGMENTS
            && batch_bytes < BATCH_BYTES
        {
            let (i, seg) = (next, &sealed[next]);
            next += 1;
            let dry = match plan_segment(ctx, seg, None, horizon, &mut throttle) {
                Ok(plan) => plan,
                Err(e) if is_stop(&e) => return stopped(report, &lengths),
                Err(e) => return Err(wal_storage_err(e)),
            };
            if !dry.changed {
                elided[i] = dry.all_elided;
                continue;
            }
            let path = compact_path(ctx.wal, seg.first_lsn);
            let file = ctx.vfs.create(&path).map_err(|e| io_err(&path, e))?;
            let written = plan_segment(ctx, seg, Some((&*file, &path)), horizon, &mut throttle);
            // Registered before anything can fail, so the file is cleaned up either way.
            let plan = match written {
                Ok(plan) => plan,
                Err(e) => {
                    pending.rewrites.push((i, dry, path));
                    return if is_stop(&e) {
                        stopped(report, &lengths)
                    } else {
                        Err(wal_storage_err(e))
                    };
                }
            };
            batch_bytes += plan.new_len;
            let synced = file.sync_data();
            drop(file);
            pending.rewrites.push((i, plan, path.clone()));
            synced.map_err(|e| io_err(&path, e))?;
            step(
                hook,
                CompactionStep::CompactFileWritten {
                    segment: seg.first_lsn,
                },
            )?;
        }
        if pending.rewrites.is_empty() {
            continue;
        }
        if (ctx.stop)() {
            return stopped(report, &lengths);
        }

        // 2. One log sync for the batch, only if something it relies on is not durable.
        let needed = pending
            .rewrites
            .iter()
            .map(|(_, plan, _)| plan.durable_needed)
            .max()
            .unwrap_or(0);
        if ctx.wal.durable_lsn() < needed {
            let durable = ctx.wal.sync_blocking().map_err(wal_storage_err)?;
            report.log_syncs += 1;
            if durable < needed {
                return Err(StorageError::Internal(format!(
                    "compaction: the log synced to LSN {durable}, short of LSN {needed} \
                     that the rewrites rely on"
                )));
            }
        }
        step(hook, CompactionStep::LogSynced)?;

        // 3. The floor covers every frame the batch changes, before any rename.
        let floor = pending
            .rewrites
            .iter()
            .map(|(_, plan, _)| plan.max_changed)
            .max()
            .unwrap_or(0);
        ctx.wal
            .raise_compacted_through(floor)
            .map_err(wal_storage_err)?;
        step(hook, CompactionStep::FloorRaised { floor })?;

        // 4. No checkpoint may outlive the first rename.
        if !checkpoints_deleted {
            delete_checkpoints(ctx)?;
            checkpoints_deleted = true;
            step(hook, CompactionStep::CheckpointsDeleted)?;
        }

        // 5. Renames, oldest first.
        let rewrites = std::mem::take(&mut pending.rewrites);
        let mut rest = rewrites.into_iter();
        while let Some((i, plan, path)) = rest.next() {
            let seg = &sealed[i];
            let index = ctx.index;
            let relocations = plan.relocations;
            let mut held = Duration::ZERO;
            let swapped = ctx.wal.replace_segment(seg.first_lsn, &path, || {
                let start = Instant::now();
                let pinned = index.pin();
                for r in relocations {
                    pinned.update(r.key, |at| if at.lsn == r.lsn { r.to } else { *at });
                }
                held = start.elapsed();
            });
            if let Err(e) = swapped {
                pending.rewrites = rest.collect();
                return Err(wal_storage_err(e));
            }
            report.longest_swap = report.longest_swap.max(held);
            report.segments_rewritten += 1;
            lengths[i] = plan.new_len;
            elided[i] = plan.all_elided;
            if let Err(e) = step(
                hook,
                CompactionStep::SegmentReplaced {
                    segment: seg.first_lsn,
                },
            ) {
                pending.rewrites = rest.collect();
                return Err(e);
            }
        }
    }

    // Release the fully elided leading segments: log start first, then the files.
    let leading = elided.iter().take_while(|e| **e).count();
    if leading > 0 && !(ctx.stop)() {
        let upto = sealed[leading - 1].next_lsn;
        ctx.wal.set_log_start(upto).map_err(wal_storage_err)?;
        step(hook, CompactionStep::LogStartRaised { log_start: upto })?;
        report.segments_removed = ctx
            .wal
            .remove_leading_segments(upto)
            .map_err(wal_storage_err)?;
        for len in &mut lengths[..leading] {
            *len = 0;
        }
        step(hook, CompactionStep::SegmentsRemoved)?;
    }
    report.bytes_after = lengths.iter().sum();

    if report.segments_rewritten + report.segments_removed > 0 && !(ctx.stop)() {
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
        log_syncs = report.log_syncs,
        longest_swap = ?report.longest_swap,
        "WAL compaction run finished"
    );
    Ok(report)
}
