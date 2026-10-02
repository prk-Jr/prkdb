//! `Wal`: the single, globally ordered write-ahead log (Task 2.6, decision record §7).
//!
//! One dedicated writer thread owns the active segment and every mutation to it. Callers
//! reserve admission (bounded by bytes), queue a payload, and either await the ack
//! (`Reservation`/`PendingAppend`) or block on it (`append_blocking`). The writer batches
//! whatever is already queued into one `write_at`, syncs per `SyncMode`, runs commit hooks
//! in LSN order, then answers every request in the batch. Any I/O error, or a panicking
//! hook, poisons the log: no failed `fsync` is ever retried (fsyncgate).
//!
//! Ported from the Task 2.1 spike's `SingleLog`/`writer_loop` and completed with
//! everything the spike deliberately omitted: error poisoning, bounded admission, reads,
//! recovery and the writer-liveness probe (decision record §6 risk 5).

use crate::vfs::{OpenMode, Vfs, VfsFile};
use crate::wal::config::{FrontRelease, SyncMode, WalConfig};
use crate::wal::frame::{encode_frame, FrameKind, Lsn, MAX_PAYLOAD_LEN};
use crate::wal::log_state::LogState;
use crate::wal::segment::{
    read_frame, scan_segment, scan_segment_flow, scan_segment_from, segment_file_name,
    write_segment_header, RecordLoc, ScanFlowVisitor, ScanVisitor, SegmentScan, SEGMENT_HEADER_LEN,
};
use crate::wal::WalError;
use std::collections::BTreeMap;
use std::ops::ControlFlow;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::sync::atomic::{fence, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::{Arc, RwLock};
use std::task::{Context as TaskContext, Poll};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{oneshot, watch, OwnedSemaphorePermit, Semaphore};

/// Options the writer thread runs under. Constructed from [`WalConfig`] with
/// [`WalOptions::from_config`].
#[derive(Debug, Clone)]
pub struct WalOptions {
    pub sync_mode: SyncMode,
    /// Fast mode's sync target (see [`SyncMode::Fast`]): a target, not a bound on what a
    /// power cut can lose. `durable_lsn` is the guarantee.
    pub sync_interval: Duration,
    pub segment_bytes: u64,
    pub max_batch_bytes: usize,
    pub max_queued_bytes: usize,
    /// What may release segments from the front of the log; fixed at open. Keyed
    /// directories keep the default, [`FrontRelease::ElidedOnly`].
    pub front_release: FrontRelease,
    /// Frame kind assigned to every append. Keyed writers retain `Batch`.
    pub append_kind: FrameKind,
    /// Exclusive bound on allocated LSNs. Streams reserve their final offset block
    /// for the next cursor; `None` preserves the keyed writer's unrestricted range.
    pub lsn_limit: Option<Lsn>,
}

impl WalOptions {
    pub fn from_config(c: &WalConfig) -> Self {
        WalOptions {
            sync_mode: c.sync_mode,
            sync_interval: Duration::from_millis(c.sync_interval_ms),
            segment_bytes: c.segment_bytes,
            max_batch_bytes: c.max_batch_bytes,
            max_queued_bytes: c.max_queued_bytes,
            front_release: FrontRelease::ElidedOnly,
            append_kind: FrameKind::Batch,
            lsn_limit: None,
        }
    }
}

/// Runs on the writer thread, in LSN order, after the frame is durable (`Durable`) or
/// written (`Fast`), and before the appender's future resolves. Must not block or panic;
/// a panic poisons the log (it runs inside the writer's `catch_unwind`).
pub type CommitHook = Box<dyn FnOnce(RecordLoc) + Send>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalHealth {
    Healthy,
    /// Requests are queued and no batch completed for longer than the stall bound.
    Stalled {
        queued_bytes: usize,
        oldest_ms: u64,
    },
    Poisoned(String),
    Closed,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RecoveryReport {
    pub segments: usize,
    pub frames: u64,
    pub next_lsn: Lsn,
    /// Set when the last segment ended in a torn frame and was truncated.
    pub truncated: Option<(PathBuf, u64, crate::wal::frame::FrameFault)>,
}

/// A sealed segment (any but the active, last one), as compaction sees it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SealedSegment {
    /// The segment's first LSN (its file-name key).
    pub first_lsn: Lsn,
    /// The first LSN of the next segment: this one holds exactly `first_lsn..next_lsn`.
    pub next_lsn: Lsn,
    /// File length in bytes.
    pub len: u64,
}

/// A segment's read handle, and how many times compaction replaced the file under it
/// since this `Wal` was opened (0 = never). A read that finds the wrong frame in a
/// segment with a nonzero generation is `WalError::Moved`, not corruption.
#[derive(Clone)]
struct SegmentHandle {
    file: Arc<dyn VfsFile>,
    generation: u64,
    /// The file this one replaced, kept readable from the swap until the caller has moved
    /// its index to the new offsets ([`Wal::release_replaced`]): a location from before
    /// the swap is read there, so the index update can run outside the segments lock.
    previous: Option<Arc<dyn VfsFile>>,
}

impl SegmentHandle {
    fn new(file: Arc<dyn VfsFile>) -> Self {
        SegmentHandle {
            file,
            generation: 0,
            previous: None,
        }
    }
}

/// `Wal::open`'s log-start rule: removes, oldest first, every segment that ends at or
/// before `state.log_start`, and returns the segments that remain. A crash between
/// `LOG_STATE`'s write and their removal leaves them. Every one is checked before any is
/// removed, so a refusal changes nothing: each must end at or before the log start, and
/// under [`FrontRelease::ElidedOnly`] be whole and fully elided (compaction released
/// it). Under [`FrontRelease::Retention`] its frames are not read: retention released it
/// with whatever it held.
fn remove_leftovers_before(
    vfs: &dyn Vfs,
    dir: &Path,
    segment_lsns: Vec<Lsn>,
    state: LogState,
    front_release: FrontRelease,
) -> Result<Vec<Lsn>, WalError> {
    let leftovers = segment_lsns
        .iter()
        .take_while(|first| **first < state.log_start)
        .count();
    for i in 0..leftovers {
        let first = segment_lsns[i];
        let path = dir.join(segment_file_name(first));
        let next = segment_lsns.get(i + 1).copied();
        if next.is_none_or(|next| next > state.log_start) {
            return Err(WalError::CorruptSegment {
                path,
                offset: 0,
                reason: format!(
                    "segment starts before the log start {} (LOG_STATE) but does not end \
                     at or before it",
                    state.log_start
                ),
            });
        }
        if front_release == FrontRelease::Retention {
            continue;
        }
        let file = vfs.open(&path, OpenMode::Read)?;
        let scan = scan_segment(&*file, &path, first, &mut |loc, kind, _| {
            if kind == FrameKind::Elided {
                Ok(())
            } else {
                Err(WalError::CorruptSegment {
                    path: path.clone(),
                    offset: loc.offset,
                    reason: format!(
                        "segment before the log start {} (LOG_STATE) holds a live frame \
                         (lsn {})",
                        state.log_start, loc.lsn
                    ),
                })
            }
        })?;
        check_whole(&path, &scan, next.expect("checked above"))?;
    }
    for &first in &segment_lsns[..leftovers] {
        let path = dir.join(segment_file_name(first));
        tracing::info!(
            path = %path.display(),
            ?front_release,
            "removing a segment already released below the log start"
        );
        vfs.remove(&path)?;
        vfs.sync_dir(dir)?;
    }
    Ok(segment_lsns[leftovers..].to_vec())
}

/// A sealed segment's scan must have read every byte as a good frame and ended exactly
/// where the next segment starts.
fn check_whole(path: &Path, scan: &SegmentScan, next_lsn: Lsn) -> Result<(), WalError> {
    if let Some((offset, fault)) = scan.stopped {
        return Err(WalError::CorruptSegment {
            path: path.to_path_buf(),
            offset,
            reason: format!("{fault:?} in a sealed segment"),
        });
    }
    if scan.next_lsn != next_lsn {
        return Err(WalError::CorruptSegment {
            path: path.to_path_buf(),
            offset: scan.valid_len,
            reason: format!(
                "sealed segment ends before LSN {}, but the next segment starts at {next_lsn}",
                scan.next_lsn
            ),
        });
    }
    Ok(())
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// State shared between `Wal` (and its clones' callers) and the writer thread.
struct Shared {
    next_lsn: AtomicU64,
    durable_lsn: AtomicU64,
    /// Watermark of LSNs that have been answered `Ok` to their caller (spec MEDIUM review
    /// fix): `== durable_lsn` in `SyncMode::Durable` (the sync happens before any reply is
    /// sent), but ahead of `durable_lsn` in `SyncMode::Fast`, where a reply is answered
    /// right after the write, before the periodic sync catches up. A batch item answered
    /// `Poisoned` (including one whose commit hook panicked, and everything after it in
    /// the same batch) never raises this watermark, even though its frame may already be
    /// physically written — "acked" means the caller was told `Ok`, nothing else.
    acked_lsn: AtomicU64,
    /// `acked_lsn`, published once per batch for readers waiting on new data
    /// ([`Wal::subscribe_acked`]). The writer skips it while no receiver exists, so a log
    /// nobody watches pays one fence and one atomic load per batch.
    acked_tx: watch::Sender<Lsn>,
    /// Fixed at open (see [`WalOptions::front_release`]).
    front_release: FrontRelease,
    health: RwLock<InternalHealth>,
    /// Read handles for `read`/`scan_from`, keyed by each segment's first LSN. Written by
    /// the writer thread (a roll adds the new active segment) and by compaction (a
    /// rewritten sealed segment's handle is replaced, a fully elided leading one removed).
    segments: RwLock<BTreeMap<Lsn, SegmentHandle>>,
    /// Every segment whose first LSN is below this was removed by compaction since open;
    /// 0 = none. A location in such a segment is stale (`Moved`), not corrupt.
    removed_below: AtomicU64,
    /// The filesystem, for compaction's renames and removals (the writer thread owns its
    /// own clone for rolls).
    vfs: Arc<dyn Vfs>,
    /// The durable `LOG_STATE` as last written (or read at open). The mutex serialises
    /// its rewrites.
    log_state: std::sync::Mutex<LogState>,
    /// `log_state.deletes_compacted_through`, readable without the lock. Raised *before*
    /// the file is written and before any rename it covers, so a reader that checks it
    /// after scanning never misses a rewrite that could have dropped a delete it scanned
    /// past.
    deletes_compacted_through: AtomicU64,
    queued_bytes: AtomicUsize,
    last_progress_ms: AtomicU64,
    oldest_enqueued_ms: AtomicU64,
    admission: Arc<Semaphore>,
    dir: PathBuf,
    sync_interval: Duration,
    max_queued_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum InternalHealth {
    Running,
    Poisoned(String),
    Closed,
}

impl Shared {
    /// Raises the published `acked_lsn` to the current one, if any receiver exists. The
    /// `SeqCst` fence pairs with the one in [`Wal::subscribe_acked`]: either this load of
    /// the receiver count sees the new subscriber, or the subscriber's load of
    /// `acked_lsn` sees every ack raised before this call. So no subscriber misses a
    /// wake-up for an ack it did not see.
    fn publish_acked(&self) {
        fence(Ordering::SeqCst);
        if self.acked_tx.receiver_count() == 0 {
            return;
        }
        self.refresh_acked();
    }

    /// Raises the published value to `acked_lsn` (monotonic: never lowers it), waking
    /// receivers only if it rose.
    fn refresh_acked(&self) {
        let acked = self.acked_lsn.load(Ordering::Acquire);
        self.acked_tx.send_if_modified(|published| {
            if acked > *published {
                *published = acked;
                true
            } else {
                false
            }
        });
    }

    fn health_snapshot(&self) -> WalHealth {
        match &*self.health.read().expect("health lock poisoned") {
            InternalHealth::Poisoned(reason) => return WalHealth::Poisoned(reason.clone()),
            InternalHealth::Closed => return WalHealth::Closed,
            InternalHealth::Running => {}
        }
        let queued_bytes = self.queued_bytes.load(Ordering::Acquire);
        if queued_bytes > 0 {
            let last_progress = self.last_progress_ms.load(Ordering::Acquire);
            let oldest = self.oldest_enqueued_ms.load(Ordering::Acquire);
            let stall_bound_ms = std::cmp::max(1000, 100 * self.sync_interval.as_millis() as u64);
            let now = now_ms();
            // Measure elapsed time from whichever of "the last batch completed" or "the
            // oldest still-queued request arrived" is more recent. Using only
            // `last_progress_ms` would misreport a freshly queued request as long-stalled
            // right after any idle period longer than the stall bound, because
            // `last_progress_ms` is stale from before the idle time even though the
            // request itself has waited no time at all.
            let reference = std::cmp::max(last_progress, oldest);
            if now.saturating_sub(reference) > stall_bound_ms {
                return WalHealth::Stalled {
                    queued_bytes,
                    oldest_ms: now.saturating_sub(oldest),
                };
            }
        }
        WalHealth::Healthy
    }
}

/// Blocks the calling thread on `fut`, outside tokio's cooperative budget.
///
/// Every blocking wait in this file (`append_blocking`, `sync_blocking`, the close in
/// `Drop`) waits on tokio primitives (the admission `Semaphore`, reply `oneshot`s) with
/// `futures::executor::block_on`. When the caller is a tokio task, those primitives charge
/// the task's cooperative budget. Once it is spent, each of their polls answers `Pending`
/// and wakes itself so the task yields back to the runtime, which refills the budget. But
/// `block_on` is not the runtime: it re-polls on the wake, gets `Pending` again, and spins
/// forever on that worker. A task reaches an empty budget after about 128 ready awaits in
/// a row, so a checkpoint or a drop after a burst of writes hung.
///
/// `tokio::task::unconstrained` turns budgeting off for the wrapped future, which is what
/// a blocking wait means anyway: it gives the runtime nothing back until it returns. It
/// fixes all three waits in one place and keeps them on the same async code as their
/// `async` versions. Replacing the replies with `std::sync::mpsc` would fix the oneshots
/// only: `append_blocking` also waits on the admission semaphore, which would need a
/// second, std-based admission path. Outside a runtime, `unconstrained` is a no-op.
fn block_on_unbudgeted<F: std::future::Future>(fut: F) -> F::Output {
    futures::executor::block_on(tokio::task::unconstrained(fut))
}

/// Admission permits for one payload of `len` bytes. Nothing is queued yet: dropping a
/// `Reservation` returns the permits and leaves no trace in the log.
pub struct Reservation {
    permit: OwnedSemaphorePermit,
    len: usize,
}

/// A slot in the pending-reply channel. Its `Drop` answers `Err(WalError::Closed)` if the
/// request is dropped (writer panic, queue teardown) without ever being answered, mirroring
/// today's `PendingWrite` drop guard.
struct Reply(Option<oneshot::Sender<Result<RecordLoc, WalError>>>);

impl Reply {
    fn answer(mut self, result: Result<RecordLoc, WalError>) {
        if let Some(tx) = self.0.take() {
            let _ = tx.send(result);
        }
    }
}

impl Drop for Reply {
    fn drop(&mut self) {
        if let Some(tx) = self.0.take() {
            let _ = tx.send(Err(WalError::Closed));
        }
    }
}

enum Request {
    Append {
        payload: Vec<u8>,
        hook: Option<CommitHook>,
        reply: Reply,
        enqueued_at_ms: u64,
        _permit: OwnedSemaphorePermit,
    },
    Sync {
        reply: oneshot::Sender<Result<Lsn, WalError>>,
    },
    /// Seal the active segment if it holds a frame (see [`Wal::roll`]).
    Roll {
        reply: oneshot::Sender<Result<Option<Lsn>, WalError>>,
    },
    Close {
        reply: oneshot::Sender<Result<(), WalError>>,
    },
}

/// An append that is already queued for the writer. Awaiting it yields the result; dropping
/// it does not cancel the write (the writer still commits it), which is why a timeout on
/// this future means "not confirmed", never "not written".
pub struct PendingAppend {
    rx: oneshot::Receiver<Result<RecordLoc, WalError>>,
}

impl std::future::Future for PendingAppend {
    type Output = Result<RecordLoc, WalError>;
    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Self::Output> {
        match std::pin::Pin::new(&mut self.rx).poll(cx) {
            Poll::Ready(Ok(r)) => Poll::Ready(r),
            Poll::Ready(Err(_)) => Poll::Ready(Err(WalError::Closed)),
            Poll::Pending => Poll::Pending,
        }
    }
}

pub struct Wal {
    shared: Arc<Shared>,
    sender: Option<mpsc::Sender<Request>>,
    writer: Option<JoinHandle<()>>,
}

/// The active segment, owned by the writer thread only.
struct ActiveSegment {
    first_lsn: Lsn,
    file: Arc<dyn VfsFile>,
    write_pos: u64,
}

impl Wal {
    /// Opens and recovers.
    ///
    /// Directory: `dir` and any missing ancestors are created with
    /// [`create_dir_all_durable`](crate::vfs::create_dir_all_durable), which syncs the
    /// parent of every ancestor entry, including existing ones (STO-16).
    /// Recovery: lists `*.wal`, sorts by first LSN, checks each segment's first LSN equals
    /// the previous segment's `next_lsn`, scans every segment, and calls `replay` for every
    /// frame with lsn >= `replay_from` in LSN order.
    ///
    /// Log start (Task 2.15): `LOG_STATE` (see [`LogState`]) records the first LSN of the
    /// log, which compaction moves forward when it removes fully elided segments from the
    /// front. Segments that start before it are what a crash between writing `LOG_STATE`
    /// and removing them leaves: each must end at or before the log start and, under
    /// [`FrontRelease::ElidedOnly`], be entirely elided; all are checked, then removed
    /// here (`remove` + `sync_dir`); one that fails a check is `CorruptSegment` and
    /// nothing is removed. Under [`FrontRelease::Retention`] (streams, Task 2.15b) their
    /// frames are not checked: retention released them live. A first segment that
    /// starts *after* the log start means segments are missing: `CorruptSegment` naming the
    /// missing LSN range. Last segment: a torn tail is logged,
    /// truncated (`set_len` + `sync_data`) and reported. Earlier segment: any fault is
    /// `CorruptSegment` and nothing is modified. A zero-length or header-only last segment
    /// is valid (a crash right after a roll or right after creation); a zero-length one is
    /// completed by writing its header (+ `sync_data`) before use.
    ///
    /// First segment: when the directory holds no segment, `open` creates
    /// `{next_lsn:020}.wal` (next_lsn = 1 for a new log) **before returning**: `create` →
    /// `write_segment_header` → `sync_data` → `sync_dir(dir)`. So the log's existence is
    /// durable before the first append in either mode, and a power cut between `open` and
    /// the first sync leaves a valid empty log, never a directory the next open cannot read.
    ///
    /// Every decode failure the caller's `replay` closure reports is wrapped with the
    /// segment path, byte offset and LSN of the frame that failed (spec §8 "refuse to open,
    /// name the file"): the closure only sees raw payload bytes, so it cannot name its own
    /// location without this wrapping.
    pub fn open(
        vfs: Arc<dyn Vfs>,
        dir: &Path,
        opts: WalOptions,
        replay_from: Lsn,
        replay: &mut ScanVisitor<'_>,
    ) -> Result<(Wal, RecoveryReport), WalError> {
        crate::vfs::create_dir_all_durable(&*vfs, dir)?;

        let mut segment_lsns: Vec<Lsn> = vfs
            .read_dir(dir)?
            .into_iter()
            .filter_map(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .and_then(crate::wal::segment::parse_segment_file_name)
            })
            .collect();
        segment_lsns.sort_unstable();

        let log_state = LogState::read(&*vfs, dir)?;
        // Stream offsets reserve the final LSN block for their terminal cursor.
        // Reject exhausted input before recovery can remove retention leftovers,
        // truncate a torn tail, or create a new active segment. Far from the bound,
        // the tail's byte length proves it cannot contain enough frames to reach it,
        // so the normal recovery scan remains the only frame scan.
        if let Some(limit) = opts.lsn_limit {
            let exhausted = || {
                WalError::InvalidRecords(format!(
                    "offset-limit {limit} exceeded by the recovered WAL"
                ))
            };
            if log_state.log_start > limit {
                return Err(exhausted());
            }
            if let Some(&first) = segment_lsns.last() {
                if first > limit {
                    return Err(exhausted());
                }
                let path = dir.join(segment_file_name(first));
                let file = vfs.open(&path, OpenMode::Read)?;
                let max_frames = file.len()?.saturating_sub(SEGMENT_HEADER_LEN)
                    / crate::wal::frame::FRAME_HEADER_LEN as u64;
                if max_frames > limit.saturating_sub(first) {
                    let scan = scan_segment(&*file, &path, first, &mut |loc, _, _| {
                        if loc.lsn >= limit {
                            Err(exhausted())
                        } else {
                            Ok(())
                        }
                    })?;
                    if scan.next_lsn > limit {
                        return Err(exhausted());
                    }
                }
            }
        }
        let segment_lsns =
            remove_leftovers_before(&*vfs, dir, segment_lsns, log_state, opts.front_release)?;
        if let Some(&first) = segment_lsns.first() {
            if first != log_state.log_start {
                return Err(WalError::CorruptSegment {
                    path: dir.join(segment_file_name(first)),
                    offset: 0,
                    reason: format!(
                        "the log starts at LSN {} (LOG_STATE), but its first segment starts \
                         at {first}: the segments holding LSNs {}..{first} are missing",
                        log_state.log_start, log_state.log_start
                    ),
                });
            }
        } else if log_state.log_start > 1 {
            return Err(WalError::CorruptSegment {
                path: dir.join(crate::wal::log_state::LOG_STATE_FILE),
                offset: 0,
                reason: format!(
                    "LOG_STATE says the log starts at LSN {}, but the directory holds no \
                     segment",
                    log_state.log_start
                ),
            });
        }

        let mut report = RecoveryReport::default();
        let mut next_lsn: Lsn = log_state.log_start;
        let mut segments: BTreeMap<Lsn, SegmentHandle> = BTreeMap::new();

        for (idx, &first_lsn) in segment_lsns.iter().enumerate() {
            let is_last = idx + 1 == segment_lsns.len();
            let path = dir.join(segment_file_name(first_lsn));
            let file = vfs.open(&path, OpenMode::ReadWrite)?;

            if first_lsn == 0 {
                return Err(WalError::CorruptSegment {
                    path: path.clone(),
                    offset: 0,
                    reason: "segment first_lsn 0: LSNs start at 1".to_string(),
                });
            }
            if first_lsn != next_lsn {
                return Err(WalError::CorruptSegment {
                    path: path.clone(),
                    offset: 0,
                    reason: format!(
                        "segment first_lsn {first_lsn} does not continue the previous segment's next_lsn {next_lsn}"
                    ),
                });
            }

            let file_len = file.len()?;
            if file_len == 0 {
                // A crash right after `create`, before the header was written. Valid only
                // for the last segment.
                if !is_last {
                    return Err(WalError::CorruptSegment {
                        path: path.clone(),
                        offset: 0,
                        reason: "zero-length segment before the last one".to_string(),
                    });
                }
                write_segment_header(&*file, first_lsn)?;
                file.sync_data()?;
                segments.insert(first_lsn, SegmentHandle::new(file.clone()));
                report.segments += 1;
                continue;
            }

            let scan = scan_segment(&*file, &path, first_lsn, &mut |loc, kind, payload| {
                report.frames += 1;
                if loc.lsn >= replay_from {
                    replay(loc, kind, payload).map_err(|e| WalError::ReplayFailed {
                        path: path.clone(),
                        offset: loc.offset,
                        lsn: loc.lsn,
                        source: Box::new(e),
                    })?;
                }
                Ok(())
            })?;

            if let Some((offset, fault)) = scan.stopped {
                if is_last {
                    tracing::warn!(
                        path = %path.display(),
                        offset,
                        ?fault,
                        "torn tail in the last WAL segment; truncating"
                    );
                    file.set_len(offset)?;
                    file.sync_data()?;
                    report.truncated = Some((path.clone(), offset, fault));
                } else {
                    return Err(WalError::CorruptSegment {
                        path: path.clone(),
                        offset,
                        reason: format!("{fault:?}"),
                    });
                }
            }

            next_lsn = scan.next_lsn;
            segments.insert(first_lsn, SegmentHandle::new(file.clone()));
            report.segments += 1;
        }

        if segments.is_empty() {
            // First segment: created and made durable before `open` returns, in either
            // mode, so a power cut right after leaves a valid empty log.
            let path = dir.join(segment_file_name(next_lsn));
            let file = vfs.create(&path)?;
            write_segment_header(&*file, next_lsn)?;
            file.sync_data()?;
            vfs.sync_dir(dir)?;
            segments.insert(next_lsn, SegmentHandle::new(file));
            report.segments += 1;
        }
        report.next_lsn = next_lsn;

        let active_first_lsn = *segments.keys().next_back().expect("at least one segment");
        let active_file = segments
            .get(&active_first_lsn)
            .expect("just inserted")
            .file
            .clone();
        // H2: a frame can be physically present and get replayed above without ever having
        // been fsynced (Fast mode, or a process restart that is not an actual power loss —
        // the bytes simply never left the page cache with a durability guarantee). Syncing
        // the active segment once here, before `durable_lsn` is set to cover everything we
        // just replayed, closes that gap: whatever we are about to report as durable really
        // is, so a real power loss right after `open` returns cannot lose it a second time.
        // Idempotent and cheap when the segment was already fully synced.
        active_file.sync_data()?;
        let write_pos = active_file.len()?;
        let active = ActiveSegment {
            first_lsn: active_first_lsn,
            file: active_file,
            write_pos,
        };

        let shared = Arc::new(Shared {
            next_lsn: AtomicU64::new(next_lsn),
            durable_lsn: AtomicU64::new(next_lsn.saturating_sub(1)),
            // Recovery: everything replayed was just made durable by the sync above, and
            // was reported to `replay` as if committed, so it counts as acked too.
            acked_lsn: AtomicU64::new(next_lsn.saturating_sub(1)),
            acked_tx: watch::Sender::new(next_lsn.saturating_sub(1)),
            front_release: opts.front_release,
            health: RwLock::new(InternalHealth::Running),
            segments: RwLock::new(segments),
            removed_below: AtomicU64::new(0),
            vfs: vfs.clone(),
            log_state: std::sync::Mutex::new(log_state),
            deletes_compacted_through: AtomicU64::new(log_state.deletes_compacted_through),
            queued_bytes: AtomicUsize::new(0),
            last_progress_ms: AtomicU64::new(now_ms()),
            oldest_enqueued_ms: AtomicU64::new(0),
            admission: Arc::new(Semaphore::new(opts.max_queued_bytes.max(1))),
            dir: dir.to_path_buf(),
            sync_interval: opts.sync_interval,
            max_queued_bytes: opts.max_queued_bytes.max(1),
        });

        let (tx, rx) = mpsc::channel::<Request>();
        let writer_shared = shared.clone();
        let writer_vfs = vfs;
        let writer_dir = dir.to_path_buf();
        let writer_opts = opts;
        let writer = std::thread::Builder::new()
            .name("prkdb-wal-writer".to_string())
            .spawn(move || {
                writer_thread(
                    writer_shared,
                    rx,
                    active,
                    writer_opts,
                    writer_dir,
                    writer_vfs,
                );
            })
            .map_err(WalError::Io)?;

        Ok((
            Wal {
                shared,
                sender: Some(tx),
                writer: Some(writer),
            },
            report,
        ))
    }

    /// Waits for admission permits (`min(len, max_queued_bytes)` bytes). Refuses a `len`
    /// over `MAX_PAYLOAD_LEN` with `RecordTooLarge` and a `len` of 0 with `EmptyRecord`
    /// (a frame of length 0 reads back as a torn tail, STO-17), and returns
    /// `Poisoned`/`Closed` without waiting when the log cannot accept writes.
    pub async fn reserve(&self, len: usize) -> Result<Reservation, WalError> {
        if len == 0 {
            return Err(WalError::EmptyRecord {
                path: self.shared.dir.clone(),
            });
        }
        if len > MAX_PAYLOAD_LEN {
            return Err(WalError::RecordTooLarge {
                path: self.shared.dir.clone(),
                len,
                max: MAX_PAYLOAD_LEN,
            });
        }
        self.fail_fast_if_unhealthy()?;

        // `min(len, max_queued_bytes)`: a single payload larger than the admission bound
        // still gets in eventually rather than blocking forever waiting for permits that
        // can never all exist.
        let cap = self.shared.max_queued_bytes;
        let n = std::cmp::min(len, cap) as u32;
        let permit = self
            .shared
            .admission
            .clone()
            .acquire_many_owned(n)
            .await
            .map_err(|_| WalError::Closed)?;
        Ok(Reservation { permit, len })
    }

    /// Queues `payload` under `r` (synchronously: when this returns `Ok`, the writer owns
    /// the request). Errors if `payload.len()` differs from the reserved length.
    pub fn append_reserved(
        &self,
        r: Reservation,
        payload: Vec<u8>,
        hook: Option<CommitHook>,
    ) -> Result<PendingAppend, WalError> {
        if payload.len() != r.len {
            return Err(WalError::Serialization(format!(
                "append_reserved: payload is {} bytes, reservation was for {}",
                payload.len(),
                r.len
            )));
        }
        let sender = self.sender.as_ref().ok_or(WalError::Closed)?;
        let (tx, rx) = oneshot::channel();
        let now = now_ms();
        // L-b: claim the "oldest still queued" stamp *before* `queued_bytes` goes above
        // zero, and only if the queue was genuinely empty (`compare_exchange`, not a
        // load-then-store): otherwise a health check that runs in the gap between the two
        // could see `queued_bytes > 0` with a stale-or-missing timestamp, and a second
        // appender racing this one could clobber the first appender's own stamp.
        let _ = self.shared.oldest_enqueued_ms.compare_exchange(
            0,
            now,
            Ordering::AcqRel,
            Ordering::Relaxed,
        );
        let request = Request::Append {
            payload,
            hook,
            reply: Reply(Some(tx)),
            enqueued_at_ms: now,
            _permit: r.permit,
        };
        self.shared.queued_bytes.fetch_add(r.len, Ordering::AcqRel);
        sender.send(request).map_err(|_| WalError::Closed)?;
        Ok(PendingAppend { rx })
    }

    /// `reserve` + `append_reserved` + await.
    pub async fn append(
        &self,
        payload: Vec<u8>,
        hook: Option<CommitHook>,
    ) -> Result<RecordLoc, WalError> {
        let r = self.reserve(payload.len()).await?;
        self.append_reserved(r, payload, hook)?.await
    }

    /// For callers that are not async (checkpoint, compaction, tests). Waits with
    /// [`block_on_unbudgeted`] on the same `reserve`/`PendingAppend` futures; never with
    /// tokio's `blocking_recv`/`blocking_lock`, which panic when called from inside a
    /// runtime. Safe on a runtime worker thread because the writer is a `std::thread`, not
    /// a task that this blocked worker would have to run; it only blocks that worker for
    /// the duration of the write.
    pub fn append_blocking(
        &self,
        payload: Vec<u8>,
        hook: Option<CommitHook>,
    ) -> Result<RecordLoc, WalError> {
        block_on_unbudgeted(self.append(payload, hook))
    }

    /// Makes every write acknowledged so far durable; returns the durable watermark.
    pub async fn sync(&self) -> Result<Lsn, WalError> {
        let sender = self.sender.as_ref().ok_or(WalError::Closed)?;
        let (tx, rx) = oneshot::channel();
        sender
            .send(Request::Sync { reply: tx })
            .map_err(|_| WalError::Closed)?;
        rx.await.unwrap_or(Err(WalError::Closed))
    }

    /// `sync` for non-async callers; waits with [`block_on_unbudgeted`] (see
    /// `append_blocking`).
    pub fn sync_blocking(&self) -> Result<Lsn, WalError> {
        block_on_unbudgeted(self.sync())
    }

    /// Seals the active segment if it holds at least one frame, exactly as a roll on size
    /// does: `sync_data` of the old segment, then the next segment created, its header
    /// synced, and the directory synced. Returns the new active segment's first LSN, or
    /// `None` (nothing done) when the active segment is empty. For a stream's retention,
    /// which can only remove sealed segments (Task 2.15b.1, design note §8.2). A failure
    /// poisons the log, like any writer I/O error.
    pub async fn roll(&self) -> Result<Option<Lsn>, WalError> {
        let sender = self.sender.as_ref().ok_or(WalError::Closed)?;
        let (tx, rx) = oneshot::channel();
        sender
            .send(Request::Roll { reply: tx })
            .map_err(|_| WalError::Closed)?;
        rx.await.unwrap_or(Err(WalError::Closed))
    }

    /// `roll` for non-async callers; waits with [`block_on_unbudgeted`] (see
    /// `append_blocking`).
    pub fn roll_blocking(&self) -> Result<Option<Lsn>, WalError> {
        block_on_unbudgeted(self.roll())
    }

    /// A receiver of `acked_lsn` (see [`Wal::acked_lsn`]), updated once per group-commit
    /// batch after every frame in it is acked, so a reader woken by
    /// [`watch::Receiver::changed`] finds those frames within [`Wal::scan_from`]'s cap.
    /// The receiver starts at the current value, already marked seen: the pattern is
    /// "scan, then wait for `changed`, then scan again", and no ack after the subscription
    /// is missed. When the log is poisoned, `changed` fires once more with the value
    /// unchanged, so a waiter can see [`Wal::health`]; it errors once the `Wal` is closed
    /// and dropped. Commit hooks are
    /// no substitute: they run before `acked_lsn` moves.
    pub fn subscribe_acked(&self) -> watch::Receiver<Lsn> {
        let mut rx = self.shared.acked_tx.subscribe();
        // Pairs with the fence in `Shared::publish_acked`; see there.
        fence(Ordering::SeqCst);
        self.shared.refresh_acked();
        rx.borrow_and_update();
        rx
    }

    /// Reads the payload of the frame at `loc`, verifying that it is that frame (CRC, LSN,
    /// payload length; see [`read_frame`]).
    ///
    /// A mismatch is `CorruptSegment`, except in a segment compaction rewrote or removed
    /// since this `Wal` was opened, where it is `Moved`: the location predates the rewrite
    /// and the caller must re-resolve it (Task 2.15). A rewrite swaps the handle and runs
    /// the caller's index update under one lock (see [`Wal::replace_segment`]), so a
    /// location read from the index after a `Moved` points into the current file.
    pub fn read(&self, loc: RecordLoc) -> Result<Vec<u8>, WalError> {
        let path = self.shared.dir.join(segment_file_name(loc.segment));
        let handle = {
            let segments = self.shared.segments.read().expect("segments lock poisoned");
            segments.get(&loc.segment).cloned()
        };
        let Some(handle) = handle else {
            if loc.segment < self.shared.removed_below.load(Ordering::Acquire) {
                return Err(WalError::Moved {
                    path,
                    offset: loc.offset,
                    lsn: loc.lsn,
                });
            }
            return Err(WalError::CorruptSegment {
                path,
                offset: loc.offset,
                reason: "no such segment".to_string(),
            });
        };
        match read_frame(&*handle.file, &path, loc) {
            Err(WalError::CorruptSegment { .. } | WalError::RecordTooLarge { .. })
                if handle.generation > 0 =>
            {
                // A location from before a swap whose index update is still running: the
                // replaced file still holds exactly that frame.
                if let Some(previous) = &handle.previous {
                    if let Ok(payload) = read_frame(&**previous, &path, loc) {
                        return Ok(payload);
                    }
                }
                Err(WalError::Moved {
                    path,
                    offset: loc.offset,
                    lsn: loc.lsn,
                })
            }
            other => other,
        }
    }

    /// Visits acked frames with lsn >= `from`, in order (reads through `Vfs`). "Acked"
    /// means answered `Ok` to the caller (`acked_lsn`, see `Shared`'s field docs) — in
    /// `SyncMode::Fast` this is ahead of `durable_lsn`, since a write is answered right
    /// after it reaches the OS, before the periodic sync catches up. This is the right
    /// default for a consumer whose own cursor must not skip an acked write (Task 2.8a's
    /// `get_changes_since`/`read_from`): capping at `durable_lsn` instead would let a
    /// cursor jump straight past a Fast-acked record that hasn't synced yet. A caller that
    /// must never see anything that could still be lost to a crash (compaction, a
    /// checkpoint) wants [`Wal::scan_durable_from`] instead.
    pub fn scan_from(&self, from: Lsn, visit: &mut ScanVisitor<'_>) -> Result<(), WalError> {
        self.scan_from_capped(from, self.acked_lsn(), &mut |loc, kind, payload| {
            visit(loc, kind, payload).map(|()| ControlFlow::Continue(()))
        })
        .map(|_| ())
    }

    /// [`Wal::scan_from`] with a visitor that can stop the scan: `ControlFlow::Break`
    /// ends it after that frame, and is returned. A bounded read uses it so it never
    /// decodes the rest of the log (Task 2.15b.1).
    pub fn scan_from_flow(
        &self,
        from: Lsn,
        visit: &mut ScanFlowVisitor<'_>,
    ) -> Result<ControlFlow<()>, WalError> {
        self.scan_from_capped(from, self.acked_lsn(), visit)
    }

    /// Like [`Wal::scan_from`], but bounded by `durable_lsn` instead of `acked_lsn`: never
    /// visits a frame that a crash right now could still lose. For compaction and
    /// checkpoint callers, which must only ever persist state built from data that is
    /// already durable.
    pub fn scan_durable_from(
        &self,
        from: Lsn,
        visit: &mut ScanVisitor<'_>,
    ) -> Result<(), WalError> {
        self.scan_from_capped(from, self.durable_lsn(), &mut |loc, kind, payload| {
            visit(loc, kind, payload).map(|()| ControlFlow::Continue(()))
        })
        .map(|_| ())
    }

    /// [`Wal::scan_durable_from`] with a visitor that can stop the scan (see
    /// [`Wal::scan_from_flow`]).
    pub fn scan_durable_from_flow(
        &self,
        from: Lsn,
        visit: &mut ScanFlowVisitor<'_>,
    ) -> Result<ControlFlow<()>, WalError> {
        self.scan_from_capped(from, self.durable_lsn(), visit)
    }

    /// Starts at the segment holding `from` (the last one whose first LSN is `<= from`,
    /// or the oldest if `from` precedes them all), so a read near the tail of a long log
    /// opens no earlier segment (Task 2.15b.1); frames below `from` in that segment are
    /// still decoded and skipped.
    ///
    /// M3: a frame past `cap` is never visited, and ends the scan (frames are in LSN
    /// order). The cap is checked in every segment, not only the last one listed: `cap`
    /// is sampled before the segments are listed, and a roll in between seals the segment
    /// it was sampled against (STO-15). It costs the same one comparison per frame.
    ///
    /// Every segment but the last one listed is sealed, and a sealed segment is whole: a
    /// scan fault in it, or an end short of the LSN the next segment starts at (a
    /// segment cut exactly between two frames scans without a fault, STO-14), is
    /// `CorruptSegment`. That end check runs once per segment the scan reads to its end.
    /// A fault on the last segment's tail is expected (a write in flight, or a crash not
    /// yet recovered from) and is silently bounded by `cap` regardless of whether
    /// `scan_segment` itself reports a fault. A CRC-valid frame of an unknown kind is
    /// `UnsupportedFormat` in any segment (STO-11).
    fn scan_from_capped(
        &self,
        from: Lsn,
        cap: Lsn,
        visit: &mut ScanFlowVisitor<'_>,
    ) -> Result<ControlFlow<()>, WalError> {
        self.scan_from_loc_capped(from, cap, None, visit)
    }

    /// Visits frames from `from` through `min(cap, acked_lsn)`, inclusive. A durable
    /// reader supplies `durable_lsn` as its cap. The cap applies to every segment.
    ///
    /// A sparse index may supply a frame location at or before `from` in the selected
    /// starting segment. A stale, unrelated, or invalid hint falls back to scanning
    /// that segment from its header. Handles are cloned under the segments lock, so
    /// retention or a roll cannot change this scan's segment snapshot.
    pub fn scan_from_loc_capped(
        &self,
        from: Lsn,
        cap: Lsn,
        start: Option<RecordLoc>,
        visit: &mut ScanFlowVisitor<'_>,
    ) -> Result<ControlFlow<()>, WalError> {
        let cap = cap.min(self.acked_lsn());
        let segments: Vec<(Lsn, Arc<dyn VfsFile>)> = {
            let segments = self.shared.segments.read().expect("segments lock poisoned");
            let first = segments
                .range(..=from)
                .next_back()
                .map_or(0, |(first, _)| *first);
            segments
                .range(first..)
                .map(|(k, v)| (*k, v.file.clone()))
                .collect()
        };
        let last_idx = segments.len().saturating_sub(1);
        for (idx, (first_lsn, file)) in segments.iter().enumerate() {
            if *first_lsn > cap {
                break;
            }
            let is_last = idx == last_idx;
            let path = self.shared.dir.join(segment_file_name(*first_lsn));
            let hint = start.filter(|loc| {
                idx == 0
                    && loc.segment == *first_lsn
                    && loc.lsn >= *first_lsn
                    && loc.lsn <= from
                    && loc.offset >= SEGMENT_HEADER_LEN
                    && read_frame(&**file, &path, *loc).is_ok()
            });
            let mut visitor_broke = false;
            let mut cap_reached = false;
            let mut capped_visit = |loc: RecordLoc, kind, payload: &[u8]| {
                if loc.lsn > cap {
                    cap_reached = true;
                    return Ok(ControlFlow::Break(()));
                }
                if loc.lsn >= from {
                    let flow = visit(loc, kind, payload)?;
                    if flow.is_break() {
                        visitor_broke = true;
                        return Ok(flow);
                    }
                }
                // Stop at the cap itself: do not decode even the following frame.
                if loc.lsn == cap {
                    cap_reached = true;
                    return Ok(ControlFlow::Break(()));
                }
                Ok(ControlFlow::Continue(()))
            };
            let scan = match hint {
                Some(loc) => scan_segment_from(
                    &**file,
                    &path,
                    *first_lsn,
                    (loc.lsn, loc.offset),
                    &mut capped_visit,
                )?,
                None => scan_segment_flow(&**file, &path, *first_lsn, &mut capped_visit)?,
            };
            if visitor_broke {
                return Ok(ControlFlow::Break(()));
            }
            if cap_reached {
                break;
            }
            if !is_last {
                check_whole(&path, &scan, segments[idx + 1].0)?;
            }
        }
        Ok(ControlFlow::Continue(()))
    }

    pub fn next_lsn(&self) -> Lsn {
        self.shared.next_lsn.load(Ordering::Acquire)
    }

    pub fn durable_lsn(&self) -> Lsn {
        self.shared.durable_lsn.load(Ordering::Acquire)
    }

    /// The watermark of LSNs answered `Ok` to their caller so far (see `Shared::acked_lsn`
    /// docs). `>= durable_lsn` always; equal to it in `SyncMode::Durable`.
    pub fn acked_lsn(&self) -> Lsn {
        self.shared.acked_lsn.load(Ordering::Acquire)
    }

    pub fn health(&self) -> WalHealth {
        self.shared.health_snapshot()
    }

    /// What may release segments from the front of this log (fixed at open).
    pub fn front_release(&self) -> FrontRelease {
        self.shared.front_release
    }

    /// First LSNs of the segments, oldest first (tests, compaction).
    pub fn segments(&self) -> Vec<Lsn> {
        self.shared
            .segments
            .read()
            .expect("segments lock poisoned")
            .keys()
            .copied()
            .collect()
    }

    /// The sealed segments, oldest first: every segment but the active (last) one.
    /// Compaction only ever touches these (Task 2.15). A segment is sealed once the writer
    /// rolled past it, which it does only after syncing it, so a sealed segment is whole
    /// and durable.
    pub fn sealed_segments(&self) -> Result<Vec<SealedSegment>, WalError> {
        let segments: Vec<(Lsn, Arc<dyn VfsFile>)> = self
            .shared
            .segments
            .read()
            .expect("segments lock poisoned")
            .iter()
            .map(|(k, v)| (*k, v.file.clone()))
            .collect();
        segments
            .windows(2)
            .map(|pair| {
                Ok(SealedSegment {
                    first_lsn: pair[0].0,
                    next_lsn: pair[1].0,
                    len: pair[0].1.len()?,
                })
            })
            .collect()
    }

    /// Total bytes of every segment file, the active one included.
    pub fn log_bytes(&self) -> Result<u64, WalError> {
        let files: Vec<Arc<dyn VfsFile>> = self
            .shared
            .segments
            .read()
            .expect("segments lock poisoned")
            .values()
            .map(|h| h.file.clone())
            .collect();
        let mut total = 0;
        for file in files {
            total += file.len()?;
        }
        Ok(total)
    }

    /// The path of the segment file starting at `first_lsn`.
    pub fn segment_path(&self, first_lsn: Lsn) -> PathBuf {
        self.shared.dir.join(segment_file_name(first_lsn))
    }

    /// How many times compaction replaced the segment starting at `first_lsn` since this
    /// `Wal` was opened; `None` if there is no such segment.
    pub fn segment_generation(&self, first_lsn: Lsn) -> Option<u64> {
        self.shared
            .segments
            .read()
            .expect("segments lock poisoned")
            .get(&first_lsn)
            .map(|h| h.generation)
    }

    /// The current handle of the sealed segment starting at `first_lsn`, and the first LSN
    /// of the segment after it.
    fn sealed_handle(&self, first_lsn: Lsn) -> Result<(SegmentHandle, Lsn), WalError> {
        let segments = self.shared.segments.read().expect("segments lock poisoned");
        let handle = segments.get(&first_lsn).cloned().ok_or_else(|| {
            WalError::CompactionRefused(format!("no segment starts at LSN {first_lsn}"))
        })?;
        let next = segments
            .range((
                std::ops::Bound::Excluded(first_lsn),
                std::ops::Bound::Unbounded,
            ))
            .next()
            .map(|(k, _)| *k)
            .ok_or_else(|| {
                WalError::CompactionRefused(format!(
                    "the segment at LSN {first_lsn} is the active segment; only sealed \
                     segments are compacted"
                ))
            })?;
        Ok((handle, next))
    }

    /// Visits every frame of the sealed segment starting at `first_lsn`, through its
    /// current handle. A sealed segment is whole: any frame fault, or an LSN range that
    /// does not end where the next segment starts, is `CorruptSegment`.
    pub fn scan_sealed(
        &self,
        first_lsn: Lsn,
        visit: &mut ScanVisitor<'_>,
    ) -> Result<SegmentScan, WalError> {
        let (handle, next_lsn) = self.sealed_handle(first_lsn)?;
        let path = self.segment_path(first_lsn);
        let scan = scan_segment(&*handle.file, &path, first_lsn, visit)?;
        check_whole(&path, &scan, next_lsn)?;
        Ok(scan)
    }

    /// Replaces the sealed segment starting at `first_lsn` with the file at `compacted`
    /// (Task 2.15). The caller has written `compacted` completely and `sync_data`ed it.
    ///
    /// 1. `compacted` is checked: a segment header for `first_lsn`, every frame whole, and
    ///    exactly the LSN range of the segment it replaces (`CompactionRefused` if not;
    ///    nothing changes).
    /// 2. `rename(compacted, {first_lsn:020}.wal)`, then `sync_dir`. The rename is atomic:
    ///    a crash leaves the old file or the new one under the name, never neither.
    /// 3. Under the segments lock, in constant time: the read handle is swapped, the
    ///    segment's generation bumped, and the replaced file kept as the segment's
    ///    `previous` handle. A location from before the swap that does not match the new
    ///    file is read from the replaced one, which still holds exactly that frame, so the
    ///    caller moves its index to the new offsets *after* this returns, outside the lock
    ///    (a segment roll on the writer thread never waits for it), and then calls
    ///    [`Wal::release_replaced`]. After the release, a stale location is `Moved` and
    ///    re-resolves to the moved index entry.
    ///
    /// If the rename succeeded, the swap happens even when the `sync_dir` fails (the name
    /// already points at the new file), and the sync error is returned afterwards so the
    /// caller stops: the rename is then not known to be durable. The caller still moves
    /// its index and releases the replaced file.
    pub fn replace_segment(&self, first_lsn: Lsn, compacted: &Path) -> Result<(), WalError> {
        let (_, next_lsn) = self.sealed_handle(first_lsn)?;
        let target = self.segment_path(first_lsn);
        let file = self.shared.vfs.open(compacted, OpenMode::Read)?;
        let scan = scan_segment(&*file, compacted, first_lsn, &mut |_, _, _| Ok(()))?;
        if scan.stopped.is_some() || scan.next_lsn != next_lsn {
            return Err(WalError::CompactionRefused(format!(
                "{} does not hold exactly LSNs {first_lsn}..{next_lsn} of the segment it \
                 would replace (scan: {scan:?})",
                compacted.display()
            )));
        }
        self.shared.vfs.rename(compacted, &target)?;
        let synced = self.shared.vfs.sync_dir(&self.shared.dir);
        {
            let mut segments = self
                .shared
                .segments
                .write()
                .expect("segments lock poisoned");
            let replaced = segments.get(&first_lsn).cloned();
            let generation = replaced.as_ref().map_or(0, |h| h.generation) + 1;
            segments.insert(
                first_lsn,
                SegmentHandle {
                    file,
                    generation,
                    previous: replaced.map(|h| h.file),
                },
            );
        }
        synced.map_err(WalError::Io)
    }

    /// Drops the file a [`Wal::replace_segment`] replaced, once the caller's index points
    /// at the new offsets. No-op if there is none.
    pub fn release_replaced(&self, first_lsn: Lsn) {
        let mut segments = self
            .shared
            .segments
            .write()
            .expect("segments lock poisoned");
        if let Some(handle) = segments.get_mut(&first_lsn) {
            handle.previous = None;
        }
    }

    /// The durable log state: where the log starts and the compaction floor.
    pub fn log_state(&self) -> LogState {
        *self
            .shared
            .log_state
            .lock()
            .expect("log state lock poisoned")
    }

    /// The change-stream compaction floor: the highest LSN of a `Delete` compaction dropped
    /// (0 = none). A cursor below it may have missed that delete; a cursor at or above it
    /// has missed only superseded puts, whose last write it still sees.
    pub fn deletes_compacted_through(&self) -> Lsn {
        self.shared
            .deletes_compacted_through
            .load(Ordering::Acquire)
    }

    /// Raises the compaction floor to `lsn` (no-op if already there) and makes it durable
    /// in `LOG_STATE`. Compaction calls it before the renames that drop a delete at `lsn`;
    /// the in-memory floor rises before the file is written.
    pub fn raise_deletes_compacted_through(&self, lsn: Lsn) -> Result<(), WalError> {
        let mut state = self
            .shared
            .log_state
            .lock()
            .expect("log state lock poisoned");
        if lsn <= state.deletes_compacted_through {
            return Ok(());
        }
        self.shared
            .deletes_compacted_through
            .fetch_max(lsn, Ordering::AcqRel);
        let next = LogState {
            deletes_compacted_through: lsn,
            ..*state
        };
        next.write(&*self.shared.vfs, &self.shared.dir)?;
        *state = next;
        Ok(())
    }

    /// Moves the durable log start to `upto` (the first LSN of a later segment, so every
    /// segment before it is sealed) after checking, under [`FrontRelease::ElidedOnly`],
    /// that every segment before it is fully elided. Under [`FrontRelease::Retention`]
    /// that check is skipped: retention releases live data on purpose. Called before
    /// [`Wal::remove_leading_segments`], so a crash in between leaves segments `open`
    /// recognises as released and removes.
    pub fn set_log_start(&self, upto: Lsn) -> Result<(), WalError> {
        let mut state = self
            .shared
            .log_state
            .lock()
            .expect("log state lock poisoned");
        if upto <= state.log_start {
            return Ok(());
        }
        let leading: Vec<Lsn> = {
            let segments = self.shared.segments.read().expect("segments lock poisoned");
            if !segments.contains_key(&upto) {
                return Err(WalError::CompactionRefused(format!(
                    "no segment starts at LSN {upto}, so it cannot become the log start"
                )));
            }
            segments.range(..upto).map(|(k, _)| *k).collect()
        };
        if self.shared.front_release == FrontRelease::ElidedOnly {
            for first in leading {
                self.check_fully_elided(first)?;
            }
        }
        let next = LogState {
            log_start: upto,
            ..*state
        };
        next.write(&*self.shared.vfs, &self.shared.dir)?;
        *state = next;
        Ok(())
    }

    fn check_fully_elided(&self, first_lsn: Lsn) -> Result<(), WalError> {
        let (handle, next_lsn) = self.sealed_handle(first_lsn)?;
        let path = self.segment_path(first_lsn);
        let scan = scan_segment(&*handle.file, &path, first_lsn, &mut |loc, kind, _| {
            if kind == FrameKind::Elided {
                Ok(())
            } else {
                Err(WalError::CompactionRefused(format!(
                    "{} still holds a live frame (lsn {}); only fully elided segments \
                     are released",
                    path.display(),
                    loc.lsn
                )))
            }
        })?;
        check_whole(&path, &scan, next_lsn)
    }

    /// Removes, oldest first, every segment that starts before `upto`. `upto` must not be
    /// past the durable log start ([`Wal::set_log_start`] first; `CompactionRefused`
    /// otherwise), each segment must be sealed, and under [`FrontRelease::ElidedOnly`]
    /// each is checked fully elided again. Each removal is `remove` + `sync_dir` before
    /// the next. Returns how many segments were removed.
    pub fn remove_leading_segments(&self, upto: Lsn) -> Result<usize, WalError> {
        let log_start = self
            .shared
            .log_state
            .lock()
            .expect("log state lock poisoned")
            .log_start;
        if upto > log_start {
            return Err(WalError::CompactionRefused(format!(
                "segments before LSN {upto} are not released: the durable log start is \
                 {log_start}"
            )));
        }
        let mut removed = 0;
        loop {
            let oldest = {
                let segments = self.shared.segments.read().expect("segments lock poisoned");
                segments.keys().next().copied()
            };
            let Some(first_lsn) = oldest.filter(|first| *first < upto) else {
                return Ok(removed);
            };
            if self.shared.front_release == FrontRelease::ElidedOnly {
                self.check_fully_elided(first_lsn)?;
            }
            let (_, next_lsn) = self.sealed_handle(first_lsn)?;
            let path = self.segment_path(first_lsn);
            self.shared.vfs.remove(&path)?;
            let synced = self.shared.vfs.sync_dir(&self.shared.dir);
            {
                let mut segments = self
                    .shared
                    .segments
                    .write()
                    .expect("segments lock poisoned");
                segments.remove(&first_lsn);
                self.shared.removed_below.store(next_lsn, Ordering::Release);
            }
            synced.map_err(WalError::Io)?;
            removed += 1;
        }
    }

    /// Drains the queue, syncs, joins the writer. `Drop` does the same and logs errors.
    pub fn close(mut self) -> Result<(), WalError> {
        self.close_internal()
    }

    fn close_internal(&mut self) -> Result<(), WalError> {
        let mut result = Ok(());
        if let Some(sender) = self.sender.take() {
            let (tx, rx) = oneshot::channel();
            if sender.send(Request::Close { reply: tx }).is_ok() {
                result = block_on_unbudgeted(rx).unwrap_or(Err(WalError::Closed));
            }
            drop(sender);
        }
        if let Some(handle) = self.writer.take() {
            if handle.join().is_err() {
                tracing::warn!("WAL writer thread panicked during shutdown");
            }
        }
        result
    }

    fn fail_fast_if_unhealthy(&self) -> Result<(), WalError> {
        match self.shared.health_snapshot() {
            WalHealth::Poisoned(reason) => Err(WalError::Poisoned(reason)),
            WalHealth::Closed => Err(WalError::Closed),
            WalHealth::Healthy | WalHealth::Stalled { .. } => Ok(()),
        }
    }
}

impl Drop for Wal {
    fn drop(&mut self) {
        if self.sender.is_some() {
            if let Err(e) = self.close_internal() {
                tracing::warn!(error = %e, "error while closing WAL on drop");
            }
        }
    }
}

// ---------------------------------------------------------------------------------------
// Writer thread
// ---------------------------------------------------------------------------------------

struct DrainedAppend {
    payload: Vec<u8>,
    hook: Option<CommitHook>,
    reply: Reply,
    permit_len: usize,
    /// Held until the item is answered (success or failure), per the design note that
    /// admission permits are released only once the writer answers the request.
    _permit: OwnedSemaphorePermit,
}

fn writer_thread(
    shared: Arc<Shared>,
    rx: mpsc::Receiver<Request>,
    active: ActiveSegment,
    opts: WalOptions,
    dir: PathBuf,
    vfs: Arc<dyn Vfs>,
) {
    let mut active = active;
    let result = catch_unwind(AssertUnwindSafe(|| {
        writer_body(&shared, &rx, &mut active, &opts, &dir, &vfs)
    }));
    if let Err(panic) = result {
        let msg = panic_message(&panic);
        poison(&shared, format!("writer panicked: {msg}"));
        // Keep answering anything already queued (and anything that arrives before the
        // sender side notices the log is poisoned) with `Poisoned`, then exit once the
        // channel disconnects.
        drain_after_poison(&shared, &rx);
    }
}

fn panic_message(panic: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = panic.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = panic.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic".to_string()
    }
}

/// Marks the log poisoned. Keeps the first cause: a later failure (for example the writer
/// panicking while it answers requests after an I/O error) must not hide the one that
/// explains it.
fn poison(shared: &Arc<Shared>, reason: String) {
    let first = {
        let mut health = shared.health.write().expect("health lock poisoned");
        let first = *health == InternalHealth::Running;
        if first {
            *health = InternalHealth::Poisoned(reason);
        }
        first
    };
    if first {
        // No ack will ever come again: wake every `subscribe_acked` waiter (the value is
        // unchanged) so it finds `health()` poisoned instead of waiting forever.
        shared.acked_tx.send_modify(|_| {});
    }
}

fn drain_after_poison(shared: &Arc<Shared>, rx: &mpsc::Receiver<Request>) {
    while let Ok(req) = rx.recv() {
        answer_poisoned(shared, req);
    }
}

fn answer_poisoned(shared: &Arc<Shared>, req: Request) {
    let reason = match &*shared.health.read().expect("health lock poisoned") {
        InternalHealth::Poisoned(r) => r.clone(),
        _ => "poisoned".to_string(),
    };
    match req {
        Request::Append { payload, reply, .. } => {
            shared.queued_bytes.fetch_sub(
                payload
                    .len()
                    .min(shared.queued_bytes.load(Ordering::Acquire)),
                Ordering::AcqRel,
            );
            reply.answer(Err(WalError::Poisoned(reason)));
        }
        Request::Sync { reply } => {
            let _ = reply.send(Err(WalError::Poisoned(reason)));
        }
        Request::Roll { reply } => {
            let _ = reply.send(Err(WalError::Poisoned(reason)));
        }
        Request::Close { reply } => {
            let _ = reply.send(Err(WalError::Poisoned(reason)));
        }
    }
}

/// Returns when the writer should exit (a clean `Close`, or the channel disconnecting
/// because every `Wal` handle was dropped without calling `close`).
fn writer_body(
    shared: &Arc<Shared>,
    rx: &mpsc::Receiver<Request>,
    active: &mut ActiveSegment,
    opts: &WalOptions,
    dir: &Path,
    vfs: &Arc<dyn Vfs>,
) {
    let mut pending: Option<Request> = None;
    // Tracks the highest LSN written (but, in Fast mode, possibly not yet synced).
    let mut last_written_lsn: Lsn = shared.durable_lsn.load(Ordering::Acquire);
    let mut unsynced_since: Option<Instant> = None;

    loop {
        let req = match pending.take() {
            Some(r) => r,
            None => {
                let timeout = unsynced_since.map(|since| {
                    opts.sync_interval
                        .saturating_sub(since.elapsed())
                        .max(Duration::from_millis(0))
                });
                match timeout {
                    None => match rx.recv() {
                        Ok(r) => r,
                        Err(_) => return, // every handle dropped: treat as an implicit close
                    },
                    Some(t) => match rx.recv_timeout(t) {
                        Ok(r) => r,
                        Err(RecvTimeoutError::Timeout) => {
                            if let Err(e) = active.file.sync_data() {
                                poison(shared, format!("periodic Fast sync failed: {e}"));
                                drain_after_poison(shared, rx);
                                return;
                            }
                            shared
                                .durable_lsn
                                .store(last_written_lsn, Ordering::Release);
                            unsynced_since = None;
                            continue;
                        }
                        Err(RecvTimeoutError::Disconnected) => return,
                    },
                }
            }
        };

        match req {
            Request::Close { reply } => {
                // Drain and commit whatever is already queued before the final sync, so a
                // caller that enqueues writes and then calls `close()` doesn't lose them.
                let mut drained = Vec::new();
                while let Ok(more) = rx.try_recv() {
                    match more {
                        Request::Append { .. } => drained.push(more),
                        // A Sync/Close racing in behind this Close cannot be honoured once
                        // shutdown has started; answer it the same way a dropped sender
                        // would (see `Reply`'s drop guard for the Append case).
                        Request::Sync { reply } => {
                            let _ = reply.send(Err(WalError::Closed));
                        }
                        Request::Roll { reply } => {
                            let _ = reply.send(Err(WalError::Closed));
                        }
                        Request::Close { reply } => {
                            let _ = reply.send(Err(WalError::Closed));
                        }
                    }
                }
                // L2: chunk by `max_batch_bytes` rather than committing everything queued
                // as one giant write, so a close under heavy backlog does not bypass the
                // same batch-size cap every other write goes through.
                let mut poisoned_during_drain = false;
                let mut chunk: Vec<Request> = Vec::new();
                let mut chunk_bytes = 0usize;
                let mut drained_iter = drained.into_iter();
                while let Some(item) = drained_iter.next() {
                    let len = request_payload_len(&item);
                    if !chunk.is_empty() && chunk_bytes + len > opts.max_batch_bytes {
                        commit_batch(
                            shared,
                            active,
                            opts,
                            dir,
                            vfs,
                            drained_into(std::mem::take(&mut chunk)),
                            &mut last_written_lsn,
                            &mut unsynced_since,
                        );
                        chunk_bytes = 0;
                        if matches!(
                            &*shared.health.read().expect("health lock poisoned"),
                            InternalHealth::Poisoned(_)
                        ) {
                            // L-a: `item` (the one that just triggered this chunk boundary)
                            // and everything still unconsumed in `drained_iter` must not
                            // fall through to a bare `Closed` via `Reply`'s drop guard when
                            // we `break` below — answer them `Poisoned`, like every other
                            // still-queued request once the log is poisoned.
                            answer_poisoned(shared, item);
                            for rest in drained_iter {
                                answer_poisoned(shared, rest);
                            }
                            poisoned_during_drain = true;
                            break;
                        }
                    }
                    chunk_bytes += len;
                    chunk.push(item);
                }
                if !poisoned_during_drain && !chunk.is_empty() {
                    commit_batch(
                        shared,
                        active,
                        opts,
                        dir,
                        vfs,
                        drained_into(chunk),
                        &mut last_written_lsn,
                        &mut unsynced_since,
                    );
                    if matches!(
                        &*shared.health.read().expect("health lock poisoned"),
                        InternalHealth::Poisoned(_)
                    ) {
                        poisoned_during_drain = true;
                    }
                }
                if poisoned_during_drain {
                    let reason = match &*shared.health.read().expect("health lock poisoned") {
                        InternalHealth::Poisoned(r) => r.clone(),
                        _ => unreachable!(),
                    };
                    let _ = reply.send(Err(WalError::Poisoned(reason)));
                    *shared.health.write().expect("health lock poisoned") = InternalHealth::Closed;
                    drain_after_poison(shared, rx);
                    return;
                }
                let final_result = if unsynced_since.is_some() {
                    active.file.sync_data().map_err(WalError::Io)
                } else {
                    Ok(())
                };
                if final_result.is_ok() {
                    shared
                        .durable_lsn
                        .store(last_written_lsn, Ordering::Release);
                }
                *shared.health.write().expect("health lock poisoned") = InternalHealth::Closed;
                let _ = reply.send(final_result);
                // Answer anything that raced in after Close but before the sender was
                // dropped, so nothing is silently lost.
                while let Ok(more) = rx.try_recv() {
                    match more {
                        Request::Append { reply, .. } => reply.answer(Err(WalError::Closed)),
                        Request::Sync { reply } => {
                            let _ = reply.send(Err(WalError::Closed));
                        }
                        Request::Roll { reply } => {
                            let _ = reply.send(Err(WalError::Closed));
                        }
                        Request::Close { reply } => {
                            let _ = reply.send(Err(WalError::Closed));
                        }
                    }
                }
                return;
            }
            Request::Sync { reply } => {
                if unsynced_since.is_some() {
                    match active.file.sync_data() {
                        Ok(()) => {
                            shared
                                .durable_lsn
                                .store(last_written_lsn, Ordering::Release);
                            unsynced_since = None;
                        }
                        Err(e) => {
                            poison(shared, format!("sync failed: {e}"));
                            let _ =
                                reply.send(Err(WalError::Poisoned(format!("sync failed: {e}"))));
                            drain_after_poison(shared, rx);
                            return;
                        }
                    }
                }
                let _ = reply.send(Ok(shared.durable_lsn.load(Ordering::Acquire)));
            }
            Request::Roll { reply } => {
                if active.write_pos == SEGMENT_HEADER_LEN {
                    let _ = reply.send(Ok(None));
                    continue;
                }
                if let Err(e) = roll_segment(shared, active, dir, vfs) {
                    let reason = format!("segment roll failed: {e}");
                    poison(shared, reason.clone());
                    let _ = reply.send(Err(WalError::Poisoned(reason)));
                    drain_after_poison(shared, rx);
                    return;
                }
                // The roll synced every frame written so far (all in the sealed segment)
                // and raised `durable_lsn` over them: nothing is left unsynced.
                unsynced_since = None;
                let _ = reply.send(Ok(Some(active.first_lsn)));
            }
            Request::Append { .. } => {
                let mut batch = vec![req];
                let mut batch_bytes = request_payload_len(batch.last().expect("just pushed"));
                loop {
                    match rx.try_recv() {
                        Ok(Request::Append {
                            payload,
                            hook,
                            reply,
                            enqueued_at_ms,
                            _permit,
                        }) => {
                            let len = payload.len();
                            if !batch.is_empty() && batch_bytes + len > opts.max_batch_bytes {
                                pending = Some(Request::Append {
                                    payload,
                                    hook,
                                    reply,
                                    enqueued_at_ms,
                                    _permit,
                                });
                                break;
                            }
                            batch_bytes += len;
                            batch.push(Request::Append {
                                payload,
                                hook,
                                reply,
                                enqueued_at_ms,
                                _permit,
                            });
                        }
                        Ok(other) => {
                            pending = Some(other);
                            break;
                        }
                        Err(_) => break,
                    }
                }
                let drained: Vec<DrainedAppend> = batch.into_iter().map(into_drained).collect();
                commit_batch(
                    shared,
                    active,
                    opts,
                    dir,
                    vfs,
                    drained,
                    &mut last_written_lsn,
                    &mut unsynced_since,
                );
                if matches!(
                    &*shared.health.read().expect("health lock poisoned"),
                    InternalHealth::Poisoned(_)
                ) {
                    // M1: a request already dequeued into `pending` (deferred past this
                    // batch's `max_batch_bytes` cap, or a Sync/Close that arrived behind
                    // it) must not simply be dropped here — that would answer it `Closed`
                    // via `Reply`'s drop guard instead of `Poisoned`.
                    if let Some(p) = pending.take() {
                        answer_poisoned(shared, p);
                    }
                    drain_after_poison(shared, rx);
                    return;
                }
                // M2: keep `oldest_enqueued_ms` tracking the oldest request that is still
                // actually queued, using its own `enqueued_at_ms` rather than a shared
                // counter that a straight reset-to-zero could desynchronize from
                // `queued_bytes` when something is still outstanding (see `health_snapshot`).
                match &pending {
                    Some(Request::Append { enqueued_at_ms, .. }) => {
                        shared
                            .oldest_enqueued_ms
                            .store(*enqueued_at_ms, Ordering::Release);
                    }
                    _ => {
                        if shared.queued_bytes.load(Ordering::Acquire) == 0 {
                            // L-b: clear only if nothing has claimed the marker since we
                            // observed it above (`compare_exchange`, not a plain store) —
                            // an appender's own `compare_exchange(0, now)` in
                            // `append_reserved` could otherwise be undone by a writer that
                            // last checked `queued_bytes` a moment too early.
                            let current = shared.oldest_enqueued_ms.load(Ordering::Acquire);
                            let cleared = current == 0
                                || shared
                                    .oldest_enqueued_ms
                                    .compare_exchange(
                                        current,
                                        0,
                                        Ordering::AcqRel,
                                        Ordering::Relaxed,
                                    )
                                    .is_ok();
                            // Re-check: if an appender raced in between our `queued_bytes`
                            // read and the clear above, restore a fresh timestamp rather
                            // than leave the marker at 0 while something is now queued. If
                            // that appender's own stamp is already in place (its
                            // `compare_exchange` ran first), this one is a harmless no-op.
                            if cleared && shared.queued_bytes.load(Ordering::Acquire) > 0 {
                                let _ = shared.oldest_enqueued_ms.compare_exchange(
                                    0,
                                    now_ms(),
                                    Ordering::AcqRel,
                                    Ordering::Relaxed,
                                );
                            }
                        }
                    }
                }
            }
        }
    }
}

fn request_payload_len(req: &Request) -> usize {
    match req {
        Request::Append { payload, .. } => payload.len(),
        _ => 0,
    }
}

fn into_drained(req: Request) -> DrainedAppend {
    match req {
        Request::Append {
            payload,
            hook,
            reply,
            _permit,
            ..
        } => {
            let permit_len = payload.len();
            DrainedAppend {
                payload,
                hook,
                reply,
                permit_len,
                _permit,
            }
        }
        _ => unreachable!("into_drained called on a non-Append request"),
    }
}

fn drained_into(reqs: Vec<Request>) -> Vec<DrainedAppend> {
    reqs.into_iter().map(into_drained).collect()
}

/// Writes one group-commit batch: frames every item into one buffer, rolls the segment
/// first if needed, issues one `write_at`, syncs per `SyncMode`, runs hooks in LSN order,
/// then answers every reply. On any I/O error the log is poisoned and every item in this
/// batch (the caller drains and poisons everything queued afterward).
#[allow(clippy::too_many_arguments)]
fn commit_batch(
    shared: &Arc<Shared>,
    active: &mut ActiveSegment,
    opts: &WalOptions,
    dir: &Path,
    vfs: &Arc<dyn Vfs>,
    mut items: Vec<DrainedAppend>,
    last_written_lsn: &mut Lsn,
    unsynced_since: &mut Option<Instant>,
) {
    // Allocate only the legal prefix before doing any I/O, including a roll. The
    // writer alone owns allocation, so concurrent producers need no extra mutex.
    let start_lsn = shared.next_lsn.load(Ordering::Acquire);
    if let Some(limit) = opts.lsn_limit {
        let allowed = limit.saturating_sub(start_lsn).min(items.len() as u64) as usize;
        for item in items.split_off(allowed) {
            shared
                .queued_bytes
                .fetch_sub(item.permit_len, Ordering::AcqRel);
            item.reply.answer(Err(WalError::InvalidRecords(format!(
                "offset-limit {limit} exhausted: append LSN must be below the exclusive bound"
            ))));
        }
        if items.is_empty() {
            return;
        }
    }
    let total_queued: usize = items.iter().map(|i| i.permit_len).sum();

    // Roll first if this batch would push a non-empty active segment past `segment_bytes`.
    // A batch larger than `segment_bytes` still lands whole in one segment; it just gets
    // that segment to itself (it is only "non-empty" here because we always roll before
    // writing into a segment that already holds data it would overflow).
    let mut frame_bytes = 0usize;
    for item in &items {
        frame_bytes += crate::wal::frame::FRAME_HEADER_LEN + item.payload.len();
    }
    if active.write_pos > SEGMENT_HEADER_LEN
        && active.write_pos + frame_bytes as u64 > opts.segment_bytes
    {
        if let Err(e) = roll_segment(shared, active, dir, vfs) {
            fail_batch(shared, items, format!("segment roll failed: {e}"));
            return;
        }
    }

    let mut buf = Vec::with_capacity(frame_bytes);
    let mut locs = Vec::with_capacity(items.len());
    let mut lsn = start_lsn;
    let base_offset = active.write_pos;
    for item in &items {
        let offset = base_offset + buf.len() as u64;
        encode_frame(&mut buf, lsn, opts.append_kind, &item.payload);
        locs.push(RecordLoc {
            lsn,
            segment: active.first_lsn,
            offset,
            payload_len: item.payload.len() as u32,
        });
        lsn += 1;
    }

    if let Err(e) = active.file.write_at(base_offset, &buf) {
        fail_batch(shared, items, format!("write_at failed: {e}"));
        return;
    }
    active.write_pos += buf.len() as u64;
    shared.next_lsn.store(lsn, Ordering::Release);
    *last_written_lsn = lsn - 1;

    // H1: under saturation, batches keep draining via `try_recv` inside the Append arm and
    // the writer never reaches the idle `recv_timeout` branch that would otherwise run the
    // periodic Fast sync. Checking the interval here too means a continuously busy writer
    // starts a sync at its next batch boundary after the `sync_interval` target passes
    // since the first unsynced batch finished writing, whether idle or busy. The
    // target does not bound fsync completion or the amount lost on a power cut.
    let mut poison_reason: Option<String> = None;
    match opts.sync_mode {
        SyncMode::Durable => {
            if let Err(e) = active.file.sync_data() {
                fail_batch(shared, items, format!("sync_data failed: {e}"));
                return;
            }
            shared
                .durable_lsn
                .store(*last_written_lsn, Ordering::Release);
            *unsynced_since = None;
        }
        SyncMode::Fast => {
            if unsynced_since.is_none() {
                *unsynced_since = Some(Instant::now());
            }
            if unsynced_since.is_some_and(|since| since.elapsed() >= opts.sync_interval) {
                match active.file.sync_data() {
                    Ok(()) => {
                        shared
                            .durable_lsn
                            .store(*last_written_lsn, Ordering::Release);
                        *unsynced_since = None;
                    }
                    Err(e) => {
                        // This batch's own writes are already answered under Fast
                        // semantics (ack-after-write, never ack-after-sync); a failed
                        // *catch-up* sync poisons the log for everything from here on,
                        // per fsyncgate, without retroactively failing what was already
                        // acked.
                        poison_reason = Some(format!("periodic Fast sync failed: {e}"));
                    }
                }
            }
        }
    }

    shared
        .queued_bytes
        .fetch_sub(total_queued, Ordering::AcqRel);
    shared.last_progress_ms.store(now_ms(), Ordering::Release);
    if let Some(reason) = poison_reason {
        poison(shared, reason);
    }

    // M1: a hook is caller code running on the writer thread; a panic in it must not take
    // the whole writer thread down (that would also lose every later-in-batch item's
    // answer to a bare `Closed`, via `Reply`'s drop guard, instead of `Poisoned`). Once a
    // hook panics, the frames for this item and everything after it in the batch are
    // still durably on disk (or written, in Fast mode), but we can no longer trust that
    // hook-driven side effects ran in order, so the log is poisoned and every remaining
    // item in this batch — including the one whose hook panicked — is answered Poisoned.
    let mut mid_batch_poison: Option<String> = None;
    for (item, loc) in items.into_iter().zip(locs) {
        if let Some(reason) = &mid_batch_poison {
            item.reply.answer(Err(WalError::Poisoned(reason.clone())));
            continue;
        }
        if let Some(hook) = item.hook {
            match catch_unwind(AssertUnwindSafe(move || hook(loc))) {
                Ok(()) => {}
                Err(panic) => {
                    let reason = format!("commit hook panicked: {}", panic_message(&panic));
                    poison(shared, reason.clone());
                    mid_batch_poison = Some(reason.clone());
                    item.reply.answer(Err(WalError::Poisoned(reason)));
                    continue;
                }
            }
        }
        // MEDIUM (review): raise the acked watermark right before answering `Ok`, so a
        // concurrent `scan_from` can never observe `acked_lsn >= loc.lsn` before the
        // caller could possibly have observed the same `Ok`. Items answered `Poisoned`
        // above (including one whose hook panicked, and everything after it) never reach
        // this line, so `acked_lsn` only ever covers what was actually acked.
        shared.acked_lsn.fetch_max(loc.lsn, Ordering::Release);
        item.reply.answer(Ok(loc));
    }
    // Once per batch, after every `Ok` above: a reader woken here finds the whole batch
    // within `scan_from`'s cap (Task 2.15b.1).
    shared.publish_acked();
}

fn fail_batch(shared: &Arc<Shared>, items: Vec<DrainedAppend>, reason: String) {
    poison(shared, reason.clone());
    let total_queued: usize = items.iter().map(|i| i.permit_len).sum();
    shared
        .queued_bytes
        .fetch_sub(total_queued, Ordering::AcqRel);
    for item in items {
        item.reply.answer(Err(WalError::Poisoned(reason.clone())));
    }
}

fn roll_segment(
    shared: &Arc<Shared>,
    active: &mut ActiveSegment,
    dir: &Path,
    vfs: &Arc<dyn Vfs>,
) -> Result<(), WalError> {
    active.file.sync_data()?;
    let new_first_lsn = shared.next_lsn.load(Ordering::Acquire);
    // L-c: the sync just above made the whole old segment durable, in every `SyncMode`
    // (Fast included). Nothing else would otherwise advance `durable_lsn` for it until the
    // next periodic/explicit sync, so a Fast-mode `durable_lsn` reader could lag behind
    // what a crash right now would actually still keep. `fetch_max` because this can race
    // a `commit_batch` sync completing concurrently is not possible here (single writer
    // thread calls both), but stays monotonic regardless.
    shared
        .durable_lsn
        .fetch_max(new_first_lsn.saturating_sub(1), Ordering::Release);
    let path = dir.join(segment_file_name(new_first_lsn));
    let file = vfs.create(&path)?;
    write_segment_header(&*file, new_first_lsn)?;
    file.sync_data()?;
    vfs.sync_dir(dir)?;
    shared
        .segments
        .write()
        .expect("segments lock poisoned")
        .insert(new_first_lsn, SegmentHandle::new(file.clone()));
    *active = ActiveSegment {
        first_lsn: new_first_lsn,
        file,
        write_pos: SEGMENT_HEADER_LEN,
    };
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vfs::StdVfs;
    use std::sync::Barrier;

    fn opts(mode: SyncMode) -> WalOptions {
        WalOptions {
            sync_mode: mode,
            sync_interval: Duration::from_millis(20),
            segment_bytes: 1 << 20,
            max_batch_bytes: 1 << 20,
            max_queued_bytes: 4096,
            front_release: crate::wal::config::FrontRelease::ElidedOnly,
            append_kind: FrameKind::Batch,
            lsn_limit: None,
        }
    }

    /// A `VfsFile` whose `write_at` blocks on a barrier starting from its *second* call.
    /// The first call is `Wal::open`'s own synchronous segment-header write, which must
    /// complete before the test (running on the same thread) can get anywhere near the
    /// barrier; the second call is the first real batch write, which is exactly the point
    /// a test wants to catch the writer thread busy.
    struct BlockingFile {
        inner: Arc<dyn VfsFile>,
        barrier: Arc<Barrier>,
        calls: AtomicU64,
    }
    impl VfsFile for BlockingFile {
        fn write_at(&self, offset: u64, buf: &[u8]) -> std::io::Result<()> {
            if self.calls.fetch_add(1, Ordering::SeqCst) == 1 {
                self.barrier.wait();
            }
            self.inner.write_at(offset, buf)
        }
        fn read_at(&self, offset: u64, buf: &mut [u8]) -> std::io::Result<usize> {
            self.inner.read_at(offset, buf)
        }
        fn set_len(&self, len: u64) -> std::io::Result<()> {
            self.inner.set_len(len)
        }
        fn len(&self) -> std::io::Result<u64> {
            self.inner.len()
        }
        fn sync_data(&self) -> std::io::Result<()> {
            self.inner.sync_data()
        }
    }
    struct BlockingVfs {
        barrier: Arc<Barrier>,
    }
    impl Vfs for BlockingVfs {
        fn open(&self, p: &Path, m: OpenMode) -> std::io::Result<Arc<dyn VfsFile>> {
            StdVfs.open(p, m)
        }
        fn create(&self, p: &Path) -> std::io::Result<Arc<dyn VfsFile>> {
            Ok(Arc::new(BlockingFile {
                inner: StdVfs.create(p)?,
                barrier: self.barrier.clone(),
                calls: AtomicU64::new(0),
            }))
        }
        fn rename(&self, a: &Path, b: &Path) -> std::io::Result<()> {
            StdVfs.rename(a, b)
        }
        fn remove(&self, p: &Path) -> std::io::Result<()> {
            StdVfs.remove(p)
        }
        fn create_dir_all(&self, p: &Path) -> std::io::Result<()> {
            StdVfs.create_dir_all(p)
        }
        fn read_dir(&self, p: &Path) -> std::io::Result<Vec<PathBuf>> {
            StdVfs.read_dir(p)
        }
        fn exists(&self, p: &Path) -> std::io::Result<bool> {
            StdVfs.exists(p)
        }
        fn sync_dir(&self, d: &Path) -> std::io::Result<()> {
            StdVfs.sync_dir(d)
        }
        fn lock_exclusive(&self, p: &Path) -> std::io::Result<Box<dyn crate::vfs::LockGuard>> {
            StdVfs.lock_exclusive(p)
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn reserve_blocks_while_the_writer_is_busy_then_completes() {
        let dir = tempfile::tempdir().unwrap();
        let barrier = Arc::new(Barrier::new(2));
        let vfs = Arc::new(BlockingVfs {
            barrier: barrier.clone(),
        });
        let mut o = opts(SyncMode::Fast);
        o.max_queued_bytes = 8; // tiny, so a second reservation must wait
        let (wal, _) = Wal::open(vfs, dir.path(), o, 1, &mut |_, _, _| Ok(())).unwrap();
        let wal = Arc::new(wal);

        // Fill the tiny admission budget; the writer will block in `write_at` on the
        // barrier once it picks this up.
        let w1 = wal.clone();
        let first = tokio::spawn(async move { w1.append(vec![0u8; 8], None).await });

        // Give the writer a moment to pick up the first request and block on the barrier.
        tokio::time::sleep(Duration::from_millis(50)).await;

        let w2 = wal.clone();
        let mut second = tokio::spawn(async move { w2.reserve(8).await });
        let res = tokio::time::timeout(Duration::from_millis(100), &mut second).await;
        assert!(
            res.is_err(),
            "reserve must stay pending while admission is exhausted"
        );

        barrier.wait(); // release the writer
        let first_result = first.await.unwrap();
        assert!(first_result.is_ok());

        // L1: the second reservation must complete promptly once admission frees up.
        let second_result = tokio::time::timeout(Duration::from_secs(2), second)
            .await
            .expect("second reservation must complete within 2s of the barrier releasing")
            .expect("join");
        assert!(second_result.is_ok());

        Arc::try_unwrap(wal).ok().unwrap().close().unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn dropping_a_pending_append_still_commits_it() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = Wal::open(
            Arc::new(StdVfs),
            dir.path(),
            opts(SyncMode::Durable),
            1,
            &mut |_, _, _| Ok(()),
        )
        .unwrap();
        let r = wal.reserve(5).await.unwrap();
        let pending = wal.append_reserved(r, b"hello".to_vec(), None).unwrap();
        drop(pending); // does not cancel the write
                       // Give the writer a moment to commit.
        tokio::time::sleep(Duration::from_millis(50)).await;
        wal.close().unwrap();

        let mut seen = Vec::new();
        let (_, _) = Wal::open(
            Arc::new(StdVfs),
            dir.path(),
            opts(SyncMode::Durable),
            1,
            &mut |loc, _, p| {
                seen.push((loc.lsn, p.to_vec()));
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(seen, vec![(1, b"hello".to_vec())]);
    }

    /// The drop guard, in isolation: a request destroyed without an answer (writer panic,
    /// queue teardown) answers `Err(Closed)` rather than closing the channel silently, so
    /// its caller is never left with a bare `oneshot` cancellation. (Moved here from the
    /// adapter's `PendingWrite` tests in Task 2.8c.)
    #[test]
    fn dropping_an_unanswered_reply_answers_closed() {
        let (tx, mut rx) = oneshot::channel();
        drop(Reply(Some(tx)));
        match rx.try_recv() {
            Ok(Err(WalError::Closed)) => {}
            other => panic!("the drop guard must answer Closed, got {other:?}"),
        }
    }

    /// The other direction: once the sender has been taken out, the guard sends nothing,
    /// or every answered request would race its own destructor.
    #[test]
    fn a_reply_whose_sender_was_taken_sends_nothing() {
        let (tx, mut rx) = oneshot::channel::<Result<RecordLoc, WalError>>();
        let mut reply = Reply(Some(tx));
        let taken = reply.0.take().expect("the reply holds its sender");
        drop(reply);
        assert!(
            matches!(rx.try_recv(), Err(oneshot::error::TryRecvError::Empty)),
            "a disarmed guard must not answer"
        );

        // `answer` is the production way to take it: the caller sees that answer only.
        drop(taken);
        let (tx, mut rx) = oneshot::channel();
        let loc = RecordLoc {
            lsn: 7,
            segment: 1,
            offset: SEGMENT_HEADER_LEN,
            payload_len: 3,
        };
        Reply(Some(tx)).answer(Ok(loc));
        match rx.try_recv() {
            Ok(Ok(got)) => assert_eq!(got, loc),
            other => panic!("the answer must arrive, not the guard's Closed: {other:?}"),
        }
    }

    #[test]
    fn append_blocking_and_sync_blocking_work_on_a_current_thread_runtime() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let (wal, _) = Wal::open(
                Arc::new(StdVfs),
                dir.path(),
                opts(SyncMode::Durable),
                1,
                &mut |_, _, _| Ok(()),
            )
            .unwrap();
            // `append_blocking`/`sync_blocking` must not need the (single) runtime worker
            // to make progress on anything else: the writer is a `std::thread`.
            let loc = wal.append_blocking(b"x".to_vec(), None).unwrap();
            assert_eq!(loc.lsn, 1);
            let synced = wal.sync_blocking().unwrap();
            assert!(synced >= 1);
            wal.close().unwrap();
        });
    }

    /// STO-15: a scan samples its cap, then lists the segments. A roll in between turns
    /// the segment the cap was taken against into a sealed one, and the cap used to be
    /// applied to the last segment only, so the frames past it were visited. The race is
    /// replayed deterministically by passing the cap a scan would have sampled before the
    /// roll.
    #[test]
    fn a_cap_sampled_before_a_roll_still_bounds_the_rolled_segment() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = Wal::open(
            Arc::new(StdVfs),
            dir.path(),
            opts(SyncMode::Durable),
            1,
            &mut |_, _, _| Ok(()),
        )
        .unwrap();
        for i in 0..4u8 {
            wal.append_blocking(vec![i; 8], None).unwrap();
        }
        let sampled_cap = 2;
        assert_eq!(wal.roll_blocking().unwrap(), Some(5));
        wal.append_blocking(vec![9; 8], None).unwrap();

        let mut seen = Vec::new();
        let flow = wal
            .scan_from_capped(1, sampled_cap, &mut |loc, _, _| {
                seen.push(loc.lsn);
                Ok(ControlFlow::Continue(()))
            })
            .unwrap();
        assert!(flow.is_continue(), "the cap is not the visitor stopping");
        assert_eq!(seen, vec![1, 2], "no frame above the sampled cap");
        wal.close().unwrap();
    }
}
