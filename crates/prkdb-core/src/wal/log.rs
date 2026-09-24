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
use crate::wal::config::{SyncMode, WalConfig};
use crate::wal::frame::{encode_frame, FrameKind, Lsn, MAX_PAYLOAD_LEN};
use crate::wal::segment::{
    read_frame, scan_segment, segment_file_name, write_segment_header, RecordLoc, ScanVisitor,
    SEGMENT_HEADER_LEN,
};
use crate::wal::WalError;
use std::collections::BTreeMap;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::{Arc, RwLock};
use std::task::{Context as TaskContext, Poll};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{oneshot, OwnedSemaphorePermit, Semaphore};

/// Options the writer thread runs under. Constructed from [`WalConfig`] with
/// [`WalOptions::from_config`].
#[derive(Debug, Clone)]
pub struct WalOptions {
    pub sync_mode: SyncMode,
    pub sync_interval: Duration,
    pub segment_bytes: u64,
    pub max_batch_bytes: usize,
    pub max_queued_bytes: usize,
}

impl WalOptions {
    pub fn from_config(c: &WalConfig) -> Self {
        WalOptions {
            sync_mode: c.sync_mode,
            sync_interval: Duration::from_millis(c.sync_interval_ms),
            segment_bytes: c.segment_bytes,
            max_batch_bytes: c.max_batch_bytes,
            max_queued_bytes: c.max_queued_bytes,
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
    health: RwLock<InternalHealth>,
    /// Read handles for `read`/`scan_from`, keyed by each segment's first LSN.
    segments: RwLock<BTreeMap<Lsn, Arc<dyn VfsFile>>>,
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
            if now.saturating_sub(last_progress) > stall_bound_ms {
                return WalHealth::Stalled {
                    queued_bytes,
                    oldest_ms: now.saturating_sub(oldest),
                };
            }
        }
        WalHealth::Healthy
    }
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
    /// Directory: if `dir` is absent, `create_dir_all(dir)` then `sync_dir` of its parent.
    /// Recovery: lists `*.wal`, sorts by first LSN, checks each segment's first LSN equals
    /// the previous segment's `next_lsn`, scans every segment, and calls `replay` for every
    /// frame with lsn >= `replay_from` in LSN order. Last segment: a torn tail is logged,
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
        if !vfs.exists(dir)? {
            vfs.create_dir_all(dir)?;
            if let Some(parent) = dir.parent() {
                if vfs.exists(parent)? {
                    vfs.sync_dir(parent)?;
                }
            }
        }

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

        let mut report = RecoveryReport::default();
        let mut next_lsn: Lsn = 1;
        let mut segments: BTreeMap<Lsn, Arc<dyn VfsFile>> = BTreeMap::new();

        for (idx, &first_lsn) in segment_lsns.iter().enumerate() {
            let is_last = idx + 1 == segment_lsns.len();
            let path = dir.join(segment_file_name(first_lsn));
            let file = vfs.open(&path, OpenMode::ReadWrite)?;

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
                segments.insert(first_lsn, file.clone());
                report.segments += 1;
                continue;
            }

            let scan = scan_segment(&*file, &path, first_lsn, &mut |loc, kind, payload| {
                report.frames += 1;
                if loc.lsn >= replay_from {
                    replay(loc, kind, payload).map_err(|e| WalError::CorruptSegment {
                        path: path.clone(),
                        offset: loc.offset,
                        reason: format!("replay of lsn {} failed: {e}", loc.lsn),
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
            segments.insert(first_lsn, file.clone());
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
            segments.insert(next_lsn, file);
            report.segments += 1;
        }
        report.next_lsn = next_lsn;

        let active_first_lsn = *segments.keys().next_back().expect("at least one segment");
        let active_file = segments
            .get(&active_first_lsn)
            .expect("just inserted")
            .clone();
        let write_pos = active_file.len()?;
        let active = ActiveSegment {
            first_lsn: active_first_lsn,
            file: active_file,
            write_pos,
        };

        let shared = Arc::new(Shared {
            next_lsn: AtomicU64::new(next_lsn),
            durable_lsn: AtomicU64::new(next_lsn.saturating_sub(1)),
            health: RwLock::new(InternalHealth::Running),
            segments: RwLock::new(segments),
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
    /// over `MAX_PAYLOAD_LEN` with `RecordTooLarge`, and returns `Poisoned`/`Closed` without
    /// waiting when the log cannot accept writes.
    pub async fn reserve(&self, len: usize) -> Result<Reservation, WalError> {
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
        let request = Request::Append {
            payload,
            hook,
            reply: Reply(Some(tx)),
            enqueued_at_ms: now_ms(),
            _permit: r.permit,
        };
        self.shared.queued_bytes.fetch_add(r.len, Ordering::AcqRel);
        if self.shared.oldest_enqueued_ms.load(Ordering::Acquire) == 0 {
            self.shared
                .oldest_enqueued_ms
                .store(now_ms(), Ordering::Release);
        }
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
    /// `futures::executor::block_on` on the same `reserve`/`PendingAppend` futures; never
    /// with tokio's `blocking_recv`/`blocking_lock`, which panic when called from inside a
    /// runtime. Safe on a runtime worker thread because the writer is a `std::thread`, not
    /// a task that this blocked worker would have to run; it only blocks that worker for
    /// the duration of the write.
    pub fn append_blocking(
        &self,
        payload: Vec<u8>,
        hook: Option<CommitHook>,
    ) -> Result<RecordLoc, WalError> {
        futures::executor::block_on(self.append(payload, hook))
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

    /// `sync` for non-async callers; waits with `futures::executor::block_on` (see
    /// `append_blocking`).
    pub fn sync_blocking(&self) -> Result<Lsn, WalError> {
        futures::executor::block_on(self.sync())
    }

    pub fn read(&self, loc: RecordLoc) -> Result<Vec<u8>, WalError> {
        let file = {
            let segments = self.shared.segments.read().expect("segments lock poisoned");
            segments.get(&loc.segment).cloned()
        };
        let file = file.ok_or_else(|| WalError::CorruptSegment {
            path: self.shared.dir.join(segment_file_name(loc.segment)),
            offset: loc.offset,
            reason: "no such segment".to_string(),
        })?;
        let path = self.shared.dir.join(segment_file_name(loc.segment));
        read_frame(&*file, &path, loc)
    }

    /// Visits committed frames with lsn >= `from`, in order (reads through `Vfs`).
    pub fn scan_from(&self, from: Lsn, visit: &mut ScanVisitor<'_>) -> Result<(), WalError> {
        let segments: Vec<(Lsn, Arc<dyn VfsFile>)> = self
            .shared
            .segments
            .read()
            .expect("segments lock poisoned")
            .iter()
            .map(|(k, v)| (*k, v.clone()))
            .collect();
        for (first_lsn, file) in segments {
            let path = self.shared.dir.join(segment_file_name(first_lsn));
            scan_segment(&*file, &path, first_lsn, &mut |loc, kind, payload| {
                if loc.lsn >= from {
                    visit(loc, kind, payload)?;
                }
                Ok(())
            })?;
        }
        Ok(())
    }

    pub fn next_lsn(&self) -> Lsn {
        self.shared.next_lsn.load(Ordering::Acquire)
    }

    pub fn durable_lsn(&self) -> Lsn {
        self.shared.durable_lsn.load(Ordering::Acquire)
    }

    pub fn health(&self) -> WalHealth {
        self.shared.health_snapshot()
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

    /// Drains the queue, syncs, joins the writer. `Drop` does the same and logs errors.
    pub fn close(mut self) -> Result<(), WalError> {
        self.close_internal()
    }

    fn close_internal(&mut self) -> Result<(), WalError> {
        let mut result = Ok(());
        if let Some(sender) = self.sender.take() {
            let (tx, rx) = oneshot::channel();
            if sender.send(Request::Close { reply: tx }).is_ok() {
                result = futures::executor::block_on(rx).unwrap_or(Err(WalError::Closed));
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

fn poison(shared: &Arc<Shared>, reason: String) {
    *shared.health.write().expect("health lock poisoned") = InternalHealth::Poisoned(reason);
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
                        Request::Close { reply } => {
                            let _ = reply.send(Err(WalError::Closed));
                        }
                    }
                }
                if !drained.is_empty() {
                    commit_batch(
                        shared,
                        active,
                        opts,
                        dir,
                        vfs,
                        drained_into(drained),
                        &mut last_written_lsn,
                        &mut unsynced_since,
                    );
                    if matches!(
                        &*shared.health.read().expect("health lock poisoned"),
                        InternalHealth::Poisoned(_)
                    ) {
                        let reason = match &*shared.health.read().expect("health lock poisoned") {
                            InternalHealth::Poisoned(r) => r.clone(),
                            _ => unreachable!(),
                        };
                        let _ = reply.send(Err(WalError::Poisoned(reason)));
                        *shared.health.write().expect("health lock poisoned") =
                            InternalHealth::Closed;
                        drain_after_poison(shared, rx);
                        return;
                    }
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
                    drain_after_poison(shared, rx);
                    return;
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
    items: Vec<DrainedAppend>,
    last_written_lsn: &mut Lsn,
    unsynced_since: &mut Option<Instant>,
) {
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
    let start_lsn = shared.next_lsn.load(Ordering::Acquire);
    let mut lsn = start_lsn;
    let base_offset = active.write_pos;
    for item in &items {
        let offset = base_offset + buf.len() as u64;
        encode_frame(&mut buf, lsn, FrameKind::Batch, &item.payload);
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

    let should_sync = match opts.sync_mode {
        SyncMode::Durable => true,
        SyncMode::Fast => false,
    };
    if should_sync {
        if let Err(e) = active.file.sync_data() {
            fail_batch(shared, items, format!("sync_data failed: {e}"));
            return;
        }
        shared
            .durable_lsn
            .store(*last_written_lsn, Ordering::Release);
        *unsynced_since = None;
    } else if unsynced_since.is_none() {
        *unsynced_since = Some(Instant::now());
    }

    shared
        .queued_bytes
        .fetch_sub(total_queued, Ordering::AcqRel);
    shared.oldest_enqueued_ms.store(0, Ordering::Release);
    shared.last_progress_ms.store(now_ms(), Ordering::Release);

    for (item, loc) in items.into_iter().zip(locs) {
        if let Some(hook) = item.hook {
            hook(loc);
        }
        item.reply.answer(Ok(loc));
    }
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
    let path = dir.join(segment_file_name(new_first_lsn));
    let file = vfs.create(&path)?;
    write_segment_header(&*file, new_first_lsn)?;
    file.sync_data()?;
    vfs.sync_dir(dir)?;
    shared
        .segments
        .write()
        .expect("segments lock poisoned")
        .insert(new_first_lsn, file.clone());
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
        let second = tokio::spawn(async move { w2.reserve(8).await });
        let res = tokio::time::timeout(Duration::from_millis(100), second).await;
        assert!(
            res.is_err(),
            "reserve must stay pending while admission is exhausted"
        );

        barrier.wait(); // release the writer
        let first_result = first.await.unwrap();
        assert!(first_result.is_ok());

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
}
