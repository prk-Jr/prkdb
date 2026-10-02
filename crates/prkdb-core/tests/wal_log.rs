//! `Wal` behaviour over StdVfs (Task 2.6). Power-loss behaviour:
//! `crates/prkdb-verify/tests/wal_power_loss.rs`.

use prkdb_core::vfs::{OpenMode, StdVfs, Vfs, VfsFile};
use prkdb_core::wal::batch::{Batch, BatchOp};
use prkdb_core::wal::frame::FrameKind;
use prkdb_core::wal::{
    CompressionConfig, FrontRelease, Lsn, RecordLoc, SyncMode, Wal, WalError, WalHealth, WalOptions,
};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::time::{Duration, Instant};

fn opts(mode: SyncMode, segment_bytes: u64) -> WalOptions {
    WalOptions {
        sync_mode: mode,
        sync_interval: Duration::from_millis(20),
        segment_bytes,
        max_batch_bytes: 1 << 20,
        max_queued_bytes: 8 << 20,
        front_release: FrontRelease::ElidedOnly,
        append_kind: prkdb_core::wal::frame::FrameKind::Batch,
        lsn_limit: None,
    }
}

fn open(vfs: Arc<dyn Vfs>, dir: &Path, o: WalOptions) -> (Wal, Vec<(Lsn, Vec<u8>)>) {
    let mut seen = Vec::new();
    let (wal, _) = Wal::open(vfs, dir, o, 1, &mut |loc, kind, p| {
        assert_eq!(kind, FrameKind::Batch);
        seen.push((loc.lsn, p.to_vec()));
        Ok(())
    })
    .unwrap();
    (wal, seen)
}

/// Wraps StdVfs, counting syncs and optionally failing them.
#[derive(Default)]
struct Probe {
    syncs: AtomicU64,
    fail_syncs: AtomicBool,
}
struct ProbeVfs(Arc<Probe>);
struct ProbeFile(Arc<dyn VfsFile>, Arc<Probe>);
impl VfsFile for ProbeFile {
    fn write_at(&self, o: u64, b: &[u8]) -> io::Result<()> {
        self.0.write_at(o, b)
    }
    fn read_at(&self, o: u64, b: &mut [u8]) -> io::Result<usize> {
        self.0.read_at(o, b)
    }
    fn set_len(&self, l: u64) -> io::Result<()> {
        self.0.set_len(l)
    }
    fn len(&self) -> io::Result<u64> {
        self.0.len()
    }
    fn sync_data(&self) -> io::Result<()> {
        if self.1.fail_syncs.load(Ordering::SeqCst) {
            return Err(io::Error::other("injected fsync failure"));
        }
        self.1.syncs.fetch_add(1, Ordering::SeqCst);
        self.0.sync_data()
    }
}
impl Vfs for ProbeVfs {
    fn open(&self, p: &Path, m: OpenMode) -> io::Result<Arc<dyn VfsFile>> {
        Ok(Arc::new(ProbeFile(StdVfs.open(p, m)?, self.0.clone())))
    }
    fn create(&self, p: &Path) -> io::Result<Arc<dyn VfsFile>> {
        Ok(Arc::new(ProbeFile(StdVfs.create(p)?, self.0.clone())))
    }
    fn rename(&self, a: &Path, b: &Path) -> io::Result<()> {
        StdVfs.rename(a, b)
    }
    fn remove(&self, p: &Path) -> io::Result<()> {
        StdVfs.remove(p)
    }
    fn create_dir_all(&self, p: &Path) -> io::Result<()> {
        StdVfs.create_dir_all(p)
    }
    fn read_dir(&self, p: &Path) -> io::Result<Vec<PathBuf>> {
        StdVfs.read_dir(p)
    }
    fn exists(&self, p: &Path) -> io::Result<bool> {
        StdVfs.exists(p)
    }
    fn sync_dir(&self, d: &Path) -> io::Result<()> {
        StdVfs.sync_dir(d)
    }
    fn lock_exclusive(&self, p: &Path) -> io::Result<Box<dyn prkdb_core::vfs::LockGuard>> {
        StdVfs.lock_exclusive(p)
    }
}

/// STO-05: replay order is append order, by global LSN, across segment rolls.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replay_order_equals_append_order_across_segment_roll() {
    let dir = tempfile::tempdir().unwrap();
    let (wal, _) = open(Arc::new(StdVfs), dir.path(), opts(SyncMode::Fast, 4096));
    let wal = Arc::new(wal);
    let mut tasks = Vec::new();
    for t in 0..8 {
        let wal = wal.clone();
        tasks.push(tokio::spawn(async move {
            let mut acked = Vec::new();
            for i in 0..200 {
                let p = format!("t{t}-{i}").into_bytes();
                let loc = wal.append(p.clone(), None).await.unwrap();
                acked.push((loc.lsn, p));
            }
            acked
        }));
    }
    let mut acked: Vec<(Lsn, Vec<u8>)> = Vec::new();
    for t in tasks {
        acked.extend(t.await.unwrap());
    }
    assert!(
        wal.segments().len() >= 3,
        "4 KiB segments must roll: {:?}",
        wal.segments()
    );
    Arc::try_unwrap(wal).ok().unwrap().close().unwrap();

    acked.sort();
    let lsns: Vec<Lsn> = acked.iter().map(|(l, _)| *l).collect();
    assert_eq!(
        lsns,
        (1..=1600).collect::<Vec<_>>(),
        "LSNs are contiguous from 1"
    );
    let (_, replayed) = open(Arc::new(StdVfs), dir.path(), opts(SyncMode::Fast, 4096));
    assert_eq!(
        replayed, acked,
        "replay yields exactly the acknowledged records, in LSN order"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn durable_appends_are_synced_before_the_ack() {
    let dir = tempfile::tempdir().unwrap();
    let probe = Arc::new(Probe::default());
    let (wal, _) = open(
        Arc::new(ProbeVfs(probe.clone())),
        dir.path(),
        opts(SyncMode::Durable, 1 << 20),
    );
    let before = probe.syncs.load(Ordering::SeqCst);
    let loc = wal.append(b"x".to_vec(), None).await.unwrap();
    assert!(
        probe.syncs.load(Ordering::SeqCst) > before,
        "ack came before any sync"
    );
    assert!(wal.durable_lsn() >= loc.lsn);
}

#[tokio::test(flavor = "multi_thread")]
async fn fast_appends_are_synced_within_the_interval_without_more_writes() {
    let dir = tempfile::tempdir().unwrap();
    let probe = Arc::new(Probe::default());
    let (wal, _) = open(
        Arc::new(ProbeVfs(probe.clone())),
        dir.path(),
        opts(SyncMode::Fast, 1 << 20),
    );
    let loc = wal.append(b"x".to_vec(), None).await.unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while wal.durable_lsn() < loc.lsn {
        assert!(std::time::Instant::now() < deadline, "never synced");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_sync_poisons_the_log_and_is_never_retried() {
    let dir = tempfile::tempdir().unwrap();
    let probe = Arc::new(Probe::default());
    let (wal, _) = open(
        Arc::new(ProbeVfs(probe.clone())),
        dir.path(),
        opts(SyncMode::Durable, 1 << 20),
    );
    wal.append(b"ok".to_vec(), None).await.unwrap();
    probe.fail_syncs.store(true, Ordering::SeqCst);
    assert!(wal.append(b"lost".to_vec(), None).await.is_err());
    probe.fail_syncs.store(false, Ordering::SeqCst);
    let later = wal.append(b"later".to_vec(), None).await;
    assert!(matches!(later, Err(WalError::Poisoned(_))), "{later:?}");
    assert!(matches!(wal.health(), WalHealth::Poisoned(_)));
}

#[tokio::test(flavor = "multi_thread")]
async fn commit_hooks_run_in_lsn_order() {
    let dir = tempfile::tempdir().unwrap();
    let (wal, _) = open(Arc::new(StdVfs), dir.path(), opts(SyncMode::Fast, 1 << 20));
    let wal = Arc::new(wal);
    let order = Arc::new(Mutex::new(Vec::new()));
    let mut tasks = Vec::new();
    for t in 0..8 {
        let (wal, order) = (wal.clone(), order.clone());
        tasks.push(tokio::spawn(async move {
            for i in 0..100 {
                let o = order.clone();
                let hook: prkdb_core::wal::CommitHook =
                    Box::new(move |loc: RecordLoc| o.lock().unwrap().push(loc.lsn));
                wal.append(format!("{t}-{i}").into_bytes(), Some(hook))
                    .await
                    .unwrap();
            }
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    let order = order.lock().unwrap().clone();
    assert_eq!(order, (1..=800).collect::<Vec<_>>());
}

#[tokio::test(flavor = "multi_thread")]
async fn oversized_records_are_refused_before_queueing() {
    let dir = tempfile::tempdir().unwrap();
    let (wal, _) = open(Arc::new(StdVfs), dir.path(), opts(SyncMode::Fast, 1 << 20));
    let big = vec![0u8; prkdb_core::wal::frame::MAX_PAYLOAD_LEN + 1];
    assert!(matches!(
        wal.append(big, None).await,
        Err(WalError::RecordTooLarge { .. })
    ));
    assert_eq!(wal.next_lsn(), 1, "a refused record consumes no LSN");
}

/// STO-16: an empty payload was accepted and written as a frame of length 0, which
/// recovery reads as `BadLength(0)`, a torn tail, and truncates: every acknowledged frame
/// after it was lost on the next open. It is refused before admission instead.
#[tokio::test(flavor = "multi_thread")]
async fn an_empty_record_is_refused_before_queueing_and_later_writes_survive_a_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let (wal, _) = open(
        Arc::new(StdVfs),
        dir.path(),
        opts(SyncMode::Durable, 1 << 20),
    );
    let refused = wal.append(Vec::new(), None).await;
    assert!(
        matches!(refused, Err(WalError::EmptyRecord { .. })),
        "{refused:?}"
    );
    assert!(matches!(
        wal.reserve(0).await,
        Err(WalError::EmptyRecord { .. })
    ));
    assert_eq!(wal.next_lsn(), 1, "a refused record consumes no LSN");

    let after = wal.append(b"after".to_vec(), None).await.unwrap();
    assert_eq!(after.lsn, 1);
    wal.close().unwrap();

    let (_wal, replayed) = open(
        Arc::new(StdVfs),
        dir.path(),
        opts(SyncMode::Durable, 1 << 20),
    );
    assert_eq!(replayed, vec![(1, b"after".to_vec())]);
}

#[tokio::test(flavor = "multi_thread")]
async fn read_returns_the_payload_and_rejects_a_stale_location() {
    let dir = tempfile::tempdir().unwrap();
    let (wal, _) = open(
        Arc::new(StdVfs),
        dir.path(),
        opts(SyncMode::Durable, 1 << 20),
    );
    let a = wal.append(b"alpha".to_vec(), None).await.unwrap();
    let b = wal.append(b"beta".to_vec(), None).await.unwrap();
    assert_eq!(wal.read(a).unwrap(), b"alpha");
    assert!(wal.read(RecordLoc { lsn: b.lsn, ..a }).is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_corrupt_sealed_segment_refuses_to_open() {
    let dir = tempfile::tempdir().unwrap();
    {
        let (wal, _) = open(Arc::new(StdVfs), dir.path(), opts(SyncMode::Durable, 4096));
        for i in 0..200 {
            wal.append(format!("record-{i}").into_bytes(), None)
                .await
                .unwrap();
        }
        assert!(wal.segments().len() >= 2);
        wal.close().unwrap();
    }
    let first = dir
        .path()
        .join(prkdb_core::wal::segment::segment_file_name(1));
    let f = StdVfs.open(&first, OpenMode::ReadWrite).unwrap();
    f.write_at(
        prkdb_core::wal::segment::SEGMENT_HEADER_LEN + 20,
        &[0xAB; 4],
    )
    .unwrap();
    let err = Wal::open(
        Arc::new(StdVfs),
        dir.path(),
        opts(SyncMode::Durable, 4096),
        1,
        &mut |_, _, _| Ok(()),
    )
    .err()
    .expect("corruption in a sealed segment must refuse to open");
    assert!(
        matches!(err, WalError::CorruptSegment { ref path, .. } if *path == first),
        "{err}"
    );
}

/// Non-plan requirement (spec §8 "refuse to open, name the file"): a decode failure that
/// the caller's `replay` closure reports (e.g. `Batch::decode` on a corrupted-but-CRC-valid
/// batch body) must be wrapped with the segment path, byte offset and LSN, never surfaced
/// as the caller's raw error.
#[tokio::test(flavor = "multi_thread")]
async fn a_corrupt_batch_body_in_an_otherwise_valid_frame_names_the_segment_and_lsn() {
    let dir = tempfile::tempdir().unwrap();
    let loc;
    {
        let (wal, _) = open(
            Arc::new(StdVfs),
            dir.path(),
            opts(SyncMode::Durable, 1 << 20),
        );
        let payload = Batch {
            ops: vec![BatchOp::Put {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
            }],
        }
        .encode(&CompressionConfig::none())
        .unwrap();
        loc = wal.append(payload, None).await.unwrap();
        wal.close().unwrap();
    }

    // Overwrite the frame with a same-shaped payload that fails `Batch::decode` (an
    // invalid version byte), recomputing the frame's own CRC/LSN so the *frame* itself
    // decodes fine and the corruption is only in the batch body it carries.
    let seg_path = dir
        .path()
        .join(prkdb_core::wal::segment::segment_file_name(loc.segment));
    let f = StdVfs.open(&seg_path, OpenMode::ReadWrite).unwrap();
    let mut header = [0u8; prkdb_core::wal::frame::FRAME_HEADER_LEN];
    f.read_at(loc.offset, &mut header).unwrap();
    let len = u32::from_le_bytes(header[0..4].try_into().unwrap()) as usize;
    let garbage = vec![0xFFu8; len];
    let mut buf = Vec::new();
    prkdb_core::wal::frame::encode_frame(&mut buf, loc.lsn, FrameKind::Batch, &garbage);
    f.write_at(loc.offset, &buf).unwrap();
    f.sync_data().unwrap();

    let err = Wal::open(
        Arc::new(StdVfs),
        dir.path(),
        opts(SyncMode::Durable, 1 << 20),
        1,
        &mut |_loc, kind, payload| {
            assert_eq!(kind, FrameKind::Batch);
            Batch::decode(payload).map(|_| ())
        },
    )
    .err()
    .expect("a corrupt batch body must refuse to open");

    match &err {
        WalError::ReplayFailed {
            path, offset, lsn, ..
        } => {
            assert_eq!(*path, seg_path, "error must name the segment file");
            assert_eq!(*lsn, loc.lsn, "error must name the LSN");
            assert_eq!(*offset, loc.offset, "error must name the byte offset");
        }
        other => panic!("expected WalError::ReplayFailed, got {other:?}"),
    }
    // The rendered message also carries the same information (spec §8 "name the file").
    let msg = err.to_string();
    assert!(
        msg.contains(&seg_path.display().to_string()),
        "error message must name the segment file: {msg}"
    );
    assert!(
        msg.contains(&loc.lsn.to_string()),
        "error message must name the LSN: {msg}"
    );
}

/// H1: a continuously saturated Fast-mode writer never reaches the idle `recv_timeout`
/// branch (there is always more work already queued by the time it finishes a batch), so
/// the periodic sync must also run from inside `commit_batch` itself, not only while idle.
/// Reviewer repro: 64 writers, 1 KiB payloads, `max_batch_bytes` small enough that most
/// batches hold only one or two items, `sync_interval` short — `durable_lsn` must not
/// freeze for multiples of `sync_interval`.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn fast_mode_keeps_syncing_under_saturation() {
    let dir = tempfile::tempdir().unwrap();
    let o = WalOptions {
        sync_mode: SyncMode::Fast,
        sync_interval: Duration::from_millis(20),
        segment_bytes: 64 << 20,
        max_batch_bytes: 2048,
        max_queued_bytes: 16 << 20,
        front_release: FrontRelease::ElidedOnly,
        append_kind: prkdb_core::wal::frame::FrameKind::Batch,
        lsn_limit: None,
    };
    let (wal, _) = open(Arc::new(StdVfs), dir.path(), o);
    let wal = Arc::new(wal);

    // Payloads bigger than half of `max_batch_bytes`, so most batches hold very few items
    // and the writer is constantly draining the channel rather than idling.
    let payload = vec![0u8; 1500];
    let stop = Arc::new(AtomicBool::new(false));
    let mut tasks = Vec::new();
    for _ in 0..64 {
        let wal = wal.clone();
        let payload = payload.clone();
        let stop = stop.clone();
        tasks.push(tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                if wal.append(payload.clone(), None).await.is_err() {
                    break;
                }
            }
        }));
    }

    // Generous deadline (tens of `sync_interval`s): the writer must sync at least once
    // even while continuously saturated with work.
    let deadline = Instant::now() + Duration::from_millis(500);
    let mut saw_progress = false;
    while Instant::now() < deadline {
        if wal.durable_lsn() > 0 {
            saw_progress = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    stop.store(true, Ordering::Relaxed);
    for t in tasks {
        let _ = t.await;
    }

    assert!(
        saw_progress,
        "durable_lsn must advance even under continuous saturation"
    );
    Arc::try_unwrap(wal).ok().unwrap().close().unwrap();
}

/// H2: a frame that was replayed at `open` (because it is physically present in the file)
/// must be made genuinely durable before `open` reports it as covered by `durable_lsn` —
/// otherwise a Fast-mode write that was never fsynced, but survived only because this was
/// a process restart rather than a real power loss, would be reported durable and a
/// following real power loss could still lose it.
#[tokio::test(flavor = "multi_thread")]
async fn recovery_syncs_the_last_segment_before_reporting_it_durable() {
    let dir = tempfile::tempdir().unwrap();
    let probe = Arc::new(Probe::default());
    {
        let (wal, _) = open(
            Arc::new(ProbeVfs(probe.clone())),
            dir.path(),
            opts(SyncMode::Fast, 1 << 20),
        );
        wal.append(b"x".to_vec(), None).await.unwrap(); // written, not (yet) synced
                                                        // Simulate a process restart (not a graceful shutdown): skip `Drop`'s own
                                                        // close-time sync, so any sync found on reopen must come from `open` itself.
        std::mem::forget(wal);
    }

    let before = probe.syncs.load(Ordering::SeqCst);
    let (wal, replayed) = open(
        Arc::new(ProbeVfs(probe.clone())),
        dir.path(),
        opts(SyncMode::Fast, 1 << 20),
    );
    assert_eq!(replayed, vec![(1, b"x".to_vec())]);
    assert!(
        probe.syncs.load(Ordering::SeqCst) > before,
        "open must sync the recovered segment before trusting it as durable"
    );
    assert!(wal.durable_lsn() >= 1);
}

/// M1: a panicking commit hook must not bring down the writer thread in a way that answers
/// later-in-batch items (or a request already dequeued into `pending`) with a bare
/// `Closed` instead of `Poisoned`. The item whose hook panics, and everything queued after
/// it, are answered `Poisoned`; everything answered *before* the panic keeps its `Ok`.
#[tokio::test(flavor = "multi_thread")]
async fn a_panicking_hook_poisons_the_log_and_the_rest_of_the_batch() {
    let dir = tempfile::tempdir().unwrap();
    let (wal, _) = open(Arc::new(StdVfs), dir.path(), opts(SyncMode::Fast, 1 << 20));

    // Enqueue synchronously and in order (reserve + append_reserved, without awaiting the
    // ack in between) so the LSN order — and therefore which item panics — is
    // deterministic, whether or not the writer batches them together.
    let mut pendings = Vec::new();
    for i in 0..5u8 {
        let r = wal.reserve(1).await.unwrap();
        let hook: prkdb_core::wal::CommitHook = if i == 1 {
            Box::new(|_loc| panic!("boom"))
        } else {
            Box::new(|_loc| {})
        };
        pendings.push(wal.append_reserved(r, vec![i], Some(hook)).unwrap());
    }

    let mut results = Vec::new();
    for p in pendings {
        results.push(p.await);
    }

    assert!(results[0].is_ok(), "item 0: {:?}", results[0]);
    for (i, r) in results.iter().enumerate().skip(1) {
        assert!(matches!(r, Err(WalError::Poisoned(_))), "item {i}: {r:?}");
    }

    let later = wal.append(vec![9], None).await;
    assert!(matches!(later, Err(WalError::Poisoned(_))), "{later:?}");
    assert!(matches!(wal.health(), WalHealth::Poisoned(_)));
}

/// M2: `health()` must not report `Stalled` for a request that just arrived, merely
/// because the log had been idle (with nothing queued) for longer than the stall bound —
/// only the age of what is actually queued should count.
#[tokio::test(flavor = "multi_thread")]
async fn health_is_not_stalled_right_after_a_long_idle_period() {
    struct BlockOnceFile {
        inner: Arc<dyn VfsFile>,
        barrier: Arc<Barrier>,
        calls: AtomicU64,
    }
    impl VfsFile for BlockOnceFile {
        fn write_at(&self, o: u64, b: &[u8]) -> io::Result<()> {
            // Skip call 0 (the header write during `Wal::open`, on the caller's thread);
            // block on call 1 (the writer thread's first real batch write).
            if self.calls.fetch_add(1, Ordering::SeqCst) == 1 {
                self.barrier.wait();
            }
            self.inner.write_at(o, b)
        }
        fn read_at(&self, o: u64, b: &mut [u8]) -> io::Result<usize> {
            self.inner.read_at(o, b)
        }
        fn set_len(&self, l: u64) -> io::Result<()> {
            self.inner.set_len(l)
        }
        fn len(&self) -> io::Result<u64> {
            self.inner.len()
        }
        fn sync_data(&self) -> io::Result<()> {
            self.inner.sync_data()
        }
    }
    struct BlockOnceVfs {
        barrier: Arc<Barrier>,
    }
    impl Vfs for BlockOnceVfs {
        fn open(&self, p: &Path, m: OpenMode) -> io::Result<Arc<dyn VfsFile>> {
            StdVfs.open(p, m)
        }
        fn create(&self, p: &Path) -> io::Result<Arc<dyn VfsFile>> {
            Ok(Arc::new(BlockOnceFile {
                inner: StdVfs.create(p)?,
                barrier: self.barrier.clone(),
                calls: AtomicU64::new(0),
            }))
        }
        fn rename(&self, a: &Path, b: &Path) -> io::Result<()> {
            StdVfs.rename(a, b)
        }
        fn remove(&self, p: &Path) -> io::Result<()> {
            StdVfs.remove(p)
        }
        fn create_dir_all(&self, p: &Path) -> io::Result<()> {
            StdVfs.create_dir_all(p)
        }
        fn read_dir(&self, p: &Path) -> io::Result<Vec<PathBuf>> {
            StdVfs.read_dir(p)
        }
        fn exists(&self, p: &Path) -> io::Result<bool> {
            StdVfs.exists(p)
        }
        fn sync_dir(&self, d: &Path) -> io::Result<()> {
            StdVfs.sync_dir(d)
        }
        fn lock_exclusive(&self, p: &Path) -> io::Result<Box<dyn prkdb_core::vfs::LockGuard>> {
            StdVfs.lock_exclusive(p)
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let vfs = Arc::new(BlockOnceVfs {
        barrier: barrier.clone(),
    });
    let (wal, _) = open(vfs, dir.path(), opts(SyncMode::Fast, 1 << 20));

    // `health_snapshot`'s stall bound floors at 1000ms regardless of `sync_interval`; go
    // well past it while genuinely idle (nothing queued at all).
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert_eq!(wal.health(), WalHealth::Healthy);

    let wal = Arc::new(wal);
    let w = wal.clone();
    let appended = tokio::spawn(async move { w.append(b"x".to_vec(), None).await });

    // Give the writer a moment to pick the request up and block mid-write, so it is
    // "queued, not yet answered" while we check health.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        wal.health(),
        WalHealth::Healthy,
        "a freshly queued item must not be Stalled just because the log was idle before it"
    );

    barrier.wait();
    assert!(appended.await.unwrap().is_ok());
    Arc::try_unwrap(wal).ok().unwrap().close().unwrap();
}

/// M3: a scan fault in a sealed (non-active) segment is corruption, same as at `open`.
#[tokio::test(flavor = "multi_thread")]
async fn scan_from_refuses_corruption_in_a_sealed_segment() {
    let dir = tempfile::tempdir().unwrap();
    let (wal, _) = open(Arc::new(StdVfs), dir.path(), opts(SyncMode::Durable, 4096));
    for i in 0..200 {
        wal.append(format!("record-{i}").into_bytes(), None)
            .await
            .unwrap();
    }
    let segments = wal.segments();
    assert!(segments.len() >= 2);
    let sealed = segments[0];
    let seg_path = dir
        .path()
        .join(prkdb_core::wal::segment::segment_file_name(sealed));
    let f = StdVfs.open(&seg_path, OpenMode::ReadWrite).unwrap();
    f.write_at(
        prkdb_core::wal::segment::SEGMENT_HEADER_LEN + 20,
        &[0xAB; 4],
    )
    .unwrap();

    let err = wal.scan_from(1, &mut |_, _, _| Ok(())).unwrap_err();
    assert!(
        matches!(err, WalError::CorruptSegment { ref path, .. } if *path == seg_path),
        "{err}"
    );
}

/// MEDIUM (review): `scan_from` is capped at `acked_lsn`, not `durable_lsn` — it must see
/// a Fast-mode write as soon as it is acked (answered `Ok`), even before the periodic sync
/// catches up, so a consumer built on it (Task 2.8a's `get_changes_since`) never skips an
/// acked change. `scan_durable_from` is the durability-bounded alternative for compaction
/// and checkpoint callers, which must wait for the sync.
#[tokio::test(flavor = "multi_thread")]
async fn scan_from_sees_fast_acked_writes_scan_durable_from_waits_for_sync() {
    let dir = tempfile::tempdir().unwrap();
    // An hour: only explicit `sync()` calls move `durable_lsn`, so the "unsynced" window
    // below is deterministic rather than racing a background timer.
    let o = WalOptions {
        sync_mode: SyncMode::Fast,
        sync_interval: Duration::from_secs(3600),
        segment_bytes: 1 << 20,
        max_batch_bytes: 1 << 20,
        max_queued_bytes: 8 << 20,
        front_release: FrontRelease::ElidedOnly,
        append_kind: prkdb_core::wal::frame::FrameKind::Batch,
        lsn_limit: None,
    };
    let (wal, _) = open(Arc::new(StdVfs), dir.path(), o);

    let loc = wal.append(b"fast".to_vec(), None).await.unwrap();
    assert!(
        wal.durable_lsn() < loc.lsn,
        "must not be durable yet: nothing has synced"
    );
    assert!(
        wal.acked_lsn() >= loc.lsn,
        "must be acked immediately: the append already returned Ok"
    );

    let mut seen_by_scan_from = Vec::new();
    wal.scan_from(1, &mut |loc, _, p| {
        seen_by_scan_from.push((loc.lsn, p.to_vec()));
        Ok(())
    })
    .unwrap();
    assert_eq!(
        seen_by_scan_from,
        vec![(1, b"fast".to_vec())],
        "scan_from must see an acked-but-unsynced Fast write"
    );

    let mut seen_by_scan_durable_from = Vec::new();
    wal.scan_durable_from(1, &mut |loc, _, p| {
        seen_by_scan_durable_from.push((loc.lsn, p.to_vec()));
        Ok(())
    })
    .unwrap();
    assert!(
        seen_by_scan_durable_from.is_empty(),
        "scan_durable_from must not see an unsynced write: {seen_by_scan_durable_from:?}"
    );

    wal.sync().await.unwrap();

    let mut seen_from_after_sync = Vec::new();
    wal.scan_from(1, &mut |loc, _, p| {
        seen_from_after_sync.push((loc.lsn, p.to_vec()));
        Ok(())
    })
    .unwrap();
    let mut seen_durable_after_sync = Vec::new();
    wal.scan_durable_from(1, &mut |loc, _, p| {
        seen_durable_after_sync.push((loc.lsn, p.to_vec()));
        Ok(())
    })
    .unwrap();
    assert_eq!(seen_from_after_sync, vec![(1, b"fast".to_vec())]);
    assert_eq!(seen_durable_after_sync, vec![(1, b"fast".to_vec())]);
}

/// L-c: rolling a segment fsyncs the outgoing one as its first step, so `durable_lsn` must
/// reflect that immediately — a Fast-mode reader must not have to wait for the (here,
/// 1-hour) periodic sync or an explicit `sync()` to see the old segment's frames as durable.
#[tokio::test(flavor = "multi_thread")]
async fn a_segment_roll_advances_durable_lsn_for_the_old_segment_in_fast_mode() {
    let dir = tempfile::tempdir().unwrap();
    let o = WalOptions {
        sync_mode: SyncMode::Fast,
        sync_interval: Duration::from_secs(3600),
        segment_bytes: 1024,
        max_batch_bytes: 1 << 20,
        max_queued_bytes: 8 << 20,
        front_release: FrontRelease::ElidedOnly,
        append_kind: prkdb_core::wal::frame::FrameKind::Batch,
        lsn_limit: None,
    };
    let (wal, _) = open(Arc::new(StdVfs), dir.path(), o);

    let mut old_segment_last_lsn = None;
    for i in 0..200u32 {
        wal.append(format!("record-{i:04}").into_bytes(), None)
            .await
            .unwrap();
        let segs = wal.segments();
        if segs.len() >= 2 {
            old_segment_last_lsn = Some(segs[1] - 1);
            break;
        }
    }
    let old_segment_last_lsn =
        old_segment_last_lsn.expect("small segments must roll within 200 records");

    assert!(
        wal.durable_lsn() >= old_segment_last_lsn,
        "durable_lsn ({}) must cover the rolled-from segment's last lsn ({}) without a \
         periodic or explicit sync",
        wal.durable_lsn(),
        old_segment_last_lsn
    );
}

/// Spends the current task's tokio cooperative budget down to nothing with awaits that
/// are always ready, the state a task reaches after a run of immediately-ready awaits.
async fn exhaust_coop_budget() {
    for _ in 0..100_000 {
        if !tokio::task::coop::has_budget_remaining() {
            return;
        }
        tokio::task::consume_budget().await;
    }
    panic!("the tokio budget never ran out; this test no longer sets up what it tests");
}

/// Runs `scenario` as a task on a fresh runtime on its own thread and fails if it has not
/// finished within 10 s. A watchdog thread rather than `tokio::time::timeout`, because a
/// task spinning inside a blocking wait never yields: on a current-thread runtime no
/// timer could fire, and the test would hang instead of failing.
fn finishes_within_deadline<F>(
    name: &str,
    multi_thread: bool,
    scenario: impl FnOnce() -> F + Send + 'static,
) where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let rt = if multi_thread {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
        } else {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
        }
        .unwrap();
        let outcome = rt.block_on(async move { tokio::spawn(scenario()).await });
        let _ = tx.send(outcome.map_err(|e| e.to_string()));
    });
    match rx.recv_timeout(Duration::from_secs(10)) {
        Ok(Ok(())) => {}
        Ok(Err(panic)) => panic!("{name} (multi_thread: {multi_thread}): {panic}"),
        Err(_) => panic!(
            "{name} (multi_thread: {multi_thread}) hung for 10 s: a blocking WAL wait spins \
             forever when the calling task's tokio budget is exhausted"
        ),
    }
}

/// `append_blocking`, `sync_blocking` and the close in `Drop` wait on tokio futures with
/// `futures::executor::block_on`. Called from a tokio task whose cooperative budget is
/// spent, every budget-aware poll (the admission semaphore, the reply oneshot) answers
/// `Pending` and wakes itself, and `block_on` never returns to the runtime that would
/// refill the budget: it spun forever. Each wait must finish whatever the budget.
#[test]
fn blocking_waits_finish_on_a_task_with_no_tokio_budget_left() {
    for multi_thread in [true, false] {
        finishes_within_deadline("blocking WAL waits", multi_thread, || async {
            let dir = tempfile::tempdir().unwrap();
            let (wal, _) = open(Arc::new(StdVfs), dir.path(), opts(SyncMode::Fast, 1 << 20));
            for i in 0..200u32 {
                wal.append(format!("record-{i}").into_bytes(), None)
                    .await
                    .unwrap();
            }
            exhaust_coop_budget().await;
            wal.append_blocking(b"blocking".to_vec(), None).unwrap();
            exhaust_coop_budget().await;
            assert_eq!(wal.sync_blocking().unwrap(), 201);
            exhaust_coop_budget().await;
            drop(wal);
        });
    }
}

/// A frame with any kind byte, hand-encoded: `len | crc | lsn | kind | payload`. Its CRC
/// is correct unless `valid_crc` is false.
fn raw_frame(lsn: Lsn, kind: u8, payload: &[u8], valid_crc: bool) -> Vec<u8> {
    let mut crc_input = lsn.to_le_bytes().to_vec();
    crc_input.push(kind);
    crc_input.extend_from_slice(payload);
    let crc = crc32fast::hash(&crc_input) ^ if valid_crc { 0 } else { 1 };
    let mut frame = (payload.len() as u32).to_le_bytes().to_vec();
    frame.extend_from_slice(&crc.to_le_bytes());
    frame.extend_from_slice(&lsn.to_le_bytes());
    frame.push(kind);
    frame.extend_from_slice(payload);
    frame
}

fn append_to_file(path: &Path, bytes: &[u8]) -> u64 {
    let mut all = std::fs::read(path).unwrap();
    all.extend_from_slice(bytes);
    std::fs::write(path, &all).unwrap();
    all.len() as u64
}

fn try_open(dir: &Path, o: WalOptions) -> Result<Wal, WalError> {
    Wal::open(Arc::new(StdVfs), dir, o, 1, &mut |_, _, _| Ok(())).map(|(wal, _)| wal)
}

/// STO-11: a frame whose CRC is valid but whose kind this build does not know was
/// written by a later build, not torn by a crash. At the end of the last segment it
/// refuses the open (`UnsupportedFormat`, naming the file) and is never truncated.
#[tokio::test(flavor = "multi_thread")]
async fn an_unknown_frame_kind_with_a_valid_crc_refuses_and_never_truncates() {
    let dir = tempfile::tempdir().unwrap();
    let o = opts(SyncMode::Durable, 1 << 20);
    let (wal, _) = open(Arc::new(StdVfs), dir.path(), o.clone());
    for i in 0..3u8 {
        wal.append(vec![i; 16], None).await.unwrap();
    }
    wal.close().unwrap();
    let path = dir
        .path()
        .join(prkdb_core::wal::segment::segment_file_name(1));
    let len = append_to_file(&path, &raw_frame(4, 99, b"a later build's frame", true));

    let err = try_open(dir.path(), o).err().expect("must refuse to open");
    assert!(
        matches!(err, WalError::UnsupportedFormat { path: ref p, .. } if *p == path),
        "{err}"
    );
    assert!(err.to_string().contains("kind 99"), "{err}");
    assert_eq!(
        std::fs::metadata(&path).unwrap().len(),
        len,
        "the segment must not be truncated"
    );
}

/// STO-11: the same frame in a sealed segment is `UnsupportedFormat` too, not
/// `CorruptSegment`.
#[tokio::test(flavor = "multi_thread")]
async fn an_unknown_frame_kind_in_a_sealed_segment_is_unsupported_format() {
    let dir = tempfile::tempdir().unwrap();
    let o = opts(SyncMode::Durable, 1024);
    let (wal, _) = open(Arc::new(StdVfs), dir.path(), o.clone());
    let mut first = None;
    for i in 0..200u32 {
        let loc = wal.append(vec![7u8; 40], None).await.unwrap();
        if i == 0 {
            first = Some(loc);
        }
    }
    assert!(wal.segments().len() >= 2);
    wal.close().unwrap();
    // Overwrite the first frame in place with one of the same length and LSN.
    let first = first.unwrap();
    let path = dir
        .path()
        .join(prkdb_core::wal::segment::segment_file_name(1));
    let f = StdVfs.open(&path, OpenMode::ReadWrite).unwrap();
    f.write_at(first.offset, &raw_frame(1, 99, &[7u8; 40], true))
        .unwrap();
    f.sync_data().unwrap();

    let err = try_open(dir.path(), o).err().expect("must refuse to open");
    assert!(
        matches!(err, WalError::UnsupportedFormat { path: ref p, .. } if *p == path),
        "{err}"
    );
}

/// The torn-tail rule is unchanged: a frame whose CRC is wrong at the end of the last
/// segment is truncated, whatever its kind byte says.
#[tokio::test(flavor = "multi_thread")]
async fn a_bad_crc_at_the_end_of_the_last_segment_is_still_a_torn_tail() {
    for kind in [1u8, 99] {
        let dir = tempfile::tempdir().unwrap();
        let o = opts(SyncMode::Durable, 1 << 20);
        let (wal, _) = open(Arc::new(StdVfs), dir.path(), o.clone());
        for i in 0..3u8 {
            wal.append(vec![i; 16], None).await.unwrap();
        }
        wal.close().unwrap();
        let path = dir
            .path()
            .join(prkdb_core::wal::segment::segment_file_name(1));
        let before = std::fs::metadata(&path).unwrap().len();
        append_to_file(&path, &raw_frame(4, kind, b"torn", false));

        let (wal, report) =
            Wal::open(Arc::new(StdVfs), dir.path(), o, 1, &mut |_, _, _| Ok(())).unwrap();
        let (_, offset, fault) = report.truncated.expect("truncated");
        assert_eq!(offset, before, "kind {kind}");
        assert_eq!(
            fault,
            prkdb_core::wal::frame::FrameFault::BadCrc,
            "kind {kind}"
        );
        assert_eq!(std::fs::metadata(&path).unwrap().len(), before);
        assert_eq!(wal.next_lsn(), 4);
    }
}

/// STO-11 for reads: `scan_from` refuses a valid frame of an unknown kind in the active
/// segment as `UnsupportedFormat`; it is not a tail in flight.
#[tokio::test(flavor = "multi_thread")]
async fn scan_from_refuses_a_valid_frame_of_an_unknown_kind() {
    let dir = tempfile::tempdir().unwrap();
    let (wal, _) = open(
        Arc::new(StdVfs),
        dir.path(),
        opts(SyncMode::Durable, 1 << 20),
    );
    let mut locs = Vec::new();
    for i in 0..3u8 {
        locs.push(wal.append(vec![i; 16], None).await.unwrap());
    }
    let path = dir
        .path()
        .join(prkdb_core::wal::segment::segment_file_name(1));
    let f = StdVfs.open(&path, OpenMode::ReadWrite).unwrap();
    f.write_at(locs[1].offset, &raw_frame(2, 99, &[1u8; 16], true))
        .unwrap();

    let err = wal.scan_from(1, &mut |_, _, _| Ok(())).unwrap_err();
    assert!(
        matches!(err, WalError::UnsupportedFormat { path: ref p, .. } if *p == path),
        "{err}"
    );
}
