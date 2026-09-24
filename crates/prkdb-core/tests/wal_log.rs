//! `Wal` behaviour over StdVfs (Task 2.6). Power-loss behaviour:
//! `crates/prkdb-verify/tests/wal_power_loss.rs`.

use prkdb_core::vfs::{OpenMode, StdVfs, Vfs, VfsFile};
use prkdb_core::wal::batch::{Batch, BatchOp};
use prkdb_core::wal::frame::FrameKind;
use prkdb_core::wal::{
    CompressionConfig, Lsn, RecordLoc, SyncMode, Wal, WalError, WalHealth, WalOptions,
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

/// M3: frames on the active segment that are not yet durable (written but unsynced under
/// `SyncMode::Fast`) must never reach `scan_from`'s visitor.
#[tokio::test(flavor = "multi_thread")]
async fn scan_from_never_visits_unsynced_frames_on_the_active_segment() {
    let dir = tempfile::tempdir().unwrap();
    // An hour: only explicit `sync()` calls move `durable_lsn`, so the "unsynced" window
    // below is deterministic rather than racing a background timer.
    let o = WalOptions {
        sync_mode: SyncMode::Fast,
        sync_interval: Duration::from_secs(3600),
        segment_bytes: 1 << 20,
        max_batch_bytes: 1 << 20,
        max_queued_bytes: 8 << 20,
    };
    let (wal, _) = open(Arc::new(StdVfs), dir.path(), o);

    wal.append(b"synced".to_vec(), None).await.unwrap();
    wal.sync().await.unwrap();
    let unsynced = wal.append(b"unsynced".to_vec(), None).await.unwrap();
    assert!(
        wal.durable_lsn() < unsynced.lsn,
        "the second record must not be durable yet"
    );

    let mut seen = Vec::new();
    wal.scan_from(1, &mut |loc, _, p| {
        seen.push((loc.lsn, p.to_vec()));
        Ok(())
    })
    .unwrap();
    assert_eq!(
        seen,
        vec![(1, b"synced".to_vec())],
        "unsynced frames must not be visited"
    );
}
