//! What the streaming log (Task 2.15b) needs from the single `Wal` (Task 2.15b.1, design
//! note §6.2 and §8.3): a scan that starts at the right segment and can stop early, a
//! segment scan from a byte offset, a wake-up when `acked_lsn` moves, an explicit roll,
//! and `FrontRelease::Retention`. Power loss during a retention release:
//! `crates/prkdb-verify/tests/wal_power_loss.rs`.

use prkdb_core::vfs::{OpenMode, StdVfs, Vfs, VfsFile};
use prkdb_core::wal::frame::FrameFault;
use prkdb_core::wal::segment::{scan_segment_from, segment_file_name};
use prkdb_core::wal::{
    FrontRelease, LogState, Lsn, RecordLoc, SyncMode, Wal, WalConfig, WalError, WalOptions,
};
use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::ops::ControlFlow;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// 300-byte segments hold two 100-byte records each (24-byte header + 2 × 117-byte
/// frames).
const SEGMENT_BYTES: u64 = 300;
const RECORD: usize = 100;

fn opts(mode: SyncMode, front_release: FrontRelease) -> WalOptions {
    WalOptions {
        sync_mode: mode,
        sync_interval: Duration::from_secs(3600),
        segment_bytes: SEGMENT_BYTES,
        max_batch_bytes: 1 << 20,
        max_queued_bytes: 8 << 20,
        front_release,
    }
}

fn open_with(vfs: Arc<dyn Vfs>, dir: &Path, o: WalOptions) -> (Wal, Vec<Lsn>) {
    let mut seen = Vec::new();
    let (wal, _) = Wal::open(vfs, dir, o, 1, &mut |loc, _, _| {
        seen.push(loc.lsn);
        Ok(())
    })
    .unwrap();
    (wal, seen)
}

fn record(i: u64) -> Vec<u8> {
    let mut r = vec![(i % 251) as u8; RECORD];
    r[..8].copy_from_slice(&i.to_le_bytes());
    r
}

/// Appends records `1..=n` one at a time (one frame per batch), so segments fill
/// predictably.
fn fill(wal: &Wal, n: u64) -> Vec<RecordLoc> {
    (1..=n)
        .map(|i| wal.append_blocking(record(i), None).unwrap())
        .collect()
}

fn scanned(wal: &Wal, from: Lsn) -> Vec<Lsn> {
    let mut lsns = Vec::new();
    wal.scan_from(from, &mut |loc, _, payload| {
        assert_eq!(payload, record(loc.lsn));
        lsns.push(loc.lsn);
        Ok(())
    })
    .unwrap();
    lsns
}

/// Counts `read_at` calls per file, to see which segments a scan touched.
#[derive(Clone, Default)]
struct CountingVfs {
    reads: Arc<Mutex<BTreeMap<PathBuf, u64>>>,
}
struct CountingFile {
    inner: Arc<dyn VfsFile>,
    path: PathBuf,
    reads: Arc<Mutex<BTreeMap<PathBuf, u64>>>,
}
impl VfsFile for CountingFile {
    fn write_at(&self, o: u64, b: &[u8]) -> io::Result<()> {
        self.inner.write_at(o, b)
    }
    fn read_at(&self, o: u64, b: &mut [u8]) -> io::Result<usize> {
        *self
            .reads
            .lock()
            .unwrap()
            .entry(self.path.clone())
            .or_default() += 1;
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
impl CountingVfs {
    fn wrap(&self, path: &Path, inner: Arc<dyn VfsFile>) -> Arc<dyn VfsFile> {
        Arc::new(CountingFile {
            inner,
            path: path.to_path_buf(),
            reads: self.reads.clone(),
        })
    }
    /// The first LSNs of the segments read since the last call.
    fn take_read_segments(&self) -> BTreeSet<Lsn> {
        std::mem::take(&mut *self.reads.lock().unwrap())
            .into_keys()
            .filter_map(|p| {
                prkdb_core::wal::segment::parse_segment_file_name(p.file_name()?.to_str()?)
            })
            .collect()
    }
}
impl Vfs for CountingVfs {
    fn open(&self, p: &Path, m: OpenMode) -> io::Result<Arc<dyn VfsFile>> {
        Ok(self.wrap(p, StdVfs.open(p, m)?))
    }
    fn create(&self, p: &Path) -> io::Result<Arc<dyn VfsFile>> {
        Ok(self.wrap(p, StdVfs.create(p)?))
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

/// A 64-segment log over a counting `Vfs`, with the counts reset.
fn counted_log(dir: &Path) -> (Wal, CountingVfs, Vec<Lsn>) {
    let vfs = CountingVfs::default();
    let (wal, _) = open_with(
        Arc::new(vfs.clone()),
        dir,
        opts(SyncMode::Durable, FrontRelease::ElidedOnly),
    );
    fill(&wal, 128);
    let segments = wal.segments();
    assert_eq!(segments.len(), 64, "two records per segment: {segments:?}");
    vfs.take_read_segments();
    (wal, vfs, segments)
}

/// (a) `scan_from` starts at the segment holding `from`: on a 64-segment log it reads
/// only that segment and the ones after it.
#[test]
fn scan_from_reads_only_the_segments_at_or_after_the_start() {
    let dir = tempfile::tempdir().unwrap();
    let (wal, vfs, segments) = counted_log(dir.path());

    let from = segments[60] + 1; // the second record of segment 60
    assert_eq!(scanned(&wal, from), (from..=128).collect::<Vec<_>>());
    assert_eq!(
        vfs.take_read_segments(),
        segments[60..].iter().copied().collect::<BTreeSet<_>>()
    );

    // From the start, every segment; past the end, only the active one.
    assert_eq!(scanned(&wal, 1), (1..=128).collect::<Vec<_>>());
    assert_eq!(vfs.take_read_segments().len(), 64);
    assert!(scanned(&wal, 500).is_empty());
    assert_eq!(
        vfs.take_read_segments(),
        BTreeSet::from([*segments.last().unwrap()])
    );
}

/// (b) A visitor that returns `Break` stops the scan: nothing after it is visited, and no
/// later segment is read.
#[test]
fn a_visitor_that_breaks_stops_the_scan() {
    let dir = tempfile::tempdir().unwrap();
    let (wal, vfs, segments) = counted_log(dir.path());

    let mut visited = Vec::new();
    let flow = wal
        .scan_from_flow(1, &mut |loc, _, _| {
            visited.push(loc.lsn);
            Ok(if visited.len() == 10 {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            })
        })
        .unwrap();
    assert_eq!(flow, ControlFlow::Break(()));
    assert_eq!(visited, (1..=10).collect::<Vec<_>>());
    assert_eq!(
        vfs.take_read_segments(),
        segments[..5].iter().copied().collect::<BTreeSet<_>>(),
        "records 1..=10 are in the first five segments"
    );

    // The durable-bounded scan stops the same way; a visitor that never breaks runs to
    // the end and reports `Continue`.
    let mut n = 0;
    let flow = wal
        .scan_durable_from_flow(100, &mut |_, _, _| {
            n += 1;
            Ok(ControlFlow::Break(()))
        })
        .unwrap();
    assert_eq!((flow, n), (ControlFlow::Break(()), 1));
    let flow = wal
        .scan_from_flow(120, &mut |_, _, _| Ok(ControlFlow::Continue(())))
        .unwrap();
    assert_eq!(flow, ControlFlow::Continue(()));
}

/// (c) `scan_segment_from` starts at a known frame boundary in the middle of a segment
/// and checks LSN continuity from the LSN it was given.
#[test]
fn scan_segment_from_starts_mid_segment() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = opts(SyncMode::Durable, FrontRelease::ElidedOnly);
    o.segment_bytes = 1 << 20;
    let (wal, _) = open_with(Arc::new(StdVfs), dir.path(), o);
    let locs = fill(&wal, 10);
    wal.close().unwrap();
    let path = dir.path().join(segment_file_name(1));
    let file = StdVfs.open(&path, OpenMode::Read).unwrap();

    let mut visited = Vec::new();
    let scan = scan_segment_from(&*file, &path, 1, (5, locs[4].offset), &mut |loc, _, p| {
        assert_eq!(p, record(loc.lsn));
        assert_eq!(loc, locs[loc.lsn as usize - 1]);
        visited.push(loc.lsn);
        Ok(ControlFlow::Continue(()))
    })
    .unwrap();
    assert_eq!(visited, (5..=10).collect::<Vec<_>>());
    assert_eq!((scan.next_lsn, scan.stopped), (11, None));
    assert!(!scan.stopped_by_visitor);
    assert_eq!(scan.valid_len, scan.file_len);

    // An early stop: the scan ends just past the frame the visitor stopped at.
    let scan = scan_segment_from(&*file, &path, 1, (5, locs[4].offset), &mut |_, _, _| {
        Ok(ControlFlow::Break(()))
    })
    .unwrap();
    assert!(scan.stopped_by_visitor);
    assert_eq!((scan.next_lsn, scan.valid_len), (6, locs[5].offset));

    // Continuity is checked from the given LSN.
    let scan = scan_segment_from(&*file, &path, 1, (6, locs[4].offset), &mut |_, _, _| {
        panic!("a frame with the wrong LSN must not be visited")
    })
    .unwrap();
    assert_eq!(
        scan.stopped,
        Some((
            locs[4].offset,
            FrameFault::LsnGap {
                expected: 6,
                found: 5
            }
        ))
    );

    // A start inside the header, before the segment's first LSN, or past the end of the
    // file is refused.
    let len = file.len().unwrap();
    for start in [(1, 0), (0, locs[0].offset), (11, len + 1)] {
        let err = scan_segment_from(&*file, &path, 1, start, &mut |_, _, _| {
            Ok(ControlFlow::Continue(()))
        })
        .unwrap_err();
        assert!(
            matches!(err, WalError::CorruptSegment { .. }),
            "{start:?}: {err}"
        );
    }
    // The end of the file is a valid (empty) start.
    let scan = scan_segment_from(&*file, &path, 1, (11, len), &mut |_, _, _| {
        panic!("nothing to visit")
    })
    .unwrap();
    assert_eq!((scan.next_lsn, scan.stopped), (11, None));
}

/// (d) `subscribe_acked` wakes after an append is acked, and the woken reader's
/// `scan_from` already includes it, in both modes.
#[tokio::test(flavor = "multi_thread")]
async fn subscribe_acked_wakes_a_reader_whose_scan_then_sees_the_append() {
    for mode in [SyncMode::Durable, SyncMode::Fast] {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = open_with(
            Arc::new(StdVfs),
            dir.path(),
            opts(mode, FrontRelease::ElidedOnly),
        );
        let wal = Arc::new(wal);
        fill(&wal, 3);

        let mut rx = wal.subscribe_acked();
        assert_eq!(*rx.borrow(), 3, "{mode:?}: starts at the current acked LSN");
        let reader = {
            let wal = wal.clone();
            tokio::spawn(async move {
                let mut seen: Vec<Lsn> = Vec::new();
                while !seen.contains(&4) {
                    rx.changed().await.expect("the WAL is open");
                    let woke_at = *rx.borrow_and_update();
                    seen = scanned(&wal, 1);
                    assert!(
                        seen.last().copied() >= Some(woke_at),
                        "woken at {woke_at}, scan saw {seen:?}"
                    );
                }
                seen
            })
        };
        tokio::time::sleep(Duration::from_millis(20)).await;
        let loc = wal.append(record(4), None).await.unwrap();
        let seen = tokio::time::timeout(Duration::from_secs(5), reader)
            .await
            .unwrap_or_else(|_| panic!("{mode:?}: the reader was never woken"))
            .unwrap();
        assert_eq!(seen, vec![1, 2, 3, loc.lsn], "{mode:?}");

        // A late subscriber starts at the current watermark; closing the WAL ends every
        // wait instead of leaving it pending.
        let mut late = wal.subscribe_acked();
        assert_eq!(*late.borrow(), 4, "{mode:?}");
        Arc::try_unwrap(wal).ok().unwrap().close().unwrap();
        assert!(late.changed().await.is_err(), "{mode:?}");
    }
}

/// (e) `roll` seals a non-empty active segment, synced, and opens the next one. On an
/// empty active segment it does nothing.
#[tokio::test(flavor = "multi_thread")]
async fn roll_seals_a_non_empty_active_segment_and_is_a_no_op_on_an_empty_one() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = opts(SyncMode::Fast, FrontRelease::ElidedOnly);
    o.segment_bytes = 1 << 20;
    let (wal, _) = open_with(Arc::new(StdVfs), dir.path(), o.clone());
    assert_eq!(
        wal.roll().await.unwrap(),
        None,
        "a fresh log has nothing to seal"
    );
    assert_eq!(wal.segments(), vec![1]);

    for i in 1..=3 {
        wal.append(record(i), None).await.unwrap();
    }
    assert!(
        wal.durable_lsn() < 3,
        "Fast, hour-long interval: not yet synced"
    );
    assert_eq!(wal.roll().await.unwrap(), Some(4));
    assert_eq!(wal.segments(), vec![1, 4]);
    assert_eq!(wal.durable_lsn(), 3, "the sealed segment was synced");
    let sealed = wal.sealed_segments().unwrap();
    assert_eq!((sealed[0].first_lsn, sealed[0].next_lsn), (1, 4));

    assert_eq!(
        wal.roll_blocking().unwrap(),
        None,
        "the new segment is empty"
    );
    assert_eq!(wal.segments(), vec![1, 4]);

    let loc = wal.append(record(4), None).await.unwrap();
    assert_eq!((loc.lsn, loc.segment), (4, 4));
    wal.close().unwrap();

    let (wal, replayed) = open_with(Arc::new(StdVfs), dir.path(), o);
    assert_eq!(replayed, vec![1, 2, 3, 4]);
    assert_eq!(wal.segments(), vec![1, 4]);
}

/// A log of 20 records over 10 segments; returns it and its segments' first LSNs.
fn ten_segments(dir: &Path, front_release: FrontRelease) -> (Wal, Vec<Lsn>) {
    let (wal, _) = open_with(
        Arc::new(StdVfs),
        dir,
        opts(SyncMode::Durable, front_release),
    );
    fill(&wal, 20);
    let segments = wal.segments();
    assert_eq!(segments.len(), 10);
    (wal, segments)
}

fn segment_files(dir: &Path) -> Vec<Lsn> {
    let mut lsns: Vec<Lsn> = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| {
            prkdb_core::wal::segment::parse_segment_file_name(e.ok()?.file_name().to_str()?)
        })
        .collect();
    lsns.sort_unstable();
    lsns
}

/// (f) With `Retention`, `set_log_start` and `remove_leading_segments` release sealed
/// segments that hold live frames, and the floor survives a reopen. The other refusals
/// stand: `upto` must start a segment, removal waits for the durable log start.
#[test]
fn retention_releases_sealed_segments_holding_live_frames() {
    let dir = tempfile::tempdir().unwrap();
    let (wal, segments) = ten_segments(dir.path(), FrontRelease::Retention);
    assert_eq!(wal.front_release(), FrontRelease::Retention);
    let upto = segments[3];

    assert!(matches!(
        wal.set_log_start(upto + 1),
        Err(WalError::CompactionRefused(_))
    ));
    assert!(matches!(
        wal.remove_leading_segments(upto),
        Err(WalError::CompactionRefused(_))
    ));
    wal.set_log_start(upto).unwrap();
    assert_eq!(wal.log_state().log_start, upto);
    assert_eq!(wal.remove_leading_segments(upto).unwrap(), 3);
    assert_eq!(wal.segments(), segments[3..].to_vec());
    assert_eq!(segment_files(dir.path()), segments[3..].to_vec());
    assert_eq!(scanned(&wal, 1), (upto..=20).collect::<Vec<_>>());

    // Every sealed segment can go; the active one never does.
    let active = *segments.last().unwrap();
    wal.set_log_start(active).unwrap();
    assert_eq!(wal.remove_leading_segments(active).unwrap(), 6);
    assert_eq!(wal.segments(), vec![active]);
    assert!(matches!(
        wal.set_log_start(active + 1),
        Err(WalError::CompactionRefused(_))
    ));
    wal.close().unwrap();

    let (wal, replayed) = open_with(
        Arc::new(StdVfs),
        dir.path(),
        opts(SyncMode::Durable, FrontRelease::Retention),
    );
    assert_eq!(wal.log_state().log_start, active);
    assert_eq!(replayed, (active..=20).collect::<Vec<_>>());
    assert_eq!(wal.append_blocking(record(21), None).unwrap().lsn, 21);
}

/// (f) With `Retention`, `open` removes segments left below the log start by a crash
/// between `set_log_start` and `remove_leading_segments`, live frames and all. A leftover
/// that does not end at or before the log start is still corruption.
#[test]
fn retention_open_removes_live_leftovers_below_the_log_start() {
    let dir = tempfile::tempdir().unwrap();
    let (wal, segments) = ten_segments(dir.path(), FrontRelease::Retention);
    wal.set_log_start(segments[4]).unwrap();
    wal.close().unwrap(); // "crash" before remove_leading_segments
    assert_eq!(segment_files(dir.path()), segments);

    let (wal, replayed) = open_with(
        Arc::new(StdVfs),
        dir.path(),
        opts(SyncMode::Durable, FrontRelease::Retention),
    );
    assert_eq!(segment_files(dir.path()), segments[4..].to_vec());
    assert_eq!(replayed, (segments[4]..=20).collect::<Vec<_>>());
    assert_eq!(wal.log_state().log_start, segments[4]);
    wal.close().unwrap();

    // A log start inside a segment: the segment before it holds LSNs on both sides.
    LogState {
        log_start: segments[6] + 1,
        deletes_compacted_through: 0,
    }
    .write(&StdVfs, dir.path())
    .unwrap();
    let err = Wal::open(
        Arc::new(StdVfs),
        dir.path(),
        opts(SyncMode::Durable, FrontRelease::Retention),
        1,
        &mut |_, _, _| Ok(()),
    )
    .err()
    .expect("refused");
    assert!(matches!(err, WalError::CorruptSegment { .. }), "{err}");
    assert_eq!(segment_files(dir.path()), segments[4..].to_vec());
}

/// (g) `ElidedOnly` is the default, also from `WalConfig`, and keeps Task 2.15's
/// refusals: no live segment is released, and a live leftover below the log start (what a
/// retention crash leaves) refuses to open.
#[test]
fn elided_only_is_the_default_and_keeps_every_refusal() {
    assert_eq!(FrontRelease::default(), FrontRelease::ElidedOnly);
    assert_eq!(
        WalOptions::from_config(&WalConfig::default()).front_release,
        FrontRelease::ElidedOnly
    );

    let dir = tempfile::tempdir().unwrap();
    let (wal, segments) = ten_segments(dir.path(), FrontRelease::ElidedOnly);
    assert_eq!(wal.front_release(), FrontRelease::ElidedOnly);
    assert!(matches!(
        wal.set_log_start(segments[3]),
        Err(WalError::CompactionRefused(_))
    ));
    assert_eq!(wal.log_state().log_start, 1);
    wal.close().unwrap();

    // Leave live segments below the log start, as a retention crash would.
    let (wal, _) = open_with(
        Arc::new(StdVfs),
        dir.path(),
        opts(SyncMode::Durable, FrontRelease::Retention),
    );
    wal.set_log_start(segments[3]).unwrap();
    wal.close().unwrap();
    let err = Wal::open(
        Arc::new(StdVfs),
        dir.path(),
        opts(SyncMode::Durable, FrontRelease::ElidedOnly),
        1,
        &mut |_, _, _| Ok(()),
    )
    .err()
    .expect("a live segment below the log start must refuse in ElidedOnly");
    assert!(matches!(err, WalError::CorruptSegment { .. }), "{err}");
    assert_eq!(segment_files(dir.path()), segments, "nothing was removed");
}
