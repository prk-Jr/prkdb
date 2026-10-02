//! Runtime scans over sealed segments (STO-13).
//!
//! Recovery checks every sealed segment whole, but a runtime scan (`scan_from` and the
//! scans built on it) took a segment that ended cleanly at a frame boundary as complete:
//! a sealed segment cut short exactly between two frames lost its tail without a word,
//! and the scan carried on into the next segment. A sealed segment must end where the
//! next one starts. (A cap sampled before a concurrent roll, STO-14, is a unit test in
//! `wal::log`, which can pass the stale cap directly.)

use prkdb_core::vfs::StdVfs;
use prkdb_core::wal::{FrontRelease, Lsn, SyncMode, Wal, WalError, WalOptions};
use std::ops::ControlFlow;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

/// 300-byte segments hold two 100-byte records each (24-byte header + 2 × 117-byte
/// frames).
const SEGMENT_BYTES: u64 = 300;
const SEGMENT_HEADER: u64 = 24;
const FRAME: u64 = 117;

fn open(dir: &Path) -> Wal {
    let opts = WalOptions {
        sync_mode: SyncMode::Durable,
        sync_interval: Duration::from_secs(3600),
        segment_bytes: SEGMENT_BYTES,
        max_batch_bytes: 1 << 20,
        max_queued_bytes: 8 << 20,
        front_release: FrontRelease::ElidedOnly,
    };
    Wal::open(Arc::new(StdVfs), dir, opts, 1, &mut |_, _, _| Ok(()))
        .unwrap()
        .0
}

/// Six records: sealed segments at LSNs 1 and 3, the active one at 5.
fn six_records(dir: &Path) -> Wal {
    let wal = open(dir);
    for i in 1..=6u8 {
        wal.append_blocking(vec![i; 100], None).unwrap();
    }
    assert_eq!(wal.segments(), vec![1, 3, 5]);
    wal
}

/// Cuts the sealed segment at LSN 1 after its first frame: a clean frame boundary, so the
/// segment scan alone sees no fault.
fn cut_first_segment_after_one_frame(wal: &Wal) {
    let path = wal.segment_path(1);
    let file = std::fs::OpenOptions::new().write(true).open(path).unwrap();
    file.set_len(SEGMENT_HEADER + FRAME).unwrap();
}

fn expect_corrupt_first_segment(result: Result<(), WalError>, seen: &[Lsn]) {
    match result {
        Err(WalError::CorruptSegment { path, reason, .. }) => {
            assert!(path.ends_with("00000000000000000001.wal"), "{path:?}");
            assert!(
                reason.contains('3'),
                "names where the next segment starts: {reason}"
            );
        }
        other => panic!("expected CorruptSegment, got {other:?} after visiting {seen:?}"),
    }
    assert!(
        !seen.contains(&3),
        "nothing after the cut segment may be visited: {seen:?}"
    );
}

#[test]
fn scan_from_refuses_a_sealed_segment_cut_at_a_frame_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let wal = six_records(dir.path());
    cut_first_segment_after_one_frame(&wal);

    let mut seen = Vec::new();
    let result = wal.scan_from(1, &mut |loc, _, _| {
        seen.push(loc.lsn);
        Ok(())
    });
    expect_corrupt_first_segment(result, &seen);
}

#[test]
fn scan_durable_from_flow_refuses_a_sealed_segment_cut_at_a_frame_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let wal = six_records(dir.path());
    cut_first_segment_after_one_frame(&wal);

    let mut seen = Vec::new();
    let result = wal
        .scan_durable_from_flow(1, &mut |loc, _, _| {
            seen.push(loc.lsn);
            Ok(ControlFlow::Continue(()))
        })
        .map(|_| ());
    expect_corrupt_first_segment(result, &seen);
}

/// The check costs nothing when it is not reached: a visitor that stops inside the cut
/// segment, or a scan that starts after it, never looks at its end.
#[test]
fn a_scan_that_never_reaches_the_cut_end_is_unaffected() {
    let dir = tempfile::tempdir().unwrap();
    let wal = six_records(dir.path());
    cut_first_segment_after_one_frame(&wal);

    let mut seen = Vec::new();
    let flow = wal
        .scan_from_flow(1, &mut |loc, _, _| {
            seen.push(loc.lsn);
            Ok(ControlFlow::Break(()))
        })
        .unwrap();
    assert!(flow.is_break());
    assert_eq!(seen, vec![1]);

    let mut seen = Vec::new();
    wal.scan_from(3, &mut |loc, _, _| {
        seen.push(loc.lsn);
        Ok(())
    })
    .unwrap();
    assert_eq!(seen, vec![3, 4, 5, 6]);
}

/// A whole log still scans whole.
#[test]
fn an_intact_log_scans_every_frame() {
    let dir = tempfile::tempdir().unwrap();
    let wal = six_records(dir.path());
    let mut seen = Vec::new();
    wal.scan_from(1, &mut |loc, _, _| {
        seen.push(loc.lsn);
        Ok(())
    })
    .unwrap();
    assert_eq!(seen, vec![1, 2, 3, 4, 5, 6]);
}
