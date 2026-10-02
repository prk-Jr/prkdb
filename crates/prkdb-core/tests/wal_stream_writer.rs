//! Writer allocation and sparse scans required by StreamLog.
use prkdb_core::vfs::{StdVfs, Vfs};
use prkdb_core::wal::frame::FrameKind;
use prkdb_core::wal::segment::{segment_file_name, write_segment_header};
use prkdb_core::wal::{
    LogState, RecordLoc, SyncMode, Wal, WalConfig, WalError, WalHealth, WalOptions,
};
use std::ops::ControlFlow;
use std::sync::{mpsc, Arc};
use std::time::Duration;

const LIMIT: u64 = (1 << 48) - 1;

fn options() -> WalOptions {
    let mut o = WalOptions::from_config(&WalConfig::default());
    o.sync_mode = SyncMode::Durable;
    o
}

fn open(dir: &std::path::Path, o: WalOptions) -> Wal {
    Wal::open(Arc::new(StdVfs), dir, o, 1, &mut |_, _, _| Ok(()))
        .unwrap()
        .0
}

fn seed_empty(dir: &std::path::Path, first: u64) {
    LogState {
        log_start: first,
        deletes_compacted_through: 0,
    }
    .write(&StdVfs, dir)
    .unwrap();
    let file = StdVfs.create(&dir.join(segment_file_name(first))).unwrap();
    write_segment_header(&*file, first).unwrap();
    file.sync_data().unwrap();
    StdVfs.sync_dir(dir).unwrap();
}

#[test]
fn append_kind_defaults_to_batch_and_stream_uses_records() {
    for kind in [FrameKind::Batch, FrameKind::Records] {
        let dir = tempfile::tempdir().unwrap();
        let mut o = options();
        assert_eq!(o.append_kind, FrameKind::Batch);
        assert_eq!(o.lsn_limit, None);
        o.append_kind = kind;
        let wal = open(dir.path(), o.clone());
        wal.append_blocking(vec![1], None).unwrap();
        let mut seen = Vec::new();
        wal.scan_from(1, &mut |_, k, _| {
            seen.push(k);
            Ok(())
        })
        .unwrap();
        assert_eq!(seen, [kind]);
        wal.close().unwrap();
        let (wal, _) = Wal::open(Arc::new(StdVfs), dir.path(), o, 1, &mut |_, k, _| {
            assert_eq!(k, kind);
            Ok(())
        })
        .unwrap();
        wal.close().unwrap();
    }
}

#[tokio::test]
async fn writer_commits_final_allowed_prefix_and_rejects_without_consuming_or_rolling() {
    let dir = tempfile::tempdir().unwrap();
    seed_empty(dir.path(), LIMIT - 3);
    let mut o = options();
    o.append_kind = FrameKind::Records;
    o.lsn_limit = Some(LIMIT);
    o.segment_bytes = 45;
    o.max_queued_bytes = 20;
    let wal = open(dir.path(), o);
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let r = wal.reserve(4).await.unwrap();
    let first = wal
        .append_reserved(
            r,
            vec![1; 4],
            Some(Box::new(move |_| {
                started_tx.send(()).unwrap();
                release_rx.recv().unwrap();
            })),
        )
        .unwrap();
    started_rx.recv().unwrap();
    // All four requests are queued while the writer is inside the first hook. They
    // must remain a group commit, with only the final two legal LSNs allocated.
    let mut pending = Vec::new();
    for _ in 0..4 {
        let r = wal.reserve(4).await.unwrap();
        pending.push(wal.append_reserved(r, vec![2; 4], None).unwrap());
    }
    release_tx.send(()).unwrap();
    assert_eq!(first.await.unwrap().lsn, LIMIT - 3);
    let second = pending.remove(0).await.unwrap();
    let third = pending.remove(0).await.unwrap();
    assert_eq!((second.lsn, third.lsn), (LIMIT - 2, LIMIT - 1));
    assert_eq!(
        second.segment, third.segment,
        "allowed prefix must commit together"
    );
    for p in pending {
        assert!(
            matches!(p.await, Err(WalError::InvalidRecords(reason)) if reason.contains("offset-limit"))
        );
    }
    assert_eq!(wal.next_lsn(), LIMIT);
    assert_eq!(wal.acked_lsn(), LIMIT - 1);
    assert_eq!(wal.health(), WalHealth::Healthy);
    let before = StdVfs.read_dir(dir.path()).unwrap();
    let lengths: Vec<_> = before
        .iter()
        .map(|p| (p.clone(), std::fs::metadata(p).unwrap().len()))
        .collect();
    for _ in 0..3 {
        assert!(matches!(
            wal.append(vec![3; 4], None).await,
            Err(WalError::InvalidRecords(_))
        ));
    }
    assert_eq!(StdVfs.read_dir(dir.path()).unwrap(), before);
    for (p, n) in lengths {
        assert_eq!(std::fs::metadata(p).unwrap().len(), n);
    }
    // Full admission is available again; no rejected request retains its permits.
    let reservation = tokio::time::timeout(Duration::from_secs(2), wal.reserve(20))
        .await
        .unwrap()
        .unwrap();
    drop(reservation);
    assert_eq!(wal.next_lsn(), LIMIT);
    assert_eq!(wal.health(), WalHealth::Healthy);
    wal.close().unwrap();
}

#[test]
fn capped_scan_honors_cap_on_sealed_segments_and_ignores_unrelated_hints() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = options();
    o.segment_bytes = 45;
    let wal = open(dir.path(), o);
    let locs: Vec<_> = (0..5)
        .map(|_| wal.append_blocking(vec![1; 4], None).unwrap())
        .collect();
    for hint in [
        None,
        Some(locs[0]),
        Some(locs[4]),
        Some(RecordLoc {
            segment: 999,
            ..locs[0]
        }),
    ] {
        let mut seen = Vec::new();
        let flow = wal
            .scan_from_loc_capped(1, 2, hint, &mut |loc, _, _| {
                seen.push(loc.lsn);
                Ok(ControlFlow::Continue(()))
            })
            .unwrap();
        assert_eq!(seen, [1, 2]);
        assert!(flow.is_continue());
    }
    let mut seen = Vec::new();
    let _ = wal
        .scan_from_loc_capped(4, u64::MAX, Some(locs[3]), &mut |loc, _, _| {
            seen.push(loc.lsn);
            Ok(ControlFlow::Continue(()))
        })
        .unwrap();
    assert_eq!(seen, [4, 5]);
    wal.close().unwrap();
}

#[test]
fn sparse_hint_skips_prefix_and_bad_byte_boundary_falls_back() {
    let dir = tempfile::tempdir().unwrap();
    let wal = open(dir.path(), options());
    let locs: Vec<_> = (0..5)
        .map(|_| wal.append_blocking(vec![1; 4], None).unwrap())
        .collect();
    // A wrong hint must never silently turn a bounded read into an empty result.
    for hint in [
        RecordLoc {
            offset: locs[3].offset + 1,
            ..locs[3]
        },
        RecordLoc {
            offset: u64::MAX,
            ..locs[3]
        },
    ] {
        let mut seen = Vec::new();
        let _ = wal
            .scan_from_loc_capped(4, 5, Some(hint), &mut |loc, _, _| {
                seen.push(loc.lsn);
                Ok(ControlFlow::Continue(()))
            })
            .unwrap();
        assert_eq!(seen, [4, 5]);
    }
    // Prefix damage demonstrates that a valid sparse boundary begins decoding there.
    let path = dir.path().join(segment_file_name(1));
    let file = StdVfs
        .open(&path, prkdb_core::vfs::OpenMode::ReadWrite)
        .unwrap();
    file.write_at(locs[0].offset + 8, &[99]).unwrap();
    let mut seen = Vec::new();
    let _ = wal
        .scan_from_loc_capped(4, 5, Some(locs[3]), &mut |loc, _, _| {
            seen.push(loc.lsn);
            Ok(ControlFlow::Continue(()))
        })
        .unwrap();
    assert_eq!(seen, [4, 5]);
    wal.close().unwrap();
}

#[test]
fn recovery_rejects_offset_exhaustion_before_cleanup_or_tail_truncation() {
    for first in [LIMIT, LIMIT + 1] {
        let dir = tempfile::tempdir().unwrap();
        seed_empty(dir.path(), first);
        if first == LIMIT {
            let file = StdVfs
                .open(
                    &dir.path().join(segment_file_name(first)),
                    prkdb_core::vfs::OpenMode::ReadWrite,
                )
                .unwrap();
            let mut frame = Vec::new();
            prkdb_core::wal::frame::encode_frame(&mut frame, first, FrameKind::Records, &[1]);
            frame.extend_from_slice(&[0, 0]); // would be truncated by ordinary recovery
            file.write_at(24, &frame).unwrap();
        }
        // Retention would remove this leftover before the normal replay starts.
        let file = StdVfs
            .create(&dir.path().join(segment_file_name(first - 1)))
            .unwrap();
        write_segment_header(&*file, first - 1).unwrap();
        let mut frame = Vec::new();
        prkdb_core::wal::frame::encode_frame(&mut frame, first - 1, FrameKind::Records, &[1]);
        file.write_at(24, &frame).unwrap();
        let mut paths = StdVfs.read_dir(dir.path()).unwrap();
        paths.sort();
        let before: Vec<_> = paths
            .iter()
            .map(|p| (p.clone(), std::fs::read(p).unwrap()))
            .collect();
        let mut o = options();
        o.lsn_limit = Some(LIMIT);
        o.front_release = prkdb_core::wal::FrontRelease::Retention;
        let result = Wal::open(Arc::new(StdVfs), dir.path(), o, 1, &mut |_, _, _| Ok(()));
        assert!(
            matches!(result, Err(WalError::InvalidRecords(reason)) if reason.contains("offset-limit"))
        );
        let mut paths = StdVfs.read_dir(dir.path()).unwrap();
        paths.sort();
        assert_eq!(
            paths,
            before.iter().map(|(p, _)| p.clone()).collect::<Vec<_>>()
        );
        for (p, bytes) in before {
            assert_eq!(std::fs::read(p).unwrap(), bytes);
        }
    }
}

#[test]
fn recovered_empty_terminal_cursor_is_allowed_but_cannot_append() {
    let dir = tempfile::tempdir().unwrap();
    seed_empty(dir.path(), LIMIT);
    let mut o = options();
    o.lsn_limit = Some(LIMIT);
    let wal = open(dir.path(), o);
    assert_eq!(wal.next_lsn(), LIMIT);
    assert!(matches!(
        wal.append_blocking(vec![1], None),
        Err(WalError::InvalidRecords(_))
    ));
    wal.close().unwrap();
}
