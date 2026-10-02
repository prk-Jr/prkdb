//! The `Wal` side of compaction (Task 2.15): replacing a sealed segment, removing fully
//! elided ones from the front, stale locations (`Moved`), and a log that starts after
//! LSN 1. The storage-level behaviour is in `crates/prkdb/tests/compaction_test.rs` and
//! `crates/prkdb-verify/tests/power_loss.rs`.

use prkdb_core::vfs::{StdVfs, Vfs};
use prkdb_core::wal::frame::{encode_frame, FrameKind};
use prkdb_core::wal::segment::{write_segment_header, SEGMENT_HEADER_LEN};
use prkdb_core::wal::{Lsn, RecordLoc, SealedSegment, SyncMode, Wal, WalError, WalOptions};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

fn opts() -> WalOptions {
    WalOptions {
        sync_mode: SyncMode::Durable,
        sync_interval: Duration::from_millis(20),
        segment_bytes: 512,
        max_batch_bytes: 1 << 20,
        max_queued_bytes: 8 << 20,
    }
}

fn open(dir: &Path) -> (Wal, Vec<(RecordLoc, FrameKind)>) {
    let mut seen = Vec::new();
    let (wal, _) = Wal::open(Arc::new(StdVfs), dir, opts(), 1, &mut |loc, kind, _| {
        seen.push((loc, kind));
        Ok(())
    })
    .unwrap();
    (wal, seen)
}

/// A log of `n` 40-byte records over 512-byte segments; returns every record's location.
fn filled(dir: &Path, n: u8) -> (Wal, Vec<RecordLoc>) {
    let (wal, _) = open(dir);
    let locs = (0..n)
        .map(|i| wal.append_blocking(vec![i; 40], None).unwrap())
        .collect();
    (wal, locs)
}

/// Writes a replacement for `seg` next to it: `keep(lsn)` frames copied, the rest elided.
fn rewrite(wal: &Wal, seg: &SealedSegment, keep: impl Fn(Lsn) -> bool) -> PathBuf {
    let mut frames: Vec<(Lsn, Vec<u8>)> = Vec::new();
    wal.scan_sealed(seg.first_lsn, &mut |loc, _, payload| {
        frames.push((loc.lsn, payload.to_vec()));
        Ok(())
    })
    .unwrap();
    let path = wal
        .segment_path(seg.first_lsn)
        .with_extension("wal.compact");
    let file = StdVfs.create(&path).unwrap();
    write_segment_header(&*file, seg.first_lsn).unwrap();
    let mut buf = Vec::new();
    for (lsn, payload) in frames {
        if keep(lsn) {
            encode_frame(&mut buf, lsn, FrameKind::Batch, &payload);
        } else {
            encode_frame(&mut buf, lsn, FrameKind::Elided, &[]);
        }
    }
    file.write_at(SEGMENT_HEADER_LEN, &buf).unwrap();
    file.sync_data().unwrap();
    path
}

/// A replaced segment keeps its LSN range; a location from before the swap is `Moved`
/// (not corruption), the new one reads; a wrong location in a segment compaction never
/// touched is still `CorruptSegment`.
#[test]
fn a_replaced_segment_reads_at_new_locations_and_old_ones_are_moved() {
    let dir = tempfile::tempdir().unwrap();
    let (wal, locs) = filled(dir.path(), 40);
    let sealed = wal.sealed_segments().unwrap();
    assert!(sealed.len() >= 3, "{sealed:?}");
    let seg = sealed[0];
    let last_in_seg = seg.next_lsn - 1;
    let path = rewrite(&wal, &seg, |lsn| lsn == last_in_seg);

    let old = locs[(last_in_seg - 1) as usize];
    let new = RecordLoc {
        offset: SEGMENT_HEADER_LEN + 17 * (last_in_seg - seg.first_lsn),
        ..old
    };
    wal.replace_segment(seg.first_lsn, &path).unwrap();
    assert!(!path.exists(), "the rewrite was renamed into place");
    assert_eq!(wal.segment_generation(seg.first_lsn), Some(1));
    assert_eq!(wal.read(new).unwrap(), vec![(last_in_seg - 1) as u8; 40]);
    // Until the caller's index has moved, a location from before the swap reads the
    // replaced file (the index update runs outside the segments lock).
    assert_eq!(wal.read(old).unwrap(), vec![(last_in_seg - 1) as u8; 40]);
    assert_eq!(wal.read(locs[0]).unwrap(), vec![0u8; 40]);
    wal.release_replaced(seg.first_lsn);
    assert!(
        matches!(wal.read(old), Err(WalError::Moved { .. })),
        "{:?}",
        wal.read(old)
    );
    // An elided record's old location is stale too.
    assert!(matches!(wal.read(locs[0]), Err(WalError::Moved { .. })));

    let untouched = locs[(sealed[1].first_lsn - 1) as usize];
    let wrong = RecordLoc {
        offset: untouched.offset + 1,
        ..untouched
    };
    assert_eq!(wal.segment_generation(sealed[1].first_lsn), Some(0));
    assert!(
        matches!(wal.read(wrong), Err(WalError::CorruptSegment { .. })),
        "{:?}",
        wal.read(wrong)
    );
    // Same LSN and offset, wrong length: not the frame asked for.
    let short = RecordLoc {
        payload_len: 39,
        ..untouched
    };
    assert!(matches!(
        wal.read(short),
        Err(WalError::CorruptSegment { .. })
    ));
    wal.close().unwrap();

    // Reopen: the rewritten segment is an ordinary segment, frames and all.
    let (wal, seen) = open(dir.path());
    assert_eq!(seen.len(), 40, "every LSN is still there");
    let elided = seen
        .iter()
        .filter(|(_, kind)| *kind == FrameKind::Elided)
        .count() as u64;
    assert_eq!(elided, seg.next_lsn - seg.first_lsn - 1);
    assert_eq!(wal.read(new).unwrap(), vec![(last_in_seg - 1) as u8; 40]);
    assert_eq!(wal.next_lsn(), 41);
}

/// Fully elided leading segments are removed; the log then starts later, reopens, keeps
/// its LSNs, and never reuses one. Removing a segment with a live frame, or the active
/// one, is refused.
#[test]
fn fully_elided_leading_segments_are_removed_and_the_log_starts_later() {
    let dir = tempfile::tempdir().unwrap();
    let (wal, locs) = filled(dir.path(), 40);
    let sealed = wal.sealed_segments().unwrap();
    let (first, second) = (sealed[0], sealed[1]);

    let refused = wal.set_log_start(first.next_lsn);
    assert!(
        matches!(refused, Err(WalError::CompactionRefused(_))),
        "a segment with live frames: {refused:?}"
    );

    for seg in [first, second] {
        let path = rewrite(&wal, &seg, |_| false);
        wal.replace_segment(seg.first_lsn, &path).unwrap();
    }
    let early = wal.remove_leading_segments(second.next_lsn);
    assert!(
        matches!(early, Err(WalError::CompactionRefused(_))),
        "removal needs the durable log start moved first: {early:?}"
    );
    wal.set_log_start(second.next_lsn).unwrap();
    assert_eq!(wal.log_state().log_start, second.next_lsn);
    assert_eq!(wal.remove_leading_segments(second.next_lsn).unwrap(), 2);
    assert_eq!(wal.segments()[0], second.next_lsn);
    assert!(!wal.segment_path(first.first_lsn).exists());
    assert!(
        matches!(wal.read(locs[0]), Err(WalError::Moved { .. })),
        "a location in a removed segment is stale"
    );
    let active = *wal.segments().last().unwrap();
    let refused = wal.set_log_start(active + 1);
    assert!(
        matches!(refused, Err(WalError::CompactionRefused(_))),
        "the active segment is never removed: {refused:?}"
    );
    wal.close().unwrap();

    let (wal, seen) = open(dir.path());
    assert_eq!(seen.first().unwrap().0.lsn, second.next_lsn);
    assert_eq!(seen.last().unwrap().0.lsn, 40);
    assert_eq!(wal.append_blocking(b"next".to_vec(), None).unwrap().lsn, 41);
}

/// A replacement whose LSN range differs from the segment's, or the active segment, is
/// refused and changes nothing.
#[test]
fn a_mismatched_replacement_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let (wal, locs) = filled(dir.path(), 40);
    let seg = wal.sealed_segments().unwrap()[0];

    // One frame short.
    let path = wal
        .segment_path(seg.first_lsn)
        .with_extension("wal.compact");
    let file = StdVfs.create(&path).unwrap();
    write_segment_header(&*file, seg.first_lsn).unwrap();
    let mut buf = Vec::new();
    for lsn in seg.first_lsn..seg.next_lsn - 1 {
        encode_frame(&mut buf, lsn, FrameKind::Elided, &[]);
    }
    file.write_at(SEGMENT_HEADER_LEN, &buf).unwrap();
    let refused = wal.replace_segment(seg.first_lsn, &path);
    assert!(
        matches!(refused, Err(WalError::CompactionRefused(_))),
        "{refused:?}"
    );
    assert_eq!(wal.segment_generation(seg.first_lsn), Some(0));
    assert_eq!(wal.read(locs[0]).unwrap(), vec![0u8; 40]);

    let active = *wal.segments().last().unwrap();
    let refused = wal.replace_segment(active, &path);
    assert!(
        matches!(refused, Err(WalError::CompactionRefused(_))),
        "{refused:?}"
    );
}

/// A log that starts after LSN 1 still refuses a gap between later segments.
#[test]
fn a_gap_after_the_first_segment_is_still_corruption() {
    let dir = tempfile::tempdir().unwrap();
    let (wal, _) = filled(dir.path(), 40);
    let sealed = wal.sealed_segments().unwrap();
    wal.close().unwrap();
    std::fs::remove_file(dir.path().join(format!("{:020}.wal", sealed[1].first_lsn))).unwrap();
    let err = Wal::open(Arc::new(StdVfs), dir.path(), opts(), 1, &mut |_, _, _| {
        Ok(())
    })
    .err()
    .expect("a missing middle segment must refuse to open");
    assert!(
        matches!(err, WalError::CorruptSegment { ref reason, .. } if reason.contains("does not continue")),
        "{err}"
    );
}

/// A crash between moving the log start and removing the released segments leaves them on
/// disk: the next open recognises them (fully elided, before the log start), removes them,
/// and opens.
#[test]
fn released_segments_left_by_a_crash_are_removed_on_open() {
    let dir = tempfile::tempdir().unwrap();
    let (wal, _) = filled(dir.path(), 40);
    let first = wal.sealed_segments().unwrap()[0];
    let path = rewrite(&wal, &first, |_| false);
    wal.replace_segment(first.first_lsn, &path).unwrap();
    wal.set_log_start(first.next_lsn).unwrap();
    wal.close().unwrap(); // "crash" before remove_leading_segments
    assert!(wal_path(dir.path(), first.first_lsn).exists());

    let (wal, seen) = open(dir.path());
    assert!(!wal_path(dir.path(), first.first_lsn).exists());
    assert_eq!(seen.first().unwrap().0.lsn, first.next_lsn);
    assert_eq!(wal.log_state().log_start, first.next_lsn);
}

/// A segment before the log start that still holds a live frame is not something
/// compaction released: corruption, refused.
#[test]
fn a_live_segment_before_the_log_start_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let (wal, _) = filled(dir.path(), 40);
    let second = wal.sealed_segments().unwrap()[1];
    wal.close().unwrap();
    prkdb_core::wal::LogState {
        log_start: second.first_lsn,
        compacted_through: 0,
    }
    .write(&StdVfs, dir.path())
    .unwrap();
    let err = Wal::open(Arc::new(StdVfs), dir.path(), opts(), 1, &mut |_, _, _| {
        Ok(())
    })
    .err()
    .expect("refused");
    assert!(matches!(err, WalError::CorruptSegment { .. }), "{err}");
}

fn wal_path(dir: &Path, first: Lsn) -> PathBuf {
    dir.join(format!("{first:020}.wal"))
}
