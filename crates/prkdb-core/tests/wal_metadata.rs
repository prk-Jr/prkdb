//! Public metadata contracts used by retention and change-stream readers.
use prkdb_core::vfs::StdVfs;
use prkdb_core::wal::log_state::{LogState, LOG_STATE_FILE};
use prkdb_core::wal::{FrontRelease, Lsn, SyncMode, Wal, WalConfig, WalOptions};
use std::path::Path;
use std::sync::Arc;

fn open(dir: &Path, mode: SyncMode) -> Wal {
    open_with_release(dir, mode, FrontRelease::ElidedOnly)
}

fn open_with_release(dir: &Path, mode: SyncMode, release: FrontRelease) -> Wal {
    let config = WalConfig {
        sync_mode: mode,
        ..WalConfig::test_config()
    };
    Wal::open(
        Arc::new(StdVfs),
        dir,
        WalOptions {
            front_release: release,
            ..WalOptions::from_config(&config)
        },
        1,
        &mut |_, _, _| Ok(()),
    )
    .unwrap()
    .0
}

fn disk_segment_bytes(wal: &Wal) -> u64 {
    wal.segments()
        .into_iter()
        .map(|first| std::fs::metadata(wal.segment_path(first)).unwrap().len())
        .sum()
}

#[test]
fn log_bytes_counts_headers_and_payloads_across_every_segment_after_reopen() {
    for mode in [SyncMode::Fast, SyncMode::Durable] {
        let dir = tempfile::tempdir().unwrap();
        let wal = open(dir.path(), mode);
        assert_eq!(wal.log_bytes().unwrap(), disk_segment_bytes(&wal));
        assert!(wal.log_bytes().unwrap() > 1, "an empty log has a header");
        for len in [17, 31, 65] {
            wal.append_blocking(vec![0x53; len], None).unwrap();
            wal.roll_blocking().unwrap();
        }
        wal.append_blocking(vec![0x94; 113], None).unwrap();
        wal.sync_blocking().unwrap();
        assert_eq!(wal.segments().len(), 4);
        let expected = disk_segment_bytes(&wal);
        assert_eq!(wal.log_bytes().unwrap(), expected, "{mode:?}");
        wal.close().unwrap();
        let reopened = open(dir.path(), mode);
        assert_eq!(reopened.segments().len(), 4);
        assert_eq!(reopened.log_bytes().unwrap(), expected, "{mode:?}");
        reopened.close().unwrap();
    }
}

#[test]
fn delete_compaction_floor_is_monotonic_and_persisted_before_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let wal = open(dir.path(), SyncMode::Durable);
    assert_eq!(wal.deletes_compacted_through(), 0);
    for _ in 0..23 {
        wal.append_blocking(vec![0x64], None).unwrap();
    }
    let check = |wal: &Wal, floor: Lsn| {
        assert_eq!(wal.deletes_compacted_through(), floor);
        assert_eq!(wal.log_state().deletes_compacted_through, floor);
        assert_eq!(
            LogState::read(&StdVfs, dir.path())
                .unwrap()
                .deletes_compacted_through,
            floor,
        );
    };
    // These are the positions of deletes a compactor has already selected.
    for floor in [7, 19] {
        wal.raise_deletes_compacted_through(floor).unwrap();
        check(&wal, floor);
    }
    let persisted = std::fs::read(dir.path().join(LOG_STATE_FILE)).unwrap();
    for lower_or_equal in [0, 7, 19] {
        wal.raise_deletes_compacted_through(lower_or_equal).unwrap();
        check(&wal, 19);
        assert_eq!(
            std::fs::read(dir.path().join(LOG_STATE_FILE)).unwrap(),
            persisted
        );
    }
    wal.close().unwrap();
    let reopened = open(dir.path(), SyncMode::Durable);
    check(&reopened, 19);
    reopened.raise_deletes_compacted_through(23).unwrap();
    check(&reopened, 23);
    reopened.close().unwrap();
}

#[test]
fn subscribing_at_the_same_ack_does_not_wake_existing_receivers() {
    for mode in [SyncMode::Fast, SyncMode::Durable] {
        let dir = tempfile::tempdir().unwrap();
        let wal = open(dir.path(), mode);
        let mut first = wal.subscribe_acked();
        assert!(!first.has_changed().expect("the WAL is open"));
        let second = wal.subscribe_acked();
        assert!(
            !first.has_changed().expect("the WAL is open"),
            "unchanged ack, {mode:?}"
        );
        assert!(!second.has_changed().expect("the WAL is open"));

        let loc = wal.append_blocking(vec![0x61], None).unwrap();
        // Append replies precede watch publication. A queued sync is processed
        // after that batch publishes, so this assertion cannot race the writer.
        wal.sync_blocking().unwrap();
        assert!(
            first.has_changed().expect("the WAL is open"),
            "ack advanced, {mode:?}"
        );
        assert_eq!(*first.borrow_and_update(), loc.lsn);
        let late = wal.subscribe_acked();
        assert_eq!(*late.borrow(), loc.lsn);
        assert!(!late.has_changed().expect("the WAL is open"));
        assert!(
            !first.has_changed().expect("the WAL is open"),
            "late subscription, {mode:?}"
        );
        wal.close().unwrap();
    }
}

#[test]
fn advancing_log_start_persists_it_without_losing_the_compaction_floor() {
    let dir = tempfile::tempdir().unwrap();
    let wal = open_with_release(dir.path(), SyncMode::Durable, FrontRelease::Retention);
    let loc = wal.append_blocking(vec![0x64], None).unwrap();
    wal.roll_blocking().unwrap();
    wal.raise_deletes_compacted_through(loc.lsn).unwrap();
    let next_segment = wal.segments()[1];
    wal.set_log_start(next_segment).unwrap();
    let expected = LogState {
        log_start: next_segment,
        deletes_compacted_through: loc.lsn,
    };
    assert_eq!(wal.log_state(), expected);
    assert_eq!(LogState::read(&StdVfs, dir.path()).unwrap(), expected);
    wal.close().unwrap();
    let reopened = open_with_release(dir.path(), SyncMode::Durable, FrontRelease::Retention);
    assert_eq!(reopened.log_state(), expected);
    assert_eq!(reopened.segments(), vec![next_segment]);
    reopened.close().unwrap();
}
