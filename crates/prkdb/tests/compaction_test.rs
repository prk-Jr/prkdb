use prkdb::raft::config::ClusterConfig;
use prkdb::raft::node::RaftNode;
use prkdb::raft::state_machine::{PrkDbStateMachine, StateMachine};
use prkdb::storage::WalStorageAdapter;
use prkdb_types::storage::StorageAdapter;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;

/// Bind an ephemeral port and return it, so concurrently-running test binaries
/// cannot collide on a fixed number.
async fn free_port() -> u16 {
    tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("binding an ephemeral port on loopback cannot fail")
        .local_addr()
        .expect("a bound listener always has a local address")
        .port()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_log_compaction() {
    // Create storage
    let temp_dir = TempDir::new().unwrap();
    let storage = Arc::new(
        WalStorageAdapter::builder(temp_dir.path().to_path_buf())
            .build()
            .expect("Failed to create storage"),
    );

    // Create state machine
    let state_machine = Arc::new(PrkDbStateMachine::new(storage.clone()));

    // Create cluster config.
    //
    // The port is allocated by the OS rather than hardcoded: test binaries in this
    // workspace run concurrently, and 50001 was previously used by both this file
    // and read_index_test.rs.
    let port = free_port().await;
    let mut peers = HashMap::new();
    peers.insert(1, format!("127.0.0.1:{port}"));
    let listen_addr = format!("127.0.0.1:{port}").parse().unwrap();

    let config = ClusterConfig {
        local_node_id: 1,
        listen_addr,
        nodes: vec![(1, listen_addr)],
        election_timeout_min_ms: 1000,
        election_timeout_max_ms: 2000,
        heartbeat_interval_ms: 200,
        partition_id: 0,
    };

    // Create Raft node
    let _node = Arc::new(RaftNode::new(
        config,
        storage.clone(),
        state_machine.clone(),
    ));

    // Write some KV pairs to storage
    for i in 0..100 {
        let key = format!("key_{}", i);
        let value = format!("value_{}", i);
        storage.put(key.as_bytes(), value.as_bytes()).await.unwrap();
    }

    // Create a snapshot using the state_machine directly
    let snapshot_result = state_machine.snapshot().await;
    assert!(snapshot_result.is_ok(), "Snapshot creation should succeed");

    let snapshot_data = snapshot_result.unwrap();
    assert!(!snapshot_data.is_empty(), "Snapshot should contain data");

    println!("✅ Snapshot created: {} bytes", snapshot_data.len());

    // Restore snapshot to a new storage
    let temp_dir2 = TempDir::new().unwrap();
    let storage2 = Arc::new(
        WalStorageAdapter::builder(temp_dir2.path().to_path_buf())
            .build()
            .expect("Failed to create storage"),
    );
    let state_machine2 = Arc::new(PrkDbStateMachine::new(storage2.clone()));

    let restore_result = state_machine2.restore(&snapshot_data).await;
    assert!(restore_result.is_ok(), "Snapshot restore should succeed");

    // Verify all keys are restored
    for i in 0..100 {
        let key = format!("key_{}", i);
        let expected_value = format!("value_{}", i);

        let retrieved = storage2.get(key.as_bytes()).await.unwrap();
        assert!(
            retrieved.is_some(),
            "Key {} should exist after restore",
            key
        );
        assert_eq!(
            String::from_utf8(retrieved.unwrap()).unwrap(),
            expected_value,
            "Value for key {} should match",
            key
        );
    }

    println!("✅ All 100 keys verified after snapshot restore");
}

// ---------------------------------------------------------------------------------------
// WAL compaction (Task 2.15)
// ---------------------------------------------------------------------------------------

/// Compaction keeps exactly the live values, shrinks the log, and survives reopen.
#[tokio::test(flavor = "multi_thread")]
async fn compaction_keeps_only_live_values_and_removes_old_segments() {
    use prkdb_core::wal::{SyncMode, WalConfig};
    let dir = tempfile::tempdir().unwrap();
    let cfg = || WalConfig {
        log_dir: dir.path().to_path_buf(),
        segment_bytes: 32 * 1024,
        sync_mode: SyncMode::Fast,
        ..WalConfig::test_config()
    };
    let wal_bytes = || -> u64 {
        std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.extension().is_some_and(|e| e == "wal"))
            .map(|p| std::fs::metadata(p).unwrap().len())
            .sum()
    };
    let db = WalStorageAdapter::new(cfg()).unwrap();
    for round in 0..50u32 {
        for k in 0..20u32 {
            db.put(format!("k{k}").as_bytes(), &vec![round as u8; 512])
                .await
                .unwrap();
        }
    }
    db.delete(b"k3").await.unwrap();
    db.flush().await.unwrap();
    let before = wal_bytes();
    let report = db.compact().await.unwrap();
    let after = wal_bytes();
    println!("compaction: {before} -> {after} bytes on disk; {report:?}");
    assert!(report.segments_rewritten > 0, "{report:?}");
    assert!(
        after * 4 < before,
        "compaction must reclaim most of 50 overwrites: {before} -> {after}"
    );
    let check = |db: WalStorageAdapter| async move {
        for k in 0..20u32 {
            let want = (k != 3).then(|| vec![49u8; 512]);
            assert_eq!(
                db.get(format!("k{k}").as_bytes()).await.unwrap(),
                want,
                "k{k}"
            );
        }
    };
    check(db).await;
    check(WalStorageAdapter::open_async(cfg()).await.unwrap()).await;
}

/// A delete survives compaction: the key stays deleted after compact() + reopen even though
/// the put it deleted sat in an older segment.
#[tokio::test(flavor = "multi_thread")]
async fn a_compacted_delete_does_not_resurrect_an_older_put() {
    use prkdb_core::wal::{SyncMode, WalConfig};
    let dir = tempfile::tempdir().unwrap();
    let cfg = || WalConfig {
        log_dir: dir.path().to_path_buf(),
        segment_bytes: 8 * 1024,
        sync_mode: SyncMode::Fast,
        ..WalConfig::test_config()
    };
    let db = WalStorageAdapter::new(cfg()).unwrap();
    db.put(b"victim", &[1u8; 512]).await.unwrap();
    for i in 0..64u32 {
        db.put(format!("filler{i}").as_bytes(), &[2u8; 512])
            .await
            .unwrap(); // roll past the put
    }
    db.delete(b"victim").await.unwrap();
    for i in 0..64u32 {
        db.put(format!("filler{i}").as_bytes(), &[3u8; 512])
            .await
            .unwrap(); // seal the delete's segment
    }
    db.flush().await.unwrap();
    db.compact().await.unwrap();
    assert_eq!(db.get(b"victim").await.unwrap(), None);
    drop(db);
    assert_eq!(
        WalStorageAdapter::open_async(cfg())
            .await
            .unwrap()
            .get(b"victim")
            .await
            .unwrap(),
        None
    );
}

/// Writers keep writing while compaction runs; nothing they wrote is lost or reverted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compaction_races_writers_safely() {
    use prkdb_core::wal::{SyncMode, WalConfig};
    use std::sync::Arc;
    let dir = tempfile::tempdir().unwrap();
    let cfg = || WalConfig {
        log_dir: dir.path().to_path_buf(),
        segment_bytes: 16 * 1024,
        sync_mode: SyncMode::Fast,
        ..WalConfig::test_config()
    };
    let db = Arc::new(WalStorageAdapter::new(cfg()).unwrap());
    for i in 0..2000u32 {
        db.put(format!("k{}", i % 50).as_bytes(), &i.to_le_bytes())
            .await
            .unwrap();
    }
    let writer = {
        let db = db.clone();
        tokio::spawn(async move {
            for i in 2000..4000u32 {
                db.put(format!("k{}", i % 50).as_bytes(), &i.to_le_bytes())
                    .await
                    .unwrap();
            }
        })
    };
    for _ in 0..5 {
        db.compact().await.unwrap();
    }
    writer.await.unwrap();
    for k in 0..50u32 {
        let last = (2000..4000u32).rev().find(|i| i % 50 == k).unwrap();
        assert_eq!(
            db.get(format!("k{k}").as_bytes()).await.unwrap(),
            Some(last.to_le_bytes().to_vec())
        );
    }
    db.flush().await.unwrap();
    drop(db);
    let db = WalStorageAdapter::open_async(cfg()).await.unwrap();
    for k in 0..50u32 {
        let last = (2000..4000u32).rev().find(|i| i % 50 == k).unwrap();
        assert_eq!(
            db.get(format!("k{k}").as_bytes()).await.unwrap(),
            Some(last.to_le_bytes().to_vec())
        );
    }
}

fn compaction_cfg(dir: &std::path::Path, segment_bytes: u64) -> prkdb_core::wal::WalConfig {
    prkdb_core::wal::WalConfig {
        log_dir: dir.to_path_buf(),
        segment_bytes,
        sync_mode: prkdb_core::wal::SyncMode::Fast,
        ..prkdb_core::wal::WalConfig::test_config()
    }
}

async fn contents(db: &WalStorageAdapter) -> std::collections::BTreeMap<Vec<u8>, Vec<u8>> {
    let mut out = std::collections::BTreeMap::new();
    for k in db.get_all_keys() {
        out.insert(
            k.clone(),
            db.get(&k).await.unwrap().expect("indexed key readable"),
        );
    }
    out
}

fn segment_files(dir: &std::path::Path) -> usize {
    std::fs::read_dir(dir)
        .unwrap()
        .filter(|e| {
            e.as_ref()
                .unwrap()
                .path()
                .extension()
                .is_some_and(|x| x == "wal")
        })
        .count()
}

/// The crash window between compaction's last segment swap and its fresh checkpoint: the
/// checkpoint taken before the run was deleted before the first rename, so a crash right
/// there reopens with no checkpoint (a full replay), never with a stale one whose offsets
/// point into the middle of rewritten frames.
#[tokio::test(flavor = "multi_thread")]
async fn a_crash_before_the_fresh_checkpoint_reopens_by_full_replay() {
    use prkdb::storage::compaction::CompactionStep;
    let dir = tempfile::tempdir().unwrap();
    let cfg = || compaction_cfg(dir.path(), 8 * 1024);
    let expected = {
        let db = WalStorageAdapter::new(cfg()).unwrap();
        for round in 0..20u32 {
            for k in 0..30u32 {
                db.put(format!("k{k}").as_bytes(), &vec![round as u8; 300])
                    .await
                    .unwrap();
            }
            if round == 10 {
                db.delete(b"k7").await.unwrap();
            }
        }
        db.save_checkpoint_async().await.unwrap();
        db.put(b"tail", b"after the checkpoint").await.unwrap();
        db.flush().await.unwrap();
        let expected = contents(&db).await;
        let err = db
            .compact_with_hook(|step| match step {
                CompactionStep::SegmentsDone => Err("crash here".to_string()),
                _ => Ok(()),
            })
            .await
            .expect_err("the hook aborts the run before its fresh checkpoint");
        assert!(err.to_string().contains("SegmentsDone"), "{err}");
        let left: Vec<_> = std::fs::read_dir(dir.path().join("checkpoints"))
            .map(|d| d.map(|e| e.unwrap().path()).collect())
            .unwrap_or_default();
        assert!(
            left.is_empty(),
            "no checkpoint may survive the run: {left:?}"
        );
        assert_eq!(
            contents(&db).await,
            expected,
            "the live adapter after the run"
        );
        expected
    };
    let db = WalStorageAdapter::open_async(cfg()).await.unwrap();
    assert_eq!(
        db.last_recovery().checkpoint_lsn,
        None,
        "{:?}",
        db.last_recovery()
    );
    assert_eq!(contents(&db).await, expected);

    // A completed run writes a fresh checkpoint, which the next open uses.
    for k in 0..30u32 {
        db.put(format!("k{k}").as_bytes(), &[99u8; 300])
            .await
            .unwrap();
    }
    let expected = contents(&db).await;
    let report = db.compact().await.unwrap();
    assert!(report.segments_rewritten > 0, "{report:?}");
    drop(db);
    let db = WalStorageAdapter::open_async(cfg()).await.unwrap();
    assert!(
        db.last_recovery().checkpoint_lsn.is_some(),
        "{report:?} {:?}",
        db.last_recovery()
    );
    assert!(
        db.last_recovery().rejected_checkpoints.is_empty(),
        "{:?}",
        db.last_recovery()
    );
    assert_eq!(contents(&db).await, expected);
}

/// Readers racing segment swaps never see a wrong value: every key only ever grows, so a
/// read must return a value at least as new as the last one that reader saw for the key,
/// and never `None` (no key is ever deleted).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn readers_racing_compaction_never_see_a_wrong_value() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    const KEYS: u32 = 200;
    let dir = tempfile::tempdir().unwrap();
    // A cache far smaller than the key set, so reads go to the WAL and meet the swaps.
    let db = Arc::new(
        WalStorageAdapter::new_with_config(prkdb::storage::config::StorageConfig {
            wal: compaction_cfg(dir.path(), 4 * 1024),
            cache_capacity: 16,
            ..prkdb::storage::config::StorageConfig::new(dir.path().to_path_buf())
        })
        .unwrap(),
    );
    for i in 0..KEYS * 20 {
        db.put(format!("k{}", i % KEYS).as_bytes(), &i.to_be_bytes())
            .await
            .unwrap();
    }
    let done = Arc::new(AtomicBool::new(false));
    let writer = {
        let (db, done) = (db.clone(), done.clone());
        tokio::spawn(async move {
            let mut i = KEYS * 20;
            while !done.load(Ordering::Acquire) {
                db.put(format!("k{}", i % KEYS).as_bytes(), &i.to_be_bytes())
                    .await
                    .unwrap();
                i += 1;
            }
        })
    };
    let readers: Vec<_> = (0..3)
        .map(|r| {
            let (db, done) = (db.clone(), done.clone());
            tokio::spawn(async move {
                let mut seen = vec![0u32; KEYS as usize];
                let mut reads = 0u64;
                while !done.load(Ordering::Acquire) {
                    for k in 0..KEYS {
                        let key = format!("k{k}");
                        let got = match r {
                            0 => db.get(key.as_bytes()).await.unwrap(),
                            1 => db
                                .snapshot_get_many(vec![key.clone().into_bytes()])
                                .await
                                .unwrap()
                                .pop()
                                .unwrap(),
                            _ => db
                                .scan_prefix(key.as_bytes())
                                .await
                                .unwrap()
                                .into_iter()
                                .find(|(found, _)| found == key.as_bytes())
                                .map(|(_, v)| v),
                        };
                        let value = got.unwrap_or_else(|| panic!("reader {r}: k{k} vanished"));
                        let v = u32::from_be_bytes(value.try_into().expect("4-byte value"));
                        assert_eq!(v % KEYS, k, "reader {r}: k{k} returned another key's value");
                        assert!(
                            v >= seen[k as usize],
                            "reader {r}: k{k} went back from {} to {v}",
                            seen[k as usize]
                        );
                        seen[k as usize] = v;
                        reads += 1;
                    }
                }
                reads
            })
        })
        .collect();
    let mut rewritten = 0;
    for _ in 0..20 {
        rewritten += db.compact().await.unwrap().segments_rewritten;
    }
    done.store(true, Ordering::Release);
    writer.await.unwrap();
    for reader in readers {
        assert!(reader.await.unwrap() > 0);
    }
    assert!(rewritten > 0, "the race needs rewrites to race against");
}

/// What a change-stream consumer sees after compaction (documented in
/// `prkdb::storage::compaction`): surviving ops keep their LSNs, dropped ops are gone, and
/// replaying the stream from 0 still rebuilds the current state exactly. LSNs are never
/// reused, even after leading segments are removed and the log reopened.
#[tokio::test(flavor = "multi_thread")]
async fn the_change_stream_after_compaction_keeps_versions_and_rebuilds_the_state() {
    use prkdb_types::replication::Change;
    use std::collections::BTreeMap;
    let dir = tempfile::tempdir().unwrap();
    let cfg = || compaction_cfg(dir.path(), 4 * 1024);
    let db = WalStorageAdapter::new(cfg()).unwrap();
    for round in 0..30u32 {
        for k in 0..10u32 {
            db.put(format!("k{k}").as_bytes(), &[round as u8; 200])
                .await
                .unwrap();
        }
    }
    db.delete(b"k4").await.unwrap();
    for k in 0..30u32 {
        db.put(format!("tail{k}").as_bytes(), &[7u8; 200])
            .await
            .unwrap(); // seal the delete's segment
    }
    db.flush().await.unwrap();
    let before = db.get_changes_since(0).await.unwrap();
    let last_lsn = db.max_offset();
    let report = db.compact().await.unwrap();
    assert!(report.segments_removed > 0, "{report:?}");
    let after = db.get_changes_since(0).await.unwrap();
    assert!(after.len() < before.len());
    assert!(
        !after
            .iter()
            .any(|c| matches!(c, Change::Delete { key, .. } if key == b"k4")),
        "a dropped delete is not reported"
    );

    let version = |c: &Change| match c {
        Change::Put { version, .. } | Change::Delete { version, .. } => *version,
    };
    // Every surviving change is one of the originals, unchanged, at its original version.
    for change in &after {
        assert!(
            before.contains(change),
            "{change:?} was not in the log before"
        );
    }
    let versions: Vec<u64> = after.iter().map(version).collect();
    assert!(
        versions.windows(2).all(|w| w[0] <= w[1]),
        "still in LSN order"
    );

    let replay = |changes: &[Change]| {
        let mut state = BTreeMap::new();
        for c in changes {
            match c {
                Change::Put { key, value, .. } => {
                    state.insert(key.clone(), value.clone());
                }
                Change::Delete { key, .. } => {
                    state.remove(key);
                }
            }
        }
        state
    };
    assert_eq!(replay(&after), replay(&before));
    assert_eq!(replay(&after), contents(&db).await);

    drop(db);
    let db = WalStorageAdapter::open_async(cfg()).await.unwrap();
    assert_eq!(
        db.max_offset(),
        last_lsn,
        "compaction never moves the log's end"
    );
    db.put(b"next", b"n").await.unwrap();
    let next = db.get_changes_since(last_lsn).await.unwrap();
    assert_eq!(next.len(), 1);
    assert_eq!(version(&next[0]), last_lsn + 1, "LSNs are never reused");
    assert_eq!(
        replay(&db.get_changes_since(0).await.unwrap()),
        contents(&db).await
    );
}

/// Compacting twice with nothing new to drop rewrites nothing the second time.
#[tokio::test(flavor = "multi_thread")]
async fn a_second_compaction_with_nothing_dead_changes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let db = WalStorageAdapter::new(compaction_cfg(dir.path(), 4 * 1024)).unwrap();
    for round in 0..10u32 {
        for k in 0..20u32 {
            db.put(format!("k{k}").as_bytes(), &[round as u8; 100])
                .await
                .unwrap();
        }
    }
    let first = db.compact().await.unwrap();
    assert!(first.segments_rewritten > 0, "{first:?}");
    let second = db.compact().await.unwrap();
    assert_eq!(second.segments_rewritten, 0, "{second:?}");
    assert_eq!(second.segments_removed, 0, "{second:?}");
    assert_eq!(second.bytes_before, second.bytes_after, "{second:?}");
}

/// The background task (Task 2.15's trigger) compacts once the log is big enough and dead
/// enough, without anyone calling `compact`.
#[tokio::test(flavor = "multi_thread")]
async fn the_background_task_compacts_when_the_thresholds_are_met() {
    use prkdb::storage::config::StorageConfig;
    use prkdb::storage::CompactionConfig;
    use std::time::{Duration, Instant};
    let dir = tempfile::tempdir().unwrap();
    let config = StorageConfig {
        wal: compaction_cfg(dir.path(), 4 * 1024),
        compaction: CompactionConfig {
            min_wal_size_bytes: 64 * 1024,
            min_interval: Duration::from_millis(50),
            min_dead_ratio: 0.5,
        },
        ..StorageConfig::new(dir.path().to_path_buf())
    };
    let db = WalStorageAdapter::new_with_config(config).unwrap();
    for round in 0..40u32 {
        for k in 0..10u32 {
            db.put(format!("k{k}").as_bytes(), &[round as u8; 200])
                .await
                .unwrap();
        }
    }
    db.flush().await.unwrap();
    // A run may start while the loop above is still writing, so the signal is what a run
    // leaves behind, not a before/after count: the fresh checkpoint it writes (nothing
    // else here writes one), and a log far smaller than what was written.
    let written = 40 * 10 * (17 + 200);
    let checkpoint_written = || {
        std::fs::read_dir(dir.path().join("checkpoints"))
            .map(|d| d.count() > 0)
            .unwrap_or(false)
    };
    let deadline = Instant::now() + Duration::from_secs(20);
    while !checkpoint_written() {
        assert!(
            Instant::now() < deadline,
            "the background task never compacted ({} segments)",
            segment_files(dir.path())
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let on_disk: u64 = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|x| x == "wal"))
        .map(|p| std::fs::metadata(p).unwrap().len())
        .sum();
    assert!(
        on_disk * 2 < written,
        "{on_disk} bytes left of {written} written"
    );
    for k in 0..10u32 {
        assert_eq!(
            db.get(format!("k{k}").as_bytes()).await.unwrap(),
            Some(vec![39u8; 200])
        );
    }
}

/// Below its thresholds the background task leaves the log alone.
#[tokio::test(flavor = "multi_thread")]
async fn the_background_task_waits_for_its_thresholds() {
    use prkdb::storage::config::StorageConfig;
    use prkdb::storage::CompactionConfig;
    use std::time::Duration;
    let dir = tempfile::tempdir().unwrap();
    let config = StorageConfig {
        wal: compaction_cfg(dir.path(), 4 * 1024),
        compaction: CompactionConfig {
            min_wal_size_bytes: 64 * 1024,
            min_interval: Duration::from_millis(20),
            min_dead_ratio: 0.5,
        },
        ..StorageConfig::new(dir.path().to_path_buf())
    };
    let db = WalStorageAdapter::new_with_config(config).unwrap();
    // Plenty of bytes, nothing dead: every key written once.
    for k in 0..400u32 {
        db.put(format!("k{k}").as_bytes(), &[1u8; 200])
            .await
            .unwrap();
    }
    let before = segment_files(dir.path());
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(segment_files(dir.path()), before);
    assert!(
        std::fs::read_dir(dir.path().join("checkpoints")).is_err(),
        "no run happened, so no checkpoint was written"
    );
}
