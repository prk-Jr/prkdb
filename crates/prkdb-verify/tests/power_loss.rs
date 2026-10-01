//! The storage adapter under simulated power loss (Task 2.8a): STO-02 and STO-04 end to end.

use prkdb::storage::compaction::CompactionStep;
use prkdb::storage::config::StorageConfig;
use prkdb::storage::WalStorageAdapter;
use prkdb_core::vfs::{OpenMode, Vfs};
use prkdb_core::wal::{SyncMode, WalConfig};
use prkdb_types::storage::StorageAdapter;
use prkdb_verify::faultfs::{FaultFs, Tear};
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

fn config(mode: SyncMode) -> StorageConfig {
    let dir = PathBuf::from("/db/wal");
    StorageConfig {
        wal: WalConfig {
            log_dir: dir.clone(),
            sync_mode: mode,
            sync_interval_ms: 3_600_000, // Fast syncs only when asked: deterministic window
            segment_bytes: 16 * 1024,
            ..WalConfig::test_config()
        },
        ..StorageConfig::new(dir)
    }
}

fn open(fs: &FaultFs, mode: SyncMode) -> WalStorageAdapter {
    WalStorageAdapter::open_with_vfs(config(mode), Arc::new(fs.clone())).expect("open")
}

fn fresh() -> FaultFs {
    let fs = FaultFs::new();
    fs.mkdir_durable(Path::new("/db")).unwrap();
    fs
}

/// STO-02: an acknowledged Durable put survives power loss, whatever the tail looks like.
#[tokio::test(flavor = "multi_thread")]
async fn durable_put_survives_power_loss() {
    for tear in [Tear::None, Tear::Prefix, Tear::ZeroTail, Tear::Garbage] {
        let fs = fresh();
        let db = open(&fs, SyncMode::Durable);
        for i in 0..50u32 {
            db.put(format!("k{i}").as_bytes(), &i.to_le_bytes())
                .await
                .unwrap();
        }
        db.delete(b"k7").await.unwrap();
        fs.power_loss(&mut ChaCha8Rng::seed_from_u64(1), tear);
        drop(db);
        let db = open(&fs, SyncMode::Durable);
        for i in 0..50u32 {
            let want = (i != 7).then(|| i.to_le_bytes().to_vec());
            assert_eq!(
                db.get(format!("k{i}").as_bytes()).await.unwrap(),
                want,
                "k{i} after {tear:?}"
            );
        }
    }
}

/// STO-04 end to end: garbage after the last synced record is truncated at open, earlier
/// keys stay readable, and writes after the reopen survive the next power loss.
#[tokio::test(flavor = "multi_thread")]
async fn a_torn_adapter_tail_keeps_earlier_keys_and_accepts_new_writes() {
    let fs = fresh();
    let db = open(&fs, SyncMode::Fast);
    for i in 0..20u32 {
        db.put(format!("k{i}").as_bytes(), b"early").await.unwrap();
    }
    db.flush().await.unwrap(); // flush == sync: everything above is durable
    for i in 20..30u32 {
        db.put(format!("k{i}").as_bytes(), b"late").await.unwrap();
    }
    fs.power_loss(&mut ChaCha8Rng::seed_from_u64(5), Tear::Garbage);
    drop(db);

    let db = open(&fs, SyncMode::Durable);
    for i in 0..20u32 {
        assert_eq!(
            db.get(format!("k{i}").as_bytes()).await.unwrap().as_deref(),
            Some(&b"early"[..])
        );
    }
    db.put(b"after", b"reopen").await.unwrap();
    fs.power_loss(&mut ChaCha8Rng::seed_from_u64(6), Tear::None);
    drop(db);

    let db = open(&fs, SyncMode::Durable);
    assert_eq!(
        db.get(b"after").await.unwrap().as_deref(),
        Some(&b"reopen"[..])
    );
    for i in 0..20u32 {
        assert!(db.get(format!("k{i}").as_bytes()).await.unwrap().is_some());
    }
}

/// A sealed segment corrupted on disk refuses to open instead of losing data silently.
#[tokio::test(flavor = "multi_thread")]
async fn a_corrupt_sealed_segment_fails_the_open_by_name() {
    let fs = fresh();
    let db = open(&fs, SyncMode::Durable);
    for i in 0..400u32 {
        db.put(format!("k{i}").as_bytes(), &[1u8; 100])
            .await
            .unwrap();
    }
    drop(db);
    let seg = Path::new("/db/wal").join(prkdb_core::wal::segment::segment_file_name(1));
    let f = fs.open(&seg, OpenMode::ReadWrite).unwrap();
    f.write_at(64, &[0xEE; 16]).unwrap();
    f.sync_data().unwrap();
    let err = WalStorageAdapter::open_with_vfs(config(SyncMode::Durable), Arc::new(fs.clone()))
        .err()
        .expect("must refuse");
    assert!(
        err.to_string().contains("00000000000000000001.wal"),
        "{err}"
    );
}

// ---------------------------------------------------------------------------------------
// Compaction under power loss (Task 2.15)
// ---------------------------------------------------------------------------------------

async fn contents(db: &WalStorageAdapter) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let mut out = BTreeMap::new();
    for k in db.get_all_keys() {
        let v = db.get(&k).await.unwrap().expect("indexed key readable");
        out.insert(k, v);
    }
    out
}

/// Enough overwrites, deletes and batches over 16 KiB segments for a compaction to rewrite
/// several sealed segments, drop deletes, and remove fully elided segments from the front;
/// a checkpoint early on, so the run has one to delete, and keys it covers that the
/// rewrite moves. Everything is flushed: the contents
/// before compaction are durable, so every crash must recover exactly them.
async fn compaction_workload(db: &WalStorageAdapter) {
    for round in 0..12u32 {
        for k in 0..24u32 {
            db.put(format!("k{k}").as_bytes(), &[round as u8; 200])
                .await
                .unwrap();
        }
        if round == 3 {
            // Written once, after dead frames in their segment: the rewrite moves them to
            // new offsets, so a checkpoint from before the run would point at stale ones.
            for i in 0..8u32 {
                db.put(format!("stable{i}").as_bytes(), &[0x5A; 100])
                    .await
                    .unwrap();
            }
        }
        if round == 4 {
            db.save_checkpoint_async().await.unwrap();
        }
        if round % 4 == 1 {
            db.delete(format!("k{round}").as_bytes()).await.unwrap();
            db.put_batch(vec![
                (format!("b{round}").into_bytes(), vec![round as u8; 64]),
                (b"k0".to_vec(), vec![0xB0 | round as u8; 32]),
            ])
            .await
            .unwrap();
        }
    }
    db.delete(b"k5").await.unwrap();
    for round in [1u32, 5, 9] {
        // Rewritten, so the oldest segments end up fully elided and are removed.
        db.put(format!("b{round}").as_bytes(), b"again")
            .await
            .unwrap();
    }
    for k in 0..24u32 {
        db.put(format!("tail{k}").as_bytes(), &[0xEE; 200])
            .await
            .unwrap(); // seal the last delete's segment
    }
    db.flush().await.unwrap();
}

type Steps = Arc<parking_lot::Mutex<Vec<CompactionStep>>>;

/// A power loss right after any step of a compaction run — with any tear, in either mode
/// — recovers exactly the contents from before the run, and a later run on the recovered
/// directory keeps them too.
#[tokio::test(flavor = "multi_thread")]
async fn compaction_is_crash_safe_at_every_step() {
    for mode in [SyncMode::Durable, SyncMode::Fast] {
        // A clean run lists the steps; every run of the same workload takes the same ones.
        let fs = fresh();
        let db = open(&fs, mode);
        compaction_workload(&db).await;
        let expected = contents(&db).await;
        let steps: Steps = Arc::default();
        let record = steps.clone();
        db.compact_with_hook(move |step| {
            record.lock().push(step);
            Ok(())
        })
        .await
        .unwrap();
        assert_eq!(contents(&db).await, expected, "{mode:?}: a clean run");
        drop(db);
        let steps = steps.lock().clone();
        for kind in [
            "CompactFileWritten",
            "LogSynced",
            "CheckpointsDeleted",
            "SegmentReplaced",
            "SegmentRemoved",
            "SegmentsDone",
            "CheckpointWritten",
        ] {
            assert!(
                steps.iter().any(|s| format!("{s:?}").starts_with(kind)),
                "{mode:?}: the workload never reaches {kind}: {steps:?}"
            );
        }

        for (cut, at) in steps.iter().enumerate() {
            for (t, tear) in [Tear::None, Tear::Prefix, Tear::ZeroTail, Tear::Garbage]
                .into_iter()
                .enumerate()
            {
                let fs = fresh();
                let db = open(&fs, mode);
                compaction_workload(&db).await;
                let mut seen = 0usize;
                let power = fs.clone();
                let seed = (cut * 4 + t) as u64;
                let outcome = db
                    .compact_with_hook(move |_| {
                        seen += 1;
                        if seen == cut + 1 {
                            power.power_loss(&mut ChaCha8Rng::seed_from_u64(seed), tear);
                            return Err("power cut".to_string());
                        }
                        Ok(())
                    })
                    .await;
                assert!(
                    outcome.is_err(),
                    "{mode:?} {at:?}: the hook must abort the run"
                );
                drop(db);

                let db = open(&fs, mode);
                let ctx = format!("{mode:?}, power cut after {at:?} (step {cut}), {tear:?}");
                assert_eq!(contents(&db).await, expected, "{ctx}");
                let report = db.compact().await.unwrap_or_else(|e| panic!("{ctx}: {e}"));
                assert_eq!(contents(&db).await, expected, "{ctx}: then {report:?}");
                drop(db);
                let db = open(&fs, mode);
                assert_eq!(contents(&db).await, expected, "{ctx}: reopened after rerun");
            }
        }
    }
}

/// Fast mode: `k = old` and `gone` are durable in a sealed segment; then, unsynced in the
/// active segment, `k = new`, a marker, and a delete of `gone`.
async fn unsynced_replacements(db: &WalStorageAdapter) {
    db.put(b"k", b"old").await.unwrap();
    db.put(b"gone", b"durable").await.unwrap();
    for i in 0..100u32 {
        db.put(format!("filler{i}").as_bytes(), &[1u8; 200])
            .await
            .unwrap(); // seal their segment
    }
    db.flush().await.unwrap(); // `old` and `gone` are durable
    db.put(b"k", b"new").await.unwrap(); // unsynced (no periodic sync for an hour)
    db.put(b"marker", b"before the delete").await.unwrap();
    db.delete(b"gone").await.unwrap(); // unsynced
}

/// The case compaction's log sync exists for (Fast mode): an overwrite or a delete that
/// superseded a durable record is still unsynced when compaction drops the old record.
/// Without the sync, a power cut after the rewrite is renamed into place would lose both
/// the old record and its replacement. The cut lands right after every step (the fresh
/// checkpoint at the end syncs the log too, so a cut only after a complete run would not
/// notice a missing sync), with every tear.
#[tokio::test(flavor = "multi_thread")]
async fn compaction_never_drops_a_record_whose_replacement_is_unsynced() {
    let fs = fresh();
    let db = open(&fs, SyncMode::Fast);
    unsynced_replacements(&db).await;
    let steps: Steps = Arc::default();
    let record = steps.clone();
    let report = db
        .compact_with_hook(move |step| {
            record.lock().push(step);
            Ok(())
        })
        .await
        .unwrap();
    assert!(report.segments_rewritten > 0, "{report:?}");
    drop(db);
    let steps = steps.lock().clone();

    for (cut, at) in steps.iter().enumerate() {
        for tear in [Tear::None, Tear::Prefix, Tear::ZeroTail, Tear::Garbage] {
            let fs = fresh();
            let db = open(&fs, SyncMode::Fast);
            unsynced_replacements(&db).await;
            let mut seen = 0usize;
            let power = fs.clone();
            let _ = db
                .compact_with_hook(move |_| {
                    seen += 1;
                    if seen == cut + 1 {
                        power.power_loss(&mut ChaCha8Rng::seed_from_u64(cut as u64), tear);
                        return Err("power cut".to_string());
                    }
                    Ok(())
                })
                .await;
            drop(db);

            let db = open(&fs, SyncMode::Fast);
            let ctx = format!("power cut after {at:?} (step {cut}), {tear:?}");
            let k = db.get(b"k").await.unwrap();
            assert!(
                k.as_deref() == Some(&b"old"[..]) || k.as_deref() == Some(&b"new"[..]),
                "{ctx}: k must be old or new, got {k:?}"
            );
            // A prefix of the log: if the delete survived, so did the put written before it.
            if db.get(b"gone").await.unwrap().is_none() {
                assert_eq!(
                    db.get(b"marker").await.unwrap().as_deref(),
                    Some(&b"before the delete"[..]),
                    "{ctx}: the delete survived without the write before it"
                );
            }
        }
    }
}
