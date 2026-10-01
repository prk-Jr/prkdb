//! Checkpoints are an optimisation, never a source of truth (spec §7 2d, Task 2.14):
//! recover(checkpoint, wal) == recover(∅, wal).

use prkdb::storage::config::StorageConfig;
use prkdb::storage::WalStorageAdapter;
use prkdb_core::vfs::Vfs;
use prkdb_core::wal::{SyncMode, WalConfig};
use prkdb_types::storage::StorageAdapter;
use prkdb_verify::faultfs::{FaultFs, Tear};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

fn cfg(dir: &Path) -> WalConfig {
    WalConfig {
        log_dir: dir.to_path_buf(),
        segment_bytes: 16 * 1024,
        sync_mode: SyncMode::Fast,
        ..WalConfig::test_config()
    }
}

async fn contents(db: &WalStorageAdapter) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let mut out = BTreeMap::new();
    for k in db.get_all_keys() {
        out.insert(
            k.clone(),
            db.get(&k).await.unwrap().expect("indexed key readable"),
        );
    }
    out
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for e in std::fs::read_dir(from).unwrap() {
        let p = e.unwrap().path();
        let dest = to.join(p.file_name().unwrap());
        if p.is_dir() {
            copy_dir(&p, &dest)
        } else {
            std::fs::copy(&p, &dest).unwrap();
        }
    }
}

fn checkpoints(dir: &Path) -> Vec<PathBuf> {
    let mut found: Vec<PathBuf> = std::fs::read_dir(dir.join("checkpoints"))
        .map(|rd| rd.map(|e| e.unwrap().path()).collect())
        .unwrap_or_default();
    found.sort();
    found
}

/// What a full replay of `dir`'s log recovers: the same directory without checkpoints.
async fn full_replay_of(dir: &Path) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let replay_dir = tempfile::tempdir().unwrap();
    copy_dir(dir, replay_dir.path());
    let _ = std::fs::remove_dir_all(replay_dir.path().join("checkpoints"));
    let db = WalStorageAdapter::open_async(cfg(replay_dir.path()))
        .await
        .unwrap();
    assert_eq!(db.last_recovery().checkpoint_lsn, None);
    contents(&db).await
}

#[tokio::test(flavor = "multi_thread")]
async fn recovery_from_a_checkpoint_equals_full_replay() {
    for seed in 0..30u64 {
        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        let dir = tempfile::tempdir().unwrap();
        {
            let db = WalStorageAdapter::new(cfg(dir.path())).unwrap();
            for i in 0..400u32 {
                let key = format!("k{}", rng.gen_range(0..40)).into_bytes();
                match rng.gen_range(0..10) {
                    0..=5 => db
                        .put(&key, format!("s{seed}-{i}").as_bytes())
                        .await
                        .unwrap(),
                    6..=7 => db.delete(&key).await.unwrap(),
                    8 => db.save_checkpoint().unwrap(),
                    _ => db
                        .put_batch(vec![
                            (key.clone(), vec![i as u8; 64]),
                            (b"batch".to_vec(), vec![1]),
                        ])
                        .await
                        .unwrap(),
                }
            }
            db.flush().await.unwrap();
        }
        let full = full_replay_of(dir.path()).await;
        let db = WalStorageAdapter::open_async(cfg(dir.path()))
            .await
            .unwrap();
        assert!(
            db.last_recovery().rejected_checkpoints.is_empty(),
            "seed {seed}: {:?}",
            db.last_recovery()
        );
        assert_eq!(contents(&db).await, full, "seed {seed}");
    }
}

/// The snapshot is taken while writers keep going (no pause): whatever it caught of the
/// writes racing it, replaying the frames after `covered` makes recovery exact.
#[tokio::test(flavor = "multi_thread")]
async fn a_checkpoint_taken_under_concurrent_writes_recovers_exactly() {
    for seed in 0..8u64 {
        let dir = tempfile::tempdir().unwrap();
        {
            let db = WalStorageAdapter::new(cfg(dir.path())).unwrap();
            let writers: Vec<_> = (0..4u64)
                .map(|w| {
                    let db = db.clone();
                    tokio::spawn(async move {
                        let mut rng = ChaCha8Rng::seed_from_u64(seed * 100 + w);
                        for i in 0..300u32 {
                            let key = format!("k{}", rng.gen_range(0..64)).into_bytes();
                            if rng.gen_bool(0.25) {
                                db.delete(&key).await.unwrap();
                            } else {
                                db.put(&key, format!("w{w}-{i}").as_bytes()).await.unwrap();
                            }
                        }
                    })
                })
                .collect();
            for _ in 0..10 {
                let db = db.clone();
                tokio::task::spawn_blocking(move || db.save_checkpoint().unwrap())
                    .await
                    .unwrap();
                tokio::task::yield_now().await;
            }
            for w in writers {
                w.await.unwrap();
            }
        }
        let full = full_replay_of(dir.path()).await;
        let db = WalStorageAdapter::open_async(cfg(dir.path()))
            .await
            .unwrap();
        assert!(db.last_recovery().checkpoint_lsn.is_some(), "seed {seed}");
        assert_eq!(contents(&db).await, full, "seed {seed}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_checkpoint_is_actually_used() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = WalStorageAdapter::new(cfg(dir.path())).unwrap();
        for i in 0..1000u32 {
            db.put(format!("k{i}").as_bytes(), b"v").await.unwrap();
        }
        db.save_checkpoint().unwrap();
        db.put(b"tail", b"v").await.unwrap();
    }
    let db = WalStorageAdapter::open_async(cfg(dir.path()))
        .await
        .unwrap();
    let r = db.last_recovery();
    assert_eq!(r.checkpoint_lsn, Some(1000), "{r:?}");
    assert_eq!(r.checkpoint_entries, 1000, "{r:?}");
    assert!(
        r.frames_replayed <= 2,
        "replayed {} frames after a checkpoint at the tail",
        r.frames_replayed
    );
    assert_eq!(r.frames_scanned, 1001, "every frame is still CRC-checked");
    assert_eq!(db.get_all_keys().len(), 1001);
    assert_eq!(
        checkpoints(dir.path()).len(),
        1,
        "one checkpoint, no temp file"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_corrupt_checkpoint_falls_back_to_full_replay() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = WalStorageAdapter::new(cfg(dir.path())).unwrap();
        for i in 0..100u32 {
            db.put(format!("k{i}").as_bytes(), b"v").await.unwrap();
        }
        db.save_checkpoint().unwrap();
    }
    let ckpt = checkpoints(dir.path()).remove(0);
    let mut bytes = std::fs::read(&ckpt).unwrap();
    let mid = bytes.len() / 2;
    bytes[mid] ^= 0xFF;
    std::fs::write(&ckpt, bytes).unwrap();
    let db = WalStorageAdapter::open_async(cfg(dir.path()))
        .await
        .unwrap();
    let r = db.last_recovery();
    assert_eq!(r.checkpoint_lsn, None);
    assert_eq!(r.frames_replayed, 100);
    let name = ckpt.file_name().unwrap().to_str().unwrap();
    assert!(
        r.rejected_checkpoints.len() == 1
            && r.rejected_checkpoints[0].contains(name)
            && r.rejected_checkpoints[0].contains("checksum"),
        "the rejection names the file and the reason: {r:?}"
    );
    assert_eq!(db.get_all_keys().len(), 100);
}

/// A torn checkpoint (cut short) is refused like a corrupt one.
#[tokio::test(flavor = "multi_thread")]
async fn a_truncated_checkpoint_falls_back_to_full_replay() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = WalStorageAdapter::new(cfg(dir.path())).unwrap();
        for i in 0..50u32 {
            db.put(format!("k{i}").as_bytes(), b"v").await.unwrap();
        }
        db.save_checkpoint().unwrap();
    }
    let ckpt = checkpoints(dir.path()).remove(0);
    let bytes = std::fs::read(&ckpt).unwrap();
    std::fs::write(&ckpt, &bytes[..bytes.len() / 3]).unwrap();
    let db = WalStorageAdapter::open_async(cfg(dir.path()))
        .await
        .unwrap();
    assert_eq!(db.last_recovery().checkpoint_lsn, None);
    assert_eq!(db.last_recovery().rejected_checkpoints.len(), 1);
    assert_eq!(db.get_all_keys().len(), 50);
}

/// A bad newest checkpoint falls back to an older one when one is still there.
#[tokio::test(flavor = "multi_thread")]
async fn a_corrupt_newest_checkpoint_falls_back_to_an_older_one() {
    let dir = tempfile::tempdir().unwrap();
    let aside = tempfile::tempdir().unwrap();
    {
        let db = WalStorageAdapter::new(cfg(dir.path())).unwrap();
        for i in 0..40u32 {
            db.put(format!("k{i}").as_bytes(), b"old").await.unwrap();
        }
        db.save_checkpoint().unwrap();
        let older = checkpoints(dir.path()).remove(0);
        std::fs::copy(&older, aside.path().join("older")).unwrap();
        for i in 0..20u32 {
            db.put(format!("k{i}").as_bytes(), b"new").await.unwrap();
        }
        db.delete(b"k39").await.unwrap();
        db.save_checkpoint().unwrap();
        assert!(!older.exists(), "a new checkpoint removes the older one");
        std::fs::copy(aside.path().join("older"), &older).unwrap();
    }
    let newest = checkpoints(dir.path()).pop().unwrap();
    let mut bytes = std::fs::read(&newest).unwrap();
    bytes[30] ^= 0x01;
    std::fs::write(&newest, bytes).unwrap();

    let full = full_replay_of(dir.path()).await;
    let db = WalStorageAdapter::open_async(cfg(dir.path()))
        .await
        .unwrap();
    let r = db.last_recovery();
    assert_eq!(r.checkpoint_lsn, Some(40), "{r:?}");
    assert_eq!(r.frames_replayed, 21, "{r:?}");
    assert_eq!(r.rejected_checkpoints.len(), 1, "{r:?}");
    assert_eq!(contents(&db).await, full);
}

/// A checkpoint that does not fit the log it sits next to (here: one from a longer log)
/// is refused after the log is opened, and the index is rebuilt by a full replay.
#[tokio::test(flavor = "multi_thread")]
async fn a_checkpoint_beyond_the_log_is_ignored() {
    let long = tempfile::tempdir().unwrap();
    {
        let db = WalStorageAdapter::new(cfg(long.path())).unwrap();
        for i in 0..300u32 {
            db.put(format!("k{i}").as_bytes(), b"long").await.unwrap();
        }
        db.save_checkpoint().unwrap();
    }
    let short = tempfile::tempdir().unwrap();
    {
        let db = WalStorageAdapter::new(cfg(short.path())).unwrap();
        for i in 0..10u32 {
            db.put(format!("s{i}").as_bytes(), b"short").await.unwrap();
        }
    }
    copy_dir(
        &long.path().join("checkpoints"),
        &short.path().join("checkpoints"),
    );

    let db = WalStorageAdapter::open_async(cfg(short.path()))
        .await
        .unwrap();
    let r = db.last_recovery();
    assert_eq!(r.checkpoint_lsn, None, "{r:?}");
    assert!(
        r.rejected_checkpoints[0].contains("beyond the log"),
        "{r:?}"
    );
    assert_eq!(r.frames_replayed, 10);
    let got = contents(&db).await;
    assert_eq!(got.len(), 10);
    assert!(got.keys().all(|k| k.starts_with(b"s")));
}

fn fault_config(mode: SyncMode) -> StorageConfig {
    let dir = PathBuf::from("/db/wal");
    StorageConfig {
        wal: WalConfig {
            log_dir: dir.clone(),
            sync_mode: mode,
            sync_interval_ms: 3_600_000, // Fast syncs only when asked
            segment_bytes: 2 * 1024,
            ..WalConfig::test_config()
        },
        ..StorageConfig::new(dir)
    }
}

fn fault_open(fs: &FaultFs, mode: SyncMode) -> WalStorageAdapter {
    WalStorageAdapter::open_with_vfs(fault_config(mode), Arc::new(fs.clone())).expect("open")
}

async fn fault_contents(fs: &FaultFs, mode: SyncMode) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let db = fault_open(fs, mode);
    contents(&db).await
}

/// In Fast mode a checkpoint never references a frame a power cut can take: writes
/// acknowledged after it are lost to the power cut, everything before it survives, the
/// checkpoint itself survives (written atomically, directory synced), and recovery from
/// it equals a full replay of what the disk kept.
#[tokio::test(flavor = "multi_thread")]
async fn a_fast_mode_checkpoint_survives_power_loss_and_recovers_exactly() {
    for (seed, tear) in [Tear::None, Tear::Prefix, Tear::ZeroTail, Tear::Garbage]
        .into_iter()
        .enumerate()
    {
        let fs = FaultFs::new();
        fs.mkdir_durable(Path::new("/db")).unwrap();
        let db = fault_open(&fs, SyncMode::Fast);
        for i in 0..200u32 {
            db.put(
                format!("k{}", i % 70).as_bytes(),
                format!("v{i}").as_bytes(),
            )
            .await
            .unwrap();
            if i % 9 == 0 {
                db.delete(format!("k{}", i % 13).as_bytes()).await.unwrap();
            }
        }
        db.save_checkpoint().unwrap();
        let at_checkpoint = contents(&db).await;
        for i in 0..30u32 {
            db.put(format!("late{i}").as_bytes(), b"unsynced")
                .await
                .unwrap();
        }
        fs.power_loss(&mut ChaCha8Rng::seed_from_u64(seed as u64), tear);
        drop(db);

        let db = fault_open(&fs, SyncMode::Fast);
        let r = db.last_recovery().clone();
        assert!(r.checkpoint_lsn.is_some(), "{tear:?}: {r:?}");
        assert!(r.rejected_checkpoints.is_empty(), "{tear:?}: {r:?}");
        let recovered = contents(&db).await;
        for (k, v) in &at_checkpoint {
            assert_eq!(recovered.get(k), Some(v), "{tear:?}: {k:?}");
        }
        drop(db);

        // Full replay of the same disk: remove the checkpoints and reopen.
        let ckpt_dir = Path::new("/db/wal/checkpoints");
        for p in fs.read_dir(ckpt_dir).unwrap() {
            fs.remove(&p).unwrap();
        }
        fs.sync_dir(ckpt_dir).unwrap();
        assert_eq!(
            fault_contents(&fs, SyncMode::Fast).await,
            recovered,
            "{tear:?}"
        );
    }
}
