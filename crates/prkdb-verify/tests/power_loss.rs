//! The storage adapter under simulated power loss (Task 2.8a): STO-02 and STO-04 end to end.

use prkdb::storage::config::StorageConfig;
use prkdb::storage::WalStorageAdapter;
use prkdb_core::vfs::{OpenMode, Vfs};
use prkdb_core::wal::{SyncMode, WalConfig};
use prkdb_types::storage::StorageAdapter;
use prkdb_verify::faultfs::{FaultFs, Tear};
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
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
