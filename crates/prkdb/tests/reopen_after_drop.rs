//! STO-12: dropping the last handle to a database releases its data directory before the
//! drop returns, so an open right after it is never refused as `Locked`.
//!
//! The Linux CI failure this pins (`backup_then_restore_preserves_every_value`, reopening
//! a directory it had just closed) came from the process-wide side of the lock, not from
//! anything the database itself kept alive: the test binary spawns child processes on
//! other threads, a child holds a copy of every descriptor between its fork and its exec,
//! and a `flock` belongs to the open file description, so closing the lock file alone did
//! not release it while a child was in that window. Every loop below therefore runs with
//! threads spawning children the whole time.
//!
//! The other tests cover the holders that did live in the database: a batching handle's
//! worker task and `TtlStorage`'s cleanup task each kept a strong reference and dropped it
//! on a runtime thread some time after the caller's last handle was gone.

use prkdb::prelude::*;
use prkdb::storage::config::StorageConfig;
use prkdb::storage::{CompactionConfig, WalStorageAdapter};
use prkdb::ttl::TtlStorage;
use prkdb_core::batch_config::BatchConfig;
use prkdb_core::wal::{SyncMode, WalConfig};
use prkdb_types::error::StorageError;
use prkdb_types::storage::StorageAdapter;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

const ROUNDS: usize = 200;

/// Spawns short-lived child processes on a few threads until dropped.
struct Spawners {
    stop: Arc<AtomicBool>,
    threads: Vec<std::thread::JoinHandle<()>>,
}

impl Spawners {
    fn start() -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let threads = (0..3)
            .map(|_| {
                let stop = stop.clone();
                std::thread::spawn(move || {
                    while !stop.load(Ordering::Relaxed) {
                        let _ = child().status();
                    }
                })
            })
            .collect();
        Self { stop, threads }
    }
}

impl Drop for Spawners {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        for t in self.threads.drain(..) {
            let _ = t.join();
        }
    }
}

/// A child that exits at once and exists on every platform the tests run on.
fn child() -> std::process::Command {
    if cfg!(windows) {
        let mut c = std::process::Command::new("cmd");
        c.args(["/C", "exit 0"]);
        c
    } else {
        std::process::Command::new("true")
    }
}

/// Background compaction on, checking every millisecond with no size threshold, and
/// segments small enough that a pass has sealed segments to look at.
fn compacting_config(dir: &Path) -> StorageConfig {
    StorageConfig {
        wal: WalConfig {
            log_dir: dir.to_path_buf(),
            segment_bytes: 4096,
            sync_mode: SyncMode::Fast,
            ..WalConfig::test_config()
        },
        compaction: CompactionConfig {
            min_wal_size_bytes: 0,
            min_interval: Duration::from_millis(1),
            min_dead_ratio: 0.0,
            tombstone_retention_lsns: 0,
            max_bytes_per_sec: None,
        },
        ..StorageConfig::new(dir.to_path_buf())
    }
}

fn expect_open<T>(result: Result<T, StorageError>, round: usize) -> T {
    match result {
        Ok(v) => v,
        Err(e) => panic!("round {round}: reopening right after the drop failed: {e:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_wal_adapter_reopens_right_after_its_drop_with_compaction_running() {
    let dir = tempfile::tempdir().unwrap();
    let _children = Spawners::start();
    for round in 0..ROUNDS {
        let adapter = expect_open(
            WalStorageAdapter::new_with_config(compacting_config(dir.path())),
            round,
        );
        // Overwrites, so sealed segments hold dead records for a pass to find.
        for i in 0..8 {
            adapter
                .put(format!("k{}", i % 2).as_bytes(), &[round as u8; 512])
                .await
                .unwrap();
        }
        assert_eq!(
            adapter.get(b"k1").await.unwrap().as_deref(),
            Some(&[round as u8; 512][..])
        );
        drop(adapter);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_prkdb_reopens_right_after_its_drop() {
    let dir = tempfile::tempdir().unwrap();
    let _children = Spawners::start();
    for round in 0..ROUNDS {
        let db = PrkDb::builder()
            .with_data_dir(dir.path())
            .with_sync_mode(SyncMode::Fast)
            .build()
            .unwrap_or_else(|e| panic!("round {round}: reopening right after the drop: {e:?}"));
        db.put(b"users:k", &round.to_le_bytes()).await.unwrap();
        assert_eq!(
            db.get(b"users:k").await.unwrap(),
            Some(round.to_le_bytes().to_vec())
        );
        drop(db);
    }
}

#[derive(Collection, serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
struct Item {
    #[id]
    id: u64,
    round: u64,
}

/// A batching handle's worker task held the database until it noticed, on a runtime
/// thread, that its channel had closed. Once every batched write has been flushed nothing
/// needs the database any more, and dropping the last handle releases it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_flushed_batching_handle_releases_the_directory_when_dropped() {
    let dir = tempfile::tempdir().unwrap();
    for round in 0..ROUNDS as u64 {
        let db = PrkDb::builder()
            .with_data_dir(dir.path())
            .with_sync_mode(SyncMode::Fast)
            .register_collection::<Item>()
            .build()
            .unwrap_or_else(|e| panic!("round {round}: reopening right after the drop: {e:?}"));
        let handle = db.collection::<Item>().with_batching(BatchConfig {
            linger_ms: 1,
            max_batch_size: 4,
            ..Default::default()
        });
        handle.put(Item { id: 1, round }).await.unwrap();
        handle.flush().await.unwrap();
        assert_eq!(
            handle.get(&1).await.unwrap(),
            Some(Item { id: 1, round }),
            "round {round}"
        );
        drop(handle);
        drop(db);
    }
}

/// `TtlStorage`'s cleanup task held the adapter until the runtime got round to dropping
/// the aborted task.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ttl_storage_releases_the_directory_when_dropped() {
    let dir = tempfile::tempdir().unwrap();
    for round in 0..ROUNDS {
        let config = StorageConfig {
            wal: WalConfig {
                log_dir: dir.path().to_path_buf(),
                sync_mode: SyncMode::Fast,
                ..WalConfig::test_config()
            },
            ..StorageConfig::new(dir.path().to_path_buf())
        };
        let adapter = Arc::new(expect_open(
            WalStorageAdapter::new_with_config(config),
            round,
        ));
        let mut ttl = TtlStorage::new(adapter.clone());
        ttl.start_cleanup(Duration::from_millis(1));
        ttl.put_with_ttl(b"k", b"v", Duration::from_secs(60))
            .await
            .unwrap();
        // Let the cleanup task tick at least once while it holds what it holds.
        tokio::time::sleep(Duration::from_millis(2)).await;
        drop(ttl);
        drop(adapter);
    }
}

/// How [`GatedDeletes`] holds its first delete.
enum Hold {
    /// Blocks the worker thread until the test sends: the delete then completes without
    /// ever yielding, so only a check inside the tick can stop the rest of it.
    Block(std::sync::Mutex<std::sync::mpsc::Receiver<()>>),
    /// Parks on a signal the test never sends.
    Park(Arc<tokio::sync::Notify>),
}

/// A storage whose first delete is held (see [`Hold`]), that counts its deletes and
/// reports its own drop.
struct GatedDeletes {
    deletes: Arc<AtomicUsize>,
    first_delete: std::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    hold: Hold,
    held_delete_completed: Arc<AtomicBool>,
    dropped: std::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
}

impl GatedDeletes {
    fn new(
        hold: Hold,
    ) -> (
        Arc<Self>,
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Receiver<()>,
    ) {
        let (first_tx, first_rx) = tokio::sync::oneshot::channel();
        let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel();
        let storage = Arc::new(Self {
            deletes: Arc::default(),
            first_delete: std::sync::Mutex::new(Some(first_tx)),
            hold,
            held_delete_completed: Arc::default(),
            dropped: std::sync::Mutex::new(Some(dropped_tx)),
        });
        (storage, first_rx, dropped_rx)
    }
}

impl Drop for GatedDeletes {
    fn drop(&mut self) {
        if let Some(tx) = self.dropped.lock().unwrap().take() {
            let _ = tx.send(());
        }
    }
}

#[async_trait::async_trait]
impl StorageAdapter for GatedDeletes {
    async fn get(&self, _: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        Ok(None)
    }
    async fn put(&self, _: &[u8], _: &[u8]) -> Result<(), StorageError> {
        Ok(())
    }
    async fn delete(&self, _: &[u8]) -> Result<(), StorageError> {
        if self.deletes.fetch_add(1, Ordering::SeqCst) == 0 {
            if let Some(tx) = self.first_delete.lock().unwrap().take() {
                let _ = tx.send(());
            }
            match &self.hold {
                Hold::Block(go) => {
                    let _ = go.lock().unwrap().recv();
                }
                Hold::Park(signal) => signal.notified().await,
            }
            self.held_delete_completed.store(true, Ordering::SeqCst);
        }
        Ok(())
    }
}

const WAIT: Duration = Duration::from_secs(20);

/// A tick deleting many expired keys, with deletes that never yield, stops at the next key
/// once the `TtlStorage` is dropped, and releases the adapter: an abort alone takes effect
/// only at the task's next yield, after the whole tick.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ttl_storage_dropped_mid_tick_stops_at_the_next_key_and_releases_the_adapter() {
    const KEYS: usize = 1000;
    let (go_tx, go_rx) = std::sync::mpsc::channel();
    let (storage, first_delete, dropped) =
        GatedDeletes::new(Hold::Block(std::sync::Mutex::new(go_rx)));
    let mut ttl = TtlStorage::new(storage.clone());
    for i in 0..KEYS {
        ttl.put_with_ttl(format!("k{i}").as_bytes(), b"v", Duration::ZERO)
            .await
            .unwrap();
    }
    ttl.start_cleanup(Duration::from_millis(1));
    tokio::time::timeout(WAIT, first_delete)
        .await
        .expect("the tick reaches its first delete")
        .unwrap();

    let deletes = storage.deletes.clone();
    drop(ttl);
    drop(storage);
    go_tx.send(()).unwrap();
    tokio::time::timeout(WAIT, dropped)
        .await
        .expect("the adapter is released after the drop")
        .unwrap();
    // The held delete and its key's metadata delete, no more.
    let deletes = deletes.load(Ordering::SeqCst);
    assert!(
        deletes <= 2,
        "{deletes} of {} deletes ran after the TtlStorage was dropped",
        2 * KEYS
    );
}

/// Dropped while the tick is parked in a delete that never completes: the adapter is
/// released without that delete completing. `Drop` cannot join the task; it stops it, and
/// the runtime drops the parked delete (and the adapter it holds) when it next runs the
/// cancelled task.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ttl_storage_dropped_during_a_parked_delete_releases_the_adapter() {
    let signal = Arc::new(tokio::sync::Notify::new());
    let (storage, first_delete, dropped) = GatedDeletes::new(Hold::Park(signal.clone()));
    let mut ttl = TtlStorage::new(storage.clone());
    ttl.put_with_ttl(b"k", b"v", Duration::ZERO).await.unwrap();
    ttl.start_cleanup(Duration::from_millis(1));
    tokio::time::timeout(WAIT, first_delete)
        .await
        .expect("the tick reaches its first delete")
        .unwrap();

    let completed = storage.held_delete_completed.clone();
    drop(ttl);
    drop(storage);
    tokio::time::timeout(WAIT, dropped)
        .await
        .expect("the adapter is released after the drop")
        .unwrap();
    assert!(
        !completed.load(Ordering::SeqCst),
        "the held delete never completed"
    );
    drop(signal);
}
