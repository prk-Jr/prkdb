//! STO-10 (Task 2.11b): the data-directory lock.
//!
//! A data directory is opened by one process at a time: `open` takes an exclusive advisory
//! lock on `<dir>/LOCK` before anything else touches the directory and holds it for the
//! adapter's lifetime. A second open, from this process or another, is refused with
//! `StorageError::Locked`; the lock goes away when the adapter drops or its process dies.

use prkdb::storage::config::StorageConfig;
use prkdb::storage::{CollectionPartitionedAdapter, WalStorageAdapter};
use prkdb_core::wal::WalConfig;
use prkdb_types::error::StorageError;
use prkdb_types::storage::StorageAdapter;
use prkdb_verify::faultfs::{FaultFs, Tear};
use rand::SeedableRng;
use std::path::Path;
use std::sync::Arc;

fn config(dir: &Path) -> WalConfig {
    WalConfig {
        log_dir: dir.to_path_buf(),
        ..WalConfig::test_config()
    }
}

fn assert_locked(result: Result<WalStorageAdapter, StorageError>, dir: &Path) -> String {
    match result {
        Err(StorageError::Locked(msg)) => {
            assert!(msg.contains(&dir.display().to_string()), "{msg}");
            assert!(msg.contains("in use by another process"), "{msg}");
            msg
        }
        Err(e) => panic!("expected Locked, got {e:?}"),
        Ok(_) => panic!("a second open of {} succeeded", dir.display()),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_second_open_in_process_is_locked() {
    let tmp = tempfile::tempdir().unwrap();
    let first = WalStorageAdapter::new(config(tmp.path())).unwrap();
    first.put(b"k", b"v").await.unwrap();

    let msg = assert_locked(WalStorageAdapter::new(config(tmp.path())), tmp.path());
    // Windows refuses reads of a locked file, so there the pid is "unknown".
    assert!(
        !cfg!(unix) || msg.contains(&format!("pid {}", std::process::id())),
        "the holder's pid is named: {msg}"
    );
    // The refused open touched nothing: the holder still works.
    assert_eq!(first.get(b"k").await.unwrap().as_deref(), Some(&b"v"[..]));
}

#[tokio::test(flavor = "multi_thread")]
async fn the_lock_is_released_on_drop() {
    let tmp = tempfile::tempdir().unwrap();
    let first = WalStorageAdapter::new(config(tmp.path())).unwrap();
    first.put(b"k", b"v").await.unwrap();
    // A clone shares the open; the lock lives until the last one drops.
    let clone = first.clone();
    drop(first);
    assert_locked(WalStorageAdapter::new(config(tmp.path())), tmp.path());
    drop(clone);

    let reopened = WalStorageAdapter::new(config(tmp.path())).unwrap();
    assert_eq!(
        reopened.get(b"k").await.unwrap().as_deref(),
        Some(&b"v"[..])
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_refused_format_1_open_does_not_keep_the_lock() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::create_dir(tmp.path().join("mmap_segment_0")).unwrap();
    for _ in 0..2 {
        let err = WalStorageAdapter::new(config(tmp.path())).err().unwrap();
        assert!(
            matches!(err, StorageError::UnsupportedFormat(_)),
            "refused as format 1 each time, never as Locked: {err:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_second_collection_partitioned_open_is_locked() {
    let tmp = tempfile::tempdir().unwrap();
    let _first = CollectionPartitionedAdapter::new(config(tmp.path())).unwrap();
    let second = CollectionPartitionedAdapter::new(config(tmp.path()));
    assert!(
        matches!(second, Err(StorageError::Locked(_))),
        "{:?}",
        second.err()
    );
}

/// Multi-raft: every partition data directory and the `meta` directory lock themselves.
#[tokio::test(flavor = "multi_thread")]
async fn every_multi_raft_data_directory_is_locked_while_open() {
    let root = tempfile::tempdir().unwrap();
    let db = prkdb::PrkDb::new_multi_raft(
        2,
        prkdb::raft::ClusterConfig::default(),
        root.path().to_path_buf(),
    )
    .unwrap();
    for member in ["meta", "partition_0", "partition_1"] {
        let dir = root.path().join(member);
        assert_locked(WalStorageAdapter::new(config(&dir)), &dir);
    }
    drop(db);
    for member in ["meta", "partition_0", "partition_1"] {
        drop(WalStorageAdapter::new(config(&root.path().join(member))).unwrap());
    }
}

/// `FaultFs` models the lock in memory: held while open, released on drop and by power
/// loss (the process that held it is gone).
#[tokio::test(flavor = "multi_thread")]
async fn faultfs_models_the_lock() {
    let fs = FaultFs::new();
    fs.mkdir_durable(Path::new("/db")).unwrap();
    let cfg = || StorageConfig {
        wal: config(Path::new("/db/wal")),
        ..StorageConfig::default()
    };
    let open = || WalStorageAdapter::open_with_vfs(cfg(), Arc::new(fs.clone()));

    let first = open().unwrap();
    assert_locked(open(), Path::new("/db/wal"));
    drop(first);

    let second = open().unwrap();
    fs.power_loss(&mut rand_chacha::ChaCha8Rng::seed_from_u64(7), Tear::None);
    let third = open().expect("power loss releases the lock");
    // The stale adapter's drop must not release the new holder's lock.
    drop(second);
    assert_locked(open(), Path::new("/db/wal"));
    drop(third);
}

#[cfg(unix)]
mod child_process {
    use super::*;
    use std::io::{BufRead, BufReader};
    use std::process::{Child, Command, Stdio};
    use std::time::Duration;

    struct KillOnDrop(Child);

    impl Drop for KillOnDrop {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    fn crash_child(dir: &Path, n: u32) -> Command {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_crash_child"));
        cmd.arg(dir.to_str().unwrap()).arg(n.to_string());
        cmd
    }

    /// Starts `crash_child`, which opens `dir`, writes one key, prints `ACK 0` and then
    /// sleeps holding the lock; returns once `ACK 0` is read.
    fn holding_child(dir: &Path) -> KillOnDrop {
        let mut child = KillOnDrop(
            crash_child(dir, 1)
                .stdout(Stdio::piped())
                .spawn()
                .expect("spawn crash_child"),
        );
        let stdout = child.0.stdout.take().unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                if tx.send(line).is_err() {
                    break;
                }
            }
        });
        loop {
            match rx.recv_timeout(Duration::from_secs(60)) {
                Ok(Ok(line)) if line == "ACK 0" => return child,
                Ok(Ok(_)) => continue,
                other => panic!(
                    "crash_child never acknowledged: {other:?} (status {:?})",
                    child.0.try_wait()
                ),
            }
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_second_open_from_a_child_process_is_locked() {
        let tmp = tempfile::tempdir().unwrap();
        let _held = WalStorageAdapter::new(config(tmp.path())).unwrap();

        let out = crash_child(tmp.path(), 1)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .expect("run crash_child");
        assert!(!out.status.success(), "{out:?}");
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            stderr.contains("in use by another process")
                && stderr.contains(&format!("pid {}", std::process::id())),
            "{stderr}"
        );
        assert!(
            !String::from_utf8_lossy(&out.stdout).contains("ACK"),
            "the child wrote nothing"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_lock_is_released_when_the_holder_is_sigkilled() {
        let tmp = tempfile::tempdir().unwrap();
        let mut child = holding_child(tmp.path());

        let msg = assert_locked(WalStorageAdapter::new(config(tmp.path())), tmp.path());
        assert!(msg.contains(&format!("pid {}", child.0.id())), "{msg}");

        child.0.kill().expect("SIGKILL crash_child");
        child.0.wait().expect("reap crash_child");

        let db = WalStorageAdapter::new(config(tmp.path()))
            .expect("the OS released the dead holder's lock");
        assert_eq!(db.get(b"k0").await.unwrap().as_deref(), Some(&b"v"[..]));
    }
}
