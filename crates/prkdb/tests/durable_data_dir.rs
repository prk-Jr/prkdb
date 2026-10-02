//! STO-16 through the adapter: opening a data directory under several missing directories
//! makes every one of them durable before the first acknowledged write.
//!
//! `WalStorageAdapter`'s open created the directory with `create_dir_all` and synced no
//! parent at all before writing `FORMAT`, `LOCK` and the first segment into it (the
//! format check then synced only the immediate parent). A directory created by the open
//! survives a power cut only if its parent was synced after it was created; the
//! recording `Vfs` below checks exactly that.

use prkdb::storage::config::StorageConfig;
use prkdb::storage::WalStorageAdapter;
use prkdb_core::vfs::{LockGuard, OpenMode, StdVfs, Vfs, VfsFile};
use prkdb_core::wal::{SyncMode, WalConfig};
use prkdb_types::storage::StorageAdapter;
use std::collections::BTreeSet;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct Dirs {
    created: Vec<PathBuf>,
    durable: BTreeSet<PathBuf>,
}

#[derive(Clone, Default)]
struct RecordingVfs(Arc<Mutex<Dirs>>);

impl Vfs for RecordingVfs {
    fn open(&self, p: &Path, m: OpenMode) -> io::Result<Arc<dyn VfsFile>> {
        StdVfs.open(p, m)
    }
    fn create(&self, p: &Path) -> io::Result<Arc<dyn VfsFile>> {
        StdVfs.create(p)
    }
    fn rename(&self, a: &Path, b: &Path) -> io::Result<()> {
        StdVfs.rename(a, b)
    }
    fn remove(&self, p: &Path) -> io::Result<()> {
        StdVfs.remove(p)
    }
    fn create_dir_all(&self, p: &Path) -> io::Result<()> {
        let mut missing = Vec::new();
        let mut current = Some(p);
        while let Some(d) = current.filter(|d| !d.exists()) {
            missing.push(d.to_path_buf());
            current = d.parent();
        }
        StdVfs.create_dir_all(p)?;
        let mut dirs = self.0.lock().unwrap();
        dirs.created.extend(missing.into_iter().rev());
        Ok(())
    }
    fn read_dir(&self, p: &Path) -> io::Result<Vec<PathBuf>> {
        StdVfs.read_dir(p)
    }
    fn exists(&self, p: &Path) -> io::Result<bool> {
        StdVfs.exists(p)
    }
    fn sync_dir(&self, d: &Path) -> io::Result<()> {
        StdVfs.sync_dir(d)?;
        let mut dirs = self.0.lock().unwrap();
        let children: Vec<PathBuf> = dirs
            .created
            .iter()
            .filter(|c| c.parent() == Some(d))
            .cloned()
            .collect();
        dirs.durable.extend(children);
        Ok(())
    }
    fn lock_exclusive(&self, p: &Path) -> io::Result<Box<dyn LockGuard>> {
        StdVfs.lock_exclusive(p)
    }
}

#[tokio::test]
async fn every_missing_ancestor_of_the_data_directory_is_durable() {
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("a").join("b").join("wal");
    let vfs = RecordingVfs::default();
    let config = StorageConfig {
        wal: WalConfig {
            log_dir: dir.clone(),
            sync_mode: SyncMode::Durable,
            ..WalConfig::test_config()
        },
        ..StorageConfig::new(dir.clone())
    };
    let adapter = WalStorageAdapter::open_with_vfs(config, Arc::new(vfs.clone())).unwrap();
    adapter.put(b"k", b"acked").await.unwrap();

    let dirs = vfs.0.lock().unwrap();
    let expected = [
        root.path().join("a"),
        root.path().join("a/b"),
        root.path().join("a/b/wal"),
    ];
    for d in &expected {
        assert!(
            dirs.created.contains(d),
            "{d:?} was created: {:?}",
            dirs.created
        );
        assert!(
            dirs.durable.contains(d),
            "a power cut would remove {d:?}, and the acknowledged write with it"
        );
    }
}

#[tokio::test]
async fn every_missing_ancestor_of_a_durable_stream_is_durable() {
    use prkdb::stream_log::{Record, StreamConfig, StreamLog};
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("a/b/stream");
    let vfs = RecordingVfs::default();
    let mut config = StreamConfig::new(&dir);
    config.wal.sync_mode = SyncMode::Durable;
    let stream = StreamLog::open_with_vfs(Arc::new(vfs.clone()), config)
        .await
        .unwrap();
    stream
        .append(vec![Record {
            key: None,
            value: b"acked".to_vec(),
            headers: vec![],
        }])
        .await
        .unwrap();
    let dirs = vfs.0.lock().unwrap();
    for created in &dirs.created {
        assert!(
            dirs.durable.contains(created),
            "Durable stream ack depends on an unsynced directory: {created:?}"
        );
    }
    drop(dirs);
    stream.close().unwrap();
}
