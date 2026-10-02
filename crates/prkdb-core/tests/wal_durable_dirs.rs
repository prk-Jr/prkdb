//! STO-15: opening a log under several missing directories makes every one of them
//! durable before the first acknowledged write.
//!
//! `Wal::open` created the directory with `create_dir_all` and synced only its immediate
//! parent. With `/existing/a/b/wal` and `a/` missing, the entries for `a/` (in
//! `/existing`) and `b/` (in `a/`) were never synced: a power cut could drop them with
//! everything under them, and the reopen created an empty database where a Durable
//! write had been acknowledged (reproduced externally on FaultFs).
//!
//! The recording `Vfs` below models exactly that rule: a directory created by the open
//! survives a power cut only if its parent was synced after it was created.

use prkdb_core::vfs::{LockGuard, OpenMode, StdVfs, Vfs, VfsFile};
use prkdb_core::wal::{FrontRelease, SyncMode, Wal, WalOptions};
use std::collections::BTreeSet;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Default)]
struct Dirs {
    /// Directories created through this `Vfs`, in creation order.
    created: Vec<PathBuf>,
    /// Those of them whose parent was synced after they were created.
    durable: BTreeSet<PathBuf>,
}

#[derive(Clone, Default)]
struct RecordingVfs(Arc<Mutex<Dirs>>);

impl RecordingVfs {
    /// The created directories a power cut would remove.
    fn not_durable(&self) -> Vec<PathBuf> {
        let dirs = self.0.lock().unwrap();
        dirs.created
            .iter()
            .filter(|d| !dirs.durable.contains(*d))
            .cloned()
            .collect()
    }
}

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
        self.0
            .lock()
            .unwrap()
            .created
            .extend(missing.into_iter().rev());
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

#[test]
fn every_missing_ancestor_is_durable_before_the_first_durable_ack() {
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("a").join("b").join("wal");
    let vfs = RecordingVfs::default();
    let opts = WalOptions {
        sync_mode: SyncMode::Durable,
        sync_interval: Duration::from_secs(3600),
        segment_bytes: 1 << 20,
        max_batch_bytes: 1 << 20,
        max_queued_bytes: 8 << 20,
        front_release: FrontRelease::ElidedOnly,
    };
    let (wal, _) = Wal::open(Arc::new(vfs.clone()), &dir, opts, 1, &mut |_, _, _| Ok(())).unwrap();
    wal.append_blocking(b"acked".to_vec(), None).unwrap();

    assert_eq!(
        vfs.0.lock().unwrap().created,
        vec![
            root.path().join("a"),
            root.path().join("a/b"),
            root.path().join("a/b/wal")
        ]
    );
    assert_eq!(
        vfs.not_durable(),
        Vec::<PathBuf>::new(),
        "a power cut would remove these, and the acknowledged write with them"
    );
    wal.close().unwrap();
}

#[test]
fn create_dir_all_durable_syncs_the_parent_of_every_directory_it_creates() {
    let root = tempfile::tempdir().unwrap();
    std::fs::create_dir(root.path().join("x")).unwrap();
    let vfs = RecordingVfs::default();
    prkdb_core::vfs::create_dir_all_durable(&vfs, &root.path().join("x/y/z")).unwrap();
    assert_eq!(
        vfs.0.lock().unwrap().created,
        vec![root.path().join("x/y"), root.path().join("x/y/z")]
    );
    assert_eq!(vfs.not_durable(), Vec::<PathBuf>::new());

    // Nothing missing: nothing created, nothing synced.
    let vfs = RecordingVfs::default();
    prkdb_core::vfs::create_dir_all_durable(&vfs, &root.path().join("x/y/z")).unwrap();
    assert!(vfs.0.lock().unwrap().created.is_empty());
}
