//! STO-16: opening a log under several missing directories makes every one of them
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
    fail_sync_once: Option<PathBuf>,
}

/// Records directory creation and syncs over `StdVfs`. With a `base`, relative paths are
/// resolved against it for the real operations (standing in for the working directory)
/// but recorded as given, so a test can open a relative data directory without changing
/// the process's working directory.
#[derive(Clone, Default)]
struct RecordingVfs {
    dirs: Arc<Mutex<Dirs>>,
    base: Option<PathBuf>,
}

/// The directory whose sync makes `dir`'s entry durable: its parent, `.` for a
/// single-component relative path (the VFS working-directory anchor).
fn parent_of(dir: &Path) -> &Path {
    dir.parent().unwrap_or(dir)
}

impl RecordingVfs {
    fn rooted_at(base: &Path) -> Self {
        Self {
            base: Some(base.to_path_buf()),
            ..Self::default()
        }
    }

    fn real(&self, p: &Path) -> PathBuf {
        match &self.base {
            Some(base) if p.is_relative() => base.join(p),
            _ => p.to_path_buf(),
        }
    }

    fn created(&self) -> Vec<PathBuf> {
        self.dirs.lock().unwrap().created.clone()
    }

    /// The created directories a power cut would remove.
    fn not_durable(&self) -> Vec<PathBuf> {
        let dirs = self.dirs.lock().unwrap();
        dirs.created
            .iter()
            .filter(|d| !dirs.durable.contains(*d))
            .cloned()
            .collect()
    }
}

impl Vfs for RecordingVfs {
    fn open(&self, p: &Path, m: OpenMode) -> io::Result<Arc<dyn VfsFile>> {
        StdVfs.open(&self.real(p), m)
    }
    fn create(&self, p: &Path) -> io::Result<Arc<dyn VfsFile>> {
        StdVfs.create(&self.real(p))
    }
    fn rename(&self, a: &Path, b: &Path) -> io::Result<()> {
        StdVfs.rename(&self.real(a), &self.real(b))
    }
    fn remove(&self, p: &Path) -> io::Result<()> {
        StdVfs.remove(&self.real(p))
    }
    fn create_dir_all(&self, p: &Path) -> io::Result<()> {
        let mut missing = Vec::new();
        let mut current = Some(p);
        while let Some(d) = current.filter(|d| !d.as_os_str().is_empty() && !self.real(d).exists())
        {
            missing.push(d.to_path_buf());
            current = d.parent();
        }
        StdVfs.create_dir_all(&self.real(p))?;
        self.dirs
            .lock()
            .unwrap()
            .created
            .extend(missing.into_iter().rev());
        Ok(())
    }
    fn read_dir(&self, p: &Path) -> io::Result<Vec<PathBuf>> {
        StdVfs.read_dir(&self.real(p))
    }
    fn exists(&self, p: &Path) -> io::Result<bool> {
        StdVfs.exists(&self.real(p))
    }
    fn sync_dir(&self, d: &Path) -> io::Result<()> {
        {
            let mut dirs = self.dirs.lock().unwrap();
            if dirs.fail_sync_once.as_deref() == Some(d) {
                dirs.fail_sync_once = None;
                return Err(io::Error::other("injected parent sync failure"));
            }
        }
        StdVfs.sync_dir(&self.real(d))?;
        let mut dirs = self.dirs.lock().unwrap();
        let children: Vec<PathBuf> = dirs
            .created
            .iter()
            .filter(|c| parent_of(c) == d)
            .cloned()
            .collect();
        dirs.durable.extend(children);
        Ok(())
    }
    fn lock_exclusive(&self, p: &Path) -> io::Result<Box<dyn LockGuard>> {
        StdVfs.lock_exclusive(&self.real(p))
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
        append_kind: prkdb_core::wal::frame::FrameKind::Batch,
        lsn_limit: None,
    };
    let (wal, _) = Wal::open(Arc::new(vfs.clone()), &dir, opts, 1, &mut |_, _, _| Ok(())).unwrap();
    wal.append_blocking(b"acked".to_vec(), None).unwrap();

    assert_eq!(
        vfs.created(),
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
        vfs.created(),
        vec![root.path().join("x/y"), root.path().join("x/y/z")]
    );
    assert_eq!(vfs.not_durable(), Vec::<PathBuf>::new());

    // Existing directories still receive ancestor barriers; no new entries are created.
    let vfs = RecordingVfs::default();
    prkdb_core::vfs::create_dir_all_durable(&vfs, &root.path().join("x/y/z")).unwrap();
    assert!(vfs.created().is_empty());
}

/// A relative data directory: the outermost directory created is an entry in the working
/// directory, which must be synced too (`.`), not skipped because its parent path is
/// empty. The empty path represents the VFS working-directory anchor.
#[test]
fn a_relative_path_syncs_the_working_directory_for_its_outermost_new_directory() {
    let cwd = tempfile::tempdir().unwrap();
    let vfs = RecordingVfs::rooted_at(cwd.path());
    prkdb_core::vfs::create_dir_all_durable(&vfs, Path::new("mydb/wal")).unwrap();
    assert_eq!(
        vfs.created(),
        vec![PathBuf::from("mydb"), PathBuf::from("mydb/wal")]
    );
    assert_eq!(vfs.not_durable(), Vec::<PathBuf>::new());
    assert!(cwd.path().join("mydb/wal").is_dir());
}

#[test]
fn retry_after_parent_sync_failure_makes_existing_ancestors_durable() {
    let root = tempfile::tempdir().unwrap();
    let vfs = RecordingVfs::default();
    vfs.dirs.lock().unwrap().fail_sync_once = Some(root.path().to_path_buf());
    let dir = root.path().join("a/b/wal");
    assert!(prkdb_core::vfs::create_dir_all_durable(&vfs, &dir).is_err());
    assert_eq!(vfs.not_durable(), vec![root.path().join("a")]);
    prkdb_core::vfs::create_dir_all_durable(&vfs, &dir).unwrap();
    assert!(
        vfs.not_durable().is_empty(),
        "retry acknowledged an undurable ancestor: {:?}",
        vfs.not_durable()
    );
}

#[test]
fn another_open_under_an_unsynced_existing_ancestor_cannot_ack_durable() {
    let root = tempfile::tempdir().unwrap();
    let vfs = RecordingVfs::default();
    // This is the intermediate state of another opener between mkdir and parent sync.
    vfs.create_dir_all(&root.path().join("a")).unwrap();
    let dir = root.path().join("a/other/wal");
    let opts = WalOptions::from_config(&prkdb_core::wal::WalConfig::default());
    let (wal, _) = Wal::open(Arc::new(vfs.clone()), &dir, opts, 1, &mut |_, _, _| Ok(())).unwrap();
    wal.append_blocking(b"durable".to_vec(), None).unwrap();
    assert!(
        vfs.not_durable().is_empty(),
        "Durable ack can be lost with an existing unsynced ancestor: {:?}",
        vfs.not_durable()
    );
    wal.close().unwrap();
}

#[cfg(unix)]
#[test]
fn std_vfs_empty_directory_sync_uses_the_working_directory_anchor() {
    // Path::parent for a relative single-component directory is the empty path.
    StdVfs.sync_dir(Path::new("")).unwrap();
}
