//! Synchronous filesystem seam (spec §7 Phase 1). Production uses `StdVfs`; the
//! harness uses a fault-injecting implementation to model power loss. The WAL is
//! routed through this trait in Phase 2a.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

mod std_vfs;
pub use std_vfs::StdVfs;

/// Access mode for [`Vfs::open`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenMode {
    /// Read-only. Any write on the returned handle must fail.
    Read,
    /// Read and write.
    ReadWrite,
}

pub trait VfsFile: Send + Sync {
    /// Writes are not durable until `sync_data` is called.
    fn write_at(&self, offset: u64, buf: &[u8]) -> io::Result<()>;
    /// Reads may observe writes that have not yet been synced.
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize>;
    /// Not durable until `sync_data` is called.
    fn set_len(&self, len: u64) -> io::Result<()>;
    fn len(&self) -> io::Result<u64>;
    fn is_empty(&self) -> io::Result<bool> {
        Ok(self.len()? == 0)
    }
    /// Durably persists file data written so far, plus any metadata needed to read
    /// it back (e.g. the length after `set_len`): `fdatasync` on Linux,
    /// `F_FULLFSYNC` on macOS (both via `std`'s `File::sync_data`).
    fn sync_data(&self) -> io::Result<()>;
}

pub trait Vfs: Send + Sync {
    /// Opens an existing file. A `Read`-mode handle must reject writes.
    fn open(&self, path: &Path, mode: OpenMode) -> io::Result<Arc<dyn VfsFile>>;
    /// Create or truncate, for read-write access. The new directory entry is not
    /// durable until `sync_dir` is called on the parent directory.
    fn create(&self, path: &Path) -> io::Result<Arc<dyn VfsFile>>;
    /// Not durable until `sync_dir` is called on the affected parent directory(ies).
    /// A rename across directories requires syncing both the source and
    /// destination parents.
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()>;
    /// Not durable until `sync_dir` is called on the parent directory.
    fn remove(&self, path: &Path) -> io::Result<()>;
    /// Not durable until `sync_dir` is called on the parent (and any newly
    /// created ancestor directories).
    fn create_dir_all(&self, path: &Path) -> io::Result<()>;
    /// Returns the files and subdirectories directly under `path`, sorted.
    fn read_dir(&self, path: &Path) -> io::Result<Vec<PathBuf>>;
    fn exists(&self, path: &Path) -> io::Result<bool>;
    /// Durably persists directory entry changes (creates, renames, removes) in
    /// `dir`. Best-effort no-op on non-Unix platforms.
    fn sync_dir(&self, dir: &Path) -> io::Result<()>;
    /// Takes an exclusive advisory lock on `path`, creating the file if it is missing
    /// (its parent must exist), without blocking. The lock excludes every other
    /// `lock_exclusive` on the same path, from this process or another, until the
    /// returned guard drops or the holding process dies.
    ///
    /// A lock held elsewhere is `Err` with [`io::ErrorKind::WouldBlock`]. The file's
    /// contents are the implementation's business (`StdVfs` writes the holder's pid).
    ///
    /// For `StdVfs` this holds across processes only on a local filesystem. On Linux NFS
    /// `flock` is emulated with per-process `fcntl` locks, and SMB/CIFS semantics vary
    /// with the server and mount options, so another process may be let in. Within one
    /// process `StdVfs` still refuses a second lock, through a process-wide registry.
    fn lock_exclusive(&self, path: &Path) -> io::Result<Box<dyn LockGuard>>;
}

/// Holds a [`Vfs::lock_exclusive`] lock; dropping it releases the lock.
pub trait LockGuard: Send + Sync {}

/// Creates `dir` and every missing ancestor, one level at a time from the outermost, and
/// syncs the parent of each directory it creates, so all the new directory entries are
/// durable when this returns (STO-15). `create_dir_all` followed by a sync of `dir`'s
/// parent alone leaves the entries of the outer new directories unsynced: a power cut
/// can remove them, and everything written under them. Creates and syncs nothing when
/// `dir` exists. For a relative `dir` the outermost new directory is an entry in the
/// working directory, which is synced as `.`.
pub fn create_dir_all_durable(vfs: &dyn Vfs, dir: &Path) -> io::Result<()> {
    let mut missing = Vec::new();
    let mut current = Some(dir);
    while let Some(d) = current.filter(|d| !d.as_os_str().is_empty()) {
        if vfs.exists(d)? {
            break;
        }
        missing.push(d);
        current = d.parent();
    }
    for created in missing.into_iter().rev() {
        vfs.create_dir_all(created)?;
        if let Some(parent) = created.parent() {
            // `Path::new("mydb").parent()` is `Some("")`: the working directory.
            let parent = if parent.as_os_str().is_empty() {
                Path::new(".")
            } else {
                parent
            };
            vfs.sync_dir(parent)?;
        }
    }
    Ok(())
}

/// Shared conformance tests; every `Vfs` implementation must pass them.
///
/// The crate denies `clippy::unwrap_used` outside `cfg(test)`, but this module is
/// also compiled under `feature = "vfs-conformance"` (for reuse by the fault-injection
/// harness crate) independent of `cfg(test)`, so it needs an explicit allow below.
#[cfg(any(test, feature = "vfs-conformance"))]
#[allow(clippy::unwrap_used)]
pub mod conformance {
    use super::*;

    pub fn run(vfs: &dyn Vfs, root: &Path) {
        let dir = root.join("d");
        vfs.create_dir_all(&dir).unwrap();
        let p = dir.join("a.log");
        let f = vfs.create(&p).unwrap();
        f.write_at(0, b"hello").unwrap();
        f.write_at(5, b" world").unwrap();
        f.sync_data().unwrap();
        vfs.sync_dir(&dir).unwrap();

        let mut buf = [0u8; 11];
        let ro = vfs.open(&p, OpenMode::Read).unwrap();
        assert_eq!(ro.read_at(0, &mut buf).unwrap(), 11);
        assert_eq!(&buf, b"hello world");
        // A read-only handle must not allow writes.
        assert!(ro.write_at(0, b"x").is_err());

        let rw = vfs.open(&p, OpenMode::ReadWrite).unwrap();
        rw.set_len(5).unwrap();
        assert_eq!(rw.len().unwrap(), 5);

        // Short read at EOF: fewer bytes than the buffer size, not an error.
        let mut short = [0u8; 11];
        assert_eq!(rw.read_at(0, &mut short).unwrap(), 5);

        let q = dir.join("b.log");
        vfs.rename(&p, &q).unwrap();
        assert!(!vfs.exists(&p).unwrap() && vfs.exists(&q).unwrap());
        assert_eq!(vfs.read_dir(&dir).unwrap(), vec![q.clone()]);
        vfs.remove(&q).unwrap();
        assert!(!vfs.exists(&q).unwrap());

        // An exclusive lock excludes a second one until it is dropped.
        let lock = dir.join("LOCK");
        let held = vfs.lock_exclusive(&lock).unwrap();
        assert!(vfs.exists(&lock).unwrap(), "the lock file is created");
        let refused = vfs.lock_exclusive(&lock).err().expect("held elsewhere");
        assert_eq!(refused.kind(), io::ErrorKind::WouldBlock);
        drop(held);
        drop(vfs.lock_exclusive(&lock).unwrap());
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn std_vfs_conformance() {
        let tmp = tempfile::tempdir().unwrap();
        super::conformance::run(&super::StdVfs, tmp.path());
    }
}
