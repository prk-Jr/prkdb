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
