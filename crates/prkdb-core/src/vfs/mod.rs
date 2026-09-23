//! Synchronous filesystem seam (spec §7 Phase 1). Production uses `StdVfs`; the
//! harness uses a fault-injecting implementation to model power loss. The WAL is
//! routed through this trait in Phase 2a.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

mod std_vfs;
pub use std_vfs::StdVfs;

pub trait VfsFile: Send + Sync {
    fn write_at(&self, offset: u64, buf: &[u8]) -> io::Result<()>;
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize>;
    fn set_len(&self, len: u64) -> io::Result<()>;
    fn len(&self) -> io::Result<u64>;
    fn is_empty(&self) -> io::Result<bool> {
        Ok(self.len()? == 0)
    }
    /// Durably persist file contents written so far (fdatasync).
    fn sync_data(&self) -> io::Result<()>;
}

pub trait Vfs: Send + Sync {
    fn open(&self, path: &Path) -> io::Result<Arc<dyn VfsFile>>;
    /// Create or truncate. The new directory entry is not durable until `sync_dir`.
    fn create(&self, path: &Path) -> io::Result<Arc<dyn VfsFile>>;
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()>;
    fn remove(&self, path: &Path) -> io::Result<()>;
    fn create_dir_all(&self, path: &Path) -> io::Result<()>;
    fn read_dir(&self, path: &Path) -> io::Result<Vec<PathBuf>>;
    fn exists(&self, path: &Path) -> bool;
    /// Durably persist directory entries (creates, renames, removes) in `dir`.
    fn sync_dir(&self, dir: &Path) -> io::Result<()>;
}

/// Shared conformance tests; every `Vfs` implementation must pass them.
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
        assert_eq!(vfs.open(&p).unwrap().read_at(0, &mut buf).unwrap(), 11);
        assert_eq!(&buf, b"hello world");
        f.set_len(5).unwrap();
        assert_eq!(f.len().unwrap(), 5);
        let q = dir.join("b.log");
        vfs.rename(&p, &q).unwrap();
        assert!(!vfs.exists(&p) && vfs.exists(&q));
        assert_eq!(vfs.read_dir(&dir).unwrap(), vec![q.clone()]);
        vfs.remove(&q).unwrap();
        assert!(!vfs.exists(&q));
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
