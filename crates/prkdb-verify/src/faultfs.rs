//! In-memory `Vfs` that separates written from synced state, so `power_loss`
//! can discard everything not yet made durable (spec §7 Phase 1).

use parking_lot::Mutex;
use prkdb_core::vfs::{OpenMode, Vfs, VfsFile};
use rand::Rng;
use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Default, Clone)]
struct Content {
    written: Vec<u8>,
    synced: Vec<u8>,
}

#[derive(Default)]
struct State {
    /// inode -> content
    inodes: BTreeMap<u64, Content>,
    /// live directory entries: path -> inode
    live: BTreeMap<PathBuf, u64>,
    /// durable directory entries per directory (as of last sync_dir)
    durable: BTreeMap<PathBuf, BTreeMap<PathBuf, u64>>,
    dirs: BTreeSet<PathBuf>,
    next_inode: u64,
}

#[derive(Clone, Default)]
pub struct FaultFs {
    state: Arc<Mutex<State>>,
}

struct FaultFile {
    state: Arc<Mutex<State>>,
    inode: u64,
    mode: OpenMode,
}

fn parent(p: &Path) -> PathBuf {
    p.parent().map(Path::to_path_buf).unwrap_or_default()
}

fn not_found(p: &Path) -> io::Error {
    io::Error::new(io::ErrorKind::NotFound, p.display().to_string())
}

fn read_only(p: &Path) -> io::Error {
    io::Error::new(
        io::ErrorKind::PermissionDenied,
        format!("write on read-only handle: {}", p.display()),
    )
}

impl FaultFs {
    pub fn new() -> Self {
        Self::default()
    }

    /// Simulate power loss: unsynced bytes and unsynced directory entries vanish.
    /// With `tear`, a random prefix of the unsynced tail of each file survives.
    pub fn power_loss(&self, rng: &mut impl Rng, tear: bool) {
        let mut s = self.state.lock();
        let mut live = BTreeMap::new();
        for entries in s.durable.values() {
            live.extend(entries.iter().map(|(p, i)| (p.clone(), *i)));
        }
        s.live = live;
        for c in s.inodes.values_mut() {
            let mut next = c.synced.clone();
            if tear && c.written.len() > c.synced.len() && c.written.starts_with(&c.synced) {
                let extra = rng.gen_range(0..=c.written.len() - c.synced.len());
                next.extend_from_slice(&c.written[c.synced.len()..c.synced.len() + extra]);
            }
            c.written = next;
        }
    }
}

impl VfsFile for FaultFile {
    fn write_at(&self, offset: u64, buf: &[u8]) -> io::Result<()> {
        if self.mode == OpenMode::Read {
            return Err(read_only(Path::new("<fault-fs handle>")));
        }
        let mut s = self.state.lock();
        let c = s.inodes.get_mut(&self.inode).expect("inode");
        let end = offset as usize + buf.len();
        if c.written.len() < end {
            c.written.resize(end, 0);
        }
        c.written[offset as usize..end].copy_from_slice(buf);
        Ok(())
    }
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        let s = self.state.lock();
        let c = &s.inodes[&self.inode];
        let start = (offset as usize).min(c.written.len());
        let n = buf.len().min(c.written.len() - start);
        buf[..n].copy_from_slice(&c.written[start..start + n]);
        Ok(n)
    }
    fn set_len(&self, len: u64) -> io::Result<()> {
        if self.mode == OpenMode::Read {
            return Err(read_only(Path::new("<fault-fs handle>")));
        }
        self.state
            .lock()
            .inodes
            .get_mut(&self.inode)
            .unwrap()
            .written
            .resize(len as usize, 0);
        Ok(())
    }
    fn len(&self) -> io::Result<u64> {
        Ok(self.state.lock().inodes[&self.inode].written.len() as u64)
    }
    fn sync_data(&self) -> io::Result<()> {
        let mut s = self.state.lock();
        let c = s.inodes.get_mut(&self.inode).unwrap();
        c.synced = c.written.clone();
        Ok(())
    }
}

impl Vfs for FaultFs {
    fn open(&self, path: &Path, mode: OpenMode) -> io::Result<Arc<dyn VfsFile>> {
        let inode = *self
            .state
            .lock()
            .live
            .get(path)
            .ok_or_else(|| not_found(path))?;
        Ok(Arc::new(FaultFile {
            state: self.state.clone(),
            inode,
            mode,
        }))
    }
    fn create(&self, path: &Path) -> io::Result<Arc<dyn VfsFile>> {
        let mut s = self.state.lock();
        if !s.dirs.contains(&parent(path)) {
            return Err(not_found(&parent(path)));
        }
        let inode = s.next_inode;
        s.next_inode += 1;
        s.inodes.insert(inode, Content::default());
        s.live.insert(path.to_path_buf(), inode);
        Ok(Arc::new(FaultFile {
            state: self.state.clone(),
            inode,
            mode: OpenMode::ReadWrite,
        }))
    }
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        let mut s = self.state.lock();
        let inode = s.live.remove(from).ok_or_else(|| not_found(from))?;
        s.live.insert(to.to_path_buf(), inode);
        Ok(())
    }
    fn remove(&self, path: &Path) -> io::Result<()> {
        self.state
            .lock()
            .live
            .remove(path)
            .map(|_| ())
            .ok_or_else(|| not_found(path))
    }
    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        let mut s = self.state.lock();
        for a in path.ancestors() {
            s.dirs.insert(a.to_path_buf());
        }
        Ok(())
    }
    fn read_dir(&self, path: &Path) -> io::Result<Vec<PathBuf>> {
        let s = self.state.lock();
        let mut entries: BTreeSet<PathBuf> = s
            .live
            .keys()
            .filter(|p| parent(p) == path)
            .cloned()
            .collect();
        entries.extend(
            s.dirs
                .iter()
                .filter(|d| parent(d) == path && *d != path)
                .cloned(),
        );
        Ok(entries.into_iter().collect())
    }
    fn exists(&self, path: &Path) -> io::Result<bool> {
        let s = self.state.lock();
        Ok(s.live.contains_key(path) || s.dirs.contains(path))
    }
    fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        let mut s = self.state.lock();
        let entries: BTreeMap<PathBuf, u64> = s
            .live
            .iter()
            .filter(|(p, _)| parent(p) == dir)
            .map(|(p, i)| (p.clone(), *i))
            .collect();
        s.durable.insert(dir.to_path_buf(), entries);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;
    use rand_chacha::ChaCha8Rng;

    #[test]
    fn conformance() {
        prkdb_core::vfs::conformance::run(&FaultFs::new(), Path::new("/r"));
    }

    #[test]
    fn power_loss_drops_unsynced_bytes() {
        let fs = FaultFs::new();
        fs.create_dir_all(Path::new("/d")).unwrap();
        let f = fs.create(Path::new("/d/a")).unwrap();
        fs.sync_dir(Path::new("/d")).unwrap();
        f.write_at(0, b"durable").unwrap();
        f.sync_data().unwrap();
        f.write_at(7, b"-lost").unwrap();
        fs.power_loss(&mut ChaCha8Rng::seed_from_u64(1), false);
        assert_eq!(
            fs.open(Path::new("/d/a"), OpenMode::Read)
                .unwrap()
                .len()
                .unwrap(),
            7
        );
    }

    #[test]
    fn power_loss_drops_unsynced_directory_entries() {
        let fs = FaultFs::new();
        fs.create_dir_all(Path::new("/d")).unwrap();
        let f = fs.create(Path::new("/d/new")).unwrap();
        f.write_at(0, b"x").unwrap();
        f.sync_data().unwrap(); // data synced, but directory entry never was
        fs.power_loss(&mut ChaCha8Rng::seed_from_u64(1), false);
        assert!(!fs.exists(Path::new("/d/new")).unwrap());
    }

    #[test]
    fn torn_write_keeps_a_prefix() {
        let fs = FaultFs::new();
        fs.create_dir_all(Path::new("/d")).unwrap();
        let f = fs.create(Path::new("/d/a")).unwrap();
        fs.sync_dir(Path::new("/d")).unwrap();
        f.write_at(0, b"0123456789").unwrap();
        fs.power_loss(&mut ChaCha8Rng::seed_from_u64(7), true);
        let len = fs
            .open(Path::new("/d/a"), OpenMode::Read)
            .unwrap()
            .len()
            .unwrap();
        assert!(len <= 10);
    }

    #[test]
    fn read_only_handle_rejects_writes() {
        let fs = FaultFs::new();
        fs.create_dir_all(Path::new("/d")).unwrap();
        let f = fs.create(Path::new("/d/a")).unwrap();
        f.write_at(0, b"x").unwrap();
        fs.sync_dir(Path::new("/d")).unwrap();
        f.sync_data().unwrap();
        let ro = fs.open(Path::new("/d/a"), OpenMode::Read).unwrap();
        assert!(ro.write_at(0, b"y").is_err());
        assert!(ro.set_len(0).is_err());
    }
}
