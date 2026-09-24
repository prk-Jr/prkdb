//! In-memory `Vfs` that separates written from synced state, so `power_loss`
//! can discard everything not yet made durable (spec §7 Phase 1).
//!
//! Durability model:
//! - A file's bytes are durable up to `sync_data`; anything written after
//!   that (or an unsynced shrink) can be lost, zeroed, garbled, or partially
//!   kept on power loss, in 512-byte sectors (see [`Tear`]).
//! - A directory entry (file or subdirectory) is durable only once
//!   `sync_dir` has been called on its parent *while the entry was live*, and
//!   only if the parent directory is itself durable. The filesystem root (a
//!   path with no parent) is the one exception: it's always durable, the same
//!   way a real WAL's `log_dir` is assumed to already exist on disk.
//! - A handle obtained before a `power_loss` call is stale afterward: every
//!   operation on it errors, instead of silently reading/writing whatever the
//!   inode now contains.

use parking_lot::Mutex;
use prkdb_core::vfs::{OpenMode, Vfs, VfsFile};
use rand::Rng;
use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Sector size used to grain unsynced, in-place overwrites of already-synced
/// data: each sector independently reverts to its old (synced) bytes or keeps
/// its new (unsynced) bytes on power loss, modeling a torn write to a block
/// that was previously durable.
const SECTOR: usize = 512;

/// How `power_loss` treats each file's unsynced tail (bytes written past the
/// last `sync_data` call, in an unsynced *growth*). Independent of this,
/// in-place overwrites of already-synced bytes are always torn per-sector
/// (see the module docs).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tear {
    /// The tail is dropped entirely: the file reverts to exactly its last
    /// synced length and content.
    None,
    /// A random prefix of the tail survives (its length picked uniformly in
    /// `0..=tail_len`).
    Prefix,
    /// The tail's length survives, but its bytes are zeroed.
    ZeroTail,
    /// The tail's length survives, but its bytes are randomized.
    Garbage,
}

impl Tear {
    /// Draws a `Tear` mode uniformly at random, for callers that want to
    /// fuzz the failure mode rather than pick one.
    pub fn random(rng: &mut impl Rng) -> Self {
        match rng.gen_range(0..4) {
            0 => Tear::None,
            1 => Tear::Prefix,
            2 => Tear::ZeroTail,
            _ => Tear::Garbage,
        }
    }
}

#[derive(Default, Clone)]
struct Content {
    written: Vec<u8>,
    synced: Vec<u8>,
    /// Sector indices (byte range `[i*SECTOR, (i+1)*SECTOR)`) touched by a
    /// `write_at`/`set_len` since the last `sync_data`.
    dirty_sectors: BTreeSet<u64>,
}

/// A live directory entry: either a file (by inode) or a subdirectory.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Entry {
    File(u64),
    Dir,
}

#[derive(Default)]
struct State {
    /// inode -> content
    inodes: BTreeMap<u64, Content>,
    /// Live directory entries (files and directories), by path.
    live: BTreeMap<PathBuf, Entry>,
    /// Snapshot of a directory's live children as of the last `sync_dir` call
    /// on it. A child (file or subdirectory) is durable only if it appears
    /// here *and* its parent directory is itself durable.
    durable_children: BTreeMap<PathBuf, BTreeMap<PathBuf, Entry>>,
    next_inode: u64,
    /// Bumped on every `power_loss`. Captured by each handle at open/create
    /// time so a handle that predates the last power loss can be rejected
    /// instead of silently operating on a different logical generation of
    /// the file.
    epoch: u64,
}

#[derive(Clone, Default)]
pub struct FaultFs {
    state: Arc<Mutex<State>>,
}

struct FaultFile {
    state: Arc<Mutex<State>>,
    inode: u64,
    mode: OpenMode,
    epoch: u64,
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

fn stale_handle() -> io::Error {
    io::Error::other("stale handle after power loss")
}

fn is_directory(p: &Path) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        format!("is a directory: {}", p.display()),
    )
}

fn mark_dirty(c: &mut Content, start: usize, end: usize) {
    if start >= end {
        return;
    }
    let first = (start / SECTOR) as u64;
    let last = ((end - 1) / SECTOR) as u64;
    for sector in first..=last {
        c.dirty_sectors.insert(sector);
    }
}

/// Computes the post-power-loss bytes for one file's `Content`.
fn tear_content(c: &Content, tear: Tear, rng: &mut impl Rng) -> Vec<u8> {
    let synced_len = c.synced.len();
    let written_len = c.written.len();

    // Step 1: the tail beyond `synced_len`. An unsynced *shrink* (or no
    // growth at all) always reverts fully to `synced`, regardless of `tear`:
    // none of the four modes describe how to handle a vanished truncation,
    // only how to handle an unsynced append.
    let mut result: Vec<u8> = if written_len > synced_len {
        match tear {
            Tear::None => c.synced.clone(),
            Tear::Prefix => {
                let mut v = c.synced.clone();
                let extra = rng.gen_range(0..=written_len - synced_len);
                v.extend_from_slice(&c.written[synced_len..synced_len + extra]);
                v
            }
            Tear::ZeroTail => {
                let mut v = c.written.clone();
                v[synced_len..].fill(0);
                v
            }
            Tear::Garbage => {
                let mut v = c.written.clone();
                for b in &mut v[synced_len..] {
                    *b = rng.gen();
                }
                v
            }
        }
    } else {
        c.synced.clone()
    };

    // Step 2: independently tear dirty sectors that overlap the
    // already-synced region (in-place overwrites of durable data) — this
    // applies regardless of `tear` and regardless of step 1's outcome, since
    // it models a different failure (a torn in-place update, not a torn
    // append).
    for &sector in &c.dirty_sectors {
        let start = sector as usize * SECTOR;
        if start >= synced_len || start >= written_len {
            // Outside the synced region (pure append, handled above), or
            // truncated away entirely: nothing new to reconsider here.
            continue;
        }
        let end = ((sector as usize + 1) * SECTOR)
            .min(synced_len)
            .min(written_len)
            .min(result.len());
        if start >= end {
            continue;
        }
        if rng.gen_bool(0.5) {
            result[start..end].copy_from_slice(&c.synced[start..end]); // reverts
        } else {
            result[start..end].copy_from_slice(&c.written[start..end]); // keeps
        }
    }

    result
}

impl FaultFs {
    pub fn new() -> Self {
        Self::default()
    }

    /// Simulates power loss: unsynced directory entries and unsynced file
    /// content vanish (or are torn, per `tear`); see the module docs for the
    /// exact durability rules. Any handle obtained before this call becomes
    /// stale and errors on every subsequent operation.
    pub fn power_loss(&self, rng: &mut impl Rng, tear: Tear) {
        let mut s = self.state.lock();
        s.epoch += 1;

        // Directory durability: BFS from the filesystem root(s) (paths with
        // no parent) down through `durable_children`.
        let mut live_dirs: BTreeSet<PathBuf> = BTreeSet::new();
        let mut frontier: Vec<PathBuf> = Vec::new();
        for (p, e) in s.live.iter() {
            if matches!(e, Entry::Dir) && p.parent().is_none() {
                live_dirs.insert(p.clone());
                frontier.push(p.clone());
            }
        }
        while let Some(d) = frontier.pop() {
            if let Some(children) = s.durable_children.get(&d) {
                for (child, entry) in children {
                    if matches!(entry, Entry::Dir) && live_dirs.insert(child.clone()) {
                        frontier.push(child.clone());
                    }
                }
            }
        }

        // File durability, given which directories survived.
        let mut new_live: BTreeMap<PathBuf, Entry> = BTreeMap::new();
        for d in &live_dirs {
            new_live.insert(d.clone(), Entry::Dir);
            if let Some(children) = s.durable_children.get(d) {
                for (child, entry) in children {
                    if let Entry::File(inode) = entry {
                        new_live.insert(child.clone(), Entry::File(*inode));
                    }
                }
            }
        }
        s.live = new_live;

        // Content tearing.
        let inodes: Vec<u64> = s.inodes.keys().copied().collect();
        for inode in inodes {
            let torn = tear_content(
                s.inodes.get(&inode).expect("inode of tracked content"),
                tear,
                rng,
            );
            let c = s.inodes.get_mut(&inode).expect("inode of tracked content");
            c.written = torn;
            c.dirty_sectors.clear();
        }
    }
}

impl FaultFile {
    fn check_live(&self) -> io::Result<()> {
        if self.state.lock().epoch != self.epoch {
            return Err(stale_handle());
        }
        Ok(())
    }

    fn check_writable(&self) -> io::Result<()> {
        self.check_live()?;
        if self.mode == OpenMode::Read {
            return Err(read_only(Path::new("<fault-fs handle>")));
        }
        Ok(())
    }
}

impl VfsFile for FaultFile {
    fn write_at(&self, offset: u64, buf: &[u8]) -> io::Result<()> {
        self.check_writable()?;
        let mut s = self.state.lock();
        let c = s.inodes.get_mut(&self.inode).expect("inode of live handle");
        let start = offset as usize;
        let end = start + buf.len();
        if c.written.len() < end {
            c.written.resize(end, 0);
        }
        c.written[start..end].copy_from_slice(buf);
        mark_dirty(c, start, end);
        Ok(())
    }
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        self.check_live()?;
        let s = self.state.lock();
        let c = s.inodes.get(&self.inode).expect("inode of live handle");
        let start = (offset as usize).min(c.written.len());
        let n = buf.len().min(c.written.len() - start);
        buf[..n].copy_from_slice(&c.written[start..start + n]);
        Ok(n)
    }
    fn set_len(&self, len: u64) -> io::Result<()> {
        self.check_writable()?;
        let mut s = self.state.lock();
        let c = s.inodes.get_mut(&self.inode).expect("inode of live handle");
        let old_len = c.written.len();
        let len = len as usize;
        c.written.resize(len, 0);
        if len > old_len {
            mark_dirty(c, old_len, len);
        }
        Ok(())
    }
    fn len(&self) -> io::Result<u64> {
        self.check_live()?;
        let s = self.state.lock();
        Ok(s.inodes
            .get(&self.inode)
            .expect("inode of live handle")
            .written
            .len() as u64)
    }
    fn sync_data(&self) -> io::Result<()> {
        self.check_live()?;
        let mut s = self.state.lock();
        let c = s.inodes.get_mut(&self.inode).expect("inode of live handle");
        c.synced = c.written.clone();
        c.dirty_sectors.clear();
        Ok(())
    }
}

impl Vfs for FaultFs {
    fn open(&self, path: &Path, mode: OpenMode) -> io::Result<Arc<dyn VfsFile>> {
        let s = self.state.lock();
        let epoch = s.epoch;
        match s.live.get(path) {
            Some(Entry::File(inode)) => Ok(Arc::new(FaultFile {
                state: self.state.clone(),
                inode: *inode,
                mode,
                epoch,
            })),
            Some(Entry::Dir) => Err(is_directory(path)),
            None => Err(not_found(path)),
        }
    }
    fn create(&self, path: &Path) -> io::Result<Arc<dyn VfsFile>> {
        let mut s = self.state.lock();
        let parent_dir = parent(path);
        if !matches!(s.live.get(&parent_dir), Some(Entry::Dir)) {
            return Err(not_found(&parent_dir));
        }
        let epoch = s.epoch;
        match s.live.get(path).cloned() {
            Some(Entry::File(inode)) => {
                // Reuse the inode: truncate `written`, but keep `synced` —
                // an unsynced truncation is undone by a subsequent power
                // loss, just like an unsynced write would be.
                let c = s.inodes.get_mut(&inode).expect("inode of live handle");
                c.written.clear();
                c.dirty_sectors.clear();
                Ok(Arc::new(FaultFile {
                    state: self.state.clone(),
                    inode,
                    mode: OpenMode::ReadWrite,
                    epoch,
                }))
            }
            Some(Entry::Dir) => Err(is_directory(path)),
            None => {
                let inode = s.next_inode;
                s.next_inode += 1;
                s.inodes.insert(inode, Content::default());
                s.live.insert(path.to_path_buf(), Entry::File(inode));
                Ok(Arc::new(FaultFile {
                    state: self.state.clone(),
                    inode,
                    mode: OpenMode::ReadWrite,
                    epoch,
                }))
            }
        }
    }
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        let mut s = self.state.lock();
        let entry = s.live.get(from).cloned().ok_or_else(|| not_found(from))?;
        if matches!(entry, Entry::Dir) {
            return Err(is_directory(from));
        }
        let to_parent = parent(to);
        if !matches!(s.live.get(&to_parent), Some(Entry::Dir)) {
            return Err(not_found(&to_parent));
        }
        s.live.remove(from);
        s.live.insert(to.to_path_buf(), entry);
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
            s.live.entry(a.to_path_buf()).or_insert(Entry::Dir);
        }
        Ok(())
    }
    fn read_dir(&self, path: &Path) -> io::Result<Vec<PathBuf>> {
        let s = self.state.lock();
        let entries: BTreeSet<PathBuf> = s
            .live
            .keys()
            .filter(|p| p.as_path() != path && parent(p) == path)
            .cloned()
            .collect();
        Ok(entries.into_iter().collect())
    }
    fn exists(&self, path: &Path) -> io::Result<bool> {
        Ok(self.state.lock().live.contains_key(path))
    }
    fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        let mut s = self.state.lock();
        let entries: BTreeMap<PathBuf, Entry> = s
            .live
            .iter()
            .filter(|(p, _)| parent(p) == dir)
            .map(|(p, e)| (p.clone(), e.clone()))
            .collect();
        s.durable_children.insert(dir.to_path_buf(), entries);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;
    use rand_chacha::ChaCha8Rng;

    /// Every test that wants a directory to survive `power_loss` must sync
    /// its parent explicitly now (I6): only the filesystem root is durable
    /// for free. This mirrors a real `Vfs` user, which must `sync_dir` a
    /// newly created directory's parent before relying on the directory
    /// itself surviving a crash.
    fn mkdir_durable(fs: &FaultFs, dir: &Path) {
        fs.create_dir_all(dir).unwrap();
        fs.sync_dir(Path::new("/")).unwrap();
    }

    #[test]
    fn conformance() {
        prkdb_core::vfs::conformance::run(&FaultFs::new(), Path::new("/r"));
    }

    #[test]
    fn power_loss_drops_unsynced_bytes() {
        let fs = FaultFs::new();
        mkdir_durable(&fs, Path::new("/d"));
        let f = fs.create(Path::new("/d/a")).unwrap();
        fs.sync_dir(Path::new("/d")).unwrap();
        f.write_at(0, b"durable").unwrap();
        f.sync_data().unwrap();
        f.write_at(7, b"-lost").unwrap();
        fs.power_loss(&mut ChaCha8Rng::seed_from_u64(1), Tear::None);
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
        mkdir_durable(&fs, Path::new("/d"));
        let f = fs.create(Path::new("/d/new")).unwrap();
        f.write_at(0, b"x").unwrap();
        f.sync_data().unwrap(); // data synced, but directory entry never was
        fs.power_loss(&mut ChaCha8Rng::seed_from_u64(1), Tear::None);
        assert!(!fs.exists(Path::new("/d/new")).unwrap());
        // "/d" itself did survive: it's the entry-for-a-*file* that's missing.
        assert!(fs.exists(Path::new("/d")).unwrap());
    }

    #[test]
    fn power_loss_drops_unsynced_subdir() {
        let fs = FaultFs::new();
        mkdir_durable(&fs, Path::new("/d"));
        // "/d/sub" is created but "/d" is never re-synced afterward, so
        // "/d/sub" never becomes a durable child of "/d".
        fs.create_dir_all(Path::new("/d/sub")).unwrap();
        let f = fs.create(Path::new("/d/sub/a")).unwrap();
        f.write_at(0, b"x").unwrap();
        f.sync_data().unwrap();
        fs.sync_dir(Path::new("/d/sub")).unwrap(); // durable *within* sub, doesn't help

        fs.power_loss(&mut ChaCha8Rng::seed_from_u64(3), Tear::None);

        assert!(fs.exists(Path::new("/d")).unwrap());
        assert!(!fs.exists(Path::new("/d/sub")).unwrap());
        assert!(!fs.exists(Path::new("/d/sub/a")).unwrap());
    }

    #[test]
    fn create_over_existing_reuses_inode_and_unsynced_truncation_is_undone() {
        let fs = FaultFs::new();
        mkdir_durable(&fs, Path::new("/d"));
        let f = fs.create(Path::new("/d/a")).unwrap();
        f.write_at(0, b"old-content").unwrap();
        f.sync_data().unwrap();
        fs.sync_dir(Path::new("/d")).unwrap();

        // Truncate-create again without syncing the (now empty) content.
        let f2 = fs.create(Path::new("/d/a")).unwrap();
        assert_eq!(f2.len().unwrap(), 0);

        fs.power_loss(&mut ChaCha8Rng::seed_from_u64(5), Tear::None);

        let after = fs.open(Path::new("/d/a"), OpenMode::Read).unwrap();
        let mut buf = vec![0u8; 11];
        let n = after.read_at(0, &mut buf).unwrap();
        assert_eq!(&buf[..n], b"old-content");
    }

    #[test]
    fn create_over_existing_then_sync_makes_truncation_durable() {
        let fs = FaultFs::new();
        mkdir_durable(&fs, Path::new("/d"));
        let f = fs.create(Path::new("/d/a")).unwrap();
        f.write_at(0, b"old-content").unwrap();
        f.sync_data().unwrap();
        fs.sync_dir(Path::new("/d")).unwrap();

        let f2 = fs.create(Path::new("/d/a")).unwrap();
        f2.sync_data().unwrap(); // durably commits the truncation to empty

        fs.power_loss(&mut ChaCha8Rng::seed_from_u64(5), Tear::None);

        let after = fs.open(Path::new("/d/a"), OpenMode::Read).unwrap();
        assert_eq!(after.len().unwrap(), 0);
    }

    #[test]
    fn stale_handle_after_power_loss_errors_on_every_op() {
        let fs = FaultFs::new();
        mkdir_durable(&fs, Path::new("/d"));
        let f = fs.create(Path::new("/d/a")).unwrap();
        f.write_at(0, b"x").unwrap();
        f.sync_data().unwrap();
        fs.sync_dir(Path::new("/d")).unwrap();

        fs.power_loss(&mut ChaCha8Rng::seed_from_u64(1), Tear::None);

        assert!(f.read_at(0, &mut [0u8; 1]).is_err());
        assert!(f.write_at(0, b"y").is_err());
        assert!(f.set_len(0).is_err());
        assert!(f.len().is_err());
        assert!(f.sync_data().is_err());
    }

    #[test]
    fn tear_none_reverts_tail_to_last_sync() {
        let fs = FaultFs::new();
        mkdir_durable(&fs, Path::new("/d"));
        let f = fs.create(Path::new("/d/a")).unwrap();
        f.write_at(0, b"0123456789").unwrap();
        f.sync_data().unwrap();
        fs.sync_dir(Path::new("/d")).unwrap();
        f.write_at(10, b"-lost").unwrap();
        fs.power_loss(&mut ChaCha8Rng::seed_from_u64(1), Tear::None);
        let after = fs.open(Path::new("/d/a"), OpenMode::Read).unwrap();
        assert_eq!(after.len().unwrap(), 10);
    }

    #[test]
    fn tear_prefix_keeps_a_random_prefix_of_the_tail() {
        let fs = FaultFs::new();
        mkdir_durable(&fs, Path::new("/d"));
        let f = fs.create(Path::new("/d/a")).unwrap();
        fs.sync_dir(Path::new("/d")).unwrap();
        f.write_at(0, b"0123456789").unwrap();
        fs.power_loss(&mut ChaCha8Rng::seed_from_u64(7), Tear::Prefix);
        let len = fs
            .open(Path::new("/d/a"), OpenMode::Read)
            .unwrap()
            .len()
            .unwrap();
        assert!(len <= 10);
    }

    #[test]
    fn tear_zero_tail_keeps_length_but_zeroes_unsynced_bytes() {
        let fs = FaultFs::new();
        mkdir_durable(&fs, Path::new("/d"));
        let f = fs.create(Path::new("/d/a")).unwrap();
        f.write_at(0, b"0123456789").unwrap();
        f.sync_data().unwrap();
        fs.sync_dir(Path::new("/d")).unwrap();
        f.write_at(10, b"abcde").unwrap();
        fs.power_loss(&mut ChaCha8Rng::seed_from_u64(2), Tear::ZeroTail);
        let after = fs.open(Path::new("/d/a"), OpenMode::Read).unwrap();
        assert_eq!(after.len().unwrap(), 15);
        let mut buf = vec![0u8; 15];
        after.read_at(0, &mut buf).unwrap();
        assert_eq!(&buf[..10], b"0123456789");
        assert_eq!(&buf[10..], &[0u8; 5]);
    }

    #[test]
    fn tear_garbage_keeps_length_but_randomizes_unsynced_bytes() {
        let fs = FaultFs::new();
        mkdir_durable(&fs, Path::new("/d"));
        let f = fs.create(Path::new("/d/a")).unwrap();
        f.write_at(0, b"0123456789").unwrap();
        f.sync_data().unwrap();
        fs.sync_dir(Path::new("/d")).unwrap();
        f.write_at(10, b"abcde").unwrap();
        fs.power_loss(&mut ChaCha8Rng::seed_from_u64(2), Tear::Garbage);
        let after = fs.open(Path::new("/d/a"), OpenMode::Read).unwrap();
        assert_eq!(after.len().unwrap(), 15);
        let mut buf = vec![0u8; 15];
        after.read_at(0, &mut buf).unwrap();
        assert_eq!(&buf[..10], b"0123456789");
    }

    #[test]
    fn tear_in_place_overwrite_reverts_or_keeps_whole_sectors() {
        let fs = FaultFs::new();
        mkdir_durable(&fs, Path::new("/d"));
        let f = fs.create(Path::new("/d/a")).unwrap();
        let original = vec![b'A'; 1536]; // 3 sectors
        f.write_at(0, &original).unwrap();
        f.sync_data().unwrap();
        fs.sync_dir(Path::new("/d")).unwrap();
        // Overwrite sector 1 in place, without syncing.
        f.write_at(512, &vec![b'B'; 512]).unwrap();
        fs.power_loss(&mut ChaCha8Rng::seed_from_u64(9), Tear::None);
        let after = fs.open(Path::new("/d/a"), OpenMode::Read).unwrap();
        let mut buf = vec![0u8; 1536];
        after.read_at(0, &mut buf).unwrap();
        let sector1 = &buf[512..1024];
        assert!(sector1.iter().all(|&b| b == b'A') || sector1.iter().all(|&b| b == b'B'));
        assert!(buf[..512].iter().all(|&b| b == b'A'));
        assert!(buf[1024..].iter().all(|&b| b == b'A'));
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

    #[test]
    fn rename_errors_when_destination_parent_missing() {
        let fs = FaultFs::new();
        fs.create_dir_all(Path::new("/d")).unwrap();
        let _f = fs.create(Path::new("/d/a")).unwrap();
        assert!(fs
            .rename(Path::new("/d/a"), Path::new("/missing/b"))
            .is_err());
    }

    #[test]
    fn rename_errors_on_directory_source() {
        let fs = FaultFs::new();
        fs.create_dir_all(Path::new("/d")).unwrap();
        assert!(fs.rename(Path::new("/d"), Path::new("/d2")).is_err());
    }
}
