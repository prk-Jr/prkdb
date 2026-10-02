//! In-memory `Vfs` that separates written from synced state, so `power_loss`
//! can discard everything not yet made durable (spec §7 Phase 1).
//!
//! Durability model:
//! - A file's bytes are durable up to `sync_data`; anything written after
//!   that can be lost, zeroed, garbled, or partially kept on power loss, in
//!   512-byte sectors (see [`Tear`]). An unsynced *truncation* (a shrinking
//!   `set_len`, or a `create` over an existing file) is a separate failure
//!   mode: on power loss it either reverts in full (the file comes back with
//!   its old, pre-truncation content) or persists (the file comes back
//!   truncated, and any bytes written after the truncation but before the
//!   next `sync_data` are torn per `Tear` on top of that shorter baseline) —
//!   modeling the same class of failure as an ext4 file that comes back
//!   zero-length after a crash mid-truncate.
//! - A directory entry (file or subdirectory) is durable only once
//!   `sync_dir` has been called on its parent *while the entry was live*, and
//!   only if the parent directory is itself durable. The filesystem root (a
//!   path with no parent) is the one exception: it's always durable for free,
//!   the way a real filesystem's root already exists on disk. A *nested*
//!   directory such as a WAL's `log_dir` is NOT durable for free — an
//!   application-created directory tree must still be synced, ancestor by
//!   ancestor, before it can be relied on to survive a crash; see
//!   [`FaultFs::mkdir_durable`] for a helper that does this in one call.
//! - A handle obtained before a `power_loss` call is stale afterward: every
//!   operation on it errors, instead of silently reading/writing whatever the
//!   inode now contains. The epoch that makes a handle stale is checked under
//!   the same lock as the operation it guards, so a `power_loss` can't slip
//!   in between the check and the operation.
//! - What survives a `power_loss` is what is on disk afterward: it becomes
//!   the new durable baseline for every file (its synced content) and every
//!   directory (its durable children). A second `power_loss` with no
//!   operations in between changes nothing; it can't re-tear a torn tail or
//!   bring back bytes or entries the first one lost.

use parking_lot::Mutex;
use prkdb_core::vfs::{LockGuard, OpenMode, Vfs, VfsFile};
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
    /// The unsynced tail (growth past the last synced/effective length) is
    /// dropped entirely. This mode only governs *appended* bytes: sectors
    /// that were overwritten in place within the already-durable region are
    /// independently torn per sector (see the module docs) regardless of
    /// `tear`, and can still keep their new, unsynced bytes even under
    /// `None`.
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
    /// The minimum length this file has been unsynced-truncated to (via a
    /// shrinking `set_len`, or a `create` over an existing file) since the
    /// last `sync_data`. `None` if no unsynced truncation has happened.
    /// `power_loss` uses this to decide, per file, whether the truncation
    /// persists (the file comes back at/around this length) or reverts (the
    /// file comes back with its full old, pre-truncation content).
    truncated_since_sync: Option<usize>,
}

/// Records an unsynced truncation of `c` down to `new_len`, keeping the
/// smallest length seen across possibly multiple truncations before the next
/// `sync_data` (the low-water mark is the worst case for what could persist).
fn record_truncation(c: &mut Content, new_len: usize) {
    c.truncated_since_sync = Some(match c.truncated_since_sync {
        Some(existing) => existing.min(new_len),
        None => new_len,
    });
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
    /// `Vfs::lock_exclusive` locks held, by path, each with the token of the guard that
    /// holds it. Process-level state, not filesystem state: `power_loss` (the process
    /// is gone) clears it, the way the OS releases a dead process's locks.
    locks: BTreeMap<PathBuf, u64>,
    next_lock: u64,
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

    // Step 0: if this file was unsynced-truncated since the last sync, power
    // loss decides — independently, per file — whether that truncation
    // persists or reverts. This is the ext4-style failure class where a
    // truncate-in-progress either lands (file comes back short) or is undone
    // entirely (file comes back with its old, longer content); real
    // filesystems don't reliably do the latter for *every* mode, but neither
    // do they promise the former, so we let the rng pick rather than always
    // reverting.
    let (effective_synced, effective_len): (&[u8], usize) = match c.truncated_since_sync {
        Some(trunc_len) if rng.gen_bool(0.5) => {
            let len = trunc_len.min(synced_len);
            (&c.synced[..len], len)
        }
        _ => (&c.synced[..], synced_len),
    };

    // Step 1: the tail beyond `effective_len`. With no unsynced truncation
    // (or one that reverted above), `effective_len == synced_len` and this
    // matches ordinary unsynced-append tearing. With a persisted truncation,
    // `effective_len == trunc_len` and this tears whatever was written after
    // the truncation but before the next `sync_data`, on top of the shorter
    // baseline. If nothing was written past `effective_len` at all (no
    // growth, or the growth didn't reach past the truncation point), the
    // result is just the (possibly truncated) synced baseline.
    let mut result: Vec<u8> = if written_len > effective_len {
        match tear {
            Tear::None => effective_synced.to_vec(),
            Tear::Prefix => {
                let mut v = effective_synced.to_vec();
                let extra = rng.gen_range(0..=written_len - effective_len);
                v.extend_from_slice(&c.written[effective_len..effective_len + extra]);
                v
            }
            Tear::ZeroTail => {
                let mut v = c.written.clone();
                v[effective_len..].fill(0);
                v
            }
            Tear::Garbage => {
                let mut v = c.written.clone();
                for b in &mut v[effective_len..] {
                    *b = rng.gen();
                }
                v
            }
        }
    } else {
        effective_synced.to_vec()
    };

    // Step 2: independently tear dirty sectors that overlap the
    // already-durable region (in-place overwrites of durable data) — this
    // applies regardless of `tear` and regardless of step 1's outcome, since
    // it models a different failure (a torn in-place update, not a torn
    // append or truncation). NOTE: this is deliberately stricter than a real
    // filesystem for the case of a `create`-truncate immediately followed by
    // a short unsynced write: a real crash there can't tear that write into
    // "new prefix + leftover old bytes" (the old bytes are gone, replaced by
    // a fresh, initially-zero extent), but modeling it that way here only
    // ever makes an already-incorrect `Vfs` implementation *more* likely to
    // be caught, never less, so it's left as-is.
    for &sector in &c.dirty_sectors {
        let start = sector as usize * SECTOR;
        if start >= effective_len || start >= written_len {
            // Outside the durable region (pure append/truncation-tail,
            // handled above), or truncated away entirely: nothing new to
            // reconsider here.
            continue;
        }
        let end = ((sector as usize + 1) * SECTOR)
            .min(effective_len)
            .min(written_len)
            .min(result.len());
        if start >= end {
            continue;
        }
        if rng.gen_bool(0.5) {
            result[start..end].copy_from_slice(&effective_synced[start..end]); // reverts
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

    /// Creates `path` (and any missing ancestors, like `create_dir_all`) and
    /// makes the whole ancestor chain durable in one call, by snapshotting
    /// each ancestor directory's children as of right now (as `sync_dir`
    /// would). This is a *setup* convenience, not something a real `Vfs`
    /// user gets for free: on a real filesystem, only the true root already
    /// exists, so an application-created directory tree (a WAL's `log_dir`,
    /// say) genuinely needs `sync_dir` called on every level before it can be
    /// relied on to survive a crash. Anything created *after* this call
    /// (e.g. a file, or a further subdirectory) still needs its own
    /// `sync_data`/`sync_dir` to become durable — this only covers the
    /// directory chain as it exists at the moment of the call.
    pub fn mkdir_durable(&self, path: &Path) -> io::Result<()> {
        self.create_dir_all(path)?;
        if path.parent().is_none() {
            // `path` is the filesystem root itself: it's always durable for
            // free, nothing to sync.
            return Ok(());
        }
        let mut dir = parent(path);
        loop {
            self.sync_dir(&dir)?;
            if dir.parent().is_none() {
                break;
            }
            dir = parent(&dir);
        }
        Ok(())
    }

    /// Simulates power loss: unsynced directory entries and unsynced file
    /// content vanish (or are torn, per `tear`); see the module docs for the
    /// exact durability rules. Any handle obtained before this call becomes
    /// stale and errors on every subsequent operation.
    pub fn power_loss(&self, rng: &mut impl Rng, tear: Tear) {
        let mut s = self.state.lock();
        s.epoch += 1;
        s.locks.clear();

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
        // What survived is what is on disk: it is the new durable baseline
        // for every directory, so a later power loss starts from it rather
        // than from directory snapshots taken before this one (which could
        // name children this crash already lost, TST-11).
        let mut durable_children: BTreeMap<PathBuf, BTreeMap<PathBuf, Entry>> = live_dirs
            .iter()
            .map(|d| (d.clone(), BTreeMap::new()))
            .collect();
        for (p, e) in &new_live {
            if p.parent().is_some() {
                if let Some(children) = durable_children.get_mut(&parent(p)) {
                    children.insert(p.clone(), e.clone());
                }
            }
        }
        s.durable_children = durable_children;

        // Content tearing. Only inodes still reachable from a surviving entry
        // matter: the rest are unreachable for good (every handle to them is
        // now stale, and no durable entry names them), so they are dropped.
        let reachable: BTreeSet<u64> = new_live
            .values()
            .filter_map(|e| match e {
                Entry::File(inode) => Some(*inode),
                Entry::Dir => None,
            })
            .collect();
        s.live = new_live;
        let inodes: Vec<u64> = s.inodes.keys().copied().collect();
        for inode in inodes {
            let torn = tear_content(
                s.inodes.get(&inode).expect("inode of tracked content"),
                tear,
                rng,
            );
            if !reachable.contains(&inode) {
                s.inodes.remove(&inode);
                continue;
            }
            let c = s.inodes.get_mut(&inode).expect("inode of tracked content");
            // The torn result is now the durable content: a later power loss
            // with no intervening writes must reproduce it exactly, never
            // re-tear it or fall back to bytes this crash already lost
            // (TST-11). Whatever `tear_content` decided about an unsynced
            // truncation (persisted or reverted) is likewise settled.
            c.synced = torn.clone();
            c.written = torn;
            c.dirty_sectors.clear();
            c.truncated_since_sync = None;
        }
    }
}

/// A held `lock_exclusive` lock. Dropping it releases the lock only if it is still this
/// guard's: after a `power_loss` another holder may have taken the path.
struct FaultLock {
    state: Arc<Mutex<State>>,
    path: PathBuf,
    token: u64,
}

impl LockGuard for FaultLock {}

impl Drop for FaultLock {
    fn drop(&mut self) {
        let mut s = self.state.lock();
        if s.locks.get(&self.path) == Some(&self.token) {
            s.locks.remove(&self.path);
        }
    }
}

impl FaultFile {
    /// Checks this handle isn't writable-blocked (read-only mode). Does NOT
    /// check liveness — that must happen under the same lock as the
    /// operation it guards (see the module docs), not here beforehand, or a
    /// `power_loss` could land in the gap between the check and the op.
    fn check_writable(&self) -> io::Result<()> {
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
        if s.epoch != self.epoch {
            return Err(stale_handle());
        }
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
        let s = self.state.lock();
        if s.epoch != self.epoch {
            return Err(stale_handle());
        }
        let c = s.inodes.get(&self.inode).expect("inode of live handle");
        let start = (offset as usize).min(c.written.len());
        let n = buf.len().min(c.written.len() - start);
        buf[..n].copy_from_slice(&c.written[start..start + n]);
        Ok(n)
    }
    fn set_len(&self, len: u64) -> io::Result<()> {
        self.check_writable()?;
        let mut s = self.state.lock();
        if s.epoch != self.epoch {
            return Err(stale_handle());
        }
        let c = s.inodes.get_mut(&self.inode).expect("inode of live handle");
        let old_len = c.written.len();
        let len = len as usize;
        if len < old_len {
            record_truncation(c, len);
        }
        c.written.resize(len, 0);
        if len > old_len {
            mark_dirty(c, old_len, len);
        }
        Ok(())
    }
    fn len(&self) -> io::Result<u64> {
        let s = self.state.lock();
        if s.epoch != self.epoch {
            return Err(stale_handle());
        }
        Ok(s.inodes
            .get(&self.inode)
            .expect("inode of live handle")
            .written
            .len() as u64)
    }
    fn sync_data(&self) -> io::Result<()> {
        let mut s = self.state.lock();
        if s.epoch != self.epoch {
            return Err(stale_handle());
        }
        let c = s.inodes.get_mut(&self.inode).expect("inode of live handle");
        c.synced = c.written.clone();
        c.dirty_sectors.clear();
        c.truncated_since_sync = None;
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
                // this is an unsynced truncation to zero, so `power_loss`
                // decides whether it persists or is undone (see
                // `truncated_since_sync`), the same as any other unsynced
                // truncation.
                let c = s.inodes.get_mut(&inode).expect("inode of live handle");
                c.written.clear();
                c.dirty_sectors.clear();
                record_truncation(c, 0);
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
        let mut s = self.state.lock();
        match s.live.get(path) {
            None => Err(not_found(path)),
            Some(Entry::Dir) => {
                let has_children = s.live.keys().any(|p| parent(p) == path);
                if has_children {
                    return Err(io::Error::new(
                        io::ErrorKind::DirectoryNotEmpty,
                        format!("directory not empty: {}", path.display()),
                    ));
                }
                s.live.remove(path);
                Ok(())
            }
            Some(Entry::File(_)) => {
                s.live.remove(path);
                Ok(())
            }
        }
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
        if !matches!(s.live.get(dir), Some(Entry::Dir)) {
            return Err(not_found(dir));
        }
        let entries: BTreeMap<PathBuf, Entry> = s
            .live
            .iter()
            .filter(|(p, _)| parent(p) == dir)
            .map(|(p, e)| (p.clone(), e.clone()))
            .collect();
        s.durable_children.insert(dir.to_path_buf(), entries);
        Ok(())
    }
    fn lock_exclusive(&self, path: &Path) -> io::Result<Box<dyn LockGuard>> {
        let mut s = self.state.lock();
        match s.live.get(path) {
            Some(Entry::Dir) => return Err(is_directory(path)),
            Some(Entry::File(_)) => {}
            None => {
                // Created like `create` creates a new file: an unsynced directory entry.
                let parent_dir = parent(path);
                if !matches!(s.live.get(&parent_dir), Some(Entry::Dir)) {
                    return Err(not_found(&parent_dir));
                }
                let inode = s.next_inode;
                s.next_inode += 1;
                s.inodes.insert(inode, Content::default());
                s.live.insert(path.to_path_buf(), Entry::File(inode));
            }
        }
        if s.locks.contains_key(path) {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                format!("{} is locked", path.display()),
            ));
        }
        let token = s.next_lock;
        s.next_lock += 1;
        s.locks.insert(path.to_path_buf(), token);
        Ok(Box::new(FaultLock {
            state: self.state.clone(),
            path: path.to_path_buf(),
            token,
        }))
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
        fs.mkdir_durable(Path::new("/d")).unwrap();
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
        fs.mkdir_durable(Path::new("/d")).unwrap();
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
        fs.mkdir_durable(Path::new("/d")).unwrap();
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
    fn create_over_existing_reuses_inode() {
        let fs = FaultFs::new();
        fs.mkdir_durable(Path::new("/d")).unwrap();
        let f = fs.create(Path::new("/d/a")).unwrap();
        f.write_at(0, b"old-content").unwrap();
        f.sync_data().unwrap();
        fs.sync_dir(Path::new("/d")).unwrap();

        // Truncate-create again without syncing the (now empty) content.
        let f2 = fs.create(Path::new("/d/a")).unwrap();
        assert_eq!(f2.len().unwrap(), 0);
    }

    /// I5: an unsynced truncation (here, a `create` over an existing file)
    /// isn't an ordinary unsynced write — a real filesystem crashing
    /// mid-truncate can either undo it entirely (old content comes back) or
    /// land it while losing whatever was written afterward but never synced
    /// (the ext4 "vanished truncation" class). Over many seeds, `power_loss`
    /// must produce BOTH outcomes for the same truncate-then-crash sequence.
    #[test]
    fn truncate_then_power_loss_before_sync_can_revert_or_persist() {
        let mut saw_reverted = false;
        let mut saw_persisted = false;
        for seed in 0..300 {
            let fs = FaultFs::new();
            fs.mkdir_durable(Path::new("/d")).unwrap();
            let f = fs.create(Path::new("/d/a")).unwrap();
            f.write_at(0, b"old-content").unwrap();
            f.sync_data().unwrap();
            fs.sync_dir(Path::new("/d")).unwrap();

            // Truncate-create again without syncing the (now empty) content.
            let f2 = fs.create(Path::new("/d/a")).unwrap();
            assert_eq!(f2.len().unwrap(), 0);

            fs.power_loss(&mut ChaCha8Rng::seed_from_u64(seed), Tear::None);

            let after = fs.open(Path::new("/d/a"), OpenMode::Read).unwrap();
            let mut buf = vec![0u8; 11];
            let n = after.read_at(0, &mut buf).unwrap();
            match n {
                11 if &buf == b"old-content" => saw_reverted = true,
                0 => saw_persisted = true,
                other => panic!(
                    "unexpected length {other} after power loss on seed {seed}: {:?}",
                    &buf[..other]
                ),
            }
            if saw_reverted && saw_persisted {
                break;
            }
        }
        assert!(
            saw_reverted,
            "expected the truncation to revert (old content restored) on some seed"
        );
        assert!(
            saw_persisted,
            "expected the truncation to persist (truncated-to-new-length) on some seed"
        );
    }

    #[test]
    fn create_over_existing_then_sync_makes_truncation_durable() {
        let fs = FaultFs::new();
        fs.mkdir_durable(Path::new("/d")).unwrap();
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
        fs.mkdir_durable(Path::new("/d")).unwrap();
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
        fs.mkdir_durable(Path::new("/d")).unwrap();
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
        fs.mkdir_durable(Path::new("/d")).unwrap();
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
        fs.mkdir_durable(Path::new("/d")).unwrap();
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
        fs.mkdir_durable(Path::new("/d")).unwrap();
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
        let mut saw_reverted = false;
        let mut saw_kept = false;
        for seed in 0..200 {
            let fs = FaultFs::new();
            fs.mkdir_durable(Path::new("/d")).unwrap();
            let f = fs.create(Path::new("/d/a")).unwrap();
            let original = vec![b'A'; 1536]; // 3 sectors
            f.write_at(0, &original).unwrap();
            f.sync_data().unwrap();
            fs.sync_dir(Path::new("/d")).unwrap();
            // Overwrite sector 1 in place, without syncing.
            f.write_at(512, &vec![b'B'; 512]).unwrap();
            fs.power_loss(&mut ChaCha8Rng::seed_from_u64(seed), Tear::None);
            let after = fs.open(Path::new("/d/a"), OpenMode::Read).unwrap();
            let mut buf = vec![0u8; 1536];
            after.read_at(0, &mut buf).unwrap();
            let sector1 = &buf[512..1024];
            assert!(buf[..512].iter().all(|&b| b == b'A'));
            assert!(buf[1024..].iter().all(|&b| b == b'A'));
            if sector1.iter().all(|&b| b == b'A') {
                saw_reverted = true;
            } else if sector1.iter().all(|&b| b == b'B') {
                saw_kept = true;
            } else {
                panic!("sector 1 is neither wholly reverted nor wholly kept: {sector1:?}");
            }
            if saw_reverted && saw_kept {
                break;
            }
        }
        assert!(saw_reverted, "expected sector 1 to revert on some seed");
        assert!(
            saw_kept,
            "expected sector 1 to keep its new bytes on some seed"
        );
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

    /// I7: a stale handle must be rejected under the *same* lock acquisition
    /// as the operation it guards. Simulated here by having the epoch check
    /// happen for every op via a fresh lock each call (rather than a
    /// check-then-relock pattern) — this test pins the observable behavior:
    /// once `power_loss` has run, every op on the old handle errors, with no
    /// window in which a stale handle could have mutated state.
    #[test]
    fn write_after_power_loss_never_mutates_state() {
        let fs = FaultFs::new();
        fs.mkdir_durable(Path::new("/d")).unwrap();
        let f = fs.create(Path::new("/d/a")).unwrap();
        f.write_at(0, b"first").unwrap();
        f.sync_data().unwrap();
        fs.sync_dir(Path::new("/d")).unwrap();

        fs.power_loss(&mut ChaCha8Rng::seed_from_u64(1), Tear::None);

        // A fresh writer opens the same path post-power-loss and writes new
        // content.
        let f2 = fs.create(Path::new("/d/a")).unwrap();
        f2.write_at(0, b"second").unwrap();
        f2.sync_data().unwrap();
        fs.sync_dir(Path::new("/d")).unwrap();

        // The stale handle must still error, and must never have been able
        // to sneak a write into the new generation's content.
        assert!(f.write_at(0, b"stale-write").is_err());
        let after = fs.open(Path::new("/d/a"), OpenMode::Read).unwrap();
        let mut buf = vec![0u8; 6];
        let n = after.read_at(0, &mut buf).unwrap();
        assert_eq!(&buf[..n], b"second");
    }

    /// N2: `mkdir_durable` makes a whole nested directory chain durable in
    /// one call — but anything created inside it afterward still needs its
    /// own sync to survive.
    #[test]
    fn mkdir_durable_makes_the_whole_chain_durable_but_not_later_children() {
        let fs = FaultFs::new();
        fs.mkdir_durable(Path::new("/tmp/x/wal")).unwrap();

        let f = fs.create(Path::new("/tmp/x/wal/a")).unwrap();
        f.write_at(0, b"hello").unwrap();
        f.sync_data().unwrap();
        fs.sync_dir(Path::new("/tmp/x/wal")).unwrap();

        // A subdir created later, without syncing "/tmp/x/wal" again, must
        // not survive.
        fs.create_dir_all(Path::new("/tmp/x/wal/sub")).unwrap();

        fs.power_loss(&mut ChaCha8Rng::seed_from_u64(1), Tear::None);

        assert!(fs.exists(Path::new("/tmp/x/wal")).unwrap());
        assert!(fs.exists(Path::new("/tmp/x/wal/a")).unwrap());
        assert!(!fs.exists(Path::new("/tmp/x/wal/sub")).unwrap());
    }

    #[test]
    fn mkdir_durable_on_the_root_itself_is_ok() {
        let fs = FaultFs::new();
        // The root has no parent to sync, so this must short-circuit to Ok
        // rather than trying (and failing) to sync_dir an empty path.
        fs.mkdir_durable(Path::new("/")).unwrap();
    }

    #[test]
    fn remove_rejects_non_empty_directory() {
        let fs = FaultFs::new();
        fs.create_dir_all(Path::new("/d")).unwrap();
        let _f = fs.create(Path::new("/d/a")).unwrap();
        let err = fs.remove(Path::new("/d")).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::DirectoryNotEmpty);
        assert!(fs.exists(Path::new("/d")).unwrap());
    }

    #[test]
    fn remove_allows_empty_directory() {
        let fs = FaultFs::new();
        fs.create_dir_all(Path::new("/d")).unwrap();
        fs.remove(Path::new("/d")).unwrap();
        assert!(!fs.exists(Path::new("/d")).unwrap());
    }

    #[test]
    fn sync_dir_on_missing_dir_returns_not_found() {
        let fs = FaultFs::new();
        let err = fs.sync_dir(Path::new("/missing")).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
    }

    /// Reads the whole of `path` through a fresh handle, or `None` if it
    /// doesn't exist.
    fn read_all(fs: &FaultFs, path: &str) -> Option<Vec<u8>> {
        let f = fs.open(Path::new(path), OpenMode::Read).ok()?;
        let mut buf = vec![0u8; f.len().unwrap() as usize];
        let n = f.read_at(0, &mut buf).unwrap();
        assert_eq!(n, buf.len());
        Some(buf)
    }

    /// Every live path and, for files, its contents.
    fn snapshot(fs: &FaultFs) -> BTreeMap<PathBuf, Option<Vec<u8>>> {
        let paths: Vec<(PathBuf, bool)> = fs
            .state
            .lock()
            .live
            .iter()
            .map(|(p, e)| (p.clone(), matches!(e, Entry::File(_))))
            .collect();
        paths
            .into_iter()
            .map(|(p, is_file)| {
                let content = is_file.then(|| read_all(fs, p.to_str().unwrap()).unwrap());
                (p, content)
            })
            .collect()
    }

    const TEARS: [Tear; 4] = [Tear::None, Tear::Prefix, Tear::ZeroTail, Tear::Garbage];

    /// TST-11: the review repro. Synced "old", an unsynced truncate, a first
    /// crash that persists the truncation (empty file), then a second crash
    /// with no writes in between must not bring "old" back.
    #[test]
    fn tst11_a_persisted_truncation_survives_a_second_power_loss() {
        let mut checked = 0;
        for seed in 0..200 {
            let fs = FaultFs::new();
            fs.mkdir_durable(Path::new("/d")).unwrap();
            let f = fs.create(Path::new("/d/a")).unwrap();
            f.write_at(0, b"old").unwrap();
            f.sync_data().unwrap();
            fs.sync_dir(Path::new("/d")).unwrap();
            f.set_len(0).unwrap();

            fs.power_loss(&mut ChaCha8Rng::seed_from_u64(seed), Tear::None);
            if read_all(&fs, "/d/a").unwrap() != b"" {
                continue; // this seed reverted the truncation
            }
            for (i, tear) in TEARS.into_iter().enumerate() {
                fs.power_loss(&mut ChaCha8Rng::seed_from_u64(seed * 31 + i as u64), tear);
                assert_eq!(
                    read_all(&fs, "/d/a").unwrap(),
                    b"",
                    "seed {seed}, second crash {tear:?} resurrected the truncated bytes"
                );
            }
            checked += 1;
        }
        assert!(checked > 0, "no seed persisted the truncation");
    }

    /// TST-11: same, for a `create` over an existing synced file followed by
    /// an unsynced write.
    #[test]
    fn tst11_a_persisted_create_truncation_survives_a_second_power_loss() {
        let mut checked = 0;
        for seed in 0..200 {
            let fs = FaultFs::new();
            fs.mkdir_durable(Path::new("/d")).unwrap();
            let f = fs.create(Path::new("/d/a")).unwrap();
            f.write_at(0, b"old-content").unwrap();
            f.sync_data().unwrap();
            fs.sync_dir(Path::new("/d")).unwrap();
            let f2 = fs.create(Path::new("/d/a")).unwrap();
            f2.write_at(0, b"new").unwrap();

            fs.power_loss(&mut ChaCha8Rng::seed_from_u64(seed), Tear::Prefix);
            let first = read_all(&fs, "/d/a").unwrap();
            if first.len() >= b"old-content".len() {
                continue; // reverted
            }
            for (i, tear) in TEARS.into_iter().enumerate() {
                fs.power_loss(&mut ChaCha8Rng::seed_from_u64(seed * 31 + i as u64), tear);
                assert_eq!(
                    read_all(&fs, "/d/a").unwrap(),
                    first,
                    "seed {seed}, {tear:?}"
                );
            }
            checked += 1;
        }
        assert!(checked > 0, "no seed persisted the truncation");
    }

    /// TST-11: an unsynced in-place overwrite whose new sector survived the
    /// first crash must not revert to the pre-overwrite bytes on a second.
    #[test]
    fn tst11_a_kept_overwrite_survives_a_second_power_loss() {
        let mut checked = 0;
        for seed in 0..200 {
            let fs = FaultFs::new();
            fs.mkdir_durable(Path::new("/d")).unwrap();
            let f = fs.create(Path::new("/d/a")).unwrap();
            f.write_at(0, &[b'A'; 1024]).unwrap();
            f.sync_data().unwrap();
            fs.sync_dir(Path::new("/d")).unwrap();
            f.write_at(512, &[b'B'; 512]).unwrap();

            fs.power_loss(&mut ChaCha8Rng::seed_from_u64(seed), Tear::None);
            let first = read_all(&fs, "/d/a").unwrap();
            if first[512] != b'B' {
                continue; // the sector reverted
            }
            for (i, tear) in TEARS.into_iter().enumerate() {
                fs.power_loss(&mut ChaCha8Rng::seed_from_u64(seed * 31 + i as u64), tear);
                assert_eq!(
                    read_all(&fs, "/d/a").unwrap(),
                    first,
                    "seed {seed}, second crash {tear:?} reverted a sector the first crash kept"
                );
            }
            checked += 1;
        }
        assert!(checked > 0, "no seed kept the overwritten sector");
    }

    /// TST-11: whatever a torn tail (`Prefix`, `ZeroTail`, `Garbage`) left
    /// behind is what is on disk; a second crash with no writes must not
    /// re-tear it (shorten a kept prefix, re-randomize garbage).
    #[test]
    fn tst11_a_torn_tail_is_not_re_torn_by_a_second_power_loss() {
        for tear in [Tear::Prefix, Tear::ZeroTail, Tear::Garbage] {
            for seed in 0..50 {
                let fs = FaultFs::new();
                fs.mkdir_durable(Path::new("/d")).unwrap();
                let f = fs.create(Path::new("/d/a")).unwrap();
                f.write_at(0, b"0123456789").unwrap();
                f.sync_data().unwrap();
                fs.sync_dir(Path::new("/d")).unwrap();
                f.write_at(10, &[b'x'; 600]).unwrap();

                fs.power_loss(&mut ChaCha8Rng::seed_from_u64(seed), tear);
                let first = read_all(&fs, "/d/a").unwrap();
                for (i, second) in TEARS.into_iter().enumerate() {
                    fs.power_loss(
                        &mut ChaCha8Rng::seed_from_u64(seed * 31 + i as u64 + 1),
                        second,
                    );
                    assert_eq!(
                        read_all(&fs, "/d/a").unwrap(),
                        first,
                        "seed {seed}, first {tear:?}, second {second:?}"
                    );
                }
            }
        }
    }

    /// TST-11: a file that was durable inside a directory the first crash
    /// lost must not come back when the directory is recreated and made
    /// durable again, then a second crash hits.
    #[test]
    fn tst11_a_lost_subdirectory_does_not_resurrect_its_children() {
        let fs = FaultFs::new();
        fs.mkdir_durable(Path::new("/d")).unwrap();
        // "/d/sub/a" is durable within "/d/sub", but "/d/sub" never becomes
        // a durable child of "/d".
        fs.create_dir_all(Path::new("/d/sub")).unwrap();
        let f = fs.create(Path::new("/d/sub/a")).unwrap();
        f.write_at(0, b"x").unwrap();
        f.sync_data().unwrap();
        fs.sync_dir(Path::new("/d/sub")).unwrap();

        fs.power_loss(&mut ChaCha8Rng::seed_from_u64(1), Tear::None);
        assert!(!fs.exists(Path::new("/d/sub")).unwrap());

        // Recreate "/d/sub" and make *it* durable, but never sync it.
        fs.create_dir_all(Path::new("/d/sub")).unwrap();
        fs.sync_dir(Path::new("/d")).unwrap();

        fs.power_loss(&mut ChaCha8Rng::seed_from_u64(2), Tear::None);
        assert!(fs.exists(Path::new("/d/sub")).unwrap());
        assert!(
            !fs.exists(Path::new("/d/sub/a")).unwrap(),
            "a file the first crash lost came back after the second"
        );
    }

    /// TST-11: a durable removal (the directory's entry removal was synced)
    /// stays removed when the directory is recreated and a second crash
    /// hits.
    #[test]
    fn tst11_a_durably_removed_subdirectory_does_not_resurrect_its_children() {
        let fs = FaultFs::new();
        fs.mkdir_durable(Path::new("/d/sub")).unwrap();
        let f = fs.create(Path::new("/d/sub/a")).unwrap();
        f.write_at(0, b"x").unwrap();
        f.sync_data().unwrap();
        fs.sync_dir(Path::new("/d/sub")).unwrap();

        fs.remove(Path::new("/d/sub/a")).unwrap();
        fs.remove(Path::new("/d/sub")).unwrap();
        fs.sync_dir(Path::new("/d")).unwrap(); // "/d/sub" is durably gone

        fs.power_loss(&mut ChaCha8Rng::seed_from_u64(1), Tear::None);
        assert!(!fs.exists(Path::new("/d/sub")).unwrap());

        fs.create_dir_all(Path::new("/d/sub")).unwrap();
        fs.sync_dir(Path::new("/d")).unwrap();

        fs.power_loss(&mut ChaCha8Rng::seed_from_u64(2), Tear::None);
        assert!(fs.exists(Path::new("/d/sub")).unwrap());
        assert!(!fs.exists(Path::new("/d/sub/a")).unwrap());
    }

    /// TST-11: an unsynced removal and an unsynced creation, resolved by the
    /// first crash (removal reverted, creation dropped), stay resolved that
    /// way across a second crash.
    #[test]
    fn tst11_unsynced_entry_changes_stay_resolved_across_a_second_power_loss() {
        let fs = FaultFs::new();
        fs.mkdir_durable(Path::new("/d")).unwrap();
        let f = fs.create(Path::new("/d/kept")).unwrap();
        f.write_at(0, b"k").unwrap();
        f.sync_data().unwrap();
        fs.sync_dir(Path::new("/d")).unwrap();

        fs.remove(Path::new("/d/kept")).unwrap();
        let g = fs.create(Path::new("/d/new")).unwrap();
        g.write_at(0, b"n").unwrap();
        g.sync_data().unwrap();

        fs.power_loss(&mut ChaCha8Rng::seed_from_u64(1), Tear::None);
        let first = snapshot(&fs);
        assert_eq!(read_all(&fs, "/d/kept").unwrap(), b"k");
        assert!(!fs.exists(Path::new("/d/new")).unwrap());

        for (i, tear) in TEARS.into_iter().enumerate() {
            fs.power_loss(&mut ChaCha8Rng::seed_from_u64(10 + i as u64), tear);
            assert_eq!(snapshot(&fs), first, "second crash {tear:?}");
        }
    }

    /// TST-11, as a property: after any power loss, a second power loss with
    /// no intervening operations changes nothing, for every tear mode.
    #[test]
    fn tst11_power_loss_is_idempotent_without_intervening_writes() {
        for seed in 0..100u64 {
            let fs = FaultFs::new();
            fs.mkdir_durable(Path::new("/d")).unwrap();
            for (i, name) in ["/d/a", "/d/b", "/d/c"].into_iter().enumerate() {
                let f = fs.create(Path::new(name)).unwrap();
                f.write_at(0, &[b'0' + i as u8; 700]).unwrap();
                f.sync_data().unwrap();
            }
            fs.sync_dir(Path::new("/d")).unwrap();
            // Unsynced changes of every kind.
            let a = fs.open(Path::new("/d/a"), OpenMode::ReadWrite).unwrap();
            a.set_len(100).unwrap();
            a.write_at(100, &[b'x'; 900]).unwrap();
            let b = fs.open(Path::new("/d/b"), OpenMode::ReadWrite).unwrap();
            b.write_at(0, &[b'y'; 1200]).unwrap();
            let _c = fs.create(Path::new("/d/c")).unwrap();
            fs.remove(Path::new("/d/b")).unwrap();
            fs.create_dir_all(Path::new("/d/sub")).unwrap();
            fs.create(Path::new("/d/sub/e"))
                .unwrap()
                .sync_data()
                .unwrap();
            fs.sync_dir(Path::new("/d/sub")).unwrap();

            let mut rng = ChaCha8Rng::seed_from_u64(seed);
            let first_tear = Tear::random(&mut rng);
            fs.power_loss(&mut rng, first_tear);
            let first = snapshot(&fs);
            for second in TEARS {
                fs.power_loss(&mut rng, second);
                assert_eq!(snapshot(&fs), first, "seed {seed}, second {second:?}");
            }
        }
    }
}
