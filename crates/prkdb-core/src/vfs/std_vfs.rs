use super::{LockGuard, OpenMode, Vfs, VfsFile};
use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

#[derive(Debug, Default, Clone, Copy)]
pub struct StdVfs;

struct StdFile(File);

impl VfsFile for StdFile {
    fn write_at(&self, offset: u64, buf: &[u8]) -> io::Result<()> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            self.0.write_all_at(buf, offset)
        }
        #[cfg(not(unix))]
        {
            use std::os::windows::fs::FileExt;
            let mut done = 0;
            while done < buf.len() {
                let n = self.0.seek_write(&buf[done..], offset + done as u64)?;
                if n == 0 {
                    return Err(io::Error::from(io::ErrorKind::WriteZero));
                }
                done += n;
            }
            Ok(())
        }
    }
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            let mut n = 0;
            while n < buf.len() {
                match self.0.read_at(&mut buf[n..], offset + n as u64) {
                    Ok(0) => break,
                    Ok(r) => n += r,
                    Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                    Err(e) => return Err(e),
                }
            }
            Ok(n)
        }
        #[cfg(not(unix))]
        {
            use std::os::windows::fs::FileExt;
            let mut n = 0;
            while n < buf.len() {
                let r = self.0.seek_read(&mut buf[n..], offset + n as u64)?;
                if r == 0 {
                    break;
                }
                n += r;
            }
            Ok(n)
        }
    }
    fn set_len(&self, len: u64) -> io::Result<()> {
        self.0.set_len(len)
    }
    fn len(&self) -> io::Result<u64> {
        Ok(self.0.metadata()?.len())
    }
    fn sync_data(&self) -> io::Result<()> {
        self.0.sync_data()
    }
}

/// An OS file lock (`std::fs::File::try_lock`: `flock(LOCK_EX | LOCK_NB)` on Unix,
/// `LockFileEx` on Windows), released when the file is closed, which the OS also does when
/// the process dies, plus this process's [`ProcessLocks`] entry for the same file.
struct StdLock {
    file: Option<File>,
    key: LockKey,
}

impl LockGuard for StdLock {}

impl Drop for StdLock {
    fn drop(&mut self) {
        // Unlock explicitly, then close, then drop the registry entry, so a thread that
        // gets past the registry never meets this guard's OS lock.
        //
        // Closing alone is not enough (STO-13). The OS lock belongs to the open file
        // description, which every descriptor duplicated from ours shares, and a child
        // process that another thread is spawning holds such a duplicate from its fork
        // until its exec closes it (`O_CLOEXEC` takes effect only at the exec). Closing
        // ours while a child is in that window left the lock held, and a reopen right
        // after the drop was refused as `Locked`. An unlock releases the description's
        // lock whichever descriptors are still open. If it fails, the close still
        // releases it (eventually, once every duplicate is closed).
        let error = ProcessLocks::release_after(&self.key, || {
            self.file.take().and_then(|file| {
                let error = file.unlock().err();
                drop(file);
                error
            })
        });
        if let Some(error) = error {
            tracing::warn!(%error, "explicit directory unlock failed; close releases the lock after inherited descriptors close");
        }
    }
}

/// Identifies a lock file independently of the path it was opened by: the inode on Unix
/// (hard links and bind mounts alias), the canonical path elsewhere.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum LockKey {
    #[cfg(unix)]
    Inode { dev: u64, ino: u64 },
    #[cfg(not(unix))]
    Path(PathBuf),
}

impl LockKey {
    fn of(file: &File, path: &Path) -> io::Result<Self> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let _ = path;
            let meta = file.metadata()?;
            Ok(Self::Inode {
                dev: meta.dev(),
                ino: meta.ino(),
            })
        }
        #[cfg(not(unix))]
        {
            let _ = file;
            Ok(Self::Path(fs::canonicalize(path)?))
        }
    }
}

/// Every lock file this process holds a [`StdLock`] on. The OS lock alone is not enough
/// within one process wherever `flock` is emulated with per-process `fcntl` locks (Linux
/// NFS): there a second open by the same process would succeed, and closing either
/// descriptor would drop the lock for both. Checking this set first makes an in-process
/// double lock fail everywhere.
struct ProcessLocks;

static PROCESS_LOCKS: Mutex<Option<HashSet<LockKey>>> = Mutex::new(None);

#[cfg(test)]
#[derive(Debug)]
enum AcquireObservation {
    Acquired,
    Contended,
}
#[cfg(test)]
thread_local! {
    // Only the regression's contender observes acquisition. Other test threads
    // and all production acquisitions retain the ordinary blocking lock path.
    static ACQUIRE_OBSERVER: std::cell::RefCell<Option<std::sync::mpsc::Sender<AcquireObservation>>> = const { std::cell::RefCell::new(None) };
}

impl ProcessLocks {
    fn table() -> std::sync::MutexGuard<'static, Option<HashSet<LockKey>>> {
        #[cfg(test)]
        if let Some(observer) = ACQUIRE_OBSERVER.with(|slot| slot.borrow().clone()) {
            match PROCESS_LOCKS.try_lock() {
                Ok(table) => {
                    let _ = observer.send(AcquireObservation::Acquired);
                    return table;
                }
                Err(std::sync::TryLockError::Poisoned(error)) => {
                    let table = error.into_inner();
                    let _ = observer.send(AcquireObservation::Acquired);
                    return table;
                }
                Err(std::sync::TryLockError::WouldBlock) => {
                    let _ = observer.send(AcquireObservation::Contended);
                }
            }
        }
        PROCESS_LOCKS.lock().unwrap_or_else(|p| p.into_inner())
    }
    /// `false` if this process already holds `key`.
    fn acquire(key: &LockKey) -> bool {
        let mut set = Self::table();
        set.get_or_insert_with(HashSet::new).insert(key.clone())
    }

    fn release_after<T>(key: &LockKey, retire: impl FnOnce() -> T) -> T {
        // The final close can make an unlinked inode available for reuse. Do not
        // let a new owner inspect its identity until the old entry is removed.
        let mut set = Self::table();
        let result = retire();
        if let Some(set) = set.as_mut() {
            set.remove(key);
        }
        result
    }

    fn release(key: &LockKey) {
        let mut set = PROCESS_LOCKS.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(set) = set.as_mut() {
            set.remove(key);
        }
    }

    #[cfg(test)]
    fn holds(key: &LockKey) -> bool {
        let set = PROCESS_LOCKS.lock().unwrap_or_else(|p| p.into_inner());
        set.as_ref().is_some_and(|set| set.contains(key))
    }
}

fn held(path: &Path) -> io::Error {
    io::Error::new(
        io::ErrorKind::WouldBlock,
        format!("{} is locked", path.display()),
    )
}

impl Vfs for StdVfs {
    fn open(&self, path: &Path, mode: OpenMode) -> io::Result<Arc<dyn VfsFile>> {
        let file = match mode {
            OpenMode::Read => OpenOptions::new().read(true).open(path)?,
            OpenMode::ReadWrite => OpenOptions::new().read(true).write(true).open(path)?,
        };
        Ok(Arc::new(StdFile(file)))
    }
    fn create(&self, path: &Path) -> io::Result<Arc<dyn VfsFile>> {
        Ok(Arc::new(StdFile(
            OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(path)?,
        )))
    }
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        fs::rename(from, to)
    }
    fn remove(&self, path: &Path) -> io::Result<()> {
        fs::remove_file(path)
    }
    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        fs::create_dir_all(path)
    }
    fn read_dir(&self, path: &Path) -> io::Result<Vec<PathBuf>> {
        let mut v: Vec<PathBuf> = fs::read_dir(path)?
            .map(|e| e.map(|e| e.path()))
            .collect::<Result<_, _>>()?;
        v.sort();
        Ok(v)
    }
    fn exists(&self, path: &Path) -> io::Result<bool> {
        path.try_exists()
    }
    fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        #[cfg(unix)]
        {
            let dir = if dir.as_os_str().is_empty() {
                Path::new(".")
            } else {
                dir
            };
            File::open(dir)?.sync_all()
        }
        #[cfg(not(unix))]
        {
            let _ = dir;
            Ok(())
        }
    }
    fn lock_exclusive(&self, path: &Path) -> io::Result<Box<dyn LockGuard>> {
        Ok(Box::new(lock_file(path)?))
    }
}

/// [`Vfs::lock_exclusive`] for [`StdVfs`], unboxed so the tests can reach the file.
fn lock_file(path: &Path) -> io::Result<StdLock> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)?;
    let key = LockKey::of(&file, path)?;
    if !ProcessLocks::acquire(&key) {
        return Err(held(path));
    }
    if let Err(e) = file.try_lock() {
        ProcessLocks::release(&key);
        return Err(match e {
            fs::TryLockError::WouldBlock => held(path),
            fs::TryLockError::Error(e) => e,
        });
    }
    // Who holds it, for the refusal message only: best effort and never synced, so a
    // failure here does not fail the lock, and a reader treats anything unparsable as
    // "unknown".
    let pid = format!("{}\n", std::process::id());
    if file.set_len(0).is_ok() {
        let _ = io::Write::write_all(&mut &file, pid.as_bytes());
    }
    Ok(StdLock {
        file: Some(file),
        key,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The process-wide registry refuses an in-process second lock by itself, as it must
    /// where the OS lock is per process (Linux NFS), and the guard's drop clears it.
    #[test]
    fn the_process_registry_refuses_an_in_process_double_lock() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("LOCK");
        let file = File::create(&path).unwrap();
        let key = LockKey::of(&file, &path).unwrap();

        // Registered without any OS lock: the registry alone must refuse.
        assert!(ProcessLocks::acquire(&key));
        let err = StdVfs.lock_exclusive(&path).err().expect("refused");
        assert_eq!(err.kind(), io::ErrorKind::WouldBlock);
        ProcessLocks::release(&key);

        let guard = StdVfs.lock_exclusive(&path).unwrap();
        assert!(ProcessLocks::holds(&key));
        // Another path to the same file is the same lock.
        #[cfg(unix)]
        {
            let link = dir.path().join("LOCK.link");
            fs::hard_link(&path, &link).unwrap();
            let err = StdVfs.lock_exclusive(&link).err().expect("same inode");
            assert_eq!(err.kind(), io::ErrorKind::WouldBlock);
        }
        drop(guard);
        assert!(!ProcessLocks::holds(&key));
        drop(StdVfs.lock_exclusive(&path).unwrap());
    }

    /// Dropping the guard releases the lock even while another descriptor of the same
    /// open file description is still open. That is what a child process spawned on
    /// another thread holds between its fork and its exec: it inherits every descriptor,
    /// `O_CLOEXEC` ones included until the exec, and a `flock` belongs to the open file
    /// description, not the descriptor. Closing ours alone left the lock held until the
    /// child exec'd, so a reopen right after a drop was refused as locked (STO-13).
    #[test]
    fn dropping_the_guard_releases_the_lock_while_a_duplicate_descriptor_is_open() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("LOCK");
        let guard = lock_file(&path).unwrap();
        // The child's inherited copy: the same open file description.
        let inherited = guard.file.as_ref().unwrap().try_clone().unwrap();
        drop(guard);
        let reopened = StdVfs.lock_exclusive(&path);
        drop(inherited);
        assert!(
            reopened.is_ok(),
            "the lock outlived its guard: {:?}",
            reopened.err()
        );
    }

    #[cfg(unix)]
    #[test]
    fn a_reused_file_identity_is_not_refused_while_the_old_guard_retires() {
        // Represent a new lock inode reusing a retired inode's identity. No filesystem
        // allocation timing is involved; exercise the actual registry handoff.
        let key = LockKey::Inode {
            dev: u64::MAX,
            ino: u64::MAX,
        };
        assert!(ProcessLocks::acquire(&key));
        let (retiring_tx, retiring_rx) = std::sync::mpsc::channel();
        let (finish_tx, finish_rx) = std::sync::mpsc::channel();
        let (observed_tx, observed_rx) = std::sync::mpsc::channel();
        std::thread::scope(|threads| {
            let retiring_key = key.clone();
            let retiring = threads.spawn(move || {
                ProcessLocks::release_after(&retiring_key, || {
                    retiring_tx.send(()).unwrap();
                    finish_rx.recv().unwrap();
                });
            });
            retiring_rx.recv().unwrap();
            let contender = threads.spawn(|| {
                ACQUIRE_OBSERVER.with(|slot| *slot.borrow_mut() = Some(observed_tx));
                let acquired = ProcessLocks::acquire(&key);
                ACQUIRE_OBSERVER.with(|slot| *slot.borrow_mut() = None);
                acquired
            });
            // Acquired carries a retained table guard; Contended precedes its blocking
            // acquisition. Either marker fixes the schedule before retirement ends.
            let observation = observed_rx.recv().unwrap();
            finish_tx.send(()).unwrap();
            retiring.join().unwrap();
            let acquired = contender.join().unwrap();
            ProcessLocks::release(&key);
            assert!(
                acquired,
                "fresh lock identity spuriously refused during retirement: {observation:?}"
            );
        });
    }
}
