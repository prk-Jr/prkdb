use super::{LockGuard, OpenMode, Vfs, VfsFile};
use std::fs::{self, File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

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
/// the process dies.
struct StdLock {
    _file: File,
}

impl LockGuard for StdLock {}

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
            File::open(dir)?.sync_all()
        }
        #[cfg(not(unix))]
        {
            let _ = dir;
            Ok(())
        }
    }
    fn lock_exclusive(&self, path: &Path) -> io::Result<Box<dyn LockGuard>> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        match file.try_lock() {
            Ok(()) => {}
            Err(fs::TryLockError::WouldBlock) => {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    format!("{} is locked", path.display()),
                ))
            }
            Err(fs::TryLockError::Error(e)) => return Err(e),
        }
        // Who holds it, for the refusal message only: best effort and never synced, so a
        // failure here does not fail the lock, and a reader treats anything unparsable as
        // "unknown".
        let pid = format!("{}\n", std::process::id());
        if file.set_len(0).is_ok() {
            let _ = io::Write::write_all(&mut &file, pid.as_bytes());
        }
        Ok(Box::new(StdLock { _file: file }))
    }
}
