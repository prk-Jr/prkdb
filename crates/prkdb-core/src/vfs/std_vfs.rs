use super::{Vfs, VfsFile};
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
        #[cfg(windows)]
        {
            use std::os::windows::fs::FileExt;
            let mut done = 0;
            while done < buf.len() {
                done += self.0.seek_write(&buf[done..], offset + done as u64)?;
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
                let r = self.0.read_at(&mut buf[n..], offset + n as u64)?;
                if r == 0 {
                    break;
                }
                n += r;
            }
            Ok(n)
        }
        #[cfg(windows)]
        {
            use std::os::windows::fs::FileExt;
            self.0.seek_read(buf, offset)
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

impl Vfs for StdVfs {
    fn open(&self, path: &Path) -> io::Result<Arc<dyn VfsFile>> {
        Ok(Arc::new(StdFile(
            OpenOptions::new().read(true).write(true).open(path)?,
        )))
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
    fn exists(&self, path: &Path) -> bool {
        path.exists()
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
}
