//! io_uring-based I/O implementation for Linux
//!
//! Uses mmap as a foundation with sendfile() for network transfers.
//! Full io_uring support requires tokio-uring runtime.

use super::PlatformIO;
use async_trait::async_trait;
use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::io::AsRawFd;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Mutex;

/// io_uring-based I/O for Linux (currently uses mmap with sendfile)
pub struct UringIO {
    file: File,
    file_size: AtomicU64,
    capacity: AtomicU64,
    mmap: Mutex<memmap2::MmapMut>,
}

unsafe impl Send for UringIO {}
unsafe impl Sync for UringIO {}

impl UringIO {
    pub async fn new(path: &Path, initial_size: u64) -> io::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        let size = std::cmp::max(initial_size, 64 * 1024 * 1024);
        file.set_len(size)?;

        let mmap = unsafe { memmap2::MmapMut::map_mut(&file)? };

        Ok(Self {
            file,
            file_size: AtomicU64::new(0),
            capacity: AtomicU64::new(size),
            mmap: Mutex::new(mmap),
        })
    }

    async fn resize(&self, new_size: u64) -> io::Result<()> {
        let mut mmap = self.mmap.lock().await;
        mmap.flush()?;
        self.file.set_len(new_size)?;
        *mmap = unsafe { memmap2::MmapMut::map_mut(&self.file)? };
        self.capacity.store(new_size, Ordering::SeqCst);
        Ok(())
    }
}

#[async_trait]
impl PlatformIO for UringIO {
    async fn write_at(&self, buf: &[u8], offset: u64) -> io::Result<usize> {
        let end = offset + buf.len() as u64;
        let capacity = self.capacity.load(Ordering::SeqCst);

        if end > capacity {
            let new_size = ((end / (64 * 1024 * 1024)) + 1) * (64 * 1024 * 1024);
            self.resize(new_size).await?;
        }

        let mmap = self.mmap.lock().await;
        let start = offset as usize;
        let end_usize = start + buf.len();

        if end_usize > mmap.len() {
            return Err(io::Error::new(io::ErrorKind::Other, "write beyond bounds"));
        }

        unsafe {
            let ptr = mmap.as_ptr() as *mut u8;
            std::ptr::copy_nonoverlapping(buf.as_ptr(), ptr.add(start), buf.len());
        }

        let current_size = self.file_size.load(Ordering::SeqCst);
        if end > current_size {
            self.file_size.store(end, Ordering::SeqCst);
        }

        Ok(buf.len())
    }

    async fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        let mmap = self.mmap.lock().await;
        let start = offset as usize;
        let len = std::cmp::min(buf.len(), mmap.len().saturating_sub(start));

        if len == 0 {
            return Ok(0);
        }

        buf[..len].copy_from_slice(&mmap[start..start + len]);
        Ok(len)
    }

    async fn sync(&self) -> io::Result<()> {
        let mmap = self.mmap.lock().await;
        mmap.flush()
    }

    fn file_size(&self) -> u64 {
        self.file_size.load(Ordering::SeqCst)
    }

    #[cfg(unix)]
    async fn sendfile(
        &self,
        socket_fd: std::os::unix::io::RawFd,
        offset: u64,
        len: u64,
    ) -> io::Result<u64> {
        self.sync().await?;

        let file_fd = self.file.as_raw_fd();
        let mut offset_val = offset as libc::off_t;
        let count = len as libc::size_t;

        let result = unsafe { libc::sendfile(socket_fd, file_fd, &mut offset_val, count) };

        if result < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(result as u64)
        }
    }
}
