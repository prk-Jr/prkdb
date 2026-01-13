//! Mmap-based I/O implementation
//!
//! This is the fallback implementation that works on all platforms.
//! Uses memory-mapped files for efficient sequential writes.

use super::PlatformIO;
use async_trait::async_trait;
use memmap2::MmapMut;
use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Mutex;

/// Memory-mapped I/O implementation
pub struct MmapIO {
    file: File,
    mmap: Mutex<MmapMut>,
    file_size: AtomicU64,
    capacity: AtomicU64,
}

// Safety: MmapIO is safe to send/sync because we protect mmap access with Mutex
unsafe impl Send for MmapIO {}
unsafe impl Sync for MmapIO {}

impl MmapIO {
    /// Create a new mmap-based I/O handler
    pub fn new(path: &Path, initial_size: u64) -> io::Result<Self> {
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

        let mmap = unsafe { MmapMut::map_mut(&file)? };

        Ok(Self {
            file,
            mmap: Mutex::new(mmap),
            file_size: AtomicU64::new(0),
            capacity: AtomicU64::new(size),
        })
    }

    async fn resize(&self, new_size: u64) -> io::Result<()> {
        let mut mmap = self.mmap.lock().await;
        mmap.flush()?;
        self.file.set_len(new_size)?;
        *mmap = unsafe { MmapMut::map_mut(&self.file)? };
        self.capacity.store(new_size, Ordering::SeqCst);
        Ok(())
    }
}

#[async_trait]
impl PlatformIO for MmapIO {
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
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "write beyond mmap bounds",
            ));
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
        use std::os::unix::io::AsRawFd;

        self.sync().await?;

        let file_fd = self.file.as_raw_fd();

        #[cfg(target_os = "linux")]
        {
            let mut offset_val = offset as libc::off_t;
            let count = len as libc::size_t;

            let result = unsafe { libc::sendfile(socket_fd, file_fd, &mut offset_val, count) };

            if result < 0 {
                Err(io::Error::last_os_error())
            } else {
                Ok(result as u64)
            }
        }

        #[cfg(target_os = "macos")]
        {
            let mut len_sent: libc::off_t = len as libc::off_t;
            let ret = unsafe {
                libc::sendfile(
                    file_fd,
                    socket_fd,
                    offset as libc::off_t,
                    &mut len_sent,
                    std::ptr::null_mut(),
                    0,
                )
            };

            if ret == 0 || (ret == -1 && unsafe { *libc::__error() } == libc::EAGAIN) {
                Ok(len_sent as u64)
            } else {
                Err(io::Error::last_os_error())
            }
        }

        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "sendfile not supported",
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_mmap_io_write_read() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.dat");

        let io = MmapIO::new(&path, 1024 * 1024).unwrap();

        let data = b"Hello, World!";
        let written = io.write_at(data, 0).await.unwrap();
        assert_eq!(written, data.len());

        let mut buf = vec![0u8; data.len()];
        let read = io.read_at(&mut buf, 0).await.unwrap();
        assert_eq!(read, data.len());
        assert_eq!(&buf, data);
    }
}
