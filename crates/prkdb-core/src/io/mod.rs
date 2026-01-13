//! Platform I/O Abstraction Layer - Phase 24B
//!
//! Provides platform-specific optimized I/O operations:
//! - Linux 5.10+: io_uring (via mmap with future upgrade path)
//! - macOS: mmap with sendfile
//! - Fallback: Standard mmap
//!
//! Also provides `sendfile()` for zero-copy network transfer.

use async_trait::async_trait;
use std::io;
use std::path::Path;

mod mmap_io;
#[cfg(target_os = "linux")]
mod uring_io;

pub use mmap_io::MmapIO;
#[cfg(target_os = "linux")]
pub use uring_io::UringIO;

/// Platform I/O trait for high-performance file operations
///
/// Implementations provide optimized I/O based on the platform:
/// - `io_uring` on Linux 5.10+ (batched async syscalls)
/// - `mmap` fallback on other platforms
#[async_trait]
pub trait PlatformIO: Send + Sync {
    /// Write data at the specified offset
    async fn write_at(&self, buf: &[u8], offset: u64) -> io::Result<usize>;

    /// Read data at the specified offset
    async fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize>;

    /// Sync data to disk
    async fn sync(&self) -> io::Result<()>;

    /// Get current file size
    fn file_size(&self) -> u64;

    /// Zero-copy network transfer (sendfile)
    ///
    /// Transfers data directly from file to network socket without
    /// copying through user-space. This is Kafka's secret weapon.
    #[cfg(unix)]
    async fn sendfile(
        &self,
        socket_fd: std::os::unix::io::RawFd,
        offset: u64,
        len: u64,
    ) -> io::Result<u64>;
}

/// Create the best available PlatformIO for this system
pub async fn create_platform_io(path: &Path, size: u64) -> io::Result<Box<dyn PlatformIO>> {
    // On Linux, try io_uring first
    #[cfg(target_os = "linux")]
    {
        if is_iouring_available() {
            match UringIO::new(path, size).await {
                Ok(io) => return Ok(Box::new(io)),
                Err(e) => {
                    tracing::warn!("io_uring unavailable, falling back to mmap: {}", e);
                }
            }
        }
    }

    // Fallback to mmap
    Ok(Box::new(MmapIO::new(path, size)?))
}

/// Check if io_uring is available on this system
#[cfg(target_os = "linux")]
fn is_iouring_available() -> bool {
    use std::fs::File;
    use std::io::Read;

    if let Ok(mut file) = File::open("/proc/version") {
        let mut version = String::new();
        if file.read_to_string(&mut version).is_ok() {
            if let Some(version_str) = version.split_whitespace().nth(2) {
                let parts: Vec<&str> = version_str.split('.').collect();
                if parts.len() >= 2 {
                    let major: u32 = parts[0].parse().unwrap_or(0);
                    let minor: u32 = parts[1].parse().unwrap_or(0);
                    return major > 5 || (major == 5 && minor >= 10);
                }
            }
        }
    }
    false
}

#[cfg(not(target_os = "linux"))]
#[allow(dead_code)]
fn is_iouring_available() -> bool {
    false
}
