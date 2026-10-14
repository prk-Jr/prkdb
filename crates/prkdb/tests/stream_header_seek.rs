//! The stream wrapper freezes retention/index/cap before header-only I/O.
use prkdb::stream_log::{Record, StreamConfig, StreamLog};
use prkdb_core::vfs::{LockGuard, OpenMode, StdVfs, Vfs, VfsFile};
use prkdb_core::wal::{CompressionConfig, SyncMode};
use prkdb_types::error::StorageError;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Default)]
struct Probe {
    armed: AtomicBool,
    reads: Mutex<Vec<(PathBuf, u64, usize)>>,
    fail: AtomicBool,
}
#[derive(Clone, Default)]
struct ProbeVfs(Arc<Probe>);
struct ProbeFile {
    inner: Arc<dyn VfsFile>,
    path: PathBuf,
    probe: Arc<Probe>,
}
impl VfsFile for ProbeFile {
    fn read_at(&self, o: u64, b: &mut [u8]) -> io::Result<usize> {
        if self.probe.armed.load(Ordering::SeqCst) {
            self.probe
                .reads
                .lock()
                .unwrap()
                .push((self.path.clone(), o, b.len()));
            if self.probe.fail.load(Ordering::SeqCst) {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "injected header I/O",
                ));
            }
        }
        self.inner.read_at(o, b)
    }
    fn write_at(&self, o: u64, b: &[u8]) -> io::Result<()> {
        self.inner.write_at(o, b)
    }
    fn len(&self) -> io::Result<u64> {
        self.inner.len()
    }
    fn set_len(&self, n: u64) -> io::Result<()> {
        self.inner.set_len(n)
    }
    fn sync_data(&self) -> io::Result<()> {
        self.inner.sync_data()
    }
}
impl ProbeVfs {
    fn wrap(&self, p: &Path, inner: Arc<dyn VfsFile>) -> Arc<dyn VfsFile> {
        Arc::new(ProbeFile {
            inner,
            path: p.into(),
            probe: self.0.clone(),
        })
    }
    fn arm(&self) {
        self.0.reads.lock().unwrap().clear();
        self.0.armed.store(true, Ordering::SeqCst);
    }
    fn reads(&self) -> Vec<(PathBuf, u64, usize)> {
        self.0.reads.lock().unwrap().clone()
    }
}
impl Vfs for ProbeVfs {
    fn open(&self, p: &Path, m: OpenMode) -> io::Result<Arc<dyn VfsFile>> {
        Ok(self.wrap(p, StdVfs.open(p, m)?))
    }
    fn create(&self, p: &Path) -> io::Result<Arc<dyn VfsFile>> {
        Ok(self.wrap(p, StdVfs.create(p)?))
    }
    fn rename(&self, p: &Path, q: &Path) -> io::Result<()> {
        StdVfs.rename(p, q)
    }
    fn remove(&self, p: &Path) -> io::Result<()> {
        StdVfs.remove(p)
    }
    fn create_dir_all(&self, p: &Path) -> io::Result<()> {
        StdVfs.create_dir_all(p)
    }
    fn read_dir(&self, p: &Path) -> io::Result<Vec<PathBuf>> {
        StdVfs.read_dir(p)
    }
    fn exists(&self, p: &Path) -> io::Result<bool> {
        StdVfs.exists(p)
    }
    fn sync_dir(&self, p: &Path) -> io::Result<()> {
        StdVfs.sync_dir(p)
    }
    fn lock_exclusive(&self, p: &Path) -> io::Result<Box<dyn LockGuard>> {
        StdVfs.lock_exclusive(p)
    }
}
fn config(p: &Path) -> StreamConfig {
    let mut cfg = StreamConfig::new(p);
    cfg.wal.sync_mode = SyncMode::Fast;
    cfg.wal.sync_interval_ms = 3600000;
    cfg.wal.compression = CompressionConfig::none();
    cfg.segment_max_age = Some(Duration::MAX);
    cfg.retention_interval = Duration::ZERO;
    cfg
}
fn record(n: usize) -> Record {
    Record {
        value: vec![7; n],
        ..Record::default()
    }
}
fn segments(p: &Path) -> Vec<PathBuf> {
    let mut paths = std::fs::read_dir(p)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "wal"))
        .collect::<Vec<_>>();
    paths.sort();
    paths
}
#[tokio::test]
async fn stream_crc_caps_require_no_payload_reads_and_survive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let fs = ProbeVfs::default();
    let cfg = config(dir.path());
    let log = StreamLog::open_with_vfs(Arc::new(fs.clone()), cfg.clone())
        .await
        .unwrap();
    log.append(vec![record(1 << 20)]).await.unwrap();
    let bytes = std::fs::read(&segments(dir.path())[0]).unwrap();
    let expected = u32::from_le_bytes(bytes[28..32].try_into().unwrap());
    fs.arm();
    assert_eq!(log.frame_crc(0, false).await.unwrap(), None);
    assert_eq!(log.frame_crc(2, false).await.unwrap(), None);
    assert_eq!(log.frame_crc(1, true).await.unwrap(), None);
    assert!(fs.reads().is_empty());
    assert_eq!(log.frame_crc(1, false).await.unwrap(), Some(expected));
    assert_eq!(
        fs.reads()
            .iter()
            .map(|(_, o, n)| (*o, *n))
            .collect::<Vec<_>>(),
        vec![(24, 17)]
    );
    log.sync().await.unwrap();
    assert_eq!(log.frame_crc(1, true).await.unwrap(), Some(expected));
    log.close().unwrap();
    fs.0.armed.store(false, Ordering::SeqCst);
    let log = StreamLog::open_with_vfs(Arc::new(fs.clone()), cfg)
        .await
        .unwrap();
    fs.arm();
    assert_eq!(log.frame_crc(1, true).await.unwrap(), Some(expected));
    assert_eq!(fs.reads().len(), 1);
    log.close().unwrap();
}
#[tokio::test]
async fn stream_crc_uses_sparse_hint_at_stride_and_large_payload_boundaries() {
    let dir = tempfile::tempdir().unwrap();
    let fs = ProbeVfs::default();
    let log = StreamLog::open_with_vfs(Arc::new(fs.clone()), config(dir.path()))
        .await
        .unwrap();
    for _ in 0..1800 {
        log.append(vec![record(0)]).await.unwrap();
    }
    let bytes = std::fs::read(&segments(dir.path())[0]).unwrap();
    let mut at = 24usize;
    let mut hint = 24usize;
    let mut locs = Vec::new();
    let mut last_crc = 0;
    while at < bytes.len() {
        if at - hint >= 65536 {
            hint = at;
        }
        locs.push(at);
        last_crc = u32::from_le_bytes(bytes[at + 4..at + 8].try_into().unwrap());
        at += 17 + u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap()) as usize;
    }
    fs.arm();
    assert_eq!(log.frame_crc(1800, false).await.unwrap(), Some(last_crc));
    let reads = fs.reads();
    let expected = locs.iter().filter(|o| **o >= hint).count();
    assert_eq!(reads.len(), expected);
    assert!(expected > 1 && expected <= 1873);
    for (_, o, n) in reads {
        assert_eq!(n, 17);
        assert!(locs.contains(&(o as usize)));
    }
    log.append(vec![record(1 << 20)]).await.unwrap();
    log.append(vec![record(0)]).await.unwrap();
    fs.arm();
    assert!(log.frame_crc(1802, false).await.unwrap().is_some());
    assert_eq!(
        fs.reads().len(),
        1,
        "large frame's successor is indexed exactly"
    );
    log.close().unwrap();
}
#[tokio::test]
async fn stream_crc_refuses_corrupt_and_io_headers_without_absence_or_reset() {
    let dir = tempfile::tempdir().unwrap();
    let fs = ProbeVfs::default();
    let log = StreamLog::open_with_vfs(Arc::new(fs.clone()), config(dir.path()))
        .await
        .unwrap();
    log.append(vec![record(1)]).await.unwrap();
    fs.arm();
    fs.0.fail.store(true, Ordering::SeqCst);
    let err = log.frame_crc(1, false).await.unwrap_err();
    assert!(matches!(err, StorageError::Internal(_)));
    assert!(err.to_string().contains("00000000000000000001.wal"));
    fs.0.fail.store(false, Ordering::SeqCst);
    let file = StdVfs
        .open(&segments(dir.path())[0], OpenMode::ReadWrite)
        .unwrap();
    file.write_at(40, &[2]).unwrap();
    fs.arm();
    assert!(matches!(
        log.frame_crc(1, false).await,
        Err(StorageError::Corruption(_))
    ));
    assert_eq!(fs.reads().len(), 1);
    log.close().unwrap();
}
#[tokio::test]
async fn stream_crc_returns_none_only_below_the_durable_retention_floor() {
    let dir = tempfile::tempdir().unwrap();
    let fs = ProbeVfs::default();
    let mut cfg = config(dir.path());
    cfg.wal.segment_bytes = 4096;
    cfg.retention.max_bytes = Some(0);
    let log = StreamLog::open_with_vfs(Arc::new(fs.clone()), cfg)
        .await
        .unwrap();
    log.append(vec![record(3000)]).await.unwrap();
    log.append(vec![record(3000)]).await.unwrap();
    assert_eq!(log.apply_retention().unwrap().segments_removed, 1);
    log.sync().await.unwrap();
    fs.arm();
    assert_eq!(log.frame_crc(1, false).await.unwrap(), None);
    assert!(fs.reads().is_empty());
    assert!(log.frame_crc(2, true).await.unwrap().is_some());
    log.close().unwrap();
}
