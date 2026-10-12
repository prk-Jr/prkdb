//! Runtime witnesses for reuse of a CRC-validated sparse-hint frame.
use parking_lot::{Condvar, Mutex};
use prkdb::stream_log::{
    Clock, EventSeq, ReadLimits, Record, StartAt, StoredRecord, StreamConfig, StreamLog,
};
use prkdb_core::vfs::{LockGuard, OpenMode, StdVfs, Vfs, VfsFile};
use prkdb_core::wal::frame::{encode_frame, FrameKind, FRAME_HEADER_LEN};
use prkdb_core::wal::records::{RecordBatch, RECORDS_HEADER_LEN};
use prkdb_core::wal::segment::{segment_file_name, SEGMENT_HEADER_LEN};
use prkdb_core::wal::{CompressionConfig, SyncMode};
use prkdb_types::error::StorageError;
use std::{
    io,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

const TIME_MS: i64 = 1234;
const MIB: usize = 1024 * 1024;

#[derive(Default)]
struct Observer {
    reads: Mutex<Vec<(u64, usize)>>,
    // Pause only the segment-header read: hint validation has completed by then.
    pause_header: Mutex<bool>,
    released: Mutex<bool>,
    resume: Condvar,
    entered: tokio::sync::Notify,
    unlocked: tokio::sync::Notify,
}

impl Observer {
    fn take(&self) -> Vec<(u64, usize)> {
        std::mem::take(&mut *self.reads.lock())
    }

    fn release(&self) {
        *self.released.lock() = true;
        self.resume.notify_all();
    }
}

struct CountingVfs(Arc<Observer>);
struct ReleaseOnDrop(Arc<Observer>);
impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        self.0.release();
    }
}
struct CountingFile {
    inner: Arc<dyn VfsFile>,
    observer: Arc<Observer>,
}
struct CountingLock {
    inner: Option<Box<dyn LockGuard>>,
    observer: Arc<Observer>,
}
impl LockGuard for CountingLock {}
impl Drop for CountingLock {
    fn drop(&mut self) {
        drop(self.inner.take());
        self.observer.unlocked.notify_one();
    }
}

impl VfsFile for CountingFile {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        if offset == 0 && std::mem::take(&mut *self.observer.pause_header.lock()) {
            self.observer.entered.notify_one();
            let mut released = self.observer.released.lock();
            while !*released {
                self.observer.resume.wait(&mut released);
            }
        }
        let n = self.inner.read_at(offset, buf)?;
        self.observer.reads.lock().push((offset, n));
        Ok(n)
    }
    fn write_at(&self, offset: u64, buf: &[u8]) -> io::Result<()> {
        self.inner.write_at(offset, buf)
    }
    fn set_len(&self, len: u64) -> io::Result<()> {
        self.inner.set_len(len)
    }
    fn len(&self) -> io::Result<u64> {
        self.inner.len()
    }
    fn sync_data(&self) -> io::Result<()> {
        self.inner.sync_data()
    }
}

impl CountingVfs {
    fn wrap(&self, path: &Path, inner: Arc<dyn VfsFile>) -> Arc<dyn VfsFile> {
        if path.extension().is_some_and(|ext| ext == "wal") {
            Arc::new(CountingFile {
                inner,
                observer: self.0.clone(),
            })
        } else {
            inner
        }
    }
}
impl Vfs for CountingVfs {
    fn open(&self, p: &Path, m: OpenMode) -> io::Result<Arc<dyn VfsFile>> {
        Ok(self.wrap(p, StdVfs.open(p, m)?))
    }
    fn create(&self, p: &Path) -> io::Result<Arc<dyn VfsFile>> {
        Ok(self.wrap(p, StdVfs.create(p)?))
    }
    fn rename(&self, a: &Path, b: &Path) -> io::Result<()> {
        StdVfs.rename(a, b)
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
        Ok(Box::new(CountingLock {
            inner: Some(StdVfs.lock_exclusive(p)?),
            observer: self.0.clone(),
        }))
    }
}

struct FixedClock;
impl Clock for FixedClock {
    fn now_ms(&self) -> i64 {
        TIME_MS
    }
}
fn config(path: &Path) -> StreamConfig {
    let mut cfg = StreamConfig::new(path);
    cfg.clock = Arc::new(FixedClock);
    cfg.retention_interval = Duration::ZERO;
    cfg.wal.sync_mode = SyncMode::Fast;
    cfg.wal.compression = CompressionConfig::none();
    cfg.wal.segment_bytes = (32 * MIB) as u64;
    cfg
}
fn input(id: usize) -> Record {
    let mut value = vec![(id % 251) as u8; 1024];
    value[..8].copy_from_slice(&(id as u64).to_le_bytes());
    Record {
        key: None,
        value,
        headers: vec![],
    }
}
fn stored(record: Record, offset: EventSeq) -> StoredRecord {
    StoredRecord {
        offset,
        append_time_ms: TIME_MS,
        key: record.key,
        value: record.value,
        headers: record.headers,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn hinted_b100_read_reuses_the_validated_frame() {
    let dir = tempfile::tempdir().unwrap();
    let observer = Arc::new(Observer::default());
    let log = StreamLog::open_with_vfs(Arc::new(CountingVfs(observer.clone())), config(dir.path()))
        .await
        .unwrap();
    // Pin the original public audit's no-key/no-header shape and input-derived output.
    let records: Vec<_> = (0..100).map(input).collect();
    let ack = log.append(records.clone()).await.unwrap();
    assert_eq!(ack.count, 100);
    let expected: Vec<_> = records
        .into_iter()
        .enumerate()
        .map(|(i, record)| stored(record, EventSeq::from_wal(ack.lsn, i as u16)))
        .collect();
    let watermark = EventSeq::from_wal(ack.lsn + 1, 0);
    assert_eq!(log.next_offset(), watermark);
    assert_eq!(log.sync().await.unwrap(), watermark);
    assert_eq!(log.durable_end(), watermark);
    observer.take();
    let read = log
        .read_from(
            StartAt::Offset(expected[0].offset),
            ReadLimits {
                max_records: 100,
                max_bytes: MIB,
            },
        )
        .await
        .unwrap();
    assert_eq!(read.records, expected);
    assert_eq!(read.next, EventSeq::from_wal(ack.lsn, 100));
    assert_eq!(read.high_watermark, watermark);
    let trace = observer.take();
    let returned: usize = trace.iter().map(|(_, n)| n).sum();
    log.close().unwrap();
    assert_eq!(
        (trace.len(), returned),
        (3, 102976),
        "WAL read_at trace: {trace:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn hinted_read_preserves_keys_headers_time_offsets_and_resume() {
    let dir = tempfile::tempdir().unwrap();
    let log = StreamLog::open(config(dir.path())).await.unwrap();
    let records: Vec<_> = (0..3)
        .map(|i| {
            let mut record = input(i);
            record.key = Some(format!("k{i:06}").into_bytes());
            record.headers = vec![
                ("identity".into(), vec![i as u8]),
                ("identity".into(), vec![]),
            ];
            record
        })
        .collect();
    let ack = log.append(records.clone()).await.unwrap();
    let watermark = EventSeq::from_wal(ack.lsn + 1, 0);
    assert_eq!(log.sync().await.unwrap(), watermark);
    let expected: Vec<_> = records
        .into_iter()
        .enumerate()
        .map(|(i, record)| stored(record, EventSeq::from_wal(ack.lsn, i as u16)))
        .collect();
    for i in 0..3 {
        let read = log
            .read_from(
                StartAt::Offset(expected[i].offset),
                ReadLimits {
                    max_records: 1,
                    max_bytes: MIB,
                },
            )
            .await
            .unwrap();
        assert_eq!(read.records, expected[i..i + 1]);
        assert_eq!(read.next, EventSeq::from_wal(ack.lsn, (i + 1) as u16));
        assert_eq!(read.high_watermark, watermark);
    }
    log.close().unwrap();
}

/// Recompute the frame CRC, leaving only the Records body malformed. Both records
/// skipped inside a delivered batch and records beyond a page limit must be decoded.
#[tokio::test(flavor = "multi_thread")]
async fn hinted_delivered_batch_refuses_malformed_skipped_and_unreturned_records() {
    for (bad_record, from_idx) in [(0, 1), (2, 0)] {
        let dir = tempfile::tempdir().unwrap();
        let log = StreamLog::open(config(dir.path())).await.unwrap();
        let records: Vec<_> = (0..3).map(input).collect();
        let ack = log.append(records.clone()).await.unwrap();
        log.sync().await.unwrap();
        let path = dir.path().join(segment_file_name(1));
        let original = std::fs::read(&path).unwrap();
        let mut payload = RecordBatch {
            append_time_ms: TIME_MS,
            records,
        }
        .encode(&CompressionConfig::none())
        .unwrap();
        payload[RECORDS_HEADER_LEN + bad_record * (5 + 1024)] |= 0x80;
        let mut bytes = original[..SEGMENT_HEADER_LEN as usize].to_vec();
        encode_frame(&mut bytes, ack.lsn, FrameKind::Records, &payload);
        assert_eq!(bytes.len(), original.len());
        assert_eq!(
            bytes.len(),
            SEGMENT_HEADER_LEN as usize + FRAME_HEADER_LEN + payload.len()
        );
        let file = StdVfs.open(&path, OpenMode::ReadWrite).unwrap();
        file.write_at(0, &bytes).unwrap();
        file.sync_data().unwrap();
        let result = log
            .read_from(
                StartAt::Offset(EventSeq::from_wal(ack.lsn, from_idx)),
                ReadLimits {
                    max_records: 1,
                    max_bytes: MIB,
                },
            )
            .await;
        // The maintainer approved pinning the existing live-read classification:
        // Serialization maps to Internal. Preserve the exact refusal diagnostic.
        let expected_reason = format!(
            "Serialization error: record batch: record {bad_record}: reserved flag bits set (0x80)"
        );
        assert!(
            matches!(result, Err(StorageError::Internal(ref reason)) if reason == &expected_reason),
            "malformed record {bad_record} was not refused: {result:?}"
        );
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
        log.close().unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn cancellation_after_hint_validation_retains_the_directory_lock() {
    let dir = tempfile::tempdir().unwrap();
    let observer = Arc::new(Observer::default());
    let cfg = config(dir.path());
    let log = Arc::new(
        StreamLog::open_with_vfs(Arc::new(CountingVfs(observer.clone())), cfg.clone())
            .await
            .unwrap(),
    );
    let ack = log.append(vec![input(0)]).await.unwrap();
    log.sync().await.unwrap();
    observer.take();
    *observer.pause_header.lock() = true;
    // A failed assertion must still release the real blocking read during teardown.
    let _release_on_drop = ReleaseOnDrop(observer.clone());
    let task = {
        let log = log.clone();
        tokio::spawn(async move {
            log.read_from(
                StartAt::Offset(ack.first()),
                ReadLimits {
                    max_records: 1,
                    max_bytes: MIB,
                },
            )
            .await
        })
    };
    observer.entered.notified().await;
    let validation_trace = observer.take();
    assert_eq!(
        validation_trace.len(),
        2,
        "header gate must follow frame validation"
    );
    assert!(validation_trace
        .iter()
        .all(|(offset, _)| *offset == SEGMENT_HEADER_LEN));
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    Arc::try_unwrap(log).ok().unwrap().close().unwrap();
    assert!(matches!(
        StreamLog::open(cfg.clone()).await,
        Err(StorageError::Locked(_))
    ));
    observer.release();
    observer.unlocked.notified().await;
    StreamLog::open(cfg).await.unwrap().close().unwrap();
}
