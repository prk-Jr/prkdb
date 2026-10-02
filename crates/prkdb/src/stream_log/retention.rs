//! Retention releases a durable, oldest-first sealed prefix before deleting its files.
use super::log::{position, Inner};
use super::{EventSeq, StreamLog};
use crate::storage::wal_adapter::wal_err;
use prkdb_core::wal::{WalError, WalHealth};
use prkdb_types::error::StorageError;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Whole sealed-segment retention. Either enabled limit can release a segment.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RetentionPolicy {
    /// Release a sealed segment only when its newest append is strictly older.
    pub max_age: Option<Duration>,
    /// Size retention keeps at least this many bytes, including the active segment.
    pub max_bytes: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetentionReport {
    pub segments_removed: usize,
    pub bytes_removed: u64,
    pub earliest_before: EventSeq,
    pub earliest_after: EventSeq,
    pub rolled: bool,
}

// Compare in i128: negative timestamps and Duration::MAX must not overflow or
// accidentally make a too-young segment eligible by clamping the cutoff.
fn expired(time: i64, now: i64, age: Duration) -> bool {
    i128::from(time) < i128::from(now) - age.as_millis() as i128
}

impl StreamLog {
    /// Remove only an eligible sealed prefix, advancing the durable floor first.
    /// Removal errors leave the writer healthy and are retried by the next run.
    /// This call performs blocking filesystem operations; async callers can use
    /// their blocking pool when an explicit run may involve a large prefix.
    pub fn apply_retention(&self) -> Result<RetentionReport, StorageError> {
        apply(&self.inner, &|| false)
    }
}

fn apply(inner: &Inner, stopped: &dyn Fn() -> bool) -> Result<RetentionReport, StorageError> {
    let _run = inner.retention_lock.write();
    let floor = inner.wal.log_state().log_start;
    let mut report = RetentionReport {
        segments_removed: 0,
        bytes_removed: 0,
        earliest_before: position(floor),
        earliest_after: position(floor),
        rolled: false,
    };
    if stopped() {
        return Ok(report);
    }
    // A failed writer must be reopened before retention can free space, including
    // files whose floor was committed during an earlier successful writer epoch.
    match inner.wal.health() {
        WalHealth::Poisoned(reason) => return Err(wal_err(WalError::Poisoned(reason))),
        WalHealth::Closed => return Err(wal_err(WalError::Closed)),
        _ => {}
    }
    // Complete an already-released prefix even if a backward clock jump makes
    // those segments ineligible now. The committed floor can never be undone.
    let pending: Vec<_> = inner
        .wal
        .sealed_segments()
        .map_err(wal_err)?
        .into_iter()
        .filter(|s| s.first_lsn < floor)
        .collect();
    if !pending.is_empty() {
        report.segments_removed += inner.wal.remove_leading_segments(floor).map_err(wal_err)?;
        report.bytes_removed += pending.iter().map(|s| s.len).sum::<u64>();
    }
    // A removal can succeed while its directory sync fails. The WAL has already
    // dropped that handle, so no pending file remains to trigger index cleanup.
    inner.index.write().remove_before(floor);
    if stopped() {
        return Ok(report);
    }
    let now = inner.cfg.clock.now_ms();
    let segment_age = inner
        .cfg
        .segment_max_age
        .or_else(|| inner.cfg.retention.max_age.map(|age| age / 4));
    if let Some(age) = segment_age {
        let active = inner.wal.segments().last().copied();
        let oldest = active.and_then(|s| inner.index.read().min_time(s));
        if oldest.is_some_and(|time| expired(time, now, age)) {
            report.rolled = inner.wal.roll_blocking().map_err(wal_err)?.is_some();
        }
    }
    if stopped() {
        return Ok(report);
    }
    let sealed = inner.wal.sealed_segments().map_err(wal_err)?;
    let mut remaining = inner.wal.log_bytes().map_err(wal_err)?;
    let mut upto = floor;
    let mut bytes = 0;
    {
        let index = inner.index.read();
        for segment in &sealed {
            let age = inner.cfg.retention.max_age.is_some_and(|age| {
                index
                    .max_time(segment.first_lsn)
                    .is_some_and(|time| expired(time, now, age))
            });
            let size = inner
                .cfg
                .retention
                .max_bytes
                .is_some_and(|max| remaining.saturating_sub(segment.len) >= max);
            if !(age || size) {
                break;
            }
            upto = segment.next_lsn;
            remaining = remaining.saturating_sub(segment.len);
            bytes += segment.len;
        }
    }
    if upto > floor && !stopped() {
        inner.wal.set_log_start(upto).map_err(wal_err)?;
        report.earliest_after = position(upto);
        // A stop after the durable floor commits can leave files for next open.
        if !stopped() {
            report.segments_removed += inner.wal.remove_leading_segments(upto).map_err(wal_err)?;
            report.bytes_removed += bytes;
            inner.index.write().remove_before(upto);
        }
    }
    Ok(report)
}

/// The public handle owns cancellation; a sleeping loop owns only a Weak.
/// An already-running blocking pass keeps Inner and the directory lock until it
/// exits. Dropping a public handle never waits on the Tokio executor.
pub(super) struct RetentionTask {
    stopped: Arc<AtomicBool>,
    abort: Option<tokio::task::AbortHandle>,
}
impl RetentionTask {
    pub fn start(inner: &Arc<Inner>) -> Self {
        let stopped = Arc::new(AtomicBool::new(false));
        let cfg = &inner.cfg;
        let enabled = cfg.retention.max_age.is_some()
            || cfg.retention.max_bytes.is_some()
            || cfg.segment_max_age.is_some();
        let mut task = Self {
            stopped: stopped.clone(),
            abort: None,
        };
        if !enabled || cfg.retention_interval.is_zero() {
            return task;
        }
        let interval = cfg.retention_interval;
        let weak = Arc::downgrade(inner);
        let handle = tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                if stopped.load(Ordering::Acquire) {
                    return;
                }
                let pass_weak = weak.clone();
                let stop = stopped.clone();
                // Upgrade on the blocking pool, not before queueing: a queued
                // pass cannot retain a public handle that has already dropped.
                let pass = tokio::task::spawn_blocking(move || {
                    if stop.load(Ordering::Acquire) {
                        return Ok(());
                    }
                    let Some(inner) = pass_weak.upgrade() else {
                        return Ok(());
                    };
                    apply(&inner, &|| stop.load(Ordering::Acquire)).map(|_| ())
                })
                .await;
                match pass {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => tracing::warn!(error = %e, "background stream retention failed"),
                    Err(e) => tracing::warn!(error = %e, "background stream retention task failed"),
                }
                // Observe liveness without owning Inner on the async executor:
                // a temporary upgrade could become its last owner and run the
                // blocking WAL drop/join here when the public handle races us.
                if weak.strong_count() == 0 {
                    return;
                }
            }
        });
        task.abort = Some(handle.abort_handle());
        task
    }
    pub fn stop(&self) {
        self.stopped.store(true, Ordering::Release);
        if let Some(abort) = &self.abort {
            abort.abort();
        }
    }
}
impl Drop for RetentionTask {
    fn drop(&mut self) {
        self.stop();
    }
}

#[cfg(test)]
#[path = "../../../prkdb-verify/src/faultfs.rs"]
mod faultfs;

#[cfg(test)]
mod tests {
    use super::faultfs::FaultFs;
    use super::*;
    use crate::stream_log::{ReadLimits, Record, StartAt, StreamConfig};
    use prkdb_core::vfs::{LockGuard, OpenMode, Vfs, VfsFile};
    use prkdb_core::wal::CompressionConfig;
    use std::io;
    use std::path::{Path, PathBuf};

    // Use the harness filesystem, failing only a removal's directory sync.
    // Unlike remove failure, the WAL has already dropped the segment handle.
    #[derive(Default)]
    struct RemovalSyncFs {
        fs: FaultFs,
        fail_sync: AtomicBool,
        removed: AtomicBool,
    }
    impl Vfs for RemovalSyncFs {
        fn open(&self, p: &Path, m: OpenMode) -> io::Result<Arc<dyn VfsFile>> {
            self.fs.open(p, m)
        }
        fn create(&self, p: &Path) -> io::Result<Arc<dyn VfsFile>> {
            self.fs.create(p)
        }
        fn rename(&self, a: &Path, b: &Path) -> io::Result<()> {
            self.fs.rename(a, b)
        }
        fn create_dir_all(&self, p: &Path) -> io::Result<()> {
            self.fs.create_dir_all(p)
        }
        fn read_dir(&self, p: &Path) -> io::Result<Vec<PathBuf>> {
            self.fs.read_dir(p)
        }
        fn exists(&self, p: &Path) -> io::Result<bool> {
            self.fs.exists(p)
        }
        fn lock_exclusive(&self, p: &Path) -> io::Result<Box<dyn LockGuard>> {
            self.fs.lock_exclusive(p)
        }
        fn remove(&self, p: &Path) -> io::Result<()> {
            self.fs.remove(p)?;
            self.removed.store(true, Ordering::SeqCst);
            Ok(())
        }
        fn sync_dir(&self, p: &Path) -> io::Result<()> {
            if self.removed.swap(false, Ordering::SeqCst)
                && self.fail_sync.swap(false, Ordering::SeqCst)
            {
                return Err(io::Error::other("injected removal directory sync failure"));
            }
            self.fs.sync_dir(p)
        }
    }

    #[tokio::test]
    async fn sync_dir_failure_after_last_remove_prunes_released_index_on_next_pass() {
        let fs = Arc::new(RemovalSyncFs::default());
        let path = Path::new("/retention-index");
        fs.fs.mkdir_durable(path).unwrap();
        let mut cfg = StreamConfig::new(path);
        cfg.wal.segment_bytes = 4096;
        cfg.wal.compression = CompressionConfig::none();
        cfg.retention.max_bytes = Some(0);
        cfg.retention_interval = Duration::ZERO;
        let log = StreamLog::open_with_vfs(fs.clone(), cfg).await.unwrap();
        let record = || {
            vec![Record {
                key: None,
                value: vec![1; 3000],
                headers: Vec::new(),
            }]
        };
        let a = log.append(record()).await.unwrap();
        let b = log.append(record()).await.unwrap();
        fs.fail_sync.store(true, Ordering::SeqCst);
        assert!(log
            .apply_retention()
            .unwrap_err()
            .to_string()
            .contains("injected removal directory sync failure"));
        assert_eq!(log.earliest(), b.first());
        assert!(matches!(log.health(), WalHealth::Healthy));
        assert_eq!(log.inner.wal.segments(), vec![b.lsn]);
        // The removed segment no longer appears in WAL metadata, so this pass
        // has no pending file to remove. It must still release its cached index.
        let report = log.apply_retention().unwrap();
        assert_eq!(report.segments_removed, 0);
        assert_eq!(report.earliest_before, b.first());
        assert_eq!(report.earliest_after, b.first());
        assert!(
            log.inner.index.read().seek(a.lsn).is_none(),
            "released segment index survived a successful cleanup pass"
        );
        assert_eq!(
            log.read_from(StartAt::Earliest, ReadLimits::default())
                .await
                .unwrap()
                .records[0]
                .offset,
            b.first()
        );
        assert!(matches!(log.health(), WalHealth::Healthy));
        log.close().unwrap();
    }
}
