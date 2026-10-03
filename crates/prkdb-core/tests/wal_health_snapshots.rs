//! Public health snapshots while real WAL writes are idle, blocked or failed.

use prkdb_core::vfs::{LockGuard, OpenMode, StdVfs, Vfs, VfsFile};
use prkdb_core::wal::{SyncMode, Wal, WalConfig, WalError, WalHealth, WalOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{mpsc, Arc, Condvar, Mutex};
use std::time::Duration;

// The public health clock has a one-second minimum stall interval. Waiting past it
// exercises that contract, rather than giving the writer time to reach a state:
// the VFS entry signal establishes that state before any health check or wait.
const PAST_MINIMUM_STALL: Duration = Duration::from_millis(1200);

#[derive(Default)]
struct GateState {
    entered: Option<mpsc::Sender<()>>,
    released: bool,
}

struct WriteGate {
    state: Mutex<GateState>,
    changed: Condvar,
    fail_write: bool,
}

impl WriteGate {
    fn new(fail_write: bool) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(GateState::default()),
            changed: Condvar::new(),
            fail_write,
        })
    }

    fn arm(self: &Arc<Self>) -> (mpsc::Receiver<()>, ReleaseWrite) {
        let (tx, rx) = mpsc::channel();
        self.state.lock().unwrap().entered = Some(tx);
        (rx, ReleaseWrite(self.clone()))
    }

    fn before_write(&self) -> io::Result<()> {
        let mut state = self.state.lock().unwrap();
        if let Some(entered) = state.entered.take() {
            let _ = entered.send(());
            state = self
                .changed
                .wait_while(state, |state| !state.released)
                .unwrap();
            if self.fail_write {
                return Err(io::Error::other("health fixture write failure"));
            }
        }
        drop(state);
        Ok(())
    }
}

// Always release before Wal drops, including when an assertion unwinds. A failed
// health assertion must not strand the writer and turn into a shutdown timeout.
struct ReleaseWrite(Arc<WriteGate>);

impl Drop for ReleaseWrite {
    fn drop(&mut self) {
        let mut state = self.0.state.lock().unwrap_or_else(|e| e.into_inner());
        state.released = true;
        self.0.changed.notify_all();
    }
}

struct GatedVfs(Arc<WriteGate>);
struct GatedFile(Arc<dyn VfsFile>, Arc<WriteGate>);

impl VfsFile for GatedFile {
    fn write_at(&self, offset: u64, buf: &[u8]) -> io::Result<()> {
        self.1.before_write()?;
        self.0.write_at(offset, buf)
    }
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        self.0.read_at(offset, buf)
    }
    fn set_len(&self, len: u64) -> io::Result<()> {
        self.0.set_len(len)
    }
    fn len(&self) -> io::Result<u64> {
        self.0.len()
    }
    fn sync_data(&self) -> io::Result<()> {
        self.0.sync_data()
    }
}

impl Vfs for GatedVfs {
    fn open(&self, path: &Path, mode: OpenMode) -> io::Result<Arc<dyn VfsFile>> {
        Ok(Arc::new(GatedFile(
            StdVfs.open(path, mode)?,
            self.0.clone(),
        )))
    }
    fn create(&self, path: &Path) -> io::Result<Arc<dyn VfsFile>> {
        Ok(Arc::new(GatedFile(StdVfs.create(path)?, self.0.clone())))
    }
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        StdVfs.rename(from, to)
    }
    fn remove(&self, path: &Path) -> io::Result<()> {
        StdVfs.remove(path)
    }
    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        StdVfs.create_dir_all(path)
    }
    fn read_dir(&self, path: &Path) -> io::Result<Vec<PathBuf>> {
        StdVfs.read_dir(path)
    }
    fn exists(&self, path: &Path) -> io::Result<bool> {
        StdVfs.exists(path)
    }
    fn sync_dir(&self, path: &Path) -> io::Result<()> {
        StdVfs.sync_dir(path)
    }
    fn lock_exclusive(&self, path: &Path) -> io::Result<Box<dyn LockGuard>> {
        StdVfs.lock_exclusive(path)
    }
}

fn open(vfs: Arc<dyn Vfs>, dir: &Path, mode: SyncMode, interval: Duration) -> Wal {
    let options = WalOptions {
        sync_mode: mode,
        sync_interval: interval,
        ..WalOptions::from_config(&WalConfig::default())
    };
    let (wal, _) = Wal::open(vfs, dir, options, 1, &mut |_, _, _| Ok(())).unwrap();
    wal
}

#[tokio::test]
async fn idle_logs_remain_healthy_past_the_minimum_stall_interval() {
    for mode in [SyncMode::Durable, SyncMode::Fast] {
        let dir = tempfile::tempdir().unwrap();
        let wal = open(Arc::new(StdVfs), dir.path(), mode, Duration::from_millis(1));
        tokio::time::sleep(PAST_MINIMUM_STALL).await;
        assert_eq!(wal.health(), WalHealth::Healthy, "idle {mode:?} log");
        assert_eq!(wal.next_lsn(), 1);
        assert_eq!(wal.acked_lsn(), 0);
        assert_eq!(wal.durable_lsn(), 0);
        wal.close().unwrap();
    }
}

#[tokio::test]
async fn blocked_writes_report_all_pending_payload_bytes_and_recover_after_progress() {
    for mode in [SyncMode::Durable, SyncMode::Fast] {
        let dir = tempfile::tempdir().unwrap();
        let gate = WriteGate::new(false);
        let wal = open(
            Arc::new(GatedVfs(gate.clone())),
            dir.path(),
            mode,
            Duration::from_millis(1),
        );
        let (entered, release) = gate.arm();
        let first_payload = b"first blocked payload".to_vec();
        let second_payload = b"second queued payload".to_vec();
        let first = wal
            .append_reserved(
                wal.reserve(first_payload.len()).await.unwrap(),
                first_payload.clone(),
                None,
            )
            .unwrap();
        entered.recv_timeout(Duration::from_secs(10)).unwrap();
        let second = wal
            .append_reserved(
                wal.reserve(second_payload.len()).await.unwrap(),
                second_payload.clone(),
                None,
            )
            .unwrap();
        tokio::time::sleep(PAST_MINIMUM_STALL).await;
        match wal.health() {
            WalHealth::Stalled {
                queued_bytes,
                oldest_ms,
            } => {
                assert_eq!(queued_bytes, first_payload.len() + second_payload.len());
                assert!(oldest_ms >= 1000, "pending request age: {oldest_ms} ms");
            }
            health => panic!("blocked {mode:?} writer must report its stall, got {health:?}"),
        }
        assert_eq!(wal.acked_lsn(), 0);
        assert_eq!(wal.durable_lsn(), 0);
        drop(release);
        let first_loc = first.await.unwrap();
        let second_loc = second.await.unwrap();
        assert_eq!((first_loc.lsn, second_loc.lsn), (1, 2));
        assert_eq!(wal.read(first_loc).unwrap(), first_payload);
        assert_eq!(wal.read(second_loc).unwrap(), second_payload);
        assert_eq!(wal.sync().await.unwrap(), 2);
        assert_eq!(wal.health(), WalHealth::Healthy);
        assert_eq!(wal.next_lsn(), 3);
        assert_eq!(wal.acked_lsn(), 2);
        assert_eq!(wal.durable_lsn(), 2);
        wal.close().unwrap();
    }
}

#[tokio::test]
async fn configured_sync_interval_extends_the_stall_bound() {
    for mode in [SyncMode::Durable, SyncMode::Fast] {
        let dir = tempfile::tempdir().unwrap();
        let gate = WriteGate::new(false);
        // 250 ms gives a 25-second stall bound. The 1.2-second observation is
        // past the minimum but well below that bound, with ample scheduling margin.
        let wal = open(
            Arc::new(GatedVfs(gate.clone())),
            dir.path(),
            mode,
            Duration::from_millis(250),
        );
        let (entered, release) = gate.arm();
        let payload = b"waiting within configured stall bound".to_vec();
        let pending = wal
            .append_reserved(
                wal.reserve(payload.len()).await.unwrap(),
                payload.clone(),
                None,
            )
            .unwrap();
        entered.recv_timeout(Duration::from_secs(10)).unwrap();
        tokio::time::sleep(PAST_MINIMUM_STALL).await;
        assert_eq!(
            wal.health(),
            WalHealth::Healthy,
            "configured {mode:?} interval"
        );
        assert_eq!(wal.acked_lsn(), 0);
        assert_eq!(wal.durable_lsn(), 0);
        drop(release);
        let loc = pending.await.unwrap();
        assert_eq!(wal.read(loc).unwrap(), payload);
        assert_eq!(wal.sync().await.unwrap(), loc.lsn);
        assert_eq!(wal.health(), WalHealth::Healthy);
        wal.close().unwrap();
    }
}

#[tokio::test]
async fn failed_writes_preserve_poison_reason_and_never_advance_acknowledgements() {
    for mode in [SyncMode::Durable, SyncMode::Fast] {
        let dir = tempfile::tempdir().unwrap();
        let gate = WriteGate::new(true);
        let wal = open(
            Arc::new(GatedVfs(gate.clone())),
            dir.path(),
            mode,
            Duration::from_millis(1),
        );
        let (entered, release) = gate.arm();
        let first = wal
            .append_reserved(wal.reserve(3).await.unwrap(), b"one".to_vec(), None)
            .unwrap();
        entered.recv_timeout(Duration::from_secs(10)).unwrap();
        let second = wal
            .append_reserved(wal.reserve(3).await.unwrap(), b"two".to_vec(), None)
            .unwrap();
        drop(release);
        let reason = match first.await.unwrap_err() {
            WalError::Poisoned(reason) => reason,
            error => panic!("failed write must poison the log: {error}"),
        };
        assert!(reason.contains("health fixture write failure"));
        assert!(matches!(second.await, Err(WalError::Poisoned(ref cause)) if cause == &reason));
        assert_eq!(wal.health(), WalHealth::Poisoned(reason.clone()));
        assert!(matches!(wal.append(b"later".to_vec(), None).await,
            Err(WalError::Poisoned(ref cause)) if cause == &reason));
        assert_eq!(wal.health(), WalHealth::Poisoned(reason));
        assert_eq!(wal.acked_lsn(), 0);
        assert_eq!(wal.durable_lsn(), 0);
        assert!(matches!(wal.close(), Err(WalError::Poisoned(_))));
    }
}
