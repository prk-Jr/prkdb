//! Shutdown must make acknowledged Fast writes durable and expose final-sync failures.

use prkdb_core::vfs::{LockGuard, OpenMode, StdVfs, Vfs, VfsFile};
use prkdb_core::wal::frame::FrameKind;
use prkdb_core::wal::{FrontRelease, SyncMode, Wal, WalError, WalOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;

#[derive(Debug, PartialEq, Eq)]
enum Event {
    SyncEntered,
    SyncCompleted,
    ShutdownReturned,
}

struct SyncGate {
    events: mpsc::Sender<Event>,
    release: mpsc::Receiver<()>,
}

#[derive(Default)]
struct Probe {
    syncs: AtomicU64,
    fail_sync: AtomicBool,
    gate: Mutex<Option<SyncGate>>,
}

struct ProbeFile {
    inner: Arc<dyn VfsFile>,
    probe: Arc<Probe>,
}

impl VfsFile for ProbeFile {
    fn write_at(&self, offset: u64, buf: &[u8]) -> io::Result<()> {
        self.inner.write_at(offset, buf)
    }
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
        self.inner.read_at(offset, buf)
    }
    fn set_len(&self, len: u64) -> io::Result<()> {
        self.inner.set_len(len)
    }
    fn len(&self) -> io::Result<u64> {
        self.inner.len()
    }
    fn sync_data(&self) -> io::Result<()> {
        self.probe.syncs.fetch_add(1, Ordering::SeqCst);
        if self.probe.fail_sync.load(Ordering::SeqCst) {
            return Err(io::Error::other("injected shutdown sync failure"));
        }
        let gate = self.probe.gate.lock().unwrap().take();
        if let Some(gate) = gate {
            gate.events.send(Event::SyncEntered).unwrap();
            gate.release.recv().unwrap();
            self.inner.sync_data()?;
            gate.events.send(Event::SyncCompleted).unwrap();
            Ok(())
        } else {
            self.inner.sync_data()
        }
    }
}

struct ProbeVfs(Arc<Probe>);

impl ProbeVfs {
    fn wrap(&self, inner: Arc<dyn VfsFile>) -> Arc<dyn VfsFile> {
        Arc::new(ProbeFile {
            inner,
            probe: self.0.clone(),
        })
    }
}

impl Vfs for ProbeVfs {
    fn open(&self, path: &Path, mode: OpenMode) -> io::Result<Arc<dyn VfsFile>> {
        Ok(self.wrap(StdVfs.open(path, mode)?))
    }
    fn create(&self, path: &Path) -> io::Result<Arc<dyn VfsFile>> {
        Ok(self.wrap(StdVfs.create(path)?))
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

fn acknowledged_unsynced_wal(dir: &Path, probe: &Arc<Probe>) -> Wal {
    let opts = WalOptions {
        sync_mode: SyncMode::Fast,
        // Keep periodic syncing outside this scenario: shutdown itself must sync the
        // acknowledged write. This is a writer policy, not a test wait/timeout.
        sync_interval: Duration::from_secs(3600),
        segment_bytes: 1 << 20,
        max_batch_bytes: 1 << 20,
        max_queued_bytes: 1 << 20,
        front_release: FrontRelease::ElidedOnly,
        append_kind: FrameKind::Batch,
        lsn_limit: None,
    };
    let (wal, _) = Wal::open(
        Arc::new(ProbeVfs(probe.clone())),
        dir,
        opts,
        1,
        &mut |_, _, _| Ok(()),
    )
    .unwrap();
    let before = probe.syncs.load(Ordering::SeqCst);
    let loc = wal
        .append_blocking(b"acknowledged Fast write".to_vec(), None)
        .unwrap();
    assert_eq!(probe.syncs.load(Ordering::SeqCst), before);
    assert!(
        wal.durable_lsn() < loc.lsn,
        "fixture must still be unsynced"
    );
    wal
}

fn assert_shutdown_waits_for_final_sync(explicit_close: bool) {
    // Keep the directory alive until shutdown and the final-sync writer both finish.
    let dir = tempfile::tempdir().unwrap();
    let probe = Arc::new(Probe::default());
    let wal = acknowledged_unsynced_wal(dir.path(), &probe);
    let before = probe.syncs.load(Ordering::SeqCst);
    let (events_tx, events_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    *probe.gate.lock().unwrap() = Some(SyncGate {
        events: events_tx.clone(),
        release: release_rx,
    });
    let shutdown = std::thread::spawn(move || {
        let result = if explicit_close {
            wal.close()
        } else {
            drop(wal);
            Ok(())
        };
        events_tx.send(Event::ShutdownReturned).unwrap();
        result
    });
    let first = events_rx.recv().unwrap();
    let premature_return = events_rx.try_recv();
    // Release before assertions/join, including when a mutant skipped the sync.
    // A skipped sync leaves the receiver in the probe, so this send still completes.
    release_tx.send(()).unwrap();
    shutdown.join().unwrap().unwrap();
    let remaining: Vec<_> = events_rx.try_iter().collect();
    assert_eq!(first, Event::SyncEntered, "shutdown skipped its final sync");
    assert!(matches!(premature_return, Err(mpsc::TryRecvError::Empty)));
    assert_eq!(remaining, [Event::SyncCompleted, Event::ShutdownReturned]);
    assert_eq!(probe.syncs.load(Ordering::SeqCst), before + 1);
}

#[test]
fn close_syncs_acknowledged_fast_write_before_returning() {
    assert_shutdown_waits_for_final_sync(true);
}

#[test]
fn drop_waits_for_final_sync_of_acknowledged_fast_write() {
    assert_shutdown_waits_for_final_sync(false);
}

#[test]
fn close_returns_final_fast_sync_failure() {
    let dir = tempfile::tempdir().unwrap();
    let probe = Arc::new(Probe::default());
    let wal = acknowledged_unsynced_wal(dir.path(), &probe);
    let before = probe.syncs.load(Ordering::SeqCst);
    probe.fail_sync.store(true, Ordering::SeqCst);
    let err = wal
        .close()
        .expect_err("explicit close must expose the final sync failure");
    match err {
        WalError::Io(error) => {
            assert_eq!(error.kind(), io::ErrorKind::Other);
            assert_eq!(error.to_string(), "injected shutdown sync failure");
        }
        other => panic!("expected the injected final-sync I/O error, got {other:?}"),
    }
    assert_eq!(probe.syncs.load(Ordering::SeqCst), before + 1);
}
