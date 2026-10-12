//! Portable descriptor-budget witnesses over real `StdVfs` files.
//!
//! The budget models live VFS file leases plus temporary directory operations. It
//! neither measures process FDs nor changes RLIMIT, and excludes runtime/stdio FDs
//! and the storage adapter's LOCK (the core `Wal` does not acquire that lock).
//! These tests characterize current segment ownership, not a production fix. A
//! directory token is conservatively modeled on every platform, including platforms
//! where `StdVfs::sync_dir` is a no-op.

use prkdb_core::vfs::{LockGuard, OpenMode, StdVfs, Vfs, VfsFile};
use prkdb_core::wal::frame::FrameKind;
use prkdb_core::wal::{FrontRelease, Lsn, SyncMode, Wal, WalError, WalHealth, WalOptions};
use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;

fn options(mode: SyncMode, front_release: FrontRelease) -> WalOptions {
    WalOptions {
        sync_mode: mode,
        // Rolls and explicit operations establish the schedule; no timer is needed.
        sync_interval: Duration::from_secs(3600),
        segment_bytes: 256,
        max_batch_bytes: 1 << 20,
        max_queued_bytes: 8 << 20,
        front_release,
        append_kind: FrameKind::Batch,
        lsn_limit: None,
    }
}

fn open(vfs: Arc<BudgetVfs>, dir: &Path, options: WalOptions) -> Wal {
    Wal::open(vfs, dir, options, 1, &mut |_, _, _| Ok(()))
        .unwrap()
        .0
}

#[test]
fn retained_segments_hold_one_file_lease_each_until_close_in_both_modes() {
    for mode in [SyncMode::Fast, SyncMode::Durable] {
        let dir = tempfile::tempdir().unwrap();
        let vfs = Arc::new(BudgetVfs::new(8));
        let wal = open(
            vfs.clone(),
            dir.path(),
            options(mode, FrontRelease::ElidedOnly),
        );
        assert_eq!(vfs.snapshot().files, 1, "{mode:?}: initial active segment");

        for i in 0..4u8 {
            let loc = wal.append_blocking(vec![i; 24], None).unwrap();
            assert_eq!(wal.roll_blocking().unwrap(), Some(loc.lsn + 1));
            let segments = wal.segments();
            assert_eq!(segments.len(), usize::from(i) + 2);
            assert_eq!(vfs.snapshot().files, segments.len(), "{mode:?}");
            assert_eq!(vfs.snapshot().transient, 0);
            for first in segments {
                assert_eq!(vfs.file_leases(&wal.segment_path(first)), 1);
            }
        }

        wal.close().unwrap();
        assert_eq!(
            vfs.snapshot().files,
            0,
            "{mode:?}: Close releases every file"
        );
        assert_eq!(vfs.snapshot().transient, 0);
    }
}

#[test]
fn roll_needs_directory_headroom_and_budget_fault_preserves_acknowledged_payloads() {
    for mode in [SyncMode::Fast, SyncMode::Durable] {
        for cap in [2, 3] {
            let dir = tempfile::tempdir().unwrap();
            let vfs = Arc::new(BudgetVfs::new(cap));
            let opts = options(mode, FrontRelease::ElidedOnly);
            let wal = open(vfs.clone(), dir.path(), opts.clone());
            assert!(
                vfs.events().iter().any(|e| {
                    e.operation == "sync_dir" && e.accepted && e.files == 1 && e.transient == 1
                }),
                "{mode:?}: initial file plus directory sync fits cap {cap}"
            );
            let payload = b"acknowledged before the roll".to_vec();
            let loc = wal.append_blocking(payload.clone(), None).unwrap();
            let acknowledged = vec![(loc.lsn, payload)];
            let rolled = wal.roll_blocking();

            if cap == 2 {
                assert!(
                    matches!(&rolled, Err(WalError::Poisoned(reason))
                    if reason.contains(BUDGET_FAULT)),
                    "{mode:?}: {rolled:?}"
                );
                assert!(matches!(wal.health(), WalHealth::Poisoned(_)));
                assert!(
                    vfs.events().iter().any(|e| {
                        e.operation == "sync_dir" && !e.accepted && e.files == 2 && e.transient == 0
                    }),
                    "the refusal occurs at directory sync, after creating the new file"
                );
                assert_eq!(wal.segments(), vec![1], "failed roll is not published");
                assert_eq!(
                    vfs.snapshot().files,
                    1,
                    "failed roll's new handle is released"
                );
                let refused = wal.append_blocking(b"never acknowledged".to_vec(), None);
                assert!(matches!(refused, Err(WalError::Poisoned(_))));
                assert!(matches!(wal.close(), Err(WalError::Poisoned(_))));
            } else {
                assert_eq!(rolled.unwrap(), Some(loc.lsn + 1), "negative control");
                assert!(matches!(wal.health(), WalHealth::Healthy));
                assert!(!vfs.events().iter().any(|e| !e.accepted));
                assert_eq!(vfs.snapshot().files, 2);
                assert_eq!(vfs.snapshot().peak_total, 3);
                wal.close().unwrap();
            }
            assert_eq!(
                vfs.snapshot().files,
                0,
                "{mode:?}, cap {cap}: all handles released"
            );
            assert_eq!(vfs.snapshot().transient, 0);

            // Lift only the local simulated cap after releasing the failed WAL. The
            // fresh open uses real surviving bytes, not a power-loss simulation.
            vfs.set_limit(8);
            let mut replayed: Vec<(Lsn, Vec<u8>)> = Vec::new();
            let (reopened, _) = Wal::open(vfs.clone(), dir.path(), opts, 1, &mut |loc, kind, p| {
                assert_eq!(kind, FrameKind::Batch);
                replayed.push((loc.lsn, p.to_vec()));
                Ok(())
            })
            .unwrap();
            assert_eq!(replayed, acknowledged, "{mode:?}, cap {cap}");
            assert_eq!(reopened.next_lsn(), loc.lsn + 1);
            reopened.close().unwrap();
            assert_eq!(vfs.snapshot().files, 0);
        }
    }
}

#[test]
fn retention_unlinks_segments_but_snapshot_leases_read_them_until_reader_finishes() {
    for mode in [SyncMode::Fast, SyncMode::Durable] {
        let dir = tempfile::tempdir().unwrap();
        let vfs = Arc::new(BudgetVfs::new(8));
        let wal = open(
            vfs.clone(),
            dir.path(),
            options(mode, FrontRelease::Retention),
        );
        let mut acknowledged = Vec::new();
        for segment in 0..3u8 {
            for record in 0..2u8 {
                let payload = vec![segment * 2 + record; 24];
                let loc = wal.append_blocking(payload.clone(), None).unwrap();
                acknowledged.push((loc.lsn, payload));
            }
            if segment < 2 {
                wal.roll_blocking().unwrap().unwrap();
            }
        }
        let segments = wal.segments();
        assert_eq!(segments, vec![1, 3, 5]);
        let first_path = wal.segment_path(1);
        let second_path = wal.segment_path(3);
        assert_eq!(vfs.snapshot().files, 3);

        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        std::thread::scope(|threads| {
            let release_reader = ReleaseReader(release_tx);
            let reader_wal = &wal;
            let reader = threads.spawn(move || {
                let mut seen = Vec::new();
                reader_wal
                    .scan_from(1, &mut |loc, kind, payload| {
                        assert_eq!(kind, FrameKind::Batch);
                        seen.push((loc.lsn, payload.to_vec()));
                        if loc.lsn == 1 {
                            // scan_from cloned the segment snapshot before this visit.
                            entered_tx.send(()).unwrap();
                            release_rx.recv().unwrap();
                        }
                        Ok(())
                    })
                    .unwrap();
                seen
            });
            entered_rx.recv().unwrap();
            // Capture every assertion's inputs before releasing the reader. Assertions
            // run after join, so a failure cannot leave the scoped reader blocked.
            let retention = wal
                .set_log_start(5)
                .and_then(|()| wal.remove_leading_segments(5));
            let visible_segments = wal.segments();
            let paths_exist = (first_path.exists(), second_path.exists());
            let held_files = vfs.snapshot().files;
            let held_removed = (vfs.file_leases(&first_path), vfs.file_leases(&second_path));
            drop(release_reader);
            let seen = reader.join().unwrap();

            assert_eq!(retention.unwrap(), 2);
            assert_eq!(visible_segments, vec![5]);
            assert_eq!(paths_exist, (false, false));
            assert_eq!(
                held_files, 3,
                "{mode:?}: snapshot retains both unlinked files"
            );
            assert_eq!(held_removed, (1, 1));
            assert_eq!(
                seen, acknowledged,
                "{mode:?}: remaining snapshot data is readable"
            );
        });
        assert_eq!(vfs.file_leases(&first_path), 0);
        assert_eq!(vfs.file_leases(&second_path), 0);
        assert_eq!(
            vfs.snapshot().files,
            1,
            "only the retained active file remains"
        );
        wal.close().unwrap();
        assert_eq!(vfs.snapshot().files, 0);
    }
}

#[test]
fn budget_reservations_release_on_inner_failures_and_last_arc_drop() {
    let dir = tempfile::tempdir().unwrap();
    let vfs = BudgetVfs::new(1);
    let missing = dir.path().join("missing");
    assert!(vfs.open(&missing, OpenMode::Read).is_err());
    assert!(vfs.create(&missing.join("file")).is_err());
    assert!(vfs.read_dir(&missing).is_err());
    // StdVfs::sync_dir is a no-op on non-Unix, but either result must release.
    let _ = vfs.sync_dir(&missing);
    assert_eq!(vfs.snapshot().files, 0);
    assert_eq!(vfs.snapshot().transient, 0);

    let path = dir.path().join("file");
    let file = vfs.create(&path).unwrap();
    let clone = file.clone();
    let refused_path = dir.path().join("refused");
    let refused = vfs.create(&refused_path).err().expect("budget is full");
    assert!(refused.to_string().contains(BUDGET_FAULT));
    assert!(
        !refused_path.exists(),
        "refused reservation must precede real create"
    );
    drop(file);
    assert_eq!(
        vfs.snapshot().files,
        1,
        "a cloned Arc is the same file lease"
    );
    clone.write_at(0, b"real file").unwrap();
    drop(clone);
    assert_eq!(std::fs::read(&path).unwrap(), b"real file");
    assert_eq!(vfs.snapshot().files, 0);
}

const BUDGET_FAULT: &str = "simulated VFS descriptor budget exhausted";

/// Also releases the reader while unwinding the scope, before the scope joins it.
struct ReleaseReader(mpsc::Sender<()>);

impl Drop for ReleaseReader {
    fn drop(&mut self) {
        let _ = self.0.send(());
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct Counts {
    files: usize,
    transient: usize,
    peak_total: usize,
}

/// Records reservation admission, not whether the inner filesystem operation
/// succeeded. A refused reservation runs no inner operation and changes no count.
#[derive(Clone, Debug)]
struct ReservationEvent {
    operation: &'static str,
    accepted: bool,
    files: usize,
    transient: usize,
}

struct BudgetState {
    limit: usize,
    counts: Counts,
    file_leases: BTreeMap<PathBuf, usize>,
    events: Vec<ReservationEvent>,
}

struct BudgetVfs {
    state: Arc<Mutex<BudgetState>>,
}

impl BudgetVfs {
    fn new(limit: usize) -> Self {
        Self {
            state: Arc::new(Mutex::new(BudgetState {
                limit,
                counts: Counts::default(),
                file_leases: BTreeMap::new(),
                events: Vec::new(),
            })),
        }
    }

    fn snapshot(&self) -> Counts {
        self.state.lock().unwrap().counts
    }

    fn events(&self) -> Vec<ReservationEvent> {
        self.state.lock().unwrap().events.clone()
    }

    fn file_leases(&self, path: &Path) -> usize {
        self.state
            .lock()
            .unwrap()
            .file_leases
            .get(path)
            .copied()
            .unwrap_or(0)
    }

    fn set_limit(&self, limit: usize) {
        self.state.lock().unwrap().limit = limit;
    }

    /// Reserve before any real open/create or temporary directory operation. All
    /// decisions and increments share one mutex, including concurrent reader drops.
    fn reserve(&self, operation: &'static str, file_path: Option<&Path>) -> io::Result<Lease> {
        let mut state = self.state.lock().unwrap();
        let accepted = state.counts.files + state.counts.transient < state.limit;
        if accepted {
            if let Some(path) = file_path {
                state.counts.files += 1;
                *state.file_leases.entry(path.to_path_buf()).or_default() += 1;
            } else {
                state.counts.transient += 1;
            }
            state.counts.peak_total = state
                .counts
                .peak_total
                .max(state.counts.files + state.counts.transient);
        }
        let counts = state.counts;
        state.events.push(ReservationEvent {
            operation,
            accepted,
            files: counts.files,
            transient: counts.transient,
        });
        if !accepted {
            return Err(io::Error::other(format!(
                "{BUDGET_FAULT}: {operation}, limit {}, files {}, temporary {}",
                state.limit, counts.files, counts.transient,
            )));
        }
        Ok(Lease {
            state: self.state.clone(),
            file_path: file_path.map(Path::to_path_buf),
        })
    }
}

struct Lease {
    state: Arc<Mutex<BudgetState>>,
    file_path: Option<PathBuf>,
}

impl Drop for Lease {
    fn drop(&mut self) {
        let mut state = self.state.lock().unwrap();
        if let Some(path) = &self.file_path {
            state.counts.files -= 1;
            let count = state.file_leases.get_mut(path).unwrap();
            *count -= 1;
            if *count == 0 {
                state.file_leases.remove(path);
            }
        } else {
            state.counts.transient -= 1;
        }
    }
}

struct LeasedFile {
    // Rust drops fields in declaration order: close the real handle before
    // releasing its reservation. Arc clones of this wrapper keep the same lease.
    inner: Arc<dyn VfsFile>,
    _lease: Lease,
}

impl VfsFile for LeasedFile {
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
        self.inner.sync_data()
    }
}

impl Vfs for BudgetVfs {
    fn open(&self, path: &Path, mode: OpenMode) -> io::Result<Arc<dyn VfsFile>> {
        let lease = self.reserve("open", Some(path))?;
        let inner = StdVfs.open(path, mode)?;
        Ok(Arc::new(LeasedFile {
            inner,
            _lease: lease,
        }))
    }

    fn create(&self, path: &Path) -> io::Result<Arc<dyn VfsFile>> {
        let lease = self.reserve("create", Some(path))?;
        let inner = StdVfs.create(path)?;
        Ok(Arc::new(LeasedFile {
            inner,
            _lease: lease,
        }))
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
        let _lease = self.reserve("read_dir", None)?;
        StdVfs.read_dir(path)
    }

    fn exists(&self, path: &Path) -> io::Result<bool> {
        StdVfs.exists(path)
    }

    fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        let _lease = self.reserve("sync_dir", None)?;
        StdVfs.sync_dir(dir)
    }

    fn lock_exclusive(&self, path: &Path) -> io::Result<Box<dyn LockGuard>> {
        // Core Wal does not request a lock. The storage adapter's LOCK is outside
        // this explicitly scoped model, like runtime and stdio descriptors.
        StdVfs.lock_exclusive(path)
    }
}
