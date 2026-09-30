//! `Wal` under simulated power loss (Task 2.6, STO-04). FaultFs rules: faultfs.rs module
//! docs.

use prkdb_core::vfs::{OpenMode, Vfs, VfsFile};
use prkdb_core::wal::frame::FrameKind;
use prkdb_core::wal::{FastSync, Lsn, SyncMode, Wal, WalOptions};
use prkdb_verify::faultfs::{FaultFs, Tear};
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;

const DIR: &str = "/db/wal";

fn opts(mode: SyncMode, segment_bytes: u64) -> WalOptions {
    WalOptions {
        sync_mode: mode,
        // Fast-mode tests must not depend on a timer firing: an hour means "only explicit
        // syncs", which makes the unsynced window deterministic.
        sync_interval: Duration::from_secs(3600),
        segment_bytes,
        max_batch_bytes: 1 << 20,
        max_queued_bytes: 8 << 20,
        fast_sync: FastSync::InWriter,
    }
}

fn fs() -> FaultFs {
    let fs = FaultFs::new();
    fs.mkdir_durable(Path::new("/db")).unwrap();
    fs
}

fn recover(fs: &FaultFs, o: WalOptions) -> (Wal, Vec<(Lsn, Vec<u8>)>) {
    let mut seen = Vec::new();
    let (wal, _) = Wal::open(
        Arc::new(fs.clone()),
        Path::new(DIR),
        o,
        1,
        &mut |loc, _, p| {
            seen.push((loc.lsn, p.to_vec()));
            Ok(())
        },
    )
    .unwrap();
    (wal, seen)
}

fn payload(i: u64) -> Vec<u8> {
    format!("rec-{i:04}").repeat(8).into_bytes()
}

const TEARS: [Tear; 4] = [Tear::None, Tear::Prefix, Tear::ZeroTail, Tear::Garbage];

/// STO-02 at the log level: Durable acks survive power loss, whatever happens to the tail.
#[tokio::test(flavor = "multi_thread")]
async fn durable_acks_survive_power_loss_with_any_tear() {
    for seed in 0..25u64 {
        for tear in TEARS {
            let fs = fs();
            let (wal, _) = recover(&fs, opts(SyncMode::Durable, 2048));
            let mut acked = Vec::new();
            for i in 0..60 {
                acked.push((wal.append(payload(i), None).await.unwrap().lsn, payload(i)));
            }
            fs.power_loss(&mut ChaCha8Rng::seed_from_u64(seed), tear);
            drop(wal); // after the loss: its handles are stale, so nothing more can be synced
            let (_, replayed) = recover(&fs, opts(SyncMode::Durable, 2048));
            assert_eq!(replayed, acked, "seed {seed} tear {tear:?}");
        }
    }
}

/// Fast mode loses at most the unsynced suffix, and what survives is a prefix: no holes.
#[tokio::test(flavor = "multi_thread")]
async fn fast_power_loss_keeps_a_prefix_no_shorter_than_the_last_sync() {
    // Task 2.7: both Fast sync placements. The hour-long interval keeps the syncer thread
    // idle, so the unsynced window is exactly the writes after the explicit `sync()`.
    for fast_sync in [FastSync::InWriter, FastSync::SyncerThread] {
        let o = || WalOptions {
            fast_sync,
            ..opts(SyncMode::Fast, 1 << 20)
        };
        for seed in 0..25u64 {
            for tear in TEARS {
                let fs = fs();
                let (wal, _) = recover(&fs, o());
                for i in 0..20 {
                    wal.append(payload(i), None).await.unwrap();
                }
                let synced = wal.sync().await.unwrap();
                for i in 20..40 {
                    wal.append(payload(i), None).await.unwrap();
                }
                fs.power_loss(&mut ChaCha8Rng::seed_from_u64(seed), tear);
                drop(wal);
                let (_, replayed) = recover(&fs, o());
                let n = replayed.len() as u64;
                assert!(
                    n >= synced && n <= 40,
                    "{fast_sync:?} seed {seed} tear {tear:?}: kept {n}, synced {synced}"
                );
                let expected: Vec<_> = (0..n).map(|i| (i + 1, payload(i))).collect();
                assert_eq!(
                    replayed, expected,
                    "{fast_sync:?} seed {seed} tear {tear:?}: not a prefix"
                );
            }
        }
    }
}

/// STO-04: a torn tail is truncated at open, and writes after the truncation survive.
#[tokio::test(flavor = "multi_thread")]
async fn torn_tail_is_truncated_and_later_appends_survive() {
    let fs = fs();
    let (wal, _) = recover(&fs, opts(SyncMode::Fast, 1 << 20));
    for i in 0..10 {
        wal.append(payload(i), None).await.unwrap();
    }
    wal.sync().await.unwrap();
    for i in 10..15 {
        wal.append(payload(i), None).await.unwrap();
    }
    fs.power_loss(&mut ChaCha8Rng::seed_from_u64(3), Tear::Garbage);
    drop(wal);

    let (wal, first) = recover(&fs, opts(SyncMode::Durable, 1 << 20));
    let kept = first.len() as u64;
    assert!(kept >= 10);
    let mut later = Vec::new();
    for i in 100..103 {
        later.push((wal.append(payload(i), None).await.unwrap().lsn, payload(i)));
    }
    fs.power_loss(&mut ChaCha8Rng::seed_from_u64(4), Tear::None);
    drop(wal);

    let (_, second) = recover(&fs, opts(SyncMode::Durable, 1 << 20));
    assert_eq!(&second[..kept as usize], &first[..]);
    assert_eq!(
        &second[kept as usize..],
        &later[..],
        "appends after the truncation were lost"
    );
}

/// STO-04: a segment created by a roll survives power loss (directory fsync on create).
#[tokio::test(flavor = "multi_thread")]
async fn a_new_segment_is_durable_after_roll() {
    let fs = fs();
    let (wal, _) = recover(&fs, opts(SyncMode::Durable, 1024));
    let mut acked = Vec::new();
    for i in 0..40 {
        acked.push((wal.append(payload(i), None).await.unwrap().lsn, payload(i)));
    }
    assert!(wal.segments().len() >= 3);
    fs.power_loss(&mut ChaCha8Rng::seed_from_u64(9), Tear::None);
    drop(wal);
    let (_, replayed) = recover(&fs, opts(SyncMode::Durable, 1024));
    assert_eq!(replayed, acked);
}

/// Invariant 9: a Fast-mode log that lost power before any sync still opens, keeps its
/// first segment, and accepts writes from LSN 1.
#[tokio::test(flavor = "multi_thread")]
async fn a_fresh_fast_log_survives_power_loss_before_any_sync() {
    for tear in TEARS {
        let fs = fs();
        let (wal, _) = recover(&fs, opts(SyncMode::Fast, 1 << 20));
        wal.append(payload(0), None).await.unwrap(); // written, never synced
        fs.power_loss(&mut ChaCha8Rng::seed_from_u64(11), tear);
        drop(wal);
        let (wal, replayed) = recover(&fs, opts(SyncMode::Fast, 1 << 20));
        assert!(replayed.len() <= 1, "{tear:?}: {replayed:?}");
        assert_eq!(
            wal.segments(),
            vec![1],
            "{tear:?}: the synced first segment must survive"
        );
        let loc = wal.append(payload(1), None).await.unwrap();
        assert_eq!(loc.lsn, replayed.len() as u64 + 1, "{tear:?}");
    }
}

/// Corruption in a sealed segment is refused, never truncated silently.
#[tokio::test(flavor = "multi_thread")]
async fn corruption_in_a_sealed_segment_refuses_to_open() {
    let fs = fs();
    let (wal, _) = recover(&fs, opts(SyncMode::Durable, 1024));
    for i in 0..40 {
        wal.append(payload(i), None).await.unwrap();
    }
    wal.close().unwrap();
    let first = Path::new(DIR).join(prkdb_core::wal::segment::segment_file_name(1));
    let f = fs.open(&first, OpenMode::ReadWrite).unwrap();
    f.write_at(
        prkdb_core::wal::segment::SEGMENT_HEADER_LEN + 30,
        &[0x5A; 8],
    )
    .unwrap();
    f.sync_data().unwrap();
    let r = Wal::open(
        Arc::new(fs.clone()),
        Path::new(DIR),
        opts(SyncMode::Durable, 1024),
        1,
        &mut |_, kind, _| {
            assert_eq!(kind, FrameKind::Batch);
            Ok(())
        },
    );
    assert!(r.is_err(), "sealed-segment corruption must refuse to open");
}

/// Holds every `sync_data` issued by the `prkdb-wal-syncer` thread: it reports the file
/// on `entered`, then waits for a message on `release` (or for `release` to be dropped).
/// Syncs from any other thread (the writer's roll, explicit `sync()`) pass straight
/// through.
struct SyncerGate {
    inner: Arc<dyn Vfs>,
    entered: Mutex<mpsc::Sender<PathBuf>>,
    release: Arc<Mutex<mpsc::Receiver<()>>>,
}
struct GatedFile {
    inner: Arc<dyn VfsFile>,
    path: PathBuf,
    entered: mpsc::Sender<PathBuf>,
    release: Arc<Mutex<mpsc::Receiver<()>>>,
}
impl VfsFile for GatedFile {
    fn write_at(&self, o: u64, b: &[u8]) -> io::Result<()> {
        self.inner.write_at(o, b)
    }
    fn read_at(&self, o: u64, b: &mut [u8]) -> io::Result<usize> {
        self.inner.read_at(o, b)
    }
    fn set_len(&self, l: u64) -> io::Result<()> {
        self.inner.set_len(l)
    }
    fn len(&self) -> io::Result<u64> {
        self.inner.len()
    }
    fn sync_data(&self) -> io::Result<()> {
        if std::thread::current().name() == Some("prkdb-wal-syncer") {
            let _ = self.entered.send(self.path.clone());
            let _ = self.release.lock().unwrap().recv();
        }
        self.inner.sync_data()
    }
}
impl SyncerGate {
    fn wrap(&self, path: &Path, inner: Arc<dyn VfsFile>) -> Arc<dyn VfsFile> {
        Arc::new(GatedFile {
            inner,
            path: path.to_path_buf(),
            entered: self.entered.lock().unwrap().clone(),
            release: self.release.clone(),
        })
    }
}
impl Vfs for SyncerGate {
    fn open(&self, p: &Path, m: OpenMode) -> io::Result<Arc<dyn VfsFile>> {
        Ok(self.wrap(p, self.inner.open(p, m)?))
    }
    fn create(&self, p: &Path) -> io::Result<Arc<dyn VfsFile>> {
        Ok(self.wrap(p, self.inner.create(p)?))
    }
    fn rename(&self, a: &Path, b: &Path) -> io::Result<()> {
        self.inner.rename(a, b)
    }
    fn remove(&self, p: &Path) -> io::Result<()> {
        self.inner.remove(p)
    }
    fn create_dir_all(&self, p: &Path) -> io::Result<()> {
        self.inner.create_dir_all(p)
    }
    fn read_dir(&self, p: &Path) -> io::Result<Vec<PathBuf>> {
        self.inner.read_dir(p)
    }
    fn exists(&self, p: &Path) -> io::Result<bool> {
        self.inner.exists(p)
    }
    fn sync_dir(&self, d: &Path) -> io::Result<()> {
        self.inner.sync_dir(d)
    }
}

/// Task 2.7 roll race: the syncer picks up segment 1 and starts syncing it; meanwhile the
/// writer rolls and writes LSN 2 into segment 2. When the syncer's sync of segment 1
/// returns, it must raise `durable_lsn` only to what it read before the sync (LSN 1),
/// never to LSN 2, which sits unsynced in a segment it never touched. The power cut then
/// shows the watermark is honest: everything at or below it survives.
#[test]
fn the_syncer_never_marks_frames_in_a_newer_segment_durable() {
    let seg = |lsn: Lsn| PathBuf::from(DIR).join(format!("{lsn:020}.wal"));
    let fs = fs();
    let (entered_tx, entered) = mpsc::channel();
    let (release, release_rx) = mpsc::channel();
    let gate = SyncerGate {
        inner: Arc::new(fs.clone()),
        entered: Mutex::new(entered_tx),
        release: Arc::new(Mutex::new(release_rx)),
    };
    // Header 24 + one 117-byte frame fits in 256 bytes; a second, 167-byte frame rolls.
    let o = WalOptions {
        sync_interval: Duration::from_millis(5),
        fast_sync: FastSync::SyncerThread,
        ..opts(SyncMode::Fast, 256)
    };
    let (wal, _) = Wal::open(Arc::new(gate), Path::new(DIR), o, 1, &mut |_, _, _| Ok(())).unwrap();
    let wait = |what: &str| {
        entered
            .recv_timeout(Duration::from_secs(5))
            .unwrap_or_else(|_| panic!("syncer never reached {what}"))
    };

    assert_eq!(wal.append_blocking(vec![1u8; 100], None).unwrap().lsn, 1);
    assert_eq!(wait("its first sync"), seg(1), "syncer syncs segment 1");

    // The syncer is inside sync_data(segment 1). Roll and write LSN 2 into segment 2.
    let loc = wal.append_blocking(vec![2u8; 150], None).unwrap();
    assert_eq!((loc.lsn, loc.segment), (2, 2), "the second write rolled");
    assert_eq!(wal.durable_lsn(), 1, "the roll's own sync covers segment 1");

    // Let the sync of segment 1 finish.
    release.send(()).unwrap();
    // A syncer that over-raised `durable_lsn` to 2 would see nothing left to sync and
    // never come back: report that as the watermark bug it is, not as a hang.
    let second = entered
        .recv_timeout(Duration::from_secs(2))
        .unwrap_or_else(|_| {
            panic!(
                "no second syncer sync; durable_lsn = {} (must be 1: LSN 2 was never synced)",
                wal.durable_lsn()
            )
        });
    assert_eq!(second, seg(2), "the syncer follows the swap");
    // The first iteration's `fetch_max` has run; the second sync is held.
    assert_eq!(
        wal.durable_lsn(),
        1,
        "a sync of segment 1 must not mark LSN 2 (segment 2) durable"
    );

    let durable = wal.durable_lsn();
    fs.power_loss(&mut ChaCha8Rng::seed_from_u64(7), Tear::Garbage);
    drop(release); // the held sync proceeds and fails on the stale handle
    drop(wal);
    let (_, replayed) = recover(&fs, opts(SyncMode::Fast, 256));
    assert!(
        replayed.len() as u64 >= durable,
        "kept {} frames, durable_lsn was {durable}",
        replayed.len()
    );
    assert_eq!(replayed[0], (1, vec![1u8; 100]));
}
