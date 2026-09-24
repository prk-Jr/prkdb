//! `Wal` under simulated power loss (Task 2.6, STO-04). FaultFs rules: faultfs.rs module
//! docs.

use prkdb_core::vfs::{OpenMode, Vfs};
use prkdb_core::wal::frame::FrameKind;
use prkdb_core::wal::{Lsn, SyncMode, Wal, WalOptions};
use prkdb_verify::faultfs::{FaultFs, Tear};
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use std::path::Path;
use std::sync::Arc;
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
    for seed in 0..25u64 {
        for tear in TEARS {
            let fs = fs();
            let (wal, _) = recover(&fs, opts(SyncMode::Fast, 1 << 20));
            for i in 0..20 {
                wal.append(payload(i), None).await.unwrap();
            }
            let synced = wal.sync().await.unwrap();
            for i in 20..40 {
                wal.append(payload(i), None).await.unwrap();
            }
            fs.power_loss(&mut ChaCha8Rng::seed_from_u64(seed), tear);
            drop(wal);
            let (_, replayed) = recover(&fs, opts(SyncMode::Fast, 1 << 20));
            let n = replayed.len() as u64;
            assert!(
                n >= synced && n <= 40,
                "seed {seed} tear {tear:?}: kept {n}, synced {synced}"
            );
            let expected: Vec<_> = (0..n).map(|i| (i + 1, payload(i))).collect();
            assert_eq!(
                replayed, expected,
                "seed {seed} tear {tear:?}: not a prefix"
            );
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
