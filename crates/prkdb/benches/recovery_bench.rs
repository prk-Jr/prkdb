//! Recovery time for a large WAL (spec §6.1: "Recovery time for a 1 GiB WAL").
//!
//! Writes `RECOVERY_BENCH_BYTES` (default: 1 GiB) of 1 KiB values via `put_batch`,
//! flushes, then times `WalStorageAdapter::open_async` re-reading and rebuilding the
//! index from that WAL. The write phase runs once, outside the measured region; each
//! measured iteration reopens the same (unmodified) directory, since recovery is
//! read-only.
//!
//! `RECOVERY_BENCH_BYTES` exists so `scripts/capture_baseline.sh` (or a maintainer) can
//! scale the size down when 1 GiB would push a local run past the ~5 minute measurement
//! budget; see `docs/benchmarks/baseline-2026-09.toml` for the size actually used to
//! produce the committed baseline.

use criterion::{criterion_group, criterion_main, Criterion};
use prkdb::storage::WalStorageAdapter;
use prkdb_core::wal::WalConfig;
use prkdb_types::storage::StorageAdapter;
use tokio::runtime::Runtime;

const DEFAULT_TOTAL_BYTES: u64 = 1024 * 1024 * 1024; // 1 GiB
const VALUE_SIZE: usize = 1024; // 1 KiB
const BATCH_LEN: usize = 1000;

fn total_bytes() -> u64 {
    std::env::var("RECOVERY_BENCH_BYTES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_TOTAL_BYTES)
}

fn bench_recovery(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let target_bytes = total_bytes();

    // `benchmark_config` (1 GiB segments, 8 segments, 16 shards), not `test_config`
    // (1 MiB segments): a 1 GiB WAL under `test_config` would roll over 1000+ times,
    // which times segment-rotation overhead rather than recovery.
    let config = WalConfig {
        log_dir: dir.path().to_path_buf(),
        ..WalConfig::benchmark_config()
    };

    // Write phase: not measured. One adapter writes `target_bytes` of 1 KiB values in
    // batches of `BATCH_LEN`, then flushes and is dropped so the recovery path below
    // reopens a clean WAL rather than one still owned by a live writer.
    let written_bytes = rt.block_on(async {
        let adapter = WalStorageAdapter::new(config.clone()).expect("adapter builds");
        let mut written: u64 = 0;
        let mut next_id: u64 = 0;
        while written < target_bytes {
            let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..BATCH_LEN)
                .map(|_| {
                    next_id += 1;
                    (
                        format!("recovery-key-{next_id}").into_bytes(),
                        vec![b'x'; VALUE_SIZE],
                    )
                })
                .collect();
            written += (BATCH_LEN * VALUE_SIZE) as u64;
            adapter.put_batch(entries).await.expect("batch write");
        }
        adapter.flush().await.expect("flush before recovery");
        written
    });
    eprintln!(
        "recovery_bench: wrote {written_bytes} bytes to {} before timing open_async",
        dir.path().display()
    );

    let mut group = c.benchmark_group("recovery");
    // Recovering a large WAL is expensive; a small sample size keeps the total run
    // within the harness's time budget while still giving Criterion a spread to report.
    group.sample_size(10);

    group.bench_function("open_async_after_large_wal", |b| {
        b.to_async(&rt).iter(|| async {
            let adapter = WalStorageAdapter::open_async(config.clone())
                .await
                .expect("recovery succeeds");
            drop(adapter);
        });
    });

    group.finish();
}

criterion_group!(benches, bench_recovery);
criterion_main!(benches);
