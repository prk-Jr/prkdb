// ULTRA-OPTIMIZED PERFORMANCE BENCHMARK
// Tests extreme configurations for maximum throughput
//
// Run: cargo run --release --example ultra_performance

use prkdb::storage::WalStorageAdapter;
use prkdb_core::wal::WalConfig;
use prkdb_types::storage::StorageAdapter;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const TEST_DURATION_SECS: u64 = 10;
const NUM_WRITERS: usize = 4;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!();
    println!("╔═══════════════════════════════════════════════════════════════╗");
    println!("║           ⚡ ULTRA PERFORMANCE BENCHMARK ⚡                    ║");
    println!("╚═══════════════════════════════════════════════════════════════╝");
    println!();
    println!("  Testing extreme configurations for maximum throughput");
    println!("  Test duration: {}s per test", TEST_DURATION_SECS);
    println!();

    // Test 1: Batch size scaling with binary keys
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  TEST 1: Extreme Batch Sizes (8-byte keys)");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    for batch_size in [5000, 10000, 20000, 50000] {
        let dir = tempfile::tempdir()?;
        let config = WalConfig::benchmark_config();
        let storage = Arc::new(WalStorageAdapter::new(WalConfig {
            log_dir: dir.path().to_path_buf(),
            ..config
        })?);

        let ops = bench_extreme_batch(&storage, batch_size).await?;
        let status = if ops > 500000.0 {
            "🏆"
        } else if ops > 200000.0 {
            "✅"
        } else {
            "⚠️"
        };
        println!(
            "  Batch {:>6}: {:>12.0} writes/sec {}",
            batch_size, ops, status
        );
    }
    println!();

    // Test 2: Multi-threaded with extreme batches
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  TEST 2: Multi-Thread + Extreme Batches");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    for batch_size in [10000, 20000] {
        let dir = tempfile::tempdir()?;
        let storage = Arc::new(WalStorageAdapter::new(WalConfig {
            log_dir: dir.path().to_path_buf(),
            ..WalConfig::benchmark_config()
        })?);

        let ops = bench_extreme_multi(&storage, batch_size).await?;
        let status = if ops > 500000.0 {
            "🏆"
        } else if ops > 200000.0 {
            "✅"
        } else {
            "⚠️"
        };
        println!(
            "  {}T x {:>5}: {:>12.0} writes/sec {}",
            NUM_WRITERS, batch_size, ops, status
        );
    }
    println!();

    // Test 3 (segment count optimization) is gone: the adapter writes one globally
    // ordered WAL and has no segment count to tune (Task 2.8a).

    // Test 4: Index interval optimization
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  TEST 4: Index Interval Optimization");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    for index_interval in [1024, 4096, 16384, 65536] {
        let dir = tempfile::tempdir()?;
        let storage = Arc::new(WalStorageAdapter::new(WalConfig {
            log_dir: dir.path().to_path_buf(),
            index_interval_bytes: index_interval,
            ..WalConfig::benchmark_config()
        })?);

        let ops = bench_extreme_batch(&storage, 10000).await?;
        println!("  Index {:>5}B: {:>12.0} writes/sec", index_interval, ops);
    }
    println!();

    // Test 5: Ultimate configuration
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  TEST 5: ULTIMATE Configuration");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    let dir = tempfile::tempdir()?;
    let storage = Arc::new(WalStorageAdapter::new(WalConfig {
        log_dir: dir.path().to_path_buf(),
        index_interval_bytes: 16384, // Less frequent indexing
        batch_size: 10000,           // Larger WAL batches
        ..WalConfig::benchmark_config()
    })?);

    // Single-thread ultimate
    let single_ops = bench_extreme_batch(&storage, 20000).await?;
    println!("  Single-thread: {:>12.0} writes/sec", single_ops);

    // Multi-thread ultimate
    let multi_ops = bench_extreme_multi(&storage, 20000).await?;
    println!(
        "  Multi-thread:  {:>12.0} writes/sec ({} writers)",
        multi_ops, NUM_WRITERS
    );

    // Read performance (cached)
    // Pre-populate
    for batch in 0..50u64 {
        let mut entries = Vec::with_capacity(5000);
        for i in 0..5000usize {
            entries.push((make_key(batch * 5000 + i as u64), b"v".to_vec()));
        }
        storage.put_batch(entries).await?;
    }

    let read_ops = bench_reads(&storage).await?;
    println!("  Read (cached): {:>12.0} reads/sec", read_ops);
    println!();

    // Summary
    let peak = single_ops.max(multi_ops);
    println!("╔═══════════════════════════════════════════════════════════════╗");
    println!("║                  ⚡ PEAK RESULTS ⚡                            ║");
    println!("╚═══════════════════════════════════════════════════════════════╝");
    println!();
    println!("  🏆 Peak Write: {:>12.0} ops/sec", peak);
    println!("  📖 Peak Read:  {:>12.0} ops/sec", read_ops);
    println!();

    if peak > 200000.0 {
        println!("  ✅ EXCEEDED 200K writes/sec target!");
    }
    if peak > 500000.0 {
        println!("  🚀 EXCEEDED 500K writes/sec - KAFKA COMPETITIVE!");
    }
    println!();

    Ok(())
}

#[inline]
fn make_key(id: u64) -> Vec<u8> {
    id.to_le_bytes().to_vec()
}

async fn bench_extreme_batch(
    storage: &Arc<WalStorageAdapter>,
    batch_size: usize,
) -> anyhow::Result<f64> {
    let start = Instant::now();
    let deadline = start + Duration::from_secs(TEST_DURATION_SECS);
    let mut total_ops = 0u64;
    let mut id = 0u64;
    let value = b"v".to_vec();

    while Instant::now() < deadline {
        let mut entries = Vec::with_capacity(batch_size);
        for _ in 0..batch_size {
            entries.push((make_key(id), value.clone()));
            id += 1;
        }
        storage.put_batch(entries).await?;
        total_ops += batch_size as u64;
    }

    Ok(total_ops as f64 / start.elapsed().as_secs_f64())
}

async fn bench_extreme_multi(
    storage: &Arc<WalStorageAdapter>,
    batch_size: usize,
) -> anyhow::Result<f64> {
    let total_ops = Arc::new(AtomicU64::new(0));
    let start = Instant::now();

    let handles: Vec<_> = (0..NUM_WRITERS)
        .map(|wid| {
            let storage = storage.clone();
            let ops = total_ops.clone();
            let base_id = wid as u64 * 10_000_000_000;

            tokio::spawn(async move {
                let deadline = Instant::now() + Duration::from_secs(TEST_DURATION_SECS);
                let mut id = base_id;
                let mut local_ops = 0u64;
                let value = b"v".to_vec();

                while Instant::now() < deadline {
                    let mut entries = Vec::with_capacity(batch_size);
                    for _ in 0..batch_size {
                        entries.push((make_key(id), value.clone()));
                        id += 1;
                    }
                    let _ = storage.put_batch(entries).await;
                    local_ops += batch_size as u64;
                }

                ops.fetch_add(local_ops, Ordering::Relaxed);
            })
        })
        .collect();

    for h in handles {
        h.await?;
    }

    Ok(total_ops.load(Ordering::Relaxed) as f64 / start.elapsed().as_secs_f64())
}

async fn bench_reads(storage: &Arc<WalStorageAdapter>) -> anyhow::Result<f64> {
    let start = Instant::now();
    let deadline = start + Duration::from_secs(TEST_DURATION_SECS);
    let mut total_ops = 0u64;
    let max_id = 250_000u64;

    while Instant::now() < deadline {
        for i in 0..1000 {
            let key = make_key(i % max_id);
            let _ = storage.get(&key).await;
            total_ops += 1;
        }
    }

    Ok(total_ops as f64 / start.elapsed().as_secs_f64())
}
