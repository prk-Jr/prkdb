// Kafka-Style Mixed Workload Benchmark
// Tests producer/consumer patterns similar to Kafka
//
// Run: cargo run --release --example kafka_comparison

use prkdb::storage::WalStorageAdapter;
use prkdb_core::wal::WalConfig;
use prkdb_types::storage::StorageAdapter;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const TEST_DURATION_SECS: u64 = 10;
const BATCH_SIZE: usize = 500;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!();
    println!("╔═══════════════════════════════════════════════════════════════╗");
    println!("║         🔥 KAFKA-STYLE MIXED WORKLOAD BENCHMARK 🔥            ║");
    println!("╚═══════════════════════════════════════════════════════════════╝");
    println!();
    println!("  Simulates Kafka producer/consumer patterns");
    println!("  Batch size: {}", BATCH_SIZE);
    println!("  Test duration: {}s per test", TEST_DURATION_SECS);
    println!();

    // Pre-populate data for reads
    println!("⏳ Pre-populating 100K records for read tests...");
    let dir = tempfile::tempdir()?;
    let storage = Arc::new(WalStorageAdapter::new(WalConfig {
        log_dir: dir.path().to_path_buf(),
        ..WalConfig::test_config()
    })?);

    // Populate in batches
    for batch in 0..200 {
        let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..500)
            .map(|i| {
                let key = format!("preload_{}_{}", batch, i);
                let value = format!("value_data_for_key_{}", i);
                (key.into_bytes(), value.into_bytes())
            })
            .collect();
        storage.put_batch(entries).await?;
    }
    println!("✅ Pre-populated 100K records");
    println!();

    // Test 1: Pure Producer (batch writes)
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  TEST 1: Producer Only (100% Writes, batched)");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    let producer_ops = bench_producer(&storage).await?;
    println!("  ✅ Producer: {:.0} ops/sec", producer_ops);
    println!();

    // Test 2: Pure Consumer (batch reads)
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  TEST 2: Consumer Only (100% Reads)");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    let consumer_ops = bench_consumer(&storage).await?;
    println!("  ✅ Consumer: {:.0} ops/sec", consumer_ops);
    println!();

    // Test 3: Mixed 50/50 (producer + consumer)
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  TEST 3: Mixed 50/50 (Producer + Consumer)");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    let mixed_ops = bench_mixed(&storage, 50, 50).await?;
    println!("  ✅ Mixed 50/50: {:.0} ops/sec", mixed_ops);
    println!();

    // Test 4: Read-heavy (80/20)
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  TEST 4: Read-Heavy 80/20 (Consumer dominant)");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    let read_heavy_ops = bench_mixed(&storage, 80, 20).await?;
    println!("  ✅ Read-heavy 80/20: {:.0} ops/sec", read_heavy_ops);
    println!();

    // Test 5: Write-heavy (20/80)
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  TEST 5: Write-Heavy 20/80 (Producer dominant)");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    let write_heavy_ops = bench_mixed(&storage, 20, 80).await?;
    println!("  ✅ Write-heavy 20/80: {:.0} ops/sec", write_heavy_ops);
    println!();

    // Test 6: Multi-threaded mixed (4 producers + 4 consumers)
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  TEST 6: Multi-Threaded (4 producers + 4 consumers)");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    let mt_ops = bench_multi_threaded(&storage, 4, 4).await?;
    println!("  ✅ Multi-threaded: {:.0} ops/sec", mt_ops);
    println!();

    // Summary
    println!("╔═══════════════════════════════════════════════════════════════╗");
    println!("║                    📊 KAFKA COMPARISON 📊                     ║");
    println!("╚═══════════════════════════════════════════════════════════════╝");
    println!();
    println!("┌────────────────────────────────┬────────────────┬──────────────┐");
    println!("│ Workload                       │   PrkDB        │ Kafka (typ.) │");
    println!("├────────────────────────────────┼────────────────┼──────────────┤");
    println!(
        "│ Producer (batch writes)        │ {:>12.0} │ 100-500K     │",
        producer_ops
    );
    println!(
        "│ Consumer (reads)               │ {:>12.0} │ 100-500K     │",
        consumer_ops
    );
    println!(
        "│ Mixed 50/50                    │ {:>12.0} │ 50-200K      │",
        mixed_ops
    );
    println!(
        "│ Read-heavy 80/20               │ {:>12.0} │ 80-300K      │",
        read_heavy_ops
    );
    println!(
        "│ Write-heavy 20/80              │ {:>12.0} │ 40-150K      │",
        write_heavy_ops
    );
    println!(
        "│ Multi-threaded (4+4)           │ {:>12.0} │ 200-600K     │",
        mt_ops
    );
    println!("└────────────────────────────────┴────────────────┴──────────────┘");
    println!();

    let total = producer_ops + consumer_ops + mixed_ops + read_heavy_ops + write_heavy_ops + mt_ops;
    let avg = total / 6.0;
    println!("  📈 Average throughput: {:.0} ops/sec", avg);
    println!();

    Ok(())
}

async fn bench_producer(storage: &Arc<WalStorageAdapter>) -> anyhow::Result<f64> {
    let start = Instant::now();
    let deadline = start + Duration::from_secs(TEST_DURATION_SECS);
    let mut total_ops = 0u64;
    let mut batch_num = 0u64;

    while Instant::now() < deadline {
        let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..BATCH_SIZE)
            .map(|i| {
                (
                    format!("produce_{}_{}", batch_num, i).into_bytes(),
                    b"message_payload_data_here".to_vec(),
                )
            })
            .collect();

        storage.put_batch(entries).await?;
        total_ops += BATCH_SIZE as u64;
        batch_num += 1;
    }

    let duration = start.elapsed();
    Ok(total_ops as f64 / duration.as_secs_f64())
}

async fn bench_consumer(storage: &Arc<WalStorageAdapter>) -> anyhow::Result<f64> {
    let start = Instant::now();
    let deadline = start + Duration::from_secs(TEST_DURATION_SECS);
    let mut total_ops = 0u64;
    let mut rng = 42u64;

    while Instant::now() < deadline {
        // Simulate consumer reading messages sequentially
        for _ in 0..BATCH_SIZE {
            rng = rng.wrapping_mul(1103515245).wrapping_add(12345);
            let batch = rng % 200;
            let idx = rng % 500;
            let key = format!("preload_{}_{}", batch, idx);
            let _ = storage.get(key.as_bytes()).await;
            total_ops += 1;
        }
    }

    let duration = start.elapsed();
    Ok(total_ops as f64 / duration.as_secs_f64())
}

async fn bench_mixed(
    storage: &Arc<WalStorageAdapter>,
    read_pct: u32,
    write_pct: u32,
) -> anyhow::Result<f64> {
    let start = Instant::now();
    let deadline = start + Duration::from_secs(TEST_DURATION_SECS);
    let mut total_ops = 0u64;
    let mut rng = 12345u64;
    let mut batch_num = 0u64;

    while Instant::now() < deadline {
        rng = rng.wrapping_mul(1103515245).wrapping_add(12345);
        let is_read = (rng % 100) < read_pct as u64;

        if is_read {
            // Consumer: batch read
            for _ in 0..BATCH_SIZE {
                rng = rng.wrapping_mul(1103515245).wrapping_add(12345);
                let batch = rng % 200;
                let idx = rng % 500;
                let key = format!("preload_{}_{}", batch, idx);
                let _ = storage.get(key.as_bytes()).await;
                total_ops += 1;
            }
        } else {
            // Producer: batch write
            let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..BATCH_SIZE)
                .map(|i| {
                    (
                        format!("mixed_{}_{}", batch_num, i).into_bytes(),
                        b"message_payload".to_vec(),
                    )
                })
                .collect();

            storage.put_batch(entries).await?;
            total_ops += BATCH_SIZE as u64;
            batch_num += 1;
        }
    }

    let duration = start.elapsed();
    Ok(total_ops as f64 / duration.as_secs_f64())
}

async fn bench_multi_threaded(
    storage: &Arc<WalStorageAdapter>,
    num_producers: usize,
    num_consumers: usize,
) -> anyhow::Result<f64> {
    let total_ops = Arc::new(AtomicU64::new(0));
    let start = Instant::now();

    // Spawn producers
    let producer_handles: Vec<_> = (0..num_producers)
        .map(|pid| {
            let storage = storage.clone();
            let ops = total_ops.clone();

            tokio::spawn(async move {
                let deadline = Instant::now() + Duration::from_secs(TEST_DURATION_SECS);
                let mut batch_num = 0u64;
                let mut local_ops = 0u64;

                while Instant::now() < deadline {
                    let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..BATCH_SIZE)
                        .map(|i| {
                            (
                                format!("p{}_{}_{}", pid, batch_num, i).into_bytes(),
                                b"msg".to_vec(),
                            )
                        })
                        .collect();

                    let _ = storage.put_batch(entries).await;
                    local_ops += BATCH_SIZE as u64;
                    batch_num += 1;
                }

                ops.fetch_add(local_ops, Ordering::Relaxed);
            })
        })
        .collect();

    // Spawn consumers
    let consumer_handles: Vec<_> = (0..num_consumers)
        .map(|cid| {
            let storage = storage.clone();
            let ops = total_ops.clone();

            tokio::spawn(async move {
                let deadline = Instant::now() + Duration::from_secs(TEST_DURATION_SECS);
                let mut rng = cid as u64 * 1000;
                let mut local_ops = 0u64;

                while Instant::now() < deadline {
                    for _ in 0..BATCH_SIZE {
                        rng = rng.wrapping_mul(1103515245).wrapping_add(12345);
                        let batch = rng % 200;
                        let idx = rng % 500;
                        let key = format!("preload_{}_{}", batch, idx);
                        let _ = storage.get(key.as_bytes()).await;
                        local_ops += 1;
                    }
                }

                ops.fetch_add(local_ops, Ordering::Relaxed);
            })
        })
        .collect();

    // Wait for all
    for h in producer_handles {
        h.await?;
    }
    for h in consumer_handles {
        h.await?;
    }

    let duration = start.elapsed();
    Ok(total_ops.load(Ordering::Relaxed) as f64 / duration.as_secs_f64())
}
