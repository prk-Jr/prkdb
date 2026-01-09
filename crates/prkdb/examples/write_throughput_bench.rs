// Write Throughput Optimization Benchmark
// Tests various write optimization strategies
//
// Run: cargo run --release --example write_throughput_bench

use prkdb::storage::WalStorageAdapter;
use prkdb_core::storage::StorageAdapter;
use prkdb_core::wal::WalConfig;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const TEST_DURATION_SECS: u64 = 10;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!();
    println!("╔═══════════════════════════════════════════════════════════════╗");
    println!("║         🚀 WRITE THROUGHPUT OPTIMIZATION BENCHMARK 🚀         ║");
    println!("╚═══════════════════════════════════════════════════════════════╝");
    println!();
    println!("  Testing various batch sizes and write strategies");
    println!("  Target: 200K+ writes/sec (Kafka-competitive)");
    println!("  Test duration: {}s per test", TEST_DURATION_SECS);
    println!();

    let dir = tempfile::tempdir()?;
    let storage = Arc::new(WalStorageAdapter::new(WalConfig {
        log_dir: dir.path().to_path_buf(),
        ..WalConfig::test_config()
    })?);

    // Test 1: Batch size optimization
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  TEST 1: Optimal Batch Size");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    for batch_size in [100, 500, 1000, 2000, 5000] {
        let ops = bench_batch_writes(&storage, batch_size).await?;
        let status = if ops > 200000.0 {
            "🏆"
        } else if ops > 100000.0 {
            "✅"
        } else {
            "⚠️"
        };
        println!(
            "  Batch {:>5}: {:>12.0} writes/sec {}",
            batch_size, ops, status
        );
    }
    println!();

    // Test 2: Multi-threaded writes
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  TEST 2: Multi-Threaded Writers");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    for num_writers in [1, 2, 4, 8] {
        let ops = bench_multi_writer(&storage, num_writers, 1000).await?;
        let status = if ops > 200000.0 {
            "🏆"
        } else if ops > 100000.0 {
            "✅"
        } else {
            "⚠️"
        };
        println!(
            "  Writers {:>2}: {:>12.0} writes/sec {}",
            num_writers, ops, status
        );
    }
    println!();

    // Test 3: Key size optimization
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  TEST 3: Key Size Impact");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    for key_size in [8, 16, 32, 64, 128] {
        let ops = bench_key_size(&storage, key_size, 1000).await?;
        println!("  Key {:>3}B: {:>12.0} writes/sec", key_size, ops);
    }
    println!();

    // Test 4: Value size optimization
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  TEST 4: Value Size Impact");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    for value_size in [64, 256, 1024, 4096] {
        let ops = bench_value_size(&storage, value_size, 1000).await?;
        let mb_sec = (ops * value_size as f64) / (1024.0 * 1024.0);
        println!(
            "  Value {:>4}B: {:>10.0} writes/sec ({:.1} MB/s)",
            value_size, ops, mb_sec
        );
    }
    println!();

    // Test 5: Pre-allocated vs dynamic
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  TEST 5: Allocation Strategy");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    let ops_format = bench_batch_writes(&storage, 1000).await?;
    let ops_prealloc = bench_preallocated(&storage, 1000).await?;

    println!("  format!():     {:>10.0} writes/sec", ops_format);
    println!("  Pre-allocated: {:>10.0} writes/sec", ops_prealloc);
    let improvement = (ops_prealloc / ops_format - 1.0) * 100.0;
    if improvement > 0.0 {
        println!("  Improvement:   {:>10.1}%", improvement);
    }
    println!();

    // Summary
    println!("╔═══════════════════════════════════════════════════════════════╗");
    println!("║                📊 RECOMMENDATIONS 📊                          ║");
    println!("╚═══════════════════════════════════════════════════════════════╝");
    println!();
    println!("  For maximum write throughput:");
    println!("    1. Use batch size 1000-2000");
    println!("    2. Use 4+ writer threads");
    println!("    3. Keep keys short (<32 bytes)");
    println!("    4. Pre-allocate key buffers");
    println!();

    Ok(())
}

async fn bench_batch_writes(
    storage: &Arc<WalStorageAdapter>,
    batch_size: usize,
) -> anyhow::Result<f64> {
    let start = Instant::now();
    let deadline = start + Duration::from_secs(TEST_DURATION_SECS);
    let mut total_ops = 0u64;
    let mut batch_num = 0u64;

    while Instant::now() < deadline {
        let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..batch_size)
            .map(|i| {
                (
                    format!("k{}_{}", batch_num, i).into_bytes(),
                    b"value_data_payload".to_vec(),
                )
            })
            .collect();

        storage.put_batch(entries).await?;
        total_ops += batch_size as u64;
        batch_num += 1;
    }

    Ok(total_ops as f64 / start.elapsed().as_secs_f64())
}

async fn bench_multi_writer(
    storage: &Arc<WalStorageAdapter>,
    num_writers: usize,
    batch_size: usize,
) -> anyhow::Result<f64> {
    let total_ops = Arc::new(AtomicU64::new(0));
    let start = Instant::now();

    let handles: Vec<_> = (0..num_writers)
        .map(|wid| {
            let storage = storage.clone();
            let ops = total_ops.clone();

            tokio::spawn(async move {
                let deadline = Instant::now() + Duration::from_secs(TEST_DURATION_SECS);
                let mut batch_num = 0u64;
                let mut local_ops = 0u64;

                while Instant::now() < deadline {
                    let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..batch_size)
                        .map(|i| {
                            (
                                format!("w{}:{}:{}", wid, batch_num, i).into_bytes(),
                                b"val".to_vec(),
                            )
                        })
                        .collect();

                    let _ = storage.put_batch(entries).await;
                    local_ops += batch_size as u64;
                    batch_num += 1;
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

async fn bench_key_size(
    storage: &Arc<WalStorageAdapter>,
    key_size: usize,
    batch_size: usize,
) -> anyhow::Result<f64> {
    let start = Instant::now();
    let deadline = start + Duration::from_secs(TEST_DURATION_SECS);
    let mut total_ops = 0u64;
    let mut batch_num = 0u64;

    // Pre-allocate key template
    let key_template: Vec<u8> = vec![b'x'; key_size];

    while Instant::now() < deadline {
        let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..batch_size)
            .map(|i| {
                let mut key = key_template.clone();
                // Overwrite first bytes with unique identifier
                let id = format!("{:08}_{:06}", batch_num, i);
                let id_bytes = id.as_bytes();
                let copy_len = id_bytes.len().min(key.len());
                key[..copy_len].copy_from_slice(&id_bytes[..copy_len]);
                (key, b"v".to_vec())
            })
            .collect();

        storage.put_batch(entries).await?;
        total_ops += batch_size as u64;
        batch_num += 1;
    }

    Ok(total_ops as f64 / start.elapsed().as_secs_f64())
}

async fn bench_value_size(
    storage: &Arc<WalStorageAdapter>,
    value_size: usize,
    batch_size: usize,
) -> anyhow::Result<f64> {
    let start = Instant::now();
    let deadline = start + Duration::from_secs(TEST_DURATION_SECS);
    let mut total_ops = 0u64;
    let mut batch_num = 0u64;

    // Pre-allocate value template
    let value_template: Vec<u8> = vec![b'v'; value_size];

    while Instant::now() < deadline {
        let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..batch_size)
            .map(|i| {
                (
                    format!("k{}_{}", batch_num, i).into_bytes(),
                    value_template.clone(),
                )
            })
            .collect();

        storage.put_batch(entries).await?;
        total_ops += batch_size as u64;
        batch_num += 1;
    }

    Ok(total_ops as f64 / start.elapsed().as_secs_f64())
}

async fn bench_preallocated(
    storage: &Arc<WalStorageAdapter>,
    batch_size: usize,
) -> anyhow::Result<f64> {
    let start = Instant::now();
    let deadline = start + Duration::from_secs(TEST_DURATION_SECS);
    let mut total_ops = 0u64;
    let mut batch_num = 0u64;

    // Pre-allocate value
    let value = b"value_data_payload".to_vec();

    while Instant::now() < deadline {
        let mut entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(batch_size);

        for i in 0..batch_size {
            // Use pre-allocated key building
            let mut key = Vec::with_capacity(20);
            key.push(b'k');

            // Fast integer to bytes
            let mut num = batch_num;
            let mut digits = [0u8; 20];
            let mut pos = 20;
            loop {
                pos -= 1;
                digits[pos] = b'0' + (num % 10) as u8;
                num /= 10;
                if num == 0 {
                    break;
                }
            }
            key.extend_from_slice(&digits[pos..]);
            key.push(b'_');

            num = i as u64;
            pos = 20;
            loop {
                pos -= 1;
                digits[pos] = b'0' + (num % 10) as u8;
                num /= 10;
                if num == 0 {
                    break;
                }
            }
            key.extend_from_slice(&digits[pos..]);

            entries.push((key, value.clone()));
        }

        storage.put_batch(entries).await?;
        total_ops += batch_size as u64;
        batch_num += 1;
    }

    Ok(total_ops as f64 / start.elapsed().as_secs_f64())
}
