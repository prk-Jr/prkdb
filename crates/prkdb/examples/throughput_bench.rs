// High-Throughput Benchmark - Matches Kafka's producer-perf-test parameters
//
// Run: cargo run --release --example throughput_bench
//
// This benchmark uses the same parameters as Kafka's kafka-producer-perf-test:
// - 1 million records
// - 100 byte record size
// - 10,000 batch size

use prkdb::storage::WalStorageAdapter;
use prkdb_core::wal::WalConfig;
use prkdb_types::storage::StorageAdapter;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

// Match Kafka's test parameters exactly
const NUM_RECORDS: usize = 1_000_000;
const RECORD_SIZE: usize = 100; // bytes
const BATCH_SIZE: usize = 10_000; // Match Kafka's batch.size

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!();
    println!("╔════════════════════════════════════════════════════════════════╗");
    println!("║    🚀 HIGH-THROUGHPUT BENCHMARK (Kafka Parameters) 🚀          ║");
    println!("╚════════════════════════════════════════════════════════════════╝");
    println!();
    println!("  Configuration (matches kafka-producer-perf-test):");
    println!("    Records: {:>12}", NUM_RECORDS);
    println!("    Record Size: {:>8} bytes", RECORD_SIZE);
    println!("    Batch Size: {:>9}", BATCH_SIZE);
    println!(
        "    Total Data: {:>9} MB",
        NUM_RECORDS * RECORD_SIZE / 1024 / 1024
    );
    println!();

    // Create storage
    let dir = tempfile::tempdir()?;
    let storage = Arc::new(WalStorageAdapter::new(WalConfig {
        log_dir: dir.path().to_path_buf(),
        ..WalConfig::test_config()
    })?);

    // Generate payload template (100 bytes of data)
    let payload: Vec<u8> = (0..RECORD_SIZE).map(|i| (i % 256) as u8).collect();

    // ═══════════════════════════════════════════════════════════════════════
    // TEST 1: Producer (Batch Writes)
    // ═══════════════════════════════════════════════════════════════════════
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!(
        "  TEST 1: Producer (Batch Writes - {} per batch)",
        BATCH_SIZE
    );
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    let start = Instant::now();
    let mut total_records = 0usize;
    let mut batch_num = 0u64;

    while total_records < NUM_RECORDS {
        let batch_count = std::cmp::min(BATCH_SIZE, NUM_RECORDS - total_records);
        let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..batch_count)
            .map(|i| {
                (
                    format!("key_{}_{}", batch_num, i).into_bytes(),
                    payload.clone(),
                )
            })
            .collect();

        storage.put_batch(entries).await?;
        total_records += batch_count;
        batch_num += 1;

        // Progress indicator
        if batch_num % 10 == 0 {
            let pct = (total_records as f64 / NUM_RECORDS as f64) * 100.0;
            print!(
                "\r  Progress: {:.1}% ({}/{})",
                pct, total_records, NUM_RECORDS
            );
            use std::io::Write;
            std::io::stdout().flush()?;
        }
    }

    let producer_duration = start.elapsed();
    let producer_records_sec = NUM_RECORDS as f64 / producer_duration.as_secs_f64();
    let producer_mb_sec =
        (NUM_RECORDS * RECORD_SIZE) as f64 / producer_duration.as_secs_f64() / 1024.0 / 1024.0;

    println!("\r  ✅ Producer Complete!                              ");
    println!();
    println!("     Records:    {:>12}", NUM_RECORDS);
    println!(
        "     Duration:   {:>12.2}s",
        producer_duration.as_secs_f64()
    );
    println!(
        "     Throughput: {:>12.0} records/sec",
        producer_records_sec
    );
    println!("     Throughput: {:>12.2} MB/sec", producer_mb_sec);
    println!();

    // ═══════════════════════════════════════════════════════════════════════
    // TEST 2: Consumer (Sequential Reads)
    // ═══════════════════════════════════════════════════════════════════════
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  TEST 2: Consumer (Random Reads)");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    let start = Instant::now();
    let mut rng = 42u64;
    let num_batches = NUM_RECORDS / BATCH_SIZE;

    for i in 0..NUM_RECORDS {
        rng = rng.wrapping_mul(1103515245).wrapping_add(12345);
        let batch = rng % num_batches as u64;
        let idx = rng % BATCH_SIZE as u64;
        let key = format!("key_{}_{}", batch, idx);
        let _ = storage.get(key.as_bytes()).await;

        if i % 100_000 == 0 && i > 0 {
            let pct = (i as f64 / NUM_RECORDS as f64) * 100.0;
            print!("\r  Progress: {:.1}% ({}/{})", pct, i, NUM_RECORDS);
            use std::io::Write;
            std::io::stdout().flush()?;
        }
    }

    let consumer_duration = start.elapsed();
    let consumer_records_sec = NUM_RECORDS as f64 / consumer_duration.as_secs_f64();
    let consumer_mb_sec =
        (NUM_RECORDS * RECORD_SIZE) as f64 / consumer_duration.as_secs_f64() / 1024.0 / 1024.0;

    println!("\r  ✅ Consumer Complete!                              ");
    println!();
    println!("     Records:    {:>12}", NUM_RECORDS);
    println!(
        "     Duration:   {:>12.2}s",
        consumer_duration.as_secs_f64()
    );
    println!(
        "     Throughput: {:>12.0} records/sec",
        consumer_records_sec
    );
    println!("     Throughput: {:>12.2} MB/sec", consumer_mb_sec);
    println!();

    // ═══════════════════════════════════════════════════════════════════════
    // TEST 3: Multi-Threaded Producer
    // ═══════════════════════════════════════════════════════════════════════
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  TEST 3: Multi-Threaded Producer (4 threads)");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    let total_mt = Arc::new(AtomicU64::new(0));
    let records_per_thread = NUM_RECORDS / 4;

    let start = Instant::now();

    let handles: Vec<_> = (0..4)
        .map(|tid| {
            let storage = storage.clone();
            let payload = payload.clone();
            let total = total_mt.clone();

            tokio::spawn(async move {
                let mut written = 0usize;
                let mut batch_num = 0u64;

                while written < records_per_thread {
                    let batch_count = std::cmp::min(BATCH_SIZE, records_per_thread - written);
                    let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..batch_count)
                        .map(|i| {
                            (
                                format!("mt_{}_{}_{}", tid, batch_num, i).into_bytes(),
                                payload.clone(),
                            )
                        })
                        .collect();

                    let _ = storage.put_batch(entries).await;
                    written += batch_count;
                    batch_num += 1;
                    total.fetch_add(batch_count as u64, Ordering::Relaxed);
                }
            })
        })
        .collect();

    for h in handles {
        h.await?;
    }

    let mt_duration = start.elapsed();
    let mt_records = total_mt.load(Ordering::Relaxed) as usize;
    let mt_records_sec = mt_records as f64 / mt_duration.as_secs_f64();
    let mt_mb_sec = (mt_records * RECORD_SIZE) as f64 / mt_duration.as_secs_f64() / 1024.0 / 1024.0;

    println!("  ✅ Multi-Threaded Complete!");
    println!();
    println!("     Records:    {:>12}", mt_records);
    println!("     Duration:   {:>12.2}s", mt_duration.as_secs_f64());
    println!("     Throughput: {:>12.0} records/sec", mt_records_sec);
    println!("     Throughput: {:>12.2} MB/sec", mt_mb_sec);
    println!();

    // ═══════════════════════════════════════════════════════════════════════
    // SUMMARY
    // ═══════════════════════════════════════════════════════════════════════
    println!("╔════════════════════════════════════════════════════════════════╗");
    println!("║                      📊 SUMMARY 📊                             ║");
    println!("╚════════════════════════════════════════════════════════════════╝");
    println!();
    println!("┌─────────────────────────┬─────────────────┬─────────────────┐");
    println!("│ Workload                │ Records/sec     │ MB/sec          │");
    println!("├─────────────────────────┼─────────────────┼─────────────────┤");
    println!(
        "│ Producer (single)       │ {:>15.0} │ {:>15.2} │",
        producer_records_sec, producer_mb_sec
    );
    println!(
        "│ Consumer (random)       │ {:>15.0} │ {:>15.2} │",
        consumer_records_sec, consumer_mb_sec
    );
    println!(
        "│ Producer (4 threads)    │ {:>15.0} │ {:>15.2} │",
        mt_records_sec, mt_mb_sec
    );
    println!("└─────────────────────────┴─────────────────┴─────────────────┘");
    println!();

    // Compare to Kafka
    println!("  📈 Kafka Reference (from your system):");
    println!("     Producer: ~37.64 MB/sec (394K records/sec)");
    println!("     Consumer: ~25.02 MB/sec");
    println!();

    Ok(())
}
