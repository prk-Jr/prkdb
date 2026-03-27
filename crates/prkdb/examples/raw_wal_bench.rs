// Raw WAL Benchmark - No Indexing
// Tests pure WAL append performance without the StorageAdapter's index overhead
//
// Run: cargo run --release --example raw_wal_bench

use prkdb_core::wal::mmap_parallel_wal::MmapParallelWal;
use prkdb_core::wal::{LogOperation, LogRecord, WalConfig};
use std::sync::Arc;
use std::time::Instant;

// Match Kafka's test parameters
const NUM_RECORDS: usize = 1_000_000;
const RECORD_SIZE: usize = 100;
const BATCH_SIZE: usize = 10_000;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!();
    println!("╔════════════════════════════════════════════════════════════════╗");
    println!("║      🚀 RAW WAL BENCHMARK (No Indexing) 🚀                     ║");
    println!("╚════════════════════════════════════════════════════════════════╝");
    println!();
    println!("  This test writes directly to the WAL, bypassing the");
    println!("  StorageAdapter's in-memory index for maximum throughput.");
    println!();
    println!("  Configuration:");
    println!("    Records: {:>12}", NUM_RECORDS);
    println!("    Record Size: {:>8} bytes", RECORD_SIZE);
    println!("    Batch Size: {:>9}", BATCH_SIZE);
    println!(
        "    Total Data: {:>9} MB",
        NUM_RECORDS * RECORD_SIZE / 1024 / 1024
    );
    println!();

    // Create WAL directly (bypassing StorageAdapter)
    let dir = tempfile::tempdir()?;
    let config = WalConfig {
        log_dir: dir.path().to_path_buf(),
        segment_count: 4,
        ..WalConfig::default()
    };

    let wal = Arc::new(MmapParallelWal::create(config, 4).await?);

    // Generate payload (100 bytes)
    let payload: Vec<u8> = (0..RECORD_SIZE).map(|i| (i % 256) as u8).collect();

    // ═══════════════════════════════════════════════════════════════════════
    // TEST 1: Raw WAL Append (No Index)
    // ═══════════════════════════════════════════════════════════════════════
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  TEST 1: Raw WAL Append (No Index Updates)");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    let start = Instant::now();
    let mut total_records = 0usize;
    let mut batch_num = 0u64;

    while total_records < NUM_RECORDS {
        let batch_count = std::cmp::min(BATCH_SIZE, NUM_RECORDS - total_records);

        // Create LogRecords directly
        let records: Vec<LogRecord> = (0..batch_count)
            .map(|i| {
                LogRecord::new(LogOperation::Put {
                    collection: "bench".to_string(),
                    id: format!("key_{}_{}", batch_num, i).into_bytes(),
                    data: payload.clone(),
                })
            })
            .collect();

        // Write directly to WAL (no index update!)
        wal.append_batch(records).await?;
        total_records += batch_count;
        batch_num += 1;

        // Progress
        if batch_num.is_multiple_of(10) {
            let pct = (total_records as f64 / NUM_RECORDS as f64) * 100.0;
            print!(
                "\r  Progress: {:.1}% ({}/{})",
                pct, total_records, NUM_RECORDS
            );
            use std::io::Write;
            std::io::stdout().flush()?;
        }
    }

    let duration = start.elapsed();
    let records_sec = NUM_RECORDS as f64 / duration.as_secs_f64();
    let mb_sec = (NUM_RECORDS * RECORD_SIZE) as f64 / duration.as_secs_f64() / 1024.0 / 1024.0;

    println!("\r  ✅ Complete!                                        ");
    println!();
    println!("     Records:    {:>12}", NUM_RECORDS);
    println!("     Duration:   {:>12.2}s", duration.as_secs_f64());
    println!("     Throughput: {:>12.0} records/sec", records_sec);
    println!("     Throughput: {:>12.2} MB/sec", mb_sec);
    println!();

    // ═══════════════════════════════════════════════════════════════════════
    // TEST 2: Multi-Threaded Raw WAL
    // ═══════════════════════════════════════════════════════════════════════
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  TEST 2: Multi-Threaded Raw WAL (4 threads)");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    // Create fresh WAL for MT test
    let dir2 = tempfile::tempdir()?;
    let config2 = WalConfig {
        log_dir: dir2.path().to_path_buf(),
        segment_count: 4,
        ..WalConfig::default()
    };
    let wal2 = Arc::new(MmapParallelWal::create(config2, 4).await?);

    let records_per_thread = NUM_RECORDS / 4;
    let total_written = Arc::new(std::sync::atomic::AtomicU64::new(0));

    let start = Instant::now();

    let handles: Vec<_> = (0..4)
        .map(|tid| {
            let wal = wal2.clone();
            let payload = payload.clone();
            let total = total_written.clone();

            tokio::spawn(async move {
                let mut written = 0usize;
                let mut batch_num = 0u64;

                while written < records_per_thread {
                    let batch_count = std::cmp::min(BATCH_SIZE, records_per_thread - written);

                    let records: Vec<LogRecord> = (0..batch_count)
                        .map(|i| {
                            LogRecord::new(LogOperation::Put {
                                collection: "bench".to_string(),
                                id: format!("mt_{}_{}_{}", tid, batch_num, i).into_bytes(),
                                data: payload.clone(),
                            })
                        })
                        .collect();

                    let _ = wal.append_batch(records).await;
                    written += batch_count;
                    batch_num += 1;
                    total.fetch_add(batch_count as u64, std::sync::atomic::Ordering::Relaxed);
                }
            })
        })
        .collect();

    for h in handles {
        h.await?;
    }

    let mt_duration = start.elapsed();
    let mt_records = total_written.load(std::sync::atomic::Ordering::Relaxed) as usize;
    let mt_records_sec = mt_records as f64 / mt_duration.as_secs_f64();
    let mt_mb_sec = (mt_records * RECORD_SIZE) as f64 / mt_duration.as_secs_f64() / 1024.0 / 1024.0;

    println!("  ✅ Complete!");
    println!();
    println!("     Records:    {:>12}", mt_records);
    println!("     Duration:   {:>12.2}s", mt_duration.as_secs_f64());
    println!("     Throughput: {:>12.0} records/sec", mt_records_sec);
    println!("     Throughput: {:>12.2} MB/sec", mt_mb_sec);
    println!();

    // Summary
    println!("╔════════════════════════════════════════════════════════════════╗");
    println!("║                      📊 COMPARISON 📊                          ║");
    println!("╚════════════════════════════════════════════════════════════════╝");
    println!();
    println!("┌─────────────────────────┬─────────────────┬─────────────────┐");
    println!("│ Mode                    │ Records/sec     │ MB/sec          │");
    println!("├─────────────────────────┼─────────────────┼─────────────────┤");
    println!(
        "│ Raw WAL (no index)      │ {:>15.0} │ {:>15.2} │",
        records_sec, mb_sec
    );
    println!(
        "│ Raw WAL (4 threads)     │ {:>15.0} │ {:>15.2} │",
        mt_records_sec, mt_mb_sec
    );
    println!("├─────────────────────────┼─────────────────┼─────────────────┤");
    println!("│ With Index (reference)  │        ~117,000 │          ~11.15 │");
    println!("│ Kafka (reference)       │        ~412,000 │          ~39.28 │");
    println!("└─────────────────────────┴─────────────────┴─────────────────┘");
    println!();

    let speedup = mb_sec / 11.15;
    println!("  📈 Raw WAL vs Indexed: {:.1}x faster", speedup);
    println!();

    Ok(())
}
