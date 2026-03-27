// Streaming Storage Benchmark - Phase 24A
//
// Tests the new StreamingStorageAdapter throughput
// Run: cargo run --release --example streaming_bench

use prkdb::storage::{StreamingConfig, StreamingRecord, StreamingStorageAdapter};
use std::time::Instant;

const NUM_RECORDS: usize = 1_000_000;
const RECORD_SIZE: usize = 100;
const BATCH_SIZE: usize = 10_000;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!();
    println!("╔════════════════════════════════════════════════════════════════╗");
    println!("║    🚀 STREAMING STORAGE BENCHMARK (Phase 24A) 🚀               ║");
    println!("╚════════════════════════════════════════════════════════════════╝");
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

    // Create streaming adapter
    let dir = tempfile::tempdir()?;
    let config = StreamingConfig {
        log_dir: dir.path().to_path_buf(),
        segment_count: 4,
        sync_each_batch: false,
        ..Default::default()
    };

    let adapter = StreamingStorageAdapter::new(config).await?;

    // Generate payload
    let payload: Vec<u8> = (0..RECORD_SIZE).map(|i| (i % 256) as u8).collect();

    // ═══════════════════════════════════════════════════════════════════════
    // TEST 1: Single-Threaded Streaming Writes
    // ═══════════════════════════════════════════════════════════════════════
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  TEST 1: Streaming Append (batch encoding, no indexing)");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    let start = Instant::now();
    let mut total_records = 0usize;
    let mut batch_num = 0u64;

    while total_records < NUM_RECORDS {
        let batch_count = std::cmp::min(BATCH_SIZE, NUM_RECORDS - total_records);

        let records: Vec<StreamingRecord> = (0..batch_count)
            .map(|i| StreamingRecord {
                key: format!("key_{}_{}", batch_num, i).into_bytes(),
                value: payload.clone(),
            })
            .collect();

        adapter.append_batch(records).await?;
        total_records += batch_count;
        batch_num += 1;

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
    // SUMMARY
    // ═══════════════════════════════════════════════════════════════════════
    println!("╔════════════════════════════════════════════════════════════════╗");
    println!("║                      📊 COMPARISON 📊                          ║");
    println!("╚════════════════════════════════════════════════════════════════╝");
    println!();
    println!("┌─────────────────────────┬─────────────────┬─────────────────┐");
    println!("│ Mode                    │ Records/sec     │ MB/sec          │");
    println!("├─────────────────────────┼─────────────────┼─────────────────┤");
    println!(
        "│ Streaming (Phase 24A)   │ {:>15.0} │ {:>15.2} │",
        records_sec, mb_sec
    );
    println!("├─────────────────────────┼─────────────────┼─────────────────┤");
    println!("│ Raw WAL (reference)     │        ~822,000 │          ~78.40 │");
    println!("│ With Index (reference)  │        ~117,000 │          ~11.15 │");
    println!("│ Kafka (reference)       │        ~412,000 │          ~39.28 │");
    println!("└─────────────────────────┴─────────────────┴─────────────────┘");
    println!();

    let kafka_ratio = mb_sec / 39.28;
    println!("  📈 Streaming vs Kafka: {:.1}x", kafka_ratio);
    println!();

    Ok(())
}
