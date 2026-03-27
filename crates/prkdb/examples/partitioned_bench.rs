// Partitioned Streaming Benchmark - Phase 24C
//
// Tests multi-partition streaming for horizontal scaling
// Run: cargo run --release --example partitioned_bench

use prkdb::storage::{
    PartitionStrategy, PartitionedStreamingAdapter, PartitionedStreamingConfig, StreamingRecord,
};
use std::time::Instant;

const NUM_RECORDS: usize = 1_000_000;
const RECORD_SIZE: usize = 100;
const BATCH_SIZE: usize = 10_000;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!();
    println!("╔════════════════════════════════════════════════════════════════╗");
    println!("║    🚀 PARTITIONED STREAMING BENCHMARK (Phase 24C) 🚀           ║");
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

    let payload: Vec<u8> = (0..RECORD_SIZE).map(|i| (i % 256) as u8).collect();

    // Test with different partition counts
    let partition_counts = [1, 2, 4, 8];
    let mut results: Vec<(usize, f64, f64)> = Vec::new();

    for &partition_count in &partition_counts {
        println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        println!("  TEST: {} Partitions (Parallel Writes)", partition_count);
        println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

        let dir = tempfile::tempdir()?;
        let config = PartitionedStreamingConfig {
            base_dir: dir.path().to_path_buf(),
            partition_count,
            segments_per_partition: 2,
            sync_each_batch: false,
        };

        let adapter =
            PartitionedStreamingAdapter::with_strategy(config, PartitionStrategy::RoundRobin)
                .await?;

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

            // Use parallel append for maximum throughput
            adapter.append_batch_parallel(records).await?;
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
        println!("     Partitions: {:>10}", partition_count);
        println!("     Records:    {:>12}", NUM_RECORDS);
        println!("     Duration:   {:>12.2}s", duration.as_secs_f64());
        println!("     Throughput: {:>12.0} records/sec", records_sec);
        println!("     Throughput: {:>12.2} MB/sec", mb_sec);
        println!();

        results.push((partition_count, records_sec, mb_sec));
    }

    // Summary
    println!("╔════════════════════════════════════════════════════════════════╗");
    println!("║                    📊 SCALING RESULTS 📊                       ║");
    println!("╚════════════════════════════════════════════════════════════════╝");
    println!();
    println!("┌─────────────┬─────────────────┬─────────────────┐");
    println!("│ Partitions  │ Records/sec     │ MB/sec          │");
    println!("├─────────────┼─────────────────┼─────────────────┤");

    for (partitions, records_sec, mb_sec) in &results {
        println!(
            "│ {:>11} │ {:>15.0} │ {:>15.2} │",
            partitions, records_sec, mb_sec
        );
    }

    println!("└─────────────┴─────────────────┴─────────────────┘");
    println!();

    // Calculate scaling efficiency
    if results.len() >= 2 {
        let base_mb = results[0].2;
        let max_mb = results.last().unwrap().2;
        let max_partitions = results.last().unwrap().0 as f64;
        let linear_expected = base_mb * max_partitions;
        let efficiency = (max_mb / linear_expected) * 100.0;

        println!("  📈 Scaling efficiency: {:.1}%", efficiency);
        println!("  📈 Peak throughput: {:.2} MB/s", max_mb);

        if max_mb > 1000.0 {
            println!("  🎉 ACHIEVED 1+ GB/s!");
        }
    }
    println!();

    Ok(())
}
