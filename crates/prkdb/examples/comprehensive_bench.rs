// Comprehensive Streaming Benchmark - Phase 24 Complete
//
// Tests:
// 1. Producer throughput (MB/s)
// 2. Consumer throughput (read from offset)
// 3. End-to-end latency (p50, p95, p99)
// 4. Multiple runs for statistical accuracy
// 5. Sustained load option (10x data)
//
// Run: cargo run --release --example comprehensive_bench -- [records]

use prkdb::storage::{StreamingConfig, StreamingRecord, StreamingStorageAdapter};
use std::env;
use std::time::{Duration, Instant};

const DEFAULT_RECORDS: usize = 1_000_000;
const RECORD_SIZE: usize = 100;
const BATCH_SIZE: usize = 10_000;
const NUM_RUNS: usize = 3;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Parse command line arguments for record count
    let args: Vec<String> = env::args().collect();
    let num_records = if args.len() > 1 {
        args[1].parse().unwrap_or(DEFAULT_RECORDS)
    } else {
        DEFAULT_RECORDS
    };

    println!();
    println!("╔════════════════════════════════════════════════════════════════╗");
    println!("║    🔬 COMPREHENSIVE STREAMING BENCHMARK 🔬                     ║");
    println!("╚════════════════════════════════════════════════════════════════╝");
    println!();
    println!("  Configuration:");
    println!("    Records: {:>12}", num_records);
    println!("    Record Size: {:>8} bytes", RECORD_SIZE);
    println!("    Batch Size: {:>9}", BATCH_SIZE);
    println!("    Runs: {:>14}", NUM_RUNS);
    println!(
        "    Total Data: {:>9} MB",
        num_records * RECORD_SIZE / 1024 / 1024
    );
    println!();

    let payload: Vec<u8> = (0..RECORD_SIZE).map(|i| (i % 256) as u8).collect();

    let mut producer_results: Vec<f64> = Vec::new();
    let mut consumer_results: Vec<f64> = Vec::new();
    let mut latencies: Vec<Duration> = Vec::new();

    for run in 1..=NUM_RUNS {
        println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        println!("  RUN {}/{}", run, NUM_RUNS);
        println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

        let dir = tempfile::tempdir()?;
        let config = StreamingConfig {
            log_dir: dir.path().to_path_buf(),
            segment_count: 4,
            sync_each_batch: false,
            ..Default::default()
        };

        let adapter = StreamingStorageAdapter::new(config).await?;

        // ═══════════════════════════════════════════════════════════════════════
        // PRODUCER BENCHMARK
        // ═══════════════════════════════════════════════════════════════════════
        println!("  📤 Producer benchmark...");

        let mut batch_latencies: Vec<Duration> = Vec::new();
        let producer_start = Instant::now();
        let mut total_records = 0usize;
        let mut batch_num = 0u64;

        while total_records < num_records {
            let batch_count = std::cmp::min(BATCH_SIZE, num_records - total_records);

            let records: Vec<StreamingRecord> = (0..batch_count)
                .map(|i| StreamingRecord {
                    key: format!("key_{}_{}", batch_num, i).into_bytes(),
                    value: payload.clone(),
                })
                .collect();

            let batch_start = Instant::now();
            adapter.append_batch(records).await?;
            batch_latencies.push(batch_start.elapsed());

            total_records += batch_count;
            batch_num += 1;
        }

        let producer_duration = producer_start.elapsed();
        let producer_mbps =
            (num_records * RECORD_SIZE) as f64 / producer_duration.as_secs_f64() / 1024.0 / 1024.0;
        producer_results.push(producer_mbps);

        // Calculate latency percentiles
        batch_latencies.sort();
        let p50 = batch_latencies[batch_latencies.len() * 50 / 100];
        let p95 = batch_latencies[batch_latencies.len() * 95 / 100];
        let p99 = batch_latencies[batch_latencies.len() * 99 / 100];
        latencies.extend(batch_latencies.clone());

        println!("     Throughput: {:.2} MB/s", producer_mbps);
        println!(
            "     Latency p50: {:?}, p95: {:?}, p99: {:?}",
            p50, p95, p99
        );

        // ═══════════════════════════════════════════════════════════════════════
        // CONSUMER BENCHMARK
        // ═══════════════════════════════════════════════════════════════════════
        println!("  📥 Consumer benchmark...");

        let consumer_start = Instant::now();
        let read_records = adapter.read_from(0, num_records).await?;
        let consumer_duration = consumer_start.elapsed();

        let records_read = read_records.len();
        let consumer_mbps =
            (records_read * RECORD_SIZE) as f64 / consumer_duration.as_secs_f64() / 1024.0 / 1024.0;
        consumer_results.push(consumer_mbps);

        println!("     Records read: {}", records_read);
        println!("     Throughput: {:.2} MB/s", consumer_mbps);
        println!();
    }

    // ═══════════════════════════════════════════════════════════════════════
    // SUMMARY
    // ═══════════════════════════════════════════════════════════════════════
    println!("╔════════════════════════════════════════════════════════════════╗");
    println!("║                    📊 FINAL RESULTS 📊                         ║");
    println!("╚════════════════════════════════════════════════════════════════╝");
    println!();

    // Calculate averages
    let avg_producer: f64 = producer_results.iter().sum::<f64>() / producer_results.len() as f64;
    let avg_consumer: f64 = consumer_results.iter().sum::<f64>() / consumer_results.len() as f64;
    let min_producer: f64 = producer_results
        .iter()
        .cloned()
        .fold(f64::INFINITY, f64::min);
    let max_producer: f64 = producer_results
        .iter()
        .cloned()
        .fold(f64::NEG_INFINITY, f64::max);

    // Latency percentiles across all runs
    latencies.sort();
    let overall_p50 = latencies[latencies.len() * 50 / 100];
    let overall_p95 = latencies[latencies.len() * 95 / 100];
    let overall_p99 = latencies[latencies.len() * 99 / 100];
    let avg_latency: Duration = latencies.iter().sum::<Duration>() / latencies.len() as u32;

    println!("┌─────────────────────────┬─────────────────┬─────────────────┐");
    println!("│ Metric                  │ Value           │ Notes           │");
    println!("├─────────────────────────┼─────────────────┼─────────────────┤");
    println!(
        "│ Producer Avg            │ {:>12.2} MB/s │ {} runs         │",
        avg_producer, NUM_RUNS
    );
    println!(
        "│ Producer Min            │ {:>12.2} MB/s │                 │",
        min_producer
    );
    println!(
        "│ Producer Max            │ {:>12.2} MB/s │                 │",
        max_producer
    );
    println!(
        "│ Consumer Avg            │ {:>12.2} MB/s │                 │",
        avg_consumer
    );
    println!("├─────────────────────────┼─────────────────┼─────────────────┤");
    println!(
        "│ Latency Avg             │ {:>12?} │ per batch       │",
        avg_latency
    );
    println!(
        "│ Latency p50             │ {:>12?} │                 │",
        overall_p50
    );
    println!(
        "│ Latency p95             │ {:>12?} │                 │",
        overall_p95
    );
    println!(
        "│ Latency p99             │ {:>12?} │                 │",
        overall_p99
    );
    println!("└─────────────────────────┴─────────────────┴─────────────────┘");
    println!();

    // Output in a format easy to parse
    println!("# BENCHMARK_RESULTS");
    println!("producer_avg_mbps={:.2}", avg_producer);
    println!("producer_min_mbps={:.2}", min_producer);
    println!("producer_max_mbps={:.2}", max_producer);
    println!("consumer_avg_mbps={:.2}", avg_consumer);
    println!("latency_avg_us={}", avg_latency.as_micros());
    println!("latency_p50_us={}", overall_p50.as_micros());
    println!("latency_p95_us={}", overall_p95.as_micros());
    println!("latency_p99_us={}", overall_p99.as_micros());

    Ok(())
}
