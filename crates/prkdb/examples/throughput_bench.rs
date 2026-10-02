// High-Throughput Benchmark - Matches Kafka's producer-perf-test parameters
//
// Run: cargo run --release --example throughput_bench -- [records] [--producer-only]
//
// This benchmark uses the same parameters as Kafka's kafka-producer-perf-test:
// - 1 million records by default (first argument overrides)
// - 100 byte record size
// - 10,000 batch size
//
// `--producer-only` skips the read and multi-task phases (the CI sustained-load step
// uses it: 10M records is a write test, and reading them back would double its time).
//
// The last lines are `key=value` pairs that the CI `benchmark` job parses; keep their
// names stable.

use prkdb::storage::WalStorageAdapter;
use prkdb_core::wal::WalConfig;
use prkdb_types::storage::StorageAdapter;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const DEFAULT_RECORDS: usize = 1_000_000;
const RECORD_SIZE: usize = 100; // bytes
const BATCH_SIZE: usize = 10_000; // Match Kafka's batch.size
const PRODUCER_TASKS: usize = 4;

fn mb_per_sec(records: usize, elapsed: Duration) -> f64 {
    (records * RECORD_SIZE) as f64 / elapsed.as_secs_f64() / 1024.0 / 1024.0
}

/// Key of the `i`-th record the single producer writes.
fn producer_key(i: usize) -> Vec<u8> {
    format!("key_{}_{}", i / BATCH_SIZE, i % BATCH_SIZE).into_bytes()
}

fn percentile(sorted: &[Duration], pct: usize) -> Duration {
    sorted[(sorted.len() * pct / 100).min(sorted.len() - 1)]
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut num_records = DEFAULT_RECORDS;
    let mut producer_only = false;
    for arg in std::env::args().skip(1) {
        if arg == "--producer-only" {
            producer_only = true;
        } else {
            num_records = arg
                .parse()
                .map_err(|e| anyhow::anyhow!("record count {arg:?}: {e}"))?;
        }
    }
    anyhow::ensure!(num_records > 0, "record count must be positive");

    println!();
    println!("╔════════════════════════════════════════════════════════════════╗");
    println!("║    🚀 HIGH-THROUGHPUT BENCHMARK (Kafka Parameters) 🚀          ║");
    println!("╚════════════════════════════════════════════════════════════════╝");
    println!();
    println!("  Configuration (matches kafka-producer-perf-test):");
    println!("    Records: {:>12}", num_records);
    println!("    Record Size: {:>8} bytes", RECORD_SIZE);
    println!("    Batch Size: {:>9}", BATCH_SIZE);
    println!(
        "    Total Data: {:>9} MB",
        num_records * RECORD_SIZE / 1024 / 1024
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
    let mut batch_latencies: Vec<Duration> = Vec::new();

    while total_records < num_records {
        let batch_count = std::cmp::min(BATCH_SIZE, num_records - total_records);
        let entries: Vec<(Vec<u8>, Vec<u8>)> = (total_records..total_records + batch_count)
            .map(|i| (producer_key(i), payload.clone()))
            .collect();

        let batch_start = Instant::now();
        storage.put_batch(entries).await?;
        batch_latencies.push(batch_start.elapsed());
        total_records += batch_count;

        // Progress indicator
        if batch_latencies.len().is_multiple_of(10) {
            let pct = (total_records as f64 / num_records as f64) * 100.0;
            print!(
                "\r  Progress: {:.1}% ({}/{})",
                pct, total_records, num_records
            );
            use std::io::Write;
            std::io::stdout().flush()?;
        }
    }

    let producer_duration = start.elapsed();
    let producer_records_sec = num_records as f64 / producer_duration.as_secs_f64();
    let producer_mb_sec = mb_per_sec(num_records, producer_duration);

    batch_latencies.sort();
    let latency_avg = batch_latencies.iter().sum::<Duration>() / batch_latencies.len() as u32;
    let latency_p50 = percentile(&batch_latencies, 50);
    let latency_p95 = percentile(&batch_latencies, 95);
    let latency_p99 = percentile(&batch_latencies, 99);

    println!("\r  ✅ Producer Complete!                              ");
    println!();
    println!("     Records:    {:>12}", num_records);
    println!(
        "     Duration:   {:>12.2}s",
        producer_duration.as_secs_f64()
    );
    println!(
        "     Throughput: {:>12.0} records/sec",
        producer_records_sec
    );
    println!("     Throughput: {:>12.2} MB/sec", producer_mb_sec);
    println!(
        "     Batch latency avg/p50/p95/p99: {:?} / {:?} / {:?} / {:?}",
        latency_avg, latency_p50, latency_p95, latency_p99
    );
    println!();

    let mut consumer = None;
    let mut multi_task = None;
    if !producer_only {
        consumer = Some(run_consumer(&storage, num_records).await?);
        multi_task = Some(run_multi_task_producer(&storage, &payload, num_records).await?);
    }

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
    if let Some((records_sec, mb_sec)) = consumer {
        println!(
            "│ Consumer (random)       │ {:>15.0} │ {:>15.2} │",
            records_sec, mb_sec
        );
    }
    if let Some((records_sec, mb_sec)) = multi_task {
        println!(
            "│ Producer ({} tasks)      │ {:>15.0} │ {:>15.2} │",
            PRODUCER_TASKS, records_sec, mb_sec
        );
    }
    println!("└─────────────────────────┴─────────────────┴─────────────────┘");
    println!();

    // Machine-readable results (parsed by .github/workflows/ci.yml `benchmark`).
    println!("producer_avg_mbps={:.2}", producer_mb_sec);
    println!("latency_avg_us={}", latency_avg.as_micros());
    println!("latency_p50_us={}", latency_p50.as_micros());
    println!("latency_p95_us={}", latency_p95.as_micros());
    println!("latency_p99_us={}", latency_p99.as_micros());
    if let Some((_, mb_sec)) = consumer {
        println!("consumer_avg_mbps={:.2}", mb_sec);
    }
    if let Some((_, mb_sec)) = multi_task {
        println!("producer_multi_task_mbps={:.2}", mb_sec);
    }

    Ok(())
}

/// Random point reads over the records the single producer wrote. Every read must
/// hit: a benchmark that times misses measures nothing.
async fn run_consumer(
    storage: &Arc<WalStorageAdapter>,
    num_records: usize,
) -> anyhow::Result<(f64, f64)> {
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  TEST 2: Consumer (Random Reads)");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    let start = Instant::now();
    let mut rng = 42u64;
    let mut misses = 0usize;

    for i in 0..num_records {
        rng = rng.wrapping_mul(1103515245).wrapping_add(12345);
        let key = producer_key((rng % num_records as u64) as usize);
        if storage.get(&key).await?.is_none() {
            misses += 1;
        }

        if i % 100_000 == 0 && i > 0 {
            let pct = (i as f64 / num_records as f64) * 100.0;
            print!("\r  Progress: {:.1}% ({}/{})", pct, i, num_records);
            use std::io::Write;
            std::io::stdout().flush()?;
        }
    }
    anyhow::ensure!(
        misses == 0,
        "{misses} of {num_records} reads missed records the producer wrote"
    );

    let consumer_duration = start.elapsed();
    let records_sec = num_records as f64 / consumer_duration.as_secs_f64();
    let mb_sec = mb_per_sec(num_records, consumer_duration);

    println!("\r  ✅ Consumer Complete!                              ");
    println!();
    println!("     Records:    {:>12}", num_records);
    println!(
        "     Duration:   {:>12.2}s",
        consumer_duration.as_secs_f64()
    );
    println!("     Throughput: {:>12.0} records/sec", records_sec);
    println!("     Throughput: {:>12.2} MB/sec", mb_sec);
    println!();
    Ok((records_sec, mb_sec))
}

/// Concurrent batch writers sharing one adapter.
async fn run_multi_task_producer(
    storage: &Arc<WalStorageAdapter>,
    payload: &[u8],
    num_records: usize,
) -> anyhow::Result<(f64, f64)> {
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  TEST 3: Multi-Task Producer ({} tasks)", PRODUCER_TASKS);
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    let total_mt = Arc::new(AtomicU64::new(0));
    let records_per_task = num_records.div_ceil(PRODUCER_TASKS);

    let start = Instant::now();

    let handles: Vec<_> = (0..PRODUCER_TASKS)
        .map(|tid| {
            let storage = storage.clone();
            let payload = payload.to_vec();
            let total = total_mt.clone();

            tokio::spawn(async move {
                let mut written = 0usize;
                let mut batch_num = 0u64;

                while written < records_per_task {
                    let batch_count = std::cmp::min(BATCH_SIZE, records_per_task - written);
                    let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..batch_count)
                        .map(|i| {
                            (
                                format!("mt_{}_{}_{}", tid, batch_num, i).into_bytes(),
                                payload.clone(),
                            )
                        })
                        .collect();

                    storage.put_batch(entries).await?;
                    written += batch_count;
                    batch_num += 1;
                    total.fetch_add(batch_count as u64, Ordering::Relaxed);
                }
                Ok::<_, prkdb_types::error::StorageError>(())
            })
        })
        .collect();

    for h in handles {
        h.await??;
    }

    let mt_duration = start.elapsed();
    let mt_records = total_mt.load(Ordering::Relaxed) as usize;
    let records_sec = mt_records as f64 / mt_duration.as_secs_f64();
    let mb_sec = mb_per_sec(mt_records, mt_duration);

    println!("  ✅ Multi-Task Complete!");
    println!();
    println!("     Records:    {:>12}", mt_records);
    println!("     Duration:   {:>12.2}s", mt_duration.as_secs_f64());
    println!("     Throughput: {:>12.0} records/sec", records_sec);
    println!("     Throughput: {:>12.2} MB/sec", mb_sec);
    println!();
    Ok((records_sec, mb_sec))
}
