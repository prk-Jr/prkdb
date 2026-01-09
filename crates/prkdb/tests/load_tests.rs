mod load_test_utils;

use load_test_utils::LoadTestHarness;
use prkdb_types::storage::StorageAdapter;
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_concurrent_writes_1000() {
    let harness = LoadTestHarness::new().await;

    println!("\n🔥 Running: Concurrent Writes (1000 ops, 50 concurrent)");
    let duration = harness.run_concurrent_writes(1000, 50).await;

    harness.metrics.print_summary(duration).await;

    // Assert reasonable performance
    assert!(
        duration < Duration::from_secs(10),
        "Write throughput too slow"
    );
    assert!(
        harness.metrics.get_failed_ops() == 0,
        "Should have no failures"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_concurrent_reads_5000() {
    let harness = LoadTestHarness::new().await;

    println!("\n🔥 Running: Concurrent Reads (5000 ops, 100 concurrent)");
    let duration = harness.run_concurrent_reads(5000, 100).await;

    harness.metrics.print_summary(duration).await;

    // Assert reasonable performance
    assert!(
        duration < Duration::from_secs(10),
        "Read throughput too slow"
    );
    assert!(
        harness.metrics.get_failed_ops() == 0,
        "Should have no failures"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_mixed_workload_70_30() {
    let harness = LoadTestHarness::new().await;

    println!("\n🔥 Running: Mixed Workload (70% reads, 30% writes, 2000 ops, 50 concurrent)");
    let duration = harness.run_mixed_workload(2000, 0.7, 50).await;

    harness.metrics.print_summary(duration).await;

    // Assert reasonable performance
    assert!(
        duration < Duration::from_secs(15),
        "Mixed workload too slow"
    );
    assert!(
        harness.metrics.get_failed_ops() == 0,
        "Should have no failures"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_bulk_operations() {
    let harness = LoadTestHarness::new().await;

    println!("\n🔥 Running: Bulk Operations (100 batches of 100 items)");
    let start = std::time::Instant::now();

    for _ in 0..100 {
        let mut items = vec![];
        for _ in 0..100 {
            let key = harness.workload_gen.next_key();
            let value = harness.workload_gen.next_value(100);
            items.push((key, value));
        }

        let op_start = std::time::Instant::now();
        match harness.adapter.put_many(items).await {
            Ok(_) => {
                let latency = op_start.elapsed();
                harness.metrics.record_success(latency);
                harness.metrics.latency_tracker.record(latency).await;
            }
            Err(_) => harness.metrics.record_failure(),
        }
    }

    let duration = start.elapsed();
    harness.metrics.print_summary(duration).await;

    assert!(
        duration < Duration::from_secs(10),
        "Bulk operations too slow"
    );
    assert!(
        harness.metrics.get_failed_ops() == 0,
        "Should have no failures"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore] // This test takes longer, run with --ignored flag
async fn test_sustained_throughput() {
    let harness = LoadTestHarness::new().await;

    println!("\n🔥 Running: Sustained Throughput (60 seconds continuous load)");
    let start = std::time::Instant::now();
    let duration_limit = Duration::from_secs(60);

    let mut op_count = 0;
    while start.elapsed() < duration_limit {
        let key = harness.workload_gen.next_key();
        let value = harness.workload_gen.next_value(100);

        let op_start = std::time::Instant::now();
        match harness.adapter.put(&key, &value).await {
            Ok(_) => {
                let latency = op_start.elapsed();
                harness.metrics.record_success(latency);
                if op_count % 100 == 0 {
                    // Sample latency every 100 ops to avoid overhead
                    harness.metrics.latency_tracker.record(latency).await;
                }
            }
            Err(_) => harness.metrics.record_failure(),
        }

        op_count += 1;
    }

    let duration = start.elapsed();
    harness.metrics.print_summary(duration).await;

    println!("Total operations in 60s: {}", op_count);
    assert!(
        harness.metrics.get_failed_ops() == 0,
        "Should have no failures"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore] // High memory test, run with --ignored flag
async fn test_large_dataset() {
    let harness = LoadTestHarness::new().await;

    println!("\n🔥 Running: Large Dataset (100k keys)");
    let start = std::time::Instant::now();

    for i in 0..100_000 {
        let key = format!("large_key_{}", i).into_bytes();
        let value = harness.workload_gen.next_value(200);

        if let Err(_) = harness.adapter.put(&key, &value).await {
            harness.metrics.record_failure();
        } else {
            harness.metrics.record_success(Duration::from_micros(0));
        }

        if i % 10000 == 0 && i > 0 {
            println!("Progress: {} keys written", i);
        }
    }

    let duration = start.elapsed();
    println!("\n✨ Wrote 100k keys in {:?}", duration);
    println!("Failed operations: {}", harness.metrics.get_failed_ops());

    // Verify some reads
    println!("\n📖 Verifying random reads...");
    for i in (0..100_000).step_by(5000) {
        let key = format!("large_key_{}", i).into_bytes();
        let result = harness.adapter.get(&key).await;
        assert!(result.is_ok(), "Failed to read key {}", i);
        assert!(result.unwrap().is_some(), "Key {} not found", i);
    }

    println!("✅ All verification reads successful!");
}
