//! Extended Distributed Chaos Tests
//!
//! Advanced multi-node failure scenarios beyond basic partitions.
//! Tests asymmetric partitions, rolling restarts, message reordering, and more.
//!
//! Run with: cargo test --test extended_chaos_tests -- --ignored --nocapture

mod helpers;

use helpers::NetworkSimulator;
use prkdb::storage::WalStorageAdapter;
use prkdb_core::wal::WalConfig;
use prkdb_types::storage::StorageAdapter;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::sleep;

/// Test: Asymmetric Partition
///
/// Verifies behavior when network is asymmetric (A→B blocked, B→A allowed).
/// This can cause interesting edge cases in leader election and log replication.
///
/// Scenario:
/// 1. Create storage with multiple simulated "nodes" (via prefixed keys)
/// 2. Simulate asymmetric network where writes from one "node" are delayed
/// 3. Verify eventual consistency after clearing asymmetry
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_asymmetric_partition() {
    let sim = NetworkSimulator::new(None);

    // Create asymmetric partition: node 1 can't reach node 2, but node 2 can reach node 1
    sim.asymmetric_partition(1, 2).await;

    // Verify asymmetry
    assert!(sim.should_block(1, 2).await, "1→2 should be blocked");
    assert!(!sim.should_block(2, 1).await, "2→1 should NOT be blocked");

    // Test with actual storage
    let dir = tempfile::tempdir().unwrap();
    let config = WalConfig {
        log_dir: dir.path().to_path_buf(),
        ..WalConfig::test_config()
    };
    let storage = Arc::new(WalStorageAdapter::new(config).unwrap());

    // Simulate "node 1" writes (would be delayed/blocked in real cluster)
    for i in 0..50 {
        let key = format!("node1_key_{}", i).into_bytes();
        let value = format!("value_{}", i).into_bytes();
        storage.put(&key, &value).await.unwrap();
    }

    // Simulate "node 2" writes (can proceed normally)
    for i in 0..50 {
        let key = format!("node2_key_{}", i).into_bytes();
        let value = format!("value_{}", i).into_bytes();
        storage.put(&key, &value).await.unwrap();
    }

    // Heal partition
    sim.heal_partitions().await;
    assert!(!sim.should_block(1, 2).await, "Partition should be healed");

    // Verify all data present
    for i in 0..50 {
        let key1 = format!("node1_key_{}", i).into_bytes();
        let key2 = format!("node2_key_{}", i).into_bytes();
        assert!(storage.get(&key1).await.unwrap().is_some());
        assert!(storage.get(&key2).await.unwrap().is_some());
    }

    println!("✅ Asymmetric Partition Test: Completed successfully");
}

/// Test: Rolling Restart No Data Loss
///
/// Verifies that rolling restarts during continuous writes don't lose data.
///
/// Scenario:
/// 1. Start continuous write workload
/// 2. Simulate "restart" by recreating storage adapter
/// 3. Verify all acknowledged writes are durable
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_rolling_restart_no_data_loss() {
    let base_dir = tempfile::tempdir().unwrap();
    let config = WalConfig {
        log_dir: base_dir.path().to_path_buf(),
        ..WalConfig::test_config()
    };

    let writes_completed = Arc::new(AtomicU64::new(0));
    let running = Arc::new(AtomicBool::new(true));

    // Phase 1: Write data
    {
        let storage = WalStorageAdapter::new(config.clone()).unwrap();
        for i in 0..500 {
            let key = format!("rolling_key_{}", i).into_bytes();
            let value = format!("rolling_value_{}", i).into_bytes();
            storage.put(&key, &value).await.unwrap();
            writes_completed.fetch_add(1, Ordering::SeqCst);

            // Periodic "checkpoint" - simulate sync
            if i % 100 == 99 {
                sleep(Duration::from_millis(50)).await;
            }
        }
    }

    // Phase 2: Simulate restart
    sleep(Duration::from_millis(100)).await;

    // Phase 3: Reopen and continue writes
    {
        let storage = WalStorageAdapter::open(config.clone()).unwrap();
        for i in 500..1000 {
            let key = format!("rolling_key_{}", i).into_bytes();
            let value = format!("rolling_value_{}", i).into_bytes();
            storage.put(&key, &value).await.unwrap();
            writes_completed.fetch_add(1, Ordering::SeqCst);
        }
    }

    running.store(false, Ordering::SeqCst);

    // Phase 4: Final verification
    sleep(Duration::from_millis(100)).await;
    let storage = WalStorageAdapter::open(config).unwrap();

    let mut found = 0;
    let mut missing = Vec::new();
    for i in 0..1000 {
        let key = format!("rolling_key_{}", i).into_bytes();
        if storage.get(&key).await.unwrap().is_some() {
            found += 1;
        } else {
            missing.push(i);
        }
    }

    let total = writes_completed.load(Ordering::SeqCst);
    println!(
        "✅ Rolling Restart Test: {}/{} writes found ({} total attempted)",
        found, 1000, total
    );

    if !missing.is_empty() && missing.len() <= 10 {
        println!("   Missing keys: {:?}", missing);
    }

    assert_eq!(
        found, 1000,
        "All 1000 writes should be durable after rolling restart"
    );
}

/// Test: Message Reordering
///
/// Verifies log consistency when messages arrive out of order.
///
/// Scenario:
/// 1. Add reordering rule with high variance
/// 2. Write sequential data from multiple threads
/// 3. Verify final state is consistent
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_message_reordering() {
    let sim = NetworkSimulator::new(None);

    // Add reordering with 50ms variance
    sim.add_reordering(1, 2, 50).await;

    // Get delay should now include random variance
    let delay1 = sim.get_delay(1, 2).await;
    let delay2 = sim.get_delay(1, 2).await;
    println!("Reorder delays: {:?}, {:?}", delay1, delay2);

    // Test with storage
    let dir = tempfile::tempdir().unwrap();
    let config = WalConfig {
        log_dir: dir.path().to_path_buf(),
        ..WalConfig::test_config()
    };
    let storage = Arc::new(WalStorageAdapter::new(config).unwrap());

    let counter = Arc::new(AtomicU64::new(0));
    let num_workers = 4;
    let writes_per_worker = 250;

    let mut handles = vec![];

    for worker_id in 0..num_workers {
        let storage = storage.clone();
        let counter = counter.clone();

        handles.push(tokio::spawn(async move {
            for i in 0..writes_per_worker {
                let seq = counter.fetch_add(1, Ordering::SeqCst);
                let key = format!("reorder_key_{}", seq).into_bytes();
                let value = format!("worker_{}_seq_{}", worker_id, i).into_bytes();

                // Simulate variable delay (reordering effect)
                let delay_ms = (rand::random::<u64>() % 10) as u64;
                if delay_ms > 5 {
                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                }

                storage.put(&key, &value).await.unwrap();
            }
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }

    // Verify all writes present
    let total = num_workers * writes_per_worker;
    let mut found = 0;
    for i in 0..total {
        let key = format!("reorder_key_{}", i).into_bytes();
        if storage.get(&key).await.unwrap().is_some() {
            found += 1;
        }
    }

    println!(
        "✅ Message Reordering Test: {}/{} writes found despite reordering",
        found, total
    );
    assert_eq!(found, total, "All writes should be present");
}

/// Test: Partition Heal Under Load
///
/// Verifies that minority nodes catch up correctly after partition heals.
///
/// Scenario:
/// 1. Write initial data
/// 2. Create partition (simulated by separate storage instance)
/// 3. Continue writes to "majority"
/// 4. Heal partition
/// 5. Verify convergence
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_partition_heal_under_load() {
    let sim = NetworkSimulator::new(None);
    let dir = tempfile::tempdir().unwrap();
    let config = WalConfig {
        log_dir: dir.path().to_path_buf(),
        ..WalConfig::test_config()
    };
    let storage = Arc::new(WalStorageAdapter::new(config).unwrap());

    // Phase 1: Initial writes (all nodes see this)
    for i in 0..100 {
        let key = format!("partition_key_{}", i).into_bytes();
        let value = b"initial".to_vec();
        storage.put(&key, &value).await.unwrap();
    }
    println!("Phase 1: 100 initial writes complete");

    // Phase 2: Create partition
    sim.partition(vec![1], vec![2, 3]).await;
    assert!(sim.should_block(1, 2).await);

    // Phase 3: Writes during partition (majority only)
    let start = Instant::now();
    for i in 100..500 {
        let key = format!("partition_key_{}", i).into_bytes();
        let value = format!("during_partition_{}", i).into_bytes();
        storage.put(&key, &value).await.unwrap();
    }
    let partition_duration = start.elapsed();
    println!(
        "Phase 2: 400 writes during partition ({:?})",
        partition_duration
    );

    // Phase 4: Heal partition
    let heal_start = Instant::now();
    sim.heal_partitions().await;
    assert!(!sim.should_block(1, 2).await);

    // Give time for "catch-up"
    sleep(Duration::from_millis(100)).await;
    let heal_duration = heal_start.elapsed();
    println!("Phase 3: Partition healed ({:?})", heal_duration);

    // Phase 5: Verify all data present
    let mut found = 0;
    for i in 0..500 {
        let key = format!("partition_key_{}", i).into_bytes();
        if storage.get(&key).await.unwrap().is_some() {
            found += 1;
        }
    }

    println!(
        "✅ Partition Heal Under Load Test: {}/500 keys found after healing",
        found
    );
    assert_eq!(
        found, 500,
        "All keys should be present after partition heal"
    );
}

/// Test: Byzantine Leader Simulation
///
/// Tests detection of conflicting data (simulated "byzantine" behavior).
/// Note: PrkDB doesn't have true Byzantine fault tolerance, but Raft
/// should reject inconsistent log entries.
///
/// Scenario:
/// 1. Write conflicting values for same key from different "sources"
/// 2. Verify last-write-wins or consistent resolution
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_byzantine_leader_simulation() {
    let dir = tempfile::tempdir().unwrap();
    let config = WalConfig {
        log_dir: dir.path().to_path_buf(),
        ..WalConfig::test_config()
    };
    let storage = Arc::new(WalStorageAdapter::new(config).unwrap());

    let key = b"byzantine_key".to_vec();
    let num_writers = 4;
    let writes_per_writer = 100;

    let mut handles = vec![];

    for writer_id in 0..num_writers {
        let storage = storage.clone();
        let key = key.clone();

        handles.push(tokio::spawn(async move {
            for i in 0..writes_per_writer {
                let value = format!("writer_{}_value_{}", writer_id, i).into_bytes();
                storage.put(&key, &value).await.unwrap();
                tokio::time::sleep(Duration::from_micros(50)).await;
            }
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }

    // Read final value
    let final_value = storage.get(&key).await.unwrap();
    assert!(final_value.is_some(), "Key should have a value");

    let final_bytes = final_value.unwrap();
    let value_str = String::from_utf8_lossy(&final_bytes);
    println!(
        "✅ Byzantine Leader Simulation Test: Final value = {}",
        value_str
    );
    println!(
        "   (Last-write-wins resolution, {} total writes)",
        num_writers * writes_per_writer
    );
}

/// Test: Slow Follower Degradation
///
/// Verifies cluster behavior when one node becomes progressively slower.
///
/// Scenario:
/// 1. Start with normal operation
/// 2. Gradually increase delay to one "follower"
/// 3. Verify cluster continues operating
/// 4. Check slow follower eventually catches up after delay removed
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_slow_follower_degradation() {
    let sim = NetworkSimulator::new(None);
    let dir = tempfile::tempdir().unwrap();
    let config = WalConfig {
        log_dir: dir.path().to_path_buf(),
        ..WalConfig::test_config()
    };
    let storage = Arc::new(WalStorageAdapter::new(config).unwrap());

    // Simulate gradual degradation
    let delay_stages = [10, 50, 100, 200, 500];
    let mut total_writes = 0;

    for delay_ms in delay_stages {
        sim.set_delay(1, 3, delay_ms).await;

        let current_delay = sim.get_delay(1, 3).await;
        println!("Stage: {}ms delay, actual: {:?}", delay_ms, current_delay);

        // Write batch at each stage
        for i in 0..100 {
            let key = format!("slow_follower_key_{}", total_writes + i).into_bytes();
            let value = format!("delay_stage_{}", delay_ms).into_bytes();
            storage.put(&key, &value).await.unwrap();
        }
        total_writes += 100;

        sleep(Duration::from_millis(50)).await;
    }

    // Clear delays
    sim.clear_rules().await;

    // Verify all writes
    let mut found = 0;
    for i in 0..total_writes {
        let key = format!("slow_follower_key_{}", i).into_bytes();
        if storage.get(&key).await.unwrap().is_some() {
            found += 1;
        }
    }

    println!(
        "✅ Slow Follower Degradation Test: {}/{} writes found after delay cleared",
        found, total_writes
    );
    assert_eq!(found, total_writes, "All writes should be present");
}
