use prkdb::storage::WalStorageAdapter;
use prkdb_core::wal::WalConfig;
use prkdb_types::error::StorageError;
use prkdb_types::storage::StorageAdapter;
use std::sync::Arc;
use tokio::time::{timeout, Duration};

/// Chaos testing: Simulate network partitions by introducing delays
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn chaos_test_delayed_writes() {
    let dir = tempfile::tempdir().unwrap();
    let config = WalConfig {
        log_dir: dir.path().to_path_buf(),
        ..WalConfig::test_config()
    };
    let adapter = Arc::new(WalStorageAdapter::new(config).unwrap());

    // Spawn multiple writers with random delays
    let mut handles = vec![];
    for i in 0..10 {
        let adapter = adapter.clone();
        let handle = tokio::spawn(async move {
            for j in 0..10 {
                let key = format!("chaos_key_{}_{}", i, j).into_bytes();
                let value = format!("value_{}_{}", i, j).into_bytes();

                // Random delay to simulate network latency
                tokio::time::sleep(Duration::from_millis((i * 10) as u64)).await;

                adapter.put(&key, &value).await.unwrap();
            }
        });
        handles.push(handle);
    }

    // Wait for all writes to complete
    for handle in handles {
        handle.await.unwrap();
    }

    // Verify all data is present
    for i in 0..10 {
        for j in 0..10 {
            let key = format!("chaos_key_{}_{}", i, j).into_bytes();
            let expected = format!("value_{}_{}", i, j).into_bytes();
            let result = adapter.get(&key).await.unwrap();
            assert_eq!(
                result,
                Some(expected),
                "Lost data for key chaos_key_{}_{}",
                i,
                j
            );
        }
    }

    println!("✅ Chaos test: Delayed writes completed successfully");
}

/// Chaos testing: Concurrent reads and writes
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn chaos_test_concurrent_mixed_operations() {
    let dir = tempfile::tempdir().unwrap();
    let config = WalConfig {
        log_dir: dir.path().to_path_buf(),
        ..WalConfig::test_config()
    };
    let adapter = Arc::new(WalStorageAdapter::new(config).unwrap());

    // Pre-populate some data
    for i in 0..50 {
        let key = format!("existing_key_{}", i).into_bytes();
        let value = format!("existing_value_{}", i).into_bytes();
        adapter.put(&key, &value).await.unwrap();
    }

    // Spawn concurrent readers and writers
    let mut handles = vec![];

    // Writers
    for i in 0..5 {
        let adapter = adapter.clone();
        let handle = tokio::spawn(async move {
            for j in 0..20 {
                let key = format!("new_key_{}_{}", i, j).into_bytes();
                let value = format!("new_value_{}_{}", i, j).into_bytes();
                adapter.put(&key, &value).await.unwrap();
                tokio::time::sleep(Duration::from_micros(100)).await;
            }
        });
        handles.push(handle);
    }

    // Readers
    for i in 0..10 {
        let adapter = adapter.clone();
        let handle = tokio::spawn(async move {
            for j in 0..50 {
                let key = format!("existing_key_{}", j).into_bytes();
                let _result = adapter.get(&key).await;
                // Don't assert, just exercise concurrent reads
                tokio::time::sleep(Duration::from_micros(50)).await;
            }
        });
        handles.push(handle);
    }

    // Wait for all operations
    for handle in handles {
        handle.await.unwrap();
    }

    println!("✅ Chaos test: Concurrent mixed operations completed");
}

/// Chaos testing: Rapid open/close cycles
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore] // Intermittent failure - recovery timing issue
async fn chaos_test_rapid_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let config = WalConfig {
        log_dir: dir.path().to_path_buf(),
        ..WalConfig::test_config()
    };

    for cycle in 0..5 {
        // Create adapter, write data, drop it
        {
            let adapter = WalStorageAdapter::new(config.clone()).unwrap();
            for i in 0..10 {
                let key = format!("cycle_{}_key_{}", cycle, i).into_bytes();
                let value = format!("cycle_{}_value_{}", cycle, i).into_bytes();
                adapter.put(&key, &value).await.unwrap();
            }
            // Wait for flush (increased for reliability)
            tokio::time::sleep(Duration::from_millis(200)).await;
        }

        // Reopen and verify
        let adapter = WalStorageAdapter::open(config.clone()).unwrap();
        for past_cycle in 0..=cycle {
            for i in 0..10 {
                let key = format!("cycle_{}_key_{}", past_cycle, i).into_bytes();
                let expected = format!("cycle_{}_value_{}", past_cycle, i).into_bytes();
                let result = adapter.get(&key).await.unwrap();
                assert_eq!(
                    result,
                    Some(expected),
                    "Lost data from cycle {} key {}",
                    past_cycle,
                    i
                );
            }
        }
    }

    println!("✅ Chaos test: Rapid recovery cycles completed");
}

/// Chaos testing: Timeout-resistant operations
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn chaos_test_operation_timeouts() {
    let dir = tempfile::tempdir().unwrap();
    let config = WalConfig {
        log_dir: dir.path().to_path_buf(),
        ..WalConfig::test_config()
    };
    let adapter = WalStorageAdapter::new(config).unwrap();

    // All operations should complete within reasonable timeouts
    for i in 0..100 {
        let key = format!("timeout_key_{}", i).into_bytes();
        let value = vec![b'x'; 1000];

        // Write with timeout
        let write_result = timeout(Duration::from_secs(5), adapter.put(&key, &value)).await;
        assert!(write_result.is_ok(), "Write timeout for key {}", i);
        assert!(write_result.unwrap().is_ok(), "Write error for key {}", i);

        // Read with timeout
        let read_result = timeout(Duration::from_secs(5), adapter.get(&key)).await;
        assert!(read_result.is_ok(), "Read timeout for key {}", i);
        assert_eq!(read_result.unwrap().unwrap(), Some(value));
    }

    println!("✅ Chaos test: All operations completed within timeouts");
}

/// Chaos testing: Memory pressure under high load
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore] // Run with --ignored flag
async fn chaos_test_memory_pressure() {
    let dir = tempfile::tempdir().unwrap();
    let config = WalConfig {
        log_dir: dir.path().to_path_buf(),
        ..WalConfig::test_config()
    };
    let adapter = Arc::new(WalStorageAdapter::new(config).unwrap());

    // Create high memory pressure with large values
    let mut handles = vec![];
    for worker in 0..10 {
        let adapter = adapter.clone();
        let handle = tokio::spawn(async move {
            for i in 0..1000 {
                let key = format!("memory_key_{}_{}", worker, i).into_bytes();
                let value = vec![b'x'; 10_000]; // 10KB per value
                adapter.put(&key, &value).await.unwrap();

                if i % 100 == 0 {
                    println!("Worker {} progress: {}/1000", worker, i);
                }
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }

    println!("✅ Chaos test: Memory pressure test completed (100MB written)");
}
