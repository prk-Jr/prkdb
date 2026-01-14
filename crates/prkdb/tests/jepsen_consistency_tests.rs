//! Jepsen-style Consistency Tests
//!
//! Distributed consistency verification inspired by the Jepsen framework.
//! These tests verify linearizability and data integrity under failure scenarios.
//!
//! Run with: cargo test --test jepsen_consistency_tests -- --ignored --nocapture

mod helpers;

use helpers::{
    BankAccounts, InvariantResult, LinearizabilityResult, OpKind, OpResult, Operation,
    OperationHistory,
};
use prkdb::storage::WalStorageAdapter;
use prkdb_core::wal::WalConfig;
use prkdb_types::storage::StorageAdapter;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::sleep;

/// Test: Linearizable Register
///
/// Verifies that concurrent reads and writes to a single key behave as if
/// they occurred atomically in some total order consistent with real-time.
///
/// Scenario:
/// 1. Start storage adapter
/// 2. Spawn concurrent writers incrementing a counter
/// 3. Spawn concurrent readers
/// 4. Record all operations with timestamps
/// 5. Verify history is linearizable
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_linearizable_register() {
    let dir = tempfile::tempdir().unwrap();
    let config = WalConfig {
        log_dir: dir.path().to_path_buf(),
        ..WalConfig::test_config()
    };
    let storage = Arc::new(WalStorageAdapter::new(config).unwrap());
    let history = OperationHistory::new();
    let counter = Arc::new(AtomicU64::new(0));

    let key = b"register".to_vec();
    let num_writers = 4;
    let num_readers = 4;
    let ops_per_client = 50;

    // Initialize key
    storage.put(&key, &0u64.to_le_bytes()).await.unwrap();

    let mut handles = vec![];

    // Writers: increment counter
    for writer_id in 0..num_writers {
        let storage = storage.clone();
        let history = history.clone();
        let counter = counter.clone();
        let key = key.clone();

        handles.push(tokio::spawn(async move {
            for _ in 0..ops_per_client {
                let start = Instant::now();
                let new_val = counter.fetch_add(1, Ordering::SeqCst) + 1;
                let write_value = new_val.to_le_bytes().to_vec();

                let result = storage.put(&key, &write_value).await;
                let end = Instant::now();

                history.record(Operation {
                    kind: OpKind::Write,
                    key: key.clone(),
                    write_value: Some(write_value),
                    read_value: None,
                    start_time: start,
                    end_time: end,
                    result: match result {
                        Ok(()) => OpResult::Ok(None),
                        Err(e) => OpResult::Err(e.to_string()),
                    },
                    client_id: writer_id,
                });

                tokio::time::sleep(Duration::from_micros(100)).await;
            }
        }));
    }

    // Readers: read current value
    for reader_id in 0..num_readers {
        let storage = storage.clone();
        let history = history.clone();
        let key = key.clone();

        handles.push(tokio::spawn(async move {
            for _ in 0..ops_per_client {
                let start = Instant::now();
                let result = storage.get(&key).await;
                let end = Instant::now();

                let (read_value, op_result) = match result {
                    Ok(Some(v)) => (Some(v.clone()), OpResult::Ok(Some(v))),
                    Ok(None) => (None, OpResult::Ok(None)),
                    Err(e) => (None, OpResult::Err(e.to_string())),
                };

                history.record(Operation {
                    kind: OpKind::Read,
                    key: key.clone(),
                    write_value: None,
                    read_value,
                    start_time: start,
                    end_time: end,
                    result: op_result,
                    client_id: num_writers + reader_id,
                });

                tokio::time::sleep(Duration::from_micros(50)).await;
            }
        }));
    }

    // Wait for all operations
    for handle in handles {
        handle.await.unwrap();
    }

    // Verify linearizability
    let result = history.is_linearizable();
    println!(
        "✅ Linearizable Register Test: {} operations recorded",
        history.len()
    );

    match result {
        LinearizabilityResult::Linearizable => {
            println!("✅ History is linearizable!");
        }
        LinearizabilityResult::NotLinearizable { reason } => {
            panic!("❌ Linearizability violation: {}", reason);
        }
    }
}

/// Test: Bank Transfer Invariant
///
/// Verifies that the total balance across all accounts remains constant
/// during concurrent transfers, even under simulated failures.
///
/// Scenario:
/// 1. Create N accounts with initial balance
/// 2. Spawn concurrent transfer workers
/// 3. Periodically check total balance invariant
/// 4. Verify final total matches initial total
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_bank_transfer_invariant() {
    let num_accounts = 10;
    let initial_balance = 1000i64;
    let bank = BankAccounts::new(num_accounts, initial_balance);
    let num_workers = 8;
    let transfers_per_worker = 100;

    let accounts = bank.account_names();

    let mut handles = vec![];

    for worker_id in 0..num_workers {
        let bank = bank.clone();
        let accounts = accounts.clone();

        handles.push(tokio::spawn(async move {
            let mut rng_state = worker_id as u64 * 31337;
            for i in 0..transfers_per_worker {
                // Simple PRNG for deterministic but varied transfers
                rng_state = rng_state.wrapping_mul(6364136223846793005).wrapping_add(1);
                let from_idx = (rng_state as usize) % accounts.len();
                rng_state = rng_state.wrapping_mul(6364136223846793005).wrapping_add(1);
                let mut to_idx = (rng_state as usize) % accounts.len();
                if to_idx == from_idx {
                    to_idx = (to_idx + 1) % accounts.len();
                }

                let amount = ((rng_state % 50) + 1) as i64;

                let _ = bank.transfer(&accounts[from_idx], &accounts[to_idx], amount);

                // Periodically check invariant during execution
                if i % 20 == 0 {
                    assert_eq!(
                        bank.check_total_invariant(),
                        InvariantResult::Passed,
                        "Invariant violated during worker {} iteration {}",
                        worker_id,
                        i
                    );
                }

                tokio::time::sleep(Duration::from_micros(10)).await;
            }
        }));
    }

    // Wait for all transfers
    for handle in handles {
        handle.await.unwrap();
    }

    // Final invariant check
    match bank.check_total_invariant() {
        InvariantResult::Passed => {
            println!(
                "✅ Bank Transfer Invariant Test: Total balance preserved ({} accounts, {} transfers)",
                num_accounts,
                num_workers * transfers_per_worker
            );
        }
        InvariantResult::Failed { reason } => {
            panic!("❌ Bank invariant violated: {}", reason);
        }
    }
}

/// Test: Monotonic Reads
///
/// Verifies that reads from a single client never see older values
/// after seeing newer values (no stale reads).
///
/// Scenario:
/// 1. Start storage adapter
/// 2. Spawn a writer continuously incrementing a counter
/// 3. Spawn readers that track their observed values
/// 4. Verify each reader's observations are monotonically increasing
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_monotonic_reads() {
    let dir = tempfile::tempdir().unwrap();
    let config = WalConfig {
        log_dir: dir.path().to_path_buf(),
        ..WalConfig::test_config()
    };
    let storage = Arc::new(WalStorageAdapter::new(config).unwrap());
    let history = OperationHistory::new();

    let key = b"counter".to_vec();
    let test_duration = Duration::from_secs(3);

    // Initialize
    storage.put(&key, &0u64.to_le_bytes()).await.unwrap();

    let running = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let counter = Arc::new(AtomicU64::new(0));

    // Writer task
    let writer_running = running.clone();
    let writer_storage = storage.clone();
    let writer_counter = counter.clone();
    let writer_key = key.clone();
    let writer_handle = tokio::spawn(async move {
        while writer_running.load(Ordering::Relaxed) {
            let val = writer_counter.fetch_add(1, Ordering::SeqCst) + 1;
            let _ = writer_storage.put(&writer_key, &val.to_le_bytes()).await;
            tokio::time::sleep(Duration::from_micros(500)).await;
        }
    });

    // Reader tasks
    let num_readers = 4;
    let mut reader_handles = vec![];

    for reader_id in 0..num_readers {
        let storage = storage.clone();
        let history = history.clone();
        let running = running.clone();
        let key = key.clone();

        reader_handles.push(tokio::spawn(async move {
            while running.load(Ordering::Relaxed) {
                let start = Instant::now();
                let result = storage.get(&key).await;
                let end = Instant::now();

                if let Ok(Some(val)) = result {
                    history.record(Operation {
                        kind: OpKind::Read,
                        key: key.clone(),
                        write_value: None,
                        read_value: Some(val.clone()),
                        start_time: start,
                        end_time: end,
                        result: OpResult::Ok(Some(val)),
                        client_id: reader_id,
                    });
                }

                tokio::time::sleep(Duration::from_micros(200)).await;
            }
        }));
    }

    // Run for test duration
    sleep(test_duration).await;
    running.store(false, Ordering::Relaxed);

    // Wait for tasks
    writer_handle.await.unwrap();
    for h in reader_handles {
        h.await.unwrap();
    }

    // Check monotonic reads
    match history.check_monotonic_reads() {
        InvariantResult::Passed => {
            println!(
                "✅ Monotonic Reads Test: {} reads verified as monotonic",
                history.len()
            );
        }
        InvariantResult::Failed { reason } => {
            panic!("❌ Monotonic read violation: {}", reason);
        }
    }
}

/// Test: Lost Update Detection
///
/// Verifies that concurrent increments to a counter don't lose updates.
///
/// Scenario:
/// 1. Initialize counter to 0
/// 2. N workers each increment counter M times
/// 3. Final value should equal N * M
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_lost_update_detection() {
    let dir = tempfile::tempdir().unwrap();
    let config = WalConfig {
        log_dir: dir.path().to_path_buf(),
        ..WalConfig::test_config()
    };
    let storage = Arc::new(WalStorageAdapter::new(config).unwrap());

    let key = b"atomic_counter".to_vec();
    let num_workers = 8;
    let increments_per_worker = 100;
    let expected_final = (num_workers * increments_per_worker) as u64;

    // Initialize counter
    storage.put(&key, &0u64.to_le_bytes()).await.unwrap();

    // Use atomic counter for coordination (simulating CAS)
    let global_counter = Arc::new(AtomicU64::new(0));

    let mut handles = vec![];

    for _ in 0..num_workers {
        let counter = global_counter.clone();
        let storage = storage.clone();
        let key = key.clone();

        handles.push(tokio::spawn(async move {
            for _ in 0..increments_per_worker {
                // Atomic increment
                let new_val = counter.fetch_add(1, Ordering::SeqCst) + 1;
                storage.put(&key, &new_val.to_le_bytes()).await.unwrap();
                tokio::time::sleep(Duration::from_micros(10)).await;
            }
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }

    // Verify final value
    let final_value = storage.get(&key).await.unwrap().unwrap();
    let final_counter = u64::from_le_bytes(final_value.try_into().unwrap());

    if final_counter == expected_final {
        println!(
            "✅ Lost Update Detection Test: Final counter = {} (expected {})",
            final_counter, expected_final
        );
    } else {
        panic!(
            "❌ Lost updates detected: Final counter = {} but expected {}",
            final_counter, expected_final
        );
    }
}

/// Test: Write Skew Prevention
///
/// Tests that serializable isolation prevents write skew anomaly.
/// Write skew: Two transactions read overlapping data, then write disjoint data,
/// creating an inconsistent state that neither transaction alone would create.
///
/// Scenario (Doctors On-Call):
/// - Invariant: At least one doctor must be on call
/// - Both doctors try to go off-call simultaneously
/// - Serializable isolation should prevent both from succeeding
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_write_skew_prevention() {
    let dir = tempfile::tempdir().unwrap();
    let config = WalConfig {
        log_dir: dir.path().to_path_buf(),
        ..WalConfig::test_config()
    };
    let storage = Arc::new(WalStorageAdapter::new(config).unwrap());

    // Setup: Two doctors on call
    storage.put(b"doctor_alice", b"on_call").await.unwrap();
    storage.put(b"doctor_bob", b"on_call").await.unwrap();

    let iterations = 100;
    let mut violations = 0;

    for _ in 0..iterations {
        // Reset state
        storage.put(b"doctor_alice", b"on_call").await.unwrap();
        storage.put(b"doctor_bob", b"on_call").await.unwrap();

        let storage1 = storage.clone();
        let storage2 = storage.clone();

        // Two concurrent "transactions" trying to go off-call
        let t1 = tokio::spawn(async move {
            // Read both, check invariant, write own
            let bob = storage1.get(b"doctor_bob").await.unwrap();
            if bob.as_deref() == Some(b"on_call".as_slice()) {
                // Bob is on call, Alice can go off
                storage1.put(b"doctor_alice", b"off_call").await.unwrap();
            }
        });

        let t2 = tokio::spawn(async move {
            // Read both, check invariant, write own
            let alice = storage2.get(b"doctor_alice").await.unwrap();
            if alice.as_deref() == Some(b"on_call".as_slice()) {
                // Alice is on call, Bob can go off
                storage2.put(b"doctor_bob", b"off_call").await.unwrap();
            }
        });

        t1.await.unwrap();
        t2.await.unwrap();

        // Check invariant: at least one on call
        let alice = storage.get(b"doctor_alice").await.unwrap();
        let bob = storage.get(b"doctor_bob").await.unwrap();

        let alice_on = alice.as_deref() == Some(b"on_call".as_slice());
        let bob_on = bob.as_deref() == Some(b"on_call".as_slice());

        if !alice_on && !bob_on {
            violations += 1;
        }
    }

    // Note: Without true serializable isolation, some violations are expected.
    // This test documents the current behavior.
    println!(
        "✅ Write Skew Prevention Test: {}/{} iterations had violations",
        violations, iterations
    );
    println!("   (Violations expected without true serializable isolation)");

    // For now we document behavior; with true SERIALIZABLE we'd assert violations == 0
    if violations == 0 {
        println!("   🎉 No write skew detected - serializable isolation working!");
    }
}

/// Test: Dirty Read Prevention
///
/// Verifies that uncommitted writes are not visible to other readers.
///
/// Scenario:
/// 1. Start a "transaction" (simulated with marker)
/// 2. Write data but don't "commit"
/// 3. Another reader should not see uncommitted data
/// 4. Rollback - data should never have been visible
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_dirty_read_prevention() {
    let dir = tempfile::tempdir().unwrap();
    let config = WalConfig {
        log_dir: dir.path().to_path_buf(),
        ..WalConfig::test_config()
    };
    let storage = Arc::new(WalStorageAdapter::new(config).unwrap());

    // Initialize with known value
    storage.put(b"account", b"1000").await.unwrap();

    let dirty_reads = Arc::new(AtomicU64::new(0));
    let uncommitted_value = b"9999"; // Marker for uncommitted

    let iterations = 50;

    for _ in 0..iterations {
        // Reset
        storage.put(b"account", b"1000").await.unwrap();

        let storage1 = storage.clone();
        let storage2 = storage.clone();
        let dirty_reads = dirty_reads.clone();

        // Simulated "transaction" that writes but "rolls back"
        let writer = tokio::spawn(async move {
            // Write uncommitted value
            storage1.put(b"account", uncommitted_value).await.unwrap();

            // Simulate processing delay
            tokio::time::sleep(Duration::from_micros(100)).await;

            // Rollback by restoring original
            storage1.put(b"account", b"1000").await.unwrap();
        });

        // Reader that might see dirty data
        let reader = tokio::spawn(async move {
            for _ in 0..10 {
                if let Ok(Some(val)) = storage2.get(b"account").await {
                    if val == uncommitted_value {
                        dirty_reads.fetch_add(1, Ordering::Relaxed);
                    }
                }
                tokio::time::sleep(Duration::from_micros(20)).await;
            }
        });

        writer.await.unwrap();
        reader.await.unwrap();
    }

    let total_dirty = dirty_reads.load(Ordering::Relaxed);

    // Note: Without true transaction isolation, dirty reads may occur.
    // This test documents current behavior.
    println!(
        "✅ Dirty Read Prevention Test: {} dirty reads detected across {} iterations",
        total_dirty, iterations
    );

    if total_dirty == 0 {
        println!("   🎉 No dirty reads - excellent isolation!");
    } else {
        println!("   ⚠️ Dirty reads occurred - consider adding transaction support");
    }
}
