//! Are batched writes atomic with respect to a concurrent reader?
//!
//! Written to diagnose S-03, where `test_bank_transfer_invariant` reported 17 units of
//! money missing from a total that every individual transaction preserves.
//!
//! `WalStorageAdapter::put_batch_impl` appended both records to the WAL as a unit, then
//! inserted them into the in-memory index **one key at a time**. Between the first and
//! second insert, a concurrent reader observed one half of a two-key write.
//!
//! # What was fixed, and what was not
//!
//! Publication is now atomic: the index and cache updates for a batch happen under
//! `publish_barrier`, and `snapshot_get_many` takes that barrier for the duration of a
//! multi-key read. A reader using it can no longer see half a commit.
//!
//! Two separate `get()` calls are still not a snapshot, and cannot be made into one —
//! there is no barrier spanning two independent calls. That is a property of the API the
//! caller chose, not a defect, and the second test below pins it down so nobody mistakes
//! it for one later.

use prkdb::storage::WalStorageAdapter;
use prkdb::transaction::{IsolationLevel, TransactionConfig, TransactionExt};
use prkdb_core::wal::WalConfig;
use prkdb_types::storage::StorageAdapter;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

fn storage() -> Arc<WalStorageAdapter> {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.keep();
    Arc::new(
        WalStorageAdapter::new(WalConfig {
            log_dir: path,
            ..WalConfig::test_config()
        })
        .expect("adapter"),
    )
}

fn balance(raw: Option<Vec<u8>>) -> i64 {
    raw.and_then(|b| b.try_into().ok())
        .map(i64::from_le_bytes)
        .unwrap_or(0)
}

/// Two keys whose sum is invariant, written together inside one Serializable
/// transaction, read repeatedly by a concurrent observer outside any transaction.
///
/// Pins down two separate facts, because conflating them is what made S-03 look like a
/// lost-write bug when it is not:
///
/// 1. **Committed state is always correct.** Nothing is ever lost.
/// 2. **A non-transactional reader can observe a commit in halves.** This is a real
///    limitation — PrkDB offers no snapshot read outside a transaction — but it is a read
///    property, not a durability one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_concurrent_reader_never_sees_half_a_batch() {
    let storage = storage();
    storage.put(b"a", &100i64.to_le_bytes()).await.unwrap();
    storage.put(b"b", &100i64.to_le_bytes()).await.unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    let torn = Arc::new(AtomicU64::new(0));
    let torn_snapshot = Arc::new(AtomicU64::new(0));
    let snapshot_reads = Arc::new(AtomicU64::new(0));

    // Two observers race the same writer: one reads with two independent `get`s, the
    // other with the snapshot primitive. Running them against one workload is what makes
    // the comparison meaningful — a difference cannot be blamed on differing timing.
    let observer = tokio::spawn({
        let storage = storage.clone();
        let stop = stop.clone();
        let torn = torn.clone();
        async move {
            while !stop.load(Ordering::Relaxed) {
                let a = balance(storage.get(b"a").await.unwrap());
                let b = balance(storage.get(b"b").await.unwrap());
                if a + b != 200 {
                    torn.fetch_add(1, Ordering::Relaxed);
                }
                tokio::task::yield_now().await;
            }
        }
    });

    let snapshot_observer = tokio::spawn({
        let storage = storage.clone();
        let stop = stop.clone();
        let torn_snapshot = torn_snapshot.clone();
        let snapshot_reads = snapshot_reads.clone();
        async move {
            while !stop.load(Ordering::Relaxed) {
                let values = storage
                    .snapshot_get_many(vec![b"a".to_vec(), b"b".to_vec()])
                    .await
                    .unwrap();
                let total = balance(values[0].clone()) + balance(values[1].clone());
                snapshot_reads.fetch_add(1, Ordering::Relaxed);
                if total != 200 {
                    torn_snapshot.fetch_add(1, Ordering::Relaxed);
                }
                tokio::task::yield_now().await;
            }
        }
    });

    // Move value back and forth; the sum never changes.
    for i in 0..400 {
        let (from, to) = if i % 2 == 0 {
            (&b"a"[..], &b"b"[..])
        } else {
            (&b"b"[..], &b"a"[..])
        };

        let config = TransactionConfig {
            isolation_level: IsolationLevel::Serializable,
            ..Default::default()
        };
        let mut tx = storage.begin_transaction_with_config(config);
        let from_balance = balance(tx.get(from).await.unwrap());
        let to_balance = balance(tx.get(to).await.unwrap());
        if from_balance < 10 {
            tx.rollback();
            continue;
        }
        tx.put(from.to_vec(), (from_balance - 10).to_le_bytes())
            .unwrap();
        tx.put(to.to_vec(), (to_balance + 10).to_le_bytes())
            .unwrap();
        let _ = tx.commit().await;
    }

    stop.store(true, Ordering::Relaxed);
    observer.await.unwrap();
    snapshot_observer.await.unwrap();

    let torn_reads = torn.load(Ordering::Relaxed);
    let final_sum =
        balance(storage.get(b"a").await.unwrap()) + balance(storage.get(b"b").await.unwrap());

    // The durable state must always be correct, whatever readers saw in flight.
    assert_eq!(
        final_sum, 200,
        "committed state must conserve the total; a mismatch here is a lost write, \
         not a torn read"
    );

    // The guarantee this test now exists to defend: a multi-key snapshot read never
    // observes a commit in halves.
    let reads = snapshot_reads.load(Ordering::Relaxed);
    assert!(
        reads > 0,
        "the snapshot observer never ran; the assertion below would be vacuous"
    );
    assert_eq!(
        torn_snapshot.load(Ordering::Relaxed),
        0,
        "snapshot_get_many observed a torn batch across {reads} reads; batch publication \
         is supposed to be atomic under publish_barrier"
    );

    // And the limitation that remains, asserted so it stays honest rather than drifting
    // into an unstated assumption: two independent `get` calls are not a snapshot. No
    // barrier spans them, so a batch committing between the two is visible to the second
    // and not the first. Callers who need atomicity must ask for it.
    assert!(
        torn_reads > 0,
        "two independent get() calls observed no torn state across this workload. That is \
         not a guarantee this design provides — if it has become one, something now spans \
         separate calls and this test should be rewritten to assert it deliberately."
    );

    // A Serializable transaction remains the stronger tool: it certifies that nothing it
    // read changed while it was reading, which a snapshot read does not attempt.
    let config = TransactionConfig {
        isolation_level: IsolationLevel::Serializable,
        ..Default::default()
    };
    let mut tx = storage.begin_transaction_with_config(config);
    let a = balance(tx.get(b"a").await.unwrap());
    let b = balance(tx.get(b"b").await.unwrap());
    tx.commit()
        .await
        .expect("a quiescent read-only transaction commits");
    assert_eq!(
        a + b,
        200,
        "a certified transactional snapshot must be consistent"
    );
}
