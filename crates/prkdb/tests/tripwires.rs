//! Tripwires assert that a known bug STILL EXISTS. They pass today and fail the
//! moment the bug is fixed, forcing the fixer to invert them into regression tests
//! and update docs/remediation/ledger.toml. See spec §4.1.
//!
//! Tests that need a fresh process re-run this test binary as a child with
//! PRKDB_TRIPWIRE_CHILD set; the child prints `CHILD_RESULT=<value>`.

use prkdb::indexed_storage::IndexedStorage;
use prkdb::storage::{InMemoryAdapter, WalStorageAdapter};
use prkdb_core::wal::WalConfig;
use prkdb_macros::Collection;
use prkdb_types::storage::StorageAdapter;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

const CHILD_ENV: &str = "PRKDB_TRIPWIRE_CHILD";

fn child_result(test_name: &str) -> String {
    // The env var carries the exact test name so the child can confirm it's the
    // process this call spawned; an incidentally-exported PRKDB_TRIPWIRE_CHILD in
    // the parent's own environment then can't make the parent branch vacuously
    // pass as if it were the child (see `is_child`).
    let out = std::process::Command::new(std::env::current_exe().unwrap())
        .args([test_name, "--exact", "--nocapture", "--test-threads=1"])
        .env(CHILD_ENV, test_name)
        .output()
        .expect("spawn child");
    let stdout = String::from_utf8_lossy(&out.stdout);
    // libtest's `--nocapture` writes `test <name> ... ` without a trailing
    // newline before the test body's own output, so the marker can appear
    // mid-line rather than at line start; search for the marker anywhere.
    stdout
        .lines()
        .find_map(|l| {
            l.find("CHILD_RESULT=")
                .map(|i| l[i + "CHILD_RESULT=".len()..].split_whitespace().next())
        })
        .flatten()
        .map(str::to_owned)
        .unwrap_or_else(|| {
            panic!(
                "child printed no result; status: {:?}; stdout:\n{stdout}\nstderr:\n{}",
                out.status,
                String::from_utf8_lossy(&out.stderr)
            )
        })
}

/// True only when this process was spawned by `child_result` for `test_name`
/// specifically. Checking the value (not just presence) of `CHILD_ENV` means an
/// unrelated `PRKDB_TRIPWIRE_CHILD` exported in the parent's own environment
/// can't make the parent take the child branch and vacuously pass.
fn is_child(test_name: &str) -> bool {
    std::env::var(CHILD_ENV).as_deref() == Ok(test_name)
}

fn wal_config(dir: &std::path::Path) -> WalConfig {
    WalConfig {
        log_dir: dir.to_path_buf(),
        ..WalConfig::test_config()
    }
}

/// STO-01: keys written before a checkpoint vanish after reopen.
#[tokio::test(flavor = "multi_thread")]
async fn sto01_checkpoint_drops_pre_checkpoint_keys_tripwire() {
    let dir = tempfile::tempdir().unwrap();
    {
        let a = WalStorageAdapter::new(wal_config(dir.path())).unwrap();
        for i in 0..5u8 {
            a.put(&[b'k', i], b"v").await.unwrap();
        }
        a.flush().await.unwrap();
        a.save_checkpoint().unwrap();
    }
    let b = WalStorageAdapter::open_async(wal_config(dir.path()))
        .await
        .unwrap();
    // Checkpoint recovery replays only from `max_offset` onward, so every key
    // written before the checkpoint is dropped on reopen. Count how many of the
    // 5 keys survive rather than pinning exact indices: because all 5 keys route
    // to the same segment (collection is "") and the recovery scan's bound is
    // inclusive, asserting on a specific index (e.g. k0 vs k4) would fail for
    // reasons unrelated to STO-01 itself (segment/routing changes, off-by-one
    // fixes elsewhere) rather than tracking the bug this tripwire is for.
    let mut recovered = 0;
    for i in 0..5u8 {
        if b.get(&[b'k', i]).await.unwrap().is_some() {
            recovered += 1;
        }
    }
    assert!(recovered < 5, "STO-01 appears fixed: invert this tripwire");
}

#[derive(Collection, Serialize, Deserialize, Clone, Debug)]
struct TwUser {
    #[id]
    id: u64,
    #[index]
    name: String,
}

#[derive(Collection, Serialize, Deserialize, Clone, Debug)]
struct TwProject {
    #[id]
    id: u64,
    #[index]
    name: String,
}

/// KEY-01: two collections with the same id overwrite each other.
#[tokio::test]
async fn key01_collections_share_primary_keys_tripwire() {
    let db = IndexedStorage::new(Arc::new(InMemoryAdapter::new()));
    db.insert(&TwUser {
        id: 1,
        name: "Alice".into(),
    })
    .await
    .unwrap();
    db.insert(&TwProject {
        id: 1,
        name: "Project".into(),
    })
    .await
    .unwrap();
    // Match rather than unwrap the result chain: a fix that makes `get` return an
    // error or `None` for the colliding id (instead of quietly returning the
    // wrong record) should also read as "fixed" here, not panic with an
    // unrelated unwrap failure.
    match db.get::<TwUser>(&1).await {
        Ok(Some(user)) if user.name == "Project" => {}
        other => panic!("KEY-01 appears fixed: invert this tripwire (got {other:?})"),
    }
}

/// KEY-03: the default partitioner is seeded per process.
///
/// Blind spot: this also flips (falsely reading as "fixed") if ahash's
/// runtime random seeding is disabled (e.g. building without the default
/// `runtime-rng` feature, or pinning a fixed seed), since the partitioner would
/// then hash deterministically across processes for reasons unrelated to
/// KEY-03 being fixed in `DefaultPartitioner` itself.
#[test]
fn key03_partition_differs_across_processes_tripwire() {
    use prkdb::partitioning::{DefaultPartitioner, Partitioner};
    if is_child("key03_partition_differs_across_processes_tripwire") {
        let p = DefaultPartitioner::<String>::new().partition(&"user-42".to_string(), 1_000_000);
        println!("CHILD_RESULT={p}");
        return;
    }
    let results: Vec<String> = (0..3)
        .map(|_| child_result("key03_partition_differs_across_processes_tripwire"))
        .collect();
    assert!(
        !(results[0] == results[1] && results[1] == results[2]),
        "KEY-03 appears fixed (stable across processes: {results:?}): invert this tripwire"
    );
}

#[derive(Collection, Serialize, Deserialize, Clone, Debug)]
struct TwEvent {
    #[id]
    id: u64,
}

/// EVT-01: the outbox sequence restarts at 1 in every process.
///
/// Blind spot: a fix that seeds `OUTBOX_SEQ` from persisted storage on startup
/// would still restart at 1 here, because this child process never opens any
/// storage — there is nothing to seed from. Such a fix would make this
/// tripwire keep passing even though EVT-01 is fixed for real deployments, so
/// the fixer must invert it by hand rather than rely on it turning red.
#[test]
fn evt01_outbox_sequence_restarts_per_process_tripwire() {
    if is_child("evt01_outbox_sequence_restarts_per_process_tripwire") {
        println!(
            "CHILD_RESULT={}",
            prkdb::outbox::make_outbox_id_for_type::<TwEvent>(None)
        );
        return;
    }
    let first = child_result("evt01_outbox_sequence_restarts_per_process_tripwire");
    let second = child_result("evt01_outbox_sequence_restarts_per_process_tripwire");
    assert_eq!(first, second, "EVT-01 appears fixed: invert this tripwire");
}

/// TXN-04: default isolation is ReadCommitted (D5 makes it Serializable).
#[test]
fn txn04_default_isolation_is_read_committed_tripwire() {
    use prkdb::transaction::{IsolationLevel, TransactionConfig};
    assert_eq!(
        TransactionConfig::default().isolation_level,
        IsolationLevel::ReadCommitted,
        "TXN-04 appears fixed: invert this tripwire"
    );
}
