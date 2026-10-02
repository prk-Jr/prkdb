//! Tripwires assert that a known bug STILL EXISTS. They pass today and fail the
//! moment the bug is fixed, forcing the fixer to invert them into regression tests
//! and update docs/remediation/ledger.toml. See spec §4.1.
//! Inverted tripwires stay here under their regression names, so the history of each
//! finding is in one file.
//!
//! Tests that need a fresh process re-run this test binary as a child with
//! PRKDB_TRIPWIRE_CHILD set; the child prints `CHILD_RESULT=<value>`.

use prkdb::indexed_storage::IndexedStorage;
use prkdb::storage::WalStorageAdapter;
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

/// STO-01 regression (was the tripwire): keys written before a checkpoint survive reopen.
#[tokio::test(flavor = "multi_thread")]
async fn sto01_checkpoint_keeps_pre_checkpoint_keys() {
    let dir = tempfile::tempdir().unwrap();
    {
        let a = WalStorageAdapter::new(wal_config(dir.path())).unwrap();
        for i in 0..5u8 {
            a.put(&[b'k', i], b"v").await.unwrap();
        }
        a.flush().await.unwrap();
        a.save_checkpoint_async().await.unwrap();
        a.put(b"after", b"checkpoint").await.unwrap();
    }
    let b = WalStorageAdapter::open_async(wal_config(dir.path()))
        .await
        .unwrap();
    for i in 0..5u8 {
        assert_eq!(
            b.get(&[b'k', i]).await.unwrap().as_deref(),
            Some(&b"v"[..]),
            "k{i}"
        );
    }
    assert_eq!(
        b.get(b"after").await.unwrap().as_deref(),
        Some(&b"checkpoint"[..])
    );
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

/// KEY-01 regression (was the tripwire): same id in two collections, through get,
/// query, delete and a restart.
#[tokio::test(flavor = "multi_thread")]
async fn key01_collections_with_same_id_are_independent() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = IndexedStorage::new(Arc::new(
            WalStorageAdapter::new(wal_config(dir.path())).unwrap(),
        ));
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
        assert_eq!(db.get::<TwUser>(&1).await.unwrap().unwrap().name, "Alice");
        assert_eq!(
            db.get::<TwProject>(&1).await.unwrap().unwrap().name,
            "Project"
        );
        assert_eq!(
            db.query_by::<TwUser>("name", &"Alice").await.unwrap().len(),
            1
        );
        assert!(db
            .query_by::<TwUser>("name", &"Project")
            .await
            .unwrap()
            .is_empty());
        db.delete(&TwProject {
            id: 1,
            name: "Project".into(),
        })
        .await
        .unwrap();
        assert!(db.get::<TwProject>(&1).await.unwrap().is_none());
        assert_eq!(db.get::<TwUser>(&1).await.unwrap().unwrap().name, "Alice");
        db.inner().flush().await.unwrap();
    }
    let db = IndexedStorage::new(Arc::new(
        WalStorageAdapter::open_async(wal_config(dir.path()))
            .await
            .unwrap(),
    ));
    assert_eq!(db.get::<TwUser>(&1).await.unwrap().unwrap().name, "Alice");
    assert!(db.get::<TwProject>(&1).await.unwrap().is_none());
}

/// KEY-03 regression (was the tripwire): a key's partition is the same in every process.
#[test]
fn key03_partition_is_stable_across_processes() {
    use prkdb::partitioning::{DefaultPartitioner, Partitioner};
    if is_child("key03_partition_is_stable_across_processes") {
        let p = DefaultPartitioner::<String>::new().partition(&"user-42".to_string(), 1_000_000);
        println!("CHILD_RESULT={p}");
        return;
    }
    let results: Vec<String> = (0..3)
        .map(|_| child_result("key03_partition_is_stable_across_processes"))
        .collect();
    assert!(
        results.windows(2).all(|w| w[0] == w[1]),
        "partition changed across processes: {results:?}"
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

/// Appends three frames to a fresh log in `dir`, closes it, then writes one hand-encoded
/// frame with kind 99 and a valid CRC after them, at the end of the only segment. Returns
/// the segment's path and its length with the extra frame.
fn log_ending_in_a_valid_frame_of_unknown_kind(dir: &std::path::Path) -> (std::path::PathBuf, u64) {
    use prkdb_core::vfs::StdVfs;
    use prkdb_core::wal::{segment::segment_file_name, Wal, WalOptions};
    let opts = WalOptions::from_config(&wal_config(dir));
    let (wal, _) = Wal::open(Arc::new(StdVfs), dir, opts, 1, &mut |_, _, _| Ok(())).unwrap();
    for i in 0..3u8 {
        wal.append_blocking(vec![i; 16], None).unwrap();
    }
    wal.close().unwrap();

    let (lsn, kind, payload) = (4u64, 99u8, b"a later build's frame");
    let mut crc_input = lsn.to_le_bytes().to_vec();
    crc_input.push(kind);
    crc_input.extend_from_slice(payload);
    let mut frame = (payload.len() as u32).to_le_bytes().to_vec();
    frame.extend_from_slice(&crc32fast::hash(&crc_input).to_le_bytes());
    frame.extend_from_slice(&lsn.to_le_bytes());
    frame.push(kind);
    frame.extend_from_slice(payload);

    let path = dir.join(segment_file_name(1));
    let mut bytes = std::fs::read(&path).unwrap();
    bytes.extend_from_slice(&frame);
    std::fs::write(&path, &bytes).unwrap();
    (path, bytes.len() as u64)
}

/// STO-11: `decode_frame` checks the kind before the CRC, so a valid frame of a kind this
/// build does not know is a torn tail in the last segment, and `Wal::open` truncates it.
#[test]
fn sto11_valid_frame_of_unknown_kind_is_truncated_as_a_torn_tail_tripwire() {
    use prkdb_core::vfs::StdVfs;
    use prkdb_core::wal::{Wal, WalOptions};
    let dir = tempfile::tempdir().unwrap();
    let (path, len) = log_ending_in_a_valid_frame_of_unknown_kind(dir.path());
    let opts = WalOptions::from_config(&wal_config(dir.path()));
    let opened = Wal::open(Arc::new(StdVfs), dir.path(), opts, 1, &mut |_, _, _| Ok(()));
    assert!(
        opened.is_ok() && std::fs::metadata(&path).unwrap().len() < len,
        "STO-11 appears fixed: invert this tripwire"
    );
}
