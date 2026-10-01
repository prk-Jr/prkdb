//! Does data survive closing and reopening the database?
//!
//! # Why this file exists
//!
//! Nothing in the suite had ever reopened a data directory and read back from it. Every
//! test built a database in a fresh `tempdir`, exercised it through one handle, and
//! dropped it. That leaves the single most basic property of a database — that it still
//! holds your data tomorrow — completely unverified, and it was in fact broken by three
//! independent defects at once:
//!
//! 1. **`MmapLogSegment::create` truncates.** Opening a data directory called `create`,
//!    which passes `truncate(true)`, so the log was zeroed on open. `open` existed and was
//!    correct; the path a user reaches never called it. Fixed by `open_or_create`.
//! 2. **`WalStorageAdapter::new_with_config` never rebuilt the index.** `open` and
//!    `open_async` both did, but the constructor `PrkDb::builder().with_data_dir()` reaches
//!    did not — so even a recovered log stayed invisible.
//! 3. **`CollectionPartitionedAdapter` never discovered collections on disk.** Collections
//!    open lazily, so a freshly opened database reported an empty collection set. This is
//!    what made `prkdb backup` produce a valid archive containing zero entries.
//!
//! Each defect alone loses the database. They masked each other: fixing the truncation
//! changed nothing observable until the index rebuild landed too.
//!
//! These tests deliberately use the public `PrkDb` API through a real data directory,
//! because that is the combination that was broken while every layer looked fine alone.

use prkdb::storage::WalStorageAdapter;
use prkdb::PrkDb;
use prkdb_core::wal::{SyncMode, WalConfig};
use prkdb_types::storage::StorageAdapter;
use std::path::Path;
use std::sync::Arc;

fn open(dir: &Path) -> PrkDb {
    PrkDb::builder()
        .with_data_dir(dir)
        .build()
        .expect("open the database")
}

/// The property the whole file exists for.
#[tokio::test(flavor = "multi_thread")]
async fn values_survive_closing_and_reopening() {
    let dir = tempfile::tempdir().unwrap();

    // Several collections, because data is partitioned per collection and a bug that
    // loses one may keep another.
    let pairs = [
        ("users:alpha", "one"),
        ("users:beta", "two"),
        ("orders:gamma", "three"),
        ("events:delta", "four"),
    ];

    {
        let db = open(dir.path());
        for (k, v) in &pairs {
            db.put(k.as_bytes(), v.as_bytes()).await.unwrap();
        }
    }

    let db = open(dir.path());
    for (k, v) in &pairs {
        assert_eq!(
            db.get(k.as_bytes()).await.unwrap().as_deref(),
            Some(v.as_bytes()),
            "{k} did not survive a reopen"
        );
    }
}

/// Reopening must not destroy the log. This is the direct regression test for the
/// `create`-instead-of-`open` truncation: it opens the database repeatedly without writing
/// anything, which under the old code wiped it on the second open.
#[tokio::test(flavor = "multi_thread")]
async fn reopening_repeatedly_does_not_discard_data() {
    let dir = tempfile::tempdir().unwrap();

    {
        let db = open(dir.path());
        db.put(b"users:persist", b"value").await.unwrap();
    }

    for round in 1..=3 {
        let db = open(dir.path());
        assert_eq!(
            db.get(b"users:persist").await.unwrap().as_deref(),
            Some(b"value".as_slice()),
            "data was lost by reopen number {round}"
        );
    }
}

/// A delete must also be durable. Recovering the log and replaying only the puts would
/// resurrect deleted keys, which is worse than losing them.
#[tokio::test(flavor = "multi_thread")]
async fn deletes_survive_reopening() {
    let dir = tempfile::tempdir().unwrap();

    {
        let db = open(dir.path());
        db.put(b"users:kept", b"yes").await.unwrap();
        db.put(b"users:removed", b"no").await.unwrap();
        db.delete(b"users:removed").await.unwrap();
    }

    let db = open(dir.path());
    assert_eq!(
        db.get(b"users:kept").await.unwrap().as_deref(),
        Some(b"yes".as_slice()),
        "the surviving key was lost"
    );
    assert_eq!(
        db.get(b"users:removed").await.unwrap(),
        None,
        "a deleted key came back after a reopen"
    );
}

/// Writes made after a reopen must not overwrite recovered ones.
///
/// The write position is restored by scanning the segment; if that scan reported zero the
/// next append would start at byte 0 and silently overwrite the recovered records.
#[tokio::test(flavor = "multi_thread")]
async fn writes_after_a_reopen_append_rather_than_overwrite() {
    let dir = tempfile::tempdir().unwrap();

    {
        let db = open(dir.path());
        db.put(b"users:first", b"1").await.unwrap();
    }
    {
        let db = open(dir.path());
        db.put(b"users:second", b"2").await.unwrap();
    }

    let db = open(dir.path());
    assert_eq!(
        db.get(b"users:first").await.unwrap().as_deref(),
        Some(b"1".as_slice()),
        "the pre-reopen write was overwritten by the post-reopen one"
    );
    assert_eq!(
        db.get(b"users:second").await.unwrap().as_deref(),
        Some(b"2".as_slice()),
        "the post-reopen write was lost"
    );
}

/// A reopened database must know its collections without being asked for a key in them
/// first. Backup depends on this: it enumerates collections and would otherwise archive an
/// empty database while reporting success.
#[tokio::test(flavor = "multi_thread")]
async fn a_reopened_database_can_be_snapshotted_without_touching_a_key_first() {
    use prkdb_types::snapshot::CompressionType;

    let dir = tempfile::tempdir().unwrap();
    let archive = dir.path().join("snap.bin");

    {
        let db = open(dir.path().join("data").as_path());
        db.put(b"users:alpha", b"one").await.unwrap();
        db.put(b"orders:beta", b"two").await.unwrap();
    }

    // Snapshot immediately after opening, with no prior read, so the collection set can
    // only come from disk.
    let db = open(dir.path().join("data").as_path());
    db.take_snapshot(&archive, CompressionType::None)
        .await
        .expect("snapshot a reopened database");

    let reader = prkdb::storage::snapshot::SnapshotReader::open(&archive).expect("read archive");
    assert_eq!(
        reader.header.index_entries, 2,
        "the archive claims {} entries; a snapshot of a reopened database must contain \
         the data that is on disk, not the empty in-memory collection map",
        reader.header.index_entries
    );
}

// ── Whole-database operations on the default adapter ─────────────────────────
//
// `CollectionPartitionedAdapter` is what `with_data_dir` builds, and it has now been
// missing three `StorageAdapter` methods that its inner adapters implement: `take_snapshot`
// (S-04), collection discovery (S-05), and `scan_prefix` (S-07). Each compiled cleanly and
// failed at runtime in whichever feature happened to call it.
//
// These tests exercise the wrapper directly, because a test that reaches for
// `WalStorageAdapter` or `SledAdapter` passes whether or not the wrapper forwards anything
// — which is exactly why S-07 shipped with no regression coverage until this was written.

/// `scan_prefix` works on the adapter the default builder produces (S-07).
#[tokio::test(flavor = "multi_thread")]
async fn scan_prefix_works_on_a_data_dir_database() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path());

    db.storage().put(b"users:alice", b"1").await.unwrap();
    db.storage().put(b"users:bob", b"2").await.unwrap();
    db.storage().put(b"orders:x", b"3").await.unwrap();

    let users = db
        .storage()
        .scan_prefix(b"users:")
        .await
        .expect("scan_prefix must be supported on the default adapter");
    assert_eq!(
        users.len(),
        2,
        "expected both users keys, got {:?}",
        users
            .iter()
            .map(|(k, _)| String::from_utf8_lossy(k))
            .collect::<Vec<_>>()
    );
    assert!(
        users.iter().all(|(k, _)| k.starts_with(b"users:")),
        "scan_prefix returned keys outside the prefix"
    );

    // Keys come back in their full `collection:id` form, which is what get/put accept.
    let (key, _) = &users[0];
    assert!(
        db.storage().get(key).await.unwrap().is_some(),
        "a key returned by scan_prefix must be readable with get"
    );

    // A prefix naming no collection must not leak other collections' keys.
    let orders = db.storage().scan_prefix(b"orders:").await.unwrap();
    assert_eq!(orders.len(), 1);
}

/// The prefix scan `list_collections` depends on works on a `--database` database.
///
/// This is the user-visible symptom of S-07: the call returned an error rather than a
/// list, on the adapter every `--database` deployment uses.
#[tokio::test(flavor = "multi_thread")]
async fn list_collections_works_on_a_data_dir_database() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path());

    db.put(b"users:alice", b"1").await.unwrap();

    db.list_collections()
        .await
        .expect("list_collections scans a prefix and must not fail on the default adapter");
}

/// `scan_range` works on the adapter the default builder produces (S-08).
///
/// `CollectionHandle::scan_range_by_id_bytes` is public API and calls
/// `storage.scan_range`, so on a `--database` database this was the fourth method missing
/// from the same wrapper.
#[tokio::test(flavor = "multi_thread")]
async fn scan_range_works_on_a_data_dir_database() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path());

    for id in ["a", "b", "c", "d"] {
        db.storage()
            .put(format!("users:{id}").as_bytes(), id.as_bytes())
            .await
            .unwrap();
    }

    let rows = db
        .storage()
        .scan_range(b"users:b", b"users:d")
        .await
        .expect("scan_range must be supported on the default adapter");

    let keys: Vec<String> = rows
        .iter()
        .map(|(k, _)| String::from_utf8_lossy(k).into_owned())
        .collect();
    assert_eq!(
        keys,
        vec!["users:b".to_string(), "users:c".to_string()],
        "scan_range is half-open [start, end): b and c, not d"
    );
}

/// A single-collection database can be replicated: the cursor is unambiguous (S-09).
#[tokio::test(flavor = "multi_thread")]
async fn get_changes_since_works_for_a_single_collection() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path());
    db.storage().put(b"users:a", b"1").await.unwrap();
    db.storage().put(b"users:b", b"2").await.unwrap();

    let changes = db
        .storage()
        .get_changes_since(0)
        .await
        .expect("one WAL, so the offset is unambiguous");
    assert!(
        changes.len() >= 2,
        "expected both writes in the change stream, got {}",
        changes.len()
    );
}

/// A multi-collection database can be replicated one collection at a time (S-09).
///
/// `fetch_segment` carries the collection name, and `changes_in_collection` returns that
/// collection's changes after the (global, since D11) log offset, keys intact.
#[tokio::test(flavor = "multi_thread")]
async fn changes_can_be_read_per_collection() {
    use prkdb_types::replication::Change;

    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path());

    db.storage().put(b"users:alice", b"1").await.unwrap();
    db.storage().put(b"users:bob", b"2").await.unwrap();
    db.storage().put(b"orders:x", b"3").await.unwrap();

    let users = db
        .storage()
        .changes_in_collection("users", 0)
        .await
        .expect("naming the collection makes the cursor unambiguous");
    assert_eq!(users.len(), 2, "expected both users writes, got {users:?}");

    // Keys must come back in the form a consumer can replay through `put`, not the
    // collection-stripped form the inner adapter stores.
    for change in &users {
        let key = match change {
            Change::Put { key, .. } | Change::Delete { key, .. } => key,
        };
        assert!(
            key.starts_with(b"users:"),
            "a replicated key must carry its collection, got {:?}",
            String::from_utf8_lossy(key)
        );
    }

    let orders = db
        .storage()
        .changes_in_collection("orders", 0)
        .await
        .expect("the other collection reads independently");
    assert_eq!(orders.len(), 1);

    // A collection that does not exist yet is early, not wrong.
    assert!(db
        .storage()
        .changes_in_collection("nonexistent", 0)
        .await
        .expect("an unknown collection is not an error")
        .is_empty());
}

/// A multi-collection database replicates through one cursor (S-09, inverted by D11).
///
/// Before D11 this adapter held one WAL per collection, each numbering its records from
/// 1, so a bare offset could not address a position across them and `get_changes_since`
/// refused. Since Task 2.9b every collection is in the one WAL at the data directory
/// root: one log has one order, and the change stream spans collections in commit order.
///
/// (The S-09 fix in `FetchSegment` stays: an unreadable log must still not look empty.)
#[tokio::test(flavor = "multi_thread")]
async fn get_changes_since_spans_collections_in_commit_order() {
    use prkdb_types::replication::Change;

    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path());
    db.storage().put(b"users:a", b"1").await.unwrap();
    db.storage().put(b"orders:x", b"2").await.unwrap();
    db.storage().put(b"users:b", b"3").await.unwrap();

    let keys: Vec<Vec<u8>> = db
        .storage()
        .get_changes_since(0)
        .await
        .expect("one WAL, so the offset is unambiguous across collections")
        .into_iter()
        .map(|c| match c {
            Change::Put { key, .. } | Change::Delete { key, .. } => key,
        })
        .collect();
    assert_eq!(
        keys,
        vec![
            b"users:a".to_vec(),
            b"orders:x".to_vec(),
            b"users:b".to_vec()
        ],
        "every collection's changes, in the order they were committed"
    );
}

fn wal_config(dir: &Path, segment_bytes: u64) -> WalConfig {
    WalConfig {
        log_dir: dir.to_path_buf(),
        segment_bytes,
        sync_mode: SyncMode::Fast, // concurrency test: sync cost is not what is measured
        ..WalConfig::test_config()
    }
}

/// STO-03 + STO-05: with many writers racing on the same keys, the value a reader sees
/// before a restart is the value recovery produces after it.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_same_key_live_equals_recovered() {
    let dir = tempfile::tempdir().unwrap();
    let live = {
        let db = Arc::new(WalStorageAdapter::new(wal_config(dir.path(), 8 * 1024)).unwrap());
        let mut tasks = Vec::new();
        for w in 0..8u32 {
            let db = db.clone();
            tasks.push(tokio::spawn(async move {
                for round in 0..300u32 {
                    let key = format!("hot-{}", round % 4);
                    if round % 17 == w {
                        db.delete(key.as_bytes()).await.unwrap();
                    } else {
                        db.put(key.as_bytes(), format!("w{w}-r{round}").as_bytes())
                            .await
                            .unwrap();
                    }
                }
            }));
        }
        for t in tasks {
            t.await.unwrap();
        }
        let mut live = Vec::new();
        for k in 0..4 {
            live.push(db.get(format!("hot-{k}").as_bytes()).await.unwrap());
        }
        db.flush().await.unwrap();
        live
    };
    let db = WalStorageAdapter::open_async(wal_config(dir.path(), 8 * 1024))
        .await
        .unwrap();
    for (k, want) in live.iter().enumerate() {
        assert_eq!(
            &db.get(format!("hot-{k}").as_bytes()).await.unwrap(),
            want,
            "hot-{k}"
        );
    }
}

/// STO-08: `WalConfig::segment_bytes` decides when a segment rolls.
#[tokio::test(flavor = "multi_thread")]
async fn segment_bytes_is_honored() {
    let dir = tempfile::tempdir().unwrap();
    let db = WalStorageAdapter::new(wal_config(dir.path(), 64 * 1024)).unwrap();
    for i in 0..256u32 {
        db.put(format!("k{i}").as_bytes(), &vec![b'v'; 4096])
            .await
            .unwrap();
    }
    db.flush().await.unwrap();
    let segments: Vec<u64> = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "wal"))
        .map(|p| std::fs::metadata(p).unwrap().len())
        .collect();
    assert!(
        segments.len() >= 16,
        "1 MiB of writes in 64 KiB segments: {segments:?}"
    );
    assert!(
        segments.iter().all(|&len| len <= 64 * 1024),
        "a segment overran: {segments:?}"
    );
}

/// A format-1 directory (old files, no `FORMAT` marker) is refused by name, never opened
/// as an empty database next to the old log (Task 2.11's open rules).
#[tokio::test(flavor = "multi_thread")]
async fn a_format_1_directory_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("mmap_segment_0")).unwrap();
    let err = WalStorageAdapter::new(wal_config(dir.path(), 1 << 20))
        .err()
        .expect("must refuse");
    assert!(err.to_string().contains("format 1"), "{err}");
}
