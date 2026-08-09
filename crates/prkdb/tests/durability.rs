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

use prkdb::PrkDb;
use std::path::Path;

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
