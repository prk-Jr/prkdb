//! Backup and restore, end to end against the built binary.
//!
//! `prkdb backup` and `prkdb restore` have existed since before this work; what did not
//! exist was any test that ran one and then the other. A backup nobody has restored is
//! not a backup — it is a file.
//!
//! # These tests found a real bug on their first run
//!
//! Two of the three are `#[ignore]`d against **S-04**: `prkdb backup` fails with
//! "take_snapshot not supported" on any database opened with `--database`.
//! `PrkDb::builder().with_data_dir()` produces a `CollectionPartitionedAdapter`, which
//! holds one `WalStorageAdapter` per collection and does not implement `take_snapshot`,
//! so the call reaches the trait default that refuses.
//!
//! They are kept rather than deleted: they are the regression test for the fix, and they
//! pass the moment it lands.
//!
//! # What is asserted, and what deliberately is not
//!
//! `handle_restore` replays the snapshot by re-`put`ing every entry, so the rebuilt WAL
//! differs byte-for-byte from the original: different offsets, different segment
//! boundaries. **Logical equivalence is the property that matters** — every key reads back
//! with the value it had. An earlier draft of the plan demanded a byte-identical
//! round-trip, which this design can never satisfy.

use prkdb::PrkDb;
use std::path::Path;
use std::process::Command;

fn run(args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_prkdb-cli"))
        .args(args)
        .output()
        .expect("the CLI binary must run")
}

/// Seed through the library rather than the CLI: `put` and `get` are remote commands
/// requiring a running server, while `backup` and `restore` operate offline on a data
/// directory. Mixing the two would mean starting a server just to write four keys.
async fn seed(dir: &Path, pairs: &[(&str, &str)]) {
    let db = PrkDb::builder()
        .with_data_dir(dir)
        .build()
        .expect("open the source database");
    for (k, v) in pairs {
        db.put(k.as_bytes(), v.as_bytes())
            .await
            .unwrap_or_else(|e| panic!("seeding {k}: {e}"));
    }
}

async fn read_back(dir: &Path, key: &str) -> Option<Vec<u8>> {
    let db = PrkDb::builder()
        .with_data_dir(dir)
        .build()
        .expect("open the restored database");
    db.get(key.as_bytes()).await.expect("read")
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "blocked by S-04: CollectionPartitionedAdapter does not implement take_snapshot, so `prkdb backup` fails on any database built with --database. Un-ignore when S-04 is fixed."]
async fn backup_then_restore_preserves_every_value() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    let target = dir.path().join("target");
    let archive = dir.path().join("snapshot.bin");

    // Keys are `collection:id`; the storage layer requires the delimiter.
    let pairs = [
        ("users:alpha", "one"),
        ("users:beta", "two"),
        ("orders:gamma", "three"),
        ("orders:delta", "four"),
    ];
    seed(&source, &pairs).await;

    let out = run(&[
        "--database",
        source.to_str().unwrap(),
        "backup",
        "--output",
        archive.to_str().unwrap(),
    ]);
    assert!(
        out.status.success(),
        "backup failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(archive.exists(), "backup must produce an archive");

    let out = run(&[
        "restore",
        "--input",
        archive.to_str().unwrap(),
        "--data-dir",
        target.to_str().unwrap(),
    ]);
    assert!(
        out.status.success(),
        "restore failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // Logical equivalence: every key reads back with its original value.
    for (k, v) in &pairs {
        let restored = read_back(&target, k).await;
        assert_eq!(
            restored.as_deref(),
            Some(v.as_bytes()),
            "restored {k} must hold its original value"
        );
    }
}

/// Restoring over a populated directory without `--force` must refuse rather than
/// silently merging two databases.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "blocked by S-04: CollectionPartitionedAdapter does not implement take_snapshot, so `prkdb backup` fails on any database built with --database. Un-ignore when S-04 is fixed."]
async fn restore_refuses_a_non_empty_target_without_force() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    let target = dir.path().join("target");
    let archive = dir.path().join("snapshot.bin");

    seed(&source, &[("users:k", "v")]).await;
    seed(&target, &[("users:existing", "data")]).await;

    let out = run(&[
        "--database",
        source.to_str().unwrap(),
        "backup",
        "--output",
        archive.to_str().unwrap(),
    ]);
    assert!(out.status.success());

    let out = run(&[
        "restore",
        "--input",
        archive.to_str().unwrap(),
        "--data-dir",
        target.to_str().unwrap(),
    ]);
    assert!(
        !out.status.success(),
        "restoring over existing data without --force must be refused"
    );

    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        text.contains("--force"),
        "the error should name the way forward, got: {text}"
    );
}

/// A corrupt archive must fail loudly. Restoring garbage into an empty data directory and
/// reporting success is the worst possible outcome for a backup tool.
#[test]
fn restore_rejects_a_corrupt_archive() {
    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().join("target");
    let archive = dir.path().join("garbage.bin");
    std::fs::write(&archive, b"this is not a prkdb snapshot").unwrap();

    let out = run(&[
        "restore",
        "--input",
        archive.to_str().unwrap(),
        "--data-dir",
        target.to_str().unwrap(),
    ]);
    assert!(
        !out.status.success(),
        "a corrupt archive must not restore successfully"
    );
}
