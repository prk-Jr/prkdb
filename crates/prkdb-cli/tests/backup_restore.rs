//! Backup and restore, end to end against the built binary.
//!
//! `prkdb backup` and `prkdb restore` have existed since before this work; what did not
//! exist was any test that ran one and then the other. A backup nobody has restored is
//! not a backup — it is a file.
//!
//! # These tests found two real bugs on their first run
//!
//! The round-trip failed immediately with "take_snapshot not supported" (**S-04**):
//! `PrkDb::builder().with_data_dir()` produces a `CollectionPartitionedAdapter`, which
//! holds one `WalStorageAdapter` per collection and never implemented `take_snapshot`, so
//! the call reached the trait default that refuses.
//!
//! Implementing it made backup *succeed* while archiving **zero entries**, which is how
//! **S-05** surfaced: reopening a data directory destroyed it. Both are fixed and all
//! three tests run.
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

// ── Manifest verification ────────────────────────────────────────────────────
//
// `restore_rejects_a_corrupt_archive` above covers garbage that cannot be parsed at all.
// These cover the harder case: an archive that still parses but is not the one that was
// backed up. Without a manifest that case restores silently and partially.

fn manifest_for(archive: &Path) -> std::path::PathBuf {
    let mut name = archive.as_os_str().to_os_string();
    name.push(".manifest");
    std::path::PathBuf::from(name)
}

#[tokio::test(flavor = "multi_thread")]
async fn backup_writes_a_manifest_describing_the_archive() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    let archive = dir.path().join("snapshot.bin");

    seed(&source, &[("users:a", "1"), ("users:b", "2")]).await;

    let out = run(&[
        "--database",
        source.to_str().unwrap(),
        "backup",
        "--output",
        archive.to_str().unwrap(),
    ]);
    assert!(out.status.success(), "backup failed");

    let manifest = manifest_for(&archive);
    assert!(manifest.exists(), "backup must write a manifest sidecar");

    let parsed: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&manifest).unwrap()).expect("manifest is JSON");

    // The digest must describe the archive on disk, not what we meant to write.
    let actual_len = std::fs::metadata(&archive).unwrap().len();
    assert_eq!(
        parsed["bytes"].as_u64(),
        Some(actual_len),
        "manifest length must match the archive"
    );
    assert_eq!(
        parsed["entries"].as_u64(),
        Some(2),
        "manifest must record the entry count read back from the archive"
    );
    assert_eq!(
        parsed["sha256"].as_str().map(str::len),
        Some(64),
        "sha256 must be a 64-char hex digest, got {:?}",
        parsed["sha256"]
    );
    // A path here would break as soon as the pair is moved, which is the normal case.
    assert_eq!(parsed["archive"].as_str(), Some("snapshot.bin"));
}

/// The case the manifest exists for: an archive that is still structurally readable but
/// whose contents changed. Flipping bytes in the middle leaves the header intact.
#[tokio::test(flavor = "multi_thread")]
async fn restore_refuses_an_archive_that_does_not_match_its_manifest() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    let target = dir.path().join("target");
    let archive = dir.path().join("snapshot.bin");

    seed(&source, &[("users:a", "1"), ("users:b", "2")]).await;
    let out = run(&[
        "--database",
        source.to_str().unwrap(),
        "backup",
        "--output",
        archive.to_str().unwrap(),
    ]);
    assert!(out.status.success());

    // Corrupt the tail, leaving the length unchanged so the digest is what catches it.
    let mut bytes = std::fs::read(&archive).unwrap();
    let n = bytes.len();
    assert!(n > 8, "archive too small to corrupt meaningfully");
    bytes[n - 4] ^= 0xff;
    std::fs::write(&archive, &bytes).unwrap();

    let out = run(&[
        "restore",
        "--input",
        archive.to_str().unwrap(),
        "--data-dir",
        target.to_str().unwrap(),
    ]);
    assert!(
        !out.status.success(),
        "an archive that disagrees with its manifest must not restore"
    );

    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        text.contains("checksum") || text.contains("truncated"),
        "the error must say why, got: {text}"
    );

    // Nothing may have been written before the check ran.
    let wrote_anything = target
        .read_dir()
        .map(|mut d| d.next().is_some())
        .unwrap_or(false);
    assert!(
        !wrote_anything,
        "verification must happen before the target is touched"
    );
}

/// Truncation is reported as truncation rather than as an opaque digest mismatch.
#[tokio::test(flavor = "multi_thread")]
async fn restore_reports_a_truncated_archive_by_length() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    let target = dir.path().join("target");
    let archive = dir.path().join("snapshot.bin");

    seed(&source, &[("users:a", "1")]).await;
    assert!(run(&[
        "--database",
        source.to_str().unwrap(),
        "backup",
        "--output",
        archive.to_str().unwrap(),
    ])
    .status
    .success());

    let bytes = std::fs::read(&archive).unwrap();
    std::fs::write(&archive, &bytes[..bytes.len() - 3]).unwrap();

    let out = run(&[
        "restore",
        "--input",
        archive.to_str().unwrap(),
        "--data-dir",
        target.to_str().unwrap(),
    ]);
    assert!(
        !out.status.success(),
        "a truncated archive must not restore"
    );

    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        text.contains("truncated"),
        "a short archive should be named as truncated, got: {text}"
    );
}

/// An archive with no manifest still restores — archives written before manifests
/// existed must not become unrestorable — but says so.
#[tokio::test(flavor = "multi_thread")]
async fn a_missing_manifest_warns_but_still_restores() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    let target = dir.path().join("target");
    let archive = dir.path().join("snapshot.bin");

    seed(&source, &[("users:a", "1")]).await;
    assert!(run(&[
        "--database",
        source.to_str().unwrap(),
        "backup",
        "--output",
        archive.to_str().unwrap(),
    ])
    .status
    .success());

    std::fs::remove_file(manifest_for(&archive)).unwrap();

    let out = run(&[
        "restore",
        "--input",
        archive.to_str().unwrap(),
        "--data-dir",
        target.to_str().unwrap(),
    ]);
    assert!(
        out.status.success(),
        "a manifest-less archive must still restore: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("No manifest"),
        "restoring without verification must be stated"
    );
    assert_eq!(
        read_back(&target, "users:a").await.as_deref(),
        Some(&b"1"[..])
    );
}

/// `--skip-verify` is an escape hatch for salvage, not a way to silence the check.
#[tokio::test(flavor = "multi_thread")]
async fn skip_verify_restores_a_mismatched_archive() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    let target = dir.path().join("target");
    let archive = dir.path().join("snapshot.bin");

    seed(&source, &[("users:a", "1")]).await;
    assert!(run(&[
        "--database",
        source.to_str().unwrap(),
        "backup",
        "--output",
        archive.to_str().unwrap(),
    ])
    .status
    .success());

    // Rewrite the manifest so it no longer describes the archive.
    let path = manifest_for(&archive);
    let mut m: serde_json::Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    m["sha256"] = serde_json::Value::String("0".repeat(64));
    std::fs::write(&path, serde_json::to_vec(&m).unwrap()).unwrap();

    assert!(
        !run(&[
            "restore",
            "--input",
            archive.to_str().unwrap(),
            "--data-dir",
            target.to_str().unwrap(),
        ])
        .status
        .success(),
        "the mismatch must be fatal by default"
    );

    let out = run(&[
        "restore",
        "--input",
        archive.to_str().unwrap(),
        "--data-dir",
        target.to_str().unwrap(),
        "--skip-verify",
    ]);
    assert!(
        out.status.success(),
        "--skip-verify must restore anyway: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(
        read_back(&target, "users:a").await.as_deref(),
        Some(&b"1"[..])
    );
}
