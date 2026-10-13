//! `prkdb-cli migrate --data-dir` (Task 2.11, spec D4).

use std::process::Command;

fn migrate(dir: &std::path::Path) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_prkdb-cli"))
        .args(["migrate", "--data-dir", dir.to_str().unwrap()])
        .output()
        .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn migrate_on_a_current_directory_says_there_is_nothing_to_do() {
    let dir = tempfile::tempdir().unwrap();
    drop(
        prkdb::storage::WalStorageAdapter::new(prkdb_core::wal::WalConfig {
            log_dir: dir.path().to_path_buf(),
            ..prkdb_core::wal::WalConfig::test_config()
        })
        .unwrap(),
    );
    let out = migrate(dir.path());
    assert!(out.status.success(), "{out:?}");
    assert!(
        String::from_utf8_lossy(&out.stdout).contains("no migrations available for format 2"),
        "{out:?}"
    );
}

#[test]
fn migrate_on_a_format_1_directory_fails_and_explains() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir(dir.path().join("mmap_segment_0")).unwrap();
    let out = migrate(dir.path());
    assert!(!out.status.success());
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        err.contains("format 1") && err.contains("docs/guide/upgrade"),
        "{err}"
    );
}

#[test]
fn migrate_on_a_newer_format_fails_and_names_it() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("FORMAT"),
        "format = 3\ncreated_by = \"9.9.9\"\n",
    )
    .unwrap();
    let out = migrate(dir.path());
    assert!(!out.status.success());
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("newer PrkDB (format 3)"), "{err}");
}

#[test]
fn migrate_on_a_missing_directory_fails_without_creating_it() {
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("absent");
    let out = migrate(&dir);
    assert!(!out.status.success());
    assert!(!dir.exists(), "migrate is read-only on a missing directory");
}

/// The multi-raft `STORAGE_PATH` holds `meta/`, `partition_<n>/` and `schemas/` and has no
/// `FORMAT` of its own: it is a container, not a format-1 directory.
#[tokio::test(flavor = "multi_thread")]
async fn migrate_on_a_multi_raft_root_reports_each_data_directory() {
    let root = tempfile::tempdir().unwrap();
    drop(
        prkdb::PrkDb::new_multi_raft(
            2,
            prkdb::raft::ClusterConfig::default(),
            root.path().to_path_buf(),
        )
        .unwrap(),
    );
    let out = migrate(root.path());
    assert!(out.status.success(), "{out:?}");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("is a container"), "{stdout}");
    for member in ["meta", "partition_0", "partition_1"] {
        let line = format!(
            "{} is a key/value store at format 2",
            root.path().join(member).display()
        );
        assert!(stdout.contains(&line), "{line} missing from {stdout}");
    }
    assert!(!stdout.contains("format 1"), "{stdout}");
}

#[test]
fn migrate_on_a_container_with_an_old_member_fails_and_names_it() {
    let root = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(root.path().join("meta")).unwrap();
    std::fs::write(
        root.path().join("meta").join("FORMAT"),
        "format = 2\ncreated_by = \"0.6.0\"\n",
    )
    .unwrap();
    std::fs::create_dir_all(root.path().join("partition_0").join("mmap_segment_0")).unwrap();
    let out = migrate(root.path());
    assert!(!out.status.success());
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        err.contains("partition_0") && err.contains("format 1"),
        "{err}"
    );
}

/// Review M4: a pre-D11 optimized-storage root (`collections/<name>/`, no root `FORMAT`)
/// is one format-1 data directory to `migrate`, as it is to the open that refuses it; it
/// used to be reported as a container of up-to-date collections the database would not
/// open.
#[test]
fn migrate_on_a_pre_d11_collections_root_reports_format_1_like_open() {
    let root = tempfile::tempdir().unwrap();
    let users = root.path().join("collections").join("users");
    std::fs::create_dir_all(&users).unwrap();
    std::fs::write(users.join("FORMAT"), "format = 2\ncreated_by = \"0.6.0\"\n").unwrap();

    let out = migrate(root.path());
    assert!(!out.status.success(), "{out:?}");
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("format 1"), "{err}");
    assert!(!String::from_utf8_lossy(&out.stdout).contains("is a container"));

    let open = prkdb::storage::CollectionPartitionedAdapter::new(prkdb_core::wal::WalConfig {
        log_dir: root.path().to_path_buf(),
        ..prkdb_core::wal::WalConfig::test_config()
    });
    assert!(
        open.err()
            .is_some_and(|e| e.to_string().contains("format 1")),
        "open and migrate agree"
    );
}

/// STO-10: `migrate` takes the data-directory lock, so it refuses a directory a live
/// database holds, and works once that database is closed.
#[tokio::test(flavor = "multi_thread")]
async fn migrate_on_a_locked_directory_refuses() {
    let dir = tempfile::tempdir().unwrap();
    let db = prkdb::storage::WalStorageAdapter::new(prkdb_core::wal::WalConfig {
        log_dir: dir.path().to_path_buf(),
        ..prkdb_core::wal::WalConfig::test_config()
    })
    .unwrap();
    let out = migrate(dir.path());
    assert!(!out.status.success(), "{out:?}");
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        err.contains("in use by another process")
            && (!cfg!(unix) || err.contains(&format!("pid {}", std::process::id()))),
        "{err}"
    );

    drop(db);
    let out = migrate(dir.path());
    assert!(out.status.success(), "{out:?}");
}

/// STO-10: a multi-raft root with a live database refuses for every member it holds.
#[tokio::test(flavor = "multi_thread")]
async fn migrate_on_a_live_multi_raft_root_refuses_each_member() {
    let root = tempfile::tempdir().unwrap();
    let db = prkdb::PrkDb::new_multi_raft(
        2,
        prkdb::raft::ClusterConfig::default(),
        root.path().to_path_buf(),
    )
    .unwrap();
    let out = migrate(root.path());
    assert!(!out.status.success(), "{out:?}");
    let err = String::from_utf8_lossy(&out.stderr);
    for member in ["meta", "partition_0", "partition_1"] {
        let dir = root.path().join(member).display().to_string();
        assert!(
            err.lines()
                .any(|l| l.contains(&dir) && l.contains("in use by another process")),
            "{member} not refused as locked: {err}"
        );
    }
    drop(db);
}

/// The lock file does not make an empty directory look like format-1 data.
#[test]
fn migrate_ignores_the_lock_file() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("LOCK"), b"").unwrap();
    let out = migrate(dir.path());
    assert!(out.status.success(), "{out:?}");
    assert!(
        String::from_utf8_lossy(&out.stdout).contains("is empty"),
        "{out:?}"
    );
}

/// Read-only media: `LOCK` cannot be created, so `migrate` inspects without the lock. A
/// report (current format) and a dry run proceed; a migration that would write is refused.
#[cfg(unix)]
#[test]
fn migrate_on_a_read_only_directory_reports_but_does_not_migrate() {
    use std::os::unix::fs::PermissionsExt;

    /// Makes `dir` read-only for the test and writable again on drop, so the temp dir
    /// can be removed even if an assertion fails.
    struct ReadOnly<'a>(&'a std::path::Path);
    impl Drop for ReadOnly<'_> {
        fn drop(&mut self) {
            let _ = std::fs::set_permissions(self.0, std::fs::Permissions::from_mode(0o755));
        }
    }
    fn read_only(dir: &std::path::Path) -> Option<ReadOnly<'_>> {
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o555)).unwrap();
        let guard = ReadOnly(dir);
        // Root ignores directory permissions; nothing to test then.
        let probe = dir.join("probe");
        if std::fs::write(&probe, b"").is_ok() {
            let _ = std::fs::remove_file(probe);
            return None;
        }
        Some(guard)
    }

    // At the current format: reported, nothing to do.
    let current = tempfile::tempdir().unwrap();
    std::fs::write(
        current.path().join("FORMAT"),
        "format = 2\ncreated_by = \"0.6.0\"\n",
    )
    .unwrap();
    let Some(_ro) = read_only(current.path()) else {
        return;
    };
    let out = migrate(current.path());
    assert!(out.status.success(), "{out:?}");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("not writable"), "{stdout}");
    assert!(
        stdout.contains("no migrations available for format 2"),
        "{stdout}"
    );
    assert!(!current.path().join("LOCK").exists());

    // Needs migrating: refused without --dry-run because the lock cannot be taken; with
    // --dry-run it gets as far as the plan (format 1 has none).
    let old = tempfile::tempdir().unwrap();
    std::fs::create_dir(old.path().join("mmap_segment_0")).unwrap();
    let _ro_old = read_only(old.path()).expect("not root: checked above");
    let out = migrate(old.path());
    assert!(!out.status.success(), "{out:?}");
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        err.contains("not writable") && err.contains("--dry-run"),
        "{err}"
    );
    let out = Command::new(env!("CARGO_BIN_EXE_prkdb-cli"))
        .args([
            "migrate",
            "--data-dir",
            old.path().to_str().unwrap(),
            "--dry-run",
        ])
        .output()
        .unwrap();
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        err.contains("no migrations available for format 1"),
        "{err}"
    );
}

/// A stream directory is reported as a stream, so a future migration step sees its kind
/// (the review found migrations kind-blind: a step that rewrote FORMAT as kv would have
/// silently turned a stream into an empty key/value store).
#[tokio::test(flavor = "multi_thread")]
async fn migrate_on_a_stream_directory_reports_its_kind() {
    let dir = tempfile::tempdir().unwrap();
    prkdb::stream_log::StreamLog::open(prkdb::stream_log::StreamConfig::new(dir.path()))
        .await
        .unwrap()
        .close()
        .unwrap();
    let out = migrate(dir.path());
    assert!(out.status.success(), "{out:?}");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("is a stream at format 2") && !stdout.contains("key/value"),
        "{stdout}"
    );
    assert!(
        std::fs::read_to_string(dir.path().join("FORMAT"))
            .unwrap()
            .contains("kind = \"stream\""),
        "migrate must not rewrite a stream marker"
    );
}

/// A partitioned stream's root holds `STREAM` and `partition_<n>/`, not `FORMAT`: it is a
/// container, and every partition is reported as a stream.
#[tokio::test(flavor = "multi_thread")]
async fn migrate_on_a_partitioned_stream_reports_each_partition_as_a_stream() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("orders");
    prkdb::stream_log::PartitionedStream::open(
        &root,
        3,
        prkdb::stream_log::StreamConfig::new(&root),
    )
    .await
    .unwrap()
    .close()
    .unwrap();
    let out = migrate(&root);
    assert!(out.status.success(), "{out:?}");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("is a container"), "{stdout}");
    for p in 0..3 {
        let line = format!("partition_{p} is a stream at format 2");
        assert!(stdout.contains(&line), "missing {line:?} in {stdout}");
    }
}

/// A key/value directory is still reported as one, in the same words as before plus its
/// kind.
#[tokio::test(flavor = "multi_thread")]
async fn migrate_on_a_kv_directory_reports_its_kind() {
    let dir = tempfile::tempdir().unwrap();
    drop(
        prkdb::storage::WalStorageAdapter::new(prkdb_core::wal::WalConfig {
            log_dir: dir.path().to_path_buf(),
            ..prkdb_core::wal::WalConfig::test_config()
        })
        .unwrap(),
    );
    let out = migrate(dir.path());
    assert!(out.status.success(), "{out:?}");
    assert!(
        String::from_utf8_lossy(&out.stdout).contains("is a key/value store at format 2"),
        "{out:?}"
    );
}
