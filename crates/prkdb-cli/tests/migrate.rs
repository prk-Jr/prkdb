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
        let line = format!("{} is at format 2", root.path().join(member).display());
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
