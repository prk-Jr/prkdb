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
