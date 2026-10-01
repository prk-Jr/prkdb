//! The data-directory `FORMAT` marker and its open rules (Task 2.11, spec 2b, D3).

use prkdb::storage::format::{read_format, FORMAT_FILE};
use prkdb::storage::WalStorageAdapter;
use prkdb_core::format::FORMAT_VERSION;
use prkdb_core::wal::WalConfig;

fn cfg(dir: &std::path::Path) -> WalConfig {
    WalConfig {
        log_dir: dir.to_path_buf(),
        ..WalConfig::test_config()
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn an_empty_directory_is_created_as_format_2() {
    let dir = tempfile::tempdir().unwrap();
    drop(WalStorageAdapter::new(cfg(dir.path())).unwrap());
    let text = std::fs::read_to_string(dir.path().join(FORMAT_FILE)).unwrap();
    assert!(text.contains("format = 2"), "{text}");
    assert!(
        text.contains(&format!("created_by = \"{}\"", env!("CARGO_PKG_VERSION"))),
        "{text}"
    );
    assert_eq!(
        read_format(dir.path()).unwrap().unwrap().format,
        FORMAT_VERSION
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_missing_directory_is_created_as_format_2() {
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("a").join("b");
    drop(WalStorageAdapter::new(cfg(&dir)).unwrap());
    assert_eq!(read_format(&dir).unwrap().unwrap().format, FORMAT_VERSION);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_format_2_directory_reopens() {
    let dir = tempfile::tempdir().unwrap();
    drop(WalStorageAdapter::new(cfg(dir.path())).unwrap());
    WalStorageAdapter::open_async(cfg(dir.path()))
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_non_empty_directory_without_format_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("something.log"), b"old data").unwrap();
    let err = WalStorageAdapter::new(cfg(dir.path()))
        .err()
        .expect("must refuse");
    let msg = err.to_string();
    assert!(
        msg.contains("format") && msg.contains("docs/guide/upgrade"),
        "{msg}"
    );
    assert!(
        !dir.path().join(FORMAT_FILE).exists(),
        "refusal must not write anything"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_newer_format_is_refused_by_number() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join(FORMAT_FILE),
        "format = 3\ncreated_by = \"9.9.9\"\n",
    )
    .unwrap();
    let err = WalStorageAdapter::new(cfg(dir.path()))
        .err()
        .expect("must refuse");
    let msg = err.to_string();
    assert!(
        msg.contains("newer PrkDB (format 3)") && msg.contains("reads format 2"),
        "{msg}"
    );
}

/// A crash while the marker was being created leaves only `FORMAT.tmp`: the directory
/// never held data, so it is created afresh and the stale temp file is gone.
#[tokio::test(flavor = "multi_thread")]
async fn a_directory_holding_only_a_stale_format_tmp_counts_as_empty() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("FORMAT.tmp"), "form").unwrap();
    drop(WalStorageAdapter::new(cfg(dir.path())).unwrap());
    assert_eq!(
        read_format(dir.path()).unwrap().unwrap().format,
        FORMAT_VERSION
    );
    assert!(!dir.path().join("FORMAT.tmp").exists());
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unreadable_format_file_is_refused_and_left_alone() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join(FORMAT_FILE), "garbage\n").unwrap();
    let err = WalStorageAdapter::new(cfg(dir.path()))
        .err()
        .expect("must refuse");
    assert!(err.to_string().contains(FORMAT_FILE), "{err}");
    assert_eq!(
        std::fs::read_to_string(dir.path().join(FORMAT_FILE)).unwrap(),
        "garbage\n"
    );
}

/// `STORAGE_PATH` is a container; `meta/` and each `partition_<n>/` are data directories
/// with their own marker, so a format-1 cluster is refused partition by partition.
#[tokio::test(flavor = "multi_thread")]
async fn multi_raft_partitions_are_format_2_data_directories() {
    let root = tempfile::tempdir().unwrap();
    let db = prkdb::PrkDb::new_multi_raft(
        1,
        prkdb::raft::ClusterConfig::default(),
        root.path().to_path_buf(),
    )
    .unwrap();
    drop(db);
    assert!(root.path().join("meta").join(FORMAT_FILE).exists());
    assert!(root.path().join("partition_0").join(FORMAT_FILE).exists());
    assert!(!root.path().join(FORMAT_FILE).exists());

    let old = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(old.path().join("partition_0").join("mmap_segment_0")).unwrap();
    let err = prkdb::PrkDb::new_multi_raft(
        1,
        prkdb::raft::ClusterConfig::default(),
        old.path().to_path_buf(),
    )
    .err()
    .expect("must refuse");
    assert!(err.to_string().contains("format 1"), "{err}");
}

/// Until Task 2.9b, each `collections/<name>/` is its own data directory, opened lazily on
/// first access. An old one is refused when the database is built, not by a panic later.
#[tokio::test(flavor = "multi_thread")]
async fn an_old_collection_directory_is_refused_when_the_database_is_built() {
    let root = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(
        root.path()
            .join("collections")
            .join("users")
            .join("mmap_segment_0"),
    )
    .unwrap();
    let err = prkdb::PrkDb::builder()
        .with_optimized_storage(root.path(), prkdb::builder::OptimizationLevel::Balanced)
        .build()
        .err()
        .expect("must refuse");
    assert!(err.to_string().contains("format 1"), "{err}");
}

/// What a filesystem puts in a fresh volume root (ext4 `lost+found`, e.g. a Kubernetes
/// PVC) or what an OS drops into any directory (`.DS_Store`) is not data.
#[tokio::test(flavor = "multi_thread")]
async fn lost_and_found_and_dotfiles_do_not_make_a_directory_format_1() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir(dir.path().join("lost+found")).unwrap();
    std::fs::write(dir.path().join(".DS_Store"), b"x").unwrap();
    drop(WalStorageAdapter::new(cfg(dir.path())).unwrap());
    assert_eq!(
        read_format(dir.path()).unwrap().unwrap().format,
        FORMAT_VERSION
    );
}
