//! Container creation, crash recovery and routing for partitioned streams.

use prkdb::stream_log::manifest::StreamManifest;
use prkdb::stream_log::partitioned::{PartitionedStream, Route};
use prkdb::stream_log::{ReadLimits, Record, StartAt, StreamConfig, StreamLog};
use prkdb_core::vfs::{LockGuard, OpenMode, StdVfs, Vfs, VfsFile};
use prkdb_types::error::StorageError;
use std::path::Path;
use std::sync::{Arc, Mutex};

fn rec() -> Vec<Record> {
    vec![Record {
        key: None,
        value: b"value".to_vec(),
        headers: vec![],
    }]
}

async fn open(root: &Path, n: u32) -> Result<PartitionedStream, StorageError> {
    PartitionedStream::open(root, n, StreamConfig::new(root)).await
}

#[tokio::test(flavor = "multi_thread")]
async fn container_creates_stream_partitions_before_publishing_manifest() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("orders");
    let stream = open(&root, 3).await.unwrap();
    assert_eq!(stream.partitions(), 3);
    assert!(!root.join("FORMAT").exists());
    assert!(std::fs::read_dir(&root).unwrap().all(|e| e
        .unwrap()
        .path()
        .extension()
        .is_none_or(|e| e != "wal")));
    for p in 0..3 {
        let path = root.join(format!("partition_{p}"));
        assert!(std::fs::read_to_string(path.join("FORMAT"))
            .unwrap()
            .contains("kind = \"stream\""));
        assert!(stream.partition(p).is_some());
    }
    assert!(stream.partition(3).is_none());
    let manifest = StreamManifest::decode(&std::fs::read(root.join("STREAM")).unwrap()).unwrap();
    assert_eq!(manifest.partitions, 3);
    assert_eq!(manifest.created_by, env!("CARGO_PKG_VERSION"));
    stream.close().unwrap();
    open(&root, 3).await.unwrap().close().unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn durable_partition_count_cannot_change_on_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("orders");
    let stream = open(&root, 3).await.unwrap();
    stream.append(Route::Partition(1), rec()).await.unwrap();
    stream.close().unwrap();
    let wal = root
        .join("partition_1")
        .join(prkdb_core::wal::segment::segment_file_name(1));
    // Reopening the partition would repair this torn tail. A count refusal must
    // happen before any partition recovery is allowed to modify the file.
    let mut bytes = std::fs::read(&wal).unwrap();
    bytes.extend_from_slice(&[1, 2, 3]);
    std::fs::write(&wal, &bytes).unwrap();
    let before = std::fs::read(root.join("STREAM")).unwrap();
    assert!(matches!(
        open(&root, 4).await,
        Err(StorageError::Validation(_))
    ));
    assert_eq!(std::fs::read(root.join("STREAM")).unwrap(), before);
    assert!(!root.join("partition_3").exists());
    assert_eq!(std::fs::read(&wal).unwrap(), bytes);
}

#[tokio::test(flavor = "multi_thread")]
async fn interrupted_creation_finishes_only_when_every_partition_has_no_frames() {
    for nonempty in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("orders");
        for p in [0, 2, 9] {
            let path = root.join(format!("partition_{p}"));
            let log = StreamLog::open(StreamConfig::new(&path)).await.unwrap();
            if nonempty && p != 0 {
                log.append(rec()).await.unwrap();
            }
            log.close().unwrap();
        }
        let snapshots: Vec<_> = [0, 2, 9]
            .into_iter()
            .map(|p| {
                let wal = root
                    .join(format!("partition_{p}"))
                    .join(prkdb_core::wal::segment::segment_file_name(1));
                let bytes = std::fs::read(&wal).unwrap();
                (wal, bytes)
            })
            .collect();
        let result = open(&root, 3).await;
        if nonempty {
            let error = result.err().unwrap().to_string();
            assert!(
                error.contains("partition_2") && error.contains("partition_9"),
                "{error}"
            );
            assert!(!root.join("STREAM").exists());
            assert!(!root.join("partition_1").exists());
            for (wal, bytes) in snapshots {
                assert_eq!(std::fs::read(wal).unwrap(), bytes);
            }
        } else {
            result.unwrap().close().unwrap();
            assert!(root.join("STREAM").exists());
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn interrupted_creation_repairs_an_empty_final_wal() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("orders");
    let partition = root.join("partition_0");
    StreamLog::open(StreamConfig::new(&partition))
        .await
        .unwrap()
        .close()
        .unwrap();
    let path = partition.join(prkdb_core::wal::segment::segment_file_name(1));
    std::fs::write(&path, []).unwrap();
    let stream = open(&root, 1)
        .await
        .expect("a zero-length final WAL contains no frames");
    assert!(root.join("STREAM").exists());
    stream.append(Route::Partition(0), rec()).await.unwrap();
    let batch = stream
        .partition(0)
        .unwrap()
        .read_from(StartAt::Earliest, ReadLimits::default())
        .await
        .unwrap();
    assert_eq!(batch.records.len(), 1);
    assert_eq!(batch.records[0].value, b"value");
    stream.close().unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn interrupted_creation_refuses_an_empty_nonfinal_wal_without_repair() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("orders");
    let partition = root.join("partition_0");
    StreamLog::open(StreamConfig::new(&partition))
        .await
        .unwrap()
        .close()
        .unwrap();
    let first = partition.join(prkdb_core::wal::segment::segment_file_name(1));
    std::fs::write(&first, []).unwrap();
    let last = partition.join(prkdb_core::wal::segment::segment_file_name(2));
    let file = StdVfs.create(&last).unwrap();
    prkdb_core::wal::segment::write_segment_header(file.as_ref(), 2).unwrap();
    file.sync_data().unwrap();
    let last_before = std::fs::read(&last).unwrap();
    assert!(matches!(
        open(&root, 1).await,
        Err(StorageError::Corruption(_))
    ));
    assert!(!root.join("STREAM").exists());
    assert_eq!(std::fs::read(&first).unwrap(), Vec::<u8>::new());
    assert_eq!(std::fs::read(&last).unwrap(), last_before);
}

#[tokio::test(flavor = "multi_thread")]
async fn invalid_stream_name_preserves_existing_data_and_torn_tail() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("Orders");
    let log = StreamLog::open(StreamConfig::new(&root)).await.unwrap();
    log.append(rec()).await.unwrap();
    log.close().unwrap();
    let wal = root.join(prkdb_core::wal::segment::segment_file_name(1));
    let mut bytes = std::fs::read(&wal).unwrap();
    bytes.extend_from_slice(&[1, 2, 3]);
    std::fs::write(&wal, &bytes).unwrap();
    let marker = std::fs::read(root.join("FORMAT")).unwrap();
    assert!(matches!(
        open(&root, 3).await,
        Err(StorageError::Validation(_))
    ));
    assert_eq!(std::fs::read(wal).unwrap(), bytes);
    assert_eq!(std::fs::read(root.join("FORMAT")).unwrap(), marker);
    assert!(!root.join("STREAM").exists());
    assert!(!root.join("partition_0").exists());
}

#[tokio::test(flavor = "multi_thread")]
async fn routes_use_stable_partitioner_round_robin_and_validated_explicit_partition() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("orders");
    let stream = open(&root, 3).await.unwrap();
    // Fixed raw-byte SeaHash routing, independent of Rust Hash framing.
    for (key, golden) in [
        (b"".as_slice(), 2),
        (b"user-0".as_slice(), 1),
        (b"user-1".as_slice(), 1),
        (&[0, 255, 1], 0),
    ] {
        let (partition, ack) = stream.append(Route::Key(key), rec()).await.unwrap();
        assert_eq!(partition, golden);
        let read = stream
            .partition(partition)
            .unwrap()
            .read_from(StartAt::Offset(ack.first()), ReadLimits::default())
            .await
            .unwrap();
        assert_eq!(read.records[0].value, b"value");
    }
    for want in [0, 1, 2, 0, 1, 2, 0] {
        assert_eq!(
            stream.append(Route::RoundRobin, rec()).await.unwrap().0,
            want
        );
    }
    assert_eq!(
        stream.append(Route::Partition(2), rec()).await.unwrap().0,
        2
    );
    assert!(matches!(
        stream.append(Route::Partition(9), rec()).await,
        Err(StorageError::Validation(_))
    ));
    stream.close().unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn invalid_stream_names_and_zero_partitions_refuse_before_creating_paths() {
    let dir = tempfile::tempdir().unwrap();
    for name in [
        "UPPER",
        "with-dash",
        "_internal",
        "9first",
        "bad.name",
        "é",
        &"a".repeat(65),
    ] {
        let root = dir.path().join(name);
        assert!(
            matches!(open(&root, 3).await, Err(StorageError::Validation(_))),
            "{name}"
        );
        assert!(!root.exists());
    }
    let root = dir.path().join("orders");
    assert!(matches!(
        open(&root, 0).await,
        Err(StorageError::Validation(_))
    ));
    assert!(!root.exists());
}

#[tokio::test(flavor = "multi_thread")]
async fn container_and_partition_locks_exclude_competing_opens() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("orders");
    let stream = open(&root, 3).await.unwrap();
    assert!(matches!(open(&root, 3).await, Err(StorageError::Locked(_))));
    assert!(matches!(
        StreamLog::open(StreamConfig::new(root.join("partition_1"))).await,
        Err(StorageError::Locked(_))
    ));
    stream.close().unwrap();
    open(&root, 3).await.unwrap().close().unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn malformed_or_oversized_manifest_refuses_without_creating_partitions() {
    for bytes in [vec![0; 4097], vec![0; 24], vec![1; 3]] {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("orders");
        std::fs::create_dir(&root).unwrap();
        std::fs::write(root.join("STREAM"), &bytes).unwrap();
        assert!(matches!(
            open(&root, 3).await,
            Err(StorageError::Corruption(_))
        ));
        assert_eq!(std::fs::read(root.join("STREAM")).unwrap(), bytes);
        assert!(!root.join("partition_0").exists());
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn keyed_or_single_stream_data_directory_cannot_be_a_container() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("orders");
    StreamLog::open(StreamConfig::new(&root))
        .await
        .unwrap()
        .close()
        .unwrap();
    assert!(matches!(
        open(&root, 3).await,
        Err(StorageError::Validation(_))
    ));
    assert!(!root.join("STREAM").exists());
    assert!(!root.join("partition_0").exists());
}

#[tokio::test(flavor = "multi_thread")]
async fn published_manifest_cannot_silently_recreate_a_missing_partition() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("orders");
    open(&root, 3).await.unwrap().close().unwrap();
    std::fs::remove_dir_all(root.join("partition_1")).unwrap();
    assert!(matches!(
        open(&root, 3).await,
        Err(StorageError::Corruption(_))
    ));
    assert!(!root.join("partition_1").exists());
}

struct TraceVfs {
    events: Arc<Mutex<Vec<String>>>,
    synced_dirs: Arc<Mutex<Vec<std::path::PathBuf>>>,
    fail: Option<&'static str>,
}
struct TraceFile {
    file: Arc<dyn VfsFile>,
    events: Arc<Mutex<Vec<String>>>,
    manifest: bool,
    fail: Option<&'static str>,
}
impl VfsFile for TraceFile {
    fn write_at(&self, o: u64, b: &[u8]) -> std::io::Result<()> {
        self.file.write_at(o, b)
    }
    fn read_at(&self, o: u64, b: &mut [u8]) -> std::io::Result<usize> {
        self.file.read_at(o, b)
    }
    fn set_len(&self, l: u64) -> std::io::Result<()> {
        self.file.set_len(l)
    }
    fn len(&self) -> std::io::Result<u64> {
        self.file.len()
    }
    fn sync_data(&self) -> std::io::Result<()> {
        if self.manifest {
            self.events.lock().unwrap().push("manifest sync".into());
            if self.fail == Some("sync") {
                return Err(std::io::Error::other("injected manifest sync"));
            }
        }
        self.file.sync_data()
    }
}
impl Vfs for TraceVfs {
    fn open(&self, p: &Path, m: OpenMode) -> std::io::Result<Arc<dyn VfsFile>> {
        StdVfs.open(p, m)
    }
    fn create(&self, p: &Path) -> std::io::Result<Arc<dyn VfsFile>> {
        Ok(Arc::new(TraceFile {
            file: StdVfs.create(p)?,
            events: self.events.clone(),
            manifest: p.file_name().is_some_and(|n| n == "STREAM.tmp"),
            fail: self.fail,
        }))
    }
    fn rename(&self, f: &Path, t: &Path) -> std::io::Result<()> {
        if t.file_name().is_some_and(|n| n == "STREAM") {
            for p in 0..3 {
                assert!(t
                    .parent()
                    .unwrap()
                    .join(format!("partition_{p}/FORMAT"))
                    .exists());
            }
            self.events.lock().unwrap().push("manifest rename".into());
            if self.fail == Some("rename") {
                return Err(std::io::Error::other("injected manifest rename"));
            }
        }
        StdVfs.rename(f, t)
    }
    fn remove(&self, p: &Path) -> std::io::Result<()> {
        StdVfs.remove(p)
    }
    fn create_dir_all(&self, p: &Path) -> std::io::Result<()> {
        StdVfs.create_dir_all(p)
    }
    fn read_dir(&self, p: &Path) -> std::io::Result<Vec<std::path::PathBuf>> {
        StdVfs.read_dir(p)
    }
    fn exists(&self, p: &Path) -> std::io::Result<bool> {
        if self.fail == Some("preflight") {
            self.events
                .lock()
                .unwrap()
                .push("unexpected filesystem access".into());
            return Err(std::io::Error::other("provisioning must not begin"));
        }
        StdVfs.exists(p)
    }
    fn sync_dir(&self, p: &Path) -> std::io::Result<()> {
        self.synced_dirs.lock().unwrap().push(p.to_path_buf());
        if p.join("STREAM").exists() {
            self.events.lock().unwrap().push("root sync".into());
            if self.fail == Some("dir") {
                return Err(std::io::Error::other("injected manifest directory sync"));
            }
        }
        StdVfs.sync_dir(p)
    }
    fn lock_exclusive(&self, p: &Path) -> std::io::Result<Box<dyn LockGuard>> {
        StdVfs.lock_exclusive(p)
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn manifest_publication_orders_sync_rename_directory_sync_and_propagates_failures() {
    for fail in [None, Some("sync"), Some("rename"), Some("dir")] {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("orders");
        let events = Arc::new(Mutex::new(vec![]));
        let result = PartitionedStream::open_with_vfs(
            Arc::new(TraceVfs {
                events: events.clone(),
                synced_dirs: Arc::default(),
                fail,
            }),
            &root,
            3,
            StreamConfig::new(&root),
        )
        .await;
        let want = match fail {
            Some("sync") => vec!["manifest sync"],
            Some("rename") => vec!["manifest sync", "manifest rename"],
            _ => vec!["manifest sync", "manifest rename", "root sync"],
        };
        assert_eq!(*events.lock().unwrap(), want);
        if fail.is_some() {
            assert!(matches!(result, Err(StorageError::Internal(_))));
            // Failed creation releases every lock and supports recovery.
            open(&root, 3).await.unwrap().close().unwrap();
        } else {
            result.unwrap().close().unwrap();
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn durable_append_under_two_new_ancestors_syncs_every_directory_entry() {
    // Vfs::create_dir_all creates unsynced entries. A durable file below a new
    // directory survives power loss only when the directory's parent was synced.
    // Record every directory sync through the real open/append path and require
    // all three newly created directory entries to be durable before the ack.
    let dir = tempfile::tempdir().unwrap();
    let outer = dir.path().join("outer");
    let inner = outer.join("inner");
    let root = inner.join("orders");
    let synced_dirs = Arc::new(Mutex::new(vec![]));
    let cfg = StreamConfig::new(&root);
    assert_eq!(cfg.wal.sync_mode, prkdb_core::wal::SyncMode::Durable);
    let stream = PartitionedStream::open_with_vfs(
        Arc::new(TraceVfs {
            events: Arc::default(),
            synced_dirs: synced_dirs.clone(),
            fail: None,
        }),
        &root,
        3,
        cfg,
    )
    .await
    .unwrap();
    let (partition, ack) = stream.append(Route::Partition(0), rec()).await.unwrap();
    assert!(stream.partition(partition).unwrap().durable_end() > ack.last());
    let synced = synced_dirs.lock().unwrap().clone();
    stream.close().unwrap();
    for created in [&outer, &inner, &root] {
        assert!(
            synced.iter().any(|p| Some(p.as_path()) == created.parent()),
            "acknowledged Durable data under {} can be lost: parent {} was not synced; syncs: {synced:?}",
            created.display(), created.parent().unwrap().display()
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn raw_byte_routing_golden_vectors_survive_reopen() {
    // Literal vectors cover empty/binary keys and either side of an eight-byte block.
    // They were calculated from SeaHash's reference algorithm, independent of Hash.
    let vectors: &[(&[u8], [u32; 4])] = &[
        (b"", [2, 4, 9, 12]),
        (b"user-0", [1, 6, 6, 16]),
        (b"user-1", [1, 3, 2, 20]),
        (&[0, 255, 1], [0, 5, 0, 4]),
        (b"12345678", [0, 6, 4, 15]),
        (b"123456789", [2, 0, 9, 0]),
        (b"0123456789abcdef0123456789abcdef", [0, 1, 5, 24]),
    ];
    for (column, count) in [3, 7, 16, 31].into_iter().enumerate() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("orders");
        for _ in 0..2 {
            let stream = open(&root, count).await.unwrap();
            for &(key, expected) in vectors {
                let (partition, ack) = stream.append(Route::Key(key), rec()).await.unwrap();
                assert_eq!(partition, expected[column], "key {key:?}, count {count}");
                let batch = stream
                    .partition(partition)
                    .unwrap()
                    .read_from(StartAt::Offset(ack.first()), ReadLimits::default())
                    .await
                    .unwrap();
                assert_eq!(batch.records[0].value, b"value");
            }
            stream.close().unwrap();
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn legacy_routing_manifest_is_refused_before_partition_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("orders");
    let stream = open(&root, 1).await.unwrap();
    stream.append(Route::Partition(0), rec()).await.unwrap();
    stream.close().unwrap();
    let wal = root
        .join("partition_0")
        .join(prkdb_core::wal::segment::segment_file_name(1));
    let mut wal_bytes = std::fs::read(&wal).unwrap();
    wal_bytes.extend_from_slice(&[1, 2, 3]); // Would be repaired if partitions were opened.
    std::fs::write(&wal, &wal_bytes).unwrap();
    let path = root.join("STREAM");
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[8..12].copy_from_slice(&1u32.to_le_bytes());
    let crc_at = bytes.len() - 4;
    let crc = crc32fast::hash(&bytes[..crc_at]);
    bytes[crc_at..].copy_from_slice(&crc.to_le_bytes());
    std::fs::write(&path, &bytes).unwrap();
    let error = open(&root, 1)
        .await
        .err()
        .expect("legacy routing must not be silently changed");
    assert!(
        matches!(error, StorageError::UnsupportedFormat(_)),
        "{error}"
    );
    assert_eq!(std::fs::read(&path).unwrap(), bytes);
    assert_eq!(std::fs::read(&wal).unwrap(), wal_bytes);
}

#[tokio::test(flavor = "multi_thread")]
async fn crc_valid_future_manifest_is_unsupported_and_preserved() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("orders");
    open(&root, 1).await.unwrap().close().unwrap();
    let path = root.join("STREAM");
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[8..12].copy_from_slice(&99u32.to_le_bytes());
    let crc_at = bytes.len() - 4;
    let crc = crc32fast::hash(&bytes[..crc_at]);
    bytes[crc_at..].copy_from_slice(&crc.to_le_bytes());
    std::fs::write(&path, &bytes).unwrap();
    let error = open(&root, 1).await.err().unwrap();
    assert!(
        matches!(error, StorageError::UnsupportedFormat(_)),
        "{error}"
    );
    assert!(error.to_string().contains("newer PrkDB"), "{error}");
    assert_eq!(std::fs::read(&path).unwrap(), bytes);
    bytes[8] ^= 1; // Same version bytes without resealing remain corruption.
    std::fs::write(&path, &bytes).unwrap();
    assert!(matches!(
        open(&root, 1).await,
        Err(StorageError::Corruption(_))
    ));
    assert_eq!(std::fs::read(&path).unwrap(), bytes);
}

#[tokio::test(flavor = "multi_thread")]
async fn excessive_partition_counts_refuse_before_filesystem_access() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("orders");
    for count in [257, 65_537, u32::MAX] {
        let events = Arc::new(Mutex::new(vec![]));
        let error = PartitionedStream::open_with_vfs(
            Arc::new(TraceVfs {
                events: events.clone(),
                synced_dirs: Arc::default(),
                fail: Some("preflight"),
            }),
            &root,
            count,
            StreamConfig::new(&root),
        )
        .await
        .err()
        .unwrap();
        assert!(matches!(error, StorageError::Validation(_)), "{error}");
        assert!(error.to_string().contains("256"), "{error}");
        assert!(events.lock().unwrap().is_empty());
        assert!(!root.exists());
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn excessive_persisted_partition_count_reports_limit_without_modification() {
    for count in [257, u32::MAX] {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("orders");
        open(&root, 1).await.unwrap().close().unwrap();
        let path = root.join("STREAM");
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[12..16].copy_from_slice(&count.to_le_bytes());
        let crc_at = bytes.len() - 4;
        let crc = crc32fast::hash(&bytes[..crc_at]);
        bytes[crc_at..].copy_from_slice(&crc.to_le_bytes());
        std::fs::write(&path, &bytes).unwrap();
        let error = open(&root, 1).await.err().unwrap();
        assert!(matches!(error, StorageError::Validation(_)), "{error}");
        assert!(
            error.to_string().contains("supported maximum of 256"),
            "{error}"
        );
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
        assert!(!root.join("partition_1").exists());
    }
}
