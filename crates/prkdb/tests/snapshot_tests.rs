use prkdb::storage::snapshot::CompressionType;
use prkdb::storage::snapshot::SnapshotReader;
use prkdb::PrkDb;
use tempfile::tempdir;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_snapshot_backup_restore() {
    // Setup directories
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("db_primary");
    let snap_path = dir.path().join("snapshot.bin");
    let restore_path = dir.path().join("db_restored");

    // 1. Initialize Primary DB and write data
    {
        // Use default builder if available or manual construction
        // Assuming PrkDb has a robust builder (check doc/code)
        // PrkDb::builder uses Builder which defaults to InMemory? No, we need WAL.
        // WalStorageAdapter::new is usually used.
        // But PrkDb::new_multi_raft is complex.
        // Let's use simple WalStorageAdapter via config if possible, or PrkDb::builder().
        // PrkDb::builder() returns Builder. Builder has .with_storage(adapter).
        // Let's construct WalStorageAdapter manually.

        use prkdb::storage::wal_adapter::WalStorageAdapter;
        use prkdb_core::wal::WalConfig;

        let config = WalConfig {
            log_dir: db_path.clone(),
            segment_bytes: 1024 * 1024,
            ..WalConfig::default()
        };
        let adapter = WalStorageAdapter::new(config).unwrap();

        let db = PrkDb::builder().with_storage(adapter).build().unwrap();

        // Write data
        for i in 0..100 {
            let key = format!("key{:03}", i).into_bytes();
            let val = format!("val{:03}", i).into_bytes();
            db.put(&key, &val).await.unwrap();
        }

        // Take snapshot
        let offset = db
            .take_snapshot(&snap_path, CompressionType::Gzip)
            .await
            .unwrap();
        println!("Snapshot taken at offset: {}", offset);
    } // Drop db

    // 2. Verify Snapshot File Exists
    assert!(snap_path.exists());
    let metadata = std::fs::metadata(&snap_path).unwrap();
    assert!(metadata.len() > 0);

    // 3. Restore to New DB
    {
        // Initialize Restore DB
        let config = prkdb_core::wal::WalConfig {
            log_dir: restore_path.clone(),
            segment_bytes: 1024 * 1024,
            ..Default::default()
        };
        let adapter = prkdb::storage::wal_adapter::WalStorageAdapter::new(config).unwrap();
        let db_restored = PrkDb::builder().with_storage(adapter).build().unwrap();

        // Open reader
        let mut reader = SnapshotReader::open(&snap_path).expect("Failed to open snapshot");
        println!(
            "Snapshot Header: Version={}, Entries={}",
            reader.header.version, reader.header.index_entries
        );

        assert_eq!(reader.header.index_entries, 100);

        let mut count = 0;
        while let Some((k, v)) = reader.next_entry().expect("Read error") {
            db_restored.put(&k, &v).await.expect("Restore put failed");
            count += 1;
        }
        assert_eq!(count, 100);

        // Verify data content
        for i in 0..100 {
            let key = format!("key{:03}", i).into_bytes();
            let expected_val = format!("val{:03}", i).into_bytes();

            let val = db_restored.get(&key).await.unwrap();
            assert_eq!(
                val,
                Some(expected_val),
                "Mismatch for key {}",
                String::from_utf8_lossy(&key)
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_snapshot_corruption() {
    use std::io::Write;
    let dir = tempdir().unwrap();
    let snap_path = dir.path().join("corrupt.snap");

    // Create a valid snapshot first
    {
        let header = prkdb::storage::snapshot::SnapshotHeader::new(100, 1, CompressionType::None);
        // Note: SnapshotWriter is not pub in prkdb::storage::snapshot directly?
        // Logic check: snapshot.rs has `pub struct SnapshotWriter`.
        // prkdb re-exports it?
        // Let's check prkdb/src/lib.rs: `pub mod snapshot;` (step 3662) so it should be checking `prkdb::storage::snapshot::SnapshotWriter`.
        // Actually `snapshot.rs` has `pub struct SnapshotWriter`.
        // But in `prkdb/src/storage/mod.rs` it is `pub mod snapshot`.
        // So `prkdb::storage::snapshot::SnapshotWriter` should work if `SnapshotWriter` is pub.

        // Wait, step 3779 replaced line 6 with `pub use ...`.
        // But `SnapshotWriter` struct definition is further down.
        // Let's assume it is reachable.

        let path = snap_path.clone();
        // We can use the lower level API if accessible, or just use PrkDb to generate it.
        // Using PrkDb is safer/easier as it's the public API.

        // Let's use PrkDb to create it.
        use prkdb::storage::wal_adapter::WalStorageAdapter;
        use prkdb_core::wal::WalConfig;

        let db_path = dir.path().join("db_temp");
        let config = WalConfig {
            log_dir: db_path,
            segment_bytes: 1024 * 1024,
            ..WalConfig::default()
        };
        let adapter = WalStorageAdapter::new(config).unwrap();
        let db = PrkDb::builder().with_storage(adapter).build().unwrap();

        db.put(b"key", b"val").await.unwrap();
        db.take_snapshot(&path, CompressionType::None)
            .await
            .unwrap();
    }

    // Corrupt it by truncating
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(&snap_path)
        .unwrap();
    file.set_len(10).unwrap(); // Truncate to arguably valid length but corrupt content

    // Try to open using Reader (which we imported)
    let result = SnapshotReader::open(&snap_path);

    // It should either fail to open (deser error) or fail to read next entry
    if let Ok(mut reader) = result {
        // If header read ok, then entries should fail
        let entry_res = reader.next_entry();
        assert!(entry_res.is_err(), "Should detect corruption in entries");
    } else {
        // Warning: it might succeed if 10 bytes is enough for header?
        // Header is bincode encoded. 10 bytes might be too small or garbage.
        // So IsErr is good.
    }
}
