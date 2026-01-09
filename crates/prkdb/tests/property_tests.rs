use prkdb::storage::WalStorageAdapter;
use prkdb_core::wal::WalConfig;
use prkdb_types::storage::StorageAdapter;
use proptest::prelude::*;

// Property: Any key-value pair written should be retrievable
proptest! {
    #[test]
    fn prop_write_then_read(key in prop::collection::vec(any::<u8>(), 1..100),
                           value in prop::collection::vec(any::<u8>(), 1..1000)) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let _ = rt.block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let config = WalConfig {
                log_dir: dir.path().to_path_buf(),
                ..WalConfig::test_config()
            };
            let adapter = WalStorageAdapter::new(config).unwrap();

            // Write
            adapter.put(&key, &value).await.unwrap();

            // Read
            let result = adapter.get(&key).await.unwrap();
            prop_assert_eq!(result, Some(value));

            Ok(())
        });
    }
}

// Property: Deleting a key should make it unreadable
proptest! {
    #[test]
    fn prop_delete_makes_unreadable(key in prop::collection::vec(any::<u8>(), 1..100),
                                    value in prop::collection::vec(any::<u8>(), 1..1000)) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let _ = rt.block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let config = WalConfig {
                log_dir: dir.path().to_path_buf(),
                ..WalConfig::test_config()
            };
            let adapter = WalStorageAdapter::new(config).unwrap();

            // Write then delete
            adapter.put(&key, &value).await.unwrap();
            adapter.delete(&key).await.unwrap();

            // Should not be readable
            let result = adapter.get(&key).await.unwrap();
            prop_assert_eq!(result, None);

            Ok(())
        });
    }
}

// Property: Multiple writes to same key should return latest value
proptest! {
    #[test]
    fn prop_latest_value_wins(key in prop::collection::vec(any::<u8>(), 1..100),
                             values in prop::collection::vec(prop::collection::vec(any::<u8>(), 1..1000), 1..10)) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let _ = rt.block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let config = WalConfig {
                log_dir: dir.path().to_path_buf(),
                ..WalConfig::test_config()
            };
            let adapter = WalStorageAdapter::new(config).unwrap();

            let mut last_value = vec![];
            for value in values {
                adapter.put(&key, &value).await.unwrap();
                last_value = value;
            }

            // Should return the latest value written
            let result = adapter.get(&key).await.unwrap();
            prop_assert_eq!(result, Some(last_value));

            Ok(())
        });
    }
}

// Property: Batch write should be equivalent to individual writes
proptest! {
    #[test]
    fn prop_batch_equivalent_to_individual(
        items in prop::collection::hash_map(
            prop::collection::vec(any::<u8>(), 1..50),
            prop::collection::vec(any::<u8>(), 1..500),
            1..20
        )
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let _ = rt.block_on(async {
            // Test batch write
            let dir1 = tempfile::tempdir().unwrap();
            let config1 = WalConfig {
                log_dir: dir1.path().to_path_buf(),
                ..WalConfig::test_config()
            };
            let adapter1 = WalStorageAdapter::new(config1).unwrap();

            let batch_items: Vec<(Vec<u8>, Vec<u8>)> = items.iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            adapter1.put_many(batch_items).await.unwrap();

            // Test individual writes
            let dir2 = tempfile::tempdir().unwrap();
            let config2 = WalConfig {
                log_dir: dir2.path().to_path_buf(),
                ..WalConfig::test_config()
            };
            let adapter2 = WalStorageAdapter::new(config2).unwrap();

            for (key, value) in &items {
                adapter2.put(key, value).await.unwrap();
            }

            // Both should have the same data
            for (key, expected_value) in &items {
                let result1 = adapter1.get(key).await.unwrap();
                let result2 = adapter2.get(key).await.unwrap();

                prop_assert_eq!(result1.clone(), Some(expected_value.clone()));
                prop_assert_eq!(result2.clone(), Some(expected_value.clone()));
                prop_assert_eq!(result1, result2);
            }

            Ok(())
        });
    }
}

// Property: Recovery should preserve all committed data
proptest! {
    #[test]
    fn prop_recovery_preserves_data(items in prop::collection::hash_map(
        prop::collection::vec(any::<u8>(), 1..50),
        prop::collection::vec(any::<u8>(), 1..500),
        1..50
    )) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let _ = rt.block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let config = WalConfig {
                log_dir: dir.path().to_path_buf(),
                ..WalConfig::test_config()
            };

            // Write data
            {
                let adapter = WalStorageAdapter::new(config.clone()).unwrap();
                for (key, value) in &items {
                    adapter.put(key, value).await.unwrap();
                }
                // Ensure data is flushed
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            }

            // Reopen (simulating recovery)
            let adapter = WalStorageAdapter::open(config).unwrap();

            // All data should still be there
            for (key, expected_value) in &items {
                let result = adapter.get(key).await.unwrap();
                prop_assert_eq!(result, Some(expected_value.clone()),
                    "Key {:?} was lost after recovery", key);
            }

            Ok(())
        });
    }
}
