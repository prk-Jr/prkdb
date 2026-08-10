//! Property-based tests for the storage layer.
//!
//! # These could not fail
//!
//! Every property here was written as
//!
//! ```rust,ignore
//! let _ = rt.block_on(async {
//!     ...
//!     prop_assert_eq!(result, Some(value));
//!     Ok(())
//! });
//! ```
//!
//! `prop_assert_eq!` reports a failure by *returning* `Err(TestCaseError)`. `let _ =`
//! discarded it, the closure's `Err` never reached proptest, and the test body then ended
//! normally — so all five properties passed unconditionally. Verified by replacing one
//! assertion with `prop_assert_eq!(result, Some(vec![0xDE, 0xAD, 0xBE, 0xEF]))`, which is
//! false for every generated input: it still passed.
//!
//! The results are now propagated with `?`. Each property below was re-checked by
//! falsifying it and observing a failure.

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
        rt.block_on(async {
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
        })?;
    }
}

// Property: Deleting a key should make it unreadable
proptest! {
    #[test]
    fn prop_delete_makes_unreadable(key in prop::collection::vec(any::<u8>(), 1..100),
                                    value in prop::collection::vec(any::<u8>(), 1..1000)) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
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
        })?;
    }
}

// Property: Multiple writes to same key should return latest value
proptest! {
    #[test]
    fn prop_latest_value_wins(key in prop::collection::vec(any::<u8>(), 1..100),
                             values in prop::collection::vec(prop::collection::vec(any::<u8>(), 1..1000), 1..10)) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
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
        })?;
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
        rt.block_on(async {
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
        })?;
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
        rt.block_on(async {
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
        })?;
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// ROUTING PROPERTIES
//
// `CollectionPartitionedAdapter` splits a `collection:id` key across one WAL per
// collection, then reassembles results. Every routing bug found so far came from a
// hand-picked example missing a case:
//
//   - `scan_prefix` dropped every collection when a prefix named one partially, because
//     each test used a prefix containing a colon and took the other branch
//   - `scan_range` narrowed to a single collection when the bounds named two, because
//     every test ranged inside one collection
//
// Both were found by mutation testing rather than by the tests themselves. Generated
// inputs cover the shapes nobody thinks to write down.
// ═══════════════════════════════════════════════════════════════════════════

use prkdb::storage::CollectionPartitionedAdapter;

/// Collection names and ids that survive a round trip through `collection:id` keys.
/// Colons are excluded from both: the format is ambiguous with them, which is a property
/// of the key format rather than of the routing.
fn collection_name() -> impl Strategy<Value = String> {
    "[a-z][a-z0-9_]{0,7}".prop_map(|s| s.to_string())
}

fn record_id() -> impl Strategy<Value = String> {
    "[a-zA-Z0-9_-]{1,10}".prop_map(|s| s.to_string())
}

/// A range bound: sometimes a full `collection:id` key, sometimes a bare collection
/// prefix. Both shapes are needed — the colon is what decides whether `scan_range`
/// narrows to a single collection, so a generator that never emits one leaves that
/// branch untested.
fn bound() -> impl Strategy<Value = String> {
    prop_oneof![
        (collection_name(), record_id()).prop_map(|(c, i)| format!("{c}:{i}")),
        collection_name(),
        collection_name().prop_map(|c| format!("{c}:")),
    ]
}

fn adapter_at(dir: &std::path::Path) -> CollectionPartitionedAdapter {
    let config = WalConfig {
        log_dir: dir.to_path_buf(),
        ..WalConfig::test_config()
    };
    CollectionPartitionedAdapter::new(config).unwrap()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]

    /// Everything written is readable, whatever the collection/id split.
    #[test]
    fn prop_routing_round_trips(
        entries in prop::collection::vec((collection_name(), record_id(), 1u8..=255), 1..12)
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let adapter = adapter_at(dir.path());

            // Later writes to the same key win, so build the expected map the same way.
            let mut expected = std::collections::HashMap::new();
            for (coll, id, byte) in &entries {
                let key = format!("{coll}:{id}").into_bytes();
                adapter.put(&key, &[*byte]).await.unwrap();
                expected.insert(key, vec![*byte]);
            }

            for (key, value) in &expected {
                let got = adapter.get(key).await.unwrap();
                prop_assert_eq!(
                    got.as_ref(),
                    Some(value),
                    "key {} did not survive routing",
                    String::from_utf8_lossy(key)
                );
            }
            Ok(())
        })?;
    }

    /// `scan_prefix` returns exactly the keys that start with the prefix — no more, and
    /// no fewer. Stated against a plain filter over what was written, so the oracle is
    /// independent of the routing being tested.
    #[test]
    fn prop_scan_prefix_matches_a_plain_filter(
        entries in prop::collection::vec((collection_name(), record_id(), 1u8..=255), 1..12),
        prefix in "[a-z][a-z0-9_]{0,4}"
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let adapter = adapter_at(dir.path());

            let mut written = std::collections::HashMap::new();
            for (coll, id, byte) in &entries {
                let key = format!("{coll}:{id}").into_bytes();
                adapter.put(&key, &[*byte]).await.unwrap();
                written.insert(key, vec![*byte]);
            }

            let mut expected: Vec<Vec<u8>> = written
                .keys()
                .filter(|k| k.starts_with(prefix.as_bytes()))
                .cloned()
                .collect();
            expected.sort();

            let mut got: Vec<Vec<u8>> = adapter
                .scan_prefix(prefix.as_bytes())
                .await
                .unwrap()
                .into_iter()
                .map(|(k, _)| k)
                .collect();
            got.sort();

            prop_assert_eq!(got, expected, "scan_prefix disagreed with a plain filter for prefix {:?}", prefix);
            Ok(())
        })?;
    }

    /// `scan_range` is half-open `[start, end)` over the whole key space, across
    /// collections. The oracle is again a plain filter, so a routing shortcut that drops
    /// a collection is visible.
    ///
    /// # Why the bounds must sometimes contain a colon
    ///
    /// `scan_range` narrows to one collection only when *both* bounds name the same one,
    /// which it decides by looking for a colon. The first version of this property drew
    /// bounds from `[a-z][a-z0-9_]{0,6}` — never a colon — so the narrowing branch was
    /// never entered and the property missed a mutation that forces the guard `true`,
    /// which drops an entire collection from a cross-collection range.
    ///
    /// Generated inputs only cover the shapes the generator can produce. `bound()` now
    /// emits full `collection:id` keys as well as bare prefixes.
    #[test]
    fn prop_scan_range_is_half_open_across_collections(
        entries in prop::collection::vec((collection_name(), record_id(), 1u8..=255), 1..12),
        a in bound(),
        b in bound()
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let adapter = adapter_at(dir.path());

            let mut written = std::collections::HashMap::new();
            for (coll, id, byte) in &entries {
                let key = format!("{coll}:{id}").into_bytes();
                adapter.put(&key, &[*byte]).await.unwrap();
                written.insert(key, vec![*byte]);
            }

            // Order the bounds so the range is never inverted; an inverted range is a
            // separate question from whether a valid one is answered correctly.
            let (start, end) = if a <= b { (a.clone(), b.clone()) } else { (b.clone(), a.clone()) };

            let mut expected: Vec<Vec<u8>> = written
                .keys()
                .filter(|k| k.as_slice() >= start.as_bytes() && k.as_slice() < end.as_bytes())
                .cloned()
                .collect();
            expected.sort();

            let mut got: Vec<Vec<u8>> = adapter
                .scan_range(start.as_bytes(), end.as_bytes())
                .await
                .unwrap()
                .into_iter()
                .map(|(k, _)| k)
                .collect();
            got.sort();

            prop_assert_eq!(got, expected, "scan_range disagreed with a plain filter over [{:?}, {:?})", start, end);
            Ok(())
        })?;
    }
}
