//! The `StorageAdapter` surface of `WalStorageAdapter`, tested directly.
//!
//! # Why this file exists
//!
//! `wal_adapter.rs` entered mutation testing for the first time when the nightly sweep was
//! widened (PR #43). Run 31411280726 reported **71 survivors** across its 168 mutants —
//! roughly 42% — in the component whose worst historical defect lost every write on reopen.
//!
//! The gap was not subtle. `put`, `get`, and `delete` were covered by `property_tests.rs`.
//! Everything else on the trait — batch reads, prefix and range scans, batch delete, the
//! outbox — had no direct coverage at all. Whatever reached those methods reached them
//! through a wrapper, and asserted something else.
//!
//! Survivors this file targets, quoted from the run:
//!
//! ```text
//! delete_many  -> Ok(())                           a delete that deletes nothing
//! outbox_save  -> Ok(())                           a save that saves nothing
//! outbox_list  -> Ok(vec![("xyzzy", ..)])          entries a drainer would act on
//! scan_range   -> Ok(vec![(vec![0], vec![1])])     invented rows
//! get_many: replace match guard id == key with true
//! get_many: replace > with <, ==, >=
//! scan_prefix: replace match guard id.starts_with(prefix) with true
//! scan_prefix: delete match arm LogOperation::PutBatch / DeleteBatch
//! ```
//!
//! Note the shape of the worst ones. `delete_many -> Ok(())` and `outbox_save -> Ok(())`
//! are silent no-ops that report success. The `get_many` guard mutants are worse still:
//! they do not return *nothing*, they return the **wrong record for a key**, which a caller
//! cannot detect. A test that only checks "some rows came back" passes against every one of
//! them, which is why each assertion below pins the exact value for a specific key.

use prkdb::storage::WalStorageAdapter;
use prkdb_core::wal::WalConfig;
use prkdb_types::replication::Change;
use prkdb_types::storage::StorageAdapter;

fn adapter(dir: &std::path::Path) -> WalStorageAdapter {
    let config = WalConfig {
        log_dir: dir.to_path_buf(),
        ..WalConfig::test_config()
    };
    WalStorageAdapter::new(config).expect("open a WAL adapter")
}

/// Bound a whole test, so a mutant that stops the accumulator flushing fails fast.
///
/// `put_many` hands its records to the accumulator and awaits a oneshot that
/// `flush_accumulator_inner` fires. Replace that function with `()` and the signal never
/// arrives, so the await blocks forever. Mutation run 31497883297 reported it as
/// `TIMEOUT ... 300s test` rather than as caught.
///
/// A hang is detection, but it costs the full per-mutant budget and reads as
/// "inconclusive" rather than "the tests noticed". Bounding converts it to an assertion.
///
/// Applied per test rather than per call: this file has thirty write sites across fifteen
/// tests, and wrapping each one individually is thirty chances to miss one. The first
/// attempt did exactly that — it bounded two tests, and the suite still hung for ten
/// minutes on the others.
async fn bounded<F: std::future::Future<Output = ()>>(name: &str, f: F) {
    if tokio::time::timeout(std::time::Duration::from_secs(30), f)
        .await
        .is_err()
    {
        panic!(
            "{name} did not finish within 30s — most likely a write whose completion \
             signal never fired, which means the accumulator never published it"
        );
    }
}

/// `get_many` returns the right value for each key, and `None` for absent ones.
///
/// Distinct values per key are the point. `replace match guard id == key_clone with true`
/// makes every stored record match every requested key, so a test asserting only that
/// results came back cannot see it — the first record is returned for everything.
#[tokio::test(flavor = "multi_thread")]
async fn get_many_returns_the_right_value_per_key() {
    bounded("get_many_returns_the_right_value_per_key", async {
        let dir = tempfile::tempdir().unwrap();
        let a = adapter(dir.path());

        for (k, v) in [("k1", "v1"), ("k2", "v2"), ("k3", "v3")] {
            a.put(k.as_bytes(), v.as_bytes()).await.unwrap();
        }

        let keys = vec![b"k1".to_vec(), b"k2".to_vec(), b"k3".to_vec()];
        let got = a.get_many(keys.clone()).await.expect("get_many succeeds");

        assert_eq!(got.len(), 3, "one result per requested key");
        for (i, (key, expected)) in [("k1", "v1"), ("k2", "v2"), ("k3", "v3")]
            .iter()
            .enumerate()
        {
            assert_eq!(
                got[i].as_deref(),
                Some(expected.as_bytes()),
                "get_many returned the wrong value for {key}; a guard that matches every key \
             returns the first record for all of them"
            );
        }

        // An absent key is None, not the previous key's value.
        let mixed = a
            .get_many(vec![b"k2".to_vec(), b"absent".to_vec()])
            .await
            .unwrap();
        assert_eq!(mixed[0].as_deref(), Some(b"v2".as_slice()));
        assert_eq!(mixed[1], None, "an absent key must be None");
    })
    .await;
}
/// Values written through `put_many` are all readable, individually and in batch.
#[tokio::test(flavor = "multi_thread")]
async fn put_many_writes_every_pair() {
    bounded("put_many_writes_every_pair", async {
        let dir = tempfile::tempdir().unwrap();
        let a = adapter(dir.path());

        let pairs: Vec<(Vec<u8>, Vec<u8>)> = (0..6)
            .map(|i| {
                (
                    format!("b{i}").into_bytes(),
                    format!("value-{i}").into_bytes(),
                )
            })
            .collect();
        a.put_many(pairs.clone()).await.expect("put_many succeeds");

        for (k, v) in &pairs {
            assert_eq!(
                a.get(k).await.unwrap().as_deref(),
                Some(v.as_slice()),
                "put_many lost {}",
                String::from_utf8_lossy(k)
            );
        }

        // And through the batch path, which is where the guard mutants live.
        let keys: Vec<Vec<u8>> = pairs.iter().map(|(k, _)| k.clone()).collect();
        let got = a.get_many(keys).await.unwrap();
        for (i, (_, v)) in pairs.iter().enumerate() {
            assert_eq!(got[i].as_deref(), Some(v.as_slice()));
        }
    })
    .await;
}
/// `delete_many` must delete. Replacing its body with `Ok(())` survived the sweep.
#[tokio::test(flavor = "multi_thread")]
async fn delete_many_removes_every_key() {
    bounded("delete_many_removes_every_key", async {
        let dir = tempfile::tempdir().unwrap();
        let a = adapter(dir.path());

        for k in ["d1", "d2", "d3", "keep"] {
            a.put(k.as_bytes(), b"x").await.unwrap();
        }

        a.delete_many(vec![b"d1".to_vec(), b"d2".to_vec(), b"d3".to_vec()])
            .await
            .expect("delete_many succeeds");

        for k in ["d1", "d2", "d3"] {
            assert_eq!(
                a.get(k.as_bytes()).await.unwrap(),
                None,
                "{k} survived delete_many, which reported success"
            );
        }
        assert_eq!(
            a.get(b"keep").await.unwrap().as_deref(),
            Some(b"x".as_slice()),
            "delete_many removed a key it was not given"
        );
    })
    .await;
}
/// `scan_prefix` selects exactly the matching keys — including ones written in a batch.
///
/// The oracle is a plain filter over what was written, so a guard forced to `true`
/// (every key matches) and a deleted `PutBatch` arm (batched keys invisible) both show up.
#[tokio::test(flavor = "multi_thread")]
async fn scan_prefix_selects_exactly_the_matching_keys() {
    bounded("scan_prefix_selects_exactly_the_matching_keys", async {
        let dir = tempfile::tempdir().unwrap();
        let a = adapter(dir.path());

        a.put(b"user:1", b"alice").await.unwrap();
        a.put(b"user:2", b"bob").await.unwrap();
        a.put(b"order:1", b"widget").await.unwrap();
        // Through the batch path, so the PutBatch match arm is exercised.
        a.put_many(vec![
            (b"user:3".to_vec(), b"carol".to_vec()),
            (b"order:2".to_vec(), b"gadget".to_vec()),
        ])
        .await
        .unwrap();

        let mut got: Vec<(String, String)> = a
            .scan_prefix(b"user:")
            .await
            .expect("scan_prefix succeeds")
            .into_iter()
            .map(|(k, v)| {
                (
                    String::from_utf8_lossy(&k).into_owned(),
                    String::from_utf8_lossy(&v).into_owned(),
                )
            })
            .collect();
        got.sort();

        assert_eq!(
            got,
            vec![
                ("user:1".to_string(), "alice".to_string()),
                ("user:2".to_string(), "bob".to_string()),
                ("user:3".to_string(), "carol".to_string()),
            ],
            "scan_prefix must return every user: key including batched ones, and no order: key"
        );

        // A deleted key must not come back, which exercises the DeleteBatch arm.
        a.delete_many(vec![b"user:2".to_vec()]).await.unwrap();
        let after: Vec<Vec<u8>> = a
            .scan_prefix(b"user:")
            .await
            .unwrap()
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        assert!(
            !after.contains(&b"user:2".to_vec()),
            "a key deleted in a batch still appears in scan_prefix"
        );
        assert_eq!(after.len(), 2);
    })
    .await;
}
/// `scan_range` is half-open `[start, end)` and returns real stored rows.
///
/// Seven separate mutants replaced this body with invented vectors — `vec![(vec![0],
/// vec![1])]` and friends. Asserting the exact keys and values kills all of them, which
/// asserting only a count would not.
#[tokio::test(flavor = "multi_thread")]
async fn scan_range_is_half_open_and_returns_stored_rows() {
    bounded("scan_range_is_half_open_and_returns_stored_rows", async {
        let dir = tempfile::tempdir().unwrap();
        let a = adapter(dir.path());

        for (k, v) in [("a", "1"), ("b", "2"), ("c", "3"), ("d", "4")] {
            a.put(k.as_bytes(), v.as_bytes()).await.unwrap();
        }

        let mut got: Vec<(String, String)> = a
            .scan_range(b"b", b"d")
            .await
            .expect("scan_range succeeds")
            .into_iter()
            .map(|(k, v)| {
                (
                    String::from_utf8_lossy(&k).into_owned(),
                    String::from_utf8_lossy(&v).into_owned(),
                )
            })
            .collect();
        got.sort();

        assert_eq!(
            got,
            vec![
                ("b".to_string(), "2".to_string()),
                ("c".to_string(), "3".to_string()),
            ],
            "scan_range is [start, end): b and c, not a and not d"
        );

        // An empty range returns nothing rather than everything.
        assert!(
            a.scan_range(b"x", b"z").await.unwrap().is_empty(),
            "a range matching no key must return no rows"
        );
    })
    .await;
}
/// The outbox round-trips: what is saved is listed, and what is removed is gone.
///
/// `outbox_save -> Ok(())` and six variants of `outbox_list` returning invented entries
/// all survived. A caller draining the outbox would act on rows that were never saved.
#[tokio::test(flavor = "multi_thread")]
async fn the_outbox_round_trips() {
    bounded("the_outbox_round_trips", async {
        let dir = tempfile::tempdir().unwrap();
        let a = adapter(dir.path());

        assert!(
            a.outbox_list().await.unwrap().is_empty(),
            "a fresh outbox must be empty, not populated with invented entries"
        );

        a.outbox_save("evt-1", b"payload-1").await.unwrap();
        a.outbox_save("evt-2", b"payload-2").await.unwrap();

        let mut listed: Vec<(String, String)> = a
            .outbox_list()
            .await
            .expect("outbox_list succeeds")
            .into_iter()
            .map(|(id, p)| (id, String::from_utf8_lossy(&p).into_owned()))
            .collect();
        listed.sort();

        assert_eq!(
            listed,
            vec![
                ("evt-1".to_string(), "payload-1".to_string()),
                ("evt-2".to_string(), "payload-2".to_string()),
            ],
            "outbox_list must return what was saved, with the right payload per id"
        );

        a.outbox_remove("evt-1").await.unwrap();
        let after: Vec<String> = a
            .outbox_list()
            .await
            .unwrap()
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        assert_eq!(
            after,
            vec!["evt-2".to_string()],
            "outbox_remove must remove exactly the named entry"
        );
    })
    .await;
}
// ═══════════════════════════════════════════════════════════════════════════
// Reads that miss the cache
//
// `get_many` has three paths, and the tests above only reach the first:
//
//   1. every key hit the cache            -> returns immediately
//   2. more than 100 cache misses          -> one scan, building a map of latest values
//   3. otherwise                           -> a per-key WAL lookup
//
// The mutants that survived live in 2 and 3: `replace > with <`, `==`, `>=` chooses the
// wrong strategy, and `replace match guard id == key_clone with true` makes every stored
// record match every requested key.
//
// A warm adapter never reaches them. Reopening one does — the WAL still holds the data and
// the cache is empty, which is exactly the state after a restart. That the read-after-
// restart path was untested is the more interesting half of this: it is the path a crash
// puts you on.
// ═══════════════════════════════════════════════════════════════════════════

/// A small batch read after reopening, so the lookup goes to the WAL rather than the cache.
#[tokio::test(flavor = "multi_thread")]
async fn get_many_reads_from_the_wal_when_the_cache_is_cold() {
    bounded(
        "get_many_reads_from_the_wal_when_the_cache_is_cold",
        async {
            let dir = tempfile::tempdir().unwrap();

            {
                let a = adapter(dir.path());
                for (k, v) in [("c1", "one"), ("c2", "two"), ("c3", "three")] {
                    a.put(k.as_bytes(), v.as_bytes()).await.unwrap();
                }
                a.flush().await.unwrap();
            }

            // Fresh adapter over the same directory: WAL populated, cache empty.
            let a = adapter(dir.path());
            let got = a
                .get_many(vec![b"c1".to_vec(), b"c2".to_vec(), b"c3".to_vec()])
                .await
                .expect("get_many succeeds on a cold cache");

            assert_eq!(
                got.iter().map(|v| v.as_deref()).collect::<Vec<_>>(),
                vec![
                    Some(b"one".as_slice()),
                    Some(b"two".as_slice()),
                    Some(b"three".as_slice())
                ],
                "a cold read returned the wrong value for a key; a guard matching every record \
         returns one record's data for all of them"
            );
        },
    )
    .await;
}
/// More than 100 cold keys, which selects the scan-based strategy.
///
/// The threshold comparison itself is what is being pinned: `>` replaced by `<` sends a
/// large batch down the per-key path and a small one down the scan path. Both must return
/// the same answers, so this asserts values rather than merely that it did not error.
#[tokio::test(flavor = "multi_thread")]
async fn a_large_cold_batch_read_returns_the_right_values() {
    bounded("a_large_cold_batch_read_returns_the_right_values", async {
        let dir = tempfile::tempdir().unwrap();
        const N: usize = 150; // > 100, so the scan strategy is chosen

        {
            let a = adapter(dir.path());
            let pairs: Vec<(Vec<u8>, Vec<u8>)> = (0..N)
                .map(|i| {
                    (
                        format!("big{i:04}").into_bytes(),
                        format!("val{i:04}").into_bytes(),
                    )
                })
                .collect();
            a.put_many(pairs).await.unwrap();
            a.flush().await.unwrap();
        }

        let a = adapter(dir.path());
        let keys: Vec<Vec<u8>> = (0..N).map(|i| format!("big{i:04}").into_bytes()).collect();
        let got = a
            .get_many(keys)
            .await
            .expect("large cold get_many succeeds");

        assert_eq!(got.len(), N);
        for (i, value) in got.iter().enumerate() {
            assert_eq!(
                value.as_deref(),
                Some(format!("val{i:04}").as_bytes()),
                "wrong value at index {i} in a {N}-key cold batch read"
            );
        }

        // The boundary in the other direction: a small cold batch must agree.
        let a2 = adapter(dir.path());
        let small = a2
            .get_many(vec![b"big0000".to_vec(), b"big0149".to_vec()])
            .await
            .unwrap();
        assert_eq!(small[0].as_deref(), Some(b"val0000".as_slice()));
        assert_eq!(small[1].as_deref(), Some(b"val0149".as_slice()));
    })
    .await;
}
// ═══════════════════════════════════════════════════════════════════════════
// Raft log appends
//
// `append_raft_entry -> Ok(0)` / `Ok(1)` and `append_raft_entries_batch -> Ok(vec![])` /
// `Ok(vec![0])` / `Ok(vec![1])` all survived. These functions return the WAL offsets the
// entries were written at, and Raft uses those offsets as log indices — a constant means
// every entry claims the same index, which is a log that cannot be replayed or matched
// against a follower's.
//
// Nothing asserted the offsets were distinct, or that a batch returned one per entry.
// ═══════════════════════════════════════════════════════════════════════════

/// Each appended entry gets its own offset, and they advance.
#[tokio::test(flavor = "multi_thread")]
async fn raft_entry_offsets_are_distinct_and_advance() {
    bounded("raft_entry_offsets_are_distinct_and_advance", async {
        let dir = tempfile::tempdir().unwrap();
        let a = adapter(dir.path());

        let mut offsets = Vec::new();
        for i in 0..5u8 {
            offsets.push(
                a.append_raft_entry(&[i, i, i])
                    .await
                    .expect("appending a raft entry succeeds"),
            );
        }

        let mut sorted = offsets.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(
            sorted.len(),
            offsets.len(),
            "two raft entries were given the same offset ({offsets:?}); Raft uses these as log \
         indices, so duplicates are a log that cannot be matched against a follower's"
        );
        assert!(
            offsets.windows(2).all(|w| w[1] > w[0]),
            "raft offsets must advance, got {offsets:?}"
        );
    })
    .await;
}
/// A batch append returns one result per entry.
///
/// # What it does *not* guarantee, and why this test says so
///
/// The offsets are **not distinct**. Writes to the same collection are merged into a
/// single `PutBatch` WAL record, and every waiter merged into it receives that record's
/// offset — so four raft entries come back as `[1, 1, 1, 1]`. My first version of this test
/// asserted distinctness and failed.
///
/// That is not a live bug: the only caller, `raft/proposal_loop.rs:113`, matches on
/// `Ok(_offsets)` and discards them, taking the Raft log index from its own `senders`
/// instead. But it is a trap — the signature promises `Vec<u64>` of offsets, and a future
/// caller using them as identities would get silent duplicates.
///
/// Asserted here as the real contract rather than the desirable one, because a test that
/// asserts a guarantee the code does not make is the kind of green this repository has
/// spent a release removing. Tracked for a follow-up: either return distinct offsets or
/// change the signature to stop implying them.
#[tokio::test(flavor = "multi_thread")]
async fn a_raft_batch_returns_one_result_per_entry() {
    bounded("a_raft_batch_returns_one_result_per_entry", async {
        let dir = tempfile::tempdir().unwrap();
        let a = adapter(dir.path());

        let entries: Vec<Vec<u8>> = (0..4u8).map(|i| vec![i; 3]).collect();
        let offsets = a
            .append_raft_entries_batch(&entries)
            .await
            .expect("batch append succeeds");

        assert_eq!(
            offsets.len(),
            entries.len(),
            "a batch of {} returned {} results; a short or empty result loses entries the \
         caller believes are durable",
            entries.len(),
            offsets.len()
        );

        // An empty batch is legitimately empty rather than an error.
        assert!(a
            .append_raft_entries_batch(&[])
            .await
            .expect("an empty batch is not an error")
            .is_empty());
    })
    .await;
}
// ═══════════════════════════════════════════════════════════════════════════
// Compressed and batch record types
//
// `WalConfig::test_config()` sets `CompressionConfig::none()`, so every test in this file
// and in property_tests.rs writes uncompressed records. The WAL's compressed and
// batch-delete operations — `CompressedPutBatch`, `DeleteBatch`, `CompressedDeleteBatch` —
// were therefore never produced, and the match arms handling them never ran.
//
// Mutation run 31475126643 reported exactly that. Nine survivors in
// `flush_accumulator_inner` and eight in `get`, most of them `delete match arm
// LogOperation::CompressedPutBatch{..}` and its siblings, in both the index-publication
// path and the read path.
//
// Deleting one of those arms means records of that type stop updating the index, or stop
// being read back. A database that compresses its WAL — which is the default
// configuration, `CompressionConfig::default()` is LZ4 — would lose reads for every
// batched write. The tests never noticed because the tests never compressed anything.
//
// Since Task 2.8a there is one frame shape, a `Batch`, and one decode path for it; the
// batch codec compresses a frame whose ops clear `min_compress_bytes`. The tests below
// keep checking that compressed frames are written, replayed and read back correctly.
// ═══════════════════════════════════════════════════════════════════════════

/// The same round trip, with the WAL compressing.
///
/// Not a duplicate of the tests above: the frames it writes are compressed, and
/// decompression is its own step on both the replay and the read path.
fn compressing_adapter(dir: &std::path::Path) -> WalStorageAdapter {
    let config = WalConfig {
        log_dir: dir.to_path_buf(),
        // Default is LZ4, and min_compress_bytes decides whether a record is actually
        // compressed — so the payloads below are comfortably above any sane threshold.
        compression: prkdb_core::wal::compression::CompressionConfig::default(),
        ..WalConfig::test_config()
    };
    WalStorageAdapter::new(config).expect("open a compressing WAL adapter")
}

/// Batched writes survive compression, individually and in bulk, warm and cold.
#[tokio::test(flavor = "multi_thread")]
async fn compressed_batches_round_trip() {
    bounded("compressed_batches_round_trip", async {
        let dir = tempfile::tempdir().unwrap();

        // Payloads large enough to be worth compressing, and distinct enough that returning
        // the wrong one is visible.
        let pairs: Vec<(Vec<u8>, Vec<u8>)> = (0..8)
            .map(|i| {
                (
                    format!("z{i:03}").into_bytes(),
                    format!("{}-{i}", "payload".repeat(64)).into_bytes(),
                )
            })
            .collect();

        {
            let a = compressing_adapter(dir.path());
            a.put_batch(pairs.clone())
                .await
                .expect("compressed batch write");
            a.flush().await.unwrap();

            for (k, v) in &pairs {
                assert_eq!(
                    a.get(k).await.unwrap().as_deref(),
                    Some(v.as_slice()),
                    "compressed key {} unreadable while warm",
                    String::from_utf8_lossy(k)
                );
            }
        }

        // Cold: the index is rebuilt and reads go through the WAL, which is where the
        // CompressedPutBatch arms live.
        let a = compressing_adapter(dir.path());
        for (k, v) in &pairs {
            assert_eq!(
                a.get(k).await.unwrap().as_deref(),
                Some(v.as_slice()),
                "compressed key {} did not survive a reopen; a deleted CompressedPutBatch arm \
             loses every batched write on a database that compresses its WAL",
                String::from_utf8_lossy(k)
            );
        }

        let keys: Vec<Vec<u8>> = pairs.iter().map(|(k, _)| k.clone()).collect();
        let got = a.get_many(keys).await.unwrap();
        for (i, (_, v)) in pairs.iter().enumerate() {
            assert_eq!(
                got[i].as_deref(),
                Some(v.as_slice()),
                "batch read differs at {i}"
            );
        }

        // And the prefix scan, which has its own arm per record type.
        let scanned = a.scan_prefix(b"z").await.unwrap();
        assert_eq!(
            scanned.len(),
            pairs.len(),
            "scan_prefix missed compressed records"
        );
    })
    .await;
}
/// Batch deletes are applied and stay applied, compressed and not.
///
/// `DeleteBatch` and `CompressedDeleteBatch` each have their own arm in the
/// index-publication path. Deleting one leaves the key readable after it was removed —
/// a delete that reports success and does not delete, which is the shape this file
/// already found once in `delete_many`.
#[tokio::test(flavor = "multi_thread")]
async fn compressed_batch_deletes_are_applied() {
    bounded("compressed_batch_deletes_are_applied", async {
        let dir = tempfile::tempdir().unwrap();

        let pairs: Vec<(Vec<u8>, Vec<u8>)> = (0..6)
            .map(|i| {
                (
                    format!("d{i:03}").into_bytes(),
                    format!("{}-{i}", "body".repeat(64)).into_bytes(),
                )
            })
            .collect();

        {
            let a = compressing_adapter(dir.path());
            a.put_batch(pairs.clone())
                .await
                .expect("compressed batch write");
            a.delete_many(pairs.iter().take(4).map(|(k, _)| k.clone()).collect())
                .await
                .expect("compressed batch delete");
            a.flush().await.unwrap();

            for (k, _) in pairs.iter().take(4) {
                assert_eq!(
                    a.get(k).await.unwrap(),
                    None,
                    "{} survived a compressed batch delete",
                    String::from_utf8_lossy(k)
                );
            }
            for (k, v) in pairs.iter().skip(4) {
                assert_eq!(a.get(k).await.unwrap().as_deref(), Some(v.as_slice()));
            }
        }

        // The deletion must still hold after a reopen: an unapplied DeleteBatch arm
        // resurrects every key the caller was told had gone.
        let a = compressing_adapter(dir.path());
        for (k, _) in pairs.iter().take(4) {
            assert_eq!(
                a.get(k).await.unwrap(),
                None,
                "{} came back after a reopen; the delete was reported and not recorded",
                String::from_utf8_lossy(k)
            );
        }
        for (k, v) in pairs.iter().skip(4) {
            assert_eq!(a.get(k).await.unwrap().as_deref(), Some(v.as_slice()));
        }
    })
    .await;
}
/// `scan_range` reads compressed batches, without owning a match arm for them.
///
/// Worth stating why this test exists rather than a fix. `scan_range` does not match on
/// `LogOperation` at all: it walks the index and calls `get` per key, and `get` decompresses.
/// So it inherits the compressed paths instead of duplicating them, and adding arms here
/// would be adding code no record ever reaches.
///
/// That is a claim about behaviour, so it is asserted rather than asserted-in-a-comment. It
/// also pins the other half: the index must *contain* the batched keys, which it only does
/// because `flush_accumulator_inner` publishes `CompressedPutBatch` ids and
/// `rebuild_index_async` decompresses them back on reopen.
#[tokio::test(flavor = "multi_thread")]
async fn scan_range_reads_compressed_batches() {
    bounded("scan_range_reads_compressed_batches", async {
        let dir = tempfile::tempdir().unwrap();

        let pairs: Vec<(Vec<u8>, Vec<u8>)> = (0..8)
            .map(|i| {
                (
                    format!("r{i:03}").into_bytes(),
                    format!("{}-{i}", "range".repeat(64)).into_bytes(),
                )
            })
            .collect();

        {
            let a = compressing_adapter(dir.path());
            a.put_batch(pairs.clone()).await.unwrap();
            a.flush().await.unwrap();
        }

        // Cold: index rebuilt from compressed records, values read back through them.
        let a = compressing_adapter(dir.path());
        let got = a
            .scan_range(b"r002", b"r005")
            .await
            .expect("scan_range succeeds over compressed records");

        let expected: Vec<(Vec<u8>, Vec<u8>)> = pairs[2..5].to_vec();
        assert_eq!(
            got, expected,
            "scan_range is [start, end) over compressed records: r002..r004 with their own values"
        );
    })
    .await;
}
/// `get_changes_since` expands a compressed batch into one change per key.
///
/// It already had both compressed arms — this is the test that was missing, not the code.
/// Replication reads the WAL through this method, so a dropped `CompressedPutBatch` arm
/// means a follower silently never receives any batched write on a compressing primary.
/// Every existing caller of this method ran with `CompressionConfig::none()`.
#[tokio::test(flavor = "multi_thread")]
async fn get_changes_since_expands_compressed_batches() {
    bounded("get_changes_since_expands_compressed_batches", async {
        let dir = tempfile::tempdir().unwrap();
        let a = compressing_adapter(dir.path());

        let pairs: Vec<(Vec<u8>, Vec<u8>)> = (0..8)
            .map(|i| {
                (
                    format!("c{i:03}").into_bytes(),
                    format!("{}-{i}", "change".repeat(64)).into_bytes(),
                )
            })
            .collect();
        a.put_batch(pairs.clone()).await.unwrap();
        a.flush().await.unwrap();

        let changes = a
            .get_changes_since(0)
            .await
            .expect("get_changes_since succeeds");

        let mut puts: Vec<(Vec<u8>, Vec<u8>)> = changes
            .iter()
            .filter_map(|c| match c {
                Change::Put { key, value, .. } => Some((key.clone(), value.clone())),
                Change::Delete { .. } => None,
            })
            .collect();
        puts.sort();

        let mut expected = pairs.clone();
        expected.sort();
        assert_eq!(
            puts, expected,
            "a compressed batch must expand into one change per key with its own value; an \
         un-decompressed batch replicates as nothing at all"
        );
    })
    .await;
}
// ═══════════════════════════════════════════════════════════════════════════
// A compressed batch delete
//
// Since Task 2.8a every write is one `Batch` frame, compressed by the batch codec when
// the adapter's `CompressionConfig` says so and the encoded ops clear `min_compress_bytes`.
// These tests used to write compressed `LogRecord`s straight into the mmap WAL, because
// the adapter's own API could not produce every record variant; now it produces every
// frame shape there is, so they write through the adapter and check that compression
// really happened (the log is far smaller than the bytes written) before trusting it.
// ═══════════════════════════════════════════════════════════════════════════

/// Total size of the WAL segment files in `dir`.
fn log_bytes(dir: &std::path::Path) -> u64 {
    std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "wal"))
        .map(|p| std::fs::metadata(p).unwrap().len())
        .sum()
}

/// `scan_prefix` must honour a compressed batch delete, not just a plain one.
#[tokio::test(flavor = "multi_thread")]
async fn scan_prefix_honours_a_compressed_batch_delete() {
    bounded("scan_prefix_honours_a_compressed_batch_delete", async {
        let dir = tempfile::tempdir().unwrap();

        // 120 keys under the `w:` prefix; the first 100 are then deleted in one batch. Both
        // batches clear `min_compress_bytes` (256): the id list alone is 1.6 KB.
        let items: Vec<(Vec<u8>, Vec<u8>)> = (0..120)
            .map(|i| {
                (
                    format!("w:key-{i:04}").into_bytes(),
                    format!("{}-{i}", "stored".repeat(32)).into_bytes(),
                )
            })
            .collect();
        let deleted: Vec<Vec<u8>> = items.iter().take(100).map(|(k, _)| k.clone()).collect();
        let raw: u64 = items.iter().map(|(k, v)| (k.len() + v.len()) as u64).sum();

        {
            let a = compressing_adapter(dir.path());
            a.put_batch(items.clone())
                .await
                .expect("compressed put batch");
            a.delete_many(deleted.clone())
                .await
                .expect("compressed delete batch");
        }
        assert!(
            log_bytes(dir.path()) < raw / 2,
            "the log holds {} bytes for {raw} bytes written: the batch was not compressed, \
             so this test would not exercise the compressed path",
            log_bytes(dir.path())
        );

        let a = compressing_adapter(dir.path());
        let scanned: Vec<Vec<u8>> = a
            .scan_prefix(b"w:")
            .await
            .expect("scan_prefix succeeds")
            .into_iter()
            .map(|(k, _)| k)
            .collect();

        let survivors: Vec<Vec<u8>> = items.iter().skip(100).map(|(k, _)| k.clone()).collect();
        assert_eq!(
            scanned, survivors,
            "scan_prefix returned keys removed by a compressed batch delete; the tombstones \
             were not replayed and every deleted key reappears in the scan"
        );
    })
    .await;
}

/// `get_many` reads each key's own value out of a **compressed** batch.
///
/// Returning the *first other* key's value is a silent wrong answer rather than a visible
/// absence, which is why every key below has a distinct value and each one is pinned.
#[tokio::test(flavor = "multi_thread")]
async fn get_many_reads_each_key_out_of_a_compressed_batch() {
    bounded("get_many_reads_each_key_out_of_a_compressed_batch", async {
        let dir = tempfile::tempdir().expect("tempdir");

        let items: Vec<(Vec<u8>, Vec<u8>)> = (0..120)
            .map(|i| {
                (
                    format!("c:key-{i:04}").into_bytes(),
                    format!("{}-{i}", "stored".repeat(32)).into_bytes(),
                )
            })
            .collect();
        let raw: u64 = items.iter().map(|(k, v)| (k.len() + v.len()) as u64).sum();

        {
            let a = compressing_adapter(dir.path());
            a.put_batch(items.clone())
                .await
                .expect("write the compressed batch");
        }
        assert!(
            log_bytes(dir.path()) < raw / 2,
            "the batch was not compressed, so this test would prove nothing about the \
             compressed path"
        );

        // Cold: every read decodes the one compressed frame.
        let a = compressing_adapter(dir.path());
        let wanted: Vec<(Vec<u8>, Vec<u8>)> = items.iter().take(60).cloned().collect();
        let keys: Vec<Vec<u8>> = wanted.iter().map(|(k, _)| k.clone()).collect();
        let got = a.get_many(keys).await.expect("get_many succeeds");

        assert_eq!(got.len(), wanted.len(), "one answer per key");
        for (i, ((key, expected), actual)) in wanted.iter().zip(got.iter()).enumerate() {
            assert_eq!(
                actual.as_ref(),
                Some(expected),
                "key {} of the compressed batch read back wrong: {:?}",
                i,
                String::from_utf8_lossy(key)
            );
        }
    })
    .await;
}

/// A read reports the bytes it moved, not the product of two lengths.
///
/// `record_read` is called with `key.len() + value.len()` on the cache-hit path and on the
/// WAL-read path. The nightly sweep once replaced `+` with `*` in every such call and
/// nothing noticed, because no test had ever read the byte counter.
///
/// Each shape needs a cold cache, which is why this reopens the adapter between phases
/// rather than reading twice: `put` populates the cache.
///
/// Lengths are chosen so sum and product cannot coincide: 5 + 7 = 12, 5 * 7 = 35.
#[tokio::test(flavor = "multi_thread")]
async fn a_read_reports_the_bytes_it_moved() {
    bounded("a_read_reports_the_bytes_it_moved", async {
        const KEY: &[u8] = b"abcde"; // 5
        const VAL: &[u8] = b"1234567"; // 7 — sum 12, product 35
        let expected = (KEY.len() + VAL.len()) as u64;

        // ── a batch frame, then the cache-hit path ──────────────────────────────────
        let batch_dir = tempfile::tempdir().expect("tempdir");
        {
            let a = adapter(batch_dir.path());
            a.put_many(vec![
                (KEY.to_vec(), VAL.to_vec()),
                (b"other".to_vec(), b"payload!".to_vec()),
            ])
            .await
            .expect("write a batch");
            a.flush().await.expect("flush");
        }

        let a = adapter(batch_dir.path());
        let before = a.metrics().read_bytes_total;
        assert_eq!(
            a.get(KEY).await.expect("read"),
            Some(VAL.to_vec()),
            "the read must return the value, or the byte assertions are vacuous"
        );
        let after_wal = a.metrics().read_bytes_total;
        assert_eq!(
            after_wal - before,
            expected,
            "a batch-served read must report key + value bytes, not their product ({})",
            KEY.len() * VAL.len()
        );

        a.get(KEY).await.expect("cached read");
        assert_eq!(
            a.metrics().read_bytes_total - after_wal,
            expected,
            "a cache-served read must report the same bytes as a WAL-served one"
        );

        // ── a single-put frame and a compressed frame ───────────────────────────────
        for compressed in [false, true] {
            let dir = tempfile::tempdir().expect("tempdir");
            {
                let a = compressing_adapter(dir.path());
                if compressed {
                    // Padded so the batch clears `min_compress_bytes` and actually shrinks.
                    let mut items = vec![(KEY.to_vec(), VAL.to_vec())];
                    items.extend((0..120).map(|i| {
                        (
                            format!("pad-{i:04}").into_bytes(),
                            format!("{}-{i}", "padding".repeat(32)).into_bytes(),
                        )
                    }));
                    a.put_batch(items).await.expect("write the padded batch");
                } else {
                    a.put(KEY, VAL).await.expect("write one key");
                }
            }

            let a = compressing_adapter(dir.path());
            let before = a.metrics().read_bytes_total;
            assert_eq!(
                a.get(KEY).await.expect("read"),
                Some(VAL.to_vec()),
                "compressed={compressed}: the read must return the value"
            );
            assert_eq!(
                a.metrics().read_bytes_total - before,
                expected,
                "compressed={compressed}: must report key + value bytes, not their product"
            );
        }
    })
    .await;
}

/// The three plain accessors report the adapter's real state, not a default.
///
/// `max_offset`, `get_log_dir` and `save_checkpoint` were each replaceable with a constant
/// — `0`, an empty `PathBuf`, and a bare `Ok(())` — with the whole suite still green.
/// `get_log_dir` is what a caller uses to find the data directory, and `save_checkpoint`
/// makes everything acknowledged so far durable (Task 2.8a; Task 2.14 adds an index
/// snapshot). A `save_checkpoint` that reports success without syncing claims work it did
/// not do, which is why this runs in `Fast` mode with the periodic sync out of reach:
/// only the checkpoint can move the durable watermark.
#[tokio::test(flavor = "multi_thread")]
async fn the_adapter_accessors_report_real_state() {
    bounded("the_adapter_accessors_report_real_state", async {
        let dir = tempfile::tempdir().expect("tempdir");
        let a = WalStorageAdapter::new(WalConfig {
            log_dir: dir.path().to_path_buf(),
            sync_mode: prkdb_core::wal::SyncMode::Fast,
            sync_interval_ms: 3_600_000,
            ..WalConfig::test_config()
        })
        .expect("open a Fast-mode adapter");

        assert_eq!(
            a.get_log_dir(),
            dir.path(),
            "get_log_dir must name the directory this adapter was opened on; an empty \
             path sends a caller looking for the data somewhere else entirely"
        );

        let before = a.max_offset();
        a.put(b"offset-key", b"offset-value")
            .await
            .expect("write something to move the offset");
        let after = a.max_offset();
        assert!(
            after > before,
            "max_offset must advance as records are appended; it reported {after} after a \
             write with {before} before it"
        );
        assert!(
            a.durable_lsn() < after,
            "nothing may have synced the write yet, or the assertion below proves nothing"
        );

        a.save_checkpoint().expect("save a checkpoint");

        assert_eq!(
            a.durable_lsn(),
            a.max_offset(),
            "save_checkpoint returned Ok without making the acknowledged writes durable"
        );
    })
    .await;
}
