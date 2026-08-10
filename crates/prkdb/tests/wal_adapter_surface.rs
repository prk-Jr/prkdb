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
use prkdb_types::storage::StorageAdapter;

fn adapter(dir: &std::path::Path) -> WalStorageAdapter {
    let config = WalConfig {
        log_dir: dir.to_path_buf(),
        ..WalConfig::test_config()
    };
    WalStorageAdapter::new(config).expect("open a WAL adapter")
}

/// `get_many` returns the right value for each key, and `None` for absent ones.
///
/// Distinct values per key are the point. `replace match guard id == key_clone with true`
/// makes every stored record match every requested key, so a test asserting only that
/// results came back cannot see it — the first record is returned for everything.
#[tokio::test(flavor = "multi_thread")]
async fn get_many_returns_the_right_value_per_key() {
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
}

/// Values written through `put_many` are all readable, individually and in batch.
#[tokio::test(flavor = "multi_thread")]
async fn put_many_writes_every_pair() {
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
}

/// `delete_many` must delete. Replacing its body with `Ok(())` survived the sweep.
#[tokio::test(flavor = "multi_thread")]
async fn delete_many_removes_every_key() {
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
}

/// `scan_prefix` selects exactly the matching keys — including ones written in a batch.
///
/// The oracle is a plain filter over what was written, so a guard forced to `true`
/// (every key matches) and a deleted `PutBatch` arm (batched keys invisible) both show up.
#[tokio::test(flavor = "multi_thread")]
async fn scan_prefix_selects_exactly_the_matching_keys() {
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
}

/// `scan_range` is half-open `[start, end)` and returns real stored rows.
///
/// Seven separate mutants replaced this body with invented vectors — `vec![(vec![0],
/// vec![1])]` and friends. Asserting the exact keys and values kills all of them, which
/// asserting only a count would not.
#[tokio::test(flavor = "multi_thread")]
async fn scan_range_is_half_open_and_returns_stored_rows() {
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
}

/// The outbox round-trips: what is saved is listed, and what is removed is gone.
///
/// `outbox_save -> Ok(())` and six variants of `outbox_list` returning invented entries
/// all survived. A caller draining the outbox would act on rows that were never saved.
#[tokio::test(flavor = "multi_thread")]
async fn the_outbox_round_trips() {
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
}

/// More than 100 cold keys, which selects the scan-based strategy.
///
/// The threshold comparison itself is what is being pinned: `>` replaced by `<` sends a
/// large batch down the per-key path and a small one down the scan path. Both must return
/// the same answers, so this asserts values rather than merely that it did not error.
#[tokio::test(flavor = "multi_thread")]
async fn a_large_cold_batch_read_returns_the_right_values() {
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
}
