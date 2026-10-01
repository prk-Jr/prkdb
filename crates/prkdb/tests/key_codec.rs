//! KEY-01: the key codec and the collection catalog (spec §7 2c, plan Task 2.12).

use prkdb::catalog::Catalog;
use prkdb::keys::{collection_prefix, decode_key, encode_key, CollectionId, SYSTEM_COLLECTION};
use prkdb::storage::{InMemoryAdapter, WalStorageAdapter};
use prkdb_core::wal::WalConfig;
use prkdb_types::collection::{default_persisted_name, Collection};
use std::sync::Arc;

#[test]
fn the_codec_is_the_documented_layout() {
    let k = encode_key(b"ns", CollectionId(7), b"id").unwrap();
    assert_eq!(k, [&[2u8][..], b"ns", &[0, 0, 0, 7], b"id"].concat());
    assert_eq!(
        decode_key(&k).unwrap(),
        (&b"ns"[..], CollectionId(7), &b"id"[..])
    );
    assert!(k.starts_with(&collection_prefix(b"ns", CollectionId(7))));
    assert!(!encode_key(b"ns", CollectionId(70), b"")
        .unwrap()
        .starts_with(&collection_prefix(b"ns", CollectionId(7))));
    assert!(
        encode_key(&[0u8; 256], CollectionId(1), b"x").is_err(),
        "namespace longer than 255 bytes"
    );
}

#[tokio::test]
async fn catalog_ids_are_stable_and_distinct() {
    let cat = Catalog::new(Arc::new(InMemoryAdapter::new()), Vec::new());
    let users = cat.id_for_name("users").await.unwrap();
    let orders = cat.id_for_name("orders").await.unwrap();
    assert_ne!(users, orders);
    assert_ne!(users, SYSTEM_COLLECTION);
    assert_eq!(cat.id_for_name("users").await.unwrap(), users);
}

#[tokio::test(flavor = "multi_thread")]
async fn catalog_ids_survive_restart() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = || WalConfig {
        log_dir: dir.path().to_path_buf(),
        ..WalConfig::test_config()
    };
    let (a, b) = {
        let cat = Catalog::new(Arc::new(WalStorageAdapter::new(cfg()).unwrap()), Vec::new());
        (
            cat.id_for_name("alpha").await.unwrap(),
            cat.id_for_name("beta").await.unwrap(),
        )
    };
    let cat = Catalog::new(
        Arc::new(WalStorageAdapter::open_async(cfg()).await.unwrap()),
        Vec::new(),
    );
    assert_eq!(cat.id_for_name("beta").await.unwrap(), b);
    assert_eq!(cat.id_for_name("alpha").await.unwrap(), a);
    let gamma = cat.id_for_name("gamma").await.unwrap();
    assert!(gamma != a && gamma != b, "an id was reused after restart");
}

/// Two catalogs over one storage (PrkDb's and IndexedStorage's) race on first use: every
/// name gets exactly one id, and no id is handed to two names.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_first_use_from_two_catalogs_allocates_each_name_once() {
    let dir = tempfile::tempdir().unwrap();
    let storage: Arc<dyn prkdb_types::storage::StorageAdapter> = Arc::new(
        WalStorageAdapter::new(WalConfig {
            log_dir: dir.path().to_path_buf(),
            ..WalConfig::test_config()
        })
        .unwrap(),
    );
    let (a, b) = (
        Arc::new(Catalog::new(storage.clone(), Vec::new())),
        Arc::new(Catalog::new(storage.clone(), Vec::new())),
    );
    let mut tasks = Vec::new();
    for i in 0..64u32 {
        let cat = if i % 2 == 0 { a.clone() } else { b.clone() };
        let name = format!("c{}", i % 16); // every name requested by both catalogs, concurrently
        tasks.push(tokio::spawn(async move {
            (name.clone(), cat.id_for_name(&name).await.unwrap())
        }));
    }
    let mut by_name = std::collections::BTreeMap::new();
    for t in tasks {
        let (name, id) = t.await.unwrap();
        assert_eq!(
            *by_name.entry(name.clone()).or_insert(id),
            id,
            "{name} got two ids"
        );
    }
    let ids: std::collections::BTreeSet<_> = by_name.values().copied().collect();
    assert_eq!(ids.len(), 16, "an id was handed to two names: {by_name:?}");
    let fresh = Catalog::new(storage, Vec::new());
    for (name, id) in &by_name {
        assert_eq!(
            fresh.id_for_name(name).await.unwrap(),
            *id,
            "{name} persisted differently"
        );
    }
}

#[test]
fn persisted_names_are_snake_case_and_pinnable() {
    #[derive(prkdb_macros::Collection, serde::Serialize, serde::Deserialize, Clone, Debug)]
    struct UserProfile {
        #[id]
        id: u64,
    }
    #[derive(prkdb_macros::Collection, serde::Serialize, serde::Deserialize, Clone, Debug)]
    #[collection(name = "people")]
    struct Person {
        #[id]
        id: u64,
    }
    assert_eq!(UserProfile::persisted_name(), "user_profile");
    assert_eq!(Person::persisted_name(), "people");
    assert!(Catalog::validate_name("Bad Name").is_err());
}

/// The derive and the default for manual impls agree, so switching a type between them
/// does not move its data.
#[test]
fn the_derive_and_the_manual_default_name_alike() {
    #[derive(prkdb_macros::Collection, serde::Serialize, serde::Deserialize, Clone, Debug)]
    struct HTTPServer2Log {
        #[id]
        id: u64,
    }
    #[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
    struct ManualItem {
        id: u64,
    }
    impl Collection for ManualItem {
        type Id = u64;
        fn id(&self) -> &u64 {
            &self.id
        }
    }
    assert_eq!(
        HTTPServer2Log::persisted_name(),
        default_persisted_name(std::any::type_name::<HTTPServer2Log>())
    );
    assert_eq!(ManualItem::persisted_name(), "manual_item");
    assert_eq!(
        default_persisted_name("a::b::Wrapper<c::Inner>"),
        "wrapper",
        "generics are stripped"
    );
}

// ── Order-preserving ids (review M1) ─────────────────────────────────────────

mod ordering {
    use prkdb::keys::{encode_id, encode_record_key, CollectionId};
    use proptest::prelude::*;

    /// Key byte order equals id order, for every pair, within one collection's keys.
    fn same_order<T: serde::Serialize + Ord + std::fmt::Debug>(a: &T, b: &T) {
        let (ka, kb) = (encode_id(a).unwrap(), encode_id(b).unwrap());
        assert_eq!(ka.cmp(&kb), a.cmp(b), "{a:?} vs {b:?}");
        let ns = b"tenant";
        let (ra, rb) = (
            encode_record_key(ns, CollectionId(3), a).unwrap(),
            encode_record_key(ns, CollectionId(3), b).unwrap(),
        );
        assert_eq!(ra.cmp(&rb), a.cmp(b), "record keys of {a:?} vs {b:?}");
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(512))]

        #[test]
        fn u64_ids_keep_their_order(a: u64, b: u64) { same_order(&a, &b); }

        #[test]
        fn i64_ids_keep_their_order(a: i64, b: i64) { same_order(&a, &b); }

        #[test]
        fn u32_and_i32_ids_keep_their_order(a: u32, b: u32, c: i32, d: i32) {
            same_order(&a, &b);
            same_order(&c, &d);
        }

        #[test]
        fn string_ids_keep_their_order(a in ".{0,40}", b in ".{0,40}") { same_order(&a, &b); }

        /// Strings sharing a long prefix, around the 8-byte group boundary.
        #[test]
        fn strings_with_a_shared_prefix_keep_their_order(
            p in "[a-z]{0,17}", a in "[\\x00-\\x7f]{0,10}", b in "[\\x00-\\x7f]{0,10}"
        ) {
            same_order(&format!("{p}{a}"), &format!("{p}{b}"));
        }

        #[test]
        fn byte_ids_keep_their_order(a in proptest::collection::vec(any::<u8>(), 0..24),
                                     b in proptest::collection::vec(any::<u8>(), 0..24)) {
            same_order(&a, &b);
        }

        #[test]
        fn tuple_ids_keep_their_order(a: (u32, String), b: (u32, String)) { same_order(&a, &b); }

        #[test]
        fn string_first_tuples_keep_their_order(a: (String, i64), b: (String, i64)) {
            same_order(&a, &b);
        }
    }
}

#[derive(
    prkdb_macros::Collection, serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq,
)]
struct RangeItem {
    #[id]
    id: u64,
}

/// Review M1 regression: with bincode's varint ids, 251 encoded as `[251, 251, 0]` and
/// sorted after 300 (`[251, 44, 1]`), and 256 (`[251, 0, 1]`) before 251, so a 100..300
/// range scan dropped 251..=255 and returned the rest out of order. Order-preserving ids
/// make it exactly 100..=299, in order.
#[tokio::test(flavor = "multi_thread")]
async fn a_range_scan_returns_exactly_the_id_range_in_order() {
    let dir = tempfile::tempdir().unwrap();
    let db = prkdb::PrkDb::builder()
        .with_storage(
            WalStorageAdapter::new(WalConfig {
                log_dir: dir.path().to_path_buf(),
                ..WalConfig::test_config()
            })
            .unwrap(),
        )
        .register_collection::<RangeItem>()
        .build()
        .unwrap();
    let items = db.collection::<RangeItem>();
    for id in (50u64..350).rev() {
        items.put(RangeItem { id }).await.unwrap();
    }
    let got: Vec<u64> = items
        .scan_range_by_id_bytes(&100, &300)
        .await
        .unwrap()
        .into_iter()
        .map(|r| r.id)
        .collect();
    assert_eq!(got, (100u64..300).collect::<Vec<_>>());
}

// ── Namespaced change streams (review M3) ────────────────────────────────────

#[derive(
    prkdb_macros::Collection, serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq,
)]
struct NsItem {
    #[id]
    id: u64,
}

/// Bytes `FetchSegment` streams for `collection`, through the gRPC service itself.
async fn fetch_segment_bytes(db: Arc<prkdb::PrkDb>, collection: &str) -> usize {
    use futures::StreamExt;
    use prkdb::raft::rpc::prk_db_service_server::PrkDbService;
    use prkdb::raft::rpc::FetchSegmentRequest;

    let svc = prkdb::raft::PrkDbGrpcService::new(db, String::new());
    let mut stream = svc
        .fetch_segment(tonic::Request::new(FetchSegmentRequest {
            collection: collection.to_string(),
            ..Default::default()
        }))
        .await
        .expect("FetchSegment starts")
        .into_inner();
    let mut bytes = 0;
    while let Some(chunk) = stream.next().await {
        bytes += chunk.expect("FetchSegment chunk").data.len();
    }
    bytes
}

/// Review M3: a `with_namespace` database's collection change stream is its own, not
/// empty. The adapter's namespace-less `changes_in_collection` cannot see it; the
/// database's, which `FetchSegment` serves, does.
#[tokio::test(flavor = "multi_thread")]
async fn a_namespaced_collection_has_a_change_stream() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(
        prkdb::PrkDb::builder()
            .with_data_dir(dir.path())
            .with_namespace("tenant")
            .register_collection::<NsItem>()
            .build()
            .unwrap(),
    );
    for id in [1, 2] {
        db.collection::<NsItem>().put(NsItem { id }).await.unwrap();
    }

    assert_eq!(
        db.changes_in_collection("ns_item", 0).await.unwrap().len(),
        2
    );
    assert!(db
        .changes_in_collection("missing", 0)
        .await
        .unwrap()
        .is_empty());
    assert!(
        db.storage()
            .changes_in_collection("ns_item", 0)
            .await
            .unwrap()
            .is_empty(),
        "the adapter's routing API is the empty namespace"
    );
    assert!(fetch_segment_bytes(db, "ns_item").await > 0);
}

// ── One type per persisted name (review H1) ──────────────────────────────────

mod one_type_per_name {
    use prkdb::indexed_storage::IndexedStorage;
    use prkdb::storage::{InMemoryAdapter, WalStorageAdapter};
    use prkdb_core::wal::WalConfig;
    use prkdb_types::collection::Collection;
    use prkdb_types::error::StorageError;
    use std::sync::Arc;

    #[derive(prkdb_macros::Collection, serde::Serialize, serde::Deserialize, Clone, Debug)]
    #[collection(name = "shared")]
    struct First {
        #[id]
        id: u64,
    }

    #[derive(prkdb_macros::Collection, serde::Serialize, serde::Deserialize, Clone, Debug)]
    #[collection(name = "shared")]
    struct Second {
        #[id]
        id: u64,
    }

    /// A generic manual impl: the default name strips generics, so every instantiation
    /// resolves "wrapper".
    #[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
    struct Wrapper<T> {
        id: u64,
        inner: T,
    }

    impl<T> Collection for Wrapper<T>
    where
        T: serde::Serialize
            + serde::de::DeserializeOwned
            + Clone
            + Send
            + Sync
            + std::fmt::Debug
            + 'static,
    {
        type Id = u64;
        fn id(&self) -> &u64 {
            &self.id
        }
    }

    impl<T> prkdb_types::index::Indexed for Wrapper<T>
    where
        T: serde::Serialize
            + serde::de::DeserializeOwned
            + Clone
            + Send
            + Sync
            + std::fmt::Debug
            + 'static,
    {
        fn indexes() -> &'static [prkdb_types::index::IndexDef] {
            &[]
        }
        fn index_values(&self) -> Vec<(&'static str, Vec<u8>)> {
            Vec::new()
        }
    }

    fn refused(err: StorageError, a: &str, b: &str) {
        let text = err.to_string();
        assert!(matches!(err, StorageError::Validation(_)), "{text}");
        assert!(
            text.contains(a) && text.contains(b) && text.contains("#[collection(name"),
            "the refusal names both types and the fix: {text}"
        );
    }

    #[tokio::test]
    async fn a_second_type_for_a_pinned_name_is_refused() {
        let db = IndexedStorage::new(Arc::new(InMemoryAdapter::new()));
        db.insert(&First { id: 1 }).await.unwrap();
        let err = db.insert(&Second { id: 1 }).await.unwrap_err();
        refused(err, "First", "Second");
        // Reads are typed resolutions too: they must not see the other type's records.
        refused(db.get::<Second>(&1).await.unwrap_err(), "First", "Second");
        assert_eq!(db.get::<First>(&1).await.unwrap().unwrap().id, 1);
    }

    #[tokio::test]
    async fn generic_instantiations_sharing_a_default_name_are_refused() {
        assert_eq!(Wrapper::<u8>::persisted_name(), "wrapper");
        let db = IndexedStorage::new(Arc::new(InMemoryAdapter::new()));
        db.insert(&Wrapper { id: 1, inner: 1u8 }).await.unwrap();
        let err = db
            .insert(&Wrapper {
                id: 1,
                inner: "a".to_string(),
            })
            .await
            .unwrap_err();
        refused(err, "Wrapper<u8>", "Wrapper<alloc::string::String>");
    }

    /// Two catalogs over one storage (a `PrkDb`'s and an `IndexedStorage`'s) share the
    /// registry, so the check holds across APIs.
    #[tokio::test]
    async fn the_check_spans_every_catalog_over_one_storage() {
        let storage = InMemoryAdapter::new();
        let db = prkdb::PrkDb::builder()
            .with_storage(storage.clone())
            .build()
            .unwrap();
        db.collection::<First>().put(First { id: 1 }).await.unwrap();
        let indexed = IndexedStorage::new(Arc::new(storage));
        refused(
            indexed.insert(&Second { id: 2 }).await.unwrap_err(),
            "First",
            "Second",
        );
    }

    #[tokio::test]
    async fn separate_storages_may_reuse_a_name() {
        let a = IndexedStorage::new(Arc::new(InMemoryAdapter::new()));
        let b = IndexedStorage::new(Arc::new(InMemoryAdapter::new()));
        a.insert(&First { id: 1 }).await.unwrap();
        b.insert(&Second { id: 1 }).await.unwrap();
    }

    /// A shared, clonable log buffer for capturing `tracing` output.
    #[derive(Clone, Default)]
    struct Logs(Arc<std::sync::Mutex<Vec<u8>>>);

    impl std::io::Write for Logs {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// After a restart the in-process registry is empty, so a different type reopening a
    /// name is compared with the type name recorded at allocation, and only logged: a
    /// type moved to another module keeps its data.
    #[tokio::test]
    async fn a_type_mismatch_after_reopen_is_logged_not_refused() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = || WalConfig {
            log_dir: dir.path().to_path_buf(),
            ..WalConfig::test_config()
        };
        {
            let db = IndexedStorage::new(Arc::new(WalStorageAdapter::new(cfg()).unwrap()));
            db.insert(&First { id: 7 }).await.unwrap();
            db.inner().flush().await.unwrap();
        }

        let logs = Logs::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer({
                let logs = logs.clone();
                move || logs.clone()
            })
            .with_ansi(false)
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);

        let storage = Arc::new(WalStorageAdapter::new(cfg()).unwrap());
        let catalog = prkdb::catalog::Catalog::new(storage.clone(), Vec::new());
        assert!(catalog
            .recorded_type("shared")
            .await
            .unwrap()
            .unwrap()
            .ends_with("one_type_per_name::First"));
        let db = IndexedStorage::new(storage);
        assert_eq!(db.get::<Second>(&7).await.unwrap().unwrap().id, 7);

        let text = String::from_utf8(logs.0.lock().unwrap().clone()).unwrap();
        assert!(
            text.contains("WARN") && text.contains("First") && text.contains("Second"),
            "expected a warning naming both types, got: {text}"
        );
    }
}

// ── Name-based (CLI / HTTP) and typed APIs see one collection (review H2) ────

mod by_name_and_by_type {
    use prkdb::indexed_storage::IndexedStorage;
    use prkdb::storage::WalStorageAdapter;
    use prkdb_core::wal::WalConfig;
    use prkdb_types::error::StorageError;
    use prkdb_types::replication::Change;
    use prkdb_types::storage::StorageAdapter;
    use std::sync::Arc;

    /// One WAL shared by a `PrkDb` (the server's name-based API) and an
    /// `IndexedStorage` (a typed API), as one data directory is in a deployment.
    struct Shared(Arc<WalStorageAdapter>);

    #[async_trait::async_trait]
    impl StorageAdapter for Shared {
        async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
            self.0.get(key).await
        }
        async fn put(&self, key: &[u8], value: &[u8]) -> Result<(), StorageError> {
            self.0.put(key, value).await
        }
        async fn delete(&self, key: &[u8]) -> Result<(), StorageError> {
            self.0.delete(key).await
        }
        async fn scan_prefix(&self, p: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StorageError> {
            self.0.scan_prefix(p).await
        }
        async fn count_prefix(&self, p: &[u8]) -> Result<usize, StorageError> {
            self.0.count_prefix(p).await
        }
        async fn outbox_list(&self) -> Result<Vec<(String, Vec<u8>)>, StorageError> {
            self.0.outbox_list().await
        }
        async fn get_changes_since(&self, offset: u64) -> Result<Vec<Change>, StorageError> {
            self.0.get_changes_since(offset).await
        }
        fn allocation_lock(&self) -> Option<Arc<tokio::sync::Mutex<()>>> {
            self.0.allocation_lock()
        }
    }

    #[derive(prkdb_macros::Collection, serde::Serialize, serde::Deserialize, Clone, Debug)]
    #[collection(name = "people")]
    struct Person {
        #[id]
        id: String,
        name: String,
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_record_written_by_name_is_read_by_type_and_vice_versa() {
        let dir = tempfile::tempdir().unwrap();
        let wal = Arc::new(
            WalStorageAdapter::new(WalConfig {
                log_dir: dir.path().to_path_buf(),
                ..WalConfig::test_config()
            })
            .unwrap(),
        );
        let db = Arc::new(
            prkdb::PrkDb::builder()
                .with_storage(Shared(wal.clone()))
                .build()
                .unwrap(),
        );
        let typed = IndexedStorage::new(Arc::new(Shared(wal)));

        // By name, as `PUT /collections/people/data` and `prkdb collection put` do.
        let ada = serde_json::json!({"id": "1", "name": "Ada"});
        db.put_collection_record("people", "1", &serde_json::to_vec(&ada).unwrap())
            .await
            .unwrap();
        assert_eq!(
            typed
                .get::<Person>(&"1".to_string())
                .await
                .unwrap()
                .unwrap()
                .name,
            "Ada"
        );

        // By type, read back by name.
        typed
            .insert(&Person {
                id: "2".into(),
                name: "Grace".into(),
            })
            .await
            .unwrap();
        let grace: serde_json::Value = serde_json::from_slice(
            &db.get_collection_record("people", "2")
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(grace["name"], "Grace");
        let hints: Vec<_> = db
            .scan_collection_records("people")
            .await
            .unwrap()
            .into_iter()
            .map(|r| r.id_hint.unwrap())
            .collect();
        assert_eq!(hints, ["1", "2"]);

        // Stats, the listing and the change stream all see both.
        assert_eq!(db.get_collection_stats("people").await.unwrap().0, 2);
        assert!(db
            .collection_names()
            .await
            .unwrap()
            .contains(&"people".to_string()));
        assert_eq!(typed.count::<Person>().await.unwrap(), 2);
        assert_eq!(
            db.changes_in_collection("people", 0).await.unwrap().len(),
            2
        );
        assert!(super::fetch_segment_bytes(db.clone(), "people").await > 0);

        // Deleting by name removes the typed record.
        db.delete_collection_record("people", "1").await.unwrap();
        assert!(typed
            .get::<Person>(&"1".to_string())
            .await
            .unwrap()
            .is_none());
        // A name never written has nothing, and nothing is allocated for it.
        assert!(db
            .scan_collection_records("ghosts")
            .await
            .unwrap()
            .is_empty());
        assert!(!db
            .collection_names()
            .await
            .unwrap()
            .contains(&"ghosts".to_string()));
    }
}

/// Review LOW: a namespace longer than the codec's one-byte length is refused when the
/// database is built, not on its first write.
#[test]
fn a_namespace_longer_than_255_bytes_is_refused_at_build() {
    let err = prkdb::PrkDb::builder()
        .with_namespace([b'n'; 256])
        .build()
        .err()
        .expect("must refuse");
    assert!(err.to_string().contains("255"), "{err}");
    assert!(prkdb::PrkDb::builder()
        .with_namespace([b'n'; 255])
        .build()
        .is_ok());
}
