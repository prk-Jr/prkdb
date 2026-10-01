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
