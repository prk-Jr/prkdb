//! SCH-02: the file-backed registry fails closed on a damaged registry, writes
//! atomically, and never hands out the same version twice.

use prkdb_schema::{
    CompatibilityMode, FileSchemaStorage, Schema, SchemaError, SchemaRegistry, SchemaStorage,
};
use prost::Message;
use prost_types::FileDescriptorProto;
use std::sync::Arc;

/// A minimal descriptor that passes `validate_descriptor` (the plan's `vec![10, 0]`
/// predates SCH-01's descriptor validation and is now rejected).
fn users_proto() -> Vec<u8> {
    FileDescriptorProto {
        name: Some("users.proto".into()),
        ..Default::default()
    }
    .encode_to_vec()
}

async fn registered(root: &std::path::Path) {
    let registry = SchemaRegistry::new(Arc::new(FileSchemaStorage::new(root.into())));
    registry
        .register("users", users_proto(), CompatibilityMode::Backward, None)
        .await
        .unwrap();
}

#[tokio::test]
async fn a_missing_descriptor_fails_the_load() {
    let root = tempfile::tempdir().unwrap();
    registered(root.path()).await;
    std::fs::remove_file(root.path().join("descriptors/users/v1.binpb")).unwrap();
    let mut reopened = FileSchemaStorage::new(root.path().into());
    let err = reopened
        .load()
        .await
        .expect_err("a missing descriptor must not load as empty");
    assert!(err.to_string().contains("v1.binpb"), "{err}");
}

#[tokio::test]
async fn a_corrupt_descriptor_fails_the_load() {
    let root = tempfile::tempdir().unwrap();
    registered(root.path()).await;
    let p = root.path().join("descriptors/users/v1.binpb");
    let mut bytes = std::fs::read(&p).unwrap();
    bytes[0] ^= 0xFF;
    std::fs::write(&p, bytes).unwrap();
    let mut reopened = FileSchemaStorage::new(root.path().into());
    assert!(reopened.load().await.is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_registrations_never_reuse_a_version() {
    let root = tempfile::tempdir().unwrap();
    let registry = Arc::new(SchemaRegistry::new(Arc::new(FileSchemaStorage::new(
        root.path().into(),
    ))));
    let tasks: Vec<_> = (0..16)
        .map(|_| {
            let r = registry.clone();
            tokio::spawn(async move {
                r.register("users", users_proto(), CompatibilityMode::Backward, None)
                    .await
            })
        })
        .collect();
    let mut versions = Vec::new();
    for t in tasks {
        versions.push(t.await.unwrap().unwrap().version);
    }
    versions.sort();
    assert_eq!(versions, (1..=16).collect::<Vec<u32>>());
    let mut reopened = FileSchemaStorage::new(root.path().into());
    reopened.load().await.unwrap();
    assert_eq!(
        reopened.get_latest("users").await.unwrap().unwrap().version,
        16
    );
}

#[tokio::test]
async fn a_leftover_temp_index_is_ignored() {
    let root = tempfile::tempdir().unwrap();
    registered(root.path()).await;
    let index_tmp = root.path().join("schemas.json.tmp");
    let descriptor_tmp = root.path().join("descriptors/users/v1.binpb.tmp");
    std::fs::write(&index_tmp, b"{ half written").unwrap();
    std::fs::write(&descriptor_tmp, b"half").unwrap();
    // The name the current writer uses: `{name}.{pid}.{n}.tmp`.
    let unique_tmp = root.path().join("schemas.json.4242.7.tmp");
    std::fs::write(&unique_tmp, b"[").unwrap();
    let mut reopened = FileSchemaStorage::new(root.path().into());
    reopened.load().await.unwrap();
    assert!(reopened.get("users", 1).await.unwrap().is_some());
    assert!(!index_tmp.exists(), "stale index temp file must be removed");
    assert!(
        !unique_tmp.exists(),
        "stale unique temp file must be removed"
    );
    assert!(
        !descriptor_tmp.exists(),
        "stale descriptor temp file must be removed"
    );
}

/// Poll `fut` exactly once, then drop it: a request cancelled mid-flight.
async fn poll_once_then_drop<F: std::future::Future>(fut: F) {
    let mut fut = std::pin::pin!(fut);
    tokio::select! {
        biased;
        _ = &mut fut => panic!("the operation was expected to still be in flight"),
        _ = std::future::ready(()) => {}
    }
}

fn read_index(root: &std::path::Path) -> Vec<serde_json::Value> {
    let raw = std::fs::read(root.join("schemas.json")).unwrap();
    serde_json::from_slice(&raw).expect("schemas.json must parse")
}

#[tokio::test]
async fn a_cancelled_registration_still_completes_before_the_next_one() {
    let root = tempfile::tempdir().unwrap();
    let registry = SchemaRegistry::new(Arc::new(FileSchemaStorage::new(root.path().into())));

    poll_once_then_drop(registry.register(
        "users",
        users_proto(),
        CompatibilityMode::Backward,
        None,
    ))
    .await;
    let second = registry
        .register("users", users_proto(), CompatibilityMode::Backward, None)
        .await
        .unwrap();
    assert_eq!(second.version, 2, "the cancelled registration owns v1");

    assert_eq!(read_index(root.path()).len(), 2);
    let mut reopened = FileSchemaStorage::new(root.path().into());
    reopened.load().await.unwrap();
    assert!(reopened.get("users", 1).await.unwrap().is_some());
    assert!(reopened.get("users", 2).await.unwrap().is_some());
}

#[tokio::test]
async fn a_cancelled_put_still_completes_before_the_next_one() {
    let root = tempfile::tempdir().unwrap();
    let storage = FileSchemaStorage::new(root.path().into());

    poll_once_then_drop(storage.put(&schema("users", 1))).await;
    storage.put(&schema("users", 2)).await.unwrap();

    assert_eq!(read_index(root.path()).len(), 2);
    let mut reopened = FileSchemaStorage::new(root.path().into());
    reopened.load().await.unwrap();
    assert!(reopened.get("users", 1).await.unwrap().is_some());
    assert!(reopened.get("users", 2).await.unwrap().is_some());
}

fn schema(collection: &str, version: u32) -> Schema {
    Schema {
        schema_id: version,
        collection: collection.to_string(),
        version,
        descriptor: users_proto(),
        compatibility: CompatibilityMode::Backward,
        is_breaking: false,
        migration_id: None,
        created_at: 0,
        descriptor_crc32: None,
    }
}

#[tokio::test]
async fn put_refuses_to_overwrite_an_existing_version() {
    let root = tempfile::tempdir().unwrap();
    let storage = FileSchemaStorage::new(root.path().into());
    storage.put(&schema("users", 1)).await.unwrap();
    let err = storage.put(&schema("users", 1)).await.unwrap_err();
    assert!(
        matches!(err, SchemaError::VersionConflict { ref collection, version: 1 } if collection == "users"),
        "{err:?}"
    );
}

#[tokio::test]
async fn a_failed_index_write_leaves_the_version_unallocated() {
    let root = tempfile::tempdir().unwrap();
    let storage = FileSchemaStorage::new(root.path().into());
    storage.put(&schema("users", 1)).await.unwrap();

    // A directory where schemas.json should be: the descriptor write succeeds,
    // the rename of the new index over it fails.
    let index = root.path().join("schemas.json");
    std::fs::remove_file(&index).unwrap();
    std::fs::create_dir(&index).unwrap();

    storage.put(&schema("users", 2)).await.unwrap_err();
    assert!(storage.get("users", 2).await.unwrap().is_none());
    assert_eq!(storage.next_version("users").await.unwrap(), 2);
    assert_eq!(storage.list().await.unwrap()[0].latest_version, 1);
}

#[tokio::test]
async fn an_index_written_before_checksums_still_loads() {
    let root = tempfile::tempdir().unwrap();
    registered(root.path()).await;

    // Rewrite the index as the pre-SCH-02 code wrote it: no descriptor_crc32.
    let mut entries = read_index(root.path());
    for entry in &mut entries {
        entry.as_object_mut().unwrap().remove("descriptor_crc32");
    }
    std::fs::write(
        root.path().join("schemas.json"),
        serde_json::to_vec(&entries).unwrap(),
    )
    .unwrap();

    let mut reopened = FileSchemaStorage::new(root.path().into());
    reopened.load().await.unwrap();
    let loaded = reopened.get("users", 1).await.unwrap().unwrap();
    assert_eq!(loaded.descriptor, users_proto());
    assert_eq!(loaded.descriptor_crc32, None);

    // The existence check still applies to such entries.
    std::fs::remove_file(root.path().join("descriptors/users/v1.binpb")).unwrap();
    let mut reopened = FileSchemaStorage::new(root.path().into());
    assert!(reopened.load().await.is_err());
}

#[tokio::test]
async fn register_returns_the_descriptor_checksum() {
    let root = tempfile::tempdir().unwrap();
    let storage = Arc::new(FileSchemaStorage::new(root.path().into()));
    let registry = SchemaRegistry::new(storage.clone());
    let registered = registry
        .register("users", users_proto(), CompatibilityMode::Backward, None)
        .await
        .unwrap();
    let stored = storage.get("users", 1).await.unwrap().unwrap();
    assert!(registered.descriptor_crc32.is_some());
    assert_eq!(registered.descriptor_crc32, stored.descriptor_crc32);
}

#[tokio::test]
async fn write_errors_do_not_reveal_the_storage_path() {
    let root = tempfile::tempdir().unwrap();
    // A file where the descriptors directory should be makes the write fail.
    std::fs::write(root.path().join("descriptors"), b"not a directory").unwrap();
    let registry = SchemaRegistry::new(Arc::new(FileSchemaStorage::new(root.path().into())));
    let err = registry
        .register("users", users_proto(), CompatibilityMode::Backward, None)
        .await
        .unwrap_err();
    let root_str = root.path().display().to_string();
    assert!(!err.to_string().contains(&root_str), "{err}");
}
