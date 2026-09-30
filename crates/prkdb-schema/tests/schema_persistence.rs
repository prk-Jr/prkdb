//! SCH-02: the file-backed registry fails closed on a damaged registry, writes
//! atomically, and never hands out the same version twice.

use prkdb_schema::{CompatibilityMode, FileSchemaStorage, SchemaRegistry, SchemaStorage};
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
    std::fs::write(root.path().join("schemas.json.tmp"), b"{ half written").unwrap();
    let mut reopened = FileSchemaStorage::new(root.path().into());
    reopened.load().await.unwrap();
    assert!(reopened.get("users", 1).await.unwrap().is_some());
}
