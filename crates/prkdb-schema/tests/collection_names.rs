use prkdb_schema::{CompatibilityMode, FileSchemaStorage, SchemaError, SchemaRegistry};
use prost::Message;
use prost_types::FileDescriptorProto;
use std::sync::Arc;

fn valid_proto() -> Vec<u8> {
    FileDescriptorProto {
        name: Some("user.proto".into()),
        ..Default::default()
    }
    .encode_to_vec()
}

#[tokio::test]
async fn sch01_traversal_names_are_rejected_before_any_write() {
    let root = tempfile::tempdir().unwrap();
    let base = root.path().join("registry");
    let registry = SchemaRegistry::new(Arc::new(FileSchemaStorage::new(base.clone())));
    for bad in [
        "../../escaped",
        "..",
        ".",
        "a/b",
        "a\\b",
        "/abs",
        "",
        ".hidden",
        "nul\0byte",
        &"x".repeat(129),
    ] {
        let err = registry
            .register(bad, valid_proto(), CompatibilityMode::Backward, None)
            .await
            .unwrap_err();
        assert!(
            matches!(err, SchemaError::InvalidCollectionName(_)),
            "{bad:?} gave {err:?}"
        );
    }
    assert!(!root.path().join("escaped").exists());
    assert!(!base.exists(), "nothing may be written for rejected names");
}

#[tokio::test]
async fn sch01_malformed_descriptor_is_rejected_before_any_write() {
    let root = tempfile::tempdir().unwrap();
    let base = root.path().join("registry");
    let registry = SchemaRegistry::new(Arc::new(FileSchemaStorage::new(base.clone())));
    let err = registry
        .register("users", vec![1, 2, 3], CompatibilityMode::Backward, None)
        .await
        .unwrap_err();
    assert!(matches!(err, SchemaError::InvalidDescriptor(_)), "{err:?}");
    assert!(!base.exists());
}

#[tokio::test]
async fn sch01_valid_names_still_register() {
    let root = tempfile::tempdir().unwrap();
    let registry = SchemaRegistry::new(Arc::new(FileSchemaStorage::new(root.path().into())));
    for good in ["users", "user_events", "v2.orders", "a-b"] {
        registry
            .register(good, valid_proto(), CompatibilityMode::Backward, None)
            .await
            .unwrap();
    }
}
