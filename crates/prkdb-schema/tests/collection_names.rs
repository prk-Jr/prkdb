use prkdb_schema::{
    CompatibilityMode, FileSchemaStorage, Schema, SchemaError, SchemaRegistry, SchemaStorage,
};
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
    for bad in [vec![1, 2, 3], vec![]] {
        let err = registry
            .register("users", bad.clone(), CompatibilityMode::Backward, None)
            .await
            .unwrap_err();
        assert!(
            matches!(err, SchemaError::InvalidDescriptor(_)),
            "{bad:?} gave {err:?}"
        );
    }
    assert!(!base.exists());
}

#[tokio::test]
async fn sch01_case_insensitive_collision_is_rejected_before_any_write() {
    let root = tempfile::tempdir().unwrap();
    let base = root.path().join("registry");
    let registry = SchemaRegistry::new(Arc::new(FileSchemaStorage::new(base.clone())));

    registry
        .register("users", valid_proto(), CompatibilityMode::Backward, None)
        .await
        .unwrap();

    let err = registry
        .register("Users", valid_proto(), CompatibilityMode::Backward, None)
        .await
        .unwrap_err();
    assert!(
        matches!(err, SchemaError::CollectionNameConflict { .. }),
        "{err:?}"
    );
    // Note: `descriptors/users` and `descriptors/Users` are the *same* path on
    // case-insensitive filesystems, so we can't assert the "Users" path is
    // absent — that's precisely the bug. Instead assert the rejected register
    // did not advance the version (i.e. nothing was written for it).
    assert_eq!(registry.get("users", None).await.unwrap().version, 1);

    // Re-registering the original (exact-case) name still works.
    let v2 = registry
        .register("users", valid_proto(), CompatibilityMode::Backward, None)
        .await
        .unwrap();
    assert_eq!(v2.version, 2);
}

#[tokio::test]
async fn sch01_round_trip_preserves_distinct_collections_after_reload() {
    let root = tempfile::tempdir().unwrap();
    let base = root.path().join("registry");

    {
        let registry = SchemaRegistry::new(Arc::new(FileSchemaStorage::new(base.clone())));
        registry
            .register("users", valid_proto(), CompatibilityMode::Backward, None)
            .await
            .unwrap();
        registry
            .register("orders", valid_proto(), CompatibilityMode::Backward, None)
            .await
            .unwrap();
    }

    let mut storage = FileSchemaStorage::new(base.clone());
    storage.load().await.unwrap();

    let users = storage.get_latest("users").await.unwrap();
    assert!(users.is_some());
    assert_eq!(users.unwrap().descriptor, valid_proto());

    let orders = storage.get_latest("orders").await.unwrap();
    assert!(orders.is_some());
    assert_eq!(orders.unwrap().descriptor, valid_proto());
}

#[tokio::test]
async fn sch01_storage_level_put_rejects_unsafe_names_before_any_write() {
    let root = tempfile::tempdir().unwrap();
    let base = root.path().join("registry");
    let storage = FileSchemaStorage::new(base.clone());

    let schema = Schema {
        schema_id: 1,
        collection: "../escaped".to_string(),
        version: 1,
        descriptor: valid_proto(),
        compatibility: CompatibilityMode::Backward,
        is_breaking: false,
        migration_id: None,
        created_at: 0,
    };

    let err = storage.put(&schema).await.unwrap_err();
    assert!(
        matches!(err, SchemaError::InvalidCollectionName(_)),
        "{err:?}"
    );
    assert!(!root.path().join("escaped").exists());
    assert!(!base.exists());
}

#[tokio::test]
async fn sch01_load_fails_closed_on_corrupt_index_with_unsafe_name() {
    let root = tempfile::tempdir().unwrap();
    let base = root.path().to_path_buf();

    std::fs::create_dir_all(&base).unwrap();
    std::fs::write(
        base.join("schemas.json"),
        r#"[{"schema_id":1,"collection":"../../etc","version":1,"descriptor":[],"compatibility":"Backward","is_breaking":false,"migration_id":null,"created_at":0}]"#,
    )
    .unwrap();

    let mut storage = FileSchemaStorage::new(base);
    let err = storage.load().await.unwrap_err();
    assert!(matches!(err, SchemaError::Storage(_)), "{err:?}");
    if let SchemaError::Storage(msg) = &err {
        assert!(msg.contains("corrupt schema index"), "{msg}");
    }
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
