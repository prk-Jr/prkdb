//! Diagnostic probes: passing tests confirm existing defects, not desired behavior.
//! Copy to crates/prkdb-schema/tests/schema_review.rs and run:
//! cargo test --offline -p prkdb-schema --test schema_review -- --nocapture
use prkdb_schema::{CompatibilityMode, FileSchemaStorage, SchemaRegistry, SchemaStorage};
use std::sync::Arc;

#[tokio::test]
async fn collection_can_escape_descriptor_directory() {
    let root = tempfile::tempdir().unwrap();
    let base = root.path().join("registry");
    let registry = SchemaRegistry::new(Arc::new(FileSchemaStorage::new(base)));
    registry
        .register("../../escaped", vec![1, 2, 3], CompatibilityMode::Backward, None)
        .await
        .unwrap();
    assert_eq!(std::fs::read(root.path().join("escaped/v1.binpb")).unwrap(), vec![1, 2, 3]);
    println!("CONFIRMED: collection traversal wrote outside the registry directory");
}

#[tokio::test]
async fn missing_descriptor_is_loaded_as_empty() {
    let root = tempfile::tempdir().unwrap();
    let registry = SchemaRegistry::new(Arc::new(FileSchemaStorage::new(root.path().into())));
    registry
        .register("users", vec![10, 0], CompatibilityMode::Backward, None)
        .await
        .unwrap();
    std::fs::remove_file(root.path().join("descriptors/users/v1.binpb")).unwrap();
    let mut reopened = FileSchemaStorage::new(root.path().into());
    reopened.load().await.unwrap();
    assert!(reopened.get("users", 1).await.unwrap().unwrap().descriptor.is_empty());
    println!("CONFIRMED: missing persisted descriptor accepted as an empty schema");
}
