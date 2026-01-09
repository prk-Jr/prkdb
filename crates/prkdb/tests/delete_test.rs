// Unit test for delete operation
use prkdb::storage::WalStorageAdapter;
use prkdb_types::storage::StorageAdapter;
use tempfile::TempDir;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_delete_removes_key() {
    // Create temp directory
    let temp_dir = TempDir::new().unwrap();
    let log_dir = temp_dir.path().to_path_buf();

    // Create storage
    let storage = WalStorageAdapter::builder(log_dir)
        .build()
        .expect("Failed to create storage");

    let key = b"test_key";
    let value = b"test_value";

    // 1. Put key-value
    storage.put(key, value).await.expect("Put failed");

    // Small delay for flush
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // 2. Verify it exists
    let result = storage.get(key).await.expect("Get failed");
    assert!(result.is_some(), "Key should exist after put");
    assert_eq!(result.unwrap(), value);

    // 3. Delete the key
    storage.delete(key).await.expect("Delete failed");

    // Small delay for flush
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // 4. Verify it's gone
    let result_after_delete = storage.get(key).await.expect("Get after delete failed");
    assert!(
        result_after_delete.is_none(),
        "Key should not exist after delete, but got: {:?}",
        result_after_delete
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_delete_nonexistent_key() {
    let temp_dir = TempDir::new().unwrap();
    let log_dir = temp_dir.path().to_path_buf();

    let storage = WalStorageAdapter::builder(log_dir)
        .build()
        .expect("Failed to create storage");

    let key = b"nonexistent";

    // Delete should succeed even if key doesn't exist
    storage.delete(key).await.expect("Delete should succeed");

    // Verify still doesn't exist
    let result = storage.get(key).await.expect("Get failed");
    assert!(result.is_none());
}
