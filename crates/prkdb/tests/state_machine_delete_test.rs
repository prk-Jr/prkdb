// Test for State Machine delete handling
use prkdb::prelude::*;
use prkdb::raft::command::Command;
use prkdb::raft::state_machine::{PrkDbStateMachine, StateMachine};
use prkdb::storage::WalStorageAdapter;
use prkdb_types::storage::StorageAdapter;
use std::sync::Arc;
use tempfile::TempDir;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_state_machine_delete() {
    // Create storage
    let temp_dir = TempDir::new().unwrap();
    let storage = Arc::new(
        WalStorageAdapter::builder(temp_dir.path().to_path_buf())
            .build()
            .expect("Failed to create storage"),
    );

    // Create state machine
    let sm = PrkDbStateMachine::new(storage.clone());

    let key = b"test_key";
    let value = b"test_value";

    // 1. Apply PUT command
    let put_cmd = Command::Put {
        key: key.to_vec(),
        value: value.to_vec(),
    };
    sm.apply(&put_cmd.serialize())
        .await
        .expect("PUT apply failed");

    // Small delay for flush
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // 2. Verify it exists
    let result = storage.get(key).await.expect("Get failed");
    assert!(result.is_some(), "Key should exist after PUT command");

    // 3. Apply DELETE command
    let delete_cmd = Command::Delete { key: key.to_vec() };
    sm.apply(&delete_cmd.serialize())
        .await
        .expect("DELETE apply failed");

    // Small delay for flush
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // 4. Verify it's gone
    let result_after_delete = storage.get(key).await.expect("Get after delete failed");
    assert!(
        result_after_delete.is_none(),
        "Key should not exist after DELETE command, but got: {:?}",
        result_after_delete
    );
}
