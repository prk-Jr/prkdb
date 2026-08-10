use prkdb::raft::config::ClusterConfig;
use prkdb::raft::node::RaftNode;
use prkdb::raft::state_machine::{PrkDbStateMachine, StateMachine};
use prkdb::storage::WalStorageAdapter;
use prkdb_types::storage::StorageAdapter;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;

/// Bind an ephemeral port and return it, so concurrently-running test binaries
/// cannot collide on a fixed number.
async fn free_port() -> u16 {
    tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("binding an ephemeral port on loopback cannot fail")
        .local_addr()
        .expect("a bound listener always has a local address")
        .port()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_log_compaction() {
    // Create storage
    let temp_dir = TempDir::new().unwrap();
    let storage = Arc::new(
        WalStorageAdapter::builder(temp_dir.path().to_path_buf())
            .build()
            .expect("Failed to create storage"),
    );

    // Create state machine
    let state_machine = Arc::new(PrkDbStateMachine::new(storage.clone()));

    // Create cluster config.
    //
    // The port is allocated by the OS rather than hardcoded: test binaries in this
    // workspace run concurrently, and 50001 was previously used by both this file
    // and read_index_test.rs.
    let port = free_port().await;
    let mut peers = HashMap::new();
    peers.insert(1, format!("127.0.0.1:{port}"));
    let listen_addr = format!("127.0.0.1:{port}").parse().unwrap();

    let config = ClusterConfig {
        local_node_id: 1,
        listen_addr,
        nodes: vec![(1, listen_addr)],
        election_timeout_min_ms: 1000,
        election_timeout_max_ms: 2000,
        heartbeat_interval_ms: 200,
        partition_id: 0,
    };

    // Create Raft node
    let _node = Arc::new(RaftNode::new(
        config,
        storage.clone(),
        state_machine.clone(),
    ));

    // Write some KV pairs to storage
    for i in 0..100 {
        let key = format!("key_{}", i);
        let value = format!("value_{}", i);
        storage.put(key.as_bytes(), value.as_bytes()).await.unwrap();
    }

    // Create a snapshot using the state_machine directly
    let snapshot_result = state_machine.snapshot().await;
    assert!(snapshot_result.is_ok(), "Snapshot creation should succeed");

    let snapshot_data = snapshot_result.unwrap();
    assert!(!snapshot_data.is_empty(), "Snapshot should contain data");

    println!("✅ Snapshot created: {} bytes", snapshot_data.len());

    // Restore snapshot to a new storage
    let temp_dir2 = TempDir::new().unwrap();
    let storage2 = Arc::new(
        WalStorageAdapter::builder(temp_dir2.path().to_path_buf())
            .build()
            .expect("Failed to create storage"),
    );
    let state_machine2 = Arc::new(PrkDbStateMachine::new(storage2.clone()));

    let restore_result = state_machine2.restore(&snapshot_data).await;
    assert!(restore_result.is_ok(), "Snapshot restore should succeed");

    // Verify all keys are restored
    for i in 0..100 {
        let key = format!("key_{}", i);
        let expected_value = format!("value_{}", i);

        let retrieved = storage2.get(key.as_bytes()).await.unwrap();
        assert!(
            retrieved.is_some(),
            "Key {} should exist after restore",
            key
        );
        assert_eq!(
            String::from_utf8(retrieved.unwrap()).unwrap(),
            expected_value,
            "Value for key {} should match",
            key
        );
    }

    println!("✅ All 100 keys verified after snapshot restore");
}
