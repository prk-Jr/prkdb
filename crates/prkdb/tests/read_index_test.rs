// Unit test for ReadIndex protocol
use prkdb::raft::config::ClusterConfig;
use prkdb::raft::node::RaftNode;
use prkdb::raft::state_machine::PrkDbStateMachine;
use prkdb::storage::WalStorageAdapter;
use std::net::SocketAddr;
use std::sync::Arc;
use tempfile::TempDir;

/// Bind an ephemeral port and return it, so concurrently-running test binaries
/// cannot collide on a fixed number. This file and compaction_test.rs both used
/// 50001 before.
async fn free_port() -> u16 {
    tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("binding an ephemeral port on loopback cannot fail")
        .local_addr()
        .expect("a bound listener always has a local address")
        .port()
}

async fn create_test_node() -> (Arc<RaftNode>, TempDir) {
    // Create storage
    let temp_dir = TempDir::new().unwrap();
    let storage = Arc::new(
        WalStorageAdapter::builder(temp_dir.path().to_path_buf())
            .build()
            .expect("Failed to create storage"),
    );

    // Create state machine
    let state_machine = Arc::new(PrkDbStateMachine::new(storage.clone()));

    // Create cluster config
    let listen_addr: SocketAddr = format!("127.0.0.1:{}", free_port().await).parse().unwrap();
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
    let node = Arc::new(RaftNode::new(config, storage, state_machine));
    (node, temp_dir)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_read_index_basic() {
    let (node, _temp) = create_test_node().await;

    // Test ReadIndex - should fail since node is not leader
    let result = node.handle_read_index(1).await;
    assert!(
        result.is_err(),
        "ReadIndex should fail when not leader (default state is Follower)"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_wait_for_apply_immediate() {
    let (node, _temp) = create_test_node().await;

    // wait_for_apply(0) should succeed immediately since last_applied starts at 0
    let result = node.wait_for_apply(0).await;
    assert!(
        result.is_ok(),
        "Should succeed when index is 0 (last_applied defaults to 0)"
    );
}

// Note: Full integration tests for ReadIndex require:
// 1. A running leader node
// 2. Ability to set commit_index and verify read_index
// 3. Ability to test wait_for_apply with notifications
// These should be tested in integration tests with a real Raft cluster.
