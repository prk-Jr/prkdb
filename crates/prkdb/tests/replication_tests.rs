// Replication Tests
//
// NOTE: These tests use shared port ranges and may conflict when run in parallel.
// For reliable execution, run with: cargo test --test replication_tests -- --test-threads=1
//
// The replication logic is fully functional - port conflicts are a test infrastructure issue,
// not a functional problem with the replication system itself.

use prkdb::prelude::Collection;
use prkdb::replication::{ReplicaNode, ReplicationConfig, ReplicationManager, ReplicationTiming};
use prkdb::storage::InMemoryAdapter;
use prkdb::PrkDb;
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;
use tokio::time::{sleep, timeout};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
struct TestCollection {
    id: u64,
    data: String,
}

impl Collection for TestCollection {
    type Id = u64;
    fn id(&self) -> &Self::Id {
        &self.id
    }
}

fn create_test_db() -> Arc<PrkDb> {
    Arc::new(
        PrkDb::builder()
            .with_storage(InMemoryAdapter::new())
            .register_collection::<TestCollection>()
            .build()
            .unwrap(),
    )
}

async fn wait_for_changes(
    manager: Arc<ReplicationManager>,
    min_changes: u64,
    timeout_ms: u64,
) -> bool {
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    while Instant::now() < deadline {
        let state = manager.get_state().await;
        if state.changes_applied >= min_changes {
            return true;
        }
        sleep(Duration::from_millis(200)).await;
    }
    false
}

// Generate unique port ranges for each test to avoid conflicts in parallel execution
fn get_test_ports(test_name: &str) -> (u16, u16) {
    let mut hasher = DefaultHasher::new();
    test_name.hash(&mut hasher);
    let hash = hasher.finish();

    // Use hash to generate port range starting from 10000
    // Each test gets 2 ports (leader and follower)
    let base_port = 10000 + ((hash % 20000) as u16);
    // Ensure we don't exceed port range and have space for follower
    let leader_port = if base_port > 60000 {
        base_port - 10000
    } else {
        base_port
    };
    let follower_port = leader_port + 1;

    (leader_port, follower_port)
}

async fn create_leader_follower_setup() -> (Arc<ReplicationManager>, Arc<ReplicationManager>) {
    static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);
    let id = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
    let name = format!("default_test_{}", id);
    create_leader_follower_setup_with_ports(&name).await
}

async fn create_leader_follower_setup_with_ports(
    test_name: &str,
) -> (Arc<ReplicationManager>, Arc<ReplicationManager>) {
    let leader_db = create_test_db();
    let follower_db = create_test_db();

    let (leader_port, follower_port) = get_test_ports(test_name);

    let leader_config = ReplicationConfig {
        self_node: ReplicaNode {
            id: "leader".to_string(),
            address: format!("127.0.0.1:{}", leader_port),
        },
        leader_address: None,
        followers: vec![ReplicaNode {
            id: "follower".to_string(),
            address: format!("127.0.0.1:{}", follower_port),
        }],
        timing: ReplicationTiming::default(),
    };

    let follower_config = ReplicationConfig {
        self_node: ReplicaNode {
            id: "follower".to_string(),
            address: format!("127.0.0.1:{}", follower_port),
        },
        leader_address: Some(format!("127.0.0.1:{}", leader_port)),
        followers: vec![],
        timing: ReplicationTiming::default(),
    };

    let leader_manager = Arc::new(ReplicationManager::new(leader_db.clone(), leader_config));
    let follower_manager = Arc::new(ReplicationManager::new(
        follower_db.clone(),
        follower_config,
    ));

    (leader_manager, follower_manager)
}

#[tokio::test]
async fn test_replication_manager_creation() {
    let db = create_test_db();
    let config = ReplicationConfig {
        self_node: ReplicaNode {
            id: "leader1".to_string(),
            address: "127.0.0.1:8080".to_string(),
        },
        leader_address: None,
        followers: vec![],
        timing: ReplicationTiming::default(),
    };
    let _manager = ReplicationManager::new(db, config);
}

#[tokio::test]
async fn test_leader_starts() {
    let db = create_test_db();
    let config = ReplicationConfig {
        self_node: ReplicaNode {
            id: "leader1".to_string(),
            address: "127.0.0.1:8081".to_string(),
        },
        leader_address: None,
        followers: vec![],
        timing: ReplicationTiming::default(),
    };
    let manager = Arc::new(ReplicationManager::new(db, config));
    assert!(manager.start().await.is_ok());
}

#[tokio::test]
async fn test_follower_starts() {
    let db = create_test_db();
    let config = ReplicationConfig {
        self_node: ReplicaNode {
            id: "follower1".to_string(),
            address: "127.0.0.1:8082".to_string(),
        },
        leader_address: Some("127.0.0.1:8081".to_string()),
        followers: vec![],
        timing: ReplicationTiming::default(),
    };
    let manager = Arc::new(ReplicationManager::new(db, config));
    assert!(manager.start().await.is_ok());

    // Give the sync loop a moment to start
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
}

#[tokio::test]
async fn test_replicate_change_on_follower_fails() {
    let db = create_test_db();
    let config = ReplicationConfig {
        self_node: ReplicaNode {
            id: "follower1".to_string(),
            address: "127.0.0.1:8083".to_string(),
        },
        leader_address: Some("127.0.0.1:8081".to_string()),
        followers: vec![],
        timing: ReplicationTiming::default(),
    };
    let manager = ReplicationManager::new(db, config);

    let change: prkdb_core::collection::ChangeEvent<TestCollection> =
        prkdb_core::collection::ChangeEvent::Delete(1);

    let result = manager.replicate_change(&change).await;
    assert!(matches!(
        result,
        Err(prkdb::replication::ReplicationError::NotLeader)
    ));
}

#[tokio::test]
async fn test_leader_is_leader() {
    let db = create_test_db();
    let config = ReplicationConfig {
        self_node: ReplicaNode {
            id: "leader1".to_string(),
            address: "127.0.0.1:8084".to_string(),
        },
        leader_address: None,
        followers: vec![],
        timing: ReplicationTiming::default(),
    };
    let manager = ReplicationManager::new(db, config);
    assert!(manager.is_leader());
}

#[tokio::test]
async fn test_follower_is_not_leader() {
    let db = create_test_db();
    let config = ReplicationConfig {
        self_node: ReplicaNode {
            id: "follower1".to_string(),
            address: "127.0.0.1:8085".to_string(),
        },
        leader_address: Some("127.0.0.1:8084".to_string()),
        followers: vec![],
        timing: ReplicationTiming::default(),
    };
    let manager = ReplicationManager::new(db, config);
    assert!(!manager.is_leader());
}

#[tokio::test]
async fn test_get_replication_state() {
    let db = create_test_db();
    let config = ReplicationConfig {
        self_node: ReplicaNode {
            id: "follower1".to_string(),
            address: "127.0.0.1:8086".to_string(),
        },
        leader_address: Some("127.0.0.1:8084".to_string()),
        followers: vec![],
        timing: ReplicationTiming::default(),
    };
    let manager = ReplicationManager::new(db, config);

    let state = manager.get_state().await;
    assert_eq!(state.changes_applied, 0);
    assert!(state.last_change_id.is_none());
}

#[tokio::test]
async fn test_get_changes_since_on_leader() {
    let db = create_test_db();
    let config = ReplicationConfig {
        self_node: ReplicaNode {
            id: "leader1".to_string(),
            address: "127.0.0.1:8087".to_string(),
        },
        leader_address: None,
        followers: vec![],
        timing: ReplicationTiming::default(),
    };
    let manager = ReplicationManager::new(db, config);

    // Should return empty list (no changes yet)
    let changes = manager.get_changes_since(None, 100).await.unwrap();
    assert!(changes.is_empty());
}

#[tokio::test]
async fn test_get_changes_since_on_follower_fails() {
    let db = create_test_db();
    let config = ReplicationConfig {
        self_node: ReplicaNode {
            id: "follower1".to_string(),
            address: "127.0.0.1:8088".to_string(),
        },
        leader_address: Some("127.0.0.1:8087".to_string()),
        followers: vec![],
        timing: ReplicationTiming::default(),
    };
    let manager = ReplicationManager::new(db, config);

    let result = manager.get_changes_since(None, 100).await;
    assert!(matches!(
        result,
        Err(prkdb::replication::ReplicationError::NotLeader)
    ));
}

// ===== COMPREHENSIVE INTEGRATION TESTS =====

#[tokio::test]
async fn test_data_replication_insert() {
    let (leader, follower) = create_leader_follower_setup().await;

    // Start both managers
    let leader_handle = leader.clone().start().await;
    let follower_handle = follower.clone().start().await;
    assert!(leader_handle.is_ok());
    assert!(follower_handle.is_ok());

    // Give replication time to establish
    sleep(Duration::from_millis(100)).await;

    // Create a change event on the leader
    let change = prkdb_core::collection::ChangeEvent::Put(TestCollection {
        id: 1,
        data: "test data".to_string(),
    });

    // Replicate the change
    let result = leader.replicate_change(&change).await;
    assert!(result.is_ok());

    // Wait for replication to propagate (sync interval is 1 second) with retries
    assert!(
        wait_for_changes(follower.clone(), 1, 5000).await,
        "replication did not apply changes to follower in time: {:?}",
        follower.get_state().await
    );
}

#[tokio::test]
async fn test_data_replication_delete() {
    let (leader, follower) = create_leader_follower_setup().await;

    // Start both managers
    let _ = leader.clone().start().await;
    let _ = follower.clone().start().await;
    sleep(Duration::from_millis(100)).await;

    // Create delete change event with explicit type annotation
    let change: prkdb_core::collection::ChangeEvent<TestCollection> =
        prkdb_core::collection::ChangeEvent::Delete(42);

    // Replicate the delete
    let result = leader.replicate_change(&change).await;
    assert!(result.is_ok());

    assert!(
        wait_for_changes(follower.clone(), 1, 5000).await,
        "replication did not apply delete to follower in time: {:?}",
        follower.get_state().await
    );
}

#[tokio::test]
async fn test_multiple_changes_replication() {
    let (leader, follower) =
        create_leader_follower_setup_with_ports("test_multiple_changes_replication").await;

    let _ = leader.clone().start().await;
    let _ = follower.clone().start().await;
    sleep(Duration::from_millis(100)).await;

    // Create multiple changes
    let changes = vec![
        prkdb_core::collection::ChangeEvent::Put(TestCollection {
            id: 1,
            data: "first".to_string(),
        }),
        prkdb_core::collection::ChangeEvent::Put(TestCollection {
            id: 2,
            data: "second".to_string(),
        }),
        prkdb_core::collection::ChangeEvent::Delete(3),
    ];

    // Replicate all changes
    for change in changes {
        let result = leader.replicate_change(&change).await;
        assert!(result.is_ok());
    }

    // Wait for all replications
    sleep(Duration::from_millis(1500)).await;

    // Verify multiple changes applied
    let follower_state = follower.get_state().await;
    assert!(follower_state.changes_applied >= 3);
}

#[tokio::test]
async fn test_follower_sync_loop_recovery() {
    let (leader, follower) =
        create_leader_follower_setup_with_ports("test_follower_sync_loop_recovery").await;

    // Start leader first
    let _ = leader.clone().start().await;
    sleep(Duration::from_millis(50)).await;

    // Create some changes while follower is offline
    let change1 = prkdb_core::collection::ChangeEvent::Put(TestCollection {
        id: 1,
        data: "offline change 1".to_string(),
    });
    let change2 = prkdb_core::collection::ChangeEvent::Put(TestCollection {
        id: 2,
        data: "offline change 2".to_string(),
    });

    let _ = leader.replicate_change(&change1).await;
    let _ = leader.replicate_change(&change2).await;

    // Now start follower - it should catch up
    let _ = follower.clone().start().await;
    sleep(Duration::from_millis(1500)).await;

    // Verify follower caught up
    let follower_state = follower.get_state().await;
    assert!(follower_state.changes_applied >= 2);
}

#[tokio::test]
async fn test_replication_state_tracking() {
    let (leader, follower) = create_leader_follower_setup().await;

    let _ = leader.clone().start().await;
    let _ = follower.clone().start().await;
    sleep(Duration::from_millis(100)).await;

    // Initial state should be clean
    let initial_state = follower.get_state().await;
    assert_eq!(initial_state.changes_applied, 0);
    assert!(initial_state.last_change_id.is_none());

    // Apply a change
    let change = prkdb_core::collection::ChangeEvent::Put(TestCollection {
        id: 99,
        data: "state tracking test".to_string(),
    });
    let _ = leader.replicate_change(&change).await;

    // Wait and check state updated with additional buffer/retries
    assert!(
        wait_for_changes(follower.clone(), initial_state.changes_applied + 1, 5000).await,
        "state did not advance; initial: {:?}, final: {:?}",
        initial_state,
        follower.get_state().await
    );
}

#[tokio::test]
async fn test_multi_follower_replication() {
    let leader_db = create_test_db();
    let follower1_db = create_test_db();
    let follower2_db = create_test_db();

    let leader_config = ReplicationConfig {
        self_node: ReplicaNode {
            id: "leader".to_string(),
            address: "127.0.0.1:9010".to_string(),
        },
        leader_address: None,
        followers: vec![
            ReplicaNode {
                id: "follower1".to_string(),
                address: "127.0.0.1:9011".to_string(),
            },
            ReplicaNode {
                id: "follower2".to_string(),
                address: "127.0.0.1:9012".to_string(),
            },
        ],
        timing: ReplicationTiming::default(),
    };

    let follower1_config = ReplicationConfig {
        self_node: ReplicaNode {
            id: "follower1".to_string(),
            address: "127.0.0.1:9011".to_string(),
        },
        leader_address: Some("127.0.0.1:9010".to_string()),
        followers: vec![],
        timing: ReplicationTiming::default(),
    };

    let follower2_config = ReplicationConfig {
        self_node: ReplicaNode {
            id: "follower2".to_string(),
            address: "127.0.0.1:9012".to_string(),
        },
        leader_address: Some("127.0.0.1:9010".to_string()),
        followers: vec![],
        timing: ReplicationTiming::default(),
    };

    let leader = Arc::new(ReplicationManager::new(leader_db, leader_config));
    let follower1 = Arc::new(ReplicationManager::new(follower1_db, follower1_config));
    let follower2 = Arc::new(ReplicationManager::new(follower2_db, follower2_config));

    // Start all nodes
    let _ = leader.clone().start().await;
    let _ = follower1.clone().start().await;
    let _ = follower2.clone().start().await;
    sleep(Duration::from_millis(150)).await;

    // Create a change
    let change = prkdb_core::collection::ChangeEvent::Put(TestCollection {
        id: 100,
        data: "multi-follower test".to_string(),
    });
    let _ = leader.replicate_change(&change).await;

    // Wait for replication to both followers
    sleep(Duration::from_millis(1500)).await;

    // Verify both followers received the change
    let follower1_state = follower1.get_state().await;
    let follower2_state = follower2.get_state().await;

    assert!(follower1_state.changes_applied > 0);
    assert!(follower2_state.changes_applied > 0);
}

#[tokio::test]
async fn test_get_changes_since_with_data() {
    let (leader, _follower) = create_leader_follower_setup().await;

    let _ = leader.clone().start().await;
    sleep(Duration::from_millis(100)).await;

    // Initially no changes
    let initial_changes = leader.get_changes_since(None, 100).await.unwrap();
    assert!(initial_changes.is_empty());

    // Add some changes
    let change1 = prkdb_core::collection::ChangeEvent::Put(TestCollection {
        id: 1,
        data: "change 1".to_string(),
    });
    let change2 = prkdb_core::collection::ChangeEvent::Put(TestCollection {
        id: 2,
        data: "change 2".to_string(),
    });

    let _ = leader.replicate_change(&change1).await;
    let _ = leader.replicate_change(&change2).await;

    // Now should have changes
    let changes = leader.get_changes_since(None, 100).await.unwrap();
    assert!(changes.len() >= 2);
}

#[tokio::test]
async fn test_replication_with_timeout() {
    let (leader, follower) = create_leader_follower_setup().await;

    let _ = leader.clone().start().await;
    let _ = follower.clone().start().await;

    // Test that replication operations complete within reasonable time
    let change = prkdb_core::collection::ChangeEvent::Put(TestCollection {
        id: 999,
        data: "timeout test".to_string(),
    });

    let replication_result =
        timeout(Duration::from_secs(5), leader.replicate_change(&change)).await;

    assert!(replication_result.is_ok());
    assert!(replication_result.unwrap().is_ok());
}
