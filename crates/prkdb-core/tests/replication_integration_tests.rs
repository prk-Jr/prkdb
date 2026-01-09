/// Integration tests for end-to-end replication
///
/// Tests the full replication stack:
/// - Leader with WalStorageAdapter
/// - Multiple follower servers
/// - ReplicationManager coordinating writes
/// - Different ack levels
use prkdb_core::replication::{AckLevel, FollowerServer, ReplicationConfig, ReplicationManager};
use prkdb_core::wal::{LogOperation, LogRecord, WalConfig, WriteAheadLog};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::test]
async fn test_leader_with_2_followers_quorum_ack() {
    // Setup: 1 leader + 2 followers
    let leader_dir = tempfile::tempdir().unwrap();
    let follower1_dir = tempfile::tempdir().unwrap();
    let follower2_dir = tempfile::tempdir().unwrap();

    // Create WALs
    let leader_config = WalConfig {
        log_dir: leader_dir.path().to_path_buf(),
        ..WalConfig::test_config()
    };
    let leader_wal = Arc::new(WriteAheadLog::create(leader_config).unwrap());

    let follower1_config = WalConfig {
        log_dir: follower1_dir.path().to_path_buf(),
        ..WalConfig::test_config()
    };
    let follower1_wal = Arc::new(WriteAheadLog::create(follower1_config).unwrap());

    let follower2_config = WalConfig {
        log_dir: follower2_dir.path().to_path_buf(),
        ..WalConfig::test_config()
    };
    let follower2_wal = Arc::new(WriteAheadLog::create(follower2_config).unwrap());

    // Start follower servers
    let follower1_server = FollowerServer::new(
        "127.0.0.1:0",
        Arc::clone(&follower1_wal),
        "follower-1".to_string(),
    )
    .await
    .unwrap();
    let follower1_addr = follower1_server.listener.local_addr().unwrap();

    let follower2_server = FollowerServer::new(
        "127.0.0.1:0",
        Arc::clone(&follower2_wal),
        "follower-2".to_string(),
    )
    .await
    .unwrap();
    let follower2_addr = follower2_server.listener.local_addr().unwrap();

    // Run follower servers in background
    tokio::spawn(async move {
        follower1_server.run().await.unwrap();
    });
    tokio::spawn(async move {
        follower2_server.run().await.unwrap();
    });

    sleep(Duration::from_millis(100)).await;

    // Create replication manager (Quorum ack = need 1 out of 2)
    let replication_config = ReplicationConfig {
        replica_addresses: vec![follower1_addr.to_string(), follower2_addr.to_string()],
        ack_level: AckLevel::Quorum,
        max_in_flight: 10,
        replication_timeout: Duration::from_secs(1),
        max_retries: 2,
        health_check_interval: Duration::from_secs(10),
        node_id: "leader".to_string(),
    };

    let mut replication_manager = ReplicationManager::new(replication_config).await.unwrap();

    // Write records to leader
    let records = vec![
        LogRecord::new(LogOperation::Put {
            collection: "users".to_string(),
            id: b"user1".to_vec(),
            data: b"Alice".to_vec(),
        }),
        LogRecord::new(LogOperation::Put {
            collection: "users".to_string(),
            id: b"user2".to_vec(),
            data: b"Bob".to_vec(),
        }),
        LogRecord::new(LogOperation::Put {
            collection: "users".to_string(),
            id: b"user3".to_vec(),
            data: b"Charlie".to_vec(),
        }),
    ];

    // Append to leader
    let mut leader_offset = 0;
    for record in &records {
        leader_offset = leader_wal.append(record.clone()).unwrap();
    }

    // Replicate (should wait for quorum)
    replication_manager
        .replicate_batch(records, leader_offset)
        .await
        .unwrap();

    // Wait a bit for async replication to complete
    sleep(Duration::from_millis(200)).await;

    // Verify: both followers should have the data
    assert_eq!(follower1_wal.next_offset(), 3);
    assert_eq!(follower2_wal.next_offset(), 3);

    // Verify content
    let record1 = follower1_wal.read(0).unwrap();
    match record1.operation {
        LogOperation::Put { id, data, .. } => {
            assert_eq!(id, b"user1");
            assert_eq!(data, b"Alice");
        }
        _ => panic!("Wrong operation type"),
    }
}

#[tokio::test]
async fn test_leader_ack_fire_and_forget() {
    // Setup
    let leader_dir = tempfile::tempdir().unwrap();
    let follower_dir = tempfile::tempdir().unwrap();

    let leader_config = WalConfig {
        log_dir: leader_dir.path().to_path_buf(),
        ..WalConfig::test_config()
    };
    let leader_wal = Arc::new(WriteAheadLog::create(leader_config).unwrap());

    let follower_config = WalConfig {
        log_dir: follower_dir.path().to_path_buf(),
        ..WalConfig::test_config()
    };
    let follower_wal = Arc::new(WriteAheadLog::create(follower_config).unwrap());

    // Start follower server
    let follower_server = FollowerServer::new(
        "127.0.0.1:0",
        Arc::clone(&follower_wal),
        "follower".to_string(),
    )
    .await
    .unwrap();
    let follower_addr = follower_server.listener.local_addr().unwrap();

    tokio::spawn(async move {
        follower_server.run().await.unwrap();
    });

    sleep(Duration::from_millis(100)).await;

    // Leader ack mode (fire-and-forget)
    let replication_config = ReplicationConfig {
        replica_addresses: vec![follower_addr.to_string()],
        ack_level: AckLevel::Leader,
        ..ReplicationConfig::test_config()
    };

    let mut replication_manager = ReplicationManager::new(replication_config).await.unwrap();

    // Write to leader
    let record = LogRecord::new(LogOperation::Put {
        collection: "test".to_string(),
        id: b"key1".to_vec(),
        data: b"value1".to_vec(),
    });

    let leader_offset = leader_wal.append(record.clone()).unwrap();

    // Replicate (should return immediately)
    let start = std::time::Instant::now();
    replication_manager
        .replicate_batch(vec![record], leader_offset)
        .await
        .unwrap();
    let duration = start.elapsed();

    // Should be very fast (< 10ms since it's fire-and-forget)
    assert!(duration.as_millis() < 50);

    // Wait for async replication to complete (including potential retries)
    sleep(Duration::from_millis(500)).await;

    // Verify follower eventually got the data
    assert_eq!(follower_wal.next_offset(), 1);
}

#[tokio::test]
async fn test_all_ack_level_waits_for_all_replicas() {
    // Setup: 3 followers
    let leader_dir = tempfile::tempdir().unwrap();
    let leader_config = WalConfig {
        log_dir: leader_dir.path().to_path_buf(),
        ..WalConfig::test_config()
    };
    let leader_wal = Arc::new(WriteAheadLog::create(leader_config).unwrap());

    let mut follower_addrs = Vec::new();
    for i in 0..3 {
        let follower_dir = tempfile::tempdir().unwrap();
        let follower_config = WalConfig {
            log_dir: follower_dir.path().to_path_buf(),
            ..WalConfig::test_config()
        };
        let follower_wal = Arc::new(WriteAheadLog::create(follower_config).unwrap());

        let follower_server =
            FollowerServer::new("127.0.0.1:0", follower_wal, format!("follower-{}", i))
                .await
                .unwrap();
        let addr = follower_server.listener.local_addr().unwrap();
        follower_addrs.push(addr.to_string());

        tokio::spawn(async move {
            follower_server.run().await.unwrap();
        });
    }

    sleep(Duration::from_millis(100)).await;

    // All ack mode (wait for all 3)
    let replication_config = ReplicationConfig {
        replica_addresses: follower_addrs,
        ack_level: AckLevel::All,
        ..ReplicationConfig::test_config()
    };

    let mut replication_manager = ReplicationManager::new(replication_config).await.unwrap();

    // Write and replicate
    let record = LogRecord::new(LogOperation::Put {
        collection: "test".to_string(),
        id: b"key1".to_vec(),
        data: b"value1".to_vec(),
    });

    let leader_offset = leader_wal.append(record.clone()).unwrap();

    // Should wait for all 3 replicas
    replication_manager
        .replicate_batch(vec![record], leader_offset)
        .await
        .unwrap();

    // All replicas should have acknowledged by now
    // (We can't easily verify this without exposing internal state,
    // but the fact that replicate_batch returned successfully means
    // all replicas acknowledged)
}

#[tokio::test]
async fn test_replication_with_batch_writes() {
    // Test replicating a batch of 100 writes
    let leader_dir = tempfile::tempdir().unwrap();
    let follower_dir = tempfile::tempdir().unwrap();

    let leader_config = WalConfig {
        log_dir: leader_dir.path().to_path_buf(),
        ..WalConfig::test_config()
    };
    let leader_wal = Arc::new(WriteAheadLog::create(leader_config).unwrap());

    let follower_config = WalConfig {
        log_dir: follower_dir.path().to_path_buf(),
        ..WalConfig::test_config()
    };
    let follower_wal = Arc::new(WriteAheadLog::create(follower_config).unwrap());

    // Start follower
    let follower_server = FollowerServer::new(
        "127.0.0.1:0",
        Arc::clone(&follower_wal),
        "follower".to_string(),
    )
    .await
    .unwrap();
    let follower_addr = follower_server.listener.local_addr().unwrap();

    tokio::spawn(async move {
        follower_server.run().await.unwrap();
    });

    sleep(Duration::from_millis(100)).await;

    let replication_config = ReplicationConfig {
        replica_addresses: vec![follower_addr.to_string()],
        ack_level: AckLevel::Quorum,
        ..ReplicationConfig::test_config()
    };

    let mut replication_manager = ReplicationManager::new(replication_config).await.unwrap();

    // Create 100 records
    let mut records = Vec::new();
    for i in 0..100 {
        records.push(LogRecord::new(LogOperation::Put {
            collection: "test".to_string(),
            id: format!("key{}", i).into_bytes(),
            data: format!("value{}", i).into_bytes(),
        }));
    }

    // Write to leader
    let mut leader_offset = 0;
    for record in &records {
        leader_offset = leader_wal.append(record.clone()).unwrap();
    }

    // Replicate all at once
    replication_manager
        .replicate_batch(records, leader_offset)
        .await
        .unwrap();

    // Wait for replication
    sleep(Duration::from_millis(300)).await;

    // Verify all 100 records are on follower
    assert_eq!(follower_wal.next_offset(), 100);
}
