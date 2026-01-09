use criterion::{black_box, criterion_group, criterion_main, Criterion};
use prkdb_core::replication::{AckLevel, FollowerServer, ReplicationConfig, ReplicationManager};
use prkdb_core::wal::{LogOperation, LogRecord, WalConfig, WriteAheadLog};
use std::sync::Arc;
use tokio::runtime::Runtime;

/// Benchmark single-node (no replication) throughput baseline
fn bench_single_node_baseline(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let dir = tempfile::tempdir().unwrap();
    let config = WalConfig {
        log_dir: dir.path().to_path_buf(),
        ..WalConfig::test_config()
    };
    let wal = Arc::new(WriteAheadLog::create(config).unwrap());

    c.bench_function("baseline/single_node_append", |b| {
        b.iter(|| {
            let wal = Arc::clone(&wal);
            rt.block_on(async move {
                let record = LogRecord::new(LogOperation::Put {
                    collection: "test".to_string(),
                    id: vec![1u8; 16],
                    data: vec![0u8; 1024],
                });
                black_box(wal.append(record).unwrap());
            })
        })
    });
}

/// Benchmark replication with Leader ack (fire-and-forget)
fn bench_leader_ack_replication(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    // Setup: 1 leader + 2 followers
    let (leader_wal, manager) = rt.block_on(async {
        let leader_dir = tempfile::tempdir().unwrap();
        let leader_config = WalConfig {
            log_dir: leader_dir.path().to_path_buf(),
            ..WalConfig::test_config()
        };
        let leader_wal = Arc::new(WriteAheadLog::create(leader_config).unwrap());

        let mut follower_addrs = Vec::new();
        for i in 0..2 {
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
                let _ = follower_server.run().await;
            });
        }

        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        let config = ReplicationConfig {
            replica_addresses: follower_addrs,
            ack_level: AckLevel::Leader,
            max_in_flight: 100,
            replication_timeout: std::time::Duration::from_secs(5),
            max_retries: 2,
            health_check_interval: std::time::Duration::from_secs(10),
            node_id: "leader".to_string(),
        };

        let manager = ReplicationManager::new(config).await.unwrap();
        let manager = Arc::new(tokio::sync::Mutex::new(manager));

        (leader_wal, manager)
    });

    c.bench_function("replication/leader_ack", |b| {
        b.iter(|| {
            let wal = Arc::clone(&leader_wal);
            let manager = Arc::clone(&manager);

            rt.block_on(async move {
                let record = LogRecord::new(LogOperation::Put {
                    collection: "test".to_string(),
                    id: vec![1u8; 16],
                    data: vec![0u8; 1024],
                });

                let offset = wal.append(record.clone()).unwrap();
                let mut mgr = manager.lock().await;
                black_box(mgr.replicate_batch(vec![record], offset).await.unwrap());
            })
        })
    });
}

/// Benchmark replication with Quorum ack
fn bench_quorum_ack_replication(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let (leader_wal, manager) = rt.block_on(async {
        let leader_dir = tempfile::tempdir().unwrap();
        let leader_config = WalConfig {
            log_dir: leader_dir.path().to_path_buf(),
            ..WalConfig::test_config()
        };
        let leader_wal = Arc::new(WriteAheadLog::create(leader_config).unwrap());

        let mut follower_addrs = Vec::new();
        for i in 0..2 {
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
                let _ = follower_server.run().await;
            });
        }

        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        let config = ReplicationConfig {
            replica_addresses: follower_addrs,
            ack_level: AckLevel::Quorum,
            max_in_flight: 100,
            replication_timeout: std::time::Duration::from_secs(5),
            max_retries: 2,
            health_check_interval: std::time::Duration::from_secs(10),
            node_id: "leader".to_string(),
        };

        let manager = ReplicationManager::new(config).await.unwrap();
        let manager = Arc::new(tokio::sync::Mutex::new(manager));

        (leader_wal, manager)
    });

    c.bench_function("replication/quorum_ack", |b| {
        b.iter(|| {
            let wal = Arc::clone(&leader_wal);
            let manager = Arc::clone(&manager);

            rt.block_on(async move {
                let record = LogRecord::new(LogOperation::Put {
                    collection: "test".to_string(),
                    id: vec![1u8; 16],
                    data: vec![0u8; 1024],
                });

                let offset = wal.append(record.clone()).unwrap();
                let mut mgr = manager.lock().await;
                black_box(mgr.replicate_batch(vec![record], offset).await.unwrap());
            })
        })
    });
}

/// Benchmark batch replication (100 records)
fn bench_batch_replication(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let (leader_wal, manager) = rt.block_on(async {
        let leader_dir = tempfile::tempdir().unwrap();
        let leader_config = WalConfig {
            log_dir: leader_dir.path().to_path_buf(),
            ..WalConfig::test_config()
        };
        let leader_wal = Arc::new(WriteAheadLog::create(leader_config).unwrap());

        let mut follower_addrs = Vec::new();
        for i in 0..2 {
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
                let _ = follower_server.run().await;
            });
        }

        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        let config = ReplicationConfig {
            replica_addresses: follower_addrs,
            ack_level: AckLevel::Quorum, // Use Quorum to measure full round-trip
            max_in_flight: 100,
            replication_timeout: std::time::Duration::from_secs(5),
            max_retries: 2,
            health_check_interval: std::time::Duration::from_secs(10),
            node_id: "leader".to_string(),
        };

        let manager = ReplicationManager::new(config).await.unwrap();
        let manager = Arc::new(tokio::sync::Mutex::new(manager));

        (leader_wal, manager)
    });

    let mut group = c.benchmark_group("replication/batch");
    group.throughput(criterion::Throughput::Elements(100));

    group.bench_function("batch_100", |b| {
        b.iter(|| {
            let wal = Arc::clone(&leader_wal);
            let manager = Arc::clone(&manager);

            rt.block_on(async move {
                // Create batch of 100 records
                let records: Vec<_> = (0..100u64)
                    .map(|i| {
                        LogRecord::new(LogOperation::Put {
                            collection: "test".to_string(),
                            id: i.to_le_bytes().to_vec(),
                            data: vec![0u8; 1024], // 1KB payload
                        })
                    })
                    .collect();

                // Write to leader WAL (batch)
                let offset = wal.append_batch(records.clone()).unwrap();

                // Replicate batch
                let mut mgr = manager.lock().await;
                black_box(mgr.replicate_batch(records, offset).await.unwrap());
            })
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_single_node_baseline,
    bench_leader_ack_replication,
    bench_quorum_ack_replication,
    bench_batch_replication
);
criterion_main!(benches);
