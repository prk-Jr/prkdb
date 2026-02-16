use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use prkdb::raft::{ClusterConfig, PrkDbStateMachine, RaftNode, RpcClientPool};
use prkdb::storage::WalStorageAdapter;
use prkdb_types::storage::StorageAdapter;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::runtime::Runtime;

/// Benchmark Raft distributed writes
fn bench_raft_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("raft_writes");

    // Test different batch sizes
    for batch_size in [1, 10, 100, 1000].iter() {
        group.throughput(Throughput::Elements(*batch_size as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            batch_size,
            |b, &batch_size| {
                let rt = Runtime::new().unwrap();

                b.to_async(&rt).iter(|| async move {
                    // Setup cluster
                    let peers = vec![1, 2, 3];

                    let (node1, _dir1) = create_node(1, 50091, peers.clone()).await;
                    let (node2, _dir2) = create_node(2, 50092, peers.clone()).await;
                    let (node3, _dir3) = create_node(3, 50093, peers.clone()).await;

                    // Wait for leader election
                    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

                    // Find leader
                    let leader = if node1.get_leader().await.is_some() {
                        node1
                    } else if node2.get_leader().await.is_some() {
                        node2
                    } else {
                        node3
                    };

                    // Write batch_size entries
                    for i in 0..batch_size {
                        let data = format!("benchmark_value_{}", i).into_bytes();
                        let _ = leader.propose(black_box(data)).await;
                    }
                });
            },
        );
    }

    group.finish();
}

/// Benchmark single-node writes (for comparison)
fn bench_single_node_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("single_node_writes");

    for batch_size in [1, 10, 100, 1000].iter() {
        group.throughput(Throughput::Elements(*batch_size as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            batch_size,
            |b, &batch_size| {
                let rt = Runtime::new().unwrap();

                b.to_async(&rt).iter(|| async move {
                    let temp_dir = TempDir::new().unwrap();
                    let mut config = WalConfig::test_config();
                    config.log_dir = temp_dir.path().to_path_buf();
                    let storage = Arc::new(WalStorageAdapter::new(config).unwrap());

                    // Write batch_size entries directly to storage
                    for i in 0..batch_size {
                        let key = format!("key_{}", i).into_bytes();
                        let value = format!("value_{}", i).into_bytes();
                        let _ = storage.put(black_box(&key), black_box(&value)).await;
                    }
                });
            },
        );
    }

    group.finish();
}

/// Benchmark Raft latency (individual write)
fn bench_raft_latency(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    // Setup cluster once
    let (leader, _dir1, _dir2, _dir3) = rt.block_on(async {
        let peers = vec![1, 2, 3];

        let (node1, dir1) = create_node(1, 50094, peers.clone()).await;
        let (node2, dir2) = create_node(2, 50095, peers.clone()).await;
        let (node3, dir3) = create_node(3, 50096, peers.clone()).await;

        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

        let leader = if node1.get_leader().await.is_some() {
            node1
        } else if node2.get_leader().await.is_some() {
            node2
        } else {
            node3
        };

        (leader, dir1, dir2, dir3)
    });

    c.bench_function("raft_single_write_latency", |b| {
        b.to_async(&rt).iter(|| async {
            let data = b"benchmark_data".to_vec();
            let _ = leader.propose(black_box(data)).await;
        });
    });
}

use prkdb_core::wal::WalConfig;

async fn create_node(id: u64, port: u16, peers: Vec<u64>) -> (Arc<RaftNode>, TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let listen_addr = format!("127.0.0.1:{}", port).parse().unwrap();

    // Create peer nodes map (id -> addr)
    let nodes = peers
        .iter()
        .map(|&p_id| {
            (
                p_id,
                format!("127.0.0.1:{}", 50090 + p_id as u16)
                    .parse()
                    .unwrap(),
            )
        })
        .collect();

    let config = ClusterConfig {
        local_node_id: id,
        listen_addr,
        nodes,
        election_timeout_min_ms: 150,
        election_timeout_max_ms: 300,
        heartbeat_interval_ms: 25,
        partition_id: 0,
    };

    let mut wal_config = WalConfig::test_config();
    wal_config.log_dir = dir.path().to_path_buf();

    // RaftNode expects Arc<WalStorageAdapter> specifically
    let storage = Arc::new(WalStorageAdapter::new(wal_config).unwrap());
    let state_machine = Arc::new(PrkDbStateMachine::new(storage.clone()));

    // RaftNode::new is synchronous and returns Self
    let node = Arc::new(RaftNode::new(config, storage, state_machine));
    let _client_pool = Arc::new(RpcClientPool::new(id));

    (node, dir)
}

criterion_group!(
    benches,
    bench_raft_writes,
    bench_single_node_writes,
    bench_raft_latency
);
criterion_main!(benches);
