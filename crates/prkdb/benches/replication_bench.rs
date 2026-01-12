use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use prkdb::prelude::{Collection, PrkDb};
use prkdb::replication::{ReplicaNode, ReplicationConfig, ReplicationManager, ReplicationTiming};
use prkdb::storage::InMemoryAdapter;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::runtime::Runtime;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
struct BenchmarkItem {
    id: u64,
    data: String,
    value: i32,
    active: bool,
}

impl Collection for BenchmarkItem {
    type Id = u64;
    fn id(&self) -> &Self::Id {
        &self.id
    }
}

fn create_test_db() -> Arc<PrkDb> {
    Arc::new(
        PrkDb::builder()
            .with_storage(InMemoryAdapter::new())
            .register_collection::<BenchmarkItem>()
            .build()
            .unwrap(),
    )
}

fn create_leader_replication_manager() -> ReplicationManager {
    let db = create_test_db();
    let config = ReplicationConfig {
        self_node: ReplicaNode {
            id: "leader_bench".to_string(),
            address: "localhost:8090".to_string(),
        },
        leader_address: None,
        followers: vec![],
        timing: ReplicationTiming::default(),
    };
    ReplicationManager::new(db, config)
}

fn bench_replication_save_outbox(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let manager = create_leader_replication_manager();

    let mut group = c.benchmark_group("replication_save_outbox");
    group.throughput(Throughput::Elements(1));

    group.bench_function("single_put_event", |b| {
        b.to_async(&rt).iter(|| async {
            let item = BenchmarkItem {
                id: black_box(rand::random::<u64>()),
                data: black_box("benchmark data".to_string()),
                value: black_box(42),
                active: black_box(true),
            };

            let change = prkdb_types::collection::ChangeEvent::Put(item);
            let result = manager.replicate_change(&change).await;
            result.unwrap();
        })
    });

    group.bench_function("single_delete_event", |b| {
        b.to_async(&rt).iter(|| async {
            let id = black_box(rand::random::<u64>());
            let change: prkdb_types::collection::ChangeEvent<BenchmarkItem> =
                prkdb_types::collection::ChangeEvent::Delete(id);
            let result = manager.replicate_change(&change).await;
            result.unwrap();
        })
    });

    group.finish();
}

fn bench_replication_batch_operations(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let manager = create_leader_replication_manager();

    let mut group = c.benchmark_group("replication_batch_operations");

    for batch_size in [10, 100, 1000].iter() {
        group.throughput(Throughput::Elements(*batch_size));
        group.bench_with_input(
            format!("batch_put_{}", batch_size),
            batch_size,
            |b, &size| {
                b.to_async(&rt).iter(|| async {
                    for i in 0..size {
                        let item = BenchmarkItem {
                            id: black_box(i),
                            data: black_box(format!("batch item {}", i)),
                            value: black_box(i as i32),
                            active: black_box(i % 2 == 0),
                        };

                        let change = prkdb_types::collection::ChangeEvent::Put(item);
                        let result = manager.replicate_change(&change).await;
                        result.unwrap();
                    }
                })
            },
        );
    }

    group.finish();
}

fn bench_replication_get_changes_since(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let manager = create_leader_replication_manager();

    // Pre-populate with some changes
    rt.block_on(async {
        for i in 0..1000 {
            let item = BenchmarkItem {
                id: i,
                data: format!("setup item {}", i),
                value: i as i32,
                active: true,
            };
            let change = prkdb_types::collection::ChangeEvent::Put(item);
            let _ = manager.replicate_change(&change).await;
        }
    });

    let mut group = c.benchmark_group("replication_get_changes");

    for limit in [10, 50, 100, 500].iter() {
        group.throughput(Throughput::Elements(*limit));
        group.bench_with_input(format!("get_changes_limit_{}", limit), limit, |b, &size| {
            b.to_async(&rt).iter(|| async {
                let changes = manager.get_changes_since(None, size as usize).await;
                black_box(changes.unwrap());
            })
        });
    }

    group.finish();
}

fn bench_replication_mixed_workload(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let manager = create_leader_replication_manager();

    let mut group = c.benchmark_group("replication_mixed_workload");
    group.throughput(Throughput::Elements(100));

    group.bench_function("mixed_put_delete_get", |b| {
        b.to_async(&rt).iter(|| async {
            // Mix of operations: 70% puts, 20% deletes, 10% gets
            for i in 0..100 {
                match i % 10 {
                    0 => {
                        // Get changes
                        let changes = manager.get_changes_since(None, 10).await;
                        black_box(changes.unwrap());
                    }
                    1 | 2 => {
                        // Delete operations
                        let id = black_box(rand::random::<u64>());
                        let change: prkdb_types::collection::ChangeEvent<BenchmarkItem> =
                            prkdb_types::collection::ChangeEvent::Delete(id);
                        let result = manager.replicate_change(&change).await;
                        result.unwrap();
                    }
                    _ => {
                        // Put operations
                        let item = BenchmarkItem {
                            id: black_box(rand::random::<u64>()),
                            data: black_box(format!("mixed workload {}", i)),
                            value: black_box(i),
                            active: black_box(i % 3 == 0),
                        };
                        let change = prkdb_types::collection::ChangeEvent::Put(item);
                        let result = manager.replicate_change(&change).await;
                        result.unwrap();
                    }
                }
            }
        })
    });

    group.finish();
}

fn bench_replication_large_items(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let manager = create_leader_replication_manager();

    let mut group = c.benchmark_group("replication_large_items");

    for data_size in [1_000, 10_000, 100_000].iter() {
        group.throughput(Throughput::Bytes(*data_size as u64));
        group.bench_with_input(
            format!("large_item_{}bytes", data_size),
            data_size,
            |b, &size| {
                b.to_async(&rt).iter(|| async {
                    let large_data = "x".repeat(size);
                    let item = BenchmarkItem {
                        id: black_box(rand::random::<u64>()),
                        data: black_box(large_data),
                        value: black_box(42),
                        active: black_box(true),
                    };

                    let change = prkdb_types::collection::ChangeEvent::Put(item);
                    let result = manager.replicate_change(&change).await;
                    result.unwrap();
                })
            },
        );
    }

    group.finish();
}

fn bench_replication_concurrent_outbox(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let manager = Arc::new(create_leader_replication_manager());

    let mut group = c.benchmark_group("replication_concurrent");
    group.throughput(Throughput::Elements(100));

    group.bench_function("concurrent_outbox_writes", |b| {
        b.to_async(&rt).iter(|| async {
            let mut handles = vec![];

            for i in 0..10 {
                let manager_clone = manager.clone();
                let handle = tokio::spawn(async move {
                    for j in 0..10 {
                        let item = BenchmarkItem {
                            id: black_box((i * 10 + j) as u64),
                            data: black_box(format!("concurrent item {}:{}", i, j)),
                            value: black_box(j),
                            active: black_box(true),
                        };

                        let change = prkdb_types::collection::ChangeEvent::Put(item);
                        let _ = manager_clone.replicate_change(&change).await;
                    }
                });
                handles.push(handle);
            }

            for handle in handles {
                handle.await.unwrap();
            }
        })
    });

    group.finish();
}

criterion_group!(
    replication_benches,
    bench_replication_save_outbox,
    bench_replication_batch_operations,
    bench_replication_get_changes_since,
    bench_replication_mixed_workload,
    bench_replication_large_items,
    bench_replication_concurrent_outbox
);

criterion_main!(replication_benches);
