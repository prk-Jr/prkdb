//! Query Layer Benchmarks
//!
//! Benchmarks for the PrkDB query layer measuring:
//! - Insert performance
//! - Query/filter performance
//! - Aggregation performance
//! - Batch operation performance
//!
//! Run with: cargo bench --bench query_bench

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use prkdb::indexed_storage::IndexedStorage;
use prkdb::storage::InMemoryAdapter;
use prkdb_macros::Collection;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

// =============================================================================
// TEST COLLECTION
// =============================================================================

#[derive(Collection, Serialize, Deserialize, Clone, Debug, PartialEq)]
struct User {
    #[id]
    id: u64,
    #[index]
    name: String,
    #[index]
    role: String,
    age: u32,
    salary: f64,
    active: bool,
}

fn create_user(id: u64) -> User {
    User {
        id,
        name: format!("User_{}", id),
        role: match id % 4 {
            0 => "admin".to_string(),
            1 => "developer".to_string(),
            2 => "manager".to_string(),
            _ => "intern".to_string(),
        },
        age: 20 + (id % 50) as u32,
        salary: 50000.0 + (id as f64 * 1000.0) % 100000.0,
        active: id % 3 != 0,
    }
}

// =============================================================================
// INSERT BENCHMARKS
// =============================================================================

fn bench_insert(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("insert");

    for size in [100, 1000, 10000].iter() {
        group.throughput(Throughput::Elements(*size as u64));

        group.bench_with_input(BenchmarkId::new("single", size), size, |b, &size| {
            b.iter(|| {
                rt.block_on(async {
                    let db = IndexedStorage::new(Arc::new(InMemoryAdapter::new()));
                    for i in 0..size {
                        let user = create_user(i as u64);
                        db.insert(&user).await.unwrap();
                    }
                    black_box(db)
                })
            })
        });

        group.bench_with_input(BenchmarkId::new("batch", size), size, |b, &size| {
            b.iter(|| {
                rt.block_on(async {
                    let db = IndexedStorage::new(Arc::new(InMemoryAdapter::new()));
                    let users: Vec<_> = (0..size).map(|i| create_user(i as u64)).collect();
                    db.insert_batch(&users).await.unwrap();
                    black_box(db)
                })
            })
        });
    }

    group.finish();
}

// =============================================================================
// QUERY BENCHMARKS
// =============================================================================

fn bench_query(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("query");

    for size in [100, 1000, 10000].iter() {
        // Setup: create database with N users
        let db = rt.block_on(async {
            let db = IndexedStorage::new(Arc::new(InMemoryAdapter::new()));
            let users: Vec<_> = (0..*size).map(|i| create_user(i as u64)).collect();
            db.insert_batch(&users).await.unwrap();
            db
        });

        group.throughput(Throughput::Elements(*size as u64));

        group.bench_with_input(BenchmarkId::new("all", size), &db, |b, db| {
            b.iter(|| {
                rt.block_on(async {
                    let users: Vec<User> = db.all().await.unwrap();
                    black_box(users)
                })
            })
        });

        group.bench_with_input(BenchmarkId::new("filter_50pct", size), &db, |b, db| {
            b.iter(|| {
                rt.block_on(async {
                    let users = db
                        .query::<User>()
                        .filter(|u| u.active)
                        .collect()
                        .await
                        .unwrap();
                    black_box(users)
                })
            })
        });

        group.bench_with_input(BenchmarkId::new("filter_10pct", size), &db, |b, db| {
            b.iter(|| {
                rt.block_on(async {
                    let users = db
                        .query::<User>()
                        .filter(|u| u.role == "admin")
                        .collect()
                        .await
                        .unwrap();
                    black_box(users)
                })
            })
        });

        group.bench_with_input(BenchmarkId::new("order_take_10", size), &db, |b, db| {
            b.iter(|| {
                rt.block_on(async {
                    let users = db
                        .query::<User>()
                        .order_by_desc(|u| u.salary as i64)
                        .take(10)
                        .collect()
                        .await
                        .unwrap();
                    black_box(users)
                })
            })
        });

        group.bench_with_input(BenchmarkId::new("first", size), &db, |b, db| {
            b.iter(|| {
                rt.block_on(async {
                    let user = db
                        .query::<User>()
                        .filter(|u| u.role == "admin")
                        .first()
                        .await
                        .unwrap();
                    black_box(user)
                })
            })
        });
    }

    group.finish();
}

// =============================================================================
// AGGREGATION BENCHMARKS
// =============================================================================

fn bench_aggregation(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("aggregation");

    for size in [100, 1000, 10000].iter() {
        let db = rt.block_on(async {
            let db = IndexedStorage::new(Arc::new(InMemoryAdapter::new()));
            let users: Vec<_> = (0..*size).map(|i| create_user(i as u64)).collect();
            db.insert_batch(&users).await.unwrap();
            db
        });

        group.throughput(Throughput::Elements(*size as u64));

        group.bench_with_input(BenchmarkId::new("count", size), &db, |b, db| {
            b.iter(|| {
                rt.block_on(async {
                    let count = db.query::<User>().count().await.unwrap();
                    black_box(count)
                })
            })
        });

        group.bench_with_input(BenchmarkId::new("sum_by", size), &db, |b, db| {
            b.iter(|| {
                rt.block_on(async {
                    let sums = db
                        .query::<User>()
                        .sum_by(|u| u.role.clone(), |u| u.salary)
                        .await
                        .unwrap();
                    black_box(sums)
                })
            })
        });

        group.bench_with_input(BenchmarkId::new("group_by", size), &db, |b, db| {
            b.iter(|| {
                rt.block_on(async {
                    let groups = db
                        .query::<User>()
                        .group_by(|u| u.role.clone())
                        .await
                        .unwrap();
                    black_box(groups)
                })
            })
        });

        group.bench_with_input(BenchmarkId::new("avg_by", size), &db, |b, db| {
            b.iter(|| {
                rt.block_on(async {
                    let avg = db.query::<User>().avg_by(|u| u.salary).await.unwrap();
                    black_box(avg)
                })
            })
        });

        group.bench_with_input(BenchmarkId::new("min_by", size), &db, |b, db| {
            b.iter(|| {
                rt.block_on(async {
                    let min = db.query::<User>().min_by(|u| u.age).await.unwrap();
                    black_box(min)
                })
            })
        });
    }

    group.finish();
}

// =============================================================================
// BATCH OPERATION BENCHMARKS
// =============================================================================

fn bench_batch_ops(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("batch_ops");

    for size in [100, 1000].iter() {
        group.throughput(Throughput::Elements(*size as u64));

        group.bench_with_input(BenchmarkId::new("update_where", size), size, |b, &size| {
            b.iter(|| {
                rt.block_on(async {
                    let db = IndexedStorage::new(Arc::new(InMemoryAdapter::new()));
                    let users: Vec<_> = (0..size).map(|i| create_user(i as u64)).collect();
                    db.insert_batch(&users).await.unwrap();

                    let updated = db
                        .update_where::<User, _, _>(|u| u.role == "developer", |u| u.salary *= 1.1)
                        .await
                        .unwrap();

                    black_box(updated)
                })
            })
        });

        group.bench_with_input(BenchmarkId::new("delete_where", size), size, |b, &size| {
            b.iter(|| {
                rt.block_on(async {
                    let db = IndexedStorage::new(Arc::new(InMemoryAdapter::new()));
                    let users: Vec<_> = (0..size).map(|i| create_user(i as u64)).collect();
                    db.insert_batch(&users).await.unwrap();

                    let deleted = db.delete_where::<User, _>(|u| !u.active).await.unwrap();

                    black_box(deleted)
                })
            })
        });
    }

    group.finish();
}

// =============================================================================
// CRITERION MAIN
// =============================================================================

criterion_group!(
    benches,
    bench_insert,
    bench_query,
    bench_aggregation,
    bench_batch_ops
);
criterion_main!(benches);
