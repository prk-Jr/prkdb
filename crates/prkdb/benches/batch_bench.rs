use criterion::{black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use prkdb::prelude::*;
use serde::{Deserialize, Serialize};
use tokio::runtime::Runtime;

#[derive(Collection, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
struct BenchItem {
    #[id]
    id: u64,
    value: u64,
}

fn bench_single_vs_batch_put(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("put_operations");
    group.sample_size(15);

    for count in [100, 1_000, 10_000] {
        // Single put benchmark
        group.bench_with_input(
            BenchmarkId::new("single_put", count),
            &count,
            |b, &count| {
                b.to_async(&rt).iter_batched(
                    || {
                        PrkDb::builder()
                            .with_storage(prkdb::InMemoryAdapter::new())
                            .register_collection::<BenchItem>()
                            .build()
                            .unwrap()
                    },
                    |db| async move {
                        let coll = db.collection::<BenchItem>();
                        for i in 0..count {
                            coll.put(BenchItem {
                                id: i,
                                value: i * 2,
                            })
                            .await
                            .unwrap();
                        }
                        black_box(count)
                    },
                    BatchSize::SmallInput,
                )
            },
        );

        // Batch put benchmark
        group.bench_with_input(BenchmarkId::new("batch_put", count), &count, |b, &count| {
            b.to_async(&rt).iter_batched(
                || {
                    PrkDb::builder()
                        .with_storage(prkdb::InMemoryAdapter::new())
                        .register_collection::<BenchItem>()
                        .build()
                        .unwrap()
                },
                |db| async move {
                    let coll = db.collection::<BenchItem>();
                    let items: Vec<BenchItem> = (0..count)
                        .map(|i| BenchItem {
                            id: i,
                            value: i * 2,
                        })
                        .collect();
                    coll.put_batch(items).await.unwrap();
                    black_box(count)
                },
                BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

fn bench_single_vs_batch_get(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("get_operations");
    group.sample_size(15);

    for count in [100, 1_000, 10_000] {
        // Prepare data outside benchmark
        let prepared_db = rt.block_on(async {
            let db = PrkDb::builder()
                .with_storage(prkdb::InMemoryAdapter::new())
                .register_collection::<BenchItem>()
                .build()
                .unwrap();
            let coll = db.collection::<BenchItem>();
            for i in 0..count {
                coll.put(BenchItem {
                    id: i,
                    value: i * 2,
                })
                .await
                .unwrap();
            }
            db
        });

        // Single get benchmark
        group.bench_with_input(
            BenchmarkId::new("single_get", count),
            &count,
            |b, &count| {
                b.to_async(&rt).iter(|| {
                    let db = prepared_db.clone();
                    async move {
                        let coll = db.collection::<BenchItem>();
                        let mut total = 0;
                        for i in 0..count {
                            if let Some(item) = coll.get(&i).await.unwrap() {
                                total += item.value;
                            }
                        }
                        black_box(total)
                    }
                })
            },
        );

        // Batch get benchmark (reuse same db)
        group.bench_with_input(BenchmarkId::new("batch_get", count), &count, |b, &count| {
            b.to_async(&rt).iter(|| {
                let db = prepared_db.clone();
                async move {
                    let coll = db.collection::<BenchItem>();
                    let ids: Vec<u64> = (0..count).collect();
                    let results = coll.get_batch(ids).await.unwrap();
                    let total: u64 = results
                        .iter()
                        .filter_map(|x| x.as_ref())
                        .map(|x| x.value)
                        .sum();
                    black_box(total)
                }
            })
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_single_vs_batch_put,
    bench_single_vs_batch_get
);
criterion_main!(benches);
