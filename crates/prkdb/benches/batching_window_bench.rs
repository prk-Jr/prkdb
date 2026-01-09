use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use prkdb::prelude::*;
use prkdb_core::batch_config::BatchConfig;
use serde::{Deserialize, Serialize};

#[derive(Collection, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
struct BenchItem {
    #[id]
    id: u64,
    value: String,
}

fn batching_window_benchmarks(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("batching_window");

    // Test with different batch sizes
    for size in [100, 1000, 5000].iter() {
        // Benchmark WITHOUT batching (baseline)
        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::new("no_batching", size), size, |b, &size| {
            b.to_async(&runtime).iter(|| async move {
                let db = PrkDb::builder().build().unwrap();
                let handle = db.collection::<BenchItem>();

                for i in 0..size {
                    let item = BenchItem {
                        id: i as u64,
                        value: format!("value_{}", i),
                    };
                    black_box(handle.put(item).await.unwrap());
                }
            });
        });

        // Benchmark WITH batching (default config: 10ms linger, 1000 batch size)
        group.bench_with_input(
            BenchmarkId::new("with_batching_default", size),
            size,
            |b, &size| {
                b.to_async(&runtime).iter(|| async move {
                    let db = PrkDb::builder().build().unwrap();
                    let handle = db
                        .collection::<BenchItem>()
                        .with_batching(BatchConfig::default());

                    for i in 0..size {
                        let item = BenchItem {
                            id: i as u64,
                            value: format!("value_{}", i),
                        };
                        black_box(handle.put(item).await.unwrap());
                    }
                });
            },
        );

        // Benchmark WITH batching (throughput optimized: 50ms linger, 10K batch size)
        group.bench_with_input(
            BenchmarkId::new("with_batching_throughput", size),
            size,
            |b, &size| {
                b.to_async(&runtime).iter(|| async move {
                    let db = PrkDb::builder().build().unwrap();
                    let handle = db
                        .collection::<BenchItem>()
                        .with_batching(BatchConfig::throughput_optimized());

                    for i in 0..size {
                        let item = BenchItem {
                            id: i as u64,
                            value: format!("value_{}", i),
                        };
                        black_box(handle.put(item).await.unwrap());
                    }
                });
            },
        );

        // Benchmark WITH batching (latency optimized: 1ms linger, 100 batch size)
        group.bench_with_input(
            BenchmarkId::new("with_batching_latency", size),
            size,
            |b, &size| {
                b.to_async(&runtime).iter(|| async move {
                    let db = PrkDb::builder().build().unwrap();
                    let handle = db
                        .collection::<BenchItem>()
                        .with_batching(BatchConfig::latency_optimized());

                    for i in 0..size {
                        let item = BenchItem {
                            id: i as u64,
                            value: format!("value_{}", i),
                        };
                        black_box(handle.put(item).await.unwrap());
                    }
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, batching_window_benchmarks);
criterion_main!(benches);
