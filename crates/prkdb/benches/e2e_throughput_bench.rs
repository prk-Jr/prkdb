use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use prkdb::prelude::*;
use prkdb_types::consumer::{AutoOffsetReset, Consumer, ConsumerConfig};
use serde::{Deserialize, Serialize};
use tokio::runtime::Runtime;

#[derive(Collection, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
struct BenchEvent {
    #[id]
    id: u64,
    data: Vec<u8>,
}

/// Benchmark end-to-end throughput: put() → WAL → consumer.poll()
/// This measures the TRUE baseline performance of the full stack
fn bench_e2e_throughput_single(c: &mut Criterion) {
    let mut group = c.benchmark_group("e2e_throughput");
    group.throughput(Throughput::Elements(1));

    let rt = Runtime::new().unwrap();

    group.bench_function("single_event_1kb", |b| {
        b.to_async(&rt).iter_batched(
            || {
                // Setup: Create fresh DB for each iteration
                let db = PrkDb::builder()
                    .with_storage(prkdb::InMemoryAdapter::new())
                    .register_collection::<BenchEvent>()
                    .build()
                    .unwrap();

                let config = ConsumerConfig {
                    group_id: "bench".to_string(),
                    auto_offset_reset: AutoOffsetReset::Earliest,
                    auto_commit: false,
                    ..Default::default()
                };

                (db, config)
            },
            |(db, config)| async move {
                // Write event
                let coll = db.collection::<BenchEvent>();
                let event = BenchEvent {
                    id: 1,
                    data: vec![0u8; 1024],
                };
                coll.put(event).await.unwrap();

                // Read event via consumer
                let mut consumer = db.consumer::<BenchEvent>(config).await.unwrap();
                let records = consumer.poll().await.unwrap();

                black_box(records);
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

/// Benchmark batch end-to-end throughput
fn bench_e2e_throughput_batch(c: &mut Criterion) {
    for batch_size in [10, 100, 1000] {
        let mut group = c.benchmark_group(format!("e2e_batch_{}", batch_size));
        group.throughput(Throughput::Elements(batch_size as u64));

        let rt = Runtime::new().unwrap();

        group.bench_function("batch_put_poll", |b| {
            b.to_async(&rt).iter_batched(
                || {
                    // Setup: Create fresh DB for each iteration
                    let db = PrkDb::builder()
                        .with_storage(prkdb::InMemoryAdapter::new())
                        .register_collection::<BenchEvent>()
                        .build()
                        .unwrap();

                    let config = ConsumerConfig {
                        group_id: "bench".to_string(),
                        auto_offset_reset: AutoOffsetReset::Earliest,
                        auto_commit: false,
                        max_poll_records: batch_size,
                        ..Default::default()
                    };

                    (db, config, batch_size)
                },
                |(db, config, size)| async move {
                    // Write batch of events
                    let coll = db.collection::<BenchEvent>();
                    let events: Vec<BenchEvent> = (0..size)
                        .map(|i| BenchEvent {
                            id: i as u64,
                            data: vec![0u8; 1024],
                        })
                        .collect();

                    coll.put_batch(events).await.unwrap();

                    // Read batch via consumer
                    let mut consumer = db.consumer::<BenchEvent>(config).await.unwrap();
                    let records = consumer.poll().await.unwrap();

                    black_box(records);
                },
                BatchSize::SmallInput,
            )
        });

        group.finish();
    }
}

/// Benchmark with WAL storage adapter (more realistic)
fn bench_e2e_throughput_wal(c: &mut Criterion) {
    let mut group = c.benchmark_group("e2e_wal");
    group.throughput(Throughput::Elements(100));

    let rt = Runtime::new().unwrap();

    group.bench_function("batch_100_wal", |b| {
        b.to_async(&rt).iter_batched(
            || {
                let dir = tempfile::tempdir().unwrap();

                // Setup: Create DB with WAL storage
                let db = PrkDb::builder()
                    .with_data_dir(dir.path())
                    .register_collection::<BenchEvent>()
                    .build()
                    .unwrap();

                let config = ConsumerConfig {
                    group_id: "bench".to_string(),
                    auto_offset_reset: AutoOffsetReset::Earliest,
                    auto_commit: false,
                    max_poll_records: 100,
                    ..Default::default()
                };

                (db, config, dir)
            },
            |(db, config, _dir)| async move {
                // Write batch
                let coll = db.collection::<BenchEvent>();
                let events: Vec<BenchEvent> = (0..100)
                    .map(|i| BenchEvent {
                        id: i,
                        data: vec![0u8; 1024],
                    })
                    .collect();

                coll.put_batch(events).await.unwrap();

                // Read batch
                let mut consumer = db.consumer::<BenchEvent>(config).await.unwrap();
                let records = consumer.poll().await.unwrap();

                black_box(records);
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_e2e_throughput_single,
    bench_e2e_throughput_batch,
    bench_e2e_throughput_wal
);
criterion_main!(benches);
