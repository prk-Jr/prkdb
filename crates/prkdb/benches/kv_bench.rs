use criterion::{black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use prkdb::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::runtime::Runtime;

#[derive(Collection, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
struct BenchRec {
    #[id]
    id: u64,
    v: u64,
}

#[derive(Clone, Debug)]
struct MockKvStore {
    data: HashMap<u64, u64>,
}

impl MockKvStore {
    fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }

    fn put(&mut self, key: u64, value: u64) -> Result<(), &'static str> {
        self.data.insert(key, value);
        Ok(())
    }

    fn get(&self, key: &u64) -> Option<u64> {
        self.data.get(key).copied()
    }

    fn delete(&mut self, key: &u64) -> bool {
        self.data.remove(key).is_some()
    }

    fn scan(&self, start: u64, end: u64) -> Vec<(u64, u64)> {
        let mut results = Vec::new();
        for (&key, &value) in &self.data {
            if key >= start && key <= end {
                results.push((key, value));
            }
        }
        results.sort_by_key(|&(k, _)| k);
        results
    }

    fn size(&self) -> usize {
        self.data.len()
    }
}

fn bench_kv_write_throughput(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("kv_write_throughput");
    group.sample_size(20);

    for record_count in [1_000, 10_000, 100_000] {
        group.bench_with_input(
            BenchmarkId::new("sequential_writes", format!("{}records", record_count)),
            &record_count,
            |b, &record_count| {
                b.to_async(&rt).iter(|| async {
                    let mut store = MockKvStore::new();

                    for i in 0..record_count {
                        store.put(i, i * 2).unwrap();
                    }

                    black_box(store.size())
                });
            },
        );
    }
    group.finish();
}

fn bench_kv_read_throughput(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("kv_read_throughput");
    group.sample_size(20);

    for record_count in [1_000, 10_000, 100_000] {
        group.bench_with_input(
            BenchmarkId::new("sequential_reads", format!("{}records", record_count)),
            &record_count,
            |b, &record_count| {
                b.to_async(&rt).iter_batched(
                    || {
                        let mut store = MockKvStore::new();
                        // Pre-populate with data
                        for i in 0..record_count {
                            store.put(i, i * 2).unwrap();
                        }
                        store
                    },
                    |store| async move {
                        let mut total_reads = 0;

                        for i in 0..record_count {
                            if let Some(value) = store.get(&i) {
                                black_box(value);
                                total_reads += 1;
                            }
                        }

                        black_box(total_reads)
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

fn bench_kv_mixed_operations(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("kv_mixed_operations");
    group.sample_size(15);

    for operation_count in [1_000, 5_000, 10_000] {
        group.bench_with_input(
            BenchmarkId::new("read_write_delete", format!("{}ops", operation_count)),
            &operation_count,
            |b, &operation_count| {
                b.to_async(&rt).iter(|| async {
                    let mut store = MockKvStore::new();
                    let mut ops_completed = 0;

                    for i in 0..operation_count {
                        match i % 3 {
                            0 => {
                                // Write operation
                                store.put(i, i * 3).unwrap();
                                ops_completed += 1;
                            }
                            1 => {
                                // Read operation
                                if let Some(value) = store.get(&(i / 2)) {
                                    black_box(value);
                                }
                                ops_completed += 1;
                            }
                            2 => {
                                // Delete operation
                                store.delete(&(i / 3));
                                ops_completed += 1;
                            }
                            _ => unreachable!(),
                        }
                    }

                    black_box(ops_completed)
                });
            },
        );
    }
    group.finish();
}

fn bench_kv_scan_operations(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("kv_scan_operations");
    group.sample_size(15);

    for scan_range in [100, 1_000, 10_000] {
        group.bench_with_input(
            BenchmarkId::new("range_scan", format!("{}range", scan_range)),
            &scan_range,
            |b, &scan_range| {
                b.to_async(&rt).iter_batched(
                    || {
                        let mut store = MockKvStore::new();
                        // Pre-populate with sparse data
                        for i in 0..scan_range * 2 {
                            if i % 3 == 0 {
                                store.put(i, i * 2).unwrap();
                            }
                        }
                        store
                    },
                    |store| async move {
                        let start = scan_range / 4;
                        let end = start + scan_range;

                        let results = store.scan(start, end);
                        black_box(results.len())
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

fn bench_kv_concurrent_access(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("kv_concurrent_access");
    group.sample_size(10);

    for worker_count in [2, 4, 8] {
        group.bench_with_input(
            BenchmarkId::new("concurrent_workers", format!("{}workers", worker_count)),
            &worker_count,
            |b, &worker_count| {
                b.to_async(&rt).iter(|| async {
                    let mut handles = Vec::new();

                    for worker_id in 0..worker_count {
                        let handle = tokio::spawn(async move {
                            let mut store = MockKvStore::new();
                            let mut operations = 0;

                            // Each worker operates on its own key range to avoid conflicts
                            let key_base = worker_id as u64 * 1000;

                            for i in 0..500 {
                                let key = key_base + i;

                                // Write
                                store.put(key, key * 2).unwrap();
                                operations += 1;

                                // Read
                                if let Some(value) = store.get(&key) {
                                    black_box(value);
                                }
                                operations += 1;

                                // Scan every 10 operations
                                if i % 10 == 0 {
                                    let results = store.scan(key_base, key_base + 50);
                                    black_box(results.len());
                                    operations += 1;
                                }
                            }

                            operations
                        });
                        handles.push(handle);
                    }

                    let mut total_operations = 0;
                    for handle in handles {
                        total_operations += handle.await.unwrap_or(0);
                    }

                    black_box(total_operations)
                });
            },
        );
    }
    group.finish();
}

fn bench_kv_large_values(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("kv_large_values");
    group.sample_size(10);

    for value_size in [1_024, 10_240, 102_400] {
        // 1KB, 10KB, 100KB
        group.bench_with_input(
            BenchmarkId::new("large_value_operations", format!("{}bytes", value_size)),
            &value_size,
            |b, &value_size| {
                b.to_async(&rt).iter(|| async {
                    let mut large_value_store: HashMap<u64, Vec<u8>> = HashMap::new();
                    let large_value = vec![0u8; value_size];

                    let mut operations = 0;

                    // Write large values
                    for i in 0..100 {
                        large_value_store.insert(i, large_value.clone());
                        operations += 1;
                    }

                    // Read large values
                    for i in 0..100 {
                        if let Some(value) = large_value_store.get(&i) {
                            black_box(value.len());
                        }
                        operations += 1;
                    }

                    black_box(operations)
                });
            },
        );
    }
    group.finish();
}

fn bench_put_get_original(c: &mut Criterion) {
    let mut group = c.benchmark_group("kv_original");
    group.bench_function("inmem_put_get", |b| {
        b.to_async(tokio::runtime::Runtime::new().unwrap())
            .iter_batched(
                || {
                    PrkDb::builder()
                        .with_storage(prkdb::storage::InMemoryAdapter::new())
                        .register_collection::<BenchRec>()
                        .build()
                        .unwrap()
                },
                |db| async move {
                    let coll = db.collection::<BenchRec>();
                    for i in 0..10_000u64 {
                        coll.put(BenchRec { id: i, v: i }).await.unwrap();
                        let _ = coll.get(&i).await.unwrap();
                    }
                },
                BatchSize::SmallInput,
            )
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_kv_write_throughput,
    bench_kv_read_throughput,
    bench_kv_mixed_operations,
    bench_kv_scan_operations,
    bench_kv_concurrent_access,
    bench_kv_large_values,
    bench_put_get_original
);
criterion_main!(benches);
