use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use prkdb::storage::wal_adapter::WalStorageAdapter;
use prkdb_core::storage::StorageAdapter;
use prkdb_core::wal::WalConfig;
use std::sync::Arc;
use tokio::runtime::Runtime;

fn bench_single_put(c: &mut Criterion) {
    let mut group = c.benchmark_group("storage_put");
    group.throughput(Throughput::Elements(1));

    let rt = Runtime::new().unwrap();

    group.bench_function("single_put", |b| {
        b.to_async(&rt).iter(|| async move {
            // Create fresh adapter for each iteration to avoid lifetime issues
            let dir = tempfile::tempdir().unwrap();
            let config = WalConfig {
                log_dir: dir.path().to_path_buf(),
                ..WalConfig::test_config()
            };
            let adapter = WalStorageAdapter::new(config).unwrap();

            let key = b"bench_key".to_vec();
            let value = vec![b'x'; 100];

            black_box(adapter.put(&key, &value).await.unwrap());
        });
    });

    group.finish();
}

fn bench_batch_put(c: &mut Criterion) {
    let mut group = c.benchmark_group("storage_batch");
    group.throughput(Throughput::Elements(100));

    let rt = Runtime::new().unwrap();

    group.bench_function("batch_put_100", |b| {
        b.to_async(&rt).iter(|| async move {
            let dir = tempfile::tempdir().unwrap();
            let config = WalConfig {
                log_dir: dir.path().to_path_buf(),
                ..WalConfig::test_config()
            };
            let adapter = WalStorageAdapter::new(config).unwrap();

            let mut items = vec![];
            for i in 0..100 {
                let key = format!("batch_key_{}", i).into_bytes();
                let value = vec![b'x'; 100];
                items.push((key, value));
            }

            black_box(adapter.put_many(items).await.unwrap());
        });
    });

    group.finish();
}

fn bench_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("storage_get");
    group.throughput(Throughput::Elements(1));

    let rt = Runtime::new().unwrap();

    // Setup: create adapter and pre-populate data once
    let dir = tempfile::tempdir().unwrap();
    let config = WalConfig {
        log_dir: dir.path().to_path_buf(),
        ..WalConfig::test_config()
    };
    let adapter = Arc::new(rt.block_on(async {
        let adapter = WalStorageAdapter::new(config).unwrap();
        for i in 0..1000 {
            let key = format!("get_key_{}", i).into_bytes();
            let value = vec![b'x'; 100];
            adapter.put(&key, &value).await.unwrap();
        }
        adapter
    }));

    group.bench_function("single_get", |b| {
        let mut counter = 0;

        b.to_async(&rt).iter(|| {
            let adapter = Arc::clone(&adapter);
            let key_index = counter;
            counter += 1;

            async move {
                let key = format!("get_key_{}", key_index % 1000).into_bytes();
                black_box(adapter.get(&key).await.unwrap());
            }
        });
    });

    group.finish();
}

fn bench_mixed_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("storage_mixed");
    group.throughput(Throughput::Elements(10));

    let rt = Runtime::new().unwrap();

    let dir = tempfile::tempdir().unwrap();
    let config = WalConfig {
        log_dir: dir.path().to_path_buf(),
        ..WalConfig::test_config()
    };
    let adapter = Arc::new(rt.block_on(async {
        let adapter = WalStorageAdapter::new(config).unwrap();
        for i in 0..1000 {
            let key = format!("mixed_key_{}", i).into_bytes();
            let value = vec![b'x'; 100];
            adapter.put(&key, &value).await.unwrap();
        }
        adapter
    }));

    group.bench_function("mixed_70_30", |b| {
        let mut counter = 0;

        b.to_async(&rt).iter(|| {
            let adapter = Arc::clone(&adapter);
            let start_counter = counter;
            counter += 10;

            async move {
                for i in 0..10 {
                    if i < 7 {
                        // 70% reads
                        let key = format!("mixed_key_{}", (start_counter + i) % 1000).into_bytes();
                        black_box(adapter.get(&key).await.unwrap());
                    } else {
                        // 30% writes
                        let key = format!("mixed_new_{}", start_counter + i).into_bytes();
                        let value = vec![b'x'; 100];
                        black_box(adapter.put(&key, &value).await.unwrap());
                    }
                }
            }
        });
    });

    group.finish();
}

fn bench_cache_performance(c: &mut Criterion) {
    let mut group = c.benchmark_group("storage_cache");
    group.throughput(Throughput::Elements(100));

    let rt = Runtime::new().unwrap();

    let dir = tempfile::tempdir().unwrap();
    let config = WalConfig {
        log_dir: dir.path().to_path_buf(),
        ..WalConfig::test_config()
    };
    let adapter = Arc::new(rt.block_on(async {
        let adapter = WalStorageAdapter::new(config).unwrap();
        for i in 0..100 {
            let key = format!("cache_key_{}", i).into_bytes();
            let value = vec![b'x'; 100];
            adapter.put(&key, &value).await.unwrap();
        }
        adapter
    }));

    group.bench_function("cache_hit_rate", |b| {
        let mut counter = 0;

        b.to_async(&rt).iter(|| {
            let adapter = Arc::clone(&adapter);
            let start_counter = counter;
            counter += 100;

            async move {
                for i in 0..100 {
                    let key = format!("cache_key_{}", (start_counter + i) % 10).into_bytes();
                    black_box(adapter.get(&key).await.unwrap());
                }
            }
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_single_put,
    bench_batch_put,
    bench_get,
    bench_mixed_workload,
    bench_cache_performance
);
criterion_main!(benches);
