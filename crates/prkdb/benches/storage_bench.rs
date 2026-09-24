use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use prkdb::storage::WalStorageAdapter;
use prkdb_core::wal::WalConfig;
use prkdb_types::storage::StorageAdapter;
use std::sync::Arc;
use tokio::runtime::Runtime;

fn bench_single_put(c: &mut Criterion) {
    let mut group = c.benchmark_group("storage_put");
    group.throughput(Throughput::Elements(1));

    let rt = Runtime::new().unwrap();

    // Adapter lifecycle (tempdir + WalStorageAdapter::new) is created once, outside the
    // timed region. The original version created and tore down a fresh adapter inside
    // `b.iter`'s async block, so the ~1.2ms it reported was mostly adapter setup, not the
    // put itself.
    let dir = tempfile::tempdir().unwrap();
    let config = WalConfig {
        log_dir: dir.path().to_path_buf(),
        ..WalConfig::test_config()
    };
    let adapter = Arc::new(rt.block_on(async { WalStorageAdapter::new(config).unwrap() }));

    group.bench_function("single_put", |b| {
        let mut counter = 0u64;

        b.to_async(&rt).iter(|| {
            let adapter = Arc::clone(&adapter);
            let key = format!("bench_key_{counter}").into_bytes();
            counter += 1;
            let value = vec![b'x'; 100];

            async move {
                adapter.put(&key, &value).await.unwrap();
                black_box(());
            }
        });
    });

    group.finish();
}

fn bench_batch_put(c: &mut Criterion) {
    let mut group = c.benchmark_group("storage_batch");
    group.throughput(Throughput::Elements(100));

    let rt = Runtime::new().unwrap();

    // Same fix as bench_single_put: the adapter is built once, outside the timed region.
    let dir = tempfile::tempdir().unwrap();
    let config = WalConfig {
        log_dir: dir.path().to_path_buf(),
        ..WalConfig::test_config()
    };
    let adapter = Arc::new(rt.block_on(async { WalStorageAdapter::new(config).unwrap() }));

    group.bench_function("batch_put_100", |b| {
        let mut batch_counter = 0u64;

        b.to_async(&rt).iter(|| {
            let adapter = Arc::clone(&adapter);
            let mut items = vec![];
            for i in 0..100 {
                let key = format!("batch_key_{batch_counter}_{i}").into_bytes();
                let value = vec![b'x'; 100];
                items.push((key, value));
            }
            batch_counter += 1;

            async move {
                adapter.put_many(items).await.unwrap();
                black_box(());
            }
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
                        adapter.put(&key, &value).await.unwrap();
                        black_box(());
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
