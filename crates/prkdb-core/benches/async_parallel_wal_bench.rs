use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use prkdb_core::wal::async_parallel_wal::AsyncParallelWal;
use prkdb_core::wal::{LogOperation, LogRecord, ParallelWal, WalConfig, WriteAheadLog};
use std::sync::Arc;

/// Benchmark single WAL (baseline)
fn bench_single_wal_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_comparison_async");
    let batch_size = 100;
    group.throughput(Throughput::Elements(batch_size as u64));

    let dir = tempfile::tempdir().unwrap();
    let config = WalConfig {
        log_dir: dir.path().to_path_buf(),
        ..WalConfig::test_config()
    };

    let wal = Arc::new(WriteAheadLog::create(config).unwrap());

    group.bench_function("single_wal_batch_100", |b| {
        b.iter(|| {
            let records: Vec<_> = (0..batch_size)
                .map(|i| {
                    LogRecord::new(LogOperation::Put {
                        collection: format!("coll_{}", i % 10),
                        id: vec![i as u8],
                        data: vec![0u8; 1024],
                    })
                })
                .collect();

            black_box(wal.append_batch(records).unwrap());
        })
    });

    group.finish();
}

/// Benchmark synchronous parallel WAL (4 segments)
fn bench_sync_parallel_wal(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_comparison_async");
    let batch_size = 100;
    group.throughput(Throughput::Elements(batch_size as u64));

    let dir = tempfile::tempdir().unwrap();
    let config = WalConfig {
        log_dir: dir.path().to_path_buf(),
        ..WalConfig::test_config()
    };

    let wal = Arc::new(ParallelWal::create(config, 4).unwrap());

    let rt = tokio::runtime::Runtime::new().unwrap();

    group.bench_function("sync_parallel_wal_4seg_batch_100", |b| {
        b.iter(|| {
            rt.block_on(async {
                let records: Vec<_> = (0..batch_size)
                    .map(|i| {
                        LogRecord::new(LogOperation::Put {
                            collection: format!("coll_{}", i % 10),
                            id: vec![i as u8],
                            data: vec![0u8; 1024],
                        })
                    })
                    .collect();

                black_box(wal.append_batch(records).await.unwrap());
            })
        })
    });

    group.finish();
}

/// Benchmark ASYNC parallel WAL (4 segments)
fn bench_async_parallel_wal(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_comparison_async");
    let batch_size = 100;
    group.throughput(Throughput::Elements(batch_size as u64));

    let dir = tempfile::tempdir().unwrap();
    let config = WalConfig {
        log_dir: dir.path().to_path_buf(),
        ..WalConfig::test_config()
    };

    // We need a runtime to create the async wal
    let rt = tokio::runtime::Runtime::new().unwrap();
    let wal = rt.block_on(async { Arc::new(AsyncParallelWal::create(config, 4).await.unwrap()) });

    group.bench_function("async_parallel_wal_4seg_batch_100", |b| {
        b.iter(|| {
            rt.block_on(async {
                let records: Vec<_> = (0..batch_size)
                    .map(|i| {
                        LogRecord::new(LogOperation::Put {
                            collection: format!("coll_{}", i % 10),
                            id: vec![i as u8],
                            data: vec![0u8; 1024],
                        })
                    })
                    .collect();

                black_box(wal.append_batch(records).await.unwrap());
            })
        })
    });

    group.finish();
}

/// Benchmark ASYNC parallel WAL scaling
fn bench_async_parallel_wal_scaling(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    for batch_size in [10, 100, 500, 1000].iter() {
        let mut group = c.benchmark_group("async_parallel_wal_scaling");
        group.throughput(Throughput::Elements(*batch_size as u64));

        let dir = tempfile::tempdir().unwrap();
        let config = WalConfig {
            log_dir: dir.path().to_path_buf(),
            ..WalConfig::test_config()
        };

        let wal =
            rt.block_on(async { Arc::new(AsyncParallelWal::create(config, 4).await.unwrap()) });

        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            batch_size,
            |b, &size| {
                b.iter(|| {
                    rt.block_on(async {
                        let records: Vec<_> = (0..size)
                            .map(|i| {
                                LogRecord::new(LogOperation::Put {
                                    collection: format!("coll_{}", i % 10),
                                    id: vec![i as u8],
                                    data: vec![0u8; 1024],
                                })
                            })
                            .collect();

                        black_box(wal.append_batch(records).await.unwrap());
                    })
                })
            },
        );

        group.finish();
    }
}

criterion_group!(
    benches,
    bench_single_wal_batch,
    bench_sync_parallel_wal,
    bench_async_parallel_wal,
    bench_async_parallel_wal_scaling
);
criterion_main!(benches);
