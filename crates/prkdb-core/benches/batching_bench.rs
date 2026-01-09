use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use prkdb_core::wal::{LogOperation, LogRecord, WalConfig, WriteAheadLog};
use std::sync::Arc;

/// Benchmark baseline: single append
fn bench_wal_single_append(c: &mut Criterion) {
    let dir = tempfile::tempdir().unwrap();
    let config = WalConfig {
        log_dir: dir.path().to_path_buf(),
        ..WalConfig::test_config()
    };
    let wal = Arc::new(WriteAheadLog::create(config).unwrap());

    c.bench_function("wal/single_append", |b| {
        b.iter(|| {
            let record = LogRecord::new(LogOperation::Put {
                collection: "test".to_string(),
                id: vec![1u8; 16],
                data: vec![0u8; 1024],
            });
            black_box(wal.append(record).unwrap());
        })
    });
}

/// Benchmark: batch append with different sizes
fn bench_wal_batch_append(c: &mut Criterion) {
    let dir = tempfile::tempdir().unwrap();
    let config = WalConfig {
        log_dir: dir.path().to_path_buf(),
        ..WalConfig::test_config()
    };
    let wal = Arc::new(WriteAheadLog::create(config).unwrap());

    for batch_size in [10, 50, 100].iter() {
        let mut group = c.benchmark_group(format!("wal/batch_append"));
        group.throughput(Throughput::Elements(*batch_size as u64));

        group.bench_function(format!("batch_{}", batch_size), |b| {
            b.iter(|| {
                let records: Vec<_> = (0..*batch_size as u64)
                    .map(|i| {
                        LogRecord::new(LogOperation::Put {
                            collection: "test".to_string(),
                            id: i.to_le_bytes().to_vec(),
                            data: vec![0u8; 1024],
                        })
                    })
                    .collect();

                black_box(wal.append_batch(records).unwrap());
            })
        });

        group.finish();
    }
}

/// Benchmark: measure throughput improvement
fn bench_wal_throughput_comparison(c: &mut Criterion) {
    let dir = tempfile::tempdir().unwrap();
    let config = WalConfig {
        log_dir: dir.path().to_path_buf(),
        ..WalConfig::test_config()
    };
    let wal = Arc::new(WriteAheadLog::create(config).unwrap());

    let mut group = c.benchmark_group("wal/throughput");

    // Baseline: 100 single appends
    group.bench_function("100_singles", |b| {
        b.iter(|| {
            for i in 0u64..100 {
                let record = LogRecord::new(LogOperation::Put {
                    collection: "test".to_string(),
                    id: i.to_le_bytes().to_vec(),
                    data: vec![0u8; 1024],
                });
                wal.append(record).unwrap();
            }
        })
    });

    // Batched: 1 batch of 100
    group.bench_function("1_batch_100", |b| {
        b.iter(|| {
            let records: Vec<_> = (0u64..100)
                .map(|i| {
                    LogRecord::new(LogOperation::Put {
                        collection: "test".to_string(),
                        id: i.to_le_bytes().to_vec(),
                        data: vec![0u8; 1024],
                    })
                })
                .collect();

            wal.append_batch(records).unwrap();
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_wal_single_append,
    bench_wal_batch_append,
    bench_wal_throughput_comparison
);
criterion_main!(benches);
