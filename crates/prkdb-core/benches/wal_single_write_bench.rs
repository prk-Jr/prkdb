use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use prkdb_core::wal::{LogOperation, LogRecord, WalConfig, WriteAheadLog};
use std::sync::Arc;
use tempfile::tempdir;

/// Benchmark WAL single write performance
/// This verifies the 197 ops/sec claim from BENCHMARK_KAFKA_COMPARISON.md
fn bench_wal_single_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_single_write");
    group.throughput(Throughput::Elements(1));

    let dir = tempdir().unwrap();
    let config = WalConfig {
        log_dir: dir.path().to_path_buf(),
        ..WalConfig::test_config()
    };

    let wal = Arc::new(WriteAheadLog::create(config).unwrap());

    // Benchmark single write with 1KB payload
    let payload = vec![0u8; 1024];
    let mut counter = 0u64;

    group.bench_function("single_write_1kb", |b| {
        b.iter(|| {
            let record = LogRecord::new(LogOperation::Put {
                collection: "bench".to_string(),
                id: counter.to_be_bytes().to_vec(),
                data: payload.clone(),
            });
            counter += 1;
            black_box(wal.append(record).unwrap());
        })
    });

    group.finish();
}

/// Benchmark with different payload sizes
fn bench_wal_single_write_various_sizes(c: &mut Criterion) {
    for size in [256, 1024, 4096, 16384] {
        let mut group = c.benchmark_group(format!("wal_single_write_{}b", size));
        group.throughput(Throughput::Bytes(size as u64));

        let dir = tempdir().unwrap();
        let config = WalConfig {
            log_dir: dir.path().to_path_buf(),
            ..WalConfig::test_config()
        };

        let wal = Arc::new(WriteAheadLog::create(config).unwrap());

        let payload = vec![0u8; size];
        let mut counter = 0u64;

        group.bench_function("single_write", |b| {
            b.iter(|| {
                let record = LogRecord::new(LogOperation::Put {
                    collection: "bench".to_string(),
                    id: counter.to_be_bytes().to_vec(),
                    data: payload.clone(),
                });
                counter += 1;
                black_box(wal.append(record).unwrap());
            })
        });

        group.finish();
    }
}

criterion_group!(
    benches,
    bench_wal_single_write,
    bench_wal_single_write_various_sizes
);
criterion_main!(benches);
