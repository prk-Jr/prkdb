use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use prkdb_core::wal::{LogOperation, LogRecord, WalConfig, WriteAheadLog};
use rand::Rng;
use std::sync::Arc;
use tempfile::tempdir;

/// Benchmark WAL random read performance
/// This verifies the 782K ops/sec claim from BENCHMARK_KAFKA_COMPARISON.md
fn bench_wal_random_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_random_read");
    group.throughput(Throughput::Elements(1));

    let dir = tempdir().unwrap();
    let config = WalConfig {
        log_dir: dir.path().to_path_buf(),
        ..WalConfig::test_config()
    };

    let wal = Arc::new(WriteAheadLog::create(config).unwrap());

    // Pre-populate WAL with 10K records
    let num_records = 10_000;
    let payload = vec![0u8; 1024]; // 1KB payload
    let mut offsets = Vec::with_capacity(num_records);

    for i in 0..num_records {
        let record = LogRecord::new(LogOperation::Put {
            collection: "bench".to_string(),
            id: i.to_be_bytes().to_vec(),
            data: payload.clone(),
        });
        offsets.push(wal.append(record).unwrap());
    }

    // Benchmark random reads
    let mut rng = rand::thread_rng();

    group.bench_function("random_read_1kb", |b| {
        b.iter(|| {
            let idx = rng.gen_range(0..num_records);
            let offset = offsets[idx];
            black_box(wal.read(offset).unwrap());
        })
    });

    group.finish();
}

/// Benchmark sequential read performance for comparison
fn bench_wal_sequential_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_sequential_read");
    group.throughput(Throughput::Elements(1));

    let dir = tempdir().unwrap();
    let config = WalConfig {
        log_dir: dir.path().to_path_buf(),
        ..WalConfig::test_config()
    };

    let wal = Arc::new(WriteAheadLog::create(config).unwrap());

    // Pre-populate WAL with 1K records
    let num_records = 1_000;
    let payload = vec![0u8; 1024]; // 1KB payload
    let mut offsets = Vec::with_capacity(num_records);

    for i in 0..num_records {
        let record = LogRecord::new(LogOperation::Put {
            collection: "bench".to_string(),
            id: i.to_be_bytes().to_vec(),
            data: payload.clone(),
        });
        offsets.push(wal.append(record).unwrap());
    }

    // Benchmark sequential reads
    let mut counter = 0;

    group.bench_function("sequential_read_1kb", |b| {
        b.iter(|| {
            let offset = offsets[counter % num_records];
            counter += 1;
            black_box(wal.read(offset).unwrap());
        })
    });

    group.finish();
}

/// Benchmark with different payload sizes
fn bench_wal_read_various_sizes(c: &mut Criterion) {
    for size in [256, 1024, 4096] {
        let mut group = c.benchmark_group(format!("wal_read_{}b", size));
        group.throughput(Throughput::Bytes(size as u64));

        let dir = tempdir().unwrap();
        let config = WalConfig {
            log_dir: dir.path().to_path_buf(),
            ..WalConfig::test_config()
        };

        let wal = Arc::new(WriteAheadLog::create(config).unwrap());

        // Pre-populate with this size
        let num_records = 1_000;
        let payload = vec![0u8; size];
        let mut offsets = Vec::with_capacity(num_records);

        for i in 0..num_records {
            let record = LogRecord::new(LogOperation::Put {
                collection: "bench".to_string(),
                id: i.to_be_bytes().to_vec(),
                data: payload.clone(),
            });
            offsets.push(wal.append(record).unwrap());
        }

        let mut rng = rand::thread_rng();

        group.bench_function("random_read", |b| {
            b.iter(|| {
                let idx = rng.gen_range(0..num_records);
                let offset = offsets[idx];
                black_box(wal.read(offset).unwrap());
            })
        });

        group.finish();
    }
}

criterion_group!(
    benches,
    bench_wal_random_read,
    bench_wal_sequential_read,
    bench_wal_read_various_sizes
);
criterion_main!(benches);
