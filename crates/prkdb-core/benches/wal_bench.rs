use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use prkdb_core::wal::{LogOperation, LogRecord, WalConfig, WriteAheadLog};
use rand::Rng;
use std::sync::Arc;
use tempfile::tempdir;

fn wal_append_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_append");
    group.throughput(Throughput::Elements(1));

    let dir = tempdir().unwrap();
    let config = WalConfig {
        log_dir: dir.path().to_path_buf(),
        ..WalConfig::test_config()
    };

    let wal = Arc::new(WriteAheadLog::create(config).unwrap());

    // Generate a payload
    let payload = vec![0u8; 1024]; // 1KB payload
    let id = vec![1u8; 16];

    group.bench_function("append_1kb", |b| {
        b.iter(|| {
            let record = LogRecord::new(LogOperation::Put {
                collection: "bench".to_string(),
                id: id.clone(),
                data: payload.clone(),
            });
            wal.append(record).unwrap();
        })
    });

    group.finish();
}

fn wal_read_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_read");

    let dir = tempdir().unwrap();
    let config = WalConfig {
        log_dir: dir.path().to_path_buf(),
        ..WalConfig::test_config()
    };

    let wal = Arc::new(WriteAheadLog::create(config).unwrap());

    // Pre-populate
    let payload = vec![0u8; 1024];
    let count = 1000;
    let mut offsets = Vec::with_capacity(count);

    for i in 0..count {
        let record = LogRecord::new(LogOperation::Put {
            collection: "bench".to_string(),
            id: i.to_be_bytes().to_vec(),
            data: payload.clone(),
        });
        offsets.push(wal.append(record).unwrap());
    }

    let mut rng = rand::thread_rng();

    group.bench_function("read_random", |b| {
        b.iter(|| {
            let idx = rng.gen_range(0..count);
            let offset = offsets[idx];
            wal.read(offset).unwrap();
        })
    });

    group.finish();
}

fn wal_batch_append_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_batch_append");
    let batch_size = 100;
    group.throughput(Throughput::Elements(batch_size as u64));

    let dir = tempdir().unwrap();
    let config = WalConfig {
        log_dir: dir.path().to_path_buf(),
        ..WalConfig::test_config()
    };

    let wal = Arc::new(WriteAheadLog::create(config).unwrap());

    // Generate a batch
    let payload = vec![0u8; 1024]; // 1KB payload
    let mut items = Vec::with_capacity(batch_size);
    for i in 0..batch_size {
        items.push((vec![i as u8; 16], payload.clone()));
    }

    group.bench_function("append_batch_100_1kb", |b| {
        b.iter(|| {
            let record = LogRecord::new(LogOperation::PutBatch {
                collection: "bench".to_string(),
                items: items.clone(),
            });
            wal.append(record).unwrap();
        })
    });

    group.finish();
}

fn wal_compression_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_compression");

    // Test different compression types
    let compression_types = vec![
        ("none", prkdb_core::wal::CompressionType::None),
        ("lz4", prkdb_core::wal::CompressionType::Lz4),
        ("snappy", prkdb_core::wal::CompressionType::Snappy),
        ("zstd", prkdb_core::wal::CompressionType::Zstd),
    ];

    // Generate realistic compressible data (JSON-like)
    let payload = br#"{"user_id":12345,"name":"John Doe","email":"john@example.com","timestamp":1234567890,"data":{"items":[1,2,3,4,5],"status":"active","metadata":{"created":"2024-01-01","updated":"2024-01-15"}}}"#.to_vec();

    for (name, compression_type) in compression_types {
        let dir = tempdir().unwrap();
        let config = prkdb_core::wal::WalConfig {
            log_dir: dir.path().to_path_buf(),
            compression: prkdb_core::wal::CompressionConfig {
                compression_type,
                min_compress_bytes: 100, // Compress anything >100 bytes
                compression_level: 3,
            },
            ..prkdb_core::wal::WalConfig::test_config()
        };

        let wal = Arc::new(prkdb_core::wal::WriteAheadLog::create(config.clone()).unwrap());

        group.bench_function(format!("write_{}", name), |b| {
            b.iter(|| {
                let record = if compression_type == prkdb_core::wal::CompressionType::None {
                    prkdb_core::wal::LogRecord::new(prkdb_core::wal::LogOperation::Put {
                        collection: "test".to_string(),
                        id: vec![1u8; 16],
                        data: payload.clone(),
                    })
                } else {
                    prkdb_core::wal::LogRecord::new_with_compression(
                        prkdb_core::wal::LogOperation::Put {
                            collection: "test".to_string(),
                            id: vec![1u8; 16],
                            data: payload.clone(),
                        },
                        &config.compression,
                    )
                    .unwrap()
                };
                wal.append(record).unwrap();
            })
        });
    }

    group.finish();
}

fn wal_compression_ratio_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_compression_ratio");

    // Test with different data sizes
    let sizes = vec![256, 1024, 4096];

    for size in sizes {
        let payload = vec![42u8; size]; // Highly compressible (all same byte)

        let _config_none = prkdb_core::wal::CompressionConfig {
            compression_type: prkdb_core::wal::CompressionType::None,
            min_compress_bytes: 0,
            compression_level: 0,
        };

        let config_lz4 = prkdb_core::wal::CompressionConfig {
            compression_type: prkdb_core::wal::CompressionType::Lz4,
            min_compress_bytes: 0,
            compression_level: 3,
        };

        group.bench_function(format!("none_{}b", size), |b| {
            b.iter(|| {
                prkdb_core::wal::LogRecord::new(prkdb_core::wal::LogOperation::Put {
                    collection: "test".to_string(),
                    id: vec![1u8],
                    data: payload.clone(),
                })
            })
        });

        group.bench_function(format!("lz4_{}b", size), |b| {
            b.iter(|| {
                prkdb_core::wal::LogRecord::new_with_compression(
                    prkdb_core::wal::LogOperation::Put {
                        collection: "test".to_string(),
                        id: vec![1u8],
                        data: payload.clone(),
                    },
                    &config_lz4,
                )
                .unwrap()
            })
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    wal_append_benchmark,
    wal_read_benchmark,
    wal_batch_append_benchmark,
    wal_compression_benchmark,
    wal_compression_ratio_benchmark
);
criterion_main!(benches);
