use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use prkdb_core::wal::mmap_parallel_wal::MmapParallelWal;
use prkdb_core::wal::{LogOperation, LogRecord, WalConfig};
use std::sync::Arc;

/// Benchmark Mmap parallel WAL scaling
fn bench_mmap_parallel_wal_scaling(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    for batch_size in [100, 500, 1000].iter() {
        let mut group = c.benchmark_group("mmap_parallel_wal_scaling");
        group.throughput(Throughput::Elements(*batch_size as u64));

        let dir = tempfile::tempdir().unwrap();
        let config = WalConfig {
            log_dir: dir.path().to_path_buf(),
            ..WalConfig::test_config()
        };

        let wal =
            rt.block_on(async { Arc::new(MmapParallelWal::create(config, 4).await.unwrap()) });

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

criterion_group!(benches, bench_mmap_parallel_wal_scaling);
criterion_main!(benches);
