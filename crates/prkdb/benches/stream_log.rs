//! Local StreamLog cost measurements. Durability is explicit; Linux competitive
//! targets and retention/cold-read cells remain Task 2.15b.8.
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use prkdb::stream_log::{ReadLimits, Record, StartAt, StreamConfig, StreamLog};
use prkdb_core::vfs::StdVfs;
use prkdb_core::wal::{CompressionConfig, SyncMode, Wal, WalConfig, WalOptions};
use std::sync::Arc;
use std::time::Duration;

fn wal_benches(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    for (mode_name, mode) in [("fast", SyncMode::Fast), ("durable", SyncMode::Durable)] {
        let mut raw_group = c.benchmark_group(format!("wal_append/{mode_name}"));
        raw_group
            .sample_size(10)
            .warm_up_time(Duration::from_secs(1))
            .measurement_time(Duration::from_secs(3));
        for writers in [1usize, 16] {
            let dir = tempfile::tempdir().unwrap();
            let cfg = WalConfig {
                log_dir: dir.path().into(),
                sync_mode: mode,
                compression: CompressionConfig::none(),
                ..WalConfig::default()
            };
            let (wal, _) = Wal::open(
                Arc::new(StdVfs),
                dir.path(),
                WalOptions::from_config(&cfg),
                1,
                &mut |_, _, _| Ok(()),
            )
            .unwrap();
            raw_group.throughput(Throughput::Bytes((writers * 1024) as u64));
            raw_group.bench_with_input(
                BenchmarkId::new("1KiB", writers),
                &writers,
                |b, &writers| {
                    b.to_async(&rt).iter(|| async {
                        let results = futures::future::join_all(
                            (0..writers).map(|_| wal.append(vec![7u8; 1024], None)),
                        )
                        .await;
                        for result in results {
                            std::hint::black_box(result.unwrap());
                        }
                    });
                },
            );
            wal.close().unwrap();
        }
        raw_group.finish();
    }
}

fn benches(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    for (mode_name, mode) in [("fast", SyncMode::Fast), ("durable", SyncMode::Durable)] {
        let mut group = c.benchmark_group(format!("stream_append/{mode_name}"));
        group
            .sample_size(10)
            .warm_up_time(Duration::from_secs(1))
            .measurement_time(Duration::from_secs(3));
        for count in [1usize, 100] {
            let dir = tempfile::tempdir().unwrap();
            let mut cfg = StreamConfig::new(dir.path());
            cfg.wal.sync_mode = mode;
            cfg.wal.compression = CompressionConfig::none();
            let log = rt.block_on(StreamLog::open(cfg)).unwrap();
            let records = vec![
                Record {
                    value: vec![7u8; 1024],
                    ..Record::default()
                };
                count
            ];
            group.throughput(Throughput::Elements(count as u64));
            group.bench_with_input(BenchmarkId::new("1KiB", count), &records, |b, records| {
                b.to_async(&rt).iter(|| async {
                    std::hint::black_box(log.append(records.clone()).await.expect("stream append"));
                });
            });
            log.close().unwrap();
        }
        group.finish();
    }
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = StreamConfig::new(dir.path());
    cfg.wal.sync_mode = SyncMode::Fast;
    cfg.wal.compression = CompressionConfig::none();
    let log = rt.block_on(StreamLog::open(cfg)).unwrap();
    let records = vec![
        Record {
            value: vec![7u8; 1024],
            ..Record::default()
        };
        100
    ];
    let mut tail = StartAt::Earliest;
    for _ in 0..100 {
        tail = StartAt::Offset(rt.block_on(log.append(records.clone())).unwrap().first());
    }
    rt.block_on(log.sync()).unwrap();
    let mut group = c.benchmark_group("stream_read");
    group.throughput(Throughput::Bytes(100 * 1024));
    group.bench_function("tail_100x1KiB", |b| {
        b.to_async(&rt).iter(|| async {
            let batch = log
                .read_from(
                    tail,
                    ReadLimits {
                        max_records: 100,
                        max_bytes: 1 << 20,
                    },
                )
                .await
                .expect("stream read");
            assert_eq!(batch.records.len(), 100);
            std::hint::black_box(batch);
        });
    });
    group.finish();
    log.close().unwrap();
}
criterion_group!(stream, wal_benches, benches);
criterion_main!(stream);
