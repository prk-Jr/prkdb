use criterion::{black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use prkdb::prelude::StorageAdapter;
use prkdb::storage::InMemoryAdapter;
use prkdb_storage_segmented::SegmentedLogAdapter;
use std::time::Duration;
use tempfile::tempdir;
use tokio::runtime::Runtime;

async fn write_seq_segmented(count: usize, segment_size: u64) {
    let dir = tempdir().expect("tmp dir");
    let adapter = SegmentedLogAdapter::new(dir.path(), segment_size, None, 10_000)
        .await
        .expect("segmented init");
    for i in 0..count {
        let key = (i as u64).to_be_bytes().to_vec();
        let val = (i as u64).to_le_bytes();
        adapter.put(&key, &val).await.expect("put");
    }
}

async fn read_seq_segmented(count: usize, segment_size: u64) {
    let dir = tempdir().expect("tmp dir");
    let adapter = SegmentedLogAdapter::new(dir.path(), segment_size, None, 10_000)
        .await
        .expect("segmented init");
    // seed
    for i in 0..count {
        let key = (i as u64).to_be_bytes().to_vec();
        let val = (i as u64).to_le_bytes();
        adapter.put(&key, &val).await.expect("put");
    }
    // read
    for i in 0..count {
        let key = (i as u64).to_be_bytes().to_vec();
        let _ = black_box(adapter.get(&key).await.expect("get"));
    }
}

async fn write_seq_inmem(count: usize) {
    let adapter = InMemoryAdapter::new();
    for i in 0..count {
        let key = (i as u64).to_be_bytes().to_vec();
        let val = (i as u64).to_le_bytes();
        adapter.put(&key, &val).await.expect("put");
    }
}

async fn read_seq_inmem(count: usize) {
    let adapter = InMemoryAdapter::new();
    for i in 0..count {
        let key = (i as u64).to_be_bytes().to_vec();
        let val = (i as u64).to_le_bytes();
        adapter.put(&key, &val).await.expect("put");
    }
    for i in 0..count {
        let key = (i as u64).to_be_bytes().to_vec();
        let _ = black_box(adapter.get(&key).await.expect("get"));
    }
}

fn bench_append_log(c: &mut Criterion) {
    let rt = Runtime::new().expect("rt");
    let mut group = c.benchmark_group("append_log_segmented_vs_inmem");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(3));

    let count = 1_000usize;
    group.bench_with_input(
        BenchmarkId::new("segmented_write", count),
        &count,
        |b, &count| {
            b.to_async(&rt).iter_batched(
                || (),
                |_| async { write_seq_segmented(count, 64 * 1024).await },
                BatchSize::SmallInput,
            );
        },
    );
    group.bench_with_input(
        BenchmarkId::new("segmented_read", count),
        &count,
        |b, &count| {
            b.to_async(&rt).iter_batched(
                || (),
                |_| async { read_seq_segmented(count, 64 * 1024).await },
                BatchSize::SmallInput,
            );
        },
    );

    group.bench_with_input(
        BenchmarkId::new("inmem_write", count),
        &count,
        |b, &count| {
            b.to_async(&rt).iter_batched(
                || (),
                |_| async { write_seq_inmem(count).await },
                BatchSize::SmallInput,
            );
        },
    );
    group.bench_with_input(
        BenchmarkId::new("inmem_read", count),
        &count,
        |b, &count| {
            b.to_async(&rt).iter_batched(
                || (),
                |_| async { read_seq_inmem(count).await },
                BatchSize::SmallInput,
            );
        },
    );

    group.finish();
}

criterion_group!(benches, bench_append_log);
criterion_main!(benches);
