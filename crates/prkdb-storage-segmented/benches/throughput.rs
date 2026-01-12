use criterion::{criterion_group, criterion_main, Criterion};
use prkdb_storage_segmented::SegmentedLogAdapter;
use prkdb_types::storage::StorageAdapter;
use tempfile::tempdir;

fn bench_puts(c: &mut Criterion) {
    c.bench_function("segmented_put_1k", |b| {
        b.to_async(tokio::runtime::Runtime::new().unwrap())
            .iter(|| async {
                let dir = tempdir().unwrap();
                let adapter = SegmentedLogAdapter::new(dir.path(), 1024, None, 500)
                    .await
                    .unwrap();
                for i in 0..1000u32 {
                    let k = format!("k{i}");
                    let v = format!("value{i}");
                    adapter.put(k.as_bytes(), v.as_bytes()).await.unwrap();
                }
            })
    });
}

criterion_group!(benches, bench_puts);
criterion_main!(benches);
