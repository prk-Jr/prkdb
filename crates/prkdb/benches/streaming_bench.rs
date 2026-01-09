use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::time::Duration;

fn bench_stream_processing(c: &mut Criterion) {
    c.bench_function("stream_processing", |b| {
        b.iter(|| {
            let data: Vec<i32> = (0..1000).collect();
            let result: i32 = data.iter().map(|x| x * 2).sum();
            black_box(result)
        });
    });
}

fn bench_async_processing(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    c.bench_function("async_processing", |b| {
        b.to_async(&rt).iter(|| async {
            let data: Vec<i32> = (0..1000).collect();

            let result: i32 = data.iter().sum();

            tokio::time::sleep(Duration::from_nanos(1)).await;

            black_box(result)
        });
    });
}

criterion_group!(benches, bench_stream_processing, bench_async_processing);
criterion_main!(benches);
