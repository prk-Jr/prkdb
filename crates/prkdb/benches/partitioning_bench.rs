use criterion::{black_box, criterion_group, criterion_main, Criterion};
use prkdb::partitioning::{DefaultPartitioner, Partitioner};
use std::hash::Hash;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct BenchKey(u64);

fn partitioning_benchmark(c: &mut Criterion) {
    let partitioner = DefaultPartitioner::<BenchKey>::new();
    let key = BenchKey(12345);
    let num_partitions = 100;

    c.bench_function("partitioning", |b| {
        b.iter(|| {
            partitioner.partition(black_box(&key), black_box(num_partitions));
        })
    });
}

criterion_group!(benches, partitioning_benchmark);
criterion_main!(benches);
