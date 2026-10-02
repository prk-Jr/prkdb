//! Wall-clock cost of the two WAL frame payload codecs (Task 2.15b.2): `Batch` (keyed
//! writes) and `RecordBatch` (stream appends), on the same data, uncompressed and LZ4.
//! The deterministic gate is `crates/prkdb/benches/iai_hot_paths.rs`; this is the local,
//! runnable-on-macOS view of the same codecs.
//!
//! ```text
//! cargo bench -p prkdb-core --bench frame_payload_codecs
//! ```

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use prkdb_core::wal::batch::{Batch, BatchOp};
use prkdb_core::wal::records::{Record, RecordBatch};
use prkdb_core::wal::{CompressionConfig, CompressionType};
use std::hint::black_box;

/// `n` puts of a 1 KiB value: the keyed adapter's frame payload.
fn batch(n: usize) -> Batch {
    Batch {
        ops: (0..n)
            .map(|i| BatchOp::Put {
                key: format!("bench-key-{i}").into_bytes(),
                value: vec![b'x'; 1024],
            })
            .collect(),
    }
}

/// `n` keyed records of a 1 KiB value: the same data as a stream append.
fn records(n: usize) -> RecordBatch {
    RecordBatch {
        append_time_ms: 1_700_000_000_000,
        records: (0..n)
            .map(|i| Record {
                key: Some(format!("bench-key-{i}").into_bytes()),
                value: vec![b'x'; 1024],
                headers: Vec::new(),
            })
            .collect(),
    }
}

fn configs() -> [(&'static str, CompressionConfig); 2] {
    [
        ("none", CompressionConfig::none()),
        (
            "lz4",
            CompressionConfig {
                compression_type: CompressionType::Lz4,
                min_compress_bytes: 0,
                compression_level: 3,
            },
        ),
    ]
}

fn codecs(c: &mut Criterion) {
    for n in [1usize, 100] {
        let mut group = c.benchmark_group(format!("frame_payload/{n}x1KiB"));
        group.throughput(Throughput::Bytes((n * 1024) as u64));
        let (b, r) = (batch(n), records(n));
        for (name, cfg) in configs() {
            group.bench_function(BenchmarkId::new("batch_encode", name), |bench| {
                bench.iter(|| black_box(b.encode(black_box(&cfg)).unwrap()))
            });
            group.bench_function(BenchmarkId::new("records_encode", name), |bench| {
                bench.iter(|| black_box(r.encode(black_box(&cfg)).unwrap()))
            });
            let b_bytes = b.encode(&cfg).unwrap();
            let r_bytes = r.encode(&cfg).unwrap();
            group.bench_function(BenchmarkId::new("batch_decode", name), |bench| {
                bench.iter(|| black_box(Batch::decode(black_box(&b_bytes)).unwrap()))
            });
            group.bench_function(BenchmarkId::new("records_decode", name), |bench| {
                bench.iter(|| black_box(RecordBatch::decode(black_box(&r_bytes)).unwrap()))
            });
            group.bench_function(BenchmarkId::new("records_peek_header", name), |bench| {
                bench.iter(|| black_box(RecordBatch::peek_header(black_box(&r_bytes)).unwrap()))
            });
        }
        group.finish();
    }
}

criterion_group!(benches, codecs);
criterion_main!(benches);
