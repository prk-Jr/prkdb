use criterion::{criterion_group, criterion_main, Criterion};
use prkdb_core::wal::{mmap_log_segment::MmapLogSegment, LogOperation, LogRecord};
use tempfile::tempdir;

fn wal_recovery_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_recovery");
    let rt = tokio::runtime::Runtime::new().unwrap();

    // 1. Setup: Create a large WAL segment (10MB data)
    let dir = tempdir().unwrap();
    let segment = rt.block_on(async {
        MmapLogSegment::create(dir.path(), 0, 50 * 1024 * 1024) // 50MB capacity
            .await
            .unwrap()
    });

    let record_count = 10_000;
    let payload = vec![0u8; 1000]; // 1KB payload
    let mut offsets = Vec::with_capacity(record_count);

    rt.block_on(async {
        for i in 0..record_count {
            let record = LogRecord::new(LogOperation::Put {
                collection: "bench".to_string(),
                id: i.to_be_bytes().to_vec(),
                data: payload.clone(),
            });
            let offset = segment.append(record).await.unwrap();
            offsets.push(offset);
        }
        segment.flush().await.unwrap();
    });

    // We want to simulate recovering from the last 10% of the log
    // Calculate the start offset for the last 1000 records
    let recovery_start_index = record_count - 1000;
    let recovery_offset = offsets[recovery_start_index];

    // 2. Bench Case 1: Full Scan (Baseline)
    group.bench_function("scan_full_10k_records", |b| {
        b.to_async(&rt).iter(|| async {
            let records = segment.scan().await.unwrap();
            assert_eq!(records.len(), record_count);
        })
    });

    // 3. Bench Case 2: Partial Scan (Optimized)
    // Should be significantly faster as it skips disk IO and deserialization for 90% of file
    group.bench_function("scan_from_last_10_percent", |b| {
        b.to_async(&rt).iter(|| async {
            let records = segment.scan_from(recovery_offset).await.unwrap();
            assert_eq!(records.len(), 1000);
        })
    });

    // 4. Bench Case 3: Scan from end (checking new Appends)
    let last_offset = offsets.last().unwrap();
    group.bench_function("scan_from_end", |b| {
        b.to_async(&rt).iter(|| async {
            let records = segment.scan_from(*last_offset).await.unwrap();
            assert_eq!(records.len(), 1);
        })
    });

    group.finish();
}

criterion_group!(benches, wal_recovery_benchmark);
criterion_main!(benches);
