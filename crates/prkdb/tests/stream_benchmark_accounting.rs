//! Deterministic witnesses for the measurement code shared by the stream benches.

#[path = "../benches/support/stream_measurement.rs"]
mod stream_measurement;

use prkdb_core::wal::compression::CompressionType;
use prkdb_core::wal::SyncMode;
use stream_measurement::{
    records_for_append, stream_config, MeasurementWindow, RetentionCounts, StreamCounts,
};

#[test]
fn stream_cells_use_the_uncompressed_reference_workload() {
    for mode in [SyncMode::Durable, SyncMode::Fast] {
        let cfg = stream_config(std::path::Path::new("unused-benchmark-path"), mode);
        assert_eq!(cfg.wal.compression.compression_type, CompressionType::None);
        assert_eq!(cfg.wal.sync_mode, mode);
        assert_eq!(cfg.wal.sync_interval_ms, 10);
        assert_eq!(cfg.wal.segment_bytes, 256 * 1024 * 1024);
        assert_eq!(cfg.wal.max_batch_bytes, 16 * 1024 * 1024);
        assert_eq!(cfg.wal.max_queued_bytes, 64 * 1024 * 1024);
        assert!(cfg.retention_interval.is_zero());
    }
}

#[test]
fn variable_read_sizes_preserve_every_measured_record() {
    // Two normal 64 KiB reads and a shorter tail: a mean rounded down to an
    // integer records/read loses two records from this actual total.
    let counts = StreamCounts {
        operations: 3,
        records: 44,
    };
    assert_eq!(counts.operations_per_second(2.0), 1.5);
    assert_eq!(counts.records_per_second(2.0), 22.0);
    assert_eq!(counts.value_megabytes_per_second(65536, 2.0), 1.441792);
}

#[test]
fn an_empty_window_reports_zero_record_throughput() {
    let counts = StreamCounts {
        operations: 0,
        records: 0,
    };
    assert_eq!(counts.records_per_second(2.0), 0.0);
    assert_eq!(counts.value_megabytes_per_second(1024, 2.0), 0.0);
}

#[test]
fn warmup_reclamation_cannot_validate_the_measured_retention_cell() {
    let start = std::time::Instant::now();
    let window = MeasurementWindow::new(
        start,
        std::time::Duration::from_secs(1),
        std::time::Duration::from_secs(3),
    );
    let mut counts = RetentionCounts::default();
    counts.observe(window, start + std::time::Duration::from_millis(500), 7);
    counts.observe(window, start + std::time::Duration::from_secs(2), 0);
    assert_eq!(counts.warmup_runs, 1);
    assert_eq!(counts.warmup_removed, 7);
    assert_eq!(counts.measured_runs, 1);
    assert_eq!(counts.measured_removed, 0);
    assert!(counts.validate().is_err());
}

#[test]
fn retention_completion_uses_the_shared_half_open_measurement_window() {
    let start = std::time::Instant::now();
    let window = MeasurementWindow::new(
        start,
        std::time::Duration::from_secs(1),
        std::time::Duration::from_secs(3),
    );
    assert_eq!(window.measure, std::time::Duration::from_secs(3));
    let mut counts = RetentionCounts::default();
    // Completion at the first measured instant counts; completion at the end
    // does not. The same window bounds govern append starts and retention.
    counts.observe(window, window.measure_start, 2);
    counts.observe(window, window.end, 100);
    assert!(window.measures(window.measure_start));
    assert!(!window.measures(window.end));
    assert_eq!(counts.measured_runs, 1);
    assert_eq!(counts.measured_removed, 2);
    assert!(counts.validate().is_ok());
}

#[test]
fn append_input_preserves_one_and_one_hundred_record_bytes() {
    let key = b"w003_k000000000017".to_vec();
    let value = (0..1024).map(|i| (i % 251) as u8).collect::<Vec<_>>();
    for count in [1, 100] {
        let records = records_for_append(key.clone(), &value, count);
        assert_eq!(records.len(), count);
        for record in records {
            assert_eq!(record.key.as_deref(), Some(key.as_slice()));
            assert_eq!(record.value, value);
            assert!(record.headers.is_empty());
        }
    }
}
