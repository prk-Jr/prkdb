//! Configuration and accounting used by the stream wall-clock and instruction benches.

use prkdb::stream_log::{Record, StreamConfig};
use prkdb_core::wal::{CompressionConfig, SyncMode};
use std::path::Path;
use std::time::{Duration, Instant};

pub fn stream_config(dir: &Path, sync_mode: SyncMode) -> StreamConfig {
    let mut cfg = StreamConfig::new(dir);
    cfg.wal.sync_mode = sync_mode;
    cfg.wal.compression = CompressionConfig::none();
    // Match open_wal: stream comparisons isolate record/commit-hook overhead.
    cfg.wal.segment_bytes = 256 * 1024 * 1024;
    cfg.wal.sync_interval_ms = 10;
    cfg.retention_interval = Duration::ZERO;
    cfg
}

pub struct StreamCounts {
    pub operations: u64,
    pub records: u64,
}

impl StreamCounts {
    pub fn operations_per_second(&self, seconds: f64) -> f64 {
        self.operations as f64 / seconds
    }

    pub fn records_per_second(&self, seconds: f64) -> f64 {
        self.records as f64 / seconds
    }

    pub fn value_megabytes_per_second(&self, value_size: usize, seconds: f64) -> f64 {
        self.records_per_second(seconds) * value_size as f64 / 1_000_000.0
    }
}

#[derive(Clone, Copy)]
pub struct MeasurementWindow {
    pub measure_start: Instant,
    pub end: Instant,
    pub measure: Duration,
}

impl MeasurementWindow {
    pub fn new(start: Instant, warmup: Duration, measure: Duration) -> Self {
        let measure_start = start + warmup;
        Self {
            measure_start,
            end: measure_start + measure,
            measure,
        }
    }

    pub fn measures(self, at: Instant) -> bool {
        at >= self.measure_start && at < self.end
    }
}

#[derive(Default)]
pub struct RetentionCounts {
    pub warmup_runs: u64,
    pub warmup_removed: usize,
    pub measured_runs: u64,
    pub measured_removed: usize,
}

impl RetentionCounts {
    // Completion inside the measurement window is a conservative witness that
    // reclamation actually finished while append samples were being collected.
    pub fn observe(&mut self, window: MeasurementWindow, completed: Instant, removed: usize) {
        if completed < window.measure_start {
            self.warmup_runs += 1;
            self.warmup_removed += removed;
        } else if window.measures(completed) {
            self.measured_runs += 1;
            self.measured_removed += removed;
        }
    }

    pub fn validate(&self) -> Result<(), &'static str> {
        if self.measured_removed == 0 {
            Err("no segments were removed during measurement")
        } else {
            Ok(())
        }
    }
}

pub fn records_for_append(key: Vec<u8>, value: &[u8], count: usize) -> Vec<Record> {
    let mut records = Vec::with_capacity(count);
    for _ in 1..count {
        records.push(Record {
            key: Some(key.clone()),
            value: value.to_vec(),
            headers: Vec::new(),
        });
    }
    if count > 0 {
        // Like the reference Batch, the last record takes the caller's owned key.
        // b1 makes no key clone; b100 clones 99 keys and moves the final one.
        records.push(Record {
            key: Some(key),
            value: value.to_vec(),
            headers: Vec::new(),
        });
    }
    records
}
