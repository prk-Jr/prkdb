//! Append-only record streams on the single WAL.
//!
//! Offsets pack the frame LSN and the record index. Reads see acknowledged writes;
//! callers that require synced data use `read_durable_from`. Fast-mode acknowledgements
//! can be lost after a power failure, as with the keyed adapter.
//!
//! `Durable` acknowledges only after the WAL's data sync succeeds. `Fast` acknowledges
//! after an OS write; its periodic sync interval is a target, not a hard loss-window
//! bound. A completed [`StreamLog::sync`] and [`StreamLog::durable_end`] describe the
//! synced prefix. Sync or write failures (including disk-full errors) poison the WAL
//! until reopen. Retention cannot free space on a poisoned writer; size a stream below
//! the volume's capacity.
//!
//! A timeout after queueing is [`prkdb_types::error::StorageError::WriteNotConfirmed`]:
//! the append may still land. Retrying that append can duplicate records. Admission
//! refusal is definite and reported separately as `WriteBackpressure`.

mod index;
mod log;
pub mod manifest;
mod retention;

pub use log::StreamLog;
pub use prkdb_core::wal::records::Record;
pub use prkdb_types::event::EventSeq;
pub use retention::{RetentionPolicy, RetentionReport};

use prkdb_core::wal::WalConfig;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Streams reserve the final LSN block for their exclusive end position.
pub(crate) const STREAM_LSN_LIMIT: u64 = (1 << 48) - 1;

/// Wall-clock time supplied to an append before admission.
pub trait Clock: Send + Sync {
    fn now_ms(&self) -> i64;
}

struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> i64 {
        match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(d) => d.as_millis().min(i64::MAX as u128) as i64,
            Err(e) => -(e.duration().as_millis().min(i64::MAX as u128) as i64),
        }
    }
}

/// Stream settings. Retention defaults to keeping every record.
#[derive(Clone)]
pub struct StreamConfig {
    pub path: PathBuf,
    pub wal: WalConfig,
    pub clock: Arc<dyn Clock>,
    pub retention: RetentionPolicy,
    /// None derives a quarter of max_age, or disables age rolling without max_age.
    pub segment_max_age: Option<Duration>,
    /// Zero disables background runs; explicit apply_retention remains available.
    pub retention_interval: Duration,
}

impl StreamConfig {
    pub fn new(path: impl AsRef<Path>) -> Self {
        let path = path.as_ref().to_path_buf();
        Self {
            wal: WalConfig {
                log_dir: path.clone(),
                segment_bytes: 128 * 1024 * 1024,
                ..WalConfig::default()
            },
            path,
            clock: Arc::new(SystemClock),
            retention: RetentionPolicy::default(),
            segment_max_age: None,
            retention_interval: Duration::from_secs(60),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppendAck {
    pub lsn: u64,
    pub count: u32,
}

impl AppendAck {
    pub fn first(&self) -> EventSeq {
        self.offset(0)
    }

    pub fn last(&self) -> EventSeq {
        self.offset(self.count - 1)
    }

    pub fn offset(&self, i: u32) -> EventSeq {
        assert!(i < self.count, "record index exceeds append count");
        EventSeq::try_from_wal(self.lsn, i as u16)
            .expect("append acknowledgements have validated representable LSNs")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredRecord {
    pub offset: EventSeq,
    pub append_time_ms: i64,
    pub key: Option<Vec<u8>>,
    pub value: Vec<u8>,
    pub headers: Vec<(String, Vec<u8>)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartAt {
    Offset(EventSeq),
    Earliest,
    Latest,
    /// Coarse seek: the first record of the oldest segment with maximum time >= t.
    Timestamp(i64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadLimits {
    pub max_records: usize,
    pub max_bytes: usize,
}

impl Default for ReadLimits {
    fn default() -> Self {
        Self {
            max_records: 1000,
            max_bytes: 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadBatch {
    pub records: Vec<StoredRecord>,
    pub next: EventSeq,
    pub high_watermark: EventSeq,
}
