use super::index::SparseIndex;
use super::{
    AppendAck, EventSeq, ReadBatch, ReadLimits, Record, StartAt, StoredRecord, StreamConfig,
    STREAM_LSN_LIMIT,
};
use crate::storage::format::{ensure_format, Kind};
use crate::storage::lock::lock_data_dir;
use crate::storage::wal_adapter::{queued_wal_err, wal_err};
use crate::storage::writer_liveness::LivenessBounds;
use parking_lot::RwLock;
use prkdb_core::vfs::{LockGuard, StdVfs, Vfs};
use prkdb_core::wal::frame::FrameKind;
use prkdb_core::wal::records::RecordBatch;
use prkdb_core::wal::{FrontRelease, Wal, WalError, WalHealth, WalOptions};
use prkdb_types::error::StorageError;
use std::ops::ControlFlow;
use std::sync::Arc;
use std::time::Duration;

struct Inner {
    // Field order matters: Wal's drop joins the writer before the lock is released.
    wal: Wal,
    index: Arc<RwLock<SparseIndex>>,
    cfg: StreamConfig,
    _lock: Box<dyn LockGuard>,
}

/// A single append-only stream, holding its directory lock until all reads finish.
pub struct StreamLog {
    inner: Arc<Inner>,
}

fn position(lsn: u64) -> EventSeq {
    EventSeq::try_from_wal(lsn, 0).expect("stream recovery and writer enforce a representable end")
}

// Kind mismatches in streams name the corrupt frame directly. Payload decode errors
// retain the WAL's ReplayFailed context.
fn open_error(e: WalError) -> StorageError {
    match e {
        WalError::ReplayFailed { source, .. }
            if matches!(*source, WalError::CorruptSegment { .. }) =>
        {
            wal_err(*source)
        }
        e => wal_err(e),
    }
}

impl StreamLog {
    pub async fn open(cfg: StreamConfig) -> Result<Self, StorageError> {
        Self::open_with_vfs(Arc::new(StdVfs), cfg).await
    }

    pub async fn open_with_vfs(vfs: Arc<dyn Vfs>, cfg: StreamConfig) -> Result<Self, StorageError> {
        tokio::task::spawn_blocking(move || {
            let dir = &cfg.path;
            if !vfs
                .exists(dir)
                .map_err(|e| StorageError::Internal(e.to_string()))?
            {
                vfs.create_dir_all(dir)
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
            }
            let lock = lock_data_dir(vfs.as_ref(), dir)?;
            ensure_format(vfs.as_ref(), dir, Kind::Stream)?;
            let index = Arc::new(RwLock::new(SparseIndex::default()));
            let mut opts = WalOptions::from_config(&cfg.wal);
            opts.front_release = FrontRelease::Retention;
            opts.append_kind = FrameKind::Records;
            opts.lsn_limit = Some(STREAM_LSN_LIMIT);
            let (wal, _) = Wal::open(vfs, dir, opts, 1, &mut |loc, kind, payload| {
                if kind != FrameKind::Records {
                    return Err(WalError::CorruptSegment {
                        path: dir.join(prkdb_core::wal::segment::segment_file_name(loc.segment)),
                        offset: loc.offset,
                        reason: format!(
                            "stream contains {kind:?} frame at lsn {} instead of Records",
                            loc.lsn
                        ),
                    });
                }
                // Open rebuilds the index from the uncompressed header after the WAL
                // verifies the frame CRC; record bodies are decoded by bounded reads.
                let (time, _) = RecordBatch::peek_header(payload)?;
                index.write().insert(loc, time);
                Ok(())
            })
            .map_err(open_error)?;
            Ok(Self {
                inner: Arc::new(Inner {
                    wal,
                    index,
                    cfg,
                    _lock: lock,
                }),
            })
        })
        .await
        .map_err(|e| StorageError::Internal(format!("stream open task: {e}")))?
    }

    /// Append one frame of 1..=65,536 records, with the default caller time bound.
    /// Durable waits for data sync; Fast waits for the OS write and may lose this frame
    /// after power failure until a sync covers it.
    pub async fn append(&self, records: Vec<Record>) -> Result<AppendAck, StorageError> {
        self.append_timeout(records, LivenessBounds::from_max_flush_ms(50).client_bound)
            .await
    }

    /// Append with one deadline across admission and the writer's answer.
    /// `WriteBackpressure` means no request was queued. `WriteNotConfirmed` means the
    /// writer owns the append and it may still land; retrying can duplicate it.
    pub async fn append_timeout(
        &self,
        records: Vec<Record>,
        bound: Duration,
    ) -> Result<AppendAck, StorageError> {
        let deadline = tokio::time::Instant::now() + bound;
        let inner = &self.inner;
        let next_lsn = inner.wal.next_lsn();
        if next_lsn >= STREAM_LSN_LIMIT || EventSeq::try_from_wal(next_lsn, 0).is_none() {
            return Err(StorageError::Validation(
                "stream LSN reaches the reserved final 2^48 offset block; no further append fits"
                    .into(),
            ));
        }
        let time = inner.cfg.clock.now_ms();
        let count = records.len();
        let payload = RecordBatch {
            append_time_ms: time,
            records,
        }
        .encode(&inner.cfg.wal.compression)
        .map_err(wal_err)?;
        if tokio::time::Instant::now() >= deadline {
            return Err(StorageError::WriteBackpressure(format!(
                "stream admission exceeded {}ms",
                bound.as_millis()
            )));
        }
        let reservation =
            match tokio::time::timeout_at(deadline, inner.wal.reserve(payload.len())).await {
                Ok(r) => r.map_err(wal_err)?,
                Err(_) => {
                    return Err(StorageError::WriteBackpressure(format!(
                        "stream admission exceeded {}ms",
                        bound.as_millis()
                    )))
                }
            };
        // timeout_at polls a ready future before its timer; recheck immediately before
        // transfer so an already-expired deadline cannot queue a ready reservation.
        if tokio::time::Instant::now() >= deadline {
            return Err(StorageError::WriteBackpressure(format!(
                "stream admission exceeded {}ms",
                bound.as_millis()
            )));
        }
        let index = inner.index.clone();
        let pending = inner
            .wal
            .append_reserved(
                reservation,
                payload,
                Some(Box::new(move |loc| {
                    index.write().insert(loc, time);
                })),
            )
            .map_err(wal_err)?;
        let loc = match tokio::time::timeout_at(deadline, pending).await {
            Ok(r) => r.map_err(queued_wal_err)?,
            Err(_) => {
                return Err(StorageError::WriteNotConfirmed(format!(
                    "no result from the stream WAL writer within {}ms",
                    bound.as_millis()
                )))
            }
        };
        Ok(AppendAck {
            lsn: loc.lsn,
            count: count as u32,
        })
    }

    /// Read from a frozen acknowledged watermark. Fast records can still be lost on
    /// power failure. Returned positions follow `last_record.offset + 1`.
    pub async fn read_from(
        &self,
        from: StartAt,
        limits: ReadLimits,
    ) -> Result<ReadBatch, StorageError> {
        self.read(from, limits, false).await
    }

    /// Read only the acknowledged, synced prefix. A cursor above that prefix but
    /// within the acknowledged range returns an empty batch with its cursor intact.
    pub async fn read_durable_from(
        &self,
        from: StartAt,
        limits: ReadLimits,
    ) -> Result<ReadBatch, StorageError> {
        self.read(from, limits, true).await
    }

    async fn read(
        &self,
        from: StartAt,
        limits: ReadLimits,
        durable: bool,
    ) -> Result<ReadBatch, StorageError> {
        let inner = self.inner.clone();
        // Freeze the bounds with the index locked. A hook can have inserted one
        // frame before its ack; SparseIndex excludes that frame's maximum time.
        let (floor, cap, end, watermark, requested, hint) = {
            let index = inner.index.read();
            let floor = inner.wal.log_state().log_start;
            let acked = inner.wal.acked_lsn();
            let cap = if durable {
                inner.wal.durable_lsn().min(acked)
            } else {
                acked
            };
            let end = position(acked + 1);
            let watermark = position(cap + 1);
            let requested = match from {
                StartAt::Offset(offset) => offset,
                StartAt::Earliest => position(floor),
                StartAt::Latest => end,
                StartAt::Timestamp(t) => index
                    .timestamp(t, floor, acked)
                    .map(position)
                    .unwrap_or(end),
            };
            (
                floor,
                cap,
                end,
                watermark,
                requested,
                index.seek(requested.raw() >> 16),
            )
        };
        if requested < position(floor) || requested > end {
            return Err(StorageError::OffsetOutOfRange {
                requested: requested.raw(),
                floor: position(floor).raw(),
                end: end.raw(),
            });
        }
        tokio::task::spawn_blocking(move || {
            let mut records = Vec::new();
            let mut bytes = 0usize;
            let mut next = requested;
            let from_lsn = requested.raw() >> 16;
            let _ = inner
                .wal
                .scan_from_loc_capped(from_lsn, cap, hint, &mut |loc, kind, payload| {
                    if kind != FrameKind::Records {
                        return Err(WalError::CorruptSegment {
                            path: inner.wal.segment_path(loc.segment),
                            offset: loc.offset,
                            reason: format!("stream contains {kind:?} at lsn {}", loc.lsn),
                        });
                    }
                    let batch = RecordBatch::decode(payload)?;
                    let skip = if loc.lsn == from_lsn {
                        requested.raw() as u16 as usize
                    } else {
                        0
                    };
                    for (i, record) in batch.records.into_iter().enumerate().skip(skip) {
                        let offset = EventSeq::from_wal(loc.lsn, i as u16);
                        let size = record
                            .key
                            .as_ref()
                            .map_or(0, Vec::len)
                            .saturating_add(record.value.len())
                            .saturating_add(record.headers.iter().fold(0usize, |n, (k, v)| {
                                n.saturating_add(k.len()).saturating_add(v.len())
                            }));
                        if !records.is_empty()
                            && (records.len() >= limits.max_records
                                || size > limits.max_bytes.saturating_sub(bytes))
                        {
                            return Ok(ControlFlow::Break(()));
                        }
                        bytes = bytes.saturating_add(size);
                        records.push(StoredRecord {
                            offset,
                            append_time_ms: batch.append_time_ms,
                            key: record.key,
                            value: record.value,
                            headers: record.headers,
                        });
                        next = EventSeq::from_raw(offset.raw() + 1);
                        // Stop immediately without reading the next frame when either limit
                        // is met. Sparse resume positions skip the rest on the next read.
                        if records.len() >= limits.max_records.max(1) || bytes >= limits.max_bytes {
                            return Ok(ControlFlow::Break(()));
                        }
                    }
                    Ok(ControlFlow::Continue(()))
                })
                .map_err(wal_err)?;
            Ok(ReadBatch {
                records,
                next,
                high_watermark: watermark,
            })
        })
        .await
        .map_err(|e| StorageError::Internal(format!("stream read task: {e}")))?
    }

    pub async fn wait_for(&self, after: EventSeq, timeout: Duration) -> Result<bool, StorageError> {
        let mut rx = self.inner.wal.subscribe_acked();
        let wait = async {
            loop {
                if self.next_offset() > after {
                    return Ok(true);
                }
                match self.health() {
                    WalHealth::Closed => return Err(wal_err(WalError::Closed)),
                    WalHealth::Poisoned(reason) => return Err(wal_err(WalError::Poisoned(reason))),
                    _ => {}
                }
                rx.changed().await.map_err(|_| wal_err(WalError::Closed))?;
            }
        };
        match tokio::time::timeout(timeout, wait).await {
            Ok(r) => r,
            Err(_) => Ok(false),
        }
    }

    pub fn earliest(&self) -> EventSeq {
        position(self.inner.wal.log_state().log_start)
    }
    pub fn next_offset(&self) -> EventSeq {
        position(self.inner.wal.acked_lsn() + 1)
    }
    pub fn durable_end(&self) -> EventSeq {
        position(self.inner.wal.durable_lsn() + 1)
    }

    pub fn offset_for_timestamp(&self, ts_ms: i64) -> Result<Option<EventSeq>, StorageError> {
        let inner = &self.inner;
        Ok(inner
            .index
            .read()
            .timestamp(
                ts_ms,
                inner.wal.log_state().log_start,
                inner.wal.acked_lsn(),
            )
            .map(position))
    }

    /// Sync acknowledged writes, returning the exclusive end of the durable prefix.
    /// A sync failure poisons the writer until reopen.
    pub async fn sync(&self) -> Result<EventSeq, StorageError> {
        self.inner
            .wal
            .sync()
            .await
            .map(|lsn| position(lsn + 1))
            .map_err(wal_err)
    }

    pub fn health(&self) -> WalHealth {
        self.inner.wal.health()
    }
    pub fn log_bytes(&self) -> Result<u64, StorageError> {
        self.inner.wal.log_bytes().map_err(wal_err)
    }

    /// Sync and close the stream, reporting sync failures to the caller.
    /// A cancelled blocking read keeps the writer and directory lock alive until it
    /// finishes, but queued writes are synced before this method returns successfully.
    pub fn close(self) -> Result<(), StorageError> {
        match Arc::try_unwrap(self.inner) {
            Ok(inner) => {
                let Inner { wal, _lock, .. } = inner;
                let result = wal.close().map_err(wal_err);
                drop(_lock);
                result
            }
            // A cancelled read may still be running on the blocking pool. Its Inner
            // keeps both the writer and the lock alive until that read finishes.
            Err(inner) => {
                let result = inner.wal.sync_blocking().map_err(wal_err).map(|_| ());
                drop(inner);
                result
            }
        }
    }
}
