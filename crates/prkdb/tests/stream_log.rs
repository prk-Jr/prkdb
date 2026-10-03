//! `StreamLog`, a record stream on the single WAL (Task 2.15b.3, streaming log design
//! note §3, §4.3, §6, §9, §10): the data-directory lock, appends and their offsets, reads
//! by offset, `Earliest`, `Latest` and time, read limits and bounds, waiting for new
//! data, frames of the wrong kind in either kind of directory, and the `STREAM` manifest
//! codec. The `FORMAT` `kind` key is tested in `format_v2.rs`.

use prkdb::storage::WalStorageAdapter;
use prkdb::stream_log::manifest::{StreamManifest, STREAM_MANIFEST_VERSION};
use prkdb::stream_log::{
    Clock, EventSeq, ReadBatch, ReadLimits, Record, StartAt, StreamConfig, StreamLog,
};
use prkdb_core::vfs::{OpenMode, StdVfs, Vfs};
use prkdb_core::wal::batch::{Batch, BatchOp};
use prkdb_core::wal::frame::{encode_frame, FrameKind, MAX_PAYLOAD_LEN};
use prkdb_core::wal::records::RecordBatch;
use prkdb_core::wal::segment::{scan_segment, segment_file_name, write_segment_header};
use prkdb_core::wal::{CompressionConfig, LogState, SyncMode, WalConfig};
use prkdb_types::error::StorageError;
use prkdb_types::storage::StorageAdapter;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// A clock the test sets.
#[derive(Default)]
struct TestClock(AtomicI64);

impl Clock for TestClock {
    fn now_ms(&self) -> i64 {
        self.0.load(Ordering::SeqCst)
    }
}

fn cfg(dir: &Path) -> StreamConfig {
    StreamConfig::new(dir)
}

/// A stream with 4 KiB segments, uncompressed, on a settable clock.
fn small_segments(dir: &Path, clock: Arc<TestClock>) -> StreamConfig {
    let mut c = StreamConfig::new(dir);
    c.wal.segment_bytes = 4096;
    c.wal.compression = CompressionConfig::none();
    c.clock = clock;
    c
}

fn rec(value: &str) -> Record {
    Record {
        key: None,
        value: value.as_bytes().to_vec(),
        headers: Vec::new(),
    }
}

fn recs(values: &[&str]) -> Vec<Record> {
    values.iter().map(|v| rec(v)).collect()
}

fn values(batch: &ReadBatch) -> Vec<String> {
    batch
        .records
        .iter()
        .map(|r| String::from_utf8(r.value.clone()).unwrap())
        .collect()
}

fn offsets(batch: &ReadBatch) -> Vec<u64> {
    batch.records.iter().map(|r| r.offset.raw()).collect()
}

fn all() -> ReadLimits {
    ReadLimits {
        max_records: usize::MAX,
        max_bytes: usize::MAX,
    }
}

fn at(raw: u64) -> StartAt {
    StartAt::Offset(EventSeq::from_raw(raw))
}

fn segment_path(dir: &Path, first_lsn: u64) -> PathBuf {
    dir.join(segment_file_name(first_lsn))
}

/// Appends a hand-encoded frame at the end of the segment starting at `first_lsn`, with
/// the LSN the next frame there must carry. Returns the byte offset it was written at.
fn append_frame(dir: &Path, first_lsn: u64, kind: FrameKind, payload: &[u8]) -> u64 {
    let path = segment_path(dir, first_lsn);
    let file = StdVfs.open(&path, OpenMode::ReadWrite).unwrap();
    let scan = scan_segment(&*file, &path, first_lsn, &mut |_, _, _| Ok(())).unwrap();
    let mut frame = Vec::new();
    encode_frame(&mut frame, scan.next_lsn, kind, payload);
    let offset = file.len().unwrap();
    file.write_at(offset, &frame).unwrap();
    file.sync_data().unwrap();
    offset
}

fn assert_out_of_range(err: StorageError, requested: u64, floor: u64, end: u64) {
    assert_eq!(
        err,
        StorageError::OffsetOutOfRange {
            requested,
            floor,
            end
        }
    );
}

// --- LOCK -----------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn a_second_open_is_locked() {
    let dir = tempfile::tempdir().unwrap();
    let first = StreamLog::open(cfg(dir.path())).await.unwrap();
    let err = StreamLog::open(cfg(dir.path())).await.err().unwrap();
    assert!(matches!(err, StorageError::Locked(_)), "{err:?}");
    first.close().unwrap();
    StreamLog::open(cfg(dir.path()))
        .await
        .unwrap()
        .close()
        .unwrap();
}

// --- Appends --------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn an_append_of_3_records_returns_lsn_shifted_offsets() {
    let dir = tempfile::tempdir().unwrap();
    let log = StreamLog::open(cfg(dir.path())).await.unwrap();
    let first = log.append(recs(&["a"])).await.unwrap();
    assert_eq!((first.lsn, first.count), (1, 1));
    let ack = log.append(recs(&["b", "c", "d"])).await.unwrap();
    let l = ack.lsn;
    assert_eq!((l, ack.count), (2, 3));
    assert_eq!(ack.first().raw(), l << 16);
    assert_eq!(ack.offset(1).raw(), l << 16 | 1);
    assert_eq!(ack.last().raw(), l << 16 | 2);
    let read = log.read_from(StartAt::Earliest, all()).await.unwrap();
    assert_eq!(
        offsets(&read),
        vec![1 << 16, l << 16, l << 16 | 1, l << 16 | 2]
    );
    assert_eq!(values(&read), ["a", "b", "c", "d"]);
    assert_eq!(read.next.raw(), (l << 16 | 2) + 1);
    log.close().unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn offsets_continue_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let log = StreamLog::open(cfg(dir.path())).await.unwrap();
    log.append(recs(&["a", "b"])).await.unwrap();
    log.append(recs(&["c"])).await.unwrap();
    log.close().unwrap();

    let log = StreamLog::open(cfg(dir.path())).await.unwrap();
    assert_eq!(log.next_offset().raw(), 3 << 16);
    let ack = log.append(recs(&["d"])).await.unwrap();
    assert_eq!(
        ack.first().raw(),
        3 << 16,
        "LSNs continue from the recovered log"
    );
    let read = log.read_from(StartAt::Earliest, all()).await.unwrap();
    assert_eq!(values(&read), ["a", "b", "c", "d"]);
    assert_eq!(offsets(&read), vec![1 << 16, 1 << 16 | 1, 2 << 16, 3 << 16]);
    log.close().unwrap();
}

/// 0 and 65,537 records are the codec's `InvalidRecords`, which the storage layer
/// reports as `Validation` (2.15b.2 review: not `Internal`). Nothing is appended.
#[tokio::test(flavor = "multi_thread")]
async fn appends_of_0_and_65_537_records_are_validation() {
    let dir = tempfile::tempdir().unwrap();
    let log = StreamLog::open(cfg(dir.path())).await.unwrap();
    for n in [0usize, 65_537] {
        let err = log.append(vec![Record::default(); n]).await.err().unwrap();
        assert!(
            matches!(&err, StorageError::Validation(m) if m.contains(&n.to_string())),
            "{n}: {err:?}"
        );
    }
    let full = log.append(vec![Record::default(); 65_536]).await.unwrap();
    assert_eq!(
        (full.lsn, full.count),
        (1, 65_536),
        "nothing was appended before"
    );
    assert_eq!(full.last().raw(), 1 << 16 | 0xFFFF);
    log.close().unwrap();
}

/// A batch whose encoding is over `MAX_PAYLOAD_LEN` is refused as too large before it is
/// admitted (the WAL's `RecordTooLarge`, reported as `Validation` as on the keyed path):
/// both an uncompressed body over the limit and a body at the limit whose frame payload
/// (body + the 18-byte header) is over it.
#[tokio::test(flavor = "multi_thread")]
async fn an_encoded_payload_over_max_payload_len_is_record_too_large() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = cfg(dir.path());
    c.wal.compression = CompressionConfig::none();
    let log = StreamLog::open(c).await.unwrap();
    // One record costs 5 bytes plus its value.
    for value_len in [MAX_PAYLOAD_LEN, MAX_PAYLOAD_LEN - 5] {
        let big = Record {
            key: None,
            value: vec![7u8; value_len],
            headers: Vec::new(),
        };
        let err = log.append(vec![big]).await.err().unwrap();
        assert!(
            matches!(&err, StorageError::Validation(m)
                if m.contains(&format!("exceeds the {MAX_PAYLOAD_LEN}-byte limit"))),
            "{value_len}: {err:?}"
        );
    }
    assert_eq!(log.next_offset().raw(), 1 << 16, "nothing was appended");
    log.close().unwrap();
}

/// The last LSN block is reserved for a representable exclusive end offset.
#[tokio::test(flavor = "multi_thread")]
async fn stream_reserves_the_final_lsn_block_for_its_exclusive_end() {
    let start = (1u64 << 48) - 2;
    let terminal = start + 1;
    let dir = tempfile::tempdir().unwrap();
    StreamLog::open(cfg(dir.path()))
        .await
        .unwrap()
        .close()
        .unwrap();
    std::fs::remove_file(segment_path(dir.path(), 1)).unwrap();
    let path = segment_path(dir.path(), start);
    let file = StdVfs.create(&path).unwrap();
    write_segment_header(&*file, start).unwrap();
    file.sync_data().unwrap();
    LogState {
        log_start: start,
        deletes_compacted_through: 0,
    }
    .write(&StdVfs, dir.path())
    .unwrap();
    let log = Arc::new(StreamLog::open(cfg(dir.path())).await.unwrap());
    let ack = log.append(vec![Record::default(); 65_536]).await.unwrap();
    assert_eq!(ack.lsn, start);
    assert_eq!(ack.last().raw() + 1, terminal << 16);
    assert_eq!(log.next_offset().raw(), terminal << 16);
    assert_eq!(log.durable_end(), log.next_offset());
    let read = log
        .read_from(at(ack.last().raw() + 1), all())
        .await
        .unwrap();
    assert!(read.records.is_empty());
    assert_eq!(read.next.raw(), terminal << 16);
    let len = std::fs::metadata(&path).unwrap().len();
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let log = log.clone();
        tasks.push(tokio::spawn(async move {
            log.append(recs(&["too many"])).await
        }));
    }
    for task in tasks {
        let err = task.await.unwrap().unwrap_err();
        assert!(
            matches!(err, StorageError::Validation(ref m) if m.contains("2^48")),
            "{err:?}"
        );
    }
    assert_eq!(std::fs::metadata(&path).unwrap().len(), len);
    Arc::try_unwrap(log).ok().unwrap().close().unwrap();
    let log = StreamLog::open(cfg(dir.path())).await.unwrap();
    assert_eq!(log.next_offset().raw(), terminal << 16);
    assert_eq!(
        log.read_from(StartAt::Earliest, all())
            .await
            .unwrap()
            .records
            .len(),
        65_536
    );
    assert!(matches!(
        log.append(recs(&["too many"])).await,
        Err(StorageError::Validation(_))
    ));
    log.close().unwrap();
}

// --- Reads ----------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn read_from_last_plus_one_is_the_next_frame_and_0xffff_plus_1_carries() {
    let dir = tempfile::tempdir().unwrap();
    let log = StreamLog::open(cfg(dir.path())).await.unwrap();
    let a = log.append(recs(&["a0", "a1"])).await.unwrap();
    let full = log.append(vec![rec("x"); 65_536]).await.unwrap();
    let c = log.append(recs(&["c0"])).await.unwrap();

    // `last + 1` after a 2-record frame: idx 2 does not exist; the next frame follows.
    let read = log
        .read_from(at(a.last().raw() + 1), ReadLimits::default())
        .await
        .unwrap();
    assert_eq!(read.records.first().unwrap().offset, full.first());

    // `0xFFFF + 1` carries into the next LSN.
    assert_eq!(full.last().raw() & 0xFFFF, 0xFFFF);
    let read = log
        .read_from(at(full.last().raw() + 1), all())
        .await
        .unwrap();
    assert_eq!(offsets(&read), vec![c.first().raw()]);
    assert_eq!(values(&read), ["c0"]);

    // Mid-frame: records below the offset's index are skipped.
    let read = log.read_from(at(a.offset(1).raw()), all()).await.unwrap();
    assert_eq!(read.records[0].offset, a.offset(1));
    assert_eq!(read.records.len(), 1 + 65_536 + 1);

    // At the end: nothing, and the resume position stays put.
    let end = log.next_offset();
    assert_eq!(end.raw(), (c.lsn + 1) << 16);
    let read = log.read_from(StartAt::Offset(end), all()).await.unwrap();
    assert!(read.records.is_empty());
    assert_eq!((read.next, read.high_watermark), (end, end));
    log.close().unwrap();
}

/// `Offset(0)` and any offset below `earliest()` or above `next_offset()` is
/// `OffsetOutOfRange { requested, floor, end }`, on both read bounds.
#[tokio::test(flavor = "multi_thread")]
async fn offsets_below_earliest_or_past_next_offset_are_out_of_range() {
    let dir = tempfile::tempdir().unwrap();
    let log = StreamLog::open(cfg(dir.path())).await.unwrap();

    // A fresh log: earliest = next_offset = 1 << 16, and 0 is below it.
    assert_eq!(log.earliest().raw(), 1 << 16);
    assert_eq!(log.next_offset().raw(), 1 << 16);
    for o in [0, 1, (1 << 16) - 1] {
        let err = log.read_from(at(o), all()).await.err().unwrap();
        assert_out_of_range(err, o, 1 << 16, 1 << 16);
    }
    let err = log.read_from(at((1 << 16) + 1), all()).await.err().unwrap();
    assert_out_of_range(err, (1 << 16) + 1, 1 << 16, 1 << 16);

    log.append(recs(&["a", "b"])).await.unwrap();
    let end = 2 << 16;
    for o in [0u64, (1 << 16) - 1, end + 1, u64::MAX] {
        for durable in [false, true] {
            let read = if durable {
                log.read_durable_from(at(o), all()).await
            } else {
                log.read_from(at(o), all()).await
            };
            assert_out_of_range(read.err().unwrap(), o, 1 << 16, end);
        }
    }
    let err = log.read_from(at(0), all()).await.err().unwrap();
    assert!(
        err.to_string().contains("removed by retention") && err.to_string().contains("65536"),
        "{err}"
    );
    // Both ends are inside the range.
    assert_eq!(
        values(&log.read_from(at(1 << 16), all()).await.unwrap()),
        ["a", "b"]
    );
    assert!(log
        .read_from(at(end), all())
        .await
        .unwrap()
        .records
        .is_empty());
    log.close().unwrap();
}

/// `Earliest` is the floor, `Latest` the end (nothing until new data), and `Timestamp(t)`
/// the first offset of the oldest segment whose newest append time is at least `t`
/// (coarse, per segment: older records of that segment come too).
#[tokio::test(flavor = "multi_thread")]
async fn earliest_latest_and_timestamp_resolve_as_specified() {
    let dir = tempfile::tempdir().unwrap();
    let clock = Arc::new(TestClock::default());
    let log = StreamLog::open(small_segments(dir.path(), clock.clone()))
        .await
        .unwrap();
    // 4 KiB segments. Segment 1: one append at t = 1000 that nearly fills it. Segment 2:
    // a small append at t = 2000 and a 3 KiB one at t = 2100. Segment 3: t = 3000.
    let huge = "x".repeat(4000);
    let big = "x".repeat(3000);
    clock.0.store(1000, Ordering::SeqCst);
    let s1 = log.append(recs(&[&huge])).await.unwrap();
    clock.0.store(2000, Ordering::SeqCst);
    let s2 = log.append(recs(&["early"])).await.unwrap();
    clock.0.store(2100, Ordering::SeqCst);
    log.append(recs(&[&big])).await.unwrap();
    clock.0.store(3000, Ordering::SeqCst);
    let s3 = log.append(recs(&[&big])).await.unwrap();
    // The appends landed in three segments.
    let segments: Vec<u64> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| {
            let name = e.unwrap().file_name().into_string().unwrap();
            name.strip_suffix(".wal").map(|n| n.parse().unwrap())
        })
        .collect::<std::collections::BTreeSet<u64>>()
        .into_iter()
        .collect();
    assert_eq!(
        segments,
        vec![s1.lsn, s2.lsn, s3.lsn],
        "one segment per time"
    );

    let first = |batch: ReadBatch| batch.records.first().map(|r| r.offset);
    assert_eq!(
        first(log.read_from(StartAt::Earliest, all()).await.unwrap()),
        Some(s1.first())
    );
    assert_eq!(log.earliest(), s1.first());

    let latest = log.read_from(StartAt::Latest, all()).await.unwrap();
    assert!(latest.records.is_empty());
    assert_eq!(latest.next, log.next_offset());

    for (t, expect) in [
        (i64::MIN, Some(s1.first())),
        (1000, Some(s1.first())),
        (1001, Some(s2.first())),
        // Coarse: 2050 is inside segment 2, whose first record (t = 2000) comes too.
        (2050, Some(s2.first())),
        (2100, Some(s2.first())),
        (2101, Some(s3.first())),
        (3000, Some(s3.first())),
        (3001, None),
    ] {
        assert_eq!(log.offset_for_timestamp(t).unwrap(), expect, "t = {t}");
        let read = log.read_from(StartAt::Timestamp(t), all()).await.unwrap();
        match expect {
            Some(o) => assert_eq!(first(read), Some(o), "t = {t}"),
            // Nothing that new: the read starts at the end, like `Latest`.
            None => {
                assert!(read.records.is_empty(), "t = {t}");
                assert_eq!(read.next, log.next_offset());
            }
        }
    }
    let times: Vec<i64> = log
        .read_from(StartAt::Timestamp(2050), all())
        .await
        .unwrap()
        .records
        .iter()
        .map(|r| r.append_time_ms)
        .collect();
    assert_eq!(times, vec![2000, 2100, 3000]);

    // The index is rebuilt at open: the same answers after a reopen.
    log.close().unwrap();
    let log = StreamLog::open(small_segments(dir.path(), clock.clone()))
        .await
        .unwrap();
    assert_eq!(log.offset_for_timestamp(2050).unwrap(), Some(s2.first()));
    assert_eq!(log.offset_for_timestamp(2101).unwrap(), Some(s3.first()));
    log.close().unwrap();
}

/// A read stops at `max_records` or `max_bytes`, whichever comes first, possibly inside
/// a frame (the resume position is then that record's offset), and always returns at
/// least one record when one exists.
#[tokio::test(flavor = "multi_thread")]
async fn read_limits_always_return_at_least_one_record() {
    let dir = tempfile::tempdir().unwrap();
    let log = StreamLog::open(cfg(dir.path())).await.unwrap();
    let a = log
        .append(recs(&["0123456789", "abcdefghij", "k"]))
        .await
        .unwrap();
    let b = log.append(recs(&["second"])).await.unwrap();

    let read = log
        .read_from(
            StartAt::Earliest,
            ReadLimits {
                max_records: 2,
                max_bytes: usize::MAX,
            },
        )
        .await
        .unwrap();
    assert_eq!(values(&read), ["0123456789", "abcdefghij"]);
    assert_eq!(read.next, a.offset(2), "resumes inside the frame");
    let rest = log
        .read_from(StartAt::Offset(read.next), all())
        .await
        .unwrap();
    assert_eq!(values(&rest), ["k", "second"]);

    // A record bigger than max_bytes still comes back alone.
    for limits in [
        ReadLimits {
            max_records: 0,
            max_bytes: 0,
        },
        ReadLimits {
            max_records: usize::MAX,
            max_bytes: 1,
        },
    ] {
        let read = log.read_from(StartAt::Earliest, limits).await.unwrap();
        assert_eq!(values(&read), ["0123456789"], "{limits:?}");
        assert_eq!(read.next, a.offset(1));
    }
    // Bytes count keys, values and headers: 10 + 10 fits 20, the third does not.
    let read = log
        .read_from(
            StartAt::Earliest,
            ReadLimits {
                max_records: usize::MAX,
                max_bytes: 20,
            },
        )
        .await
        .unwrap();
    assert_eq!(read.records.len(), 2);
    let read = log
        .read_from(StartAt::Offset(b.first()), all())
        .await
        .unwrap();
    assert_eq!(values(&read), ["second"]);
    log.close().unwrap();
}

/// Records come back with their keys, headers and append time, through LZ4 as well.
#[tokio::test(flavor = "multi_thread")]
async fn keys_headers_and_times_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let clock = Arc::new(TestClock(AtomicI64::new(1_700_000_000_000)));
    let mut c = cfg(dir.path());
    c.clock = clock;
    c.wal.compression = CompressionConfig {
        min_compress_bytes: 0,
        ..CompressionConfig::default()
    };
    let log = StreamLog::open(c).await.unwrap();
    let records = vec![
        Record {
            key: Some(b"user:1".to_vec()),
            value: "pending ".repeat(20).into_bytes(),
            headers: vec![
                ("trace".into(), b"abc".to_vec()),
                ("v".into(), b"2".to_vec()),
            ],
        },
        Record {
            key: Some(Vec::new()),
            value: Vec::new(),
            headers: Vec::new(),
        },
    ];
    let ack = log.append(records.clone()).await.unwrap();
    let read = log.read_from(StartAt::Earliest, all()).await.unwrap();
    assert_eq!(read.records.len(), 2);
    for (i, (got, want)) in read.records.iter().zip(&records).enumerate() {
        assert_eq!(got.offset, ack.offset(i as u32));
        assert_eq!(got.append_time_ms, 1_700_000_000_000);
        assert_eq!(
            (&got.key, &got.value, &got.headers),
            (&want.key, &want.value, &want.headers)
        );
    }
    log.close().unwrap();
}

/// In `Fast` mode an acknowledged record that is not yet synced is visible to
/// `read_from` (acked bound) and not to `read_durable_from` (durable bound) until a sync.
#[tokio::test(flavor = "multi_thread")]
async fn fast_acked_but_unsynced_is_visible_to_read_from_not_read_durable_from() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = cfg(dir.path());
    c.wal.sync_mode = SyncMode::Fast;
    c.wal.sync_interval_ms = 3_600_000;
    let log = StreamLog::open(c).await.unwrap();
    let ack = log.append(recs(&["acked"])).await.unwrap();

    let acked = log.read_from(StartAt::Earliest, all()).await.unwrap();
    assert_eq!(values(&acked), ["acked"]);
    assert_eq!(acked.high_watermark, log.next_offset());

    let durable = log
        .read_durable_from(StartAt::Earliest, all())
        .await
        .unwrap();
    assert!(durable.records.is_empty(), "{durable:?}");
    assert_eq!(durable.next, ack.first(), "resume at the unsynced record");
    assert_eq!(durable.high_watermark, log.durable_end());
    assert_eq!(log.durable_end(), ack.first());

    assert_eq!(log.sync().await.unwrap(), log.next_offset());
    let durable = log
        .read_durable_from(StartAt::Earliest, all())
        .await
        .unwrap();
    assert_eq!(values(&durable), ["acked"]);
    log.close().unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn wait_for_returns_when_an_append_lands_and_false_on_timeout() {
    let dir = tempfile::tempdir().unwrap();
    let log = Arc::new(StreamLog::open(cfg(dir.path())).await.unwrap());
    let end = log.next_offset();
    assert!(!log.wait_for(end, Duration::from_millis(50)).await.unwrap());

    let waiter = {
        let log = log.clone();
        tokio::spawn(async move { log.wait_for(end, Duration::from_secs(30)).await })
    };
    let ack = log.append(recs(&["new"])).await.unwrap();
    assert!(waiter.await.unwrap().unwrap(), "woken by the append");
    // Already past: returns at once.
    assert!(log.wait_for(end, Duration::ZERO).await.unwrap());
    let read = log.read_from(StartAt::Offset(end), all()).await.unwrap();
    assert_eq!(read.records[0].offset, ack.first());
    Arc::try_unwrap(log).ok().unwrap().close().unwrap();
}

// --- Frames of the wrong kind ---------------------------------------------------------

/// A stream directory holds only `Records` frames: a `Batch` frame in one is
/// `CorruptSegment` naming the file and the frame, and the open truncates nothing.
#[tokio::test(flavor = "multi_thread")]
async fn a_batch_frame_in_a_stream_directory_is_corrupt_segment() {
    let dir = tempfile::tempdir().unwrap();
    let log = StreamLog::open(cfg(dir.path())).await.unwrap();
    log.append(recs(&["a"])).await.unwrap();
    log.close().unwrap();

    let batch = Batch {
        ops: vec![BatchOp::Put {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
        }],
    }
    .encode(&CompressionConfig::none())
    .unwrap();
    let offset = append_frame(dir.path(), 1, FrameKind::Batch, &batch);
    let path = segment_path(dir.path(), 1);
    let len = std::fs::metadata(&path).unwrap().len();

    let err = StreamLog::open(cfg(dir.path())).await.err().unwrap();
    let StorageError::Corruption(msg) = &err else {
        panic!("expected Corruption, got {err:?}");
    };
    assert!(
        msg.starts_with(&format!(
            "corrupt WAL: {} at byte {offset}:",
            path.display()
        )),
        "{msg}"
    );
    assert!(msg.contains("Batch") && msg.contains("lsn 2"), "{msg}");
    assert_eq!(
        std::fs::metadata(&path).unwrap().len(),
        len,
        "not truncated"
    );
}

/// A keyed directory never holds `Records` frames: one there fails replay
/// (`ReplayFailed`, naming the frame), instead of being skipped as today.
#[tokio::test(flavor = "multi_thread")]
async fn a_records_frame_in_a_kv_directory_is_replay_failed() {
    let dir = tempfile::tempdir().unwrap();
    let wal = WalConfig {
        log_dir: dir.path().to_path_buf(),
        ..WalConfig::test_config()
    };
    let adapter = WalStorageAdapter::new(wal.clone()).unwrap();
    adapter.put(b"k", b"v").await.unwrap();
    drop(adapter);

    let records = RecordBatch {
        append_time_ms: 5,
        records: recs(&["stream data"]),
    }
    .encode(&CompressionConfig::none())
    .unwrap();
    let offset = append_frame(dir.path(), 1, FrameKind::Records, &records);

    let err = WalStorageAdapter::new(wal).err().unwrap();
    let StorageError::Corruption(msg) = &err else {
        panic!("expected Corruption, got {err:?}");
    };
    assert!(msg.starts_with("WAL replay failed for lsn"), "{msg}");
    assert!(
        msg.contains(&format!("at byte {offset}")) && msg.contains("Records"),
        "{msg}"
    );
}

// --- STREAM manifest ------------------------------------------------------------------

#[test]
fn the_stream_manifest_round_trips_and_refuses_bit_flips_and_unknown_versions() {
    let manifest = StreamManifest {
        partitions: 3,
        created_by: "0.6.0".into(),
    };
    let bytes = manifest.encode().unwrap();
    assert_eq!(StreamManifest::decode(&bytes), Ok(manifest.clone()));
    assert_eq!(StreamManifest::current(3), {
        let mut m = manifest.clone();
        m.created_by = env!("CARGO_PKG_VERSION").into();
        m
    });

    for i in 0..bytes.len() {
        for bit in 0..8 {
            let mut bad = bytes.clone();
            bad[i] ^= 1 << bit;
            assert!(
                StreamManifest::decode(&bad).is_err(),
                "byte {i} bit {bit} decoded"
            );
        }
    }
    for len in 0..bytes.len() {
        assert!(
            StreamManifest::decode(&bytes[..len]).is_err(),
            "prefix {len}"
        );
    }

    // A future version, correctly checksummed, is refused by its number.
    let mut v2 = bytes.clone();
    v2[8..12].copy_from_slice(&(STREAM_MANIFEST_VERSION + 1).to_le_bytes());
    let body = v2.len() - 4;
    let crc = crc32fast::hash(&v2[..body]);
    v2[body..].copy_from_slice(&crc.to_le_bytes());
    let err = StreamManifest::decode(&v2).unwrap_err();
    assert!(
        err.to_string()
            .contains(&format!("version {}", STREAM_MANIFEST_VERSION + 1)),
        "{err}"
    );

    // Zero partitions is no stream.
    let zero = StreamManifest {
        partitions: 0,
        created_by: "x".into(),
    };
    assert!(zero.encode().is_err());
}

/// Durable reads accept any cursor in the acknowledged range without moving it back
/// when its requested frame has not synced yet.
#[tokio::test(flavor = "multi_thread")]
async fn durable_read_above_its_watermark_keeps_the_requested_cursor() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = cfg(dir.path());
    c.wal.sync_mode = SyncMode::Fast;
    c.wal.sync_interval_ms = 3_600_000;
    let log = StreamLog::open(c).await.unwrap();
    let a = log.append(recs(&["a", "b"])).await.unwrap();
    for cursor in [a.offset(1), log.next_offset()] {
        let read = log
            .read_durable_from(StartAt::Offset(cursor), all())
            .await
            .unwrap();
        assert!(read.records.is_empty());
        assert_eq!(read.next, cursor);
        assert_eq!(read.high_watermark, a.first());
    }
    log.close().unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn checkpoint_covered_records_frames_are_refused_by_keyed_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let wal = WalConfig {
        log_dir: dir.path().to_path_buf(),
        ..WalConfig::test_config()
    };
    let adapter = WalStorageAdapter::new(wal.clone()).unwrap();
    adapter.put(b"k", b"v").await.unwrap();
    adapter.save_checkpoint().unwrap();
    drop(adapter);
    let path = segment_path(dir.path(), 1);
    let file = StdVfs.open(&path, OpenMode::ReadWrite).unwrap();
    let mut replacement = None;
    scan_segment(&*file, &path, 1, &mut |loc, _, payload| {
        let mut frame = Vec::new();
        encode_frame(&mut frame, loc.lsn, FrameKind::Records, payload);
        replacement = Some((loc.offset, frame));
        Ok(())
    })
    .unwrap();
    let (offset, frame) = replacement.unwrap();
    file.write_at(offset, &frame).unwrap();
    file.sync_data().unwrap();
    let before = std::fs::read(&path).unwrap();
    let err = WalStorageAdapter::new(wal).err().unwrap();
    assert!(
        matches!(err, StorageError::Corruption(ref m) if m.contains("WAL replay failed") && m.contains("Records")),
        "{err:?}"
    );
    assert_eq!(std::fs::read(path).unwrap(), before);
}

// Count the actual segment reads, including reads through Wal's cached handles.
#[derive(Default)]
struct ReadCounters {
    bytes: std::sync::atomic::AtomicU64,
    segments: parking_lot::Mutex<std::collections::BTreeSet<PathBuf>>,
    armed: std::sync::atomic::AtomicBool,
    write_armed: std::sync::atomic::AtomicBool,
    fail_sync: std::sync::atomic::AtomicBool,
    syncs: std::sync::atomic::AtomicU64,
    unlocked: tokio::sync::Notify,
    entered: tokio::sync::Notify,
    released: parking_lot::Mutex<bool>,
    resume: parking_lot::Condvar,
}

impl ReadCounters {
    fn reset(&self) {
        self.bytes.store(0, Ordering::SeqCst);
        self.segments.lock().clear();
    }
    fn arm(&self) {
        *self.released.lock() = false;
        self.armed.store(true, Ordering::SeqCst);
    }
    fn arm_write(&self) {
        *self.released.lock() = false;
        self.write_armed.store(true, Ordering::SeqCst);
    }
    fn pause(&self) {
        self.entered.notify_one();
        let mut released = self.released.lock();
        while !*released {
            self.resume.wait(&mut released);
        }
    }
    fn release(&self) {
        *self.released.lock() = true;
        self.resume.notify_all();
    }
}

struct CountingVfs(Arc<ReadCounters>);
struct CountingLock {
    guard: Option<Box<dyn prkdb_core::vfs::LockGuard>>,
    counters: Arc<ReadCounters>,
}
impl prkdb_core::vfs::LockGuard for CountingLock {}
impl Drop for CountingLock {
    fn drop(&mut self) {
        drop(self.guard.take());
        self.counters.unlocked.notify_one();
    }
}
struct CountingFile {
    file: Arc<dyn prkdb_core::vfs::VfsFile>,
    path: PathBuf,
    counters: Arc<ReadCounters>,
}

impl prkdb_core::vfs::VfsFile for CountingFile {
    fn write_at(&self, offset: u64, buf: &[u8]) -> std::io::Result<()> {
        if self.path.extension().is_some_and(|e| e == "wal")
            && self.counters.write_armed.swap(false, Ordering::SeqCst)
        {
            self.counters.pause();
        }
        self.file.write_at(offset, buf)
    }
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.path.extension().is_some_and(|e| e == "wal") {
            self.counters.segments.lock().insert(self.path.clone());
            if self.counters.armed.swap(false, Ordering::SeqCst) {
                self.counters.pause();
            }
            let n = self.file.read_at(offset, buf)?;
            self.counters.bytes.fetch_add(n as u64, Ordering::SeqCst);
            Ok(n)
        } else {
            self.file.read_at(offset, buf)
        }
    }
    fn set_len(&self, len: u64) -> std::io::Result<()> {
        self.file.set_len(len)
    }
    fn len(&self) -> std::io::Result<u64> {
        self.file.len()
    }
    fn sync_data(&self) -> std::io::Result<()> {
        if self.path.extension().is_some_and(|e| e == "wal") {
            if self.counters.fail_sync.swap(false, Ordering::SeqCst) {
                return Err(std::io::Error::other("injected stream sync failure"));
            }
            self.counters.syncs.fetch_add(1, Ordering::SeqCst);
        }
        self.file.sync_data()
    }
}

impl CountingVfs {
    fn wrap(
        &self,
        path: &Path,
        file: Arc<dyn prkdb_core::vfs::VfsFile>,
    ) -> Arc<dyn prkdb_core::vfs::VfsFile> {
        Arc::new(CountingFile {
            file,
            path: path.to_path_buf(),
            counters: self.0.clone(),
        })
    }
}

impl Vfs for CountingVfs {
    fn open(
        &self,
        path: &Path,
        mode: OpenMode,
    ) -> std::io::Result<Arc<dyn prkdb_core::vfs::VfsFile>> {
        Ok(self.wrap(path, StdVfs.open(path, mode)?))
    }
    fn create(&self, path: &Path) -> std::io::Result<Arc<dyn prkdb_core::vfs::VfsFile>> {
        Ok(self.wrap(path, StdVfs.create(path)?))
    }
    fn rename(&self, from: &Path, to: &Path) -> std::io::Result<()> {
        StdVfs.rename(from, to)
    }
    fn remove(&self, path: &Path) -> std::io::Result<()> {
        StdVfs.remove(path)
    }
    fn create_dir_all(&self, path: &Path) -> std::io::Result<()> {
        StdVfs.create_dir_all(path)
    }
    fn read_dir(&self, path: &Path) -> std::io::Result<Vec<PathBuf>> {
        StdVfs.read_dir(path)
    }
    fn exists(&self, path: &Path) -> std::io::Result<bool> {
        StdVfs.exists(path)
    }
    fn sync_dir(&self, path: &Path) -> std::io::Result<()> {
        StdVfs.sync_dir(path)
    }
    fn lock_exclusive(&self, path: &Path) -> std::io::Result<Box<dyn prkdb_core::vfs::LockGuard>> {
        Ok(Box::new(CountingLock {
            guard: Some(StdVfs.lock_exclusive(path)?),
            counters: self.0.clone(),
        }))
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn seek_near_the_tail_of_64_segments_reads_only_the_last_segment() {
    let dir = tempfile::tempdir().unwrap();
    let counters = Arc::new(ReadCounters::default());
    let mut c = cfg(dir.path());
    c.wal.segment_bytes = 4096;
    c.wal.compression = CompressionConfig::none();
    let log = StreamLog::open_with_vfs(Arc::new(CountingVfs(counters.clone())), c)
        .await
        .unwrap();
    let mut last = None;
    for _ in 0..64 {
        last = Some(log.append(vec![rec(&"x".repeat(4000))]).await.unwrap());
    }
    let last = last.unwrap();
    assert_eq!(
        std::fs::read_dir(dir.path())
            .unwrap()
            .filter(|e| e
                .as_ref()
                .unwrap()
                .path()
                .extension()
                .is_some_and(|e| e == "wal"))
            .count(),
        64
    );
    counters.reset();
    let read = log
        .read_from(
            StartAt::Offset(last.first()),
            ReadLimits {
                max_records: 1,
                max_bytes: usize::MAX,
            },
        )
        .await
        .unwrap();
    assert_eq!(read.records[0].offset, last.first());
    assert_eq!(
        *counters.segments.lock(),
        [segment_path(dir.path(), last.lsn)]
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>()
    );
    log.close().unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn sparse_seek_near_the_tail_of_a_large_segment_reads_under_128_kib() {
    let dir = tempfile::tempdir().unwrap();
    let counters = Arc::new(ReadCounters::default());
    let mut c = cfg(dir.path());
    c.wal.compression = CompressionConfig::none();
    let log = StreamLog::open_with_vfs(Arc::new(CountingVfs(counters.clone())), c.clone())
        .await
        .unwrap();
    let mut last = None;
    for _ in 0..256 {
        last = Some(log.append(vec![rec(&"x".repeat(4096))]).await.unwrap());
    }
    let last = last.unwrap();
    // Recovery must rebuild the same sparse index.
    log.close().unwrap();
    let log = StreamLog::open_with_vfs(Arc::new(CountingVfs(counters.clone())), c)
        .await
        .unwrap();
    counters.reset();
    let read = log
        .read_from(
            StartAt::Offset(last.first()),
            ReadLimits {
                max_records: 1,
                max_bytes: usize::MAX,
            },
        )
        .await
        .unwrap();
    assert_eq!(read.records.len(), 1);
    assert_eq!(read.records[0].offset, last.first());
    let bytes = counters.bytes.load(Ordering::SeqCst);
    assert!(bytes < 128 * 1024, "late seek read {bytes} bytes");
    log.close().unwrap();
}

/// Capture the read cap, pause its actual I/O, then make the formerly active segment
/// sealed. Neither that segment nor a new one may expose records above the cap.
#[tokio::test(flavor = "multi_thread")]
async fn read_snapshot_does_not_leak_appends_that_roll_the_active_segment() {
    let dir = tempfile::tempdir().unwrap();
    let counters = Arc::new(ReadCounters::default());
    let mut c = cfg(dir.path());
    c.wal.segment_bytes = 4096;
    c.wal.compression = CompressionConfig::none();
    let log = Arc::new(
        StreamLog::open_with_vfs(Arc::new(CountingVfs(counters.clone())), c)
            .await
            .unwrap(),
    );
    log.append(recs(&["visible"])).await.unwrap();
    let end = log.next_offset();
    counters.arm();
    let task = {
        let log = log.clone();
        tokio::spawn(async move { log.read_from(StartAt::Earliest, all()).await })
    };
    counters.entered.notified().await;
    log.append(recs(&["too new in old segment"])).await.unwrap();
    log.append(vec![rec(&"x".repeat(4000))]).await.unwrap();
    counters.release();
    let read = task.await.unwrap().unwrap();
    assert_eq!(values(&read), ["visible"]);
    assert_eq!(read.high_watermark, end);
    Arc::try_unwrap(log).ok().unwrap().close().unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_cancelled_blocking_read_keeps_the_directory_locked_until_it_finishes() {
    let dir = tempfile::tempdir().unwrap();
    let counters = Arc::new(ReadCounters::default());
    let log = Arc::new(
        StreamLog::open_with_vfs(Arc::new(CountingVfs(counters.clone())), cfg(dir.path()))
            .await
            .unwrap(),
    );
    log.append(recs(&["a"])).await.unwrap();
    counters.arm();
    let task = {
        let log = log.clone();
        tokio::spawn(async move { log.read_from(StartAt::Earliest, all()).await })
    };
    counters.entered.notified().await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    Arc::try_unwrap(log).ok().unwrap().close().unwrap();
    assert!(matches!(
        StreamLog::open(cfg(dir.path())).await,
        Err(StorageError::Locked(_))
    ));
    counters.release();
    counters.unlocked.notified().await;
    StreamLog::open(cfg(dir.path()))
        .await
        .unwrap()
        .close()
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn append_timeout_distinguishes_admission_from_queued_uncertainty() {
    let dir = tempfile::tempdir().unwrap();
    let counters = Arc::new(ReadCounters::default());
    let mut c = cfg(dir.path());
    c.wal.max_queued_bytes = 32;
    c.wal.compression = CompressionConfig::none();
    let log = Arc::new(
        StreamLog::open_with_vfs(Arc::new(CountingVfs(counters.clone())), c)
            .await
            .unwrap(),
    );
    counters.arm_write();
    let first = {
        let log = log.clone();
        tokio::spawn(async move {
            log.append_timeout(vec![rec(&"x".repeat(32))], Duration::from_millis(100))
                .await
        })
    };
    counters.entered.notified().await;
    let rejected = log
        .append_timeout(vec![rec(&"y".repeat(32))], Duration::from_millis(10))
        .await
        .unwrap_err();
    assert!(
        matches!(rejected, StorageError::WriteBackpressure(_)),
        "{rejected:?}"
    );
    let queued = first.await.unwrap().unwrap_err();
    assert!(
        matches!(queued, StorageError::WriteNotConfirmed(_)),
        "{queued:?}"
    );
    counters.release();
    log.sync().await.unwrap();
    let read = log.read_from(StartAt::Earliest, all()).await.unwrap();
    assert_eq!(values(&read), ["x".repeat(32)]);
    Arc::try_unwrap(log).ok().unwrap().close().unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn byte_limit_at_a_frame_boundary_resumes_at_last_returned_plus_one() {
    let dir = tempfile::tempdir().unwrap();
    let log = StreamLog::open(cfg(dir.path())).await.unwrap();
    let first = log.append(recs(&["first"])).await.unwrap();
    let second = log
        .append(recs(&["second record exceeds remainder"]))
        .await
        .unwrap();
    let batch = log
        .read_from(
            StartAt::Earliest,
            ReadLimits {
                max_records: usize::MAX,
                max_bytes: 10,
            },
        )
        .await
        .unwrap();
    assert_eq!(values(&batch), ["first"]);
    assert_eq!(batch.next.raw(), first.last().raw() + 1);
    let resumed = log
        .read_from(StartAt::Offset(batch.next), all())
        .await
        .unwrap();
    assert_eq!(resumed.records[0].offset, second.first());
    log.close().unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn recovered_offsets_past_the_stream_limit_refuse_without_repairing_a_torn_tail() {
    let limit = (1u64 << 48) - 1;
    for (start, with_frame) in [(limit, true), (limit + 1, false)] {
        let dir = tempfile::tempdir().unwrap();
        StreamLog::open(cfg(dir.path()))
            .await
            .unwrap()
            .close()
            .unwrap();
        std::fs::remove_file(segment_path(dir.path(), 1)).unwrap();
        let path = segment_path(dir.path(), start);
        let file = StdVfs.create(&path).unwrap();
        write_segment_header(&*file, start).unwrap();
        if with_frame {
            let payload = RecordBatch {
                append_time_ms: 0,
                records: recs(&["illegal recovered frame"]),
            }
            .encode(&CompressionConfig::none())
            .unwrap();
            let mut frame = Vec::new();
            encode_frame(&mut frame, start, FrameKind::Records, &payload);
            file.write_at(file.len().unwrap(), &frame).unwrap();
        }
        file.write_at(file.len().unwrap(), &[1, 2, 3]).unwrap();
        file.sync_data().unwrap();
        LogState {
            log_start: start,
            deletes_compacted_through: 0,
        }
        .write(&StdVfs, dir.path())
        .unwrap();
        let before = std::fs::read(&path).unwrap();
        let state = std::fs::read(dir.path().join("LOG_STATE")).unwrap();
        assert!(matches!(
            StreamLog::open(cfg(dir.path())).await,
            Err(StorageError::Validation(_))
        ));
        assert_eq!(
            std::fs::read(&path).unwrap(),
            before,
            "recovery did not truncate"
        );
        assert_eq!(std::fs::read(dir.path().join("LOG_STATE")).unwrap(), state);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_appends_can_use_only_the_last_valid_stream_lsn_once() {
    let dir = tempfile::tempdir().unwrap();
    let start = (1u64 << 48) - 2;
    StreamLog::open(cfg(dir.path()))
        .await
        .unwrap()
        .close()
        .unwrap();
    std::fs::remove_file(segment_path(dir.path(), 1)).unwrap();
    let path = segment_path(dir.path(), start);
    let file = StdVfs.create(&path).unwrap();
    write_segment_header(&*file, start).unwrap();
    file.sync_data().unwrap();
    LogState {
        log_start: start,
        deletes_compacted_through: 0,
    }
    .write(&StdVfs, dir.path())
    .unwrap();
    let log = Arc::new(StreamLog::open(cfg(dir.path())).await.unwrap());
    let barrier = Arc::new(tokio::sync::Barrier::new(16));
    let mut tasks = Vec::new();
    for _ in 0..16 {
        let log = log.clone();
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            log.append(recs(&["last"])).await
        }));
    }
    let mut accepted = 0;
    for task in tasks {
        match task.await.unwrap() {
            Ok(ack) => {
                assert_eq!(ack.lsn, start);
                accepted += 1;
            }
            Err(StorageError::Validation(_)) => {}
            other => panic!("unexpected append outcome: {other:?}"),
        }
    }
    assert_eq!(accepted, 1);
    let read = log.read_from(StartAt::Earliest, all()).await.unwrap();
    assert_eq!(read.records.len(), 1);
    assert_eq!(log.next_offset().raw(), (start + 1) << 16);
    Arc::try_unwrap(log).ok().unwrap().close().unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn close_with_a_cancelled_read_syncs_fast_data_and_propagates_sync_failure() {
    for fail in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let counters = Arc::new(ReadCounters::default());
        let mut c = cfg(dir.path());
        c.wal.sync_mode = SyncMode::Fast;
        c.wal.sync_interval_ms = 3_600_000;
        let log = Arc::new(
            StreamLog::open_with_vfs(Arc::new(CountingVfs(counters.clone())), c)
                .await
                .unwrap(),
        );
        log.append(recs(&["fast data"])).await.unwrap();
        assert_eq!(log.durable_end(), log.earliest());
        counters.arm();
        let task = {
            let log = log.clone();
            tokio::spawn(async move { log.read_from(StartAt::Earliest, all()).await })
        };
        counters.entered.notified().await;
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        let before = counters.syncs.load(Ordering::SeqCst);
        counters.fail_sync.store(fail, Ordering::SeqCst);
        let result = Arc::try_unwrap(log).ok().unwrap().close();
        let remained_locked = matches!(
            StreamLog::open(cfg(dir.path())).await,
            Err(StorageError::Locked(_))
        );
        let syncs_after = counters.syncs.load(Ordering::SeqCst);
        counters.release();
        counters.unlocked.notified().await;
        assert!(remained_locked);
        if fail {
            assert!(
                matches!(result, Err(StorageError::Internal(ref m)) if m.contains("injected stream sync failure")),
                "{result:?}"
            );
        } else {
            result.unwrap();
            assert!(syncs_after > before, "close synced before returning");
        }
        let log = StreamLog::open(cfg(dir.path())).await.unwrap();
        assert_eq!(
            values(
                &log.read_durable_from(StartAt::Earliest, all())
                    .await
                    .unwrap()
            ),
            ["fast data"]
        );
        log.close().unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn an_expired_append_deadline_does_not_admit_a_ready_reservation() {
    let dir = tempfile::tempdir().unwrap();
    let log = StreamLog::open(cfg(dir.path())).await.unwrap();
    let result = log
        .append_timeout(recs(&["never queued"]), Duration::ZERO)
        .await;
    log.sync().await.unwrap();
    assert_eq!(log.next_offset(), log.earliest());
    assert!(
        matches!(result, Err(StorageError::WriteBackpressure(_))),
        "{result:?}"
    );
    log.close().unwrap();
}
