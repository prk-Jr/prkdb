#[path = "../benches/support/stream_read_profile.rs"]
pub mod profile;
use prkdb::stream_log::{AppendAck, EventSeq, ReadBatch, StoredRecord};
use profile::{ExpectedFrame, Fixture};

fn fixture() -> (Fixture, ReadBatch) {
    let expected = Fixture {
        frames: vec![ExpectedFrame {
            ack: AppendAck { lsn: 1, count: 2 },
            time_ms: 1234,
        }],
        batch: 2,
        value: vec![0x42; 16],
    };
    let records = (0..2)
        .map(|i| StoredRecord {
            offset: EventSeq::from_wal(1, i),
            append_time_ms: 1234,
            key: Some(format!("k{i:06}").into_bytes()),
            value: vec![0x42; 16],
            headers: vec![],
        })
        .collect();
    let page = ReadBatch {
        records,
        next: EventSeq::from_wal(1, 2),
        high_watermark: EventSeq::from_wal(2, 0),
    };
    (expected, page)
}

#[test]
fn fixture_accepts_all_fields_and_independent_cursors() {
    let (expected, page) = fixture();
    assert_eq!(
        expected
            .verify_page(0, EventSeq::from_wal(1, 0), &page)
            .unwrap(),
        2
    );
    let empty = ReadBatch {
        records: vec![],
        next: page.next,
        high_watermark: page.high_watermark,
    };
    assert_eq!(expected.verify_page(2, page.next, &empty).unwrap(), 0);
}
#[test]
fn fixture_refuses_corrupt_fields_order_and_missing_records() {
    let (expected, page) = fixture();
    for defect in 0..8 {
        let mut bad = page.clone();
        match defect {
            0 => bad.records[0].value[0] ^= 1,
            1 => bad.records[0].key = None,
            2 => bad.records[0].headers.push(("unexpected".into(), vec![1])),
            3 => bad.records[0].append_time_ms += 1,
            4 => bad.records[0].offset = EventSeq::from_raw(0),
            5 => bad.records.reverse(),
            6 => {
                bad.records.pop();
            }
            _ => bad.records.push(bad.records[0].clone()),
        }
        assert!(
            expected
                .verify_page(0, EventSeq::from_wal(1, 0), &bad)
                .is_err(),
            "defect {defect}"
        );
    }
}
#[test]
fn fixture_refuses_wrong_resume_and_watermark() {
    let (expected, page) = fixture();
    let mut bad = page.clone();
    bad.next = EventSeq::from_raw(0);
    assert!(expected
        .verify_page(0, EventSeq::from_wal(1, 0), &bad)
        .is_err());
    let mut bad = page;
    bad.high_watermark = EventSeq::from_raw(0);
    assert!(expected
        .verify_page(0, EventSeq::from_wal(1, 0), &bad)
        .is_err());
}
#[test]
fn fixture_refuses_bad_ack_count_and_empty_fixture() {
    let (mut expected, page) = fixture();
    expected.frames[0].ack.count = 1;
    assert!(expected
        .verify_page(0, EventSeq::from_wal(1, 0), &page)
        .is_err());
    expected.frames.clear();
    assert!(expected
        .verify_page(0, EventSeq::from_wal(1, 0), &page)
        .is_err());
}

#[test]
fn recorded_clock_binds_one_timestamp_to_each_append() {
    use prkdb::stream_log::Clock;
    let mut probe = profile::ReadProfiler::new(&[0x42; 16], 2);
    assert!(probe.record_append(AppendAck { lsn: 1, count: 2 }).is_err());
    let time = probe.clock.now_ms();
    probe.record_append(AppendAck { lsn: 1, count: 2 }).unwrap();
    assert_eq!(probe.fixture.frames[0].time_ms, time);
    probe.clock.now_ms();
    probe.clock.now_ms();
    assert!(probe.record_append(AppendAck { lsn: 2, count: 2 }).is_err());
}

#[tokio::test]
async fn fixture_verifies_real_stream_across_frames_and_sparse_empty_cursor() {
    use prkdb::stream_log::{Record, StreamConfig, StreamLog};
    use prkdb_core::wal::{CompressionConfig, SyncMode};
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = StreamConfig::new(dir.path());
    cfg.retention_interval = std::time::Duration::ZERO;
    cfg.wal.sync_mode = SyncMode::Fast;
    cfg.wal.segment_bytes = 1024 * 1024;
    cfg.wal.compression = CompressionConfig::none();
    let mut probe = profile::ReadProfiler::new(&[0x42; 1024], 1024);
    cfg.clock = probe.clock.clone();
    let log = StreamLog::open(cfg).await.unwrap();
    for _ in 0..2 {
        let records = (0..1024)
            .map(|i| Record {
                key: Some(format!("k{i:06}").into_bytes()),
                value: vec![0x42; 1024],
                headers: vec![],
            })
            .collect();
        probe
            .record_append(log.append(records).await.unwrap())
            .unwrap();
    }
    log.sync().await.unwrap();
    assert!(
        std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(Result::ok)
            .filter(|e| e.path().extension().is_some_and(|x| x == "wal"))
            .count()
            >= 2,
        "fixture must roll segments"
    );
    probe.fixture.verify_log(&log).await.unwrap();
    probe.fixture.frames[1].time_ms += 1;
    assert!(probe.fixture.verify_log(&log).await.is_err());
    log.close().unwrap();
}

#[tokio::test]
async fn malformed_ack_is_refused_before_real_fixture_cursor_construction() {
    use prkdb::stream_log::{Record, StreamConfig, StreamLog};
    let dir = tempfile::tempdir().unwrap();
    let log = StreamLog::open(StreamConfig::new(dir.path()))
        .await
        .unwrap();
    let ack = log
        .append(vec![Record {
            key: Some(b"k000000".to_vec()),
            value: vec![0x42; 16],
            headers: vec![],
        }])
        .await
        .unwrap();
    log.sync().await.unwrap();
    let expected = Fixture {
        frames: vec![ExpectedFrame {
            ack: AppendAck { count: 0, ..ack },
            time_ms: 0,
        }],
        batch: 1,
        value: vec![0x42; 16],
    };
    assert!(expected.verify_log(&log).await.is_err());
    log.close().unwrap();
}
