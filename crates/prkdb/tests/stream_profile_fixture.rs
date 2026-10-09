#[path = "../benches/support/stream_read_profile.rs"]
mod profile;
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
