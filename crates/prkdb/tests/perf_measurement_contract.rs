//! Native contract for the helpers shared with the instruction-count benchmark.
//! No Valgrind installation or benchmark runner is needed to exercise the operation
//! bodies and postconditions. Linux raw-profile checks verify instrumentation itself.

#[path = "../benches/support/perf_gate_measurement.rs"]
mod measurement;

use measurement::*;

fn controls(skip: bool) -> Controls {
    Controls::parse(Some("0"), Some(if skip { "1" } else { "0" })).unwrap()
}

#[test]
fn canary_returns_the_number_of_volatile_updates() {
    assert_eq!(canary(0), 0);
    assert_eq!(canary(4096), 4096);
}

#[test]
fn controls_accept_only_the_documented_exact_values() {
    assert_eq!(Controls::parse(None, None).unwrap(), controls(false));
    for canary_value in ["0", "4096"] {
        for skip in ["0", "1"] {
            assert!(Controls::parse(Some(canary_value), Some(skip)).is_ok());
        }
    }
    for invalid in ["", "1", "4095", "4097", "-1", "00", " 0", "0 ", "4096\n"] {
        assert!(
            Controls::parse(Some(invalid), None).is_err(),
            "canary {invalid:?}"
        );
    }
    for invalid in ["", "2", "-1", "00", "true", " 1", "1 ", "0\n"] {
        assert!(
            Controls::parse(None, Some(invalid)).is_err(),
            "skip {invalid:?}"
        );
    }
}

#[test]
fn native_controls_do_not_instrument_but_benchmark_controls_always_do() {
    for canary in [0, 4096] {
        for skip in [false, true] {
            assert!(!Controls::new(canary, skip)
                .unwrap()
                .instrumentation_enabled());
        }
    }
    assert!(!Controls::parse(None, None)
        .unwrap()
        .instrumentation_enabled());
    // Reading the environment does not invoke any client requests or mutate global
    // state. Benchmark setup uses this constructor exclusively.
    assert!(Controls::from_env().unwrap().instrumentation_enabled());
}

#[test]
fn put_preserves_one_hundred_exact_one_kib_values() {
    let fixture = run_wal_put(setup_wal_put(controls(false)));
    validate_wal_put(&fixture).unwrap();
    fixture.rt.block_on(async {
        for i in 0..100 {
            let key = format!("bench-key-{i}");
            assert_eq!(
                fixture.adapter.get(key.as_bytes()).await.unwrap(),
                Some(vec![b'x'; 1024])
            );
        }
        assert_eq!(fixture.adapter.get(b"bench-key-100").await.unwrap(), None);
    });
}

#[test]
fn skipped_put_fails_stored_data_postcondition() {
    let fixture = run_wal_put(setup_wal_put(controls(true)));
    assert!(validate_wal_put(&fixture).is_err());
    assert_eq!(
        fixture
            .rt
            .block_on(fixture.adapter.get(b"bench-key-0"))
            .unwrap(),
        None
    );
}

#[test]
fn get_preserves_the_exact_one_kib_read_result() {
    let fixture = run_wal_get_hit(setup_wal_get_hit(controls(false)));
    validate_wal_get_hit(&fixture).unwrap();
    assert_eq!(fixture.observed, Some(Some(vec![b'x'; 1024])));
}

#[test]
fn skipped_get_fails_read_result_postcondition_even_with_fixture_data_present() {
    let fixture = run_wal_get_hit(setup_wal_get_hit(controls(true)));
    assert!(validate_wal_get_hit(&fixture).is_err());
    assert_eq!(fixture.observed, None);
    assert_eq!(
        fixture
            .rt
            .block_on(fixture.adapter.get(b"bench-key"))
            .unwrap(),
        Some(vec![b'x'; 1024])
    );
}

#[test]
fn batch_preserves_one_hundred_exact_one_kib_entries() {
    let fixture = run_wal_put_batch_100(setup_wal_put_batch_100(controls(false)));
    validate_wal_put_batch_100(&fixture).unwrap();
    fixture.rt.block_on(async {
        for i in 0..100 {
            let key = format!("key-{i}");
            assert_eq!(
                fixture.adapter.get(key.as_bytes()).await.unwrap(),
                Some(vec![b'x'; 1024])
            );
        }
        assert_eq!(fixture.adapter.get(b"key-100").await.unwrap(), None);
    });
}

#[test]
fn skipped_batch_fails_stored_data_postcondition() {
    let fixture = run_wal_put_batch_100(setup_wal_put_batch_100(controls(true)));
    assert!(validate_wal_put_batch_100(&fixture).is_err());
    assert_eq!(
        fixture.rt.block_on(fixture.adapter.get(b"key-0")).unwrap(),
        None
    );
}

#[test]
fn indexed_insert_preserves_record_and_secondary_index() {
    let fixture = run_indexed_insert_one(setup_indexed_insert_one(controls(false)));
    validate_indexed_insert_one(&fixture).unwrap();
    fixture.rt.block_on(async {
        assert_eq!(
            fixture.storage.get::<IaiIndexedRecord>(&1).await.unwrap(),
            Some(fixture.record.clone())
        );
        assert_eq!(
            fixture
                .storage
                .query_by::<IaiIndexedRecord>("name", &"bench-record")
                .await
                .unwrap(),
            vec![fixture.record.clone()]
        );
        assert_eq!(
            fixture.storage.count::<IaiIndexedRecord>().await.unwrap(),
            1
        );
    });
    assert_eq!(fixture.record.data, vec![b'x'; 1024]);
}

#[test]
fn skipped_indexed_insert_fails_record_and_index_postconditions() {
    let fixture = run_indexed_insert_one(setup_indexed_insert_one(controls(true)));
    assert!(validate_indexed_insert_one(&fixture).is_err());
    fixture.rt.block_on(async {
        assert_eq!(
            fixture.storage.get::<IaiIndexedRecord>(&1).await.unwrap(),
            None
        );
        assert!(fixture
            .storage
            .query_by::<IaiIndexedRecord>("name", &"bench-record")
            .await
            .unwrap()
            .is_empty());
    });
}

use prkdb_types::storage::StorageAdapter;
