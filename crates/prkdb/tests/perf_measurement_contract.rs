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
fn native_controls_do_not_instrument() {
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
}

#[test]
fn benchmark_controls_require_explicit_environment_values() {
    for (case, canary, skip) in [
        ("missing_both", None, None),
        ("missing_canary", None, Some("0")),
        ("missing_skip", Some("0"), None),
        ("explicit_zero", Some("0"), Some("0")),
        ("explicit_canary", Some("4096"), Some("0")),
        ("explicit_skip", Some("0"), Some("1")),
    ] {
        // Each child owns its environment. No process-wide env mutation can race
        // with the operation tests or another control case in this test process.
        let mut command = std::process::Command::new(std::env::current_exe().unwrap());
        command
            .args(["--exact", "benchmark_controls_env_child", "--nocapture"])
            .env("PRKDB_PERF_CONTRACT_CHILD_CASE", case)
            .env_remove("PRKDB_PERF_CANARY")
            .env_remove("PRKDB_PERF_SKIP_OPERATION");
        if let Some(value) = canary {
            command.env("PRKDB_PERF_CANARY", value);
        }
        if let Some(value) = skip {
            command.env("PRKDB_PERF_SKIP_OPERATION", value);
        }
        let output = command.output().unwrap();
        assert!(
            output.status.success(),
            "control case {case} failed:\n{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let stderr = String::from_utf8_lossy(&output.stderr);
        let receipts: Vec<_> = stderr
            .lines()
            .filter(|line| line.starts_with("PRKDB_PERF_EFFECTIVE "))
            .collect();
        if let (Some(canary), Some(skip)) = (canary, skip) {
            assert_eq!(
                receipts,
                vec![format!("PRKDB_PERF_EFFECTIVE canary={canary} skip={skip}")],
                "control case {case} must report the effective child controls"
            );
        } else {
            assert!(receipts.is_empty(), "invalid control case {case}: {stderr}");
        }
    }
}

#[test]
fn benchmark_controls_env_child() {
    let Ok(case) = std::env::var("PRKDB_PERF_CONTRACT_CHILD_CASE") else {
        return;
    };
    // Parsing benchmark controls never issues a client request, so this subprocess
    // remains portable even though successful controls must enable instrumentation.
    let result = Controls::from_env();
    match case.as_str() {
        "missing_both" | "missing_canary" => {
            let error = result.expect_err("missing canary must fail before measurement");
            assert!(error.contains("PRKDB_PERF_CANARY"), "{error}");
        }
        "missing_skip" => {
            let error = result.expect_err("missing skip control must fail before measurement");
            assert!(error.contains("PRKDB_PERF_SKIP_OPERATION"), "{error}");
        }
        "explicit_zero" | "explicit_canary" | "explicit_skip" => {
            let controls = result.unwrap();
            assert!(controls.instrumentation_enabled());
            assert_eq!(
                controls.canary_iterations,
                if case == "explicit_canary" { 4096 } else { 0 }
            );
            assert_eq!(controls.skip_operation, case == "explicit_skip");
        }
        _ => panic!("unknown control subprocess case: {case}"),
    }
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
