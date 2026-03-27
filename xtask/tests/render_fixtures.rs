use serde_json::Value;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::{SystemTime, UNIX_EPOCH};

fn run_xtask(args: &[&str], cwd: &Path) -> Output {
    Command::new(env!("CARGO_BIN_EXE_xtask"))
        .current_dir(cwd)
        .args(args)
        .output()
        .expect("failed to run xtask")
}

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(name)
}

fn temp_fixture_copy(name: &str) -> PathBuf {
    let source = fixture_path(name);
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("current time must be after unix epoch")
        .as_nanos();
    let target = std::env::temp_dir().join(format!(
        "xtask-render-fixture-{}-{}-{}",
        name,
        std::process::id(),
        stamp
    ));
    copy_dir_recursively(&source, &target).expect("fixture copy must succeed");
    target
}

fn copy_dir_recursively(source: &Path, target: &Path) -> std::io::Result<()> {
    fs::create_dir_all(target)?;
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let entry_path = entry.path();
        let target_path = target.join(entry.file_name());
        let metadata = entry.metadata()?;
        if metadata.is_dir() {
            copy_dir_recursively(&entry_path, &target_path)?;
        } else {
            fs::copy(entry_path, target_path)?;
        }
    }
    Ok(())
}

#[test]
fn snapshot_json_is_machine_readable_and_written_to_deterministic_target_path() {
    let fixture_root = temp_fixture_copy("docs_contract_mismatch");

    let first = run_xtask(&["repo-status", "snapshot"], &fixture_root);
    assert!(
        first.status.success(),
        "snapshot failed: {:?}",
        first.status
    );
    let first_stdout = String::from_utf8(first.stdout).expect("stdout must be utf8");
    let first_json: Value = serde_json::from_str(&first_stdout).expect("stdout must be JSON");
    assert!(first_json.get("dimensions").is_some());
    assert!(first_json.get("findings").is_some());

    let snapshot_path = fixture_root
        .join("target")
        .join("repo-status")
        .join("repo-status.snapshot.json");
    assert!(
        snapshot_path.exists(),
        "snapshot JSON should be written to {}",
        snapshot_path.display()
    );

    let first_file = fs::read_to_string(&snapshot_path).expect("snapshot file should be readable");
    let first_file_json: Value =
        serde_json::from_str(&first_file).expect("snapshot file must be JSON");
    assert_eq!(first_json, first_file_json);

    let second = run_xtask(&["repo-status", "snapshot"], &fixture_root);
    assert!(
        second.status.success(),
        "snapshot failed: {:?}",
        second.status
    );
    let second_stdout = String::from_utf8(second.stdout).expect("stdout must be utf8");
    assert_eq!(
        first_stdout, second_stdout,
        "snapshot stdout should be deterministic across runs"
    );
}

#[test]
fn markdown_summary_lists_dimensions_and_findings() {
    let fixture_root = temp_fixture_copy("docs_contract_mismatch");
    let output = run_xtask(&["repo-status", "render"], &fixture_root);
    assert!(
        output.status.success(),
        "render failed: status={:?}, stderr={}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr)
    );

    let md = String::from_utf8(output.stdout).expect("markdown output should be utf8");
    assert!(md.contains("Current State"));
    assert!(md.contains("Evidence fingerprint"));
    assert!(md.contains("repo-status-evidence"));

    let rendered_path = fixture_root
        .join("docs")
        .join("status")
        .join("repo-status.md");
    assert!(
        rendered_path.exists(),
        "render should write {}",
        rendered_path.display()
    );
    let rendered = fs::read_to_string(&rendered_path).expect("rendered status page should exist");
    assert!(rendered.contains("Evidence fingerprint"));
    assert!(rendered.contains("repo-status-evidence"));
}

#[test]
fn snapshot_detects_stale_public_status_page() {
    let fixture_root = temp_fixture_copy("docs_contract_mismatch");

    let render_output = run_xtask(&["repo-status", "render"], &fixture_root);
    assert!(
        render_output.status.success(),
        "render failed: status={:?}, stderr={}",
        render_output.status.code(),
        String::from_utf8_lossy(&render_output.stderr)
    );

    let fresh_snapshot = run_xtask(&["repo-status", "snapshot"], &fixture_root);
    assert!(
        fresh_snapshot.status.success(),
        "snapshot failed: status={:?}, stderr={}",
        fresh_snapshot.status.code(),
        String::from_utf8_lossy(&fresh_snapshot.stderr)
    );
    let fresh_report: Value =
        serde_json::from_slice(&fresh_snapshot.stdout).expect("snapshot output must be JSON");
    assert!(
        !has_finding(&fresh_report, "stale_repo_status_summary"),
        "up-to-date rendered status page should not be flagged as stale"
    );

    let rendered_path = fixture_root
        .join("docs")
        .join("status")
        .join("repo-status.md");
    let rendered = fs::read_to_string(&rendered_path).expect("rendered status page should exist");
    let stale = rendered.replace("repo-status-evidence:", "repo-status-evidence:stale-");
    fs::write(&rendered_path, stale).expect("stale status page should be writable");

    let stale_snapshot = run_xtask(&["repo-status", "snapshot"], &fixture_root);
    assert!(
        stale_snapshot.status.success(),
        "snapshot failed: status={:?}, stderr={}",
        stale_snapshot.status.code(),
        String::from_utf8_lossy(&stale_snapshot.stderr)
    );
    let stale_report: Value =
        serde_json::from_slice(&stale_snapshot.stdout).expect("snapshot output must be JSON");
    assert!(has_finding(&stale_report, "stale_repo_status_summary"));
}

fn has_finding(report: &Value, finding_code: &str) -> bool {
    report
        .get("findings")
        .and_then(Value::as_array)
        .map(|findings| {
            findings.iter().any(|finding| {
                finding
                    .get("id")
                    .and_then(Value::as_str)
                    .is_some_and(|id| id == finding_code)
            })
        })
        .unwrap_or(false)
}
