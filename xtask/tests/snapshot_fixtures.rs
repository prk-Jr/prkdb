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

fn snapshot_fixture(name: &str) -> Value {
    let fixture_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(name);
    let output = run_xtask(&["repo-status", "snapshot"], &fixture_root);
    assert!(
        output.status.success(),
        "xtask failed with status {:?}, stderr: {}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("snapshot output must be JSON")
}

fn temp_fixture_copy(name: &str) -> PathBuf {
    let source = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(name);
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("current time must be after unix epoch")
        .as_nanos();
    let target = std::env::temp_dir().join(format!(
        "xtask-snapshot-fixture-{}-{}-{}",
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

#[test]
fn snapshot_output_includes_expected_dimensions() {
    let workspace_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("..");
    let output = run_xtask(&["repo-status", "snapshot"], &workspace_root);
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("\"dimensions\""));
}

#[test]
fn flags_roadmap_feature_claim_that_conflicts_with_codegen_support() {
    let report = snapshot_fixture("roadmap_sdk_drift");
    assert!(has_finding(&report, "roadmap_feature_drift"));
}

#[test]
fn flags_missing_benchmark_caveat_language() {
    let report = snapshot_fixture("missing_benchmark_caveat");
    assert!(has_finding(&report, "missing_benchmark_caveat"));
}

#[test]
fn flags_docs_contract_mismatch_for_codegen_vs_serve() {
    let report = snapshot_fixture("docs_contract_mismatch");
    assert!(has_finding(&report, "docs_contract_mismatch"));
}

#[test]
fn flags_stale_repo_status_summary_when_status_page_is_out_of_date() {
    let fixture_root = temp_fixture_copy("docs_contract_mismatch");
    let status_page = fixture_root
        .join("docs")
        .join("status")
        .join("repo-status.md");
    fs::create_dir_all(status_page.parent().expect("status page parent")).expect("status dir");
    fs::write(
        &status_page,
        "# Repo Status\n\n## Status Stamp\n\nEvidence fingerprint: `repo-status-evidence:v1:stale`\n\n<!-- repo-status-evidence: repo-status-evidence:v1:stale -->\n",
    )
    .expect("status page write");

    let output = run_xtask(&["repo-status", "snapshot"], &fixture_root);
    assert!(
        output.status.success(),
        "snapshot failed with status {:?}, stderr: {}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr)
    );
    let report: Value =
        serde_json::from_slice(&output.stdout).expect("snapshot output must be JSON");
    assert!(has_finding(&report, "stale_repo_status_summary"));
}

#[test]
fn audit_mode_emits_detailed_markdown_sections() {
    let workspace_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("..");
    let output = run_xtask(&["repo-status", "audit"], &workspace_root);
    assert!(
        output.status.success(),
        "audit failed: status={:?}, stderr={}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout).expect("audit output should be utf8");
    assert!(
        stdout.contains("Advisory Findings"),
        "audit output missing advisory findings section:\n{stdout}"
    );
    assert!(
        stdout.contains("Next Actions"),
        "audit output missing next actions section:\n{stdout}"
    );

    let audit_path = workspace_root
        .join("target")
        .join("repo-status")
        .join("repo-audit.latest.md");
    assert!(
        audit_path.exists(),
        "audit markdown should be written to {}",
        audit_path.display()
    );

    let audit_file = fs::read_to_string(&audit_path).expect("audit markdown should be readable");
    assert!(audit_file.contains("Advisory Findings"));
    assert!(audit_file.contains("Next Actions"));
}
