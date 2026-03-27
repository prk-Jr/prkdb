use std::fs;
use std::path::PathBuf;

#[test]
fn ci_integration_markers_present() {
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("..");
    let ci_yaml =
        fs::read_to_string(repo_root.join(".github/workflows/ci.yml")).expect("ci workflow");

    assert!(ci_yaml.contains("repo-status snapshot"));
    assert!(ci_yaml.contains("repo-status.snapshot.json"));
    assert!(ci_yaml.contains("--fail-on-objective-drift"));
}

#[test]
fn repo_audit_workflow_markers_present() {
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("..");
    let audit_yaml = fs::read_to_string(repo_root.join(".github/workflows/repo-audit.yml"))
        .expect("repo-audit workflow");

    assert!(audit_yaml.contains("schedule:"));
    assert!(audit_yaml.contains("workflow_dispatch:"));
    assert!(audit_yaml.contains("repo-status audit"));
    assert!(audit_yaml.contains("repo-audit.latest.md"));
    assert!(audit_yaml.contains("repo-status.snapshot.json"));
}
