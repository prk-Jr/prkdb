pub(super) mod markdown;

use super::model::StatusReport;
use anyhow::Result;
use std::fs;
use std::path::{Path, PathBuf};

const SNAPSHOT_OUTPUT_RELATIVE_PATH: &str = "target/repo-status/repo-status.snapshot.json";

pub(super) fn render_snapshot_json(report: &StatusReport) -> Result<String> {
    Ok(serde_json::to_string_pretty(report)?)
}

#[allow(dead_code)]
pub(super) fn render_summary_markdown(report: &StatusReport) -> String {
    markdown::render_summary_markdown(report)
}

pub(super) fn render_public_status_markdown(
    report: &StatusReport,
    evidence_fingerprint: &str,
) -> String {
    markdown::render_public_status_markdown(report, evidence_fingerprint)
}

pub(super) fn write_snapshot_json(repo_root: &Path, json: &str) -> Result<PathBuf> {
    let output_path = snapshot_output_path(repo_root);
    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&output_path, format!("{json}\n"))?;
    Ok(output_path)
}

pub(super) fn write_public_status_markdown(repo_root: &Path, markdown: &str) -> Result<PathBuf> {
    let output_path = repo_root.join("docs/status/repo-status.md");
    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&output_path, format!("{markdown}\n"))?;
    Ok(output_path)
}

fn snapshot_output_path(repo_root: &Path) -> PathBuf {
    repo_root.join(SNAPSHOT_OUTPUT_RELATIVE_PATH)
}
