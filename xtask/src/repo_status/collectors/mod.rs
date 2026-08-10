pub(super) mod contracts;
pub(super) mod docs;
pub(super) mod versions;
pub(super) mod workflows;
pub(super) mod workspace;

use super::model::Finding;
use anyhow::Result;
use std::fs;
use std::path::Path;

pub(super) fn collect_findings(repo_root: &Path) -> Result<Vec<Finding>> {
    let snapshot = RepoSnapshot::load(repo_root)?;
    let mut findings = Vec::new();
    findings.extend(docs::collect(&snapshot));
    findings.extend(contracts::collect(&snapshot));
    findings.extend(versions::collect(&snapshot));
    Ok(findings)
}

pub(super) struct RepoSnapshot {
    pub(super) readme: Option<String>,
    pub(super) roadmap_doc: Option<String>,
    pub(super) codegen_doc: Option<String>,
    pub(super) streaming_doc: Option<String>,
    pub(super) codegen_command: Option<String>,
    pub(super) has_codegen_support: bool,
    pub(super) has_benchmark_workflow: bool,
    /// `workspace.package.version` from the root manifest.
    pub(super) workspace_version: Option<String>,
}

impl RepoSnapshot {
    fn load(repo_root: &Path) -> Result<Self> {
        let readme = read_text(repo_root, "README.md");
        let roadmap_doc = read_text(repo_root, "docs/guide/roadmap.md");
        let codegen_doc = read_text(repo_root, "docs/guide/codegen.md");
        let streaming_doc = read_text(repo_root, "docs/guide/streaming-kafka-comparison.md");
        let codegen_command = read_text(repo_root, "crates/prkdb-cli/src/commands/codegen.rs");
        let ci_workflow = read_text(repo_root, ".github/workflows/ci.yml");

        Ok(Self {
            has_codegen_support: workspace::detect_codegen_support(codegen_command.as_deref()),
            has_benchmark_workflow: workflows::benchmark_job_present(ci_workflow.as_deref()),
            readme,
            roadmap_doc,
            codegen_doc,
            streaming_doc,
            codegen_command,
            workspace_version: workspace_version(repo_root),
        })
    }
}

fn read_text(repo_root: &Path, relative_path: &str) -> Option<String> {
    fs::read_to_string(repo_root.join(relative_path)).ok()
}

/// Read `workspace.package.version` without pulling in a TOML parser.
///
/// The first `version = "..."` under `[workspace.package]` is the workspace version; the
/// naive "first version line in the file" would pick up whichever dependency happened to
/// sort first.
fn workspace_version(repo_root: &Path) -> Option<String> {
    let manifest = read_text(repo_root, "Cargo.toml")?;
    let mut in_workspace_package = false;
    for line in manifest.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with('[') {
            in_workspace_package = trimmed == "[workspace.package]";
            continue;
        }
        if in_workspace_package {
            if let Some(rest) = trimmed.strip_prefix("version") {
                if let Some(value) = rest.split('=').nth(1) {
                    return Some(value.trim().trim_matches('"').to_owned());
                }
            }
        }
    }
    None
}

#[cfg(test)]
impl RepoSnapshot {
    /// Build a snapshot with only the fields a collector under test reads.
    pub(super) fn for_test(workspace_version: Option<String>, roadmap_doc: Option<String>) -> Self {
        Self {
            readme: None,
            roadmap_doc,
            codegen_doc: None,
            streaming_doc: None,
            codegen_command: None,
            has_codegen_support: false,
            has_benchmark_workflow: false,
            workspace_version,
        }
    }
}
