pub(super) mod contracts;
pub(super) mod docs;
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
        })
    }
}

fn read_text(repo_root: &Path, relative_path: &str) -> Option<String> {
    fs::read_to_string(repo_root.join(relative_path)).ok()
}
