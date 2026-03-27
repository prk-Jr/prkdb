mod collectors;
mod model;
mod render;

use anyhow::{anyhow, Result};
use model::{
    CommandEvidenceBlock, Confidence, DimensionId, DimensionReport, Finding, RankedAction,
    Severity, Status, StatusReport,
};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

const AUDIT_OUTPUT_RELATIVE_PATH: &str = "target/repo-status/repo-audit.latest.md";
const STATUS_PAGE_RELATIVE_PATH: &str = "docs/status/repo-status.md";
const STATUS_EVIDENCE_PREFIX: &str = "<!-- repo-status-evidence:";
const MAX_CAPTURED_COMMAND_CHARS: usize = 4_000;

pub fn snapshot(fail_on_objective_drift: bool) -> Result<()> {
    let repo_root = env::current_dir()?;
    let report = build_report(&repo_root, true)?;
    let json = render::render_snapshot_json(&report)?;
    println!("{json}");
    render::write_snapshot_json(&repo_root, &json)?;
    if fail_on_objective_drift && has_objective_drift(&report) {
        return Err(anyhow!("objective drift detected in repo-status snapshot"));
    }
    Ok(())
}

pub fn audit(run_commands: bool) -> Result<()> {
    let repo_root = env::current_dir()?;
    let report = build_report(&repo_root, false)?;
    let command_evidence = if run_commands {
        collect_command_evidence(&repo_root)
    } else {
        Vec::new()
    };

    let markdown = render_audit_markdown(&report, &command_evidence, run_commands);
    println!("{markdown}");
    write_audit_markdown(&repo_root, &markdown)?;

    let snapshot_json = render::render_snapshot_json(&report)?;
    render::write_snapshot_json(&repo_root, &snapshot_json)?;
    Ok(())
}

pub fn render() -> Result<()> {
    let repo_root = env::current_dir()?;
    let report = build_report(&repo_root, false)?;
    let evidence_fingerprint = report_fingerprint(&report);
    let summary = render::render_public_status_markdown(&report, &evidence_fingerprint);
    println!("{summary}");
    render::write_public_status_markdown(&repo_root, &summary)?;
    Ok(())
}

fn build_report(repo_root: &Path, include_status_page_check: bool) -> Result<StatusReport> {
    let mut findings = collectors::collect_findings(repo_root)?;
    findings.sort_by(|left, right| {
        let left_key = (
            dimension_rank(left.dimension),
            severity_rank(left.severity),
            left.id.as_str(),
        );
        let right_key = (
            dimension_rank(right.dimension),
            severity_rank(right.severity),
            right.id.as_str(),
        );
        left_key.cmp(&right_key)
    });

    let preliminary_dimensions = build_dimensions(&findings);
    if include_status_page_check {
        let expected_fingerprint =
            report_fingerprint_from_parts(&preliminary_dimensions, &findings);
        if let Some(finding) = collect_stale_status_summary(repo_root, &expected_fingerprint) {
            findings.push(finding);
            findings.sort_by(|left, right| {
                let left_key = (
                    dimension_rank(left.dimension),
                    severity_rank(left.severity),
                    left.id.as_str(),
                );
                let right_key = (
                    dimension_rank(right.dimension),
                    severity_rank(right.severity),
                    right.id.as_str(),
                );
                left_key.cmp(&right_key)
            });
        }
    }

    Ok(build_report_from_findings(findings))
}

fn build_dimensions(findings: &[Finding]) -> Vec<DimensionReport> {
    vec![
        build_dimension_report(
            DimensionId::Verification,
            findings,
            "Verification checks are not collected in snapshot mode yet.",
        ),
        build_dimension_report(
            DimensionId::DocsCoverage,
            findings,
            "Documentation drift checks passed for the current snapshot scope.",
        ),
        build_dimension_report(
            DimensionId::ContractConsistency,
            findings,
            "Docs and command-surface contract checks passed for the current snapshot scope.",
        ),
        build_dimension_report(
            DimensionId::BenchmarkCredibility,
            findings,
            "Benchmark caveat checks passed for the current snapshot scope.",
        ),
    ]
}

fn build_report_from_findings(findings: Vec<Finding>) -> StatusReport {
    let dimensions = build_dimensions(&findings);
    StatusReport {
        dimensions,
        findings,
    }
}

fn report_fingerprint(report: &StatusReport) -> String {
    report_fingerprint_from_parts(&report.dimensions, &report.findings)
}

fn collect_stale_status_summary(repo_root: &Path, expected_fingerprint: &str) -> Option<Finding> {
    let status_page = fs::read_to_string(repo_root.join(STATUS_PAGE_RELATIVE_PATH)).ok();
    let observed_fingerprint = status_page
        .as_deref()
        .and_then(extract_status_fingerprint)
        .unwrap_or_else(|| "<missing>".to_owned());

    if observed_fingerprint == expected_fingerprint {
        return None;
    }

    Some(Finding {
        id: "stale_repo_status_summary".to_owned(),
        dimension: DimensionId::DocsCoverage,
        severity: Severity::Error,
        confidence: Confidence::High,
        message: "Committed repo-status page does not match the current repo-status evidence fingerprint.".to_owned(),
        evidence: vec![
            model::Evidence::new(
                STATUS_PAGE_RELATIVE_PATH,
                &format!(
                    "Expected evidence fingerprint `{expected_fingerprint}` but found `{observed_fingerprint}`."
                ),
            ),
        ],
    })
}

fn report_fingerprint_from_parts(dimensions: &[DimensionReport], findings: &[Finding]) -> String {
    let dimension_bits = dimensions
        .iter()
        .map(|dimension| {
            format!(
                "{}:{}:{}",
                dimension_label(dimension.id),
                status_label(dimension.status),
                confidence_label(dimension.confidence)
            )
        })
        .collect::<Vec<_>>()
        .join("|");

    let finding_bits = findings
        .iter()
        .map(|finding| {
            format!(
                "{}:{}:{}",
                finding.id,
                dimension_label(finding.dimension),
                severity_label(Some(finding.severity))
            )
        })
        .collect::<Vec<_>>()
        .join("|");

    format!("repo-status-evidence:v1:{dimension_bits}::{finding_bits}")
}

fn extract_status_fingerprint(markdown: &str) -> Option<String> {
    markdown.lines().find_map(|line| {
        let trimmed = line.trim();
        if !trimmed.starts_with(STATUS_EVIDENCE_PREFIX) {
            return None;
        }

        let fingerprint = trimmed
            .strip_prefix(STATUS_EVIDENCE_PREFIX)?
            .strip_suffix("-->")?
            .trim();

        if fingerprint.is_empty() {
            None
        } else {
            Some(fingerprint.to_owned())
        }
    })
}

fn dimension_rank(dimension: DimensionId) -> u8 {
    match dimension {
        DimensionId::Verification => 0,
        DimensionId::DocsCoverage => 1,
        DimensionId::ContractConsistency => 2,
        DimensionId::BenchmarkCredibility => 3,
    }
}

fn severity_rank(severity: Severity) -> u8 {
    match severity {
        Severity::Error => 0,
        Severity::Warning => 1,
    }
}

fn build_dimension_report(
    id: DimensionId,
    findings: &[Finding],
    passing_summary: &'static str,
) -> DimensionReport {
    if id == DimensionId::Verification {
        return DimensionReport {
            id,
            status: Status::Unknown,
            confidence: Confidence::Low,
            summary: passing_summary.to_owned(),
        };
    }

    let relevant = findings.iter().filter(|finding| finding.dimension == id);
    let mut has_error = false;
    let mut has_warning = false;
    let mut confidence = Confidence::High;

    for finding in relevant {
        match finding.severity {
            Severity::Error => has_error = true,
            Severity::Warning => has_warning = true,
        }

        if finding.confidence == Confidence::Medium {
            confidence = Confidence::Medium;
        }
    }

    let (status, summary) = if has_error {
        (
            Status::Red,
            format!(
                "{} objective drift finding(s) detected.",
                count_findings(id, findings)
            ),
        )
    } else if has_warning {
        (
            Status::Yellow,
            format!(
                "{} advisory finding(s) detected.",
                count_findings(id, findings)
            ),
        )
    } else {
        (Status::Green, passing_summary.to_owned())
    };

    DimensionReport {
        id,
        status,
        confidence,
        summary,
    }
}

fn count_findings(id: DimensionId, findings: &[Finding]) -> usize {
    findings
        .iter()
        .filter(|finding| finding.dimension == id)
        .count()
}

fn has_objective_drift(report: &StatusReport) -> bool {
    report
        .findings
        .iter()
        .any(|finding| finding.severity == Severity::Error)
}

fn collect_command_evidence(repo_root: &Path) -> Vec<CommandEvidenceBlock> {
    let hooks: [(&str, &str, &[&str]); 2] = [
        ("Workspace changes", "git", &["status", "--short"]),
        (
            "Focused repo-status tests",
            "cargo",
            &[
                "test",
                "-p",
                "xtask",
                "--test",
                "snapshot_fixtures",
                "audit_mode_emits_detailed_markdown_sections",
                "--",
                "--nocapture",
            ],
        ),
    ];

    hooks
        .iter()
        .map(|(label, program, args)| run_command_hook(repo_root, label, program, args))
        .collect()
}

fn run_command_hook(
    repo_root: &Path,
    label: &str,
    program: &str,
    args: &[&str],
) -> CommandEvidenceBlock {
    let command = if args.is_empty() {
        program.to_owned()
    } else {
        format!("{program} {}", args.join(" "))
    };

    match Command::new(program)
        .current_dir(repo_root)
        .args(args)
        .output()
    {
        Ok(output) => CommandEvidenceBlock {
            label: label.to_owned(),
            command,
            exit_code: output.status.code(),
            stdout: truncate_for_markdown(&String::from_utf8_lossy(&output.stdout)),
            stderr: truncate_for_markdown(&String::from_utf8_lossy(&output.stderr)),
        },
        Err(error) => CommandEvidenceBlock {
            label: label.to_owned(),
            command,
            exit_code: None,
            stdout: String::new(),
            stderr: format!("failed to execute command hook: {error}"),
        },
    }
}

fn truncate_for_markdown(value: &str) -> String {
    let normalized = value.replace("\r\n", "\n");
    if normalized.chars().count() <= MAX_CAPTURED_COMMAND_CHARS {
        return normalized;
    }

    let truncated = normalized
        .chars()
        .take(MAX_CAPTURED_COMMAND_CHARS)
        .collect::<String>();
    format!("{truncated}\n...[truncated command output]")
}

fn render_audit_markdown(
    report: &StatusReport,
    command_evidence: &[CommandEvidenceBlock],
    command_hooks_enabled: bool,
) -> String {
    let mut markdown = String::new();
    markdown.push_str("# Repo Status Deep Audit\n\n");
    markdown.push_str(&format!(
        "Overall status: `{}`\n\n",
        status_label(overall_status(report))
    ));

    markdown.push_str("## Dimension Severity Summary\n\n");
    markdown.push_str("| Dimension | Status | Severity | Confidence | Summary |\n");
    markdown.push_str("| --- | --- | --- | --- | --- |\n");
    for dimension in &report.dimensions {
        markdown.push_str(&format!(
            "| {} | {} | {} | {} | {} |\n",
            dimension_label(dimension.id),
            status_label(dimension.status),
            severity_label(severity_for_dimension(dimension.id, &report.findings)),
            confidence_label(dimension.confidence),
            escape_table_cell(&dimension.summary),
        ));
    }

    markdown.push_str("\n## Objective Findings\n\n");
    let objective_findings = report
        .findings
        .iter()
        .filter(|finding| finding.severity == Severity::Error)
        .collect::<Vec<_>>();
    if objective_findings.is_empty() {
        markdown.push_str("No objective findings.\n");
    } else {
        for (index, finding) in objective_findings.iter().enumerate() {
            markdown.push_str(&format!(
                "{}. `{}` ({}) - {}\n",
                index + 1,
                finding.id,
                dimension_label(finding.dimension),
                finding.message
            ));
        }
    }

    markdown.push_str("\n## Advisory Findings\n\n");
    let advisory_findings = report
        .findings
        .iter()
        .filter(|finding| finding.severity == Severity::Warning)
        .collect::<Vec<_>>();
    if advisory_findings.is_empty() {
        markdown.push_str("No advisory findings.\n");
    } else {
        for (index, finding) in advisory_findings.iter().enumerate() {
            markdown.push_str(&format!(
                "{}. `{}` ({}) - {}\n",
                index + 1,
                finding.id,
                dimension_label(finding.dimension),
                finding.message
            ));
        }
    }

    markdown.push_str("\n## Next Actions\n\n");
    let next_actions = build_ranked_next_actions(report);
    for action in next_actions {
        markdown.push_str(&format!(
            "{}. {} {} - {}\n",
            action.rank,
            severity_tag(action.severity),
            action.title,
            action.rationale
        ));
    }

    if command_hooks_enabled {
        markdown.push_str("\n## Command Evidence\n\n");
        if command_evidence.is_empty() {
            markdown.push_str("No command evidence captured.\n");
        } else {
            for block in command_evidence {
                markdown.push_str(&format!(
                    "### {}\n\nCommand: `{}`\n\nExit code: `{}`\n\n",
                    block.label,
                    block.command,
                    block
                        .exit_code
                        .map(|code| code.to_string())
                        .unwrap_or_else(|| "none".to_owned())
                ));

                if !block.stdout.trim().is_empty() {
                    markdown.push_str("```text\n");
                    markdown.push_str(&block.stdout);
                    if !block.stdout.ends_with('\n') {
                        markdown.push('\n');
                    }
                    markdown.push_str("```\n\n");
                }

                if !block.stderr.trim().is_empty() {
                    markdown.push_str("stderr:\n\n```text\n");
                    markdown.push_str(&block.stderr);
                    if !block.stderr.ends_with('\n') {
                        markdown.push('\n');
                    }
                    markdown.push_str("```\n\n");
                }
            }
        }
    } else {
        markdown.push_str(
            "\nCommand evidence hooks are disabled by default. Re-run with `repo-status audit --run-commands` to include execution evidence.\n",
        );
    }

    markdown
}

fn severity_for_dimension(dimension: DimensionId, findings: &[Finding]) -> Option<Severity> {
    if findings
        .iter()
        .any(|finding| finding.dimension == dimension && finding.severity == Severity::Error)
    {
        return Some(Severity::Error);
    }

    if findings
        .iter()
        .any(|finding| finding.dimension == dimension && finding.severity == Severity::Warning)
    {
        return Some(Severity::Warning);
    }

    None
}

fn build_ranked_next_actions(report: &StatusReport) -> Vec<RankedAction> {
    let mut prioritized = report.findings.iter().collect::<Vec<_>>();
    prioritized.sort_by(|left, right| {
        let left_key = (
            severity_rank(left.severity),
            dimension_rank(left.dimension),
            left.id.as_str(),
        );
        let right_key = (
            severity_rank(right.severity),
            dimension_rank(right.dimension),
            right.id.as_str(),
        );
        left_key.cmp(&right_key)
    });

    let mut actions = prioritized
        .iter()
        .enumerate()
        .map(|(idx, finding)| RankedAction {
            rank: idx + 1,
            severity: Some(finding.severity),
            title: format!(
                "Address `{}` in `{}`",
                finding.id,
                dimension_label(finding.dimension)
            ),
            rationale: finding.message.clone(),
        })
        .collect::<Vec<_>>();

    if actions.is_empty() {
        actions.push(RankedAction {
            rank: 1,
            severity: None,
            title: "No remediation needed for current snapshot scope".to_owned(),
            rationale: "Continue monitoring and rerun repo-status audit after meaningful code or docs changes.".to_owned(),
        });
    }

    actions
}

fn write_audit_markdown(repo_root: &Path, markdown: &str) -> Result<PathBuf> {
    let output_path = audit_output_path(repo_root);
    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&output_path, format!("{markdown}\n"))?;
    Ok(output_path)
}

fn audit_output_path(repo_root: &Path) -> PathBuf {
    repo_root.join(AUDIT_OUTPUT_RELATIVE_PATH)
}

fn overall_status(report: &StatusReport) -> Status {
    if report
        .dimensions
        .iter()
        .any(|dimension| dimension.status == Status::Red)
    {
        return Status::Red;
    }

    if report
        .dimensions
        .iter()
        .any(|dimension| dimension.status == Status::Yellow)
    {
        return Status::Yellow;
    }

    if report
        .dimensions
        .iter()
        .any(|dimension| dimension.status == Status::Unknown)
    {
        return Status::Unknown;
    }

    Status::Green
}

fn dimension_label(dimension: DimensionId) -> &'static str {
    match dimension {
        DimensionId::Verification => "verification",
        DimensionId::DocsCoverage => "docs_coverage",
        DimensionId::ContractConsistency => "contract_consistency",
        DimensionId::BenchmarkCredibility => "benchmark_credibility",
    }
}

fn status_label(status: Status) -> &'static str {
    match status {
        Status::Green => "green",
        Status::Yellow => "yellow",
        Status::Red => "red",
        Status::Unknown => "unknown",
    }
}

fn confidence_label(confidence: Confidence) -> &'static str {
    match confidence {
        Confidence::High => "high",
        Confidence::Medium => "medium",
        Confidence::Low => "low",
    }
}

fn severity_label(severity: Option<Severity>) -> &'static str {
    match severity {
        Some(Severity::Error) => "error",
        Some(Severity::Warning) => "warning",
        None => "none",
    }
}

fn severity_tag(severity: Option<Severity>) -> &'static str {
    match severity {
        Some(Severity::Error) => "[error]",
        Some(Severity::Warning) => "[warning]",
        None => "[info]",
    }
}

fn escape_table_cell(value: &str) -> String {
    value.replace('|', "\\|")
}
