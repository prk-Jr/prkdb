use super::super::model::{Confidence, DimensionId, Finding, Severity, Status, StatusReport};
use std::collections::BTreeSet;

#[allow(dead_code)]
pub(super) fn render_summary_markdown(report: &StatusReport) -> String {
    let mut markdown = String::new();
    markdown.push_str("# Repo Status Summary\n\n");
    markdown.push_str(&format!(
        "Overall status: `{}`\n\n",
        status_label(overall_status(report))
    ));
    markdown.push_str("| Dimension | Status | Confidence | Summary |\n");
    markdown.push_str("| --- | --- | --- | --- |\n");

    for dimension in &report.dimensions {
        markdown.push_str(&format!(
            "| {} | {} | {} | {} |\n",
            dimension_label(dimension.id),
            status_label(dimension.status),
            confidence_label(dimension.confidence),
            escape_table_cell(&dimension.summary),
        ));
    }

    markdown.push_str("\n## Top Findings\n");
    let objective_findings = top_objective_findings(&report.findings, 3);
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

    let advisory_count = report
        .findings
        .iter()
        .filter(|finding| finding.severity == Severity::Warning)
        .count();
    markdown.push_str(&format!("\nAdvisory findings: {advisory_count}\n"));

    markdown.push_str("\n## evidence references\n");
    let mut evidence_references = BTreeSet::new();
    for finding in &report.findings {
        for evidence in &finding.evidence {
            evidence_references.insert(format!("{} - {}", evidence.file, evidence.detail));
        }
    }

    if evidence_references.is_empty() {
        markdown.push_str("No evidence references.\n");
    } else {
        for evidence in evidence_references {
            markdown.push_str(&format!("- {evidence}\n"));
        }
    }

    markdown
}

pub(super) fn render_public_status_markdown(
    report: &StatusReport,
    evidence_fingerprint: &str,
) -> String {
    let mut markdown = String::new();
    markdown.push_str("# Repo Status\n\n");
    markdown.push_str("This page is generated from structured evidence and should stay aligned with the latest repo-status snapshot.\n\n");
    markdown.push_str(&format!(
        "Current status: `{}`\n\n",
        status_label(overall_status(report))
    ));
    markdown.push_str("## Current State\n\n");
    markdown.push_str("| Dimension | Status | Confidence | Summary |\n");
    markdown.push_str("| --- | --- | --- | --- |\n");

    for dimension in &report.dimensions {
        markdown.push_str(&format!(
            "| {} | {} | {} | {} |\n",
            dimension_label(dimension.id),
            status_label(dimension.status),
            confidence_label(dimension.confidence),
            escape_table_cell(&dimension.summary),
        ));
    }

    markdown.push_str("\n## Current Findings\n\n");
    let objective_findings = top_objective_findings(&report.findings, 3);
    if objective_findings.is_empty() {
        markdown.push_str("No objective findings in the current snapshot scope.\n");
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

    markdown.push_str("\n## Status Stamp\n\n");
    markdown.push_str(&format!(
        "Evidence fingerprint: `{}`\n\n",
        evidence_fingerprint
    ));
    markdown.push_str(&format!(
        "<!-- repo-status-evidence: {} -->\n",
        evidence_fingerprint
    ));

    markdown
}

fn top_objective_findings(findings: &[Finding], limit: usize) -> Vec<&Finding> {
    findings
        .iter()
        .filter(|finding| finding.severity == Severity::Error)
        .take(limit)
        .collect()
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

fn escape_table_cell(value: &str) -> String {
    value.replace('|', "\\|")
}
