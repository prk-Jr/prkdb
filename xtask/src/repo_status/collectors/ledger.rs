//! Verification dimension from the remediation ledger (spec §4.3): open critical
//! findings make it red. Warning severity: visible, never fails objective-drift CI.

use super::super::model::{Confidence, DimensionId, Evidence, Finding, Severity};
use crate::remediation::model::{Ledger, Severity as LSev, Status};
use std::path::Path;

pub(in super::super) fn collect(repo_root: &Path) -> Vec<Finding> {
    let Ok(text) = std::fs::read_to_string(repo_root.join("docs/remediation/ledger.toml")) else {
        return vec![];
    };
    let Ok(ledger) = Ledger::parse(&text) else {
        return vec![];
    };
    let open: Vec<_> = ledger
        .finding
        .iter()
        .filter(|f| {
            f.severity == LSev::Critical
                && !matches!(
                    f.status,
                    Status::Verified | Status::WontFix | Status::Duplicate
                )
        })
        .collect();
    if open.is_empty() {
        return vec![];
    }
    vec![Finding {
        id: "open-critical-remediation-findings".into(),
        dimension: DimensionId::Verification,
        severity: Severity::Warning,
        confidence: Confidence::High,
        message: format!(
            "{} critical remediation finding(s) not yet verified: {}",
            open.len(),
            open.iter()
                .map(|f| f.id.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ),
        evidence: vec![Evidence::new(
            "docs/remediation/ledger.toml",
            "see docs/status/remediation.md",
        )],
    }]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_ledger(dir: &Path, body: &str) {
        std::fs::create_dir_all(dir.join("docs/remediation")).unwrap();
        std::fs::write(dir.join("docs/remediation/ledger.toml"), body).unwrap();
    }

    #[test]
    fn open_critical_produces_a_single_warning_naming_only_the_open_finding() {
        let tmp = tempfile::tempdir().unwrap();
        write_ledger(
            tmp.path(),
            r#"
[[finding]]
id = "STO-01"
title = "Open critical finding"
area = "storage"
severity = "critical"
phase = 0
status = "open"

[[finding]]
id = "STO-02"
title = "Verified critical finding"
area = "storage"
severity = "critical"
phase = 0
status = "verified"
"#,
        );

        let findings = collect(tmp.path());

        assert_eq!(findings.len(), 1);
        let finding = &findings[0];
        assert_eq!(finding.dimension, DimensionId::Verification);
        assert_eq!(finding.severity, Severity::Warning);
        assert!(finding.message.contains("STO-01"));
        assert!(!finding.message.contains("STO-02"));
    }

    #[test]
    fn no_open_criticals_produces_no_findings() {
        let tmp = tempfile::tempdir().unwrap();
        write_ledger(
            tmp.path(),
            r#"
[[finding]]
id = "STO-02"
title = "Verified critical finding"
area = "storage"
severity = "critical"
phase = 0
status = "verified"
"#,
        );

        assert!(collect(tmp.path()).is_empty());
    }

    #[test]
    fn missing_ledger_produces_no_findings() {
        let tmp = tempfile::tempdir().unwrap();
        assert!(collect(tmp.path()).is_empty());
    }
}
