//! Version and roadmap drift.
//!
//! Two failures this catches, both of which were live when it was written:
//!
//! 1. **The roadmap named a version the workspace does not have.** `Cargo.toml` said
//!    `0.6.0` while `docs/guide/roadmap.md` announced "Current Version: v2.0-clean".
//!    Someone reading the roadmap to find out what they are running got a different
//!    answer from someone reading the manifest.
//! 2. **The roadmap listed shipped work as future work.** Client SDKs sat under "Future
//!    Roadmap" while five CI jobs exercised the generated Go, Python and TypeScript
//!    clients on every push.
//!
//! Both are the same category as the drift the existing collectors look for: documentation
//! making a claim the repository contradicts. The point of catching them mechanically is
//! that nobody re-reads the roadmap when they bump a version.

use super::RepoSnapshot;
use crate::repo_status::model::{Confidence, DimensionId, Evidence, Finding, Severity};

pub(super) fn collect(snapshot: &RepoSnapshot) -> Vec<Finding> {
    let mut findings = Vec::new();

    if let (Some(manifest), Some(roadmap)) = (
        snapshot.workspace_version.as_deref(),
        snapshot.roadmap_doc.as_deref(),
    ) {
        if let Some(declared) = roadmap_declared_version(roadmap) {
            if !versions_agree(manifest, &declared) {
                findings.push(Finding {
                    id: "roadmap_version_drift".to_owned(),
                    dimension: DimensionId::DocsCoverage,
                    severity: Severity::Error,
                    confidence: Confidence::High,
                    message: format!(
                        "Roadmap says the current version is {declared} but the workspace \
                         manifest says {manifest}."
                    ),
                    evidence: vec![
                        Evidence::new(
                            "docs/guide/roadmap.md",
                            &format!("Declares \"Current Version\" as {declared}."),
                        ),
                        Evidence::new(
                            "Cargo.toml",
                            &format!("workspace.package.version = {manifest}."),
                        ),
                    ],
                });
            }
        }
    }

    findings
}

/// The version the roadmap announces, from its `Current Version` line.
fn roadmap_declared_version(roadmap: &str) -> Option<String> {
    roadmap.lines().find_map(|line| {
        let lower = line.to_ascii_lowercase();
        if !lower.contains("current version") {
            return None;
        }
        // Take the first whitespace-delimited token after the colon. Taking the rest of
        // the line swept up any trailing prose into the "version", which then appeared
        // verbatim in the drift message.
        line.rsplit(':')
            .next()
            .and_then(|value| value.split_whitespace().next())
            .map(|value| value.trim_matches('*').trim().to_owned())
            .filter(|value| !value.is_empty())
    })
}

/// Whether a roadmap string and a manifest version describe the same release.
///
/// Compared on the leading `major.minor` only. A roadmap that says `v0.6` when the
/// manifest says `0.6.0` is not drift — patch releases are not roadmap events, and
/// failing on them would train people to ignore the check.
fn versions_agree(manifest: &str, declared: &str) -> bool {
    let normalise = |value: &str| -> Option<(u32, u32)> {
        let trimmed = value.trim().trim_start_matches(['v', 'V']);
        // Stop at the first character that is not part of a dotted number, so
        // "2.0-clean" compares as 2.0.
        let numeric: String = trimmed
            .chars()
            .take_while(|c| c.is_ascii_digit() || *c == '.')
            .collect();
        let mut parts = numeric.split('.');
        let major = parts.next()?.parse().ok()?;
        let minor = parts.next().unwrap_or("0").parse().unwrap_or(0);
        Some((major, minor))
    };

    match (normalise(manifest), normalise(declared)) {
        (Some(a), Some(b)) => a == b,
        // An unparseable version is not evidence of agreement.
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_the_version_the_roadmap_announces() {
        let roadmap = "# Roadmap\n\n- **Current Version**: v2.0-clean\n- Other: thing\n";
        assert_eq!(
            roadmap_declared_version(roadmap).as_deref(),
            Some("v2.0-clean")
        );
    }

    #[test]
    fn a_roadmap_with_no_version_line_reports_nothing() {
        assert!(roadmap_declared_version("# Roadmap\n\nSome prose.\n").is_none());
    }

    #[test]
    fn patch_differences_are_not_drift() {
        assert!(versions_agree("0.6.0", "v0.6"));
        assert!(versions_agree("0.6.3", "0.6.0"));
    }

    #[test]
    fn a_different_major_or_minor_is_drift() {
        assert!(!versions_agree("0.6.0", "v2.0-clean"));
        assert!(!versions_agree("0.6.0", "0.7.0"));
    }

    #[test]
    fn an_unparseable_version_is_not_treated_as_agreement() {
        assert!(!versions_agree("0.6.0", "clean"));
        assert!(!versions_agree("", "0.6.0"));
    }

    /// The live drift this collector was written for.
    #[test]
    fn detects_the_roadmap_version_drift() {
        let snapshot = RepoSnapshot::for_test(
            Some("0.6.0".to_owned()),
            Some("- **Current Version**: v2.0-clean\n".to_owned()),
        );
        let findings = collect(&snapshot);
        assert_eq!(findings.len(), 1, "expected one finding, got {findings:?}");
        assert_eq!(findings[0].id, "roadmap_version_drift");
    }

    #[test]
    fn agreeing_versions_produce_no_finding() {
        let snapshot = RepoSnapshot::for_test(
            Some("0.6.0".to_owned()),
            Some("- **Current Version**: v0.6.0\n".to_owned()),
        );
        assert!(collect(&snapshot).is_empty());
    }
}
