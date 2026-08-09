use super::RepoSnapshot;
use crate::repo_status::model::{Confidence, DimensionId, Evidence, Finding, Severity};

pub(super) fn collect(snapshot: &RepoSnapshot) -> Vec<Finding> {
    let mut findings = Vec::new();

    // Section-aware on purpose. This used to match "Client SDKs" anywhere in the file,
    // so moving the entry from Future to Completed — the exact fix the finding asks for —
    // left it firing. A drift detector that cannot be satisfied gets muted, and then it
    // detects nothing at all.
    if snapshot.has_codegen_support
        && snapshot
            .roadmap_doc
            .as_deref()
            .is_some_and(|text| future_section_mentions(text, "Client SDKs"))
    {
        findings.push(Finding {
            id: "roadmap_feature_drift".to_owned(),
            dimension: DimensionId::DocsCoverage,
            severity: Severity::Error,
            confidence: Confidence::High,
            message: "Roadmap still describes client SDKs as future work even though codegen support exists.".to_owned(),
            evidence: vec![
                Evidence::new("docs/guide/roadmap.md", "Contains a future-work entry for client SDKs."),
                Evidence::new(
                    "crates/prkdb-cli/src/commands/codegen.rs",
                    "Exports TypeScript, Python, and Go codegen support.",
                ),
            ],
        });
    }

    if snapshot.has_benchmark_workflow && !has_required_benchmark_caveat(snapshot) {
        findings.push(Finding {
            id: "missing_benchmark_caveat".to_owned(),
            dimension: DimensionId::BenchmarkCredibility,
            severity: Severity::Error,
            confidence: Confidence::High,
            message: "Benchmark-facing docs are missing the required caveat about non-comparable Kafka and PrkDB measurements.".to_owned(),
            evidence: vec![
                Evidence::new("README.md", "Benchmark section does not include the required caveat language."),
                Evidence::new(
                    "docs/guide/streaming-kafka-comparison.md",
                    "Streaming comparison page does not include the required caveat language.",
                ),
            ],
        });
    }

    findings
}

fn has_required_benchmark_caveat(snapshot: &RepoSnapshot) -> bool {
    [
        snapshot.readme.as_deref(),
        snapshot.streaming_doc.as_deref(),
    ]
    .into_iter()
    .flatten()
    .any(contains_benchmark_caveat)
}

fn contains_benchmark_caveat(text: &str) -> bool {
    text.contains("not an apples-to-apples")
        || text.contains("not a fair head-to-head system comparison")
}

/// Whether the roadmap's future-work section mentions `needle`.
///
/// "Future" runs from the first heading containing "Future" to the next heading at the
/// same level, so an item moved into a Completed section stops matching.
fn future_section_mentions(roadmap: &str, needle: &str) -> bool {
    let mut in_future = false;
    for line in roadmap.lines() {
        if line.starts_with("## ") {
            in_future = line.to_ascii_lowercase().contains("future");
            continue;
        }
        if in_future && line.contains(needle) {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod future_section_tests {
    use super::future_section_mentions;

    const ROADMAP: &str = "\
# Roadmap
## Completed
- [x] **Client SDKs**: shipped
## Future Roadmap
- [ ] **SQL Layer**: later
## Other
- [ ] **Client SDKs**: not here either
";

    #[test]
    fn an_item_under_completed_does_not_count_as_future_work() {
        assert!(!future_section_mentions(ROADMAP, "Client SDKs"));
    }

    #[test]
    fn an_item_under_future_counts() {
        assert!(future_section_mentions(ROADMAP, "SQL Layer"));
    }

    #[test]
    fn a_section_after_future_is_not_future() {
        // "Other" follows Future, so its entries must not be attributed to it.
        assert!(!future_section_mentions(ROADMAP, "not here either"));
    }
}
