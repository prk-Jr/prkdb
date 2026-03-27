use super::RepoSnapshot;
use crate::repo_status::model::{Confidence, DimensionId, Evidence, Finding, Severity};

pub(super) fn collect(snapshot: &RepoSnapshot) -> Vec<Finding> {
    let mut findings = Vec::new();

    if snapshot.has_codegen_support
        && snapshot
            .roadmap_doc
            .as_deref()
            .is_some_and(|text| text.contains("Client SDKs"))
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
