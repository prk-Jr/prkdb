use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct StatusReport {
    pub dimensions: Vec<DimensionReport>,
    pub findings: Vec<Finding>,
}

#[derive(Debug)]
pub struct RankedAction {
    pub rank: usize,
    pub severity: Option<Severity>,
    pub title: String,
    pub rationale: String,
}

#[derive(Debug)]
pub struct CommandEvidenceBlock {
    pub label: String,
    pub command: String,
    pub exit_code: Option<i32>,
    pub stdout: String,
    pub stderr: String,
}

#[derive(Debug, Serialize)]
pub struct DimensionReport {
    pub id: DimensionId,
    pub status: Status,
    pub confidence: Confidence,
    pub summary: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DimensionId {
    Verification,
    DocsCoverage,
    ContractConsistency,
    BenchmarkCredibility,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    Green,
    Yellow,
    Red,
    Unknown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Confidence {
    High,
    Medium,
    Low,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
#[allow(dead_code)]
pub enum Severity {
    Error,
    Warning,
}

#[derive(Debug, Serialize)]
pub struct Finding {
    pub id: String,
    pub dimension: DimensionId,
    pub severity: Severity,
    pub confidence: Confidence,
    pub message: String,
    pub evidence: Vec<Evidence>,
}

#[derive(Debug, Serialize)]
pub struct Evidence {
    pub file: String,
    pub detail: String,
}

impl Evidence {
    pub fn new(file: &str, detail: &str) -> Self {
        Self {
            file: file.to_owned(),
            detail: detail.to_owned(),
        }
    }
}
