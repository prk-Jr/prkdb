//! Ledger schema (spec §4.1). Unknown fields are rejected so a typo cannot silently
//! drop evidence.

use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Ledger {
    #[serde(default)]
    pub finding: Vec<Finding>,
    #[serde(default)]
    pub phase: Vec<Phase>,
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    Open,
    InProgress,
    Fixed,
    Verified,
    WontFix,
    Duplicate,
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum Severity {
    Critical,
    High,
    Medium,
    Low,
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PhaseStatus {
    NotStarted,
    InProgress,
    GatePassed,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
// `area`, `sources`, `evidence`, and `perf_note` are part of the ledger schema (spec §4.1)
// but not yet read by `check` or `render`; later tasks in the remediation program consume them.
#[allow(dead_code)]
pub struct Finding {
    pub id: String,
    pub title: String,
    pub area: String,
    pub severity: Severity,
    pub phase: u8,
    pub status: Status,
    #[serde(default)]
    pub security: bool,
    #[serde(default)]
    pub sources: Vec<String>,
    #[serde(default)]
    pub evidence: Vec<String>,
    #[serde(default)]
    pub tripwire: String,
    #[serde(default)]
    pub regression_tests: Vec<String>,
    #[serde(default)]
    pub changes: Vec<String>,
    #[serde(default)]
    pub harness: String,
    #[serde(default)]
    pub ci_evidence: String,
    #[serde(default)]
    pub perf_note: String,
    #[serde(default)]
    pub decision: String,
    #[serde(default)]
    pub duplicate_of: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
// `gate` is part of the ledger schema (spec §4.1) but not yet read by `check` or `render`.
#[allow(dead_code)]
pub struct Phase {
    pub id: u8,
    pub title: String,
    pub status: PhaseStatus,
    #[serde(default)]
    pub gate: Vec<String>,
    #[serde(default)]
    pub gate_evidence: Vec<String>,
}

impl Ledger {
    pub fn parse(text: &str) -> anyhow::Result<Self> {
        Ok(toml::from_str(text)?)
    }
}

impl Finding {
    /// Prefix before the dash, e.g. "STO" for "STO-01".
    pub fn prefix(&self) -> &str {
        self.id.split('-').next().unwrap_or("")
    }

    /// Spec §4.1: harness evidence is required for these areas, except KEY-03,
    /// which is proven by golden hash vectors.
    pub fn needs_harness(&self) -> bool {
        matches!(self.prefix(), "STO" | "KEY" | "EVT" | "TXN" | "TTL" | "RFT")
            && self.id != "KEY-03"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"
[[finding]]
id = "STO-01"
title = "Checkpoint recovery drops pre-checkpoint keys"
area = "storage"
severity = "critical"
phase = 2
status = "open"
tripwire = "test:crates/prkdb/tests/tripwires.rs::sto01_checkpoint_drops_pre_checkpoint_keys_tripwire"

[[phase]]
id = 2
title = "Format v2"
status = "not_started"
"#;

    #[test]
    fn parses_sample_ledger() {
        let ledger = Ledger::parse(SAMPLE).unwrap();
        assert_eq!(ledger.finding.len(), 1);
        assert_eq!(ledger.finding[0].status, Status::Open);
        assert!(ledger.finding[0].needs_harness());
        assert_eq!(ledger.phase[0].status, PhaseStatus::NotStarted);
    }

    #[test]
    fn rejects_unknown_field() {
        let bad = SAMPLE.replace("tripwire =", "tripwrie =");
        assert!(Ledger::parse(&bad).is_err());
    }

    #[test]
    fn key03_is_exempt_from_harness() {
        let text = SAMPLE.replace("STO-01", "KEY-03");
        assert!(!Ledger::parse(&text).unwrap().finding[0].needs_harness());
    }
}
