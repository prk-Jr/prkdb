# Repo Status

This page is generated from structured evidence and should stay aligned with the latest repo-status snapshot.

Current status: `red`

## Current State

| Dimension | Status | Confidence | Summary |
| --- | --- | --- | --- |
| verification | unknown | low | Verification checks are not collected in snapshot mode yet. |
| docs_coverage | red | high | 1 objective drift finding(s) detected. |
| contract_consistency | red | high | 1 objective drift finding(s) detected. |
| benchmark_credibility | green | high | Benchmark caveat checks passed for the current snapshot scope. |

## Current Findings

1. `roadmap_feature_drift` (docs_coverage) - Roadmap still describes client SDKs as future work even though codegen support exists.
2. `docs_contract_mismatch` (contract_consistency) - Codegen docs point `prkdb-cli serve` users at the HTTP port instead of the local gRPC port.

## Status Stamp

Evidence fingerprint: `repo-status-evidence:v1:verification:unknown:low|docs_coverage:red:high|contract_consistency:red:high|benchmark_credibility:green:high::roadmap_feature_drift:docs_coverage:error|docs_contract_mismatch:contract_consistency:error`

<!-- repo-status-evidence: repo-status-evidence:v1:verification:unknown:low|docs_coverage:red:high|contract_consistency:red:high|benchmark_credibility:green:high::roadmap_feature_drift:docs_coverage:error|docs_contract_mismatch:contract_consistency:error -->

