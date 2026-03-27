# Repo Status Schema

This page defines the status vocabulary used by the repo-status tool and the curated status page.

## Dimensions

- `verification`: build, lint, and test posture.
- `docs_coverage`: whether the docs match what the codebase actually ships.
- `contract_consistency`: whether CLI, server, and docs agree on ports, endpoints, and behavior.
- `benchmark_credibility`: whether benchmark claims carry the right caveats and comparisons.

## Severity

- `error`: objective drift that should fail CI.
- `warning`: advisory risk that should inform maintainers but not block PRs by default.

## Confidence

- `high`: the evidence is direct and specific.
- `medium`: the evidence is strong but somewhat indirect.
- `low`: the signal is partial or not yet collected.

## Finding Types

- Objective drift means the repo contradicts itself in a way that can be checked mechanically.
- Interpretive risk means the wording or structure is easy to misread, but not necessarily wrong.

## Evidence Fingerprint

The public status page includes a generated evidence fingerprint. `cargo run -p xtask -- repo-status snapshot`
uses that fingerprint to detect when the committed status page has drifted from the current evidence.
