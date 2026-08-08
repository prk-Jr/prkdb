# Repo Status System Design

## Purpose

PrkDB needs a reliable way to answer two recurring questions without relying on stale docs
or memory:

1. Where is the repository today?
2. What should be worked on next?

The current repo has useful signals spread across workflows, tests, scripts, examples, CLI
surfaces, and docs, but there is no single evidence-backed system that converts those signals
into a truthful status view. As a result, roadmap drift, incomplete docs, benchmark
overclaims, and CI/runtime gaps are easy to miss until someone manually audits the repo.

This design defines a hybrid repository status system that combines automated evidence
collection with curated summaries for maintainers and public readers.

## Goals

- Provide an evidence-backed answer for current repository health.
- Provide a maintainable way to prioritize next steps across the whole repo.
- Prevent roadmap, docs, and benchmark messaging from drifting away from code reality.
- Keep lightweight status checks in normal CI without turning PR workflows into a full audit.
- Avoid adding new runtime dependencies beyond the Rust toolchain already required by the
  repo.

## Non-Goals

- Replacing all existing validation/test workflows with one mega-job.
- Turning benchmark output into a universal product comparison score.
- Automatically inferring architectural truth from weak heuristics.
- Exposing repository audit tooling as end-user product CLI surface.

## Design Summary

The system has two layers:

- **Evidence layer**: an `xtask`-style Rust tool collects facts from the repo and emits
  structured outputs.
- **Narrative layer**: curated status docs summarize those facts for public readers and
  maintainers while linking back to concrete evidence.

The status system operates in three tiers:

- **PR/push CI**: lightweight snapshot only.
- **Nightly/manual CI**: heavier hosted audit.
- **Local maintainer run**: deepest audit with optional expensive checks.

Only objective drift fails normal CI. Interpretive findings remain advisory.

## Audience

The system serves two audiences at once:

- **Public readers and contributors** need a concise, public-safe understanding of current
  repo posture.
- **Maintainers** need a sharper audit that identifies concrete risks, stale claims, and
  highest-value next steps.

This requires a public summary plus deeper maintainer detail built from the same evidence.

## Architecture

### Tooling Placement

Implement the status system as a dedicated `xtask` crate instead of:

- a Python script
- shell-only tooling
- a `prkdb-cli` end-user command

Reasons:

- No new dependency beyond Rust.
- Better fit for repo-internal tooling.
- Easier parsing of workspace metadata and structured outputs.
- Avoids expanding product CLI surface with maintainer-only commands.

### Command Surface

The `xtask` tool will support three commands:

- `cargo run -p xtask -- repo-status snapshot`
  - cheap, deterministic, CI-safe
  - produces a structured JSON snapshot and short markdown summary
- `cargo run -p xtask -- repo-status audit`
  - deeper local or hosted audit
  - may execute selected validation commands
  - produces richer JSON plus a detailed maintainer-facing markdown report
- `cargo run -p xtask -- repo-status render`
  - regenerates curated status docs from the latest structured evidence

An optional wrapper script may exist for convenience, but the Rust tool remains the source
of truth.

## File Layout

### New Code

- `xtask/Cargo.toml`
- `xtask/src/main.rs`
- `xtask/src/repo_status/mod.rs`
- `xtask/src/repo_status/model.rs`
- `xtask/src/repo_status/collectors/`
- `xtask/src/repo_status/render/`
- `xtask/tests/`

### New Docs / Outputs

- `docs/status/repo-status.md`
  - public-facing status summary
- `docs/status/status-schema.md`
  - defines dimensions, severities, confidence levels, and evidence rules
- `target/repo-status/repo-status.snapshot.json`
  - generated CI/local snapshot output
- `target/repo-status/repo-audit.json`
  - generated deep-audit structured output
- `target/repo-status/repo-audit.latest.md`
  - generated detailed audit report

Generated artifacts should live under `target/` by default to avoid noisy checked-in files.
Only curated docs under `docs/status/` should be source-controlled.

## Evidence Model

The tool must use bounded, inspectable evidence sources rather than vague heuristics.

### Evidence Sources

- Workspace metadata
  - crates
  - examples
  - benches
  - scripts
  - docs pages
  - workflows
- Workflow/config analysis
  - required jobs
  - ignored tests
  - binary reuse opportunities
  - benchmark caveat presence
  - drift-check presence
- Docs analysis
  - roadmap claims
  - getting-started accuracy
  - codegen/server/CLI contract consistency
  - benchmark wording
- Code surface analysis
  - CLI command defaults
  - serve defaults
  - exported features
  - example presence
- Optional command evidence in `audit` mode
  - selected `cargo test`
  - selected `cargo clippy`
  - selected builds
  - selected validation scripts

### Output Model

Each status dimension emits:

- `status`: `green`, `yellow`, `red`, or `unknown`
- `confidence`: `high`, `medium`, or `low`
- `summary`: concise sentence
- `evidence`: concrete file refs and/or command results
- `findings`: objective issues
- `next_actions`: ranked recommendations

### Initial Dimensions

- `verification`
- `ci_health`
- `runtime_hardening`
- `docs_coverage`
- `contract_consistency`
- `benchmark_credibility`
- `release_readiness`

## Objective Drift vs Interpretive Risk

The tool must explicitly separate two classes of output:

### Objective Drift

Facts that contradict repo-backed evidence and should be CI-failable in snapshot mode.

Examples:

- roadmap says a feature is future work even though code and docs show it shipped
- docs say codegen uses one endpoint contract while CLI/server defaults use another
- benchmark-facing docs omit required caveats
- curated status docs reference stale evidence fingerprints

### Interpretive Risk

Findings that are important but involve editorial judgment. These should be advisory by
default.

Examples:

- benchmark messaging is technically caveated but still easy to overread
- a feature is only partially documented
- a workflow is correct but more expensive than necessary

## CI Model

### PR / Push CI

Run the lightweight snapshot only.

Responsibilities:

- generate `repo-status.snapshot.json`
- publish a short step summary
- upload snapshot artifacts
- fail only on objective drift

Normal PR CI should not run a full release-certification audit.

### Nightly / Manual Workflow

Run a heavier hosted audit.

Responsibilities:

- collect deeper repo health evidence
- publish richer artifacts
- surface advisory findings
- avoid blocking normal contribution flow

This tier is suitable for slower checks and more exhaustive repo posture reporting.

### Local Maintainer Audit

Run the fullest audit locally.

Responsibilities:

- optional expensive validation commands
- detailed maintainer-facing markdown report
- stronger prioritization guidance for follow-up work

## Curated Summary Flow

`docs/status/repo-status.md` should not be freehand status writing. It must be rendered from
structured evidence plus small curated narrative sections.

To prevent drift:

- rendered summaries include an evidence fingerprint or generation stamp
- snapshot mode checks that committed curated docs match the latest expected evidence identity
- stale rendered docs become an objective drift failure

This ensures the public status page cannot quietly diverge from current evidence.

## Rollout Plan

### Phase 1: Foundation

- add `xtask`
- implement `repo-status snapshot`
- emit JSON and short markdown summary
- wire snapshot into PR/push CI
- fail on a small initial set of objective drift checks:
  - roadmap contradictions
  - benchmark caveat presence
  - obvious docs/CLI/server contract mismatches

### Phase 2: Deeper Coverage

- implement `repo-status audit`
- classify findings by dimension and severity
- add nightly/manual workflow
- add stale-summary detection
- generate maintainer-facing detailed audit markdown

### Phase 3: Operating System for Repo Status

- add curated public status docs under `docs/status/`
- use evidence-backed outputs as the source for planning
- align roadmap and documentation updates with the status system

## Testing Strategy

- unit tests for collectors and classifiers
- golden-file tests for JSON and markdown rendering
- fixture-based drift tests for:
  - stale roadmap claims
  - missing benchmark caveats
  - docs/code contract mismatches
  - ignored/manual test classification
- CI self-check that the tool can analyze this repo and produce stable output

## Maintenance Rules

- No freehand roadmap/status edits that assert factual state without corresponding evidence.
- Curated status docs must be regenerated from evidence before merge when relevant inputs
  change.
- Interpretive commentary must be clearly distinguished from measured facts.

## Risks

- Overly broad heuristics could create noisy or misleading findings.
- If snapshot mode becomes too expensive, contributors will stop trusting it.
- If the rendered summary is too verbose, maintainers will ignore it and drift returns.

These risks are mitigated by:

- bounded evidence sources
- strict separation between objective drift and interpretive risk
- staged rollout
- keeping normal CI limited to cheap deterministic checks

## Success Criteria

This design is successful when:

- maintainers can answer “where are we?” from evidence-backed outputs rather than memory
- roadmap/docs contradictions are caught automatically
- benchmark caveat regressions are caught automatically
- PR CI remains fast enough to be trusted
- deeper repo posture is available without requiring manual archaeology across workflows,
  scripts, and docs
