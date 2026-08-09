# Repo Status System Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build an evidence-backed repository status system that can answer where the repo is today and what should be worked on next, with a lightweight CI snapshot, a deeper hosted/manual audit tier, and curated status docs rendered from structured evidence.

**Architecture:** Implement the status system as a new `xtask` crate so the repo gains a Rust-native maintainer tool without expanding `prkdb-cli`. Roll the system out in phases: first scaffold the tool and make `snapshot` generate stable JSON/markdown plus a few objective drift checks, then add `audit` and the nightly/manual workflow, and finally add rendered curated status docs with stale-summary detection. Keep PR CI cheap and deterministic, and keep interpretive findings advisory.

**Tech Stack:** Rust workspace tooling (`xtask`, `serde`, `serde_json`, `clap` or manual arg parsing, `toml`, `regex`), GitHub Actions, Markdown docs, fixture-based tests, existing repo scripts/workflows

---

## Current Repo State

- There is no `xtask` crate today.
- There is no `docs/status/` status hub today.
- The repo already has human-facing validation/progress scripts in `scripts/`, but they are not evidence-backed truth sources:
  - `scripts/track_progress.sh`
  - `scripts/validate_all.sh`
  - `scripts/validate_core.sh`
- The repo already has known drift examples that the new status system should catch:
  - benchmark methodology/message risk in `.github/workflows/ci.yml`, `README.md`, and `docs/guide/streaming-kafka-comparison.md`
  - roadmap drift in `docs/guide/roadmap.md`
  - docs/CLI/server contract drift examples in `docs/guide/codegen.md`, `README.md`, and the CLI command surfaces

## File Structure Map

- Create: `xtask/Cargo.toml`
  Purpose: define the maintainer-tool crate and dependencies.
- Create: `xtask/src/main.rs`
  Purpose: command dispatch for `repo-status snapshot|audit|render`.
- Create: `xtask/src/repo_status/mod.rs`
  Purpose: top-level orchestration for collectors, classifiers, and renderers.
- Create: `xtask/src/repo_status/model.rs`
  Purpose: shared structured output types (`StatusReport`, `DimensionReport`, findings, evidence).
- Create: `xtask/src/repo_status/collectors/mod.rs`
  Purpose: collector registry and shared collector helpers.
- Create: `xtask/src/repo_status/collectors/workspace.rs`
  Purpose: collect workspace crates, scripts, docs pages, examples, benches.
- Create: `xtask/src/repo_status/collectors/docs.rs`
  Purpose: inspect roadmap/docs/codegen/benchmark text for objective drift and missing caveats.
- Create: `xtask/src/repo_status/collectors/workflows.rs`
  Purpose: inspect CI workflows for snapshot/audit integration and benchmark/verification posture.
- Create: `xtask/src/repo_status/collectors/contracts.rs`
  Purpose: inspect CLI/server/docs port and endpoint-contract consistency.
- Create: `xtask/src/repo_status/render/mod.rs`
  Purpose: render JSON, short markdown summary, and detailed markdown audit.
- Create: `xtask/src/repo_status/render/markdown.rs`
  Purpose: deterministic markdown generation for summary/audit docs.
- Create: `xtask/tests/snapshot_fixtures.rs`
  Purpose: golden/fixture tests for snapshot mode.
- Create: `xtask/tests/render_fixtures.rs`
  Purpose: golden/fixture tests for markdown and JSON rendering.
- Create: `xtask/tests/fixtures/`
  Purpose: tiny fake repo states or extracted snippets that exercise roadmap/docs/contract drift classifiers.
- Create: `docs/status/repo-status.md`
  Purpose: public-facing rendered status summary.
- Create: `docs/status/status-schema.md`
  Purpose: define dimensions, severities, confidence semantics, and evidence rules.
- Modify: `.github/workflows/ci.yml`
  Purpose: run `repo-status snapshot`, upload artifacts, and fail on objective drift.
- Modify: `.github/workflows/deploy-docs.yml`
  Purpose: ensure rendered docs are included in the docs site, if needed.
- Create: `.github/workflows/repo-audit.yml`
  Purpose: nightly/manual heavier hosted audit tier.
- Modify: `Cargo.toml`
  Purpose: add `xtask` to the workspace members.
- Modify: `README.md`
  Purpose: document the repo-status commands and point readers to `docs/status/repo-status.md`.

## Task 1: Scaffold the `xtask` Status Tool

**Files:**
- Modify: `Cargo.toml`
- Create: `xtask/Cargo.toml`
- Create: `xtask/src/main.rs`
- Create: `xtask/src/repo_status/mod.rs`
- Create: `xtask/src/repo_status/model.rs`
- Test: `xtask/tests/snapshot_fixtures.rs`

- [ ] **Step 1: Add a failing smoke test for the new command surface**

Create `xtask/tests/snapshot_fixtures.rs` with a minimal test that expects `repo-status snapshot` to return success and emit the top-level dimensions once implemented.

```rust
#[test]
fn snapshot_output_includes_expected_dimensions() {
    let output = run_xtask(["repo-status", "snapshot"]);
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("\"verification\""));
    assert!(stdout.contains("\"docs_coverage\""));
}
```

- [ ] **Step 2: Run the new test to verify it fails**

Run: `cargo test -p xtask snapshot_output_includes_expected_dimensions -- --nocapture`

Expected: FAIL because the `xtask` crate and command do not exist yet.

- [ ] **Step 3: Add `xtask` to the workspace and create the crate skeleton**

Implement:
- workspace membership in `Cargo.toml`
- `xtask/Cargo.toml`
- `xtask/src/main.rs`
- `xtask/src/repo_status/mod.rs`
- `xtask/src/repo_status/model.rs`

Keep the initial command surface minimal:

```rust
match args.as_slice() {
    ["repo-status", "snapshot"] => repo_status::snapshot(),
    ["repo-status", "audit"] => repo_status::audit(),
    ["repo-status", "render"] => repo_status::render(),
    _ => print_usage_and_exit(),
}
```

- [ ] **Step 4: Implement the first passing placeholder snapshot**

Return deterministic placeholder JSON from `snapshot()` using `StatusReport` and `DimensionReport` types so the test can pass before collectors exist.

```rust
StatusReport {
    dimensions: vec![
        DimensionReport::unknown("verification"),
        DimensionReport::unknown("docs_coverage"),
    ],
    findings: vec![],
}
```

- [ ] **Step 5: Re-run the smoke test**

Run: `cargo test -p xtask snapshot_output_includes_expected_dimensions -- --nocapture`

Expected: PASS.

- [ ] **Step 6: Commit the scaffold**

```bash
git add Cargo.toml xtask/
git commit -m "feat: scaffold xtask repo-status tool"
```

## Task 2: Build the Snapshot Evidence Model and Objective Drift Checks

**Files:**
- Modify: `xtask/src/repo_status/mod.rs`
- Modify: `xtask/src/repo_status/model.rs`
- Create: `xtask/src/repo_status/collectors/mod.rs`
- Create: `xtask/src/repo_status/collectors/workspace.rs`
- Create: `xtask/src/repo_status/collectors/docs.rs`
- Create: `xtask/src/repo_status/collectors/workflows.rs`
- Create: `xtask/src/repo_status/collectors/contracts.rs`
- Test: `xtask/tests/snapshot_fixtures.rs`
- Test: `xtask/tests/fixtures/`

- [ ] **Step 1: Add failing fixture tests for the first three objective drift classes**

Create fixture-driven tests for:
- roadmap contradiction
- missing benchmark caveat
- docs/contract mismatch

```rust
#[test]
fn flags_roadmap_feature_claim_that_conflicts_with_codegen_support() {
    let report = snapshot_fixture("roadmap_sdk_drift");
    assert!(has_finding(&report, "roadmap_feature_drift"));
}
```

- [ ] **Step 2: Run the fixture tests to verify they fail**

Run: `cargo test -p xtask --test snapshot_fixtures -- --nocapture`

Expected: FAIL because the collectors and classifiers do not exist yet.

- [ ] **Step 3: Implement the collector registry and structured finding model**

Add:
- `collectors/mod.rs`
- typed `Finding`
- typed `Evidence`
- `Status`, `Confidence`, and `DimensionId`

Keep the model explicit rather than stringly-typed.

- [ ] **Step 4: Implement workspace and docs collectors**

Parse repo files directly for:
- presence of roadmap page and codegen docs
- codegen support in `crates/prkdb-cli/src/commands/codegen.rs`
- roadmap “future work” claims in `docs/guide/roadmap.md`
- benchmark caveat wording in `README.md` and `docs/guide/streaming-kafka-comparison.md`

- [ ] **Step 5: Implement workflow and contract collectors**

Parse:
- `.github/workflows/ci.yml`
- CLI/docs references for codegen/server endpoint expectations

Initial objective checks should cover:
- roadmap says client SDKs are future work while codegen exists
- benchmark-facing docs missing the required caveat language
- docs using the wrong port/contract for codegen vs `prkdb-cli serve`

- [ ] **Step 6: Re-run fixture tests**

Run: `cargo test -p xtask --test snapshot_fixtures -- --nocapture`

Expected: PASS, with findings emitted for the failing fixtures.

- [ ] **Step 7: Commit the snapshot evidence layer**

```bash
git add xtask/
git commit -m "feat: add repo-status snapshot collectors"
```

## Task 3: Render Stable Snapshot Outputs

**Files:**
- Modify: `xtask/src/repo_status/mod.rs`
- Create: `xtask/src/repo_status/render/mod.rs`
- Create: `xtask/src/repo_status/render/markdown.rs`
- Test: `xtask/tests/render_fixtures.rs`

- [ ] **Step 1: Add failing renderer tests for JSON and markdown output**

Add tests that assert:
- snapshot JSON is stable and machine-readable
- markdown summary includes dimension status, top findings, and evidence refs

```rust
#[test]
fn markdown_summary_lists_dimensions_and_findings() {
    let report = sample_report();
    let md = render_summary_markdown(&report);
    assert!(md.contains("verification"));
    assert!(md.contains("Top Findings"));
}
```

- [ ] **Step 2: Run renderer tests to verify they fail**

Run: `cargo test -p xtask --test render_fixtures -- --nocapture`

Expected: FAIL because the renderer does not exist yet.

- [ ] **Step 3: Implement deterministic JSON serialization**

Emit snapshot JSON to stdout by default and support writing to:
- `target/repo-status/repo-status.snapshot.json`

Keep keys and ordering stable enough for fixtures and CI diffs.

- [ ] **Step 4: Implement short markdown summary rendering**

Generate a concise summary with:
- overall status
- dimension table
- top objective findings
- advisory findings count
- evidence references

- [ ] **Step 5: Re-run renderer tests**

Run: `cargo test -p xtask --test render_fixtures -- --nocapture`

Expected: PASS.

- [ ] **Step 6: Verify the snapshot command end-to-end**

Run: `cargo run -p xtask -- repo-status snapshot`

Expected:
- exit code `0` on current branch if no objective drift remains
- JSON on stdout or deterministic output to `target/repo-status/`

- [ ] **Step 7: Commit rendering support**

```bash
git add xtask/
git commit -m "feat: render repo-status snapshot outputs"
```

## Task 4: Integrate Snapshot Mode Into PR/Push CI

**Files:**
- Modify: `.github/workflows/ci.yml`
- Modify: `README.md`

- [ ] **Step 1: Add a failing CI integration test locally through workflow linting expectations**

Add or extend an `xtask` test that asserts required CI integration markers exist once implemented:

```rust
assert!(ci_yaml.contains("repo-status snapshot"));
assert!(ci_yaml.contains("repo-status.snapshot.json"));
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p xtask ci_integration_markers_present -- --nocapture`

Expected: FAIL because `ci.yml` is not wired yet.

- [ ] **Step 3: Add the snapshot step to `.github/workflows/ci.yml`**

The snapshot job should:
- build/run `xtask`
- run `cargo run -p xtask -- repo-status snapshot`
- save outputs under `target/repo-status/`
- upload the snapshot artifact
- publish a concise step summary

- [ ] **Step 4: Make PR CI fail only on objective drift**

Use `snapshot` exit codes or a `--fail-on-objective-drift` switch so:
- objective contradictions fail the job
- advisory findings remain non-blocking

- [ ] **Step 5: Document the new snapshot command in `README.md`**

Add a short maintainer section:

```bash
cargo run -p xtask -- repo-status snapshot
cargo run -p xtask -- repo-status audit
```

- [ ] **Step 6: Re-run the CI integration test and a local smoke command**

Run:
- `cargo test -p xtask ci_integration_markers_present -- --nocapture`
- `cargo run -p xtask -- repo-status snapshot`

Expected: PASS.

- [ ] **Step 7: Commit PR CI integration**

```bash
git add .github/workflows/ci.yml README.md xtask/
git commit -m "ci: add repo-status snapshot to PR workflow"
```

## Task 5: Add the Deep Audit Mode and Hosted Audit Workflow

**Files:**
- Modify: `xtask/src/main.rs`
- Modify: `xtask/src/repo_status/mod.rs`
- Modify: `xtask/src/repo_status/model.rs`
- Create: `.github/workflows/repo-audit.yml`
- Test: `xtask/tests/snapshot_fixtures.rs`

- [ ] **Step 1: Add a failing test for audit mode output shape**

Add a test expecting:
- richer markdown output
- advisory findings section
- optional command evidence blocks

```rust
#[test]
fn audit_mode_emits_detailed_markdown_sections() {
    let output = run_xtask(["repo-status", "audit"]);
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("Advisory Findings"));
    assert!(stdout.contains("Next Actions"));
}
```

- [ ] **Step 2: Run the audit test to verify it fails**

Run: `cargo test -p xtask audit_mode_emits_detailed_markdown_sections -- --nocapture`

Expected: FAIL because `audit` is still a placeholder.

- [ ] **Step 3: Implement audit-mode enrichment**

Add:
- dimension summaries with severity
- optional command execution hooks
- ranked next actions
- detailed markdown output written to `target/repo-status/repo-audit.latest.md`

Keep command execution opt-in so the tool can stay CI-safe.

- [ ] **Step 4: Add the nightly/manual workflow**

Create `.github/workflows/repo-audit.yml` with:
- `schedule`
- `workflow_dispatch`
- `cargo run -p xtask -- repo-status audit`
- artifact upload for JSON and markdown outputs

- [ ] **Step 5: Re-run the audit test and local audit command**

Run:
- `cargo test -p xtask audit_mode_emits_detailed_markdown_sections -- --nocapture`
- `cargo run -p xtask -- repo-status audit`

Expected: PASS.

- [ ] **Step 6: Commit the deeper audit tier**

```bash
git add xtask/ .github/workflows/repo-audit.yml
git commit -m "feat: add repo-status audit mode"
```

## Task 6: Add Rendered Status Docs and Stale-Summary Detection

**Files:**
- Create: `docs/status/repo-status.md`
- Create: `docs/status/status-schema.md`
- Modify: `xtask/src/repo_status/mod.rs`
- Modify: `xtask/src/repo_status/render/markdown.rs`
- Modify: `.github/workflows/deploy-docs.yml`
- Test: `xtask/tests/render_fixtures.rs`

- [ ] **Step 1: Add a failing test for rendered curated docs**

Add a test that expects `render` mode to produce:
- a public summary markdown file
- an embedded evidence fingerprint or generation stamp

```rust
#[test]
fn render_mode_includes_evidence_fingerprint() {
    let report = sample_report();
    let md = render_public_status(&report, "evidence:abc123");
    assert!(md.contains("evidence:abc123"));
}
```

- [ ] **Step 2: Run the render-docs test to verify it fails**

Run: `cargo test -p xtask render_mode_includes_evidence_fingerprint -- --nocapture`

Expected: FAIL because public render mode and fingerprint support do not exist yet.

- [ ] **Step 3: Create the status docs and schema page**

Create:
- `docs/status/repo-status.md`
- `docs/status/status-schema.md`

The status page should be rendered from structured evidence plus a small curated narrative section. The schema page should define:
- dimensions
- severity levels
- confidence levels
- difference between objective drift and interpretive risk

- [ ] **Step 4: Implement stale-summary detection**

Teach `snapshot` mode to verify that the committed `docs/status/repo-status.md` carries the expected evidence identity. If not, emit an objective drift finding.

- [ ] **Step 5: Wire rendered docs into docs deployment**

Update `.github/workflows/deploy-docs.yml` only as needed so `docs/status/` is included in the published docs site.

- [ ] **Step 6: Re-run render tests and commands**

Run:
- `cargo test -p xtask --test render_fixtures -- --nocapture`
- `cargo run -p xtask -- repo-status render`
- `cargo run -p xtask -- repo-status snapshot`

Expected:
- rendered docs are generated
- snapshot passes when the rendered summary is up to date

- [ ] **Step 7: Commit rendered status docs support**

```bash
git add docs/status/ xtask/ .github/workflows/deploy-docs.yml
git commit -m "docs: add rendered repo status pages"
```

## Task 7: Verify the Status System Against the Current Repo

**Files:**
- Modify: `xtask/tests/snapshot_fixtures.rs`
- Modify: `xtask/tests/render_fixtures.rs`
- Modify: `README.md`

- [ ] **Step 1: Add a self-hosting regression test for this repo**

Add a test or command harness that runs the status tool against the real repo and asserts a stable minimum structure:

```rust
#[test]
fn repo_snapshot_reports_expected_dimensions() {
    let report = run_real_repo_snapshot();
    assert!(dimension_names(&report).contains(&"verification".to_string()));
    assert!(dimension_names(&report).contains(&"benchmark_credibility".to_string()));
}
```

- [ ] **Step 2: Run the self-hosting test to verify it fails if expectations are wrong**

Run: `cargo test -p xtask repo_snapshot_reports_expected_dimensions -- --nocapture`

Expected: initially FAIL if the real-repo harness or expected dimensions are incomplete.

- [ ] **Step 3: Tighten the implementation until the real-repo snapshot is stable**

Fix any collector or renderer gaps exposed by the test. Keep the assertions structural, not overly brittle.

- [ ] **Step 4: Run the full verification slice**

Run:
- `cargo fmt --all`
- `cargo test -p xtask -- --nocapture`
- `cargo run -p xtask -- repo-status snapshot`
- `cargo run -p xtask -- repo-status audit`
- `cargo run -p xtask -- repo-status render`

Expected: PASS.

- [ ] **Step 5: Update the maintainer docs with the final command set**

Ensure `README.md` or a maintainer section documents:
- when to run `snapshot`
- when to run `audit`
- when to run `render`
- what CI does automatically

- [ ] **Step 6: Commit the final verification pass**

```bash
git add README.md xtask/ docs/status/
git commit -m "test: self-host repo status system"
```

## Notes for Implementation

- Keep JSON and markdown schemas intentionally small in Phase 1.
- Prefer explicit typed classifiers over fuzzy scoring systems.
- Avoid shelling out in snapshot mode except where strictly necessary.
- Do not make PR CI depend on benchmark interpretation or expensive integration runs.
- Treat `docs/status/repo-status.md` as rendered output with a tiny curated layer, not as a freehand narrative document.

## Final Verification Checklist

- `cargo test -p xtask -- --nocapture`
- `cargo run -p xtask -- repo-status snapshot`
- `cargo run -p xtask -- repo-status audit`
- `cargo run -p xtask -- repo-status render`
- confirm `.github/workflows/ci.yml` runs snapshot on PR/push
- confirm `.github/workflows/repo-audit.yml` exists for nightly/manual runs
- confirm `docs/status/repo-status.md` and `docs/status/status-schema.md` render and publish correctly
