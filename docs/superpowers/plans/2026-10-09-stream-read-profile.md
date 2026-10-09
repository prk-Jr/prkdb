# Stream read measured-phase profile implementation plan

> **For agentic workers:** Use superpowers:executing-plans with independent reviews.

**Goal:** Obtain one qualified measured-phase Linux CPU profile before one correction.
**Architecture:** Optional existing-benchmark fixture checks/monotonic markers;
stdlib Python fail-closed orchestration/parser; separate labelled CI probe.
**Tech Stack:** Rust1.98.1, Linux perf CPU-clock/DWARF, Python3, existing Actions.

Exact Files and contract: 2026-10-09-stream-read-profile-design.md in specs.
Read AGENTS/spec/plan Conventions; all output retained, no production changes.

- [x] Review profile contract and tentative frame-reuse proof independently.
- [x] Python parser RED: stub qualification fails tests for missing/false fixtures,
  wrong cell, reordered/overlapping/missing phases, malformed timestamps/periods,
  loss, insufficient samples and stale provenance. Preserve positive controls.
- [x] Implement minimal strict parser; GREEN; freeze300sample/10%unknown rules.
- [x] Rust fixture validator RED through integration test importing bench helper;
  reject changed value/key/header/time/offset/cursor/watermark and missing records.
- [x] Implement recording clock, append-input expectations, complete preflight
  verification and monotonic begin/end markers. Default benchmark stays unchanged.
- [x] Wire optional helper into actual stream_read_cell; same workload/window.
- [x] Add Linux script preflight/perf orchestration, source/binary hashes, quiet
  process checks, host metadata, raw/unfiltered and measured interval outputs.
  Reject nonLinux, missing tools/permissions and failed record/report commands.
- [x] Add explicitly labelled profile choice/job to existing remediation workflow;
  preserve other jobs/gates; pinned setup, isolated target, artifacts always.
- [x] Independent code/protocol review before any profiling execution.
- [x] Run fmt, workspaceclippy/nextest/docs, ledger/render and fullprepush (CI change).
  Criterion is not applicable to profiling-only bench/CI code (no production paths).
- [x] Commit tested milestone and update handoff on every commit. No publication
  until reviewed source and existing authorization confirmed; no main/PR changes.
- [x] After host selection, qualify Linux tools/kernel/permissions/source/workflow,
  build first, perform one fixed three-repetition profile round. Do not retry for
  favorable data or claim instrumented throughput acceptance.
- [x] Preserve raw results/hashes and report causal cost or unqualified reason.
  STOP before production correction unless qualified profile and safety review pass.
- [x] Report AGENTS§7; clean only owned target, retain source/evidence. If host
  remains unknown, finish reviewable infrastructure and explicitly pause dispatch.

Commits: test: qualify stream read profile evidence; test: validate profiled stream
fixtures; ci: add measured-phase stream read profile probe; docs: record profile status.
No finding/spec revision assigned by profiling infrastructure alone.

## Prepared checkpoint, 2026-10-09

Independent Rust and evidence/CI reviews approve the implementation. Final checks:
1,509 nextest tests passed (10 skipped); 69 doctests passed (14 ignored); seven
fixture tests and 53 Python tests passed; fmt, workspace clippy, ledger/render,
actionlint and diff/doc-claim checks passed. The full pre-push run passed before
the last fixture-only guard/test change, including concurrent workspace tests and
200 seeds in each durability mode. The final workspace rerun and focused fixture
suite cover that guard. No production hot path changed and Criterion was not
applicable. No Linux CPU result is claimed.

The verified task-owned Mac target was cleaned (8.0 GiB logical files removed);
source, RED/GREEN logs and handoff remain. GitHub Actions was subsequently selected;
run37908155101 failed qualification because perf_event_paranoid=4 blocks userspace
perf. No build or CPU recording occurred. At that checkpoint the profile/correction steps were incomplete; original failure artifacts and hashes are retained. See the pending
checkpoint decision record and handoff for commands and the complete report.

Permission amendment explicitly approved: temporary level2 only in the disposable profile job, restore original in always() cleanup with setup/restore logs. Workflow amended before the first actual CPU recording round; helper/workload/quality thresholds unchanged.

## Fixed-round result, 2026-10-09

Run37909749321 at06965f2 built and recorded successfully, then failed the fixed
unknown-leaf rule (483/1310, 36.870229%; limit10%). All three phase counts are
nonzero (446/427/437), all43original artifact hashes match, all9commands returned
zero, and raw loss/throttle/stderr diagnostic checks pass. The permission policy
was restored to its original4. Original failure and full recording are retained.

STOP: no qualified CPU cost or production correction. The separate offline symbol recovery
was subsequently authorized as described below. It decodes the same recording
with exact-build-ID symbols and unchanged rules, without rerunning the workload
or editing the failed original manifests.


## Approved same-recording offline recovery implementation

Maintainer requested "Do the needful" after the concrete recovery proposal.
Files: existing scripts/stream_read_profile.py, scripts/test_stream_read_profile.py,
.github/workflows/remediation-gate.yml and the three already scoped task documents.
No Rust/production changes. Use executing-plans with independent review.

- [x] Add RED tests for chain-IP/DSO cohort comparison: accept symbol-name-only
  changes; reject dropped/reordered/changed sample metadata and chain identities,
  wrong build IDs and resolution of unapproved vDSO/other external frames.
- [x] Add RED tests for pinned eligible original failure and unchanged normal
  failure rejection. Missing artifacts, changed hashes, failed commands/loss,
  mismatched source/parser and a still-failing10% threshold remain refused.
- [x] Implement minimal separate recovery functions; keep ordinary qualification
  semantics. Require all original provenance, exact verified ELF/debug files,
  same perf version, isolated caches/symfs and unchanged cohort/interval checks.
- [x] Run `python3 -m unittest discover -s scripts -p test_stream_read_profile.py -v`
  to GREEN; archive genuine RED/GREEN command logs outside source worktree.
- [x] Add isolated offline Actions choice/job; download only fixed original
  artifact, actions:read only there; no benchmark/build/record/stat/sysctl.
  Preserve errors/raw output with always-upload. Expose retained step errors.
- [x] Independent protocol/code review, actionlint, diff/doc checks and required
  pre-push checks; commit small checkpoints and update handoff each time.
- [x] Publish one reviewed milestone and dispatch offline probe at exact analysis
  SHA. Preserve original/derived artifacts and statuses; no measured-round retry.
- [ ] Report qualified derived CPU costs only if fixed original rules pass;
  otherwise report remaining blocker. Production correction stays separately scoped.

Commits: `test: pin offline profile recovery provenance and sample identities`;
`ci: recover saved stream profile with exact glibc symbols`;
`docs: record offline stream profile recovery result`.


Implementation checkpoint:73Python tests pass, actionlint/diff/doc-claim checks
pass, independent final helper/workflow review has no HIGH/MEDIUM blockers.
Fullprepush passes:1509nextest/10skipped, concurrentworkspace tests/doctests,
200seeds68256checks eachmode and every script/ledger/status/doc gate.
No production/hot-path change; Criterion does not apply. First offline extraction
ran at4ba16f7 and failed metadata/stack qualification; original failure remains
immutable. The bounded correction is recorded below.


## Bounded metadata correction, same recording

First offline run37916402867 failed the strict stack comparison; preserve it.
The reviewed design amendment permits explicit no-inline extraction and only the
exact recorded vDSO ELF, with actual symfs/.debug cache binding. No CPU round rerun.

- [x] RED tests: no-inline commands, exact recorded/candidate vDSO build IDs,
  mismatched/missing/duplicate IDs, actual-used cache confinement and hashes.
- [x] Implement the bounded metadata fix; keep all physical stack/cohort and
  frozen qualification checks unchanged. Missing matching vDSO remains STOP.
- [x] Include hidden files only in generated offline evidence upload; retain
  original and first offline failures without rewriting their manifests.
- [ ] Focused Python, actionlint, diff/doc checks and independent review; publish
  one metadata milestone and dispatch only the offline probe at its exact SHA.
  Fullprepush passed on4ba16f7; Rust sources remain unchanged, owned target cleaned.
- [ ] Download all result artifacts, verify every hash including hidden files,
  and report either qualified CPU attribution or the precise remaining blocker.
