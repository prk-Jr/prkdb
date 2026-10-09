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
- [ ] After host selection, qualify Linux tools/kernel/permissions/source/workflow,
  build first, perform one fixed three-repetition profile round. Do not retry for
  favorable data or claim instrumented throughput acceptance.
- [ ] Preserve raw results/hashes and report causal cost or unqualified reason.
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
perf. No build or CPU recording occurred. The profile/correction steps above
remain unchecked; original failure artifacts and hashes are retained. See the pending
checkpoint decision record and handoff for commands and the complete report.

Permission amendment explicitly approved: temporary level2 only in the disposable profile job, restore original in always() cleanup with setup/restore logs. Workflow amended before the first actual CPU recording round; helper/workload/quality thresholds unchanged.
