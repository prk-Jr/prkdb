# Live Linux WAL trace qualification implementation plan

> For agentic workers: root executes with independent plan/code/evidence review.
> Controlled subagents may review assigned documents/evidence read-only. Root is
> sole Cargo, mutation, commit and dispatch owner.

Goal: qualify corrected Linux instrumentation before a single causal capture,
or close custom tracing with an explicit finite limit.
Architecture: thin qualifier over the reviewed existing runner; isolated Actions
probe with qualification-only and conditional capture modes.
Tech stack: Python stdlib, Linux perf, Actions; Rust 1.98.1 checks only.

### 1. Claim and review
- [x] New phase-2 worktree and immediate handoff; closed-task tools copied unchanged.
- [ ] Baseline 73 Python tests, actionlint; independent spec/plan review.
- [ ] Commit docs: define finite live Linux trace qualification.

### 2. Test-first qualifier
Files: scripts/wal_trace_qualification.py and test_wal_trace_qualification.py.
- [ ] RED: live /proc proof runs before smoke; failed parser launches nothing;
  fresh output, exact recorder reuse, result/inventory/ownership on failure.
- [ ] GREEN: implement qualify(output) using live ordinary records and existing
  profile_command/execute/render/qualify_smoke. Never call benchmark run().
- [ ] Tests and independent implementation review; commit ci: qualify live Linux
  resource records before trace capture.

### 3. Isolated workflow
File: .github/workflows/remediation-gate.yml; original gates unchanged.
- [ ] Add wal-trace-qualification and wal-trace-capture choices, one 20-minute job.
  Qualifier always first; artifact download and benchmark command only in capture.
- [ ] Retain exact resolved source SHA/hashes, tools/tests, errors and always artifact.
- [ ] Full own-target pre-push, Python/actionlint; review; one milestone push.

### 4. Finite Linux checks
- [ ] Dispatch qualifier once. Download/hash/review result, live /proc, identity,
  root child limits, full smoke clock/events/thread/loss records independently.
- [ ] If demonstrated tooling bug, one test-first correction and one further
  qualification-only dispatch; stop custom tracing if it still fails.
- [ ] Only on success: one reviewed exact-head capture dispatch, fresh-host smoke
  first, then original fixed four controls/four profiles. No retry or gate waiver.

### 5. Decision and handoff
- [ ] Distinguish measured gains, failed product gates and instrumentation limits;
  evaluate existing architecture evidence independently, no speculative rewrite.
- [ ] Commit result and precise next decision; full handoff each commit.
- [ ] Clean only own target; preserve source, failed prefixes and original gates.
