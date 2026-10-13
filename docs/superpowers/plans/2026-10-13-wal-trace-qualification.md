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
- [x] Baseline73 and final78 Python tests pass; actionlint passes; independent
  spec/plan/code/workflow review approved.
- [x] b65836f commits finite live Linux trace qualification protocol.

### 2. Test-first qualifier
Files: scripts/wal_trace_qualification.py and test_wal_trace_qualification.py.
- [x] RED: five new tests fail before qualifier implementation; live /proc proof runs before smoke; failed parser launches nothing;
  fresh output, exact recorder reuse, result/inventory/ownership on failure.
- [x] GREEN: implement qualify(output) using live ordinary records and existing
  profile_command/execute/render/qualify_smoke. Never call benchmark run().
- [x] 13eb10e commits live Linux resource qualification;78 tests and independent
  implementation review pass.

### 3. Isolated workflow
File: .github/workflows/remediation-gate.yml; original gates unchanged.
- [x] Add wal-trace-qualification and wal-trace-capture choices, one 20-minute job.
  Qualifier always first; artifact download and benchmark command only in capture.
- [x] Retain exact resolved source SHA/hashes, tools/tests, errors and always artifact.
- [x] Full own-target pre-push passes:1509 nextest tests/10 skips, concurrent
  tests/doctests, both200seed modes/68256checks each, lint/format/ledger/docs.
  Python78/actionlint and independent review pass. Milestone push follows.

### 4. Finite Linux checks
- [x] Dispatch qualifier once (38369782043: tooling format failure, no database). Download/hash/review result, live /proc, identity,
  root child limits, full smoke clock/events/thread/loss records independently.
- [x] Demonstrated Linux 6.17 compatibility bugs reproduced test-first;85 tests
  and offline saved-smoke analysis pass. One correction and one further
  qualification-only dispatch; stop custom tracing if it still fails.
- [ ] Only on success: one reviewed exact-head capture dispatch, fresh-host smoke
  first, then original fixed four controls/four profiles. No retry or gate waiver.

### 5. Decision and handoff
- [ ] Distinguish measured gains, failed product gates and instrumentation limits;
  evaluate existing architecture evidence independently, no speculative rewrite.
- [ ] Commit result and precise next decision; full handoff each commit.
- [ ] Clean only own target; preserve source, failed prefixes and original gates.
