# Bounded WAL stall diagnostic implementation plan

> For agentic workers: use executing-plans, with independent spec and quality review.

Goal: explain qualified write/cold tails or finish explicitly inconclusive.
Architecture: run exact archived binaries through a fixed diagnostic-only schedule;
retain raw native and host/trace evidence, never change database semantics or gates.
Tech stack: Python3 stdlib, existing Linux benchmark binaries, perf, GitHub Actions.
Spec: docs/superpowers/specs/2026-10-12-wal-stall-diagnostic-design.md.
Files: exactly the spec scope. Set CARGO_INCREMENTAL=0 and own CARGO_TARGET_DIR.

### Task1: reviewed bounded protocol
- [x] Claim separate worktree/handoff from phase-2; explicitly release inactive owned CI claim.
- [x] Baseline Python83 tests and actionlint pass before code changes.
- [x] Independent spec/plan review; resolve blockers before runner implementation.
- [x] Commit docs: define bounded WAL stall diagnostic protocol; update handoff.

### Task2: evidence runner test-first
Files: scripts/test_wal_stall_diagnostic.py, scripts/wal_stall_diagnostic.py.
- [ ] Add negative and positive provenance fixtures, fixed36-run schedule tests,
  native exact3-rep/4-worker/1s+3s/positive-cell parser and diagnostic-only verdict tests.
- [ ] Run python3 -m unittest discover -s scripts -p test_wal_stall_diagnostic.py -v;
  record genuine missing-module RED separately from runtime negative controls.
- [ ] Implement verify_artifact, schedule, parse_native, summarize, fixed_environment,
  host_snapshot, Linux preflight/run and exact trace commands with stdlib only.
- [ ] Tests must reject modified binaries, manifest identities/path traversal/symlinks,
  wrong/missing/duplicate cells/reps/header, nonpositive/NaN metrics and traced timing
  inclusion. No perf fallback or variable sequence. Save partial results on error.
- [ ] Review runner, run tests and real original-artifact offline validation; commit
  test: pin bounded WAL diagnostic evidence (tests) then ci: add fixed WAL stall runner.

### Task3: isolated dispatch and complete local gate
File: .github/workflows/remediation-gate.yml.
- [ ] Add choice wal-stall-diagnostic and one30-minute Ubuntu24.04 job, actions read,
  exact cross-run artifact download, parser tests, run with no build, always upload.
  Existing jobs/choices/gates/commands unchanged except adding choice.
- [ ] actionlint; full bash scripts/pre-push-check.sh on own target, no parallel build.
- [ ] Independent quality review, commit ci: dispatch retained-binary WAL stall diagnostic.
- [ ] One milestone push and dispatch probe=wal-stall-diagnostic at immutable task head;
  phase=2/ref=head. No PR, default gate or optimization acceptance claim.

### Task4: qualify results and finish
Files: docs/remediation/decisions/2026-10-12-wal-stall-diagnostic.md and own handoff.
- [ ] Monitor/download all evidence, verify hashes/source/schedule/exits/native counts.
- [ ] Independently analyze A/A and A/B pairs, scheduler/syscall/block and CPU evidence;
  account for tracing losses and whole-process/filtered-workload limits.
- [ ] Record cause if demonstrated, otherwise inconclusive. Original failed gate stays
  failed; no new optimization, rerun or architectural adoption in this task.
- [ ] Commit report, update exact handoff, clean owned target after evidence retention.
