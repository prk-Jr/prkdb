# Feasibility implementation plan: revision 2 screening profile

> For agentic workers: use executing-plans and read-only independent protocol/plan
> reviews. Root owns mutations/builds/dispatch. Each implementation slice is claimed
> separately with its own exact files and test-first plan.

Goal: honest stream and indexed engineering screens under revised workload,
memory and recovery proposals. Architecture: eligible public APIs, immutable
manifest, distinct ordinary/resource/recovery jobs, fail-closed completeness.
Tech stack: Rust 1.98.1, existing FaultFs/xtask and Linux Actions later.
Authority: the remediation spec/plan/ledger and revision 2 design. Existing
performance, crash and phase gates are independent and remain unchanged.

## Documentation revision (current claim)
Files:
- docs/superpowers/specs/2026-10-13-production-feasibility-design.md
- docs/superpowers/plans/2026-10-13-production-feasibility.md
- docs/remediation/decisions/2026-10-13-production-feasibility.md

- [x] Record maintainer: both lanes; R1 requirements not approved; GitHub SCREENING
  ONLY; revise memory, recovery and workloads first.
- [x] Preserve R1 commit754d05a and original immutable root snapshots.
- [x] Both independent reviewers approved R2 workload, source contracts and schedule
  arithmetic after two rounds.
- [ ] Verify docs, commit checkpoint, update notes, preserve reports and clean worktree.
- [x] Maintainer approved revised screening protocol/working sets/memory/recovery
  and execution caps on2026-10-13 ("Use the revised screening requirements").
- [ ] Set offered loads and pin exact manifest/read/fixture/host evidence; no timings
  before source eligibility. Full production qualification remains deferred.

## P0: eligibility and immutable schema, NO timings
Separate claim only after this protocol is approved. Proposed files:
scripts/production_feasibility.py, scripts/test_production_feasibility.py,
docs/benchmarks/production-feasibility.toml, decision document. Exact schema/tests
must be written in that slice's reviewed plan before code.
- [ ] RED: absence of approval, unresolved relevant findings, unmerged prerequisites,
  wrong source/mode/API, unsupported deadline or missing evidence all block.
- [ ] Per-lane/per-capability eligibility; whole current phase2 production profile
  must remain BLOCKED. Scoped raw-stream probes cannot imply consumer readiness.
- [ ] Preflight dataset byte/cardinality, index/consumer requirements, effective host
  limits, separate RSS/cgroup/cache budgets, complete fixed schedule and artifact caps.
- [ ] Reject impossible fixtures/sample counts, missing required cells, partial output,
  changed budgets, unchecked API downgrade and post-run schedule adjustment.
- [ ] Unit fixtures are controls, not synthetic production certification.

## P1: real workload and resource/recovery harness
Separate stream/indexed claims after actual source correctness prerequisites and P0.
Proposed files (confirm exact scope in each new reviewed slice):
crates/prkdb/examples/stream_production_feasibility.rs,
crates/prkdb/examples/indexed_production_feasibility.rs,
crates/prkdb/examples/support/production_feasibility.rs,
crates/prkdb/tests/production_feasibility_contract.rs,
scripts/test_production_feasibility.py.
- [ ] RED: causal negative controls for actual public paths, exact data/index/offsets,
  memory/backlogs, actual segment-granular deleted-open files and release, real
  reclaimed bytes, fixture state and recovery-ready phases.
- [ ] Ordinary CI populations:100k1KiB or10k64KiB indexed; stream retained1GiB.
  Fixed indexed60/30/10 mix, all real indexes; source-eligible RMW/consumer semantics.
- [ ] Fixed memory state/ten-minute probe PER MODE plus2min warmup each;
  stream preloaded to1GiB, approved arrivals roll another segment and demonstrate
  actual reclamation, not a no-op retention pass. process high-water RSS AND cgroup peak/file
  cache, bounded client buffers, no full-memory claim from WAL queued bytes alone.
- [ ] Indexed recovery128MiB/1GiB xcheckpoint/no-checkpoint xmode x3 fresh-process
  reps (24 attempts). Rawstream:2sizes xmode x3 reps (12 attempts), actual sparse
  index rebuilt at open; no invented stream checkpoint.
  exact live population/retained-byte feasibility before generating fixtures;
  freeze public construction calls/counts/batches (at most100), overwrite
  generations and complete10min generation budget. No raw-KV bypass.
- [ ] Cumulative WAL/index/consumer readiness times; warm OS cache labeled explicitly.
  Eligibility and physical crash/fault model remain independent requirements.
- [ ] Preflight full resource/recovery native schedule <=55min: fixture generation10min,
  BOTH-mode warmup/measure24min, indexed24/stream12 recovery attempts at most30s,
  diagnostics6min, drain/validation3min. Actual stricter phase ceilings independent;
  budget exhaustion cannot skip cells or change inputs.
- [ ] Full task checks, independent reviews and source/binary/evidence hashes.

## P2: isolated GitHub screening jobs
Separate CI claim after approved workloads/loads and P0/P1 completion. Proposed
files: .github/workflows/remediation-gate.yml, scripts/production_feasibility.py.
- [ ] No replacement of normal remediation gates or original benchmark grid.
- [ ] Ordinary stream/indexed jobs:30s warmup/120s measurement; native25min/15min.
- [ ] Separate resource/recovery jobs per lane, each native55min; each job90min
  total, with25min build/preflight and10min publication allowances. Aggregate nonresetting budgets and partial artifact export.
- [ ] Host/build eligibility failures are BLOCKED; unexpected runtime regressions name
  their cause separately from known source blockers or existing mutation survivors.
- [ ] SCREEN PASS is engineering evidence only; no production, p99.9 or capacity
  certification from thin samples. First required failure stops that lane.
- [ ] Any correction has one independent root-cause/test-first scope, unchanged old
  gates; no automatic architecture rewrite or favorable rerun.

## Deferred production and competition stages
Unapproved R1 full-host/125-hour schedule is not executable from this revision.
Later define an actual deployment and immutable supported profile, then approve
its memory/cardinality/disk bounds, cold1/4/16GiB recovery, long maintenance and
sustained load/capacity/tail sampling. Transactions/events/consumer guarantees
remain their existing roadmap prerequisites. Real competitor versions/transport,
replicas, durability, indexes and workloads must be matched before any parity claim.
