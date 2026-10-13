# Production feasibility implementation plan

> For agentic workers: use executing-plans with read-only independent spec/plan,
> code and evidence reviews. Root owns mutations/builds/dispatch; each later harness
> slice is separately claimed and waits on its actual prerequisites.

Goal: approved, machine-checked stream and indexed production profiles, or honest
fail/blocked decisions. Architecture: eligibility first, fixed manifest, real public
workloads and bounded screens/soaks; no replacement architecture in this plan.
Tech stack: Rust1.98.1/public APIs, existing FaultFs/xtask, Linux evidence/CI later.

## Current documentation task
Files only:
- docs/superpowers/specs/2026-10-13-production-feasibility-design.md
- docs/superpowers/plans/2026-10-13-production-feasibility.md
- docs/remediation/decisions/2026-10-13-production-feasibility.md

- [x] User selected both separate lanes; read governing spec/plan/ledger and8eab751.
- [x] Safely retire seven clean completed worktrees, keeping branches/report snapshots
  and restore commands. Preserve paused/in-progress worktrees and their caches.
- [x] Independent design/plan review; both read-only reviewers approved round 3.
- [x] Draft maintainer decision table with concrete requirements and readiness.
- [ ] Verify docs and required repo checks in owned target; commit docs checkpoint,
  update handoff; clean target and retire clean docs worktree after preserving report.
- [ ] Obtain concrete requirement/host decisions; do not call absence an approval.

## Task P0: eligibility inventory (NO timings)
Separate claim wip-feasibility-eligibility, only after protocol approval.
Files: proposed scripts/production_feasibility.py, scripts/test_production_feasibility.py,
docs/benchmarks/production-feasibility.toml, task decision document. Exact interfaces
and schema are specified in that slice's reviewed test-first plan before coding.
- [ ] RED: open shipping finding, unmerged prerequisite, missing regression/CI,
  unsupported operation, absent approval, wrong host/source/durability all block.
- [ ] Implement immutable manifest with independent lane status, required capability
  and finding/task mappings; never default missing evidence to PASS or zero.
- [ ] GREEN: P0 output at current source MUST say BLOCKED. This is expected product
  ineligibility, not a reason to weaken validation. Valid synthetic eligible fixture
  is a unit control, not production certification.
- [ ] Commit test:first eligibility gate; preserve all source/evidence decisions.

## Task P1: ordinary harness correctness
Separate per-lane claims after applicable correctness work is integrated and P0
qualifies assembled source. Do not take over2.17/2.15b.6 or touch their open claims.
Proposed files (must be confirmed in each actual slice):
crates/prkdb/examples/stream_production_feasibility.rs,
crates/prkdb/examples/indexed_production_feasibility.rs,
crates/prkdb/examples/support/production_feasibility.rs,
crates/prkdb/tests/production_feasibility_contract.rs,
scripts/test_production_feasibility.py.
- [ ] RED deterministic real-path/exact-data/unique/RMW/offset/transaction/maintenance
  probes; every attestation has a causal negative control. Unsupported API blocks.
- [ ] Public production paths only, uninstrumented timings separate from diagnostics;
  scheduled-arrival IDs/timestamps, bounded client/engine work, precise phase markers.
- [ ] Selftests reject partial/unordered/corrupted rows and leaked calibration/observer
  work in timing. Native fresh-index/offset/recovery checks; no fake callback/hooks.
- [ ] Relevant/full repository checks, independent reviews and manifest hashes.

## Task P2: screen and single reviewed correction budget
After approved host/manifest and P0/P1 completion, separate CI claim:
.github/workflows/remediation-gate.yml isolated probe plus scripts/production_feasibility.py.
- [ ] Qualify host/resources and fixed reference before first candidate; preserved
  historical Fast/Durable/IAI/T1-T5 gates remain independently required.
- [ ] Run only the finite approved 22-cell screen and <=60-cell staircase; fixed
  120s warmup/300s measurement. Comparison blocks only for a separately authorized
  optimization, within the design native caps. Screens do not certify thin tails. All rows,
  outcomes and raw resource evidence, bounded complete prefixes on failure.
- [ ] Required failure stops lane; no soak, favorable retry or automatic prototype.
- [ ] One cause-backed correction needs separate plan/root-cause test-first scope and
  unchanged acceptance. Failure returns architecture/profile/stop decision.

## Task P3: sustained and deployment failure gates
Only after screens pass. Separate reviewed manifest/run schedules with explicit
source, devices, resources, absolute approved rates and time/output caps.
- [ ] The design's 12 ordinary cells at one-hour 70/90/110% frozen-capacity tiers,
  3 independent Linux repetitions plus 12 one-hour frozen 100% capacity
  verification cells, within 125-hour nonresetting native cap. Absolute capacity
  minima independent of tier rates; >=50,000 terminal outcomes per ordinary tail
  class/repetition plus prespecified confidence rule. Preflight rate/count/duration,
  no pooled classes/records/repetitions, fixed generator/outstanding/drain.
- [ ] Mixed data/readers/hot keys, actual retention/compaction and progress, fixed-live
  storage/memory soak including cold leading key; independent unrelated-collection
  interference and multi-key fairness. No bypass via extra storage engine.
- [ ] Faithful fault profiles plus cold1/4/16GiB recovery and complete ready state;
  match guarantees, do not treat process crash as physical-power-cut proof.
- [ ] Independent data/results integrity and policy review; all required cells,
  no averaging, PASS/FAIL/BLOCKED/UNJUDGEABLE. Phase/ship decision stays separate.

## Task P4: scoped competition (later, blocked today)
Equivalent production transport and source semantics, exact competitor versions,
primary docs, hardware, clients and immutable configurations before implementation.
No embedded-versus-server speed claim, no better batching/index-free workaround,
no replica/fsync downgrade. Actual prereqs Phase4+ for network brokers. Separate
claim/review/run, no automatic execution authorized by this doc task.
