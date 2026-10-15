# Corrected Durable diagnostic qualification — 2026-10-15

Status: maintainer requested next work with parallel subagents after the prior
report identified Linux qualification as the next step. One follow-up milestone
push and one bounded qualification-only job are authorized. No automatic retry,
merge, performance exception, architecture change or database measurement.

## Goal and inherited contract

Establish whether the corrected source/build and actual recorder chains qualify
on Linux. Prior run38569346117 remains immutable, failed and inconclusive with
zero database calls. Tooling foundation65ae1ba132e521d90195cf3d940c077c59cff036
contains locally reviewed rendering/fresh-build corrections. Root phase2 stays
f2a798413b0575abd6416d23a16f417a45aab70b. This diagnostic continuation uses that
unmerged tooling foundation; no production prerequisite is adopted or merged.

The2026-10-14 diagnostic design supplies exact A/B sources, overlay, compiler,
resource/identity, monotonic clock, evidence/provenance and caps. This task only
strengthens event-specific qualification and invokes existing `qualify`, never
`run`. Retain both original executable copies and both freshly compiled marked
variants; no source or durability change. All six donated helper files remain
byte-identical to444c84b.

## Scope

- scripts/stream_durable_diagnostic.py and scripts/test_stream_durable_diagnostic.py.
- .github/workflows/remediation-gate.yml: isolated qualification choice and exact
  mode selection in the existing diagnostic job. The frozen frame-reuse task's
  workflow claim is respected through this limited authorized continuation.
- This spec, matching2026-10-15 plan and matching decision record.
- Ignored task handoff and verification evidence.

No Rust source, benchmark body, dependency, global ledger/spec revision, R2
requirement, production flag or old evidence is edited.

## Qualification and stopping rule

Build four executables from immutable A/B + common overlay. Save complete
compiler JSON/proof/source hashes. Marked-only package cleaning uses its own
explicit target. Every benchmark artifact must report fresh:false; original and
marked executable contents differ per source. Source bytes alone do not prove
that Cargo compiled the overlay. Preserve the clean command/stdout/stderr/exit.

CPU software samples remain99Hz userspace/DWARF8192. Lifecycle and rename
tracepoints must record every occurrence (period1, frequency disabled), without
per-event DWARF stacks. Validate each required event's actual saved attributes,
not merely the existence of one tracking event. Keep timed COMM/FORK/EXIT, CPU/
TID/TIME/raw payloads, native exact exec/lifetimes/rename and checked monotonic
clock. Configured event names must still match parsers on real perf.

Qualify CPU, syscall and scheduler chains with synthetic non-database smoke,
including readable CPU callchains/sample counts, complete syscall entry/exit
status pairs and scheduler wake/switch/name/lifetime coverage. Preserve ordinary
UID/GID/groups/NOFILE and root's original limits. No privilege/resource widening,
new sysctl, fallback, missing-event inference or retries.

Success requires complete=true, qualification_only=true, benchmark_invocations=0,
all three category qualifications, fresh compiled overlay identity and verified
cleanup/inventories/caps. A failure closes this finite attempt as inconclusive,
with every available prefix retained. Even success supplies no database latency,
throughput, production qualification, regression explanation or performance waiver.
A later paired benchmark schedule requires a separate decision/authorization.

## Fixed budgets

One90minute GitHub job; existing55minute build step and nonresetting54minute build
budget, no native launch after job minute60. Native aggregate20minutes includes
19minutes operation and1minute finalization reserve; each command <=120seconds.
Native step22minutes, always-upload step8minutes. CPU raw64MiB, syscall/scheduler
128MiB each,1GiB aggregate raw; rendered512MiB/file and2GiB total; diagnostic
buffer256MiB/process. Runtime/WAL pruned before observer accounting. Caps include
failed/rendered prefixes and post-exit overshoot. Terminate only owned groups.

## Verification and report

Baseline135Python tests; regression RED/GREEN for event-specific commands and
saved-attribute refusal. Independent provenance, spec and quality reviews. Actual
workflow-mode control must show the qualification dispatch invokes `qualify` and
not the DB schedule. Run full pre-push with task-owned Cargo target, actionlint,
Python suites, immutable-donor and production-diff checks before sole dispatch.

Download actual artifacts and independently verify exact tooling/source/hash/
clean/build identities, every recorder's attributes/loss/clock/native resources/
CPU or syscall or scheduler premises, zero DB invocations and final cleanup.
Record full outcome and unresolved acceptance in decision/report/handoff. Clean
only this task's cache/worktree after review; retain branch and all evidence.


## Execution disposition

2026-10-15: sole job38686445143 at e4c12d5 failed before any database timing.
Four fresh compiled variants/provenance are verified. Actual CPU settings and
lifecycle payloads were correct; perf6.17's configured event spelling was refused
by the parser. Local strict-label fix7aa3424 is tested/reviewed but unpushed and
not live-qualified. CPU reports and syscall/scheduler qualifications remain open.
Finite attempt closed inconclusive, original performance hold unchanged. See
matching decision and ignored evidence REPORT/NEXT. No automatic new job.
