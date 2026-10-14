# Durable append diagnostic: bounded proposal

Date: 2026-10-14. Status: approved by maintainer on 2026-10-14; exact scope, workflow overlap and one bounded dispatch authorized.
Readiness: sole finite cycle closed as inconclusive; local corrections need fresh Linux qualification. No new database timing or merge.

## Question and evidence

Explain, or explicitly fail to explain, the Durable single-writer 1 KiB/b1 loss
between immutable A=f2a798413b0575abd6416d23a16f417a45aab70b and
B=947ef54eac3ccf2b93d3f2e083edf9c9f6f95e9b. Prior Linux throughput changed
-5.856459% and mean per-repetition p99 +45.60271%. Mac Durable cells were also
adverse. The 15-row Linux instruction gate and 13 floors passed; neither resolves
this loss. Keep original outcomes, source branches and all thresholds unchanged.

Read-only source audit: new header accessor is uncalled by append benches. The
executed change is a fixed Shared field initialized at open and an equal-valued
sparse-index stride alias. This does not establish generated-code or host cause.
One outstanding append traverses record construction, encoding, admission,
channel handoff, writer framing/write/sync, sparse-index hook and reply/Tokio
resumption. Index-hook CPU samples are useful; their absence proves nothing.

## Approaches and recommendation

1. Recommended: untouched benchmark timing controls plus a separately labelled,
identically instrumented benchmark variant for measured-window/VFS attribution.
No Rust production edits. This can locate storage waits and constrain the residual;
CPU/scheduler sampling may leave internal queue/hook attribution inconclusive.
2. Untouched whole-process traces only: fewer changes, but no measured-window
markers; b100, warmup, setup and teardown contaminate attribution.
3. Instrument production submission/hook/reply boundaries: greatest precision,
but changes hot-path code and creates more ownership/performance obligations.
Defer it unless this finite investigation demonstrates a specific need.

## Scope and isolation

New branch/worktree wip-stream-durable-diagnostic from A. Original CRC task stays
paused/unmerged. Never adopt frame reuse or change the R2 screening template.
Proposed files, and no others without reporting a new STOP:

- scripts/stream_durable_diagnostic.py and scripts/test_stream_durable_diagnostic.py
- crates/prkdb/benches/wal_write_path.rs (opt-in diagnostic plumbing only)
- crates/prkdb/benches/support/stream_durable_diagnostic.rs (new)
- crates/prkdb/tests/stream_durable_diagnostic_support.rs (new support tests)
- scripts/wal_stall_diagnostic.py and scripts/test_wal_stall_diagnostic.py
- scripts/wal_descriptor_diagnostic.py and scripts/test_wal_descriptor_diagnostic.py
- scripts/wal_trace_qualification.py and scripts/test_wal_trace_qualification.py
- .github/workflows/remediation-gate.yml (one isolated probe choice/job)
- docs/superpowers/specs/2026-10-14-durable-append-diagnostic-design.md
- docs/superpowers/plans/2026-10-14-durable-append-diagnostic.md
- docs/remediation/decisions/2026-10-14-durable-append-diagnostic.md

The six older helper/test files are copied from the completed trace qualification
milestone 444c84b609d0eaa141bea79adbdd0bcc2a94ae95; retain their pure tests and do
not invoke their old schedules/hardcoded executable trusts. That successor already
qualified the live padded /proc parser and Linux trace formats. Fresh qualification
of this task's actual commands is still required. Existing frame-reuse note owns
the workflow file: approval must explicitly permit this isolated addition while
that frozen branch remains untouched. No production-file claim overlaps.

## Source, executable and observer identity

Original Linux WAL run did not retain its executable manifests; saved IAI binaries
are another target. Build original wal_write_path once per exact A/B source, same
pinned compiler/config/features/environment. Save compiler JSON, Git/source/tree/
Cargo/support-file hashes, executable SHA256, dependency/runtime libraries and all
exits before timing. These are new builds of original sources, not the original
adverse run's identical executables. No source SHA guessing or self-referencing SHA.

Then apply exactly the same benchmark-only patch to each clean source snapshot and
build the marked variant once each. Preserve original executable copies untouched.
Store both original Git identities, overlay diff/hash, compiler manifests and
marked executable hashes. Reject any production-tree change. Four builds total;
all complete before native work. Explicit separate CARGO_TARGET_DIR per worktree.

No host tuning, CPU pinning, cache dropping, altered sync mode, deadlines, payload,
segment size or resource budget. Same local temp filesystem/device for all calls,
fresh directories per invocation. Capture runner/kernel/CPU/affinity/cgroup/RAM/
mount/device/governor/IO pressure and competing processes before/after every call.
Do not assert a shared runner is a production host or stable reference.

## Qualification before timings

One Ubuntu24.04 x86_64 four-CPU job; four Tokio workers. First qualify exact live
ordinary/post-exec native UID/GID/groups/NOFILE and hashes against saved proofs.
Root recorders preserve ordinary native identity/groups and original NOFILE pair;
root-supervisor limits are separately recorded, never raised for the native child.
No new sysctl change is requested; unsupported privilege/events stop the run.

Qualify each actual separate recorder chain with a synthetic non-database smoke:
CPU99Hz userspace DWARF8192; syscall entry/exit for write/pwrite64/fsync/fdatasync/
futex; scheduler switch/wakeup and native exec/fork/exit/rename metadata. Common to each category: qualified monotonic clock, TIME/TID/CPU, native PID/TID
identity/lifetimes and exec/exit bounds, loss/throttle rejection and actual
post-exec resource proof. CPU and syscall commands must enable and retain perf
tracking COMM/FORK/EXIT metadata with timed sample IDs for inherited threads;
retain native task-name/rename evidence where interpretation depends on it.
Missing common identity/lifetime evidence fails that category. Do not use the
old all-three-recordings analyzer unchanged: category analyzers validate their
own evidence plus common tracking records.

Category-specific qualification:
- CPU: readable userspace callchains, sample counts and unresolved symbol weight.
  No syscall-pair requirement and no exact unsampled-function-cost claim.
- Syscalls: entry/exit pairing and return status for every captured native call,
  including pairs crossing a marker; no DWARF or scheduler-event requirement.
- Scheduler: complete switch/wakeup and native thread/name/rename coverage,
  including unsampled threads; global CPU attachment proof and timed lifecycle
  metadata. Missing coverage is incomplete accounting, never zero wait. Deterministic parser fixtures test
unsampled coverage; smoke must not depend on a live thread receiving zero samples.
No global block capture; no DWARF on every syscall. No weaker fallback or retries.

## Fixed schedule and instrumentation

Untouched controls: A,A,B,B,A,B,B,A, eight invocations. Each retains three
repetitions, 1000ms warmup, 3000ms measurement, original config and payload. The
existing substring SPIKE_FILTER=stream_append/durable/1w/1k/b1 ALSO SELECTS b100.
Keep both cells, requiring exactly ordered b1,b100 per repetition (six rows/call).
Retain all 48 rows. No timings from profilers enter acceptance or replace old rows.
This tests source/order/within-source variation, without a new pass threshold.

Marked variant keeps the SAME b1/b100 envelope and three repetitions. Only exact
b1 receives a transparent VFS timing wrapper and per-operation timestamp capture.
Markers identify each of three repetitions with source/cell/config/PID/TID and
operation totals; begin precedes the first eligible operation and end follows the
last eligible completion. Capture monotonic bounds, append start/return and the
preparation-to-append interval. Pair syscall/scheduler events over the full
recording before interval selection; retain crossing-boundary events explicitly. VFS delegates identical bytes/offsets/errors,
write/sync/lock operations and records write_at/sync_data entry/exit, path, TID.
Do not bypass fsync, batch more, alter clocks used in record contents, use sleeps or
change production default behavior. Place append-start AFTER records_for_append, immediately before log.append;
preparation includes key formatting and record construction. Delegate each VFS
operation exactly once; observer failure must not synthesize an engine I/O error,
skip a sync or change the engine error/result. Buffer observations and write them
after the cell; label clock/conversion uncertainty and observer allocation cost.
Before an observation would exceed the preset buffer cap, mark diagnostic failure,
finish the already-started delegated engine operation, stop further submission,
flush the existing bounded observations and a breach record, then exit nonzero.
Do not silently drop observations and continue, discard the buffered prefix, or
resume the schedule. Prefix output uses a bounded binary representation within
the buffer's byte ceiling and counts toward observer disk limits; exporter failure
is retained as incomplete evidence. Test this containment path deterministically.

Record absolute CLOCK_MONOTONIC timestamps from a checked conversion/witness,
not assumed wall-clock equivalence. Keep actual measured append intervals, including
completions beyond the planned end; choose operations by original t0 semantics.
Only confidently corresponding write/sync events may be attributed to an append.
Use interval arithmetic, not subtraction of unrelated p99 values. Missing/overlap/
extra sync correspondence makes that attribution inconclusive. Raw data stays intact.

Two marked ordinary controls A/B quantify observer/codegen perturbation relative
to the original controls; they cannot certify its absence or serve as gate timing.
Then six marked captures: syscall A/B, scheduler B/A, CPU A/B, one call/side/type.
Six rows per call retained; attribute only exact b1 operation/window intervals.
Maximum 16 benchmark invocations; no repeat-on-loss or favorable-only rerun.
Different recorder invocations cannot be added as one operation's critical path.

## Caps and interpretation

One nonresetting 20-minute aggregate budget: at most 19 minutes for native/smoke/render work, reserving one minute for contained ownership repair, final metadata, inventory and post-write byte accounting. Every invocation is at most 120 seconds,
and 90-minute total workflow cap including builds/checks. Remaining aggregate
budget bounds each operation. CPU64MiB, syscall128MiB, scheduler128MiB raw per
capture; 1GiB total raw, 512MiB per rendered file/2GiB rendered total. Diagnostic
buffer total256MiB per process. Limits cover smoke, errors, buffered-prefix files, observer logs and
post-exit overshoot; prune runtime/WAL directories BEFORE observer traversal; do not cap engine WAL to fit tracing. Owned process groups
are terminated on breach/failure; save failure prefix, exit/cleanup/inventory proof.
One milestone push/dispatch only after tests and independent review; retain red too.

Classify evidence conservatively: reproduced source-associated loss; order/host/
observer sensitivity; demonstrated sync-interval contribution; scheduler delay
observations; or inconclusive. Balanced controls provide evidence, not proof that
source or host is the sole cause. Sum sync durations only for confidently matched
operations; sync elapsed includes scheduler/kernel waits and overlaps off-CPU data.
Total minus write/sync is a mixed residual, not queue residence or caller cost.
CPU sampling names actual executed caller/hook functions when resolvable; report
sample counts/unresolved weight. Sparse/inlined hook evidence cannot establish a
precise duration. No exact internal-stage claim without boundaries. If evidence
cannot explain the old loss, close this finite cycle as inconclusive, no automatic
new diagnostic or rewrite. Merge and performance disposition remain separate.

## Required verification and completion

Test-first filter inventory, source/overlay/hash equality, deadline/budget/loss/
identity failures, monotonic markers, transparency of VFS, unchanged acknowledgement
and phase selection. Deterministic support tests compare bytes/errors/clock mapping,
and rejection of malformed or incomplete correspondence. Run helper tests, focused
Rust support tests, fmt/clippy/workspace nextest/doctests, ledger/render and full
pre-push script, actionlint/diff checks before dispatch. No Criterion claim: Rust
production hot paths stay untouched; new timings are diagnostic, not perf acceptance.
Independent protocol/code/evidence review. Update task note each commit. No new
ledger finding/spec-table revision without a demonstrated reviewed defect and
separately authorized scope. No perf_note exception, weakened gate or production
qualification. Final report all outcomes/exact identities, limitations, unresolved
follow-ups with destination and acceptance; clean only this task's cache/worktree
once no reviewer needs it. Preserve branches/binaries/raw evidence.


## 2026-10-14 execution disposition

The sole approved run38569346117 at859b525 failed before any DB invocation;
CPU rendering omitted recorded exec payloads. Marked Cargo builds also reused
original executables. The retained failure prefix is independently audited.
Local repair requires event-type-specific CPU/lifecycle rendering, explicit
marked-package release-cache invalidation with retained own-target proof,
`fresh:false` compiler artifacts and different original/marked ELF contents.
These strengthen the existing compilation/qualification contract without
changing timings, checks, budgets or the consumed one-run authorization.

No repeat push or dispatch is authorized. Original performance losses remain
open. Actual Linux qualification of the corrections and complete lifecycle
coverage is outstanding. See the task decision and ignored verification report.
