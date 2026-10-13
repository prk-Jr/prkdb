# Live Linux trace qualification before one bounded WAL capture

The maintainer's 2026-10-13 Go ahead authorizes a finite successor to the closed
inconclusive descriptor diagnostic. This task fixes no database behavior and
changes no durability, benchmark settings, thresholds or architecture.

## Choice and scope

First qualify the already corrected parser against live ordinary Linux /proc
status/limits, then execute the existing full three-recorder smoke with the same
resource propagation, ordinary UID/GID/groups, thread coverage and loss rules.
This is preferable to immediately relaunching native benchmarks, which could
again be interrupted by unsupported instrumentation. Architecture replacement
would be premature without a qualified cause or bounded performance evidence.

Branch wip-wal-trace-qualification starts from phase-2 at
9b88229d29cf4ea1a384b1357139f0b0f6c7f313. Copy only reviewed closed-task tools from
c2b1c4a; do not adopt production changes or merge another experiment.
Files: scripts/wal_stall_diagnostic.py and its tests (unchanged); corrected
scripts/wal_descriptor_diagnostic.py and its tests; new scripts/wal_trace_qualification.py
and test_wal_trace_qualification.py; .github/workflows/remediation-gate.yml;
this spec, companion plan and docs/remediation/decisions/2026-10-13-wal-trace-qualification.md.
Narrow runner corrections are allowed only when qualification demonstrates the
specific root cause, with failing tests and independent review. No unrelated refactor.

## Finite sequence

At most two qualification-only Linux runs: initial and one test-first correction
for a demonstrated tooling defect. Neither launches a database benchmark. Failure
on the second run stops custom tracing and reports its limit; it is not evidence
that the database or project should be abandoned. No retry for favorable numbers.

After a successful qualifier, run at most one fixed capture at a reviewed exact
SHA. The capture job first repeats the same smoke on its own disposable host,
then invokes the existing descriptor runner's four controls/four profiles exactly
once. Failure stops with its prefix retained. Original failed acceptance remains
FAILED; filtered/traced rows cannot clear it. No A/A/A/B or full-grid rerun here.

## Qualifier contract

Fresh output only. Require x86_64 Linux, four CPUs, expected tools and no competing
build/perf. Preserve current source hashes and exact command environment. Record
ordinary resource_proof and raw /proc before decoding; validate real/effective/saved
IDs and groups plus both NOFILE limits against the ordinary proof before smoke.

Copy the host Python executable to a <=15-byte wal-diag-smoke alias; verify its
hash, execute the existing synthetic CPU/syscall/scheduler/short-thread/rename smoke
through exactly the existing three nested recorders and root supervisor. It never
runs a database or downloads benchmark binaries in qualification-only mode.
Reuse existing fixed descriptor equality, 120-second containment, 100 ms FD/budget
monitor, raw per-file 64/128/128 MiB and cycle 1 GiB caps, rendered 512 MiB/file
and 2 GiB/cycle caps. No host limits/sysctl changes or wider client deadlines.

Qualification requires actual post-exec identity/limits, all three saved clock/
tracking attributes, loss-free parsed events, native lifecycle/name/rename brackets,
unsampled thread coverage and paired syscalls. Keep unresolved CPU symbol flags;
no claim of complete unwind or source-line precision. Unknown events/identities,
missing evidence or containment failures fail closed and preserve their status.

Retain result.json on all failures, current command exit statuses, raw /proc,
recorders, rendering errors, source proof and SHA256SUMS. Restore ownership only
within the fresh owned output so artifacts remain downloadable. No event dropping,
truncation or partial-success report. Workflow always uploads evidence.

## Demonstrated Linux 6.17 format compatibility

Qualification run 38369782043 retained its failed prefix before any database run.
For the system-wide outer recorder, `inherit=0` is expected: require the raw
THREAD_MAP's single global task (-1) and CPU_MAP exactly matching the four ordinary
CPUs instead. CPU/syscall per-task recorders still require inheritance. All three
retain TIME/TID/CPU, sample_id_all, comm/task and CLOCK_MONOTONIC attributes.

Linux task_rename can lack a target PID. Never use its caller TID as the target.
Pair it one-to-one with the timed COMM transition: matching caller sample IDs,
old/new names and non-overlapping time bracket; the COMM body supplies target PID/
TID. Explicit-PID formats remain supported. Missing/ambiguous pairs, unknown names
and corrupted native target groups fail closed, including no-op renames.

libtraceevent may render scheduler bodies as `comm:pid [prio] state ==> ...` or
`comm:pid [prio] CPU:cpu`. Accept these and the existing keyed format only through
anchored parsers; preserve colon/space-containing names, exact target IDs and
states. Malformed bodies fail; unsuccessful wakeups do not become runnable waits.
Tests reproduce all three defects against the first captured prefix, followed by
complete offline smoke analysis on a copy. This is the one allowed tooling
correction; it authorizes no additional benchmark or qualification retry.

Primary format sources: Linux v6.17 tools/perf/util/evsel.c, kernel/events/core.c,
include/trace/events/task.h and tools/perf/builtin-script.c; libtraceevent's
plugins/plugin_sched_switch.c. Source proof and unmodified failed artifacts remain
in the main checkout's .agents/verification/wal-trace-qualification-2026-10-13/.

## Decision boundary

The final report separates database performance evidence from diagnostic failures.
A cause-backed local optimization requires its own reviewed task and unchanged
correctness/performance gates; none is adopted here. If no causal capture can be
qualified within this bound, stop this tracing approach and give a concrete
alternative and unresolved acceptance conditions. Never equate instrumentation
failure with proof of project non-viability or competitiveness with Kafka/Iggy.
Independent review of existing qualified evidence should identify measured gains,
failed design gates, confounders and one finite next decision, without new builds.
