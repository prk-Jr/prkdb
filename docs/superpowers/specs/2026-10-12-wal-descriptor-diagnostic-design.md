# Descriptor-qualified WAL tracing: one finite follow-up

Date:2026-10-12. User Go ahead approves the previous report's concrete next scope:
qualify descriptor limits/growth and capture scoped waits, without changing database
semantics, benchmark settings, performance gates or architecture. No adoption.

## Scope and base
New branch wip-wal-descriptor-diagnostic from phase2 at9b88229d29cf4ea1a384b1357139f0b0f6c7f313.
Copy the reviewed pure diagnostic helper/tests from closed d0055f1 (new files on
phase2); they are inputs to this self-contained tooling task, not an unmerged
production prerequisite. Never run its old36-process schedule. Reuse pinned original
base/head executables from run38206215742 without rebuilding; original failed gate,
qualified108diagnostic rows and prior partial traces stay immutable.

Files: scripts/wal_stall_diagnostic.py, scripts/test_wal_stall_diagnostic.py;
scripts/wal_descriptor_diagnostic.py, scripts/test_wal_descriptor_diagnostic.py;
crates/prkdb-core/tests/wal_descriptor_budget.rs (test only);
.github/workflows/remediation-gate.yml (one new isolated probe);
this spec, companion plan, docs/remediation/decisions/2026-10-12-wal-descriptor-diagnostic.md.
Rootownsbuilds/commits/workflow; independent spec/quality review. Conditional ledger
or global spec edits only after a reviewed finding is actually established, and
explicitly reported as scope extension. No production correction in this task.

## Bounded descriptor witness
A portable real-StdVfs wrapper counts live file leases and temporary directory
operations against a small explicit simulated budget. It never lowers process
RLIMIT or affects other tests. Observe growth with retained WAL segments under
Fast and Durable, release on Close even after a refused roll, required temporary
sync_dir headroom, acknowledgement recovery and snapshot leases across retention.
Use deterministic gates/channels, no sleeps. These characterize current behavior;
no claim that an FD management defect is fixed. Record whether the growth is a
new contract defect or an operational limitation requiring later design.

## Resource qualification and execution identity
One Ubuntu24.04 x86_64 job/fourCPUs. Capture ordinary UID/GID/groups, descriptor
soft/hard limits, other relevant limits, actual FD counts, TMPDIR mount/device,
CPU/cgroup/IO pressure and exact source/executable hashes before any benchmark.
A root supervisor records its inherited limits and explicitly sets ONLY its
child's NOFILE pair to the captured ordinary pair, then verifies equality before
starting perf. This restores equivalent descriptor budgets; never increase the
ordinary budget, pick a higher arbitrary value, mutate host sysctl/limits or alter
a gate. Refuse unsupported/failed limit propagation. Every native launch wrapper
again verifies the actual inherited pair, drops to ordinary UID/GID/groups when
inside root perf, records final identity/limits/counts and execs the pinned binary.
Qualify the complete three-recorder chain across that privilege drop before
benchmark profiles. The fixed smoke workload performs paired read/write/futex
operations, CPU work and scheduler transitions; it includes a short-lived thread
and a covered rename. Require CPU samples, paired syscalls, scheduler events and
time-qualified COMM/FORK/EXIT metadata. Retain perf evlist -i RECORDING -v
attribute proof: tracking event sample_id_all, TIME/TID/CPU, comm/task,
inheritance and CLOCK_MONOTONIC must be present, alongside timed native COMM
smoke records. Fail rather than accepting a perf fallback. An excluded rename is a negative parser
fixture and must be rejected, never silently assigned zero wait. The shared root session supervisor contains recording/rendering
commands; timeout120s remains containment, not a larger client deadline.

## Fixed Linux sequence, not acceptance timing
Four untraced resource controls, one base/head for each original selected write
and cold-read cell; then exactly four profiles in the same fixed base/head order.
Each invokes the unmodified archived binary with original3reps/1s warmup/3s measure,
4Tokio workers and same SPIKE_FILTER. These are resource/observer controls, not
reruns of A/A/A/B or the full Fast gate, and cannot clear old failure. Record
actual native PID from launch proof and sample its descriptors every 100 ms;
record count, maximum descriptor number, sampling errors and gaps, retaining the
series in memory until exit. Pre-exec samples are labelled separately; native
samples require /proc/PID/exe equality with the pinned executable. This gives a
maximum observed count, not the true peak or a guarantee of available headroom. Sampling/whole-process profile timings are excluded
from acceptance. Failure stops the fixed cycle with prefix retained, no retries.
Job20minutes. Ordinary resource-control failure ends profiling rather than raising
its budget. All native/profile rows, errors and resource proofs are retained.

## Lower-volume tracing with coverage checks
Three separate recorders: process-targeted CPU sampling99Hz/DWARF8192; process-
targeted syscall entry/exit for futex,write,pwrite64,read,pread64,fdatasync,fsync,
with no callgraphs on syscall records; outer system scheduler/block/lifecycle.
Only scheduler switch/wakeup is kernel-filtered to native program aliaseswal-diag-*
and recorded Tokio/WAL thread-name patterns(tokio*,prkdb-wal-*). Lifecycleexec/fork/
exit and blockissue/complete remain available for unsampled-thread identity and
kernel-worker completions. No system-wide hot futex/write syscall feedback.
Filter syntax documented by docs.kernel.org/trace/events.html; fail preflight if
unsupported. Render perf metadata with --show-task-events --ns and require timestamped
PERF_RECORD_COMM/FORK/EXIT identity for native threads, including threads without
CPU samples and every intermediate rename. Check each native name interval
against scheduler filters;
unrecognized/missing native-thread coverage means incomplete accounting, not zero
wait. Keep systemblock data separate unless request correspondence is established.
Explicit CPU sample field and monotonic clocks; preserve exact allowlisted environment
through sudo. Raw recordings remain untouched; rendered loss/throttle/unknown/parse
quality checks fail closed. Fixed byte containment limits, selected before capture: each profile may produce
at most 64 MiB CPU, 128 MiB syscall and 128 MiB scheduler raw data; the entire
cycle has a stricter 1 GiB aggregate raw limit. Rendered outputs are limited to
512 MiB per file and 2 GiB for the cycle. These allow considerably more than
the expected scoped data while bounding disk use; they are diagnostic limits,
not performance gates. Check sizes every 100 ms. On breach, terminate the actual
owned recorder/render group, preserve the written prefix and report inconclusive.
Apply caps to smoke, all render copies and error/stdout files, including a final
post-exit size check for the last sampling interval. Do not drop events, truncate
retained files, retry or move these caps. Record
observed overshoot, data size and perturbation limitations.

Whole native process remains the only qualified attribution window: archive lacks
internal write/cold measured-phase markers. Exclude wrapper startup by native exec
lifecycle timestamp; do not call setup/fill/warmup/close weights exact3s attribution.
Archived ELF has symbols/unwind metadata, no debugsections; no source-line promise.
No p99 or throughput adoption inference from traced rows. If original stalls do not
occur or traces cannot establish linkage, report inconclusive, preserve coldp99
concern and oldgate failure. Any next fix requires separate reviewed task.

## Completion
Test-first resource propagation/privilege identity/capture boundary controls and
VFS lease witness. Independent design and implementation review; copied36tests
plus new tests and full own-targetprepush before one milestonepush and dispatch.
No otherbuilds/perf during native jobs. Qualify exact source/binary/hash/commands/
rows/resource proofs/losses independently; conclude demonstrated cause or specific
inconclusive boundary, no architecture rewrite. Clean only own target; preserve
branch/worktree/rawbinaries/evidence/handoff. Ledger IDs only for confirmed reviewed
findings; no performanceexception requested or thresholds changed.
