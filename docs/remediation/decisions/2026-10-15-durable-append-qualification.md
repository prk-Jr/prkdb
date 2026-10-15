# Corrected Durable qualification outcome — 2026-10-15

The single authorized follow-up is closed as **inconclusive**. Build provenance
is verified; full recorder qualification is incomplete. No database timings,
production changes, merges or performance exceptions occurred.

## Actual run and identity

[Run38686445143](https://github.com/prk-Jr/prkdb/actions/runs/38686445143) used exact
reviewed milestone `e4c12d5d19d9657dc5c688c9619bc028b0e08c6f`, mode `qualify`.
It built four variants successfully, then failed in the CPU attribute parser with
`unexpected/duplicate recorder event attributes`. Retained result:
`qualification_only=true`, `benchmark_invocations=0`, `complete=false`,
`timing_eligible=false`; `runs=[]`. Syscall/scheduler smokes were not reached.
The original failed run38569346117 is unchanged.

Phase2 remains `f2a798413b0575abd6416d23a16f417a45aab70b`. Diagnostic tooling
foundation is `65ae1ba132e521d90195cf3d940c077c59cff036`. The task continuation
uses unmerged diagnostic infrastructure only, without adopting production work.

## Verified progress

Independent offline audits match all 13 archived tooling files, immutable A/B
source inventories (658/661 blobs and Git tree IDs), the exact common overlay,
and all 34 manifest members. Four compiler artifacts are `fresh:false` with
matching pinned compiler/profile/features/environment. Both marked-only package
cleans succeeded, preserving originals. Four saved paths now contain four
different ELF contents. This establishes that the prior stale-build defect was
corrected in the actual Linux run.

CPU recording now uses 99Hz userspace/DWARF8192, while lifecycle/rename events
use fixed period1 without graphs. Typed rendering preserves tracepoint bodies.
The archive demonstrates these settings and complete recorded native lifetimes
and renames for the synthetic Python smoke. These are recorder observations,
not PrkDB performance measurements.

## Failure and local correction

Our static canonical-name inference was wrong. Actual perf6.17.13 on kernel
6.17.0-1022-azure preserves the full event selector
`cpu-clock/freq=99,call-graph=dwarf,stack-size=8192/u`, while the parser expected
`cpu-clock:u`. The numeric attributes were correct; the parser refused its name
before running native-context reports. This was a diagnostic compatibility
error, not missing recording coverage or a demonstrated database regression.

Local correction `7aa342443f80c41ccdc0f754ac30846f8bd50789` accepts only those
two exact labels, preserves original names/details, retains every numeric/clock/
identity check, and refuses mixed, unknown or conflicting labels. CPU reports
and callchains must agree. Readable chains alone cannot qualify a missing report.
Actual-artifact regression tests fail before correction and pass afterward.
This correction is unpushed and not qualified by a fresh live Linux run.

Retrospective offline parsing of the unchanged archive finds 38 native CPU
samples for PID23204 and TIDs23204/23206, complete exec/exit/rename coverage, and
readable callchains. Native-context reports and filtered chains were never run,
so no unresolved-symbol weight or full CPU qualification is claimed.

## Integrity, limits and verification

All 74 run-inventory member hashes match. RawCPU453470bytes; rendered evidence
including inventory2872508bytes, largest file2194913bytes. Caps pass; no loss/
throttle indication in 12 retained text outputs. Ordinary UID/GID/groups/NOFILE
and root limits were preserved. Six command exits and finalization succeeded,
with no cleanup errors and ownership complete. Runtime-library hashes were
recorded on runner; their bytes were not uploaded for independent local rehash.

Full pre-push before the sole push passed: fmt/strict workspace clippy, nextest
1533passed/10existing skips, concurrent tests+doctests1602passed/0failed/24existing
ignored, Durable and Fast200seeds each68256checks (952/700rolls), ledger/render
and all required script/doc checks. No LEAK flag appeared in this run.
Final local correction:145combined Python tests passed, actionlint/AST/diff
checks and six immutable donor comparisons passed. Independent reviews approved
code, protocol, workflow and evidence. No Rust rebuild after the Python-only
correction; no Criterion/IAI/performance claim.

## Disposition and next step

The earlier Durable throughput -5.856459% and mean per-repetition p99 +45.60271%
remain unresolved. The bounded-header accessor remains unmerged. No new result
permits production qualification, performance waiver or architecture decision.

Do not spend another full build cycle merely discovering recorder-format errors.
The four verified executables and proofs are retained. A separately approved
follow-up should qualify the corrected recorders independently of compilation,
or restore the verified executables with exact source/overlay/runtime-library
checks before a bounded paired diagnostic. No automatic rebuild/fallback/rerun.
A concrete unapproved follow-up outline is retained in ignored evidence.

Evidence: `.agents/verification/durable-append-qualification-2026-10-15/`, including
all artifacts, `build-audit.json`, `run-audit.json`,
`configured-cpu-name-offline-audit.json`, RED/GREEN logs and final `REPORT.md`.
Only this task's cache/worktree is cleaned; branch and evidence remain. No global
ledger/spec revision, R2 requirement, threshold or production flag changed.
