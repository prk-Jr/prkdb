# Bounded Durable append diagnostic outcome

2026-10-14. The approved finite cycle is closed as **inconclusive**. No production
source was changed, no performance exception was taken, and nothing was merged.

## Identity and actual outcome

The sole authorized milestone was
`859b52538b4fb3027a355ecb39ff3026e95fbbf4` on
`wip-stream-durable-diagnostic`, based on
`f2a798413b0575abd6416d23a16f417a45aab70b`.
[GitHub run 38569346117](https://github.com/prk-Jr/prkdb/actions/runs/38569346117)
failed in the first CPU smoke qualification, before any database benchmark.
Its retained result has `benchmark_invocations=0`, `complete=false`,
`timing_eligible=false`, and error `missing exact native exec`. There are no new
throughput, latency, syscall, scheduler or database CPU measurements.

The source pair remains:
- A: `f2a798413b0575abd6416d23a16f417a45aab70b`.
- B: `947ef54eac3ccf2b93d3f2e083edf9c9f6f95e9b`.

The normal phase gate, mutation sweep, WAL benchmark and IAI jobs were skipped for
this isolated probe. The green harness-result job confirms the legitimate skip;
it supplies no new acceptance evidence. This CI failure was caused by newly
authored diagnostic tooling, rather than observed mutation survivors or a newly
demonstrated database regression.

## Confirmed diagnostic defects

1. The CPU renderer requested `comm,pid,tid,cpu,time,event` and omitted tracepoint
   payloads. Its output contains the native exec timestamp with an empty body,
   while raw events contain the exact executable filename and PID 22569. The
   strict lifetime parser correctly refused that incomplete rendering. Local
   repair uses event-type-specific fields for software samples and tracepoints,
   preserving separate callchains and exact lifecycle checks.
2. Both marked Cargo artifacts reported `fresh: true`. Each retained marked ELF
   is byte-identical to its original, so four saved paths contain only two
   executables. Applying overlays with `copy2` retained older timestamps while
   reusing the original target; this is consistent with stale Cargo reuse. Exact
   source timestamps were not saved, so the proven result is stale reuse, not a
   fully reconstructed timestamp history. Local repair copies source bytes
   without preserving timestamps, explicitly cleans only the marked source
   package's release artifacts in its own target, and requires fresh compilation
   and distinct original/marked content in the manifest. Saved originals and
   dependency caches remain intact.

Both repairs are within the approved helper/workflow scope. Regression tests and
a deterministic execution of the workflow build loop precede the fixes. These
local repairs have not been pushed or qualified on Linux. No second dispatch or
benchmark invocation is authorized by the consumed protocol.

## Evidence and limitations

Independent review and root verification match 11 tooling files to the milestone,
2 overlay files, 28 manifest members and all 73 run-inventory members. Source
inventories contain 658 A blobs and 661 B blobs matching immutable Git trees.
All four build commands exited zero; compiler/profile/features and retained
hashes agree. That does not rescue the marked builds' incorrect compilation
identity.

The synthetic native process retained ordinary UID/GID/groups and NOFILE values.
Root limits were unchanged. Raw CPU recording is 546,906 bytes; rendered evidence
including inventory is 3,339,911 bytes. Caps passed and no loss/throttle indicators
were found in the retained text outputs. Ownership finalization succeeded with no
cleanup errors. Full CPU lifecycle/callchain qualification did not complete.

CPU attributes also show lifecycle tracepoints inheriting global frequency and
DWARF settings. The archive contains the smoke's exec/fork/exit/rename events; no
lifecycle event loss is proved. A future qualification must establish complete
lifecycle coverage for its actual command, not infer it from one smoke. Recorded
Linux library hashes agree, but the library bytes were not uploaded for an
independent local rehash.

Retained local evidence lives under
`.agents/verification/durable-append-diagnostic-2026-10-14/`: proposal, reviews,
RED/GREEN logs, full pre-push log, run metadata/log, complete artifacts,
`EVIDENCE-AUDIT.json`, `REPORT.md` and `NEXT.md`. These ignored files and binaries
are preserved separately from the task branch.

## Performance disposition and follow-ups

The prior Linux Durable 1-writer/1-KiB/b1 result remains open: throughput
-5.856459% and mean per-repetition p99 +45.60271%. The earlier Mac losses and
unmerged bounded-header accessor remain held. Passing instruction counts does
not explain fsync wall time. This failed diagnostic supplies no evidence to
waive the loss, abandon the project, certify production, or select another
architecture.

- Renderer repair -> diagnostic helper correction; acceptance: archived mixed
  event regression plus live exact-command CPU/lifecycle/callchain qualification.
  Local tests alone do not complete that acceptance.
- Build identity repair -> diagnostic workflow correction; acceptance: stale
  artifact rejection and fresh marked compiles with retained clean logs and
  different ELF contents. Actual Linux acceptance remains outstanding.
- Lifecycle configuration -> next explicitly approved qualification; acceptance:
  event attributes and complete native identity coverage using the actual command.
- Durable loss -> next explicitly approved investigation; acceptance: complete
  immutable control and qualified trace evidence, or another explicit
  inconclusive disposition. No automatic new run or production rewrite.
- Accessor integration -> Task 2.15b.6; acceptance: independent performance
  disposition and all original gates. Still unmerged.

Local code quality checks and the diagnostic outcome are separate from readiness
to merge production work. The task-owned build cache and worktree may be cleaned
after final review; retain this branch and all evidence. No ledger finding,
remediation spec revision, R2 requirement, gate threshold or production flag changed.
