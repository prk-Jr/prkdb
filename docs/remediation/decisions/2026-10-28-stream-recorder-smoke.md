# Recorder qualification and failed balanced comparison

Readiness: local checks pass, Linux checks outstanding for the postmortem fixes.
Diagnostic-only branch; no production merge or performance waiver.

## Actual result

[Workflow 40722095223](https://github.com/prk-Jr/prkdb/actions/runs/40722095223)
ran once at tooling commit `6dbea992fc9fe4641887ebd706430cf8a7a5ccbf`.
The recorder-only job succeeded. Independent offline audit rehashed all 185
inventory members and matched all 13 archived tooling files to that commit.
The result records complete qualification of the actual CPU, syscall and scheduler
chains, zero database invocations and `build_provenance_checked=false`.

The CPU chain contained 37 native samples with readable callchains; 48.65% of
sample weight remained unresolved. Syscalls contained 36 complete pairs covering
all five required categories. Scheduler evidence contained eight off-CPU and
seven wakeup-to-run intervals. Resource, lifetime, loss, size-cap and cleanup
checks passed. This qualifies synthetic recorder operation, not database speed.
The independent audit used retained raw-derived text and hashes; it did not
regenerate reports with perf.

The gated balanced job restored the retained executables and passed exact source,
binary and current runtime-library verification. Its native step subsequently
exhausted the nonresetting aggregate deadline and the 22-minute step limit.
The always-upload step then failed with EACCES reading an unreadable
`balanced-run/qualify-cpu/cpu.data`. GitHub retained only the recorder artifact
(12831296589), not the balanced-run evidence. The balanced job's final stage and
number of database invocations are therefore UNKNOWN. A qualification filename
in the upload error does not establish when the native run stopped.

There are no usable new database comparison results. The prior Durable findings
remain unresolved: throughput -5.856459% and mean per-repetition p99 +45.60271%
in the earlier Linux single-writer 1-KiB/batch-1 workload. No architecture decision,
production-readiness claim or performance exception follows from this failed
investigation. The CRC accessor remains unmerged.

## Local postmortem corrections

Observer validation repeatedly scanned the entire event prefix for each operation
and rebuilt the full operation-ID set for each VFS event. One-pass indexing now
preserves operation/VFS order, duplicate/missing-return rejection and unmatched
VFS attribution while removing that quadratic work. Deterministic dictionary-read
checks failed before the change and pass afterward: 64/512 operations went from
71,771/4,243,995 reads to 6,751/53,791. Forty-eight valid/malformed fixture
comparisons produced identical old/new results or errors. This is a proven local
cost defect; lost remote evidence prevents attributing the actual timeout to it.

Both new jobs now attempt bounded, postmortem ownership recovery before upload.
The privileged command targets only their canonical native-evidence directory,
does not follow symlinks, and has a 30-second command/35-second subprocess bound
inside a one-minute step. Separate proof describes artifact-readability recovery;
it never changes a native failure to success or resumes measurement. The step
uses one minute of the original two-minute margin; native and upload budgets,
seed counts, workload, caps and performance thresholds remain unchanged.

These corrections are local only, independently reviewed, and have not been
pushed or qualified on Linux. No additional workflow dispatch or rebuild was made.

## Verification and scope

The published milestone passed the complete pre-push script, security scanners,
160 Python tests, actionlint and independent helper/workflow review. After the
postmortem corrections, 163 Python tests pass (149 diagnostic, five trace
qualification, nine restore), actionlint and diff checks pass, and controls on the
actual inline recovery code verify bounded scope and refusal of directory
symlinks. An initial sandboxed rerun could not invoke ps in an existing process
containment test; the access-qualified rerun supplies its valid result.
No Rust source changed in the postmortem corrections; no new Criterion or Linux
instruction-count result is claimed for them. The earlier full Rust verification
and scanner logs are retained with the milestone evidence.

Branch `wip-stream-recorder-smoke` continues diagnostic foundation `f75e34f`;
phase-2 remains at `f2a798413b0575abd6416d23a16f417a45aab70b`. Commits `1ce2be1`
(security prerequisite) and `6dbea99` (recorder-first workflow) preceded the local
corrections. Historical benchmark sources and executables remain distinct from
current tooling/dependency revisions. No global ledger finding, spec revision,
production requirement, waiver or implementation change was added. Root-cause
analysis and artifact retention fixes are within the existing helper/workflow
scope. Task-owned build cleanup removed 6.3 GiB logical data; branches and ignored
evidence are preserved when the worktree is removed.

## Handoff

Evidence is in the root checkout's ignored
`.agents/verification/stream-recorder-smoke-2026-10-28/`, including the complete
workflow log, artifact index, recorder artifact/audit, failing/passing observer
controls, fixture-equivalence log and ownership-recovery controls.

A future explicitly approved diagnostic milestone must first validate these
local corrections on Linux and retain the complete failed or successful prefix.
Acceptance: all unchanged source/runtime/recorder requirements, deadlines and caps
pass, or an auditable inconclusive result is uploaded. The old Durable regression
is closed only by an eligible balanced comparison and its independent analysis;
it is not closed by this report. Both follow-ups remain open. No rerun or
production implementation rewrite is authorized by this handoff.
