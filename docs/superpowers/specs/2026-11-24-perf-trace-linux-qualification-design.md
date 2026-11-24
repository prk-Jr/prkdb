# Recorder-only Linux qualification after framed trace integration

Date: 2026-11-24 (IST). Status: proposed; implementation and execution require the bounded approvals below.

## Problem and next checkpoint

The completed offline integration at `67ee4850e64257f185cf31e1b92bf2193bbcaec3`
fixes physical-line framing of Linux 7 dynamic syscall payloads. It has 345 passing
Python tests, independent spec/code reviews and a complete pinned Phase 2 local
gate. Those are historical results for that exact branch, not fresh Linux evidence.

The last finite Linux attempt, run 40854574502, failed syscall interpretation
before any database invocation. CPU qualification passed; scheduler qualification
was not attempted. Successful ownership recovery during that parser failure did
not test rescue after a finalization deadline or a genuinely unreadable prefix.

This successor qualifies recorders and those privilege/cleanup boundaries only.
The hosted probe must never build, restore, launch or benchmark prkdb. Ordinary
local repository verification is required separately and may build/test the public
workspace and exercise its database correctness harnesses; those checks are not
part of the hosted probe or performance evidence. A successful hosted run still does
not resolve the original Durable throughput/tail-latency hold or make a production
candidate eligible.

Authority remains the root remediation spec (D6, D9/D10, §5–7), plan Conventions,
and AGENTS.md. This design creates no exception to publication restrictions.

## Chosen approach and alternatives

Recommended: retain the exact reviewed Python-only integration, add a manual-only
recorder/cleanup probe, and stop after its evidence is audited. This gives a small
qualification result without mixing new measurements with tooling acceptance.

Reusing the old conditional recorder-plus-database workflow would couple success
to an unapproved historical executable restore and database schedule. Publishing
the entire private diagnostic or Phase 2 branch would also expose unrelated source
and inherit held experimental changes. Neither is part of this successor.

A local Linux substitute would help develop controls, but this Mac cannot establish
the actual hosted kernel/perf/root boundary. Offline tests remain prerequisites,
not a substitute for the single separately approved hosted execution.

## Source isolation and publication boundary

The documentation task is `wip-perf-trace-linux-qualification`, based on Phase 2
`f2a798413b0575abd6416d23a16f417a45aab70b`; it stays local. No other task is taken
over, no phase integration is performed, and existing worktrees remain untouched.

After approval of this written design, implementation uses a dedicated local
execution branch based on the current public main SHA, recorded before edits.
Extract selected Git blobs into that branch; do not merge or cherry-pick private
history. Its ancestry must contain only public main and the new scoped commits.
Do not publish the documentation task or the private integration branch.

Import exactly the five Python diagnostic modules, four decoder/integration
modules, their nine test modules and the seven perf_trace data/provenance files
from 67ee485, plus its integration-donors.json. Record the exact path list, Git
blob identity, SHA256 and donor SHA in a new qualification source manifest. The
five diagnostic modules are stream_durable_diagnostic, stream_durable_restore,
wal_descriptor_diagnostic, wal_stall_diagnostic and wal_trace_qualification. The
four framing modules are perf_trace_format, perf_trace_records, perf_trace_decode
and perf_trace_integration. Tests have corresponding test_ names.

stream_durable_restore is retained only as an import/test prerequisite; its
restore CLI and database commands are forbidden in the new workflow. Existing
source-build manifests and original capture archives are never rewritten.
No Rust, Cargo, dependency, pin, benchmark, global ledger or generated status edit
is permitted in this task. The public CI Rust upgrade remains separate; this
Python-only qualification does not change Phase 2's Rust 1.98.1 requirement.

Before publication, inspect the complete tree diff and all new ancestors against
the selected public base. Only the listed Python/data, new qualification helper
and tests, this design, the later implementation plan and the narrowly extended
remediation workflow may differ. Review all selected source for open security
findings under D10. A problem there is a STOP, not authority to publish anyway.

## Manual entry point and finite schedule

Extend the existing `.github/workflows/remediation-gate.yml` in the execution
branch with a distinct `stream-framed-qualification` probe value. Existing public
probes and phase-gate behavior remain unchanged. Select the exact execution branch
as workflow_dispatch ref, and require the `ref` input to resolve to the approved
full commit SHA. Read back the run's workflow ref and checked-out SHA. The workflow
file already exists on public main; verify branch dispatch supports this scoped
input before execution. If GitHub rejects it, stop and report; do not alter main,
add a public PR, or substitute an unapproved run.

Two probe jobs form one finite workflow execution. They use ubuntu-24.04,
read-only contents permission, persist-credentials false, exact-SHA checkout,
non-overlapping evidence directories and no secrets. No automatic trigger,
retry, conditional database job or database artifact download is added.

1. Recorder job: preserve the prior 35-minute job cap, 22-minute native step,
   120-second per-command limit and 20-minute aggregate native/render budget
   (1140 seconds operation plus 60 seconds finalization). Preserve the 180-second
   preparation cutoff and eight-minute upload reserve. Run `qualify-recorders`
   exactly once; its existing CPU, syscall and scheduler loop stops at its first
   failure. No subsequent category is forced through after refusal.
2. Privilege/cleanup control job: a separate ten-minute cap, at most two minutes
   for all three fixed controls, one minute for the existing bounded upload
   ownership rescue and five minutes for upload; two minutes remain for setup.
   Run after the recorder job terminates, even if that job failed. This is a
   separately identified containment control, never a retry of a failed recorder.
   Stop this control sequence on its first unexpected result. No performance
   evidence is gathered here and no command's existing timeout is widened.

Both jobs upload their complete failed prefixes using always(), with explicit
if-no-files-found failure and 30-day retention. New implementation must verify
that the installed perf, tracing support, sudo, timeout, Python and required
system tools exist before recording. Missing tools cause an explicit failure;
no weaker replacement or implicit privileged package installation is allowed.

## Exact tooling and evidence package

An explicit manifest enumerates all executed Python modules, all selected tests,
fixtures/provenance, this design, the implementation plan and the workflow bytes.
No wildcard-only packaging is admissible: the old packaging omitted the newly
introduced perf_trace modules. Validate relative paths, uniqueness, regular-file
and symlink refusal, hash agreement and completeness before running tooling.
Archive both this current-tooling manifest and the unchanged historical donor
manifest. Compare source hashes before and after execution; source drift fails.

Record workflow/run/job IDs, event, selected ref, resolved SHA, source tree state,
kernel, perf version, Python interpreter, ordinary/root identity and resources,
CPU affinity, command arrays, monotonic deadlines, stdout/stderr/exit records,
raw captures, typed record inventories, canonical text, decoder proofs, attribute
rendering, semantic quality summaries, ownership/containment receipts and final
artifact inventory. Retain all modules invoked under privilege and the workflow
body itself. Mark all summaries diagnostic_only=true and timing_eligible=false.

Keep existing raw limits (64 MiB CPU, 128 MiB syscall/scheduler, 1 GiB aggregate),
render limits (512 MiB/file, 2 GiB aggregate), 256 MiB observer/decoder output
bounds and decoder metadata/record/event caps. No source/frame/loss/clock/CPU/
identity/lifetime/pairing/attachment check, threshold, seed or workload is relaxed.

## Recorder acceptance

Invoke the existing actual `qualify_recorders_only` path with the integrated
render_category and existing supervised helper. There are exactly three actual
chains: CPU, syscalls, scheduler. Require each to pass the unchanged native
identity, lifetime/rename, required syscall-pair or scheduler interval and
recording/clock/loss/attachment rules. CPU stays on its reviewed path.

Require complete=true, qualification_only=true, mode=recorder-qualification,
benchmark_invocations=0 and build_provenance_checked=false. qualification.json
must contain exactly three accepted category proofs. Inventory and source binding
must be complete; a successful process exit without these receipts is failure.
No offline fixture can stand in for a fresh recorder chain.

## Fixed real Linux privilege and cleanup controls

A new small qualification driver owns controls only. It may call the reviewed
supervisor/cleanup functions and select disposable fixture subprocess commands;
it must not change production analyzer functions, environment policy or deadlines.
All privileged paths are beneath one freshly created per-job evidence directory,
with validated realpath boundaries and no symlinks. No shared directory or unrelated
process may be targeted. Use controlled pipe/event blocking, never arbitrary sleeps.

1. Protected input: copy the immutable Linux7 syscall fixture byte-for-byte into
   a disposable root-owned mode-0600 file. Preserve its known input hash. Prove an
   ordinary open raises EACCES, then use the actual existing privileged decoder
   command/supervisor and verify-input receipt. Require identical input hash,
   114 syscall samples and canonical/inventory agreement with the fixture contract, plus
   unchanged bytes. This proves the protected-input helper boundary; it is not
   presented as fresh recorder or full live-analyzer qualification.
2. Deadline containment: exercise the actual ownership_cleanup path with its
   command factory selecting a disposable root fixture supervisor. That supervisor
   uses the existing owned-group witness format and blocks a child on a pipe.
   A short control deadline expires while the witnessed child is live, reserving
   the original cleanup/finalization slices. Require the real timeout/failure,
   signal/probe records for the exact witnessed group and process absence after
   bounded cleanup. Report an unknown root group as uncontained failure. Never
   fabricate success, signal an inferred unrelated PID or lengthen the deadline.
3. Failed-finalization rescue: preserve a root-owned unreadable known-content
   prefix and force the existing ownership-finalization invocation to fail with
   a controlled nonzero fixture exit. Establish ordinary EACCES before rescue.
   Run the same narrowly bounded postmortem ownership repair used for upload;
   prove complete bytes/hash inventory is now readable and no process remains.
   The native failure/result remains failed/inconclusive; the rescue receipt is
   separate and cannot turn it green. This must exercise actual root permissions
   and finalization failure, not only a mocked PermissionError.

Each case records actual uid/gid/mode, before/after hashes, command exits, deadlines
and owned process identities. Expected controlled failure is distinguished from
an unexpected control failure in a separate qualification verdict. Production
exit/result artifacts are immutable after failure except their existing finalizer;
postmortem evidence lives beside them. The control harness only succeeds when all
three expected outcomes are established and the failed prefixes are readable.

## Local implementation acceptance and independent review

After design approval, write a checkbox implementation plan with exact files,
test-first steps and commands. New files are scripts/perf_trace_linux_qualification.py
and scripts/test_perf_trace_linux_qualification.py; extend the new test module with
workflow/source-manifest contracts rather than modifying donor tests. Create
scripts/testdata/perf_trace/linux-qualification-sources.json for exact donor hashes.
Only the execution-branch remediation workflow and these new helper/test/data/docs
files may change beyond the selected immutable imports.

Tests must fail before implementation for incomplete source packaging, source
mutation, path escape/symlinks, nonzero or absent helper, malformed qualification
receipts, missing recorder category, attempted DB/restore launch, unexpected child
survival, premature success after timeout and rescue masking original failure.
Keep the 222 selected tooling tests (163 diagnostic/restore, 43 decoder and 16
actual integration tests), plus every existing public-base test. The historical
345-test private run also covered unrelated private tools; do not import those
solely to duplicate its count. Run new tests with fake OS boundaries on the Mac
and label those simulations honestly.
Actionlint must have no new diagnostics; workflow contracts must prove only the
new probe launches its two bounded jobs and no database job can follow success.

Run the full repository pre-push gate on the assembled public-base execution
branch and its declared pinned compiler before proposing a push. Its counts and
source SHA must be recorded anew, not copied from the historical private branch.
No Rust/hot-path edit means no Criterion claim or Linux instruction measurement.
Require independent implementation/spec review before exact-head approval.

## Stop conditions, approvals and outcome

Current request authorizes design preparation. Approval of this written design
authorizes local implementation and verification only. After implementation,
present the actual signed head, public-base ancestry/diff, packaged source hashes,
review and local verification results, exact dispatch inputs, limits and artifact
names. Obtain approval for that one scoped push and one workflow dispatch under
AGENTS.md and D10. Prior PR98 push permission and the exhausted October execution
permission do not authorize publishing private branches or another Linux run.

After an approved run, download both artifacts and independently rehash/reconcile
every retained file, executed tool and final result. Stop on any unexpected
qualification failure, missing prefix, uncontained process, source drift, missing
tool, unsupported perf layout or exceeded bound. No automatic rerun or parser
loosening. Report category/control outcomes and incomplete evidence explicitly.

Successful qualification means only the three live recorder chains plus the
specified root/cleanup controls passed at that exact tooling/kernel/perf identity.
It does not qualify source/build reuse or database timing. Historical executable
restore and the fixed 16-invocation A/B diagnostic schedule, if still needed,
require their own later evidence review and finite execution authorization.

## Tracking and follow-ups

No global finding status, perf_note, ledger evidence or authoritative spec revision
changes in this tooling task. Preserve the original Durable hold, consumer/index
holds and the unpublished task ownership table.

- Exact tooling/live recorders -> this bounded qualification; acceptance above;
  open until one approved run and independent artifact audit.
- Root timeout/EACCES rescue -> the three fixed controls above; open until real
  Linux evidence, irrespective of offline tests.
- Durable production performance -> existing Task 2.15b performance/consumer
  programme; unchanged paired correctness/performance acceptance, no waiver.
