# Recorder-only Linux qualification implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox syntax for tracking.

**Goal:** Prepare a verified, reviewable manual Linux qualification at a public-main-based head, without publishing or executing it.

**Architecture:** Exact selected Python blobs from67ee485 plus one qualification driver/test module and an isolated remediation-gate probe. The driver binds all source/evidence bytes and exercises existing real Linux privilege/cleanup functions with disposable fixture commands; it never launches a database. Existing analyzer/decoder/donor implementations remain byte-identical.

**Tech stack:** Python standard library, existing root supervisor, perf and GitHub Actions; public base9c4345a0636a7aaca08f3b9a28c9a12e3a0a445b Rust1.99.1 for ordinary local full verification only. Phase2Rust1.98.1 stays unchanged.

**Approved design:** docs/superpowers/specs/2026-11-24-perf-trace-linux-qualification-design.md, approved by maintainer on2026-11-24 via Approve local implementation. Publication and dispatch are not authorized.

## Task 1: Isolated exact-source assembly

**Files:** Import the five diagnostic/restore modules, four framing modules, corresponding nine test modules and seven perf_trace data files from67ee4850e64257f185cf31e1b92bf2193bbcaec3; include integration-donors.json. Copy approved design and this plan only. Create scripts/testdata/perf_trace/linux-qualification-sources.json. Handoff wip-perf-trace-linux-execution records all exact paths.

- [x] Verify public main still equals9c4345a, inspect worktree/branch/handoff ownership, then create wip-perf-trace-linux-execution outside root from that exact public commit. Do not cherry-pick/merge private commits.
- [x] Explicitly enumerate nine modules: stream_durable_diagnostic, stream_durable_restore, wal_descriptor_diagnostic, wal_stall_diagnostic, wal_trace_qualification, perf_trace_format, perf_trace_records, perf_trace_decode, perf_trace_integration; import corresponding test_ files.
- [x] Import seven data files: README.md, provenance.json, linux7-syscalls.perf, linux617-syscalls.perf, linux617-scheduler.perf, linux617-syscalls.perf.expected.json, linux617-scheduler.perf.expected.json; import integration-donors.json separately. Extract each Git blob with subprocess binary stdout, not shell interpolation.
- [x] Record exact path/donor/blob SHA/SHA256/bytes per26imported files. New source manifest version1, public_base, tooling_donor and files map. Do not claim it binds its own content hash or reuse historical build manifests as current evidence.
- [x] Run222selected tests using unittest explicit nine module names with PYTHONPATH=scripts and PYTHONDONTWRITEBYTECODE=1; allow process-list permission for unchanged containment tests. No source fixture or count weakening. Record baseline logs.
- [x] Commit signed `chore: assemble exact recorder qualification tooling`; verify signature and update handoff. This mechanical assembly has no executable change to the selected modules.

## Task 2: Qualification driver and workflow (one coherent implementation)

**Files:** Create scripts/perf_trace_linux_qualification.py, scripts/test_perf_trace_linux_qualification.py. Modify only .github/workflows/remediation-gate.yml beyond Task1 imports/docs. Existing selected modules/tests and all Rust/build inputs immutable.

### Source package and receipts

- [x] Write failing tests for `package_tooling(repo, output)` and `verify_tooling(repo, receipt)` before implementation: reject missing/extra source manifest keys, duplicate/escaping/absolute paths, symlinks including ancestor links, nonregular/oversized inputs, altered donor hashes, absent runtime modules and post-execution source drift. Reject output that exists or aliases source. No partial success receipt.
- [x] Driver's fixed allowlist includes26selected imports plus this driver, its test, the new source manifest, exact workflow, approved design and implementation plan. Source manifest entries bind imports; packaging records SHA256/bytes for every current file. Explicitly list/verify all runtime imports; ordinary shared public workflow files need not be copied unless executed by this probe. Do not recursively collect unrelated repository files.
- [x] Copy exact bytes exclusively beneath tooling/, retain commit/event/run/job provenance and clean-tree check, write versioned receipt and validate it before native work and after native work. Bound ordinary file reads and all JSON/control output. Fail on unsupported schema or source mutation. Archival inventory excludes its own self-hash and is deterministically reconciled.
- [x] A `check-recorders` command reads bounded JSON from recorder output and requires exact category set, actual successful category flags, complete/qualification-only/mode, zero benchmark_invocations and false build_provenance_checked. Reconcile hashes/inventory and reject changed source before success.

### Real-root fixture controls

- [x] Write failing unit tests (fake only root/perf/OS boundary) for all three fixed cases and for unexpected child survival, missing witness, wronguid/mode, no EACCES, digest drift, helper failure, wrong receipt, rescue failure and native-result masking. Test real CLI subprocess protocol locally where possible. Preserve original donor tests unchanged.
- [x] Implement `controls --output <fresh dir>` requiring Linux, ordinary nonroot UID, sudo-n, timeout, Python and existing required utilities. Generate fresh validated directories and fixed command arrays; never interpolate commands or permit arbitrary control command/path input. Fixed control order: protected input, timeout containment, failed-finalization rescue. Stop first unexpected result.
- [x] Protected input: known immutable linux7-syscalls.perf copy via scoped root fixture subcommand, rootuid/mode0600; verify ordinary EACCES; invoke actual root_command/execute+integration CLI and verify-input under existing supervisor. Bind knownbytes/hash/114samples to fixture; fixture evidence is explicitly not live recording qualification. Preserve before/after raw bytes and process receipts.
- [x] Deadline case: call real ownership_cleanup with only its command factory temporarily selecting a fixed root fixture supervisor. The fixture imports legacy.supervise, writes actual root-group.json, and blocks its child on an inherited pipe/event until killed. Deadline is within120secondtotal control cap, with original15second finalization/containment reserves; do not widen production deadlines. Assert timeout/failure, exact known-group signal/probe and no remaining child. Restore monkeypatched command factory even on exceptions; do not change analyzer/supervisor policies.
- [x] Rescue case: create known-content root-owned0600 prefix, force actual ownership cleanup invocation nonzero with a fixed disposable fixture exit; run existing finalization flow far enough to retain failed/inconclusive result and establish actual ordinary EACCES. Invoke the bounded fixed upload ownership repair, preserve original native/result failure, retain separate rescue receipt and reconcile all ordinary-readable bytes/hashes. Verify no remaining owned process. Do not synthesize a passing native result.
- [x] Add fixed `root-fixture` CLI modes only for the above commands, gated rootuid and validated path/owner/deadline; no arbitraryexec or external path option. Reuse legacy owned-group witness and identity/resource proof. No sleeps, retries or new DB target.
- [x] Controls have a separate receipt: diagnostic_only=true, timing_eligible=false, benchmark_invocations=0; native failure remains separate. Use real monotonic deadline/boundedsubprocesses throughout, preserve command/stdout/stderr/exit prefixes on refusal.
- [x] Implement shared `rescue-upload --output <validated evidence>` with exact bounded sudo timeout30 chown -R -h -P --preserve-root uid:gid -- path and35s subprocess timeout, never change native result. Artifact-root path validation and symlink refusal must precede privilege. It is upload preparation only; it cannot make failed recorder/control jobs pass.

### Manual workflow isolation

- [x] First write workflow contracts in the new test file: new probe only dispatch, existing phase behavior unchanged, all DB ledger/harness jobs skip the new probe, two bounded sequential jobs, always-upload/failure semantics, exact ref checkout, source manifest retained, no restore/build/DB step, no full diagnostic run or source mutation.
- [x] Add probe choice `none` (default) and `stream-framed-qualification`; no existing probe behavior/input removed. Resolver keeps existing default behavior, but new probe requires inputs.ref is a40hexfullSHA equal HEAD and selected workflow_ref ref resolves to same approved HEAD. Output that resolvedSHA, and include unmistakable diagnostic run-name.
- [x] Guard existing ledger-and-tests and harness jobs with probe != stream-framed-qualification, preserving original phase conditions. Guard harness-result with always() && probe != newprobe, preserving original dependency/result checks. Do not let an irrelevant green Harness result imply a real harness run.
- [x] Recorder job only newprobe: needs resolve, ubuntu24.04,35min, PYTHONDONTWRITEBYTECODE1, fixedTOKIO4, checkout exactSHA persistfalse; firststeprecord monotonic start; package+tests finish within180s. `qualify-recorders` once,22min step, existing1200aggregate. Validate result/source receipts. Always bounded rescue and eightminute upload with missingartifacterror/30days.
- [x] Controls job only newprobe: needs resolve and recorder; if always() && newprobe && resolver success;10min. Checkout exactSHA persistfalse; newfresh package output. Run fixed threecontrols once with two-minute aggregate control cap; always one-minute rescue and five-minute upload. No artefact restore or database following success. Capture actual step exits and preserve native failure even when postmortem upload succeeds.
- [x] Keep global contentsread, existing nongate behavior and compiler unchanged. No native build, perf installation, secrets or broad permission added by new jobs.
- [x] Run new tests RED/GREEN; then222selected tests plus complete public Python discovery. Compare actionlint baseline vs final diagnostics by file/kind/message; no newly introduced diagnostic permitted. Validate yaml and everyembedded script using the contract tests. Parent review must inspect actual workflow graph and real control code, not only test assertions.
- [x] Signed commit `feat: prepare bounded Linux recorder qualification`; update handoff with exact new head, test counts, sources and known Linux limitations. No push.

## Task 3: Independent reviews and complete local gate

**Files:** Only correct approved new module/test/workflow/docs if reviewers identify consequential defects. Update this plan acceptance and handoff/evidence. Import hashes must remain unchanged.

- [x] Independent spec compliance review of final diff against approved design. Fix and re-review every consequential issue.
- [ ] After spec compliance, independent code-quality review of final implementation and failure/cleanup/test evidence. Fix and re-review; preserve RED controls for any correction.
- [ ] Set CARGO_INCREMENTAL=0 and explicit own CARGO_TARGET_DIR from execution-worktree root; use existingRust1.99.1, conservative buildjobs/debug0. Run `bash scripts/pre-push-check.sh` tocompletion; workspacefmt/clippy/nextest/docs/Durable200seeds/ledger/render/repostatus/readme/docclaims mandatory. Record counts and toolversions. No weaker substitute for missingtool.
- [ ] Run Fast200seedcorrectness harness too if supported by public-base command, retaining unchanged seeds/checks. Full local test gate may exercise DB correctness; this is not hosted recorder evidence or benchmark timing.
- [ ] Rehash26immutableimports, compare all Rust/Cargo/pin files byte-for-byte to publicbase, inspect whole ancestry/diffallowlist, gitdiffcheck, verify everynewcommit signature. No globalledger/generatedfile change. Explicitly report anysecurityfinding before contemplatingpublication; no waiver.
- [ ] Commit acceptance docs signed; update report/handoff and retain exactsource/logs/reviews. Clean only this own target after verification/integrator review no longer needsbuild, retaining sourcebranch/worktree.
- [ ] Present concrete verifiedhead, planneddispatch inputs/jobs/artifactnames and finitebudgets to maintainer for a separate onepush/onedispatch approval. STOP here; do not invoke ghworkflowrun, gitpush, PRcreate or merge from this task.

## Execution receipt

Local implementation is retained at code checkpoint `798b3ec10cb2f7509a05e47372725287ea559796`, with public base `9c4345a0636a7aaca08f3b9a28c9a12e3a0a445b`. All 26 donor imports and 392 Rust/build inputs remain unchanged. Signed assembly/implementation/correction commits are local only.

- Final Python discovery: **270 passed** (222 selected donor tests,25 public-base tests,23 new tests),20.512s. Root-entry API and rescue ownership regressions were RED before correction. Spec compliance Approved for this local snapshot.
- Actual clean-checkout package API verified all32current files before the negative import control. This establishes local packaging only.
- **Code-quality review is blocked:** the immutable donor privileged Python launches clear PYTHONDONTWRITEBYTECODE and omit bytecode suppression. A real nonprivileged env-i import in an isolated package produced three __pycache__ files; unchanged exact package validation then refused `packaged module closure differs`. Do not ignore cache files, broaden environment policy or modify immutable donors as a workaround. Existing tests mock launch boundaries and did not catch this behavior.
- **Repository gate failed:** fmt and workspace/all-target Clippy passed; nextest stopped with789passed,1failed,10skipped,210unrun. The unchanged `prkdb-core::replication_integration_tests::test_replication_with_batch_writes` failed `AllReplicasFailed` at its source line326. No retry, added sleep/timeout, ignore or root-cause dismissal. Doctests, harness and later checks were not reached.
- **D10 publication blocked:** cargo audit --no-fetch rejects public-base url2.5.8 for RUSTSEC-2026-0592. PR98contains the fix but is not merged. No Cargo/pin/security policy change is folded into this Python-only task.
- Fast harness is unavailable on this public base (CLI explicitly supports Durable only until Phase2a); no substitute or new Fast result claimed.

Readiness: **implementing, stopped at approved scope boundary**. No Linux run, capture, dispatch, push, PR, merge or database measurement occurred. No production/performance qualification claim.

Follow-ups:
1. `wip-perf-trace-bytecode-launches` (proposed bounded successor): review a scope exception for the immutable executed Python supervisor/child-limit/scoped/smoke/fixture launch paths and updated current-source provenance. Acceptance: real cleared-environment multi-hop imports create no unexpected package files, exact validation remains strict, source/privilege/deadline checks preserved, all local regressions pass. Open, no fixing commit.
2. Existing `wip-replication-batch-test` follow-up: investigate AllReplicasFailed under controlled load and prove correct acknowledgements without wider timeouts/retries. Open; actual root cause unestablished. This issue was already carried in the PR96 handoff.
3. PR98maintainer integration, then a safe-public-base qualification assembly with new exact source audit and prescribed checks. No rewriting shared history or claiming old-source results qualify a new base. Open.
4. Fresh Linux recorder/privilege/cleanup qualification only after above prerequisites, reviews and a later exact-head execution approval. Open.

Detailed logs, exact sources, review receipts and failing-binary evidence are retained in the main checkout `.agents/verification/perf-trace-linux-execution-2026-11-24/`.
