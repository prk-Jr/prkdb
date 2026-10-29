# TST-09: same-harness instruction measurement repair

The maintainer approved the focused proposal after PR90's Rust upgrade failed
its unchanged5% instruction gate (222→519 for the old indexed-insert benchmark).
This does not establish a2.34x full-operation slowdown. The older source still
has HIGH/open TST-09; nested async poll symbols can match the default wrapper
collection toggle and exclude the operation. Preserve the existing failure and
prove coverage before interpreting new counts. Production code remains untouched.

## Measurement contract

Four renamed async benchmarks keep the original workloads:100puts, one hit get,
100-entry put_batch and one indexed insert, all with1KiB values. Setup, canary
configuration, validation and fixture destruction stay outside instrumentation.
Explicit whole-process client requests replace the wildcard toggle; all runtime
threads are counted. WAL fixtures keep the old single-worker multi-thread runtime
because the old adapter requires block_in_place. The memory/index fixture uses a
current-thread runtime to avoid irrelevant worker startup, identically on both
sources. Pure LogRecord encode/decode references are unchanged.

A noinline free canary performs0or4096 black-box-dependent volatile updates inside
the actual operation body. Corrected measurements must show at least4096additional
instructions on the main thread AND all-thread total for every async workload.
Canary results are checked, and exact storage/read/index postconditions run after
instrumentation stops. Deliberately skipped operations must fail their native
postconditions. Native constructors disable Valgrind requests solely for portable
operation tests; benchmark from_env always enables requests, with an explicit
setup assertion and no environment setting to disable them. Native tests are not
instruction-count evidence. Gungraun clears subprocess environments by default:
fixed and legacy configs must explicitly forward exactly the two controls.
Benchmark setup refuses missing values and emits effective child-control receipts
outside instrumentation. The runner requires four matching receipts per case and
forwards child stderr for their retention. Portable native new/parse defaults are
not used to infer benchmark controls. Standalone execution must explicitly set
PRKDB_PERF_CANARY=0 and PRKDB_PERF_SKIP_OPERATION=0 (or the fixed canary 4096).

The legacy canary reproduction derives from benchmark bytes matching the frozen
55fcfa1 SHA256, using the baseline bytes already loaded by the runner; native
protocol tests use an identical tracked fixture so a future squash/delete cannot
make them depend on an unavailable historical Git object. It
retains its original async bodies/default toggle, and injects the same checked
canary. It runs separately from fixed and normal baselines. If it already counts
the index canary fully, do not claim this mechanism explains the exact index
counter change. On later baselines already using the repaired named regions,
the obsolete frozen legacy reproduction is skipped; fixed controls remain mandatory
on both sources. Unknown measurement definitions fail closed. At least one legacy async region must demonstrate excluded work
before this protocol can claim to reproduce TST-09. Exact causal attribution and
full-operation performance remain separate conclusions.

## Source and dependency provenance

Use separate base/head worktrees and their actual compiler pins. Apply identical
reviewed benchmark/support/native-contract code plus the client_requests dev
feature to both. Freeze only the reviewed dev dependency records, independently of the full head
lockfile. Overlay those necessary records; preserve
each source's original production dependency versions. Record source SHAs,
unchanged-source hashes, overlay hashes, compilers, executables and raw reports.
Do not claim the overlaid trees are pristine commits. New measured IDs require
matching positive baseline/head summaries; no new-benchmark bootstrap escape.

Cargo metadata globally unions dev/build features, so its feature list alone can
change even when production compilation does not. A real control found only the
dev bindgen prettyplease/verbatim union changed. The guard therefore compares
normal/build package-version/edge graphs AND exact normal/build cargo tree feature
contexts before/after each overlay; it does not discard actual production feature
changes. Real clean2a7dcdb and55fcfa1 controls resolve with --locked and pass these
guards. Manifest/lockfile/path allowlists reject unrelated changes.

Raw per-thread files, identities and totals must exactly match every JSON part,
and their sum must match the summary total. Runner 0.19.4 copies temporary output
files without rewriting summary paths; resolve only its exact temporary-directory
convention in the same summary directory, then perform every identity/count check.
Real archived two- and three-thread Linux profiles validate this behavior. Missing, zero, duplicated, symlinked,
unmatched or unaccounted evidence fails closed. Idle zero-count worker parts are
retained; the benchmark thread and aggregate must be positive. Use raw totals
before summary because client-request summary lines have a documented Valgrind
bug. Strict arithmetic uses head*100 > base*105 and must agree with gungraun's
existing ir=5% exit verdict. Any real regression remains blocking; no perf_note
is added.

## Execution and retention

The existing45-minute job cap stays. Setup must leave its fixed reserve (at most
240seconds before launch); the native step is35minutes, runner budget34minutes,
and always-upload step5minutes. A timed-out command's process group is killed;
any failure produces an inconclusive result. Each case sets TMPDIR inside its uploaded folder so live raw files survive a
killed runner before the normal output copy. Every available raw prefix and binary
candidate is copied to an uploaded case directory in finally BEFORE parsing or
qualification, preserving the original error. Canary outputs and baseline files
are isolated from normal comparisons. Do not overwrite an existing evidence root,
reuse stale measurement state, retry opportunistically or widen limits.

One reviewed milestone push starts the existing Linux job. The raw/summary/canary
and source evidence must be independently audited after it finishes. Actual Linux
coverage and eligible comparisons are still unmeasured locally. Keep TST-09 in
progress until that qualification; record closure with the audited evidence at
the subsequent integration/tracking milestone rather than prematurely claiming
fixed. User merges only after required CI and review pass.

## Files and checks

Scope: bench/support/native-contract files; gungraun dev feature and necessary
Cargo.lock closure; strict Python guards/runner and tests; performance workflow;
TST-09 ledger/spec revision and generated status pages; this design/plan/report.
No production storage/index/WAL code, check threshold, mutation coverage or security
policy change. Local required checks include native RED/GREEN, Python RED/GREEN,
actual-source overlay controls, both harness variants' no-run compile, full
pre-push and scanners, then independent reviews. Linux alone establishes counters.

## First bounded Linux outcome

Run 40838719175 at a859c4c is inconclusive: controls were cleared before reaching
benchmark children, so both nominal cases used zero. The sensitivity gate blocked
qualification. No legacy-exclusion proof, normal base/head comparison or candidate
performance verdict exists. Standard CI passed 30 jobs. The reviewed correction
above preserves the protocol; a further push/job needs approval because the first
execution boundary allowed one. No blind retry or budget/floor change is permitted.
