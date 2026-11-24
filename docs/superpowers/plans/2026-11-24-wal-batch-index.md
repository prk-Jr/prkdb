> Historical prerequisite document from the Rust1.99.1 local fix at0126808. Its source/compiler/results describe that branch only. Current Rust1.100 assembly, new performance concern and acceptance are recorded in `docs/superpowers/plans/2026-11-24-linux-qualification-candidate.md`; no historical receipt is relabelled.

# WAL batch index accounting implementation plan

> **For agentic workers:** Use executing-plans or subagent-driven development task by task; preserve repository authorization boundaries.

**Goal:** Correct STO-25 without weakening sync/error checks and retain genuine before/after performance evidence.

**Architecture:** Per-record interval accounting matching append, exact existing regression and meaningful fresh-segment Criterion case. Public9c-derived isolated branch; no privatePhase2 adoption.

**Tech stack:** Rust1.99.1, Criterion0.5.1, existing xtask/nextest. Base9d2adae.

## Task 1: Regressions, finding claim and pre-fix benchmark

**Files:** regression integrationtest, batching_bench.rs, design/plan, ledger/spec row.

- [x] Verify prior exact RED source and clean branch, claim STO-25 across all branch IDs and revision12 locally.
- [x] Add any mixed single/batch remainder regression, run new targeted test before fix and observe expected mismatch; preserve existing122record checks.
- [x] Add bench wal/sparse_index_durable/batch_100_short with fresh segment/100shortrecords/interval1024. Use iter_batched setup and output tuple(segment,tempdir,offset), BatchSize::PerIteration. Keep existing batch100case unchanged as control.
- [x] Sign benchmark/regression/openfinding checkpoint; record full baseline SHA, pinned compiler and mode. Do not edit production before retained baseline executable exists.
- [x] Set own CARGO_TARGET_DIR and CARGO_INCREMENTAL0, conservative jobs2/debug0; release only for mandatory Criterion. Build `cargo bench -p prkdb-core --bench batching_bench --no-run --message-format=json`, collect unique compiler-artifact executable, retain exact bytes/hash and baseline source.

## Task 2: Small root fix, measured paired result and signed commit

**Files:** log_segment.rs; regression only for meaningful preservation additions.

- [x] In append_batch, replace load+total_bytes interval expression by fetch_add(record_size,SeqCst); reset counter only after successful index append; remove final total_bytes counter update. Preserve fileposition, final data sync, all index sync/error propagation and append implementation.
- [x] Run targeted index interval regression(s) and core WAL/replication tests; old original replication test may now run as verification of changed source, not an identical retry of unchanged code. Passing does not prove historical causality.
- [x] Build/retain fixed release benchmark executable through CargoJSON. No other agent/compiler builds during measurements.
- [x] Verify pgrep rustc empty, then run prebuilt baseline/fixed executables back-to-back with full defaults and regex selecting `wal/batch_append/batch_100` plus `wal/sparse_index_durable/batch_100_short`. Baseline save and fixed compare under same own target. Retain raw stdout/stderr/exit and CriterionJSON; report estimates/deltas without false Linux instruction counts.

Exact paired executable procedure (retained paths recorded from Cargo artifact JSON):

```bash
export CARGO_INCREMENTAL=0
export CARGO_TARGET_DIR="$(git rev-parse --show-toplevel)/target"
export CRITERION_HOME="$CARGO_TARGET_DIR/criterion-sto25"
"$STO25_BEFORE_BIN" --bench '^wal/(batch_append/batch_100|sparse_index_durable/batch_100_short)$' --save-baseline before
"$STO25_AFTER_BIN" --bench '^wal/(batch_append/batch_100|sparse_index_durable/batch_100_short)$' --baseline before
```

The explicit --bench is mandatory: Criterion0.5.1 otherwise enters test mode. Require both successful exits and nonempty before/new estimates.json, benchmark.json and sample.json for both IDs; inspect raw measurement/warmup/sample output. Retain change/estimates.json where produced. Empty JSON inventory or a test-only exit is not performance evidence. No default samples/duration reduction.

- [x] Independent review of sync/error/concurrency behavior and metric input reach. Fix scoped defects test-first; stop on a required wider design.
- [x] Sign `fix: preserve WAL batch sparse-index intervals` with named benchmark, before/after delta, full baselineSHA and Durable mode in body; verify signature. No attribution trailer/nohistoryrewrite.

## Task 3: Finding update, complete verification and handoff

**Files:** ledger/spec status, generated status pages, acceptance docs.

- [x] Mark STO-25 fixed with exact tests and signedfixSHA; root spec claim remains evidence-backed, no verified/CI/performance waiver. Render generated status viaxtask; never edit generated files manually.
- [x] Run full required pre-push gate and applicable guards at fixedsource. Report any newfailure; no retries/sleeps/threshold/test weakening. Explicit docs and workspace test coverage as required; native Fast/source-only scripts unavailable on this public baseline remain disclosed.
- [x] Independent final code/spec review; verify exact allowed diff, allnewsignatures, source/compiler/mode/binary receipts, currentclean status. Check generated repo-status if fingerprints changed.
- [x] Sign acceptance docs/ledger checkpoint, preserve raw evidence and owncachecleanup after retaining required binaries. Report branch/worktree/base/head, commits, before/after mode, exactcheckcounts, findings/revision and remainingpublicsecurity/Linux gates. No push/PR/merge/dispatch.

## Local acceptance — 2026-11-24

Production correction signed at99a73593277c929c77dd22e792cf0ac46697a004. Pre-fix benchmark source1015ff134c30165a5773e2ae36b7700075b6bf43, same compiled-input hashes except log_segment.rs. Later signed test-only ce60959 uses fixed-size chunks with unchanged complete-entry validation; no runtime or benchmark change.

- Both deterministic interval regressions GREEN, including all122record checksums/read/reopen checks and mixed43record single/batch remainder checks. Existing replication integration4/4PASS, originaldeadlines unchanged. Historical aggregate failure attribution remainsunknown.
- Required paired Criterion on local Mac/Rust1.99.1/Durable legacy sync_all path, prebuilt executables back-to-back, no rustc at boundaries, full100samples each. Short-record batch mean138.6346021ms to14.4346459475ms(-89.58799%,95%CI-89.8503..-89.3101%). Existinglarge-record control297.58749705ms to296.52858042ms(-0.35583%,CI-3.3665..+2.7530%,nochange). Real before/new benchmark, sample andestimate JSON validated. No Linux instruction-count claim or waiver.
- Fullprepush gate passes: fmt, all-target workspaceClippy-Dwarnings, nextest1002PASS/10existing skips(45.870s), doctests71PASS/14existing ignores, blockingDurable200seeds/55408checks, ledgercheck/render, repo-statusdriftcheck, README36examples/docclaims. Benchharnessguard also passes.
- Initialfullgate caught test-only chunks_exact_to_as_chunks lint; retainedREDlog, correctedAPI withsameassertion andrerancompletegate. No suppression or unchanged-testretry.
- Independent design/plan, implementation spec and code-quality reviews Approved; tiny test-reader follow-up also Approved. Broaderlegacy shared-writer/cursor limitations remain outside this correction.
- STO25fixed locally with both tests and99a7359 in ledger; root spec revisions12(finding) and13(fix). Generatedremediationpage rendered byxtask; repo-statusrender output unchanged. No CIverified/phasegate status or performance exemption.

Readiness: local code checks pass; Linux instruction-count/phase qualification and safe-source publication remain outstanding. This preserved branch still carries the old public-base url2.5.8 dependency. PR98merge was confirmed during final verification, so the next step is fresh assembly on the merged safe public base rather than publishing this old base. Fast mode and privatePhase2 single-WAL/perf-floor scripts are absent on this publicsourcebase; no substitute/importedprivatecheck claimed. No push/PR/mainintegration/privatePhase2merge/dispatch or new hosted measurement.

PR98merge receipt: merged2026-11-24T15:15:54Z; sourcehead5be1fc3. This agent did not merge it. Existing measured source and compiler evidence remain Rust1.99.1 and are not relabelled as Rust1.100 qualification.
