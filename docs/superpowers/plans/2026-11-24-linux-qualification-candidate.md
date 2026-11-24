# Safe public-base qualification implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans. Execute sequential tasks with independent spec and quality reviews.

**Goal:** Prepare a locally verified recorder-only candidate on merged PR98 without publishing or dispatching it.

**Architecture:** Selected reviewed Python blobs and a narrow workflow delta, plus the separately approved STO-25 prerequisite patch. Historical provenance stays immutable; current receipts bind clean candidate HEAD.

**Tech Stack:** Rust1.100.0, Python3.12, Criterion0.5.1, existing public xtask/nextest/security tools.

## Task 1: Exact assembly and baseline checkpoint

**Files:** The35 paths in donor68383f3 scripts/perf_trace_linux_qualification.py ALLOWLIST, plus this design/plan; crates/prkdb-core/tests/wal_batch_index_interval.rs; crates/prkdb-core/benches/batching_bench.rs. No production WAL edit yet.

- [x] Record clean public basec34f26e; run existing public Python tests and fmt without changing source. Write handoff immediately and inspect competing claims. No other worktree edits.
- [x] Extract donor ALLOWLIST blobs with Python subprocess binary stdout except workflow. Apply `git diff 9c4345a 68383f3 -- .github/workflows/remediation-gate.yml` to current workflow (not whole overwrite), verify both ordinary Rust pins remain1.100.0.
- [x] Copy the exact reviewed WAL test/benchmark blobs from0126808; do not copy old Cargo/pin/global docs.
- [x] Add candidate closure/pin/advisory workflow contracts to existing qualification test first, observe RED at35-file driver, then add precisely this design and plan to ALLOWLIST and adjust prior exact count35 to37. Keep original26-row manifest and four-file overlay bytes immutable. Verify negative controls still discriminate.
- [x] Run complete Python discovery with -B and process-list access. Run both WAL regressions on1.100 and retain expected RED before production edit. Preserve full read/checksum/reopen validation.
- [x] Sign baseline checkpoint, verify signature, record full SHA, compiler, unchanged production hash. Build `cargo bench -p prkdb-core --bench batching_bench --no-run --message-format=json` in own target with CARGO_INCREMENTAL0; collect unique executable via authoritative CargoJSON; retain exact binary/source before production edit.

## Task 2: Exact prerequisite correction and paired performance

**Files:** crates/prkdb-core/src/wal/log_segment.rs; docs/remediation/ledger.toml; docs/superpowers/specs/2026-09-23-root-cause-remediation-design.md; generated docs/status/remediation.md; copy prior WAL design/plan for evidence without adding them to executed Python closure.

- [x] Copy ONLY exact reviewed log_segment.rs blob from0126808 after asserting current file equals public9c/c34 baseline. Preserve sync/error/lock/cursor semantics and existing deadline. Regression tests must now pass; run existing core replication integration unchanged.
- [x] Build fixed benchmark via CargoJSON, retain binary/compiler/source hashes. Confirm same compiled-input identities except log_segment.rs. No compiler/build active during measurement.
- [x] Set own CRITERION_HOME and run retained prebuilt baseline then fixed back-to-back: `--bench '^wal/(batch_append/batch_100|sparse_index_durable/batch_100_short)$' --save-baseline before` then same regex `--baseline before`. Full defaults100samples; inspect nonempty before/new benchmark.json/estimates.json/sample.json and change estimates. No test-mode receipt accepted. Record pgrep rustc at boundaries, exact means/deltas/CIs/modeDurable/baseSHA. Local Mac wall clock only.
- [x] Sign `fix: preserve WAL batch sparse-index intervals on Rust 1.100` with named benchmark/fullbaseSHA/Durable/deltas in body, no attribution. Narrowly apply STO25/spec revision12/13 deltas only if unchanged global ancestor matches; update changes to new fixingSHA, generated status through xtask. Preserve all PR98 policy/docs.

## Task 3: Complete local acceptance and review

**Files:** Scope above plus acceptance docs only. For any required correction outside this scope STOP/report first.

- [x] Full `bash scripts/pre-push-check.sh` with own explicit target/incremental0 and pinned1.100, jobs2/debug0. Require fmt, all-target workspaceClippy-Dwarnings, workspace nextest, docs, Durable200seeds, ledger/render, repo status/README/doc claims. Report actualcounts, skips and missing public-only capabilities honestly; no invented Fast/privatechecks.
- [x] Run all Python tests and check_bench_harness.sh. Compare actionlint diagnostics baseline/final by file/kind/message (no additions). Run cargo audit and cargo deny check per repository policy; no waiver.
- [ ] Independent spec review then code-quality review against actual diff, workflow graph, source closure, security prerequisites, counter semantics, performance receipts and test evidence. Resolve scoped findings and re-review.
- [ ] Sign acceptance docs, verify all new signatures/public-only ancestry/allowed diff/currentmanifest hashes; package actual cleanHEAD37files, execute real env-i ownership interpreter --help prefix and verify zero caches/source drift before and after.
- [ ] Retain exact source/log/binaries/hashes/reviews/counts in root .agents/verification/linux-qualification-candidate-2026-11-24 and update handoff every commit. Clean only own target when no longer needed.
- [ ] Present exacthead, onepush/onedispatch inputs(probe stream-framed-qualification, phase2, ref=fullHEAD, selectedworkflowref=newbranch),35min/10min limits and actualartifactnames for approval. STOP before publication or execution; Linuxroot/perf and originalprivateDurableholds remain outstanding.

## Fresh local evidence and performance hold

The clean pre-fix benchmark checkpoint is e7caa5f85642780593791fb58e2ca03bb6edd9e8. Both regressions fail on its unchanged public production, then pass after exact reviewed patch2cc8a3c0b4b680e7179e6b1096575129361936d8; unchanged replication integration4/4 also passes. Production correction preserves required WAL/index sync and errors. Initial command used a nonexistent test target and ran no tests; the corrected actual target is replication_integration_tests.

Both retained release binaries use actual rustc1.100.0(0ef1bc386), Cargo1.100.0, identical425input identities except log_segment.rs. Full-default paired Criterion with100samples and no active rustc at boundaries gives:

| Durable local Mac benchmark | Before mean | After mean | Change (95% CI) |
| --- | --- | --- | --- |
| wal/sparse_index_durable/batch_100_short |133.97283584ms|14.25168596ms|-89.3623%(-89.5344..-89.1786%)|
| wal/batch_append/batch_100 control |244.79456328ms|288.14707793ms|+17.7098%(+11.4372..+24.6638%)|

The control regression is **unresolved**, independently reviewed and retained. It is not dismissed as noise or offset by the short-record improvement. No repeated benchmark, lowered floor, correctness exception, perf_note waiver or Linux instruction-count result. Both control variants have records exceeding the1024byte interval, so source predicts the same100index syncs plus1final WAL sync. The patch adds atomic writes; the observed slowdown has no measured atomic/IO attribution. These are source observations, not a causal runtime diagnosis.

Readiness remains implementing due to this performance hold. Completing native/security/source-package checks establishes only those prerequisites. The recorder-only Linux workflow does not measure the database and cannot resolve this hold. Before any additional hot-path design or runtime attribution, stop for a separately reviewed bounded diagnostic scope retaining both binaries, workloads, durability and thresholds. No push, dispatch or merge from this checkpoint.

## Native and policy acceptance receipt

Complete Rust1.100 pre-push exits0: fmt, workspace/all-targetClippy-Dwarnings, nextest1004PASS/10existing skips(56.981s), doctests71PASS/14existing ignores, blockingDurable200seeds/55408checks, ledgercheck/render, repo-statusdrift, README36examples/docclaims. Independent source/spec and documentation reviews found no consequential scope or preservation defect. All288Python tests pass; actionlint retains54existingdiagnostics withzeroadditions; cargoaudit andfullcargodenycheck pass under unchanged policy. Benchharnessguard anddiffcheck pass. Fastmode/privatePhase2single-WAL/perffloor scripts are absent from this public source and are not claimed.

Final clean-head source/package/signature receipts and detailed raw logs live in the local handoff/evidence record, outside shared source. Source-only acceptance documentation follows measured production2cc8a3c; no runtime or benchmark change after measurement. Final sourceclosure must still be rehashed at the signed head.

STOP: the control regression remains unresolved. Native/security correctness checks do not qualify performance or authorize publication. The next proposed task is a separately reviewed attribution design using retained baseline/fixed binaries, unchanged workloads and durability, sync latency plus CPU/instruction evidence on a suitable environment. No new runtime measurement or patch design was executed. The original recorder-only Linux and private production holds remain open.
