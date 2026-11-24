# Explicit Python launch policy implementation plan

> **For agentic workers:** Use subagent-driven development or executing-plans task by task. Preserve repository authorization and verification boundaries.

**Goal:** Fix bytecode creation in frozen tooling under cleared environments without weakening source/closure validation.

**Architecture:** Add explicit interpreter -B arguments on actual Python launch producers, and retain historical donor provenance beside a narrow current-launch overlay. No root environment policy, analyzer or Rust code changes.

**Tech stack:** Python standard library and existing diagnostic supervisors. Base e4542a1, isolated wip-perf-trace-bytecode-launches. Existing actual private/public Rust checks remain separate and blocked.

## Task 1: Test-first launch and current-source correction

**Files:** scripts/{stream_durable_diagnostic,wal_descriptor_diagnostic,wal_stall_diagnostic,wal_trace_qualification,perf_trace_linux_qualification,test_perf_trace_linux_qualification}.py; scripts/testdata/perf_trace/linux-qualification-launches.json; this spec/plan.

- [x] Run unchanged270 Python baseline, retaining existing failure evidence and no root actions.
- [x] Add real env-i subprocess import tests deriving Python prefixes from actual root/supervisor/ownership/scoped commands; assert no bytecode/extras and strict package verification. Capture nested child-limit and Python-smoke arguments through the actual producer; use only boundary adapters. New tests must fail for generated files/missing -B, not missing scaffolding.
- [x] Exercise negative control without -B and prove extra-file validator still rejects it. Keep source before/after hashes. Interpreter -B precedes script/-c; no native Rust argv mutation.
- [x] Add failing provenance tests for exact historical/current binding and all malformed overlay/mutation cases in the spec.
- [x] Implement explicit -B on every declared Python producer, preserving original command tails and all resource/privilege checks. No caches removed/ignored, no added env whitelist keys, wrapper or timeout change.
- [x] Retain historical source manifest unchanged, pin its SHA256. Add version1 current overlay with exact four-file keys, base_revision and reason; rows base_blob/base_sha256/current blob/sha256/bytes. Validate against historical rows and actual current content. All other22 donor entries must still equal original bytes.
- [x] Include overlay and addendum spec/plan in explicit current allowlist. Keep clean HEAD, archive closure and source-before/after binding strict. Current receipts describe modified source as modified.
- [x] Run all Python tests, including legacy command-path/negative controls. Preserve original nine donor test files. Record RED/GREEN and actual import traces. Freeze source for independent review.
- [x] Parent inspect exact diff and arguments, historical manifest unchanged and modified modules limited to argv. Sign commit `fix: suppress bytecode in diagnostic Python launches`; verify signature and update handoff.

## Task 2: Review, scope verification and receipt

**Files:** Only corrections within Task1 files and acceptance docs/handoff.

- [x] Independent spec/code review of all actual Python launch paths, provenance and failure controls. Fix consequential issues with new regressions and rerun only affected checks.
- [x] Verify actual clean-checkout package API and cleared-environment import chain; source exactness/extra-file and negative tests remain enforced. Bound subprocess time/output; do not run root/perf/Linux actions.
- [x] Verify22 untouched donors, unchanged historical manifests,392 Rust/build inputs, all golden captures and exact allowed diff. Run docclaims, diffcheck and actionlint; no new diagnostics. Verify every new commit signature locally.
- [x] Do not rebuild/retry the known unchanged Rust test to produce green. Report existing fullgate failure, security advisory, actual scope and unrun hosted qualification alongside new Python evidence. No Criterion or instruction-count claim for Python-only edits.
- [x] Preserve source/logs/negative controls and truthful handoff with actual commit/base/head, counts, blockers and next steps. No push, PR, merge or Linux dispatch; later publication remains a separate exact-head decision after prerequisites.

## Local acceptance — 2026-11-24

Implemented at signed code commit389fc6965eb0c77fa07316fd46c0f202c18982d8, following signed planning9f8e53243929c0c0ae4d4fd22053572ed06fbe40. Independent design/plan, spec-compliance and code-quality reviews approved.

- Python3.12.6: unchanged270-test baseline passed20.038s; final277tests passed22.866s; targeted30passed2.450s. Sevennewtests include actual cleared-environment imports, scoped exec to copied Python, protected decoder import, missingflag negatives and strict original/current provenance mutation controls.
- The initial fullsuite without process-list permission hit oneexistingpsdenial; the unchanged command with required permission passed. No tests/limits changed.
- Four diagnostic ASTs are identical to base except-B list elements. Historical26-rowmanifest remains byte-identical;22otherdonors, allnine donor tests and all392Rust/buildinputs remain unchanged. Workflow unchanged.
- Real clean-checkout35-file package verified before/after actual env-i ownership-command interpreter/script import; zero pyc created, source/extra-file validation retained. This is nonprivileged local import evidence, not Linux/root recording qualification.
- Documentation claims, diff checks and unchanged-workflow inspection pass. No new native build or target was created in this task; prior fullRustgate failure and publicurladvisory were not retried or waived.

The bytecode/closure defect is locally corrected. Full qualification still requires a safe publicbase after PR98normalintegration (still OPEN at final check), prescribed native gates and separately approved exact-head Linux execution. No push/PR/merge/dispatch or DBperformance measurement occurred.

Separate investigation: signed test-only branchwip-replication-batch-test at9d2adaeb38cade3d014d6b012f48ff1b67c0dc1a confirms batchindexdrift in one deterministic RED run:4single versus80batch entries at100records;6versus102at122. Allrecords/checksums verified before/afterreopen. This does not prove historicalAllReplicasFailedwas a timeout. Productionaccountingfix, concurrency/durability review and Criterionbefore/after remain separate open work.
