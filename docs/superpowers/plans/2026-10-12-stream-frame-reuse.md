# Stream frame reuse implementation plan

> **For agentic workers:** Use superpowers:subagent-driven-development with independent spec and quality review, or executing-plans. Follow test-driven-development and isolated worktrees.

**Goal:** Eliminate duplicate hint frame I/O and CRC in one read, preserving public behavior.
**Architecture:** Constructor-controlled private owned validated-frame token binds same snapshot Arc, loc/kind/extent/length; seeded scanner visits token then continues unchanged scanner.
**Tech Stack:** Rust1.98.1, StdVfs/FaultFs, nextest, Criterion, existing Linux probes.

Spec: docs/superpowers/specs/2026-10-12-stream-frame-reuse-design.md.
Base:9b88229d29cf4ea1a384b1357139f0b0f6c7f313. Allfiles exactly as spec Files.
Set CARGO_INCREMENTAL=0 and CARGO_TARGET_DIR=$worktree/target in everyCargo shell.
Root is sole build/performance/commit/metadata owner; agents may edit only assigned
files; no parallel Cargo work. Criterion before production edits is mandatory.

### Task1: prerequisites and reviewed contract
- [x] Integrate reviewed profile infrastructure at9b88229 (fresh83Python/actionlint/diff); claim wip-2.15b.8-frame-reuse.
- [x] Read production lifecycle/snapshot proof and benchmark. Baseline core26tests passed.
- [x] Independent safety/spec/plan review; address blockers before production code.
- [x] Baseline Criterion: `cargo bench -p prkdb --bench stream_log -- stream_read/tail_100x1KiB --save-baseline frame-reuse-before`; archive stdout and target/criterion data. Expected source unchanged9b88229, Fast setup synced.
- [x] Commit reviewed contract: `docs: define validated stream frame reuse contract`.

### Task2: test-first byte and safety witnesses
Files: new prkdb/tests/stream_frame_reuse.rs, core segment.rs tests and named core testfiles.
- [x] Add public `hinted_b100_read_reuses_the_validated_frame` using real StdVfs wrapper and original input-derived exact records/cursors/watermark. Require3calls102976bytes; original4calls205911bytes must be observed as runtime RED.
- [x] Run `cargo nextest run -p prkdb --test stream_frame_reuse`; archive genuine RED and passing boundary controls. Do not change production until byte witness RED and baseline Criterion completes.
- [x] Add deterministic seed-boundary tests for snapshot Arc/captured length/cap/header/visitor break/continuation faults. For private API addition, distinguish missing-API compile evidence from actual runtime failures; prove meaningful negative controls after implementation.
- [x] Commit tests/RED checkpoint: `test: reproduce duplicate hinted stream frame reads`.

### Task3: one bounded implementation
Files: core wal/segment.rs and wal/log.rs only (tests allowed as above).
- [x] Private `ValidatedFrame` owns verified frame bytes/kind/loc/checkedend/filelen/Arc with fields private. Existing frame validator can share a private byte-returning helper with public read_frame; preserve public returns/errors. Exact Arc/filelen bound captured once.
- [x] Replace predicate validation/discard with optional token construction using current eligibility and error-to-fallback behavior.
- [x] Seeded scan verifies header before visitor, validates capturedextent, uses same capped visitor; Break returns correct advanced scan metadata without continuation read; Continue calls unchanged scan_frames at seedend/nextLSN using token Arc/length.
- [x] Run `cargo nextest run -p prkdb-core`; `cargo nextest run -p prkdb --test stream_frame_reuse --test stream_log --test stream_retention`; verify exact byteGREEN and safety controls.
- [x] Independent spec review then code-quality review. Resolve every blocker; no extra optimization/decoder rewrite.
- [x] After Criterion: `cargo bench -p prkdb --bench stream_log -- stream_read/tail_100x1KiB --baseline frame-reuse-before`. Record delta/rawdata/source/mode; no other builds during measurement.
- [x] Commit fix with exact benchmark delta: `perf: reuse validated hint frames in WAL scans`.

### Task4: completion and Linux proof
- [x] Full `bash scripts/pre-push-check.sh`, plus singleWAL/perffloors and scoped decoder fuzz only if decoder behavior actually changed. Never report absentLinux checks as passing.
- [ ] Update own handoff everycommit; ledger only for separately reproduced newfinding. Record no spec-table revision for performance-only change.
- [ ] Reviewed milestonepush, sequential `wal-bench` against base9b88229 and `iai` probe. Extend existing optionalbase_ref foriai using perf-gate nativebaseline protocol and unchanged5%gate; archive source/build/summaryprovenance and verify defaultno-basefloors unchanged. IndependentCIreview/actionlint beforedispatch.
- [ ] Download/hash/review evidence. Report allunchangedT1-T5 misses separately from newregressions; STOP if singlecorrectionfailsacceptance. No automatic merge of correction into phase2.
- [ ] Decision/report and ownedtargetcleanup after integrator no longer needsbuild. Keep source/evidence/handoff, no worktree removal before taskreview.


Checkpoint2026-10-12: contractc5257e0,test643dce1,fix4b01aeb,pairedCI43e5d3f.
ExactpublicruntimeRED4calls205911bytes ->GREEN3calls102976; allstoredfields/cursors
unchanged. PrivateAPIcompileREDseparate; two runtimepayloadidentityREDcases also
confirmedthenfixed. Finalcore213/public83PASSnofilter. Fullprepush1526PASS10skip,
concurrentworkspace+doctests,both200seed68256checks andallchecksPASS. NativeCriterion
comparison-37.633%time,+60.342%throughput; fixeduncompressedsyncedFastfixture.
IndependentSPEC/QUALITY/CIreviewsAPPROVED. Linuxpairedexecutionpending; noT3/phase
completionclaim. Newmalformedtest expectation corrected onlyafterexplicitmaintainer
approval; exactlegacyInternalerror+fullrecorddiagnostic pinned, mappingunchanged.
