# Durable append diagnostic implementation plan

> For agentic workers: use superpowers:subagent-driven-development with controlled independent file ownership and root-only integration/build coordination.

Goal: execute the approved finite diagnostic, report cause or explicit inconclusive result, retain all evidence.
Architecture: immutable original controls plus identical benchmark-only observation overlays; one isolated qualified Linux job. Rust production code stays unchanged.
Tech stack: Rust1.98.1, Python3, perf, GitHub Actions.
Spec: ../specs/2026-10-14-durable-append-diagnostic-design.md.

## Task1: transparent benchmark observations
Files: crates/prkdb/benches/wal_write_path.rs; benches/support/stream_durable_diagnostic.rs; tests/stream_durable_diagnostic_support.rs.
- [x] Add deterministic tests of monotonic records, marker/op correspondence, VFS exact delegation and failure/error/sync preservation, buffer breach prefix.
- [x] Run cargo test -p prkdb --test stream_durable_diagnostic_support for missing-feature RED.
- [x] Implement opt-in support through existing public open_with_vfs; disabled default, exactb1 only, preserve b100 execution and t0 semantics. Coordinate exported evidence schema with runner agent.
- [x] Run focused tests GREEN, fmt and strict clippy; root reviews production diff.
Commit: test: add measured-window Durable append observations.

## Task2: bounded runner/evidence qualification
Files: scripts/stream_durable_diagnostic.py and test; six copied helper/test files named in spec.
- [x] Copy latest qualified helper source from444c84b, run unchanged helper tests as baseline; never execute old schedules.
- [x] Write fail-closed source/binary/overlay/inventory/schedule/marker/resource/loss/deadline/budget/cleanup tests, run RED.
- [x] Implement manifest/run commands using fixed AABBABBA original + ABmarked + syscallAB/schedBA/cpuAB captures; qualify actual commands before timings.
- [x] Parse six ordered rows per call, attribute only qualified b1 spans; keep residual/CPU/scheduler limits.
- [x] Run unittest discover for all diagnostic helper suites GREEN.
Commit: ci: add bounded qualified Durable append diagnostic runner.

## Task3: isolated workflow and integration
Files: .github/workflows/remediation-gate.yml; task spec/plan/decision.
- [x] Add isolated dispatch choice with90minute job and20minute aggregate native cap. Build two original and two identical overlay executables, save identities, no production changes.
- [x] Configure all builds before measurement; upload every prefix on failure. No automatic retries, mutation/fullphase sweep or threshold changes.
- [x] Actionlint; Python tests; focused Rust test; full scripts/pre-push-check.sh with own target and CARGO_INCREMENTAL=0.
- [x] Independent spec compliance, then implementation quality review; resolve blockers before push.
Commit: ci: run finite Durable append diagnosis on Linux.

## Task4: evidence and close
- [x] One milestone push and one dispatch approved, immutable A/B sourceSHAs from Git. Wait for actual completion, no reruns on failure.
- [x] Download all artifacts and verify source/overlay/binary/exit/clock/resource/row/cap/loss/ownership inventories; independent evidence review.
- [x] Document complete or prefix/inconclusive outcome in task decision and ignored evidence/report; update handoff per commit.
- [x] Keep CRC/task source losses unresolved absent accepted cause; no merge/exception/R2 qualification.
- [x] Clean own target and retain branch/evidence/executables; remove the clean source worktree after the final report commit, recording that post-commit action in the handoff.
Commit: docs: record bounded Durable append diagnostic findings.


## Sole-cycle result and local corrections

Run38569346117 at859b525 failed before DB benchmarks: zero calls/rows,
`missing exact native exec`, inconclusive. Independent audit confirms CPU raw
events contain the exec while its rendered lifecycle body is empty. Both marked
Cargo artifacts are `fresh:true` and match their originals byte for byte.

Test-first local corrections preserve all budgets/checks: render CPU software
and tracepoint fields separately by event type; invalidate only the marked
source package's release artifacts with its explicit own target; save clean
logs; require `fresh:false` and different original/marked contents. The actual
failed run and published milestone are immutable. Local fixes have not been
pushed or live-qualified; no second push/dispatch is authorized.

Future follow-ups require separately approved scope:
- Exact CPU/lifecycle/callchain qualification on Linux, including the inherited
  lifecycle frequency/DWARF configuration concern. No event loss is proved here.
- Fresh marked compiles with retained clean provenance and actual Linux identity.
- Original Durable loss investigation after qualification, without a waiver.

See decision `docs/remediation/decisions/2026-10-14-durable-append-diagnostic.md`
and ignored verification `REPORT.md` / `NEXT.md` for evidence and acceptance.
