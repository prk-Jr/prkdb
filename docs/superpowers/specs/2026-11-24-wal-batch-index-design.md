> Historical prerequisite document from the Rust1.99.1 local fix at0126808. Its source/compiler/results describe that branch only. Current Rust1.100 assembly, new performance concern and acceptance are recorded in `docs/superpowers/plans/2026-11-24-linux-qualification-candidate.md`; no historical receipt is relabelled.

# Legacy WAL batch sparse-index accounting correction

## Scope and established defect

Continue the maintainer-requested local remediation from deterministic RED checkpoint9d2adae on public source9c4345a. Correct LogSegment::append_batch index interval accounting only. The private Phase2 rewrite removed this legacy path; do not merge this source into Phase2 or claim its gates passed. No push, PR, main integration, workflow dispatch, dependency/pin change or waiver.

The retained test demonstrates index entries4/80 at100records,5/81at101,5/82at102 and6/102at122 for single/batch writes. All122records/checksums read correctly before/afterreopen. The batch loop adds cumulative total_bytes when testing every record and never resets its interval counter; after threshold, each remaining record inserts an index entry and performs index sync_all. The previous AllReplicasFailed aggregate still lacks inner error attribution; solving this independent defect does not prove a historical timeout.

## Selected correction and preservation

Track bytes-since-index per serialized record using the same SeqCst fetch_add and successful-insertion reset as append. Keep total_bytes solely for batch file-position accounting. On each successful data write, fetch_add(record_size); when the returned previous count plus record_size reaches interval, call index.append(offset,position), then store0 only after success. Remove the final extra total_bytes counter addition. Preserve the final data-file sync_all, every required index sync_all, error propagation, record/offset format, public signatures and position calculation.

Do not move syncs, relax durability, defer index updates, retry errors or widen timeouts. Do not change append's lock scope or redesign the existing shared writer/concurrency protocol in this task. Review interactions with its post-file-lock index accounting explicitly; make no new concurrency-safety claim for unrelated legacy defects. If the proposed patch worsens supported concurrent behavior or needs a wider correction, stop and report before improvising.

## Meaningful performance evidence

Existing wal_bench batch label calls append on a PutBatch record, so it does not reach the defective method. Existing batching_bench batch100 does call append_batch but uses records at least as large as its1024byte interval; one index entry per record is then expected even after correction. Retain it as a control.

Add a Criterion case wal/sparse_index_durable/batch_100_short using a fresh LogSegment, interval1024, exact100short test records, PerIteration setup, measured append_batch and output retaining segment/tempdir until after timing. Criterion0.5.1 source confirms PerIteration setup and output destruction are outside the measured section. This isolates actual batch serialization/writes and required index/WAL syncs; no DB calls or required sync is mocked. No sample, warmup, duration, floor or threshold is reduced.

Build and retain baseline executable at a signed benchmark-only commit before production edits, and retain the fixed executable after. Direct executable invocation must include --bench, anchored two-case filter and --save-baseline before / --baseline before; require real before/new estimate/sample JSON, not Criterion test mode. Then run the two prebuilt executables back-to-back with no rustc processes, same cwd/target/compiler/inputs/mode and Criterion defaults. Compare both sparse shortrecord case and unchanged real batch100control. Record executable/source hashes, raw output and estimate JSON. Label mode Durable (both segment and index use synchronous sync_all), local Mac wall-clock only. No Linux instruction-count claim. If a measured regression needs explanation, investigate; do not lower gates or grant a performance waiver.

## Acceptance and tracking

Existing RED regression must become green with exact sparse entries/positions and all data/reopen/checksum checks. Add a mixed single/batch/repeated-boundary regression if needed to distinguish carrying vs incorrectly clearing interval remainder. Establish its RED before fix. Keep error and sync behavior reviewed through code and existing tests.

Reserve STO-25 (cross-branch scan found STO01..24 only). Add ledger/spec row and revision12 on this branch, initially open; after signed fix, set fixed with exact regression tests/fixingSHA, render generated pages viaxtask. Do not claim verified/CI/harness qualification. Other Phase2findings/phase statuses and historical revisions remain unchanged. Generated repo-status may need refresh if objective fingerprint changes.

Full local verification on actual public-base Rust1.99.1: fmt, all-target workspaceClippy-Dwarnings, workspace nextest, doctests, blocking Durable200seeds, ledger/render/repo/readme/docclaims and pre-push script. Fast mode and privatePhase2 perf-floor/single-WAL scripts are absent on this public base; report them as unavailable source obligations rather than importing unrelated implementations. Run applicable benchmark guard present on this base. Independent spec/code review required. Existing url2.5.8 security advisory remains a publication blocker until safe public-base integration.

## Files

crates/prkdb-core/src/wal/log_segment.rs; crates/prkdb-core/tests/wal_batch_index_interval.rs; crates/prkdb-core/benches/batching_bench.rs; this design and companion plan; docs/remediation/ledger.toml; root remediation spec finding/revision rows; generated docs/status/remediation.md and repo-status.md if needed.
