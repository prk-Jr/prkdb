# Validated stream frame reuse

Date:2026-10-12. Task2.15b.8 / evaluationTask3. Base9b88229d29cf4ea1a384b1357139f0b0f6c7f313.
User approved profile integration and one measured correction via 'Go ahead'.
Governing remediation spec/plan/ledger and unchanged performance gates apply.

## Problem and approved scope

scan_from_loc_capped validates a sparse hint with read_frame, discards its owned
payload, then scan_segment_from reads and CRC-checks that same frame again.
The qualified saved CPU profile (source06965f2, analysis3fa91aa, run37922246060)
attributes32.75% exclusive CPU to CRC, including20.08percentagepoints with
segment::read_frame in the physical stack. This is evidence for an experiment,
not a predicted speedup. The public100-record fixture has4readcalls/205911bytes.

Reuse only the already validated first frame inside the same scan. No persistent
cache, decoder shortcut, checksum removal, public API change, file format change,
index/consumer rewrite, timing-rule change or second optimization round.

## Ownership and snapshot proof

A private constructor-controlled token in wal/segment.rs owns complete frame
bytes, parsed validated kind, exact RecordLoc, checked end extent, captured
file length and exact Arc<dyn VfsFile>. Only successful capped length, kind,
CRC, LSN and payload-identity checks construct it. Private fields prevent callers
inventing validation. A seed is consumed once and never survives its scan call.
The seeded scanner uses the token's same Arc for both segment-header verification
and continuation; it never reopens a path or rechecks the live map.

The Arc itself supplies inode/generation identity; a numeric generation is not
required for this one-call token. If implementation retains numeric generation,
capture it under the original segments-lock snapshot, never after validation.
Production append advances active.write_pos without rewriting existing frames;
recovery truncates before handles publish. Compaction writes another inode,
atomically renames/swaps Arc handles. Retention unlinks without truncating captured
handles. Stream blocking read owns Inner and retention guard after caller
cancellation. Protection against arbitrary external file mutation is not added.
No token persists across modeled power loss.

## Scan behavior and failure boundaries

Eligibility keeps idx0, selected segment, valid offset/LSN and hint<=from checks.
Validation obtains bounded frame bytes with progressing read_full, checks payload
limits and overflow-safe extent within captured file length. Preserve public
read_frame semantics; shared private validation may return bytes/kind without
copying a payload that the hint caller immediately discards.

The seeded scanner verifies segment magic/format/firstLSN on the token Arc before
any visit, checks seed extent against its captured file length, then feeds seed
loc/kind/payload to exactly the existing capped_visit. Release seed byte ownership before allocating a continuation scan buffer.
Advance next_lsn and
valid_len after that visit even if it breaks. On Break, return stopped_by_visitor
with no continuation I/O. On Continue, call the existing scan_frames loop from
seedend/nextLSN with the same captured length and Arc. Do not read later bytes
appended beyond the snapshot. Preserve SegmentScan file_len/fault/stop semantics.

Validation errors preserve the current fallback: discard token attempt and scan
from the segment header; do not return validation errors directly. Thus fallback
still controls UnsupportedFormat, I/O, CorruptSegment, active-tail tolerance and
sealed-segment completeness. Bad/stale hints cannot bypass header checks.
The existing cap may precede a hint registered by a commit hook; the seed goes
through that same cap visitor and is never delivered past min(cap,acked_lsn).

Whole skipped frames retain CRC/kind checks. StreamLog continues to decode all
Records bodies in a delivered batch, including intra-batch skipped and unreturned
records. Whole frames below from retain their existing behavior; this correction
does not add Records decoding for frames the stream visitor never receives.

## Tests and acceptance

Test-first public witness: exact100unkeyed1KiB records with empty headers, fixed append clock, Fast
uncompressed setup followed bysync, original key/value/header/time/offset/cursor/
watermark expectation; reset real StdVfs read counters after setup. Expected
4calls205911bytes before,3calls102976bytes after, with output unchanged.
Counting observes read_at returned bytes, not physical disk I/O. A separate keyed
record/header control checks those semantic fields without borrowing the unkeyed
byte totals. The unkeyed payload is18+100*(1+4+1024)=102918bytes; frame102935.
Before reads17+102935+24+102935=205911; after17+102935+24=102976.

Core tests cover header magic/version/firstLSN before visit; seed visitor-break
nextLSN/valid_len/file_len; continuation CRC/gap/unsupportedkind/fault offsets;
invalid/extreme/oversized/wrongidentity hint fallback; captured cap and length;
progressing short reads and later I/O failure; sealed/active behavior; seed and
continuation both pinned to old inode after rename/release. Stream tests preserve
malformed skipped/unreturned body refusal and cancellation retention lifetime.
Use deterministic gates/channels, no sleeps/retries/timeouts to hide failure.
Reuse existing relevant tests where they already establish the property; add
specific seed-boundary witnesses when the old test cannot reach that boundary.

Before/after Criterion: stream_log/stream_read/tail_100x1KiB, baseline9b88229,
Fast uncompressed setup synced before reads; explicit own target/incremental0.
Run without competing compiler/build/test work; archive raw source/tool/binary/
Criterion data and state noise/inconclusive results honestly. Full local checks,
200seedDurable+Fast harness, singleWAL/floors/ledger/render unchanged. Fresh Linux
wal-bench and instruction probes paired against9b88229 after reviewed milestone.
The existing dispatchable iai probe currently lacks a baseline comparison. Extend
only its optional existing base_ref input using the reviewed perf-gate native
baseline protocol on the same disposable runner; preserve default no-base floors
and all thresholds. Retain exact source/build/raw provenance for both sides.
Require unchanged5% instruction gate and existing task targets; do not manufacture
a paired delta from an unpaired instruction probe. T3 remains50%pread ceiling;
T1/T4/T5 and index misses remain visible. No competitor timing conclusion.

STOP if snapshot proof fails, public behavior changes, relevant correctness check
fails, a tracked gate regresses, or this single correction yields no measured
benefit. Report evidence rather than starting another design. An improving cell
alone does not complete T3 or Phase2. No automatic production merge is authorized.

## Files

- crates/prkdb-core/src/wal/log.rs
- crates/prkdb-core/src/wal/segment.rs (includes private token unit tests)
- crates/prkdb-core/tests/wal_scan_edges.rs
- crates/prkdb-core/tests/wal_segment.rs
- crates/prkdb/tests/stream_frame_reuse.rs (new public witness and boundary tests)
- docs/superpowers/specs/2026-10-12-stream-frame-reuse-design.md
- docs/superpowers/plans/2026-10-12-stream-frame-reuse.md
- docs/remediation/decisions/2026-10-12-stream-frame-reuse.md
- .github/workflows/remediation-gate.yml (optional paired iai and WAL evidence retention)

No other files without a documented necessary scope update. No new correctness
finding claimed by this performance correction; ledger changes only if a new
independent defect is reproduced. Profile/audit evidence remains immutable.


### Throughput evidence retention

The existing WAL probe prints base/head tables but previously did not retain
actual executable identities or raw files as artifacts. Extend only its evidence
handling: immutable baseline/head, compiler-artifact executable hash/copy before
each run, unchangedSPIKE_REPS=3 andallworkloads/rules, rawtables/nativeexitcodes,
source/build/hostmetadata andalways-upload hashinventory. Restore head onfailure.
This is necessarymeasurementprovenance, not an additional optimization or a change
to timing/qualityacceptance. The alreadydispatched40c2968IAIpair measures identical
Rustsources; any laterCI-onlycommit mustproveRusttreeidentity beforeusingthatresult.
