# Stream read measured-phase profile design

Date: 2026-10-09. Base: 797f8c679b1d9f6922f4afb1fe0fff4ba928735e.
Task: 2.15b.8 / competitive evaluation Task 3; profiling infrastructure only.

## Approved direction and scope

The maintainer requested the next step after portable audit175488b: a Linux CPU
profile of the existing read workload, then at most one reviewed correction.
The portable audit isolated duplicate hint validation but measured no CPU-time share.
This task adds optional benchmark instrumentation and a labelled Linux probe.
No production, format, durability, gate threshold or index changes are authorized.
Hardware selection is pending; preparation can proceed independently, execution
must record the selected host and actual tools/permissions before measurement.

## Profile contract

Reuse `wal_write_path` `stream_read/tail/1k`: 256 MiB values, 256 appends of
1,024 keyed1KiB records, seven-byte keys, empty headers, uncompressed Fast setup
followed by sync, 256 MiB segment setting, no retention, one reader,1MiB limits,
full-log cycling,1s warmup/3s nominal measurement,3 chronological repetitions.
Do not substitute actual final-frame seeking, a tiny log or a partial decoder.

Optional `SPIKE_STREAM_PROFILE=1` changes setup/instrumentation only. A recording
clock returns the same SystemTime-derived milliseconds as the production clock
and retains each append time. Append acknowledgements and original fixture fields
form the expected records. Before warmup, read the complete fixture and compare
all fields, ordering, offsets, cursors, watermark and terminal empty result.
Do not retain a second256MiB copy: original shared value and per-frame ack/time
metadata suffice. A failure aborts before any measured-phase marker.

Inside the existing loop, emit one JSON begin marker at the first measured read
and one end marker after the final eligible iteration. They use CLOCK_MONOTONIC
nanoseconds. Work continues through the last eligible read's completion as in
the current benchmark; actual marker duration is separate from the row's nominal
3s denominator. Setup, verification, warmup and close are outside the interval.
Markers identify source schema, cell and process. Default benchmark execution
emits no markers and performs no preflight. Report profiled rows only as diagnostic
output; no unprofiled speed or unchanged >5% instruction gate is inferred.

Build first, then run one `perf record` over the executable and its threads:
`-e cpu-clock:u -F199 --strict-freq --period --clockid mono --call-graph dwarf,16384`.
The software event measures sampled userspace CPU, not hardware instruction counts.
Compilerflags add debuginfo1 and frame pointers for profiling only. Preserve exact
binary, source, rustc, flags, perf version and all raw stdout/stderr/perf.data.
No compiler may run during profiling. Record CPU/RAM/kernel/filesystem/load,
affinity/cpuset and before/after process checks. Do not dump credentials/environment.

Record the full process to retain original evidence, then filter samples using
the exact three marker intervals and perf's matching monotonic timestamps.
Extraction is `perf script --ns --show-lost-events -F pid,tid,time,event,period,ip,sym,dso`.
Keep a separate `perf script -D` raw-event audit, because ordinary script output
can omit throttle records. Reject LOST, LOST_SAMPLES, THROTTLE and UNTHROTTLE
in that audit. Isolate all perf commands with `PERF_CONFIG=/dev/null` so a user
configuration cannot reverse call-chain order. Support the real multiline sample
header and stack grammar; the first callee frame supplies the exclusive leaf.
Use integer nanoseconds and [begin,end) bounds. Since perf interval commands
include their end, retain their end timestamp as marker end minus one nanosecond;
requalification validates every interval command against the markers. Samples must belong to the marker's
process; record every thread represented. Keep per-phase sample/period totals,
classify unresolved/hex-only leaves as unknown, weight by sampled period, and
count a recursive symbol only once per sample for inclusive attribution. Reject
loss/unwind/throttling diagnostics in retained raw/stderr. Pooled300sample quality
does not prove repetition stability; all three counts are reported individually.
Retain unfiltered data/script/raw-event audit plus interval-specific perf
report/script output and exit statuses. Requalification requires every expected
command to have succeeded and every phase artifact to be present and hashed;
a failed recording round must never become qualified by reparsing incomplete files.
Parser rejects missing/reordered/overlapping intervals, false fixture verification,
unexpected cell, malformed timestamps, sample periods, source or hashes, missing
repetitions, lost records or unreadable stacks. Before results, freeze quality:
at least300 total measured samples, nonzero samples in every interval, no lost
records, and no more than10% unknown leaf samples. Failure is unqualified evidence,
not permission to tune thresholds or retry the whole round.

Aggregate period-weighted leaf symbols for exclusive cost and call-chain membership
for inclusive hypotheses; inclusive categories overlap and must never be added
to100%. Preserve raw symbols instead of merging inconvenient buckets. CPU sampling
does not quantify blocked time, physical bytes, allocations or memory bandwidth;
mark these unmeasured and combine only with the prior deterministic byte evidence.

Host `perf` and kernel permissions are qualification prerequisites. Missing tools
must fail with their install command for maintainer, not install silently or
substitute walltime. Existing hosted Actions VM may be used if selected, with
shared-host/steal limits explicitly recorded. Such walltime is CI trend evidence,
not dedicated competitive timing qualification. A dedicated SSH host uses the
same script/config and is a stronger future timing environment.
Same-ref Actions dispatches are sequential to avoid existing workflow cancellation.

## Workflow and publication

Extend only the existing remediation gate input with `stream-read-profile`, adding
one separate probe job. Resolve source SHA as today; immutable selected source must
contain the profile hooks. The ordinary gate/WAL/IAI jobs and checks remain unchanged.
Read-only permissions, no credentials retained, standard pinned1.98.1 CI preamble,
own target/incremental settings, one serialized job, artifacts even on failure.
No deployment, main edit, PR or security disclosure. Existing milestone push
authorization applies only after reviewed local checks; no dispatch until host
selection and workflow/source qualification are settled.

## Tentative correction for review, not implementation

Candidate: retain the already CRC-validated hinted frame, rather than discard its
owned payload and reread/check the same frame in the scanner. A private immutable
validated-frame type binds bytebuffer, checked RecordLoc, kind and checked extent
to the exact snapshot file handle. Only the validator can construct it. Both
the seed and continuation retain that same Arc handle; never relook up its path.
The seed passes through the existing capped visitor: a commit hook may have
registered a successfully written hint before acknowledgement, beyond the frozen cap.

The seeded scanner verifies segment magic/format/firstLSN before any visitor,
checks extent against its captured file length, visits that frame with verified
kind/LSN/payload identity, then continues the current frame scanner at the next
LSN/byte position. Preserve SegmentScan valid_len/next_lsn/visitor-stop semantics.
No CRC-free header trust, disabled hints or public signature change. Invalid
hint validation follows the existing from-header fallback, including typed
unsupported kind/I/O/corruption behavior after fallback. Full Records decoding
and skipped/unreturned body validation remain unchanged.

The snapshot proof requires confirming all production writers: append never
mutates an acknowledged frame; compaction replaces inodes rather than overwrite
snapshot bytes; retention handle/guard lifetime persists through read cancellation.
Cached bytes cannot be reused across calls, handles or compaction generations.
A mismatch of this proof blocks the correction. Reusing one validated buffer is
not a persistent cache and changes no routing/on-disk format.

Predicted witness for the single b100 frame: public whole read changes4calls /
205911bytes to3calls /102976bytes (header17 +frame102935 +segmentheader24).
This is a hypothesis for deterministic RED/GREEN, not a predicted Linux speedup.
Singleton full Records decode still repeats; this candidate must not quietly
expand into a partial decoder or a second optimization round.

Future acceptance: exact output, invalid/extreme hint fallback, intentional prefix
skip, malformed headers/CRC/identity/unknown kind, progressing short reads and later
I/O failure, sealed/active faults, oversized frames, capped scan, retention/roll/
cancellation lifetime; full existing checks, both200seed modes, unchanged Criterion,
paired Linux instruction gate and unchanged T3. Keep T1/T4/T5 misses visible.
Begin production work only after the measured profile supports this candidate and
its safety design passes review. If one correction fails, return evidence to the
maintainer instead of starting another rewrite. A/B frozen, C retired.

## Exact files

- docs/superpowers/specs/2026-10-09-stream-read-profile-design.md
- docs/superpowers/plans/2026-10-09-stream-read-profile.md
- crates/prkdb/benches/wal_write_path.rs
- crates/prkdb/benches/support/stream_read_profile.rs
- crates/prkdb/tests/stream_profile_fixture.rs
- scripts/stream_read_profile.py
- scripts/test_stream_read_profile.py
- .github/workflows/remediation-gate.yml
- docs/remediation/decisions/2026-10-09-stream-read-profile.md (when results available)

Future production files require a separate claim; none are edited by this task.

## Approved Actions permission provisioning, 2026-10-09

Run37908155101 at77af282 refused cpu-clock:u because the runner's
perf_event_paranoid was4. No build or CPU recording happened. The maintainer
explicitly approved setting kernel.perf_event_paranoid=2 only on the disposable
profile job, retaining its original value and restoring it in an always() step.
The workflow performs this provisioning openly before the unchanged preflight;
the profiling helper itself never modifies host settings. No persistent sysctl
configuration, elevated benchmark process, event/workload/quality/gate change,
or favorable-data retry is authorized. Record setup and restore logs alongside
raw artifacts; restore failure must fail the job and remain visible.


## Proposed recovery of the existing stream CPU recording

Status: awaiting maintainer approval; no recovery implementation or execution.

Run [37909749321](https://github.com/prk-Jr/prkdb/actions/runs/37909749321)
captured the one fixed round at source `06965f2cbad61628488a423106084fdc34fe0be4`.
It failed the unchanged unknown-leaf limit: 483 of 1,310 measured samples
(36.8702%) were unresolved. Of those, 476 belong to libc and seven to vDSO.
The original failure is retained. Temporary perf permission was restored to 4.

The current protocol prohibits qualifying a failed manifest. This proposal asks
for a narrow amendment allowing separate derived analysis of a complete recording
that failed only symbol resolution. It does not authorize a benchmark rerun.

1. Preserve the original failed qualification, source manifest and 43 hashed
   artifacts byte-for-byte. Independently validate all nine command exit statuses,
   raw loss/throttle/unwind checks, fixture, source and interval contracts.
2. Retrieve the recorded libc executable and debug symbols only by exact ELF
   build ID `a4a7992a8e66555c8141ab2a08a8465ff6e0ea65`. Verify both build IDs,
   retain download provenance and hashes, and isolate the symbol root/cache.
   Availability remains unverified. Keep the exact retained benchmark; never
   substitute the analysis host's libc or vDSO.
3. Re-extract only the existing perf.data using the original extraction options
   plus the approved symbol-root/cache configuration. No benchmark, perf record,
   perf stat, cargo build, or sysctl change is part of recovery.
4. Require unchanged ordered sample identities, addresses, periods, events,
   process/thread IDs and timestamps. Verify all 1,910 samples and the 1,310
   measured samples split 446/427/437 across the original intervals. Symbol names
   may change; changes to unwind frames need explicit review and cannot silently
   change the sampled cohort or leaf identity.
5. Write a separate derived manifest and report referencing the original failed
   manifest, perf.data hash, analysis revision, tool versions, symbol files and
   extraction commands/statuses. Never flip the original qualified/complete flags.
6. Apply all original thresholds unchanged: at least 300 pooled samples, nonzero
   samples per phase, no loss/throttling/unwind failures, and at most 10% unknown
   leaves. Unresolved vDSO leaves remain unknown. At least 352 unknown leaves must
   resolve. Missing matching symbols or another quality failure remains a failure.

Original perf.data SHA256:
`11e4022abb2edcc29aaef169b39eb6d4796f775a7e65f3a848de36e1a203eb3b`.

The implementation must be test-first and independently reviewed before offline
Actions execution. Incomplete, failed-command or lossy rounds remain ineligible.
No production correction or performance acceptance is authorized by this proposal.
