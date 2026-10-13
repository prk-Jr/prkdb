# WAL trace qualification and finite performance decision

Status: implementation, independent review and local full checks pass;
Linux qualification/capture outstanding. No production adoption or performance
exception is authorized.

## Existing evidence: useful work, bounded failures

Independent read-only review found a concrete stream-lane optimization:
`bench_stream_read_100` on Linux, Fast/uncompressed/synced fixture, 100 keyed
1 KiB records: 508769 to 250897 instructions, -50.685%, against baseline
9b88229d29cf4ea1a384b1357139f0b0f6c7f313. Correction 4b01aeb is contained in measured
heads 40c2968 and a64f774. Original warm stream_read/tail/1k medians improved
1790.6 to 2213.7 MB/s (+23.63%); retained-binary diagnostic pairs later improved
26.88-32.70% with lower p99. These are genuine measured opportunities, but the
full original acceptance remains FAILED and the correction stays unmerged.

The original full-grid run's Fast adapter_put/8w/64k throughput fell 45.24%,
cold stream throughput fell 68.26%, and tails worsened. Later isolated throughput
losses did not reproduce, while cold p99 still rose 6.4-9.9%. Baseline-to-itself
write pairs varied 28.8% and 33.7%. Different workload histories/runner and
unqualified original temporary-filesystem equivalence prevent dismissing the
original regression or attributing it to noise.

Stop the failed indexed-write placements: B on bf9f7c7 versus db0c430, Mac Fast
8 writers, real indexes/1 KiB blind inserts, fixed 50000 setup plus 96000 measured,
reached 87.475% of its reference; C's extra ordered apply worker on 7e0117d versus
the same reference reached 86.58%/82.59% at 1/8 writers. Both missed their 97%
bounded adoption floor. SmallVec trimming had no demonstrated gain. These failed
placements do not prove every ordering/coordination cost is unavoidable.

Evidence: ../wip-2.15b.8-frame-reuse/docs/remediation/decisions/2026-10-12-stream-frame-reuse.md;
../wip-wal-stall-diagnostic/docs/remediation/decisions/2026-10-12-wal-stall-diagnostic.md;
main .agents/verification/index-perf-b-2026-10-09/SUMMARY.md and
index-perf-c-2026-10-09/SUMMARY.md. No matched Kafka/Iggy/PostgreSQL comparison
exists; neither competitive success nor project-wide impossibility is established.

## Current decision boundary

First prove live Linux /proc identity and corrected descriptor parsing without
starting a database benchmark, then qualify the existing synthetic three-recorder
smoke. At most two qualification-only runs allow one demonstrated tooling
correction. Only after success, one fixed capture repeats qualification on its
own host before four native controls/four profiles. No benchmark retry, gate
change or architecture prototype is part of this task.

A cause-backed stream correction is a separate reviewed task. If this finite
capture cannot qualify attribution, stop custom tracing and report a concrete
alternative with unresolved acceptance conditions. Do not infer abandoning the
project from diagnostic failures. Linux result will be recorded below.
