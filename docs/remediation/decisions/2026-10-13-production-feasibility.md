# Feasibility decisions: revision 2

Date:2026-10-13. Status: revised proposals, numerical requirements not approved.
R1 is preserved at754d05a and original root evidence snapshots; no old result is
reclassified. The maintainer requested memory/recovery/workload revisions.

| Decision | Current state |
| --- | --- |
| Separate streams and indexed lanes | Maintainer selected both |
| Immediate environment | Maintainer selected GitHub Actions SCREENING ONLY |
| R1 numerical production requirements | Not approved; revision requested |
| Revision priorities | Memory, recovery and workload requirements |
| Full production host/qualification | Deferred; no executable125h qualification authorization |
| Source correctness eligibility | Required; broad current phase2 profiles BLOCKED |
| Revised indexed working sets | Proposed100k1KiB /10k64KiB, real indexes, fixed60/30/10 mix |
| Revised stream working set | Proposed1GiB retention,128MiB segments, actual consumers/retention where eligible |
| Memory | Proposed engineRSS2GiB/clientRSS256MiB; separate cgroup peaks4GiB/512MiB; fixed-live growth64MiB over10min |
| Recovery | Proposed128MiB/1GiB, indexed checkpoint/no-checkpoint, rawstream actual sparse-index rebuild, both modes,3 fresh-process repetitions; separately timed WAL, index, consumer readiness |
| Recovery ceilings | Proposed WAL1/5s, indexed ready5/20s, consumer ready2/10s, cumulative; warm filesystem explicitly labeled |
| Screening schedule | Proposed ordinary30+120s cells, separate resource/recovery native55min jobs (both-mode resource probes;24 indexed/12 stream recovery attempts);90min perjob including build |
| Offered loads / latency / throughput minima | Still pending; R1 values not silently adopted or changed |
| Existing perf/crash/phase gates | Required unchanged; shorter/smaller screens never replace them |

No code, CI, API, disk format, ledger status, seed count or existing floor changes.
No new timing is authorized. Maintainer must review the concrete revised design
and remaining offered loads; later P0/harness tasks are separate reviewed claims.
