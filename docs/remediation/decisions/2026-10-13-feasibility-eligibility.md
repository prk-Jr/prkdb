# Static feasibility inventory and current findings

Date:2026-10-13. Stage:P0, approved R2 GitHub SCREENING ONLY.
This is a static source/requirements/ledger inventory, not a timing result.

## What the tool can establish

Run from the exact assembled clean checkout:
```bash
PYTHONDONTWRITEBYTECODE=1 python3 scripts/production_feasibility.py inventory --repo .
```
Optional `--output` creates a NEW artifact atomically; existing files, inputs,
tracked sources and symlink aliases are refused. Exit2 is an honest BLOCKED
inventory, not a test regression; exit0 means READY_FOR_REVIEW only. It never
runs a build/benchmark, authorizes timings or certifies production. JSON records
source/protocol/ledger/manifest hashes, per-capability reasons and phase status.
The new Python suite is invoked explicitly with:
```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts -p test_production_feasibility.py -v
```
This slice adds no CI workflow; wiring and real probes belong to P2. Do not assume
cargo test or existing workflow jobs run this new Python suite.

The committed template deliberately leaves source/task pins, offered loads and
execution evidence unset. Zero loads are UNSET configuration sentinels, never
published benchmark results. Referencing an existing report/function or an
ancestor commit establishes static presence/integration only, not truthful CI,
performance acceptance or task completion. P1/P2 must qualify actual evidence.

## Current source blockers and boundaries

- Indexed data: KEY-02 is open; acknowledged index consistency and unique-value
  enforcement depend on the existing2.17 task and its unchanged acceptance gates.
- Native stream consumers: the existing2.15b.6 work and EVT-07 remain needed.
  EVT-07's global closure also includes2.15b.10; CRC-based resume checks alone
  cannot claim exact Fast reissue detection for byte-identical replacement frames.
- Raw StreamLog append/read is a separate capability. The current relevant ledger
  rows contain no unresolved data-path finding; that is NOT proof of runtime SLOs,
  memory/recovery bounds or passed T1-T5, and all execution prerequisites still apply.
- Legacy events/outbox EVT-01..06, transactions TXN-01..04, TTL and cluster issues
  remain roadmap/shipping blockers for those advertised features. Excluding them
  from a narrow engineering profile does not fix them or remove them from the
  production inventory. Native stream resume is not the legacy event-consumer API.
- Phase2 remains in_progress; later phase gates are not completed. Passing local
  checks and fixed storage findings do not complete the phase.

## Retained performance findings (not new P0 measurements)

- Frame reuse4b01aeb vs9b88229 reduced `bench_stream_read_100` instructions50.685%
  on the Fast synced-fixture Linux cell and improved warm read results. Its original
  full-grid acceptance failed; it is unmerged and cannot be shipped on that win.
- Indexed B bf9f7c7 and C7e0117d failed their declared Fast screen comparisons
  against db0c430. These implementations remain stopped, not adopted.
- Later tracing observed long I/O waits, with declared source/whole-process limits.
  The baseline-only history control found no consistent >=20% envelope effect
  under its fixed schedule. Neither explained away the original failures or
  established that the frame-reuse change caused a production write regression.
- No accepted indexed performance, sustained resource/recovery qualification or
  matched Kafka/Iggy/PostgreSQL parity follows. The old6ce73a8 baseline omitted
  required fsync work; its throughput is not a like-for-like Durable competitor target.

## Next bounded work

1. Preserve this inventory on the actual integrated source. Do not substitute
   branch-only fixes or synthesize missing benchmark rows.
2. Continue existing ownership-respecting2.15b.6/2.15b.10 and2.17 roadmap tasks:
   source correctness plus unchanged explicit performance acceptance, no B/C rewrite
   or new baseline diagnostic loop.
3. Set offered arrival rates and pin exact real runtime/read/fixture/host manifests
   BEFORE eligible source screens. Approved R2 budgets stay fixed.
4. Real P1/P2 resource/recovery/ordinary screens are independently reviewed tasks.
   Dedicated-host production qualification and competition remain deferred.

No ledger findings/status changes, no API/on-disk/hot-path edits, no old threshold,
seed count or durability relaxation. P0 produces findings/ineligibility evidence,
not a new architectural verdict or permission to ship.
