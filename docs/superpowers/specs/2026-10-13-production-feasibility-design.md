# Feasibility requirements, revision 2: GitHub screening first

Date: 2026-10-13. Status: Revised screening requirements APPROVED by the maintainer.
Approved direction: BOTH streams and indexed data, separate gates; GitHub Actions
SCREENING ONLY for now. The maintainer requested revisions to memory, recovery
and workload requirements and did not adopt revision 1 numerical requirements.

Revision 1 remains immutable at commit
754d05a6c90c8a30403da1891617feca2a655383 and in the main checkout's
.agents/verification/production-feasibility-2026-10-13/proposal/ snapshots.
This revision replaces its proposed immediate workload/resource/recovery profile.
It does not approve its other numbers or authorize its 125-hour qualification run.

## Authority, claim and eligibility

The remediation spec, plan and ledger win. Existing T1-T5, Task 2.17
Fast >= 95% / Durable >= 97% and InMemory requirements, the 5% instruction gate,
crash seed counts and deadline/settlement guarantees remain independent and
unchanged. Smaller CI datasets or shorter screens cannot replace those checks.
The existing evaluation direction at 8eab751 is retained; B/C remain retired,
frame reuse remains unmerged and old failed evidence remains visible.

This milestone asks whether an explicitly scoped supported API has correctness
or gross resource/recovery problems under reproducible bounded engineering
workloads. A SCREEN PASS does not certify production, sustained capacity, p99.9,
competitive speed, multi-node resilience or AI checkpoint/event atomicity.
Production requirements need a declared deployment and later approval.

Current assembled phase-2 source 9b88229 is BLOCKED for full indexed and consumer
profiles: KEY-02 / Task 2.17, TXN-01..04, Task 2.15b.6 and relevant EVT guarantees
remain unresolved. P0 maps each claimed operation to its actual source/regressions.
Unmerged branches and passing microbenchmarks cannot clear shipping findings.
Narrow raw StreamLog append/read evidence may be scoped explicitly; consumer
resume stays BLOCKED until its prerequisites land. Indexed writes/index queries
wait for acknowledged consistent real index maintenance; they are not replaced
with raw KV. Atomic indexed transactions, outbox/change feed and multi-node claims
are excluded from this initial engineering profile, remain roadmap blockers for
any product that advertises them, and cannot be declared production ready here.

No new harness or timing starts before this revised protocol is approved and the
specific measured operations have passed eligibility. Existing task owners keep
their correctness work; this document does not claim or redesign their code.

## GitHub runner qualification

Use a pinned Linux runner image and immutable source/binary/manifest digests.
Record actual CPU affinity/topology/quota, memory and cgroup limits, free disk,
filesystem/mount options, kernel, Rust 1.98.1 and durability configuration.
The historical run 38391818290 recorded four available CPUs and about 15.6 GiB
physical RAM; that is historical evidence, not a promise about future runners.

Proposed minimum usable environment: four available CPUs, 6 GiB available RAM
within the effective host/cgroup limits and 8 GiB free disk AFTER build. Pin two
engine CPUs, one client CPU and one observer CPU in the approved manifest; do not
pretend the runner has the proposed production host's four plus four CPUs.
If affinity/isolation/counter requirements cannot be qualified, mark the affected
measurement UNJUDGEABLE or host BLOCKED. Do not shrink data or increase limits.
No compilation/profiling in timing. Diagnostics and faults run separately.

Every proposed screening job has a 90-minute wall-clock deadline including build,
preflight, fixtures, native work and artifact publication. Missing build/host
qualification is infrastructure BLOCKED, not a database regression. Archive
available partial evidence on failure; incomplete cells cannot pass.

## Revised workload proposals

All datasets are deterministic, uncompressed and validated byte-for-byte. Record
logical values, encoded key/header/index bytes, retained WAL bytes and total files
separately; sparse file size or compressible filler is not a working set.

| Lane | Initial ordinary screen | Resource state |
| --- | --- | --- |
| Streams | 8 producers; b1 and b100; 1 KiB and 64 KiB values; Fast and Durable separately: 8 cells | One partition; retention target 1 GiB, 128 MiB segments; exact floor and per-partition ordering |
| Indexed | 8 clients; 1 KiB and 64 KiB values; Fast and Durable separately: 4 cells | 100,000 live 1 KiB records OR 10,000 live 64 KiB records; one unique, one nonunique and the production version index |

Indexed value populations are approximately 97.7 MiB and 625 MiB, before metadata
or copies. These are distinct CI working sets, not substitutes for revision 1's
200,000 / 50,000-record production proposals or original historical bench cells.
Indexed ordinary mix: 60% overwrite/RMW writes, 30% point reads, 10% secondary
queries. Each ordinary read/query fully materializes exactly one matching record.
New inserts/growth and bulk transactions are not claimed by this steady-state
mix. Separate blind writes from RMW in results; an unsupported RMW path blocks
that cell rather than silently converting it to put.

Each stream record has a deterministic key, one 16-byte header name and a 64-byte
header value. Readers consume and verify the stream continuously using explicitly
fixed record/byte caps in the manifest. Proposed ordinary read cap is one record,
including encoded metadata in the byte limit. Never treat b100 as 100 independent
append-latency observations. Large reader batches need their own approved budgets.
A 16 KiB encoded-header stress record is a separate diagnostic workload proposal,
not a codec limit and not an ordinary throughput cell.

Resource stage per lane: one fixed 10-minute steady-state measurement PER MODE, following
a fixed 2-minute warmup for each (24 minutes combined), at a prespecified offered load (pending separately;
not derived from favorable results). Indexed uses 1 KiB / 100,000-live state and
keeps one cold live key near the leading WAL segment; the remaining state is
updated cyclically. Include 90% hot-key traffic, conflicting unique updates and
cancellation in separate deterministic diagnostic subphases with exact outcomes.
Streams use 1 KiB / b100 with active consumers and real retention. Preload to the
1 GiB retention target with sealed 128 MiB segments before the warmup. The fixed
approved arrival count must roll at least one further segment during measurement;
verify actual reclaimed segments/bytes and documented slow-consumer out-of-range
behavior if eligible. No reclamation at all is incomplete coverage, not PASS.
Range interference is limited to 1,000 and 10,000 exact rows of the 1 KiB indexed
population. The former 100,000-row query is deferred, not silently replaced with
fewer results. No unrelated-collection independence claim without a separate
same-storage-identity probe. Offsets/consumer guarantees still require eligibility.

## Revised memory and storage proposals

| Quantity | Proposed screening ceiling / obligation |
| --- | --- |
| Engine process RSS high-water mark | 2 GiB, including startup, data loading, index rebuild and operation buffers |
| Client/generator RSS high-water mark | 256 MiB, separate process/cgroup accounting |
| Engine cgroup memory peak | 4 GiB including charged file cache; observe with recorded isolation semantics |
| Client cgroup memory peak | 512 MiB including file cache; distinct from client RSS |
| Outstanding generator requests | At most 16; scheduled backlog, timeouts and refusals counted separately |
| WAL queued bytes / max batch | 64 MiB / 16 MiB, matching current defaults; not the whole memory budget |
| Fixed-live engine RSS growth | At most 64 MiB between the median of minutes 1-2 and minutes 9-10 of the resource measurement |
| Retained bytes | Streams: 1 GiB + one 128 MiB active segment + fixed manifest allowance; deleted-but-open reader-pinned bytes separately capped at 1 GiB + 128 MiB in the declared one-delayed-snapshot diagnostic |
| Indexed disk after drain | At most 4x exact live encoded state + two 128 MiB segments; total job database files capped at 6 GiB |

These are proposed requirements, not observed ability or gate exceptions. Account
for every waiting caller, decoded batch, index delta, key reservation, future,
receipt, generator buffer and storage queue. The 64 MiB WAL admission cap does not
bound allocations made before admission. OS high-water counters must capture
between-sample peaks; report periodic samples/gaps and memory.events/oom counts.
If cgroup isolation/peak accounting is unavailable, that memory claim is
UNJUDGEABLE; do not substitute RSS and call it equivalent. A 10-minute stable
result cannot establish absence of leaks over hours or at larger working sets.

Real compaction/retention must make progress, reclaim actual bytes and respect
bounds. Do not delete the cold live key, remove indexes, disable consumers or
shrink a dataset to pass. Stop on a resource limit; drain admitted work under the
existing settlement policy. Maintenance drain is separately proposed at 30 seconds
and cannot extend write settlement. Streams with delayed pinned readers must
account for the extra pinned bytes and prove release within the approved policy.
Pinning is segment/snapshot granular: core `Wal::scan_from_loc_capped` clones file
handle Arcs from the selected start through the tail. A one-record read can retain
many 128 MiB files; there is no proposed 32 MiB physical-pinning limit. The controlled
scenario holds one snapshot spanning the declared retention window, measures ALL
deleted-but-open files across holders, and requires release within 30 seconds after
the read finishes or cancellation settles. Existing per-write bounds are unchanged.
If that actual state cannot be observed through a reviewed real-path probe, pinning
is BLOCKED, not measured as zero or approximated by decoded record length.

## Revised recovery proposals

Recovery is measured in distinct phases, not one ambiguous open time:
1. WAL open/replay and acknowledged durable state verified.
2. All declared indexes rebuilt and correct, constraints enforced, query service ready.
3. Stream resume/retention-floor/divergence checks complete, only if that API is eligible.

Fixtures contain 128 MiB and 1 GiB of ACTUAL retained WAL bytes, including encoded
records, with exact manifests of live keys/indexes/offsets. Construct indexed checkpoint-near-tail and no-checkpoint fixtures in Fast and
Durable: eight indexed cells (2 sizes x 2 checkpoint states x 2 modes), three
fresh-process repetitions per cell. Raw StreamLog has no checkpoint operation or
key index; it reconstructs its actual sparse read index on open. Stream recovery
uses four cells (2 sizes x 2 modes), three fresh-process repetitions each, with
sparse-index/offset integrity probes. Never construct a stream checkpoint via a
keyed adapter or invent a checkpoint API to satisfy the matrix. Preserve guarantees; Fast reissue/loss checks require the appropriate
fault model and consumer capability, not just a kill/reopen.

| Recovery phase | 128 MiB | 1 GiB |
| --- | ---: | ---: |
| WAL open/replay, data available | 1 s | 5 s |
| Indexed fully ready, including rebuild/constraint probes | 5 s | 20 s |
| Stream consumer-ready, including resume probes | 2 s | 10 s |

Clocks all start when the fresh process enters the public open/recovery operation;
phase ceilings are cumulative from that origin, not added to each other. Outside-
timing exact-data probes are still required before a result is accepted. Fixture
construction uses explicit recovery populations: indexed 50,000 live 1 KiB
records for 128 MiB and 100,000 live 1 KiB records for 1 GiB; stream 1 KiB records
with exact retained offsets/bytes. The ordinary 10,000 x 64 KiB working set is not
forced into the smaller recovery fixture. Use overwritten indexed generations to
reach retained size; no sparse padding or fake replay. Fixture generation cost is
recorded, not hidden as recovery. Construction uses public stream append batches
of 100 records and, when eligible, real IndexedStorage insert_batch/upsert_batch
with at most 100 records. The generator must calculate and freeze encoded bytes,
operation counts, overwrite generations, batches and finite duration BEFORE run,
including WAL control frames and checkpoint effects. Public capability or fixture
math that cannot meet the declared size/cardinality blocks construction; no direct
raw-KV bypass or invented stream checkpoint. Ten minutes is a fixed generation
allowance for the complete per-lane inventory, not permission to use fewer records. Every fixture must satisfy payload/live-state
requirements before timing; impossible combinations block, not truncate data.

Fresh-process restart on a runner with a warm OS page cache is explicitly labeled
WARM-FILESYSTEM recovery. True cold-storage recovery needs a separately approved
and attested cache/device procedure; do not label process restart as cold disk.
Process SIGKILL is not physical power-loss proof. Existing FaultFs/full crash
harness and production deployment recovery remain independent. T5's matched keyed
1 GiB comparison and Task 2.25's 1/4/16 GiB requirements are unchanged; this first
screen is not their replacement. The 4/16 GiB deployment suite remains deferred.

## Bounded schedule and result

Ordinary cells: proposed 30-second warmup + 120-second measurement each; one source
per dispatch, no implicit candidate/baseline comparison. Stream ordinary native cap
25 minutes, indexed cap 15 minutes. Short screens report actual terminal counts and
observed tails; they cannot certify p99.9 or the unapproved capacity minima.
Preserve the entire original benchmark grid when running its independent gates.

Resource/recovery runs use separate per-lane jobs, each with a 55-minute native
cap. The maximum schedule is predeclared: fixture generation 10 minutes; resource
warmup/measurement 24 minutes for BOTH modes; recovery attempts at most 30 seconds
each (24 indexed attempts = 12 minutes; 12 stream attempts = 6 minutes); diagnostics
6 minutes; drain/validation 3 minutes. Thus indexed maximum 55 minutes, stream
49 minutes. Recovery's 30-second process guard is an execution limit, never a
replacement for the tighter phase ceilings. Build/preflight have a separate
25-minute job allowance and artifact publication 10 minutes, fitting the total
90-minute job deadline. These maxima include failed attempts; no reset. A manifest must prove the complete proposed schedule fits before dispatch;
otherwise refine the pending schedule BEFORE approval, not during execution.
All caps are aggregate/nonresetting. Deadline exhaustion is UNJUDGEABLE, not a
favorable retry or permission to skip a required cell. Stop a lane at its first
required failure. One separately reviewed cause-backed correction is allowed by
the existing direction; this draft authorizes no rewrite or successor prototype.

Report SCREEN PASS / FAIL / BLOCKED / UNJUDGEABLE by lane, capability and metric.
Do not average away failed cells or let the stream result offset indexed failures.
Rates/latencies, overload admission, malformed data, unique conflicts and recovery
must retain typed outcomes. An unsupported indexed 250/750 ms end-to-end deadline
remains BLOCKED: current WAL adapter has per-step bounds with 6.4 s default, unlike
StreamLog append_timeout's single duration. No wrapper cancellation or flush tuning
may fake a new contract.

## Pending decisions and next step

The maintainer has selected both lanes and GitHub SCREENING ONLY, and specified
revision priorities. The maintainer approved the revised screening protocol, working sets,
memory/storage/recovery ceilings and fixed execution envelopes on 2026-10-13.
The approval explicitly leaves offered loads to be set and production qualification
deferred. The protocol must still pin exact read byte limits and fixture/capability
attestations before dispatch; it makes no qualified statistical tail claim. Revision 1 throughput and latency numbers remain unapproved, not
silently adopted or lowered. No native timing, production qualification or harness
implementation follows automatically from this documentation task.

Next: separately specify/test-first the P0 eligibility manifest. It must reject
missing offered loads or exact read/fixture/host evidence before any run. Full production host, memory efficiency at larger cardinality,
cold recovery, sustained load, transaction/event guarantees and competitor parity
remain explicit future requirements, not screening passes.
