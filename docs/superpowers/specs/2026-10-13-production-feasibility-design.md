# Production feasibility: separate stream and indexed gates

Date: 2026-10-13. Status: DRAFT requirements awaiting maintainer approval.
The maintainer selected BOTH lanes, with separate gates. This task defines the
concrete gate; it does not authorize shipment, another architecture prototype,
benchmark reruns or implementation on ineligible source.

## Authority and goal

The remediation spec, plan and ledger win. This concretizes the accepted evaluation
direction at wip-perf-architecture-eval commit8eab7519e2e8510c606098e5d6d8c22a46cea5e7
(docs/superpowers/specs/2026-10-09-competitive-architecture-evaluation-design.md).
That source remains recoverable from its preserved branch; its report snapshot is
in main .agents/verification/worktree-cleanup-2026-10-13/reports/.
No old experiment is reopened. Keep B/C retired, frame reuse unmerged and all
original failures visible. A lane cannot offset another lane's failed criteria.

Goal: decide whether an explicitly supported production profile has correct
semantics, bounded latency/resources and sufficient sustained capacity. Passing
microbenchmarks, Rust memory safety or high batch throughput cannot certify it.
The gate is additional to T1-T5, Task2.17 Fast>=95%/Durable>=97%, its InMemory
requirement, the unchanged 5% instruction rule and full remediation gates.

## P0: eligibility before timings

Pin the entire assembled source, feature flags, regression tests and APIs actually
advertised. Check relevant ledger findings and required phase gates on that source,
not historical passing worktrees. Performance never clears a correctness blocker.

| Profile requirement | Current blocker / required destination |
| --- | --- |
| Indexed unique constraints, acknowledged index publication, RMW | KEY-02 and Task2.17 unmerged; verify every relevant KEY finding and reviewed regression suite |
| Atomic indexed transactions and serializable default | TXN-01..04, Phase3 tasks3.1..3.3; current source is not eligible |
| Resumable stream consumers/offsets and loss/divergence handling | Task2.15b.6, EVT-07, and exact consumer API's generation/assignment requirements (EVT-06 if that API is used) |
| Atomic outbox/change feed | EVT-01..05, Task2.19/Phase3; optional only if excluded from the declared profile and public claims |
| Multi-node persistence/availability | Phase4 Raft gates; explicitly outside initial single-node gate |
| Atomic AI checkpoint/events/idempotency | Shared Phase6 M1 plus relevant Phase2-3 prerequisites; not established by separate stream and KV writes |

At phase-2 HEAD9b88229, indexed transactions and the full consumer profile are
BLOCKED. Fixes that exist only on branches cannot certify phase-2. A narrower
operation may produce explicitly scoped engineering evidence, never product-wide
production readiness. All critical/high findings in shipping surfaces must be
resolved and verified; unsupported operations must be explicitly excluded from
capability claims, not silently omitted from the test matrix.

Current verdict: STREAM full profile BLOCKED; INDEXED full profile BLOCKED.
No throughput run is needed to establish these current blockers. Eligibility is
rechecked after integration before building the production feasibility harness.

## P1: approve workload, hardware and requirements

These numbers are PROPOSALS for a four-core single-node embedded production-health
profile. They are not measured ability, competitive targets or adopted exceptions.
The maintainer must approve or replace them BEFORE measurement; record that decision
and immutable manifest. Never fit budgets to an implementation's results.

### Proposed hardware and resource envelope

Dedicated x86_64 Linux, four allocated engine CPUs, four separate client CPUs,
16 GiB engine RAM, local NVMe, explicit filesystem/mount/durability policy. Pin CPU
model, physical/logical topology, kernel, allocator, Rust1.98.1, governor, device,
FD limits, CPU quota and RAM. Shared Actions runners provide screening evidence;
production qualification needs an approved reproducible host/resource envelope.
If unavailable, report BLOCKED rather than substitute Mac or uncontrolled numbers.
Client threads/processes, generators and data-validation probes have separate
budgets; a Rust runtime cannot consume unbounded machine CPUs.

Use Durable and Fast separately; uncompressed first. Durable success means the
specified durable commit, Fast means written plus exact durable_lsn semantics.
Fast sync_interval is a target, not a power-loss bound. No fake fsync/empty hooks,
relaxed unique checks, unindexed substitutes or mock backends.

### Proposed end-to-end latency ceilings

Measure scheduled arrival to terminal response, including producer/client queue,
admission, key reservations, storage, index publication and callback work declared
inside the operation. Reader row excludes deliberate empty-poll wait but reports
consumer lag separately. Ordinary read/secondary-query requests fully materialize
exactly one matching record of the declared value size; predicates/unique indexed
values ensure that cardinality. Ordinary stream reads also return one record with
an approved byte limit including key and metadata. Multi-record reader batches
are not certified by this initial latency table; they need a separately approved
rows/bytes/SLO entry. Warm/cold and record/batch sizes remain distinct.

| Operation | p99 | p99.9 |
| --- | ---: | ---: |
| Fast write,1 KiB |10 ms|25 ms|
| Fast write,64 KiB |20 ms|50 ms|
| Durable write,1 KiB |25 ms|75 ms|
| Durable write,64 KiB |50 ms|150 ms|
| Ready warm read/query,1 KiB returned |5 ms|20 ms|
| Ready warm read/query,64 KiB returned |20 ms|50 ms|
| Cold64 KiB read |50 ms|150 ms|
| Consumer lag since write acknowledgement, healthy poller |100 ms|250 ms|

Apply identical approved write ceilings during actual retention/compaction.
Index query includes complete materialization; document result rows/bytes. Large
range requests have separate approved work/byte caps, not this small-read budget.
Report all outcomes, queue delay, deadline expirations and unresolved writes;
successful-only quantiles cannot pass. Errors/timeouts at declared normal load
fail the lane. The API's configured client/settlement bounds remain unchanged;
propose Fast client deadline 250 ms and Durable 750 ms. StreamLog's public
`append_timeout` accepts one duration covering admission and response. The indexed
WAL adapter derives `client_bound` from `max_flush_ms * 128` and separately bounds
admission and acknowledgement; its default is 6.4 seconds per step. It does not
establish the proposed indexed end-to-end deadline. That indexed request profile
is BLOCKED pending a separately reviewed contract. Do not tune `max_flush_ms`,
wrap/cancel writes, change settlement bounds or widen this requirement to fake
support. See `stream_log/log.rs`, `storage/writer_liveness.rs` and
`storage/wal_adapter.rs` on the pinned source.

Consumer lag starts at the operation's acknowledgement in both modes; record
append, visibility and durable-horizon timestamps separately. Fast acknowledgement
is not durable acknowledgement. A lag-since-durability result requires an actual
observable durable horizon, otherwise that additional metric is BLOCKED.

### Proposed minimum sustained capacity (aggregate per node)

Keep single-writer latency/scaling screens separate from the following multi-client
capacity requirements. These minima are product-health proposals, not Kafka/Iggy
parity. Existing relative targets remain independently necessary.

| Required cell,8 producers or clients | Fast | Durable |
| --- | ---: | ---: |
| Stream append,b1,1 KiB |100000 records/s|1000 records/s|
| Stream append,b100,1 KiB |250000 records/s|150000 records/s|
| Stream append,b1,64 KiB |2500 records/s|500 records/s|
| Stream append,b100,64 KiB |5000 records/s|2500 records/s|
| Indexed write component of mixed workload,1 KiB |25000 writes/s|1000 writes/s|
| Indexed write component of mixed workload,64 KiB |1000 writes/s|250 writes/s|

Same payload, real indexes, live cardinality and durability throughout. Offered
read/query rates are additional to the write rate; never label total requests/s
as writes/s. Absolute rates, aggregate memory/storage budgets and deadlines all
need maintainer approval. If a requirement is unattainable, report FAIL; do not
lower it during the run. Different product aspirations require a new requirement
revision and decision, not a retroactive pass.

## P2: first bounded screen, then sustained gate

Build a separate noninstrumented timing binary and diagnostic/fault binary from
the eligible assembled source, with source/binary/manifest digests and real-path
attestation. Correctness checks outside timing still cover exact values, keys,
headers, indexes, unique claims, ordered offsets, native rebuild and reopened data.
No profiler/compiler/symbol repair work belongs in this product workload.

First screen: predefined120-second warmup and300-second measurement for each
mandatory cell, three fixed alternating reference/candidate blocks when evaluating
an optimization. Reference must itself satisfy correctness and qualifying protocol;
be28e42's known qualifications cannot be waived. No extension for inconclusive
results, dropped outliers or favorable retry. Stop a lane on its first required
failure; preserve its complete prefix. Do not begin one-hour runs on a failed screen.

### Finite required matrix and execution caps

The matrix below is exhaustive for this initial single-node profile, not a
Cartesian product of every knob. A wider claim requires a new approved manifest.

| Stage | Required cells | Fixed native timing cap |
| --- | --- | --- |
| Ordinary capacity screen | The six capacity-table rows, each in Fast and Durable, 8 clients, 1 stream partition, uniform keys | 12 cells, 120 s warmup + 300 s measurement each |
| Scaling/contention screen | Each lane and mode: 1 client/uniform and 32 clients/90% hot partition or key; 1 KiB, stream b1, 4 partitions only in the 32-client stream case | 8 additional cells, same duration |
| Growing indexed screen | 1 KiB, 8 clients, uniform new keys, each mode; 200,000 to 1,000,000 records, fixed count and 300 s deadline | 2 additional cells, 120 s warmup + at most 300 s measurement |
| Qualified capacity staircase | Each of the 12 ordinary cells, offered rates at 1, 1.25, 1.5, 2 and 3 times its absolute minimum; fixed corresponding read/query rates | At most 60 cells, same duration; stop each staircase at first failure |
| Capacity verification | Each of the 12 ordinary cells at its frozen 100% screening capacity, one independent one-hour run | 12 measurements, 120 s warmup and at most 30 s drain each |
| Sustained qualification | Each of the 12 ordinary cells at 70%, 90% and 110% of its frozen qualified capacity, 3 independent Linux repetitions | 108 one-hour measurements, 120 s warmup and at most 30 s drain each |

Screen-only scaling cells diagnose and gate their declared liveness/latency; they
do not expand the certified sustained-capacity profile to 32 clients/4 partitions.
The growing screen has its own count/time completion verdict, not a stationary
capacity result. Timing work stops on the first required failure of a lane.
For initial qualification, 22 screens plus at most 60 staircase cells cost at most
9 hours 34 minutes of native measurement/warmup, capped at 10 hours including drain.
When comparing an optimization, three alternating reference/candidate blocks
replace the 22 screens: at most 23 hours including the same staircase and drain.
No optimization comparison is authorized here. Capacity verification plus sustained qualification has
an independent 125-hour native cap (120 measured hours plus warmup/drain). Every
stage cap is aggregate and nonresetting: calibration, warmup and incomplete
measurement still consume it. A cap exhaustion yields UNJUDGEABLE; it is not a
promise that every worst-case drain will finish. Rare
failure/recovery probes have a separate proposed 2-hour cap and bounded inventories
that must be reviewed in P1. Across stages, no favorable rerun or cell extension.
These are separate scheduled jobs on the approved host, not one giant Actions job.
The first failing screen provides an early answer; a full production certificate
necessarily takes longer than that screen.

### Sampling and capacity policy

The screening capacity reference is the highest passing fixed staircase rate
meeting observed normal-load latency, errors and absolute minima. Its source,
workload and schedule are frozen before any candidate run. A staircase alone does
not statistically qualify capacity: the separate fixed one-hour 100% verification
must meet final sample/latency/error criteria at that frozen rate, and therefore
also the absolute minimum. Failure does not select a lower staircase rung or
trigger recalibration; it stops the lane. Only after that verification is the
capacity qualified for the sustained 70%/90%/110% tiers. Failure at the first absolute-minimum step is FAIL. If the top
step passes, capacity/saturation is UNJUDGEABLE, not permission to extend the
staircase. At 70% and 90%, completed ordinary operations must sustain their frozen
offered rates with zero errors; those rates need not themselves exceed the
capacity minimum. The absolute minimum applies to qualified capacity separately.
At 110%, report admitted work, rejection outcomes and all-request latency separately
so fast rejections cannot mask slow writes. Actual saturation is required for an
overload verdict. The 70%/90% and 110% stages have separate outcome rules.

A latency sample is one terminal request: a batch of 100 records supplies ONE
append sample and 100 throughput records. Point reads, secondary queries and
writes each have distinct populations; no record duplication or pooling classes.
A 300-second screen reports sample counts, observed tails and clear failures, but
cannot certify final p99.9 when its counts are insufficient. Passing it means
eligible for the sustained stage, not a production PASS.

Proposed final minimum: 50,000 terminal outcomes per ordinary-operation class in
EACH fixed one-hour normal-load repetition, about 50 expected tail observations
at p99.9. Also require a prespecified one-sided 95% exact binomial upper bound on
the probability of exceeding each approved latency ceiling to be at most 0.01 for
p99 and 0.001 for p99.9. This is a per-cell statistical qualification, not a 95%
simultaneous confidence claim across the whole matrix. Report all counts and
observed quantiles alongside the bound. Independence assumptions must be stated;
correlated/burst phases are reported separately with contiguous-window worst
latencies, not concealed behind a binomial inference. If the approved statistical
model is unsupported, the confidence claim is UNJUDGEABLE even with enough rows.

At the slowest proposed normal cell, Durable 64 KiB b100 at 70% of the minimum
capacity gives 17.5 append calls/s, or 63,000 terminal samples/hour. Indexed Durable
64 KiB secondary queries give 105,000/hour at that load. Manifest preflight must
verify rate * duration PER operation class and allow for its declared interference
schedule. Insufficient actual samples are UNJUDGEABLE; do not raise load, pool
repetitions or extend the hour. Rare range/fault classes report exact observed
latencies and bounded completion, with p99.9 explicitly unqualified unless a
separate feasible count/duration plan was approved before execution.

Use deterministic offered-arrival schedules with per-operation IDs/timestamps,
separate client CPU, fixed bounded outstanding requests and all outcomes retained.
The loader must prove it generated the schedule; a client-side bottleneck is
UNJUDGEABLE, not an engine pass. Client queue/admission delay is included to avoid
coordinated omission. Closed-loop throughput cells remain separate.

### STREAM workload

Actual StreamLog, with client/partition/skew cells enumerated above; batches 1/100
and 1/64 KiB values remain separate. Ordinary records carry one 16-byte name and
64-byte header value. A diagnostic metadata stress case uses a proposed 16 KiB
aggregate encoded-header-bytes workload cap per record; this is NOT a codec/API
limit. Validate it against actual per-batch header count, per-record name/count
and 64 MiB frame bounds; no format change. Pin record/byte read
limits and consumer counts. Partition scaling preserves total load, not per-partition
work multiplication. Ordering guarantees are per partition; no global-order claim.
Concurrent producers/consumers plus actual retention at approved byte limit. Verify
no silent gaps/duplicates/divergence, committed consumer resume after retention,
reader lag, per-partition throughput and old-segment lifetime. Slow/stopped consumers
must report documented retention out-of-range behavior, not block unrelated producers.
Any consumer API still waiting on2.15b.6 stays BLOCKED.

### INDEXED workload

Real primary keys, one unique and one nonunique secondary index; production version
index also retained. Initial200000 live records for1 KiB,50000 for64 KiB; fixed
live population during soak. Separate grow/insert screen200000->1000000 records,
which must report its different final size instead of replacing the fixed-population
screen. Verify data and every index after each phase and close/reopen.
Mixed workload60% overwrite/update writes,30% point reads,10% secondary queries;
write-rate table denotes the60% component. Uniform and90% hot-key skew,1/8/32 writers,
conflicting unique updates, cancellation and callback-at-most-once semantics.
Keep blind insert, RMW and eligible transactions separate. Transaction cell performs
an atomic two-key transfer with invariant checking, read set conflicts and readers.
Within every ordinary indexed soak, use a fixed interference schedule: minutes
10-20 and 40-50 add one same-collection range query/s returning 1,000 rows; minutes
20-30 and 50-60 add one query/5 s returning 10,000 rows. In the 1 KiB population
only, minutes 30-40 add one query/20 s returning 100,000 rows. The 50,000-record
64 KiB population never substitutes fewer rows for a 100,000-row query. Keep
ordinary class offered rates unchanged. Proposed maximum completion times for
these rare range operations are 150 ms, 750 ms and 3 s respectively; report every
completion and maximum, not qualified p99.9 from sparse samples. Range probes must
return exact values from the same indexes contended by foreground writers.
Unrelated-collection point reads/writes add a fixed 10% of the ordinary request
rate throughout the measurement, split 50% writes and 50% point reads, with the
same applicable latency ceilings. At the lowest proposed 70% indexed 64 KiB
load this supplies 52,500 terminal samples per unrelated operation class/hour;
actual counts still need to qualify independently.
Every interference operation has its own outcome/sample accounting and bounded
client allocation. Both collections use the declared common storage identity and
single WAL. Do not hide interference by giving unrelated collections an
extra independent storage engine. Fair same-key FIFO and multi-key starvation
must be tested; approved source liveness requirements remain in force.

### Maintenance, overload and failure stages shared by both lanes

Require real reclaimed segments and bytes/s; a no-op compact/retention run cannot
pass. Pin log-state checkpoints, retained bytes and progress measurements.
Indexed overwrite soak includes one cold live key near the leading log segment;
measure disk/live-byte amplification over time. Proposed bound: on-disk bytes <=
4x approved live bytes +2 configured segment sizes after maintenance drain, with
no increasing dead-byte backlog. Stop on bound violation; do not delete the cold
key to enable reclamation. Stream retention bound: limit + active segment + fixed
manifest allowance, with deferred pinned-reader bytes independently bounded and
released within approved client/drain policy.

Proposed engine RSS cap8 GiB on16 GiB host and additional steady-state growth <=
256 MiB from the post-warmup sample over the final30 minutes at fixed live state.
Separate data/index/cache baseline from queued-request memory and bound the client
memory independently. `max_queued_bytes` bounds WAL admission, not every allocated
caller/index/key-wait state; attest every queue/backlog. Retain maxima, FD/thread
counts and all sampling gaps. A rising outstanding/unresolved backlog fails.

At110% of qualified capacity, prove actual admission saturation, bounded queues,
backpressure/typed outcomes and bounded drain. If not saturated, do not call it an
overload pass; use only the approved finite load staircase, without retargeting the
normal SLO/minimum. Queue budget64 MiB and batch16 MiB are initial proposals matching
current defaults, not a full-process memory guarantee. After offered load stops,
all admitted write outcomes must settle within 2x configured client_bound, and
maintenance must separately finish within its declared drain bound. Do not add
the maintenance allowance to extend the existing write-settlement requirement. Proposed maintenance drain30 seconds; uncertain writes
remain fenced or explicitly quarantined per approved implementation. Quarantine is
not successful recovery; operator reopen and exact state checks are separate.

Inject deterministic delayed IO, partial reads/writes, failed sync, cancelled and
never-polled callers, publication panic and process/power loss in fault-capable
runs. Preserve actual Durable/Fast loss guarantees. Prove an affected-key conflict
cannot hold unrelated operations indefinitely; shared storage failure may fail
closed across its dependents and must be observable, not disguised as latency.
No accepted callback may run twice. Fault runs may produce specified errors; normal
SLO success percentiles cannot hide those outcomes. No timeout widening/retry fix.

Recovery: cold start retained logs1/4/16 GiB with checkpoint near tail; proposed
ready-to-serve/index-ready budgets5/20/80 seconds respectively. Verify exact
recovered state, cursor divergence/floors, unique/index consistency and readable
receipt status before readiness. Lazy incomplete indexes do not meet readiness.
Stream T5's matched keyed1 GiB comparison remains independently required.
A process crash is not a faithful power cut; require existing FaultFs/harness
coverage plus separately declared deployment recovery tests, no invented proof.

## P3: result and finite engineering decision

Lane results PASS/FAIL/BLOCKED/UNJUDGEABLE, with completeness and exact supported
profile. All required cells must pass; no averaging or selected winning cells.
Open correctness blockers, missing host/approved requirements or incomplete data
prevent certification. A normal screen failure permits at most one reviewed,
cause-backed correction within its separate task before reevaluation; correctness
violations stop immediately and require a ledger finding/regression. No new B/C
or broad architecture prototype from this document. Failed correction returns the
choice to maintainer: targeted architecture change, narrower explicit product
profile, or stop that lane. Never silently narrow both-lane product requirement.

Competitive comparison is a later independent gate: actual equivalent public
server/embedded transport, replicas, fsync/ack guarantees, clients, batching, indexes
and workload. Kafka/Iggy/PostgreSQL versions/configs need primary documentation at
execution; embedded StreamLog versus network broker remains noncomparable.
No vector/GPU/AI-platform capability or universal production-readiness claim follows
from these two gates. Only the approved tested capabilities can be claimed.

## Approval and current status

Both lanes selected by maintainer. Latency, throughput, host, hour-tier workload,
resource and recovery values above are PENDING. No native timings or new harness
implementation starts until those requirements and source eligibility are settled.
Current blocked findings remain assigned to existing roadmap tasks; this document
changes no ledger status, phase gate, thresholds, API or on-disk format.
