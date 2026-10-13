# P0 implementation: static feasibility eligibility inventory

Status: implementation contract for the approved GitHub-screening R2 plan.
Use test-driven-development, isolated worktree and read-only independent reviews.
Root owns mutations/builds/integration; reviewers do not build. No timing command.

## Scope and trust boundary

A static inventory checks pinned source, approved requirements, selected capabilities,
ledger completeness, declared prerequisites/loads/evidence references. It NEVER
executes a benchmark, declares tests passed, verifies remote CI, or authorizes
production/timing. Successful synthetic controls mean READY_FOR_REVIEW only.
All reports have timing_authorized=false, production_qualified=false and
measurements_performed=false, including positive controls. Real execution remains
P1/P2, requires reviewed actual evidence and separate dispatch. A commit containing
code is evidence of integration, not evidence that performance gates passed.

Files:
- scripts/production_feasibility.py (stdlib Python3.11+, CLI + pure inventory logic)
- scripts/test_production_feasibility.py (unittest controls + actual CLI/repository tests)
- docs/benchmarks/production-feasibility.toml (approved R2 values; intentionally unpinned source/loads/prereqs)
- docs/superpowers/plans/2026-10-13-feasibility-eligibility.md (this contract)
- docs/remediation/decisions/2026-10-13-feasibility-eligibility.md (actual source findings/report instructions)
No engine, API, ledger, old gate or CI changes. Full pre-push verifies task before
integration; this Python test suite is invoked explicitly, not assumed included.

## Manifest schema1 (exact keys; unknown/missing keys reject)

Top-level: schema=1, profile="github-screening-r2", source_sha (blank template or
full40lowercasehex), protocol_sha256 (approved actual R2 file digest),
loads_approved (strict bool), modes=["Fast","Durable"], exclusions (exact declared
transactions/outbox/ttl/cluster/legacy_event_consumer scope), requirements table,
loads table, prerequisites table, evidence table.

Requirements table pins actual approved values, strict integer types:
engine_rss_bytes=2147483648, client_rss_bytes=268435456,
engine_cgroup_bytes=4294967296, client_cgroup_bytes=536870912,
rss_growth_bytes=67108864, queued_bytes=67108864, batch_bytes=16777216,
outstanding_requests=16, indexed_1k_count=100000,indexed_64k_count=10000,
stream_retained_bytes=1073741824,segment_bytes=134217728,
pinned_bytes=1207959552,resource_seconds_per_mode=600,
resource_warmup_seconds_per_mode=120,ordinary_warmup_seconds=30,
ordinary_measurement_seconds=120,stream_recovery_attempts=12,
indexed_recovery_attempts=24,native_resource_seconds=3300,job_seconds=5400,
wal_ready_128m_ms=1000,wal_ready_1g_ms=5000,
index_ready_128m_ms=5000,index_ready_1g_ms=20000,
consumer_ready_128m_ms=2000,consumer_ready_1g_ms=10000.
New revisions require review; edited config cannot waive these through a switch.
This is an approved-input check, not a runtime measurement.

Loads:streams_fast,streams_durable,indexed_fast,indexed_durable, explicit finite
positive numeric request/batch-operation rates, bool not number. Integer rates
must fit signed64bit TOML/counter representation; floating rates must be finite.
Overflow/malformed numbers produce BLOCKED JSON, never conversion traceback. Zero is only an
UNSET template sentinel and blocks; it is never emitted as a benchmark result.
Prerequisites:index_maintenance,stream_consumer; blank or full40SHA. Nonblank must
be ancestor of observed source; branch-only commits do not count as integration.
Evidence:local_checks,linux_perf,host,fixtures (relative tracked paths, blank when
unavailable). Nonblank references must resolve inside repo, be tracked, and name
nonempty files; P0 only verifies references, not their claims/content/CI truth.
Missing evidence blocks review readiness. No fake host or native-output fixture
is used to qualify current source; unit fixtures remain explicitly synthetic.

## Ledger/source contract

Required baseline rows (frozen from integrated source7e493449; deleting/moving a
known row blocks globally): storage STO-01 through STO-20 and STO-24; streams
KEY-09,STO-21,STO-22,STO-23; keys KEY-01,KEY-02,KEY-03,KEY-05; events EVT-07.
Raw stream requires storage+streams; native consumer adds EVT-07; indexed requires
storage+keys. KEY-05 is explicitly PRESENCE-ONLY for this single-node profile:
its medium multi-Raft catalog behavior is outside the approved cluster exclusion.
It remains unresolved in the full production inventory; it is never marked fixed.
All other required storage/streams/key rows require closure as scoped below.
Preserve baseline area's identity, dynamically include new relevant
critical/high findings. Closure accepted only fixed/verified with evidence.
Statuses wont_fix/duplicate are recognized but NOT accepted as closure: affected
required findings need reviewed decision/canonical resolution, otherwise BLOCKED
with CLOSURE_REVIEW_REQUIRED. Other statuses are malformed. Areas tests AND testing
are legitimate. Unsupported ci-job/xtask regression grammar is an unresolved
reference capability, not invalid ledger syntax. This task does not invent its
validation or silently treat it as a passing test.

The requirements table is a STATIC subset; the complete approved R2 protocol
remains authoritative via its frozen digest. P1 must materialize and validate ALL
runtime inputs omitted from this inventory. READY_FOR_REVIEW never waives them.
Prerequisite ancestor SHAs are DECLARED REFERENCES, not verified task completion;
regression fn existence is static presence, not test success.

Read ledger TOML from observed repository and validate shape/status/types/duplicate
IDs and phases, required shipping rows, fixed findings' changes/regression refs.
Relevant critical/high storage findings block both lanes; streams-area findings
block raw streams; critical/high keys block indexed. KEY-09 remains required
stream routing evidence. Consumer requires EVT-07 (including medium) and integrated
consumer task; indexed requires KEY-02 and integrated2.17. Native resume excludes
legacy EVT-06 by declared API; legacy event/outbox/transactions/TTL/cluster never
become shipping claims merely because excluded from engineering timing scope.
Report all unresolved critical/high ledger rows separately as production inventory,
and Phase2+ gate state; no global production-ready result.
Missing required IDs, unknown statuses/severity/area, missing regression references
or empty ledger cannot make an existing blocker vanish. Resolve supported test:path::fn
and script:path references against tracked nonempty files and exact Rust fn names;
reject traversal/untracked references. Use fixed repository paths, no remote URLs
or arbitrary subprocess execution. Other reference grammar blocks pending review.
Source HEAD fullSHA, tracked/untracked dirty state, protocol digest, ledger and
manifest digests are included. Git snapshot derives from actual read-only argv
commands; no shell expansion. Approved protocol commit064b7e0 must be integrated.
Missing/incorrect source pin, dirty checkout or unknown/unmerged prerequisite blocks.

## CLI and result

`python3 scripts/production_feasibility.py inventory --repo <root>
 --manifest docs/benchmarks/production-feasibility.toml [--output <artifact.json>]`
stdout is complete JSON. Exit2=BLOCKED, exit0=READY_FOR_REVIEW. Invalid/unavailable
input gives machine-readable BLOCKED with reason, never broken JSON or zero data.
Top fields:schema,kind="static-eligibility-inventory",status,source,requirements,
capabilities,lanes,production_blockers,production_inventory,phase_gates,limitations,three false flags.
Capabilities:stream_append_read,stream_consumer_resume,indexed_data. Each has
status/reasons/finding IDs; stream lane combines its two capabilities, indexed
separate. Global reason applies to every capability. Stable deterministic reason
codes, sorted unique IDs, no selected-cell average. A missing source may omit
unavailable fields explicitly as null, never invent identity or read arbitrary data.
Output atomic/non-replacing, reject ALL existing outputs (including tracked
files/inputs and symlink aliases); ledger/protocol/regression source reads require
tracked nonsymlink in-repo files, including the resolved target. Default
stdout-only. Tool performs no network, builds, benchmark, kernel or ledger mutation.

## Test-first steps

- [x] Plan reviewed independently; explicit baseline ID sets/closure states added
  following source review. Final plan check before production code.
- [x] RED tests for open KEY-02, EVT-07, new critical storage finding, missing ID,
  duplicate/malformed ledger, fixed finding missing regression/fix/reference,
  missing/stale source, dirty repo, unmerged task, approved-protocol mismatch,
  modes/scope omissions, edited/unknown/missing budgets, unset/NaN/Inf/bool rates,
  missing/untracked/outside evidence, and stale protocol approval.
- [x] Positive pure synthetic all-evidence control is READY_FOR_REVIEW yet three
  execution/certification flags false. Baseline real-source control remains BLOCKED.
- [x] Actual CLI emits JSON/exit2 for current template, malformed input, missingrepo;
  refuses input/tracked-file output overwrite without changing source. No network.
- [x] Run tests first, record expected missing-feature failures (use missing-script
  CLI test with explanatory assertion; no import-error-only RED claim).
- [x] Implement minimal stdlib schema/inventory + read-only Git/reference snapshot;
  strict failure states and atomic output. GREEN all tests; causal negative controls.
- [ ] Commit implementing step and update handoff; generate current inventory after
  commit on clean source. Source template stays unpinned/loads unset by design.
- [ ] Independent implementation review, full local pre-push + explicit Python suite;
  reviewed integration, regenerate current inventory on actual integrated SHA.
- [ ] Record findings/evidence and remaining exact next step, no status/gate waiver.
  Preserve reports and clean task target/worktree.

## Implemented verification checkpoint

32 Python tests pass, after missing-command RED and causal malformed-number,
tracked/external/cyclic-symlink and full-inventory RED controls. Two isolated
negative mutations (disable keys validation; forge production flag) are killed
by the intended assertion tests with no errors. Contract review approved round3;
independent code quality review approved. Full pre-push check exit0:1509nextest
pass/10existing skipped, concurrent workspace tests/doctests,200blocking seeds
per mode, format/lint/ledger/render/static rules/README/doc claims. Root owns these
checks; reviewers did not execute builds/tests. No new benchmark/CI/native run.

## Historical STOP: source-pin fixed point discovered after integration

Tracked manifest source_sha cannot equal its own clean commit SHA. Implementation
stopped; proposal at main .agents/verification/feasibility-eligibility-2026-10-13/SOURCE-PIN-PROPOSAL.md.
Keep approved requirements tracked and propose separate exact-source ignored run
artifact, pinned back to the tracked template. This was the STOP state before approval;
the maintainer-approved resolution and implementation contract follow below.

## Approved correction: exact source identity without self-reference

The maintainer approved a separate Git-ignored, untracked run artifact on
2026-10-13, resolving the source-pin STOP above. R2 product requirements remain
unchanged. Keep --manifest at the fixed tracked template path; add explicit
--run-manifest <relative-path> for a per-run full schema1 TOML plus exactly one
additional key, template_sha256. The template is validated separately. Bind its
checkout bytes to the actual observed HEAD blob, and bind the run file to that
template digest and exact HEAD. Only a regular untracked AND ignored file physically
under .agents/verification is eligible; reject leaf/ancestor symlinks and external
paths. Read/hash/parse one bytes snapshot per input. Include template/run hashes
separately and the parsed run bytes as a content-addressed snapshot in the report.
Default template remains unset and BLOCKED. Preserve all prior static checks and
three false flags. Source/template/ledger/reference checks are not certificates.

Test-first: a disposable local shared clone retaining real 064b7e0 approval
history, with committed synthetic ledger/evidence and run file generated AFTER
its source commit. Require clean Git before/after, READY_FOR_REVIEW only, separate
hashes and snapshot; negative controls cover stale/dirty source, dirty template,
wrong digest, changed/unknown budgets, missing evidence, unset loads, tracked,
unignored, outside and symlinked artifacts. Review read-only, verify, commit on
existing task branch, integrate, regenerate actual-source BLOCKED inventory, and
remove only the completed worktree/cache. No native measurements or CI dispatch.

## Correction implementation and review checkpoint

40 Python controls pass, including the real clean-Git descendant positive,
artifact/input negative controls and retained original inventory checks. An
isolated template-binding-disabled mutation is killed by the exact negative
assertion without test errors or source edits. Independent contract and quality
reviewers approve the four-file correction read-only, with no blockers. The
requirements template itself remains byte-identical. Full local checks and
post-integration source inventory are recorded in the task handoff/evidence.
The source model is resolved; actual database eligibility remains BLOCKED.
