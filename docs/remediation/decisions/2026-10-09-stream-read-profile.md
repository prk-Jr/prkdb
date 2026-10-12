# Stream read profile: saved recording recovered and qualified

Date: 2026-10-09. Base: `797f8c679b1d9f6922f4afb1fe0fff4ba928735e`.
Branch: `wip-2.15b.8-read-profile`.

The profiling infrastructure is prepared and GitHub Actions was selected.
[Run 37908155101](https://github.com/prk-Jr/prkdb/actions/runs/37908155101) failed
host qualification at source and workflow SHA
`77af28216db88ca693c447274dec7cc9a28541e9`. The runner has perf installed but its
`perf_event_paranoid=4` policy refuses `perf stat -e cpu-clock:u -- true`.
The build and recording steps were skipped: no CPU samples, production correction,
or new speed result was produced. The previous benchmark misses remain unchanged.

Downloaded run/job metadata, complete Actions logs and both preflight artifacts
are retained and SHA256-hashed under the main checkout's ignored
`.agents/verification/stream-read-profile-2026-10-09/actions-37908155101/`.
This is an infrastructure permission failure, not mutation survivors or an observed
product regression. Ordinary gate jobs were skipped as expected for this probe.

## What will be measured

The existing `stream_read/tail/1k` cell cycles through the full log. It contains
256 MiB of values in 256 keyed batches of 1,024 records, uses uncompressed Fast
setup followed by sync, and reads with a 1 MiB limit. The probe keeps the existing
one-second warmup, three-second nominal measurement, and three repetitions.

Optional hooks check every stored field against the append inputs before warmup.
The profiler records the whole process, then attributes only samples within the
three measured CLOCK_MONOTONIC intervals. Setup, validation, warmup and close are
excluded. Reported costs are period-weighted exclusive leaves and overlapping
inclusive callchains. This cannot establish unprofiled throughput, physical disk
bytes, allocations, blocked time, instruction counts, or competitive performance.

The [design](../../superpowers/specs/2026-10-09-stream-read-profile-design.md)
fixes the workload and quality rules. The [plan](../../superpowers/plans/2026-10-09-stream-read-profile.md)
records the remaining execution steps. Independent reviews closed the actual perf
multiline grammar, hidden throttle events, incomplete-round requalification, and
inclusive interval-end issues. Raw events, commands, successful exit statuses,
binary/source hashes and phase artifacts are mandatory; failed evidence stays
unqualified. Tests and logs live in the main checkout's ignored
`.agents/verification/stream-read-profile-2026-10-09/`.

## Actual fixed round and failure classification

[Run 37909749321](https://github.com/prk-Jr/prkdb/actions/runs/37909749321)
used workflow and source `06965f2cbad61628488a423106084fdc34fe0be4`.
Host qualification, all 53 Python tests and the pinned Linux release build passed.
Recording and all eight extraction commands returned zero. The final evidence
qualification failed because 483 of 1,310 measured leaf samples were unresolved
(36.870229%), exceeding the unchanged 10% limit. Phase counts are 446, 427 and 437;
the unfiltered recording contains 1,910 samples. Of the unresolved leaves, 476
belong to libc and seven to vDSO. This does not support qualified CPU attribution
or a production optimization decision.

The run is red because of symbol-resolution quality, rather than known mutation
survivors or an observed product regression. Ordinary gates were skipped for this
isolated probe. The approved permission step changed the original value 4 to 2,
and the always-run restoration successfully restored 4 before artifact upload.

Run/job metadata, complete Actions logs, the exact benchmark and raw recording
are retained under the main checkout's ignored
`.agents/verification/stream-read-profile-2026-10-09/actions-37909749321/`.
All 43 original artifact hashes match, all nine retained command statuses are
zero, and the raw loss/throttle and stderr diagnostic checks pass. The original
`qualification.json` and source manifest remain failed and unchanged.
The perf.data SHA256 is
`11e4022abb2edcc29aaef169b39eb6d4796f775a7e65f3a848de36e1a203eb3b`.
The source-manifest SHA256 is
`d120181c09c8452f5b54649da8bb43f65877601f93860ce4970e52f06429fa7a`.

## Authorized recovery in progress

Do not rerun this measured round. Exact-build-ID glibc debug symbols may recover
the same recording. Ubuntu debuginfod returned404; official libc6/libc6-dbg
2.39-0ubuntu8.9 amd64 packages contain the recorded debug-ID file. Recovery must
verify executable/debug ELF IDs and pinned hashes before using them. An independent
read-only review supports this technical approach while confirming that the
current failed-manifest policy forbids qualifying it through the existing helper.

The maintainer instructed "Do the needful" after the concrete proposal, authorizing
the separate offline derived analysis. The design preserves immutable original
failed evidence, exact symbol-file identities, unchanged sample cohort and every
original quality rule. Incomplete, failed-command or lossy recordings remain
ineligible; ordinary failed-manifest rejection stays unchanged. Implementation
and test-first verification are complete:73Python tests, independent review,
actionlint and fullprepush pass. The first offline extraction result is recorded below.

Only a qualified profile can support the next production change. Validated-frame
reuse remains a reviewed candidate, without an implemented or measured speedup.
Prior benchmark misses and thresholds remain unchanged. A/B stay frozen and C
stays retired. No task integration or production correction occurred.

## Approved temporary permission amendment

The maintainer explicitly approved the change. It sets `kernel.perf_event_paranoid=2` only in the disposable
profile job, records its original value, and restores that value in an `always()`
cleanup step before artifact upload. It makes no persistent sysctl configuration
change. The profiling process stays unprivileged, the event remains `cpu-clock:u`,
and workload, sampling, quality thresholds and all durability/performance gates
stay unchanged. [Linux perf security documentation](https://www.kernel.org/doc/html/latest/admin-guide/perf-security.html)
describes level 2's per-process userspace scope and kernel-profile restriction.

The execution policy above forbids implicit sysctl changes. This explicit approval
authorizes only the temporary job-level provisioning described here; the helper
remains unable to modify host settings. Setup and restoration logs must be retained,
and a restoration failure must fail the job. The first attempt recorded no CPU samples; a subsequent run
after approved host provisioning would be the first actual fixed profiling round,
not a retry to obtain favorable measured data.


## First offline extraction and metadata correction

[Run37916402867](https://github.com/prk-Jr/prkdb/actions/runs/37916402867), analysis
revision4ba16f7, fetched the exact glibc symbols, then failed strict physical-stack
identity. Inline expansion changed presentation; twelve vDSO-led stacks also failed
to unwind without the recorded vDSO ELF. This is an analysis-metadata failure,
not a product regression or mutation-survivor result. No new CPU samples were taken.
All original hashes remained unchanged. Preserve this derived failed qualification.

The downloaded derived bundle contains 374 verified present files and 274 missing
hidden `.build-id` files (no present hash mismatches). Future generated evidence
uploads explicitly retain hidden files; the old bundle remains incomplete as saved.

The bounded reviewed correction uses explicit `--no-inline`, the exact recorded
vDSO GNU ID f0566cac49ca64809e998c75b1373572e3fbc598, and the actual perf cache
under symfs/.debug. Candidate, actual lookup copy and all used symbols must be
verified, confined and hashed. Exact ordered physical stack/sample comparison and
all original quality thresholds remain unchanged. No qualified CPU cost or speed
claim is made until the corrected offline extraction passes those checks.


### Empty perf probe-cache metadata

Offline run [37918798049](https://github.com/prk-Jr/prkdb/actions/runs/37918798049)
verified the exact recorded/candidate vDSO ID, then refused an unexpected cache
entry: an empty regular `probes` file beside the ELF. Perf's SDT cache creation
commits this auxiliary metadata even when there are zero tracepoint definitions.
Preserve that failed extraction. Allow only an optional owned, regular,
non-symlink, zero-byte probes file (SHA256 of the empty file), retain its path/hash
and recheck it. Refuse nonempty probes, additional ELF/debug sources, symlinks or
escaping paths. The actual materialized lookup remains exactly `elf` and `vdso`.
Physical stack/sample identity and every frozen quality check remain unchanged.
This is offline metadata handling; no new CPU recording or database change.


## Qualified same-recording result

[Run37922246060](https://github.com/prk-Jr/prkdb/actions/runs/37922246060) passed
at analysis/workflow3fa91aa93e2c7efc6ce3c95b8d5ce9fd3a807fbb. The benchmark
recording remains source06965f2; recovery recorded no CPU samples. All83 Python
tests passed in Actions. All678 derived artifact hashes verify, including hidden
symbol/cache files; none missing or mismatched. Original43 hashes and failed
flags remain unchanged. Exact ordered physical sample/stack comparison passes
for all1910 samples. Measured counts remain1310, split446/427/437. Unknown leaves
are7/1310 (0.534351%), below the unchanged10% limit; those seven vDSO leaves remain
unknown. Original loss/throttle/unwind checks and all extraction statuses pass.
Source/parser, recording and supplied/actual-used symbols verify before and after.

Period-weighted exclusive CPU leaves in this fixed workload:

| Leaf | Sampled userspace CPU |
|---|---:|
| CRC32 AVX2 calculation |32.75%|
| libc memmove |27.25%|
| Records body decode |9.54%|
| libc memset |9.08%|

CRC leaves whose physical stack contains segment::read_frame account for20.08
percentage points of the total sampled CPU; scanner CRC leaves account for12.67.
These are attribution evidence, not predicted speedups. Inclusive costs overlap
and must not be summed. The data supports evaluating the already-reviewed
validated hinted-frame reuse candidate: eliminate duplicate reading/checking of
the same immutable validated bytes while preserving CRC/decode checks for every
unseen frame and all existing fallback/cap/snapshot invariants. Production work
requires its own claim, safety review, deterministic RED/GREEN and unchanged
paired performance gates. No production change or speedup was delivered here.

Qualification is pooled CPU attribution only. Repetition stability, blocked time,
physical bytes, allocation counts, unprofiled throughput and competitive timing
remain unqualified/unmeasured. Prior T1/T3/T4/T5 misses stay open; A/B frozen,
C retired. Failed original and both failed offline analyses are retained.

Final metadata checks:83Python tests, actionlint, diff/docclaim checks and
independent review pass. Fullprepush passed at4ba16f7 (1509 nextest,10skipped,
concurrent workspace tests/doctests, both200seed crash modes, ledger/status/script
checks); subsequent changes are Python/workflow/docs only with unchanged Rust.
Criterion and instruction-count comparisons do not apply to this infrastructure
change. The owned Mac target was cleaned (11387files,6.3GiB logical), with other
build directories left untouched. Task branch remains separate from phase-2.
