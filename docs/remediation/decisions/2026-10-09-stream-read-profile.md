# Stream read profile: prepared checkpoint

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

## Required next step

Actions was selected and its temporary permission amendment was explicitly
approved. The next steps are to commit/publish the validated amendment and
dispatch the first actual fixed CPU profiling round. The hosted VM's wall time
remains CI trend evidence.
The helper checks perf availability and kernel permissions. Missing tools or
permissions stop execution with a maintainer installation/host request. The helper
never installs perf or changes sysctl settings; the approved workflow provisioning
amendment is documented below. It never substitutes Mac timing or retries a round
for favorable samples.

After host selection, reviewed local checks, and a milestone push, the Actions
option can use these commands from the task worktree:

```bash
profile_sha=$(git rev-parse HEAD)
git push origin wip-2.15b.8-read-profile
gh workflow run remediation-gate.yml --ref wip-2.15b.8-read-profile \
  -f ref="$profile_sha" -f phase=2 -f probe=stream-read-profile
```

Record the run ID, workflow definition SHA, resolved source SHA, artifact hashes
and actual qualification result. Do not dispatch another same-ref run concurrently.
One execution round means three chronological repetitions, not three attempts to
get a good result. `qualify` may reparse retained files without rerunning perf.

Only a qualified profile can support the next production change. Validated-frame
reuse is a reviewed candidate, not an implemented or measured speedup. If the
profile does not support it, return that evidence before choosing a different
design. Preserve all existing durability checks and performance floors. A/B stay
frozen and C stays retired.

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
