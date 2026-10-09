# Stream read profile: prepared checkpoint

Date: 2026-10-09. Base: `797f8c679b1d9f6922f4afb1fe0fff4ba928735e`.
Branch: `wip-2.15b.8-read-profile`.

The profiling infrastructure is prepared. Linux host selection and qualification
are pending. No CPU profile, production correction, or new speed result has been
produced. The previous benchmark misses remain unchanged.

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

Choose the existing GitHub Actions Linux runner for CPU diagnosis, or provide a
dedicated Linux host. The hosted VM's wall time would remain CI trend evidence.
The probe first checks perf availability and kernel permissions. Missing tools or
permissions stop execution with a maintainer installation/host request; it does
not install perf, change sysctl settings, substitute Mac timing, or retry a round
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
