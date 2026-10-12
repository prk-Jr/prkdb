# One bounded Linux WAL stall diagnostic

Date:2026-10-12. Maintainer approved the proposed baseline-repeatability,
alternating comparison and stall tracing cycle. This is diagnostic-only: no
production fix, architecture adoption, performance waiver or favorable-number retry.

## Scope and prerequisites
Base phase-2:9b88229d29cf4ea1a384b1357139f0b0f6c7f313. The original qualified failed
run38206215742 and unmerged frame-reuse correction stay immutable. Download its
wal-bench-a64f774f965806fb2cd93c91b276d504ec92208a artifact. Verify all28 inventory
hashes, both source manifests and binary manifest source/SHA identities, six
zero build/native/tee exits, and exact known binary SHA256 values before execution:
base7cbfc742202a97839b125e6a52720ab74d7fc185a1e65abbf40e5649bc1cfd38;
head1561972eedb4823765487b5996e99a995c888bf759c5f0ae252f6967ba3be4b7.
No rebuild changes layout or toolchain. No security findings or main edits.

Files: scripts/wal_stall_diagnostic.py; scripts/test_wal_stall_diagnostic.py;
.github/workflows/remediation-gate.yml (one new isolated probe); this spec,
companion plan and docs/remediation/decisions/2026-10-12-wal-stall-diagnostic.md.
Root owns edits/build/commits/evidence. Independent reviewer read-only. Old paused
root-owned CI file claim released for this diagnostic only; other claims untouched.

## Fixed diagnostic protocol
One Ubuntu24.04 x86_64 Actions job, four available CPUs/four Tokio workers, fixed
native environment, no other compiler/benchmark/perf process. Retain host/cgroup
CPU pressure/throttle, memory/IO pressure, CPU counters, disk stats and mount info
before/after each invocation; retain command/env allowlist, monotonic clock bounds,
raw stdout/stderr/exit and binary hash after execution. No concurrent native cells.
Do not change kernel settings. Root privilege is limited to the later trace capture,
not ordinary untraced comparisons. Refuse a missing tool or failed qualification;
retain partial evidence and report inconclusive rather than substituting a check.

Three exact existing SPIKE_FILTER cells: adapter_put/8w/64k; stream_read/cold/64k;
stream_read/tail/1k control. Each invocation keeps original1s warmup,3s measure,
SPIKE_REPS=3 and exact dataset/value/key/clock/mode/runtime semantics. Each cell has
three A/A pairs (same baseline binary, distinct fixed order labels), then three
A/B pairs with orders base/head, head/base, base/head. Total36 untraced processes,
108 native rows. No schedule adaptation, stopping at a favorable result, discarded
outlier, threshold amendment or extra repetition. A subprocess has120s ceiling
only to contain this diagnostic; hitting it means unjudgeable, not a wider product
client deadline. Fixed job budget30minutes including evidence upload.
Filtering isolates cells and removes original full-grid workload history; it does
not constitute a complete original Fast gate or automatic reproduction of it.

Summaries preserve every raw sample, medians, per-pair ratios, tails and A/A range.
The existing0.85 Fast ratio is a reference for the write diagnostic, not a new gate.
If all three A/B write ratios are below0.85 and A/A pairs remain above0.85, call
that a consistent isolated observation, not proof of source/architecture cause.
If any A/A ratio falls below0.85 or exceeds1/0.85, explicitly flag repeatability
limitations in either direction; this is a symmetric diagnostic flag, not a gate.
No p-values, confidence claim or original failed gate waiver from three pairs.
Isolated non-reproduction does not prove noise: full-grid workload history and
runner context differ. Warm read gains, cold ratio/throughput and other gates stay
separate. Original T1-T5 and instruction/Fast acceptance results are unchanged.

## Fixed trace phase, excluded from timing acceptance
After all untraced cells, collect exactly four whole-process profiles: base/head
for adapter_put/8w/64k and base/head for stream_read/cold/64k, each3native reps.
Use sudo perf for CPU sampling at99Hz (cpu-clock:u, DWARF8192), nested in a separate
system-wide tracepoint recording of scheduler switch/wakeup, block issue/complete,
and enter/exit futex/write/pwrite64/fdatasync/fsync. Qualify exact supported events
before starting; do not replace missing events. No sysctl modification, retry or
kernel-level code. Capture rawperf.data, perf report/script/header/stat diagnostics.
Traces contain setup/fill/warmup/measure/close/cleanup; current benchmark has no
write/cold measurement delimiters. Never present whole-process weights as exact
measured-phase attribution, or tracing-perturbed rows as acceptance timing.
Record scheduler process exec/fork/exit and the exact executable path to identify
the benchmark leader and every descendant thread, including threads too blocked
to produce CPU samples. CPU-sampled TIDs are supplemental only. Keep system
block events separate because their completion may occur on kernel worker threads.
Retain trace losses/throttling; any loss prevents claiming complete wait accounting.
If traces do not align a bottleneck with affected work, verdict is inconclusive.
No guessed lock/publication/IO fix based solely on a function's inclusive weight.

## Completion and STOP
Test-first fail-closed provenance/parser/schedule/trace command boundaries, independent
spec and quality review, actionlint plus full pre-push-check before milestone push.
Default gate and all existing jobs/scripts/floors remain unchanged. New probe is
explicitly named diagnostic, uploads failures and never sets phase acceptance.
One authorized milestone push/dispatch; no PR or production merge. Hash/download
and independently review final evidence. Conclude demonstrated cause (with bounds)
or inconclusive, record next decision, clean only owned build cache. Any potential
fix needs a separately reviewed test-first task. New finding/ledger only if a
specific reproduced defect is established; no speculative finding numbers.

Review refinements: preserve native allowlisted environment through sudo using
explicit env -i arguments; kill the entire own subprocess group on containment
timeout, including root perf descendants. Record actual TMPDIR mount/device. Refuse
a cold cell whose native output reports cache drop unavailable. Known ELF symbols
and unwind sections exist but debug sections do not: symbol-level attribution only,
no claimed source-line/inline precision. No concurrent native/trace processes other
than the two intentional recorders enclosing the one traced benchmark.

Implementation review refinements: inner CPU recording explicitly requests
--sample-cpu. A root supervisor inside sudo starts and records the actual privileged
command session/group, owns the120s limit and kills all group members after TERM
even when the parent already exits. Stat/render/record all use it; outer containment
uses the recorded root group rather than assuming sudo's PTY group. Real userland
process-model tests exercise a TERM-ignoring descendant; privileged Linux execution
remains separately qualified. Before/after snapshots survive invocation failure,
metadata failure does not replace primary failure. Exact current-cgroup counters
and dynamic linked-library qualification are retained. No additional timing runs.
