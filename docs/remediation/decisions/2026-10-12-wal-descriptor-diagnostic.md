# Descriptor-qualified WAL diagnostic — evidence pending

This is a finite resource investigation, not performance acceptance or an
architecture decision. It uses phase-2 baseline
`9b88229d29cf4ea1a384b1357139f0b0f6c7f313` and the exact original Linux binaries
from run `38206215742`; their original failed gate remains unchanged.

## Local segment-handle witness

The four test-only `wal_descriptor_budget` tests pass, each applicable scenario
covering Fast and Durable. A scoped lease budget over real StdVfs files establishes:

- Every retained WAL segment keeps one real file lease. Close releases them.
- Rolling needs transient directory-sync headroom as well as old and new files.
  A modeled cap of two refuses the roll, while the matched cap-three control
  succeeds; the refusal poisons subsequent writes and Close.
- After releasing all handles, reopening surviving bytes replays exactly the
  acknowledged payloads, with the expected next LSN. This is not a power-loss test.
- Retention unlinks old segments while an existing reader snapshot keeps them
  readable; the removed handles release when that reader finishes.

Independent review found intentional resource ownership, not a demonstrated
contract violation. This adds no ledger finding and makes no production fix claim.
The model excludes runtime, stdio and storage LOCK descriptors, conservatively
models directory tokens on non-Unix, and does not qualify Windows deletion behavior.

## Linux follow-up

The runner and isolated job passed independent review. Fresh local verification:
71 Python tests (35 new plus 36 copied), actionlint, and the full pre-push script
are green. The workspace run passed 1513 tests with 10 skips; concurrent tests,
doctests and both 200-seed modes (68256 checks each; 952 Durable/700 Fast rolls)
passed. No hot path changes require Criterion. One milestone dispatch is pending. The
fixed protocol proves ordinary, root-supervisor and actual native descriptor
budgets, retains final identities and observed FD counts, and records separately
scoped CPU, syscall and scheduler evidence. Whole-process attribution cannot
isolate the archived benchmark's internal measured phase. Failure stops the cycle,
retains its prefix and reports an explicit inconclusive boundary; no retries or
performance gate changes are permitted.
