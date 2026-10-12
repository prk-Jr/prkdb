# Descriptor-qualified WAL diagnostic implementation plan

> For agentic workers: execute with independent spec and quality review; root is
> sole Cargo/commit/performance owner. Controlled agents may edit assigned new tests.

Goal: qualify descriptor growth and obtain lower-volume causal waits or finish
explicitly inconclusive. Architecture: reuse reviewed helper, realVFS-budget
witness and same pinned native binaries with proven resource identity.
Tech stack: Rust1.98.1 test-only integration, Python stdlib, Linuxperf/Actions.
Files: exactly spec scope. BothCargo env variables explicit, own target, no release.

### Task1: reviewed protocol
- [x] Claim independent phase2worktree/note, preserve previous evidence/branches.
- [x] Copied 36 Python tests and baseline actionlint pass.
- [x] Independent protocol review approved after byte containment, full-chain
  smoke and timestamped thread-name coverage were added in response to review.
- [x] Commit 4b6f089: define descriptor-qualified WAL diagnostic.

### Task2: portable segment-handle witness
File: crates/prkdb-core/tests/wal_descriptor_budget.rs only.
- [x] Write assertions for live segment-lease growth and release, directory-sync
  temporary headroom/refusal, acknowledged payload recovery after budget fault,
  and gated snapshot-retention ownership. Characterize current behavior; no fixclaim.
- [x] Implement test-only RAII budgetStdVfs adapter after test/helper compileRED;
  root runs focused nextest. Compare refusal without injected cap as negativecontrol.
- [x] Independent review approved: operational resource characterization, no
  demonstrated contract defect or new finding ID.
- [x] Commit 8dafe50: characterize WAL segment descriptor budgets; handoff updated.

### Task3: descriptor-qualified trace runner test-first
Files: scripts/wal_descriptor_diagnostic.py and test_wal_descriptor_diagnostic.py.
- [x] New tests RED: exact limits/noordinaryincrease, inherited mismatch refusal,
  root-before/after proof, ordinary final identity, fixed4controls/4profiles,
  environment/3recorders/filters/readsyscalls/nontraced-ineligible boundaries.
- [x] Implement stdlib runner using pinned artifact verification and root supervisor.
- [x] Add real child/witness fault controls and explicit lifecycle/FD sampler coverage.
- [x] Root verifies 71 Python tests GREEN, zero skips. Independent implementation
  review approved; e1b1237 commits descriptor qualification and scoped events.

### Task4: one isolated job
File: remediation-gate.yml.
- [x] Add wal-descriptor-diagnostic choice/new20minute job; originalartifactdownload,
  current source proof, newtests, run, alwaysupload. Existing jobs/gates unchanged.
- [x] actionlint and diff checks pass; 71 Python controls pass; 4 focused Rust
  witnesses pass. Full pre-push: 1513 tests pass, 10 skips, concurrent tests and
  doctests, both 200-seed modes, floors/ledger/render/status/docs all green.
- [ ] Reviewedmilestonepush+oneexactSHAjob, no PR or optimizationmerge.

### Task5: qualified result and stop
- [ ] Download/hash/review resourceproofs/FDseries/VFS witness/native rows/raw/rendered
  trace; do not infer sudo changes from omitted evidence or CPU-only samples.
- [ ] Decision record cause or inconclusive with exact constraints and next boundary;
  no automaticfix/retry/gatewaiver/architecturechange.
- [ ] Commit report, refresh fullhandoff everycommit, clean ownedtarget.
