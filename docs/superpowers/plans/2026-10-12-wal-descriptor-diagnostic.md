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
- [ ] Commit docs: define descriptor-qualified WAL diagnostic.

### Task2: portable segment-handle witness
File: crates/prkdb-core/tests/wal_descriptor_budget.rs only.
- [ ] Write assertions for live segment-lease growth and release, directory-sync
  temporary headroom/refusal, acknowledged payload recovery after budget fault,
  and gated snapshot-retention ownership. Characterize current behavior; no fixclaim.
- [ ] Implement test-only RAII budgetStdVfs adapter after test/helper compileRED;
  root runs focused nextest. Compare refusal without injected cap as negativecontrol.
- [ ] Independent review; decide confirmed finding versus operational resource limit.
- [ ] Commit test: characterize WAL segment descriptor budgets; update handoff.

### Task3: descriptor-qualified trace runner test-first
Files: scripts/wal_descriptor_diagnostic.py and test_wal_descriptor_diagnostic.py.
- [ ] New tests RED: exact limits/noordinaryincrease, inherited mismatch refusal,
  root-before/after proof, ordinary final identity, fixed4controls/4profiles,
  environment/3recorders/filters/readsyscalls/nontraced-ineligible boundaries.
- [ ] Implement stdlib runner using pinned artifact verification and root supervisor.
- [ ] Add real child/witness fault controls and explicit lifecycle/FD sampler coverage.
- [ ] Tests GREEN; independent implementationreview; commit ci: qualify WAL trace
  descriptor budgets and scope recorder events.

### Task4: one isolated job
File: remediation-gate.yml.
- [ ] Add wal-descriptor-diagnostic choice/new20minute job; originalartifactdownload,
  current source proof, newtests, run, alwaysupload. Existing jobs/gates unchanged.
- [ ] actionlint/Python/newRust/full scripts/pre-push-check.sh; record counts.
- [ ] Reviewedmilestonepush+oneexactSHAjob, no PR or optimizationmerge.

### Task5: qualified result and stop
- [ ] Download/hash/review resourceproofs/FDseries/VFS witness/native rows/raw/rendered
  trace; do not infer sudo changes from omitted evidence or CPU-only samples.
- [ ] Decision record cause or inconclusive with exact constraints and next boundary;
  no automaticfix/retry/gatewaiver/architecturechange.
- [ ] Commit report, refresh fullhandoff everycommit, clean ownedtarget.
