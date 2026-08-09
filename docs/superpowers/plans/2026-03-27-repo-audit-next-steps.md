# Repository Audit Next Steps Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Align the repository with the code that actually exists today by retiring stale hardening assumptions, fixing remaining contract drift, removing CI build waste, tightening quality gates, and replacing misleading benchmark claims with defensible measurements.

**Architecture:** Treat this as six small workstreams executed in order: contract cleanup, CI build-topology cleanup, lint and quality-gate hardening, runtime error-surface cleanup, verification tiering, and benchmark credibility. The immediate priority is to stop internal docs, CLI metadata, and endpoint examples from lying about current behavior, then remove avoidable rebuilds and benchmark overclaims so CI and published evidence both reflect the real system.

**Tech Stack:** Rust (`clap`, `tokio`, `tonic`, `axum`), GitHub Actions, shell integration scripts, benchmark examples, Markdown docs/VitePress

---

## Audit Snapshot

- Verified: `cargo test --workspace --all-targets --no-run`
- Verified: `cargo test -p prkdb --test security_tests -- --nocapture`
- Verified: `cargo test -p prkdb-cli --test http_api_integration -- --nocapture`
- Verified: `cargo test -p prkdb-client -- --nocapture`
- Verified: `cargo test -p prkdb-cli --test websocket_integration -- --ignored --nocapture`
- Verified: `cargo test -p prkdb --test raft_chaos_tests test_cascading_failures -- --ignored --nocapture`
- Failing quality gate: `cargo clippy --workspace --all-targets -- -D warnings`
- CI issue: the same `prkdb-cli` binary is rebuilt in multiple jobs and rebuilt again inside several integration scripts.
- Benchmark issue: current Kafka comparison jobs and docs compare Kafka perf-tool networked broker results against local PrkDB storage-adapter benchmarks, then publish direct “x faster than Kafka” conclusions that the measurement code does not justify.
- Conclusion: the current `docs/superpowers/specs/2026-03-27-production-hardening-round-2.md` and companion plan no longer describe the real highest-priority repo work. Security, smart-client read routing, HTTP integration, WebSocket streaming, and the previously called-out cascading-failure recovery path are currently green in the verified local slice.

## File Structure Map

- Modify: `crates/prkdb-cli/src/main.rs`
  Purpose: remove hardcoded CLI version drift and tighten remote endpoint UX/help text.
- Modify: `crates/prkdb-cli/src/commands/serve.rs`
  Purpose: remove hardcoded HTTP root version drift and reduce brittle HTTP forwarding behavior.
- Modify: `crates/prkdb-cli/src/commands/data.rs`
- Modify: `crates/prkdb-cli/src/commands/schema.rs`
- Modify: `crates/prkdb-cli/src/commands/codegen.rs`
  Purpose for remote command modules: make gRPC endpoint expectations explicit and consistent with the binaries that ship today.
- Modify: `crates/prkdb/src/bin/prkdb-server.rs`
  Purpose: make bind-address behavior match the advertised-address contract instead of silently binding `0.0.0.0`.
- Modify: `crates/prkdb/src/outbox.rs`
- Modify: `crates/prkdb/src/raft/partition_manager.rs`
- Modify: `crates/prkdb/src/builder.rs`
  Purpose: replace reachable panic paths with structured errors.
- Modify: `crates/prkdb-core/src/io/mmap_io.rs`
- Modify: `crates/prkdb-core/src/wal/buffer_pool.rs`
- Modify: `crates/prkdb-core/src/wal/mmap_parallel_wal.rs`
  Purpose: clear the currently failing `clippy -D warnings` baseline.
- Modify: `.github/workflows/ci.yml`
- Modify: `.github/workflows/chaos-tests.yml`
  Purpose: remove avoidable rebuilds, make CI enforce the real quality bar, and classify ignored tests intentionally.
- Modify: `scripts/test_client_features.sh`
- Modify: `scripts/test_client_features_ts.sh`
- Modify: `scripts/test_client_features_go.sh`
- Modify: `scripts/test_schema_app_e2e.sh`
- Modify: `scripts/test_schema_nested_e2e.sh`
- Modify: `scripts/test_schema_cli.sh`
  Purpose: allow CI jobs to reuse prebuilt binaries instead of rebuilding inside each script.
- Modify: `crates/prkdb/examples/comprehensive_bench.rs`
- Modify: `crates/prkdb/examples/partitioned_bench.rs`
- Modify: `crates/prkdb/examples/streaming_bench.rs`
- Modify: `crates/prkdb/examples/throughput_bench.rs`
- Modify: `scripts/benchmark_suite.sh`
  Purpose: separate local-storage benchmarks from distributed-system comparisons and stop mixing incomparable metrics in one report.
- Modify: `crates/prkdb-cli/tests/http_api_integration.rs`
- Modify: `crates/prkdb-cli/tests/websocket_integration.rs`
- Modify: `crates/prkdb/tests/chaos_tests.rs`
  Purpose: lock endpoint/version fixes and promote stable ignored tests.
- Modify: `README.md`
- Modify: `docs/guide/getting-started.md`
- Modify: `docs/guide/client/smart-client.md`
- Modify: `docs/guide/streaming-kafka-comparison.md`
- Modify: `docs/superpowers/specs/2026-03-27-production-hardening-round-2.md`
- Modify: `docs/superpowers/plans/2026-03-27-production-hardening-round-2.md`
  Purpose: retire stale assumptions, document the current contract, and remove benchmark claims the repository cannot currently defend.

### Task 1: Reset CLI, HTTP, and Docs to the Current Runtime Contract

**Files:**
- Modify: `crates/prkdb-cli/src/main.rs`
- Modify: `crates/prkdb-cli/src/commands/serve.rs`
- Modify: `crates/prkdb-cli/src/commands/data.rs`
- Modify: `crates/prkdb-cli/src/commands/schema.rs`
- Modify: `crates/prkdb-cli/src/commands/codegen.rs`
- Modify: `README.md`
- Modify: `docs/guide/getting-started.md`
- Modify: `docs/guide/client/smart-client.md`
- Modify: `docs/superpowers/specs/2026-03-27-production-hardening-round-2.md`
- Modify: `docs/superpowers/plans/2026-03-27-production-hardening-round-2.md`
- Test: `crates/prkdb-cli/tests/http_api_integration.rs`

- [ ] **Step 1: Add a failing version-regression test**

Add a focused assertion in `crates/prkdb-cli/tests/http_api_integration.rs` that the HTTP root payload exposes the package version from Cargo metadata rather than a hardcoded string.

```rust
assert_eq!(body["version"], env!("CARGO_PKG_VERSION"));
```

- [ ] **Step 2: Replace hardcoded version strings with Cargo package metadata**

Use `env!("CARGO_PKG_VERSION")` in both:
- the Clap `version = ...` field in `crates/prkdb-cli/src/main.rs`
- the HTTP root payload in `crates/prkdb-cli/src/commands/serve.rs`

- [ ] **Step 3: Make endpoint expectations explicit in command help and docs**

Update command help strings and docs so they clearly state:
- remote `put/get/delete/schema/codegen` commands speak gRPC
- `prkdb-server` uses its single gRPC port directly
- `prkdb-cli serve` exposes HTTP on `--port` and gRPC on `--grpc-port`

Do not leave examples that tell users to point gRPC commands at the HTTP port.

- [ ] **Step 4: Retire stale hardening docs instead of letting them drift**

Rewrite the March 27 hardening spec/plan to record which issues are already fixed and which items are still actionable, or explicitly archive them in favor of this repo-audit plan.

- [ ] **Step 5: Verify the contract slice**

Run:
- `cargo test -p prkdb-cli --test http_api_integration -- --nocapture`
- `cargo run -p prkdb-cli -- --version`

Expected:
- integration tests pass
- CLI version and HTTP root version both report `0.6.0` on this branch

### Task 2: Stop Rebuilding the Same Binary Across CI and Integration Scripts

**Files:**
- Modify: `.github/workflows/ci.yml`
- Modify: `.github/workflows/chaos-tests.yml`
- Modify: `scripts/test_client_features.sh`
- Modify: `scripts/test_client_features_ts.sh`
- Modify: `scripts/test_client_features_go.sh`
- Modify: `scripts/test_schema_app_e2e.sh`
- Modify: `scripts/test_schema_nested_e2e.sh`
- Modify: `scripts/test_schema_cli.sh`

- [ ] **Step 1: Inventory the current build duplication and choose the reuse boundary**

Capture the existing duplication points:
- separate workflow jobs rebuild the workspace independently
- schema/client integration scripts call `cargo build -p prkdb-cli` internally
- some scripts still use `cargo run -p prkdb-cli --bin prkdb-cli -- ...` instead of an already-built binary
- benchmark steps build examples, then invoke them with `cargo run --release --example ...`, which still performs cargo orchestration per run

Pick the reuse boundary explicitly:
- reuse binaries within a job by default
- only introduce cross-job artifacts where a downstream job truly needs the same release binary and the artifact download cost is lower than rebuilding

- [ ] **Step 2: Refactor scripts to accept prebuilt binaries**

Update the integration scripts so they support:
- `PRKDB_BIN` for direct binary execution, defaulting to `./target/debug/prkdb-cli`
- optional `SKIP_BUILD=1` or equivalent to suppress internal `cargo build`

Do not leave scripts that always rebuild even when CI already produced the binary.

- [ ] **Step 3: Make workflow jobs build once, then execute binaries directly**

Change CI jobs so they:
- build `prkdb-cli` once per job in the desired profile
- run scripts with `PRKDB_BIN=./target/debug/prkdb-cli` or `./target/release/prkdb-cli`
- stop using `cargo run` in script-level control paths where the binary is already available

- [ ] **Step 4: Remove avoidable cargo orchestration from benchmark execution**

The benchmark job already builds release examples. After that, execute:
- `./target/release/examples/comprehensive_bench`
- `./target/release/examples/partitioned_bench`

instead of `cargo run --release --example ...`.

- [ ] **Step 5: Add cross-job artifact reuse only where it pays for itself**

If measurement shows continued rebuild waste in PR CI, add a dedicated producer job that uploads the exact binaries needed by downstream benchmark or chaos jobs. Keep the scope narrow:
- release `prkdb-cli`
- release benchmark example executables only if a downstream job consumes them

Do not create a broad artifact fan-out if `rust-cache` plus per-job builds is cheaper and simpler.

- [ ] **Step 6: Verify the new CI execution model**

Run locally where practical:
- the updated integration scripts against a prebuilt `PRKDB_BIN`
- the benchmark executables directly from `target/release/examples/`

Expected:
- no redundant `cargo build` inside the touched scripts when reuse is enabled
- no `cargo run` left in CI paths that already built the same executable

### Task 3: Turn CI Into a Real Quality Gate

**Files:**
- Modify: `crates/prkdb-core/src/io/mmap_io.rs`
- Modify: `crates/prkdb-core/src/wal/buffer_pool.rs`
- Modify: `crates/prkdb-core/src/wal/mmap_parallel_wal.rs`
- Modify: `.github/workflows/ci.yml`
- Modify: warning-producing example/test files only if needed to support the chosen gate

- [ ] **Step 1: Reproduce the current strict lint failures**

Run: `cargo clippy --workspace --all-targets -- -D warnings`

Expected: FAIL in the currently observed locations:
- `crates/prkdb-core/src/io/mmap_io.rs`
- `crates/prkdb-core/src/wal/buffer_pool.rs`
- `crates/prkdb-core/src/wal/mmap_parallel_wal.rs`

- [ ] **Step 2: Fix the concrete clippy issues first**

Implement the minimal fixes:
- define explicit truncate behavior for `OpenOptions`
- replace `io::Error::new(io::ErrorKind::Other, ...)` with `io::Error::other(...)`
- replace `len() >= 1` assertions with `!is_empty()`
- initialize defaulted structs without field reassignment where clippy requires it

- [ ] **Step 3: Decide and document the actual lint policy**

Choose one policy and encode it in CI:
- strict `--all-targets -D warnings` for the whole workspace, or
- a blocking strict lane for maintained crates plus a separate non-blocking examples/bench lane

Do not keep the current `-W clippy::all` job if the team expects CI to catch regressions.

- [ ] **Step 4: Update CI to enforce the chosen policy**

Change `.github/workflows/ci.yml` so the lint lane fails on the policy selected in Step 3.

- [ ] **Step 5: Re-run the full quality gate**

Run:
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo test --workspace --all-targets --no-run`

Expected: PASS.

### Task 4: Remove Brittle and Stringly-Typed Runtime Failure Paths

**Files:**
- Modify: `crates/prkdb/src/bin/prkdb-server.rs`
- Modify: `crates/prkdb-cli/src/commands/serve.rs`
- Modify: `crates/prkdb/src/outbox.rs`
- Modify: `crates/prkdb/src/raft/partition_manager.rs`
- Modify: `crates/prkdb/src/builder.rs`
- Test: `crates/prkdb-cli/tests/http_api_integration.rs`

- [ ] **Step 1: Write targeted regressions for the remaining brittle contracts**

Add or extend tests that cover:
- versioned/bind-address behavior for `prkdb-server`
- HTTP forwarding behavior without relying on parsing human-readable error strings
- builder or batch paths returning structured errors instead of panicking

- [ ] **Step 2: Stop canonical server bind behavior from silently widening exposure**

Refactor `crates/prkdb/src/bin/prkdb-server.rs` so the bind host is explicit and consistent with the advertised-address model. The current code advertises one address but always binds gRPC on `0.0.0.0`.

- [ ] **Step 3: Replace string parsing for leader redirects**

Remove `parse_leader_id()` style parsing in `crates/prkdb-cli/src/commands/serve.rs`. Prefer a structured error or redirect signal that survives message wording changes.

- [ ] **Step 4: Replace reachable panic paths with typed errors**

Convert these runtime assumptions into recoverable errors:
- mixed outbox batch event types
- missing partition lookup in `get_raft_for_key`
- storage-construction `expect(...)` in builder helpers

- [ ] **Step 5: Verify the runtime cleanup**

Run:
- `cargo test -p prkdb-cli --test http_api_integration -- --nocapture`
- targeted unit/integration tests added in Step 1

Expected: PASS with no reachable panic-based control flow left in the touched production paths.

### Task 5: Promote Stable Ignored Tests and Tier the Expensive Ones

**Files:**
- Modify: `crates/prkdb-cli/tests/websocket_integration.rs`
- Modify: `crates/prkdb/tests/chaos_tests.rs`
- Modify: `.github/workflows/ci.yml`
- Modify: `.github/workflows/chaos-tests.yml`

- [ ] **Step 1: Classify ignored tests by intent**

Split the ignored suites into three buckets:
- cheap and stable enough for PR CI
- slow but stable enough for scheduled or reusable chaos workflows
- intentionally heavy/manual workloads

- [ ] **Step 2: Unignore the stable WebSocket integration tests**

The local audit already verified:
- `cargo test -p prkdb-cli --test websocket_integration -- --ignored --nocapture`

Promote these tests into normal CI if they remain stable after one more local re-run.

- [ ] **Step 3: Investigate the explicitly flaky rapid-recovery WAL test**

Use `crates/prkdb/tests/chaos_tests.rs` to either:
- fix `chaos_test_rapid_recovery`, or
- keep it out of PR CI with a concrete comment explaining the remaining failure mode and owning issue

- [ ] **Step 4: Make slow suites visible rather than invisible**

For long-running load, corruption, and deep chaos tests, wire them into:
- scheduled CI, or
- `workflow_dispatch`

Do not leave important coverage only behind forgotten `#[ignore]` annotations.

- [ ] **Step 5: Verify the new verification tiers**

Run:
- `cargo test -p prkdb-cli --test websocket_integration -- --nocapture`
- `cargo test -p prkdb --test raft_chaos_tests test_cascading_failures -- --ignored --nocapture`
- the revised CI workflow commands locally where practical

Expected: the cheap suite is PR-ready and the expensive suite has an explicit home.

### Task 6: Rebuild the Benchmark Story So It Survives Technical Scrutiny

**Files:**
- Modify: `.github/workflows/ci.yml`
- Modify: `scripts/benchmark_suite.sh`
- Modify: `crates/prkdb/examples/comprehensive_bench.rs`
- Modify: `crates/prkdb/examples/partitioned_bench.rs`
- Modify: `crates/prkdb/examples/streaming_bench.rs`
- Modify: `crates/prkdb/examples/throughput_bench.rs`
- Modify: `README.md`
- Modify: `docs/guide/streaming-kafka-comparison.md`

- [ ] **Step 1: Classify the current benchmarks by what they actually measure**

Document the current benchmark types explicitly:
- local storage-engine append/read benchmarks
- local partitioned throughput benchmarks
- HTTP cross-language client benchmarks
- Kafka broker perf-tool benchmarks

Do not keep reports that present those as one directly comparable “PrkDB vs Kafka” result.

- [ ] **Step 2: Remove invalid direct-comparison claims from public surfaces**

Replace or delete claims such as:
- “21.8x faster than Kafka”
- “24.5x faster consumer”
- “sub-millisecond vs Kafka” when the underlying metrics are not the same measurement

If the benchmark suite remains local-only, describe it as local log/storage throughput, not as a distributed-system bakeoff.

- [ ] **Step 3: Make benchmark outputs honest about durability and topology**

Update benchmark code and report generation so each report states:
- single-node vs distributed
- local adapter vs networked broker
- sync/fsync behavior
- partitioning/concurrency model
- whether reads are local replay, cached reads, or end-to-end consumer fetches

- [ ] **Step 4: Split benchmark suites instead of forcing false equivalence**

Create two lanes:
- local-engine benchmarks for PrkDB internals and regressions
- external-system comparison benchmarks, only if the methodology is made meaningfully comparable

At minimum, stop printing hardcoded `vs Kafka` ratios inside the local partitioned benchmark and stop generating Markdown summaries that claim direct advantage from incomparable runs.

- [ ] **Step 5: Define a credible external comparison protocol**

Before reintroducing any Kafka comparison, specify:
- same durability target or an explicit caveat
- same concurrency model
- same hardware budget
- same data size and batching semantics
- end-to-end metric definitions that match on both sides

If those constraints are not met, keep the benchmark internal and avoid public winner/loser framing.

- [ ] **Step 6: Verify the revised benchmark/reporting contract**

Run the revised benchmark commands and inspect the generated Markdown/JSON outputs.

Expected:
- no direct “x faster than Kafka” conclusions remain unless the comparison methodology actually supports them
- benchmark reports clearly state what was measured and what was not
