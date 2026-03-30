# Mixed-Client Integration And Cross-Language Benchmarks Design

## Purpose

PrkDB currently validates generated Python, TypeScript, and Go clients in separate CI jobs.
It also runs a "cross-language benchmark" job, but that job only exercises Python and
TypeScript, and it does so sequentially. That leaves two gaps:

1. There is no correctness-focused CI job proving that multiple generated clients can write
   to the same live PrkDB server at the same time.
2. The benchmark coverage is incomplete for this category because Go is not included.

This design adds a mixed-client integration job for correctness under concurrent writes and
expands the existing benchmark category to cover Go as well.

## Goals

- Prove that generated Python, TypeScript, and Go clients can concurrently write to one live
  PrkDB server in CI.
- Verify the mixed-client run deterministically, even when each language writes a different
  number of records.
- Keep existing isolated per-language client feature checks for failure isolation.
- Expand the current cross-language benchmark category to include Go.
- Clean up benchmark harness drift so the output is easier to interpret and maintain.
- Update user-facing docs where CI coverage or benchmark wording would otherwise become stale.

## Non-Goals

- Replacing the existing per-language client feature jobs with one mega-job.
- Treating the mixed-client integration job as a benchmark source of truth.
- Producing apples-to-apples cross-language performance claims from noisy CI timings.
- Refactoring the code generator beyond what is required for the new tests and benchmark
  harnesses.

## Current Problems

### CI Coverage Gap

The repository currently has isolated client feature integration jobs for Python,
TypeScript, and Go. Those jobs are useful smoke checks, but they do not answer whether all
generated clients can operate against the same live server concurrently.

### Benchmark Category Gap

The existing cross-language benchmark job generates Python and TypeScript clients and runs
their benchmark scripts, but it does not benchmark Go. As a result, the job name overstates
the current coverage.

### Benchmark Credibility Gap

The current benchmark harnesses are not aligned closely enough to support strong comparisons:

- languages do not have identical runner structure
- the benchmark job runs languages sequentially on the same live server
- the current output is better interpreted as internal telemetry than a clean comparison

These issues do not make the benchmark category useless, but they do mean the repo should
treat the output as trend tracking rather than precise language ranking.

## Design Summary

The implementation will add one new correctness-first CI job and improve one existing
performance-oriented CI job.

- **New job:** mixed-client integration
  - one PrkDB server
  - one schema and one target collection
  - generated Python, TypeScript, and Go clients
  - all three client writers launched concurrently
  - deterministic post-run verification of expected records
- **Updated job:** cross-language benchmark
  - keep this as benchmark telemetry
  - add Go benchmark coverage
  - clean up benchmark harnesses and reporting

The existing isolated client feature jobs remain in place.

## Proposed CI Structure

### 1. Keep Existing Isolated Client Feature Jobs

Retain the current Python, TypeScript, and Go client feature integration jobs. They serve a
different purpose from the mixed-client run:

- faster failure isolation by language
- simpler debugging when one generated client regresses
- focused validation of language-specific generated APIs

These jobs should not be replaced by the mixed-client integration test.

### 2. Add Mixed-Client Integration Job

Add a new CI job dedicated to correctness under concurrent mixed-language writes.

Responsibilities:

- build `prkdb-cli`
- start one PrkDB server with one HTTP port and one gRPC port
- define and register a benchmark/integration schema
- generate Python, TypeScript, and Go clients for the same collection
- launch one writer per language concurrently
- wait for all writers to finish
- run a deterministic verification pass
- print a concise summary
- clean up temporary files and background processes

This job should fail only on correctness issues, not on performance thresholds.

### 3. Expand Cross-Language Benchmark Job

Update the existing benchmark job so it includes Go in addition to Python and TypeScript.

Responsibilities:

- generate all three clients
- run one benchmark script per language
- print per-language timing and throughput
- keep wording and docs aligned with the fact that these are CI telemetry numbers

This job should remain separate from the mixed-client integration test.

## Mixed-Client Integration Design

### Shared Test Topology

The job runs one PrkDB server and one collection. All clients target the same HTTP endpoint
and the same collection.

The server lifecycle is owned by one shell script that:

- reserves ports
- starts the server
- waits for both HTTP health and schema gRPC readiness
- registers the schema
- generates the three clients
- launches the language runners concurrently
- runs verification
- tears everything down in `trap` cleanup

### Writer Shape

Each language gets a dedicated writer script:

- Python writer
- TypeScript writer
- Go writer

Each writer accepts configuration from environment variables or CLI arguments:

- server URL
- record count
- ID prefix
- collection name

Each writer emits deterministic IDs such as:

- `py-000001`
- `ts-000001`
- `go-000001`

Each writer prints:

- start banner
- configured record count
- completion count
- duration

Timing is diagnostic only.

### Uneven Counts

Uneven record counts are explicitly allowed. The mixed-client integration job should support
configuring different record totals per language, for example:

- Python: 700
- TypeScript: 1000
- Go: 1300

The verifier uses configured expected counts instead of assuming equal load.

### Verification Model

After all writers finish, one verifier checks:

- total record count equals the sum of all expected counts
- Python-prefixed record count equals expected Python count
- TypeScript-prefixed record count equals expected TypeScript count
- Go-prefixed record count equals expected Go count
- sampled IDs from each language are retrievable

The verification must fail on missing or extra records.

The verifier should avoid fragile assumptions about ordering. It only needs deterministic ID
and count correctness.

### Verification Mechanism

Use one shared verification path rather than three language-specific verifiers. The verifier
can be implemented in the language with the lowest operational cost in CI, but it must use
repo-supported interfaces and keep the logic simple.

Recommended approach:

- verify via generated or existing client access to the live server
- page through collection data if needed
- count records by ID prefix
- validate sampled IDs directly

The verifier does not need to inspect server internals or local database files.

## Benchmark Design

### Keep Benchmark Purpose Narrow

The benchmark job exists to provide internal timing telemetry for generated clients in CI. It
does not provide a trustworthy basis for product-level language performance claims.

This must remain explicit in code comments and docs.

### Add Go Benchmark Coverage

Add a Go benchmark runner so the benchmark category truly covers:

- Python
- TypeScript
- Go

The Go benchmark should match the broad structure of the existing Python and TypeScript
scripts:

- connect to one live server
- write a configurable number of records
- print total duration
- print approximate throughput

### Benchmark Harness Cleanup

While adding Go coverage, align the harnesses where practical:

- consistent environment variables and defaults
- deterministic ID prefixes by language
- similar output shape
- clear distinction between benchmark scripts and integration scripts
- cleanup of stale comments or path assumptions that no longer match generated output

The goal is not perfect uniformity. The goal is enough consistency that CI output remains
understandable and maintenance cost stays low.

## Script And File Layout

Exact filenames may vary slightly during implementation, but the design expects:

- one new mixed-client integration shell script under `scripts/`
- one verifier script or runner used by that integration flow
- one new Go benchmark runner under `benches/`
- updates to existing Python and TypeScript benchmark scripts under `benches/`
- CI workflow updates in `.github/workflows/ci.yml`

The mixed-client integration should use temporary working directories and should not leave
generated clients or benchmark descriptors behind after completion.

## Cleanup Requirements

Cleanup is part of the design, not an afterthought.

### Runtime Cleanup

Each orchestration script must:

- terminate the background server on exit
- remove temporary working directories
- remove temporary generated client directories when they are created outside temp roots
- remove temporary schema descriptor files

Cleanup must run on both success and failure via `trap`.

### Repo Cleanup

As part of implementing this work, remove or fix obvious script drift in the current
benchmark flow when those paths or comments are already being touched. This includes:

- stale comments about import paths
- inconsistent generated-client path assumptions
- benchmark wording that implies stronger comparability than the repo actually provides

This cleanup should stay scoped to the benchmark and mixed-client test area.

## Documentation Changes

Update docs that describe benchmark coverage or CI validation so they remain truthful after
the change.

Expected doc updates:

- `README.md`
  - mention mixed-client integration coverage
  - reflect that the cross-language benchmark category includes Go
  - preserve the existing caveat that CI benchmark numbers are trend-tracking telemetry
- optionally related guide pages if they directly describe the old benchmark coverage

Generated status docs such as `docs/status/repo-status.md` should only be updated if the
normal repo-status generation flow requires it. They should not be hand-edited casually.

## Error Handling

The orchestration scripts should fail fast and emit actionable output.

Required behavior:

- stop on shell errors with `set -euo pipefail`
- print server logs on failure when useful
- validate required external tools before starting work
- fail if generated clients are missing expected files
- fail if any language writer exits non-zero
- fail if verification counts do not match expectations

Writers should report failed request counts clearly enough that CI logs identify which
language failed.

## Testing Strategy

### CI Coverage

The main test surface is CI itself:

- isolated Python client feature integration
- isolated TypeScript client feature integration
- isolated Go client feature integration
- new mixed-client integration
- updated cross-language benchmark with Go

### Local Validation

The repo should provide a local script path that mirrors the mixed-client CI flow closely
enough for debugging and iteration.

### Scope Control

Do not turn this effort into a generalized benchmark framework. The scope is:

- one mixed-client correctness flow
- one updated benchmark category
- reasonable harness cleanup

## Implementation Notes

Recommended implementation order:

1. add or normalize standalone writer scripts for Python, TypeScript, and Go
2. add the Go benchmark runner
3. add the mixed-client integration orchestration and verification flow
4. wire new and updated jobs into `.github/workflows/ci.yml`
5. update README wording
6. run the relevant local verification paths

## Risks And Mitigations

### Risk: flaky mixed-client verification

Mitigation:

- use deterministic IDs
- verify by counts and direct retrieval, not ordering
- wait for all writers before verification

### Risk: benchmark output overread as precise comparison

Mitigation:

- keep benchmark job separate from correctness job
- preserve and tighten benchmark caveat wording in docs
- keep timing output informational rather than gatekeeping

### Risk: CI maintenance cost grows

Mitigation:

- keep isolated jobs unchanged unless required
- add one focused mixed-client job instead of collapsing all jobs together
- reuse existing codegen and server startup patterns

## Open Decisions Resolved By This Design

- **Should we have both an integration test and benchmark coverage?**
  - Yes. They solve different problems and should remain separate.
- **Should mixed-client load require equal counts across languages?**
  - No. Uneven counts are allowed as long as verification uses configured expectations.
- **Should Go be added to this benchmark category?**
  - Yes.

## Success Criteria

This work is successful when:

- CI has a dedicated mixed-client integration job that runs Python, TypeScript, and Go
  writers concurrently against one live PrkDB server
- the mixed-client job deterministically verifies the final result set
- the benchmark category includes Go alongside Python and TypeScript
- benchmark and integration scripts clean up after themselves
- README and related wording stay aligned with the new coverage and benchmark caveats
