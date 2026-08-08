# Production Hardening Round 2 Spec

## Status

Archived on 2026-03-27 in favor of the broader repository audit plan at:

- `docs/superpowers/plans/2026-03-27-repo-audit-next-steps.md`

## Why This Spec Was Archived

This document no longer matched the codebase by the time the repo-wide audit completed.
Several items listed here had already been fixed or were no longer the highest-priority
production issues.

## Verified Current State

The following slices were re-verified during the repo audit and are not the main remaining
blockers described in this archived spec:

- `cargo test --workspace --all-targets --no-run`
- `cargo test -p prkdb --test security_tests -- --nocapture`
- `cargo test -p prkdb-cli --test http_api_integration -- --nocapture`
- `cargo test -p prkdb-client -- --nocapture`
- `cargo test -p prkdb-cli --test websocket_integration -- --ignored --nocapture`
- `cargo test -p prkdb --test raft_chaos_tests test_cascading_failures -- --ignored --nocapture`

## What Replaced It

The active follow-up work now focuses on:

1. CLI, docs, and endpoint-contract drift.
2. CI build duplication and weak quality gates.
3. Remaining brittle runtime error paths.
4. Intentional verification tiers for ignored tests.
5. Benchmark methodology and reporting credibility.

## Notes

Keep this file only as historical context for the original hardening review. Do not use it as
the execution source for new implementation work.
