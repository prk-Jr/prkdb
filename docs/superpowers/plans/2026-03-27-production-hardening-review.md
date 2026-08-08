# Production Hardening Review Fix Plan

Date: 2026-03-27
Branch: `fix/production-hardening-review`

## Scope

Address the previously identified production-readiness gaps across:

- Rust gRPC service auth, schema storage, topology metadata, and watch behavior
- Rust smart client correctness and secret handling
- HTTP server correctness in single-node and Multi-Raft modes
- CLI/runtime safety issues and duplicate/dead entrypoints
- Documentation drift, missing examples, and README docs URL

## Execution Order

1. Add regression tests for the highest-risk correctness and security issues.
2. Fix gRPC auth, schema persistence, topology metadata, and watch delivery.
3. Fix client partial-failure behavior and WebSocket credential handling.
4. Fix HTTP collection/data/count/health behavior so it works with the database API instead of raw storage assumptions.
5. Replace unsafe global initialization and remove duplicate/dead binaries or protocol sources that create build ambiguity.
6. Update docs and examples to match the actual CLI, Rust client, Python client, and deployed docs site.
7. Run focused tests first, then broader verification.

## Regression Targets

- Schema registration/listing reject missing or invalid admin tokens.
- Schema data persists across service restart when file-backed storage is used.
- Smart client `batch_put()` returns errors on missing leaders, RPC failures, and partial failures.
- HTTP route surface includes `GET /collections/:name/data/:id`.
- Collection counts use the same key convention as writes.
- Health and collection reads work in Multi-Raft mode.
- WebSocket client no longer places auth tokens in query strings or logs secrets.

## Design Notes

- Prefer durable defaults over feature flags where possible.
- Keep admin auth failures as gRPC `Unauthenticated`.
- Avoid secret disclosure in logs, URLs, and generated examples.
- Reuse `PrkDb` APIs for reads and metrics instead of direct storage scans when the latter breaks in Raft mode.
- Keep docs examples executable against the current codebase.

## Verification

- Focused `cargo test` runs for `prkdb`, `prkdb-client`, and `prkdb-cli`
- `cargo test --workspace --all-targets --no-run`
- Spot-check docs examples for API and CLI accuracy
