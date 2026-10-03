#!/usr/bin/env bash
# Run before any push to origin (spec §5.1). Public CI should confirm, not discover.
#
# If a ledger change closes the last open critical finding, or opens the first one, the
# repo-status Verification dimension flips and the evidence fingerprint embedded in
# docs/status/repo-status.md changes with it — re-run `cargo xtask repo-status render`
# and commit the regenerated page, or the `repo-status` step below will fail.
set -euo pipefail
cd "$(dirname "$0")/.."
step() { printf '\n==> %s\n' "$*"; }
step fmt;          cargo fmt --all -- --check
step clippy;       cargo clippy --workspace --all-targets -- -D warnings
step tests
if command -v cargo-nextest >/dev/null; then
  cargo nextest run --workspace
  # Also exactly what the remediation gate runs ("Ledger and Tests"), which the Coverage
  # job's `cargo llvm-cov` matches: libtest runs each binary's tests concurrently in one
  # process, while nextest isolates them and serializes the serial-servers group. A test
  # that only fails concurrently (shared ports, shared state) reached CI that way
  # (http_authz on 0d1ca02). This also covers the doctests nextest does not run.
  step tests-concurrent; cargo test --workspace --no-fail-fast
else
  cargo test --workspace
fi
if [ -d crates/prkdb-verify ]; then
  step harness;      cargo xtask verify --profile blocking --seeds 200 --mode durable
  step harness-fast; cargo xtask verify --profile blocking --seeds 200 --mode fast
fi
step wal-fast-rule; python3 scripts/wal_fast_rule.py --self-test
step perf-floors;   bash scripts/check_perf_gate_floors.sh
step single-wal;    bash scripts/check_single_wal.sh
step ledger;        cargo xtask remediation check
step ledger-render; cargo xtask remediation render --check
step repo-status;   cargo xtask repo-status snapshot --fail-on-objective-drift > /dev/null
step readme-tests;  cargo xtask readme-tests --check
step doc-claims;    bash scripts/check_doc_claims.sh
echo; echo "pre-push-check: all green"
