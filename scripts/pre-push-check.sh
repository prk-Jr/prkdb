#!/usr/bin/env bash
# Run before any push to origin (spec §5.1). Public CI should confirm, not discover.
set -euo pipefail
cd "$(dirname "$0")/.."
step() { printf '\n==> %s\n' "$*"; }
step fmt;          cargo fmt --all -- --check
step clippy;       cargo clippy --workspace --all-targets -- -D warnings
step tests
if command -v cargo-nextest >/dev/null; then cargo nextest run --workspace; else cargo test --workspace; fi
if [ -d crates/prkdb-verify ]; then step harness; cargo xtask verify --profile blocking --seeds 200 --mode durable; fi
step ledger;       cargo xtask remediation check && cargo xtask remediation render --check
step repo-status;  cargo xtask repo-status snapshot --fail-on-objective-drift
step readme-tests; cargo xtask readme-tests --check
step doc-claims;   bash scripts/check_doc_claims.sh
echo; echo "pre-push-check: all green"
