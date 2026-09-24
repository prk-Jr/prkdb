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
  # nextest does not run doctests; cover them separately.
  step doctests; cargo test --workspace --doc
else
  cargo test --workspace
fi
if [ -d crates/prkdb-verify ]; then step harness; cargo xtask verify --profile blocking --seeds 200 --mode durable; fi
step wal-fast-rule; python3 scripts/wal_fast_rule.py --self-test
step ledger;        cargo xtask remediation check
step ledger-render; cargo xtask remediation render --check
step repo-status;   cargo xtask repo-status snapshot --fail-on-objective-drift > /dev/null
step readme-tests;  cargo xtask readme-tests --check
step doc-claims;    bash scripts/check_doc_claims.sh
echo; echo "pre-push-check: all green"
