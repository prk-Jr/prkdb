#!/usr/bin/env bash
# Fails if known-false documentation claims return (DOC-01, DOC-06, DOC-09, DOC-11).
set -euo pipefail
cd "$(dirname "$0")/.."
fail=0
check() { [ -e "$2" ] || { echo "  ✗ missing path: $2"; fail=1; return; }; if grep -rqn -- "$1" "$2"; then echo "  ✗ found forbidden claim: $1"; fail=1; fi; }
check "Serializable\*\* isolation mode by default" docs/guide
check "Rust-1.95" README.md
check "Rust 1.75" docs/guide
check "99\.4%" README.md
check "10x less" docs/guide

# Positive checks: docs must stay in sync with the actual defaults/behavior.
grep -q "Rust-$(sed -n 's/^rust-version = "\(.*\)"/\1/p' Cargo.toml)+" README.md || { echo "  ✗ README Rust badge does not match Cargo.toml rust-version"; fail=1; }
grep -q "ReadCommitted" docs/guide/features/transactions.md || { echo "  ✗ transactions.md no longer documents the ReadCommitted default"; fail=1; }
grep -q "RFT-08" docs/guide/deployment.md || { echo "  ✗ deployment.md lacks the RFT-08 known-issue note"; fail=1; }
grep -q "PRKDB_ALLOW_UNAUTHENTICATED_PEERS" docs/guide/deployment.md || { echo "  ✗ deployment.md lacks PRKDB_ALLOW_UNAUTHENTICATED_PEERS"; fail=1; }
grep -q "RFT-08" README.md || { echo "  ✗ README.md lacks the RFT-08 known-issue note"; fail=1; }
grep -q "PRKDB_ALLOW_UNAUTHENTICATED_PEERS" README.md || { echo "  ✗ README.md lacks PRKDB_ALLOW_UNAUTHENTICATED_PEERS"; fail=1; }

exit $fail
