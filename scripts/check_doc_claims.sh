#!/usr/bin/env bash
# Fails if known-false documentation claims return (DOC-01, DOC-06, DOC-09, DOC-11).
set -euo pipefail
cd "$(dirname "$0")/.."
fail=0
check() { if grep -rqn -- "$1" $2; then echo "  ✗ found forbidden claim: $1"; fail=1; fi; }
check "Serializable\*\* isolation mode by default" docs/guide
check "Rust-1.95" README.md
check "Rust 1.75" docs/guide
check "99.4%" README.md
check "10x less" docs/guide
grep -q "PRKDB_CLUSTER_SECRET" docs/guide/deployment.md || { echo "  ✗ deployment.md lacks PRKDB_CLUSTER_SECRET"; fail=1; }
grep -q "PRKDB_CLUSTER_SECRET" docker-compose.yml || { echo "  ✗ docker-compose.yml lacks PRKDB_CLUSTER_SECRET"; fail=1; }
exit $fail
