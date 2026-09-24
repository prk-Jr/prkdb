#!/usr/bin/env bash
# TST-09 regression evidence: every WAL benchmark in iai_hot_paths.rs has a floor, and the
# floor logic rejects a vacuous measurement. Runs without Valgrind.
set -euo pipefail
cd "$(dirname "$0")/.."
python3 scripts/perf_gate_deltas.py floors --self-test
missing=0
for b in $(grep -oE '^fn (bench_wal_[a-z0-9_]+)' crates/prkdb/benches/iai_hot_paths.rs | awk '{print $2}'); do
  if ! grep -q "^\[floors\.$b\]" scripts/perf_gate_floors.toml; then
    echo "no floor for $b in scripts/perf_gate_floors.toml"; missing=1
  fi
done
exit "$missing"
