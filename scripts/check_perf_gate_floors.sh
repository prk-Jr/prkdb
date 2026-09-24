#!/usr/bin/env bash
# TST-09 regression evidence: every WAL benchmark in iai_hot_paths.rs has a floor, every
# floor names a benchmark that still exists, and the floor/extract logic rejects a
# vacuous measurement or a renamed-bench false failure. Runs without Valgrind.
set -euo pipefail
cd "$(dirname "$0")/.."
python3 scripts/perf_gate_deltas.py floors --self-test
python3 scripts/perf_gate_deltas.py extract --self-test
missing=0

# Every `bench_wal_*` function has a `[floors.*]` entry (a renamed/added WAL benchmark
# with no floor would silently escape the gate).
for b in $(grep -oE '^fn (bench_wal_[a-z0-9_]+)' crates/prkdb/benches/iai_hot_paths.rs | awk '{print $2}'); do
  if ! grep -q "^\[floors\.$b\]" scripts/perf_gate_floors.toml; then
    echo "no floor for $b in scripts/perf_gate_floors.toml"; missing=1
  fi
done

# Every `[floors.*]` entry names a function that still exists (a stale entry left behind
# by a rename would otherwise report "missing benchmark" against a name nothing ever
# produces again, forever failing the gate for the wrong reason).
for f in $(grep -oE '^\[floors\.[A-Za-z0-9_]+\]' scripts/perf_gate_floors.toml | sed -E 's/^\[floors\.(.+)\]$/\1/'); do
  if ! grep -q "^fn $f(" crates/prkdb/benches/iai_hot_paths.rs; then
    echo "scripts/perf_gate_floors.toml has [floors.$f] but crates/prkdb/benches/iai_hot_paths.rs has no fn $f"; missing=1
  fi
done

exit "$missing"
