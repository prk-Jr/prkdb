#!/usr/bin/env bash
# TST-09 regression evidence: every WAL benchmark in iai_hot_paths.rs has a floor, every
# floor names a benchmark that still exists, and the floor/extract logic rejects a
# vacuous measurement or a renamed-bench false failure, and every declared floor can fail.
# Runs without Valgrind (CI: ci.yml `perf-gate-floors`; locally: pre-push-check.sh).
set -euo pipefail
cd "$(dirname "$0")/.."
python3 scripts/perf_gate_deltas.py floors --self-test
python3 scripts/perf_gate_deltas.py extract --self-test
# The per-benchmark perf_note override (perf-gate.yml) justifies only the regressions a
# note names.
python3 scripts/perf_gate_deltas.py justify --self-test
# Every declared floor is usable: a reference, and a ratio of at least 0.01 (a lower
# floor admits an empty measured region and can never fail).
python3 scripts/perf_gate_deltas.py floors --validate scripts/perf_gate_floors.toml
missing=0

# This check is also exercised against fixtures below: adding a stream benchmark
# must not let it silently escape mandatory instruction-count floors.
required_floors() {
  local source_file="$1" floors_file="$2" absent=0
  for b in $(grep -oE '^fn (bench_(wal|stream)_[a-z0-9_]+)' "$source_file" | awk '{print $2}'); do
    if ! grep -q "^\[floors\.$b\]" "$floors_file"; then
      echo "no floor for $b in $floors_file"; absent=1
    fi
  done
  return "$absent"
}

required_floors_self_test() {
  local fixture_dir
  fixture_dir=$(mktemp -d)
  cat > "$fixture_dir/benches.rs" <<'SOURCE'
fn bench_wal_existing() {}
fn bench_stream_new() {}
SOURCE
  cat > "$fixture_dir/floors.toml" <<'FLOORS'
[floors.bench_wal_existing]
[floors.bench_stream_new]
FLOORS
  if ! required_floors "$fixture_dir/benches.rs" "$fixture_dir/floors.toml"; then
    rm -rf "$fixture_dir"
    echo "mandatory-floor self-test: complete floors rejected"; return 1
  fi
  cat > "$fixture_dir/floors.toml" <<'FLOORS'
[floors.bench_wal_existing]
FLOORS
  if required_floors "$fixture_dir/benches.rs" "$fixture_dir/floors.toml" > /dev/null; then
    rm -rf "$fixture_dir"
    echo "mandatory-floor self-test: missing stream floor accepted"; return 1
  fi
  cat > "$fixture_dir/floors.toml" <<'FLOORS'
[floors.bench_stream_new]
FLOORS
  if required_floors "$fixture_dir/benches.rs" "$fixture_dir/floors.toml" > /dev/null; then
    rm -rf "$fixture_dir"
    echo "mandatory-floor self-test: missing WAL floor accepted"; return 1
  fi
  rm -rf "$fixture_dir"
  echo "mandatory-floor self-test: ok"
}

required_floors_self_test
if ! required_floors crates/prkdb/benches/iai_hot_paths.rs scripts/perf_gate_floors.toml; then
  missing=1
fi

# Every `[floors.*]` entry names a function that still exists (a stale entry left behind
# by a rename would otherwise report "missing benchmark" against a name nothing ever
# produces again, forever failing the gate for the wrong reason).
for f in $(grep -oE '^\[floors\.[A-Za-z0-9_]+\]' scripts/perf_gate_floors.toml | sed -E 's/^\[floors\.(.+)\]$/\1/'); do
  if ! grep -q "^fn $f(" crates/prkdb/benches/iai_hot_paths.rs; then
    echo "scripts/perf_gate_floors.toml has [floors.$f] but crates/prkdb/benches/iai_hot_paths.rs has no fn $f"; missing=1
  fi
done

exit "$missing"
