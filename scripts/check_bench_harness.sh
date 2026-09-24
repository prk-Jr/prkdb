#!/usr/bin/env bash
#
# TST-04 regression guard: `e2e_throughput_bench` uses Criterion's own `main`
# (`criterion_main!`), which conflicts with the default libtest bench harness. Without a
# `[[bench]] ... harness = false` entry for it in Cargo.toml, `cargo bench` silently ran it
# under the wrong harness and Criterion's output never appeared — the bug this guard
# exists to catch a return of.
set -uo pipefail
cd "$(dirname "$0")/.."

toml=${PRKDB_BENCH_CARGO_TOML:-crates/prkdb/Cargo.toml}

if [[ ! -f "$toml" ]]; then
  echo "check_bench_harness: $toml not found"
  exit 1
fi

# A `[[bench]]` table declares `name` and `harness` as sibling keys within the same
# table, in either order, so match a `[[bench]]` block that contains both
# `name = "e2e_throughput_bench"` and `harness = false` before the next `[[bench]]` (or
# end of file).
if awk '
  /^\[\[bench\]\]/ { if (name == "e2e_throughput_bench" && harness_false) { found = 1 }; name = ""; harness_false = 0; next }
  /^name[ \t]*=[ \t]*"e2e_throughput_bench"/ { name = "e2e_throughput_bench" }
  /^harness[ \t]*=[ \t]*false/ { harness_false = 1 }
  END { if (name == "e2e_throughput_bench" && harness_false) { found = 1 }; exit !found }
' "$toml"; then
  exit 0
fi

echo "check_bench_harness: $toml has no [[bench]] entry for \"e2e_throughput_bench\" with harness = false (TST-04)"
exit 1
