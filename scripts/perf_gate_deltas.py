#!/usr/bin/env python3
"""Extract per-benchmark instruction-count deltas from a gungraun/iai-callgrind run.

Used by .github/workflows/perf-gate.yml (Task 1.13, spec §6.2).

Why this parses stdout, not a JSON summary file: gungraun (like iai-callgrind, its
predecessor under the old name) compares each benchmark id against the *previous* run
recorded in the target directory for that same id, and prints the comparison to stdout.
The workflow benches the PR's base SHA first (which becomes "the previous run") and then
the head SHA in the same job, on the same runner, without clearing target/ in between —
so the head SHA's own terminal output already contains the base-vs-head diff for every
benchmark, one line per tracked metric:

    bench_group::bench_function_name
      Instructions:            1234|1000        (+23.4000%) [...]
      L1 Hits:                 ...

This script reads the *head* run's log, keyed by the benchmark id line that precedes each
metric block, and pulls out the "Instructions:" line's new count, old count, and percent
change. A first-ever run (no previous baseline) prints "N/A" for old and no percentage;
that is reported as a 0% delta rather than a crash, since there is nothing to regress
against yet.

NOTE ON CONFIDENCE: this format matches iai-callgrind's long-standing, documented
terminal output. It has not been (and cannot be, on macOS, since Valgrind does not run
there) verified against a real gungraun 0.19.4 run. If the format has changed, this
script's regex needs updating — the workflow's job summary step prints the raw deltas.json
either way, so a parsing failure is visible rather than silently gating on zero rows.
"""
from __future__ import annotations

import json
import re
import sys

# A benchmark id line, e.g. "hot_paths::bench_wal_put id:1" or "hot_paths::bench_wal_put".
# Deliberately permissive: gungraun/iai-callgrind's id suffix format has varied across
# versions, but every version's id line is a bare line (no leading whitespace) that is
# immediately followed by indented "Metric:" lines.
ID_LINE = re.compile(r"^(?!\s)(?!Executable\b)(?!\s*$)([A-Za-z0-9_:]+(?:\s+id:\S+)?)\s*$")

INSTRUCTIONS_LINE = re.compile(
    r"^\s*Instructions:\s+"
    r"([\d,]+|N/A)\s*\|\s*([\d,]+|N/A)"
    r"(?:\s+\(([+-]?[\d.]+)%\))?"
)


def parse_count(token: str) -> int | None:
    if token == "N/A":
        return None
    return int(token.replace(",", ""))


def extract(log_path: str) -> list[dict]:
    with open(log_path, encoding="utf-8", errors="replace") as f:
        lines = f.readlines()

    rows: list[dict] = []
    current_id: str | None = None
    for line in lines:
        id_match = ID_LINE.match(line.rstrip("\n"))
        if id_match:
            current_id = id_match.group(1).strip()
            continue

        instr_match = INSTRUCTIONS_LINE.match(line)
        if instr_match and current_id is not None:
            new_raw, old_raw, pct_raw = instr_match.groups()
            new = parse_count(new_raw)
            old = parse_count(old_raw)
            if pct_raw is not None:
                pct = float(pct_raw)
            elif new is not None and old is not None and old != 0:
                pct = (new - old) / old * 100.0
            else:
                # No previous baseline to compare against (first run for this id).
                pct = 0.0
            rows.append(
                {
                    "name": current_id,
                    "base": old if old is not None else "N/A",
                    "head": new if new is not None else "N/A",
                    "pct": pct,
                }
            )
            current_id = None

    return rows


REGRESSION_THRESHOLD_PCT = 5.0


def summary(deltas_path: str) -> int:
    """Print the deltas as a markdown table (for $GITHUB_STEP_SUMMARY)."""
    with open(deltas_path) as f:
        rows = json.load(f)
    print("| Benchmark | Base | Head | Delta |")
    print("|---|---|---|---|")
    for r in rows:
        print(f"| {r['name']} | {r['base']} | {r['head']} | {r['pct']:+.2f}% |")
    return 0


def regressed(deltas_path: str) -> int:
    """Print one benchmark name per line for every regression above the threshold."""
    with open(deltas_path) as f:
        rows = json.load(f)
    for r in rows:
        if r["pct"] > REGRESSION_THRESHOLD_PCT:
            print(r["name"])
    return 0


def main() -> int:
    if len(sys.argv) == 3 and sys.argv[1] == "--summary":
        return summary(sys.argv[2])
    if len(sys.argv) == 3 and sys.argv[1] == "--regressed":
        return regressed(sys.argv[2])
    if len(sys.argv) != 3:
        print(
            "usage: perf_gate_deltas.py <base-bench.log> <head-bench.log>\n"
            "       perf_gate_deltas.py --summary <deltas.json>\n"
            "       perf_gate_deltas.py --regressed <deltas.json>",
            file=sys.stderr,
        )
        return 2

    # base-bench.log is accepted for symmetry with how the workflow calls this script and
    # for future use (e.g. cross-checking base's own instruction counts), but the head
    # run's own comparison output is what actually carries the base-vs-head diff; see the
    # module docstring.
    _base_log, head_log = sys.argv[1], sys.argv[2]

    rows = extract(head_log)
    print(json.dumps(rows, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
