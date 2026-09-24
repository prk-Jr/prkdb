#!/usr/bin/env python3
"""Extract per-benchmark instruction-count deltas from gungraun's own JSON summaries.

Used by .github/workflows/perf-gate.yml (Task 1.13, spec §6.2).

Why this reads gungraun's `--save-summary=json` files instead of scraping terminal
output: gungraun-runner forces color output from `CARGO_TERM_COLOR`/`GUNGRAUN_COLOR`
(see `gungraun-runner-0.19.4/src/main.rs`), so a plain-text scrape of `cargo bench`'s
stdout previously found zero rows on every run and the gate always passed. gungraun's
`--save-summary=json` writes one `summary.json` file per benchmark (version-6 schema,
`gungraun_runner::summary::model::BenchmarkSummary`) under `target/gungraun/`, and that
schema is stable and documented in the crate source, unlike scraping colored terminal
output.

Schema notes (from gungraun-runner 0.19.4's `src/summary/model.rs` and
`src/metrics/model.rs`, read directly since Valgrind cannot run on macOS to produce a
real sample):

  BenchmarkSummary.profiles is a `Profiles(Vec<Profile>)` tuple struct, which serializes
  transparently as a JSON array. Each `Profile.summaries.total` is a `ProfileTotal` with:
    - `regressions: Vec<ToolRegression>` — gungraun's own regression verdict for this
      benchmark (populated when `--callgrind-limits` is set and a soft/hard limit was
      exceeded). A non-empty list is authoritative: it is what actually drove gungraun's
      exit code 3, so this script trusts it rather than recomputing a threshold.
    - `summary: ToolMetricSummary` — externally tagged by tool, e.g.
      `{"Callgrind": {"Ir": {"metrics": ..., "diffs": ...}, "Dr": {...}, ...}}`, where the
      inner map is a `MetricsSummary<EventKind>` (an `IndexMap`, so it serializes as a
      plain JSON object keyed by the (unrenamed) `EventKind` variant name, e.g. "Ir").

  Each `MetricsDiff.metrics` is an `EitherOrBoth<Metric>` (from the `either-or-both`
  crate, derived Serialize -> externally tagged: `{"Left": ..}`, `{"Both": [new, old]}`,
  or `{"Right": ..}`). Per convention (documented on `MetricsDiff`) the new run is the
  left/first side and the old run is the right/second side. `Metric` is
  `{"Int": <u64>}` or `{"Float": <f64>}`. `MetricsDiff.diffs` is `None` when there is
  nothing to compare against (first-ever run for that benchmark id) and otherwise a
  `Diffs { diff_pct, factor }`, both serialized as strings (not JSON numbers) to
  preserve +-inf/NaN without becoming `null`.

  A `Left`-only `metrics` value (no `diffs`) means this benchmark has no prior baseline
  to compare against. Whether that is an error depends on whether the benchmark existed
  at base at all (see `--base-list` below): a *new* benchmark added by this PR has never
  been benched at base and a missing comparison is expected ("new: no comparison"); a
  benchmark that *did* exist at base but still shows up Left-only means the comparison
  gungraun was asked to do silently didn't happen, which is a bug worth failing on.

`--base-list`: a text file, one benchmark name per line (as produced by this script's
own `list-names` subcommand, run against target/gungraun right after the base bench
step and before the head step overwrites those same summary.json files — both base and
head write to `dir.join("summary.json")` for a given benchmark, per
`SummaryOutput::init` in `src/summary/model.rs`, so nothing from base's summaries
survives the head run). Names not in this file are new; names in this file with no
matching head summary are reported as removed (a warning, not a failure — the base
benchmark may have been intentionally renamed or deleted).

NOTE ON CONFIDENCE: the field names, tagging conventions, and the "totals of parts" /
"Ir under Callgrind" structure above were read directly from gungraun-runner 0.19.4's
source (not from a real captured summary.json — Valgrind does not run on macOS, so no
run was possible locally). This has been verified against `cargo bench -p prkdb --bench
iai_hot_paths --no-run` for compilation only; the actual JSON shape can only be
confirmed by a real Linux CI run of this workflow.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path


class SummaryError(Exception):
    """Raised for a summary.json that does not match the expected schema."""


def metric_value(metric: dict) -> float:
    if "Int" in metric:
        return metric["Int"]
    if "Float" in metric:
        return metric["Float"]
    raise SummaryError(f"unrecognized Metric encoding: {metric!r}")


def find_ir_total(benchmark: dict) -> tuple[dict, list] | None:
    """Return (Ir MetricsDiff, regressions) from the Callgrind profile's total, or None."""
    for profile in benchmark.get("profiles", []):
        if profile.get("tool") != "Callgrind":
            continue
        total = profile.get("summaries", {}).get("total", {})
        callgrind = total.get("summary", {}).get("Callgrind")
        if callgrind is None:
            continue
        ir = callgrind.get("Ir")
        if ir is None:
            continue
        return ir, total.get("regressions", [])
    return None


def benchmark_name(benchmark: dict) -> str:
    name = benchmark.get("module_path", "<unknown module_path>")
    bench_id = benchmark.get("id")
    if bench_id:
        name = f"{name}::{bench_id}"
    return name


def load_benchmark(path: Path) -> dict | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def extract_row(path: Path, base_names: set[str] | None) -> dict:
    benchmark = load_benchmark(path)
    if benchmark is None:
        return {"name": str(path), "error": f"could not parse {path} as JSON"}

    name = benchmark_name(benchmark)
    existed_at_base = base_names is not None and name in base_names

    found = find_ir_total(benchmark)
    if found is None:
        return {"name": name, "error": "no Callgrind Instructions (Ir) metric found"}
    ir, regressions = found

    metrics = ir.get("metrics", {})
    diffs = ir.get("diffs")

    try:
        if "Both" in metrics:
            new_raw, old_raw = metrics["Both"]
            head = metric_value(new_raw)
            base = metric_value(old_raw)
            if diffs is not None:
                pct = float(diffs["diff_pct"])
            elif base:
                pct = (head - base) / base * 100.0
            else:
                pct = 0.0
            bootstrap = False
        elif "Left" in metrics:
            head = metric_value(metrics["Left"])
            base = None
            pct = 0.0
            bootstrap = True
        elif "Right" in metrics:
            # Old data only, no new run. Not expected for a head-run summary, but handled
            # rather than crashing the gate on an unanticipated shape.
            base = metric_value(metrics["Right"])
            head = None
            pct = None
            bootstrap = True
        else:
            raise SummaryError(f"unrecognized EitherOrBoth encoding: {metrics!r}")
    except SummaryError as error:
        return {"name": name, "error": str(error)}

    row = {
        "name": name,
        "base": base,
        "head": head,
        "pct": pct,
        "bootstrap": bootstrap,
        "regressed": len(regressions) > 0,
        "regressions": regressions,
    }
    if bootstrap:
        row["new"] = base_names is not None and not existed_at_base
    return row


def find_summaries(root: Path) -> list[Path]:
    return sorted(root.rglob("summary.json"))


def read_base_list(path: str | None) -> set[str] | None:
    if path is None:
        return None
    text = Path(path).read_text(encoding="utf-8")
    return {line.strip() for line in text.splitlines() if line.strip()}


def extract(root: Path, base_names: set[str] | None) -> tuple[list[dict], list[str]]:
    """Return (rows, problems). `problems` is non-empty iff the gate should fail closed."""
    paths = find_summaries(root)
    rows = [extract_row(path, base_names) for path in paths]

    problems: list[str] = []
    if not rows:
        problems.append(f"found zero benchmark summaries under {root}")

    head_names = {row["name"] for row in rows}
    for row in rows:
        if "error" in row:
            problems.append(f"{row['name']}: {row['error']}")
        elif row.get("bootstrap") and base_names is not None and not row.get("new"):
            problems.append(
                f"{row['name']}: no base comparison, but this benchmark existed at base"
            )

    if base_names is not None:
        for missing in sorted(base_names - head_names):
            rows.append(
                {
                    "name": missing,
                    "removed": True,
                    "note": "existed at base, no summary at head (deleted or renamed?)",
                }
            )

    return rows, problems


def list_names(root: Path) -> int:
    """Print one benchmark name per line, for capturing as a `--base-list` file."""
    for path in find_summaries(root):
        benchmark = load_benchmark(path)
        if benchmark is not None:
            print(benchmark_name(benchmark))
    return 0


def summary(deltas_path: str) -> int:
    """Print the deltas as a markdown table (for $GITHUB_STEP_SUMMARY)."""
    with open(deltas_path, encoding="utf-8") as f:
        rows = json.load(f)
    print("| Benchmark | Base | Head | Delta | Regressed |")
    print("|---|---|---|---|---|")
    for r in rows:
        if r.get("removed"):
            print(f"| {r['name']} | - | - | REMOVED: {r['note']} | - |")
            continue
        if "error" in r:
            print(f"| {r['name']} | - | - | ERROR: {r['error']} | - |")
            continue
        if r.get("bootstrap"):
            label = "new: no comparison" if r.get("new") else "bootstrap: no comparison"
            print(f"| {r['name']} | - | {r['head']} | {label} | - |")
            continue
        print(
            f"| {r['name']} | {r['base']} | {r['head']} | {r['pct']:+.2f}% "
            f"| {'yes' if r.get('regressed') else 'no'} |"
        )
    return 0


def regressed(deltas_path: str) -> int:
    """Print one benchmark name per line for every gungraun-flagged regression.

    Trusts gungraun's own `regressions` verdict (populated from `--callgrind-limits`
    and driving its exit code 3) exclusively — this used to also independently flag
    any row with pct > 5%, which could disagree with gungraun's own limit check
    (e.g. a different metric, or a hard limit) and made this list not actually
    reflect what gungraun decided.
    """
    with open(deltas_path, encoding="utf-8") as f:
        rows = json.load(f)
    for r in rows:
        if r.get("regressed"):
            print(r["name"])
    return 0


def main() -> int:
    if len(sys.argv) == 3 and sys.argv[1] == "--summary":
        return summary(sys.argv[2])
    if len(sys.argv) == 3 and sys.argv[1] == "--regressed":
        return regressed(sys.argv[2])
    if len(sys.argv) == 3 and sys.argv[1] == "list-names":
        return list_names(Path(sys.argv[2]))
    if len(sys.argv) >= 3 and sys.argv[1] == "extract":
        root = Path(sys.argv[2])
        rest = sys.argv[3:]
        base_list_path = None
        if "--base-list" in rest:
            idx = rest.index("--base-list")
            try:
                base_list_path = rest[idx + 1]
            except IndexError:
                print("perf_gate_deltas.py: --base-list requires a file path", file=sys.stderr)
                return 2
        base_names = read_base_list(base_list_path)

        rows, problems = extract(root, base_names)
        print(json.dumps(rows, indent=2))
        if problems:
            for problem in problems:
                print(f"perf_gate_deltas.py: {problem}", file=sys.stderr)
            return 1
        return 0

    print(
        "usage: perf_gate_deltas.py extract <gungraun-target-dir> [--base-list <file>]\n"
        "       perf_gate_deltas.py list-names <gungraun-target-dir>\n"
        "       perf_gate_deltas.py --summary <deltas.json>\n"
        "       perf_gate_deltas.py --regressed <deltas.json>",
        file=sys.stderr,
    )
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
