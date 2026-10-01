#!/usr/bin/env python3
"""Apply the spec's 2a rule to raw WAL write-path bench output (Tasks 2.2, 2.9).

Rule (spec §7 2a, decision record §6 risk 1): the new write path in Fast mode may lose at
most 15 % put throughput against the path it replaces, per (writers, value size) cell.

Input is the bench's raw stdout (`print_row` in `crates/prkdb/benches/wal_write_path.rs`),
never a hand-edited table.

Base/head mode (`--base`) compares the adapter cell in the head run against the same cell
in the base run, both benched in the same job on the same runner. The cell is named
`adapter_put` since Task 2.9 and `current_adapter_put` before it; each run's own name is
used, so an old base ref still compares.

Head-only mode compares `wal_fast` (and any `wal_fast_*` variant) in the head run against
the `wal_fast` rows of a reference file (`--reference`). The old in-run reference,
`current_mmap_wal`, went with the mmap WAL in Task 2.9 (D13). The default reference is the
decision record, whose only `wal_fast` rows are the raw Task 2.6 rows in §9.3 (§8 holds
Task 2.2 rows for the old cells and §10 holds `wal_fast_*` variants; both are ignored).

Exit status 1 if any cell loses more than 15 %. `--self-test` checks the parser and both
modes against scripts/testdata/wal_bench_sample.md and exits non-zero on any surprise.
"""
from __future__ import annotations

import argparse
import contextlib
import io
import re
import statistics
import sys
import tempfile
from pathlib import Path

ROW = re.compile(r"^\| (?P<cell>[a-z_]+)/(?P<w>\d+)w/(?P<v>\d+)k \| \d+ \| \d+ KiB \| (?P<ops>\d+) \|")
LIMIT = 0.85
HERE = Path(__file__).resolve().parent
FIXTURE = HERE / "testdata" / "wal_bench_sample.md"
DEFAULT_REFERENCE = HERE.parent / "docs" / "remediation" / "decisions" / "2026-09-24-single-log-spike.md"
ADAPTER_CELLS = ("adapter_put", "current_adapter_put")


def parse(path: str | Path) -> dict[tuple[str, int, int], float]:
    samples: dict[tuple[str, int, int], list[float]] = {}
    with open(path, encoding="utf-8") as f:
        for line in f:
            m = ROW.match(line)
            if m:
                key = (m["cell"], int(m["w"]), int(m["v"]))
                samples.setdefault(key, []).append(float(m["ops"]))
    if not samples:
        sys.exit(f"{path}: no bench rows found; the row format changed or the bench failed")
    return {k: statistics.median(v) for k, v in samples.items()}


def adapter_cells(run: dict) -> dict[tuple[int, int], float]:
    """The adapter cell of one run, under whichever name that run printed."""
    for name in ADAPTER_CELLS:
        cells = {(w, v): ops for (cell, w, v), ops in run.items() if cell == name}
        if cells:
            return cells
    return {}


def pairs_for(head: dict, base: dict | None, reference: dict | None) -> list[tuple[str, float, float]]:
    pairs = []
    if base is not None:
        new, old = adapter_cells(head), adapter_cells(base)
        for (w, v), old_ops in sorted(old.items()):
            if (w, v) in new:
                pairs.append((f"adapter_put/{w}w/{v}k", new[(w, v)], old_ops))
    else:
        # `wal_fast`, plus any `wal_fast_*` variant cells: each is held to the same rule
        # against the reference's plain `wal_fast` rows.
        for (cell, w, v), new in sorted(head.items()):
            if cell.startswith("wal_fast") and ("wal_fast", w, v) in reference:
                pairs.append((f"{cell}/{w}w/{v}k", new, reference[("wal_fast", w, v)]))
    return pairs


def compare(pairs: list[tuple[str, float, float]]) -> dict[str, bool]:
    print("| cell | new ops/s | old ops/s | ratio | verdict |")
    print("|---|--:|--:|--:|---|")
    verdicts = {}
    for name, new, old in pairs:
        ratio = new / old if old else float("inf")
        verdicts[name] = ratio >= LIMIT
        print(f"| {name} | {new:.0f} | {old:.0f} | {ratio:.2f} | {'ok' if verdicts[name] else 'LOSS > 15 %'} |")
    return verdicts


def run(head_path, base_path=None, reference_path=DEFAULT_REFERENCE) -> tuple[int, dict[str, bool]]:
    head = parse(head_path)
    if base_path:
        pairs = pairs_for(head, parse(base_path), None)
    else:
        pairs = pairs_for(head, None, parse(reference_path))
    if not pairs:
        sys.exit(
            "no comparable cells: expected adapter_put (or current_adapter_put) in both runs, "
            "or wal_fast in the head run and the reference"
        )
    verdicts = compare(pairs)
    failed = sum(not ok for ok in verdicts.values())
    print(f"\n{failed} cell(s) lose more than 15 %" if failed else "\nall cells within the 15 % rule")
    return (1 if failed else 0), verdicts


def self_test() -> int:
    problems = []
    lines = FIXTURE.read_text(encoding="utf-8").splitlines(keepends=True)
    with tempfile.TemporaryDirectory() as tmp:
        old_only = Path(tmp) / "old.md"
        new_only = Path(tmp) / "new.md"
        old_only.write_text("".join(l for l in lines if l.startswith("| current_adapter_put/")))
        new_only.write_text("".join(l for l in lines if l.startswith("| adapter_put/")))
        with contextlib.redirect_stdout(io.StringIO()):
            # Reference mode against itself: every wal_fast cell at ratio 1.00.
            self_code, self_v = run(FIXTURE, reference_path=FIXTURE)
            # Default reference (decision record §9.3): the fixture's 1w/64k row loses.
            code, v = run(FIXTURE)
            base_code, bv = run(FIXTURE, FIXTURE)
            # An old base ref (current_adapter_put) against a new head (adapter_put).
            mixed_code, mv = run(new_only, old_only)

    want_self = {"wal_fast/1w/1k": True, "wal_fast/8w/1k": True, "wal_fast/1w/64k": True}
    if self_v != want_self or self_code != 0:
        problems.append(f"reference=fixture: got {self_v} exit {self_code}, want {want_self} exit 0")
    want = {"wal_fast/1w/1k": True, "wal_fast/8w/1k": True, "wal_fast/1w/64k": False}
    if v != want or code != 1:
        problems.append(f"default reference: got {v} exit {code}, want {want} exit 1")
    if bv != {"adapter_put/1w/1k": True} or base_code != 0:
        problems.append(f"base/head: got {bv} exit {base_code}, want adapter_put/1w/1k ok, exit 0")
    if mv != {"adapter_put/1w/1k": True} or mixed_code != 0:
        problems.append(f"old base, new head: got {mv} exit {mixed_code}, want adapter_put/1w/1k ok, exit 0")
    for p in problems:
        print(f"self-test FAILED: {p}", file=sys.stderr)
    if not problems:
        print("wal_fast_rule.py self-test: ok")
    return 1 if problems else 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--head")
    ap.add_argument("--base")
    ap.add_argument(
        "--reference",
        default=str(DEFAULT_REFERENCE),
        help="head-only mode: file whose wal_fast rows are the reference (default: the decision record)",
    )
    ap.add_argument("--self-test", action="store_true")
    args = ap.parse_args()
    if args.self_test:
        return self_test()
    if not args.head:
        ap.error("--head is required unless --self-test")
    return run(args.head, args.base, args.reference)[0]


if __name__ == "__main__":
    sys.exit(main())
