#!/usr/bin/env python3
"""Apply the spec's 2a rule to raw WAL write-path bench output (Task 2.2).

Rule (spec §7 2a, decision record §6 risk 1): the new write path in Fast mode may lose at
most 15 % put throughput against the path it replaces, per (writers, value size) cell.

Input is the bench's raw stdout (`print_row` in the bench), never a hand-edited table.
Head-only mode compares cells inside one run: `wal_fast` vs `current_mmap_wal`.
Base/head mode compares `current_adapter_put` in the head run (new adapter) against the
same cell in the base run (old adapter), both benched in the same job on the same runner.
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
from pathlib import Path

ROW = re.compile(r"^\| (?P<cell>[a-z_]+)/(?P<w>\d+)w/(?P<v>\d+)k \| \d+ \| \d+ KiB \| (?P<ops>\d+) \|")
LIMIT = 0.85
FIXTURE = Path(__file__).resolve().parent / "testdata" / "wal_bench_sample.md"


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


def pairs_for(head: dict, base: dict | None) -> list[tuple[str, float, float]]:
    pairs = []
    if base is not None:
        for (cell, w, v), old in sorted(base.items()):
            if cell == "current_adapter_put" and (cell, w, v) in head:
                pairs.append((f"adapter_put/{w}w/{v}k", head[(cell, w, v)], old))
    else:
        for (cell, w, v), new in sorted(head.items()):
            if cell == "wal_fast" and ("current_mmap_wal", w, v) in head:
                pairs.append((f"wal_fast/{w}w/{v}k", new, head[("current_mmap_wal", w, v)]))
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


def run(head_path, base_path=None) -> tuple[int, dict[str, bool]]:
    head = parse(head_path)
    pairs = pairs_for(head, parse(base_path) if base_path else None)
    if not pairs:
        sys.exit("no comparable cells: expected wal_fast + current_mmap_wal, or current_adapter_put in both runs")
    verdicts = compare(pairs)
    failed = sum(not ok for ok in verdicts.values())
    print(f"\n{failed} cell(s) lose more than 15 %" if failed else "\nall cells within the 15 % rule")
    return (1 if failed else 0), verdicts


def self_test() -> int:
    with contextlib.redirect_stdout(io.StringIO()):
        code, v = run(FIXTURE)
        base_code, bv = run(FIXTURE, FIXTURE)
    want = {"wal_fast/1w/1k": True, "wal_fast/8w/1k": True, "wal_fast/1w/64k": False}
    problems = []
    if v != want or code != 1:
        problems.append(f"head-only: got {v} exit {code}, want {want} exit 1")
    if bv != {"adapter_put/1w/1k": True} or base_code != 0:
        problems.append(f"base/head: got {bv} exit {base_code}, want adapter_put/1w/1k ok, exit 0")
    for p in problems:
        print(f"self-test FAILED: {p}", file=sys.stderr)
    if not problems:
        print("wal_fast_rule.py self-test: ok")
    return 1 if problems else 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--head")
    ap.add_argument("--base")
    ap.add_argument("--self-test", action="store_true")
    args = ap.parse_args()
    if args.self_test:
        return self_test()
    if not args.head:
        ap.error("--head is required unless --self-test")
    return run(args.head, args.base)[0]


if __name__ == "__main__":
    sys.exit(main())
