#!/usr/bin/env python3
"""Apply the spec's 2a rule, and the compaction tail-latency rule, to raw WAL write-path
bench output (Tasks 2.2, 2.9, 2.15).

Input is the bench's raw stdout (`print_row` in `crates/prkdb/benches/wal_write_path.rs`),
never a hand-edited table.

Fast rule (spec §7 2a, decision record §6 risk 1): the new write path in Fast mode may lose
at most 15 % put throughput against the path it replaces, per (writers, value size) cell.

- Base/head mode (`--base`) compares the adapter cell in the head run against the same
  cell in the base run, both benched in the same job on the same runner. The cell is named
  `adapter_put` since Task 2.9 and `current_adapter_put` before it; each run's own name is
  used, so an old base ref still compares.
- Head-only mode compares `wal_fast` (and any `wal_fast_*` variant) in the head run against
  the `wal_fast` rows of a reference file (`--reference`). The default reference is the
  decision record, whose only `wal_fast` rows are the raw Task 2.6 rows in §9.3 (§10 holds
  `wal_fast_*` variants, which are ignored as references).

Compaction tail rule (`--compaction-tail`, Task 2.15): in the head run, per cell,
p99(compaction_concurrent_put) <= K x p99(adapter_put). See COMPACTION_P99_K for K.

Fail closed. Every cell of the bench grid (writers 1/8/64 x values 1/64 KiB) must be
present for the cells a mode compares, on both sides; a bench run must declare its rep
count (`reps N` in its header, which `--reps` can pin) and every cell must have exactly
that many rows; every measurement must be positive. A comparison that cannot be made is a
failure, never a pass.

Exit status: 0 all cells pass, 1 a cell breaks a rule, 2 the input cannot be judged
(missing cell, wrong rep count, non-positive measurement, no rows). `--self-test` checks
the parser, every mode and every fail-closed case against
scripts/testdata/wal_bench_sample.md and exits non-zero on any surprise.
"""
from __future__ import annotations

import argparse
import contextlib
import io
import re
import statistics
import sys
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

ROW = re.compile(
    r"^\| (?P<cell>[a-z_]+)/(?P<w>\d+)w/(?P<v>\d+)k \| \d+ \| \d+ KiB \| (?P<ops>[-\d.]+) \|"
    r" [^|]* \| [^|]* \| (?P<p99>[-\d.]+) \|"
)
REPS = re.compile(r"^- warm-up .*\breps (?P<reps>\d+)\b")
LIMIT = 0.85
# Compaction tail rule: p99 with compaction running back to back may be at most K times
# the ordinary adapter put's p99 in the same run, per cell.
#
# NOT YET CALIBRATED. No Linux probe has run the compaction cells: the last wal-bench probe
# (https://github.com/prk-Jr/prkdb/actions/runs/36956028167, at c7609dd) predates the cell
# (7befbeb). K is therefore set from that probe's other rows, conservatively: the tightest
# cell is adapter_put/1w/1k at p99 ~66 us, and a put there that waits behind one 1 MiB
# segment rewrite and its sync_data (~2.6 ms at the 380 writes/s ceiling the same run
# measured) plus the log sync compaction forces in Fast mode (~0.4 ms p99 for a small
# write) sees ~3 ms, about 45x. K = 50 admits that and still fails a put path that stalls
# for a whole compaction pass (many segments, tens to hundreds of ms) in any cell.
# Calibrate on the next wal-bench probe: K = about 2x the worst per-cell ratio observed,
# not below the stall bound above, with the raw rows recorded in the decision record.
COMPACTION_P99_K = 50.0
HERE = Path(__file__).resolve().parent
FIXTURE = HERE / "testdata" / "wal_bench_sample.md"
DEFAULT_REFERENCE = HERE.parent / "docs" / "remediation" / "decisions" / "2026-09-24-single-log-spike.md"
ADAPTER_CELLS = ("adapter_put", "current_adapter_put")
GRID = tuple((w, v) for v in (1, 64) for w in (1, 8, 64))

Key = tuple[str, int, int]


class Unjudgeable(Exception):
    """The input cannot support a verdict; the gate fails closed (exit 2)."""


@dataclass
class Run:
    path: str
    declared_reps: int | None
    samples: dict[Key, list[tuple[float, float]]] = field(default_factory=dict)

    def ops(self, key: Key) -> float:
        return statistics.median(o for o, _ in self.samples[key])

    def p99(self, key: Key) -> float:
        return statistics.median(p for _, p in self.samples[key])


def parse(path: str | Path) -> Run:
    run = Run(str(path), None)
    try:
        f = open(path, encoding="utf-8")
    except OSError as e:
        raise Unjudgeable(f"{path}: {e.strerror or e}") from e
    with f:
        for line in f:
            if run.declared_reps is None and (r := REPS.match(line)):
                run.declared_reps = int(r["reps"])
            m = ROW.match(line)
            if m:
                key = (m["cell"], int(m["w"]), int(m["v"]))
                run.samples.setdefault(key, []).append((float(m["ops"]), float(m["p99"])))
    if not run.samples:
        raise Unjudgeable(f"{path}: no bench rows found; the row format changed or the bench failed")
    return run


def require(run: Run, cell: str, reps: int | None, *, bench_run: bool = True) -> None:
    """Every grid cell of `cell` present in `run`, with the rep count and positive values.

    `bench_run=False` is for a reference document (the decision record), which carries no
    single rep declaration of its own: only presence and positivity are checked there.
    """
    missing = [f"{cell}/{w}w/{v}k" for w, v in GRID if (cell, w, v) not in run.samples]
    if missing:
        raise Unjudgeable(f"{run.path}: missing cell(s) {', '.join(missing)}")
    if bench_run:
        if run.declared_reps is None:
            raise Unjudgeable(f"{run.path}: no 'reps N' header line; cannot check the rep count")
        if reps is not None and run.declared_reps != reps:
            raise Unjudgeable(f"{run.path}: declares reps {run.declared_reps}, expected {reps}")
        want = run.declared_reps
        short = [
            f"{cell}/{w}w/{v}k has {len(run.samples[(cell, w, v)])}"
            for w, v in GRID
            if len(run.samples[(cell, w, v)]) != want
        ]
        if short:
            raise Unjudgeable(f"{run.path}: expected {want} rep(s) per cell; {', '.join(short)}")
    bad = [
        f"{cell}/{w}w/{v}k"
        for w, v in GRID
        if any(o <= 0 or p <= 0 for o, p in run.samples[(cell, w, v)])
    ]
    if bad:
        raise Unjudgeable(f"{run.path}: non-positive ops/s or p99 in {', '.join(bad)}")


def adapter_name(run: Run) -> str:
    """The adapter cell's name in one run (`adapter_put`, or `current_adapter_put` before 2.9)."""
    for name in ADAPTER_CELLS:
        if any(cell == name for cell, _, _ in run.samples):
            return name
    raise Unjudgeable(f"{run.path}: no adapter_put (or current_adapter_put) rows")


def fast_pairs(head: Run, base: Run | None, reference: Run | None, reps: int | None) -> list[tuple[str, float, float]]:
    pairs = []
    if base is not None:
        new, old = adapter_name(head), adapter_name(base)
        require(head, new, reps)
        require(base, old, reps)
        for w, v in GRID:
            pairs.append((f"adapter_put/{w}w/{v}k", head.ops((new, w, v)), base.ops((old, w, v))))
    else:
        assert reference is not None
        require(head, "wal_fast", reps)
        require(reference, "wal_fast", None, bench_run=False)
        # `wal_fast`, plus any `wal_fast_*` variant cells: each is held to the same rule
        # against the reference's plain `wal_fast` rows.
        variants = sorted({c for c, _, _ in head.samples if c.startswith("wal_fast")})
        for cell in variants:
            if cell != "wal_fast":
                require(head, cell, reps)
            for w, v in GRID:
                pairs.append((f"{cell}/{w}w/{v}k", head.ops((cell, w, v)), reference.ops(("wal_fast", w, v))))
    return pairs


def compare(pairs: list[tuple[str, float, float]]) -> dict[str, bool]:
    print("| cell | new ops/s | old ops/s | ratio | verdict |")
    print("|---|--:|--:|--:|---|")
    verdicts = {}
    for name, new, old in pairs:
        ratio = new / old  # both sides were required positive
        verdicts[name] = ratio >= LIMIT
        print(f"| {name} | {new:.0f} | {old:.0f} | {ratio:.2f} | {'ok' if verdicts[name] else 'LOSS > 15 %'} |")
    return verdicts


def compaction_tail(head: Run, reps: int | None, k: float) -> dict[str, bool]:
    require(head, "adapter_put", reps)
    require(head, "compaction_concurrent_put", reps)
    print(f"| cell | compaction p99 µs | adapter_put p99 µs | ratio | verdict (<= {k:g}x) |")
    print("|---|--:|--:|--:|---|")
    verdicts = {}
    for w, v in GRID:
        comp = head.p99(("compaction_concurrent_put", w, v))
        plain = head.p99(("adapter_put", w, v))
        name = f"compaction_concurrent_put/{w}w/{v}k"
        verdicts[name] = comp <= k * plain
        print(f"| {name} | {comp:.1f} | {plain:.1f} | {comp / plain:.1f} | {'ok' if verdicts[name] else 'TAIL > K'} |")
    return verdicts


def run(head_path, base_path=None, reference_path=DEFAULT_REFERENCE, reps=None) -> tuple[int, dict[str, bool]]:
    head = parse(head_path)
    if base_path:
        pairs = fast_pairs(head, parse(base_path), None, reps)
    else:
        pairs = fast_pairs(head, None, parse(reference_path), reps)
    verdicts = compare(pairs)
    failed = sum(not ok for ok in verdicts.values())
    print(f"\n{failed} cell(s) lose more than 15 %" if failed else "\nall cells within the 15 % rule")
    return (1 if failed else 0), verdicts


def run_compaction(head_path, reps=None, k=COMPACTION_P99_K) -> tuple[int, dict[str, bool]]:
    verdicts = compaction_tail(parse(head_path), reps, k)
    failed = sum(not ok for ok in verdicts.values())
    print(
        f"\n{failed} cell(s) exceed {k:g}x the adapter_put p99 under compaction"
        if failed
        else f"\nall cells within {k:g}x the adapter_put p99 under compaction"
    )
    return (1 if failed else 0), verdicts


def self_test() -> int:
    problems = []
    text = FIXTURE.read_text(encoding="utf-8")
    lines = text.splitlines(keepends=True)
    header = [l for l in lines if not l.startswith("| ") or l.startswith("| cell ")]

    def variant(tmp: str, name: str, keep=lambda l: True, edit=lambda l: l) -> Path:
        p = Path(tmp) / name
        p.write_text("".join(edit(l) for l in lines if l in header or keep(l)))
        return p

    def expect_unjudgeable(label: str, needle: str, fn) -> None:
        try:
            with contextlib.redirect_stdout(io.StringIO()):
                got = fn()
        except Unjudgeable as e:
            if needle not in str(e):
                problems.append(f"{label}: refused, but for {e!s} (want {needle!r})")
            return
        problems.append(f"{label}: got {got}, want a fail-closed refusal mentioning {needle!r}")

    with tempfile.TemporaryDirectory() as tmp:
        old_only = variant(tmp, "old.md", keep=lambda l: l.startswith("| current_adapter_put/"))
        new_only = variant(tmp, "new.md", keep=lambda l: l.startswith("| adapter_put/"))
        with contextlib.redirect_stdout(io.StringIO()):
            # Reference mode against itself: every wal_fast cell at ratio 1.00.
            self_code, self_v = run(FIXTURE, reference_path=FIXTURE, reps=2)
            # Default reference (decision record §9.3): the fixture's 1w/64k row loses.
            code, v = run(FIXTURE, reps=2)
            base_code, bv = run(FIXTURE, FIXTURE, reps=2)
            # An old base ref (current_adapter_put) against a new head (adapter_put).
            mixed_code, mv = run(new_only, old_only, reps=2)
            comp_code, cv = run_compaction(FIXTURE, reps=2)

        all_ok = lambda cell: {f"{cell}/{w}w/{v}k": True for w, v in GRID}  # noqa: E731
        if self_v != all_ok("wal_fast") or self_code != 0:
            problems.append(f"reference=fixture: got {self_v} exit {self_code}, want all ok, exit 0")
        want = {**all_ok("wal_fast"), "wal_fast/1w/64k": False}
        if v != want or code != 1:
            problems.append(f"default reference: got {v} exit {code}, want {want} exit 1")
        if bv != all_ok("adapter_put") or base_code != 0:
            problems.append(f"base/head: got {bv} exit {base_code}, want all ok, exit 0")
        if mv != all_ok("adapter_put") or mixed_code != 0:
            problems.append(f"old base, new head: got {mv} exit {mixed_code}, want all ok, exit 0")
        if cv != all_ok("compaction_concurrent_put") or comp_code != 0:
            problems.append(f"compaction tail: got {cv} exit {comp_code}, want all ok, exit 0")

        # A head run that lost a cell (bench crashed, filter left on): refuse, don't skip it.
        no_cell = variant(tmp, "no_cell.md", keep=lambda l: not l.startswith("| adapter_put/64w/64k "))
        expect_unjudgeable("missing head cell", "missing cell(s) adapter_put/64w/64k", lambda: run(no_cell, FIXTURE, reps=2))
        no_fast = variant(tmp, "no_fast.md", keep=lambda l: not l.startswith("| wal_fast/8w/1k "))
        expect_unjudgeable("missing head wal_fast cell", "wal_fast/8w/1k", lambda: run(no_fast, reference_path=FIXTURE, reps=2))
        # A zero-throughput reference used to give ratio inf and pass.
        zero = variant(tmp, "zero.md", edit=lambda l: l.replace("| 27200 |", "| 0 |"))
        expect_unjudgeable("zero base", "non-positive", lambda: run(FIXTURE, zero, reps=2))
        zero_fast = variant(tmp, "zero_fast.md", edit=lambda l: l.replace("| 80000 |", "| 0 |"))
        expect_unjudgeable("zero reference", "non-positive", lambda: run(FIXTURE, reference_path=zero_fast, reps=2))
        # One rep of one cell missing (the bench died in its last rep).
        first = next(l for l in lines if l.startswith("| adapter_put/8w/1k "))
        fewer = variant(tmp, "fewer.md", keep=lambda l: l is not first)
        expect_unjudgeable("fewer reps", "expected 2 rep(s)", lambda: run(fewer, FIXTURE, reps=2))
        expect_unjudgeable("rep count pinned", "declares reps 2, expected 3", lambda: run(FIXTURE, FIXTURE, reps=3))
        no_reps = variant(tmp, "no_reps.md", keep=lambda l: True, edit=lambda l: "" if l.startswith("- warm-up") else l)
        expect_unjudgeable("no rep declaration", "no 'reps N'", lambda: run(no_reps, FIXTURE))

        # Compaction tail: one cell's p99 past K x adapter_put's fails (exit 1) ...
        # (64w/1k: adapter_put p99 median 1616.5 µs, so K = 50 allows 80825 µs.)
        slow = variant(
            tmp,
            "slow.md",
            edit=lambda l: l.replace("| 9800.0 |", "| 99000.0 |").replace("| 9900.0 |", "| 99000.0 |"),
        )
        with contextlib.redirect_stdout(io.StringIO()):
            slow_code, sv = run_compaction(slow, reps=2)
        want_slow = {**all_ok("compaction_concurrent_put"), "compaction_concurrent_put/64w/1k": False}
        if sv != want_slow or slow_code != 1:
            problems.append(f"compaction tail over K: got {sv} exit {slow_code}, want {want_slow} exit 1")
        # ... and a run without the compaction cells, or a zero p99, cannot be judged.
        no_comp = variant(tmp, "no_comp.md", keep=lambda l: not l.startswith("| compaction_concurrent_put/"))
        expect_unjudgeable("compaction cells missing", "missing cell(s) compaction_concurrent_put", lambda: run_compaction(no_comp, reps=2))
        zero_p99 = variant(tmp, "zero_p99.md", edit=lambda l: l.replace("| 61.0 |", "| 0.0 |"))
        expect_unjudgeable("zero adapter p99", "non-positive", lambda: run_compaction(zero_p99, reps=2))

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
    ap.add_argument("--reps", type=int, help="rep count every bench run must declare and contain (SPIKE_REPS)")
    ap.add_argument(
        "--compaction-tail",
        action="store_true",
        help="check compaction_concurrent_put p99 against adapter_put p99 in the head run instead of the Fast rule",
    )
    ap.add_argument("--k", type=float, default=COMPACTION_P99_K, help="--compaction-tail: the allowed p99 ratio")
    ap.add_argument("--self-test", action="store_true")
    args = ap.parse_args()
    if args.self_test:
        return self_test()
    if not args.head:
        ap.error("--head is required unless --self-test")
    try:
        if args.compaction_tail:
            return run_compaction(args.head, args.reps, args.k)[0]
        return run(args.head, args.base, args.reference, args.reps)[0]
    except Unjudgeable as e:
        print(f"wal_fast_rule.py: FAIL (cannot judge): {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
