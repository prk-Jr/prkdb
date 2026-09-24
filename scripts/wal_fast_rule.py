#!/usr/bin/env python3
"""Apply the Task 2.1 spike's Fast-mode <=15% rule to a `wal_write_path_spike` run.

Background: `docs/remediation/decisions/2026-09-24-single-log-spike.md` §1 records the
spec's stop condition for the Task 2.1 spike: "Does `Fast` mode lose more than 15% put
throughput against the current path?" If yes, the spike escalates to the maintainer
rather than proceeding. §6 Risk 1 notes that answer was measured on macOS, where the
current path pays an unrelated `msync` penalty that Linux does not, so the 1-writer
cells specifically need to be re-measured on Linux before Task 2.2 merges the writer
(Task 2.2, spec §7 Phase 2 preamble).

This script reads the bench's own markdown table (the format `wal_write_path_spike.rs`
prints via `print_header`/`print_row`) from a raw output file or stdin, and for each
(writers, value size) cell present, compares `single_log_fast`'s ops/s against
`current_adapter_put`'s ops/s (the public write path the spike is meant to replace).

Verdict per cell:
  ratio = fast_ops_s / current_ops_s
  loss_pct = (1 - ratio) * 100
  PASS  if loss_pct <= 15.0 (Fast is at most 15% slower, or faster)
  LOSS  otherwise

The 1-writer cells are the ones the decision record's Risk 1 flags as the open
condition; every LOSS cell fails the run, but a LOSS on a 1-writer cell is called out
explicitly, since only the task text's specified mitigation (a bounded `try_recv` spin
before the writer parks, per §6 Risk 1) may be applied before escalating a 1-writer
LOSS to the maintainer.

Usage:
  wal_fast_rule.py check <raw-output-file>   # '-' or omitted = stdin
  wal_fast_rule.py --self-test               # verify parsing + verdict logic

Exit codes: 0 = every cell PASS, 1 = at least one cell LOSS, 2 = usage/parse error.
"""
from __future__ import annotations

import sys
from pathlib import Path

FAST_KIND = "single_log_fast"
CURRENT_KIND = "current_adapter_put"
LOSS_THRESHOLD_PCT = 15.0

FIXTURE_PATH = Path(__file__).resolve().parent / "fixtures" / "wal_fast_rule_sample.md"


class ParseError(Exception):
    """Raised when the raw bench output does not contain a well-formed table."""


def parse_row(line: str) -> dict | None:
    """Parse one `| cell | writers | value | ops/s | ... |` row, or None if not a data row."""
    if not line.startswith("|"):
        return None
    cols = [c.strip() for c in line.strip().strip("|").split("|")]
    if len(cols) < 4:
        return None
    name, writers_s, value_s, ops_s = cols[0], cols[1], cols[2], cols[3]
    if name in ("cell", "") or set(name) <= {"-"}:
        return None  # header or separator row
    try:
        writers = int(writers_s)
    except ValueError:
        return None
    value_kib = value_s.replace("KiB", "").strip()
    try:
        ops = float(ops_s)
    except ValueError:
        return None
    kind = name.rsplit("/", 2)[0] if "/" in name else name
    return {"name": name, "kind": kind, "writers": writers, "value_kib": value_kib, "ops_s": ops}


def parse_table(text: str) -> list[dict]:
    rows = [r for r in (parse_row(line) for line in text.splitlines()) if r is not None]
    if not rows:
        raise ParseError("no bench table rows found (expected '| cell | writers | ... |' lines)")
    return rows


def pair_cells(rows: list[dict]) -> list[dict]:
    """Match `single_log_fast` and `current_adapter_put` rows by (writers, value_kib)."""
    by_key: dict[tuple, dict] = {}
    for row in rows:
        if row["kind"] not in (FAST_KIND, CURRENT_KIND):
            continue
        key = (row["writers"], row["value_kib"])
        by_key.setdefault(key, {})[row["kind"]] = row

    pairs = []
    for (writers, value_kib), kinds in sorted(by_key.items()):
        if FAST_KIND not in kinds or CURRENT_KIND not in kinds:
            continue
        fast = kinds[FAST_KIND]
        current = kinds[CURRENT_KIND]
        if current["ops_s"] <= 0:
            continue
        ratio = fast["ops_s"] / current["ops_s"]
        loss_pct = (1.0 - ratio) * 100.0
        verdict = "PASS" if loss_pct <= LOSS_THRESHOLD_PCT else "LOSS"
        pairs.append(
            {
                "writers": writers,
                "value_kib": value_kib,
                "fast_ops_s": fast["ops_s"],
                "current_ops_s": current["ops_s"],
                "ratio": ratio,
                "loss_pct": loss_pct,
                "verdict": verdict,
                "is_one_writer": writers == 1,
            }
        )
    return pairs


def format_report(pairs: list[dict]) -> str:
    lines = [
        "| writers | value | fast ops/s | current ops/s | ratio | loss % | verdict |",
        "|---|---|---|---|---|---|---|",
    ]
    for p in pairs:
        lines.append(
            f"| {p['writers']} | {p['value_kib']} KiB | {p['fast_ops_s']:.0f} | "
            f"{p['current_ops_s']:.0f} | {p['ratio']:.3f} | {p['loss_pct']:+.1f}% | "
            f"{p['verdict']} |"
        )
    return "\n".join(lines)


def evaluate(text: str) -> tuple[list[dict], int]:
    """Return (pairs, exit_code). exit_code 0 = all PASS, 1 = some LOSS."""
    rows = parse_table(text)
    pairs = pair_cells(rows)
    if not pairs:
        raise ParseError(
            f"found rows, but no matching '{FAST_KIND}'/'{CURRENT_KIND}' pairs "
            "by (writers, value size)"
        )
    exit_code = 1 if any(p["verdict"] == "LOSS" for p in pairs) else 0
    return pairs, exit_code


def check(path_arg: str | None) -> int:
    if path_arg is None or path_arg == "-":
        text = sys.stdin.read()
    else:
        text = Path(path_arg).read_text(encoding="utf-8")

    try:
        pairs, exit_code = evaluate(text)
    except ParseError as error:
        print(f"wal_fast_rule.py: {error}", file=sys.stderr)
        return 2

    print(format_report(pairs))
    print()
    one_writer_losses = [p for p in pairs if p["is_one_writer"] and p["verdict"] == "LOSS"]
    if one_writer_losses:
        print(
            "STOP: 1-writer LOSS >15% found. Apply only the mitigation the task text "
            "specifies (spec decision record §6 Risk 1: a bounded try_recv spin before "
            "the writer parks, and avoid a second caller-side handoff) before "
            "re-measuring; otherwise escalate to the maintainer. Do not proceed past "
            "this STOP.",
            file=sys.stderr,
        )
    elif exit_code != 0:
        print("LOSS found on a non-1-writer cell.", file=sys.stderr)
    else:
        print("PASS: every cell is within the Fast-mode <=15% rule.")
    return exit_code


def self_test() -> int:
    if not FIXTURE_PATH.exists():
        print(f"wal_fast_rule.py --self-test: missing fixture {FIXTURE_PATH}", file=sys.stderr)
        return 2

    text = FIXTURE_PATH.read_text(encoding="utf-8")
    rows = parse_table(text)
    assert len(rows) >= 4, f"expected >=4 parsed rows, got {len(rows)}"

    pairs, exit_code = evaluate(text)
    by_writers = {p["writers"]: p for p in pairs}

    # Fixture is built so that 1w is a clean PASS (~9% loss) and 8w is a deliberate
    # LOSS (~25% loss), exercising both branches of the rule and the parser's ability
    # to tell them apart.
    assert 1 in by_writers, "fixture must include a 1-writer cell"
    assert by_writers[1]["verdict"] == "PASS", f"expected 1w PASS, got {by_writers[1]}"
    assert 8 in by_writers, "fixture must include an 8-writer cell"
    assert by_writers[8]["verdict"] == "LOSS", f"expected 8w LOSS, got {by_writers[8]}"
    assert exit_code == 1, "fixture has a LOSS cell, evaluate() must return exit_code 1"

    # A malformed / empty input must be a parse error (exit 2), not a false PASS.
    try:
        parse_table("no table here\njust text\n")
    except ParseError:
        pass
    else:
        raise AssertionError("parse_table must raise ParseError on non-table input")

    print("wal_fast_rule.py --self-test: OK")
    return 0


def main(argv: list[str]) -> int:
    if argv[:1] == ["--self-test"]:
        return self_test()
    if argv[:1] == ["check"]:
        return check(argv[1] if len(argv) > 1 else None)
    print(
        "usage: wal_fast_rule.py check <raw-output-file|->\n"
        "       wal_fast_rule.py --self-test",
        file=sys.stderr,
    )
    return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
