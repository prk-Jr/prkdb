#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from typing import Any, Dict, Iterable, List
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen


DEFAULT_COLLECTION = "benchmark"
DEFAULT_SERVER = "http://127.0.0.1:8080"
PAGE_SIZE = 1000


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify mixed-client benchmark results")
    parser.add_argument("--server", default=DEFAULT_SERVER)
    parser.add_argument("--collection", default=DEFAULT_COLLECTION)
    parser.add_argument("--expect-py", type=int, required=True)
    parser.add_argument("--expect-ts", type=int, required=True)
    parser.add_argument("--expect-go", type=int, required=True)
    parser.add_argument("--sample-id", action="append", default=[])
    args = parser.parse_args()

    if not args.sample_id:
        parser.error("at least one --sample-id is required")

    return args


def fetch_page(server: str, collection: str, offset: int, limit: int) -> List[Dict[str, Any]]:
    query = urlencode({"limit": limit, "offset": offset})
    url = f"{server.rstrip('/')}/collections/{collection}/data?{query}"

    try:
        with urlopen(url) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        raise RuntimeError(f"failed to fetch {url}: {exc}") from exc
    except URLError as exc:
        raise RuntimeError(f"failed to fetch {url}: {exc}") from exc

    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, dict):
            nested = data.get("data")
            if isinstance(nested, list):
                return [item for item in nested if isinstance(item, dict)]
        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)]

    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]

    return []


def collect_rows(server: str, collection: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    offset = 0

    while True:
        page = fetch_page(server, collection, offset=offset, limit=PAGE_SIZE)
        if not page:
            break
        rows.extend(page)
        if len(page) < PAGE_SIZE:
            break
        offset += len(page)

    return rows


def main() -> None:
    args = parse_args()

    rows = collect_rows(args.server, args.collection)
    counts = Counter()
    ids = set()
    unexpected_ids = []

    for row in rows:
        row_id = row.get("id")
        if not isinstance(row_id, str):
            unexpected_ids.append(row)
            continue
        ids.add(row_id)
        prefix = row_id.split("-", 1)[0]
        counts[prefix] += 1

    errors = []
    expected = {"py": args.expect_py, "ts": args.expect_ts, "go": args.expect_go}
    for prefix, count in expected.items():
        actual = counts.get(prefix, 0)
        if actual != count:
            errors.append(f"expected {count} rows for prefix {prefix}, found {actual}")

    if unexpected_ids:
        errors.append(f"encountered {len(unexpected_ids)} rows without a string id")

    missing_samples = [sample_id for sample_id in args.sample_id if sample_id not in ids]
    if missing_samples:
        errors.append(f"missing sample ids: {', '.join(missing_samples)}")

    total_expected = sum(expected.values())
    if len(rows) != total_expected:
        errors.append(f"expected {total_expected} total rows, found {len(rows)}")

    if errors:
        for error in errors:
            print(f"❌ {error}", file=sys.stderr)
        raise SystemExit(1)

    print(
        "✅ verified mixed-client results: "
        + f"collection={args.collection}, total={len(rows)}, "
        + ", ".join(f"{prefix}={count}" for prefix, count in expected.items())
        + f", sample_ids={len(args.sample_id)}"
    )


if __name__ == "__main__":
    main()
