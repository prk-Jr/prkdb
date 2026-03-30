#!/usr/bin/env python3

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import time
from typing import Any, Dict, Iterable, List


DEFAULT_SERVER = "http://127.0.0.1:8080"
DEFAULT_COLLECTION = "benchmark"
DEFAULT_RECORDS = 1000
DEFAULT_ID_PREFIX = "py"


def env_or_default(key: str, fallback: str) -> str:
    value = os.environ.get(key)
    if value is None:
        return fallback
    if value.strip() == "":
        raise ValueError(f"{key} must not be empty")
    return value


def env_int(key: str, fallback: int) -> int:
    raw_value = os.environ.get(key)
    if raw_value is None:
        return fallback

    try:
        value = int(raw_value)
    except ValueError as exc:
        raise ValueError(f"{key} must be an integer, got {raw_value!r}") from exc

    if value <= 0:
        raise ValueError(f"{key} must be greater than 0, got {value}")
    return value


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Mixed-client Python runner")
    parser.add_argument("--mode", choices=("write", "read"), default=env_or_default("PRKDB_MODE", "write"))
    parser.add_argument("--server", default=env_or_default("PRKDB_SERVER", DEFAULT_SERVER))
    parser.add_argument("--collection", default=env_or_default("PRKDB_COLLECTION", DEFAULT_COLLECTION))
    parser.add_argument("--records", type=int, default=env_int("NUM_RECORDS", DEFAULT_RECORDS))
    parser.add_argument("--id-prefix", default=env_or_default("PRKDB_ID_PREFIX", DEFAULT_ID_PREFIX))
    parser.add_argument("--client-dir", default=env_or_default("PRKDB_CLIENT_DIR", "."))
    parser.add_argument("--sample-id", action="append", default=[])
    args = parser.parse_args()

    if args.records <= 0:
        parser.error("--records must be greater than 0")

    return args


def load_client_class(client_dir: str):
    resolved_client_dir = os.path.abspath(client_dir)
    if resolved_client_dir not in sys.path:
        sys.path.insert(0, resolved_client_dir)

    try:
        from prkdb_client import PrkDbClient
    except ImportError as exc:
        raise RuntimeError(f"failed to import generated Python client from {resolved_client_dir}: {exc}") from exc

    return PrkDbClient


def build_record(index: int, id_prefix: str) -> Dict[str, Any]:
    record_id = f"{id_prefix}-{index + 1:06d}"
    return {
        "id": record_id,
        "payload": record_id,
        "timestamp": int(time.time() * 1000),
    }


async def run_write(client_class, server: str, collection: str, id_prefix: str, records: int) -> None:
    async with client_class(host=server) as client:
        for index in range(records):
            record = build_record(index, id_prefix)
            await client.put(collection, record)


async def run_read(client_class, server: str, collection: str, sample_ids: Iterable[str]) -> None:
    async with client_class(host=server) as client:
        rows = await client.list(collection, limit=10000)
        rows_by_id = {str(row.get("id")): row for row in rows if isinstance(row, dict)}

        missing_rows = [sample_id for sample_id in sample_ids if sample_id not in rows_by_id]
        if missing_rows:
            raise RuntimeError(f"missing expected sample ids: {', '.join(missing_rows)}")


async def main() -> None:
    try:
        args = parse_args()
    except ValueError as exc:
        print(f"❌ Configuration error: {exc}", file=sys.stderr)
        raise SystemExit(2)

    try:
        client_class = load_client_class(args.client_dir)
        if args.mode == "write":
            await run_write(client_class, args.server, args.collection, args.id_prefix, args.records)
        else:
            if not args.sample_id:
                print("❌ Configuration error: at least one --sample-id is required", file=sys.stderr)
                raise SystemExit(2)
            await run_read(client_class, args.server, args.collection, args.sample_id)
    except KeyboardInterrupt:
        print("\n⚠️ Interrupted", file=sys.stderr)
        raise SystemExit(130)
    except Exception as exc:
        print(f"❌ {exc}", file=sys.stderr)
        raise SystemExit(1)


if __name__ == "__main__":
    asyncio.run(main())
