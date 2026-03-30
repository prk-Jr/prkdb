import argparse
import asyncio
import os
import sys
import time


DEFAULT_SERVER = "http://127.0.0.1:8080"
DEFAULT_RECORDS = 10000
DEFAULT_COLLECTION = "benchmark"
DEFAULT_ID_PREFIX = "bench_py"
DEFAULT_CLIENT_DIR = "benches/client_py"
PAYLOAD = "x" * 100
BATCH_SIZE = 100


def env_or_default(key, fallback):
    value = os.environ.get(key)
    if value is None:
        return fallback
    if value.strip() == "":
        raise ValueError(f"{key} must not be empty")
    return value


def env_int(key, fallback):
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


def parse_args():
    parser = argparse.ArgumentParser(description="PrkDB Python Benchmark")
    parser.add_argument(
        "--server",
        default=env_or_default("PRKDB_SERVER", DEFAULT_SERVER),
        help="PrkDB Server URL(s), comma-separated",
    )
    parser.add_argument(
        "--records",
        type=int,
        default=env_int("NUM_RECORDS", DEFAULT_RECORDS),
        help="Number of records",
    )
    parser.add_argument(
        "--collection",
        default=env_or_default("PRKDB_COLLECTION", DEFAULT_COLLECTION),
        help="PrkDB collection name",
    )
    parser.add_argument(
        "--id-prefix",
        default=env_or_default("PRKDB_ID_PREFIX", DEFAULT_ID_PREFIX),
        help="Deterministic ID prefix",
    )
    parser.add_argument(
        "--client-dir",
        default=env_or_default("PRKDB_CLIENT_DIR", DEFAULT_CLIENT_DIR),
        help="Generated Python client output directory",
    )
    args = parser.parse_args()

    if args.records <= 0:
        parser.error("--records must be greater than 0")

    return args


def load_client_class(client_dir):
    resolved_client_dir = os.path.abspath(client_dir)
    if resolved_client_dir not in sys.path:
        sys.path.insert(0, resolved_client_dir)

    try:
        from prkdb_client import PrkDbClient
    except ImportError as exc:
        print(f"❌ Error: PrkDB Client not found or dependency missing: {exc}")
        sys.exit(1)

    return PrkDbClient


def build_record(index, id_prefix):
    return {
        "id": f"{id_prefix}_{index}",
        "payload": PAYLOAD,
        "timestamp": int(time.time() * 1000),
    }


async def put_with_retry(clients, collection, data):
    last_err = None
    for client in clients:
        try:
            await client.put(collection, data)
            return
        except Exception as exc:  # pragma: no cover - exercised during live benchmark failures
            last_err = exc
    raise last_err


async def run_benchmark(client_class, servers, collection, id_prefix, num_records):
    print(f"🚀 Connecting to {servers}...")
    clients = [client_class(host=server) for server in servers]

    print(f"  📤 Starting Producer: {num_records} records...")
    produce_start = time.time()
    success_count = 0
    failure_count = 0

    try:
        for start in range(0, num_records, BATCH_SIZE):
            batch_end = min(start + BATCH_SIZE, num_records)
            current_batch = []

            for index in range(start, batch_end):
                data = build_record(index, id_prefix)
                current_batch.append(put_with_retry(clients, collection, data))

            results = await asyncio.gather(*current_batch, return_exceptions=True)
            for result in results:
                if isinstance(result, Exception):
                    failure_count += 1
                    print(f"Error: {result}")
                    continue
                success_count += 1
    finally:
        for client in clients:
            await client.close()

    duration = time.time() - produce_start
    mbps = (success_count * len(PAYLOAD)) / duration / 1024 / 1024

    print(f"✅ Producer Finished: {success_count}/{num_records} records")
    if failure_count:
        print(f"❌ Failed Writes: {failure_count}")
    print(f"⏱️  Duration: {duration:.2f}s")
    print(f"📈 Throughput: {mbps:.2f} MB/s")

    if failure_count:
        raise RuntimeError(f"benchmark failed with {failure_count} write errors")


def main():
    try:
        args = parse_args()
    except ValueError as exc:
        print(f"❌ Configuration error: {exc}", file=sys.stderr)
        sys.exit(2)

    servers = [server.strip() for server in args.server.split(",") if server.strip()]
    if not servers:
        print("❌ Configuration error: --server must provide at least one URL", file=sys.stderr)
        sys.exit(2)

    client_class = load_client_class(args.client_dir)

    try:
        asyncio.run(
            run_benchmark(
                client_class,
                servers,
                args.collection,
                args.id_prefix,
                args.records,
            )
        )
    except KeyboardInterrupt:
        print("\n⚠️ Interrupted")
        sys.exit(130)
    except RuntimeError as exc:
        print(f"❌ {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
