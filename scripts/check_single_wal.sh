#!/usr/bin/env bash
# One WAL: one implementation (STO-06, Task 2.9) and one WAL per data directory (D11,
# Task 2.9b). Each check is a self-contained block that exits 1 with its reason; add new
# ones as further blocks.
set -euo pipefail
cd "$(dirname "$0")/.."

# STO-06: exactly one WAL implementation. Fails if a deleted type returns.
if grep -rnwE 'ParallelWal|AsyncParallelWal|MmapParallelWal|WriteAheadLog|MmapLogSegment|AsyncLogSegment' crates --include='*.rs'; then
  echo "a second WAL implementation is back (STO-06)"; exit 1
fi
test "$(grep -rlF 'impl Wal {' crates/prkdb-core/src/wal | wc -l | tr -d ' ')" = "1"

# D11: one WAL per data directory; no per-collection or outbox WALs beside it.
# (The format-1 guard's check for an old `collections/` directory is allowed; a map of
# adapters or an `__outbox` directory is not. The behavioural proof is the test
# a_partitioned_directory_has_one_wal.)
if grep -rnE 'DashMap<String, Arc<WalStorageAdapter>>|"__outbox' crates/prkdb/src/storage; then
  echo "a second WAL per data directory is back (D11)"; exit 1
fi
