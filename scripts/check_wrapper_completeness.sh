#!/usr/bin/env bash
#
# Does the wrapper implement everything the adapter it wraps implements?
#
# `CollectionPartitionedAdapter` is what `PrkDb::builder().with_data_dir()` produces, and
# it delegates to one `WalStorageAdapter` per collection. `StorageAdapter` gives most of
# its methods a default body that returns "not supported", so a wrapper that forgets one
# compiles cleanly and fails at runtime — in whichever feature happens to call it.
#
# That has now happened four times:
#
#   S-04  take_snapshot        `prkdb backup` refused outright
#   S-05  collection discovery backup then archived zero entries
#   S-07  scan_prefix          list_collections and principal loading failed
#   S-08  scan_range           CollectionHandle::scan_range_by_id_bytes failed
#
# Each was found by accident, by someone using the feature. This finds the fifth on
# purpose. It is a text comparison, not a type check — the compiler cannot help here,
# because a missing method is a *silently inherited default*, which is exactly the problem.
set -uo pipefail
cd "$(dirname "$0")/.."

INNER="crates/prkdb/src/storage/wal_adapter.rs"
WRAPPER="crates/prkdb/src/storage/collection_partitioned_adapter.rs"
TRAIT="crates/prkdb-types/src/storage.rs"

# Only methods whose *default body returns an error* matter here.
#
# The trait has two kinds of default. `get_many` and friends fall back to looping over the
# single-key operation: correct, merely slower, and a wrapper that inherits one loses
# nothing but throughput. `scan_prefix` and friends return "not supported": a wrapper that
# inherits one is broken, and only at runtime.
#
# Flagging both kinds would report three benign performance fallbacks alongside every real
# defect, and a check that cries wolf gets muted.
dangerous=$(awk '/pub trait StorageAdapter/{t=1} t' "$TRAIT" \
  | awk '/^}/{exit} {print}' \
  | grep -B1 -A6 'async fn ' \
  | awk '/async fn /{name=$0; body=""} {body=body $0} /not supported/{if (name!="") {print name; name=""}}' \
  | grep -oE 'fn [a-z_]+' | awk '{print $2}' | sort -u)

# Methods the inner adapter implements as part of the trait.
inner_impl_start=$(grep -n 'impl StorageAdapter for WalStorageAdapter' "$INNER" | cut -d: -f1)
inner_methods=$(awk -v s="$inner_impl_start" 'NR>=s' "$INNER" \
  | awk '/^}/{if (seen) exit} /impl StorageAdapter/{seen=1} seen' \
  | grep -oE 'async fn [a-z_]+' | awk '{print $3}' | sort -u)

wrapper_start=$(grep -n 'impl StorageAdapter for CollectionPartitionedAdapter' "$WRAPPER" | cut -d: -f1)
wrapper_methods=$(awk -v s="$wrapper_start" 'NR>=s' "$WRAPPER" \
  | awk '/^}/{if (seen) exit} /impl StorageAdapter/{seen=1} seen' \
  | grep -oE 'async fn [a-z_]+' | awk '{print $3}' | sort -u)

# Methods the wrapper cannot meaningfully forward, with the reason it cannot.
# Anything listed here must have a test pinning the current behaviour.
declare -A EXEMPT=(
)

missing=""
for m in $inner_methods; do
  # Benign defaults are not this check's business.
  grep -qx "$m" <<< "$dangerous" || continue
  if ! grep -qx "$m" <<< "$wrapper_methods"; then
    if [[ -v "EXEMPT[$m]" ]]; then
      printf '  ~ %-20s exempt: %s\n' "$m" "${EXEMPT[$m]}"
    else
      missing="$missing $m"
      printf '  ✗ %-20s implemented by WalStorageAdapter, missing from the wrapper\n' "$m"
    fi
  fi
done

if [[ -n "$missing" ]]; then
  echo
  echo "CollectionPartitionedAdapter is missing:$missing"
  echo "Each falls through to a trait default that returns \"not supported\" at runtime."
  echo "Implement it, or add it to EXEMPT above with the reason and a test pinning the behaviour."
  exit 1
fi

echo "  ✓ the wrapper implements everything its inner adapter does"
