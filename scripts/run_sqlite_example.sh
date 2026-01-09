#!/usr/bin/env bash
set -euo pipefail

# Optional override: export DB_URL="sqlite:///path/to/file.db"
# DB_URL="${DB_URL:-sqlite:///tmp/prkdb_orm_sqlite_demo.db}"
DB_URL="sqlite::memory:"

echo "Using database: $DB_URL"

# Clean any existing file if it's a file URL
if [[ "$DB_URL" == sqlite://* ]]; then
  path="${DB_URL#sqlite://}"
  if [[ -f "$path" ]]; then
    rm -f "$path"
  fi
fi

cargo run -p prkdb-orm --example sqlite_demo --features prkdb-orm/sqlite -- "$DB_URL"
