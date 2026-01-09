#!/usr/bin/env bash
set -euo pipefail

DB_URL="${DB_URL:-sqlite::memory:}"
echo "Using DB_URL=${DB_URL}"

cargo run -p prkdb --example sqlite_storage -- "$@" 