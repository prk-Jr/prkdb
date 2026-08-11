#!/usr/bin/env bash
#
# Every flag `serve` accepts must be documented, and every documented flag must exist.
#
# # Why this check exists
#
# Every other guard in this repository verifies code against code: plan_status.sh reads
# repository state, the README is compile-checked, mutation testing mutates source. The
# published documentation — the one artefact users actually read — had no guard at all.
#
# The result was not a stale sentence. `serve` was changed to refuse starting without
# authorization, which is correct, and getting-started.md went on telling readers to run it
# without any. The first command in the documentation failed, and every check was green.
#
# Drift runs both ways, so both are checked. A flag that exists and is undocumented is a
# feature nobody can use; a flag documented and removed is worse, because the reader trusts
# it and it fails.
set -uo pipefail
cd "$(dirname "$0")/.."

R=$'\e[31m'; G=$'\e[32m'; D=$'\e[2m'; N=$'\e[0m'
failed=0

BIN="${PRKDB_BIN:-./target/debug/prkdb-cli}"
if [[ ! -x "$BIN" ]]; then
  cargo build -q -p prkdb-cli --bin prkdb-cli || { echo "  ${R}✗${N} cannot build prkdb-cli"; exit 1; }
fi

mapfile -t flags < <("$BIN" serve --help 2>/dev/null | grep -oE '^\s+--[a-z0-9-]+' | tr -d ' ' | sort -u)
[[ ${#flags[@]} -eq 0 ]] && { echo "  ${R}✗${N} no flags parsed from serve --help"; exit 1; }

docs=$(cat docs/guide/*.md 2>/dev/null)

for flag in "${flags[@]}"; do
  if grep -qF -- "$flag" <<<"$docs"; then
    printf '  %s✓%s %s\n' "$G" "$N" "$flag"
  else
    printf '  %s✗%s %-32s %sundocumented%s\n' "$R" "$N" "$flag" "$R" "$N"
    failed=1
  fi
done

# The other direction: a flag the guide names that serve no longer accepts.
mapfile -t documented < <(grep -ohE '\-\-[a-z][a-z0-9-]{3,}' docs/guide/*.md 2>/dev/null | sort -u)
for flag in "${documented[@]}"; do
  # Only judge flags that look like ours; skip curl/cargo/docker options.
  case "$flag" in
    --tls-*|--allow-*|--rate-limit|--grpc-port|--num-partitions|--advertised-*|--websockets|--prometheus|--peers|--cors) ;;
    *) continue ;;
  esac
  if ! printf '%s\n' "${flags[@]}" | grep -qxF -- "$flag"; then
    printf '  %s✗%s %-32s %sdocumented but serve does not accept it%s\n' "$R" "$N" "$flag" "$R" "$N"
    failed=1
  fi
done

echo
if [[ $failed -eq 0 ]]; then
  echo "  ${G}✓${N} the guide documents every flag serve accepts, and no flag it does not"
else
  echo "  ${D}Document it in docs/guide/, or remove the claim. The published docs are the"
  echo "  only artefact users read, and the only one nothing else checks.${N}"
fi
exit $failed
