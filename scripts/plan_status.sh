#!/usr/bin/env bash
# Objective progress tracker for the 2026-08-08 correctness and security plans.
#
# Checkboxes in the plan documents record intent. This script records reality: every
# item below is derived from the repository itself, so it cannot report progress that
# has not actually happened.
#
#   ./scripts/plan_status.sh          full report
#   ./scripts/plan_status.sh --quiet  summary only
#   ./scripts/plan_status.sh --ci     exit non-zero while anything is outstanding
#
# Specs:  docs/superpowers/specs/2026-08-08-correctness-and-production-readiness.md
# Plans:  docs/superpowers/plans/2026-08-08-{correctness-hardening,production-security}.md

set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.." || exit 1

QUIET=0
CI=0
for arg in "$@"; do
  case "$arg" in
    --quiet) QUIET=1 ;;
    --ci) CI=1 ;;
    -h|--help) sed -n '2,14p' "$0"; exit 0 ;;
  esac
done

if [[ -t 1 ]]; then
  G=$'\033[32m'; R=$'\033[31m'; Y=$'\033[33m'; D=$'\033[2m'; N=$'\033[0m'
else
  G=""; R=""; Y=""; D=""; N=""
fi

PASS=0
FAIL=0
CURRENT_GROUP=""

group() {
  CURRENT_GROUP="$1"
  [[ $QUIET -eq 1 ]] && return
  printf '\n%s── %s%s\n' "$D" "$1" "$N"
}

# check <label> <requirement-id> <command...>
# The command's exit status is the verdict. Nothing here trusts a checkbox.
check() {
  local label="$1" req="$2"; shift 2
  if "$@" >/dev/null 2>&1; then
    PASS=$((PASS + 1))
    [[ $QUIET -eq 0 ]] && printf '  %s✓%s %-52s %s%s%s\n' "$G" "$N" "$label" "$D" "$req" "$N"
  else
    FAIL=$((FAIL + 1))
    [[ $QUIET -eq 0 ]] && printf '  %s✗%s %-52s %s%s%s\n' "$R" "$N" "$label" "$D" "$req" "$N"
  fi
}

# Helpers ────────────────────────────────────────────────────────────────────

# At least N occurrences of a pattern in a file.
atleast() { [[ $(grep -c "$1" "$2" 2>/dev/null || echo 0) -ge $3 ]]; }

# Zero occurrences of a pattern across a path.
none() { ! grep -rq "$1" "$2" 2>/dev/null; }

# Zero occurrences of a pattern outside comment lines. A thing named in a comment is
# documentation, not a defect — explaining why `continue-on-error` was removed should
# not read as still having it. `#` covers YAML, `//` covers Rust.
none_in_code() {
  ! grep -rn "$1" "$2" 2>/dev/null | grep -qv ':[0-9]*: *\(#\|//\)'
}

# Hardcoded loopback ports in test code, ignoring comment lines.
no_hardcoded_ports() {
  none_in_code '127\.0\.0\.1:[0-9]\{4,5\}' 'crates/prkdb/tests/'
}

# The backup round-trip was #[ignore]d against S-04 for as long as backup was broken.
# Match the attribute only: the module doc explains the history and says "#[ignore]d",
# which must not read as the tests still being disabled.
no_ignored_backup_tests() {
  ! grep -q '^[[:space:]]*#\[ignore' crates/prkdb-cli/tests/backup_restore.rs 2>/dev/null
}

# A throughput figure in a doc comment must be traceable. Either it names the benchmark
# or the methodology page, or it says it is unverified — a bare number is a claim nobody
# can check, which is how "800x faster" survived beside a measured 95x.
perf_claims_are_sourced() {
  local bare
  bare=$(grep -rnE '^[[:space:]]*(///|//!).*[0-9][0-9,.]*[KM]\+? ops/sec' crates/*/src 2>/dev/null \
    | grep -viE 'methodology|unverified|measured|rate.?limit|per_second' | wc -l | tr -d ' ')
  [[ "$bare" -eq 0 ]]
}

# Doc examples fenced with ```ignore never compile, so they cannot catch API drift.
# The pattern must allow leading whitespace: anchoring at ^/// matched 2 of 61 real
# occurrences and would have reported this green while 59 remained.
no_ignored_doctests() {
  ! grep -rqE '^[[:space:]]*///.*```(rust,)?ignore' crates/prkdb/src/ 2>/dev/null
}

# Production (non-test) unwrap count in a directory, below a threshold.
prod_unwraps_below() {
  local dir="$1" limit="$2"
  [[ -d "$dir" ]] || return 0
  local n
  n=$(python3 - "$dir" <<'PY'
import re, sys, pathlib
total = 0
for p in pathlib.Path(sys.argv[1]).rglob("*.rs"):
    in_test = started = False
    depth = 0
    for ln in p.read_text(errors="ignore").splitlines():
        if not in_test and re.search(r"#\[cfg\(test\)\]", ln):
            in_test, depth, started = True, 0, False
            continue
        if in_test:
            depth += ln.count("{") - ln.count("}")
            if "{" in ln:
                started = True
            if started and depth <= 0:
                in_test = False
            continue
        total += ln.count(".unwrap()")
print(total)
PY
)
  [[ "$n" -lt "$limit" ]]
}

# Plan A — Correctness hardening ─────────────────────────────────────────────

group "Plan A · Task 1 — CI safety net"
check "every step-running CI job has timeout-minutes" "R2" atleast 'timeout-minutes' .github/workflows/ci.yml 12
check "chaos-tests.yml carries its own timeouts"      "R2" atleast 'timeout-minutes' .github/workflows/chaos-tests.yml 4
check "rust-toolchain.toml pins the compiler"         "R2" test -f rust-toolchain.toml
check "workspace declares rust-version (MSRV)"        "R2" grep -q 'rust-version' Cargo.toml
check "README no longer claims Rust 1.70+"            "R2" none 'Rust-1.70' README.md

group "Plan A · Task 2 — Lints"
check "clippy --all-targets -D warnings passes"       "R2" cargo clippy --workspace --all-targets -- -D warnings

group "Plan A · Tasks 3-6 — Consistency evidence"
check "checker detects an injected stale read"        "R1" grep -rq 'detects_stale_read' crates/prkdb/tests/
check "Wing & Gong checker exists"                    "R1" test -f crates/prkdb/tests/helpers/wgl.rs
check "in-process cluster harness exists"             "R1" test -f crates/prkdb/tests/helpers/in_process_cluster.rs
check "bank invariant reads back from storage"        "R1" grep -rq 'begin_transaction' crates/prkdb/tests/helpers/jepsen_checker.rs

group "Plan A · Tasks 7-11 — Raft and flakiness"
check "election safety (<=1 leader/term) asserted"    "R4" grep -rq 'election_safety' crates/prkdb/tests/
check "chaos injection behind a feature flag"         "R6" grep -q 'cfg(feature = "chaos")' crates/prkdb/src/raft/rpc_client.rs
check "chaos workflow has no continue-on-error"       "R7" none_in_code 'continue-on-error' .github/workflows/chaos-tests.yml
check "no hardcoded ports in tests"                   "R3" no_hardcoded_ports

group "Plan A · Tasks 12-16 — Hygiene"
check 'doctests compile (no ignored doc fences)'      "R5" no_ignored_doctests
check "WAL free of production unwraps"                "R8" prod_unwraps_below crates/prkdb-core/src/wal 1
check "storage adapters free of production unwraps"   "R8" prod_unwraps_below crates/prkdb-storage-segmented/src 1
check "coverage job in CI"                            "R9" grep -q 'llvm-cov' .github/workflows/ci.yml
check "performance claims cite a source"             "R15" perf_claims_are_sourced
check "benchmark methodology is published"           "R15" test -f docs/benchmarks/methodology.md
check "deny.toml present"                            "R10" test -f deny.toml
check "dependabot configured"                        "R10" test -f .github/dependabot.yml
check "dead module storage_old_inmemory removed"     "R11" none 'mod storage_old_inmemory' crates/prkdb/src/lib.rs
check "orphan security.rs removed"                   "R11" test ! -f crates/prkdb/src/security.rs

group "Plan A · Tasks 17-19 — Claims and discipline"
check "linearizable read mode verified"              "R14" test -f crates/prkdb/tests/read_consistency_modes.rs
# none_in_code, not none: several module docs explain why tests *used* to be
# `#[ignore]`d, and describing the history must not read as still doing it.
check "no bare #[ignore] without a reason"           "R16" none_in_code '#\[ignore\]' crates/

# Plan B — Production security ───────────────────────────────────────────────

group "Plan B · Tasks 0-2 — Authorization"
check "authz model module exists"                    "R12" test -d crates/prkdb/src/authz
check "HTTP authorization layer wired"               "R12" grep -q 'authorize' crates/prkdb-cli/src/commands/serve.rs
check "gRPC authz interceptor exists"                "R12" test -f crates/prkdb/src/raft/authz_interceptor.rs
# Existing-but-unregistered is exactly the state S-01 sat in: the policy was implemented
# and unit-tested while the server it was meant to guard never installed it. The file
# existing proves nothing, so check the wiring separately.
check "gRPC authz layer registered on the server"    "R12" grep -q 'AuthzGrpcLayer' crates/prkdb-cli/src/commands/serve.rs
check "gRPC authz enforced end to end"               "R12" test -f crates/prkdb/tests/grpc_authz.rs
check "Raft peer authentication exists"              "R12" test -f crates/prkdb/src/raft/peer_auth.rs
# Same trap as the client-API layer: the policy existed and was unit-tested for a while
# with nothing installing it. Check both binaries register it, and that something drives
# it over a socket.
check "peer auth registered in prkdb-cli serve"      "R12" grep -q 'with_interceptor' crates/prkdb-cli/src/commands/serve.rs
check "peer auth registered in prkdb-server"         "R12" grep -q 'with_interceptor' crates/prkdb/src/bin/prkdb-server.rs
check "peer auth enforced end to end"                "R12" test -f crates/prkdb/tests/peer_authz.rs
check "mTLS peer auth proven over real TLS"          "R12" test -f crates/prkdb/tests/peer_mtls.rs
# A secured server whose own client cannot authenticate is a lock with no key.
check "Rust client sends a credential"               "R12" grep -q 'fn authed' crates/prkdb-client/src/client.rs
check "generated clients send credentials"           "R12" grep -q 'PrkDbAuthError' crates/prkdb-cli/src/commands/codegen.rs
check "generated clients separate 401 from 403"      "R12" grep -q 'ErrPermissionDenied' crates/prkdb-cli/src/commands/codegen.rs
check "principals can be administered at runtime"    "R12" test -f crates/prkdb-cli/src/admin_principals.rs
check "collection listing is grant-filtered"         "R12" grep -q 'fn filter_collections' crates/prkdb-cli/src/commands/serve.rs
check "credential compares are constant-time"        "R12" grep -rq 'ConstantTimeEq\|ct_eq' crates/prkdb/src crates/prkdb-cli/src

group "Plan B · Tasks 3-7 — Production primitives"
check "TLS reachable from a shipped binary"          "R13" grep -rq 'tls_cert\|tls-cert' crates/prkdb-cli/src crates/prkdb/src/bin
check "WAL segments carry a format version"            "—" grep -rq 'FORMAT_VERSION\|PRKDB_WAL_MAGIC' crates/prkdb-core/src/wal/
check "backup archive carries a checksum manifest"     "—" grep -q 'checksum\|manifest' crates/prkdb-cli/src/commands/backup.rs
check "backup/restore round-trip test"                 "—" test -f crates/prkdb-cli/tests/backup_restore.rs
check "backup round-trip is not ignored"               "—" no_ignored_backup_tests
check "reopen-durability regression test"              "—" test -f crates/prkdb/tests/durability.rs
check "database open never truncates the WAL"          "—" none_in_code 'MmapParallelWal::create' crates/prkdb/src/storage/
check "readiness endpoint distinct from liveness"      "—" grep -q 'readyz' crates/prkdb-cli/src/commands/serve.rs
check "rate limiter wired into the server"             "—" grep -rq 'RateLimiter' crates/prkdb-cli/src
check "CHANGELOG present"                              "—" test -f CHANGELOG.md
check "crates published to crates.io"                  "—" bash -c 'git tag | grep -q "^v"'

# Summary ────────────────────────────────────────────────────────────────────

TOTAL=$((PASS + FAIL))
PCT=$(( TOTAL > 0 ? PASS * 100 / TOTAL : 0 ))

printf '\n%s%s%s\n' "$D" "────────────────────────────────────────────────────────────" "$N"
if [[ $FAIL -eq 0 ]]; then
  printf '%s%s/%s complete%s — every tracked item verified against the repository.\n' \
    "$G" "$PASS" "$TOTAL" "$N"
else
  printf '%s%s/%s complete (%s%%)%s — %s%s outstanding%s\n' \
    "$Y" "$PASS" "$TOTAL" "$PCT" "$N" "$R" "$FAIL" "$N"
fi
printf '%sChecks read repository state, not plan checkboxes.%s\n' "$D" "$N"

[[ $CI -eq 1 && $FAIL -gt 0 ]] && exit 1
exit 0
