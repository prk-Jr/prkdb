#!/usr/bin/env bash
#
# Every discipline check must be shown to fail before its pass is believed.
#
# # Why
#
# A check that cannot fail is indistinguishable from a check that passes. This repository
# has shipped several: a linearizability checker that accepted any history, five property
# tests that discarded the `Err` signalling failure, a required status check that could
# never report, a mutation job that timed out before testing a mutant.
#
# `scripts/check_*.sh` are the same kind of artefact — a script that reports a verdict —
# and were held to a weaker standard than the tests they police. This closes that.
#
# # Reading exit codes correctly
#
# Twice while writing this, a working guard was reported broken by a faulty measurement:
# once by a text substitution that silently did not apply, once by reading `$?` after a
# pipe, which returns the *last* command's status rather than the guard's. Both are the
# same error the guards exist to catch, one level up.
#
# So: no pipes between the guard and `$?`, and every fixture asserts the guard's output
# names the specific problem rather than merely exiting non-zero. A guard that fails for
# an unrelated reason — a missing file, a syntax error — would otherwise look correct.
set -uo pipefail
cd "$(dirname "$0")/.."

R=$'\e[31m'; G=$'\e[32m'; D=$'\e[2m'; N=$'\e[0m'
failed=0
FIXTURES=tests/guard-fixtures

# must_fail <name> <expected-substring> <command...>
#
# Runs the command, requires a non-zero exit AND that the output names the problem.
must_fail() {
  local name="$1" expect="$2"; shift 2
  local out status
  out=$("$@" 2>&1)
  status=$?          # directly after the command; never after a pipe

  if [[ $status -eq 0 ]]; then
    printf '  %s✗%s %-34s %spassed on a known-bad input — it is checking nothing%s\n' \
      "$R" "$N" "$name" "$R" "$N"
    failed=1
  elif ! grep -qF -- "$expect" <<< "$out"; then
    printf '  %s✗%s %-34s %sfailed, but not for the expected reason%s\n' \
      "$R" "$N" "$name" "$R" "$N"
    printf '      %sexpected output to mention: %s%s\n' "$D" "$expect" "$N"
    failed=1
  else
    printf '  %s✓%s %-34s %srejects its known-bad input%s\n' "$G" "$N" "$name" "$D" "$N"
  fi
}

# must_pass <name> <command...> — the guard must be content with the real repository,
# or every result above is meaningless: a guard that always fails also "rejects" fixtures.
must_pass() {
  local name="$1"; shift
  local status
  "$@" >/dev/null 2>&1
  status=$?
  if [[ $status -ne 0 ]]; then
    printf '  %s✗%s %-34s %sfails on the real repository%s\n' "$R" "$N" "$name" "$R" "$N"
    failed=1
  else
    printf '  %s✓%s %-34s %saccepts the real repository%s\n' "$G" "$N" "$name" "$D" "$N"
  fi
}

echo "Guards must reject a known-bad input:"

# 1. ignore-reasons, against a fixture tree holding a bare #[ignore] and a "flaky" reason.
must_fail "check_ignore_reasons" "Reason is not in an allowed category" \
  env PRKDB_SCAN_ROOT="$FIXTURES/bare-ignore/crates" bash scripts/check_ignore_reasons.sh

# 2. docs-cover-cli, with no binary to read: no flags to compare means nothing is compared.
must_fail "check_docs_cover_cli" "no flags parsed" \
  env PRKDB_BIN=/nonexistent/prkdb-cli bash scripts/check_docs_cover_cli.sh

# 3. chaos-tests-run, against a workflow directory that names no --features chaos step.
must_fail "check_chaos_tests_run" "NEVER RUN" \
  env PRKDB_TEST_GLOB="$FIXTURES/no-chaos-step/crates/*/tests/*.rs" \
      PRKDB_WORKFLOW_GLOB="$FIXTURES/no-chaos-step/*.yml" \
      bash scripts/check_chaos_tests_run.sh

echo
echo "And must accept the repository as it stands:"
must_pass "check_ignore_reasons" bash scripts/check_ignore_reasons.sh
must_pass "check_docs_cover_cli" bash scripts/check_docs_cover_cli.sh
must_pass "check_chaos_tests_run" bash scripts/check_chaos_tests_run.sh
must_pass "check_wrapper_completeness" bash scripts/check_wrapper_completeness.sh

echo
if [[ $failed -eq 0 ]]; then
  echo "  ${G}✓${N} every guard was observed to fail on bad input and pass on good"
else
  echo "  ${D}A guard that cannot fail is indistinguishable from one that passes.${N}"
fi
exit $failed
