#!/usr/bin/env bash
#
# Every #[ignore] must give a reason from an allowed category.
#
# R16 already requires a reason. A reason is not enough: `chaos_test_rapid_recovery`
# carried
#
#     #[ignore = "Manual investigation: integration harness still diverges from
#                 WAL recovery unit tests"]
#
# which reads like diligence and was in fact a correct test switched off because it had
# found a real bug. The harness did not diverge; `WalStorageAdapter::new` truncated the
# WAL, so every reopen destroyed the previous cycle (spec S-05). The test said "Lost data
# from cycle 0 key 0" and was believed to be wrong.
#
# So the categories are closed, not open. A test may be skipped because it is slow, or
# because it needs something the default runner does not have — and in both cases it must
# still run somewhere. It may never be skipped because it fails, is flaky, disagrees with
# other tests, or is awaiting investigation. Those are findings, not maintenance.
set -uo pipefail
cd "$(dirname "$0")/.."

# Substrings that make an #[ignore] legitimate. Each implies the test runs in some job.
ALLOWED='slow:|needs a built prkdb-server binary|blocked by S-'

# Substrings that are never acceptable, however they are phrased.
FORBIDDEN='manual investigation|flaky|intermittent|diverges|under investigation|todo|fixme|for now|temporarily|disabled|broken'

fail=0
while IFS= read -r line; do
  [[ -z "$line" ]] && continue
  file=${line%%:*}
  rest=${line#*:}
  lineno=${rest%%:*}
  text=${line#*:*:}

  lower=$(tr '[:upper:]' '[:lower:]' <<< "$text")

  if grep -qiE "$FORBIDDEN" <<< "$lower"; then
    echo "  ✗ $file:$lineno"
    echo "      $(sed 's/^[[:space:]]*//' <<< "$text")"
    echo "      A test is not skipped because it fails. Fix the code, or record the"
    echo "      finding and reference it (e.g. \"blocked by S-04: …\")."
    fail=1
  elif ! grep -qE "$ALLOWED" <<< "$text"; then
    echo "  ✗ $file:$lineno"
    echo "      $(sed 's/^[[:space:]]*//' <<< "$text")"
    echo "      Reason is not in an allowed category. Allowed: a 'slow:' test that runs"
    echo "      in the nightly job, one needing the server binary that runs in the chaos"
    echo "      workflow, or one blocked by a numbered spec finding."
    fail=1
  fi
done < <(grep -rn '#\[ignore' crates/ --include='*.rs' 2>/dev/null | grep -v '///\|//!')

if [[ $fail -eq 0 ]]; then
  echo "  ✓ every #[ignore] gives a reason from an allowed category"
fi
exit $fail
