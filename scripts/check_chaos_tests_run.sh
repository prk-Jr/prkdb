#!/usr/bin/env bash
#
# Every #[cfg(feature = "chaos")] test must be run by a workflow that enables the feature.
#
# `check_ignore_reasons.sh` polices #[ignore] because a switched-off test is invisible.
# Feature-gating hides one *more* thoroughly: an ignored test still appears in `cargo test
# -- --list` output as `ignored`, so it is at least countable. A cfg-gated test leaves no
# trace at all — it does not exist in the binary unless the feature is on.
#
# That is not hypothetical. Two acceptance tests named in the spec were correct, committed,
# and had never executed:
#
#   - jepsen_consistency_tests::a_replicated_register_is_linearizable_across_a_partition
#     R1 acceptance #2, and the workload that found S-06
#   - peer_authz::a_cluster_elects_and_replicates_with_peer_auth_configured
#     R12 acceptance #16, the regression the spec says "would otherwise be found in
#     production"
#
# `chaos-tests.yml` ran both files, neither with `--features chaos`. The comment sitting
# directly below those two steps already stated the rule — "they must be run somewhere that
# enables it, or they never run at all" — and the steps above it did not follow it. Writing
# the rule down is not the same as enforcing it, which is what this script is for.
#
# Meanwhile `plan_status.sh` reported 55/55 complete, because nothing it checks can see a
# test that was compiled out.
set -uo pipefail

cd "$(dirname "$0")/.."

R=$'\e[31m'; G=$'\e[32m'; D=$'\e[2m'; N=$'\e[0m'
failed=0

# Files that contain at least one chaos-gated test function.
# PRKDB_TEST_GLOB / PRKDB_WORKFLOW_GLOB let check_guards_fail.sh point this at a fixture
# and confirm it rejects a chaos-gated test no workflow runs. Both default to the real
# paths.
mapfile -t gated < <(grep -rl '#\[cfg(feature = "chaos")\]' ${PRKDB_TEST_GLOB:-crates/*/tests/*.rs} 2>/dev/null | sort)

if [[ ${#gated[@]} -eq 0 ]]; then
  echo "  ${R}✗${N} no chaos-gated tests found at all — this check has lost its target"
  exit 1
fi

for path in "${gated[@]}"; do
  base=$(basename "$path" .rs)
  count=$(grep -c '#\[cfg(feature = "chaos")\]' "$path")

  # A workflow must run this test binary *with* the feature. Matching the binary name
  # rather than the file path, because that is how cargo is invoked.
  if grep -rqE -- "--features chaos[^|&]*--test $base\b|--test $base\b[^|&]*--features chaos" \
      ${PRKDB_WORKFLOW_GLOB:-.github/workflows/*.yml}; then
    printf '  %s✓%s %-32s %s%s gated test(s), run with --features chaos%s\n' \
      "$G" "$N" "$base" "$D" "$count" "$N"
  else
    printf '  %s✗%s %-32s %s%s gated test(s) that NEVER RUN%s\n' \
      "$R" "$N" "$base" "$R" "$count" "$N"
    grep -n '#\[cfg(feature = "chaos")\]' -A 3 "$path" \
      | grep -E 'async fn|fn ' | sed 's/^/        /' | head -5
    failed=1
  fi
done

echo
if [[ $failed -eq 0 ]]; then
  echo "  ${G}✓${N} every chaos-gated test is run by a workflow that enables the feature"
else
  cat <<'EOF'
  Add a step to .github/workflows/chaos-tests.yml:

      - name: Run <Name> Tests
        run: cargo test --features chaos --test <binary> -- --nocapture

  Do not resolve this by deleting the gate. The feature exists so fault injection
  cannot reach a release build (spec R6) — that is a security property, not a
  convenience.
EOF
fi

exit $failed
