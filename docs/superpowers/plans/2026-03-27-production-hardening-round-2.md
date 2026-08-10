# Production Hardening Round 2 Implementation Plan

> Archived on 2026-03-27. Use `docs/superpowers/plans/2026-03-27-repo-audit-next-steps.md` for active execution.

## Archived Status

This implementation plan was superseded after the staff-level repo audit showed that the
remaining production work had shifted.

## Why It Was Superseded

The original plan assumed that auth, client-routing, HTTP integration, and cascading-failure
recovery were still the dominant unresolved issues. Fresh verification showed that assumption
was stale. The repo's active next steps are broader and more concrete:

1. Fix user-facing CLI and documentation contract drift.
2. Remove repeated binary builds across CI jobs and test scripts.
3. Raise CI from advisory linting to a real quality gate.
4. Replace remaining panic/string-parsing control flow with typed behavior.
5. Promote stable ignored tests and tier the expensive ones intentionally.
6. Remove benchmark claims that the current methodology does not justify.

## Historical Value

Keep this file only as a snapshot of the earlier production-hardening hypothesis. Do not use
its task list for new implementation work.
