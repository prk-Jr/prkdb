# Corrected Durable qualification implementation plan

Use controlled parallel subagents for independent trace, compilation and
protocol review; root coordinates all writes/builds/commits/push/dispatch.
Spec: ../specs/2026-10-15-durable-append-qualification-design.md.

1. [x] Claim isolated diagnostic continuation from65ae1ba, rootphase2 unchanged.
2. [x] Run135baselinePython tests; sandbox ps restriction needs scoped escalation,
   not weaker containment tests. Start full task-owned pre-push check.
3. [ ] Test-first CPU/lifecycle per-event settings and strict actual-attribute
   validation in new runner/test only. Keep six donor helpers immutable.
4. [x] Test-first qualification-only workflow choice and exact `qualify` invocation.
   Preserve old full schedule behind its old choice; do not dispatch it.
5. [ ] Verify final Python suites/actionlint/inline AST/diff, fullprepush, original
   binaries/source/overlay guards, independent spec then quality review.
6. [ ] Commit reviewed milestone, update note, push once and dispatch once using
   probe=stream-durable-qualification and exact actual tooling SHA.
7. [ ] Await actual result, preserve/download prefix or full evidence, verify
   independently. No rerun if red, no DB measurements even if green.
8. [ ] Record qualified/inconclusive verdict separately from production readiness
   and old Durable performance hold; clean only owned cache/worktree.

Commits: docs: define corrected Durable recorder qualification; fix: isolate
Durable recorder event sampling; ci: qualify corrected Durable diagnostic tools;
docs: record corrected Durable qualification outcome.

Follow-ups stay open until actual evidence: exact renderer/lifecycle qualification,
fresh marked compilation, then separately authorized Durable control measurements.
No ledger finding/spec revision/perf_note/global plan changes in this task.
