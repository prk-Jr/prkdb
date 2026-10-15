# Corrected Durable qualification implementation plan

Use controlled parallel subagents for independent trace, compilation and
protocol review; root coordinates all writes/builds/commits/push/dispatch.
Spec: ../specs/2026-10-15-durable-append-qualification-design.md.

1. [x] Claim isolated diagnostic continuation from65ae1ba, rootphase2 unchanged.
2. [x] Run135baselinePython tests; sandbox ps restriction needs scoped escalation,
   not weaker containment tests. Start full task-owned pre-push check.
3. [x] Test-first CPU/lifecycle per-event settings and strict actual-attribute
   validation in new runner/test only. Keep six donor helpers immutable.
4. [x] Test-first qualification-only workflow choice and exact `qualify` invocation.
   Preserve old full schedule behind its old choice; do not dispatch it.
5. [x] Verify final Python suites/actionlint/inline AST/diff, fullprepush, original
   binaries/source/overlay guards, independent spec then quality review.
6. [x] Commit reviewed milestone, update note, push once and dispatch once using
   probe=stream-durable-qualification and exact actual tooling SHA.
7. [x] Await actual result, preserve/download prefix or full evidence, verify
   independently. No rerun if red, no DB measurements even if green.
8. [x] Record qualified/inconclusive verdict separately from production readiness
   and old Durable performance hold; clean only owned cache/worktree.

Commits: docs: define corrected Durable recorder qualification; fix: isolate
Durable recorder event sampling; ci: qualify corrected Durable diagnostic tools;
docs: record corrected Durable qualification outcome.

Follow-ups stay open until actual evidence: exact renderer/lifecycle qualification,
fresh marked compilation, then separately authorized Durable control measurements.
No ledger finding/spec revision/perf_note/global plan changes in this task.


## Actual outcome

Sole run38686445143 at e4c12d5 built four real fresh variants and retained full
provenance, then failed before any DB call because perf6.17 retained the full
configured CPU selector. Build audit passes; recorder qualification incomplete.
Local strict-name compatibility fix7aa3424 passes145combined Python tests and
independent review; not pushed/live-qualified. All failure evidence is unchanged.
Report/decision preserve zeroDB and original performance hold. Own target cleaned
(11411files/6.3GiB logical); worktree removal is a post-report-commit handoff action.
No further push/job is authorized. Next work should separate recorder smoke from
expensive compilation or reuse exact verified executable proofs without weakening
source/runtime-library checks.
