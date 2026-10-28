# Recorder-first Durable investigation — 2026-10-28

Maintainer approved the short recorder qualification followed by the balanced
comparison, with controlled parallel subagents. No production implementation
rewrite, merge, performance exception or automatic retry is authorized.

## Design and scope

Continue isolated diagnostic tooling from f75e34f; phase2 remains f2a7984.
Add explicit `qualify-recorders --output`: it uses exactly the existing actual
CPU/syscall/scheduler checks, ordinary resources/identities, clock/lifetimes,
loss/caps/watchdog and cleanup, without a manifest, Cargo or DB execution. Existing
`run`/`qualify` retain mandatory complete manifest verification. Results distinguish
recorder qualification from build provenance. No synthetic success substitutes
for actual reports or complete category evidence.

One reviewed milestone push/dispatch starts a35minute recorder job. Only if all
three chains pass does a separate45minute balanced job restore artifact11950404099
from38686445143. Original manifest SHA256 is
b395160e7bb2d332045ae6f73e2bb099eefc8b0c6285ad8aabba2c4bbfe1c60b. Preserve all
34members/oldprefix/proofs and oldtooling unchanged at the original absolute
evidence root. Recreate exact A/B Git worktrees, apply the archived overlay and
invoke unchanged verify_manifest, including current runtime-library hashes. No
build caches/Cargo/compiler/ldd/proof rewriting/rebuild fallback. Changed library,
occupied source path, missing artifact or any mismatch stops before DB launch.

Balanced run uses a fresh balanced-run directory. It requalifies all3 chains on
its own host before the unchanged AABBABBA originals +ABmarked +syscallAB/
schedulerBA/CPUAB16calls, b1+b100×3repetitions. Historic sourceA=f2a7984 and
B=947ef54 are labelled separately from current tooling/dependency revisions.
These are the verified old executables, not newly compiled current-source results.
Original performance holds and thresholds stay unchanged.

Both jobs retain20minute nonresetting native/render budget,19minute operation+
1minute finalization reserve and120second command limit. Keep raw64/128/128MiB,
1GiB aggregate, rendered512MiB/file/2GiB total, observer256MiB/process. Restore
commands<=120seconds and8minute nonresetting outer bound. Job-level prelaunch
checks reserve22minutes native step+8minutes upload+2minutes margin. All failed
prefixes/ownership evidence are uploaded; no conditional favourable reruns.

Files: new recorder CLI and tests; new restore helper/tests; isolated additions to
remediation-gate.yml; this spec/plan/decision and ignored evidence/handoff. Six
donor helpers and benchmark bodies remain immutable. The separate CI40671318321
fix owns ci.yml. Its targeted Cargo.lock/coreCargo.toml security updates are
projected into this branch as a necessary publishing prerequisite; scanner/test
evidence is kept distinct from historical benchmark provenance. No new global
ledger finding/spec revision/R2requirement/production flag or waiver.

## Verification and completion

Test-first helper/CLI guards and strictrestore positive/negative controls, actual
Git source fixtures, all helper tests, actionlint/inlineAST/diff, securityscanners,
and full required prepush in own Cargo target. Independent cross-reviews of both
helpers and workflow before publish. Read actual job completion, download/audit
all source/hash/library/resource/event/cap/cleanup evidence and controls; report
qualified/partial/inconclusive honestly. Clean own target/worktree only.
