# Recorder-first investigation plan

Use controlled parallel workers: recorder CLI; restore helper; independentCI
fix40671318321. Root owns workflow/docs/integration/build coordination.

1. [x] Claim continuation; baseline helpers, immutable old evidence, roots checked.
2. [x] Test-first explicit recorder-only CLI; mandatory manifest on old modes.
3. [x] Test-first exact manifest/source/library restore with realGit fixtures.
4. [x] Independent cross-reviews; combined160Python tests pass.
5. [x] Apply separately verified7dependency security fixes; scannerchecks pass.
6. [x] Fullprepush/actionlint/AST/diff/final workflow review; commit milestone.
7. [x] One milestone push/workflowdispatch, recorder job then conditional balanced
   reuse job. No extra dispatch if either red, no automatic rebuild.
8. [x] Download/audit actual outcomes; retain failures and original performance
   hold; decision/report/handoff; remove only owned target/worktree.

Separate CI fix is reviewed/published/validated on its own branch. The maintainer
subsequently authorized its exact reviewed head to merge through a PR only after
all checks pass. That authority does not cover this diagnostic branch.
No API/production implementation rewrite.

Outcome: recorder qualification passed; balanced native deadline and artifact
upload failed. No new performance result. Local observer-index and bounded
postmortem-upload fixes are reviewed; no second dispatch. See the dated
recorder-smoke decision for evidence, limitations and open follow-ups.
