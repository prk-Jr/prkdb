# Recorder-first investigation plan

Use controlled parallel workers: recorder CLI; restore helper; independentCI
fix40671318321. Root owns workflow/docs/integration/build coordination.

1. [x] Claim continuation; baseline helpers, immutable old evidence, roots checked.
2. [x] Test-first explicit recorder-only CLI; mandatory manifest on old modes.
3. [x] Test-first exact manifest/source/library restore with realGit fixtures.
4. [x] Independent cross-reviews; combined160Python tests pass.
5. [x] Apply separately verified7dependency security fixes; scannerchecks pass.
6. [x] Fullprepush/actionlint/AST/diff/final workflow review; commit milestone.
7. [ ] One milestone push/workflowdispatch, recorder job then conditional balanced
   reuse job. No extra dispatch if either red, no automatic rebuild.
8. [ ] Download/audit actual outcomes; retain failures and original performance
   hold; decision/report/handoff; remove only owned target/worktree.

Separate CI fix is reviewed/published/validated on its own branch, never merged
into main or phase2 automatically. No API/production implementation rewrite.
