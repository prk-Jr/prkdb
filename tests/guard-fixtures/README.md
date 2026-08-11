# Guard fixtures

Known-bad inputs, so each discipline check can be shown to fail before its pass is
believed.

A check that cannot fail is indistinguishable from a check that passes, and this
repository has shipped several: a linearizability checker that accepted any history, five
property tests that discarded the `Err` signalling failure, a required status check that
could never report. The scripts in `scripts/check_*.sh` are the same kind of artefact and
were never held to the same standard.

Each fixture is deliberately wrong in exactly one way. `scripts/check_guards_fail.sh`
points the relevant guard at it and fails if the guard is content.
