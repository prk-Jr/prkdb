# Baseline invocation-envelope implementation plan

> For agentic workers: root executes using executing-plans; controlled independent
> agents review design/code/evidence read-only. Root is sole build/mutation owner.

Goal: one fixed baseline-only history control, explicit outcome and stop.
Architecture: immutable closed helpers plus thin parser/classifier and separate
unprivileged executor; isolated Actions probe. Python stdlib, retained Rust ELF.

## 1. Claim and reviewed protocol
- [x] Dedicated phase-2 worktree, immediate note, unchanged helper copies.
- [x] Independent design review approved, then commit docs: define baseline envelope control.
- [x] Baseline80 tests PASS: PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts -p 'test_wal*py'.

## 2. Test-first parser and classifier
Files scripts/wal_workload_history.py and test_wal_workload_history.py.
- [x] Tests RED: missing module; schedule has9 calls/459 rows; golden original
  base.md parses147; reject modified config/count/order/unknown/nonfinite/cache;
  separate ceiling output; conservative ratio and complete-schedule classifiers.
- [x] Implement grid_order()->list[(cell,writers,value)], schedule()->list[dict],
  parse_output(text,kind)->all rows, summarize(runs)->per-block full/isolated
  medians/envelopes/classification. Alternate trust fixtures only in unit tests,
  CLI pinned trust fixed. Never infer equivalence from nonclassification.
- [x] GREEN all93 controls; commit test: add fail-closed workload history controls.

## 3. One bounded executor and workflow
Files same scripts plus .github/workflows/remediation-gate.yml isolated new choice.
- [x] RED boundary controls: inherited identity failure launches nothing; fresh
  output; fixed budgets/aggregate deadline; failure retains prefix and result;
  only baseline copied/launched; every call uses same config/temp filesystem.
- [x] Implement qualify_host(runtime), execute(command,env,directory,output,
  timeout_s), run(artifact,output), main(). Reuse unchanged artifact proof/scoped
  launcher/CaptureWatch/host snapshot helpers; unprivileged process groups only.
- [x] One50min workflow job records7 tool/workflow hashes, runs Python controls,
  downloads pinned run38206215742 artifact, runs schedule once, always uploads.
- [x] Independent code review approved; full own-target pre-push PASS,93 Python/actionlint PASS.
  CARGO_INCREMENTAL=0,CARGO_TARGET_DIR=worktree/target,CARGO_BUILD_JOBS=2.
- [ ] Commit ci: run bounded baseline workload history control; milestone push.

## 4. Linux evidence and closure
- [ ] Dispatch exactly once at full reviewed SHA; collect complete artifact.
- [ ] Independent inventory/source/resource/row/classifier audit and interpretation.
- [ ] Result doc docs/remediation/decisions/2026-10-13-wal-workload-history.md,
  explicit original gates unchanged, exact next decision and stop. No retry.
- [ ] Commit docs: record baseline invocation envelope evidence; update note each
  commit, clean only owned target after verification, full AGENTS section7 report.

Original golden independently parses147 canonical cells,6ceilings,21 commentary.
Independent final implementation review approved after RED/GREEN host-after and
final-deadline controls. Full own-target pre-push is running before the milestone.

Fresh full pre-push exit0:1509 nextest PASS/10 existing skips, concurrent workspace
/doctests PASS, both200seed modes/68256checks each, lint/ledger/docs all green.
Final Python93 PASS with process-inspection permission; restricted ps access
caused one sandbox-only check failure, resolved without a code/test change.
