# Explicit Python bytecode policy for frozen trace tooling

## Purpose and authority

Resolve the actual cleared-environment package failure retained at Linux qualification checkpoint e4542a1d1e3aac00bc949d99a0442c258a08e348. The maintainer requested continued remediation after the blocked report; this is the narrow local launch/provenance follow-up described there. No publication, Linux dispatch, private integration, database measurement or policy waiver is authorized.

This addendum changes the prior immutability assumption only for Python launch argument lists in scripts/stream_durable_diagnostic.py, scripts/wal_descriptor_diagnostic.py scripts/wal_stall_diagnostic.py and scripts/wal_trace_qualification.py. Original donor bytes/manifests/captures remain archived and unchanged. Analyzers, privilege/environment allowlists, identities, NOFILE handling, process containment, deadlines, sample/loss/clock checks, workload/seed counts and artifact caps remain intact. The qualification driver and its current-source validation change only as needed for explicit launch flags and truthful provenance.

## Established cause and selected correction

The ordinary workflow sets PYTHONDONTWRITEBYTECODE, but sudo env-i intentionally removes it. Root supervisor, child-limit, scoped and Python smoke processes import diagnostic modules and write __pycache__ files inside the packaged source. Exact closure validation then correctly rejects the package. A real nonprivileged env-i import reproduced three generated files and rejection with unchanged source hashes.

Use the interpreter's explicit -B option at every diagnostic Python launch. This survives cleared environments without adding environment keys, wrappers, writable cache exceptions or privilege. Retain strict extra-file rejection. Existing native Rust executable arguments are not Python interpreter arguments and must not gain -B. Only the copied Python smoke executable receives the flag before its script argument. The helper's fixed Python fixture children and protected decoder commands receive it too. No parser/analyzer or ordinary shell command semantics change.

Allowing generated cache files would change the frozen closure and admitted executable inputs. Environment/PATH shims would add a new launch mechanism. Neither is needed for this root fix.

## Launch inventory

- stream diagnostic: ownership-supervisor; privileged supervisor; copied Python smoke target. Existing framed decoder already uses -B.
- descriptor diagnostic: scoped launcher; privileged supervisor; child-limit subprocess; standalone copied Python smoke target.
- stall diagnostic: privileged supervisor.
- standalone trace qualification: copied Python smoke target, including the exec transition from scoped launcher into the copied interpreter.
- qualification driver: fixed root-fixture interpreter, fixed -c control children and protected framed decoder invocation.
- Existing workflow outer commands retain their explicit ordinary environment. No root environment whitelist is expanded.

All interpreter options precede script/-c. Preserve the actual command prefixes, argument tails, supervised execution, source/input identities and exact failed native results.

## Historical and current source identity

Keep scripts/testdata/perf_trace/linux-qualification-sources.json byte-identical to e4542a1 (SHA256 e45a97433b27ade26e98ab3ed00b6184a14d3a571283b59115eccfa7c7aa6a1a). It continues to identify all26 original donor files. integration-donors.json and original source/build archives stay unchanged.

Add scripts/testdata/perf_trace/linux-qualification-launches.json: version1, base_revision=e4542a1d1e3aac00bc949d99a0442c258a08e348, reason=explicit-python-bytecode-suppression, files containing exactly the four listed diagnostic paths. Each row records base_blob/base_sha256 from the historical manifest and current blob/SHA256/bytes. It must never relabel modified source as an unchanged donor.

The qualification source validator pins the historical manifest hash, validates exact schemas/types/paths/base/reason/four-file set and original-to-current binding, and computes current Git blob identity from actual bytes. Other22 donor files remain byte-identical. Source and archived-current hashes, full clean HEAD identity and exact package closure stay checked before/after execution. The new overlay and this spec/plan are included explicitly in the current package allowlist. Historical evidence is not rewritten or used as a new success flag.

## Discriminating local acceptance

Write tests before changing launch code. Build real command lists through existing producers, adapt only root/Linux/perf process boundaries, and execute their actual interpreter/script import prefixes in an env-i subprocess without PYTHONDONTWRITEBYTECODE. Use --help where a real root/Linux action would otherwise follow import. Check every declared producer (including nested child-limit/scoped and Python smoke argv) and real multi-hop imports. No root or capture required for this import boundary.

The old command must reproduce unexpected cache creation; fixed commands produce none and leave the exact package file set/hash unchanged. Positive checks use the actual package validation path, not only flags in strings. A negative control omitting -B on a cleared-environment hop must still create a cache and be refused. Existing extra-file/source drift/wrong category/deadline/EACCES controls remain. Do not delete caches to make verification pass.

Cover provenance refusal for altered historical manifest, missing/extra overlay path, wrong original binding/current hash/blob/byte count, wrong base/schema/type and post-package mutation. Run existing270 Python tests plus new regressions and targeted actionlint/doc/diff checks. Record each baseline/new count and interpreter. Keep all392 Rust/build inputs unchanged.

The prior full Rust gate failed at the unchanged replication batch test and audit failed for public url2.5.8; do not retry them solely to obtain green. Carry those evidence-backed blockers. Separate replication root-cause investigation may add a deterministic counterexample under its own claim. PR98 normal integration and a safe-public-base assembly still precede any D10 publication consideration.

## Files and completion

Modify only the four diagnostic launch modules, scripts/perf_trace_linux_qualification.py, scripts/test_perf_trace_linux_qualification.py, the new launch overlay and this spec/plan. Other donor tests, decoders, captures, Cargo/Rust/workflow and global ledger/status remain unchanged.

Independent plan and code review are required. Sign and retain local commits with handoff updates. Completion of this task means the actual cleared-environment import/package defect is corrected locally with truthful current-source evidence. It does not establish real recorder, root cleanup, source-build reuse or performance qualification. No new Linux attempt occurs.
