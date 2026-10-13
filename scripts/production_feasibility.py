#!/usr/bin/env python3
"""Read-only static eligibility inventory. Never authorizes a timing or shipment."""
import argparse
import hashlib
import json
import math
import os
from pathlib import Path, PurePosixPath
import re
import subprocess
import tempfile
import tomllib

PROTOCOL = "docs/superpowers/specs/2026-10-13-production-feasibility-design.md"
PROTOCOL_COMMIT = "064b7e0dc180ab27e72811bdfab191386ac0989c"
PROTOCOL_SHA256 = "202cf458fb7edb297320abfbda2bc2e4d2763a0c272585d4770c767b19a70556"
DEFAULT_MANIFEST = "docs/benchmarks/production-feasibility.toml"
REQUIREMENTS = {
    "engine_rss_bytes": 2147483648, "client_rss_bytes": 268435456,
    "engine_cgroup_bytes": 4294967296, "client_cgroup_bytes": 536870912,
    "rss_growth_bytes": 67108864, "queued_bytes": 67108864,
    "batch_bytes": 16777216, "outstanding_requests": 16,
    "indexed_1k_count": 100000, "indexed_64k_count": 10000,
    "stream_retained_bytes": 1073741824, "segment_bytes": 134217728,
    "pinned_bytes": 1207959552, "resource_seconds_per_mode": 600,
    "resource_warmup_seconds_per_mode": 120, "ordinary_warmup_seconds": 30,
    "ordinary_measurement_seconds": 120, "stream_recovery_attempts": 12,
    "indexed_recovery_attempts": 24, "native_resource_seconds": 3300,
    "job_seconds": 5400, "wal_ready_128m_ms": 1000, "wal_ready_1g_ms": 5000,
    "index_ready_128m_ms": 5000, "index_ready_1g_ms": 20000,
    "consumer_ready_128m_ms": 2000, "consumer_ready_1g_ms": 10000,
}
STORAGE_IDS = {f"STO-{n:02d}" for n in list(range(1, 21)) + [24]}
STREAM_IDS = {"KEY-09", "STO-21", "STO-22", "STO-23"}
KEY_IDS = {"KEY-01", "KEY-02", "KEY-03", "KEY-05"}
REQUIRED_AREAS = {**dict.fromkeys(STORAGE_IDS, "storage"),
                  **dict.fromkeys(STREAM_IDS, "streams"),
                  **dict.fromkeys(KEY_IDS, "keys"), "EVT-07": "events"}
AREAS = {"storage", "streams", "keys", "events", "transactions", "ttl", "raft",
         "schema", "release", "tests", "testing", "docs"}
EXCLUSIONS = ["transactions", "outbox", "ttl", "cluster", "legacy_event_consumer"]
LOADS = {"streams_fast", "streams_durable", "indexed_fast", "indexed_durable"}
PREREQUISITES = {"index_maintenance", "stream_consumer"}
EVIDENCE = {"local_checks", "linux_perf", "host", "fixtures"}
CAPABILITIES = ("stream_append_read", "stream_consumer_resume", "indexed_data")


def digest(data):
    return hashlib.sha256(data).hexdigest()


def sha(value, size=40, blank=False):
    return isinstance(value, str) and ((blank and value == "") or
                                      re.fullmatch(r"[0-9a-f]{%d}" % size, value) is not None)


def keys(value, expected):
    if not isinstance(value, dict) or set(value) != set(expected):
        raise ValueError("Missing or unknown schema keys")


def validate_manifest(m):
    keys(m, {"schema", "profile", "source_sha", "protocol_sha256", "loads_approved",
             "modes", "exclusions", "requirements", "loads", "prerequisites", "evidence"})
    if (type(m["schema"]) is not int or m["schema"] != 1 or
            m["profile"] != "github-screening-r2" or not sha(m["source_sha"], blank=True) or
            not sha(m["protocol_sha256"], 64) or type(m["loads_approved"]) is not bool or
            m["modes"] != ["Fast", "Durable"] or m["exclusions"] != EXCLUSIONS):
        raise ValueError("Invalid profile/source/modes/approval/scope")
    keys(m["requirements"], REQUIREMENTS)
    if any(type(m["requirements"][k]) is not int or m["requirements"][k] != v
           for k, v in REQUIREMENTS.items()):
        raise ValueError("Requirements differ from approved R2")
    for field, expected in (("loads", LOADS), ("prerequisites", PREREQUISITES), ("evidence", EVIDENCE)):
        keys(m[field], expected)
    if any(not sha(v, blank=True) for v in m["prerequisites"].values()):
        raise ValueError("Prerequisites require immutable commit SHAs")
    if any(not isinstance(v, str) for v in m["evidence"].values()):
        raise ValueError("Evidence references require paths")


def validate_ledger(ledger):
    if not isinstance(ledger, dict):
        raise ValueError("Invalid ledger")
    findings, phases = ledger.get("finding"), ledger.get("phase")
    if not isinstance(findings, list) or not findings or not isinstance(phases, list):
        raise ValueError("Empty or incomplete ledger")
    rows = {}
    for f in findings:
        if (not isinstance(f, dict) or not isinstance(f.get("id"), str) or
                re.fullmatch(r"[A-Z]+-[0-9]+", f["id"]) is None or f["id"] in rows or
                not isinstance(f.get("area"), str) or f["area"] not in AREAS or
                not isinstance(f.get("severity"), str) or f["severity"] not in {"critical", "high", "medium", "low"} or
                not isinstance(f.get("status"), str) or f["status"] not in {"open", "fixed", "verified", "wont_fix", "duplicate"} or
                type(f.get("phase")) is not int or not 0 <= f["phase"] <= 6 or
                not isinstance(f.get("title"), str)):
            raise ValueError("Malformed or duplicate finding")
        rows[f["id"]] = f
    if any(name not in rows or rows[name]["area"] != area for name, area in REQUIRED_AREAS.items()):
        raise ValueError("Missing or reclassified baseline finding")
    phase_ids = set()
    for p in phases:
        if (not isinstance(p, dict) or type(p.get("id")) is not int or not 0 <= p["id"] <= 6 or
                p["id"] in phase_ids or not isinstance(p.get("status"), str) or
                p["status"] not in {"gate_passed", "in_progress", "not_started"}):
            raise ValueError("Malformed or duplicate phase")
        phase_ids.add(p["id"])
    if not set(range(6)) <= phase_ids:
        raise ValueError("Missing phase gates")
    return rows


def reason(code, message, finding_id=None):
    result = {"code": code, "message": message}
    if finding_id is not None:
        result["finding_id"] = finding_id
    return result


def relevant(capability, finding):
    name, area = finding["id"], finding["area"]
    if name in STORAGE_IDS or area == "storage" and finding["severity"] in {"critical", "high"}:
        return True
    if capability.startswith("stream_"):
        return name in STREAM_IDS or area == "streams" and finding["severity"] in {"critical", "high"} or (
            capability == "stream_consumer_resume" and name == "EVT-07")
    return name in KEY_IDS - {"KEY-05"} or area == "keys" and finding["severity"] in {"critical", "high"}


def inventory(manifest, ledger, context):
    """Static checks only. Context is derived from read-only Git by the CLI."""
    global_reasons = []
    try:
        rows = validate_ledger(ledger)
    except ValueError as error:
        rows = {}; global_reasons.append(reason("LEDGER_INVALID", str(error)))
    valid = True
    try:
        validate_manifest(manifest)
    except ValueError as error:
        valid = False; global_reasons.append(reason("MANIFEST_INVALID", str(error)))
    if context.get("template_binding_valid") is False:
        global_reasons.append(reason("TEMPLATE_MISMATCH", "Run artifact is not bound to the observed HEAD template"))
    if valid:
        if (manifest["protocol_sha256"] != PROTOCOL_SHA256 or
                context.get("protocol_sha256") != PROTOCOL_SHA256 or
                context.get("approved_protocol_integrated") is not True):
            global_reasons.append(reason("PROTOCOL_UNAPPROVED", "Approved protocol blob/commit does not match"))
        if not manifest["source_sha"]:
            global_reasons.append(reason("SOURCE_UNPINNED", "An immutable measured-source commit has not been selected"))
        elif context.get("head") != manifest["source_sha"]:
            global_reasons.append(reason("SOURCE_MISMATCH", "Selected source is not observed HEAD"))
        if context.get("dirty") is not False:
            global_reasons.append(reason("SOURCE_DIRTY", "Source has uncommitted or untracked changes"))
        if context.get("manifest_tracked") is not True:
            global_reasons.append(reason("MANIFEST_UNTRACKED", "Requirements template is not tracked in observed source"))
        if manifest["loads_approved"] is not True:
            global_reasons.append(reason("LOADS_UNAPPROVED", "Offered rates have not been approved"))
        for name, value in sorted(manifest["loads"].items()):
            if not usable_rate(value):
                global_reasons.append(reason("LOADS_UNSET", "A finite positive offered rate is required: " + name))
        for name, path in sorted(manifest["evidence"].items()):
            if not relative_path(path) or path not in context.get("evidence_files", set()):
                global_reasons.append(reason("EVIDENCE_REFERENCE_INVALID", "Missing/untracked/outside declared reference: " + name))
    caps = {}
    for cap in CAPABILITIES:
        reasons = list(global_reasons)
        for f in sorted(rows.values(), key=lambda f: f["id"]):
            if not relevant(cap, f):
                continue
            name = f["id"]
            if f["status"] in {"duplicate", "wont_fix"}:
                reasons.append(reason("CLOSURE_REVIEW_REQUIRED", "Canonical closure/exception needs review", name))
            elif f["status"] not in {"fixed", "verified"}:
                reasons.append(reason("FINDING_UNRESOLVED", f["title"], name))
            elif (not string_list(f.get("changes")) or not string_list(f.get("regression_tests"))):
                reasons.append(reason("FIX_EVIDENCE_MISSING", "Fixed row lacks changes/regression references", name))
            else:
                for ref in f["regression_tests"]:
                    if not ref.startswith(("test:", "script:")):
                        reasons.append(reason("REGRESSION_REFERENCE_UNSUPPORTED", "Reference grammar requires separate validation", name))
                    elif ref not in context.get("references", set()):
                        reasons.append(reason("REGRESSION_REFERENCE_INVALID", "Regression path/function presence is unresolved", name))
        task = {"stream_consumer_resume": "stream_consumer", "indexed_data": "index_maintenance"}.get(cap)
        if valid and task:
            commit = manifest["prerequisites"][task]
            if not commit:
                reasons.append(reason("PREREQUISITE_UNPINNED", "Integrated task reference has not been selected: " + task))
            elif commit not in context.get("integrated_commits", set()):
                reasons.append(reason("PREREQUISITE_UNMERGED", "Declared task commit is not an ancestor of source: " + task))
        caps[cap] = {"status": "BLOCKED" if reasons else "READY_FOR_REVIEW", "reasons": reasons,
                     "finding_ids": sorted({r["finding_id"] for r in reasons if "finding_id" in r})}
    lane = lambda names: "BLOCKED" if any(caps[n]["status"] == "BLOCKED" for n in names) else "READY_FOR_REVIEW"
    lanes = {"streams": lane(CAPABILITIES[:2]), "indexed": lane(CAPABILITIES[2:])}
    return {"schema": 1, "kind": "static-eligibility-inventory",
            "status": "BLOCKED" if "BLOCKED" in lanes.values() else "READY_FOR_REVIEW",
            "source": {"observed_sha": context.get("head"),
                       "declared_sha": manifest.get("source_sha") if isinstance(manifest, dict) and sha(manifest.get("source_sha"), blank=True) else None,
                       "dirty": context.get("dirty")},
            "requirements": dict(REQUIREMENTS), "capabilities": caps, "lanes": lanes,
            "production_blockers": [{k: f[k] for k in ("id", "area", "severity", "status", "title")}
                                    for f in sorted(rows.values(), key=lambda f: f["id"])
                                    if f["severity"] in {"critical", "high"} and f["status"] not in {"fixed", "verified"}],
            "production_inventory": [{k: f[k] for k in ("id", "area", "severity", "status", "title")}
                                     for f in sorted(rows.values(), key=lambda f: f["id"])
                                     if f["status"] not in {"fixed", "verified"}],
            "phase_gates": [{"id": p["id"], "status": p["status"]} for p in ledger.get("phase", [])] if rows else [],
            "limitations": ["STATIC inventory only; no benchmark, test execution or remote CI verification",
                            "Ancestor SHAs are declared references, not verified task completion",
                            "File/function presence is not evidence of passing checks",
                            "Full approved protocol/runtime manifest and actual P1/P2 evidence still required"],
            "timing_authorized": False, "production_qualified": False, "measurements_performed": False}


def usable_rate(value):
    if type(value) is int:
        return 0 < value <= (1 << 63) - 1
    return type(value) is float and math.isfinite(value) and value > 0


def string_list(value):
    return isinstance(value, list) and bool(value) and all(isinstance(v, str) and bool(v.strip()) for v in value)


def relative_path(path):
    return isinstance(path, str) and bool(path) and not PurePosixPath(path).is_absolute() and not any(
        p in {".", ".."} for p in path.split("/")) and "\\" not in path


def git(repo, *args):
    return subprocess.check_output(["git", "-C", str(repo), *args], stderr=subprocess.DEVNULL)


def integrated(repo, commit, head):
    if not sha(commit):
        return False
    return subprocess.run(["git", "-C", str(repo), "merge-base", "--is-ancestor", commit, head],
                          stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0


def nonsymlink_file(repo, path):
    """Check lexical containment and every component before resolving/reading."""
    candidate = repo/path
    if not candidate.is_relative_to(repo):
        return None
    relative = candidate.relative_to(repo)
    if ".." in relative.parts:
        return None
    current = repo
    for part in relative.parts:
        current = current/part
        if current.is_symlink():
            return None
    return candidate if candidate.is_file() else None


def run_file(repo, path, tracked):
    candidate = nonsymlink_file(repo, path)
    if candidate is None or not candidate.is_relative_to(repo/".agents/verification"):
        raise ValueError("Run artifact must be a nonsymlink file below .agents/verification")
    relative = candidate.relative_to(repo).as_posix()
    ignored = subprocess.run(["git", "-C", str(repo), "check-ignore", "--quiet", "--", relative],
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0
    if relative in tracked or not ignored:
        raise ValueError("Run artifact must be untracked AND Git-ignored")
    return candidate


def repo_file(repo, path, tracked):
    if not relative_path(path) or path not in tracked:
        return None
    candidate = nonsymlink_file(repo, Path(path))
    if candidate is None:
        return None
    resolved = candidate.resolve()
    if (not resolved.is_relative_to(repo) or not resolved.is_file() or
            resolved.relative_to(repo).as_posix() not in tracked):
        return None
    return resolved


def context_from_repo(repo, manifest_path, manifest, ledger):
    head = git(repo, "rev-parse", "HEAD").decode().strip()
    tracked = set(git(repo, "ls-files", "-z").decode().split("\0")) - {""}
    proto = repo_file(repo, PROTOCOL, tracked)
    refs = set()
    findings = ledger.get("finding", [])
    findings = findings if isinstance(findings, list) else []
    for finding in findings:
        if not isinstance(finding, dict) or not isinstance(finding.get("regression_tests", []), list):
            continue
        for ref in finding.get("regression_tests", []):
            if not isinstance(ref, str):
                continue
            if ref.startswith("script:"):
                file = repo_file(repo, ref[7:], tracked)
                if file and file.stat().st_size:
                    refs.add(ref)
            elif ref.startswith("test:") and "::" in ref:
                path, name = ref[5:].rsplit("::", 1)
                file = repo_file(repo, path, tracked)
                if file and re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*", name):
                    if re.search(r"\bfn\s+" + re.escape(name) + r"\b", file.read_text()):
                        refs.add(ref)
    evidence = manifest.get("evidence", {}) if isinstance(manifest, dict) else {}
    prerequisites = manifest.get("prerequisites", {}) if isinstance(manifest, dict) else {}
    evidence = evidence if isinstance(evidence, dict) else {}
    prerequisites = prerequisites if isinstance(prerequisites, dict) else {}
    files = set()
    for path in evidence.values():
        file = repo_file(repo, path, tracked)
        if file and file.stat().st_size:
            files.add(path)
    return {"head": head, "dirty": bool(git(repo, "status", "--porcelain", "--untracked-files=all")),
            "protocol_sha256": digest(proto.read_bytes()) if proto else None,
            "approved_protocol_integrated": integrated(repo, PROTOCOL_COMMIT, head) and
            digest(git(repo, "show", PROTOCOL_COMMIT + ":" + PROTOCOL)) == PROTOCOL_SHA256,
            "integrated_commits": {c for c in prerequisites.values() if integrated(repo, c, head)},
            "references": refs, "evidence_files": files,
            "manifest_tracked": manifest_path.is_relative_to(repo) and manifest_path.relative_to(repo).as_posix() in tracked,
            "tracked_files": tracked}


def atomic_output(path, text, repo, tracked, inputs):
    # Noreplace hardlink publishes the fully-written same-directory temp atomically.
    resolved = path.resolve()
    if (path.is_symlink() or path.exists() or resolved in inputs or
            resolved.is_relative_to(repo) and resolved.relative_to(repo).as_posix() in tracked):
        raise ValueError("Output must be new and must not overwrite input/tracked files or aliases")
    path.parent.mkdir(parents=True, exist_ok=True)
    name = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent, delete=False) as handle:
            name = handle.name; handle.write(text); handle.flush(); os.fsync(handle.fileno())
        os.link(name, path, follow_symlinks=False)
    finally:
        if name is not None:
            os.unlink(name)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("inventory")
    p.add_argument("--repo", type=Path, default=Path(__file__).resolve().parent.parent)
    p.add_argument("--manifest", type=Path, default=Path(DEFAULT_MANIFEST))
    p.add_argument("--run-manifest", type=Path, help="Explicit ignored per-run TOML below .agents/verification")
    p.add_argument("--output", type=Path)
    args = parser.parse_args()
    repo = args.repo; tracked = set(); inputs = set()
    try:
        repo = repo.resolve()
        root = Path(git(repo, "rev-parse", "--show-toplevel").decode().strip()).resolve()
        if root != repo:
            raise ValueError("--repo must be the worktree root")
        head = git(repo, "rev-parse", "HEAD").decode().strip()
        tracked = set(git(repo, "ls-files", "-z").decode().split("\0")) - {""}
        if repo/args.manifest != repo/DEFAULT_MANIFEST:
            raise ValueError("--manifest must select the fixed tracked requirements template")
        manifest_path = repo_file(repo, DEFAULT_MANIFEST, tracked)
        ledger_path = repo_file(repo, "docs/remediation/ledger.toml", tracked)
        if manifest_path is None or ledger_path is None:
            raise ValueError("Template and ledger must be tracked nonsymlink repository files")
        inputs = {manifest_path, ledger_path, (repo/PROTOCOL).resolve()}
        template_bytes = manifest_path.read_bytes()
        template_blob = git(repo, "show", head + ":" + DEFAULT_MANIFEST)
        if template_bytes != template_blob:
            raise ValueError("Template checkout differs from exact HEAD blob")
        template = tomllib.loads(template_bytes.decode())
        validate_manifest(template)
        manifest_bytes = template_bytes; manifest = template; run_bytes = None
        template_hash = digest(template_blob); binding_valid = True
        if args.run_manifest is not None:
            path = run_file(repo, args.run_manifest, tracked)
            inputs.add(path)
            run_bytes = path.read_bytes()
            manifest_bytes = run_bytes
            manifest = tomllib.loads(run_bytes.decode())
            binding = manifest.pop("template_sha256", None)
            binding_valid = sha(binding, 64) and binding == template_hash
        ledger_bytes = ledger_path.read_bytes()
        ledger = tomllib.loads(ledger_bytes.decode())
        context = context_from_repo(repo, manifest_path, manifest, ledger)
        if context["head"] != head or git(repo, "rev-parse", "HEAD").decode().strip() != head:
            raise ValueError("Source HEAD changed during snapshot")
        context["dirty"] = bool(git(repo, "status", "--porcelain", "--untracked-files=all"))
        context["template_binding_valid"] = binding_valid
        report = inventory(manifest, ledger, context)
        report["source"].update(manifest_sha256=digest(manifest_bytes), ledger_sha256=digest(ledger_bytes),
                                protocol_sha256=context["protocol_sha256"], template_sha256=template_hash,
                                run_manifest_sha256=digest(run_bytes) if run_bytes is not None else None)
        if run_bytes is not None:
            report["run_manifest_snapshot"] = {"sha256": digest(run_bytes), "content_utf8": run_bytes.decode()}
    except (OSError, RuntimeError, ValueError, subprocess.CalledProcessError) as error:
        report = inventory({}, {}, {})
        report["input_error"] = type(error).__name__ + ": repository/manifest/ledger input unavailable or invalid"
    text = json.dumps(report, indent=2, sort_keys=True, allow_nan=False) + "\n"
    if args.output:
        try:
            # Re-read tracked paths even when input parsing failed, to protect source.
            if not tracked:
                tracked = set(git(repo, "ls-files", "-z").decode().split("\0"))
            atomic_output(args.output, text, repo, tracked, inputs)
        except (OSError, RuntimeError, ValueError, subprocess.CalledProcessError):
            report["status"] = "BLOCKED"
            report["output_error"] = "OUTPUT_UNSAFE: unable to create a new non-source artifact"
            text = json.dumps(report, indent=2, sort_keys=True, allow_nan=False) + "\n"
    print(text, end="")
    return 2 if report["status"] == "BLOCKED" else 0


if __name__ == "__main__":
    raise SystemExit(main())
