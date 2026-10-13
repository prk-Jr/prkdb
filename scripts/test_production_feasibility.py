#!/usr/bin/env python3
"""Static eligibility tests. Synthetic controls never certify a database."""
import copy
import importlib.util
import json
import math
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import tomllib
import unittest

SCRIPT = Path(__file__).with_name("production_feasibility.py")
ROOT = SCRIPT.parent.parent
SHA = "a" * 40
PREREQ = "b" * 40
PROTOCOL_HASH = "202cf458fb7edb297320abfbda2bc2e4d2763a0c272585d4770c767b19a70556"
APPROVED = {
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


def fixture():
    # Source/evidence metadata are SYNTHETIC UNIT CONTROLS, not native results.
    manifest = {"schema": 1, "profile": "github-screening-r2", "source_sha": SHA,
                "protocol_sha256": PROTOCOL_HASH, "loads_approved": True,
                "modes": ["Fast", "Durable"],
                "exclusions": ["transactions", "outbox", "ttl", "cluster", "legacy_event_consumer"],
                "requirements": copy.deepcopy(APPROVED),
                "loads": dict.fromkeys(["streams_fast", "streams_durable", "indexed_fast", "indexed_durable"], 100),
                "prerequisites": {"index_maintenance": PREREQ, "stream_consumer": PREREQ},
                "evidence": dict.fromkeys(["local_checks", "linux_perf", "host", "fixtures"], "checks/synthetic.txt")}
    findings = []
    ids = {f"STO-{n:02d}": "storage" for n in list(range(1, 21)) + [24]}
    ids.update({"KEY-09": "streams", "STO-21": "streams", "STO-22": "streams",
                "STO-23": "streams", "KEY-01": "keys", "KEY-02": "keys",
                "KEY-03": "keys", "KEY-05": "keys", "EVT-07": "events"})
    for name, area in ids.items():
        findings.append({"id": name, "area": area, "title": "synthetic control",
                         "severity": "medium" if name in ("KEY-09", "EVT-07") else "high",
                         "phase": 2, "status": "fixed", "changes": [SHA],
                         "regression_tests": ["script:checks/synthetic.sh"]})
    ledger = {"finding": findings,
              "phase": [{"id": n, "status": "gate_passed"} for n in range(6)]}
    context = {"head": SHA, "dirty": False, "protocol_sha256": PROTOCOL_HASH,
               "approved_protocol_integrated": True, "integrated_commits": {PREREQ},
               "references": {"script:checks/synthetic.sh"},
               "evidence_files": {"checks/synthetic.txt"}, "manifest_tracked": True}
    return manifest, ledger, context


class InventoryTests(unittest.TestCase):
    def module(self):
        self.assertTrue(SCRIPT.is_file(), "Missing offline production feasibility inventory command")
        spec = importlib.util.spec_from_file_location("production_feasibility_test_subject", SCRIPT)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def result(self, manifest=None, ledger=None, context=None):
        m, l, c = fixture()
        return self.module().inventory(manifest if manifest is not None else m, ledger if ledger is not None else l,
                                       context if context is not None else c)

    def blocked(self, result, code, capability=None):
        self.assertEqual(result["status"], "BLOCKED")
        self.assertFalse(result["timing_authorized"])
        self.assertFalse(result["production_qualified"])
        self.assertFalse(result["measurements_performed"])
        reasons = result["capabilities"][capability]["reasons"] if capability else [
            reason for cap in result["capabilities"].values() for reason in cap["reasons"]]
        self.assertIn(code, [r["code"] for r in reasons])

    def test_positive_unit_control_is_review_only_never_execution_or_certification(self):
        result = self.result()
        self.assertEqual(result["status"], "READY_FOR_REVIEW")
        for flag in ("timing_authorized", "production_qualified", "measurements_performed"):
            self.assertIs(result[flag], False)
        self.assertTrue(result["limitations"])

    def test_open_key02_blocks_indexed_but_does_not_invent_raw_stream_bug(self):
        m, l, c = fixture()
        next(f for f in l["finding"] if f["id"] == "KEY-02")["status"] = "open"
        result = self.result(m, l, c)
        self.blocked(result, "FINDING_UNRESOLVED", "indexed_data")
        self.assertEqual(result["capabilities"]["stream_append_read"]["status"], "READY_FOR_REVIEW")

    def test_medium_evt07_blocks_consumer_not_raw_append(self):
        m, l, c = fixture()
        next(f for f in l["finding"] if f["id"] == "EVT-07")["status"] = "open"
        result = self.result(m, l, c)
        self.blocked(result, "FINDING_UNRESOLVED", "stream_consumer_resume")
        self.assertEqual(result["capabilities"]["stream_append_read"]["status"], "READY_FOR_REVIEW")

    def test_new_critical_storage_finding_blocks_both_lanes(self):
        m, l, c = fixture()
        l["finding"].append({"id": "STO-99", "area": "storage", "title": "new control",
                             "severity": "critical", "phase": 2, "status": "open"})
        result = self.result(m, l, c)
        self.blocked(result, "FINDING_UNRESOLVED", "stream_append_read")
        self.blocked(result, "FINDING_UNRESOLVED", "indexed_data")

    def test_excluded_legacy_outbox_stays_in_production_inventory(self):
        m, l, c = fixture()
        l["finding"].append({"id": "EVT-01", "area": "events", "title": "legacy control",
                             "severity": "critical", "phase": 2, "status": "open"})
        result = self.result(m, l, c)
        self.assertEqual(result["status"], "READY_FOR_REVIEW")
        self.assertIn("EVT-01", [f["id"] for f in result["production_blockers"]])
        self.assertFalse(result["production_qualified"])

    def test_deleted_or_reclassified_baseline_storage_row_cannot_hide(self):
        for kind in ("missing", "reclassified"):
            m, l, c = fixture()
            if kind == "missing": l["finding"] = [f for f in l["finding"] if f["id"] != "STO-08"]
            else: next(f for f in l["finding"] if f["id"] == "STO-08")["area"] = "docs"
            self.blocked(self.result(m, l, c), "LEDGER_INVALID")

    def test_duplicate_or_wont_fix_needs_reviewed_resolution(self):
        for status in ("duplicate", "wont_fix"):
            m, l, c = fixture(); l["finding"][0]["status"] = status
            self.blocked(self.result(m, l, c), "CLOSURE_REVIEW_REQUIRED")

    def test_unsupported_ci_reference_is_unvalidated_not_malformed_ledger(self):
        m, l, c = fixture(); l["finding"][0]["regression_tests"] = ["ci-job:.github/workflows/ci.yml#test"]
        self.blocked(self.result(m, l, c), "REGRESSION_REFERENCE_UNSUPPORTED")

    def test_key05_presence_only_scope_preserves_medium_global_inventory(self):
        m, l, c = fixture(); row = next(f for f in l["finding"] if f["id"] == "KEY-05")
        row.update(status="open", severity="medium", phase=4)
        result = self.result(m, l, c)
        self.assertEqual(result["status"], "READY_FOR_REVIEW")
        self.assertIn("KEY-05", [f["id"] for f in result["production_inventory"]])
        self.assertFalse(result["production_qualified"])

    def test_missing_required_finding_cannot_hide_a_blocker(self):
        m, l, c = fixture(); l["finding"] = [f for f in l["finding"] if f["id"] != "KEY-02"]
        self.blocked(self.result(m, l, c), "LEDGER_INVALID")

    def test_duplicate_unknown_status_bad_severity_or_empty_ledger_fail_closed(self):
        for kind in ("duplicate", "status", "severity", "empty", "phase"):
            with self.subTest(kind=kind):
                m, l, c = fixture()
                if kind == "duplicate": l["finding"].append(copy.deepcopy(l["finding"][0]))
                elif kind == "status": l["finding"][0]["status"] = "maybe"
                elif kind == "severity": l["finding"][0]["severity"] = "tiny"
                elif kind == "phase": l["phase"].append(copy.deepcopy(l["phase"][0]))
                else: l = {}
                self.blocked(self.result(m, l, c), "LEDGER_INVALID")

    def test_fixed_finding_missing_fix_or_regression_metadata_blocks(self):
        for field in ("changes", "regression_tests"):
            with self.subTest(field=field):
                m, l, c = fixture(); l["finding"][0][field] = []
                self.blocked(self.result(m, l, c), "FIX_EVIDENCE_MISSING")

    def test_missing_regression_reference_blocks_even_with_fixed_status(self):
        m, l, c = fixture(); c["references"] = set()
        self.blocked(self.result(m, l, c), "REGRESSION_REFERENCE_INVALID")

    def test_unpinned_stale_dirty_or_untracked_source_blocks(self):
        for kind, code in (("empty", "SOURCE_UNPINNED"), ("stale", "SOURCE_MISMATCH"),
                           ("dirty", "SOURCE_DIRTY"), ("untracked", "MANIFEST_UNTRACKED")):
            with self.subTest(kind=kind):
                m, l, c = fixture()
                if kind == "empty": m["source_sha"] = ""
                elif kind == "stale": m["source_sha"] = "c" * 40
                elif kind == "dirty": c["dirty"] = True
                else: c["manifest_tracked"] = False
                self.blocked(self.result(m, l, c), code)

    def test_unmerged_prerequisites_never_count_branch_work_as_integration(self):
        m, l, c = fixture(); c["integrated_commits"] = set()
        self.blocked(self.result(m, l, c), "PREREQUISITE_UNMERGED", "indexed_data")
        self.blocked(self.result(m, l, c), "PREREQUISITE_UNMERGED", "stream_consumer_resume")

    def test_missing_task_pin_blocks_relevant_capability(self):
        m, l, c = fixture(); m["prerequisites"]["stream_consumer"] = ""
        self.blocked(self.result(m, l, c), "PREREQUISITE_UNPINNED", "stream_consumer_resume")

    def test_protocol_change_or_unintegrated_approval_blocks(self):
        for kind in ("input", "source", "approval"):
            with self.subTest(kind=kind):
                m, l, c = fixture()
                if kind == "input": m["protocol_sha256"] = "0" * 64
                elif kind == "source": c["protocol_sha256"] = "0" * 64
                else: c["approved_protocol_integrated"] = False
                self.blocked(self.result(m, l, c), "PROTOCOL_UNAPPROVED")

    def test_omitted_mode_or_excluded_scope_cannot_narrow_claim_silently(self):
        for field in ("modes", "exclusions"):
            m, l, c = fixture(); m[field].pop()
            self.blocked(self.result(m, l, c), "MANIFEST_INVALID")

    def test_unknown_missing_or_changed_budget_fails_closed(self):
        for kind in ("unknown", "missing", "changed", "bool"):
            m, l, c = fixture()
            if kind == "unknown": m["requirements"]["extra_budget"] = 100
            elif kind == "missing": del m["requirements"]["engine_rss_bytes"]
            elif kind == "bool": m["requirements"]["engine_rss_bytes"] = True
            else: m["requirements"]["engine_rss_bytes"] *= 2
            self.blocked(self.result(m, l, c), "MANIFEST_INVALID")

    def test_unapproved_unset_nonfinite_or_bool_loads_block(self):
        for rate in (0, -1, math.nan, math.inf, True, "100"):
            m, l, c = fixture(); m["loads"]["streams_fast"] = rate
            self.blocked(self.result(m, l, c), "LOADS_UNSET")
        m, l, c = fixture(); m["loads_approved"] = False
        self.blocked(self.result(m, l, c), "LOADS_UNAPPROVED")

    def test_unknown_or_missing_manifest_keys_and_nonbool_approval_block(self):
        for kind in ("unknown", "missing", "approval_type", "bad_sha"):
            m, l, c = fixture()
            if kind == "unknown": m["skip_checks"] = True
            elif kind == "missing": del m["evidence"]
            elif kind == "approval_type": m["loads_approved"] = "yes"
            else: m["source_sha"] = "HEAD"
            self.blocked(self.result(m, l, c), "MANIFEST_INVALID")

    def test_missing_or_outside_evidence_blocks(self):
        for value in ("", "../outside.json", "/private/tmp/outside.json", "untracked.json"):
            m, l, c = fixture(); m["evidence"]["host"] = value
            self.blocked(self.result(m, l, c), "EVIDENCE_REFERENCE_INVALID")

    def test_malformed_ledger_field_types_fail_closed_without_traceback(self):
        for field in ("area", "severity", "status"):
            m, l, c = fixture(); l["finding"][0][field] = []
            self.blocked(self.result(m, l, c), "LEDGER_INVALID")
        m, l, c = fixture(); l["phase"][0]["status"] = {}
        self.blocked(self.result(m, l, c), "LEDGER_INVALID")

    def test_cli_nonfinite_invalid_source_never_breaks_json(self):
        self.module()
        with tempfile.TemporaryDirectory(dir=ROOT) as tmp:
            path = Path(tmp)/"bad.toml"
            original = (ROOT/"docs/benchmarks/production-feasibility.toml").read_text()
            path.write_text(original.replace('source_sha = ""', 'source_sha = [nan]'))
            result = subprocess.run([sys.executable, str(SCRIPT), "inventory", "--repo", str(ROOT),
                                     "--manifest", str(path)], capture_output=True, text=True)
            self.assertEqual(result.returncode, 2, result.stderr)
            self.assertEqual(json.loads(result.stdout)["status"], "BLOCKED")

    def test_huge_integer_rate_fails_closed_in_pure_and_actual_cli(self):
        m, l, c = fixture(); m["loads"]["streams_fast"] = 10 ** 400
        self.blocked(self.result(m, l, c), "LOADS_UNSET")
        with tempfile.TemporaryDirectory(dir=ROOT) as tmp:
            path = Path(tmp)/"huge.toml"
            original = (ROOT/"docs/benchmarks/production-feasibility.toml").read_text()
            path.write_text(original.replace("streams_fast = 0", "streams_fast = " + str(10 ** 400)))
            result = subprocess.run([sys.executable, str(SCRIPT), "inventory", "--repo", str(ROOT),
                                     "--manifest", str(path)], capture_output=True, text=True)
            self.assertEqual(result.returncode, 2, result.stderr)
            self.blocked(json.loads(result.stdout), "LOADS_UNSET")

    def test_tracked_symlink_reference_cannot_read_untracked_target(self):
        subject = self.module()
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp).resolve(); target = repo/"untracked.rs"; target.write_text("fn control() {}")
            (repo/"tracked.rs").symlink_to(target)
            self.assertIsNone(subject.repo_file(repo, "tracked.rs", {"tracked.rs"}))

    def test_cli_refuses_external_ledger_symlink_before_reading_it(self):
        self.module()
        with tempfile.TemporaryDirectory() as tmp:
            outer = Path(tmp); repo = outer/"repo"; repo.mkdir()
            subprocess.run(["git", "init", "-q", str(repo)], check=True, capture_output=True)
            ledger = repo/"docs/remediation/ledger.toml"; ledger.parent.mkdir(parents=True)
            external = outer/"external.toml"; external.write_bytes((ROOT/"docs/remediation/ledger.toml").read_bytes())
            ledger.symlink_to(external)
            manifest = repo/"docs/benchmarks/production-feasibility.toml"; manifest.parent.mkdir(parents=True)
            manifest.write_bytes((ROOT/"docs/benchmarks/production-feasibility.toml").read_bytes())
            subprocess.run(["git", "-C", str(repo), "add", "docs"], check=True, capture_output=True)
            subprocess.run(["git", "-C", str(repo), "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid",
                            "commit", "-qm", "test: synthetic source control"], check=True, capture_output=True)
            result = subprocess.run([sys.executable, str(SCRIPT), "inventory", "--repo", str(repo)],
                                    capture_output=True, text=True)
            self.assertEqual(result.returncode, 2, result.stderr)
            report = json.loads(result.stdout)
            self.assertTrue("input_error" in report, "External ledger symlink must be refused before reading")
            self.assertNotIn("ledger_sha256", report["source"])

    def test_cli_cyclic_repo_manifest_and_output_emit_blocked_json(self):
        self.module()
        with tempfile.TemporaryDirectory(dir=ROOT) as tmp:
            first, second = Path(tmp)/"first", Path(tmp)/"second"
            first.symlink_to(second); second.symlink_to(first)
            for flag in ("--repo", "--manifest", "--output"):
                with self.subTest(flag=flag):
                    command = [sys.executable, str(SCRIPT), "inventory", "--repo", str(ROOT), flag, str(first)]
                    result = subprocess.run(command, capture_output=True, text=True)
                    self.assertEqual(result.returncode, 2, result.stderr)
                    self.assertEqual(json.loads(result.stdout)["status"], "BLOCKED")

    def test_cli_current_template_reports_json_blocked_without_running_a_measurement(self):
        self.module()
        result = subprocess.run([sys.executable, str(SCRIPT), "inventory", "--repo", str(ROOT)],
                                capture_output=True, text=True, env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"})
        self.assertEqual(result.returncode, 2, result.stderr)
        report = json.loads(result.stdout)
        self.blocked(report, "LOADS_UNAPPROVED")
        self.assertIn("KEY-02", report["capabilities"]["indexed_data"]["finding_ids"])
        self.assertIn("EVT-07", report["capabilities"]["stream_consumer_resume"]["finding_ids"])

    def test_cli_missing_repo_is_machine_readable_blocked(self):
        self.module()
        with tempfile.TemporaryDirectory() as tmp:
            result = subprocess.run([sys.executable, str(SCRIPT), "inventory", "--repo", str(Path(tmp)/"absent")],
                                    capture_output=True, text=True)
            self.assertEqual(result.returncode, 2)
            self.assertEqual(json.loads(result.stdout)["status"], "BLOCKED")

    def test_cli_refuses_overwriting_input_or_tracked_source(self):
        self.module()
        for target in (ROOT/"docs/benchmarks/production-feasibility.toml", ROOT/"README.md"):
            before = target.read_bytes()
            result = subprocess.run([sys.executable, str(SCRIPT), "inventory", "--repo", str(ROOT),
                                     "--output", str(target)], capture_output=True, text=True)
            self.assertEqual(result.returncode, 2)
            self.assertIn("OUTPUT_UNSAFE", result.stdout)
            self.assertEqual(target.read_bytes(), before)

    def test_cli_atomic_output_matches_stdout(self):
        self.module()
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)/"inventory.json"
            result = subprocess.run([sys.executable, str(SCRIPT), "inventory", "--repo", str(ROOT),
                                     "--output", str(out)], capture_output=True, text=True)
            self.assertEqual(result.returncode, 2)
            self.assertEqual(json.loads(out.read_text()), json.loads(result.stdout))


if __name__ == "__main__":
    unittest.main()
