"""Contract checks for CI entry points, shared measurements and compiler selection.

Run with Python >=3.11 and PyYAML 6.0.3. These inspect YAML structure;
actionlint and hosted Actions separately validate execution semantics.
"""
import pathlib
import tomllib
import unittest

import yaml

ROOT = pathlib.Path(__file__).resolve().parents[1]


def workflow(name):
    # BaseLoader keeps the YAML key `on` as text rather than YAML 1.1 boolean True.
    path = ROOT / ".github/workflows" / name
    assert path.is_file(), f"Missing workflow entry point: {name}"
    return yaml.load(path.read_text(), Loader=yaml.BaseLoader)


class WorkflowContracts(unittest.TestCase):
    def test_local_and_ordinary_ci_compilers_agree(self):
        pin = tomllib.loads((ROOT / "rust-toolchain.toml").read_text())["toolchain"]["channel"]
        self.assertEqual(pin, "1.100.0")
        self.assertEqual(tomllib.loads((ROOT / "Cargo.toml").read_text())["workspace"]["package"]["rust-version"], pin)
        for path in (ROOT / ".github/workflows").glob("*.yml"):
            for job in workflow(path.name)["jobs"].values():
                for step in job.get("steps", []):
                    action = step.get("uses", "")
                    if action.startswith("dtolnay/rust-toolchain@"):
                        self.assertIn(action.split("@", 1)[1], (pin, "nightly"), path.name)

    def test_forward_looking_nightly_check_remains(self):
        actions = [s.get("uses", "") for s in workflow("ci.yml")["jobs"]["nightly-check"]["steps"]]
        self.assertIn("dtolnay/rust-toolchain@nightly", actions)

    def test_scheduled_chaos_retains_prerequisites(self):
        ci = workflow("ci.yml")
        self.assertIn("schedule", ci["on"])
        chaos = ci["jobs"]["chaos-tests"]
        self.assertNotIn("if", chaos, "chaos must run after successful prerequisites on every CI event")
        self.assertEqual(chaos["needs"], ["check", "test"])
        self.assertEqual(chaos["uses"], "./.github/workflows/chaos-tests.yml")

    def test_benchmark_has_independent_scheduled_and_manual_entrypoints(self):
        bench = workflow("benchmarks.yml")
        self.assertTrue({"schedule", "workflow_dispatch", "workflow_call"} <= bench["on"].keys())
        self.assertNotEqual(bench["on"]["schedule"], workflow("ci.yml")["on"]["schedule"])
        self.assertEqual(bench["permissions"], {"contents": "read"})
        self.assertNotEqual(bench["concurrency"]["group"], workflow("ci.yml")["concurrency"]["group"])
        self.assertNotIn("if", bench["jobs"]["benchmark"])
        self.assertEqual(set(bench["jobs"]), {"benchmark"}, "scheduled measurements must not launch the mutation sweep")

    def test_push_and_pr_share_the_same_benchmark(self):
        caller = workflow("ci.yml")["jobs"]["benchmark"]
        self.assertEqual(caller["uses"], "./.github/workflows/benchmarks.yml")
        self.assertEqual(caller["if"], "github.event_name == 'push' || github.event_name == 'pull_request'")
        self.assertNotIn("steps", caller)

    def test_measurement_provenance_is_retained_with_results(self):
        steps = workflow("benchmarks.yml")["jobs"]["benchmark"]["steps"]
        runs = "\n".join(step.get("run", "") for step in steps)
        self.assertIn("rustc -Vv", runs)
        self.assertIn("GITHUB_EVENT_NAME", runs)
        self.assertIn("GITHUB_SHA", runs)
        self.assertIn("benchmark_results/provenance.txt", runs)
        uploads = [step for step in steps if step.get("uses", "").startswith("actions/upload-artifact@")]
        self.assertTrue(any(step["with"]["path"] == "benchmark_results/" for step in uploads))


if __name__ == "__main__":
    unittest.main()
