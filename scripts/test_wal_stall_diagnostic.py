"""Fail-closed controls for the bounded, diagnostic-only retained-binary probe."""
import copy
import hashlib
import json
import os
from pathlib import Path
import struct
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import wal_stall_diagnostic as diagnostic


CELLS = ("adapter_put/8w/64k", "stream_read/cold/64k", "stream_read/tail/1k")
WRITE_HEADER = "| cell | writers | value | ops/s | MB/s | p50 µs | p99 µs | p99.9 µs | load1 |"
STREAM_HEADER = "| cell | writers | value | ops/s | MB/s | p50 µs | p99 µs | p99.9 µs | records/s | load1 |"
NATIVE_HEADER = "- warm-up 1000 ms, measure 3000 ms, reps 3, tokio worker threads = 4"


def digest(data):
    return hashlib.sha256(data).hexdigest()


def native(cell=CELLS[0], values=(1000, 1200, 1100)):
    stream = cell.startswith("stream_read/")
    size = "1 KiB" if cell.endswith("/1k") else "64 KiB"
    writers = "8" if cell == CELLS[0] else "1"
    lines = ["# wal_write_path", NATIVE_HEADER, "- load average at start: 1.1", WRITE_HEADER,
             "|---|---|---|---|---|---|---|---|---|"]
    if stream:
        lines += [STREAM_HEADER, "|---|---|---|---|---|---|---|---|---|---|"]
    for value in values:
        metrics = f"{value} | 65.5 | 4.0 | 20.0 | 30.0"
        if stream:
            metrics += " | 100000"
        lines.append(f"| {cell} | {writers} | {size} | {metrics} | 1.0 |")
    if not stream:
        lines += [STREAM_HEADER, "|---|---|---|---|---|---|---|---|---|---|"]
    return "\n".join(lines) + "\n"


def artifact_fixture(directory):
    """Original artifact schema, independently pinned fixture binary/source identities."""
    expected = {}
    files = {}
    elf_header = b"\x7fELF\x02\x01\x01" + b"\x00" * 9 + struct.pack("<HHI", 3, 62, 1) + b"\x00" * 40
    for side, sha in (("base", "a" * 40), ("head", "b" * 40)):
        binary = elf_header + side.encode()
        expected[side] = {"source_sha": sha, "binary_sha256": digest(binary)}
        manifest = {
            "source_sha": sha, "binary_sha256": digest(binary), "saved_binary_sha256": digest(binary),
            "executable": f"/home/runner/target/release/deps/wal_write_path-{side}",
            "profile": "bench", "benchmark": "wal_write_path",
            "command": "SPIKE_REPS=3 cargo bench -p prkdb --bench wal_write_path",
            "rustc": "rustc 1.98.1\nhost: x86_64-unknown-linux-gnu\nrelease: 1.98.1\n",
            "cargo": "cargo 1.98.1\n",
            "environment": {"CARGO_TARGET_DIR": "/home/runner/target", "CARGO_INCREMENTAL": "0",
                            "RUSTFLAGS": None, "RUSTUP_TOOLCHAIN": None},
        }
        files[f"{side}-benchmark-executable"] = binary
        files[f"{side}-binary-manifest.json"] = json.dumps(manifest).encode()
        files[f"{side}-binary-verified.txt"] = b"binary provenance verified\n"
        for exit_name in ("build-exit-code", "exit-code", "log-exit-code"):
            files[f"{side}-{exit_name}.txt"] = b"0\n"
        files[f"{side}-build.jsonl"] = (json.dumps({"reason": "compiler-artifact", "target": {"name": "wal_write_path", "kind": ["bench"]}, "executable": manifest["executable"]}) + "\n").encode()
        files[f"{side}-build.stderr"] = b"Finished bench profile\n"
        files[f"{side}-sha.txt"] = (sha + "\n").encode()
        paths = ("Cargo.lock", "Cargo.toml", "rust-toolchain.toml", "crates/prkdb/Cargo.toml", "crates/prkdb/benches/wal_write_path.rs")
        files[f"{side}-source-manifest.txt"] = (sha + "\n" + "c" * 40 + "\n" + "\n".join(digest(path.encode()) + "  " + path for path in paths) + "\n").encode()
        files[f"{side}.md"] = native().encode()
        files[f"{side}.stderr"] = b""
    for name in ("binary_provenance.py", "wal_fast_rule.py", "host.txt", "lscpu.txt"):
        files[name] = ("fixture " + name + "\n").encode()
    for name, data in files.items():
        (directory / name).write_bytes(data)
    inventory(directory)
    return expected


def inventory(directory):
    entries = []
    for path in sorted(directory.iterdir()):
        if path.name != "SHA256SUMS" and path.is_file():
            entries.append(f"{digest(path.read_bytes())}  {path.name}\n")
    (directory / "SHA256SUMS").write_text("".join(entries))


class ArtifactTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.expected = artifact_fixture(self.root)

    def reject(self):
        with self.assertRaises(diagnostic.DiagnosticError):
            diagnostic.verify_artifact(self.root, expected=self.expected)

    def test_complete_original_shaped_fixture_is_qualified(self):
        result = diagnostic.verify_artifact(self.root, expected=self.expected)
        self.assertIsInstance(result, dict)

    def test_default_trust_does_not_accept_fixture_provided_identities(self):
        with self.assertRaises(diagnostic.DiagnosticError):
            diagnostic.verify_artifact(self.root)

    def test_binary_mutation_without_inventory_update_is_rejected(self):
        (self.root / "base-benchmark-executable").write_bytes(b"tampered")
        self.reject()

    def test_self_consistent_rehashed_binary_forgery_is_rejected(self):
        binary = (self.root / "head-benchmark-executable").read_bytes() + b"changed"
        (self.root / "head-benchmark-executable").write_bytes(binary)
        path = self.root / "head-binary-manifest.json"
        manifest = json.loads(path.read_text())
        manifest["binary_sha256"] = manifest["saved_binary_sha256"] = digest(binary)
        path.write_text(json.dumps(manifest))
        inventory(self.root)
        self.reject()

    def test_source_identity_mismatch_is_rejected_even_when_rehashed(self):
        for filename in ("base-source-manifest.txt", "base-sha.txt"):
            with self.subTest(filename=filename):
                expected = artifact_fixture(self.root)
                path = self.root / filename
                path.write_text(path.read_text().replace("a" * 40, "d" * 40))
                inventory(self.root)
                with self.assertRaises(diagnostic.DiagnosticError):
                    diagnostic.verify_artifact(self.root, expected=expected)

    def test_manifest_source_and_saved_binary_disagreement_are_rejected(self):
        for field, value in (("source_sha", "d" * 40), ("saved_binary_sha256", "f" * 64), ("benchmark", "another_bench")):
            with self.subTest(field=field):
                artifact_fixture(self.root)
                path = self.root / "base-binary-manifest.json"
                manifest = json.loads(path.read_text())
                manifest[field] = value
                path.write_text(json.dumps(manifest))
                inventory(self.root)
                self.reject()

    def test_missing_inventoried_member_is_rejected(self):
        (self.root / "base-build.stderr").unlink()
        self.reject()

    def test_required_exit_missing_even_from_inventory_is_rejected(self):
        (self.root / "head-exit-code.txt").unlink()
        inventory(self.root)
        self.reject()

    def test_nonzero_or_malformed_native_build_or_log_exit_is_rejected(self):
        for side in ("base", "head"):
            for name in ("build-exit-code", "exit-code", "log-exit-code"):
                with self.subTest(side=side, name=name):
                    artifact_fixture(self.root)
                    (self.root / f"{side}-{name}.txt").write_text("1\n")
                    inventory(self.root)
                    self.reject()
        artifact_fixture(self.root)
        (self.root / "head-exit-code.txt").write_text("0 trailing\n")
        inventory(self.root)
        self.reject()

    def test_duplicate_inventory_and_unlisted_file_are_rejected(self):
        path = self.root / "SHA256SUMS"
        path.write_text(path.read_text() + path.read_text().splitlines()[0] + "\n")
        self.reject()
        inventory(self.root)
        (self.root / "uninventoried").write_text("hidden")
        self.reject()

    def test_inventory_traversal_and_absolute_paths_are_rejected(self):
        for name in ("../outside", "/tmp/outside", "nested/../../outside"):
            with self.subTest(name=name):
                artifact_fixture(self.root)
                path = self.root / "SHA256SUMS"
                path.write_text(path.read_text() + f"{'0' * 64}  {name}\n")
                self.reject()

    def test_inventory_symlink_is_rejected_even_with_matching_hash(self):
        path = self.root / "host.txt"
        data = path.read_bytes()
        outside = self.root.parent / (self.root.name + "-external")
        outside.write_bytes(data)
        self.addCleanup(outside.unlink)
        path.unlink()
        path.symlink_to(outside)
        self.reject()


class NativeParserTests(unittest.TestCase):
    def test_write_three_native_rows_are_preserved(self):
        rows = diagnostic.parse_native(native(), CELLS[0])
        self.assertEqual([row["ops_s"] for row in rows], [1000, 1200, 1100])
        self.assertEqual(rows[0]["p999_us"], 30)

    def test_both_stream_cells_preserve_record_rate(self):
        for cell in CELLS[1:]:
            with self.subTest(cell=cell):
                rows = diagnostic.parse_native(native(cell), cell)
                self.assertEqual(len(rows), 3)
                self.assertEqual(rows[0]["records_s"], 100000)

    def test_cold_cache_drop_failure_cannot_be_qualified_as_cold(self):
        text = native(CELLS[1]) + "- stream_read/cold/64k: page cache could not be dropped here; equals tail\n"
        with self.assertRaises(diagnostic.DiagnosticError):
            diagnostic.parse_native(text, CELLS[1])

    def test_wrong_or_duplicate_run_configuration_header_is_rejected(self):
        alterations = ("warm-up 999 ms", "measure 2999 ms", "reps 2", "worker threads = 8")
        originals = ("warm-up 1000 ms", "measure 3000 ms", "reps 3", "worker threads = 4")
        for original, replacement in zip(originals, alterations):
            with self.subTest(replacement=replacement):
                with self.assertRaises(diagnostic.DiagnosticError):
                    diagnostic.parse_native(native().replace(original, replacement), CELLS[0])
        with self.assertRaises(diagnostic.DiagnosticError):
            diagnostic.parse_native(native() + NATIVE_HEADER + "\n", CELLS[0])

    def test_wrong_missing_or_extra_data_cell_is_rejected(self):
        for text in (native(CELLS[1]), native().replace(CELLS[0], "unknown"),
                     native() + native(CELLS[1]).splitlines()[-1] + "\n"):
            with self.subTest(text=text):
                with self.assertRaises(diagnostic.DiagnosticError):
                    diagnostic.parse_native(text, CELLS[0])

    def test_missing_or_fourth_repetition_is_rejected(self):
        lines = native().splitlines()
        data_index = next(i for i, line in enumerate(lines) if line.startswith("| " + CELLS[0] + " |"))
        for text in ("\n".join(lines[:data_index] + lines[data_index + 1:]), "\n".join(lines[:data_index] + [lines[data_index]] + lines[data_index:])):
            with self.assertRaises(diagnostic.DiagnosticError):
                diagnostic.parse_native(text, CELLS[0])

    def test_malformed_duplicate_or_wrong_metric_table_is_rejected(self):
        for text in (native().replace(WRITE_HEADER, STREAM_HEADER),
                     native() + WRITE_HEADER + "\n", native().replace("| 65.5 |", "| 65.5 | extra |", 1),
                     native().replace("| 8 | 64 KiB |", "| 1 | 1 KiB |", 1)):
            with self.subTest(text=text):
                with self.assertRaises(diagnostic.DiagnosticError):
                    diagnostic.parse_native(text, CELLS[0])

    def test_nonfinite_or_nonpositive_metrics_are_rejected(self):
        for original in ("1000", "65.5", "4.0", "20.0", "30.0"):
            for invalid in ("NaN", "inf", "0", "-1"):
                with self.subTest(original=original, invalid=invalid):
                    text = native().replace(f"| {original} |", f"| {invalid} |", 1)
                    with self.assertRaises(diagnostic.DiagnosticError):
                        diagnostic.parse_native(text, CELLS[0])


class ScheduleAndSummaryTests(unittest.TestCase):
    def runs(self):
        results = []
        for item in diagnostic.schedule():
            value = 200 if item["side"] == "head" else 100
            row = {"ops_s": value, "mb_s": value, "p50_us": 1, "p99_us": 2, "p999_us": 3}
            if item["cell"].startswith("stream_read/"):
                row["records_s"] = value * 100
            results.append(dict(item, rows=[dict(row) for _ in range(3)],
                                exit_code=0, timing_eligible=True, binary_side=item["side"]))
        return results

    def test_schedule_is_exact_complete_fixed_alternating_protocol(self):
        schedule = diagnostic.schedule()
        self.assertEqual(len(schedule), 36)
        self.assertEqual({row["cell"] for row in schedule}, set(CELLS))
        self.assertEqual(len({row["index"] for row in schedule}), 36)
        for cell in CELLS:
            subset = [row for row in schedule if row["cell"] == cell]
            self.assertEqual([(row["phase"], row["pair"], row["slot"], row["side"]) for row in subset],
                             [("aa", pair, slot, "base") for pair in range(3) for slot in range(2)] +
                             [("ab", pair, slot, ("base", "head")[slot if pair != 1 else 1 - slot])
                              for pair in range(3) for slot in range(2)])

    def test_summary_uses_head_over_base_even_when_order_reversed_and_never_accepts_gate(self):
        result = diagnostic.summarize(self.runs())
        self.assertIs(result["diagnostic_only"], True)
        for cell in CELLS:
            summary = result["cells"][cell]
            self.assertEqual(summary["aa_ratios"], [1, 1, 1])
            self.assertEqual(summary["ab_ratios"], [2, 2, 2])
            self.assertFalse(summary["aa_repeatability_warning"])
        for forbidden in ("passed", "accepted", "gate_passed", "ready_to_merge"):
            self.assertNotIn(forbidden, result)

    def test_aa_variation_warns_in_both_directions_without_becoming_a_gate(self):
        for multiplier in (0.80, 1.25):
            runs = self.runs()
            for run in runs:
                if run["cell"] == CELLS[0] and run["phase"] == "aa" and run["pair"] == 0 and run["slot"] == 1:
                    for row in run["rows"]:
                        row["ops_s"] = row["mb_s"] = 100 * multiplier
            self.assertTrue(diagnostic.summarize(runs)["cells"][CELLS[0]]["aa_repeatability_warning"])

    def test_trace_rows_are_excluded_from_untraced_pairs(self):
        runs = self.runs()
        trace = dict(runs[0], phase="trace", timing_eligible=False, rows=[{"ops_s": 999999}] * 3)
        expected = diagnostic.summarize(runs)
        self.assertEqual(diagnostic.summarize(runs + [trace])["cells"], expected["cells"])

    def test_incomplete_duplicate_or_modified_schedule_is_rejected(self):
        runs = self.runs()
        variants = [runs[:-1], runs + [copy.deepcopy(runs[0])]]
        changed = copy.deepcopy(runs)
        changed[0]["side"] = "head"
        variants.append(changed)
        changed = copy.deepcopy(runs)
        changed[0]["pair"] = 99
        variants.append(changed)
        for changed in variants:
            with self.subTest(variant=changed[0]):
                with self.assertRaises(diagnostic.DiagnosticError):
                    diagnostic.summarize(changed)

    def test_failed_wrong_binary_or_unqualified_native_rows_are_rejected(self):
        for field, value in (("exit_code", 1), ("binary_side", "head"), ("rows", []),
                             ("timing_eligible", False), ("rows", [{"ops_s": float("nan")}] * 3)):
            with self.subTest(field=field, value=value):
                runs = self.runs()
                runs[0][field] = value
                with self.assertRaises(diagnostic.DiagnosticError):
                    diagnostic.summarize(runs)

    def test_summary_revalidates_finite_positive_tail_and_throughput_metrics(self):
        for field in ("ops_s", "mb_s", "p50_us", "p99_us", "p999_us"):
            runs = self.runs()
            runs[0]["rows"] = copy.deepcopy(runs[0]["rows"])
            runs[0]["rows"][0][field] = -1
            with self.subTest(field=field):
                with self.assertRaises(diagnostic.DiagnosticError):
                    diagnostic.summarize(runs)


class EnvironmentTests(unittest.TestCase):
    def test_environment_is_fixed_and_does_not_inherit_profiler_or_native_overrides(self):
        with patch.dict(os.environ, {"LD_PRELOAD": "/tmp/evil.so", "RUSTFLAGS": "-C opt-level=0",
                                    "SPIKE_REPS": "1", "SPIKE_WARMUP_MS": "0", "TOKIO_WORKER_THREADS": "99",
                                    "PERF_RECORD_OPTS": "bad", "SECRET": "hidden"}):
            environment = diagnostic.fixed_environment("/tmp/isolated-native")
        self.assertEqual(environment["SPIKE_REPS"], "3")
        self.assertEqual(environment["SPIKE_WARMUP_MS"], "1000")
        self.assertEqual(environment["SPIKE_MEASURE_MS"], "3000")
        self.assertEqual(environment["TOKIO_WORKER_THREADS"], "4")
        self.assertEqual(environment["TMPDIR"], "/tmp/isolated-native")
        for forbidden in ("LD_PRELOAD", "RUSTFLAGS", "PERF_RECORD_OPTS", "SECRET"):
            self.assertNotIn(forbidden, environment)


class TraceBoundaryTests(unittest.TestCase):
    def trace(self):
        return "\n".join([
            "native 100/100 [000] 10.000000000: sched:sched_process_exec: filename=/tmp/native pid=100 old_pid=100",
            "native 100/100 [000] 10.010000000: sched:sched_process_fork: parent_comm=native parent_pid=100 child_comm=writer child_pid=101",
            "writer 100/101 [001] 10.020000000: syscalls:sys_enter_fdatasync: fd: 4",
            "writer 100/101 [001] 10.030000000: sched:sched_switch: prev_comm=writer prev_pid=101 prev_prio=120 prev_state=D ==> next_comm=idle next_pid=0 next_prio=120",
            "kworker 5/5 [001] 10.080000000: block:block_rq_complete: 8,0 WS () 100 + 8 [0]",
            "kworker 5/5 [001] 10.090000000: sched:sched_wakeup: comm=writer pid=101 prio=120 target_cpu=001",
            "idle 0/0 [001] 10.100000000: sched:sched_switch: prev_comm=idle prev_pid=0 prev_state=R ==> next_comm=writer next_pid=101 next_prio=120",
            "writer 100/101 [001] 10.110000000: syscalls:sys_exit_fdatasync: ret: 0",
            "writer 100/101 [001] 10.120000000: sched:sched_process_exit: comm=writer pid=101 prio=120",
        ])

    def test_blocked_thread_is_identified_without_any_cpu_sample(self):
        result = diagnostic.analyze_waits(self.trace(), '/tmp/native')
        self.assertEqual(result['native_pid'], 100)
        self.assertEqual(result['thread_ids'], [100, 101])
        self.assertAlmostEqual(result['syscalls']['fdatasync']['total_ms'], 90)
        self.assertAlmostEqual(result['offcpu_by_state']['D']['total_ms'], 70)
        self.assertAlmostEqual(result['wakeup_to_run']['total_ms'], 10)
        self.assertEqual(result['system_block_events']['block:block_rq_complete'], 1)
        self.assertIs(result['whole_process_only'], True)

    def test_unrelated_process_io_is_not_counted_as_benchmark_wait(self):
        text = self.trace() + '\nother 555/555 [000] 10.130000000: syscalls:sys_enter_fsync: fd: 3\nother 555/555 [000] 10.230000000: syscalls:sys_exit_fsync: ret: 0'
        result = diagnostic.analyze_waits(text, '/tmp/native')
        self.assertNotIn('fsync', result['syscalls'])

    def test_missing_or_duplicate_exact_native_exec_refuses_attribution(self):
        for text in (self.trace().replace('/tmp/native', '/tmp/other'), self.trace()+'\n'+self.trace().splitlines()[0]):
            with self.assertRaises(diagnostic.DiagnosticError):
                diagnostic.analyze_waits(text, '/tmp/native')

    def test_unparsed_loss_or_out_of_order_trace_refuses_attribution(self):
        for text in (self.trace()+'\nmalformed trace', self.trace()+'\nPERF_RECORD_LOST 100', self.trace().replace('10.110000000', '10.000000001')):
            with self.assertRaises(diagnostic.DiagnosticError):
                diagnostic.analyze_waits(text, '/tmp/native')

    def test_nested_recorders_preserve_allowlisted_env_and_no_sysctl(self):
        environment = diagnostic.fixed_environment('/tmp/runtime')
        environment['SPIKE_FILTER'] = CELLS[0]
        command = diagnostic.trace_command('/tmp/native', Path('/tmp/trace'), environment)
        self.assertEqual(command[:4], ['sudo', '-n', 'env', '-i'])
        for key, value in environment.items():
            self.assertIn(f'{key}={value}', command)
        for event in ('sched:sched_process_exec', 'sched:sched_process_fork', 'sched:sched_process_exit', 'block:block_rq_complete'):
            self.assertIn(event, command)
        self.assertEqual(command[-2:], ['--', '/tmp/native'])
        self.assertNotIn('sysctl', command)
        self.assertIn('--sample-cpu', command)
        self.assertIn('supervise', command)


class ContainmentTests(unittest.TestCase):
    def test_supervisor_kills_term_ignoring_descendant_even_after_parent_exits(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            child_code = "import os,signal; signal.signal(signal.SIGTERM,signal.SIG_IGN); print(os.getpid(),flush=True); signal.pause()"
            parent_code = "import pathlib,signal,subprocess,sys; p=subprocess.Popen([sys.executable,'-c',sys.argv[1]],stdout=subprocess.PIPE,text=True); pathlib.Path(sys.argv[2]).write_text(p.stdout.readline()); signal.pause()"
            pidfile = directory/'child.pid'
            groupfile = directory/'group.json'
            with patch.object(diagnostic.os, 'geteuid', return_value=0):
                with self.assertRaises(diagnostic.DiagnosticError):
                    diagnostic.supervise([sys.executable, '-c', parent_code, child_code, str(pidfile)], groupfile, timeout=2)
            self.assertTrue(pidfile.exists(), 'child must start and install TERM handler')
            witness = json.loads(groupfile.read_text())
            self.assertGreater(witness['pgid'], 1)
            child_pid = int(pidfile.read_text())
            state = subprocess.run(['ps', '-o', 'stat=', '-p', str(child_pid)], capture_output=True, text=True, check=False).stdout.strip()
            self.assertTrue(not state or state.startswith('Z'), f'live descendant survived: {state}')

    def test_witness_write_failure_cannot_leave_launched_child_running(self):
        children = []
        actual_popen = subprocess.Popen
        def spawn(*args, **kwargs):
            child = actual_popen(*args, **kwargs)
            children.append(child)
            return child
        try:
            with tempfile.TemporaryDirectory() as tmp:
                with patch.object(diagnostic.os, 'geteuid', return_value=0), patch.object(diagnostic, 'dump', side_effect=OSError('witness write refused')), patch.object(diagnostic.subprocess, 'Popen', side_effect=spawn):
                    with self.assertRaises(OSError):
                        diagnostic.supervise([sys.executable, '-c', 'import signal; signal.pause()'], Path(tmp)/'group.json', timeout=2)
                self.assertTrue(children)
                self.assertIsNotNone(children[0].poll(), 'witness failure leaked launched child')
        finally:
            for child in children:
                if child.poll() is None:
                    os.killpg(child.pid, diagnostic.signal.SIGKILL)
                    child.wait(timeout=5)

    def test_supervisor_records_own_root_child_group_before_command(self):
        with tempfile.TemporaryDirectory() as tmp:
            groupfile = Path(tmp)/'group.json'
            with patch.object(diagnostic.os, 'geteuid', return_value=0):
                self.assertEqual(diagnostic.supervise([sys.executable, '-c', 'pass'], groupfile, timeout=2), 0)
            witness = json.loads(groupfile.read_text())
            self.assertEqual(witness['pgid'], witness['child_pid'])
            self.assertNotEqual(witness['pgid'], os.getpgrp())


if __name__ == "__main__":
    unittest.main()
