#!/usr/bin/env python3
"""Synthetic protocol tests; these never run a benchmark or perf."""
import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import stream_read_profile as profile

SHA = "a" * 40
PID = 321


def fixture(identity):
    return {"schema": 1, "phase": "fixture", "cell": "stream_read/tail/1k",
            "id": identity, "pid": PID, "source_sha": SHA, "verified": True,
            "frames": 256, "batch": 1024, "records": 262144,
            "value_bytes": 268435456, "value_size": 1024, "key_size": 7,
            "compression": "none", "mode": "Fast", "synced": True,
            "retention": False, "segment_bytes": 268435456}


def markers():
    output = []
    for identity in range(1, 4):
        first = fixture(identity)
        output.append(first)
        for phase, delta in (("begin", 0), ("end", 3_000_000_000)):
            item = {k: first[k] for k in ("schema", "cell", "id", "pid", "source_sha")}
            item.update(phase=phase, mono_ns=identity * 5_000_000_000 + delta)
            output.append(item)
    return output


def stdout(items=None):
    return "ordinary bench header\n" + "\n".join(
        "# STREAM_PROFILE " + json.dumps(item) for item in (markers() if items is None else items))


def sample(at, leaf="hot", period=5_025_125, pid=PID, tid=322, chain=None):
    chain = [leaf, "caller"] if chain is None else chain
    stamp = f"{at // 1_000_000_000}.{at % 1_000_000_000:09d}"
    header = f"{pid} {tid} {stamp}: {period} cpu-clock:u: deadbeef {leaf} (binary)"
    return header + "\n" + "\n".join(f"\tdeadbeef {s} (binary)" for s in chain) + "\n\n"


def samples(count=100):
    return "".join(sample(identity * 5_000_000_000 + (i + 1) * 10_000_000)
                   for identity in range(1, 4) for i in range(count))


def retained_fixture(output):
    binary = output/"benchmark-profiled"; binary.write_bytes(SHA.encode())
    build = {"source_sha": SHA, "binary_sha256": profile.sha256(binary), "profile": "bench",
             "toolchain": "1.98.1", "rustflags": "-Cdebuginfo=1 -Cforce-frame-pointers=yes"}
    (output/"record.stdout").write_text(stdout())
    (output/"script.stdout").write_text(samples())
    (output/"record.stderr").write_text("")
    (output/"script.stderr").write_text("")
    (output/"dump.stdout").write_text("synthetic raw events, no loss/throttle\n")
    (output/"dump.stderr").write_text("")
    (output/"perf.data").write_bytes(b"synthetic test-only perf.data")
    profile.save_json(output/"record-command.json", profile.perf_record_command(binary, output))
    profile.save_json(output/"build-manifest.json", build)
    profile.save_json(output/"profile-settings.json", {k:v for k,v in profile.profile_env({}).items()})
    profile.save_json(output/"host-before.json", {"synthetic": True})
    profile.save_json(output/"host-after.json", {"synthetic": True})
    profile.save_json(output/"script-command.json", ["perf", "script", "-i", str(output/"perf.data"), "--ns",
                      "--show-lost-events", "-F", "pid,tid,time,event,period,ip,sym,dso"])
    profile.save_json(output/"dump-command.json", ["perf", "script", "-i", str(output/"perf.data"), "-D"])
    labels = ["record", "script", "dump"]
    for identity in range(1,4):
        for suffix in ("script", "report"):
            label = f"phase-{identity}-{suffix}"; labels.append(label)
            (output/f"{label}.stdout").write_text("synthetic phase artifact\n")
            (output/f"{label}.stderr").write_text("")
            time_range = f"{identity*5}.000000000,{identity*5+2}.999999999"
            command = (["perf", "script", "-i", str(output/"perf.data"), "--ns", "--show-lost-events", "-F",
                        "pid,tid,time,event,period,ip,sym,dso", "--time", time_range] if suffix == "script" else
                       ["perf", "report", "-i", str(output/"perf.data"), "--stdio", "--no-children", "--time", time_range])
            profile.save_json(output/f"{label}-command.json", command)
    for label in labels:
        profile.save_json(output/f"{label}-status.json", {"exit_code": 0})
    return {"schema": 1, "source_sha": SHA, "build": build, "complete_round": True,
            "parser_sha256": profile.sha256(profile.__file__), "execution_rounds": 1,
            "artifacts": {p.name: profile.sha256(p) for p in output.iterdir()}}


class QualificationTests(unittest.TestCase):
    def accept(self, script):
        try:
            return profile.qualify(stdout(), script, SHA)
        except profile.QualificationError as error:
            self.fail(f"valid source-shaped perf grammar refused: {error}")
    def test_upstream_dwarf_multiline_header_and_leaf_first_callchain(self):
        # builtin-script.c prints PID/TID, time, period, event, then a newline
        # when a DWARF cursor exists; evsel_fprintf.c prints the callchain below.
        script = "".join(
            f"    321/322     {identity*5}.{(i+1)*9_000_000:09d}:    5025125 cpu-clock:u:\n"
            "\t        deadbeef actual_leaf (binary)\n"
            "\t        01234567 caller (binary)\n\n"
            for identity in range(1,4) for i in range(100))
        result = self.accept(script)
        self.assertEqual(result["samples"], 300)
        self.assertEqual(result["exclusive"][0]["symbol"], "actual_leaf")
        self.assertEqual(result["exclusive"][0]["percent"], 100)

    def test_multiline_inlined_frame_has_no_fabricated_dso(self):
        script = "".join(
            f"321/322 {identity*5}.{(i+1)*9_000_000:09d}: 5025125 cpu-clock:u:\n"
            "\tdeadbeef inlined_leaf (inlined)\n\t01234567 caller (binary)\n\n"
            for identity in range(1,4) for i in range(100))
        result = self.accept(script)
        self.assertEqual(result["exclusive"][0]["symbol"], "inlined_leaf")
        self.assertIsNone(result["exclusive"][0]["dso"])

    def test_bare_dwarf_header_without_callchain_is_unqualified(self):
        self.reject(script=samples() + "321/322 5.050000000: 5025125 cpu-clock:u:\n\n")

    def reject(self, output=None, script=None, sha=SHA, diagnostics=""):
        with self.assertRaises(profile.QualificationError):
            profile.qualify(stdout() if output is None else output,
                            samples() if script is None else script, sha, diagnostics)

    def test_accepts_exact_three_verified_phases(self):
        self.assertTrue(profile.qualify(stdout(), samples(), SHA)["qualified"])

    def test_rejects_missing_markers(self):
        self.reject(output="bench output only")

    def test_rejects_missing_repetition(self):
        self.reject(output=stdout(markers()[:-3]))

    def test_rejects_false_fixture(self):
        items = markers(); items[0]["verified"] = False
        self.reject(output=stdout(items))

    def test_rejects_every_wrong_fixture_dimension(self):
        for key in ("frames", "batch", "records", "value_bytes", "value_size", "key_size",
                    "compression", "mode", "synced", "retention", "segment_bytes"):
            with self.subTest(key=key):
                items = markers(); items[0][key] = None
                self.reject(output=stdout(items))

    def test_rejects_missing_fixture_dimension(self):
        items = markers(); del items[0]["key_size"]
        self.reject(output=stdout(items))

    def test_rejects_wrong_cell(self):
        items = markers(); items[4]["cell"] = "stream_read/cold/64k"
        self.reject(output=stdout(items))

    def test_rejects_schema_as_bool(self):
        items = markers(); items[0]["schema"] = True
        self.reject(output=stdout(items))

    def test_rejects_wrong_phase_order(self):
        items = markers(); items[1], items[2] = items[2], items[1]
        self.reject(output=stdout(items))

    def test_rejects_duplicate_phase(self):
        items = markers(); items.insert(1, copy.deepcopy(items[1]))
        self.reject(output=stdout(items))

    def test_rejects_overlapping_intervals(self):
        items = markers(); items[4]["mono_ns"] = 7_000_000_000
        self.reject(output=stdout(items))

    def test_rejects_reversed_interval(self):
        items = markers(); items[2]["mono_ns"] = items[1]["mono_ns"]
        self.reject(output=stdout(items))

    def test_rejects_noninteger_or_negative_monotonic_time(self):
        for value in (True, 5.0, "5000000000", -1):
            with self.subTest(value=value):
                items = markers(); items[1]["mono_ns"] = value
                self.reject(output=stdout(items))

    def test_rejects_source_mismatch(self):
        self.reject(sha="b" * 40)

    def test_rejects_malformed_expected_source(self):
        self.reject(sha="branch-name")

    def test_rejects_inconsistent_process(self):
        items = markers(); items[4]["pid"] += 1
        self.reject(output=stdout(items))

    def test_rejects_malformed_marker_json(self):
        self.reject(output=stdout() + "\n# STREAM_PROFILE {broken")

    def test_rejects_duplicate_json_keys(self):
        self.reject(output=stdout().replace('"verified": true', '"verified": false, "verified": true', 1))

    def test_rejects_raw_lost_records(self):
        self.reject(script=samples() + "PERF_RECORD_LOST lost 1 events\n")

    def test_rejects_loss_unwind_or_throttle_diagnostics(self):
        for text in ("lost 1 chunks", "failed to unwind stack", "frequency throttled", "PERF_RECORD_THROTTLE"):
            with self.subTest(text=text):
                self.reject(diagnostics=text)

    def test_rejects_missing_samples(self):
        self.reject(script="")

    def test_rejects_insufficient_pooled_samples(self):
        self.reject(script=samples(99))

    def test_rejects_empty_phase_despite_pooled_count(self):
        self.reject(script="".join(sample(5_010_000_000 + i) for i in range(300)))

    def test_rejects_non_nanosecond_timestamp(self):
        self.reject(script=samples().replace("5.010000000:", "5.010000:"))

    def test_rejects_zero_or_malformed_period(self):
        for period in ("0", "-1", "NaN", "1.5"):
            with self.subTest(period=period):
                self.reject(script=samples().replace("5025125 cpu-clock", period + " cpu-clock", 1))

    def test_rejects_wrong_event(self):
        self.reject(script=samples().replace("cpu-clock:u:", "cycles:", 1))

    def test_rejects_foreign_process_inside_interval(self):
        self.reject(script=samples() + sample(5_050_000_000, pid=999))

    def test_rejects_unreadable_callchain(self):
        self.reject(script=samples() + sample(5_050_000_000, chain=[]))

    def test_rejects_unknown_leaf_over_ten_percent(self):
        script = "".join(sample(identity * 5_000_000_000 + (i+1)*10_000_000,
                                 leaf="[unknown]" if i < 11 else "hot")
                         for identity in range(1, 4) for i in range(100))
        self.reject(script=script)

    def test_rejects_hex_only_leaf_over_ten_percent(self):
        self.reject(script=samples().replace(" hot (binary)", " 0xdeadbeef (binary)"))

    def test_rejects_arbitrary_unparsed_script_line(self):
        self.reject(script=samples() + "silently corrupted report\n")

    def test_half_open_integer_bounds_exclude_setup_warmup_end_and_close(self):
        script = samples() + sample(4_999_999_999, leaf="setup") + sample(5_000_000_000, leaf="at_begin")
        script += sample(8_000_000_000, leaf="at_end") + sample(8_000_000_001, leaf="close")
        result = profile.qualify(stdout(), script, SHA)
        self.assertEqual(result["samples"], 301)
        self.assertEqual(result["phases"][0]["samples"], 101)
        self.assertEqual({s["symbol"] for s in result["exclusive"]}, {"hot", "at_begin"})

    def test_period_weighted_leaf_and_inclusive_recursion_deduplication(self):
        script = "".join(sample(identity*5_000_000_000 + (i+1)*10_000_000,
                                leaf="large" if i < 50 else "small", period=9 if i < 50 else 1,
                                chain=["recursive", "recursive", "caller"])
                         for identity in range(1, 4) for i in range(100))
        result = profile.qualify(stdout(), script, SHA)
        exclusive = {s["symbol"]: s for s in result["exclusive"]}
        inclusive = {s["symbol"]: s for s in result["inclusive"]}
        self.assertEqual(result["period"], 1500)
        self.assertEqual(exclusive["large"]["percent"], 90)
        self.assertEqual(exclusive["small"]["percent"], 10)
        self.assertEqual(inclusive["recursive"]["period"], 1500)
        self.assertEqual(inclusive["caller"]["percent"], 100)
        self.assertTrue(result["inclusive_overlaps"])

    def test_exact_ten_percent_unknown_boundary_and_period_fraction_are_separate(self):
        script = "".join(sample(identity*5_000_000_000 + (i+1)*10_000_000,
                                leaf="[unknown]" if i < 10 else "hot", period=9 if i < 10 else 1)
                         for identity in range(1, 4) for i in range(100))
        result = profile.qualify(stdout(), script, SHA)
        self.assertEqual(result["unknown_leaf_sample_percent"], 10)
        self.assertEqual(result["unknown_leaf_period_percent"], 50)

    def test_pooled_samples_do_not_claim_repetition_stability(self):
        script = "".join(sample(5_010_000_000+i) for i in range(298))
        script += sample(10_010_000_000, tid=323) + sample(15_010_000_000, tid=324)
        result = profile.qualify(stdout(), script, SHA)
        self.assertEqual([p["samples"] for p in result["phases"]], [298, 1, 1])
        self.assertEqual([p["threads"] for p in result["phases"]], [[322], [323], [324]])
        self.assertFalse(result["repetition_stability_qualified"])


class RunnerTests(unittest.TestCase):
    def test_retained_phase_command_time_input_and_leaf_report_must_match(self):
        for suffix, mutation in (("script", "time"), ("report", "time"), ("script", "input"),
                                 ("report", "input"), ("report", "no_children")):
            with self.subTest(suffix=suffix, mutation=mutation), tempfile.TemporaryDirectory() as tmp:
                output = Path(tmp); manifest = retained_fixture(output)
                path = output/f"phase-2-{suffix}-command.json"
                command = json.loads(path.read_text())
                if mutation == "time": command[-1] = "0.000000000,999.000000000"
                elif mutation == "input": command[3] = "/wrong/perf.data"
                else: command.remove("--no-children")
                profile.save_json(path, command); manifest["artifacts"][path.name] = profile.sha256(path)
                profile.save_json(output/"source-manifest.json", manifest)
                with self.assertRaises(profile.QualificationError):
                    profile.qualify_retained(output)
    def test_retained_raw_dump_rejects_throttle_hidden_by_normal_script(self):
        for event in ("PERF_RECORD_THROTTLE", "PERF_RECORD_UNTHROTTLE", "PERF_RECORD_LOST", "PERF_RECORD_LOST_SAMPLES"):
            with self.subTest(event=event), tempfile.TemporaryDirectory() as tmp:
                output = Path(tmp); manifest = retained_fixture(output)
                path = output/"dump.stdout"; path.write_text(event+"\n")
                manifest["artifacts"][path.name] = profile.sha256(path)
                profile.save_json(output/"source-manifest.json", manifest)
                with self.assertRaises(profile.QualificationError):
                    profile.qualify_retained(output)

    def test_retained_failure_manifest_cannot_be_requalified(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp); manifest = retained_fixture(output); manifest["qualified"] = False
            profile.save_json(output/"source-manifest.json", manifest)
            with self.assertRaises(profile.QualificationError):
                profile.qualify_retained(output)

    def test_retained_missing_or_failed_phase_artifacts_are_unqualified(self):
        for identity in range(1,4):
            for suffix in ("script", "report"):
                with self.subTest(identity=identity,suffix=suffix), tempfile.TemporaryDirectory() as tmp:
                    output = Path(tmp); manifest = retained_fixture(output)
                    path = output/f"phase-{identity}-{suffix}-status.json"
                    profile.save_json(path, {"exit_code": 4}); manifest["artifacts"][path.name] = profile.sha256(path)
                    profile.save_json(output/"source-manifest.json", manifest)
                    with self.assertRaises(profile.QualificationError):
                        profile.qualify_retained(output)
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp); manifest = retained_fixture(output)
            del manifest["artifacts"]["phase-3-report.stdout"]
            profile.save_json(output/"source-manifest.json", manifest)
            with self.assertRaises(profile.QualificationError):
                profile.qualify_retained(output)
    def test_environment_removes_ambient_spike_knobs_and_isolates_perf_config(self):
        base = {"SPIKE_STREAM_EXTRA": "modified-shape", "PERF_CONFIG": "/private/config", "PATH": "/usr/bin"}
        env = profile.profile_env(base)
        self.assertNotIn("SPIKE_STREAM_EXTRA", env)
        self.assertEqual(env["PERF_CONFIG"], "/dev/null")
        self.assertEqual(env["PATH"], "/usr/bin")
        self.assertEqual(base["PERF_CONFIG"], "/private/config")

    def test_manifest_rejects_later_flag_override(self):
        with tempfile.TemporaryDirectory() as tmp:
            binary = Path(tmp)/"binary"; binary.write_bytes(SHA.encode())
            manifest = {"source_sha": SHA, "binary_sha256": profile.sha256(binary), "profile": "bench",
                        "toolchain": "1.98.1", "rustflags": "-Cdebuginfo=1 -Cforce-frame-pointers=yes -Cdebuginfo=0"}
            with self.assertRaises(profile.QualificationError):
                profile.validate_build_manifest(manifest, SHA, binary)

    def test_retained_qualification_rejects_changed_sampling_command_even_with_new_hash(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp); manifest = retained_fixture(output)
            path = output/"record-command.json"
            command = json.loads(path.read_text()); command[command.index("-F")+1] = "99"
            profile.save_json(path, command); manifest["artifacts"][path.name] = profile.sha256(path)
            profile.save_json(output/"source-manifest.json", manifest)
            with self.assertRaises(profile.QualificationError):
                profile.qualify_retained(output)

    def test_retained_qualification_rejects_stale_parser_hash(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp); manifest = retained_fixture(output); manifest["parser_sha256"] = "0"*64
            profile.save_json(output/"source-manifest.json", manifest)
            with self.assertRaises(profile.QualificationError):
                profile.qualify_retained(output)

    def test_retained_qualification_requires_hashes_for_consumed_artifacts(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp)
            binary = output/"benchmark-profiled"; binary.write_bytes(SHA.encode())
            build = {"source_sha": SHA, "binary_sha256": profile.sha256(binary), "profile": "bench",
                     "toolchain": "1.98.1", "rustflags": "-Cdebuginfo=1 -Cforce-frame-pointers=yes"}
            (output/"record.stdout").write_text(stdout())
            (output/"script.stdout").write_text(samples())
            profile.save_json(output/"source-manifest.json", {"source_sha": SHA, "build": build, "artifacts": {}})
            with self.assertRaises(profile.QualificationError):
                profile.qualify_retained(output)

    def test_fixed_command_and_benchmark_settings(self):
        self.assertEqual(profile.perf_record_command("/binary", "/out"),
                         ["perf", "record", "-o", "/out/perf.data", "-e", "cpu-clock:u", "-F", "199",
                          "--strict-freq", "--period", "--clockid", "mono", "--call-graph", "dwarf,16384", "--", "/binary"])
        base = {"SPIKE_FILTER": "wrong", "SPIKE_REPS": "1", "PRIVATE_TOKEN": "not-logged"}
        env = profile.profile_env(base)
        self.assertEqual(env["SPIKE_FILTER"], "stream_read/tail/1k")
        self.assertEqual(env["SPIKE_REPS"], "3")
        self.assertEqual(env["SPIKE_WARMUP_MS"], "1000")
        self.assertEqual(env["SPIKE_MEASURE_MS"], "3000")
        self.assertEqual(env["SPIKE_STREAM_PROFILE"], "1")
        self.assertEqual(base["SPIKE_REPS"], "1")

    def test_nonlinux_preflight_never_invokes_a_tool(self):
        with mock.patch.object(profile.platform, "system", return_value="Darwin"), \
             mock.patch.object(profile.subprocess, "run") as run:
            with self.assertRaises(profile.QualificationError):
                profile.preflight()
            run.assert_not_called()

    def test_manifest_checks_source_hash_flags_toolchain_and_embedded_marker(self):
        with tempfile.TemporaryDirectory() as tmp:
            binary = Path(tmp)/"binary"
            binary.write_bytes(b"synthetic source marker " + SHA.encode())
            manifest = {"source_sha": SHA, "binary_sha256": profile.sha256(binary), "profile": "bench",
                        "toolchain": "1.98.1", "rustflags": "-C debuginfo=1 -C force-frame-pointers=yes"}
            profile.validate_build_manifest(manifest, SHA, binary)
            for field, value in (("source_sha", "b"*40), ("binary_sha256", "0"*64), ("binary_sha256", "bad"),
                                 ("rustflags", ""), ("profile", "debug"), ("toolchain", "nightly")):
                with self.subTest(field=field, value=value):
                    wrong = {**manifest, field: value}
                    with self.assertRaises(profile.QualificationError):
                        profile.validate_build_manifest(wrong, SHA, binary)
            binary.write_bytes(b"different binary")
            manifest["binary_sha256"] = profile.sha256(binary)
            with self.assertRaises(profile.QualificationError):
                profile.validate_build_manifest(manifest, SHA, binary)

    def test_failed_command_retains_stdout_stderr_and_does_not_retry(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp)
            with self.assertRaises(profile.QualificationError):
                profile.logged_command([__import__('sys').executable, "-c",
                                        "import sys; print('raw output'); print('error', file=sys.stderr); sys.exit(4)"],
                                       output, "failure")
            self.assertEqual((output/"failure.stdout").read_text(), "raw output\n")
            self.assertEqual((output/"failure.stderr").read_text(), "error\n")
            self.assertTrue((output/"failure-command.json").is_file())

    def test_run_refuses_existing_output_before_host_or_tool_work(self):
        with tempfile.TemporaryDirectory() as tmp, mock.patch.object(profile, "preflight") as before:
            with self.assertRaises(profile.QualificationError):
                profile.run_profile(Path("/binary"), Path(tmp), Path("/manifest"))
            before.assert_not_called()

    def test_retained_qualification_detects_changed_raw_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp)
            manifest = retained_fixture(output)
            profile.save_json(output/"source-manifest.json", manifest)
            self.assertTrue(profile.qualify_retained(output)["qualified"])
            (output/"record.stdout").write_text(stdout().replace('"verified": true', '"verified": false', 1))
            with self.assertRaises(profile.QualificationError):
                profile.qualify_retained(output)


if __name__ == "__main__":
    unittest.main()
