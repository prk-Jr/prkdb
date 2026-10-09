#!/usr/bin/env python3
"""Synthetic protocol tests; these never run a benchmark or perf."""
import copy
import io
import sys
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

class RecoveryTests(unittest.TestCase):
    def original(self, output):
        manifest = retained_fixture(output)
        text = samples().replace('hot (binary)', '[unknown] (binary)')
        (output/'script.stdout').write_text(text)
        profile.save_json(output/'qualification.json', {'qualified': False, 'reason': 'more than 10 percent unknown leaf samples', 'retry_permitted': False, 'timing_claim': False})
        manifest.update(qualified=False, complete_round=False)
        manifest['artifacts'] = {p.name: profile.sha256(p) for p in output.iterdir()}
        profile.save_json(output/'source-manifest.json', manifest)
        return mock.patch.multiple(profile, ORIGINAL_MANIFEST_SHA=profile.sha256(output/'source-manifest.json'),
              ORIGINAL_PERF_SHA=profile.sha256(output/'perf.data'), ORIGINAL_SOURCE=SHA,
              ORIGINAL_PARSER=profile.sha256(profile.__file__), ORIGINAL_ARTIFACT_COUNT=len(manifest['artifacts']),
              ORIGINAL_FULL_SAMPLES=300, ORIGINAL_PHASE_SAMPLES=(100,100,100), ORIGINAL_UNKNOWN=300)

    def test_parser_preserves_callchain_ips_and_dsos(self):
        parsed = profile.parse_samples(sample(5_100_000_000))[0]
        self.assertEqual(parsed.get('chain_ips'), ['deadbeef', 'deadbeef'])
        self.assertEqual(parsed.get('chain_dsos'), ['binary', 'binary'])

    def test_symbol_only_changes_preserve_ordered_sample_identity(self):
        before = profile.parse_samples(samples())
        after = copy.deepcopy(before)
        after[0]['leaf'] = ('renamed', 'binary')
        after[0]['chain'][0] = ('renamed', 'binary')
        profile.compare_recovery_samples(before, after, {'binary'})

    def test_recovery_rejects_cohort_and_chain_changes(self):
        before = profile.parse_samples(samples())
        for field in ('pid', 'tid', 'mono_ns', 'period', 'event', 'ip', 'chain_ips', 'chain_dsos'):
            with self.subTest(field=field):
                after = copy.deepcopy(before)
                value = after[0].get(field)
                after[0][field] = value + 1 if isinstance(value, int) else ['changed'] if isinstance(value,list) else 'changed'
                with self.assertRaises(profile.QualificationError): profile.compare_recovery_samples(before,after,{'binary'})
        for change in ('drop','reorder','extra'):
            with self.subTest(change=change):
                after = copy.deepcopy(before)
                if change=='drop': after.pop()
                elif change=='reorder': after[0],after[1]=after[1],after[0]
                else: after.append(copy.deepcopy(after[0]))
                with self.assertRaises(profile.QualificationError): profile.compare_recovery_samples(before,after,{'binary'})

    def test_recovery_rejects_vdso_and_unapproved_resolution(self):
        for dso in ('[vdso]', '/usr/lib/other.so'):
            before=profile.parse_samples(sample(5_100_000_000,leaf='[unknown]').replace('(binary)',f'({dso})'))
            after=copy.deepcopy(before);after[0]['leaf']=('resolved',dso);after[0]['chain'][0]=('resolved',dso)
            with self.subTest(dso=dso),self.assertRaises(profile.QualificationError):
                profile.compare_recovery_samples(before,after,{'binary'})

    def test_pinned_unknown_only_failure_is_recoverable_but_normal_qualify_still_refuses(self):
        with tempfile.TemporaryDirectory() as directory:
            output=Path(directory)
            with self.original(output):
                with self.assertRaises(profile.QualificationError): profile.qualify_retained(output)
                value=profile.validate_recovery_original(output)
                self.assertEqual(value.get('source_sha'),SHA)

    def test_recovery_original_rejects_unpinned_or_incomplete_lossy_failure(self):
        for change in ('pin','source','parser','missing','status','loss','fixture','reason','not_unknown'):
            with self.subTest(change=change),tempfile.TemporaryDirectory() as directory:
                output=Path(directory)
                with self.original(output):
                    manifest=json.loads((output/'source-manifest.json').read_text())
                    if change=='pin': (output/'perf.data').write_bytes(b'wrong')
                    elif change in ('source','parser'): manifest['source_sha' if change=='source' else 'parser_sha256']='b'*40
                    elif change=='missing': (output/'phase-1-report.stdout').unlink()
                    elif change=='status': profile.save_json(output/'phase-2-script-status.json',{'exit_code':1})
                    elif change=='loss': (output/'dump.stdout').write_text('PERF_RECORD_LOST')
                    elif change=='fixture': (output/'record.stdout').write_text(stdout().replace('"verified": true','"verified": false',1))
                    elif change=='reason': profile.save_json(output/'qualification.json',{'qualified':False,'reason':'other'})
                    else: (output/'script.stdout').write_text(samples())
                    if change!='pin':
                        manifest['artifacts']={name:profile.sha256(output/name) for name in manifest['artifacts'] if (output/name).exists()}
                        profile.save_json(output/'source-manifest.json',manifest)
                    with mock.patch.object(profile,'ORIGINAL_MANIFEST_SHA',profile.sha256(output/'source-manifest.json')):
                        with self.assertRaises(profile.QualificationError): profile.validate_recovery_original(output)

    def test_elf_build_id_and_file_hash_are_both_required(self):
        with tempfile.TemporaryDirectory() as directory:
            output=Path(directory);binary=output/'elf';binary.write_bytes(b'test')
            def command(argv,dest,label,env=None):
                profile.save_json(dest/f'{label}-status.json',{'exit_code':0})
                (dest/f'{label}.stdout').write_text('Build ID: '+ 'a'*40+'\n')
                (dest/f'{label}.stderr').write_text('')
            with mock.patch.object(profile,'logged_command',side_effect=command):
                for expected_hash,expected_id in (('wrong','a'*40),(profile.sha256(binary),'b'*40)):
                    with self.subTest(expected_id=expected_id),self.assertRaises(profile.QualificationError):
                        profile.verify_elf(binary,expected_hash,expected_id,output,'elf-id')

    def test_recovery_command_is_offline_and_isolates_symbols(self):
        command=profile.recovery_command(['perf','script','-i','old','--ns'],Path('/derived'))
        self.assertEqual(command,['perf','--buildid-dir','/derived/symfs/.debug','script','-i','/derived/perf.data','--ns','--symfs','/derived/symfs','--no-inline'])
        env=profile.recovery_env({'DEBUGINFOD_URLS':'https://ambient','PERF_CONFIG':'ambient'})
        self.assertEqual(env['DEBUGINFOD_URLS'],'');self.assertEqual(env['PERF_CONFIG'],'/dev/null')
        self.assertNotIn('record',command);self.assertNotIn('stat',command)

    def test_recover_refuses_existing_output_before_tools(self):
        with tempfile.TemporaryDirectory() as directory, mock.patch.object(profile,'capture') as capture:
            with self.assertRaises(profile.QualificationError):profile.run_recovery(Path('/original'),Path(directory))
            capture.assert_not_called()

    def test_derived_quality_threshold_remains_unchanged(self):
        before=profile.parse_samples(samples().replace('hot (binary)','[unknown] (binary)'))
        after=copy.deepcopy(before)
        profile.compare_recovery_samples(before,after,{'binary'})
        with self.assertRaisesRegex(profile.QualificationError,'10 percent'):profile.qualify(stdout(),samples().replace('hot (binary)','[unknown] (binary)'),SHA)


    def test_recover_cli_routes_only_to_offline_recovery(self):
        with mock.patch.object(sys,'argv',['profile','recover','--original','/original','--output','/derived']),mock.patch.object(sys,'stdout',new_callable=io.StringIO),mock.patch.object(profile,'run_recovery',return_value={'qualified':True}) as run:
            try: result=profile.main()
            except SystemExit as error: result=error.code
            self.assertEqual(result,0)
            run.assert_called_once_with(Path('/original'),Path('/derived'))

    def test_download_preserves_headers_and_rejects_wrong_hash_and_redirect(self):
        url='https://security.ubuntu.com/fixed.deb'
        for change in ('hash','redirect'):
            with self.subTest(change=change),tempfile.TemporaryDirectory() as directory:
                output=Path(directory);response=io.BytesIO(b'package')
                response.status=200;response.headers={'Content-Type':'application/octet-stream'}
                response.geturl=lambda: url if change=='hash' else 'https://other/mirror'
                with mock.patch.object(profile.urllib.request,'urlopen',return_value=response):
                    with self.assertRaises(profile.QualificationError):profile.download_package('libc',url,'wrong',output)
                metadata=json.loads((output/'libc-download.json').read_text())
                self.assertEqual(metadata['status'],200);self.assertTrue(metadata['headers'])

    def run_fixture(self, base, outcome='success'):
        original=base/'original';original.mkdir();output=base/'derived'
        pins=self.original(original)
        identity={'source_sha':'b'*40,'source_tree':'c'*40,'parser_sha256':profile.sha256(profile.__file__)}
        calls=[]
        def command(argv,dest,label,env=None):
            calls.append((label,argv,env))
            profile.save_json(dest/f'{label}-command.json',argv)
            failed=outcome=='failed_command' and label=='phase-2-script'
            profile.save_json(dest/f'{label}-status.json',{'exit_code':1 if failed else 0})
            (dest/f'{label}.stderr').write_text('')
            value='perf version 6.17.13\n' if label=='perf-version' else 'synthetic tool version\n'
            if label=='script': value=samples() if outcome!='unknown' else samples().replace('hot (binary)','[unknown] (binary)')
            if label.startswith('phase-') and label.endswith('-script'):
                identity_number=int(label.split('-')[1]);value=''.join(sample(identity_number*5_000_000_000+(i+1)*10_000_000) for i in range(100))
            (dest/f'{label}.stdout').write_text(value)
            if failed:raise profile.QualificationError('phase-2-script failed (1); raw files retained')
        return original,output,pins,identity,calls,command

    def test_complete_mocked_recovery_preserves_original_and_retains_derivation(self):
        with tempfile.TemporaryDirectory() as directory:
            original,output,pins,identity,calls,command=self.run_fixture(Path(directory))
            snapshot={p.name:profile.sha256(p) for p in original.iterdir()}
            with pins,mock.patch.object(profile.platform,'system',return_value='Linux'),mock.patch.object(profile.shutil,'which',return_value='/existing/tool'),mock.patch.object(profile,'recovery_analysis_identity',return_value=identity),mock.patch.object(profile,'prepare_recovery_symbols',side_effect=self.mock_symbols),mock.patch.object(profile,'prepare_recovery_vdso',side_effect=self.mock_vdso),mock.patch.object(profile,'logged_command',side_effect=command),mock.patch.object(profile,'BENCHMARK_DSO','binary'),mock.patch.object(profile,'preflight') as preflight:
                result=profile.run_recovery(original,output)
            self.assertTrue(result['qualified']);preflight.assert_not_called()
            self.assertEqual(snapshot,{p.name:profile.sha256(p) for p in original.iterdir()})
            manifest=json.loads((output/'derived-manifest.json').read_text())
            self.assertTrue(manifest['qualified']);self.assertEqual(manifest['recording_attempts'],0)
            self.assertTrue((output/'original-source-manifest.json').is_file())
            self.assertEqual((output/'original-source-manifest.json').read_bytes(),(original/'source-manifest.json').read_bytes())
            self.assertEqual((output/'original-qualification.json').read_bytes(),(original/'qualification.json').read_bytes())
            self.assertTrue(manifest['original_verified_after']);self.assertTrue(manifest['analysis_verified_after'])
            self.assertEqual(len([label for label,_,_ in calls if label=='script']),1)
            self.assertTrue(all(env['DEBUGINFOD_URLS']=='' for _,_,env in calls))
            for name,digest in manifest['artifacts'].items():self.assertEqual(profile.sha256(output/name),digest)

    def test_failed_offline_command_retains_failure_and_never_retries(self):
        with tempfile.TemporaryDirectory() as directory:
            original,output,pins,identity,calls,command=self.run_fixture(Path(directory),'failed_command')
            with pins,mock.patch.object(profile.platform,'system',return_value='Linux'),mock.patch.object(profile.shutil,'which',return_value='/existing/tool'),mock.patch.object(profile,'recovery_analysis_identity',return_value=identity),mock.patch.object(profile,'prepare_recovery_symbols',side_effect=self.mock_symbols),mock.patch.object(profile,'prepare_recovery_vdso',side_effect=self.mock_vdso),mock.patch.object(profile,'logged_command',side_effect=command):
                with self.assertRaisesRegex(profile.QualificationError,'phase-2-script failed'):profile.run_recovery(original,output)
            manifest=json.loads((output/'derived-manifest.json').read_text())
            self.assertFalse(manifest['qualified']);self.assertTrue(manifest['original_verified_after'])
            self.assertIn('phase-2-script-status.json',manifest['artifacts'])
            self.assertEqual(len([label for label,_,_ in calls if label=='phase-2-script']),1)
            self.assertFalse(json.loads((output/'qualification.json').read_text())['qualified'])

    def test_analysis_change_in_final_recheck_cannot_leave_success_report(self):
        with tempfile.TemporaryDirectory() as directory:
            original,output,pins,identity,calls,command=self.run_fixture(Path(directory))
            with pins,mock.patch.object(profile.platform,'system',return_value='Linux'),mock.patch.object(profile.shutil,'which',return_value='/existing/tool'),mock.patch.object(profile,'recovery_analysis_identity',side_effect=[identity,identity,{**identity,'source_sha':'d'*40}]),mock.patch.object(profile,'prepare_recovery_symbols',side_effect=self.mock_symbols),mock.patch.object(profile,'prepare_recovery_vdso',side_effect=self.mock_vdso),mock.patch.object(profile,'logged_command',side_effect=command),mock.patch.object(profile,'BENCHMARK_DSO','binary'):
                with self.assertRaises(profile.QualificationError):profile.run_recovery(original,output)
            self.assertFalse(json.loads((output/'derived-manifest.json').read_text())['qualified'])
            self.assertFalse(json.loads((output/'qualification.json').read_text())['qualified'])


    def mock_symbols(self, original, output, manifest):
        symbols={}
        for label in ('libc','debug','benchmark'):
            path=output/'symfs'/label;path.parent.mkdir(exist_ok=True);path.write_bytes(label.encode())
            symbols[label]={'path':str(path),'sha256':profile.sha256(path),'build_id':'a'*40}
        self.current_symbols=symbols
        return symbols

    def test_actual_used_symfs_files_are_elf_verified_and_recorded(self):
        with tempfile.TemporaryDirectory() as directory:
            output=Path(directory);original=output/'original';original.mkdir();(original/'benchmark-profiled').write_bytes(b'benchmark')
            used=[]
            def download(label,url,digest,dest):
                package=dest/(label+'.deb');package.write_bytes(b'package');return package
            def extract(argv,dest,label,env=None):
                root=Path(argv[-1]);relative=Path(profile.LIBC_DSO.lstrip('/')) if label=='libc6-extract' else profile.DEBUG_PATH
                path=root/relative;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(label.encode())
            def verify(path,digest,build_id,dest,label):
                used.append(path);return {'path':str(path),'sha256':profile.sha256(path),'build_id':build_id}
            with mock.patch.object(profile,'download_package',side_effect=download),mock.patch.object(profile,'logged_command',side_effect=extract),mock.patch.object(profile,'verify_elf',side_effect=verify):
                symbols=profile.prepare_recovery_symbols(original,output,{'build':{'binary_sha256':profile.sha256(original/'benchmark-profiled')}})
            for label,relative in (('libc',Path(profile.LIBC_DSO.lstrip('/'))),('debug',profile.DEBUG_PATH),('benchmark',Path(profile.BENCHMARK_DSO.lstrip('/')))):
                path=output/'symfs'/relative
                self.assertIn(path,used);self.assertEqual(symbols[label]['path'],str(path))

    def test_mutated_actual_symbol_copies_cannot_qualify(self):
        for label in ('libc','debug','benchmark','vdso'):
            with self.subTest(label=label),tempfile.TemporaryDirectory() as directory:
                original,output,pins,identity,calls,command=self.run_fixture(Path(directory))
                def mutate(argv,dest,command_label,env=None):
                    command(argv,dest,command_label,env)
                    if command_label=='script':Path(self.current_symbols[label]['path']).write_bytes(b'changed')
                with pins,mock.patch.object(profile.platform,'system',return_value='Linux'),mock.patch.object(profile.shutil,'which',return_value='/existing/tool'),mock.patch.object(profile,'recovery_analysis_identity',return_value=identity),mock.patch.object(profile,'prepare_recovery_symbols',side_effect=self.mock_symbols),mock.patch.object(profile,'prepare_recovery_vdso',side_effect=self.mock_vdso),mock.patch.object(profile,'logged_command',side_effect=mutate),mock.patch.object(profile,'BENCHMARK_DSO','binary'):
                    with self.assertRaises(profile.QualificationError):profile.run_recovery(original,output)
                self.assertFalse(json.loads((output/'qualification.json').read_text())['qualified'])
                self.assertFalse(json.loads((output/'derived-manifest.json').read_text())['qualified'])


    def test_success_report_write_failure_cannot_leave_qualified_manifest(self):
        with tempfile.TemporaryDirectory() as directory:
            original,output,pins,identity,calls,command=self.run_fixture(Path(directory))
            actual_save=profile.save_json;failed=False
            def save(path,value):
                nonlocal failed
                if path.name=='qualification.json' and value.get('qualified') is True and not failed:
                    failed=True;raise OSError('injected success report write failure')
                actual_save(path,value)
            with pins,mock.patch.object(profile.platform,'system',return_value='Linux'),mock.patch.object(profile.shutil,'which',return_value='/existing/tool'),mock.patch.object(profile,'recovery_analysis_identity',return_value=identity),mock.patch.object(profile,'prepare_recovery_symbols',side_effect=self.mock_symbols),mock.patch.object(profile,'prepare_recovery_vdso',side_effect=self.mock_vdso),mock.patch.object(profile,'logged_command',side_effect=command),mock.patch.object(profile,'BENCHMARK_DSO','binary'),mock.patch.object(profile,'save_json',side_effect=save):
                with self.assertRaisesRegex(OSError,'success report write failure'):profile.run_recovery(original,output)
            self.assertFalse(json.loads((output/'qualification.json').read_text())['qualified'])
            self.assertFalse(json.loads((output/'derived-manifest.json').read_text())['qualified'])

    def test_recover_cli_prints_failure_reason_and_returns_two(self):
        with mock.patch.object(sys,'argv',['profile','recover','--original','/original','--output','/derived']),mock.patch.object(sys,'stderr',new_callable=io.StringIO) as stderr,mock.patch.object(profile,'run_recovery',side_effect=profile.QualificationError('retained reason')):
            self.assertEqual(profile.main(),2)
            self.assertEqual(json.loads(stderr.getvalue())['reason'],'retained reason')


    def test_mutated_used_recording_cannot_qualify_even_if_samples_unchanged(self):
        with tempfile.TemporaryDirectory() as directory:
            original,output,pins,identity,calls,command=self.run_fixture(Path(directory))
            def mutate(argv,dest,label,env=None):
                command(argv,dest,label,env)
                if label=='script':(dest/'perf.data').write_bytes(b'changed recording metadata')
            with pins,mock.patch.object(profile.platform,'system',return_value='Linux'),mock.patch.object(profile.shutil,'which',return_value='/existing/tool'),mock.patch.object(profile,'recovery_analysis_identity',return_value=identity),mock.patch.object(profile,'prepare_recovery_symbols',side_effect=self.mock_symbols),mock.patch.object(profile,'prepare_recovery_vdso',side_effect=self.mock_vdso),mock.patch.object(profile,'logged_command',side_effect=mutate),mock.patch.object(profile,'BENCHMARK_DSO','binary'):
                with self.assertRaises(profile.QualificationError):profile.run_recovery(original,output)
            self.assertFalse(json.loads((output/'qualification.json').read_text())['qualified'])
            self.assertFalse(json.loads((output/'derived-manifest.json').read_text())['qualified'])


    def test_recorded_vdso_id_is_exact_and_unique(self):
        self.assertEqual(profile.recorded_vdso_id(profile.VDSO_ID+' [vdso]\n'),profile.VDSO_ID)
        for text in ('','a'*40+' [vdso]\n',(profile.VDSO_ID+' [vdso]\n')*2):
            with self.subTest(text=text),self.assertRaises(profile.QualificationError):profile.recorded_vdso_id(text)

    def vdso_fixture(self, output, candidate_id=None, outside=False):
        candidate=output/'vdso-candidate.elf';candidate.write_bytes(b'ELF-vdso')
        expected=profile.VDSO_ID if candidate_id is None else candidate_id
        def command(argv,dest,label,env=None):
            value=''
            if label=='vdso-buildids':value=profile.VDSO_ID+' [vdso]\n'
            elif label=='vdso-cache-add':
                cache=dest/'symfs'/'.debug';target=cache/'objects'/profile.VDSO_ID
                if outside:target=dest/'outside-cache'
                target.mkdir(parents=True,exist_ok=True);(target/'elf').write_bytes(candidate.read_bytes())
                link=cache/'.build-id'/profile.VDSO_ID[:2]/profile.VDSO_ID[2:]
                link.parent.mkdir(parents=True,exist_ok=True);link.symlink_to(target)
            elif label.endswith('-elf'):value='Build ID: '+expected+'\n'
            profile.save_json(dest/f'{label}-command.json',argv);profile.save_json(dest/f'{label}-status.json',{'exit_code':0})
            (dest/f'{label}.stdout').write_text(value);(dest/f'{label}.stderr').write_text('')
        return candidate,command

    def test_exact_vdso_candidate_and_actual_cache_copies_are_verified(self):
        with tempfile.TemporaryDirectory() as directory:
            output=Path(directory);candidate,command=self.vdso_fixture(output)
            with mock.patch.object(profile,'capture_self_vdso',return_value=candidate),mock.patch.object(profile,'logged_command',side_effect=command):
                metadata=profile.prepare_recovery_vdso(output)
            self.assertEqual(metadata.get('build_id'),profile.VDSO_ID)
            self.assertTrue(Path(metadata['path']).is_file())
            self.assertEqual(Path(metadata['path']).name,'vdso')
            for name in ('path','candidate_path','cache_elf_path'):
                self.assertEqual(profile.sha256(Path(metadata[name])),metadata['sha256'])
            argv=json.loads((output/'vdso-cache-add-command.json').read_text())
            self.assertEqual(argv[:4],['perf','--buildid-dir',str(output/'symfs'/'.debug'),'buildid-cache'])

    def test_vdso_candidate_mismatch_or_cache_escape_is_refused(self):
        for wrong,escape in (('a'*40,False),(None,True)):
            with self.subTest(wrong=wrong,escape=escape),tempfile.TemporaryDirectory() as directory:
                output=Path(directory);candidate,command=self.vdso_fixture(output,wrong,escape)
                with mock.patch.object(profile,'capture_self_vdso',return_value=candidate),mock.patch.object(profile,'logged_command',side_effect=command):
                    with self.assertRaises(profile.QualificationError):profile.prepare_recovery_vdso(output)

    def test_vdso_resolution_count_uses_only_original_measured_vdso_leaves(self):
        before=profile.parse_samples(sample(5_100_000_000,leaf='[unknown]').replace('(binary)','([vdso])')+sample(5_200_000_000,leaf='[unknown]').replace('(binary)','(libc)')+sample(1_000_000_000,leaf='[unknown]').replace('(binary)','([vdso])'))
        after=copy.deepcopy(before)
        for item in after:item['leaf']=('clock_gettime',item['leaf'][1])
        self.assertEqual(profile.resolved_vdso_leaves(before,after,profile.parse_intervals(stdout(),SHA)),1)


    def mock_vdso(self, output):
        link=output/'symfs'/'.debug'/'.build-id'/profile.VDSO_ID[:2]/profile.VDSO_ID[2:]
        link.mkdir(parents=True,exist_ok=True)
        for name in ('elf','vdso'):(link/name).write_bytes(b'vdso')
        candidate=output/'vdso-candidate.elf';candidate.write_bytes(b'vdso')
        metadata={'path':str(link/'vdso'),'sha256':profile.sha256(link/'vdso'),'build_id':profile.VDSO_ID,
                  'candidate_path':str(candidate),'cache_elf_path':str(link/'elf'),'resolved_path':str(link/'vdso'),
                  'cache_add_elf_path':str(link/'elf'),'cache_link':str(link),'cache_root':str(output/'symfs'/'.debug'),
                  'cache_target':str(link.resolve())}
        self.current_symbols['vdso']=metadata
        return metadata


    def test_capture_reads_only_own_vdso_mapping(self):
        data=b'\x7fELF'+bytes(60)
        with tempfile.TemporaryDirectory() as directory,mock.patch.object(profile.Path,'read_text',return_value='1000-1040 r-xp 00000000 00:00 0 [vdso]\n'),mock.patch.object(profile.os,'open',return_value=77) as opened,mock.patch.object(profile.os,'pread',return_value=data) as read,mock.patch.object(profile.os,'close') as close:
            output=Path(directory);candidate=profile.capture_self_vdso(output)
            self.assertEqual(candidate.read_bytes(),data)
            opened.assert_called_once_with('/proc/self/mem',profile.os.O_RDONLY)
            read.assert_called_once_with(77,64,0x1000);close.assert_called_once_with(77)

    def test_vdso_lookup_topology_and_all_supplied_files_are_rechecked(self):
        for change in ('retarget','unexpected','candidate','cache_elf'):
            with self.subTest(change=change),tempfile.TemporaryDirectory() as directory:
                output=Path(directory);symbols=self.mock_symbols(Path('/original'),output,{})
                symbols['vdso']=self.mock_vdso(output);metadata=symbols['vdso']
                if change=='retarget':
                    link=Path(metadata['cache_link']);replacement=output/'replacement';link.rename(replacement);link.symlink_to(replacement)
                elif change=='unexpected':(Path(metadata['cache_link'])/'debug').write_bytes(b'unapproved')
                else:Path(metadata['candidate_path' if change=='candidate' else 'cache_elf_path']).write_bytes(b'changed')
                with self.assertRaises(profile.QualificationError):profile.recheck_recovery_symbols(symbols)


    def test_vdso_preparation_failure_remains_primary_reason(self):
        with tempfile.TemporaryDirectory() as directory:
            original,output,pins,identity,calls,command=self.run_fixture(Path(directory))
            with pins,mock.patch.object(profile.platform,'system',return_value='Linux'),mock.patch.object(profile.shutil,'which',return_value='/existing/tool'),mock.patch.object(profile,'recovery_analysis_identity',return_value=identity),mock.patch.object(profile,'prepare_recovery_symbols',side_effect=self.mock_symbols),mock.patch.object(profile,'prepare_recovery_vdso',side_effect=profile.QualificationError('exact candidate vDSO ID mismatch')),mock.patch.object(profile,'logged_command',side_effect=command):
                with self.assertRaisesRegex(profile.QualificationError,'exact candidate vDSO ID mismatch'):profile.run_recovery(original,output)
            qualification=json.loads((output/'qualification.json').read_text());manifest=json.loads((output/'derived-manifest.json').read_text())
            self.assertFalse(qualification['qualified']);self.assertEqual(qualification['reason'],'exact candidate vDSO ID mismatch')
            self.assertFalse(manifest['qualified']);self.assertEqual(manifest['primary_failure'],'exact candidate vDSO ID mismatch')


if __name__ == '__main__':
    unittest.main()
