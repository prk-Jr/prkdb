#!/usr/bin/env python3
"""One Linux CPU profile; strict phase qualification, never a build/throughput gate.

preflight checks the selected Linux host before an external build. run requires a
clean committed source and a build manifest. qualify reparses retained raw files;
it never reruns perf. No tool installs, sysctl writes, or profile retries occur.
"""
import argparse
import collections
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shlex
import shutil
import subprocess
import sys
import urllib.request
import urllib.error

CELL = "stream_read/tail/1k"
PREFIX = "# STREAM_PROFILE "
REPO = Path(__file__).resolve().parents[1]
FREQUENCY = 199
MIN_SAMPLES = 300
MAX_UNKNOWN_PERCENT = 10
SHA_RE = re.compile(r"[0-9a-f]{40}\Z")
# Upstream builtin-script.c prints period before event regardless of -F order.
# Resolved DWARF callchains begin on the following line, with a leaf-first cursor.
HEADER_RE = re.compile(r"^\s*(\d+)(?:\s+|/)(\d+)\s+(\d+)\.(\d{9}):\s+(\d+)\s+(cpu-clock:u):(?:\s+((?:0x)?[0-9a-fA-F]+)\s+(.+)\s+\((.+)\))?\s*$")
CHAIN_RE = re.compile(r"^\s+((?:0x)?[0-9a-fA-F]+)\s+(.+)\s+\((.+)\)\s*$")
LOSS_RE = re.compile(r"PERF_RECORD_(?:LOST(?:_SAMPLES)?|(?:UN)?THROTTLE)\b", re.I)
DIAGNOSTIC_RE = re.compile(r"\b(?:lost|throttl\w*|unwind\w*)\b", re.I)
FIXTURE = {"verified": True, "frames": 256, "batch": 1024, "records": 262144,
           "value_bytes": 268435456, "value_size": 1024, "key_size": 7,
           "compression": "none", "mode": "Fast", "synced": True,
           "retention": False, "segment_bytes": 268435456}


class QualificationError(ValueError):
    """Evidence cannot support the frozen profile contract."""


def require(condition, message):
    if not condition:
        raise QualificationError(message)


def _json_object(pairs):
    value = {}
    for key, item in pairs:
        require(key not in value, f"duplicate JSON key: {key}")
        value[key] = item
    return value


def read_json(text):
    try:
        return json.loads(text, object_pairs_hook=_json_object)
    except (ValueError, TypeError) as error:
        raise QualificationError(f"malformed JSON: {error}") from error


def integer(value, minimum=0):
    return type(value) is int and minimum <= value <= (1 << 64) - 1


def parse_intervals(stdout, source):
    require(isinstance(source, str) and SHA_RE.fullmatch(source), "invalid source SHA")
    markers = []
    for line in stdout.splitlines():
        if line.startswith(PREFIX):
            marker = read_json(line[len(PREFIX):])
            require(isinstance(marker, dict), "marker must be an object")
            markers.append(marker)
    require(len(markers) == 9, "exactly three fixture/begin/end triples required")
    pid = markers[0].get("pid")
    require(integer(pid, 1), "invalid marker process")
    intervals, previous_end = [], 0
    for index in range(3):
        triple = markers[index * 3:index * 3 + 3]
        for phase, marker in zip(("fixture", "begin", "end"), triple):
            require(type(marker.get("schema")) is int and marker["schema"] == 1, "unsupported marker schema")
            require(marker.get("phase") == phase, "marker phase order differs")
            require(marker.get("cell") == CELL, "unexpected profile cell")
            require(type(marker.get("id")) is int and marker["id"] == index + 1, "repetition order differs")
            require(type(marker.get("pid")) is int and marker["pid"] == pid, "inconsistent process")
            require(marker.get("source_sha") == source, "marker source differs")
        for key, wanted in FIXTURE.items():
            got = triple[0].get(key)
            require(type(got) is type(wanted) and got == wanted, f"fixture {key} differs")
        begin, end = triple[1].get("mono_ns"), triple[2].get("mono_ns")
        require(integer(begin) and integer(end), "invalid monotonic timestamp")
        require(previous_end <= begin < end, "reversed or overlapping profile intervals")
        previous_end = end
        intervals.append({"id": index + 1, "pid": pid, "begin_ns": begin, "end_ns": end})
    return intervals


def parse_samples(text):
    require(not LOSS_RE.search(text), "lost or throttled perf records")
    samples, current = [], None
    for line in text.splitlines():
        if not line.strip() or line.startswith("#"):
            continue
        match = HEADER_RE.fullmatch(line)
        if match:
            pid, tid, sec, nanos, period, event, ip, symbol, dso = match.groups()
            period = int(period)
            require(integer(period, 1), "invalid sample period")
            stamp = int(sec) * 1_000_000_000 + int(nanos)
            require(integer(stamp) and int(pid) > 0 and int(tid) > 0, "invalid sample timestamp/process/thread")
            current = {"pid": int(pid), "tid": int(tid), "mono_ns": stamp, "period": period,
                       "event": event, "ip": ip,
                       "leaf": (symbol.strip(), dso.strip()) if symbol is not None else None,
                       "chain": [], "chain_ips": [], "chain_dsos": []}
            samples.append(current)
            continue
        match = CHAIN_RE.fullmatch(line)
        require(match is not None and current is not None, f"unparsed perf line: {line[:180]}")
        ip, symbol, dso = match.groups()
        # perf emits '(inlined)' without a DSO for an inlined frame.
        frame = (symbol.strip(), None if dso == "inlined" else dso.strip())
        current["chain"].append(frame)
        current["chain_ips"].append(ip)
        current["chain_dsos"].append(frame[1])
        if current["leaf"] is None:
            current["leaf"], current["ip"] = frame, ip
    return samples


def unknown(symbol):
    return symbol in ("[unknown]", "unknown", "??", "[unresolved]") or bool(re.fullmatch(r"(?:0x)?[0-9a-fA-F]+", symbol))


def _symbols(counter, total_period):
    return [{"symbol": symbol, "dso": dso, "period": period, "percent": period * 100 / total_period}
            for (symbol, dso), period in sorted(counter.items(), key=lambda item: (-item[1], item[0][0], item[0][1] or ""))]


def qualify(stdout, perf_script, expected_source_sha, diagnostics="", raw_events=""):
    require(not DIAGNOSTIC_RE.search(diagnostics) and not LOSS_RE.search(diagnostics), "loss, throttling or unwind diagnostics")
    require(not LOSS_RE.search(raw_events), "lost or throttled raw perf events")
    intervals, samples = parse_intervals(stdout, expected_source_sha), parse_samples(perf_script)
    leaf, inclusive = collections.Counter(), collections.Counter()
    total_samples = total_period = unknown_samples = unknown_period = 0
    phases = []
    for interval in intervals:
        selected = [s for s in samples if interval["begin_ns"] <= s["mono_ns"] < interval["end_ns"]]
        require(selected, f"no measured samples for repetition {interval['id']}")
        phase_period = phase_unknown = phase_unknown_period = 0
        threads = set()
        for sample in selected:
            require(sample["pid"] == interval["pid"], "foreign process inside measured interval")
            require(sample["chain"] and sample["leaf"] is not None, "unreadable measured callchain")
            period = sample["period"]
            threads.add(sample["tid"])
            phase_period += period
            leaf[sample["leaf"]] += period
            for symbol in set([sample["leaf"]] + sample["chain"]):
                inclusive[symbol] += period
            if unknown(sample["leaf"][0]):
                phase_unknown += 1
                phase_unknown_period += period
        phases.append({**interval, "samples": len(selected), "period": phase_period,
                       "threads": sorted(threads), "unknown_samples": phase_unknown, "unknown_period": phase_unknown_period})
        total_samples += len(selected)
        total_period += phase_period
        unknown_samples += phase_unknown
        unknown_period += phase_unknown_period
    require(total_samples >= MIN_SAMPLES, "fewer than 300 pooled measured samples")
    require(unknown_samples * 100 <= MAX_UNKNOWN_PERCENT * total_samples, "more than 10 percent unknown leaf samples")
    return {"schema": 1, "qualified": True, "source_sha": expected_source_sha,
            "evidence_kind": "sampled userspace CPU; not throughput or instruction gate",
            "sampling": {"event": "cpu-clock:u", "frequency": FREQUENCY, "clock": "CLOCK_MONOTONIC",
                         "bounds": "[begin,end)", "min_pooled_samples": MIN_SAMPLES,
                         "max_unknown_leaf_percent": MAX_UNKNOWN_PERCENT},
            "samples": total_samples, "period": total_period, "unknown_leaf_samples": unknown_samples,
            "unknown_leaf_sample_percent": unknown_samples * 100 / total_samples,
            "unknown_leaf_period_percent": unknown_period * 100 / total_period,
            "phases": phases, "exclusive": _symbols(leaf, total_period), "inclusive": _symbols(inclusive, total_period),
            "inclusive_overlaps": True, "repetition_stability_qualified": False,
            "unmeasured": ["blocked time", "physical bytes", "allocations", "RSS", "memory bandwidth",
                           "unprofiled throughput", "competitive timing"]}


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as file:
        for block in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def capture(command, cwd=REPO, env=None):
    result = subprocess.run(command, cwd=cwd, env=profile_env(os.environ) if env is None else env,
                            text=True, capture_output=True, check=False)
    require(result.returncode == 0, f"command failed ({result.returncode}): {command!r}\n{result.stderr[-2000:]}")
    return result.stdout


def quiet_processes():
    found = []
    for path in Path("/proc").glob("[0-9]*/comm"):
        try:
            name = path.read_text().strip()
        except (FileNotFoundError, ProcessLookupError):
            continue
        if name in {"rustc", "cargo", "gcc", "g++", "cc1", "cc1plus", "clang", "clang++", "perf"}:
            found.append({"pid": int(path.parent.name), "comm": name})
    require(not found, f"concurrent compiler/build/profile processes: {found}")
    return found


def preflight():
    require(platform.system() == "Linux", "Linux perf host required; Mac walltime cannot qualify")
    for tool, install in (("perf", "sudo apt-get install linux-tools-common linux-tools-generic"),
                          ("git", "sudo apt-get install git"), ("rustc", "rustup toolchain install 1.98.1"),
                          ("df", "sudo apt-get install coreutils"), ("true", "sudo apt-get install coreutils")):
        require(shutil.which(tool), f"missing {tool}; maintainer install command: {install}")
    quiet = quiet_processes()
    permission = subprocess.run(["perf", "stat", "-e", "cpu-clock:u", "--", "true"], cwd=REPO,
                                env=profile_env(os.environ), text=True, capture_output=True, check=False)
    require(permission.returncode == 0, "perf permission/kernel qualification failed; maintainer must configure a qualified host: " + permission.stderr[-2000:])
    cpu = [line for line in Path("/proc/cpuinfo").read_text().splitlines()
           if line.split(":", 1)[0].strip() in {"processor", "model name", "Hardware", "CPU implementer"}]
    return {"platform": platform.platform(), "kernel": platform.release(), "cpu": cpu,
            "ram": Path("/proc/meminfo").read_text(), "load": Path("/proc/loadavg").read_text().strip(),
            "cpu_stat": Path("/proc/stat").read_text(), "affinity": sorted(os.sched_getaffinity(0)),
            "cpuset": [line for line in Path("/proc/self/status").read_text().splitlines()
                       if line.startswith(("Cpus_allowed_list:", "Mems_allowed_list:"))],
            "filesystem": capture(["df", "-PT", str(REPO)]), "perf": capture(["perf", "--version"]).strip(),
            "perf_permissions": permission.stderr,
            "perf_event_paranoid": Path("/proc/sys/kernel/perf_event_paranoid").read_text().strip(),
            "max_sample_rate": Path("/proc/sys/kernel/perf_event_max_sample_rate").read_text().strip(),
            "rustc": capture(["rustc", "-Vv"]), "quiet_processes": quiet,
            "host_limits": "CPU attribution only; shared VM/steal possible, no dedicated timing qualification"}


def perf_record_command(binary, output):
    return ["perf", "record", "-o", str(Path(output) / "perf.data"), "-e", "cpu-clock:u", "-F", str(FREQUENCY),
            "--strict-freq", "--period", "--clockid", "mono", "--call-graph", "dwarf,16384", "--", str(binary)]


def phase_commands(script, interval):
    def timestamp(value):
        return f"{value // 1_000_000_000}.{value % 1_000_000_000:09d}"
    # perf --time includes its end; integer end-1 agrees with [begin, end).
    window = f"{timestamp(interval['begin_ns'])},{timestamp(interval['end_ns'] - 1)}"
    return {"script": script + ["--time", window],
            "report": ["perf", "report", "-i", script[3], "--stdio", "--no-children", "--time", window]}


def profile_env(base):
    return {**{key: value for key, value in base.items() if not key.startswith("SPIKE_")},
            "PERF_CONFIG": "/dev/null", "SPIKE_FILTER": CELL, "SPIKE_WARMUP_MS": "1000",
            "SPIKE_MEASURE_MS": "3000", "SPIKE_REPS": "3", "SPIKE_STREAM_PROFILE": "1"}


def validate_build_manifest(manifest, source, binary):
    require(isinstance(manifest, dict), "build manifest must be an object")
    require(isinstance(source, str) and SHA_RE.fullmatch(source) and manifest.get("source_sha") == source, "stale build source")
    wanted = manifest.get("binary_sha256")
    require(isinstance(wanted, str) and re.fullmatch(r"[0-9a-f]{64}", wanted), "invalid build binary hash")
    require(sha256(binary) == wanted, "stale or changed binary")
    require(manifest.get("profile") == "bench" and manifest.get("toolchain") == "1.98.1", "wrong build profile/toolchain")
    flags = manifest.get("rustflags", "")
    require(isinstance(flags, str), "invalid profiling flags")
    tokens = shlex.split(flags)
    settings, index = {}, 0
    while index < len(tokens):
        token = tokens[index]
        if token == "-C":
            index += 1
            require(index < len(tokens), "incomplete codegen flag")
            token = "-C" + tokens[index]
        if token.startswith("-C") and "=" in token:
            key, value = token[2:].split("=", 1)
            settings[key] = value
        index += 1
    require(settings.get("debuginfo") == "1" and settings.get("force-frame-pointers") == "yes",
            "effective profiling debuginfo/frame-pointer flags differ")
    require(source.encode() in Path(binary).read_bytes(), "source marker absent from built binary")


def save_json(path, value):
    Path(path).write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def logged_command(command, output, label, env=None):
    save_json(output / f"{label}-command.json", command)
    with (output / f"{label}.stdout").open("w") as stdout, (output / f"{label}.stderr").open("w") as stderr:
        result = subprocess.run(command, cwd=REPO, env=profile_env(os.environ) if env is None else env,
                                stdout=stdout, stderr=stderr, check=False)
    save_json(output / f"{label}-status.json", {"exit_code": result.returncode})
    require(result.returncode == 0, f"{label} failed ({result.returncode}); raw files retained")


def validate_retained_provenance(output, expected_parser):
    manifest = read_json((output / "source-manifest.json").read_text())
    require(isinstance(manifest, dict), "source manifest must be an object")
    require(type(manifest.get("schema")) is int and manifest["schema"] == 1,
            "unsupported provenance schema")
    require(type(manifest.get("execution_rounds")) is int and manifest["execution_rounds"] == 1,
            "one execution round required")
    require(manifest.get("parser_sha256") == expected_parser, "stale parser hash")
    required = {"perf.data", "benchmark-profiled", "build-manifest.json",
                "profile-settings.json", "host-before.json", "host-after.json"}
    labels = ["record", "script", "dump"] + [f"phase-{identity}-{suffix}" for identity in range(1,4)
                                               for suffix in ("script", "report")]
    for label in labels:
        required.update({f"{label}-command.json", f"{label}-status.json", f"{label}.stdout", f"{label}.stderr"})
    require(isinstance(manifest.get("artifacts"), dict) and required <= manifest["artifacts"].keys(),
            "required raw/provenance artifact hashes missing")
    require(all(path.name in manifest["artifacts"] for path in output.glob("*.stderr")),
            "unhashed diagnostic file")
    for name, wanted in manifest["artifacts"].items():
        require(Path(name).name == name and (output / name).is_file() and sha256(output / name) == wanted, f"artifact hash differs: {name}")
    for label in labels:
        status = read_json((output / f"{label}-status.json").read_text())
        require(type(status.get("exit_code")) is int and status["exit_code"] == 0,
                f"failed {label} cannot qualify")
    record = read_json((output / "record-command.json").read_text())
    require(isinstance(record, list) and len(record) > 4 and all(isinstance(x, str) for x in record),
            "invalid perf recording command")
    require(Path(record[3]).name == "perf.data"
            and record == perf_record_command(record[-1], Path(record[3]).parent),
            "recording command differs from frozen contract")
    script = read_json((output / "script-command.json").read_text())
    require(script == ["perf", "script", "-i", record[3], "--ns", "--show-lost-events", "-F",
                       "pid,tid,time,event,period,ip,sym,dso"], "sample extraction command differs")
    require(read_json((output / "dump-command.json").read_text()) == ["perf", "script", "-i", record[3], "-D"],
            "raw event audit command differs")
    for interval in parse_intervals((output / "record.stdout").read_text(), manifest["source_sha"]):
        for suffix, command in phase_commands(script, interval).items():
            label = f"phase-{interval['id']}-{suffix}"
            require(read_json((output / f"{label}-command.json").read_text()) == command,
                    f"{label} command differs from measured interval/frozen flags")
    require(read_json((output / "profile-settings.json").read_text()) == profile_env({}),
            "benchmark settings differ")
    require(read_json((output / "build-manifest.json").read_text()) == manifest["build"],
            "build provenance differs")
    validate_build_manifest(manifest["build"], manifest["source_sha"], output / "benchmark-profiled")
    return manifest


def qualify_retained(output):
    manifest = read_json((output / "source-manifest.json").read_text())
    require(manifest.get("qualified") is not False and manifest.get("complete_round") is True,
            "failed or incomplete round cannot be requalified")
    manifest = validate_retained_provenance(output, sha256(Path(__file__)))
    diagnostics = "\n".join(path.read_text() for path in output.glob("*.stderr"))
    return qualify((output / "record.stdout").read_text(), (output / "script.stdout").read_text(),
                   manifest["source_sha"], diagnostics, (output / "dump.stdout").read_text())


def run_profile(binary, output, build_manifest):
    require(not output.exists(), "output directory already exists; no overwrite or profiling retry")
    require(not output.resolve().is_relative_to(REPO), "artifacts must be outside the source worktree")
    output.mkdir(parents=True, mode=0o700)
    record_attempts = 0
    try:
        host = preflight()
        save_json(output / "host-before.json", host)
        require("release: 1.98.1\n" in host["rustc"], "actual rustc differs from pinned 1.98.1")
        require(not capture(["git", "status", "--porcelain"]).strip(), "source worktree must be clean")
        source = capture(["git", "rev-parse", "HEAD"]).strip()
        build = read_json(build_manifest.read_text())
        validate_build_manifest(build, source, binary)
        require(binary.is_file() and os.access(binary, os.X_OK), "benchmark binary is not executable")
        shutil.copy2(binary, output / "benchmark-profiled")
        save_json(output / "build-manifest.json", build)
        environment = profile_env(os.environ)
        save_json(output / "profile-settings.json", {key: environment[key] for key in
                  ("PERF_CONFIG", "SPIKE_FILTER", "SPIKE_WARMUP_MS", "SPIKE_MEASURE_MS", "SPIKE_REPS", "SPIKE_STREAM_PROFILE")})
        quiet_processes()
        record_attempts += 1
        logged_command(perf_record_command(binary, output), output, "record", environment)
        quiet_processes()
        save_json(output / "host-after.json", {"load": Path("/proc/loadavg").read_text().strip(),
                  "cpu_stat": Path("/proc/stat").read_text(), "quiet_processes": []})
        require(capture(["git", "rev-parse", "HEAD"]).strip() == source and not capture(["git", "status", "--porcelain"]).strip(), "source changed during recording")
        require(sha256(binary) == build["binary_sha256"], "benchmark changed during recording")
        command = ["perf", "script", "-i", str(output / "perf.data"), "--ns", "--show-lost-events",
                   "-F", "pid,tid,time,event,period,ip,sym,dso"]
        logged_command(command, output, "script")
        # Normal perf script suppresses throttle records; -D audits every raw type.
        logged_command(["perf", "script", "-i", str(output / "perf.data"), "-D"], output, "dump")
        intervals = parse_intervals((output / "record.stdout").read_text(), source)
        for interval in intervals:
            for suffix, phase_command in phase_commands(command, interval).items():
                logged_command(phase_command, output, f"phase-{interval['id']}-{suffix}")
        manifest = {"schema": 1, "source_sha": source, "source_tree": capture(["git", "rev-parse", "HEAD^{tree}"]).strip(),
                    "build": build, "execution_rounds": record_attempts, "complete_round": True,
                    "parser_sha256": sha256(Path(__file__)),
                    "artifacts": {path.name: sha256(path) for path in sorted(output.iterdir()) if path.is_file()}}
        save_json(output / "source-manifest.json", manifest)
        result = qualify_retained(output)
        save_json(output / "qualification.json", result)
        return result
    except (QualificationError, OSError, KeyError) as error:
        save_json(output / "qualification.json", {"qualified": False, "reason": str(error), "timing_claim": False, "retry_permitted": False})
        # Preserve hashes even when recording/extraction fails, without a second run.
        if "source" in locals() and "build" in locals():
            save_json(output / "source-manifest.json", {
                "schema": 1, "source_sha": source, "build": build, "execution_rounds": record_attempts,
                "parser_sha256": sha256(Path(__file__)), "qualified": False, "complete_round": False,
                "artifacts": {path.name: sha256(path) for path in sorted(output.iterdir())
                              if path.is_file() and path.name != "source-manifest.json"}})
        raise


# This recovery is intentionally pinned to one failed recording, with no bypass flag.
ORIGINAL_MANIFEST_SHA = "d120181c09c8452f5b54649da8bb43f65877601f93860ce4970e52f06429fa7a"
ORIGINAL_PERF_SHA = "11e4022abb2edcc29aaef169b39eb6d4796f775a7e65f3a848de36e1a203eb3b"
ORIGINAL_SOURCE = "06965f2cbad61628488a423106084fdc34fe0be4"
ORIGINAL_PARSER = "f1dfd77d57ab029cea162232ce936fddc6f2b56bce0cc522d6c9273013b28909"
ORIGINAL_ARTIFACT_COUNT = 43
ORIGINAL_FULL_SAMPLES = 1910
ORIGINAL_PHASE_SAMPLES = (446, 427, 437)
ORIGINAL_UNKNOWN = 483

UNKNOWN_FAILURE = "more than 10 percent unknown leaf samples"
LIBC_ID = "a4a7992a8e66555c8141ab2a08a8465ff6e0ea65"
BENCHMARK_ID = "ae824675e91ad3102468a2d3a6ca1c3a0487ca97"
LIBC_DSO = "/usr/lib/x86_64-linux-gnu/libc.so.6"
BENCHMARK_DSO = "/home/runner/work/prkdb/prkdb/target/release/deps/wal_write_path-7dc6268dee3e4454"
LIBC_SHA = "3a15d66867d83762c7f2f1e37359cb8f6c5743edb369c65285cb0b1c4f7498bf"
DEBUG_SHA = "93213939b6f3f01720b8c336143fc405999109d18b64b195a99e1a0c4301214b"
DEBUG_PATH = Path("usr/lib/debug/.build-id") / LIBC_ID[:2] / (LIBC_ID[2:] + ".debug")
PACKAGES = (
    ("libc6", "https://security.ubuntu.com/ubuntu/pool/main/g/glibc/libc6_2.39-0ubuntu8.9_amd64.deb",
     "ff5557d99b51f761c4b7c92368b9cc45565eda17df9bf9eb4b134d09825008be"),
    ("libc6-dbg", "https://security.ubuntu.com/ubuntu/pool/main/g/glibc/libc6-dbg_2.39-0ubuntu8.9_amd64.deb",
     "c4b086cef3a6bbb6a90299969b3c0bf3867f40cb8e9bbbb0edfe5ae23f2c04f0"))


def compare_recovery_samples(before, after, allowed_dsos):
    require(len(before) == len(after), "recovery sample count changed")
    fields = ("pid", "tid", "mono_ns", "event", "period", "ip", "chain_ips", "chain_dsos")
    for index, (original, derived) in enumerate(zip(before, after)):
        require(all(original[key] == derived[key] for key in fields),
                f"sample/stack identity changed at {index}; stop for review")
        require(original["leaf"][1] == derived["leaf"][1]
                and len(original["chain"]) == len(derived["chain"]),
                f"leaf/stack identity changed at {index}; stop for review")
        for old, new in zip([original["leaf"]] + original["chain"], [derived["leaf"]] + derived["chain"]):
            require(old[1] == new[1], f"stack DSO changed at {index}")
            require(old == new or old[1] in allowed_dsos,
                    f"unapproved external symbol resolution at {index}: {old[1]}")


def validate_recovery_original(output):
    require(sha256(output / "source-manifest.json") == ORIGINAL_MANIFEST_SHA, "original manifest pin differs")
    require(sha256(output / "perf.data") == ORIGINAL_PERF_SHA, "original perf.data pin differs")
    manifest = validate_retained_provenance(output, ORIGINAL_PARSER)
    require(manifest["source_sha"] == ORIGINAL_SOURCE, "original source pin differs")
    require(manifest.get("qualified") is False and manifest.get("complete_round") is False,
            "only the pinned failed recording is eligible")
    require(len(manifest["artifacts"]) == ORIGINAL_ARTIFACT_COUNT, "original artifact count differs")
    failure = read_json((output / "qualification.json").read_text())
    require(failure == {"qualified": False, "reason": UNKNOWN_FAILURE,
                        "retry_permitted": False, "timing_claim": False}, "original failure differs")
    diagnostics = "\n".join(path.read_text() for path in output.glob("*.stderr"))
    try:
        qualify((output / "record.stdout").read_text(), (output / "script.stdout").read_text(),
                ORIGINAL_SOURCE, diagnostics, (output / "dump.stdout").read_text())
    except QualificationError as error:
        require(str(error) == UNKNOWN_FAILURE, "recording has another qualification failure: " + str(error))
    else:
        raise QualificationError("original no longer has its sole unknown-leaf failure")
    intervals = parse_intervals((output / "record.stdout").read_text(), ORIGINAL_SOURCE)
    samples = parse_samples((output / "script.stdout").read_text())
    counts = tuple(sum(i["begin_ns"] <= s["mono_ns"] < i["end_ns"] for s in samples) for i in intervals)
    measured = [s for s in samples if any(i["begin_ns"] <= s["mono_ns"] < i["end_ns"] for i in intervals)]
    require(len(samples) == ORIGINAL_FULL_SAMPLES and counts == ORIGINAL_PHASE_SAMPLES,
            "original full/measured cohort counts differ")
    require(sum(unknown(s["leaf"][0]) for s in measured) == ORIGINAL_UNKNOWN, "original unknown cohort differs")
    return manifest


def recovery_env(base):
    return {**profile_env(base), "DEBUGINFOD_URLS": ""}


def recovery_command(command, output):
    require(command[:1] == ["perf"] and len(command) > 3 and command[1] in {"script", "report"},
            "only offline extraction commands are allowed")
    relocated = list(command[1:])
    require(relocated[1] == "-i", "offline command input differs")
    relocated[2] = str(output / "perf.data")
    return ["perf", "--buildid-dir", str(recovery_cache(output))] + relocated + ["--symfs", str(output / "symfs"), "--no-inline"]


def verify_elf(path, expected_hash, expected_id, output, label):
    require(path.is_file() and sha256(path) == expected_hash, f"ELF file hash differs: {label}")
    logged_command(["readelf", "--notes", str(path)], output, label, recovery_env(os.environ))
    ids = re.findall(r"Build ID:\s*([0-9a-fA-F]+)", (output / f"{label}.stdout").read_text())
    require(ids == [expected_id], f"ELF GNU build ID differs: {label}")
    return {"path": str(path), "sha256": expected_hash, "build_id": expected_id}


def download_package(label, url, expected_hash, output):
    package = output / (label + ".deb")
    provenance = {"url": url, "expected_sha256": expected_hash}
    try:
        with urllib.request.urlopen(url, timeout=60) as response:
            provenance.update(status=response.status, final_url=response.geturl(), headers=list(response.headers.items()))
            save_json(output / f"{label}-download.json", provenance)
            require(response.status == 200 and response.geturl() == url, "package download response differs")
            with package.open("xb") as file:
                shutil.copyfileobj(response, file)
        provenance["sha256"] = sha256(package)
        require(provenance["sha256"] == expected_hash, f"package hash differs: {label}")
    except urllib.error.HTTPError as error:
        provenance.update(status=error.code, headers=list(error.headers.items()), error=str(error))
        raise QualificationError(f"matching symbols unavailable: {label} HTTP {error.code}") from error
    except Exception as error:
        provenance["error"] = str(error)
        raise
    finally:
        save_json(output / f"{label}-download.json", provenance)
    return package


def prepare_recovery_symbols(original, output, manifest):
    symbols = {}
    for label, url, digest in PACKAGES:
        package = download_package(label, url, digest, output)
        extracted = output / (label + "-extracted")
        logged_command(["dpkg-deb", "--extract", str(package), str(extracted)], output,
                       label + "-extract", recovery_env(os.environ))
    libc = output / "libc6-extracted" / LIBC_DSO.lstrip("/")
    debug = output / "libc6-dbg-extracted" / DEBUG_PATH
    symbols["libc"] = verify_elf(libc, LIBC_SHA, LIBC_ID, output, "libc-elf")
    symbols["debug"] = verify_elf(debug, DEBUG_SHA, LIBC_ID, output, "debug-elf")
    symbols["benchmark"] = verify_elf(original / "benchmark-profiled", manifest["build"]["binary_sha256"],
                                       BENCHMARK_ID, output, "benchmark-elf")
    symfs = output / "symfs"
    for label, source, relative in (("libc", libc, Path(LIBC_DSO.lstrip("/"))), ("debug", debug, DEBUG_PATH),
                                    ("benchmark", original / "benchmark-profiled", Path(BENCHMARK_DSO.lstrip("/")))):
        destination = symfs / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, destination)
        symbols[label] = verify_elf(destination, symbols[label]["sha256"], symbols[label]["build_id"],
                                    output, label + "-used-elf")
    # symfs contains only the exact two ELF objects and matching libc debug file.
    # A fresh empty global cache prevents access to ambient build-ID entries.
    recovery_cache(output).mkdir()
    return symbols


def recheck_recovery_symbols(symbols):
    require(set(symbols) == {"libc", "debug", "benchmark", "vdso"}, "exact used symbol objects required")
    for label, metadata in symbols.items():
        require(sha256(Path(metadata["path"])) == metadata["sha256"], f"used symbol file changed: {label}")
        if label == "vdso":
            link = Path(metadata["cache_link"])
            require(link.is_dir() and not link.is_symlink() and str(link.resolve()) == metadata["cache_target"]
                    and link.resolve().is_relative_to(Path(metadata["cache_root"]).resolve()),
                    "vDSO cache binding changed")
            require({path.name for path in link.iterdir()} == {"elf", "vdso"}, "unexpected vDSO cache entry")
            source = Path(metadata["cache_add_target"])
            require(str(source.resolve()) == metadata["cache_add_target"]
                    and source.resolve().is_relative_to(Path(metadata["cache_root"]).resolve()),
                    "vDSO cache-add target changed")
            auxiliary = metadata.get("auxiliary_probes")
            require({path.name for path in source.iterdir()} == ({"elf", "probes"} if auxiliary else {"elf"}),
                    "unexpected retained vDSO cache-add entry")
            if auxiliary is not None:
                require(empty_probe_metadata(Path(auxiliary["path"]), Path(metadata["cache_root"])) == auxiliary,
                        "retained probe metadata changed")
            for name in ("candidate_path", "cache_elf_path", "resolved_path", "cache_add_elf_path"):
                path = Path(metadata[name])
                require(not path.is_symlink() and sha256(path) == metadata["sha256"], "vDSO supplied file changed")


def recheck_recovery_recording(output):
    require((output / "perf.data").is_file() and sha256(output / "perf.data") == ORIGINAL_PERF_SHA,
            "used recording copy differs from pinned perf.data")


def recovery_analysis_identity():
    require(not capture(["git", "status", "--porcelain"]).strip(), "analysis checkout must be clean")
    return {"source_sha": capture(["git", "rev-parse", "HEAD"]).strip(),
            "source_tree": capture(["git", "rev-parse", "HEAD^{tree}"]).strip(),
            "parser_sha256": sha256(Path(__file__))}


def recovery_artifacts(output):
    return {str(path.relative_to(output)): sha256(path) for path in sorted(output.rglob("*"))
            if path.is_file() and path.name != "derived-manifest.json"}


def run_recovery(original, output):
    require(not output.exists(), "recovery output already exists; no overwrite or retry")
    require(not output.resolve().is_relative_to(REPO) and not output.resolve().is_relative_to(original.resolve()),
            "derived output must be outside source and original directories")
    output.mkdir(parents=True, mode=0o700)
    derived = {"schema": 1, "operation": "same-recording symbol recovery", "qualified": False,
               "original_manifest_sha256": ORIGINAL_MANIFEST_SHA, "original_perf_sha256": ORIGINAL_PERF_SHA,
               "original_source_sha": ORIGINAL_SOURCE, "original_parser_sha256": ORIGINAL_PARSER,
               "original_path": str(original), "recording_attempts": 0, "recovery_attempts": 1,
               "timing_claim": False, "retry_permitted": False}
    try:
        manifest = validate_recovery_original(original)
        derived["original_verified_before"] = True
        for name in ("source-manifest.json", "qualification.json"):
            shutil.copyfile(original / name, output / ("original-" + name))
        require(platform.system() == "Linux", "offline recovery requires the original Linux perf version")
        for tool, install in (("perf", "sudo apt-get install linux-tools-common linux-tools-generic"),
                              ("readelf", "sudo apt-get install binutils"),
                              ("dpkg-deb", "sudo apt-get install dpkg"), ("git", "sudo apt-get install git")):
            require(shutil.which(tool), f"missing {tool}; maintainer install command: {install}")
        derived["analysis"] = recovery_analysis_identity()
        environment = recovery_env(os.environ)
        save_json(output / "recovery-settings.json", {"PERF_CONFIG": environment["PERF_CONFIG"],
                  "DEBUGINFOD_URLS": environment["DEBUGINFOD_URLS"], "buildid_dir": str(recovery_cache(output)),
                  "symfs": str(output / "symfs")})
        for tool, command in (("perf", ["perf", "--version"]), ("readelf", ["readelf", "--version"]),
                              ("dpkg-deb", ["dpkg-deb", "--version"]), ("git", ["git", "--version"])):
            logged_command(command, output, tool + "-version", environment)
        require((output / "perf-version.stdout").read_text().strip() == "perf version 6.17.13",
                "offline perf version differs from recorded 6.17.13")
        derived["symbols"] = prepare_recovery_symbols(original, output, manifest)
        shutil.copyfile(original / "perf.data", output / "perf.data")
        recheck_recovery_recording(output)
        derived["recording_verified_before"] = True
        derived["symbols"]["vdso"] = prepare_recovery_vdso(output)
        shutil.copyfile(original / "record.stdout", output / "record.stdout")
        labels = ["script"] + [f"phase-{identity}-{suffix}" for identity in range(1,4)
                                 for suffix in ("script", "report")]
        for label in labels:
            command = recovery_command(read_json((original / f"{label}-command.json").read_text()), output)
            logged_command(command, output, label, environment)
        recheck_recovery_symbols(derived["symbols"])
        recheck_recovery_recording(output)
        before = parse_samples((original / "script.stdout").read_text())
        after = parse_samples((output / "script.stdout").read_text())
        compare_recovery_samples(before, after, {BENCHMARK_DSO, LIBC_DSO, "[vdso]"})
        # Every interval extraction is also the exact ordered subset of the full script.
        intervals = parse_intervals((original / "record.stdout").read_text(), ORIGINAL_SOURCE)
        for interval in intervals:
            expected = [s for s in after if interval["begin_ns"] <= s["mono_ns"] < interval["end_ns"]]
            actual = parse_samples((output / f"phase-{interval['id']}-script.stdout").read_text())
            compare_recovery_samples(expected, actual, set())
        result = qualify((original / "record.stdout").read_text(), (output / "script.stdout").read_text(),
                         ORIGINAL_SOURCE, "\n".join(p.read_text() for p in output.glob("*.stderr")),
                         (original / "dump.stdout").read_text())
        require([p["samples"] for p in result["phases"]] == list(ORIGINAL_PHASE_SAMPLES), "derived phase counts differ")
        require(recovery_analysis_identity() == derived["analysis"], "analysis source/parser changed during recovery")
        validate_recovery_original(original)
        derived.update(qualified=True, original_verified_after=True, full_samples=len(after), measured_samples=result["samples"])
        result.update(derived_analysis=True, original_failure_retained=True, original_manifest_sha256=ORIGINAL_MANIFEST_SHA,
                      original_unknown_leaf_samples=ORIGINAL_UNKNOWN,
                      vdso_resolved_measured_leaves=resolved_vdso_leaves(before, after, intervals))
        save_json(output / "qualification.json", result)
        return result
    except (QualificationError, OSError, KeyError, ValueError) as error:
        derived["qualified"] = False
        derived["reason"] = str(error)
        derived["primary_failure"] = str(error)
        save_json(output / "qualification.json", {"qualified": False, "reason": str(error),
                  "timing_claim": False, "retry_permitted": False, "original_failure_retained": True})
        raise
    finally:
        try:
            validate_recovery_original(original)
            derived["original_verified_after"] = True
        except (QualificationError, OSError, KeyError, ValueError) as error:
            derived["original_verified_after"] = False
            derived["original_recheck_error"] = str(error)
            derived["qualified"] = False
            save_json(output / "qualification.json", {"qualified": False, "reason": derived.get("primary_failure", str(error)),
                      "timing_claim": False, "retry_permitted": False})
        if "analysis" in derived:
            try:
                require(recovery_analysis_identity() == derived["analysis"], "analysis source/parser changed during recovery")
                derived["analysis_verified_after"] = True
            except (QualificationError, OSError, KeyError, ValueError) as error:
                derived.update(qualified=False, analysis_verified_after=False, analysis_recheck_error=str(error), reason=str(error))
                save_json(output / "qualification.json", {"qualified": False, "reason": derived.get("primary_failure", str(error)),
                          "timing_claim": False, "retry_permitted": False, "original_failure_retained": True})
        if "symbols" in derived:
            try:
                recheck_recovery_symbols(derived["symbols"])
                derived["symbols_verified_after"] = True
            except (QualificationError, OSError, KeyError, ValueError) as error:
                derived.update(qualified=False, symbols_verified_after=False, symbol_recheck_error=str(error), reason=str(error))
                save_json(output / "qualification.json", {"qualified": False, "reason": derived.get("primary_failure", str(error)),
                          "timing_claim": False, "retry_permitted": False, "original_failure_retained": True})
        if derived.get("recording_verified_before"):
            try:
                recheck_recovery_recording(output)
                derived["recording_verified_after"] = True
            except (QualificationError, OSError, KeyError, ValueError) as error:
                derived.update(qualified=False, recording_verified_after=False, recording_recheck_error=str(error), reason=str(error))
                save_json(output / "qualification.json", {"qualified": False, "reason": derived.get("primary_failure", str(error)),
                          "timing_claim": False, "retry_permitted": False, "original_failure_retained": True})
        derived["artifacts"] = recovery_artifacts(output)
        save_json(output / "derived-manifest.json", derived)
        require(derived.get("original_verified_after") is True and derived.get("analysis_verified_after", True) is True
                and derived.get("symbols_verified_after", True) is True and derived.get("recording_verified_after", True) is True,
                derived.get("primary_failure", derived.get("reason", "original, analysis or symbol provenance changed; derived evidence refused")))


VDSO_ID = "f0566cac49ca64809e998c75b1373572e3fbc598"


def recovery_cache(output):
    # perf's --symfs callback overrides the global cache with symfs/.debug.
    return output / "symfs" / ".debug"


def recorded_vdso_id(text):
    ids = re.findall(r"^([0-9a-f]{40})\s+\[vdso\]\s*$", text, re.M)
    require(ids == [VDSO_ID], "recorded vDSO build ID differs or is missing/duplicated")
    return ids[0]


def capture_self_vdso(output):
    lines = [line for line in Path("/proc/self/maps").read_text().splitlines() if line.endswith(" [vdso]")]
    require(len(lines) == 1, "one self vDSO mapping required")
    fields = lines[0].split()
    begin, end = (int(value, 16) for value in fields[0].split("-"))
    require(fields[1].startswith("r-x") and 64 <= end - begin <= 1_048_576, "invalid self vDSO mapping")
    fd = os.open("/proc/self/mem", os.O_RDONLY)
    try:
        data = os.pread(fd, end - begin, begin)
    finally:
        os.close(fd)
    require(len(data) == end - begin and data.startswith(b"\x7fELF"), "self vDSO candidate read failed")
    candidate = output / "vdso-candidate.elf"
    candidate.write_bytes(data)
    save_json(output / "vdso-capture.json", {"source": "/proc/self/maps [vdso] and /proc/self/mem",
              "pid": os.getpid(), "mapping": lines[0], "bytes": len(data), "sha256": sha256(candidate)})
    return candidate


def empty_probe_metadata(path, cache):
    digest = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    require(path.is_file() and not path.is_symlink() and path.resolve().is_relative_to(cache.resolve())
            and path.stat().st_size == 0 and sha256(path) == digest, "unexpected vDSO probe metadata")
    return {"path": str(path.resolve()), "bytes": 0, "sha256": digest}


def prepare_recovery_vdso(output):
    cache = recovery_cache(output)
    cache.mkdir(parents=True, exist_ok=True)
    environment = recovery_env(os.environ)
    logged_command(["perf", "--buildid-dir", str(cache), "buildid-list", "-i", str(output / "perf.data")],
                   output, "vdso-buildids", environment)
    recorded_vdso_id((output / "vdso-buildids.stdout").read_text())
    candidate = capture_self_vdso(output)
    digest = sha256(candidate)
    verify_elf(candidate, digest, VDSO_ID, output, "vdso-candidate-elf")
    logged_command(["perf", "--buildid-dir", str(cache), "buildid-cache", "--add", str(candidate)],
                   output, "vdso-cache-add", environment)
    link = cache / ".build-id" / VDSO_ID[:2] / VDSO_ID[2:]
    require(link.is_symlink() and link.is_dir() and link.resolve().is_relative_to(cache.resolve()),
            "vDSO build-ID cache link escapes owned cache")
    entries = {path.name for path in link.iterdir()}
    require(entries in ({"elf"}, {"elf", "probes"}), "unexpected vDSO cache symbol source")
    auxiliary = empty_probe_metadata(link / "probes", cache) if "probes" in entries else None
    cached_elf = link / "elf"
    require(cached_elf.resolve().is_relative_to(cache.resolve()), "cached vDSO ELF escapes owned cache")
    verify_elf(cached_elf, digest, VDSO_ID, output, "vdso-cache-elf")
    # buildid-cache --add uses 'elf' for a regular candidate; vDSO lookup uses 'vdso'.
    cache_add_target = str(link.resolve())
    cache_add_link_target = os.readlink(link)
    cache_add_elf = cached_elf.resolve()
    # Materialize the lookup directory: uploaded artifacts need no directory symlink.
    link.unlink()
    link.mkdir()
    cached_elf = link / "elf"
    shutil.copyfile(cache_add_elf, cached_elf)
    verify_elf(cached_elf, digest, VDSO_ID, output, "vdso-materialized-elf")
    used = link / "vdso"
    shutil.copyfile(cached_elf, used)
    metadata = verify_elf(used, digest, VDSO_ID, output, "vdso-used-elf")
    metadata.update(candidate_path=str(candidate), cache_elf_path=str(cached_elf),
                    resolved_path=str(used.resolve()), cache_link=str(link), cache_add_link_target=cache_add_link_target, cache_add_target=cache_add_target,
                    cache_add_elf_path=str(cache_add_elf), cache_layout="materialized build-ID directory",
                    cache_root=str(cache), cache_target=str(link.resolve()), recorded_build_id=VDSO_ID)
    if auxiliary is not None:
        metadata["auxiliary_probes"] = auxiliary
    return metadata


def resolved_vdso_leaves(before, after, intervals):
    return sum(old["leaf"][1] == "[vdso]" and unknown(old["leaf"][0]) and not unknown(new["leaf"][0])
               and any(i["begin_ns"] <= old["mono_ns"] < i["end_ns"] for i in intervals)
               for old, new in zip(before, after))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    before = commands.add_parser("preflight", help="Linux tools/permissions/quiet-host checks, no build/profile")
    before.add_argument("--output", type=Path, help="optional new JSON file; no environment dump")
    run = commands.add_parser("run", help="one three-repetition profile; no build or retry")
    run.add_argument("--binary", type=Path, required=True)
    run.add_argument("--output", type=Path, required=True)
    run.add_argument("--build-manifest", type=Path, required=True)
    retained = commands.add_parser("qualify", help="reparse retained evidence without running perf")
    retained.add_argument("--output", type=Path, required=True)
    recovery = commands.add_parser("recover", help="offline symbols for the pinned original recording; no collection/retry")
    recovery.add_argument("--original", type=Path, required=True)
    recovery.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        if args.command == "preflight":
            result = preflight()
            if args.output:
                require(not args.output.exists(), "preflight output already exists")
                save_json(args.output, result)
        elif args.command == "run":
            result = run_profile(args.binary.resolve(), args.output.resolve(), args.build_manifest.resolve())
        elif args.command == "recover":
            result = run_recovery(args.original.resolve(), args.output.resolve())
        else:
            result = qualify_retained(args.output.resolve())
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (QualificationError, OSError, KeyError) as error:
        print(json.dumps({"qualified": False, "reason": str(error), "timing_claim": False}), file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
