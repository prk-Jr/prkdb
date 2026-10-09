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
                       "leaf": (symbol.strip(), dso.strip()) if symbol is not None else None, "chain": []}
            samples.append(current)
            continue
        match = CHAIN_RE.fullmatch(line)
        require(match is not None and current is not None, f"unparsed perf line: {line[:180]}")
        ip, symbol, dso = match.groups()
        # perf emits '(inlined)' without a DSO for an inlined frame.
        frame = (symbol.strip(), None if dso == "inlined" else dso.strip())
        current["chain"].append(frame)
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


def qualify_retained(output):
    manifest = read_json((output / "source-manifest.json").read_text())
    require(manifest.get("qualified") is not False and manifest.get("complete_round") is True,
            "failed or incomplete round cannot be requalified")
    require(type(manifest.get("schema")) is int and manifest["schema"] == 1,
            "unsupported provenance schema")
    require(type(manifest.get("execution_rounds")) is int and manifest["execution_rounds"] == 1,
            "one execution round required")
    require(manifest.get("parser_sha256") == sha256(Path(__file__)), "stale parser hash")
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
        require(Path(name).name == name and sha256(output / name) == wanted, f"artifact hash differs: {name}")
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
    args = parser.parse_args()
    try:
        if args.command == "preflight":
            result = preflight()
            if args.output:
                require(not args.output.exists(), "preflight output already exists")
                save_json(args.output, result)
        elif args.command == "run":
            result = run_profile(args.binary.resolve(), args.output.resolve(), args.build_manifest.resolve())
        else:
            result = qualify_retained(args.output.resolve())
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (QualificationError, OSError, KeyError) as error:
        print(json.dumps({"qualified": False, "reason": str(error), "timing_claim": False}), file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
