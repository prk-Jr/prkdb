#!/usr/bin/env python3
"""One retained-binary diagnostic cycle. Never grants performance acceptance."""
import argparse
import collections
import datetime
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import re
import shutil
import signal
import statistics
import subprocess
import sys
import time

CELLS = ('adapter_put/8w/64k', 'stream_read/cold/64k', 'stream_read/tail/1k')
PINNED = {
    'base': {'source_sha': '9b88229d29cf4ea1a384b1357139f0b0f6c7f313',
             'tree_sha': '757236cbcc853523ecc9db6ada11dd5ecf5c7028',
             'binary_sha256': '7cbfc742202a97839b125e6a52720ab74d7fc185a1e65abbf40e5649bc1cfd38'},
    'head': {'source_sha': 'a64f774f965806fb2cd93c91b276d504ec92208a',
             'tree_sha': '6172dd0e5b8a015d54e3a47ad2d3dfeb06272f59',
             'binary_sha256': '1561972eedb4823765487b5996e99a995c888bf759c5f0ae252f6967ba3be4b7'},
}
SOURCE_FILES = {
    'Cargo.lock': 'b96a3b64cfc027b528e38a53974d37e23d079f9d985e5796a944e67e3ef183ad',
    'Cargo.toml': '040a393baea2474a6ed294ac3a4f999ee78d098d0fefd82c647859b72e2b4887',
    'rust-toolchain.toml': '247d87416a2833297e360bb2dfa6b3345deeed70bd91b6a1727d00c5401a6393',
    'crates/prkdb/Cargo.toml': 'c2b8ab4d2e11089908fe9cedb9b0d7cbeb6079d8af7121281e64b75b616b4691',
    'crates/prkdb/benches/wal_write_path.rs': '6d271a42dba0824d12ed9c87abca843aad1edfafd9317500e926f499b85dd41c',
}
CONFIG_HEADER = '- warm-up 1000 ms, measure 3000 ms, reps 3, tokio worker threads = 4'
WRITE_HEADER = '| cell | writers | value | ops/s | MB/s | p50 µs | p99 µs | p99.9 µs | load1 |'
STREAM_HEADER = '| cell | writers | value | ops/s | MB/s | p50 µs | p99 µs | p99.9 µs | records/s | load1 |'
EVENTS = ('sched:sched_switch', 'sched:sched_wakeup', 'sched:sched_process_exec',
          'sched:sched_process_fork', 'sched:sched_process_exit',
          'block:block_rq_issue', 'block:block_rq_complete') + tuple(
    f'syscalls:sys_{phase}_{name}' for name in ('futex', 'write', 'pwrite64', 'fdatasync', 'fsync')
    for phase in ('enter', 'exit'))
LOSS = re.compile(r'PERF_RECORD_(?:LOST|THROTTLE|UNTHROTTLE)|\bLOST_SAMPLES\b|lost\s+\d+|\d+\s+lost', re.I)


class DiagnosticError(Exception):
    pass


def require(condition, message):
    if not condition:
        raise DiagnosticError(message)


def digest(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def dump(path, value):
    Path(path).write_text(json.dumps(value, indent=2, allow_nan=False) + '\n')


def verify_artifact(path, expected=None):
    """CLI trust is fixed. An explicit alternate trust map is for unit fixtures only."""
    path = Path(path)
    trusted = PINNED if expected is None else expected
    require(path.is_dir() and not path.is_symlink(), 'artifact must be a real directory')
    inventory = path / 'SHA256SUMS'
    require(inventory.is_file() and not inventory.is_symlink(), 'missing inventory')
    hashes = {}
    for line in inventory.read_text().splitlines():
        match = re.fullmatch(r'([0-9a-f]{64})  ([^/]+)', line)
        require(match is not None, 'invalid inventory path/hash')
        sha, name = match.groups()
        require(name not in {'.', '..', 'SHA256SUMS'} and name not in hashes, 'duplicate/invalid inventory name')
        member = path / name
        require(member.is_file() and not member.is_symlink(), f'missing/linked member {name}')
        require(digest(member) == sha, f'artifact hash mismatch {name}')
        hashes[name] = sha
    require(len(hashes) == 28, 'expected all 28 original evidence members')
    require({p.name for p in path.iterdir()} == set(hashes) | {'SHA256SUMS'}, 'unlisted artifact members')
    manifests = {}
    for side in ('base', 'head'):
        for suffix in ('build-exit-code', 'exit-code', 'log-exit-code'):
            member = path / f'{side}-{suffix}.txt'
            require(member.name in hashes and member.read_text().strip() == '0', f'unqualified {side} {suffix}')
        m = json.loads((path / f'{side}-binary-manifest.json').read_text())
        require(m.get('source_sha') == trusted[side]['source_sha'], f'{side} source identity')
        sha = trusted[side]['binary_sha256']
        require(m.get('binary_sha256') == m.get('saved_binary_sha256') == sha, f'{side} binary manifest identity')
        binary = path / f'{side}-benchmark-executable'
        require(digest(binary) == sha, f'{side} pinned executable mismatch')
        with binary.open('rb') as source:
            elf = source.read(20)
        require(elf[:7] == b'\x7fELF\x02\x01\x01' and elf[18:20] == b'\x3e\x00', 'expected ELF64 x86_64 executable')
        require(m.get('profile') == 'bench' and m.get('benchmark') == 'wal_write_path', 'wrong native build profile')
        require(m.get('command') == 'SPIKE_REPS=3 cargo bench -p prkdb --bench wal_write_path', 'wrong original command')
        require('release: 1.98.1' in m.get('rustc', '') and 'host: x86_64-unknown-linux-gnu' in m['rustc'], 'wrong compiler')
        require(m.get('environment', {}).get('RUSTFLAGS') is None, 'unexpected original RUSTFLAGS')
        require((path / f'{side}-sha.txt').read_text().strip() == trusted[side]['source_sha'], 'source SHA disagreement')
        lines = (path / f'{side}-source-manifest.txt').read_text().splitlines()
        require(len(lines) == 7 and lines[0] == trusted[side]['source_sha'], 'source manifest disagreement')
        require(re.fullmatch('[0-9a-f]{40}', lines[1]), 'invalid source tree identity')
        if 'tree_sha' in trusted[side]:
            require(lines[1] == trusted[side]['tree_sha'], 'source tree mismatch')
        files = {}
        for line in lines[2:]:
            match = re.fullmatch(r'([0-9a-f]{64})  (.+)', line)
            require(match is not None, 'invalid source file inventory')
            file_sha, name = match.groups()
            require(name not in files, 'duplicate source file')
            files[name] = file_sha
        require(set(files) == set(SOURCE_FILES), 'source file set mismatch')
        if expected is None:
            require(files == SOURCE_FILES, 'pinned source file hash mismatch')
        artifacts = [json.loads(line) for line in (path / f'{side}-build.jsonl').read_text().splitlines()]
        executable = [a['executable'] for a in artifacts if a.get('reason') == 'compiler-artifact'
                      and a.get('target', {}).get('name') == 'wal_write_path' and a.get('executable')]
        require(executable == [m.get('executable')], 'compiler artifact/executable disagreement')
        manifests[side] = m
    return {'inventory_files': len(hashes), 'inventory': hashes, 'manifests': manifests}


def schedule():
    result = []
    for cell in CELLS:
        for phase in ('aa', 'ab'):
            for pair in range(3):
                sides = ('base', 'base') if phase == 'aa' else (('head', 'base') if pair == 1 else ('base', 'head'))
                for slot, side in enumerate(sides):
                    result.append(dict(index=len(result), cell=cell, phase=phase, pair=pair, slot=slot, side=side))
    return result


def validate_rows(rows, cell):
    require(len(rows) == 3, 'exactly three native rows required')
    keys = ('ops_s', 'mb_s', 'p50_us', 'p99_us', 'p999_us')
    if cell.startswith('stream_read/'):
        keys += ('records_s',)
    for row in rows:
        for key in keys:
            number = row.get(key)
            require(isinstance(number, (float, int)) and math.isfinite(number) and number > 0, f'invalid native {key}')
        require(row['p50_us'] <= row['p99_us'] <= row['p999_us'], 'unordered latency percentiles')


def parse_native(text, cell):
    require(cell in CELLS, 'unknown diagnostic cell')
    lines = text.splitlines()
    headers = [line for line in lines if line.startswith('- warm-up')]
    require(headers == [CONFIG_HEADER], 'wrong/duplicate native configuration')
    require(lines.count('# wal_write_path') == 1 and lines.count(WRITE_HEADER) == 1, 'wrong/duplicate write table')
    stream = cell.startswith('stream_read/')
    require(lines.count(STREAM_HEADER) == 1, 'missing/duplicate stream table')
    require(not any('UNJUDGEABLE' in line or 'page cache could not be dropped' in line for line in lines), 'unqualified native data/cache drop')
    rows = []
    for line in lines:
        if not line.startswith('| ') or line in (WRITE_HEADER, STREAM_HEADER):
            continue
        values = [v.strip() for v in line.strip('|').split('|')]
        require(values[0] == cell and len(values) == (10 if stream else 9), 'unexpected/malformed native cell')
        require(values[1] == ('8' if cell == CELLS[0] else '1'), 'wrong writer count')
        require(values[2] == ('1 KiB' if cell.endswith('/1k') else '64 KiB'), 'wrong value size')
        try:
            metrics = [float(v) for v in values[3:9 if stream else 8]]
        except ValueError as error:
            raise DiagnosticError('invalid numeric native metric') from error
        names = ('ops_s', 'mb_s', 'p50_us', 'p99_us', 'p999_us') + (('records_s',) if stream else ())
        rows.append(dict(zip(names, metrics)))
    validate_rows(rows, cell)
    return rows


def summarize(runs):
    ordinary = [run for run in runs if run.get('phase') != 'trace']
    require(len(ordinary) == len(schedule()), 'incomplete/extra untraced schedule')
    for run, planned in zip(ordinary, schedule()):
        require(all(run.get(k) == v for k, v in planned.items()), 'modified schedule/order')
        require(run.get('exit_code') == 0 and run.get('timing_eligible') is True
                and run.get('binary_side') == run['side'], 'unqualified native invocation')
        validate_rows(run['rows'], run['cell'])
    cells = {}
    for cell in CELLS:
        values = [run for run in ordinary if run['cell'] == cell]
        item = {'aa_ratios': [], 'ab_ratios': [], 'pairs': []}
        for phase in ('aa', 'ab'):
            for pair in range(3):
                two = [r for r in values if r['phase'] == phase and r['pair'] == pair]
                left, right = two if phase == 'aa' else (next(r for r in two if r['side'] == 'base'), next(r for r in two if r['side'] == 'head'))
                metrics = {key: statistics.median(row[key] for row in right['rows']) / statistics.median(row[key] for row in left['rows'])
                           for key in left['rows'][0]}
                item[phase + '_ratios'].append(metrics['mb_s' if cell.startswith('stream_read/') else 'ops_s'])
                item['pairs'].append({'phase': phase, 'pair': pair, 'ordered_sides': [r['side'] for r in two], 'ratios': metrics,
                                      'left_medians': {k: statistics.median(r[k] for r in left['rows']) for k in left['rows'][0]},
                                      'right_medians': {k: statistics.median(r[k] for r in right['rows']) for k in right['rows'][0]}})
        item['aa_repeatability_warning'] = any(r < .85 or r > 1 / .85 for r in item['aa_ratios'])
        item['consistent_isolated_write_loss'] = cell == CELLS[0] and all(r < .85 for r in item['ab_ratios']) and not item['aa_repeatability_warning']
        cells[cell] = item
    return {'diagnostic_only': True, 'cells': cells, 'cause': 'not established by timing ratios',
            'limits': 'Filtered workload history differs; shared runner; three pairs; original failed gate unchanged.'}


def fixed_environment(tmpdir):
    return {'PATH': os.environ.get('PATH', '/usr/bin:/bin'), 'LANG': 'C.UTF-8', 'TMPDIR': str(tmpdir),
            'SPIKE_REPS': '3', 'SPIKE_WARMUP_MS': '1000', 'SPIKE_MEASURE_MS': '3000', 'TOKIO_WORKER_THREADS': '4'}


def quiet():
    bad = []
    for p in Path('/proc').glob('[0-9]*/comm'):
        try:
            name = p.read_text().strip()
            if name in {'rustc', 'cargo', 'cc1', 'cc1plus', 'clang', 'gcc', 'perf'} or name.startswith('wal_write_path'):
                bad.append({'pid': int(p.parent.name), 'comm': name})
        except (OSError, ValueError):
            continue
    require(not bad, f'competing build/profile/benchmark processes: {bad}')


def host_snapshot(tmpdir):
    paths = ('/proc/stat', '/proc/diskstats', '/proc/meminfo', '/proc/loadavg',
             '/proc/pressure/cpu', '/proc/pressure/io', '/proc/pressure/memory',
             '/proc/self/mountinfo', '/proc/self/cgroup', '/sys/fs/cgroup/cpu.stat',
             '/sys/fs/cgroup/cpu.max', '/sys/fs/cgroup/io.stat', '/sys/fs/cgroup/memory.events')
    data = {'monotonic_ns': time.monotonic_ns(), 'utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
            'tmpdir': str(tmpdir), 'affinity': sorted(os.sched_getaffinity(0))}
    for name in paths:
        try:
            data[name] = Path(name).read_text()
        except OSError as error:
            data[name] = {'unavailable': str(error)}
    group = next((line.split('::', 1)[1] for line in str(data['/proc/self/cgroup']).splitlines() if line.startswith('0::')), None)
    if group is not None:
        current = Path('/sys/fs/cgroup') / group.lstrip('/')
        for name in ('cpu.stat', 'cpu.max', 'io.stat', 'memory.events'):
            try:
                data[f'current_cgroup/{name}'] = (current / name).read_text()
            except OSError as error:
                data[f'current_cgroup/{name}'] = {'unavailable': str(error)}
    df = subprocess.run(['df', '-PT', str(tmpdir)], capture_output=True, text=True, check=False)
    require(df.returncode == 0, 'cannot identify native temporary filesystem')
    data['tmpdir_df'] = df.stdout
    return data


def snapshot_after(directory, tmpdir):
    # Do not replace an invocation's original failure with a metadata failure.
    try:
        snapshot = host_snapshot(tmpdir)
    except (OSError, DiagnosticError) as error:
        snapshot = {'capture_error': str(error)}
    dump(directory / 'host-after.json', snapshot)
    return snapshot


def supervise(command, group_file, timeout=120):
    """Runs inside sudo, owning the actual command session rather than sudo's PTY."""
    require(os.geteuid() == 0, 'root supervisor required for privileged commands')
    require(0 < timeout <= 120 and command, 'invalid root command/containment budget')
    group_file = Path(group_file)
    require(not group_file.exists(), 'fresh root group witness required')
    p = subprocess.Popen(command, start_new_session=True)
    previous = {}
    def kill_group(sig):
        try:
            os.killpg(p.pid, sig)
        except ProcessLookupError:
            pass
    def interrupted(signum, frame):
        kill_group(signal.SIGKILL)
        raise DiagnosticError(f'root supervisor interrupted by {signum}')
    try:
        dump(group_file, {'supervisor_pid': os.getpid(), 'child_pid': p.pid, 'pgid': p.pid})
        group_file.chmod(0o644)
        for sig in (signal.SIGTERM, signal.SIGINT):
            previous[sig] = signal.signal(sig, interrupted)
        try:
            return p.wait(timeout=timeout)
        except subprocess.TimeoutExpired as error:
            kill_group(signal.SIGTERM)
            try:
                p.wait(timeout=1)
            except subprocess.TimeoutExpired:
                pass
            raise DiagnosticError('root command exceeded containment budget') from error
    finally:
        # Even if the parent exited on TERM, a descendant may have ignored it.
        kill_group(signal.SIGKILL)
        p.wait(timeout=5)
        for sig, handler in previous.items():
            signal.signal(sig, handler)


def root_command(command, env, directory, timeout=120):
    return ['sudo', '-n', 'env', '-i'] + [f'{k}={v}' for k, v in sorted(env.items())] + [
        'python3', str(Path(__file__).resolve()), 'supervise', '--group-file', str(directory / 'root-group.json'),
        '--timeout', str(timeout), '--', *command]


def execute(command, env, directory, privileged=False, timeout=120):
    directory.mkdir(exist_ok=True)
    require(not (directory / 'exit.json').exists(), 'cannot overwrite prior invocation')
    dump(directory / 'command.json', {'argv': command, 'environment': env, 'privileged': privileged, 'timeout_s': timeout})
    begin = time.monotonic_ns()
    timed_out = False
    with (directory / 'stdout.txt').open('w') as out, (directory / 'stderr.txt').open('w') as err:
        p = subprocess.Popen(command, env=env, stdout=out, stderr=err, start_new_session=True)
        try:
            # Root supervisor has its own unchanged120s limit; allow only its
            # termination/reaping margin before this outer failsafe.
            code = p.wait(timeout=timeout + (15 if privileged else 0))
        except subprocess.TimeoutExpired:
            timed_out = True
            if privileged:
                witness = json.loads((directory / 'root-group.json').read_text())
                require(witness['pgid'] == witness['child_pid'] and witness['pgid'] > 1, 'invalid actual root group witness')
                for sig in ('-TERM', '-KILL'):
                    subprocess.run(['sudo', '-n', 'kill', sig, '--', f'-{witness["pgid"]}'], check=False, capture_output=True)
                os.killpg(p.pid, signal.SIGTERM)
            else:
                os.killpg(p.pid, signal.SIGTERM)
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                pass
            # The parent may exit before a descendant that ignores TERM. Always
            # finish containing the whole owned process group, not just the parent.
            if privileged:
                try:
                    os.killpg(p.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            else:
                try:
                    os.killpg(p.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            p.wait(timeout=5)
            code = p.returncode
    data = {'exit_code': code, 'timed_out': timed_out, 'begin_ns': begin, 'end_ns': time.monotonic_ns(), 'pid': p.pid}
    dump(directory / 'exit.json', data)
    require(not timed_out and code == 0, f'invocation failed/contained: {directory.name}, exit={code}, timeout={timed_out}')
    return data


def trace_command(binary, directory, env):
    command = ['perf', 'record', '-a', '--clockid', 'mono', '-m', '32M', '-o', str(directory / 'waits.data')]
    for event in EVENTS:
        command += ['-e', event]
    command += ['--', 'perf', 'record', '--clockid', 'mono', '--strict-freq', '--sample-cpu', '-e', 'cpu-clock:u', '-F', '99',
                '--call-graph', 'dwarf,8192', '-o', str(directory / 'cpu.data'), '--', str(binary)]
    return root_command(command, env, directory)


def analyze_waits(text, binary):
    require(not LOSS.search(text), 'lost/throttled trace')
    events = []
    pattern = re.compile(r'^\s*.+?\s+(\d+)/(\d+)\s+\[(\d+)\]\s+(\d+)\.(\d+):\s+(\w+:\w+):\s*(.*)$')
    for line in text.splitlines():
        if not line.strip() or line.startswith('#'):
            continue
        match = pattern.match(line)
        require(match is not None, 'unparsed trace line')
        pid, tid, cpu, seconds, fraction, event, body = match.groups()
        stamp = int(seconds) * 1000000000 + int(fraction.ljust(9, '0'))
        fields = dict(re.findall(r'(\w+)=([^\s]+)', body))
        events.append((stamp, int(pid), int(tid), event, fields))
    require(events and all(a[0] <= b[0] for a, b in zip(events, events[1:])), 'empty/out-of-order trace')
    execs = [e for e in events if e[3] == 'sched:sched_process_exec' and e[4].get('filename') == str(binary)]
    require(len(execs) == 1, 'exact native executable lifecycle not uniquely recorded')
    leader = int(execs[0][4]['pid'])
    tids = {leader}
    forks = [e[4] for e in events if e[3] == 'sched:sched_process_fork']
    while True:
        children = {int(f['child_pid']) for f in forks if int(f['parent_pid']) in tids}
        if children <= tids:
            break
        tids |= children
    calls, pending, sleeping, wakes = {}, {}, {}, {}
    offcpu, ready, blocks = {}, [], collections.Counter()
    unmatched_exits = 0
    for stamp, pid, tid, event, fields in events:
        if event.startswith('block:'):
            blocks[event] += 1
        if event.startswith('syscalls:sys_') and tid in tids:
            phase, name = event.removeprefix('syscalls:sys_').split('_', 1)
            key = (tid, name)
            if phase == 'enter':
                pending[key] = stamp
            elif phase == 'exit':
                begin = pending.pop(key, None)
                if begin is None:
                    unmatched_exits += 1
                else:
                    calls.setdefault(name, []).append((stamp - begin) / 1000000)
        if event == 'sched:sched_wakeup' and int(fields.get('pid', -1)) in tids:
            wakes[int(fields['pid'])] = stamp
        if event == 'sched:sched_switch':
            previous, next_tid = int(fields['prev_pid']), int(fields['next_pid'])
            if previous in tids:
                sleeping[previous] = (stamp, fields['prev_state'])
            if next_tid in tids:
                before = sleeping.pop(next_tid, None)
                if before is not None:
                    offcpu.setdefault(before[1], []).append((stamp - before[0]) / 1000000)
                wake = wakes.pop(next_tid, None)
                if wake is not None:
                    ready.append((stamp - wake) / 1000000)
    def totals(values):
        return {'count': len(values), 'total_ms': sum(values), 'max_ms': max(values, default=0)}
    return {'native_pid': leader, 'thread_ids': sorted(tids), 'syscalls': {k: totals(v) for k, v in calls.items()},
            'offcpu_by_state': {k: totals(v) for k, v in offcpu.items()}, 'wakeup_to_run': totals(ready),
            'system_block_events': dict(blocks), 'unmatched_syscall_exits': unmatched_exits,
            'unfinished_syscalls': len(pending), 'whole_process_only': True,
            'limits': 'Matched intervals only; sums across threads; block events are system-wide; no measured-phase or specific-lock proof.'}


def render_trace(directory):
    commands = {
        'cpu-report': ['perf', 'report', '-i', str(directory / 'cpu.data'), '--stdio', '--no-children', '--sort', 'dso,symbol'],
        'cpu-script': ['perf', 'script', '-i', str(directory / 'cpu.data'), '--ns', '-F', 'comm,pid,tid,cpu,time,event'],
        'waits-script': ['perf', 'script', '-i', str(directory / 'waits.data'), '--ns', '-F', 'comm,pid,tid,cpu,time,event,trace'],
        'cpu-raw-events': ['perf', 'script', '-i', str(directory / 'cpu.data'), '-D'],
        'waits-raw-events': ['perf', 'script', '-i', str(directory / 'waits.data'), '-D'],
    }
    diagnostics = (directory / 'stderr.txt').read_text()
    for name, command in commands.items():
        child = directory / f'render-{name}'
        env = {'PATH': os.environ.get('PATH', '/usr/bin:/bin'), 'LANG': 'C.UTF-8'}
        meta = execute(root_command(command, env, child), env, child, privileged=True)
        shutil.copyfile(child / 'stdout.txt', directory / f'{name}.txt')
        shutil.copyfile(child / 'stderr.txt', directory / f'{name}.stderr')
        dump(directory / f'{name}-exit.json', {'exit_code': meta['exit_code'], 'argv': command})
        diagnostics += (directory / f'{name}.stderr').read_text()
    lost = bool(LOSS.search(diagnostics))
    for name in ('cpu-raw-events', 'waits-raw-events'):
        with (directory / f'{name}.txt').open() as source:
            lost = lost or any(LOSS.search(line) for line in source)
    dump(directory / 'trace-quality.json', {'loss_or_throttling': lost, 'whole_process_only': True,
                                          'timing_eligible': False, 'source_line_precision': False})
    require(not lost, 'lost/throttled trace: complete wait attribution unavailable')


def preflight(tmpdir):
    require(platform.system() == 'Linux' and platform.machine() == 'x86_64', 'qualified x86_64 Linux runner required')
    for name, install in (('perf', 'sudo apt-get install linux-tools-common linux-tools-generic'), ('sudo', 'sudo apt-get install sudo'),
                          ('df', 'sudo apt-get install coreutils'), ('ldd', 'sudo apt-get install libc-bin')):
        require(shutil.which(name), f'missing {name}; maintainer install command: {install}')
    require(len(os.sched_getaffinity(0)) == 4, 'original benchmark used four available CPUs; runner differs')
    quiet()
    return {'kernel': platform.release(), 'platform': platform.platform(), 'cpuinfo': Path('/proc/cpuinfo').read_text(),
            'perf': subprocess.check_output(['perf', '--version'], text=True).strip(),
            'perf_event_paranoid': Path('/proc/sys/kernel/perf_event_paranoid').read_text().strip(),
            'host': host_snapshot(tmpdir), 'no_sysctl_change': True}


def save_inventory(output):
    entries = []
    for p in sorted(output.rglob('*')):
        if p.is_file() and p.name != 'SHA256SUMS' and not p.is_symlink():
            entries.append(f'{digest(p)}  {p.relative_to(output).as_posix()}\n')
    (output / 'SHA256SUMS').write_text(''.join(entries))


def run(artifact, output):
    output = Path(output).resolve()
    require(not output.exists(), 'fresh diagnostic output required; no retry/overwrite')
    output.mkdir(parents=True)
    runs = []
    result = {'diagnostic_only': True, 'verdict': 'inconclusive', 'original_acceptance': 'FAILED, unchanged'}
    try:
        proof = verify_artifact(artifact)
        dump(output / 'original-qualified.json', proof)
        runtime = output / 'runtime'
        runtime.mkdir()
        dump(output / 'preflight.json', preflight(runtime))
        binaries = {}
        for side in ('base', 'head'):
            binary = output / f'{side}-benchmark-executable'
            shutil.copyfile(Path(artifact) / f'{side}-benchmark-executable', binary)
            binary.chmod(0o755)
            binaries[side] = binary
            linked = subprocess.run(['ldd', str(binary)], capture_output=True, text=True, check=False)
            dump(output / f'{side}-runtime-libraries.json', {'argv': ['ldd', str(binary)], 'stdout': linked.stdout,
                                                           'stderr': linked.stderr, 'exit_code': linked.returncode})
            require(linked.returncode == 0 and 'not found' not in linked.stdout + linked.stderr, 'native library qualification failed')
        dump(output / 'schedule.json', schedule())
        for planned in schedule():
            side, cell = planned['side'], planned['cell']
            quiet()
            require(digest(binaries[side]) == PINNED[side]['binary_sha256'], 'binary changed before invocation')
            directory = output / f'native-{planned["index"]:02d}'
            env = fixed_environment(runtime)
            env['SPIKE_FILTER'] = cell
            before = host_snapshot(runtime)
            directory.mkdir()
            dump(directory / 'host-before.json', before)
            try:
                meta = execute([str(binaries[side])], env, directory)
            finally:
                after = snapshot_after(directory, runtime)
            require('capture_error' not in after, 'native after-snapshot unavailable')
            require(digest(binaries[side]) == PINNED[side]['binary_sha256'], 'binary changed during invocation')
            rows = parse_native((directory / 'stdout.txt').read_text(), cell)
            runs.append(dict(planned, rows=rows, exit_code=meta['exit_code'], timing_eligible=True,
                             binary_side=side, binary_sha256=digest(binaries[side]), elapsed_ns=meta['end_ns'] - meta['begin_ns']))
            dump(output / 'runs.json', runs)
        result['timing_summary'] = summarize(runs)
        dump(output / 'result.json', result)
        # Exact event qualification after native comparisons, before any diagnostic recording.
        directory = output / 'trace-preflight'
        env = {'PATH': os.environ.get('PATH', '/usr/bin:/bin'), 'LANG': 'C.UTF-8'}
        command = root_command(['perf', 'stat', '-a', '-e', ','.join(EVENTS), '--', 'true'], env, directory)
        execute(command, env, directory, privileged=True)
        for cell in CELLS[:2]:
            for side in ('base', 'head'):
                quiet()
                env = fixed_environment(runtime)
                env['SPIKE_FILTER'] = cell
                directory = output / f'trace-{cell.replace("/", "-")}-{side}'
                before = host_snapshot(runtime)
                directory.mkdir()
                dump(directory / 'host-before.json', before)
                try:
                    meta = execute(trace_command(binaries[side], directory, env), env, directory, privileged=True)
                finally:
                    after = snapshot_after(directory, runtime)
                require('capture_error' not in after, 'trace after-snapshot unavailable')
                # perf writes mode0600 as root; transfer only these two recordings
                # so normal runner hashing/upload can retain the actual evidence.
                owned = [str(directory / name) for name in ('cpu.data', 'waits.data')]
                subprocess.run(['sudo', '-n', 'chown', f'{os.getuid()}:{os.getgid()}', '--', *owned], check=True)
                require(digest(binaries[side]) == PINNED[side]['binary_sha256'], 'profile binary changed')
                rows = parse_native((directory / 'stdout.txt').read_text(), cell)
                render_trace(directory)
                dump(directory / 'wait-summary.json', analyze_waits((directory / 'waits-script.txt').read_text(), binaries[side]))
                runs.append({'phase': 'trace', 'cell': cell, 'side': side, 'binary_side': side, 'rows': rows,
                             'exit_code': meta['exit_code'], 'timing_eligible': False, 'directory': directory.name})
                dump(output / 'runs.json', runs)
        result['trace_capture_complete'] = True
        result['verdict'] = 'evidence collected; causal interpretation pending independent review'
        dump(output / 'result.json', result)
        print(json.dumps(result, indent=2))
    except (DiagnosticError, OSError, ValueError, KeyError, subprocess.SubprocessError) as error:
        result['error'] = str(error)
        dump(output / 'result.json', result)
        raise DiagnosticError(str(error)) from error
    finally:
        for directory in output.glob('trace-*'):
            for name in ('cpu.data', 'waits.data'):
                p = directory / name
                if p.is_file() and not p.is_symlink() and p.stat().st_uid != os.getuid():
                    subprocess.run(['sudo', '-n', 'chown', f'{os.getuid()}:{os.getgid()}', '--', str(p)], check=True)
        save_inventory(output)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    verify = sub.add_parser('verify')
    verify.add_argument('--artifact', required=True)
    execute_parser = sub.add_parser('run')
    execute_parser.add_argument('--artifact', required=True)
    execute_parser.add_argument('--output', required=True)
    supervisor = sub.add_parser('supervise')
    supervisor.add_argument('--group-file', required=True)
    supervisor.add_argument('--timeout', type=float, default=120)
    supervisor.add_argument('argv', nargs=argparse.REMAINDER)
    args = parser.parse_args()
    try:
        if args.command == 'verify':
            print(json.dumps(verify_artifact(args.artifact), indent=2))
        elif args.command == 'run':
            run(args.artifact, args.output)
        else:
            command = args.argv[1:] if args.argv[:1] == ['--'] else args.argv
            return supervise(command, args.group_file, args.timeout)
    except (DiagnosticError, OSError, ValueError, KeyError) as error:
        print(f'UNQUALIFIED/INCONCLUSIVE: {error}', file=sys.stderr)
        return 2
    return 0


if __name__ == '__main__':
    sys.exit(main())
