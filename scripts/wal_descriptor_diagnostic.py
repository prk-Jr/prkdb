#!/usr/bin/env python3
"""Finite descriptor-qualified profiling. Never supplies gate acceptance."""
import argparse
import collections
import ctypes
import json
import os
from pathlib import Path
import platform
import re
import resource
import shutil
import signal
import subprocess
import sys
import threading
import time

from wal_stall_diagnostic import (DiagnosticError, LOSS, PINNED, digest, dump,
                                  fixed_environment, host_snapshot, parse_native,
                                  require, save_inventory, supervise, verify_artifact)

CELLS = ('adapter_put/8w/64k', 'stream_read/cold/64k')
RAW_LIMITS = {'cpu.data': 64 << 20, 'syscalls.data': 128 << 20, 'waits.data': 128 << 20}
RAW_TOTAL = 1 << 30
RENDER_FILE = 512 << 20
RENDER_TOTAL = 2 << 30
INTERVAL = .1
NAMES = ('wal-diag-*', 'tokio*', 'prkdb-wal-*')
SYSCALLS = ('futex', 'read', 'pread64', 'write', 'pwrite64', 'fsync', 'fdatasync')
LIFECYCLE = ('sched:sched_process_exec', 'sched:sched_process_fork', 'sched:sched_process_exit')
BLOCK = ('block:block_rq_issue', 'block:block_rq_complete')
RENAME = 'task:task_rename'
SELF = str(Path(__file__).resolve())


def sequence():
    return [dict(index=index, phase=phase, cell=cell, side=side, timing_eligible=False)
            for index, (phase, cell, side) in enumerate(
                (phase, cell, side) for phase in ('control', 'profile')
                for cell in CELLS for side in ('base', 'head'))]


def environment(tmpdir, cell):
    require(cell in CELLS, 'unexpected selected cell')
    return dict(fixed_environment(tmpdir), SPIKE_FILTER=cell)


def fd_state(pid):
    numbers = [int(p.name) for p in (Path('/proc') / str(pid) / 'fd').iterdir() if p.name.isdigit()]
    return {'fd_count': len(numbers), 'max_fd': max(numbers, default=-1)}


def identity_from_proc(status, limits):
    data = {}
    for name, key in (('Uid', 'uids'), ('Gid', 'gids'), ('Groups', 'groups')):
        match = re.search(rf'^{name}:\s*(.*)$', status, re.M)
        require(match is not None, f'missing native /proc {name}')
        data[key] = [int(n) for n in match.group(1).split()]
    data.update(uid=data['uids'][0], gid=data['gids'][0])
    matches = re.findall(r'^Max open files[ \t]+(\d+|unlimited)[ \t]+(\d+|unlimited)[ \t]+files[ \t]*$', limits, re.M)
    require(len(matches) == 1, 'missing or ambiguous actual native NOFILE pair')
    data['nofile'] = [resource.RLIM_INFINITY if n == 'unlimited' else int(n) for n in matches[0]]
    return data


def proc_identity(pid, evidence_directory=None):
    directory = Path('/proc') / str(pid)
    status, limits = (directory / 'status').read_text(), (directory / 'limits').read_text()
    if evidence_directory is not None:
        # Retain the exact kernel records even when decoding fails.
        (Path(evidence_directory) / 'native-status.txt').write_text(status)
        (Path(evidence_directory) / 'native-limits.txt').write_text(limits)
    return identity_from_proc(status, limits)


def resource_proof():
    if Path('/proc/self/fd').is_dir():
        fds = fd_state('self')
    else:
        # Portable unit controls only; production preflight requires Linux /proc.
        numbers = []
        for name in Path('/dev/fd').iterdir():
            try:
                os.fstat(int(name.name))
                numbers.append(int(name.name))
            except (OSError, ValueError):
                pass
        fds = {'fd_count': len(numbers), 'max_fd': max(numbers, default=-1)}
    data = dict(pid=os.getpid(), uid=os.getuid(), gid=os.getgid(), groups=sorted(os.getgroups()),
                nofile=list(resource.getrlimit(resource.RLIMIT_NOFILE)), monotonic_ns=time.monotonic_ns(),
                limits={name: list(resource.getrlimit(getattr(resource, name)))
                        for name in sorted(dir(resource)) if name.startswith('RLIMIT_')}, **fds)
    if Path('/proc/self/status').is_file():
        status = Path('/proc/self/status').read_text()
        for name, key in (('Uid', 'uids'), ('Gid', 'gids')):
            data[key] = [int(n) for n in re.search(rf'^{name}:\s+(.+)$', status, re.M).group(1).split()]
    else:
        data.update(uids=[os.getuid(), os.geteuid(), os.geteuid(), os.geteuid()],
                    gids=[os.getgid(), os.getegid(), os.getegid(), os.getegid()])
    return data


def propagate_nofile(ordinary):
    pair = ordinary.get('nofile')
    require(isinstance(pair, list) and len(pair) == 2 and all(isinstance(n, int) for n in pair), 'invalid ordinary NOFILE proof')
    resource.setrlimit(resource.RLIMIT_NOFILE, tuple(pair))
    require(list(resource.getrlimit(resource.RLIMIT_NOFILE)) == pair, 'child NOFILE propagation failed')


def validate_identity(actual, ordinary):
    for field in ('uid', 'gid', 'nofile'):
        require(field in actual and actual[field] == ordinary[field], f'native {field} identity/resource mismatch')
    require(sorted(actual.get('groups', [])) == sorted(ordinary['groups']), 'native supplementary group mismatch')
    require(actual.get('uids') == [ordinary['uid']] * 4, 'native UID drop incomplete')
    require(actual.get('gids') == [ordinary['gid']] * 4, 'native GID drop incomplete')


def atomic_dump(path, value):
    path = Path(path)
    temporary = path.with_suffix(path.suffix + '.pending')
    dump(temporary, value)
    temporary.chmod(0o644)
    temporary.replace(path)


def scoped(ordinary_path, directory, binary, sha256, arguments):
    ordinary = json.loads(Path(ordinary_path).read_text())
    require(list(resource.getrlimit(resource.RLIMIT_NOFILE)) == ordinary['nofile'], 'inherited NOFILE differs from ordinary pair')
    require(digest(binary) == sha256, 'scoped executable hash differs')
    if os.geteuid() == 0 and ordinary['uid'] != 0:
        os.setgroups(ordinary['groups'])
        os.setgid(ordinary['gid'])
        os.setuid(ordinary['uid'])
    final = resource_proof()
    validate_identity(final, ordinary)
    final.update(binary=str(Path(binary).resolve()), binary_sha256=sha256, preexec_only=True,
                 argv=[str(binary), *arguments])
    atomic_dump(Path(directory) / 'native-launch.json', final)
    # The native proof above is deliberately only a launch proof. /proc and perf
    # lifecycle must independently establish actual exec before attribution.
    os.execve(str(binary), [str(binary), *arguments], dict(os.environ))


def scoped_command(binary, directory, ordinary_path, sha256, arguments=()):
    return ['python3', SELF, 'scoped', '--proof', str(ordinary_path), '--directory', str(directory),
            '--binary', str(binary), '--sha256', sha256, '--', *arguments]


def filter_for(fields):
    return ' || '.join(f'{field} ~ "{pattern}"' for field in fields for pattern in NAMES)


def profile_command(binary, directory, ordinary_path, sha256, arguments=()):
    require(len(Path(binary).name.encode()) <= 15 and Path(binary).name.startswith('wal-diag-'), 'native alias must fit Linux comm')
    command = ['perf', 'record', '-a', '--clockid', 'mono', '--sample-cpu', '-m', '8M', '-o', str(directory / 'waits.data'),
               '-e', 'sched:sched_switch', '--filter', filter_for(('prev_comm', 'next_comm')),
               '-e', 'sched:sched_wakeup', '--filter', filter_for(('comm',))]
    for event in (*LIFECYCLE, RENAME, *BLOCK):
        command += ['-e', event]
    command += ['--', 'perf', 'record', '--clockid', 'mono', '--sample-cpu', '-c', '1', '-m', '8M',
                '-o', str(directory / 'syscalls.data')]
    for name in SYSCALLS:
        for phase in ('enter', 'exit'):
            command += ['-e', f'syscalls:sys_{phase}_{name}']
    command += ['--', 'perf', 'record', '--clockid', 'mono', '--strict-freq', '--sample-cpu', '-e', 'cpu-clock:u',
                '-F', '99', '--call-graph', 'dwarf,8192', '-o', str(directory / 'cpu.data'), '--',
                *scoped_command(binary, directory, ordinary_path, sha256, arguments)]
    return command


def root_command(command, env, directory, ordinary_path, output=None):
    require(set(env) <= {'PATH', 'LANG', 'TMPDIR', 'SPIKE_REPS', 'SPIKE_WARMUP_MS', 'SPIKE_MEASURE_MS',
                         'TOKIO_WORKER_THREADS', 'SPIKE_FILTER'}, 'nonallowlisted privileged environment')
    return ['sudo', '-n', 'env', '-i', *[f'{k}={v}' for k, v in sorted(env.items())],
            'python3', SELF, 'supervisor', '--proof', str(ordinary_path), '--directory', str(directory),
            '--output', str(output or directory.parent), '--', *command]


def budget_breach(directory, output, limits=None, total_limit=None):
    limits = RAW_LIMITS if limits is None else limits
    # Optional explicit caps let unit controls exercise the real file accounting
    # with small files, without weakening the CLI's fixed production budgets.
    if total_limit is not None:
        total = 0
        for path in Path(directory).rglob('*'):
            if path.is_file() and not path.is_symlink():
                size = path.stat().st_size
                if path.name in limits and size > limits[path.name]:
                    return f'file budget exceeded: {path.name}'
                total += size
        return 'total budget exceeded' if total > total_limit else None
    raw, rendered = 0, 0
    for path in Path(output).rglob('*'):
        relative = path.relative_to(output)
        if relative.parts[0] == 'runtime' or (len(relative.parts) == 1 and path.name.startswith('wal-diag-')):
            continue
        if not path.is_file() or path.is_symlink():
            continue
        size = path.stat().st_size
        is_raw = path.name in RAW_LIMITS
        if is_raw:
            raw += size
            if size > limits[path.name]:
                return f'raw file budget exceeded: {path}'
        else:
            rendered += size
            if size > RENDER_FILE:
                return f'rendered file budget exceeded: {path}'
    if raw > RAW_TOTAL:
        return 'overall raw budget exceeded'
    if rendered > RENDER_TOTAL:
        return 'overall rendered budget exceeded'
    return None


def fd_summary(samples, unavailable):
    return {'samples': samples, 'sampling_interval_ns': int(INTERVAL * 1e9), 'unavailable_samples': unavailable,
            'max_gap_ns': max((b['monotonic_ns'] - a['monotonic_ns'] for a, b in zip(samples, samples[1:])), default=0),
            'max_observed_fd_count': max((s['fd_count'] for s in samples), default=None),
            'max_observed_fd_number': max((s['max_fd'] for s in samples), default=None),
            'proves_absolute_peak': False, 'limits': '100ms observations can miss peaks; gaps and preexec observations are retained.'}


class CaptureWatch:
    """Monitor actual native exec and finite files; never retries an invocation."""
    def __init__(self, directory, output, kill):
        self.directory, self.output, self.kill = Path(directory), Path(output), kill
        self.done = threading.Event()
        self.samples, self.unavailable, self.error, self.actual_exec = [], 0, None, None
        self.unavailable_observations = []
        self.thread = threading.Thread(target=self.watch, name='descriptor-watch', daemon=True)

    def start(self):
        self.thread.start()

    def watch(self):
        try:
            while True:
                breach = budget_breach(self.directory, self.output)
                if breach:
                    raise DiagnosticError(breach)
                launch_file = self.directory / 'native-launch.json'
                if launch_file.exists():
                    launch = json.loads(launch_file.read_text())
                    pid = launch['pid']
                    try:
                        executable = str((Path('/proc') / str(pid) / 'exe').resolve(strict=True))
                        native = executable == launch['binary']
                        observation = dict(monotonic_ns=time.monotonic_ns(), pid=pid, executable=executable,
                                           phase='native-exec' if native else 'preexec', **fd_state(pid))
                        self.samples.append(observation)
                        if native and self.actual_exec is None:
                            identity = proc_identity(pid, self.directory)
                            validate_identity(identity, launch)
                            self.actual_exec = dict(observation, launch_sha256=launch['binary_sha256'], **identity)
                            atomic_dump(self.directory / 'native-exec-observed.json', self.actual_exec)
                    except OSError as error:
                        # setuid/exec changes /proc access briefly. Record the gap;
                        # successful actual-exec coverage is still mandatory later.
                        self.unavailable += 1
                        self.unavailable_observations.append(dict(monotonic_ns=time.monotonic_ns(), pid=pid, error=str(error)))
                if self.done.wait(INTERVAL):
                    break
        except (DiagnosticError, OSError, ValueError, KeyError) as error:
            self.error = str(error)
            atomic_dump(self.directory / 'capture-failure.json', {'error': self.error, 'inconclusive': True})
            # Witness publication can lag Popen by a few instructions. This is
            # containment of the same invocation, never a benchmark retry.
            while self.kill() is False and not self.done.wait(.01):
                pass

    def finish(self):
        self.done.set()
        self.thread.join(timeout=5)
        require(not self.thread.is_alive(), 'capture monitor did not stop')
        atomic_dump(self.directory / 'native-fd-series.json', dict(fd_summary(self.samples, self.unavailable),
                                                                unavailable_observations=self.unavailable_observations))
        require(not self.error, self.error)
        require(not budget_breach(self.directory, self.output), 'postexit evidence budget exceeded')


def kill_group(pgid):
    require(isinstance(pgid, int) and pgid > 1 and pgid != os.getpgrp(), 'invalid owned group')
    try:
        os.killpg(pgid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    return True


def read_owned_group(witness):
    """None means publication is incomplete; never invent ownership from a prefix."""
    try:
        group = json.loads(Path(witness).read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return None
    pgid = group.get('pgid')
    require(isinstance(pgid, int) and pgid > 1 and group.get('child_pid') == pgid, 'invalid actual root group witness')
    return pgid


def qualified_supervisor(ordinary_path, directory, output, command):
    require(os.geteuid() == 0, 'root supervisor required')
    directory = Path(directory)
    ordinary = json.loads(Path(ordinary_path).read_text())
    before = resource_proof()
    dump(directory / 'root-before.json', before)
    witness = directory / 'root-group.json'
    def stop():
        pgid = read_owned_group(witness)
        return False if pgid is None else kill_group(pgid)
    require(not budget_breach(directory, output), 'evidence budget exceeded before root launch')
    watch = CaptureWatch(directory, output, stop)
    watch.start()
    code = None
    try:
        # Reviewed helper owns the actual session and its descendant cleanup. Only
        # its child restores NOFILE; sudo/root supervisor limits remain unchanged.
        child = ['python3', SELF, 'child-limit', '--proof', str(ordinary_path), '--directory', str(directory), '--', *command]
        code = supervise(child, witness, timeout=120)
    finally:
        after = resource_proof()
        dump(directory / 'root-after.json', after)
        watch.finish()
        require(before['nofile'] == after['nofile'], 'root supervisor NOFILE unexpectedly changed')
    require(code == 0, f'root recording/rendering child failed: {code}')
    child_proof = json.loads((directory / 'child-limits.json').read_text())
    require(child_proof['after']['nofile'] == ordinary['nofile'], 'missing child propagation proof')
    return code


def child_limit(ordinary_path, directory, command):
    ordinary = json.loads(Path(ordinary_path).read_text())
    before = resource_proof()
    propagate_nofile(ordinary)
    after = resource_proof()
    atomic_dump(Path(directory) / 'child-limits.json', {'before': before, 'after': after})
    os.execvpe(command[0], command, dict(os.environ))


def contain_root(directory):
    """Retain signal/probe results; sudo failure never establishes containment."""
    directory = Path(directory)
    proof = dict(actual_group_known=False, qualified_cleanup=False, uncontained=True,
                 supervisor_gone=False, signal=None, probe=None)
    def call(argv):
        result = subprocess.run(argv, check=False, capture_output=True, text=True)
        return dict(argv=argv, exit_code=result.returncode, stdout=result.stdout, stderr=result.stderr)
    def absent(result):
        return result['exit_code'] != 0 and 'no such process' in result['stderr'].lower()
    try:
        pgid = read_owned_group(directory / 'root-group.json')
        if pgid is not None:
            proof['actual_group_known'] = True
            target, signum = f'-{pgid}', '-KILL'
        else:
            before = directory / 'root-before.json'
            require(before.is_file(), 'root containment supervisor identity unavailable')
            pid = json.loads(before.read_text())['pid']
            require(isinstance(pid, int) and pid > 1, 'invalid root supervisor identity')
            target, signum = str(pid), '-TERM'
        proof['signal'] = call(['sudo', '-n', 'kill', signum, '--', target])
        proof['probe'] = call(['sudo', '-n', 'kill', '-0', '--', target])
        signal_accepted = proof['signal']['exit_code'] == 0 or absent(proof['signal'])
        target_gone = absent(proof['probe'])
        proof['supervisor_gone'] = target_gone if not proof['actual_group_known'] else False
        proof['qualified_cleanup'] = proof['actual_group_known'] and signal_accepted and target_gone
        proof['uncontained'] = not proof['qualified_cleanup']
    except (DiagnosticError, OSError, ValueError, KeyError, subprocess.SubprocessError) as error:
        proof['error'] = str(error)
    finally:
        dump(directory / 'root-containment.json', proof)
    require(proof['qualified_cleanup'], 'root containment unqualified; retained signal/probe results or missing group identity')
    return proof


def execute(command, env, directory, output, privileged=False):
    directory = Path(directory)
    require(directory.is_dir() and not (directory / 'exit.json').exists(), 'fresh prepared invocation required')
    dump(directory / 'command.json', dict(argv=command, environment=env, privileged=privileged, timeout_s=120))
    begin, timed_out, p, code, failure, watch = time.monotonic_ns(), False, None, None, None, None
    cleanup_errors = []
    try:
        with (directory / 'stdout.txt').open('w') as stdout, (directory / 'stderr.txt').open('w') as stderr:
            p = subprocess.Popen(command, env=env, stdout=stdout, stderr=stderr, start_new_session=True)
            watch = None if privileged else CaptureWatch(directory, output, lambda: kill_group(p.pid))
            if watch:
                watch.start()
            try:
                code = p.wait(timeout=135 if privileged else 120)
            except subprocess.TimeoutExpired:
                timed_out = True
                if privileged:
                    contain_root(directory)
    except (DiagnosticError, OSError, ValueError, KeyError, subprocess.SubprocessError) as error:
        failure = str(error)
    finally:
        if p is not None:
            try:
                kill_group(p.pid)
            except (DiagnosticError, OSError) as error:
                cleanup_errors.append(str(error))
                if privileged:
                    try:
                        contain_root(directory)
                    except (DiagnosticError, OSError) as root_error:
                        cleanup_errors.append(str(root_error))
            try:
                code = p.wait(timeout=5)
            except (OSError, subprocess.SubprocessError) as error:
                cleanup_errors.append(str(error))
            if watch:
                try:
                    watch.finish()
                except (DiagnosticError, OSError, ValueError, KeyError) as error:
                    failure = failure or str(error)
        failure = failure or ('outer cleanup unqualified: ' + '; '.join(cleanup_errors) if cleanup_errors else None)
        result = dict(exit_code=code, timed_out=timed_out, begin_ns=begin, end_ns=time.monotonic_ns(),
                      pid=None if p is None else p.pid, error=failure, cleanup_errors=cleanup_errors,
                      outer_cleanup_complete=not cleanup_errors)
        dump(directory / 'exit.json', result)
    require(not failure, failure)
    require(not timed_out and code == 0, f'invocation failed/contained: {directory.name}, exit={code}')
    require(not budget_breach(directory, output), 'postexit evidence budget exceeded')
    return result


def validate_native_evidence(directory, ordinary=None):
    directory = Path(directory)
    for name in ('native-launch.json', 'native-exec-observed.json', 'native-fd-series.json'):
        require((directory / name).is_file(), f'missing actual native evidence: {name}')
    launch = json.loads((directory / 'native-launch.json').read_text())
    actual = json.loads((directory / 'native-exec-observed.json').read_text())
    series = json.loads((directory / 'native-fd-series.json').read_text())
    require(actual.get('pid') == launch.get('pid') and actual.get('executable') == launch.get('binary')
            and actual.get('launch_sha256') == launch.get('binary_sha256'), 'actual native exec proof mismatch')
    require(any(row.get('phase') == 'native-exec' for row in series.get('samples', [])), 'no actual native FD observations')
    if ordinary is not None:
        validate_identity(launch, ordinary)
        validate_identity(actual, ordinary)
    return launch


PREFIX = re.compile(r'^\s*(.*?)\s+(-?\d+)/\s*(-?\d+)\s+\[(-?\d+)\]\s+(\d+)\.(\d{9}):\s+(.*)$')
COMM = re.compile(r'^PERF_RECORD_COMM( exec)?: (.+):(\d+)/(\d+)$')
TASK = re.compile(r'^PERF_RECORD_(FORK|EXIT)\((\d+):(\d+)\):\((\d+):(\d+)\)$')
SAMPLE = re.compile(r'^([\w:-]+):\s+(.*)$')


def scheduler_fields(event, detail):
    # libtraceevent's sched plugin prints compact bodies, overriding TP_printk.
    if event == 'sched:sched_switch':
        patterns = (
            r'prev_comm=(.+) prev_pid=(\d+) prev_prio=(\d+) prev_state=(\S+) ==> next_comm=(.+) next_pid=(\d+) next_prio=(\d+)',
            r'(.+):(\d+) \[(\d+)\] (\S+) ==> (.+):(\d+) \[(\d+)\]',
        )
        names = ('prev_comm', 'prev_pid', 'prev_prio', 'prev_state', 'next_comm', 'next_pid', 'next_prio')
    else:
        patterns = (
            r'comm=(.+) pid=(\d+) prio=(\d+)(?: success=([01]))? target_cpu=(\d+)',
            r'(.+):(\d+) \[(\d+)\](?: success=([01]))? CPU:(\d+)',
        )
        names = ('comm', 'pid', 'prio', 'success', 'target_cpu')
    for pattern in patterns:
        match = re.fullmatch(pattern, detail)
        if match:
            return {name: value for name, value in zip(names, match.groups()) if value is not None}
    raise DiagnosticError(f'unparsed scheduler payload: {event}')


def parse_records(text, task_events=False):
    require(not LOSS.search(text), 'lost/throttled trace')
    rows = []
    for line in text.splitlines():
        if not line.strip() or line.startswith('#'):
            continue
        match = PREFIX.fullmatch(line)
        require(match is not None, 'unparsed trace line')
        comm, pid, tid, cpu, seconds, fraction, body = match.groups()
        stamp = int(seconds) * 10**9 + int(fraction)
        row = dict(stamp=stamp, comm=comm.strip(), pid=int(pid), tid=int(tid), cpu=int(cpu))
        cm, tm, sm = COMM.fullmatch(body), TASK.fullmatch(body), SAMPLE.fullmatch(body)
        if cm and task_events:
            is_exec, name, body_pid, body_tid = cm.groups()
            row.update(event='COMM', name=name, body_pid=int(body_pid), body_tid=int(body_tid), comm_exec=bool(is_exec))
        elif tm and task_events:
            event, body_pid, body_tid, parent_pid, parent_tid = tm.groups()
            row.update(event=event, body_pid=int(body_pid), body_tid=int(body_tid), parent_pid=int(parent_pid), parent_tid=int(parent_tid))
        else:
            require(sm is not None and not body.startswith('PERF_RECORD_'), 'unknown/unparsed trace event')
            event, detail = sm.groups()
            fields = scheduler_fields(event, detail) if event in ('sched:sched_switch', 'sched:sched_wakeup') else dict(re.findall(r'(\w+)=([^\s]+)', detail))
            row.update(event=event, fields=fields, detail=detail)
        rows.append(row)
    require(rows and all(a['stamp'] <= b['stamp'] for a, b in zip(rows, rows[1:])), 'empty/out-of-order trace')
    return rows


def covered(name):
    return bool(re.fullmatch(r'(?:wal-diag-.*|tokio.*|prkdb-wal-.*)', name)) and len(name.encode()) <= 15


def totals(values):
    return {'count': len(values), 'total_ms': sum(values), 'max_ms': max(values, default=0)}


def analyze_traces(waits_text, syscall_text, cpu_text, binary, native_pid):
    waits, calls, cpu = parse_records(waits_text, True), parse_records(syscall_text), parse_records(cpu_text)
    execs = [r for r in waits if r['event'] == LIFECYCLE[0] and r['fields'].get('filename') == str(binary)]
    require(len(execs) == 1 and int(execs[0]['fields'].get('pid', -1)) == native_pid, 'exact native exec lifecycle missing/ambiguous')
    require(all(r['event'] in {*LIFECYCLE, *BLOCK, RENAME, 'sched:sched_switch', 'sched:sched_wakeup', 'COMM', 'FORK', 'EXIT'} for r in waits),
            'unknown waits recorder event')
    require(all(r['event'] == 'cpu-clock:u' for r in cpu), 'unknown CPU recorder event')
    require(all(r['event'] in {f'syscalls:sys_{phase}_{name}' for name in SYSCALLS for phase in ('enter', 'exit')} for r in calls),
            'unknown syscall recorder event')
    begin = execs[0]['stamp']
    exits = [r for r in waits if r['event'] == LIFECYCLE[2] and int(r['fields'].get('pid', -1)) == native_pid and r['stamp'] > begin]
    require(len(exits) == 1, 'native exit lifecycle missing/ambiguous')
    end = exits[0]['stamp']
    births, deaths, names = {native_pid: begin}, {native_pid: end}, {native_pid: [(begin, Path(binary).name)]}
    relevant = [r for r in waits if begin <= r['stamp'] <= end]
    for r in relevant:
        if r['event'] == LIFECYCLE[1]:
            f = r['fields']
            parent, child = int(f['pid']), int(f['child_pid'])
            if parent in births:
                require(child not in births, 'native TID reuse/duplicate lifetime unsupported')
                births[child] = r['stamp']
                names[child] = [(r['stamp'], f['child_comm'])]
        if r['event'] == LIFECYCLE[2]:
            tid = int(r['fields']['pid'])
            if tid in births:
                deaths[tid] = r['stamp']
    task_lifetimes = {}
    for r in waits:
        if r['event'] == 'FORK' and r['parent_tid'] in births and begin <= r['stamp'] <= end:
            require(r['body_tid'] in births and r['body_pid'] == native_pid, 'native metadata fork missing tracepoint coverage')
        if r['event'] == 'COMM' and r['body_pid'] == native_pid and begin <= r['stamp'] <= end:
            require(r['body_tid'] in births, 'native COMM thread missing fork coverage')
    for tid in births:
        require(tid in deaths, f'missing native thread exit: {tid}')
        if tid != native_pid:
            forks = [r for r in waits if r['event'] == 'FORK' and r['body_tid'] == tid
                     and r['parent_tid'] in births and births[r['parent_tid']] <= r['stamp'] <= deaths[tid]]
            require(len(forks) == 1, 'missing native fork metadata proof')
        task_exits = [r for r in waits if r['event'] == 'EXIT' and r['body_tid'] == tid and r['stamp'] >= births[tid]]
        require(len(task_exits) == 1, 'missing native exit metadata proof')
        task_lifetimes[str(tid)] = dict(fork_ns=None if tid == native_pid else forks[0]['stamp'], exit_ns=task_exits[0]['stamp'])
    for r in waits:
        if r['event'] == 'COMM' and r['body_tid'] in births:
            tid = r['body_tid']
            require(r['stamp'] > 0, 'native COMM rename lacks timestamp coverage')
            if births[tid] <= r['stamp'] <= deaths[tid]:
                require(r['body_pid'] == native_pid, 'native COMM target group mismatch')
                names[tid].append((r['stamp'], r['name']))
    native_comms = [r for r in waits if r['event'] == 'COMM' and r['comm_exec'] and r['body_tid'] == native_pid
                    and r['body_pid'] == native_pid and r['name'] == Path(binary).name and 0 < r['stamp'] <= end]
    require(len(native_comms) == 1, 'missing timed native exec COMM coverage')
    # COMM bodies identify the target. PID-less task_rename records identify
    # only the caller; sample_id_all preserves that caller in the COMM prefix.
    renames = [r for r in relevant if r['event'] == RENAME and
               (int(r['fields'].get('pid', -1)) in births or
                ('pid' not in r['fields'] and r['pid'] == native_pid and r['tid'] in births))]
    for r in renames:
        require(covered(r['fields'].get('oldcomm', '')) and covered(r['fields'].get('newcomm', '')),
                'uncovered native rename name')
    rename_brackets, consumed = {tid: [] for tid in births}, set()
    previous = {tid: (born, names[tid][0][1]) for tid, born in births.items()}
    for comm in waits:
        if comm['event'] != 'COMM' or comm['body_tid'] not in births:
            continue
        tid, stamp = comm['body_tid'], comm['stamp']
        if not births[tid] <= stamp <= deaths[tid]:
            continue
        if comm['comm_exec']:
            previous[tid] = (stamp, comm['name'])
            continue  # native exec identity is proven separately above
        old_stamp, old = previous[tid]
        candidates = []
        for index, rename in enumerate(renames):
            fields = rename['fields']
            explicit = fields.get('pid')
            target_matches = int(explicit) == tid if explicit is not None else (
                rename['pid'] == comm['pid'] and rename['tid'] == comm['tid'])
            if target_matches and fields['oldcomm'] == old and fields['newcomm'] == comm['name'] and old_stamp <= rename['stamp'] <= stamp:
                candidates.append(index)
        require(len(candidates) == 1, 'native rename transition lacks unique tracepoint/COMM bracket coverage')
        index = candidates[0]
        require(index not in consumed, 'native rename record consumed twice')
        consumed.add(index)
        rename_brackets[tid].append(dict(begin_ns=renames[index]['stamp'], end_ns=stamp,
                                        old=old, new=comm['name'], target_tid=tid))
        previous[tid] = (stamp, comm['name'])
    require(len(consumed) == len(renames), 'unpaired native rename lacks timed COMM coverage')
    for brackets in rename_brackets.values():
        ordered = sorted(brackets, key=lambda b: b['begin_ns'])
        require(all(a['end_ns'] <= b['begin_ns'] for a, b in zip(ordered, ordered[1:])),
                'overlapping native rename transitions unsupported; coverage inconclusive')
    def inside(row, tid):
        return tid in births and births[tid] <= row['stamp'] <= deaths[tid]
    def check_name(tid, stamp, name):
        require(covered(name), f'uncovered native name: {name}')
        valid = [n for when, n in names[tid] if when <= stamp]
        bracket_names = {n for b in rename_brackets[tid] if b['begin_ns'] <= stamp <= b['end_ns'] for n in (b['old'], b['new'])}
        require(valid and (name == valid[-1] or name in bracket_names), f'native scheduler/name metadata coverage disagreement: {tid}:{name}')
    for tid, history in names.items():
        for stamp, name in history:
            require(covered(name), f'uncovered native name: {name}')
    off, sleeping, wakes, ready, blocks = {}, {}, {}, [], collections.Counter()
    for r in relevant:
        event, f, stamp = r['event'], r.get('fields', {}), r['stamp']
        if event in BLOCK:
            blocks[event] += 1
        if event == 'sched:sched_wakeup':
            if f.get('success') == '0':
                continue
            tid = int(f['pid'])
            if inside(r, tid):
                check_name(tid, stamp, f['comm'])
                wakes[tid] = stamp
        if event == 'sched:sched_switch':
            previous, next_tid = int(f['prev_pid']), int(f['next_pid'])
            if inside(r, previous):
                check_name(previous, stamp, f['prev_comm'])
                sleeping[previous] = (stamp, f['prev_state'])
            if inside(r, next_tid):
                check_name(next_tid, stamp, f['next_comm'])
                before = sleeping.pop(next_tid, None)
                if before:
                    off.setdefault(before[1], []).append((stamp - before[0]) / 1e6)
                wake = wakes.pop(next_tid, None)
                if wake is not None:
                    ready.append((stamp - wake) / 1e6)
    pending, durations = {}, {}
    for r in calls:
        if not inside(r, r['tid']):
            continue
        require(r['event'].startswith('syscalls:sys_'), 'unknown syscall recorder event')
        phase, name = r['event'].removeprefix('syscalls:sys_').split('_', 1)
        require(name in SYSCALLS and phase in ('enter', 'exit'), 'unexpected syscall')
        key = (r['tid'], name)
        if phase == 'enter':
            require(key not in pending, 'overlapping syscall entry')
            pending[key] = r['stamp']
        else:
            require(key in pending, 'unpaired native syscall exit')
            durations.setdefault(name, []).append((r['stamp'] - pending.pop(key)) / 1e6)
    require(not pending, 'unfinished native syscall within attribution window')
    sampled = {r['tid'] for r in cpu if inside(r, r['tid']) and r['event'] == 'cpu-clock:u'}
    require(sampled, 'no actual native CPU samples')
    require(durations, 'no paired actual native syscalls')
    return dict(native_pid=native_pid, thread_ids=sorted(births), cpu_sampled_thread_ids=sorted(sampled),
                unsampled_thread_ids=sorted(set(births) - sampled),
                thread_names={str(tid): list(dict.fromkeys(n for _, n in history)) for tid, history in names.items()},
                name_coverage_complete=True, window_ns=[begin, end],
                thread_lifetimes_ns={str(t): [births[t], deaths[t]] for t in births}, task_metadata_lifetimes_ns=task_lifetimes,
                rename_transition_brackets={str(t): b for t, b in rename_brackets.items()},
                syscalls={name: totals(v) for name, v in durations.items()},
                offcpu_by_state={state: totals(v) for state, v in off.items()}, wakeup_to_run=totals(ready),
                system_block_events=dict(blocks), whole_process_only=True, timing_eligible=False,
                limits='Matched intervals only. System block events have no established request linkage. Archive has no measured-phase markers or source-line debug sections.')


def validate_attributes(text, require_inherit=True):
    # All fields must describe one tracking event, not be collected from separate
    # entries. evlist omits zero fields, so absence also fails qualification.
    require(any(all(re.search(rf'\b{field}\s*:\s*1\b', line) for field in
                        (('inherit',) if require_inherit else ()) + ('comm', 'task', 'sample_id_all', 'use_clockid', 'clockid'))
                and all(token in line for token in ('TIME', 'TID', 'CPU')) for line in text.splitlines()),
            'missing complete monotonic lifecycle tracking attributes')


def validate_system_attachment(raw, expected_cpus):
    require(expected_cpus and len(expected_cpus) == len(set(expected_cpus)), 'missing expected CPU coverage')
    threads = re.findall(r'PERF_RECORD_THREAD_MAP nr: (\d+) thread: (-?\d+)[ \t]*$', raw, re.MULTILINE)
    require(threads == [('1', '-1')], 'system recorder lacks global thread attachment')
    maps = re.findall(r'PERF_RECORD_CPU_MAP: ([^\n]+)', raw)
    require(len(maps) == 1 and re.fullmatch(r'\d+(?:-\d+)?(?:,\d+(?:-\d+)?)*', maps[0].strip()),
            'missing or ambiguous saved CPU map')
    cpus = []
    for token in maps[0].strip().split(','):
        ends = [int(n) for n in token.split('-')]
        start, end = ends[0], ends[-1]
        require(0 <= start <= end <= max(expected_cpus), 'saved CPU map out of expected range')
        cpus.extend(range(start, end + 1))
    require(len(cpus) == len(set(cpus)) and sorted(cpus) == sorted(expected_cpus), 'system recorder CPU coverage mismatch')


def validate_recording_attributes(directory, expected_cpus=None):
    directory = Path(directory)
    for kind in ('cpu', 'syscalls'):
        validate_attributes((directory / f'render-{kind}-attributes/stdout.txt').read_text())
    # Per-CPU system events intentionally have inherit=0; prove actual global
    # thread/all-CPU attachment instead. Task-scoped recorders still inherit.
    validate_attributes((directory / 'render-waits-attributes/stdout.txt').read_text(), require_inherit=False)
    validate_system_attachment((directory / 'render-waits-raw-events/stdout.txt').read_text(), expected_cpus)


def cpu_symbol_quality(text):
    rows = [line for line in text.splitlines() if re.search(r'\d+(?:\.\d+)?%', line)]
    unknown = [line for line in rows if re.search(r'\[unknown\]|\[\.\]\s+0x[0-9a-f]+\b', line, re.I)]
    return dict(unknown_symbols_present=bool(unknown), unknown_symbol_rows=len(unknown),
                cpu_symbol_attribution_complete=bool(rows) and not unknown,
                cpu_symbol_limits='Unresolved symbols remain in the scoped report; incomplete symbols do not support complete CPU attribution. Wait coverage is checked separately.')


def render_commands(directory):
    result = {'cpu-report': ['perf', 'report', '-i', str(directory / 'cpu.data'), '--stdio', '--no-children', '--sort', 'dso,symbol']}
    for kind in ('cpu', 'syscalls', 'waits'):
        result[f'{kind}-script'] = ['perf', 'script', '-i', str(directory / f'{kind}.data'), '--ns',
                                  '--hide-call-graph', '-F', 'comm,pid,tid,cpu,time,event' + (',trace' if kind != 'cpu' else '')]
        if kind == 'waits':
            result[f'{kind}-script'] += ['--show-task-events']
        result[f'{kind}-raw-events'] = ['perf', 'script', '-i', str(directory / f'{kind}.data'), '-D']
        result[f'{kind}-attributes'] = ['perf', 'evlist', '-i', str(directory / f'{kind}.data'), '-v']
    return result


def render(directory, output, ordinary_path):
    diagnostics = (directory / 'stderr.txt').read_text()
    for name, command in render_commands(directory).items():
        child = directory / f'render-{name}'
        child.mkdir()
        env = {'PATH': os.environ.get('PATH', '/usr/bin:/bin'), 'LANG': 'C.UTF-8'}
        execute(root_command(command, env, child, ordinary_path, output), env, child, output, privileged=True)
        diagnostics += (child / 'stderr.txt').read_text()
        # No duplicate rendered copies: keep each bounded prefix in its command directory.
        with (child / 'stdout.txt').open() as source:
            require(not any(LOSS.search(line) for line in source), 'lost/throttled trace')
    require(not LOSS.search(diagnostics), 'lost/throttled recorder/rendering diagnostics')
    validate_recording_attributes(directory, json.loads((Path(output) / 'preflight.json').read_text())['host']['affinity'])
    launch = validate_native_evidence(directory)
    summary = analyze_traces(*( (directory / f'render-{kind}-script/stdout.txt').read_text()
                               for kind in ('waits', 'syscalls', 'cpu')), launch['binary'], launch['pid'])
    dump(directory / 'wait-summary.json', summary)
    native_report = ['perf', 'report', '-i', str(directory / 'cpu.data'), '--stdio', '--no-children', '--sort', 'dso,symbol',
                     '--pid', str(launch['pid']), '--time', ','.join(f'{n // 10**9}.{n % 10**9:09d}' for n in summary['window_ns']),
                     '--percentage', 'relative', '--percent-limit', '0']
    child = directory / 'render-native-cpu-report'
    child.mkdir()
    env = {'PATH': os.environ.get('PATH', '/usr/bin:/bin'), 'LANG': 'C.UTF-8'}
    execute(root_command(native_report, env, child, ordinary_path, output), env, child, output, privileged=True)
    require(not LOSS.search((child / 'stderr.txt').read_text() + (child / 'stdout.txt').read_text()), 'lost/throttled native CPU report')
    symbols = cpu_symbol_quality((child / 'stdout.txt').read_text())
    dump(directory / 'cpu-symbol-quality.json', symbols)
    summary.update(symbols)
    dump(directory / 'wait-summary.json', summary)
    dump(directory / 'trace-quality.json', dict(loss_or_throttling=False, name_coverage_complete=True, **symbols,
                                               whole_process_only=True, timing_eligible=False, source_line_precision=False,
                                               cpu_report_scope='cpu-report includes wrapper startup; native-cpu-report filters exact native PID and lifecycle window with relative percentages.'))
    return summary


def smoke_target(directory):
    """Exercise privilege drop, CPU, I/O, unsampled lifetime and timed renaming."""
    directory = Path(directory)
    libc = ctypes.CDLL(None, use_errno=True)
    def name(text):
        require(libc.prctl(15, text.encode(), 0, 0, 0) == 0, 'smoke thread name failed')
    name('wal-diag-smoke')
    started, finish = threading.Event(), threading.Event()
    def short():
        name('tokio-smoke')
        tid = threading.get_native_id()
        started.set()
        finish.wait()
        name('prkdb-wal-smoke')
        atomic_dump(directory / 'smoke-thread.json', dict(tid=tid, names=['tokio-smoke', 'prkdb-wal-smoke']))
    worker = threading.Thread(target=short)
    worker.start()
    started.wait()
    fd = os.open(directory / 'smoke-io', os.O_CREAT | os.O_RDWR, 0o600)
    try:
        os.write(fd, b'x' * 4096)
        os.pwrite(fd, b'x', 0)
        os.pread(fd, 1, 0)
        os.lseek(fd, 0, os.SEEK_SET)
        os.read(fd, 1)
        os.fdatasync(fd)
        os.fsync(fd)
        deadline = time.monotonic() + .35
        count = 0
        while time.monotonic() < deadline:
            count += 1
        finish.set()
        worker.join()
        print(json.dumps({'smoke': True, 'iterations': count}), flush=True)
    finally:
        os.close(fd)


def qualify_smoke(directory, summary):
    expected = json.loads((directory / 'smoke-thread.json').read_text())
    tid = expected['tid']
    require(tid in summary['unsampled_thread_ids'], 'smoke short-lived thread sampled; unsampled coverage unqualified')
    require(all(n in summary['thread_names'][str(tid)] for n in expected['names']), 'smoke timed rename coverage incomplete')
    require(summary['offcpu_by_state'], 'smoke scheduler interval unqualified')
    require(all(summary['syscalls'].get(name, {}).get('count', 0) > 0 for name in SYSCALLS),
            'smoke read/write/futex/sync syscall pairs incomplete')
    # Built-in fail-closed fixture control proves that filtering out an unknown
    # unsampled native rename cannot be interpreted as zero wait.
    waits = (directory / 'render-waits-script/stdout.txt').read_text()
    calls = (directory / 'render-syscalls-script/stdout.txt').read_text()
    cpu = (directory / 'render-cpu-script/stdout.txt').read_text()
    corrupted = waits.replace('PERF_RECORD_COMM: tokio-smoke:', 'PERF_RECORD_COMM: unknown-native:')
    require(corrupted != waits, 'smoke lacks exact timed rename fixture')
    try:
        analyze_traces(corrupted, calls, cpu, summary['binary'] if 'binary' in summary else
                       json.loads((directory / 'native-launch.json').read_text())['binary'], summary['native_pid'])
    except DiagnosticError as error:
        require('name' in str(error), 'smoke unknown-name control failed for an unrelated reason')
    else:
        raise DiagnosticError('smoke unknown-name negative fixture unexpectedly covered')
    dump(directory / 'smoke-qualified.json', dict(full_nested_chain=True, unsampled_rename=True, unknown_name_rejected=True,
                                                 native_identity=True, descriptor_pair_equivalent=True, timing_eligible=False))


def quiet():
    for comm in Path('/proc').glob('[0-9]*/comm'):
        try:
            value = comm.read_text().strip()
        except OSError:
            continue
        require(value not in {'rustc', 'cargo', 'perf', 'clang', 'gcc', 'cc1', 'cc1plus'}
                and not value.startswith(('wal_write_path', 'wal-diag-')), f'competing native/build/profile process: {value}')


def preflight(runtime):
    require(platform.system() == 'Linux' and platform.machine() == 'x86_64', 'qualified x86_64 Linux runner required')
    require(os.getuid() != 0, 'ordinary caller must be nonroot')
    require(len(os.sched_getaffinity(0)) == 4, 'four-CPU original benchmark required')
    for name, install in (('perf', 'sudo apt-get install linux-tools-common linux-tools-generic'),
                          ('sudo', 'sudo apt-get install sudo'), ('df', 'sudo apt-get install coreutils'),
                          ('ldd', 'sudo apt-get install libc-bin')):
        require(shutil.which(name), f'missing {name}; maintainer install command: {install}')
    quiet()
    return dict(kernel=platform.release(), perf=subprocess.check_output(['perf', '--version'], text=True).strip(),
                cpuinfo=Path('/proc/cpuinfo').read_text(),
                perf_event_paranoid=Path('/proc/sys/kernel/perf_event_paranoid').read_text().strip(),
                host=host_snapshot(runtime), no_sysctl_change=True)


def run(artifact, output):
    output = Path(output).resolve()
    require(not output.exists(), 'fresh output required; no retries or overwrite')
    output.mkdir(parents=True)
    result = dict(diagnostic_only=True, verdict='inconclusive', original_acceptance='FAILED, unchanged', timing_eligible=False)
    runs = []
    try:
        dump(output / 'original-qualified.json', verify_artifact(artifact))
        runtime = output / 'runtime'
        runtime.mkdir()
        dump(output / 'preflight.json', preflight(runtime))
        ordinary = resource_proof()
        ordinary_path = output / 'ordinary-resources.json'
        dump(ordinary_path, ordinary)
        binaries = {}
        for side in ('base', 'head'):
            binary = output / f'wal-diag-{side}'
            shutil.copyfile(Path(artifact) / f'{side}-benchmark-executable', binary)
            binary.chmod(0o755)
            require(digest(binary) == PINNED[side]['binary_sha256'], 'copied pinned executable differs')
            binaries[side] = binary
            linked = subprocess.run(['ldd', str(binary)], capture_output=True, text=True, check=False)
            dump(output / f'{side}-runtime-libraries.json', dict(exit_code=linked.returncode, stdout=linked.stdout, stderr=linked.stderr))
            require(linked.returncode == 0 and 'not found' not in linked.stdout + linked.stderr, 'native library qualification failed')
        dump(output / 'schedule.json', sequence())
        for planned in sequence():
            quiet()
            if planned['index'] == 4:
                # Exact same three recorders and launch wrapper as every profile.
                smoke = output / 'trace-smoke'
                smoke.mkdir()
                binary = output / 'wal-diag-smoke'
                shutil.copyfile(Path(shutil.which('python3')).resolve(), binary)
                binary.chmod(0o755)
                env = fixed_environment(runtime)
                command = profile_command(binary, smoke, ordinary_path, digest(binary),
                                          [SELF, 'smoke-target', '--directory', str(smoke)])
                execute(root_command(command, env, smoke, ordinary_path, output), env, smoke, output, privileged=True)
                validate_native_evidence(smoke, ordinary)
                qualify_smoke(smoke, render(smoke, output, ordinary_path))
            phase, side, cell = planned['phase'], planned['side'], planned['cell']
            directory = output / f'{phase}-{planned["index"]:02d}-{side}'
            directory.mkdir()
            require(digest(binaries[side]) == PINNED[side]['binary_sha256'], 'native executable changed before invocation')
            env = environment(runtime, cell)
            dump(directory / 'host-before.json', host_snapshot(runtime))
            try:
                if phase == 'control':
                    command = scoped_command(binaries[side], directory, ordinary_path, PINNED[side]['binary_sha256'])
                else:
                    command = root_command(profile_command(binaries[side], directory, ordinary_path, PINNED[side]['binary_sha256']),
                                           env, directory, ordinary_path, output)
                meta = execute(command, env, directory, output, privileged=phase == 'profile')
            finally:
                dump(directory / 'host-after.json', host_snapshot(runtime))
            launch = validate_native_evidence(directory, ordinary)
            require(digest(binaries[side]) == PINNED[side]['binary_sha256'], 'native executable changed during invocation')
            rows = parse_native((directory / 'stdout.txt').read_text(), cell)
            if phase == 'profile':
                render(directory, output, ordinary_path)
            runs.append(dict(planned, rows=rows, binary_sha256=PINNED[side]['binary_sha256'], native_pid=launch['pid'],
                             exit_code=meta['exit_code'], directory=directory.name, timing_eligible=False))
            dump(output / 'runs.json', runs)
        result.update(trace_capture_complete=True, verdict='qualified evidence collected; causal interpretation pending independent review',
                      limits='Whole native process only; 100ms FD maxima are observations. Observer controls do not clear original gate failure.')
    except (DiagnosticError, OSError, ValueError, KeyError, subprocess.SubprocessError) as error:
        result['error'] = str(error)
        raise DiagnosticError(str(error)) from error
    finally:
        dump(output / 'runs.json', runs)
        dump(output / 'result.json', result)
        # Root raw files remain intact and uploadable even after a killed recorder.
        # This operation is scoped to the fresh owned diagnostic directory.
        if platform.system() == 'Linux' and shutil.which('sudo'):
            ownership = subprocess.run(['sudo', '-n', 'chown', '-R', f'{os.getuid()}:{os.getgid()}', '--', str(output)],
                                       capture_output=True, text=True, check=False)
            dump(output / 'ownership.json', dict(exit_code=ownership.returncode, stderr=ownership.stderr))
            require(ownership.returncode == 0, 'cannot retain privileged evidence ownership')
        save_inventory(output)
    print(json.dumps(result, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='operation', required=True)
    run_parser = sub.add_parser('run')
    run_parser.add_argument('--artifact', required=True)
    run_parser.add_argument('--output', required=True)
    for operation in ('supervisor', 'child-limit', 'scoped'):
        p = sub.add_parser(operation)
        p.add_argument('--proof', required=True)
        p.add_argument('--directory', required=True)
        if operation == 'supervisor':
            p.add_argument('--output', required=True)
        if operation == 'scoped':
            p.add_argument('--binary', required=True)
            p.add_argument('--sha256', required=True)
        p.add_argument('command', nargs=argparse.REMAINDER)
    p = sub.add_parser('smoke-target')
    p.add_argument('--directory', required=True)
    args = parser.parse_args()
    command = getattr(args, 'command', [])
    if command[:1] == ['--']:
        command = command[1:]
    try:
        if args.operation == 'run':
            run(args.artifact, args.output)
        elif args.operation == 'supervisor':
            return qualified_supervisor(args.proof, args.directory, args.output, command)
        elif args.operation == 'child-limit':
            require(command, 'missing child command')
            child_limit(args.proof, args.directory, command)
        elif args.operation == 'scoped':
            scoped(args.proof, args.directory, args.binary, args.sha256, command)
        else:
            smoke_target(args.directory)
    except (DiagnosticError, OSError, ValueError, KeyError, subprocess.SubprocessError) as error:
        print(f'inconclusive: {error}', file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
