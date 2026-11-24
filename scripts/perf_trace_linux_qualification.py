#!/usr/bin/env python3
"""Finite recorder-only evidence packaging and Linux containment controls.

No database/build/restore entry point exists. Local tests simulate OS boundaries;
only a separately approved Linux workflow can establish privilege qualification.
"""
import argparse
import errno
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import platform
import re
import resource
import shutil
import stat
import subprocess
import sys
import time

Error = ValueError
PUBLIC_BASE = '9c4345a0636a7aaca08f3b9a28c9a12e3a0a445b'
DONOR = '67ee4850e64257f185cf31e1b92bf2193bbcaec3'
RUNTIME_MODULES = ('stream_durable_diagnostic', 'stream_durable_restore',
                   'wal_descriptor_diagnostic', 'wal_stall_diagnostic', 'wal_trace_qualification',
                   'perf_trace_format', 'perf_trace_records', 'perf_trace_decode', 'perf_trace_integration')
DATA = ('README.md', 'provenance.json', 'linux7-syscalls.perf', 'linux617-syscalls.perf',
        'linux617-scheduler.perf', 'linux617-syscalls.perf.expected.json',
        'linux617-scheduler.perf.expected.json', 'integration-donors.json')
IMPORTED = tuple('scripts/' + prefix + name + '.py' for name in RUNTIME_MODULES for prefix in ('', 'test_')) + tuple('scripts/testdata/perf_trace/' + name for name in DATA)
SOURCE_MANIFEST = 'scripts/testdata/perf_trace/linux-qualification-sources.json'
SOURCE_MANIFEST_SHA256 = 'e45a97433b27ade26e98ab3ed00b6184a14d3a571283b59115eccfa7c7aa6a1a'
LAUNCH_MANIFEST = 'scripts/testdata/perf_trace/linux-qualification-launches.json'
LAUNCH_BASE = 'e4542a1d1e3aac00bc949d99a0442c258a08e348'
LAUNCH_PATHS = tuple('scripts/' + name + '.py' for name in (
    'stream_durable_diagnostic', 'wal_descriptor_diagnostic',
    'wal_stall_diagnostic', 'wal_trace_qualification'))
ALLOWLIST = IMPORTED + (SOURCE_MANIFEST, LAUNCH_MANIFEST, 'scripts/perf_trace_linux_qualification.py',
    'scripts/test_perf_trace_linux_qualification.py', '.github/workflows/remediation-gate.yml',
    'docs/superpowers/specs/2026-11-24-perf-trace-linux-qualification-design.md',
    'docs/superpowers/plans/2026-11-24-perf-trace-linux-qualification.md',
    'docs/superpowers/specs/2026-11-24-perf-trace-bytecode-design.md',
    'docs/superpowers/plans/2026-11-24-perf-trace-bytecode.md',
    'docs/superpowers/specs/2026-11-24-linux-qualification-candidate-design.md',
    'docs/superpowers/plans/2026-11-24-linux-qualification-candidate.md')
FIXTURE_HASH = '129a535da6ecbadbb6e4d17c2584602f2f6426ee64c3f8ae85eedfe9a4795935'
FLAGS = dict(diagnostic_only=True, timing_eligible=False, benchmark_invocations=0)
READ_LIMIT = 32 << 20
SELF = Path(__file__).resolve()


def require(ok, message):
    if not ok:
        raise Error(message)


def relative(name):
    require(type(name) is str and name and '\\' not in name, 'invalid relative path')
    path = PurePosixPath(name)
    require(not path.is_absolute() and str(path) == name and all(p not in ('.', '..') for p in path.parts), 'escaping/noncanonical path')
    return path


def no_links(path):
    path = Path(path).absolute()
    for part in (path, *path.parents):
        require(not part.is_symlink(), 'symlink path refused: ' + str(part))
    return path


def read_regular(path, limit=READ_LIMIT):
    path = no_links(path)
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK | getattr(os, 'O_NOFOLLOW', 0))
        with os.fdopen(fd, 'rb') as stream:
            before = os.fstat(stream.fileno())
            require(stat.S_ISREG(before.st_mode) and 0 < before.st_size <= limit, 'nonregular/empty/oversized input')
            data = stream.read(before.st_size + 1)
            after = os.fstat(stream.fileno())
            require((before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) ==
                    (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns) and len(data) == before.st_size, 'file changed while reading')
            return data
    except OSError as error:
        raise Error('cannot read regular input: ' + str(error)) from error


def strict_json(path):
    def pairs(items):
        out = {}
        for key, value in items:
            require(key not in out, 'duplicate JSON key')
            out[key] = value
        return out
    def constant(value):
        raise Error('nonfinite JSON')
    try:
        return json.loads(read_regular(path), object_pairs_hook=pairs, parse_constant=constant)
    except (UnicodeError, RecursionError, json.JSONDecodeError) as error:
        raise Error('malformed JSON') from error


def write_json(path, value):
    data = (json.dumps(value, sort_keys=True, indent=2, allow_nan=False) + '\n').encode()
    require(len(data) <= READ_LIMIT, 'receipt output cap')
    with Path(path).open('xb') as stream:
        stream.write(data)


def identity(data):
    return dict(bytes=len(data), sha256=hashlib.sha256(data).hexdigest())


def git_identity(repo):
    def git(*args):
        proc = subprocess.run(['git', '-C', str(repo), *args], capture_output=True, text=True, timeout=10, check=False)
        require(proc.returncode == 0 and len(proc.stdout) <= READ_LIMIT, 'git identity failed')
        return proc.stdout.strip()
    head = git('rev-parse', 'HEAD')
    require(re.fullmatch('[0-9a-f]{40}', head), 'invalid HEAD')
    clean = not git('status', '--porcelain', '--untracked-files=all')
    require(clean, 'clean source checkout required')
    return dict(head=head, clean=clean)


def sources(repo):
    repo = no_links(repo)
    require(identity(read_regular(repo / SOURCE_MANIFEST))['sha256'] == SOURCE_MANIFEST_SHA256, 'historical source manifest changed')
    manifest = strict_json(repo / SOURCE_MANIFEST)
    require(type(manifest) is dict and set(manifest) == {'version', 'public_base', 'tooling_donor', 'files'}, 'source manifest schema')
    require(type(manifest['version']) is int and manifest['version'] == 1 and manifest['public_base'] == PUBLIC_BASE and manifest['tooling_donor'] == DONOR, 'source manifest identity')
    require(type(manifest['files']) is dict and set(manifest['files']) == set(IMPORTED), 'exact imported source inventory required')
    launches = strict_json(repo / LAUNCH_MANIFEST)
    require(type(launches) is dict and set(launches) == {'version', 'base_revision', 'reason', 'files'}, 'launch manifest schema')
    require(type(launches['version']) is int and launches['version'] == 1
            and launches['base_revision'] == LAUNCH_BASE
            and launches['reason'] == 'explicit-python-bytecode-suppression', 'launch manifest identity')
    require(type(launches['files']) is dict and set(launches['files']) == set(LAUNCH_PATHS), 'exact launch overlay inventory required')
    files = {}
    for name in ALLOWLIST:
        relative(name)
        data = read_regular(repo / name)
        files[name] = identity(data)
        if name in IMPORTED:
            row = manifest['files'][name]
            require(type(row) is dict and set(row) == {'blob', 'bytes', 'donor', 'sha256'}, 'donor entry schema')
            blob = hashlib.sha1(b'blob ' + str(len(data)).encode() + b'\0' + data).hexdigest()
            require(type(row['bytes']) is int and row['bytes'] > 0 and row['donor'] == DONOR
                    and type(row['sha256']) is str and re.fullmatch('[0-9a-f]{64}', row['sha256'])
                    and type(row['blob']) is str and re.fullmatch('[0-9a-f]{40}', row['blob']), 'donor entry identity')
            if name in LAUNCH_PATHS:
                launch = launches['files'][name]
                require(type(launch) is dict and set(launch) == {'base_blob', 'base_sha256', 'blob', 'sha256', 'bytes'}, 'launch entry schema')
                require(type(launch['bytes']) is int and launch['bytes'] > 0
                        and all(type(launch[key]) is str for key in ('base_blob', 'base_sha256', 'blob', 'sha256')), 'launch entry types')
                require(launch == dict(files[name], blob=blob, base_blob=row['blob'], base_sha256=row['sha256']), 'launch source binding changed: ' + name)
            else:
                require(row == dict(files[name], blob=blob, donor=DONOR), 'donor source bytes changed: ' + name)
    return files


def package_tooling(repo, output):
    repo, output = no_links(repo), no_links(output)
    require(not output.exists() and output != repo and repo not in output.parents, 'exclusive fresh external evidence output required')
    files = sources(repo)
    git = git_identity(repo)
    output.mkdir(mode=0o700)
    for name in ALLOWLIST:
        target = output / 'tooling' / name
        target.parent.mkdir(parents=True, exist_ok=True)
        data = read_regular(repo / name)
        require(identity(data) == files[name], 'source drift while packaging')
        with target.open('xb') as stream:
            stream.write(data)
    receipt = dict(version=1, files=files, source=str(repo), output=str(output), git=git,
                   provenance={k: os.environ.get(k) for k in ('GITHUB_EVENT_NAME', 'GITHUB_RUN_ID', 'GITHUB_RUN_ATTEMPT', 'GITHUB_JOB', 'GITHUB_SHA', 'GITHUB_REF', 'GITHUB_WORKFLOW_REF', 'GITHUB_WORKFLOW_SHA')}, **FLAGS)
    verify_tooling(repo, receipt)
    write_json(output / 'tooling-receipt.json', receipt)
    return receipt


def verify_tooling(repo, receipt):
    require(type(receipt) is dict and set(receipt) == {'version', 'files', 'source', 'output', 'git', 'provenance', *FLAGS}, 'tooling receipt schema')
    require(type(receipt['version']) is int and receipt['version'] == 1, 'tooling receipt version')
    for key, val in FLAGS.items():
        require(type(receipt[key]) is type(val) and receipt[key] == val, 'tooling receipt flag')
    require(receipt['source'] == str(no_links(repo)), 'source root identity')
    require(type(receipt['git']) is dict and receipt['git'].get('clean') is True and re.fullmatch('[0-9a-f]{40}', receipt['git'].get('head', '')), 'clean HEAD receipt required')
    require(git_identity(repo) == receipt['git'], 'source HEAD/clean state changed')
    require(type(receipt['provenance']) is dict and set(receipt['provenance']) == {'GITHUB_EVENT_NAME', 'GITHUB_RUN_ID', 'GITHUB_RUN_ATTEMPT', 'GITHUB_JOB', 'GITHUB_SHA', 'GITHUB_REF', 'GITHUB_WORKFLOW_REF', 'GITHUB_WORKFLOW_SHA'} and all(value is None or type(value) is str for value in receipt['provenance'].values()), 'provenance schema')
    files = sources(repo)
    require(receipt['files'] == files, 'complete current source binding required')
    output = no_links(receipt['output'])
    actual = set()
    for directory, dirs, names in os.walk(output / 'tooling', followlinks=False):
        for name in (*dirs, *names):
            require(not (Path(directory) / name).is_symlink(), 'packaged symlink refused')
        actual.update(str((Path(directory) / name).relative_to(output / 'tooling')) for name in names)
    require(actual == set(ALLOWLIST), 'packaged module closure differs')
    for name in ALLOWLIST:
        require(identity(read_regular(output / 'tooling' / name)) == files[name], 'archived source drift: ' + name)
    return files


def make_inventory(root):
    root = no_links(root)
    rows = []
    total = 0
    for directory, dirs, names in os.walk(root, followlinks=False):
        for name in dirs:
            no_links(Path(directory) / name)
        for name in sorted(names):
            path = no_links(Path(directory) / name)
            if path == root / 'inventory.json':
                continue
            h = hashlib.sha256()
            fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK | getattr(os, 'O_NOFOLLOW', 0))
            with os.fdopen(fd, 'rb') as stream:
                info = os.fstat(stream.fileno())
                require(stat.S_ISREG(info.st_mode) and info.st_size <= 512 << 20, 'inventory regular/file cap')
                size = 0
                while chunk := stream.read(1 << 20):
                    size += len(chunk)
                    total += len(chunk)
                    require(size <= info.st_size and total <= 2 << 30, 'inventory aggregate cap')
                    h.update(chunk)
                after = os.fstat(stream.fileno())
                require(size == info.st_size == after.st_size and info.st_mtime_ns == after.st_mtime_ns, 'inventory changed')
            rows.append(dict(path=str(path.relative_to(root)), bytes=size, sha256=h.hexdigest()))
    return dict(version=1, self_hash_excluded='inventory.json', files=sorted(rows, key=lambda row: row['path']))


def verify_inventory(root, inventory):
    require(type(inventory) is dict and set(inventory) == {'version', 'self_hash_excluded', 'files'}, 'inventory schema')
    require(inventory == make_inventory(root), 'inventory incomplete, duplicate, escaping or hash mismatch')


def validate_recorders(result, categories):
    required = dict(complete=True, qualification_only=True, mode='recorder-qualification',
                    benchmark_invocations=0, build_provenance_checked=False, diagnostic_only=True, timing_eligible=False)
    require(type(result) is dict, 'missing result')
    for key, value in required.items():
        require(type(result.get(key)) is type(value) and result[key] == value, 'recorder result refused: ' + key)
    require(type(categories) is dict and set(categories) == {'cpu', 'syscalls', 'scheduler'}, 'exact three category proofs required')
    for proof in categories.values():
        require(type(proof) is dict and proof.get('qualified') is True and type(proof.get('benchmark_invocations')) is int and proof['benchmark_invocations'] == 0, 'unqualified category')


def check_recorders(repo, receipt, output):
    verify_tooling(repo, receipt)
    validate_recorders(strict_json(output / 'result.json'), strict_json(output / 'qualification.json'))
    inventory = strict_json(output / 'inventory.json')
    # Native inventories have their own unchanged schema, excluding runtime.
    require(type(inventory) is dict and set(inventory) == {'runtime_pruned_before_traversal', 'files'} and inventory['runtime_pruned_before_traversal'] is True, 'native inventory schema')
    import stream_durable_diagnostic as native
    for directory, dirs, names in os.walk(output, followlinks=False):
        if Path(directory) == output:
            dirs[:] = [name for name in dirs if name != 'runtime']
        for name in (*dirs, *names):
            path = no_links(Path(directory) / name)
            require(path.is_dir() or stat.S_ISREG(path.lstat().st_mode), 'native nonregular evidence')
    expected = []
    end = time.monotonic_ns() + 50 * 10**9
    for path in native.observer_files(output):
        if path.name == 'inventory.json':
            continue
        digest = hashlib.sha256()
        fd = os.open(no_links(path), os.O_RDONLY | os.O_NONBLOCK | getattr(os, 'O_NOFOLLOW', 0))
        with os.fdopen(fd, 'rb') as stream:
            info = os.fstat(stream.fileno())
            require(stat.S_ISREG(info.st_mode) and info.st_size <= 512 << 20, 'native file cap/type')
            size = 0
            while chunk := stream.read(1 << 20):
                size += len(chunk)
                require(size <= info.st_size and time.monotonic_ns() < end, 'native hashing bound')
                digest.update(chunk)
            after = os.fstat(stream.fileno())
            require(size == info.st_size == after.st_size and info.st_mtime_ns == after.st_mtime_ns, 'native evidence changed')
        expected.append(dict(path=str(path.relative_to(output)), bytes=size, sha256=digest.hexdigest()))
    rows = inventory['files']
    require(type(rows) is list and len(rows) == len(expected) and sorted(rows, key=lambda r: r['path']) == sorted(expected, key=lambda r: r['path']), 'native inventory mismatch')
    require(len({row['path'] for row in rows}) == len(rows), 'duplicate native inventory')
    for row in rows:
        relative(row['path'])
        no_links(output / row['path'])
    require(not native.budget_breach(output, output), 'native evidence cap exceeded')
    verify_tooling(repo, receipt)
    return dict(accepted=True, categories=['cpu', 'syscalls', 'scheduler'], **FLAGS)


def run_command(argv, directory, timeout):
    require(0 < timeout <= 120, 'command timeout bound')
    directory = Path(directory)
    directory.mkdir(exist_ok=False)
    begin = time.monotonic_ns()
    write_json(directory / 'command.json', dict(argv=argv, begin_ns=begin, timeout_s=timeout))
    code = None
    error = None
    try:
        with (directory / 'stdout.txt').open('xb') as out, (directory / 'stderr.txt').open('xb') as err:
            proc = subprocess.run(argv, stdout=out, stderr=err, timeout=timeout, check=False, start_new_session=True)
            code = proc.returncode
    except (OSError, subprocess.SubprocessError) as failure:
        error = str(failure)
    result = dict(exit_code=code, error=error, end_ns=time.monotonic_ns())
    write_json(directory / 'exit.json', result)
    return result


def validate_root(root, nonce, uid, gid, deadline=None, runner_temp=None):
    require(type(uid) is int and type(gid) is int and uid > 0 and gid >= 0, 'ordinary owner required')
    require(re.fullmatch('[0-9a-f]{32}', nonce or ''), 'invalid fixed nonce')
    temp = no_links(runner_temp or os.environ.get('RUNNER_TEMP', ''))
    require(temp.is_dir() and temp.is_absolute(), 'RUNNER_TEMP required')
    root = no_links(root)
    require(root.parent == temp and root.name == 'qualification-' + nonce, 'fresh per-job evidence boundary')
    info = root.stat()
    require(stat.S_ISDIR(info.st_mode) and info.st_uid == uid and stat.S_IMODE(info.st_mode) == 0o700, 'evidence root owner/mode')
    marker = strict_json(root / 'control-root.json')
    require(marker == dict(version=1, nonce=nonce, uid=uid, gid=gid, root=str(root), runner_temp=str(temp)), 'control root identity')
    require((root / 'control-root.json').stat().st_uid == uid, 'control marker owner')
    if deadline is not None:
        require(type(deadline) is int and 0 < deadline - time.monotonic_ns() <= 120 * 10**9, 'invalid aggregate deadline')
    for directory, dirs, files in os.walk(root, followlinks=False):
        for name in (*dirs, *files):
            path = no_links(Path(directory) / name)
            mode = path.lstat().st_mode
            require(stat.S_ISDIR(mode) or stat.S_ISREG(mode), 'nonregular evidence entry')
    return root


def rescue_upload(root, nonce, uid, gid, control=False):
    root = validate_root(root, nonce, uid, gid)
    result = run_command(['sudo', '-n', 'timeout', '--signal=KILL', '30s', 'chown', '-R', '-h', '-P', '--preserve-root', f'{uid}:{gid}', '--', str(root)], root / ('control-rescue' if control else 'upload-rescue'), timeout=35)
    require(result['exit_code'] == 0 and not result.get('error'), 'bounded ownership rescue failed')
    return result


def file_stat(path):
    info = no_links(path).stat()
    require(stat.S_ISREG(info.st_mode), 'prefix must be a regular file')
    return dict(uid=info.st_uid, gid=info.st_gid, mode=stat.S_IMODE(info.st_mode))


def eacces(path):
    try:
        with path.open('rb') as stream:
            stream.read(1)
    except OSError as error:
        require(error.errno == errno.EACCES, 'expected actual EACCES')
        return True
    raise Error('ordinary input unexpectedly readable')


def validate_control(mode, proof):
    required = {'protected': ('eacces', 'unchanged', 'decoder_qualified'),
                'deadline': ('native_failed', 'timed_out', 'actual_group_known', 'contained', 'child_absent'),
                'rescue': ('native_failed', 'eacces', 'unchanged', 'rescued', 'actual_group_known', 'contained', 'child_absent')}
    require(mode in required and type(proof) is dict, 'unknown control mode')
    for key in required[mode]:
        require(proof.get(key) is True, 'control refused: ' + key)


def root_fixture(args):
    require(sys.platform == 'linux' and os.geteuid() == 0, 'real Linux root fixture required')
    root = validate_root(args.root, args.nonce, args.uid, args.gid, args.deadline_ns, args.runner_temp)
    import stream_durable_diagnostic as native
    legacy = native.legacy
    before = legacy.resource_proof()
    if args.mode == 'protect':
        target = root / 'protected' / 'input.perf'
        require(not target.exists(), 'fresh protected input required')
        source = read_regular(root / 'tooling/scripts/testdata/perf_trace/linux7-syscalls.perf')
        require(hashlib.sha256(source).hexdigest() == FIXTURE_HASH, 'immutable fixture hash')
        with target.open('xb') as stream:
            stream.write(source)
        target.chmod(0o600)
        write_json(root / 'protected/root-input.json', dict(uid=target.stat().st_uid, gid=target.stat().st_gid, mode=stat.S_IMODE(target.stat().st_mode), **identity(source), resources=before))
        return 0
    directory = root / args.mode / 'finalization'
    require(directory.is_dir() and not (directory / 'root-group.json').exists(), 'fresh finalization witness required')
    if args.mode == 'rescue':
        target = root / 'rescue' / 'unreadable-prefix'
        with target.open('xb') as stream:
            stream.write(b'controlled finalization failure prefix\n')
        target.chmod(0o600)
        write_json(root / 'rescue/prefix-before.json', dict(file_stat(target), **identity(read_regular(target))))
        command = [sys.executable, '-B', '-c', 'raise SystemExit(23)']
    else:
        # The child retains both pipe ends and blocks on an event, without sleeps.
        # Its readiness proof precedes the blocking read and outer deadline.
        code = "import os,time,json,sys; r,w=os.pipe(); p=sys.argv[1]; f=open(p,'x'); json.dump({'pid':os.getpid(),'pgid':os.getpgrp(),'live_ns':time.monotonic_ns()},f); f.close(); os.read(r,1)"
        command = [sys.executable, '-B', '-c', code, str(root / 'deadline/child-live.json')]
    write_json(directory / 'fixture-root-before.json', before)
    try:
        return legacy.supervise(command, directory / 'root-group.json', timeout=120)
    finally:
        write_json(directory / 'fixture-root-after.json', legacy.resource_proof())


def fixture_command(root, mode, nonce, uid, gid, deadline, runner_temp):
    return ['sudo', '-n', sys.executable, '-B', str(root / 'tooling/scripts/perf_trace_linux_qualification.py'), 'root-fixture',
            '--mode', mode, '--root', str(root), '--runner-temp', str(runner_temp), '--nonce', nonce,
            '--uid', str(uid), '--gid', str(gid), '--deadline-ns', str(deadline)]


def process_absent(group):
    # Group disappearance, including descendants, is established by actual sudo kill -0.
    require(type(group) is int and group > 1, 'unknown root group')
    p = subprocess.run(['sudo', '-n', 'kill', '-0', '--', '-' + str(group)], capture_output=True, text=True, timeout=5, check=False)
    return dict(argv=p.args, exit_code=p.returncode, stdout=p.stdout, stderr=p.stderr,
                absent=p.returncode != 0 and 'no such process' in p.stderr.lower())


def fixed_control(root, mode, nonce, uid, gid, deadline, runner_temp):
    import stream_durable_diagnostic as native
    legacy = native.legacy
    directory = root / mode
    directory.mkdir()
    proof = dict(mode=mode, **FLAGS)
    if mode == 'protected':
        command = fixture_command(root, 'protect', nonce, uid, gid, deadline, runner_temp)
        ordinary_path = directory / 'ordinary-resources.json'
        write_json(ordinary_path, legacy.resource_proof())
        env = dict(PATH=os.environ['PATH'], LANG='C.UTF-8')
        create = directory / 'create'
        create.mkdir()
        limit = native.Deadline(min(deadline, time.monotonic_ns() + 15 * 10**9), cleanup_end_ns=deadline)
        actual = native.root_command(command[2:], env, create, ordinary_path, root, limit)
        native.execute(actual, env, create, root, limit, privileged=True)
        path = directory / 'input.perf'
        proof['eacces'] = eacces(path)
        metadata = strict_json(directory / 'root-input.json')
        require(metadata['uid'] == 0 and metadata['mode'] == 0o600 and metadata['sha256'] == FIXTURE_HASH, 'protected identity differs')
        import perf_trace_integration as integration
        fixture = root / 'tooling/scripts/testdata/perf_trace/linux7-syscalls.perf'
        expected = integration.build_envelope('syscalls', fixture)
        require(expected['sample_count'] == 114, 'fixture sample contract')
        env = dict(PATH=os.environ['PATH'], LANG='C.UTF-8')
        for label, extra in [('decode', []), ('verify-input', ['--verify-input'])]:
            call = directory / label
            argv = [sys.executable, '-B', str(root / 'tooling/scripts/perf_trace_integration.py'), str(path), '--category', 'syscalls', *extra]
            call.mkdir()
            limit = native.Deadline(min(deadline, time.monotonic_ns() + 15 * 10**9), cleanup_end_ns=deadline)
            command = native.root_command(argv, env, call, ordinary_path, root, limit)
            native.execute(command, env, call, root, limit, privileged=True)
        actual = integration.read_envelope(directory / 'decode/stdout.txt', 'syscalls', integration.tooling_hashes())
        integration.read_receipt(directory / 'verify-input/stdout.txt', 'syscalls', integration.tooling_hashes(), expected['input'])
        proof.update(unchanged=actual['input'] == expected['input'] and eacces(path), decoder_qualified=actual == expected, fixture_only=True, fresh_live_qualification=False, input=metadata)
    else:
        final = native.Deadline(min(deadline, time.monotonic_ns() + 16 * 10**9))
        original = native.ownership_command
        def factory(output, call, command_deadline):
            return fixture_command(root, mode, nonce, uid, gid, deadline, runner_temp)
        native.ownership_command = factory
        try:
            try:
                native.ownership_cleanup(directory, final)
            except native.DiagnosticError:
                proof['native_failed'] = True
            else:
                raise Error('controlled finalization unexpectedly succeeded')
        finally:
            native.ownership_command = original
        result = dict(complete=False, verdict='inconclusive', qualification_only=True, mode='containment-control', **FLAGS)
        try:
            native.finalize_metadata(directory, [], result, final)
        except native.DiagnosticError:
            pass  # The actual failed finalizer preserves its own inconclusive result.
        require(strict_json(directory / 'result.json').get('complete') is False, 'native failure masked')
        exitproof = strict_json(directory / 'ownership.json')
        group = legacy.read_owned_group(directory / 'finalization/root-group.json')
        require(group is not None, 'actual root group unknown')
        probe = process_absent(group)
        write_json(directory / 'postmortem-group-probe.json', probe)
        proof.update(actual_group_known=True, child_absent=probe['absent'], contained=probe['absent'], native_exit=exitproof)
        if mode == 'deadline':
            live = strict_json(directory / 'child-live.json')
            containment = strict_json(directory / 'finalization/root-containment.json')
            command = strict_json(directory / 'finalization/command.json')
            require(live['pgid'] == live['pid'] == group and live['live_ns'] < command['deadline_ns'], 'child not live before deadline')
            proof.update(timed_out=exitproof['error'] is not None and exitproof['root_contained'] is True,
                         contained=containment.get('qualified_cleanup') is True and containment.get('actual_group') == group and probe['absent'], live=live)
        else:
            prefix = directory / 'unreadable-prefix'
            expected = identity(b'controlled finalization failure prefix\n')
            before = strict_json(directory / 'prefix-before.json')
            require(type(before) is dict and set(before) == {'uid', 'gid', 'mode', 'bytes', 'sha256'} and before == dict(file_stat(prefix), **expected) and before['uid'] == 0 and before['mode'] == 0o600, 'root prefix ownership/mode/hash evidence differs')
            proof['prefix_before'] = before
            proof['eacces'] = eacces(prefix)
            # Keep the failed native result byte-for-byte; rescue writes beside it.
            failed_bytes = {name: read_regular(directory / name) for name in ('ownership.json', 'result.json')}
            rescue = rescue_upload(root, nonce, uid, gid, control=True)
            after = dict(file_stat(prefix), **identity(read_regular(prefix)))
            write_json(directory / 'prefix-after.json', after)
            require(after == dict(uid=uid, gid=gid, mode=0o600, **expected), 'rescued ordinary prefix ownership/mode/hash differs')
            proof['prefix_after'] = after
            proof.update(rescued=rescue['exit_code'] == 0, unchanged=identity(read_regular(prefix)) == expected and all(read_regular(directory / name) == data for name, data in failed_bytes.items()))
    validate_control(mode, proof)
    write_json(directory / 'control-receipt.json', proof)
    return proof


def controls(repo, receipt, root, nonce):
    require(sys.platform == 'linux' and os.getuid() > 0 and os.geteuid() > 0, 'ordinary real Linux UID required')
    verify_tooling(repo, receipt)
    uid, gid = os.getuid(), os.getgid()
    root = validate_root(root, nonce, uid, gid)
    deadline = time.monotonic_ns() + 120 * 10**9
    import stream_durable_diagnostic as native
    proofs = []
    outcome = dict(complete=False, **FLAGS)
    try:
        for mode in ('protected', 'deadline', 'rescue'):
            require(time.monotonic_ns() < deadline, 'control aggregate deadline exhausted')
            proofs.append(fixed_control(root, mode, nonce, uid, gid, deadline, root.parent))
        require(time.monotonic_ns() < deadline, 'control aggregate deadline exhausted')
        verify_tooling(repo, receipt)
        outcome['complete'] = True
    except (Error, OSError, native.DiagnosticError, subprocess.SubprocessError) as error:
        outcome['error'] = str(error)
        raise Error(str(error)) from error
    finally:
        outcome.update(controls=proofs, deadline_ns=deadline, end_ns=time.monotonic_ns())
        write_json(root / 'controls-result.json', outcome)


def preflight(root):
    require(sys.platform == 'linux' and os.getuid() > 0, 'ordinary Linux runner required')
    for utility in ('sudo', 'timeout', 'python3', 'perf', 'chown', 'kill', 'env', 'df', 'cat', 'sh', 'taskset'):
        require(shutil.which(utility), 'missing required tool: ' + utility)
    result = run_command(['sudo', '-n', 'true'], root / 'sudo-preflight', timeout=5)
    require(result['exit_code'] == 0, 'noninteractive sudo unavailable')
    perf = run_command(['perf', '--version'], root / 'perf-version', timeout=5)
    require(perf['exit_code'] == 0, 'perf unavailable')
    write_json(root / 'host.json', dict(kernel=platform.uname()._asdict(), python=sys.version, interpreter=sys.executable,
        uid=os.getuid(), gid=os.getgid(), euid=os.geteuid(), affinity=sorted(os.sched_getaffinity(0)),
        nofile=resource.getrlimit(resource.RLIMIT_NOFILE), **FLAGS))


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest='operation', required=True)
    pack = sub.add_parser('package')
    pack.add_argument('--repo', type=Path, required=True)
    pack.add_argument('--output', type=Path, required=True)
    pack.add_argument('--nonce', required=True)
    for name in ('controls', 'check-recorders'):
        cmd = sub.add_parser(name)
        cmd.add_argument('--repo', type=Path, required=True)
        cmd.add_argument('--root', type=Path, required=True)
        cmd.add_argument('--nonce', required=True)
    rescue = sub.add_parser('rescue-upload')
    rescue.add_argument('--root', type=Path, required=True)
    rescue.add_argument('--nonce', required=True)
    fixture = sub.add_parser('root-fixture')
    fixture.add_argument('--mode', choices=('protect', 'deadline', 'rescue'), required=True)
    fixture.add_argument('--root', type=Path, required=True)
    fixture.add_argument('--runner-temp', type=Path, required=True)
    fixture.add_argument('--nonce', required=True)
    fixture.add_argument('--uid', type=int, required=True)
    fixture.add_argument('--gid', type=int, required=True)
    fixture.add_argument('--deadline-ns', type=int, required=True)
    return p


def main(argv=None):
    args = parser().parse_args(argv)
    try:
        if args.operation == 'root-fixture':
            return root_fixture(args)
        if args.operation == 'package':
            temp = no_links(os.environ['RUNNER_TEMP'])
            require(args.output == temp / ('qualification-' + args.nonce) and re.fullmatch('[0-9a-f]{32}', args.nonce), 'fixed output nonce/path required')
            receipt = package_tooling(args.repo, args.output)
            write_json(args.output / 'control-root.json', dict(version=1, nonce=args.nonce, uid=os.getuid(), gid=os.getgid(), root=str(args.output), runner_temp=str(temp)))
            preflight(args.output)
        elif args.operation == 'rescue-upload':
            rescue_upload(args.root, args.nonce, os.getuid(), os.getgid())
            inventory = make_inventory(args.root)
            write_json(args.root / 'inventory.json', inventory)
            verify_inventory(args.root, strict_json(args.root / 'inventory.json'))
        else:
            receipt = strict_json(args.root / 'tooling-receipt.json')
            validate_root(args.root, args.nonce, os.getuid(), os.getgid())
            if args.operation == 'controls':
                controls(args.repo, receipt, args.root, args.nonce)
            else:
                proof = check_recorders(args.repo, receipt, args.root / 'recorders')
                write_json(args.root / 'recorder-acceptance.json', proof)
        return 0
    except (Error, OSError, KeyError, subprocess.SubprocessError) as error:
        print('qualification refused: ' + str(error), file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
