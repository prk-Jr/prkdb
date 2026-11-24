"""Local contracts; Linux privilege boundaries are simulated, never qualified here."""
import copy
import importlib.util
import json
import os
from pathlib import Path
import shutil
import ast
import subprocess
import sys
import time
import tomllib
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import patch

MODULE = Path(__file__).with_name('perf_trace_linux_qualification.py')
REPO = MODULE.parent.parent


class QualificationTests(unittest.TestCase):
    def driver(self):
        self.assertTrue(MODULE.is_file(), 'qualification driver must exist')
        spec = importlib.util.spec_from_file_location('qualification_driver', MODULE)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.base = Path(self.tmp.name).resolve()

    def source(self, q):
        root = self.base / 'repo'
        root.mkdir()
        for name in q.ALLOWLIST:
            dest = root / name
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(REPO / name, dest)
        return root

    def package(self, q):
        root = self.source(q)
        mock = patch.object(q, 'git_identity', return_value={'head': 'a' * 40, 'clean': True})
        mock.start()
        self.addCleanup(mock.stop)
        receipt = q.package_tooling(root, self.base / 'evidence')
        return root, receipt

    def import_prefix(self, command):
        """Keep actual interpreter flags/script; stop before privileged operation."""
        index = next(i for i, value in enumerate(command)
                     if value == 'python3' or value == sys.executable)
        tail = list(command[index:])
        script = next(i for i, value in enumerate(tail) if str(value).endswith('.py'))
        return [*tail[:script + 1], '--help']

    def check_import_command(self, q, root, receipt, command, negative=False):
        before = q.sources(root)
        env = ['env', '-i', 'PATH=' + os.environ['PATH'], 'LANG=C.UTF-8']
        result = subprocess.run([*env, *command], capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(q.sources(root), before)
        caches = list((Path(receipt['output']) / 'tooling').rglob('*.pyc'))
        if negative:
            self.assertTrue(caches, 'negative control must actually write bytecode')
            with self.assertRaisesRegex(q.Error, 'closure'):
                q.verify_tooling(root, receipt)
        else:
            self.assertEqual(caches, [], 'cleared-environment import changed package closure')
            q.verify_tooling(root, receipt)

    def test_actual_env_i_supervisor_scoped_and_fixture_imports_preserve_package(self):
        q = self.driver()
        root, receipt = self.package(q)
        import stream_durable_diagnostic as stream
        import wal_descriptor_diagnostic as descriptor
        import wal_stall_diagnostic as stall
        archive = Path(receipt['output']) / 'tooling'
        directory = self.base / 'launch'
        directory.mkdir()
        proof = directory / 'ordinary.json'
        proof.write_text(json.dumps(descriptor.resource_proof()))
        deadline = stream.Deadline(time.monotonic_ns() + 60 * 10**9)
        with patch.object(stream, 'SELF', str(archive / 'scripts/stream_durable_diagnostic.py')), patch.object(descriptor, 'SELF', str(archive / 'scripts/wal_descriptor_diagnostic.py')), patch.object(stall, '__file__', str(archive / 'scripts/wal_stall_diagnostic.py')):
            commands = {
                'ownership': stream.ownership_command(directory, directory, deadline),
                'stream-root': stream.root_command(['true'], {}, directory, proof, directory, deadline),
                'descriptor-root': descriptor.root_command(['true'], {}, directory, proof),
                'scoped': descriptor.scoped_command(sys.executable, directory, proof, 'a'*64),
                'stall-root': stall.root_command(['true'], {}, directory),
                'fixture': q.fixture_command(archive.parent, 'protect', 'a'*32, os.getuid(), os.getgid(), deadline.end_ns, self.base),
            }
            # Capture the real nested child-limit producer at its process boundary.
            class Captured(Exception):
                pass
            def capture(command, *args, **kwargs):
                commands['child-limit'] = command
                raise Captured()
            with patch.object(descriptor.os, 'geteuid', return_value=0), patch.object(descriptor, 'budget_breach', return_value=False), patch.object(descriptor.CaptureWatch, 'start'), patch.object(descriptor.CaptureWatch, 'finish'), patch.object(descriptor, 'supervise', side_effect=capture):
                with self.assertRaises(Captured):
                    descriptor.qualified_supervisor(proof, directory, directory, ['true'])
            for label, command in commands.items():
                with self.subTest(producer=label):
                    prefix = self.import_prefix(command)
                    self.check_import_command(q, root, receipt, prefix)
                    self.assertEqual(prefix[1], '-B')

    def test_actual_smoke_producers_cross_scoped_exec_without_bytecode(self):
        q = self.driver()
        root, receipt = self.package(q)
        import stream_durable_diagnostic as stream
        import wal_descriptor_diagnostic as descriptor
        import wal_trace_qualification as trace
        archive = Path(receipt['output']) / 'tooling'
        smoke_script = str(archive / 'scripts/wal_descriptor_diagnostic.py')
        captured = []
        class Captured(Exception):
            pass
        def capture(binary, directory, proof, sha, arguments=()):
            captured.append((binary, directory, proof, sha, arguments))
            raise Captured()
        # These stop at the recorder boundary, after each real smoke argv producer.
        with patch.object(descriptor, 'SELF', smoke_script), patch.object(descriptor, 'profile_command', side_effect=capture), patch.object(descriptor, 'preflight', return_value={}), patch.object(descriptor, 'proc_identity', return_value=descriptor.resource_proof()), patch.object(trace, 'retain_ownership'), patch.object(descriptor, 'save_inventory'):
            with self.assertRaises(Captured):
                trace.qualify(self.base / 'trace')
        output = self.base / 'stream'
        output.mkdir()
        runtime = output / 'runtime'
        runtime.mkdir()
        with patch.object(descriptor, 'SELF', smoke_script), patch.object(stream, 'cpu_frequency_limit', return_value={}), patch.object(stream, 'profile_command', side_effect=lambda category, *args: capture(*args)):
            with self.assertRaises(Captured):
                stream.qualify_recorders(output, runtime, output / 'ordinary.json', {}, stream.Deadline(time.monotonic_ns()+60*10**9))
        # Standalone descriptor run reaches the smoke cell without running a benchmark.
        artifact = self.base / 'artifact'
        artifact.mkdir()
        for side in ('base', 'head'):
            (artifact / (side + '-benchmark-executable')).write_bytes(b'fixture')
        pins = {side: dict(binary_sha256=descriptor.digest(artifact / (side + '-benchmark-executable'))) for side in ('base', 'head')}
        with patch.object(descriptor, 'SELF', smoke_script), patch.object(descriptor, 'verify_artifact', return_value={}), patch.object(descriptor, 'preflight', return_value={}), patch.object(descriptor, 'PINNED', pins), patch.object(descriptor, 'quiet'), patch.object(descriptor, 'sequence', return_value=[dict(index=4)]), patch.object(descriptor.subprocess, 'run', return_value=SimpleNamespace(returncode=0, stdout='', stderr='')), patch.object(descriptor, 'profile_command', side_effect=capture), patch.object(descriptor, 'save_inventory'):
            with self.assertRaises(Captured):
                descriptor.run(artifact, self.base / 'descriptor')
        self.assertEqual(len(captured), 3)
        for index, (binary, directory, proof, sha, arguments) in enumerate(captured):
            # Execute the actual scoped command through its real os.execve transition.
            ordinary = descriptor.resource_proof()
            Path(proof).write_text(json.dumps(ordinary))
            with patch.object(descriptor, 'SELF', smoke_script):
                command = descriptor.scoped_command(binary, directory, proof, sha, [*arguments[:arguments.index('smoke-target')], '--help'])
            with self.subTest(producer=index):
                self.check_import_command(q, root, receipt, command)
                self.assertIn('-B', arguments[:arguments.index('smoke-target')])

    def test_missing_bytecode_flag_still_creates_cache_and_is_refused(self):
        q = self.driver()
        root, receipt = self.package(q)
        import stream_durable_diagnostic as stream
        archive = Path(receipt['output']) / 'tooling'
        with patch.object(stream, 'SELF', str(archive / 'scripts/stream_durable_diagnostic.py')):
            command = self.import_prefix(stream.ownership_command(self.base, self.base, stream.Deadline(time.monotonic_ns()+10**9)))
        command = [value for value in command if value != '-B']
        self.check_import_command(q, root, receipt, command, negative=True)

    def test_scoped_flag_does_not_hide_missing_smoke_exec_flag(self):
        q = self.driver()
        root, receipt = self.package(q)
        import wal_descriptor_diagnostic as descriptor
        archive = Path(receipt['output']) / 'tooling'
        directory = self.base / 'exec-negative'
        directory.mkdir()
        proof = directory / 'ordinary.json'
        proof.write_text(json.dumps(descriptor.resource_proof()))
        binary = directory / 'wal-diag-smoke'
        shutil.copyfile(Path(sys.executable).resolve(), binary)
        binary.chmod(0o755)
        with patch.object(descriptor, 'SELF', str(archive / 'scripts/wal_descriptor_diagnostic.py')):
            # The outer scoped interpreter is protected; os.execve starts a new
            # interpreter whose missing policy must still be detected.
            command = descriptor.scoped_command(binary, directory, proof, descriptor.digest(binary), [descriptor.SELF, '--help'])
        self.assertEqual(command[1], '-B')
        self.check_import_command(q, root, receipt, command, negative=True)

    def test_actual_protected_driver_decode_import_preserves_package(self):
        q = self.driver()
        root, receipt = self.package(q)
        import stream_durable_diagnostic as native
        evidence = Path(receipt['output'])
        fixture = evidence / 'tooling/scripts/testdata/perf_trace/linux7-syscalls.perf'
        commands = []
        class Captured(Exception):
            pass
        def execute(command, *args, **kwargs):
            if not commands:
                target = evidence / 'protected/input.perf'
                target.write_bytes(fixture.read_bytes())
                q.write_json(evidence / 'protected/root-input.json', dict(uid=0, mode=0o600, sha256=q.FIXTURE_HASH))
                commands.append(command)
            else:
                commands.append(command)
                raise Captured()
        with patch.object(native, 'execute', side_effect=execute), patch.object(q, 'eacces', return_value=True):
            with self.assertRaises(Captured):
                q.fixed_control(evidence, 'protected', 'a'*32, os.getuid(), os.getgid(), time.monotonic_ns()+60*10**9, self.base)
        # Remove the outer supervisor/perf boundary, preserving the actual driver
        # interpreter and its script prefix, and execute its real import closure.
        index = commands[-1].index(sys.executable)
        prefix = self.import_prefix(commands[-1][index:])
        self.check_import_command(q, root, receipt, prefix)
        self.assertEqual(prefix[1], '-B')

    def test_candidate_closure_preserves_public_security_and_workflow_contracts(self):
        q = self.driver()
        candidate_docs = {
            'docs/superpowers/specs/2026-11-24-linux-qualification-candidate-design.md',
            'docs/superpowers/plans/2026-11-24-linux-qualification-candidate.md',
        }
        self.assertTrue(candidate_docs <= set(q.ALLOWLIST), 'candidate design and plan must be packaged')
        self.assertEqual(len(q.ALLOWLIST), 37)
        root = self.source(q)
        self.assertTrue(candidate_docs <= set(q.sources(root)))
        # The packaged closure excludes Cargo inputs; inspect the actual checkout,
        # including when this test is run from the workflow's archived tooling.
        checkout = Path(os.environ.get('GITHUB_WORKSPACE', str(REPO)))
        toolchain = tomllib.loads((checkout / 'rust-toolchain.toml').read_text())
        cargo = tomllib.loads((checkout / 'Cargo.toml').read_text())
        lock = tomllib.loads((checkout / 'Cargo.lock').read_text())
        self.assertEqual(toolchain['toolchain']['channel'], '1.100.0')
        self.assertEqual(cargo['workspace']['package']['rust-version'], '1.100.0')
        self.assertEqual([row['version'] for row in lock['package'] if row['name'] == 'url'], ['2.5.9'])
        workflow = (root / '.github/workflows/remediation-gate.yml').read_text()
        ordinary = workflow[:workflow.index('  stream-framed-recorders:')]
        self.assertEqual(ordinary.count('dtolnay/rust-toolchain@1.100.0'), 2)
        self.assertIn('cargo xtask verify --profile blocking --seeds 1000 --seed-offset "$seed_offset" --mode durable', ordinary)
        self.assertIn('timeout-minutes: 120', ordinary)
        self.assertIn('timeout-minutes: 35', workflow)
        self.assertIn('timeout-minutes: 10', workflow[workflow.index('  stream-framed-controls:'):])

    def test_launch_overlay_exact_schema_and_bindings(self):
        q = self.driver()
        root = self.source(q)
        overlay_path = root / 'scripts/testdata/perf_trace/linux-qualification-launches.json'
        self.assertTrue(overlay_path.is_file(), 'explicit launch overlay required')
        original = json.loads(overlay_path.read_text())
        self.assertEqual(len(q.ALLOWLIST), 37)
        self.assertEqual(len(original['files']), 4)
        q.sources(root)
        mutations = []
        for key, value in [('version', True), ('version', 2), ('base_revision', 'a'*40), ('base_revision', []), ('reason', 'other'), ('reason', False), ('files', [])]:
            doc = copy.deepcopy(original)
            doc[key] = value
            mutations.append(doc)
        doc = copy.deepcopy(original)
        doc['extra'] = 1
        mutations.append(doc)
        first = next(iter(original['files']))
        doc = copy.deepcopy(original)
        doc['files'].pop(first)
        mutations.append(doc)
        doc = copy.deepcopy(original)
        doc['files']['scripts/arbitrary.py'] = doc['files'][first]
        mutations.append(doc)
        for key, value in [('base_blob', '0'*40), ('base_blob', []), ('base_sha256', '0'*64), ('base_sha256', False), ('blob', '0'*40), ('blob', []), ('sha256', '0'*64), ('sha256', 7), ('bytes', original['files'][first]['bytes'] + 1), ('bytes', True), ('bytes', -1), ('bytes', str(original['files'][first]['bytes'])), ('extra', 1)]:
            doc = copy.deepcopy(original)
            doc['files'][first][key] = value
            mutations.append(doc)
        for index, doc in enumerate(mutations):
            with self.subTest(mutation=index):
                overlay_path.write_text(json.dumps(doc))
                with self.assertRaises(q.Error):
                    q.sources(root)
        for text in ('{', '{"version":1,"version":1}', '[]', 'null'):
            overlay_path.write_text(text)
            with self.assertRaises(q.Error):
                q.sources(root)
        overlay_path.write_text(json.dumps(original))
        historical = root / q.SOURCE_MANIFEST
        historical.write_bytes(historical.read_bytes() + b'\n')
        with self.assertRaises(q.Error):
            q.sources(root)

    def test_launch_overlay_archive_and_current_source_drift_are_refused(self):
        q = self.driver()
        root, receipt = self.package(q)
        path = 'scripts/testdata/perf_trace/linux-qualification-launches.json'
        archived = Path(receipt['output']) / 'tooling' / path
        archived.write_bytes(archived.read_bytes() + b'\n')
        with self.assertRaises(q.Error):
            q.verify_tooling(root, receipt)
        shutil.copyfile(root / path, archived)
        overlay = root / path
        data = overlay.read_bytes()
        overlay.write_bytes(data + b'\n')
        with self.assertRaises(q.Error):
            q.verify_tooling(root, receipt)
        overlay.write_bytes(data)
        (root / 'scripts/wal_descriptor_diagnostic.py').write_bytes(b'changed')
        with self.assertRaises(q.Error):
            q.verify_tooling(root, receipt)

    def test_complete_exact_package_and_drift_refusal(self):
        q = self.driver()
        root, receipt = self.package(q)
        self.assertEqual(set(receipt['files']), set(q.ALLOWLIST))
        q.verify_tooling(root, receipt)
        for name in q.RUNTIME_MODULES:
            self.assertIn('scripts/' + name + '.py', receipt['files'])
        path = root / 'scripts/perf_trace_format.py'
        path.write_bytes(path.read_bytes() + b'\n')
        with self.assertRaises(q.Error):
            q.verify_tooling(root, receipt)

    def test_missing_module_and_nonfresh_output_fail(self):
        q = self.driver()
        root = self.source(q)
        (root / 'scripts/perf_trace_records.py').unlink()
        with self.assertRaises(q.Error):
            q.package_tooling(root, self.base / 'out')
        root = REPO
        with self.assertRaises(q.Error):
            q.package_tooling(root, self.base)

    def test_manifest_schema_and_path_attacks(self):
        q = self.driver()
        root = self.source(q)
        path = root / q.SOURCE_MANIFEST
        original = json.loads(path.read_text())
        for attack in ('../escape', '/absolute', 'scripts/../escape', 'scripts//double'):
            doc = copy.deepcopy(original)
            doc['files'][attack] = doc['files'].pop(next(iter(doc['files'])))
            path.write_text(json.dumps(doc))
            with self.assertRaises(q.Error, msg=attack):
                q.package_tooling(root, self.base / 'out')
        path.write_text('{"version":1,"version":1}')
        with self.assertRaises(q.Error):
            q.package_tooling(root, self.base / 'out')

    def test_file_and_ancestor_symlinks_are_refused(self):
        q = self.driver()
        root = self.source(q)
        path = root / 'scripts/perf_trace_format.py'
        path.unlink()
        path.symlink_to(REPO / 'scripts/perf_trace_format.py')
        with self.assertRaises(q.Error):
            q.package_tooling(root, self.base / 'out')
        path.unlink()
        shutil.copyfile(REPO / 'scripts/perf_trace_format.py', path)
        scripts = root / 'scripts'
        scripts.rename(root / 'moved')
        scripts.symlink_to(root / 'moved', target_is_directory=True)
        with self.assertRaises(q.Error):
            q.package_tooling(root, self.base / 'out')

    def test_receipt_missing_hash_and_package_tampering(self):
        q = self.driver()
        root, receipt = self.package(q)
        bad = copy.deepcopy(receipt)
        bad['files'].pop('scripts/perf_trace_decode.py')
        with self.assertRaises(q.Error):
            q.verify_tooling(root, bad)
        (self.base / 'evidence/tooling/scripts/perf_trace_decode.py').write_text('tampered')
        with self.assertRaises(q.Error):
            q.verify_tooling(root, receipt)

    def test_recorders_refuse_partial_success_and_db(self):
        q = self.driver()
        good = dict(complete=True, qualification_only=True, mode='recorder-qualification',
                    benchmark_invocations=0, build_provenance_checked=False,
                    diagnostic_only=True, timing_eligible=False)
        categories = {n: {'qualified': True, 'benchmark_invocations': 0} for n in ('cpu', 'syscalls', 'scheduler')}
        q.validate_recorders(good, categories)
        for key, value in [('complete', False), ('qualification_only', False), ('mode', 'database-diagnostic'),
                           ('benchmark_invocations', 1), ('benchmark_invocations', False),
                           ('build_provenance_checked', True)]:
            with self.assertRaises(q.Error, msg=key):
                q.validate_recorders(dict(good, **{key: value}), categories)
        for bad in ({}, dict(categories, other={}), {'cpu': categories['cpu']},
                    dict(categories, scheduler={'qualified': False})):
            with self.assertRaises(q.Error):
                q.validate_recorders(good, bad)

    def test_inventory_refuses_duplicate_escape_hash_and_missing_files(self):
        q = self.driver()
        root = self.base / 'native'
        root.mkdir()
        (root / 'data').write_bytes(b'evidence')
        inventory = q.make_inventory(root)
        q.verify_inventory(root, inventory)
        for rows in (inventory['files'] * 2, [], [dict(inventory['files'][0], path='../data')],
                     [dict(inventory['files'][0], sha256='0' * 64)]):
            with self.assertRaises(q.Error):
                q.verify_inventory(root, dict(inventory, files=rows))

    def test_control_verdict_rejects_unknown_group_survival_and_masking(self):
        q = self.driver()
        proof = {'native_failed': True, 'eacces': True, 'unchanged': True, 'rescued': True,
                 'actual_group_known': True, 'contained': True, 'child_absent': True}
        q.validate_control('rescue', proof)
        for key in proof:
            with self.assertRaises(q.Error, msg=key):
                q.validate_control('rescue', dict(proof, **{key: False}))
        with self.assertRaises(q.Error):
            q.validate_control('deadline', dict(proof, timed_out=False))

    def test_no_arbitrary_cli_or_database_mode(self):
        q = self.driver()
        for args in (['run'], ['restore'], ['root-fixture', '--mode', 'exec'], ['controls', '--command', 'cargo']):
            with self.assertRaises(SystemExit):
                q.parser().parse_args(args)

    def test_fixed_rescue_command_and_timeout_failure(self):
        q = self.driver()
        with patch.object(q, 'validate_root', return_value=self.base), patch.object(q, 'run_command') as run:
            run.return_value = {'exit_code': 1}
            with self.assertRaises(q.Error):
                q.rescue_upload(self.base, 'a' * 32, os.getuid(), os.getgid())
            argv = run.call_args.args[0]
            self.assertEqual(argv[:6], ['sudo', '-n', 'timeout', '--signal=KILL', '30s', 'chown'])
            self.assertIn('--preserve-root', argv)
            self.assertEqual(run.call_args.kwargs['timeout'], 35)


    def test_inventory_fifo_is_bounded_refusal(self):
        fifo = self.base / 'fifo'
        os.mkfifo(fifo)
        code = "import perf_trace_linux_qualification as q,sys;\ntry:q.make_inventory(sys.argv[1])\nexcept q.Error:raise SystemExit(0)\nraise SystemExit(1)"
        result = subprocess.run([sys.executable, '-c', code, str(self.base)], env=dict(os.environ, PYTHONPATH=str(MODULE.parent)), capture_output=True, timeout=1)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_head_change_and_receipt_schema_fail(self):
        q = self.driver()
        root, receipt = self.package(q)
        with patch.object(q, 'git_identity', return_value={'head': 'b' * 40, 'clean': True}):
            with self.assertRaises(q.Error):
                q.verify_tooling(root, receipt)
        for key, value in [('version', True), ('provenance', {}), ('git', {'head': 'a' * 40, 'clean': False})]:
            with self.assertRaises(q.Error):
                q.verify_tooling(root, dict(receipt, **{key: value}))

    def test_regular_read_refuses_fifo_and_oversize(self):
        q = self.driver()
        path = self.base / 'fifo'
        os.mkfifo(path)
        with self.assertRaises(q.Error):
            q.read_regular(path)
        path.unlink()
        path.write_bytes(b'12345')
        with self.assertRaises(q.Error):
            q.read_regular(path, limit=4)

    def test_real_eacces_only_and_missing_helper(self):
        q = self.driver()
        path = self.base / 'prefix'
        path.write_bytes(b'readable')
        with self.assertRaises(q.Error):
            q.eacces(path)
        with patch.object(Path, 'open', side_effect=PermissionError(13, 'Permission denied')):
            self.assertTrue(q.eacces(path))
        with patch.object(Path, 'open', side_effect=FileNotFoundError(2, 'absent')):
            with self.assertRaises(q.Error):
                q.eacces(path)
        result = q.run_command(['/definitely/absent/helper'], self.base / 'missing-helper', timeout=1)
        self.assertIsNone(result['exit_code'])
        self.assertTrue(result['error'])
        self.assertTrue((self.base / 'missing-helper/command.json').exists())

    def test_fixed_root_boundary_nonce_owner_deadline_symlink(self):
        q = self.driver()
        nonce = 'a' * 32
        root = self.base / ('qualification-' + nonce)
        root.mkdir(mode=0o700)
        q.write_json(root / 'control-root.json', dict(version=1, nonce=nonce, uid=os.getuid(), gid=os.getgid(), root=str(root), runner_temp=str(self.base)))
        q.validate_root(root, nonce, os.getuid(), os.getgid(), time.monotonic_ns()+10**9, self.base)
        for bad in ('b' * 32, '../escape', ''):
            with self.assertRaises(q.Error):
                q.validate_root(root, bad, os.getuid(), os.getgid(), runner_temp=self.base)
        with self.assertRaises(q.Error):
            q.validate_root(root, nonce, os.getuid(), os.getgid(), time.monotonic_ns()-1, self.base)
        (root / 'link').symlink_to(self.base)
        with self.assertRaises(q.Error):
            q.validate_root(root, nonce, os.getuid(), os.getgid(), runner_temp=self.base)

    def test_actual_cleanup_and_finalizer_rescue_path_preserves_failure(self):
        q = self.driver()
        import stream_durable_diagnostic as native
        root = self.base / 'root'
        root.mkdir()
        original = native.ownership_command
        class Process:
            pid = 99999
            def __init__(self, command, **kwargs):
                self.command = command
                directory = root / 'rescue'
                (directory / 'unreadable-prefix').write_bytes(b'controlled finalization failure prefix\n')
                q.write_json(directory / 'prefix-before.json', dict(uid=0, gid=0, mode=0o600, **q.identity(b'controlled finalization failure prefix\n')))
                q.write_json(directory / 'finalization/root-group.json', {'child_pid': 424242, 'pgid': 424242})
            def wait(self, timeout):
                return 23
        def repair(*args, **kwargs):
            self.assertTrue(kwargs['control'])
            self.assertIs(native.ownership_command, original)
            result = q.strict_json(root / 'rescue/result.json')
            self.assertFalse(result['complete'])
            self.assertEqual(result['verdict'], 'inconclusive')
            return {'exit_code': 0}
        with patch.object(native.subprocess, 'Popen', Process), patch.object(native.legacy, 'kill_group'), patch.object(q, 'eacces', return_value=True) as denied, patch.object(q, 'process_absent', return_value={'absent': True}), patch.object(q, 'rescue_upload', side_effect=repair), patch.object(q, 'file_stat', side_effect=[dict(uid=0,gid=0,mode=0o600), dict(uid=os.getuid(),gid=os.getgid(),mode=0o600)]):
            proof = q.fixed_control(root, 'rescue', 'a'*32, os.getuid(), os.getgid(), time.monotonic_ns()+120*10**9, self.base)
        self.assertTrue(proof['native_failed'])
        self.assertTrue(proof['unchanged'])
        self.assertEqual(proof['prefix_before'], dict(uid=0,gid=0,mode=0o600,**q.identity(b'controlled finalization failure prefix\n')))
        self.assertEqual(proof['prefix_after'], dict(uid=os.getuid(),gid=os.getgid(),mode=0o600,**q.identity(b'controlled finalization failure prefix\n')))
        self.assertEqual(q.strict_json(root / 'rescue/prefix-after.json'), proof['prefix_after'])
        denied.assert_called_once()
        self.assertIs(native.ownership_command, original)
        self.assertEqual(q.strict_json(root / 'rescue/ownership.json')['exit_code'], 23)

    def test_rescue_rejects_wrong_before_or_after_owner_mode_hash(self):
        q = self.driver()
        import stream_durable_diagnostic as native
        payload = b'controlled finalization failure prefix\n'
        for scenario in ('before-owner', 'before-mode', 'before-hash', 'after-owner'):
            root = self.base / scenario
            root.mkdir()
            before = dict(uid=0, gid=0, mode=0o600, **q.identity(payload))
            if scenario == 'before-owner':
                before['uid'] = os.getuid()
            elif scenario == 'before-mode':
                before['mode'] = 0o644
            elif scenario == 'before-hash':
                before['sha256'] = '0'*64
            class Process:
                pid = 99999
                def __init__(self, command, **kwargs):
                    directory = root / 'rescue'
                    (directory / 'unreadable-prefix').write_bytes(payload)
                    q.write_json(directory / 'prefix-before.json', before)
                    q.write_json(directory / 'finalization/root-group.json', {'child_pid':424242, 'pgid':424242})
                def wait(self, timeout):
                    return 23
            with patch.object(native.subprocess, 'Popen', Process), patch.object(native.legacy, 'kill_group'), patch.object(q, 'process_absent', return_value={'absent':True}), patch.object(q, 'file_stat', return_value=dict(uid=0,gid=0,mode=0o600)), patch.object(q, 'eacces', return_value=True) as denied, patch.object(q, 'rescue_upload', return_value={'exit_code':0}) as repair:
                with self.assertRaises(q.Error):
                    q.fixed_control(root, 'rescue', 'a'*32, os.getuid(), os.getgid(), time.monotonic_ns()+120*10**9, self.base)
            if scenario.startswith('before-'):
                denied.assert_not_called()
                repair.assert_not_called()
            else:
                denied.assert_called_once()
                repair.assert_called_once()
                self.assertTrue((root / 'rescue/prefix-after.json').is_file())
            self.assertFalse((root / 'rescue/control-receipt.json').exists())
            self.assertFalse(q.strict_json(root / 'rescue/result.json')['complete'])

    def test_actual_cleanup_rejects_survivor_and_unknown_witness(self):
        q = self.driver()
        import stream_durable_diagnostic as native
        original = native.ownership_command
        for witness, absent in [(False, True), (True, False)]:
            root = self.base / ('case-' + str(witness))
            root.mkdir()
            class Process:
                pid = 99999
                def __init__(self, *args, **kwargs):
                    directory = root / 'rescue'
                    (directory / 'unreadable-prefix').write_bytes(b'controlled finalization failure prefix\n')
                    if witness:
                        q.write_json(directory / 'finalization/root-group.json', {'child_pid': 424242, 'pgid': 424242})
                def wait(self, timeout):
                    return 23
            with patch.object(native.subprocess, 'Popen', Process), patch.object(native.legacy, 'kill_group'), patch.object(q, 'process_absent', return_value={'absent': absent}), patch.object(q, 'eacces', return_value=True), patch.object(q, 'rescue_upload', return_value={'exit_code': 0}):
                with self.assertRaises(q.Error):
                    q.fixed_control(root, 'rescue', 'a'*32, os.getuid(), os.getgid(), time.monotonic_ns()+120*10**9, self.base)
            self.assertIs(native.ownership_command, original)

    def test_actual_root_fixture_entries_use_descriptor_resource_and_supervise_apis(self):
        q = self.driver()
        import stream_durable_diagnostic as native
        nonce = 'a' * 32
        resources = {'uid': 0, 'gid': 0, 'nofile': [1024, 1024], 'fixture_os_boundary_simulation': True}
        for mode, expected in (('protect', 0), ('deadline', -9), ('rescue', 23)):
            temp = self.base / mode
            temp.mkdir()
            root = temp / ('qualification-' + nonce)
            root.mkdir(mode=0o700)
            q.write_json(root / 'control-root.json', dict(version=1, nonce=nonce, uid=os.getuid(), gid=os.getgid(), root=str(root), runner_temp=str(temp)))
            fixture = root / 'tooling/scripts/testdata/perf_trace/linux7-syscalls.perf'
            fixture.parent.mkdir(parents=True)
            shutil.copyfile(REPO / 'scripts/testdata/perf_trace/linux7-syscalls.perf', fixture)
            if mode == 'protect':
                (root / 'protected').mkdir()
            else:
                (root / mode / 'finalization').mkdir(parents=True)
            args = q.parser().parse_args(['root-fixture', '--mode', mode, '--root', str(root), '--runner-temp', str(temp), '--nonce', nonce, '--uid', str(os.getuid()), '--gid', str(os.getgid()), '--deadline-ns', str(time.monotonic_ns()+60*10**9)])
            with patch.object(q.sys, 'platform', 'linux'), patch.object(q.os, 'geteuid', return_value=0), patch.object(native.legacy, 'resource_proof', return_value=resources) as proof, patch.object(native.legacy, 'supervise', return_value=expected) as supervise:
                self.assertEqual(q.root_fixture(args), expected)
            if mode == 'protect':
                protected = root / 'protected/input.perf'
                self.assertEqual(q.identity(protected.read_bytes())['sha256'], q.FIXTURE_HASH)
                self.assertEqual(protected.stat().st_mode & 0o777, 0o600)
                self.assertEqual(q.strict_json(root / 'protected/root-input.json')['resources'], resources)
                proof.assert_called_once_with()
                supervise.assert_not_called()
            else:
                self.assertEqual(proof.call_count, 2)
                command, witness = supervise.call_args.args
                self.assertEqual(witness, root / mode / 'finalization/root-group.json')
                self.assertEqual(supervise.call_args.kwargs, {'timeout': 120})
                self.assertEqual(q.strict_json(root / mode / 'finalization/fixture-root-before.json'), resources)
                self.assertEqual(q.strict_json(root / mode / 'finalization/fixture-root-after.json'), resources)
                if mode == 'rescue':
                    self.assertEqual(command, [sys.executable, '-B', '-c', 'raise SystemExit(23)'])
                    child = subprocess.run(['env', '-i', *command], capture_output=True, timeout=5)
                    self.assertEqual(child.returncode, 23)
                    prefix = root / 'rescue/unreadable-prefix'
                    self.assertEqual(prefix.read_bytes(), b'controlled finalization failure prefix\n')
                    self.assertEqual(prefix.stat().st_mode & 0o777, 0o600)
                    receipt = root / 'rescue/prefix-before.json'
                    self.assertTrue(receipt.is_file(), 'root fixture must retain actual prefix ownership/mode/hash')
                    self.assertEqual(q.strict_json(receipt), dict(uid=prefix.stat().st_uid, gid=prefix.stat().st_gid, mode=0o600, **q.identity(prefix.read_bytes())))
                else:
                    self.assertEqual(command[:3], [sys.executable, '-B', '-c'])
                    self.assertIn('os.pipe()', command[3])
                    self.assertIn('os.read(r,1)', command[3])
                    self.assertNotIn('sleep', command[3])
                    self.assertEqual(command[4], str(root / 'deadline/child-live.json'))

    def test_protected_control_runs_actual_execute_decoder_and_verify_input(self):
        q = self.driver()
        import stream_durable_diagnostic as native
        import perf_trace_integration as integration
        fixture = REPO / 'scripts/testdata/perf_trace/linux7-syscalls.perf'
        for scenario in ('success', 'helper-failure', 'missing-eacces', 'hash-drift'):
            root = self.base / scenario
            target = root / 'tooling/scripts/testdata/perf_trace/linux7-syscalls.perf'
            target.parent.mkdir(parents=True)
            shutil.copyfile(fixture, target)
            denied = []
            launched = []
            class Process:
                pid = 99999
                def __init__(self, command, stdout, **kwargs):
                    launched.append(command)
                    if '--mode' in command:
                        (root / 'protected/input.perf').write_bytes(fixture.read_bytes())
                        q.write_json(root / 'protected/root-input.json', dict(uid=0,gid=0,mode=0o600,**q.identity(fixture.read_bytes())))
                    else:
                        self.assert_denied()
                        doc = (integration.build_receipt if '--verify-input' in command else integration.build_envelope)('syscalls', fixture)
                        if scenario == 'hash-drift':
                            doc['input']['sha256'] = '0'*64
                        stdout.write(json.dumps(doc))
                def assert_denied(self):
                    if not denied:
                        raise AssertionError('privileged decode launched without ordinary EACCES proof')
                def wait(self, timeout):
                    return 9 if scenario == 'helper-failure' else 0
            def deny(path):
                if scenario == 'missing-eacces':
                    raise q.Error('ordinary input unexpectedly readable')
                denied.append(path)
                return True
            with patch.object(native.subprocess, 'Popen', Process), patch.object(native.legacy, 'kill_group'), patch.object(q, 'eacces', side_effect=deny):
                if scenario == 'success':
                    proof = q.fixed_control(root, 'protected', 'a'*32, os.getuid(), os.getgid(), time.monotonic_ns()+120*10**9, self.base)
                    self.assertTrue(proof['decoder_qualified'])
                    self.assertTrue(proof['fixture_only'])
                    self.assertEqual(len(launched), 3)
                    self.assertIn('--verify-input', launched[-1])
                    self.assertTrue(all('supervisor' in command for command in launched))
                else:
                    with self.assertRaises((q.Error, native.DiagnosticError)):
                        q.fixed_control(root, 'protected', 'a'*32, os.getuid(), os.getgid(), time.monotonic_ns()+120*10**9, self.base)
                    self.assertFalse((root / 'protected/control-receipt.json').exists())

    def test_actual_cleanup_timeout_requires_signal_probe_and_live_witness(self):
        q = self.driver()
        import stream_durable_diagnostic as native
        for probe_absent in (True, False):
            root = self.base / ('deadline-' + str(probe_absent))
            root.mkdir()
            class Process:
                pid = 99999
                calls = 0
                def __init__(self, *args, **kwargs):
                    directory = root / 'deadline'
                    q.write_json(directory / 'finalization/root-group.json', {'supervisor_pid': 313131, 'child_pid': 424242, 'pgid': 424242})
                    q.write_json(directory / 'child-live.json', {'pid':424242, 'pgid':424242, 'live_ns':time.monotonic_ns()})
                def wait(self, timeout):
                    self.calls += 1
                    if self.calls == 1:
                        raise subprocess.TimeoutExpired('fixed root fixture', timeout)
                    return -9
            def signal_probe(argv, **kwargs):
                self.assertEqual(argv[-1], '-424242')
                is_probe = '-0' in argv
                return SimpleNamespace(returncode=1 if is_probe and probe_absent else 0, stdout='', stderr='No such process' if is_probe and probe_absent else '')
            with patch.object(native.subprocess, 'Popen', Process), patch.object(native.subprocess, 'run', side_effect=signal_probe) as signals, patch.object(native.legacy, 'kill_group'), patch.object(q, 'process_absent', return_value={'absent':probe_absent}):
                if probe_absent:
                    proof = q.fixed_control(root, 'deadline', 'a'*32, os.getuid(), os.getgid(), time.monotonic_ns()+120*10**9, self.base)
                    self.assertTrue(proof['timed_out'])
                    self.assertEqual(signals.call_count, 2)
                else:
                    with self.assertRaises(q.Error):
                        q.fixed_control(root, 'deadline', 'a'*32, os.getuid(), os.getgid(), time.monotonic_ns()+120*10**9, self.base)

    def test_controls_stop_after_first_unexpected_result(self):
        q = self.driver()
        with patch.object(q.sys, 'platform', 'linux'), patch.object(q, 'verify_tooling'), patch.object(q, 'validate_root', return_value=self.base), patch.object(q, 'fixed_control', side_effect=q.Error('unexpected')) as control:
            with self.assertRaises(q.Error):
                q.controls(REPO, {}, self.base, 'a'*32)
        self.assertEqual(control.call_count, 1)
        self.assertFalse(q.strict_json(self.base / 'controls-result.json')['complete'])

    def test_workflow_embedded_python_and_actual_resolver_shell(self):
        workflow = (REPO / '.github/workflows/remediation-gate.yml').read_text()
        lines = workflow.splitlines()
        for index, line in enumerate(lines):
            if line.strip() == "python3 - <<'PY'":
                indent = len(line)-len(line.lstrip())
                end = next(i for i in range(index+1, len(lines)) if lines[i].strip() == 'PY')
                ast.parse('\n'.join(value[indent:] for value in lines[index+1:end]))
        begin = lines.index('          sha="$(git rev-parse HEAD)"')
        end = next(i for i in range(begin, len(lines)) if 'Gate phase $PHASE' in lines[i])+1
        script = '\n'.join(line[10:] for line in lines[begin:end])
        gitroot = Path(os.environ.get('GITHUB_WORKSPACE', str(REPO)))
        head = subprocess.check_output(['git', '-C', str(gitroot), 'rev-parse', 'HEAD'], text=True).strip()
        for probe, ref, workflow_sha, accepted in [('none', 'branch', 'irrelevant', True), ('stream-framed-qualification', head, head, True), ('stream-framed-qualification', 'branch', head, False), ('stream-framed-qualification', head, 'a'*40, False)]:
            env = dict(os.environ, PROBE=probe, REF=ref, WORKFLOW_SHA=workflow_sha, PHASE='2', SELECTED_REF='refs/heads/probe', REPOSITORY='owner/repo', WORKFLOW_REF='owner/repo/.github/workflows/remediation-gate.yml@refs/heads/probe', GITHUB_OUTPUT=str(self.base/'out'), GITHUB_STEP_SUMMARY=str(self.base/'summary'))
            result = subprocess.run(['bash', '-e', '-c', script], cwd=gitroot, env=env, capture_output=True, text=True, timeout=5)
            self.assertEqual(result.returncode == 0, accepted, result.stderr)
        self.assertEqual(workflow.count(' qualify-recorders --output '), 1)
        self.assertIn("if: ${{ always() && inputs.probe != 'stream-framed-qualification' }}", workflow)
        self.assertIn("needs: [resolve, stream-framed-recorders]", workflow)
        self.assertIn("needs.resolve.result == 'success'", workflow)
        probes = workflow[workflow.index('  stream-framed-recorders:'):]
        for forbidden in ('cargo ', ' restore ', 'download-artifact', 'apt-get', 'secrets.'):
            self.assertNotIn(forbidden, probes)
        self.assertEqual(probes.count('runs-on: ubuntu-24.04'), 2)
        self.assertEqual(probes.count('if-no-files-found: error'), 2)
        self.assertEqual(probes.count('retention-days: 30'), 2)
        self.assertEqual(probes.count('persist-credentials: false'), 2)


if __name__ == '__main__':
    unittest.main()
