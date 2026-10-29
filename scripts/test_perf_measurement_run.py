import hashlib
import os
import shutil
import sys
from pathlib import Path
import subprocess
import tempfile
import unittest
import perf_measurement_run as run
import perf_measurement_gate as gate


class OverlayTests(unittest.TestCase):
    def test_legacy_probe_is_derived_only_from_frozen_original(self):
        original = Path('scripts/testdata/perf_measurement_legacy.rs.txt').read_bytes()
        result = run.legacy_harness(original)
        self.assertEqual(result.count('prove_legacy_canary();'), 4)
        self.assertIn('LEGACY_CANARY.store', result)
        self.assertIn('fn bench_indexed_storage_insert(', result)
        self.assertNotIn('EntryPoint::None', result)
        with self.assertRaises(gate.GateError): run.legacy_harness(original + b'\n')

    def test_legacy_probe_explicitly_forwards_both_controls_through_runner_env_clear(self):
        original = Path('scripts/testdata/perf_measurement_legacy.rs.txt').read_bytes()
        generated = run.legacy_harness(original)
        group = generated[generated.index('library_benchmark_group!('):]
        self.assertIn('pass_through_envs(["PRKDB_PERF_CANARY", "PRKDB_PERF_SKIP_OPERATION"])', group)
        self.assertNotIn('env_clear(false)', generated)

    def test_legacy_reproduction_is_only_needed_for_original_measurement_definition(self):
        original = Path('scripts/testdata/perf_measurement_legacy.rs.txt').read_bytes()
        self.assertTrue(run.needs_legacy(original))
        self.assertFalse(run.needs_legacy(Path(run.BENCH).read_bytes()))
        with self.assertRaises(gate.GateError): run.needs_legacy(b'unknown benchmark definition')

    def test_controls_work_in_clean_history_without_original_pr_commit(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            shutil.copytree('scripts', root / 'scripts', ignore=shutil.ignore_patterns('__pycache__'))
            target = root / run.BENCH
            target.parent.mkdir(parents=True)
            shutil.copy2(run.BENCH, target)
            shutil.copy2('Cargo.lock', root / 'Cargo.lock')
            subprocess.run(['git', 'init', '-q', str(root)], check=True)
            subprocess.run(['git', '-C', str(root), 'add', '.'], check=True)
            subprocess.run(['git', '-C', str(root), '-c', 'user.name=Control', '-c',
                            'user.email=control@example.invalid', 'commit', '-qm', 'control'], check=True)
            tests = ('test_legacy_probe_is_derived_only_from_frozen_original',
                     'test_legacy_reproduction_is_only_needed_for_original_measurement_definition',
                     'test_later_source_dependency_change_is_not_rejected_by_frozen_origin')
            result = subprocess.run([sys.executable, '-m', 'unittest'] +
                                    ['test_perf_measurement_run.OverlayTests.' + name for name in tests],
                                    cwd=root, env=dict(os.environ, PYTHONPATH='scripts',
                                                      PYTHONDONTWRITEBYTECODE='1'),
                                    capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_effective_child_control_receipts_are_complete_and_match_selected_case(self):
        valid = 'cargo build noise\n' + 'PRKDB_PERF_EFFECTIVE canary=4096 skip=0\n' * 4
        self.assertEqual(len(run.control_receipts(valid, 4096)), 4)
        for bad in ('', valid.replace('canary=4096', 'canary=0'),
                    valid.replace('skip=0', 'skip=1'), valid + 'PRKDB_PERF_EFFECTIVE canary=4096 skip=0\n',
                    valid.replace('PRKDB_PERF_EFFECTIVE', 'malformed', 1)):
            with self.subTest(bad=bad), self.assertRaises(gate.GateError): run.control_receipts(bad, 4096)

    def test_lock_overlay_preserves_old_versions_and_only_adds_dev_closure(self):
        before = '''version = 4
[[package]]
name = "gungraun"
version = "0.19.4"
dependencies = []
[[package]]
name = "normal"
version = "1"
dependencies = [
 "shlex",
]
[[package]]
name = "shlex"
version = "1.3.0"
'''
        repaired = before.replace('dependencies = []', 'dependencies = [\n "cty",\n "valgrind-requests",\n]', 1).replace(' "shlex",', ' "shlex 1.3.0",') + '''[[package]]
name = "valgrind-requests"
version = "1.2.0"
dependencies = [
 "shlex 2.0.1",
]
[[package]]
name = "shlex"
version = "2.0.1"
[[package]]
name = "cty"
version = "0.2.2"
'''
        _, blocks = run.lock_blocks(repaired)
        old_keys = set(gate.lock_packages(before))
        overlay = 'version = 4\n' + ''.join(v for k, v in blocks.items() if k not in old_keys or k[0] == 'gungraun')
        actual = run.overlay_lock_text(before, overlay)
        gate.lock_overlay(before, actual)
        self.assertEqual(gate.lock_packages(actual), gate.lock_packages(repaired))
        with self.assertRaises(gate.GateError): run.overlay_lock_text(before, overlay.replace("valgrind-requests", "unrelated"))

    def test_later_source_dependency_change_is_not_rejected_by_frozen_origin(self):
        repaired = Path('Cargo.lock').read_text()
        changed = repaired.replace('name = "libc"\nversion = "0.2.189"', 'name = "libc"\nversion = "0.2.190"')
        self.assertNotEqual(changed, repaired)
        try:
            result = run.overlay_lock_text(changed, Path(run.DEV_OVERLAY).read_text())
        except gate.GateError as error:
            self.fail(f'legitimate source dependency version was rejected: {error}')
        self.assertIn('name = "libc"\nversion = "0.2.190"', result)

    def test_dirty_overlay_refuses_any_unexpected_tracked_or_untracked_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            subprocess.run(['git', 'init', '-q', str(root)], check=True)
            subprocess.run(['git', '-C', str(root), 'config', 'user.name', 'Control'], check=True)
            subprocess.run(['git', '-C', str(root), 'config', 'user.email', 'control@example.invalid'], check=True)
            (root / 'production.rs').write_text('original')
            subprocess.run(['git', '-C', str(root), 'add', '.'], check=True)
            subprocess.run(['git', '-C', str(root), 'commit', '-qm', 'control'], check=True)
            run.verify_dirty_paths(root)
            (root / 'production.rs').write_text('changed')
            with self.assertRaises(gate.GateError): run.verify_dirty_paths(root)
            (root / 'production.rs').write_text('original')
            (root / 'unexpected').write_text('new')
            with self.assertRaises(gate.GateError): run.verify_dirty_paths(root)

    def test_killed_command_keeps_live_temporary_raw_in_uploaded_case(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); work = root / 'source'; output = root / 'case'
            (work / run.BENCH).parent.mkdir(parents=True)
            runner = object.__new__(run.PairedRun)
            runner.output = root
            runner.assets = {run.BENCH: b'fixed harness'}
            def command(*args, **kwargs):
                temporary = Path(args[2]['TMPDIR'])
                self.assertTrue(temporary.is_relative_to(output))
                raw = temporary / 'gungraun.tmp0123456789bench_example' / 'callgrind.bench_example.out'
                raw.parent.mkdir(parents=True)
                raw.write_bytes(b'live partial profile')
                raise gate.GateError('simulated command timeout')
            runner.command = command
            with self.assertRaisesRegex(gate.GateError, 'simulated command timeout'):
                runner.benchmark(work, {}, output)
            raw = output / 'temporary/gungraun.tmp0123456789bench_example/callgrind.bench_example.out'
            self.assertEqual(raw.read_bytes(), b'live partial profile')
            self.assertFalse((output / 'gungraun').exists(), 'control must test raw not yet moved to target')

    def test_failed_execution_or_bad_summary_retains_raw_prefix_and_binary(self):
        for failure in ('execution', 'validation'):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp); work = root / 'source'; output = root / 'case'
                (work / run.BENCH).parent.mkdir(parents=True)
                runner = object.__new__(run.PairedRun)
                runner.output = root
                runner.assets = {run.BENCH: b'fixed harness'}
                def command(*args, **kwargs):
                    self.assertIn('--nocapture=stderr', args[0])
                    (root / 'stderr').write_text('PRKDB_PERF_EFFECTIVE canary=0 skip=0\n' * 4)
                    raw = work / 'target' / 'gungraun'; raw.mkdir(parents=True)
                    (raw / 'partial.out').write_text('incomplete raw prefix')
                    binary = work / 'target' / 'release' / 'deps' / 'iai_hot_paths-control'
                    binary.parent.mkdir(parents=True); binary.write_bytes(b'partial binary'); binary.chmod(0o755)
                    if failure == 'execution': raise gate.GateError('original execution failure')
                    return 0, '', root
                runner.command = command
                with self.assertRaises(gate.GateError): runner.benchmark(work, {}, output)
                self.assertTrue((output / 'gungraun' / 'partial.out').is_file(), 'failed raw prefix was lost')
                self.assertEqual((output / 'gungraun' / 'partial.out').read_text(), 'incomplete raw prefix')
                self.assertEqual((output / 'benchmark').read_bytes(), b'partial binary')
                self.assertTrue((output / 'binary.json').exists())
                self.assertFalse(__import__('json').loads((output / 'binary.json').read_text())['raw_profile_validated'])

    def test_overlay_feature_patch_does_not_copy_head_production_dependencies(self):
        old = '[dependencies]\nnormal="1"\n[dev-dependencies]\ngungraun = { version = "=0.19.4" }\n'
        patched = gate.patch_manifest(old)
        self.assertIn('normal="1"', patched)
        self.assertIn('client_requests', patched)


if __name__ == '__main__': unittest.main()
