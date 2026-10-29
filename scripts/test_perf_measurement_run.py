import hashlib
from pathlib import Path
import subprocess
import tempfile
import unittest
import perf_measurement_run as run
import perf_measurement_gate as gate


class OverlayTests(unittest.TestCase):
    def test_legacy_probe_is_derived_only_from_frozen_original(self):
        original = subprocess.check_output(['git', 'show', run.LEGACY_SHA + ':' + run.BENCH])
        result = run.legacy_harness(original)
        self.assertEqual(result.count('prove_legacy_canary();'), 4)
        self.assertIn('LEGACY_CANARY.store', result)
        self.assertIn('fn bench_indexed_storage_insert(', result)
        self.assertNotIn('EntryPoint::None', result)
        with self.assertRaises(gate.GateError): run.legacy_harness(original + b'\n')

    def test_legacy_reproduction_is_only_needed_for_original_measurement_definition(self):
        original = subprocess.check_output(['git', 'show', run.LEGACY_SHA + ':' + run.BENCH])
        self.assertTrue(run.needs_legacy(original))
        self.assertFalse(run.needs_legacy(Path(run.BENCH).read_bytes()))
        with self.assertRaises(gate.GateError): run.needs_legacy(b'unknown benchmark definition')

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
        original = subprocess.check_output(['git', 'show', run.LEGACY_SHA + ':Cargo.lock'], text=True)
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

    def test_failed_execution_or_bad_summary_retains_raw_prefix_and_binary(self):
        for failure in ('execution', 'validation'):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp); work = root / 'source'; output = root / 'case'
                (work / run.BENCH).parent.mkdir(parents=True)
                runner = object.__new__(run.PairedRun)
                runner.output = root
                runner.assets = {run.BENCH: b'fixed harness'}
                def command(*args, **kwargs):
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
