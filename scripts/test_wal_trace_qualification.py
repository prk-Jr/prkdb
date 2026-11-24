"""Failure boundaries for qualification before any database benchmark."""
import importlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch


class QualificationTests(unittest.TestCase):
    def setUp(self):
        try:
            self.q = importlib.import_module('wal_trace_qualification')
        except ModuleNotFoundError:
            self.fail('live Linux qualifier has not been implemented')

    def test_live_identity_failure_does_not_launch_smoke_or_benchmark(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / 'qualification'
            d = self.q.d
            with patch.object(d, 'preflight', return_value={}), \
                 patch.object(d, 'proc_identity', side_effect=d.DiagnosticError('live parser rejected')), \
                 patch.object(d, 'execute') as execute, \
                 patch.object(d, 'run', side_effect=AssertionError('must not benchmark')):
                with self.assertRaises(d.DiagnosticError):
                    self.q.qualify(output)
            execute.assert_not_called()
            result = json.loads((output / 'result.json').read_text())
            self.assertFalse(result['qualified'])
            self.assertEqual(result['benchmark_invocations'], 0)
            self.assertIn('live parser rejected', result['error'])
            self.assertTrue((output / 'SHA256SUMS').is_file())

    def test_fresh_output_is_required(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp)
            (output / 'existing').write_text('keep')
            with self.assertRaises(self.q.d.DiagnosticError):
                self.q.qualify(output)
            self.assertEqual((output / 'existing').read_text(), 'keep')

    def test_smoke_reuses_exact_recorder_chain_after_live_proof(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / 'qualification'
            d, order = self.q.d, []
            ordinary = d.resource_proof()
            def live(*args):
                order.append('live')
                return ordinary
            def profile(*args):
                self.assertEqual(order, ['live'])
                order.append('smoke')
                self.assertTrue(str(args[0]).endswith('wal-diag-smoke'))
                self.assertIn('smoke-target', args[-1])
                return ['reviewed-three-recorders']
            with patch.object(d, 'preflight', return_value={}), \
                 patch.object(d, 'proc_identity', side_effect=live), \
                 patch.object(d, 'profile_command', side_effect=profile), \
                 patch.object(d, 'root_command', return_value=['sudo', 'qualified']), \
                 patch.object(d, 'execute', return_value={'exit_code': 0}), \
                 patch.object(d, 'validate_native_evidence', return_value={}), \
                 patch.object(d, 'render', return_value={'name_coverage_complete': True}), \
                 patch.object(d, 'qualify_smoke') as smoke, \
                 patch.object(d, 'run', side_effect=AssertionError('must not benchmark')):
                result = self.q.qualify(output)
            self.assertEqual(order, ['live', 'smoke'])
            smoke.assert_called_once()
            self.assertTrue(result['qualified'])
            self.assertEqual(result['benchmark_invocations'], 0)
            self.assertFalse(result['timing_eligible'])

    def test_failed_smoke_retains_failure_and_scoped_ownership(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / 'qualification'
            d = self.q.d
            with patch.object(d, 'preflight', return_value={}), \
                 patch.object(d, 'proc_identity', return_value=d.resource_proof()), \
                 patch.object(d, 'execute', side_effect=d.DiagnosticError('lost events')), \
                 patch.object(self.q, 'retain_ownership') as ownership:
                with self.assertRaises(d.DiagnosticError):
                    self.q.qualify(output)
            ownership.assert_called_once_with(output.resolve())
            result = json.loads((output / 'result.json').read_text())
            self.assertFalse(result['qualified'])
            self.assertIn('lost events', result['error'])
            self.assertTrue((output / 'SHA256SUMS').exists())

    def test_ownership_refusal_is_not_qualified_success(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / 'qualification'
            d = self.q.d
            with patch.object(d, 'preflight', side_effect=d.DiagnosticError('qualification failed')), \
                 patch.object(self.q, 'retain_ownership', side_effect=d.DiagnosticError('ownership failed')):
                with self.assertRaises(d.DiagnosticError):
                    self.q.qualify(output)
            result = json.loads((output / 'result.json').read_text())
            self.assertFalse(result['qualified'])
            self.assertEqual(result['error'], 'qualification failed')
            self.assertEqual(result['ownership_error'], 'ownership failed')


if __name__ == '__main__':
    unittest.main()
