import copy
import json
from pathlib import Path
import tempfile
import unittest
import perf_measurement_gate as gate


class MeasurementGateTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)

    def raw(self, name, ir, thread=1):
        path = self.root / name
        path.write_text(f'version: 1\npid: 42\nthread: {thread}\npart: 1\nevents: Ir Dr Dw\nsummary: 0 0 0\ntotals: {ir} 1 1\n')
        return path

    def summary(self, counts=(100, 20), base=None):
        paths = [self.raw(f'out-{i}', n, i + 1) for i, n in enumerate(counts)]
        def metric(n):
            return {'metrics': {'Left': {'Int': n}}}
        parts = []
        for i, (path, n) in enumerate(zip(paths, counts)):
            info = {'path': str(path), 'pid': 42, 'thread': i + 1, 'part': 1}
            parts.append({'details': {'Left': info}, 'metrics_summary': {'Callgrind': {'Ir': metric(n)}}})
        ir = metric(sum(counts))
        if base is not None:
            ir = {'metrics': {'Both': [{'Int': sum(counts)}, {'Int': base}]}}
        return {'module_path': gate.PREFIX + gate.ASYNC_NAMES[0], 'profiles': [
            {'tool': 'Callgrind', 'out_paths': [str(p) for p in paths],
             'summaries': {'parts': parts, 'total': {'summary': {'Callgrind': {'Ir': ir}}, 'regressions': []}}}]}

    def test_raw_uses_totals_not_known_broken_client_request_summary(self):
        self.assertEqual(gate.raw_metric(self.raw('raw', 321))['ir'], 321)

    def test_raw_refuses_missing_or_duplicate_identity_or_totals(self):
        path = self.raw('raw', 321)
        original = path.read_text()
        for text in (original.replace('thread: 1\n', ''), original + 'totals: 1 1 1\n',
                     original.replace('events: Ir Dr Dw', 'events: Dr Dw'), original.replace('321', '-1')):
            path.write_text(text)
            with self.subTest(text=text), self.assertRaises(gate.GateError):
                gate.raw_metric(path)

    def test_profile_counts_every_thread_and_proves_main_thread(self):
        result = gate.profile(self.summary(), self.root)
        self.assertEqual(result['ir'], 120)
        self.assertEqual(result['main_ir'], 100)
        self.assertEqual(len(result['parts']), 2)

    def test_idle_thread_zero_is_retained(self):
        result = gate.profile(self.summary((100, 0)), self.root)
        self.assertEqual(result['ir'], 100)
        self.assertEqual(len(result['parts']), 2)

    def test_profile_refuses_unaccounted_or_missing_raw_thread(self):
        for mutate in ('extra', 'missing', 'duplicate', 'wrong-total', 'wrong-part', 'wrong-pid', 'symlink'):
            summary = self.summary()
            p = summary['profiles'][0]
            if mutate == 'extra': p['out_paths'].append(str(self.raw('extra', 4, 3)))
            elif mutate == 'missing': Path(p['out_paths'][1]).unlink()
            elif mutate == 'duplicate': p['summaries']['parts'].append(copy.deepcopy(p['summaries']['parts'][0]))
            elif mutate == 'wrong-total': p['summaries']['total']['summary']['Callgrind']['Ir']['metrics']['Left']['Int'] = 121
            elif mutate == 'wrong-part': p['summaries']['parts'][1]['metrics_summary']['Callgrind']['Ir']['metrics']['Left']['Int'] = 21
            elif mutate == 'wrong-pid': p['summaries']['parts'][1]['details']['Left']['pid'] = 99
            else:
                original = Path(p['out_paths'][1]); moved = original.with_suffix('.saved')
                original.rename(moved); original.symlink_to(moved)
            with self.subTest(mutate=mutate), self.assertRaises(gate.GateError): gate.profile(summary, self.root)
            for path in self.root.iterdir(): path.unlink()

    def test_profile_resolves_gungraun_moved_outputs_only_in_exact_summary_directory(self):
        summary = self.summary()
        profile = summary['profiles'][0]
        moved = self.root / 'bench_example'; moved.mkdir()
        for part, old in zip(profile['summaries']['parts'], profile['out_paths']):
            source = Path(old)
            name = 'callgrind.bench_example.t' + str(part['details']['Left']['thread']) + '.p1.out'
            source.rename(moved / name)
            original = '/tmp/gungraun.tmp0123456789bench_example/' + name
            part['details']['Left']['path'] = original
        profile['out_paths'] = [p['details']['Left']['path'] for p in profile['summaries']['parts']]
        result = gate.profile(summary, self.root, retained_dir=moved)
        self.assertEqual(result['ir'], 120)
        # Successful Gungraun cleanup removes temp files even when TMPDIR was
        # inside the evidence tree; the retained copy must still be resolved.
        inside = copy.deepcopy(summary)
        for part in inside['profiles'][0]['summaries']['parts']:
            part['details']['Left']['path'] = part['details']['Left']['path'].replace('/tmp/', str(self.root / 'temporary') + '/')
        inside['profiles'][0]['out_paths'] = [p['details']['Left']['path'] for p in inside['profiles'][0]['summaries']['parts']]
        self.assertEqual(gate.profile(inside, self.root, retained_dir=moved)['ir'], 120)
        for malformed in ('/tmp/unrecognized/', '/tmp/gungraun.tmp0123456789another_bench/'):
            bad = copy.deepcopy(summary)
            for part in bad['profiles'][0]['summaries']['parts']:
                part['details']['Left']['path'] = malformed + Path(part['details']['Left']['path']).name
            bad['profiles'][0]['out_paths'] = [p['details']['Left']['path'] for p in bad['profiles'][0]['summaries']['parts']]
            with self.assertRaises(gate.GateError): gate.profile(bad, self.root, retained_dir=moved)

    def test_profile_requires_comparison_when_requested(self):
        with self.assertRaises(gate.GateError): gate.profile(self.summary(), self.root, require_base=True)
        self.assertEqual(gate.profile(self.summary(base=111), self.root, require_base=True)['base_ir'], 111)

    def test_profile_does_not_accept_float_or_negative_instructions(self):
        for value in ({'Float': 120.0}, {'Int': -1}, {'Int': True}):
            summary = self.summary(); summary['profiles'][0]['summaries']['total']['summary']['Callgrind']['Ir']['metrics'] = {'Left': value}
            with self.subTest(value=value), self.assertRaises(gate.GateError): gate.profile(summary, self.root)

    def records(self, main=100, total=120):
        return {gate.PREFIX + n: {'ir': total, 'main_ir': main} for n in gate.ASYNC_NAMES}

    def test_canary_requires_fixed_main_and_total_sensitivity_for_all_four(self):
        zero = self.records(); high = self.records(4196, 4216)
        proof = gate.coverage(zero, high)
        self.assertEqual(len(proof), 4)
        for values in proof.values(): self.assertEqual(values['main_delta'], 4096)
        for bad in (self.records(4195, 4216), self.records(4196, 4215), {}):
            with self.subTest(bad=bad), self.assertRaises(gate.GateError): gate.coverage(zero, bad)

    def test_comparison_rejects_missing_new_or_uncompared_names(self):
        base = {gate.PREFIX + n: {'ir': 100} for n in gate.ALL_NAMES}
        head = {n: {'ir': 105, 'base_ir': 100} for n in base}
        self.assertFalse(gate.comparison(base, head)['regressed'])
        head[gate.PREFIX + gate.ASYNC_NAMES[0]]['ir'] = 106
        self.assertEqual(len(gate.comparison(base, head)['regressed']), 1)
        for mutate in ('missing', 'extra', 'zero', 'base-drift'):
            bad = copy.deepcopy(head)
            if mutate == 'missing': bad.pop(next(iter(bad)))
            elif mutate == 'extra': bad['new-uncounted'] = {'ir': 100, 'base_ir': 100}
            elif mutate == 'zero': bad[next(iter(bad))]['ir'] = 0
            else: bad[next(iter(bad))]['base_ir'] = 101
            with self.subTest(mutate=mutate), self.assertRaises(gate.GateError): gate.comparison(base, bad)

    def test_manifest_patch_is_exact_and_does_not_reformat_other_content(self):
        manifest = '[dev-dependencies]\ngungraun = { version = "=0.19.4" }\nother = "1"\n'
        patched = gate.patch_manifest(manifest)
        self.assertEqual(patched, manifest.replace('version = "=0.19.4"', 'version = "=0.19.4", features = ["client_requests"]'))
        self.assertEqual(gate.patch_manifest(patched), patched)
        for bad in (manifest.replace('0.19.4', '0.20.0'), manifest + manifest):
            with self.assertRaises(gate.GateError): gate.patch_manifest(bad)

    def metadata(self):
        return {'workspace_members': ['db'], 'resolve': {'nodes': [
            {'id': 'db', 'features': [], 'deps': [
                {'name': 'normal', 'pkg': 'normal1', 'dep_kinds': [{'kind': None, 'target': None}]},
                {'name': 'bench', 'pkg': 'bench1', 'dep_kinds': [{'kind': 'dev', 'target': None}]}]},
            {'id': 'normal1', 'features': ['std'], 'deps': []}, {'id': 'bench1', 'features': [], 'deps': []}]},
            'packages': [{'id': 'db', 'name': 'db', 'version': '1', 'source': None},
                         {'id': 'normal1', 'name': 'normal', 'version': '1', 'source': 'registry+x'},
                         {'id': 'bench1', 'name': 'bench', 'version': '1', 'source': 'registry+x'}]}

    def test_normal_graph_ignores_dev_only_additions_but_not_production_changes(self):
        metadata = self.metadata(); baseline = gate.production_graph(metadata)
        changed = copy.deepcopy(metadata); changed['resolve']['nodes'][2]['features'] = ['client_requests']
        self.assertEqual(gate.production_graph(changed), baseline)
        for mutate in ('version', 'features', 'edge'):
            changed = copy.deepcopy(metadata)
            if mutate == 'version': changed['packages'][1]['version'] = '2'
            elif mutate == 'features': changed['resolve']['nodes'][1]['features'].append('new')
            else: changed['resolve']['nodes'][0]['deps'][0]['dep_kinds'][0]['target'] = 'cfg(unix)'
            self.assertNotEqual(gate.production_graph(changed), baseline)

    def test_context_features_ignore_dev_union_but_still_refuse_normal_feature_changes(self):
        before = self.metadata(); after = copy.deepcopy(before)
        after['resolve']['nodes'][1]['features'].append('dev-build-only-union')
        gate.verify_production(before, after, 'normal1 [std]\n', 'normal1 [std]\n')
        with self.assertRaises(gate.GateError):
            gate.verify_production(before, after, 'normal1 [std]\n', 'normal1 [std,new]\n')
        after['packages'][1]['version'] = '2'
        with self.assertRaises(gate.GateError):
            gate.verify_production(before, after, 'normal1 [std]\n', 'normal1 [std]\n')

    def test_lock_overlay_refuses_production_updates_and_unrelated_additions(self):
        before = 'version = 4\n[[package]]\nname = "normal"\nversion = "1"\n'
        after = before + '[[package]]\nname = "valgrind-requests"\nversion = "1.1.0"\n'
        gate.lock_overlay(before, after)
        for bad in (after.replace('version = "1"', 'version = "2"'), after.replace('valgrind-requests', 'unrelated')):
            with self.assertRaises(gate.GateError): gate.lock_overlay(before, bad)


if __name__ == '__main__': unittest.main()
