#!/usr/bin/env python3
"""Exact-path archived executable reuse; never compiles or launches the database."""
import argparse
from contextlib import contextmanager
import importlib.util
import json
from pathlib import Path
import shutil
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import stream_durable_diagnostic as q
import test_stream_durable_diagnostic as fixtures


class RestoreTests(unittest.TestCase):
    def helper(self):
        path = Path(__file__).with_name('stream_durable_restore.py')
        self.assertTrue(path.is_file(), 'exact-path restore helper is missing')
        spec = importlib.util.spec_from_file_location('restore_under_test', path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    @contextmanager
    def fixture(self):
        r = self.helper()
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            sources, args, library = fixtures.DurableDiagnosticTests().provenance_fixture(root)
            evidence = Path(args.evidence)
            (evidence / 'sources').mkdir()
            replacements = {}
            for side in sources:
                old = root / side
                new = evidence / 'sources' / side.lower()
                shutil.move(str(old), new)
                replacements[str(old)] = str(new)
            shutil.move(str(root / 'overlay'), evidence / 'overlay')
            args.overlay = str(evidence / 'overlay')
            for path in evidence.glob('*'):
                if path.suffix in ('.json', '.jsonl'):
                    text = path.read_text()
                    for old, new in replacements.items():
                        text = text.replace(old, new)
                    path.write_text(text)
            repo = root / 'repository'
            subprocess.run(['git', 'init', '--initial-branch=fixture', str(repo)], check=True, capture_output=True)
            for side, sha in sources.items():
                subprocess.run(['git', '-C', str(repo), 'fetch', str(evidence / 'sources' / side.lower()), sha], check=True, capture_output=True)
            subprocess.run(['git', '-C', str(repo), 'checkout', '--detach', sources['A']], check=True, capture_output=True)
            real_run = subprocess.run
            linked = SimpleNamespace(returncode=0, stdout=f'libfixture => {library} (0x0)\n', stderr='')
            def build_run(argv, *pos, **kw):
                return linked if argv[0] == 'ldd' else real_run(argv, *pos, **kw)
            with patch.dict(q.SOURCES, sources, clear=True), patch.object(q.subprocess, 'run', side_effect=build_run):
                q.build_manifest(args)
            manifest = json.loads(Path(args.output).read_text())
            for side in sources:
                shutil.rmtree(evidence / 'sources' / side.lower())
            with patch.dict(q.SOURCES, sources, clear=True), patch.object(r, 'PINNED_MANIFEST_SHA256', q.digest(args.output)):
                yield r, evidence, repo, manifest, library

    def assert_pristine_sources(self, evidence):
        self.assertFalse((evidence / 'sources' / 'a').exists())
        self.assertFalse((evidence / 'sources' / 'b').exists())
        self.assertFalse((evidence / 'reuse-proof.json').exists())

    def test_restores_exact_git_overlay_and_proves_unchanged_original_graph(self):
        with self.fixture() as (r, evidence, repo, manifest, library):
            before = {p: Path(p).read_bytes() for p in manifest['members']}
            proof = r.restore(evidence, repo)
            self.assertEqual(proof['build_run_id'], 38686445143)
            self.assertEqual(proof['manifest_sha256'], q.digest(evidence / 'manifest.json'))
            self.assertEqual(proof['graph_sha256'], q.seal_manifest(manifest)['graph_sha256'])
            self.assertEqual(proof['current_tooling_sha'], q.git(repo, 'rev-parse', 'HEAD'))
            self.assertEqual(json.loads((evidence / 'reuse-proof.json').read_text()), proof)
            q.verify_manifest(manifest)
            for path, content in before.items():
                self.assertEqual(Path(path).read_bytes(), content)
            self.assertFalse(any((evidence / 'sources' / side / 'target').exists() for side in ('a', 'b')))

    def test_wrong_manifest_pin_stops_before_source_mutation(self):
        with self.fixture() as (r, evidence, repo, manifest, library):
            with patch.object(r, 'PINNED_MANIFEST_SHA256', '0' * 64):
                with self.assertRaisesRegex(q.DiagnosticError, 'pinned manifest'):
                    r.restore(evidence, repo)
            self.assert_pristine_sources(evidence)

    def test_wrong_source_identity_stops_before_source_mutation(self):
        with self.fixture() as (r, evidence, repo, manifest, library):
            with patch.dict(q.SOURCES, {'A': '0' * 40}):
                with self.assertRaisesRegex(q.DiagnosticError, 'source identity'):
                    r.restore(evidence, repo)
            self.assert_pristine_sources(evidence)

    def test_extra_overlay_stops_before_source_mutation(self):
        with self.fixture() as (r, evidence, repo, manifest, library):
            (evidence / 'overlay' / 'extra').write_text('unexpected')
            with self.assertRaisesRegex(q.DiagnosticError, 'overlay inventory'):
                r.restore(evidence, repo)
            self.assert_pristine_sources(evidence)

    def test_tampered_member_stops_before_source_mutation(self):
        with self.fixture() as (r, evidence, repo, manifest, library):
            (evidence / 'build-original-a.stderr').write_text('tampered')
            with self.assertRaisesRegex(q.DiagnosticError, 'member changed'):
                r.restore(evidence, repo)
            self.assert_pristine_sources(evidence)

    def test_library_change_stops_without_reuse_proof(self):
        with self.fixture() as (r, evidence, repo, manifest, library):
            library.write_bytes(b'changed ABI')
            with self.assertRaisesRegex(q.DiagnosticError, 'library changed'):
                r.restore(evidence, repo)
            self.assert_pristine_sources(evidence)

    def test_occupied_source_is_preserved_before_other_source_creation(self):
        with self.fixture() as (r, evidence, repo, manifest, library):
            occupied = evidence / 'sources' / 'b'
            occupied.mkdir()
            sentinel = occupied / 'owned-by-someone-else'
            sentinel.write_bytes(b'preserve')
            with self.assertRaisesRegex(q.DiagnosticError, 'occupied source'):
                r.restore(evidence, repo)
            self.assertEqual(sentinel.read_bytes(), b'preserve')
            self.assertFalse((evidence / 'sources' / 'a').exists())

    def test_wrong_evidence_root_stops_before_source_mutation(self):
        with self.fixture() as (r, evidence, repo, manifest, library):
            moved = evidence.with_name('relocated')
            evidence.rename(moved)
            with self.assertRaisesRegex(q.DiagnosticError, 'exact archived evidence root'):
                r.restore(moved, repo)
            self.assert_pristine_sources(moved)

    def test_expired_deadline_never_launches_subprocess(self):
        r = self.helper()
        with patch.object(r.subprocess, 'run') as launch:
            with self.assertRaises(q.DiagnosticError):
                r.git(Path('/unused'), q.Deadline(1), 'rev-parse', 'HEAD')
            launch.assert_not_called()


if __name__ == '__main__':
    unittest.main()
