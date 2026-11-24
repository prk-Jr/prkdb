#!/usr/bin/env python3
"""Restore the one archived Durable provenance graph at its original paths."""
import argparse
from contextlib import contextmanager
import json
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import time

import stream_durable_diagnostic as q

PINNED_MANIFEST_SHA256 = 'b395160e7bb2d332045ae6f73e2bb099eefc8b0c6285ad8aabba2c4bbfe1c60b'
BUILD_RUN_ID = 38686445143
RESTORE_SECONDS = 480


def git(repo, deadline, *arguments, check=True):
    result = subprocess.run(['git', '-C', str(repo), *arguments], capture_output=True,
                            text=True, timeout=deadline.timeout(), check=False)
    q.require(not check or result.returncode == 0,
              f'restore git {arguments[0]} failed: {result.stderr.strip()}')
    deadline.remaining()
    return result


@contextmanager
def overall_deadline(deadline):
    # The unchanged verifier also performs blocking Git calls. An outer timer
    # keeps those inside this same nonresetting budget without changing it.
    def expired(_signal, _frame):
        raise q.DiagnosticError('nonresetting restore deadline exhausted')
    previous_handler = signal.getsignal(signal.SIGALRM)
    previous_timer = signal.getitimer(signal.ITIMER_REAL)
    started = time.monotonic()
    signal.signal(signal.SIGALRM, expired)
    signal.setitimer(signal.ITIMER_REAL, deadline.remaining())
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous_handler)
        if previous_timer[0]:
            signal.setitimer(signal.ITIMER_REAL, max(0.000001, previous_timer[0] - (time.monotonic() - started)), previous_timer[1])


def preflight(evidence, deadline):
    manifest_path = evidence / 'manifest.json'
    q.require(manifest_path.is_file() and not manifest_path.is_symlink(), 'missing or linked archived manifest')
    q.require(q.checked_digest(manifest_path, deadline) == PINNED_MANIFEST_SHA256, 'pinned manifest bytes differ')
    manifest = json.loads(manifest_path.read_text())
    q.require(str(evidence) == manifest['evidence_root'], 'exact archived evidence root required')
    q.require(set(manifest['sources']) == set(q.SOURCES) == {'A', 'B'}, 'source identity inventory differs')
    q.require(manifest['overlay_root'] == str(evidence / 'overlay'), 'archived overlay root differs')
    overlay = evidence / 'overlay'
    paths = list(overlay.rglob('*'))
    q.require(not any(p.is_symlink() for p in paths) and not overlay.is_symlink(), 'linked overlay prohibited')
    q.require({str(p.relative_to(overlay)) for p in paths if p.is_file()} == set(q.OVERLAY_FILES), 'overlay inventory differs')
    q.require(set(manifest['overlay_files']) == set(q.OVERLAY_FILES), 'manifest overlay inventory differs')
    for side, sha in q.SOURCES.items():
        source = manifest['sources'][side]
        root = evidence / 'sources' / side.lower()
        q.require(source['source_sha'] == sha and source['side'] == side and source['clean'] is True,
                  f'archived source identity differs: {side}')
        q.require(source['root'] == str(root) and manifest['source_paths'][side] == str(evidence / f'source-{side.lower()}.json'),
                  f'archived source paths differ: {side}')
        q.require(not root.exists() and not root.is_symlink(), f'occupied source path: {root}')
        q.require(root.parent.resolve() == root.parent, 'linked source parent prohibited')
    q.require(set(manifest['binaries']) == {'original-a', 'original-b', 'marked-a', 'marked-b'}, 'four archived executables required')
    q.require(len(manifest['members']) == 34, 'complete 34-member archive required')
    for path, expected in manifest['members'].items():
        member = Path(path)
        q.require(member.is_relative_to(evidence) and member.resolve() == member and member.is_file(),
                  f'missing or linked archived member: {path}')
        q.require(q.checked_digest(member, deadline) == expected, f'archived member changed: {path}')
    for key, binary in manifest['binaries'].items():
        q.require(manifest['members'].get(binary['path']) == binary['sha256'], f'archived executable binding differs: {key}')
        q.require(q.checked_digest(binary['path'], deadline) == binary['sha256'], f'archived executable changed: {key}')
        for path, expected in binary['runtime_libraries'].items():
            library = Path(path)
            q.require(library.is_file() and not library.is_symlink() and q.checked_digest(library, deadline) == expected,
                      f'current runtime library changed or missing: {path}; stop, no rebuild')
    for name, expected in manifest['overlay_files'].items():
        q.require(q.checked_digest(overlay / name, deadline) == expected, f'archived overlay changed: {name}')
    q.require(not (evidence / 'reuse-proof.json').exists() and not (evidence / 'reuse-proof.json').is_symlink(), 'reuse proof already exists; no overwrite')
    return manifest


def restore(evidence, repo):
    evidence = Path(evidence).resolve()
    repo = Path(repo).resolve()
    deadline = q.Deadline(time.monotonic_ns() + RESTORE_SECONDS * 10**9)
    with overall_deadline(deadline):
        manifest = preflight(evidence, deadline)
        tooling_sha = git(repo, deadline, 'rev-parse', 'HEAD').stdout.strip()
        for side, sha in q.SOURCES.items():
            if git(repo, deadline, 'cat-file', '-e', sha + '^{commit}', check=False).returncode:
                git(repo, deadline, 'fetch', 'origin', sha)
                git(repo, deadline, 'cat-file', '-e', sha + '^{commit}')
        for side, sha in q.SOURCES.items():
            root = Path(manifest['sources'][side]['root'])
            git(repo, deadline, 'worktree', 'add', '--detach', str(root), sha)
            for name in q.OVERLAY_FILES:
                destination = root / name
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(evidence / 'overlay' / name, destination)
        q.verify_manifest(manifest, deadline=deadline)
        deadline.remaining()
        proof = dict(build_run_id=BUILD_RUN_ID, manifest_sha256=PINNED_MANIFEST_SHA256,
                     graph_sha256=q.seal_manifest(manifest)['graph_sha256'], current_tooling_sha=tooling_sha,
                     source_paths={side: source['root'] for side, source in manifest['sources'].items()},
                     evidence_root=str(evidence), restore_seconds=RESTORE_SECONDS, rebuilt=False,
                     helper_sha256=q.checked_digest(__file__, deadline))
        with (evidence / 'reuse-proof.json').open('x') as output:
            json.dump(proof, output, indent=2)
            output.write('\n')
        return proof


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--evidence', required=True)
    parser.add_argument('--repo', required=True)
    args = parser.parse_args()
    try:
        print(json.dumps(restore(args.evidence, args.repo), indent=2))
    except (q.DiagnosticError, OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError) as error:
        print(f'inconclusive: exact archived reuse stopped: {error}', file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
