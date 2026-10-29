#!/usr/bin/env python3
"""Execute one bounded, same-harness base/head comparison with coverage evidence."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import time
import tomllib

import perf_measurement_gate as gate

LEGACY_HASH = '98f6fb5d44cfc8ef358472794e4031b5a01f8c34e3f48ed925443283dd53612b'
BENCH = 'crates/prkdb/benches/iai_hot_paths.rs'
SUPPORT = 'crates/prkdb/benches/support/perf_gate_measurement.rs'
CONTRACT = 'crates/prkdb/tests/perf_measurement_contract.rs'
MANIFEST = 'crates/prkdb/Cargo.toml'
DEV_OVERLAY = 'scripts/testdata/perf_measurement_dev_overlay.toml'
OVERLAY = (BENCH, SUPPORT, CONTRACT, MANIFEST, 'Cargo.lock')
LEGACY_NAMES = ('bench_wal_put', 'bench_wal_get_hit', 'bench_wal_put_batch_100', 'bench_indexed_storage_insert')
LEGACY_MAP = dict(zip(LEGACY_NAMES, gate.ASYNC_NAMES))


def legacy_harness(original):
    gate.require(hashlib.sha256(original).hexdigest() == LEGACY_HASH, 'unexpected original measurement harness')
    text = original.decode()
    needle = 'fn single_worker_runtime() -> Runtime {\n'
    gate.require(text.count(needle) == 1, 'legacy setup shape changed')
    text = text.replace(needle, needle + '    LEGACY_CANARY.store(legacy_measurement::Controls::from_env().expect("valid legacy canary").canary_iterations, std::sync::atomic::Ordering::Relaxed);\n', 1)
    for name in LEGACY_NAMES:
        start = text.index('fn ' + name + '(')
        boundary = text.find('\nfn ', start + 4)
        position = text.index('rt.block_on(async {', start)
        gate.require(boundary < 0 or position < boundary, 'legacy operation body not found')
        position += len('rt.block_on(async {')
        text = text[:position] + '\n        prove_legacy_canary();' + text[position:]
    return text + '''
// Canary-only reproduction of the frozen pre-repair measurement definition.
#[path = "support/perf_gate_measurement.rs"]
mod legacy_measurement;
static LEGACY_CANARY: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
fn prove_legacy_canary() {
    let n = LEGACY_CANARY.load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(legacy_measurement::canary(n), n, "legacy canary must actually execute");
}
'''


def needs_legacy(base_harness):
    if hashlib.sha256(base_harness).hexdigest() == LEGACY_HASH:
        return True
    gate.require(all(('fn ' + name + '(').encode() in base_harness for name in gate.ASYNC_NAMES),
                 'unknown baseline measurement definition requires explicit protocol review')
    return False


def lock_blocks(text):
    chunks = re.split(r'(?m)^\[\[package\]\]\n', text)
    blocks = {}
    for chunk in chunks[1:]:
        raw = '[[package]]\n' + chunk
        package = tomllib.loads(raw)['package'][0]
        key = gate.package_id(package)
        gate.require(key not in blocks, 'duplicate locked package')
        blocks[key] = raw
    return chunks[0], blocks


def overlay_lock_text(before, reviewed_overlay):
    prefix, blocks = lock_blocks(before)
    _, reviewed = lock_blocks(reviewed_overlay)
    prior = gate.lock_packages(before)
    overlay_packages = gate.lock_packages(reviewed_overlay)
    gungraun = [key for key in reviewed if key[0] == 'gungraun']
    gate.require(len(gungraun) == 1 and gungraun[0][1] == '0.19.4' and gungraun[0] in prior,
                 'unexpected reviewed gungraun version')
    gate.require(all(k[0] in gate.DEV_PACKAGES or k == gungraun[0] for k in reviewed),
                 'non-dev package in reviewed overlay')
    for key, block in reviewed.items():
        if key == gungraun[0]:
            continue
        if key in blocks:
            old = {k: v for k, v in prior[key].items() if k != 'dependencies'}
            new = {k: v for k, v in overlay_packages[key].items() if k != 'dependencies'}
            gate.require(old == new, 'immutable reviewed package identity/checksum changed')
        else:
            blocks[key] = block
    # Keep the actual source's existing gungraun dependencies; append only client edges.
    block = blocks[gungraun[0]]
    dependencies = list(prior[gungraun[0]].get('dependencies', []))
    for edge in ('cty', 'valgrind-requests'):
        if not any(d.split()[0] == edge for d in dependencies):
            dependencies.append(edge)
    replacement = 'dependencies = [\n' + ''.join(' "' + edge + '",\n' for edge in sorted(dependencies)) + ']'
    gate.require(block.count('dependencies = [') == 1, 'unexpected dependency block')
    blocks[gungraun[0]] = re.sub(r'dependencies = \[.*?\]', lambda _: replacement, block, count=1, flags=re.S)
    all_keys = set(blocks)
    for key in prior:
        def disambiguate(match):
            edge = match.group(2)
            parts = edge.split()
            if len(parts) != 1 or sum(k[0] == parts[0] for k in all_keys) < 2:
                return match.group(0)
            matches = [k for k in prior if k[0] == parts[0]]
            if not matches:  # a new client edge from the reviewed overlay
                matches = [k for k in reviewed if k[0] == parts[0]]
            gate.require(len(matches) == 1, 'ambiguous original lock dependency')
            return match.group(1) + '"' + edge + ' ' + matches[0][1] + '",' + match.group(3)
        blocks[key] = re.sub(r'(?m)^(\s*)"([^"\n]+)",(\s*)$', disambiguate, blocks[key])
    result = prefix + ''.join(blocks.values())
    gate.lock_overlay(before, result)
    return result


def verify_dirty_paths(root):
    root = Path(root)
    tracked = subprocess.check_output(['git', '-C', str(root), 'diff', '--name-only', 'HEAD'], text=True).splitlines()
    untracked = subprocess.check_output(['git', '-C', str(root), 'ls-files', '--others', '--exclude-standard'], text=True).splitlines()
    gate.require(set(tracked + untracked) <= set(OVERLAY), f'unexpected overlay source changes: {set(tracked + untracked) - set(OVERLAY)}')


def save(path, data):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(json.dumps(data, indent=2, sort_keys=True) + '\n')


class PairedRun:
    def __init__(self, repo, base, head, output):
        self.repo, self.base, self.head = Path(repo).resolve(), base, head
        self.output = Path(output).resolve()
        gate.require(not self.output.exists(), 'output exists; no overwrite or automatic retry')
        gate.require(not self.output.is_relative_to(self.repo), 'evidence must be outside source checkout')
        for sha in (base, head):
            gate.require(re.fullmatch('[0-9a-f]{40}', sha) is not None, 'full source SHA required')
        self.output.mkdir(parents=True)
        self.deadline = time.monotonic() + 2040  # 34m; the workflow reserves upload/finalization.
        self.index = 0
        gate.require(subprocess.check_output(['git', '-C', str(self.repo), 'rev-parse', 'HEAD'], text=True).strip() == head, 'tooling checkout must be exact head')
        gate.require(not subprocess.check_output(['git', '-C', str(self.repo), 'status', '--porcelain'], text=True).strip(), 'tooling source must be clean')
        self.result = {'complete': False, 'verdict': 'inconclusive', 'base': base, 'head': head,
                       'gate_percent': 5, 'canary_iterations': [0, 4096], 'overlaid_comparison': True,
                       'clean_source_claim': False, 'sources': {}, 'production_code_changed_by_overlay': False}
        self.assets = {}
        for path in (BENCH, SUPPORT, CONTRACT, DEV_OVERLAY):
            self.assets[path] = subprocess.check_output(['git', '-C', str(self.repo), 'show', head + ':' + path])
        base_harness = subprocess.check_output(['git', '-C', str(self.repo), 'show', base + ':' + BENCH])
        self.legacy_required = needs_legacy(base_harness)
        self.legacy = legacy_harness(base_harness) if self.legacy_required else None
        self.result['legacy_reproduction_required'] = self.legacy_required
        save(self.output / 'overlay.json', {p: hashlib.sha256(v).hexdigest() for p, v in self.assets.items()})

    def command(self, args, cwd=None, env=None, allowed=(0,), label='command'):
        remaining = self.deadline - time.monotonic()
        gate.require(remaining > 0, 'aggregate benchmark budget exhausted')
        self.index += 1
        folder = self.output / 'commands' / f'{self.index:03d}-{label}'
        folder.mkdir(parents=True)
        save(folder / 'command.json', {'argv': [str(a) for a in args], 'cwd': str(cwd or self.repo)})
        with (folder / 'stdout').open('wb') as stdout, (folder / 'stderr').open('wb') as stderr:
            process = subprocess.Popen([str(a) for a in args], cwd=cwd or self.repo, env=env,
                                       stdout=stdout, stderr=stderr, start_new_session=True)
            try:
                code = process.wait(timeout=min(1200, remaining))
            except subprocess.TimeoutExpired as error:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=10)
                save(folder / 'exit.json', {'returncode': process.returncode, 'timeout': True})
                raise gate.GateError('command or aggregate benchmark deadline exhausted') from error
        save(folder / 'exit.json', {'returncode': code, 'timeout': False})
        gate.require(code in allowed, f'{label} failed with {code}; see {folder}')
        return code, (folder / 'stdout').read_text(), folder

    def environment(self, work):
        pin = tomllib.loads((work / 'rust-toolchain.toml').read_text())['toolchain']['channel']
        gate.require(re.fullmatch(r'1\.\d+\.\d+', pin) is not None, 'exact source compiler pin required')
        env = {k: v for k, v in os.environ.items() if not k.startswith(('PRKDB_PERF_', 'GUNGRAUN_'))}
        env.update(CARGO_INCREMENTAL='0', CARGO_TARGET_DIR=str(work / 'target'), CARGO_BUILD_JOBS='2',
                   CARGO_PROFILE_DEV_DEBUG='0', CARGO_PROFILE_TEST_DEBUG='0', RUSTUP_TOOLCHAIN=pin,
                   GUNGRAUN_COLOR='never', CARGO_TERM_COLOR='never', PYTHONDONTWRITEBYTECODE='1')
        _, identity, _ = self.command(['rustc', '-Vv'], work, env, label='compiler')
        gate.require('release: ' + pin + '\n' in identity, 'selected compiler differs from source pin')
        return env, identity

    def metadata(self, work, env):
        _, data, _ = self.command(['cargo', 'metadata', '--locked', '--format-version', '1'], work, env, label='metadata')
        return json.loads(data)

    def prepare(self, label, sha):
        work = self.output / 'sources' / label
        self.command(['git', 'worktree', 'add', '--detach', work, sha], label='worktree')
        env, identity = self.environment(work)
        metadata_before = self.metadata(work, env)
        _, tree_before, _ = self.command(['cargo', 'tree', '--locked', '--workspace', '--edges', 'normal,build', '--prefix', 'none', '--format', '{p} [{f}]'], work, env, label='normal-feature-tree')
        original_manifest = (work / MANIFEST).read_text()
        original_lock = (work / 'Cargo.lock').read_text()
        tracked = subprocess.check_output(['git', '-C', str(work), 'ls-files', '-z']).decode().split('\0')
        hashes = {p: gate.digest(work / p) for p in tracked if p and p not in OVERLAY and (work / p).is_file()}
        for path in (BENCH, SUPPORT, CONTRACT):
            target = work / path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(self.assets[path])
        (work / MANIFEST).write_text(gate.patch_manifest(original_manifest))
        (work / 'Cargo.lock').write_text(overlay_lock_text(original_lock, self.assets[DEV_OVERLAY].decode()))
        metadata_after = self.metadata(work, env)
        _, tree_after, _ = self.command(['cargo', 'tree', '--locked', '--workspace', '--edges', 'normal,build', '--prefix', 'none', '--format', '{p} [{f}]'], work, env, label='normal-feature-tree')
        graph_before = gate.verify_production(metadata_before, metadata_after, tree_before, tree_after)
        graph_after = gate.production_graph(metadata_after, include_global_features=False)
        gate.require(all(gate.digest(work / p) == value for p, value in hashes.items()), 'overlay changes production/source files')
        gate.require((work / MANIFEST).read_text() == gate.patch_manifest(original_manifest), 'manifest drift')
        verify_dirty_paths(work)
        proof = {'sha': sha, 'compiler': identity, 'production_graph_before': graph_before,
                 'production_graph_after': graph_after, 'normal_feature_tree_before': tree_before,
                 'normal_feature_tree_after': tree_after, 'metadata_before': metadata_before,
                 'metadata_after': metadata_after, 'unchanged_source_hashes': hashes,
                 'overlay_hashes': {p: gate.digest(work / p) for p in OVERLAY}}
        save(self.output / label / 'provenance.json', proof)
        self.result['sources'][label] = {'sha': sha, 'compiler': identity, 'graph_unchanged': True}
        return work, env

    def benchmark(self, work, env, folder, *, legacy=False, count=0, normal=False, baseline=None):
        data = work / 'target' / 'gungraun'
        gate.require(not data.exists(), 'unexpected leftover benchmark state')
        if baseline is not None:
            shutil.copytree(baseline, data)
        (work / BENCH).write_text(self.legacy if legacy else self.assets[BENCH].decode())
        folder.mkdir(parents=True)
        args = ['cargo', 'bench', '-p', 'prkdb', '--bench', 'iai_hot_paths', '--', '--save-summary=json',
                '--callgrind-args=--separate-threads=yes']
        if baseline is not None:
            args += ['--baseline=base', '--callgrind-limits=ir=5%']
        else:
            args += ['--save-baseline=' + ('base' if normal else 'coverage')]
        case_env = dict(env, PRKDB_PERF_CANARY=str(count), PRKDB_PERF_SKIP_OPERATION='0')
        code, original_error, binaries = None, None, []
        try:
            code, _, _ = self.command(args, work, case_env, allowed=(0, 3) if baseline else (0,), label='benchmark')
        except BaseException as error:
            original_error = error
            raise
        finally:
            # Preserve even malformed/interrupted output BEFORE attempting validation.
            try:
                if data.exists():
                    gate.require(not any(p.is_symlink() for p in data.rglob('*')), 'symlink in raw prefix')
                    shutil.copytree(data, folder / 'gungraun')
                binaries = [p for p in (work / 'target' / 'release' / 'deps').glob('iai_hot_paths-*')
                            if p.is_file() and not p.is_symlink() and os.access(p, os.X_OK)]
                for binary in binaries:
                    target = folder / 'benchmark' if len(binaries) == 1 else folder / 'binaries' / binary.name
                    target.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(binary, target)
                save(folder / 'binary.json', {'candidates': [{'path': str(p), 'sha256': gate.digest(p)} for p in binaries],
                                             'harness_sha256': gate.digest(work / BENCH), 'canary': count,
                                             'legacy_measurement': legacy, 'exit_code': code,
                                             'execution_returned': code is not None,
                                             'raw_profile_validated': False})
            except (OSError, gate.GateError) as error:
                save(folder / 'retention-error.json', {'error': str(error)})
                if original_error is None:
                    raise
        gate.require(len(binaries) == 1, 'ambiguous or missing benchmark executable')
        names = LEGACY_NAMES + ('bench_log_record_encode', 'bench_log_record_decode') if legacy else gate.ALL_NAMES
        values = gate.collect(data, self.output, {gate.PREFIX + n for n in names}, require_base=baseline is not None)
        for value in values.values():
            for part in value['parts']:
                relative = Path(part['path']).relative_to(data)
                part['artifact_path'] = str((folder / 'gungraun' / relative).relative_to(self.output))
        save(folder / 'counts.json', values)
        binary_proof = json.loads((folder / 'binary.json').read_text())
        binary_proof['raw_profile_validated'] = True
        save(folder / 'binary.json', binary_proof)
        shutil.rmtree(data)
        return values, code

    def one_source(self, label, sha, baseline=None):
        work, env = self.prepare(label, sha)
        self.command(['cargo', 'test', '-p', 'prkdb', '--test', 'perf_measurement_contract', '--locked'], work, env, label='native-contract')
        cases = {}
        for legacy in ((True, False) if self.legacy_required else (False,)):
            for count in (0, 4096):
                name = ('legacy' if legacy else 'fixed') + '-' + str(count)
                values, _ = self.benchmark(work, env, self.output / label / name, legacy=legacy, count=count)
                if legacy:
                    values = {gate.PREFIX + LEGACY_MAP[n]: values[gate.PREFIX + n] for n in LEGACY_NAMES}
                else:
                    values = {gate.PREFIX + n: values[gate.PREFIX + n] for n in gate.ASYNC_NAMES}
                cases[name] = values
        fixed = gate.coverage(cases['fixed-0'], cases['fixed-4096'])
        legacy = {name: {'main_delta': cases['legacy-4096'][name]['main_ir'] - cases['legacy-0'][name]['main_ir'],
                         'total_delta': cases['legacy-4096'][name]['ir'] - cases['legacy-0'][name]['ir']}
                  for name in cases['legacy-0']} if self.legacy_required else {}
        save(self.output / label / 'coverage.json', {'fixed': fixed, 'legacy': legacy})
        self.result['sources'][label]['coverage'] = {'fixed': fixed, 'legacy': legacy}
        normal, code = self.benchmark(work, env, self.output / label / 'normal', normal=True, baseline=baseline)
        (work / BENCH).write_bytes(self.assets[BENCH])
        verify_dirty_paths(work)
        self.command(['cargo', 'clean', '--target-dir', work / 'target'], work, env, label='clean')
        return normal, code

    def run(self):
        try:
            base, _ = self.one_source('base', self.base)
            head, code = self.one_source('head', self.head, self.output / 'base' / 'normal' / 'gungraun')
            compared = gate.comparison(base, head)
            gate.require(code == (3 if compared['regressed'] else 0), 'runner and strict 5% verdict differ')
            legacy_missing = [name for source in self.result['sources'].values()
                              for name, counts in source['coverage']['legacy'].items() if counts['main_delta'] < 4096]
            if self.legacy_required:
                gate.require(legacy_missing, 'legacy canary did not demonstrate excluded work; exact original failure remains unproven')
            save(self.output / 'deltas.json', compared['rows'])
            self.result.update(complete=True, verdict='regressed' if compared['regressed'] else 'pass',
                               comparison=compared, legacy_excluded_work=sorted(set(legacy_missing)), candidate_exit_code=code)
            if os.environ.get('GITHUB_OUTPUT'):
                with Path(os.environ['GITHUB_OUTPUT']).open('a') as stream:
                    stream.write(f'exit_code={code}\n')
            return 0  # The separate unchanged regression-enforcement step handles exit3.
        except (gate.GateError, OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError) as error:
            self.result['error'] = str(error)
            print('inconclusive:', error, file=sys.stderr)
            return 2
        finally:
            save(self.output / 'result.json', self.result)
            inventory = {str(p.relative_to(self.output)): {'sha256': gate.digest(p), 'bytes': p.stat().st_size}
                         for p in self.output.rglob('*') if p.is_file() and '.git' not in p.parts and 'sources' not in p.relative_to(self.output).parts}
            save(self.output / 'inventory.json', inventory)
            print(json.dumps({k: self.result.get(k) for k in ('complete', 'verdict', 'error')}, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', type=Path, required=True)
    parser.add_argument('--base', required=True)
    parser.add_argument('--head', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    return PairedRun(args.repo, args.base, args.head, args.output).run()


if __name__ == '__main__':
    raise SystemExit(main())
