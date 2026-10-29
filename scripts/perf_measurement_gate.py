#!/usr/bin/env python3
"""Fail-closed measurement, canary and overlay guards for the TST-09 repair."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import re
import tomllib


class GateError(Exception):
    """Missing, inconsistent or unqualified measurement evidence."""


PREFIX = 'iai_hot_paths::hot_paths::'
ASYNC_NAMES = ('bench_wal_put_100_measured', 'bench_wal_get_hit_measured',
               'bench_wal_put_batch_100_measured', 'bench_indexed_insert_one_measured')
ALL_NAMES = ASYNC_NAMES + ('bench_log_record_encode', 'bench_log_record_decode')
DEV_PACKAGES = {'cty', 'valgrind-requests', 'bindgen', 'clang-sys', 'cexpr', 'glob',
                'nom', 'minimal-lexical', 'rustc-hash', 'strum', 'strum_macros', 'prettyplease',
                'gungraun-common', 'libloading', 'shlex'}


def require(ok, message):
    if not ok:
        raise GateError(message)


def integer(value, label, positive=False):
    require(type(value) is int and value >= (1 if positive else 0), f'invalid {label}: {value!r}')
    return value


def digest(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def raw_metric(path):
    """Use totals: per gungraun, summary: is unreliable with client requests."""
    path = Path(path)
    require(path.is_file() and not path.is_symlink(), f'missing/symlink raw output: {path}')
    fields = {}
    for line in path.read_text().splitlines():
        key, sep, value = line.partition(':')
        if sep and key in ('pid', 'thread', 'part', 'events', 'summary', 'totals'):
            require(key not in fields, f'duplicate raw {key}: {path}')
            fields[key] = value.strip()
    require(all(k in fields for k in ('pid', 'thread', 'part', 'events')), f'incomplete raw identity: {path}')
    identity = {}
    for key in ('pid', 'thread', 'part'):
        require(re.fullmatch(r'[0-9]+', fields[key]) is not None, f'invalid raw {key}')
        identity[key] = integer(int(fields[key]), key, positive=True)
    events = fields['events'].split()
    require(events.count('Ir') == 1, 'raw output must contain exactly one Ir event')
    data = fields.get('totals', fields.get('summary'))
    require(data is not None, 'raw output has no totals or summary')
    values = data.split()
    require(len(values) > events.index('Ir') and all(re.fullmatch(r'[0-9]+', v) for v in values), 'invalid raw counts')
    identity['ir'] = int(values[events.index('Ir')])
    identity['sha256'] = digest(path)
    identity['path'] = str(path.resolve())
    return identity


def side(value, which='current'):
    require(isinstance(value, dict) and len(value) == 1, 'invalid EitherOrBoth')
    if 'Both' in value:
        pair = value['Both']
        require(isinstance(pair, list) and len(pair) == 2, 'invalid Both')
        return pair[0 if which == 'current' else 1]
    if 'Left' in value:
        return value['Left'] if which == 'current' else None
    if 'Right' in value:
        return value['Right'] if which == 'base' else None
    raise GateError('unknown EitherOrBoth variant')


def ir_value(summary, which='current'):
    try:
        metric = side(summary['Callgrind']['Ir']['metrics'], which)
    except (KeyError, TypeError) as error:
        raise GateError('missing Callgrind Ir metric') from error
    if metric is None:
        return None
    require(isinstance(metric, dict) and set(metric) == {'Int'}, 'instructions must be integer metrics')
    return integer(metric['Int'], 'instructions')


def profile(summary, root, require_base=False, retained_dir=None):
    root = Path(root).resolve()

    def retained_path(original):
        path = Path(original)
        require(path.is_absolute(), 'raw path must be absolute')
        if path.resolve().is_relative_to(root):
            return path
        # Runner 0.19.4 copies temporary files verbatim, but does not rewrite
        # their summary paths. Only resolve that exact convention beside summary.json.
        require(retained_dir is not None, 'raw path outside evidence roots')
        directory = Path(retained_dir).resolve()
        require(directory.is_relative_to(root), 'retained directory outside evidence roots')
        require(re.fullmatch(r'gungraun\.tmp[A-Za-z0-9]{10}' + re.escape(directory.name), path.parent.name)
                is not None, 'unrecognized temporary raw path')
        require(path.name.startswith('callgrind.' + directory.name + '.'), 'unexpected temporary raw filename')
        return directory / path.name
    matches = [p for p in summary.get('profiles', []) if p.get('tool') == 'Callgrind']
    require(len(matches) == 1, 'exactly one Callgrind profile required')
    data = matches[0]
    try:
        total = ir_value(data['summaries']['total']['summary'])
        previous = ir_value(data['summaries']['total']['summary'], 'base')
        parts = data['summaries']['parts']
    except (KeyError, TypeError) as error:
        raise GateError('invalid profile summary') from error
    integer(total, 'total instructions', positive=True)
    if require_base:
        integer(previous, 'base instructions', positive=True)
    require(isinstance(parts, list) and parts, 'missing raw thread parts')
    proofs = []
    identities = set()
    for part in parts:
        details = side(part.get('details'))
        value = ir_value(part.get('metrics_summary', {}))
        if details is None:
            require(value is None, 'metric without current part identity')
            continue
        require(isinstance(details, dict) and value is not None, 'part identity/metric mismatch')
        path = retained_path(details.get('path', ''))
        raw = raw_metric(path)
        require(all(raw[k] == details.get(k) for k in ('pid', 'thread', 'part')), 'raw identity differs from summary')
        require(raw['ir'] == value, 'raw thread count differs from part summary')
        identity = tuple(raw[k] for k in ('pid', 'thread', 'part'))
        require(identity not in identities, 'duplicate raw thread/part')
        identities.add(identity)
        proofs.append(raw)
    outputs = data.get('out_paths', [])
    require(isinstance(outputs, list) and outputs, 'missing profile output paths')
    resolved = [str(retained_path(p).resolve()) for p in outputs]
    require(len(set(resolved)) == len(resolved), 'duplicate profile output path')
    require(set(resolved) == {p['path'] for p in proofs}, 'unaccounted or missing raw thread')
    require(sum(p['ir'] for p in proofs) == total, 'all-thread raw sum differs from summary total')
    require(len({p['pid'] for p in proofs}) == 1, 'unexpected multiple benchmark processes')
    main = [p for p in proofs if p['thread'] == 1]
    require(len(main) == 1 and main[0]['ir'] > 0, 'missing/ambiguous benchmark main-thread evidence')
    return {'ir': total, 'main_ir': main[0]['ir'], 'base_ir': previous, 'parts': proofs}


def collect(directory, allowed_root, expected, require_base=False):
    found = {}
    for path in sorted(Path(directory).rglob('summary.json')):
        try:
            data = json.loads(path.read_text())
        except (ValueError, OSError) as error:
            raise GateError(f'unreadable summary: {path}') from error
        name = data.get('module_path')
        if data.get('id'):
            name = str(name) + '::' + str(data['id'])
        require(name not in found, 'duplicate benchmark summary')
        found[name] = profile(data, allowed_root, require_base, retained_dir=path.parent)
    require(set(found) == set(expected), f'missing/unexpected benchmarks: {set(found) ^ set(expected)}')
    return found


def coverage(zero, high):
    wanted = {PREFIX + n for n in ASYNC_NAMES}
    require(set(zero) == set(high) == wanted, 'canary requires all four async workloads')
    result = {}
    for name in sorted(wanted):
        main = high[name]['main_ir'] - zero[name]['main_ir']
        total = high[name]['ir'] - zero[name]['ir']
        require(main >= 4096 and total >= 4096, f'operation-body canary excluded or incomplete: {name}')
        result[name] = {'main_delta': main, 'total_delta': total}
    return result


def comparison(base, head):
    wanted = {PREFIX + n for n in ALL_NAMES}
    require(set(base) == set(head) == wanted, 'incomplete or renamed comparison baseline')
    rows = []
    bad = []
    for name in sorted(wanted):
        old = integer(base[name]['ir'], 'base', positive=True)
        new = integer(head[name]['ir'], 'head', positive=True)
        require(head[name].get('base_ir') == old, 'saved comparison baseline drift')
        regressed = new * 100 > old * 105
        if regressed:
            bad.append(name)
        rows.append({'name': name, 'base': old, 'head': new, 'delta_percent': (new - old) * 100 / old, 'pct': (new - old) * 100 / old, 'regressed': regressed})
    return {'rows': rows, 'regressed': bad}


def patch_manifest(text):
    old = 'gungraun = { version = "=0.19.4" }'
    new = 'gungraun = { version = "=0.19.4", features = ["client_requests"] }'
    require(text.count(old) + text.count(new) == 1, 'unexpected gungraun manifest definition')
    return text.replace(old, new)


def production_graph(metadata, include_global_features=True):
    """Canonical normal/build graph from all workspace roots, excluding dev edges."""
    try:
        nodes = {n['id']: n for n in metadata['resolve']['nodes']}
        packages = {p['id']: p for p in metadata['packages']}
        pending = list(metadata['workspace_members'])
    except (KeyError, TypeError) as error:
        raise GateError('incomplete Cargo metadata') from error
    result = {}
    while pending:
        package_id = pending.pop()
        if package_id in result:
            continue
        require(package_id in nodes and package_id in packages, 'unresolved production package')
        node, package = nodes[package_id], packages[package_id]
        edges = []
        for dep in node['deps']:
            kinds = [k for k in dep['dep_kinds'] if k['kind'] != 'dev']
            if kinds:
                edges.append((dep['name'], dep['pkg'], sorted((k['kind'] or 'normal', k.get('target') or '') for k in kinds)))
                pending.append(dep['pkg'])
        result[package_id] = {'name': package['name'], 'version': package['version'], 'source': package['source'],
                              'features': sorted(node['features']) if include_global_features else None, 'edges': sorted(edges)}
    return result


def package_id(package):
    return (package['name'], package['version'], package.get('source', ''))


def lock_packages(text):
    return {package_id(p): p for p in tomllib.loads(text).get('package', [])}


def canonical_dependencies(package, packages):
    result = set()
    for edge in package.get('dependencies', []):
        parts = edge.split()
        matches = [key for key in packages if key[0] == parts[0] and (len(parts) < 2 or key[1] == parts[1])
                   and (len(parts) < 3 or key[2] == parts[2].strip('()'))]
        require(len(matches) == 1, f'ambiguous/unresolved locked edge: {edge}')
        result.add(matches[0])
    return result


def lock_overlay(before, after):
    old, new = lock_packages(before), lock_packages(after)
    require(set(old) <= set(new), 'overlay removed or updated a locked package')
    for key in new.keys() - old.keys():
        require(key[0] in DEV_PACKAGES, f'unapproved new dev package: {key}')
    for key, prior in old.items():
        current = new[key]
        require({k: v for k, v in prior.items() if k != 'dependencies'} ==
                {k: v for k, v in current.items() if k != 'dependencies'}, 'overlay changed locked package fields')
        prior_deps = canonical_dependencies(prior, old)
        current_deps = canonical_dependencies(current, new)
        if key[0] == 'gungraun':
            require(prior_deps <= current_deps and all(d[0] in ('cty', 'valgrind-requests') for d in current_deps - prior_deps), 'unapproved gungraun edge')
        else:
            require(prior_deps == current_deps, f'overlay changed existing package dependencies: {key}')


def verify_production(before, after, before_tree, after_tree):
    # Cargo metadata unions dev/build features; cargo tree excludes dev contexts.
    old = production_graph(before, include_global_features=False)
    new = production_graph(after, include_global_features=False)
    require(old == new, 'overlay changes production packages/dependency edges')
    require(before_tree == after_tree and before_tree.strip(),
            'overlay changes normal/build production feature contexts')
    return old
