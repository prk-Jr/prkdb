"""Independent failure controls for descriptor-qualified, diagnostic-only profiling."""
import json
import os
from pathlib import Path
import resource
import re
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import wal_descriptor_diagnostic as d


class ImplementationTests(unittest.TestCase):
    def test_runner_exists(self):
        self.assertIsNotNone(d, 'descriptor-qualified runner has not been implemented')


class ProtocolTests(unittest.TestCase):
    def test_exact_four_controls_then_four_profiles(self):
        expected = [(phase, cell, side) for phase in ('control', 'profile')
                    for cell in ('adapter_put/8w/64k', 'stream_read/cold/64k')
                    for side in ('base', 'head')]
        self.assertEqual([(p['phase'], p['cell'], p['side']) for p in d.sequence()], expected)
        self.assertTrue(all(not p['timing_eligible'] for p in d.sequence()))

    def test_three_recorders_are_scoped_with_distinct_rates(self):
        cmd = d.profile_command('/tmp/wal-diag-base', Path('/tmp/trace'), Path('/tmp/ordinary.json'), 'a' * 64)
        self.assertEqual(cmd.count('record'), 3)
        outer, middle, inner = [], [], []
        current = outer
        for word in cmd:
            if word == '--' and current is outer:
                current = middle
            elif word == '--' and current is middle:
                current = inner
            else:
                current.append(word)
        self.assertIn('-a', outer)
        self.assertNotIn('-a', middle)
        self.assertNotIn('-a', inner)
        self.assertNotIn('--call-graph', middle)
        self.assertEqual(middle[middle.index('-c') + 1], '1')
        self.assertEqual(inner[inner.index('-F') + 1], '99')
        self.assertIn('dwarf,8192', inner)
        self.assertIn('cpu-clock:u', inner)
        for name in ('futex', 'read', 'pread64', 'write', 'pwrite64', 'fsync', 'fdatasync'):
            for phase in ('enter', 'exit'):
                self.assertIn(f'syscalls:sys_{phase}_{name}', middle)
                self.assertNotIn(f'syscalls:sys_{phase}_{name}', outer)
        for recorder in (outer, middle, inner):
            self.assertEqual(recorder[recorder.index('--clockid') + 1], 'mono')
        switch = outer[outer.index('sched:sched_switch') + 2]
        self.assertIn('prev_comm', switch)
        self.assertIn('next_comm', switch)
        self.assertIn('||', switch)
        self.assertIn('comm', outer[outer.index('sched:sched_wakeup') + 2])
        for event in ('sched:sched_process_fork', 'sched:sched_process_exec', 'sched:sched_process_exit',
                      'block:block_rq_issue', 'block:block_rq_complete'):
            at = outer.index(event)
            self.assertTrue(at + 1 == len(outer) or outer[at + 1] != '--filter')

    def test_environment_and_privilege_boundary_do_not_leak_secrets(self):
        with patch.dict(os.environ, {'TOP_SECRET': 'must-not-leak', 'LD_PRELOAD': '/bad.so'}):
            env = d.environment('/tmp/native', 'adapter_put/8w/64k')
        self.assertEqual(set(env), {'PATH', 'LANG', 'TMPDIR', 'SPIKE_REPS', 'SPIKE_WARMUP_MS',
                                    'SPIKE_MEASURE_MS', 'TOKIO_WORKER_THREADS', 'SPIKE_FILTER'})
        self.assertEqual([env[k] for k in ('SPIKE_REPS', 'SPIKE_WARMUP_MS', 'SPIKE_MEASURE_MS', 'TOKIO_WORKER_THREADS')],
                         ['3', '1000', '3000', '4'])
        root = d.root_command(['true'], env, Path('/tmp/run'), Path('/tmp/proof'))
        self.assertEqual(root[:4], ['sudo', '-n', 'env', '-i'])
        self.assertNotIn('TOP_SECRET', ' '.join(root))
        self.assertNotIn('LD_PRELOAD', ' '.join(root))

    def test_ordinary_limits_are_captured_without_any_setrlimit(self):
        with patch.object(resource, 'setrlimit', side_effect=AssertionError('ordinary budget must stay unchanged')):
            proof = d.resource_proof()
        self.assertEqual(proof['nofile'], list(resource.getrlimit(resource.RLIMIT_NOFILE)))
        self.assertEqual(proof['uid'], os.getuid())
        self.assertEqual(proof['groups'], sorted(os.getgroups()))
        self.assertGreaterEqual(proof['fd_count'], 3)
        self.assertIn('RLIMIT_STACK', proof['limits'])

    def test_child_restores_only_captured_pair_and_verifies(self):
        proof = {'nofile': [1024, 4096]}
        with patch.object(resource, 'setrlimit') as setter, patch.object(resource, 'getrlimit', return_value=(1024, 4096)):
            d.propagate_nofile(proof)
        setter.assert_called_once_with(resource.RLIMIT_NOFILE, (1024, 4096))
        with patch.object(resource, 'setrlimit'), patch.object(resource, 'getrlimit', return_value=(1024, 8192)):
            with self.assertRaises(d.DiagnosticError):
                d.propagate_nofile(proof)

    def test_missing_or_mismatched_final_identity_is_rejected(self):
        ordinary = dict(uid=1000, gid=1000, groups=[1000, 27], nofile=[1024, 4096])
        final = dict(ordinary, uids=[1000] * 4, gids=[1000] * 4)
        d.validate_identity(final, ordinary)
        for field, value in [('uid', 0), ('uids', [1000, 0, 1000, 1000]), ('groups', [1000]), ('nofile', [2048, 4096])]:
            invalid = dict(final, **{field: value})
            with self.subTest(field=field), self.assertRaises(d.DiagnosticError):
                d.validate_identity(invalid, ordinary)
        with self.assertRaises(d.DiagnosticError):
            d.validate_identity(ordinary, ordinary)

    def test_linux_padded_nofile_row_preserves_exact_limits(self):
        # fs/proc/base.c proc_pid_limits prints the unit with %-10s.
        status = 'Uid:\t1001 1001 1001 1001\nGid:\t1001 1001 1001 1001\nGroups:\t4 27 1001\n'
        for soft, hard in (('65536', '65536'), ('1024', 'unlimited')):
            line = f'{"Max open files":<25} {soft:<20} {hard:<20} {"files":<10}\n'
            result = d.identity_from_proc(status, line)
            self.assertEqual(result['nofile'], [int(soft), resource.RLIM_INFINITY if hard == 'unlimited' else int(hard)])
        for malformed in ('Max open files 65536 65536 bytes\n',
                          'Max open files 65536\n65536 files\n',
                          'Max open files 65536 65536 files garbage\n'):
            with self.subTest(malformed=malformed), self.assertRaises(d.DiagnosticError):
                d.identity_from_proc(status, malformed)

    def test_raw_proc_identity_is_retained_before_failed_decode(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            status = 'Uid:\t1001 1001 1001 1001\nGid:\t1001 1001 1001 1001\nGroups:\t1001\n'
            limits = 'malformed kernel fixture\n'
            with patch.object(Path, 'read_text', side_effect=[status, limits]):
                with self.assertRaises(d.DiagnosticError):
                    d.proc_identity(1001, directory)
            self.assertEqual((directory / 'native-status.txt').read_text(), status)
            self.assertEqual((directory / 'native-limits.txt').read_text(), limits)

    def test_all_caps_are_finite_and_overflow_rejects(self):
        self.assertEqual(d.RAW_LIMITS, {'cpu.data': 64 << 20, 'syscalls.data': 128 << 20, 'waits.data': 128 << 20})
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / 'cpu.data').write_bytes(b'0123456789')
            self.assertIsNone(d.budget_breach(root, root, limits={'cpu.data': 10}, total_limit=10))
            self.assertIsNotNone(d.budget_breach(root, root, limits={'cpu.data': 9}, total_limit=10))
            self.assertIsNotNone(d.budget_breach(root, root, limits={'cpu.data': 100}, total_limit=9))

    def test_runtime_wal_bytes_are_not_observer_render_bytes(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / 'runtime').mkdir()
            with (root / 'runtime/native.wal').open('wb') as wal:
                wal.truncate(d.RENDER_TOTAL + 1)
            self.assertIsNone(d.budget_breach(root, root))

    def test_explicit_cpu_and_task_metadata_render_fields(self):
        commands = d.render_commands(Path('/tmp/trace'))
        self.assertIn('--show-task-events', commands['waits-script'])
        for name in ('cpu-script', 'waits-script', 'syscalls-script'):
            self.assertIn('--ns', commands[name])
            self.assertIn('--hide-call-graph', commands[name])
            self.assertIn('cpu', commands[name][commands[name].index('-F') + 1].split(','))
        self.assertIn('-v', commands['waits-attributes'])


def sample(stamp, event, body, comm='wal-diag-base', pid=10, tid=10):
    return f'{comm:>16} {pid:7}/{tid:<7} [002] {stamp}: {event}: {body}\n'


def task(stamp, body, pid=77, tid=88):
    return f'{"perf":>16} {pid:7}/{tid:<7} [002] {stamp}: {body}\n'


def traces(extra_name='tokio-short'):
    waits = sample('1.000000000', 'sched:sched_process_exec', 'filename=/tmp/wal-diag-base pid=10 old_pid=10')
    waits += task('1.000000001', 'PERF_RECORD_COMM exec: wal-diag-base:10/10')
    waits += sample('1.010000000', 'sched:sched_process_fork', 'comm=wal-diag-base pid=10 child_comm=wal-diag-base child_pid=11')
    waits += task('1.010000001', 'PERF_RECORD_FORK(10:11):(10:10)')
    waits += sample('1.010000001', 'task:task_rename', f'pid=11 oldcomm=wal-diag-base newcomm={extra_name} oom_score_adj=0')
    waits += task('1.010000002', f'PERF_RECORD_COMM: {extra_name}:10/11')
    waits += sample('1.020000000', 'sched:sched_switch', 'prev_comm=tokio-short prev_pid=11 prev_prio=120 prev_state=S ==> next_comm=swapper next_pid=0 next_prio=120')
    waits += sample('1.030000000', 'sched:sched_wakeup', 'comm=tokio-short pid=11 prio=120 target_cpu=002')
    waits += sample('1.040000000', 'sched:sched_switch', 'prev_comm=swapper prev_pid=0 prev_prio=120 prev_state=R ==> next_comm=tokio-short next_pid=11 next_prio=120')
    waits += sample('1.049999999', 'task:task_rename', 'pid=11 oldcomm=tokio-short newcomm=prkdb-wal-short oom_score_adj=0')
    waits += task('1.050000000', 'PERF_RECORD_COMM: prkdb-wal-short:10/11')
    waits += sample('1.060000000', 'sched:sched_process_exit', 'comm=prkdb-wal-short pid=11 prio=120')
    waits += task('1.060000001', 'PERF_RECORD_EXIT(10:11):(10:10)')
    waits += sample('2.000000000', 'sched:sched_process_exit', 'comm=wal-diag-base pid=10 prio=120')
    waits += task('2.000000001', 'PERF_RECORD_EXIT(10:10):(77:77)')
    calls = sample('0.900000000', 'syscalls:sys_enter_write', 'fd: 1, buf: 0x1, count: 4')
    calls += sample('1.100000000', 'syscalls:sys_enter_read', 'fd: 3, buf: 0x1, count: 4')
    calls += sample('1.110000000', 'syscalls:sys_exit_read', 'ret: 4')
    calls += sample('2.100000000', 'syscalls:sys_exit_write', 'ret: 4')
    cpu = sample('1.200000000', 'cpu-clock:u', '1')
    return waits, calls, cpu


class CoverageTests(unittest.TestCase):
    def analyze(self, values=None):
        waits, calls, cpu = traces() if values is None else values
        return d.analyze_traces(waits, calls, cpu, '/tmp/wal-diag-base', 10)

    def test_compact_scheduler_plugin_preserves_target_ids_and_intervals(self):
        waits, calls, cpu = traces()
        waits = waits.replace('prev_comm=tokio-short prev_pid=11 prev_prio=120 prev_state=S ==> next_comm=swapper next_pid=0 next_prio=120',
                              'tokio-short:11 [120] S ==> swapper:0 [120]')
        waits = waits.replace('comm=tokio-short pid=11 prio=120 target_cpu=002', 'tokio-short:11 [120] CPU:002')
        waits = waits.replace('prev_comm=swapper prev_pid=0 prev_prio=120 prev_state=R ==> next_comm=tokio-short next_pid=11 next_prio=120',
                              'swapper:0 [120] R ==> tokio-short:11 [120]')
        result = self.analyze((waits, calls, cpu))
        self.assertEqual(result['offcpu_by_state']['S']['total_ms'], 20)
        self.assertEqual(result['wakeup_to_run']['total_ms'], 10)
        row = d.parse_records(sample('1.000000000', 'sched:sched_switch', 'kworker/0:0:9 [120] R+ ==> name with space:20 [100]'))[0]
        self.assertEqual(row['fields']['prev_comm'], 'kworker/0:0')
        self.assertEqual(row['fields']['prev_pid'], '9')
        self.assertEqual(row['fields']['next_comm'], 'name with space')
        wake = d.parse_records(sample('1.000000000', 'sched:sched_wakeup', 'kworker/0:0:9 [120] success=1 CPU:002'))[0]
        self.assertEqual(wake['fields']['pid'], '9')

    def test_malformed_scheduler_payloads_fail_closed(self):
        for event, detail in (
                ('sched:sched_switch', 'tokio-short:11 [120] S ==> swapper:0'),
                ('sched:sched_switch', 'tokio-short:11 [120] S ==> swapper:0 [120] garbage'),
                ('sched:sched_wakeup', 'tokio-short:11 [120] CPU:bad'),
                ('sched:sched_wakeup', 'tokio-short:11 [120] CPU:002 garbage'),
                ('sched:sched_switch', 'prev_comm=tokio-short prev_pid=11'),
                ('sched:sched_wakeup', 'comm=tokio-short pid=11')):
            with self.subTest(detail=detail), self.assertRaises(d.DiagnosticError):
                d.parse_records(sample('1.000000000', event, detail))

    def test_native_comm_corrupted_target_group_is_rejected(self):
        for body in ('PERF_RECORD_COMM exec: wal-diag-base:10/10', 'PERF_RECORD_COMM: tokio-short:10/11'):
            waits, calls, cpu = traces()
            waits = waits.replace(body, body.replace(':10/', ':999/'))
            with self.subTest(body=body), self.assertRaises(d.DiagnosticError):
                self.analyze((waits, calls, cpu))

    def test_body_metadata_ids_unsampled_thread_and_rename_are_covered(self):
        result = self.analyze()
        self.assertEqual(result['thread_ids'], [10, 11])
        self.assertEqual(result['cpu_sampled_thread_ids'], [10])
        self.assertEqual(result['unsampled_thread_ids'], [11])
        self.assertEqual(result['thread_names']['11'], ['wal-diag-base', 'tokio-short', 'prkdb-wal-short'])
        self.assertTrue(result['name_coverage_complete'])
        self.assertEqual(result['syscalls']['read']['count'], 1)
        self.assertAlmostEqual(result['syscalls']['read']['total_ms'], 10)
        self.assertEqual(result['window_ns'], [1000000000, 2000000000])
        self.assertFalse(result['timing_eligible'])

    def test_exec_comm_precedes_tracepoint_and_exit_metadata_follows_it(self):
        waits, calls, cpu = traces()
        lines = waits.splitlines(True)
        lines[1] = lines[1].replace('1.000000001', '0.999999999')
        lines[0], lines[1] = lines[1], lines[0]
        result = self.analyze((''.join(lines), calls, cpu))
        self.assertEqual(result['window_ns'], [1000000000, 2000000000])

    def test_unknown_name_negative_fixture_fails_coverage(self):
        with self.assertRaisesRegex(d.DiagnosticError, 'name.*coverage|uncovered.*name'):
            self.analyze(traces('invisible-name'))

    def test_missing_exec_exit_or_fork_metadata_fails_closed(self):
        for needle in ('sched:sched_process_exec', 'PERF_RECORD_FORK(', 'sched:sched_process_exit'):
            waits, calls, cpu = traces()
            waits = ''.join(line for line in waits.splitlines(True) if needle not in line)
            with self.subTest(needle=needle), self.assertRaises(d.DiagnosticError):
                self.analyze((waits, calls, cpu))

    def test_zero_timestamp_comm_unknown_line_and_loss_fail_closed(self):
        waits, calls, cpu = traces()
        corruptions = [waits.replace('1.050000000', '0.000000000'), waits + 'unparsed\n', waits + 'PERF_RECORD_LOST 1\n']
        for bad in corruptions:
            with self.subTest(bad=bad[-80:]), self.assertRaises(d.DiagnosticError):
                self.analyze((bad, calls, cpu))

    def test_unpaired_in_window_syscall_fails_quality(self):
        waits, calls, cpu = traces()
        calls = calls.replace('1.110000000', '2.110000000')
        with self.assertRaises(d.DiagnosticError):
            self.analyze((waits, calls, cpu))

    def test_scheduler_name_disagreement_is_coverage_failure(self):
        waits, calls, cpu = traces()
        waits = waits.replace('prev_comm=tokio-short', 'prev_comm=unknown')
        with self.assertRaises(d.DiagnosticError):
            self.analyze((waits, calls, cpu))

    def test_scheduler_new_name_in_exact_rename_bracket_is_covered(self):
        waits, calls, cpu = traces()
        wake = sample('1.010000001', 'sched:sched_wakeup', 'comm=tokio-short pid=11 prio=120 target_cpu=002')
        lines = waits.splitlines(True)
        at = next(i for i, line in enumerate(lines) if 'PERF_RECORD_COMM: tokio-short' in line)
        lines.insert(at, wake)
        self.assertTrue(self.analyze((''.join(lines), calls, cpu))['name_coverage_complete'])

    def test_noop_rename_without_timed_comm_fails_coverage(self):
        waits, calls, cpu = traces()
        lines = waits.splitlines(True)
        lines.insert(2, sample('1.005000000', 'task:task_rename', 'pid=10 oldcomm=wal-diag-base newcomm=wal-diag-base oom_score_adj=0'))
        with self.assertRaises(d.DiagnosticError):
            self.analyze((''.join(lines), calls, cpu))

    def test_changed_comm_requires_observed_rename_transition(self):
        waits, calls, cpu = traces()
        waits = ''.join(line for line in waits.splitlines(True) if 'task:task_rename' not in line)
        with self.assertRaises(d.DiagnosticError):
            self.analyze((waits, calls, cpu))

    def test_unknown_recorder_event_is_not_silently_ignored(self):
        waits, calls, cpu = traces()
        for values in ((waits.replace('sched:sched_wakeup', 'unknown:event'), calls, cpu),
                       (waits, calls, cpu.replace('cpu-clock:u', 'unknown:event'))):
            with self.assertRaises(d.DiagnosticError):
                self.analyze(values)

    def test_metadata_only_native_thread_cannot_escape_coverage(self):
        waits, calls, cpu = traces()
        waits = ''.join(line for line in waits.splitlines(True) if 'sched:sched_process_fork' not in line)
        with self.assertRaises(d.DiagnosticError):
            self.analyze((waits, calls, cpu))

    def test_unknown_cpu_symbols_are_retained_as_incomplete_attribution(self):
        known = '  90.00% libc.so.6 [.] write\n'
        complete = d.cpu_symbol_quality(known)
        self.assertTrue(complete['cpu_symbol_attribution_complete'])
        incomplete = d.cpu_symbol_quality(known + '  10.00% [unknown] [.] 0x1234\n')
        self.assertFalse(incomplete['cpu_symbol_attribution_complete'])
        self.assertTrue(incomplete['unknown_symbols_present'])
        self.assertEqual(incomplete['unknown_symbol_rows'], 1)

    def test_all_recorders_must_have_qualified_attribute_proof(self):
        good = 'sample_type: IP|TID|TIME|CPU, sample_id_all: 1, inherit: 1, comm: 1, task: 1, use_clockid: 1, clockid: 1'
        for missing in ('cpu', 'syscalls', 'waits'):
            with tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                for kind in ('cpu', 'syscalls', 'waits'):
                    (root / f'render-{kind}-attributes').mkdir()
                    (root / f'render-{kind}-attributes/stdout.txt').write_text(good.replace('TIME', '') if kind == missing else good)
                (root / 'render-waits-raw-events').mkdir()
                (root / 'render-waits-raw-events/stdout.txt').write_text('PERF_RECORD_THREAD_MAP nr: 1 thread: -1\nPERF_RECORD_CPU_MAP: 0-3\n')
                with self.subTest(missing=missing), self.assertRaisesRegex(d.DiagnosticError, 'attributes'):
                    d.validate_recording_attributes(root, [0, 1, 2, 3])

    def test_system_recorder_requires_all_cpu_global_attachment_instead_of_inheritance(self):
        attrs = 'sample_type: IP|TID|TIME|CPU, sample_id_all: 1, comm: 1, task: 1, use_clockid: 1, clockid: 1'
        d.validate_attributes(attrs, require_inherit=False)
        maps = 'PERF_RECORD_THREAD_MAP nr: 1 thread: -1\nPERF_RECORD_CPU_MAP: 0-3\n'
        d.validate_system_attachment(maps, [0, 1, 2, 3])
        for bad in (maps.replace('0-3', '0-2'), maps.replace('thread: -1', 'thread: 123'),
                    maps.replace('0-3', '0-4294967295'), maps + maps):
            with self.subTest(bad=bad), self.assertRaises(d.DiagnosticError):
                d.validate_system_attachment(bad, [0, 1, 2, 3])
        with self.assertRaises(d.DiagnosticError):
            d.validate_attributes(attrs)  # CPU/syscalls still need inherited child coverage.

    def test_global_thread_map_rejects_trailing_targets(self):
        raw = 'PERF_RECORD_THREAD_MAP nr: 1 thread: -1 thread: 123\nPERF_RECORD_CPU_MAP: 0-3\n'
        with self.assertRaises(d.DiagnosticError):
            d.validate_system_attachment(raw, [0, 1, 2, 3])

    def test_pidless_rename_uses_target_comm_identity_not_caller_tid(self):
        waits, calls, cpu = traces()
        waits = re.sub(r'(task:task_rename: )pid=11 ', r'\1', waits)
        # Caller is TID 10, target is TID 11; metadata body is authoritative.
        waits = re.sub(r'perf\s+77/88\s+(\[002\].*PERF_RECORD_COMM:)', r'perf 10/10 \1', waits)
        result = self.analyze((waits, calls, cpu))
        self.assertEqual(result['thread_names']['11'], ['wal-diag-base', 'tokio-short', 'prkdb-wal-short'])
        self.assertTrue(result['name_coverage_complete'])

    def test_pidless_rename_ambiguity_and_unpaired_events_fail_closed(self):
        waits, calls, cpu = traces()
        waits = re.sub(r'(task:task_rename: )pid=11 ', r'\1', waits)
        waits = re.sub(r'perf\s+77/88\s+(\[002\].*PERF_RECORD_COMM:)', r'perf 10/10 \1', waits)
        line = next(line for line in waits.splitlines(True) if 'task:task_rename:' in line)
        duplicate = waits.replace(line, line + line)
        extra = sample('1.070000000', 'task:task_rename', 'oldcomm=wal-diag-base newcomm=tokio-lost oom_score_adj=0')
        lines = waits.splitlines(True)
        at = next(i for i,line in enumerate(lines) if '2.000000000' in line)
        unpaired = ''.join(lines[:at]) + extra + ''.join(lines[at:])
        for invalid in (duplicate, unpaired):
            with self.subTest(invalid=invalid[-100:]), self.assertRaises(d.DiagnosticError):
                self.analyze((invalid, calls, cpu))

    def test_attrproof_requires_sample_id_all_time_and_cpu(self):
        good = 'sched:sched_switch: sample_type: IP|TID|TIME|CPU, sample_id_all: 1, inherit: 1, comm: 1, task: 1, use_clockid: 1, clockid: 1\n'
        d.validate_attributes(good)
        for field in ('sample_id_all', 'inherit', 'comm', 'task', 'use_clockid', 'clockid'):
            with self.subTest(field=field), self.assertRaises(d.DiagnosticError):
                d.validate_attributes(good.replace(field + ': 1', field + ': 0'))
        for bad in ('sample_type: IP|TID|CPU\nsample_id_all: 1', good.replace('TIME', '')):
            with self.assertRaises(d.DiagnosticError):
                d.validate_attributes(bad)


class ChildTests(unittest.TestCase):
    def test_real_child_scoped_launch_preserves_identity_and_limits(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            proof = d.resource_proof()
            d.dump(directory / 'ordinary.json', proof)
            executable = Path(sys.executable).resolve()
            cmd = [sys.executable, d.__file__, 'scoped', '--proof', str(directory / 'ordinary.json'),
                   '--directory', str(directory), '--binary', str(executable), '--sha256', d.digest(executable)]
            child = subprocess.run(cmd + ['--', '-c', 'import resource;print(resource.getrlimit(resource.RLIMIT_NOFILE))'],
                                   capture_output=True, text=True, check=False)
            self.assertEqual(child.returncode, 0, child.stderr)
            final = json.loads((directory / 'native-launch.json').read_text())
            d.validate_identity(final, proof)
            self.assertIn(str(tuple(proof['nofile'])), child.stdout)

    def test_real_child_inherited_mismatch_refuses_before_exec(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            proof = d.resource_proof()
            proof['nofile'][0] -= 1
            d.dump(directory / 'ordinary.json', proof)
            executable = Path(sys.executable).resolve()
            child = subprocess.run([sys.executable, d.__file__, 'scoped', '--proof', str(directory / 'ordinary.json'),
                                    '--directory', str(directory), '--binary', str(executable), '--sha256', d.digest(executable),
                                    '--', '-c', 'print("should-not-exec")'], capture_output=True, text=True, check=False)
            self.assertNotEqual(child.returncode, 0)
            self.assertNotIn('should-not-exec', child.stdout)
            self.assertFalse((directory / 'native-launch.json').exists())

    def test_real_budget_breach_contains_child_and_retains_exit_prefix(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            script = "import os,signal;signal.pause()"
            with patch.object(d, 'RENDER_FILE', 16):
                # Metadata predates command monitoring and exceeds the tiny test
                # cap too. The owned child still must be contained, with status.
                with self.assertRaises(d.DiagnosticError):
                    d.execute([sys.executable, '-c', script], {'PATH': os.environ.get('PATH', '/usr/bin:/bin')},
                              directory, directory)
            self.assertTrue((directory / 'exit.json').is_file())
            status = json.loads((directory / 'exit.json').read_text())
            self.assertNotEqual(status['exit_code'], 0)
            with self.assertRaises(ProcessLookupError):
                os.kill(status['pid'], 0)
            self.assertTrue((directory / 'native-fd-series.json').is_file())

    def test_transient_proc_gap_has_timestamp_and_reason(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            d.dump(directory / 'native-launch.json', {'pid': 2147483647, 'binary': '/nonexistent'})
            watch = d.CaptureWatch(directory, directory, lambda: True)
            watch.done.set()
            watch.watch()
            self.assertIsNone(watch.error)
            self.assertEqual(watch.unavailable, 1)
            self.assertGreater(watch.unavailable_observations[0]['monotonic_ns'], 0)
            self.assertIn('error', watch.unavailable_observations[0])

    def test_partial_root_group_witness_is_not_treated_as_valid(self):
        with tempfile.TemporaryDirectory() as tmp:
            witness = Path(tmp) / 'root-group.json'
            witness.write_text('{"child_pid":')
            self.assertIsNone(d.read_owned_group(witness))
            d.dump(witness, {'child_pid': 98765, 'pgid': 98765})
            self.assertEqual(d.read_owned_group(witness), 98765)
            d.dump(witness, {'child_pid': 98765, 'pgid': 1})
            with self.assertRaises(d.DiagnosticError):
                d.read_owned_group(witness)

    def test_missing_root_witness_still_contains_outer_process(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            d.dump(directory / 'root-before.json', {'pid': 98765})
            from unittest.mock import Mock
            process = Mock(pid=98766)
            process.wait.side_effect = [subprocess.TimeoutExpired(['sudo'], 135), -9, -9]
            responses = [subprocess.CompletedProcess([], 0, '', ''), subprocess.CompletedProcess([], 1, '', 'No such process')]
            with patch.object(d.subprocess, 'Popen', return_value=process), patch.object(d.subprocess, 'run', side_effect=responses) as run, \
                    patch.object(d, 'kill_group', return_value=True) as kill:
                with self.assertRaises(d.DiagnosticError):
                    d.execute(['sudo'], {}, directory, directory, privileged=True)
            kill.assert_any_call(98766)
            self.assertEqual(run.call_args_list[0].args[0], ['sudo', '-n', 'kill', '-TERM', '--', '98765'])
            cleanup = json.loads((directory / 'root-containment.json').read_text())
            self.assertTrue(cleanup['supervisor_gone'])
            self.assertFalse(cleanup['qualified_cleanup'])
            self.assertFalse(cleanup['actual_group_known'])
            self.assertTrue(json.loads((directory / 'exit.json').read_text())['timed_out'])

    def test_outer_group_permission_failure_still_retains_exit_json(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            from unittest.mock import Mock
            process = Mock(pid=98766)
            process.wait.return_value = 0
            with patch.object(d.subprocess, 'Popen', return_value=process), \
                    patch.object(d, 'kill_group', side_effect=PermissionError('group kill denied')):
                with self.assertRaises(d.DiagnosticError):
                    d.execute(['true'], {}, directory, directory)
            status = json.loads((directory / 'exit.json').read_text())
            self.assertIn('group kill denied', status['error'])
            self.assertFalse(status['outer_cleanup_complete'])
            self.assertTrue(status['cleanup_errors'])

    def test_refused_root_kill_records_uncontained_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            d.dump(directory / 'root-group.json', {'child_pid': 98765, 'pgid': 98765})
            from unittest.mock import Mock
            process = Mock(pid=98766)
            process.wait.side_effect = [subprocess.TimeoutExpired(['sudo'], 135), -9, -9]
            refused = subprocess.CompletedProcess([], 1, '', 'sudo: a password is required')
            with patch.object(d.subprocess, 'Popen', return_value=process), \
                    patch.object(d.subprocess, 'run', return_value=refused), patch.object(d, 'kill_group', return_value=True):
                with self.assertRaises(d.DiagnosticError):
                    d.execute(['sudo'], {}, directory, directory, privileged=True)
            cleanup = json.loads((directory / 'root-containment.json').read_text())
            self.assertFalse(cleanup['qualified_cleanup'])
            self.assertTrue(cleanup['uncontained'])
            self.assertEqual(cleanup['signal']['exit_code'], 1)
            self.assertEqual(cleanup['probe']['exit_code'], 1)
            self.assertIn('root containment', json.loads((directory / 'exit.json').read_text())['error'])

    def test_actual_exec_proof_cannot_be_replaced_by_preexec_launch(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            d.dump(root / 'native-launch.json', {'pid': os.getpid(), 'binary': '/unknown'})
            with self.assertRaises(d.DiagnosticError):
                d.validate_native_evidence(root)

    def test_fd_series_reports_observed_maximum_and_gaps(self):
        result = d.fd_summary([{'monotonic_ns': 0, 'fd_count': 3, 'max_fd': 8},
                               {'monotonic_ns': 250000000, 'fd_count': 6, 'max_fd': 100}], 1)
        self.assertEqual(result['max_observed_fd_count'], 6)
        self.assertEqual(result['max_observed_fd_number'], 100)
        self.assertEqual(result['max_gap_ns'], 250000000)
        self.assertEqual(result['sampling_interval_ns'], 100000000)
        self.assertFalse(result['proves_absolute_peak'])


if __name__ == '__main__':
    unittest.main()
