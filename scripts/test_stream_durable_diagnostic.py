"""Fail-closed controls for the finite Durable append diagnostic."""
import copy
import argparse
import subprocess
from types import SimpleNamespace
import json
import os
from pathlib import Path
import struct
import tempfile
import unittest
from unittest.mock import patch

try:
    import stream_durable_diagnostic as d
except ImportError:
    d = None
import wal_stall_diagnostic as s
from test_wal_descriptor_diagnostic import traces, sample


class DurableDiagnosticTests(unittest.TestCase):
    def setUp(self):
        self.assertIsNotNone(d, 'finite Durable runner has not been implemented')

    def native(self):
        rows = ['# wal_write_path', s.CONFIG_HEADER, s.WRITE_HEADER, s.STREAM_HEADER]
        for _ in range(3):
            for cell in d.CELLS:
                rows.append(f'| {cell} | 1 | 1 KiB | 1000 | 1.0 | 10 | 20 | 30 | 1000 | 1.39 1.61 1.45 |')
        return '\n'.join(rows)

    def test_retained_linux_syscall_exit_hex_returns_are_signed_and_complete(self):
        # Qualified Linux perf-script output retained in actions-38374274969:
        # sys_exit_write: 0x20; sys_exit_futex/fdatasync: 0x0.
        common,_,_=traces()
        calls=sample('1.100000000','syscalls:sys_enter_write','fd: 0x00000003, buf: 0x1, count: 0x20')
        calls+=sample('1.110000000','syscalls:sys_exit_write','0x20')
        calls+=sample('1.120000000','syscalls:sys_enter_futex','uaddr: 0x1, op: 0x80, val: 0x0')
        calls+=sample('1.130000000','syscalls:sys_exit_futex','0xfffffffffffffff5')
        result=d.analyze_category('syscalls',common+calls,'/tmp/wal-diag-base',10)
        self.assertEqual([p['return_value'] for p in result['pairs']],[32,-11])
        self.assertEqual(d.syscall_return('0x0'),0)
        self.assertEqual(d.syscall_return('ret: -5'),-5)
        self.assertEqual(d.syscall_return('ret=0xfffffffffffffff5'),-11)
        for invalid in ('','unknown: 0','0x10000000000000000','-9223372036854775809','0x0 trailing'):
            with self.subTest(body=invalid),self.assertRaises(s.DiagnosticError):d.syscall_return(invalid)

    def test_bounded_root_containment_records_cleanup_timeout(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            (root/'root-group.json').write_text(json.dumps(dict(supervisor_pid=111,child_pid=1234567,pgid=1234567)))
            with patch.object(d.subprocess,'run',side_effect=subprocess.TimeoutExpired(['sudo','kill'],0.1)) as command:
                with self.assertRaises(s.DiagnosticError):d.contain_owned_root(root,d.Deadline())
            proof=json.loads((root/'root-containment.json').read_text())
            self.assertFalse(proof['qualified_cleanup']);self.assertTrue(proof['uncontained'])
            self.assertIn('timeout',proof['error'].lower())
            self.assertGreater(command.call_args.kwargs['timeout'],0)
            self.assertLessEqual(command.call_args.kwargs['timeout'],5)

    def test_root_containment_never_calls_with_expired_budget(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            (root/'root-group.json').write_text(json.dumps(dict(supervisor_pid=111,child_pid=1234567,pgid=1234567)))
            with patch.object(d.subprocess,'run') as command:
                with self.assertRaises(s.DiagnosticError):d.contain_owned_root(root,d.Deadline(1))
                command.assert_not_called()
            self.assertTrue((root/'root-containment.json').exists())

    def test_actual_privileged_executor_uses_bounded_local_containment(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);call=root/'call';call.mkdir()
            process=SimpleNamespace(pid=1234567,wait=None)
            waits=[subprocess.TimeoutExpired(['fixture'],0.1),-9]
            process.wait=lambda **kwargs: (_ for _ in ()).throw(waits.pop(0)) if isinstance(waits[0],Exception) else waits.pop(0)
            with patch.object(d.subprocess,'Popen',return_value=process),patch.object(d.legacy,'kill_group'),\
                 patch.object(d,'contain_owned_root',side_effect=s.DiagnosticError('cleanup deadline expired')) as bounded,\
                 patch.object(d.legacy,'contain_root',side_effect=AssertionError('unbounded donor cleanup forbidden')):
                with self.assertRaises(s.DiagnosticError):d.execute(['fixture'],{},call,root,d.Deadline(),True)
                bounded.assert_called_once()
            proof=json.loads((call/'exit.json').read_text())
            self.assertFalse(proof['outer_cleanup_complete']);self.assertIn('cleanup deadline expired',proof['cleanup_errors'])

    def test_measured_operation_totals_match_rounded_native_b1_rates(self):
        rows=d.parse_native(self.native())
        observed=[]
        for repetition,count in enumerate((3000,3001,3002)):
            rows[2*repetition]['ops_s']=round(count/3)
            rows[2*repetition]['records_s']=round(count/3)
            observed.append(dict(operations=[None]*count))
        d.validate_counter_rows(rows,observed)
        rows[2]['records_s']+=1
        with self.assertRaises(s.DiagnosticError):d.validate_counter_rows(rows,observed)
        rows[2]['records_s']-=1;rows[4]['ops_s']+=1
        with self.assertRaises(s.DiagnosticError):d.validate_counter_rows(rows,observed)

    def test_retained_linux_rows_have_six_metrics_and_three_load_values(self):
        # bounded-header-seek-2026-10-14/linux-head.md lines 141-142.
        b1='| stream_append/durable/1w/1k/b1 | 1 | 1 KiB | 3336 | 3.4 | 266.8 | 711.5 | 3730.4 | 3336 | 1.39 1.61 1.45 |'
        b100='| stream_append/durable/1w/1k/b100 | 1 | 1 KiB | 1873 | 191.8 | 499.9 | 955.8 | 4057.0 | 187267 | 1.36 1.60 1.45 |'
        text='\n'.join(['# wal_write_path',s.CONFIG_HEADER,s.WRITE_HEADER,s.STREAM_HEADER]+[b1,b100]*3)
        rows=d.parse_native(text)
        self.assertEqual(rows[0]['load1'],'1.39 1.61 1.45')
        self.assertEqual(rows[0]['load_averages'],[1.39,1.61,1.45])
        self.assertEqual(rows[1]['records_s'],187267)
        for malformed in ('0.1','1.39 1.61','1.39 1.61 1.45 2.0','1.39 nan 1.45','1.39 -1 1.45','n/a'):
            with self.subTest(load=malformed),self.assertRaises(s.DiagnosticError):
                d.parse_native(text.replace('1.39 1.61 1.45',malformed))

    def test_fixed_single_cycle(self):
        schedule = d.sequence()
        self.assertEqual(''.join(r['side'] for r in schedule[:8]), 'AABBABBA')
        self.assertEqual([(r['phase'], r['side']) for r in schedule[8:]],
                         [('marked','A'),('marked','B'),('syscalls','A'),('syscalls','B'),
                          ('scheduler','B'),('scheduler','A'),('cpu','A'),('cpu','B')])
        self.assertEqual(len(schedule), 16)
        self.assertTrue(all(not r['timing_eligible'] for r in schedule))

    def test_six_ordered_rows_b1_b100_each_repetition(self):
        rows = d.parse_native(self.native())
        self.assertEqual([r['cell'] for r in rows], list(d.CELLS) * 3)
        self.assertEqual([r['repetition'] for r in rows], [0,0,1,1,2,2])
        for bad in (self.native().replace('/b100', '/b1'), self.native()+'\n'+self.native(),
                    self.native().replace('3000 ms','3001 ms'), self.native().replace(' | 1 | 1 KiB',' | 2 | 1 KiB')):
            with self.subTest(bad=bad), self.assertRaises(s.DiagnosticError):
                d.parse_native(bad)

    def test_deadline_is_nonresetting_and_bounds_every_command(self):
        with patch.object(d.time, 'monotonic_ns', return_value=100):
            budget = d.Deadline(1000)
            self.assertAlmostEqual(budget.remaining(), 900 / 1e9)
            self.assertAlmostEqual(budget.timeout(), 900 / 1e9)
        with patch.object(d.time, 'monotonic_ns', return_value=1001):
            with self.assertRaises(s.DiagnosticError):
                budget.timeout()

    def test_runtime_pruned_before_any_observer_traversal(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp); (root/'runtime'/'deep').mkdir(parents=True)
            (root/'runtime'/'deep'/'wal').write_bytes(b'engine')
            (root/'events.bin').write_bytes(b'observer')
            seen=list(d.observer_files(root))
            self.assertEqual([p.name for p in seen], ['events.bin'])
            self.assertIsNone(d.budget_breach(root, root))
            with patch.object(d, 'OBSERVER_LIMIT', 7):
                self.assertIsNotNone(d.budget_breach(root, root))

    def test_independent_recorders_with_common_tracking(self):
        for category in ('cpu','syscalls','scheduler'):
            cmd=d.profile_command(category, '/tmp/wal-diag-A', '/tmp/run', '/tmp/proof', 'a'*64)
            self.assertEqual(cmd.count('record'), 1)
            for flag in ('--clockid','--sample-cpu','--timestamp','--switch-events'):
                self.assertIn(flag, cmd)
            for event in (*d.LIFECYCLE,d.RENAME):
                self.assertIn(event, cmd)
            self.assertNotIn('block:block_rq_issue', cmd)
            if category=='cpu':
                self.assertIn('dwarf,8192',cmd); self.assertIn('99',cmd)
                self.assertNotIn('syscalls:sys_enter_write',cmd)
            if category=='syscalls':
                self.assertNotIn('--call-graph',cmd)
                self.assertIn('syscalls:sys_exit_futex',cmd)
            if category=='scheduler':
                self.assertIn('-a',cmd)
                self.assertIn('sched:sched_switch',cmd)

    def test_categories_do_not_require_other_category_evidence(self):
        common,calls,cpu=traces()
        cpu_summary=d.analyze_category('cpu',common+cpu,'/tmp/wal-diag-base',10)
        self.assertEqual(cpu_summary['cpu_sample_count'],1)
        syscall_summary=d.analyze_category('syscalls',common+calls.replace('_read', '_write'),'/tmp/wal-diag-base',10)
        self.assertEqual(syscall_summary['pairs'][0]['name'],'write')
        scheduler=d.analyze_category('scheduler',common,'/tmp/wal-diag-base',10)
        self.assertIn(11,scheduler['thread_ids'])
        self.assertTrue(scheduler['name_coverage_complete'])

    def test_lifecycle_loss_missing_identity_fail_each_category(self):
        common,calls,cpu=traces()
        for category,body in [('cpu',cpu),('syscalls',calls),('scheduler','')]:
            for broken in (common.replace('PERF_RECORD_EXIT','BAD_EXIT')+body,
                           common.replace('filename=/tmp/wal-diag-base','filename=/wrong')+body,
                           common+body+'\nPERF_RECORD_LOST\n'):
                with self.subTest(category=category), self.assertRaises(s.DiagnosticError):
                    d.analyze_category(category,broken,'/tmp/wal-diag-base',10)

    def test_syscall_pairs_cross_markers_retained_and_status_required(self):
        common,_,_=traces()
        calls=sample('1.100000000','syscalls:sys_enter_write','fd: 3, count: 4')
        calls+=sample('1.200000000','syscalls:sys_exit_write','ret: -5')
        summary=d.analyze_category('syscalls',common+calls,'/tmp/wal-diag-base',10)
        self.assertEqual(summary['pairs'][0]['return'],-5)
        self.assertTrue(d.select_intervals(summary['pairs'],[1150000000,1250000000])[0]['crosses_marker'])
        for broken in (calls.splitlines()[0]+'\n', calls.replace('ret: -5','unknown: 0')):
            with self.assertRaises(s.DiagnosticError):
                d.analyze_category('syscalls',common+broken,'/tmp/wal-diag-base',10)

    def test_unsampled_unknown_thread_is_incomplete_not_zero(self):
        common,_,_=traces('unknown-native')
        with self.assertRaises(s.DiagnosticError):
            d.analyze_category('scheduler',common,'/tmp/wal-diag-base',10)

    def test_environment_has_fixed_envelope_and_marked_only_switch(self):
        env=d.environment('/tmp/runtime', {'phase':'original','side':'A'}, '/tmp/obs')
        self.assertEqual(env['SPIKE_FILTER'],d.CELLS[0])
        self.assertNotIn('PRKDB_STREAM_DURABLE_DIAGNOSTIC_DIR',env)
        marked=d.environment('/tmp/runtime', {'phase':'cpu','side':'B'}, '/tmp/obs')
        self.assertEqual(marked['PRKDB_STREAM_DURABLE_DIAGNOSTIC_SOURCE'],d.SOURCES['B'])
        self.assertEqual(marked['SPIKE_REPS'],'3')

    def metadata(self):
        return dict(schema_version=1, record_bytes=88, byte_order='little',clock='CLOCK_MONOTONIC',
                    clock_witness=dict(before_ns=1,anchor_ns=2,after_ns=3,uncertainty_ns=2),
                    source=d.SOURCES['A'],cell=d.CELLS[0],repetition=0,pid=10,
                    config=dict(writers=1,value_bytes=1024,batch=1,sync_mode='Durable',segment_bytes=268435456,
                                sync_interval_ms=10,warmup_ms=1000,measure_ms=3000),
                    phase_totals=dict(warmup=0,measurement=1),paths=['/tmp/log'],failed=False,complete=True,
                    failure=None,record_count=6,buffer_limit_bytes=268435456)

    def events(self):
        return [dict(kind=1,phase=1,operation_id=0,start_ns=100,append_start_ns=0,end_ns=100,tid=10,arg=0,amount=0,outcome=0,path_id=0),
                dict(kind=4,phase=1,operation_id=1,start_ns=120,append_start_ns=0,end_ns=130,tid=11,arg=0,amount=1024,outcome=0,path_id=1),
                dict(kind=5,phase=1,operation_id=1,start_ns=131,append_start_ns=0,end_ns=145,tid=11,arg=0,amount=0,outcome=0,path_id=1),
                dict(kind=3,phase=1,operation_id=1,start_ns=110,append_start_ns=115,end_ns=150,tid=10,arg=0,amount=1,outcome=0,path_id=0),
                dict(kind=7,phase=1,operation_id=1,start_ns=150,append_start_ns=0,end_ns=150,tid=12,arg=0,amount=0,outcome=0,path_id=0),
                dict(kind=2,phase=1,operation_id=0,start_ns=151,append_start_ns=0,end_ns=151,tid=10,arg=0,amount=1,outcome=0,path_id=0)]

    def test_marker_and_vfs_correspondence_is_interval_arithmetic(self):
        result=d.validate_observations(self.metadata(),self.events(),d.SOURCES['A'],0,10)
        self.assertEqual(result['operations'][0]['sync_ns'],14)
        self.assertEqual(result['operations'][0]['mixed_residual_ns'],11)
        self.assertEqual(result['operations'][0]['preparation_ns'],5)
        bad=self.events();bad.insert(2,copy.copy(bad[2]))
        result=d.validate_observations(dict(self.metadata(),record_count=7),bad,d.SOURCES['A'],0,10)
        self.assertFalse(result['operations'][0]['attribution_complete'])

    def test_incomplete_marker_identity_clock_or_breach_fails(self):
        for field,value in [('failed',True),('complete',False),('clock','wall'),('source','wrong'),('pid',11),('record_count',7)]:
            with self.subTest(field=field), self.assertRaises(s.DiagnosticError):
                d.validate_observations(dict(self.metadata(),**{field:value}),self.events(),d.SOURCES['A'],0,10)
        bad=self.events();bad[3]['append_start_ns']=151
        with self.assertRaises(s.DiagnosticError):
            d.validate_observations(self.metadata(),bad,d.SOURCES['A'],0,10)

    def test_binary_prefix_decoder_rejects_truncation(self):
        with tempfile.TemporaryDirectory() as tmp:
            p=Path(tmp)/'events.bin'; fields=d.EVENT_FIELDS
            p.write_bytes(b''.join(struct.pack('<9QqQ',*(e[k] for k in fields)) for e in self.events()))
            self.assertEqual(d.read_events(p),self.events())
            p.write_bytes(p.read_bytes()[:-1])
            with self.assertRaises(s.DiagnosticError): d.read_events(p)

    def test_return_tid_witness_required(self):
        bad=self.events();bad[4]['end_ns']=149
        with self.assertRaises(s.DiagnosticError):
            d.validate_observations(self.metadata(),bad,d.SOURCES['A'],0,10)

    def test_cpu_requires_native_readable_chain_and_unresolved_weight(self):
        quality=d.cpu_quality("# Samples: 1 of event 'cpu-clock:u'\n"+' 50.00% app [.] hook\n 50.00% [unknown] [.] 0xffff', sample('1.200000000','cpu-clock:u','7fff hook (app)'))
        self.assertEqual(quality['unresolved_weight_pct'],50)
        with self.assertRaises(s.DiagnosticError):
            d.cpu_quality("# Samples: 1 of event 'cpu-clock:u'\n"+' 100.00% app [.] hook', sample('1.200000000','cpu-clock:u','7fff [unknown] (app)'))

    def test_marked_observations_must_fit_qualified_native_lifetimes(self):
        rep=d.validate_observations(self.metadata(),self.events(),d.SOURCES['A'],0,10)
        trace=dict(category='cpu',window_ns=[90,160],thread_lifetimes_ns={'10':[90,160],'11':[90,160]},samples=[])
        with self.assertRaises(s.DiagnosticError): d.annotate_trace(trace,[rep])

    def test_sync_syscall_linkage_is_unique_and_not_guessed(self):
        rep=d.validate_observations(self.metadata(),self.events(),d.SOURCES['A'],0,10)
        trace=dict(category='syscalls',window_ns=[90,160],thread_lifetimes_ns={str(t):[90,160] for t in (10,11,12)},
                   pairs=[dict(name='pwrite64',tid=11,begin_ns=121,end_ns=129,return_value=1024,enter_detail='fd: 3, count: 1024, pos: 0'),
                          dict(name='fdatasync',tid=11,begin_ns=132,end_ns=144,return_value=0,enter_detail='fd: 3')])
        result=d.annotate_trace(trace,[rep])
        self.assertTrue(result['repetitions'][0]['operations'][0]['syscall_correspondence_complete'])
        trace['pairs'].append(copy.copy(trace['pairs'][1]))
        result=d.annotate_trace(trace,[rep])
        self.assertFalse(result['repetitions'][0]['operations'][0]['syscall_correspondence_complete'])

    def test_manifest_verification_rejects_changed_member(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);p=root/'binary';p.write_bytes(b'original');sha=s.digest(p)
            files={name:sha for name in d.OVERLAY_FILES}
            overlay=root/'overlay'
            for name in d.OVERLAY_FILES:
                f=overlay/name;f.parent.mkdir(parents=True,exist_ok=True);f.write_bytes(b'original')
            manifest=dict(schema_version=1,sources={side:dict(source_sha=source_sha,source_file_hashes={}) for side,source_sha in d.SOURCES.items()},
                          members={str(p):sha},overlay_root=str(overlay),overlay_files=files,binaries={},builds={})
            for variant in ('original','marked'):
                for side in d.SOURCES:
                    key=variant+'-'+side.lower()
                    manifest['binaries'][key]=dict(path=str(p),sha256=sha,runtime_libraries={str(p):sha})
                    manifest['builds'][key]=dict(source_file_hashes={} if variant=='original' else dict(files),
                       executable_sha256=sha,command=d.BUILD_COMMAND,feature_args=[],rustc_vv='release: 1.98.1\nhost: x86_64-unknown-linux-gnu',
                       environment={'CARGO_INCREMENTAL':'0','RUSTFLAGS':None})
            with self.assertRaises(s.DiagnosticError):d.verify_manifest(manifest)
            for change in ('source','overlay','hash','build'):
                broken=copy.deepcopy(manifest)
                if change=='source':broken['sources']['A']['source_sha']='0'*40
                if change=='overlay':broken['overlay_files'][d.OVERLAY_FILES[0]]='0'*64
                if change=='hash':broken['binaries']['original-a']['sha256']='0'*64
                if change=='build':broken['builds']['original-a']['source_file_hashes']={'production.rs':'0'*64}
                with self.subTest(change=change),self.assertRaises(s.DiagnosticError):d.verify_manifest(broken)
            p.write_bytes(b'changed')
            with self.assertRaises(s.DiagnosticError):d.verify_manifest(manifest)

    def test_command_failure_retains_exit_and_owned_cleanup(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);call=root/'call';call.mkdir()
            deadline=d.Deadline()
            with self.assertRaises(s.DiagnosticError):
                d.execute([os.sys.executable,'-c','raise SystemExit(7)'],{'PATH':os.environ.get('PATH','')},call,root,deadline)
            result=json.loads((call/'exit.json').read_text())
            self.assertEqual(result['exit_code'],7)
            self.assertTrue(result['outer_cleanup_complete'])
            self.assertTrue((call/'stdout.txt').exists())

    def test_timeout_contains_native_group_preserves_prefix(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);call=root/'call';call.mkdir()
            deadline=d.Deadline(d.time.monotonic_ns()+100_000_000)
            with self.assertRaises(s.DiagnosticError):
                d.execute([os.sys.executable,'-c','print("prefix",flush=True); import threading; threading.Event().wait()'],
                          {'PATH':os.environ.get('PATH','')},call,root,deadline)
            result=json.loads((call/'exit.json').read_text())
            self.assertTrue(result['outer_cleanup_complete'])
            self.assertIn('prefix',(call/'stdout.txt').read_text())

    def test_run_missing_manifest_keeps_failure_and_inventory(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);output=root/'output'
            with patch.object(d.sys,'platform','darwin'):
                with self.assertRaises(s.DiagnosticError):d.run(root/'missing',output)
            result=json.loads((output/'result.json').read_text())
            self.assertFalse(result['complete']);self.assertEqual(result['benchmark_invocations'],0)
            self.assertEqual(result['verdict'],'inconclusive')
            self.assertTrue((output/'inventory.json').exists())
            with self.assertRaises(s.DiagnosticError):d.run(root/'missing',output)

    def test_operation_deadline_preserves_finalization_reserve(self):
        with patch.object(d.time,'monotonic_ns',return_value=1_000_000_000):
            operation,final=d.run_deadlines()
        self.assertEqual(final.end_ns-operation.end_ns,60*10**9)
        self.assertEqual(operation.end_ns,1_000_000_000+1140*10**9)

    def test_final_inventory_size_is_checked_after_all_metadata_writes(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);result=dict(complete=True,verdict='pending review');runs=[]
            def oversize_inventory(output,deadline=None):
                (output/'inventory.json').write_bytes(b'x'*1024)
            with patch.object(d,'inventory',side_effect=oversize_inventory),patch.object(d,'RENDER_FILE',1000):
                with self.assertRaises(s.DiagnosticError):d.finalize_metadata(root,runs,result,d.Deadline())
            saved=json.loads((root/'result.json').read_text())
            self.assertFalse(saved['complete'])
            self.assertEqual(saved['verdict'],'inconclusive')

    def test_ownership_cleanup_is_contained_and_preserves_real_exit(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            with patch.object(d,'ownership_command',return_value=[os.sys.executable,'-c','raise SystemExit(7)']):
                with self.assertRaises(s.DiagnosticError):d.ownership_cleanup(root,d.Deadline())
            proof=json.loads((root/'ownership.json').read_text())
            self.assertEqual(proof['exit_code'],7)
            self.assertTrue(proof['outer_cleanup_complete'])
            self.assertTrue((root/'finalization'/'exit.json').exists())

    def test_empty_source_graph_and_text_binaries_cannot_reach_smoke(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);binary=root/'binary';binary.write_bytes(b'not ELF');overlay=root/'overlay'
            files={}
            for name in d.OVERLAY_FILES:
                p=overlay/name;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(b'overlay');files[name]=s.digest(p)
            manifest=dict(schema_version=1,overlay_root=str(overlay),overlay_files=files,members={str(binary):s.digest(binary)},
                          sources={side:dict(source_sha=sha,source_file_hashes={}) for side,sha in d.SOURCES.items()},binaries={},builds={})
            for variant in ('original','marked'):
                for side in d.SOURCES:
                    key=variant+'-'+side.lower()
                    manifest['binaries'][key]=dict(path=str(binary),sha256=s.digest(binary),runtime_libraries={})
                    manifest['builds'][key]=dict(source_file_hashes={} if variant=='original' else dict(files),
                       executable_sha256=s.digest(binary),command=d.BUILD_COMMAND,feature_args=[],rustc_vv='release: 1.98.1\nhost: x86_64-unknown-linux-gnu',
                       environment={'CARGO_INCREMENTAL':'0','RUSTFLAGS':None})
            p=root/'manifest.json';p.write_text(json.dumps(manifest))
            with patch.object(d.sys,'platform','darwin'),patch.object(d,'qualify_recorders') as qualify:
                with self.assertRaises(s.DiagnosticError):d.run(p,root/'output')
                qualify.assert_not_called()

    def test_measured_cpu_renders_exact_b1_windows_not_whole_process(self):
        commands=d.cpu_window_commands('/tmp/capture',10,[1_150_000_000,1_300_000_000])
        for command in commands.values():
            self.assertEqual(command[command.index('--time')+1],'1.150000000,1.300000000')
            self.assertEqual(command[command.index('--pid')+1],'10')
        no_samples=d.measured_cpu_quality("# Samples: 1 of event 'cpu-clock:u'\n"+' 100.00% app [.] outside_b1',sample('1.200000000','cpu-clock:u','7fff outside_b1 (app)'),0)
        self.assertFalse(no_samples['attribution_complete'])
        self.assertEqual(no_samples['verdict'],'inconclusive')
        missing_chain=d.measured_cpu_quality("# Samples: 1 of event 'cpu-clock:u'\n"+' 100.00% app [.] measured_hook',sample('1.200000000','cpu-clock:u','7fff [unknown] (app)'),1)
        self.assertFalse(missing_chain['attribution_complete'])

    def test_cpu_annotation_requires_per_repetition_measured_symbol_evidence(self):
        rep=d.validate_observations(self.metadata(),self.events(),d.SOURCES['A'],0,10)
        summary=dict(category='cpu',window_ns=[90,160],thread_lifetimes_ns={str(t):[90,160] for t in (10,11,12)},
                     samples=[dict(stamp=125,event='cpu-clock:u')],measured_cpu=[])
        with self.assertRaises(s.DiagnosticError):d.annotate_trace(summary,[rep])
        quality=d.measured_cpu_quality("# Samples: 1 of event 'cpu-clock:u'\n"+' 100.00% app [.] measured_hook',sample('1.200000000','cpu-clock:u','7fff measured_hook (app)'),1)
        summary['measured_cpu']=[dict(window_ns=rep['window_ns'],sample_count=1,**quality)]
        result=d.annotate_trace(summary,[rep])
        self.assertEqual(result['repetitions'][0]['symbol_rows'][0]['row'],' 100.00% app [.] measured_hook')
        self.assertTrue(result['repetitions'][0]['attribution_complete'])

    def test_failed_run_restores_donor_helper_functions(self):
        before_budget,before_supervise=d.legacy.budget_breach,d.legacy.supervise
        with tempfile.TemporaryDirectory() as tmp:
            with patch.object(d.sys,'platform','darwin'):
                with self.assertRaises(s.DiagnosticError):d.run(Path(tmp)/'missing',Path(tmp)/'output')
        self.assertIs(d.legacy.budget_breach,before_budget)
        self.assertIs(d.legacy.supervise,before_supervise)

    def test_failed_supervisor_restores_donor_helper_functions(self):
        before_budget,before_supervise=d.legacy.budget_breach,d.legacy.supervise
        with patch.object(d.legacy,'qualified_supervisor',side_effect=s.DiagnosticError('expected fixture failure')):
            with self.assertRaises(s.DiagnosticError):d.qualified_supervisor('proof','directory','output',['true'],d.Deadline())
        self.assertIs(d.legacy.budget_breach,before_budget)
        self.assertIs(d.legacy.supervise,before_supervise)

    def test_host_snapshots_include_competition_governor_device_and_runner(self):
        with tempfile.TemporaryDirectory() as tmp:
            with patch.object(d,'base_host_snapshot',return_value={'affinity':[0,1,2,3]}):
                snapshot=d.host_snapshot(tmp)
            self.assertIn('competing_processes',snapshot)
            self.assertIn('governors',snapshot)
            self.assertEqual(snapshot['temporary_device'],os.stat(tmp).st_dev)
            self.assertIn('runner_identity',snapshot)

    def test_snapshot_rejects_wrong_pinned_source_without_output(self):
        with tempfile.TemporaryDirectory() as tmp:
            output=Path(tmp)/'source.json'
            with patch.object(d,'git',return_value='0'*40):
                with self.assertRaises(s.DiagnosticError):d.snapshot_source(tmp,'A',output)
            self.assertFalse(output.exists())

    def test_budget_retains_raw_and_render_postexit_overshoot(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);p=root/'cpu.data'
            p.write_bytes(b'12345')
            with patch.dict(d.RAW_LIMITS,{'cpu.data':4}):self.assertIsNotNone(d.budget_breach(root,root))
            p.unlink();p=root/'stderr.txt';p.write_bytes(b'12345')
            with patch.object(d,'RENDER_FILE',4):self.assertIsNotNone(d.budget_breach(root,root))

    def test_marked_controls_report_observer_perturbation(self):
        runs=[]
        for planned in d.sequence()[:10]:
            rows=d.parse_native(self.native())
            if planned['phase']=='marked':
                for row in rows:row['ops_s']*=0.5;row['p99_us']*=2
            runs.append(dict(planned,rows=rows,exit_code=0))
        summary=d.summarize_controls(runs)
        self.assertEqual(summary['observer_controls'][d.CELLS[0]]['A']['ops_s_ratio'],0.5)
        self.assertEqual(summary['observer_controls'][d.CELLS[0]]['B']['mean_repetition_p99_ratio'],2)

    def provenance_fixture(self,root):
        sources={};snapshots={};roots={};overlay=root/'overlay';evidence=root/'evidence';evidence.mkdir()
        for name in d.OVERLAY_FILES:
            p=overlay/name;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(b'identical overlay '+name.encode())
        library=root/'libfixture.so';library.write_bytes(b'runtime library')
        for side in ('A','B'):
            tree=root/side;tree.mkdir();roots[side]=tree
            subprocess.run(['git','init','--initial-branch','fixture',str(tree)],check=True,capture_output=True)
            for name,content in {d.OVERLAY_FILES[0]:b'original bench','production.rs':side.encode(),'Cargo.toml':b'fixture','Cargo.lock':b'fixture lock'}.items():
                p=tree/name;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(content)
            subprocess.run(['git','-C',str(tree),'add','.'],check=True,capture_output=True)
            subprocess.run(['git','-C',str(tree),'-c','user.name=Fixture','-c','user.email=fixture@example.invalid',
                            '-c','commit.gpgsign=false','commit','-m','test: immutable source fixture'],check=True,capture_output=True)
            sources[side]=d.git(tree,'rev-parse','HEAD')
        with patch.dict(d.SOURCES,sources,clear=True):
            for side in sources:
                path=evidence/f'source-{side.lower()}.json';d.snapshot_source(roots[side],side,path)
                snapshots[side]=json.loads(path.read_text())
        args=dict(evidence=str(evidence),overlay=str(overlay),source_a=str(evidence/'source-a.json'),source_b=str(evidence/'source-b.json'),
                  output=str(evidence/'manifest.json'))
        elf=b'\x7fELF\x02\x01\x01'+b'\0'*11+b'\x3e\x00'+b'fixture'
        for variant in ('original','marked'):
            for side in sources:
                key=variant+'-'+side.lower();binary=evidence/key;binary.write_bytes(elf+key.encode());args[key.replace('-','_')]=str(binary)
                expected=dict(snapshots[side]['source_file_hashes'])
                if variant=='marked':expected.update({name:s.digest(overlay/name) for name in d.OVERLAY_FILES})
                artifact=dict(reason='compiler-artifact',executable=str(roots[side]/'target'/key),
                              target=dict(name='wal_write_path',kind=['bench'],crate_types=['bin'],test=False,src_path=str(roots[side]/d.OVERLAY_FILES[0])),
                              profile=dict(opt_level='3',debuginfo=0,debug_assertions=False,overflow_checks=False,test=True),features=[])
                (evidence/f'build-{key}.jsonl').write_text(json.dumps(artifact)+'\n'+json.dumps(dict(reason='build-finished',success=True))+'\n')
                (evidence/f'build-{key}.stderr').write_text('');(evidence/f'build-{key}.exit').write_text('0\n')
                proof=dict(command=d.BUILD_COMMAND,feature_args=[],rustc_vv='release: 1.98.1\nhost: x86_64-unknown-linux-gnu',cargo_v='cargo 1.98.1',
                           environment=dict(CARGO_INCREMENTAL='0',RUSTFLAGS=None,CARGO_TARGET_DIR=str(roots[side]/'target')),
                           source_root=str(roots[side]),source_file_hashes=expected,cargo_executable=artifact['executable'],executable_sha256=s.digest(binary))
                (evidence/f'build-{key}.proof.json').write_text(json.dumps(proof))
        for side in sources:
            for name in d.OVERLAY_FILES:
                p=roots[side]/name;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes((overlay/name).read_bytes())
        return sources,argparse.Namespace(**args),library

    def test_ownership_prunes_runtime_before_recursive_chown(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);(root/'runtime').mkdir();(root/'finalization').mkdir();(root/'cpu.data').write_bytes(b'raw')
            argv=d.ownership_targets(root,'1000:1000')
            self.assertNotIn(str(root/'runtime'),argv)
            self.assertIn(str(root/'cpu.data'),argv)
            self.assertIn(str(root/'finalization'),argv)

    def test_interval_analysis_obeys_nonresetting_operation_deadline(self):
        rep=d.validate_observations(self.metadata(),self.events(),d.SOURCES['A'],0,10)
        trace=dict(category='syscalls',window_ns=[90,160],thread_lifetimes_ns={str(t):[90,160] for t in (10,11,12)},pairs=[])
        with self.assertRaises(s.DiagnosticError):d.annotate_trace(trace,[rep],d.Deadline(1))

    def test_cpu_report_ignores_other_event_symbol_weights(self):
        report="# Samples: 2 of event 'cpu-clock:u'\n 50.00% app [.] hook\n 50.00% [unknown] [.] 0xffff\n# Samples: 1 of event 'sched:sched_process_fork'\n 100.00% [unknown] [.] 0xffff"
        quality=d.cpu_quality(report,sample('1.200000000','cpu-clock:u','7fff hook (app)'))
        self.assertEqual(quality['unresolved_weight_pct'],50)
        self.assertEqual(len(quality['symbol_rows']),2)

    def test_archived_cargo_harness_false_bench_artifact_contract(self):
        # Exact target/profile shape retained from the qualified Linux artifact:
        # actions-38374274969/artifacts/original/base-build.jsonl. Cargo profile
        # marks a bench as test=true while harness=false is target.test=false.
        with tempfile.TemporaryDirectory() as tmp:
            sources,args,library=self.provenance_fixture(Path(tmp))
            real_run=subprocess.run
            result=SimpleNamespace(returncode=0,stdout=f'libfixture => {library} (0x0)\n',stderr='')
            def fixture_run(argv,*pos,**kw):return result if argv[0]=='ldd' else real_run(argv,*pos,**kw)
            with patch.dict(d.SOURCES,sources,clear=True),patch.object(d.subprocess,'run',side_effect=fixture_run):
                d.build_manifest(args)
                manifest=json.loads(Path(args.output).read_text())
                self.assertTrue(manifest['builds']['original-a']['compiler_artifact']['profile']['test'])
                self.assertFalse(manifest['builds']['original-a']['compiler_artifact']['target']['test'])
                log=Path(args.evidence)/'build-original-a.jsonl';saved=log.read_text()
                messages=[json.loads(line) for line in saved.splitlines()]
                messages[0]['profile']['test']=False;log.write_text('\n'.join(json.dumps(m) for m in messages)+'\n')
                with self.assertRaises(s.DiagnosticError):d.verify_manifest(manifest)
                messages[0]['profile']['test']=True;messages[0]['target']['test']=True
                log.write_text('\n'.join(json.dumps(m) for m in messages)+'\n')
                with self.assertRaises(s.DiagnosticError):d.verify_manifest(manifest)

    def test_runtime_library_aliases_are_canonicalized_before_sealing(self):
        with tempfile.TemporaryDirectory() as tmp:
            sources,args,library=self.provenance_fixture(Path(tmp))
            alias=Path(tmp)/'loader-link.so';alias.symlink_to(library)
            result=SimpleNamespace(returncode=0,stdout=f'loader => {alias} (0x0)\n',stderr='')
            real_run=subprocess.run
            def fixture_run(argv,*pos,**kw):return result if argv[0]=='ldd' else real_run(argv,*pos,**kw)
            with patch.dict(d.SOURCES,sources,clear=True),patch.object(d.subprocess,'run',side_effect=fixture_run):
                d.build_manifest(args);manifest=json.loads(Path(args.output).read_text())
                self.assertEqual(set(manifest['binaries']['original-a']['runtime_libraries']),{str(library.resolve())})
                d.verify_seal(manifest,d.seal_manifest(manifest))

    def test_complete_provenance_graph_revalidated_then_sealed(self):
        with tempfile.TemporaryDirectory() as tmp:
            sources,args,library=self.provenance_fixture(Path(tmp))
            result=SimpleNamespace(returncode=0,stdout=f'libfixture => {library} (0x0)\n',stderr='')
            real_run=subprocess.run
            def fixture_run(argv,*pos,**kw):
                return result if argv[0]=='ldd' else real_run(argv,*pos,**kw)
            with patch.dict(d.SOURCES,sources,clear=True),patch.object(d.subprocess,'run',side_effect=fixture_run):
                d.build_manifest(args)
                manifest=json.loads(Path(args.output).read_text());d.verify_manifest(manifest)
                seal=d.seal_manifest(manifest);d.verify_seal(manifest,seal)
                broken=copy.deepcopy(manifest);broken['sources']['A']['source_file_hashes'].pop('production.rs')
                with self.assertRaises(s.DiagnosticError):d.verify_manifest(broken)
                source_path=Path(manifest['source_paths']['A']);saved=source_path.read_bytes()
                source=json.loads(saved);source['source_file_hashes'].pop('production.rs');source_path.write_text(json.dumps(source))
                with self.assertRaises(s.DiagnosticError):d.verify_manifest(manifest)
                source_path.write_bytes(saved)
                proof=Path(args.evidence)/'build-original-a.proof.json';saved=proof.read_bytes()
                fields=json.loads(saved);fields['executable_sha256']='0'*64;proof.write_text(json.dumps(fields))
                with self.assertRaises(s.DiagnosticError):d.verify_manifest(manifest)
                with self.assertRaises(s.DiagnosticError):d.verify_seal(manifest,seal)
                proof.write_bytes(saved)
                binary=Path(args.original_a);binary.write_bytes(b'not ELF')
                with self.assertRaises(s.DiagnosticError):d.verify_manifest(manifest)

if __name__=='__main__': unittest.main()
