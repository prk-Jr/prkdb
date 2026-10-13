"""Fail-closed controls for the finite baseline invocation-envelope experiment."""
import json
import contextlib
import io
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch, MagicMock

import wal_stall_diagnostic as s
import wal_descriptor_diagnostic as d
import wal_workload_history as h


def fixture(kind='full'):
    writes = [(f'{k}/{w}w/{v}k', w, v) for _ in range(3) for v in (1, 64)
              for w in (1, 8, 64) for k in ('adapter_put', 'compaction_concurrent_put', 'model_memcpy_only', 'wal_durable', 'wal_fast')]
    streams = []
    for _ in range(3):
        streams += [(f'stream_append/{m}/{w}w/{v}k/b{b}', w, v) for v in (1, 64)
                    for w in (1, 64) for m in ('durable', 'fast') for b in (1, 100)]
        streams += [('stream_append_retention/durable/64w/1k/b1',64,1), ('stream_read/tail/1k',1,1), ('stream_read/cold/64k',1,64)]
    selected = {'write': 'adapter_put/8w/64k', 'cold': 'stream_read/cold/64k'}
    lines = ['# wal_write_path', s.CONFIG_HEADER]
    if kind == 'full':
        lines += ['- disk ceiling, pwrite 1 MiB, no sync: 100 MB/s (100 writes/s)',
                  '- disk ceiling, pwrite 1 MiB + sync_data: 10 MB/s (10 writes/s)',
                  '- device: pread 1 MiB, page cache: 100 MB/s',
                  '- device: pread 1 MiB, cold (fadvise DONTNEED per pass): 10 MB/s',
                  '- 4 KiB write + sync_data (F_FULLFSYNC on macOS): p50 10 µs, p99 20 µs (100 samples)',
                  '- 4 KiB write + plain fsync(2): p50 10 µs, p99 20 µs (100 samples)']
    for header, rows in ((s.WRITE_HEADER,writes),(s.STREAM_HEADER,streams)):
        lines += [header]
        for cell,w,v in rows:
            if kind != 'full' and cell != selected[kind]:
                continue
            extra = ' | 100' if header == s.STREAM_HEADER else ''
            lines += [f'| {cell} | {w} | {v} KiB | 100 | 100 | 10 | 20 | 30{extra} | 0.0 0.0 0.0 |']
    return '\n'.join(lines)+'\n'


def runs(ratio=1.5):
    out=[]
    for planned in h.schedule():
        rows=h.parse_output(fixture(planned['kind']),planned['kind'])['rows']
        if planned['kind']=='full':
            for row in rows:
                for key in ('ops_s','mb_s','p50_us','p99_us','p999_us'):
                    row[key]*=ratio
        out.append(dict(planned,rows=rows,exit_code=0,binary_sha256=s.PINNED['base']['binary_sha256'],timing_eligible=False))
    return out


class HistoryTests(unittest.TestCase):
    def test_schedule_is_fixed_latin_and_complete(self):
        self.assertEqual([x['kind'] for x in h.schedule()], ['full','write','cold','cold','full','write','write','cold','full'])
        self.assertEqual(sum(len(h.parse_output(fixture(x['kind']),x['kind'])['rows']) for x in h.schedule()),459)
        self.assertTrue(all(x['side']=='base' for x in h.schedule()))

    def test_full_inventory_and_separate_ceilings(self):
        result=h.parse_output(fixture(),'full')
        self.assertEqual(len(result['rows']),147)
        self.assertEqual(len(result['ceilings']),6)
        self.assertEqual(len({x['cell'] for x in result['rows']}),49)
        self.assertEqual(len(h.parse_output(fixture('cold'),'cold')['rows']),3)

    def test_bad_config_count_order_numbers_and_cache_fail(self):
        text=fixture();lines=text.splitlines(True)
        index=next(i for i,line in enumerate(lines) if line.startswith('| adapter_put'))
        swapped=lines[:];swapped[index],swapped[index+1]=swapped[index+1],swapped[index]
        for bad in (text.replace('reps 3','reps 1'), text+lines[index], ''.join(lines[:index]+lines[index+1:]),
                    ''.join(swapped),text.replace('| 100 | 100 |','| nan | 100 |',1),
                    text+'- stream_read/cold/64k: page cache could not be dropped here; equals tail\n'):
            with self.subTest(bad=bad[:30]),self.assertRaises(d.DiagnosticError):h.parse_output(bad,'full')

    def test_ceiling_missing_or_invalid_is_rejected(self):
        text=fixture()
        for bad in (text.replace('100 MB/s (100 writes/s)','0 MB/s (100 writes/s)',1),
                    '\n'.join(line for line in text.splitlines() if 'page cache:' not in line)):
            with self.assertRaises(d.DiagnosticError):h.parse_output(bad,'full')

    def test_classifier_requires_all_blocks_and_nonoverlap(self):
        result=h.summarize(runs())
        self.assertEqual(result['cells']['adapter_put/8w/64k']['ops_s']['classification'],'consistent larger grid values')
        self.assertEqual(h.summarize(runs(1.1))['cells']['adapter_put/8w/64k']['ops_s']['classification'],'no consistent >=20% envelope effect under this protocol')
        changed=runs();changed[-1]['rows']=h.parse_output(fixture(),'full')['rows']
        self.assertEqual(h.summarize(changed)['cells']['adapter_put/8w/64k']['ops_s']['classification'],'no consistent >=20% envelope effect under this protocol')
        changed=runs();next(x for x in changed[0]['rows'] if x['cell']=='adapter_put/8w/64k')['ops_s']=90
        self.assertEqual(h.summarize(changed)['cells']['adapter_put/8w/64k']['ops_s']['classification'],'no consistent >=20% envelope effect under this protocol')

    def test_incomplete_or_modified_schedule_is_not_qualified(self):
        values=runs()
        with self.assertRaises(d.DiagnosticError):h.summarize(values[:-1])
        values[0]['kind']='cold'
        with self.assertRaises(d.DiagnosticError):h.summarize(values)

    def test_budget_uses_remaining_cycle_without_reset(self):
        self.assertEqual(h.invocation_budget('full',1000,800),200)
        self.assertEqual(h.invocation_budget('cold',1000,800),120)
        with self.assertRaises(d.DiagnosticError):h.invocation_budget('full',1000,1000)

    def test_failing_host_preflight_launches_nothing_and_retains_result(self):
        with tempfile.TemporaryDirectory() as tmp:
            output=Path(tmp)/'new'
            with patch.object(h,'verify_artifact',return_value={}),patch.object(h,'qualify_host',side_effect=d.DiagnosticError('host proof refused')),patch.object(h,'execute') as execute:
                with self.assertRaises(d.DiagnosticError):h.run('/unused',output)
            execute.assert_not_called()
            self.assertEqual(json.loads((output/'result.json').read_text())['verdict'],'inconclusive')
            self.assertTrue((output/'SHA256SUMS').exists())
            with self.assertRaises(d.DiagnosticError):h.run('/unused',output)

    def mocked_run(self, output, after=None, expire=False, fail=False):
        calls=[];clock=[0]
        ordinary=d.resource_proof()
        artifact=output.parent/'artifact';artifact.mkdir(exist_ok=True)
        (artifact/'base-benchmark-executable').write_bytes(b'fixture')
        def launch(command,env,directory,owned,timeout):
            calls.append((command,env,timeout))
            if fail:raise d.DiagnosticError('primary native failure')
            kind='full' if not env['SPIKE_FILTER'] else 'write' if env['SPIKE_FILTER']==h.CELLS['write'] else 'cold'
            (directory/'stdout.txt').write_text(fixture(kind))
            if expire and len(calls)==9:clock[0]=2701
            return {'exit_code':0}
        with contextlib.ExitStack() as stack:
            for target,values in (
                    ('verify_artifact',{'return_value':{}}),
                    ('qualify_host',{'side_effect':lambda runtime: {'ordinary':ordinary,'runtime_device':os.stat(runtime).st_dev}}),
                    ('execute',{'side_effect':launch})):
                stack.enter_context(patch.object(h,target,**values))
            stack.enter_context(patch.object(h.time,'monotonic',side_effect=lambda:clock[0]))
            stack.enter_context(patch.object(d,'quiet'))
            stack.enter_context(patch.object(d,'proc_identity',return_value=ordinary))
            stack.enter_context(patch.object(d,'host_snapshot',return_value={'affinity':[0,1,2,3]}))
            stack.enter_context(patch.object(d,'validate_native_evidence',return_value={}))
            stack.enter_context(patch.object(d,'digest',return_value=s.PINNED['base']['binary_sha256']))
            stack.enter_context(patch.object(h.subprocess,'run',return_value=subprocess.CompletedProcess([],0,'linked','')))
            stack.enter_context(patch.object(h.s,'snapshot_after',return_value=after or {'captured':True}))
            stack.enter_context(contextlib.redirect_stdout(io.StringIO()))
            h.run(artifact,output)
        return calls

    def test_host_after_failure_stops_before_complete(self):
        with tempfile.TemporaryDirectory() as tmp:
            output=Path(tmp)/'output'
            with self.assertRaises(d.DiagnosticError):self.mocked_run(output,after={'capture_error':'missing metadata'})
            self.assertFalse(json.loads((output/'result.json').read_text()).get('complete',False))

    def test_final_processing_cannot_extend_aggregate_deadline(self):
        with tempfile.TemporaryDirectory() as tmp:
            output=Path(tmp)/'output'
            with self.assertRaises(d.DiagnosticError):self.mocked_run(output,expire=True)
            self.assertFalse(json.loads((output/'result.json').read_text()).get('complete',False))

    def test_complete_schedule_executes_only_baseline_with_fixed_configuration(self):
        with tempfile.TemporaryDirectory() as tmp:
            output=Path(tmp)/'output';calls=self.mocked_run(output)
            self.assertEqual(len(calls),9)
            self.assertTrue(all(str(output.resolve()/'wal-diag-base') in command and 'head' not in str(command) for command,env,budget in calls))
            self.assertEqual({env['TMPDIR'] for command,env,budget in calls},{str(output.resolve()/'runtime')})
            self.assertTrue(all(env['SPIKE_REPS']=='3' and env['TOKIO_WORKER_THREADS']=='4' for command,env,budget in calls))
            self.assertEqual(json.loads((output/'result.json').read_text())['cell_rows'],459)

    def test_primary_failure_survives_host_after_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(d.DiagnosticError,'primary native failure'):
                self.mocked_run(Path(tmp)/'output',after={'capture_error':'metadata'},fail=True)

    def test_timeout_retains_exit_and_contains_owned_process(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory=Path(tmp);process=MagicMock(pid=999999)
            process.wait.side_effect=[subprocess.TimeoutExpired(['native'],1),-9]
            with patch.object(h.subprocess,'Popen',return_value=process),patch.object(h.d,'CaptureWatch'),patch.object(h.d,'kill_group') as kill:
                with self.assertRaises(d.DiagnosticError):h.execute(['native'],{},directory,directory,1)
            kill.assert_called_once_with(999999)
            self.assertTrue(json.loads((directory/'exit.json').read_text())['timed_out'])

if __name__=='__main__':unittest.main()
