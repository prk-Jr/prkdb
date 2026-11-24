"""Bounded framed trace producer/consumer regressions."""
import copy
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

FIXTURES = Path(__file__).parent / 'testdata/perf_trace'

# Stable semantic snapshot from the unchanged donor analyzer and immutable Linux7 capture.
LINUX7_EXPECTED = {'native_pid': 3188,
 'thread_ids': [3188, 3190],
 'window_ns': [42992601312, 43405059680],
 'thread_lifetimes_ns': {'3188': [42992601312, 43405059680], '3190': [43046585559, 43398198784]},
 'thread_names': {'3188': ['wal-diag-smoke'],
                  '3190': ['wal-diag-smoke', 'tokio-smoke', 'prkdb-wal-smoke']},
 'pairs': [{'name': 'futex',
            'tid': 3188,
            'begin_ns': 42993300572,
            'end_ns': 42993302406,
            'return_value': 0},
           {'name': 'futex',
            'tid': 3188,
            'begin_ns': 43039036040,
            'end_ns': 43039039026,
            'return_value': 0},
           {'name': 'futex',
            'tid': 3188,
            'begin_ns': 43039058342,
            'end_ns': 43039059384,
            'return_value': 0},
           {'name': 'futex',
            'tid': 3188,
            'begin_ns': 43039062540,
            'end_ns': 43039063441,
            'return_value': 0},
           {'name': 'futex',
            'tid': 3188,
            'begin_ns': 43039067068,
            'end_ns': 43039067950,
            'return_value': 0},
           {'name': 'futex',
            'tid': 3188,
            'begin_ns': 43039068921,
            'end_ns': 43039069723,
            'return_value': 0},
           {'name': 'futex',
            'tid': 3188,
            'begin_ns': 43039078169,
            'end_ns': 43039079070,
            'return_value': 0},
           {'name': 'futex',
            'tid': 3188,
            'begin_ns': 43039139783,
            'end_ns': 43039140715,
            'return_value': 0},
           {'name': 'futex',
            'tid': 3188,
            'begin_ns': 43039155703,
            'end_ns': 43039156624,
            'return_value': 0},
           {'name': 'futex',
            'tid': 3188,
            'begin_ns': 43039463235,
            'end_ns': 43039464176,
            'return_value': 0},
           {'name': 'futex',
            'tid': 3188,
            'begin_ns': 43039465399,
            'end_ns': 43039466210,
            'return_value': 0},
           {'name': 'futex',
            'tid': 3188,
            'begin_ns': 43039467643,
            'end_ns': 43039468464,
            'return_value': 0},
           {'name': 'futex',
            'tid': 3188,
            'begin_ns': 43039648900,
            'end_ns': 43039649882,
            'return_value': 0},
           {'name': 'futex',
            'tid': 3188,
            'begin_ns': 43039771889,
            'end_ns': 43039772890,
            'return_value': 0},
           {'name': 'futex',
            'tid': 3188,
            'begin_ns': 43039779222,
            'end_ns': 43039780144,
            'return_value': 0},
           {'name': 'futex',
            'tid': 3188,
            'begin_ns': 43039782047,
            'end_ns': 43039782889,
            'return_value': 0},
           {'name': 'futex',
            'tid': 3188,
            'begin_ns': 43039784783,
            'end_ns': 43039785594,
            'return_value': 0},
           {'name': 'futex',
            'tid': 3188,
            'begin_ns': 43039803387,
            'end_ns': 43039804309,
            'return_value': 0},
           {'name': 'futex',
            'tid': 3190,
            'begin_ns': 43046690567,
            'end_ns': 43046696818,
            'return_value': 1},
           {'name': 'futex',
            'tid': 3188,
            'begin_ns': 43046608932,
            'end_ns': 43046712024,
            'return_value': 0},
           {'name': 'futex',
            'tid': 3190,
            'begin_ns': 43046718990,
            'end_ns': 43046722476,
            'return_value': 1},
           {'name': 'futex',
            'tid': 3188,
            'begin_ns': 43046715711,
            'end_ns': 43046732522,
            'return_value': 0},
           {'name': 'futex',
            'tid': 3188,
            'begin_ns': 43046734135,
            'end_ns': 43046735117,
            'return_value': 0},
           {'name': 'write',
            'tid': 3188,
            'begin_ns': 43046815757,
            'end_ns': 43046835984,
            'return_value': 4096},
           {'name': 'pwrite64',
            'tid': 3188,
            'begin_ns': 43046840102,
            'end_ns': 43046842497,
            'return_value': 1},
           {'name': 'fdatasync',
            'tid': 3188,
            'begin_ns': 43046852986,
            'end_ns': 43047673565,
            'return_value': 0},
           {'name': 'fsync',
            'tid': 3188,
            'begin_ns': 43047679135,
            'end_ns': 43047680748,
            'return_value': 0},
           {'name': 'futex',
            'tid': 3188,
            'begin_ns': 43397701307,
            'end_ns': 43397711255,
            'return_value': 1},
           {'name': 'futex',
            'tid': 3190,
            'begin_ns': 43046723909,
            'end_ns': 43397747675,
            'return_value': 0},
           {'name': 'write',
            'tid': 3190,
            'begin_ns': 43398033396,
            'end_ns': 43398059795,
            'return_value': 77},
           {'name': 'futex',
            'tid': 3190,
            'begin_ns': 43398135296,
            'end_ns': 43398144994,
            'return_value': 1},
           {'name': 'futex',
            'tid': 3188,
            'begin_ns': 43397733777,
            'end_ns': 43398179486,
            'return_value': 0},
           {'name': 'write',
            'tid': 3188,
            'begin_ns': 43398220252,
            'end_ns': 43398242574,
            'return_value': 39}]}

class ProtocolTests(unittest.TestCase):
    def integration(self):
        self.assertIsNotNone(importlib.util.find_spec('perf_trace_integration'), 'bounded integration producer is missing')
        import perf_trace_integration
        return perf_trace_integration

    def test_real_fixtures_roundtrip_complete_inventory(self):
        m=self.integration()
        for filename,category,count in [('linux7-syscalls.perf','syscalls',114),('linux617-syscalls.perf','syscalls',120),('linux617-scheduler.perf','scheduler',36)]:
            with self.subTest(filename=filename),tempfile.TemporaryDirectory() as tmp:
                doc=m.build_envelope(category,FIXTURES/filename)
                path=Path(tmp)/'envelope.json';path.write_text(json.dumps(doc))
                self.assertEqual(m.read_envelope(path,category,m.tooling_hashes()),doc)
                self.assertEqual(doc['sample_count'],count)
                self.assertEqual(sum(r['size'] for r in doc['records']),doc['input']['data_size'])
                self.assertFalse(doc['recording_qualified'])
                self.assertNotIn('__buf_val',doc['canonical']['text'])

    def test_malformed_protocol_and_identity_refused(self):
        m=self.integration();good=m.build_envelope('syscalls',FIXTURES/'linux7-syscalls.perf')
        mutations=[lambda d:d.update(version=True),lambda d:d.update(category='scheduler'),lambda d:d.update(sample_count=0),lambda d:d['canonical'].update(sha256='0'*64),lambda d:d['records'][0].update(offset=0),lambda d:d['cpu_map'].update(record_offset=0),lambda d:d['tooling'].update({'perf_trace_decode.py':'0'*64}),lambda d:d.update(recording_qualified=True),lambda d:d.update(extra=1),lambda d:d['event_counts'].update(COMM=True)]
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'envelope'
            for change in mutations:
                doc=copy.deepcopy(good);change(doc);path.write_text(json.dumps(doc))
                with self.subTest(change=change),self.assertRaises(m.DecodeError):m.read_envelope(path,'syscalls',m.tooling_hashes())
            for raw in (b'{"version":1,"version":1}',b'{"n":NaN}',b'\xff',b'[]'):
                path.write_bytes(raw)
                with self.assertRaises(m.DecodeError):m.read_envelope(path,'syscalls',m.tooling_hashes())
            path.write_text(json.dumps(good))
            with patch.object(m,'MAX_OUTPUT',10),self.assertRaises(m.DecodeError):m.read_envelope(path,'syscalls',m.tooling_hashes())

    def test_cli_and_verification_receipt_bind_same_input(self):
        m=self.integration();path=FIXTURES/'linux7-syscalls.perf'
        with tempfile.TemporaryDirectory() as tmp:
            output=Path(tmp)/'stdout'
            for verify in (False,True):
                result=subprocess.run([sys.executable,str(Path(m.__file__)),str(path),'--category','syscalls',*(['--verify-input'] if verify else [])],capture_output=True)
                self.assertEqual(result.returncode,0,result.stderr);output.write_bytes(result.stdout)
                if verify:m.read_receipt(output,'syscalls',m.tooling_hashes(),doc['input'])
                else:doc=m.read_envelope(output,'syscalls',m.tooling_hashes())
            with self.assertRaises(m.DecodeError):m.read_receipt(output,'syscalls',m.tooling_hashes(),dict(doc['input'],sha256='0'*64))
        with self.assertRaises(m.DecodeError):m.build_envelope('cpu',path)

    def test_binary_loss_and_truncation_refused(self):
        m=self.integration();import struct
        doc=m.build_envelope('syscalls',FIXTURES/'linux7-syscalls.perf');raw=(FIXTURES/'linux7-syscalls.perf').read_bytes()
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'bad.perf'
            for kind in (2,5,6,999):
                data=bytearray(raw);struct.pack_into('<I',data,doc['records'][0]['offset'],kind);path.write_bytes(data)
                with self.assertRaises(m.DecodeError):m.build_envelope('syscalls',path)
            path.write_bytes(raw[:-1])
            with self.assertRaises(m.DecodeError):m.build_envelope('syscalls',path)


    def test_producer_bounds_whole_envelope_before_returning(self):
        m=self.integration()
        doc=m.build_envelope('syscalls',FIXTURES/'linux7-syscalls.perf')
        # Canonical alone fits; the inventory pushes total JSON over the cap.
        limit=len(doc['canonical']['text'].encode())+1
        with patch.object(m,'MAX_OUTPUT',limit),self.assertRaises(m.DecodeError):
            m.build_envelope('syscalls',FIXTURES/'linux7-syscalls.perf')


    def test_json_preallocation_size_matches_encoder_at_escape_boundaries(self):
        m=self.integration()
        for code in [*range(256),0x7ff,0x800,0xffff,0x10000,0x10ffff]:
            doc={'value':chr(code)}
            expected=len(json.dumps(doc,ensure_ascii=True,separators=(',',':')).encode())
            with self.subTest(code=code):self.assertEqual(m.enforce_json_cap(doc),expected)
        doc={'value':chr(127)}
        with patch.object(m,'MAX_OUTPUT',17),self.assertRaises(m.DecodeError):m.enforce_json_cap(doc)


class RunnerTests(unittest.TestCase):
    def replay(self, filename='linux7-syscalls.perf', *, category=None, mutate=None,
               attrs=None, stderr='', affinity=None, raw=None, root_hook=None, text=None, launch=None, existing=None):
        import stream_durable_diagnostic as d
        import perf_trace_integration as m
        from test_stream_durable_diagnostic import DurableDiagnosticTests
        category=category or ('scheduler' if 'scheduler' in filename else 'syscalls')
        fixture=FIXTURES/filename
        decoded=m.build_envelope(category,fixture)
        import perf_trace_records
        events=perf_trace_records.decode_file(fixture)['events']
        executable=next(e for e in events if e['event']=='sched:sched_process_exec' and b'wal-diag-smoke' in e['fields']['filename'])
        binary=executable['fields']['filename'].decode().split('\0')[0]
        native_pid=executable['fields']['pid']
        self.last_commands=[]
        with tempfile.TemporaryDirectory() as tmp:
            output=Path(tmp);directory=output/'category';directory.mkdir()
            (directory/f'{category}.data').write_bytes(raw if raw is not None else fixture.read_bytes())
            (directory/'stderr.txt').write_text(stderr)
            for name,value in (existing or {}).items():(directory/name).write_text(value)
            cpus=list(range(decoded['cpu_map']['first'],decoded['cpu_map']['last']+1))
            (output/'preflight.json').write_text(json.dumps({'host':{'affinity':cpus if affinity is None else affinity}}))
            attribute_text=attrs if attrs is not None else '\n'.join(DurableDiagnosticTests().attributes(category))
            def root(command,env,child,ordinary_path,out,deadline):
                self.last_commands.append(command)
                if command[0]=='perf':
                    self.assertEqual(command[command.index('-i')+1],str(directory/f'{category}.data'))
                    if command[1]=='evlist':value=attribute_text
                    elif '-D' in command:
                        value=f"PERF_RECORD_THREAD_MAP nr: 1 thread: -1\nPERF_RECORD_CPU_MAP: {cpus[0]}-{cpus[-1]}\n"
                    else:
                        # Model perf's physical-line rendering using actual opaque
                        # fixture bytes. The original consumer reaches its frozen
                        # parser and rejects the payload's continuation lines.
                        value=decoded['canonical']['text']
                        if filename=='linux7-syscalls.perf':
                            payload=next(e['fields']['__buf_val'] for e in events if b'\n' in e.get('fields',{}).get('__buf_val',b''))
                            lines=value.splitlines(keepends=True)
                            index=next(i for i,line in enumerate(lines) if 'syscalls:sys_enter_write:' in line)
                            lines[index]=lines[index].rstrip('\n')+' __buf_val="'+payload.decode()+'"\n'
                            value=''.join(lines)
                    return [sys.executable,'-c','import sys;sys.stdout.write('+repr(value)+')']
                self.assertEqual(command[1],'-B')
                self.assertIn('perf_trace_integration.py',command[2])
                self.assertEqual(command[3],str(directory/f'{category}.data'))
                if root_hook:return root_hook(command,env,child,out,deadline)
                return command
            execute=d.execute
            def supervised(command,env,child,out,deadline,privileged=False):
                result=execute(command,env,child,out,deadline,privileged)
                if mutate and Path(child).name=='render-decode':
                    path=Path(child)/'stdout.txt';doc=json.loads(path.read_text());mutate(doc)
                    path.write_text(json.dumps(doc))
                return result
            with patch.object(d,'root_command',side_effect=root),patch.object(d,'execute',side_effect=supervised),patch.object(d.legacy,'validate_native_evidence',return_value=launch or dict(binary=binary,pid=native_pid)):
                try:
                    summary=d.render_category(category,directory,output,'proof',d.Deadline(),{})
                except Exception:
                    for name in ('canonical.txt','decode-proof.json','trace-quality.json'):
                        if name in (existing or {}):self.assertEqual((directory/name).read_text(),existing[name])
                        else:self.assertFalse((directory/name).exists())
                    raise
                self.assertTrue((directory/'trace-quality.json').exists())
                self.assertTrue((directory/'canonical.txt').exists())
                self.assertTrue((directory/'decode-proof.json').exists())
                return summary,decoded

    def test_real_consumer_fixture_semantic_goldens(self):
        for filename,pairs in [('linux7-syscalls.perf',33),('linux617-syscalls.perf',36),('linux617-scheduler.perf',None)]:
            with self.subTest(filename=filename):
                summary,doc=self.replay(filename)
                self.assertIn('--verify-input',self.last_commands[-1])
                self.assertEqual(len(self.last_commands),3)
                expected_path=FIXTURES/(filename+'.expected.json')
                if filename=='linux7-syscalls.perf' or expected_path.exists():
                    expected=LINUX7_EXPECTED if filename=='linux7-syscalls.perf' else json.loads(expected_path.read_text())
                    for field in ('native_pid','thread_ids','window_ns','thread_lifetimes_ns','thread_names'):
                        self.assertEqual(summary[field],expected[field])
                    if pairs:
                        fields=('name','tid','begin_ns','end_ns','return_value')
                        self.assertEqual([{k:p[k] for k in fields} for p in summary['pairs']],expected['pairs'])
                    else:
                        self.assertEqual(summary['offcpu'],expected['offcpu']);self.assertEqual(summary['wakeup_to_run'],expected['wakeup_to_run'])
                if pairs:self.assertEqual(len(summary['pairs']),pairs)
                else:self.assertEqual((len(summary['offcpu']),len(summary['wakeup_to_run'])),(8,7))

    def test_actual_consumer_refuses_protocol_attributes_stderr_affinity(self):
        import stream_durable_diagnostic as d
        import perf_trace_integration as m
        for mutate in [lambda v:v.update(version=2),lambda v:v.update(category='scheduler'),lambda v:v.update(sample_count=0),lambda v:v['canonical'].update(sha256='0'*64),lambda v:v['cpu_map'].update(first=2)]:
            with self.subTest(mutate=mutate),self.assertRaises((d.DiagnosticError,m.DecodeError)):self.replay(mutate=mutate)
        for args in (dict(attrs='bad'),dict(stderr='PERF_RECORD_LOST'),dict(filename='linux617-scheduler.perf',affinity=[0])):
            with self.subTest(args=args),self.assertRaises((d.DiagnosticError,m.DecodeError)):self.replay(**args)

    def test_twelve_original_semantic_negative_controls_through_consumer(self):
        import stream_durable_diagnostic as d
        import perf_trace_integration as m
        from test_wal_descriptor_diagnostic import traces
        from test_stream_durable_diagnostic import DurableDiagnosticTests
        import hashlib
        # Preserve the original lifecycle/identity/loss matrix on all categories.
        controls=0
        for category in ('syscalls','scheduler','cpu'):
            if category=='cpu':
                common,_,cpu=traces()
                for bad in (common.replace('PERF_RECORD_EXIT','BAD_EXIT')+cpu,
                            common.replace('filename=/tmp/wal-diag-base','filename=/wrong')+cpu,
                            common+cpu+'\nPERF_RECORD_LOST\n'):
                    controls+=1
                    with self.subTest(category=category,bad=bad),tempfile.TemporaryDirectory() as tmp:
                        root=Path(tmp);(root/'stderr.txt').write_text('')
                        attributes='\n'.join(DurableDiagnosticTests().attributes('cpu'))
                        def boundary(command,*args):
                            value=attributes if command[1]=='evlist' else '' if '-D' in command else bad
                            return [sys.executable,'-c','print('+repr(value)+')']
                        with patch.object(d,'root_command',side_effect=boundary),patch.object(d.legacy,'validate_native_evidence',return_value={'binary':'/tmp/wal-diag-base','pid':10}),patch.object(m,'tooling_hashes',side_effect=AssertionError('CPU must not load framed integration')),self.assertRaises(d.DiagnosticError):
                            d.render_category('cpu',root,root,'proof',d.Deadline(),{})
                continue
            filename='linux617-'+category+'.perf'
            for mode in ('lifecycle','identity','loss'):
                controls+=1
                def mutate(doc):
                    text=doc['canonical']['text']
                    if mode=='lifecycle':text=text.replace('PERF_RECORD_EXIT(', 'PERF_RECORD_EXIT(999')
                    elif mode=='identity':text=text.replace('filename=/home/runner/work/_temp/recorder-smoke-evidence/run/wal-diag-smoke','filename=/wrong')
                    else:text+='\nPERF_RECORD_LOST\n'
                    doc['canonical']={'text':text,'sha256':hashlib.sha256(text.encode()).hexdigest()}
                with self.subTest(category=category,mode=mode),self.assertRaises((d.DiagnosticError,m.DecodeError)):self.replay(filename,mutate=mutate)
        for mode in ('unfinished','return','unknown-thread'):
            controls+=1
            def mutate(doc):
                text=doc['canonical']['text']
                if mode=='unfinished':
                    # Preserve event/sample inventory while breaking matching.
                    text=text.replace('syscalls:sys_exit_write:', 'syscalls:sys_enter_write:')
                    counts=doc['event_counts'];counts['syscalls:sys_enter_write']+=counts.pop('syscalls:sys_exit_write')
                elif mode=='return':text=text.replace('ret=', 'unknown=')
                else:
                    import re
                    # Give a native lifetime an uncovered name while removing
                    # all scheduler body references to that TID. It remains an
                    # unsampled native thread, rather than a zero-cost claim.
                    text=text.replace('prkdb-wal-smoke','unknown-native')
                    lines=[]
                    for line in text.splitlines(keepends=True):
                        if 'sched:sched_switch:' in line or 'sched:sched_wakeup:' in line:
                            line=re.sub(r'(?<![A-Za-z_])(prev_pid|next_pid|pid)=3069\b',r'\1=9999',line)
                        lines.append(line)
                    text=''.join(lines)
                doc['canonical']={'text':text,'sha256':hashlib.sha256(text.encode()).hexdigest()}
            filename='linux617-scheduler.perf' if mode=='unknown-thread' else 'linux617-syscalls.perf'
            with self.subTest(mode=mode),self.assertRaises((d.DiagnosticError,m.DecodeError)):self.replay(filename,mutate=mutate)
        self.assertEqual(controls,12)

    def test_missing_nonzero_helper_and_decode_failures_preserve_prefix(self):
        import stream_durable_diagnostic as d
        import perf_trace_integration as m
        for command in ([sys.executable,'-B','/nonexistent/perf_trace_integration.py'],[sys.executable,'-c','raise SystemExit(7)']):
            with self.subTest(command=command),self.assertRaises(d.DiagnosticError):self.replay(root_hook=lambda *args:command)
        raw=(FIXTURES/'linux7-syscalls.perf').read_bytes()
        with self.assertRaises(d.DiagnosticError):self.replay(raw=raw[:-1])
        doc=m.build_envelope('syscalls',FIXTURES/'linux7-syscalls.perf')
        import struct
        data=bytearray(raw);struct.pack_into('<I',data,doc['records'][0]['offset'],2)
        with self.assertRaises(d.DiagnosticError):self.replay(raw=data)

    def test_raw_and_tooling_drift_around_attributes_refused(self):
        import stream_durable_diagnostic as d
        import perf_trace_integration as m
        def boundary(command,*args):
            if '--verify-input' in command:
                raw=Path(command[3]);data=bytearray(raw.read_bytes());data[-1]^=1;raw.write_bytes(data)
            return command
        with self.assertRaises((d.DiagnosticError,m.DecodeError)):self.replay(root_hook=boundary)
        original=m.tooling_hashes
        calls=[]
        def hashes():
            calls.append(1);value=original()
            if len(calls)>3:value['perf_trace_integration.py']='0'*64
            return value
        with patch.object(m,'tooling_hashes',side_effect=hashes),self.assertRaises((d.DiagnosticError,m.DecodeError)):self.replay()

    def test_opaque_payload_cannot_fabricate_loss_events_or_attachment(self):
        import perf_trace_records
        raw=(FIXTURES/'linux7-syscalls.perf').read_bytes()
        events=perf_trace_records.decode_file(FIXTURES/'linux7-syscalls.perf')['events']
        payload=next(e['fields']['__buf_val'] for e in events if len(e.get('fields',{}).get('__buf_val',b''))>60)
        offset=raw.find(payload);self.assertGreaterEqual(offset,0)
        fakes=[b'\nPERF_RECORD_LOST\n',b'\nPERF_RECORD_THREAD_MAP nr: 1 thread: -1\n',
               b'\nPERF_RECORD_CPU_MAP: 0-4095\n',
               b'\nx 1/1 [000] 1.000000000: PERF_RECORD_EXIT(1:1):(1:1)\n']
        before,_=self.replay()
        for fake in fakes:
            replacement=(fake+b'X'*len(payload))[:len(payload)]
            changed=raw[:offset]+replacement+raw[offset+len(payload):]
            after,_=self.replay(raw=changed)
            self.assertEqual(before,after)
        self.assertEqual((FIXTURES/'linux7-syscalls.perf').read_bytes(),raw)

    def test_malformed_oversized_stdout_and_helper_loss_diagnostics(self):
        import stream_durable_diagnostic as d
        import perf_trace_integration as m
        for body in ('not-json','{"version":1,"version":1}','{"n":NaN}', 'x'*200001):
            command=[sys.executable,'-c','import sys;sys.stdout.write('+repr(body)+')']
            with self.subTest(body=body[:40]),patch.object(m,'MAX_OUTPUT',200000),self.assertRaises((d.DiagnosticError,m.DecodeError)):
                self.replay(root_hook=lambda *args:command)
        def loss(command,*args):
            # Run the actual helper and retain valid stdout with bad stderr.
            return [sys.executable,'-c','import subprocess,sys; r=subprocess.run(sys.argv[1:],stdout=sys.stdout);sys.stderr.write("PERF_RECORD_LOST\\n");sys.exit(r.returncode)',*command]
        with self.assertRaises(d.DiagnosticError):self.replay(root_hook=loss)

    def test_binary_clock_layout_and_cpu_identity_fail_actual_consumer(self):
        import stream_durable_diagnostic as d
        from test_perf_trace_format import feature
        import struct
        original=(FIXTURES/'linux7-syscalls.perf').read_bytes()
        clock,_=feature(original,29)
        mutations=[(clock,'<I',2),(2792+32,'<Q',3),(15912+40,'<I',1),(15912+60,'<H',65535)]
        for offset,fmt,value in mutations:
            raw=bytearray(original);struct.pack_into(fmt,raw,offset,value)
            with self.subTest(offset=offset),self.assertRaises(d.DiagnosticError):self.replay(raw=raw)


    def test_exclusive_artifacts_and_partial_write_cleanup(self):
        import perf_trace_integration as m
        for name in ('canonical.txt','decode-proof.json','trace-quality.json'):
            with self.subTest(name=name),self.assertRaises(m.DecodeError):
                self.replay(existing={name:'original evidence'})
        original=m.write_output
        def fail_proof(path,chunks,**kwargs):
            if Path(path).name=='decode-proof.json':raise OSError('injected artifact write failure')
            return original(path,chunks,**kwargs)
        with patch.object(m,'write_output',side_effect=fail_proof),self.assertRaises(OSError):self.replay()


    def test_final_artifacts_still_obey_aggregate_budget(self):
        import stream_durable_diagnostic as d
        original=d.budget_breach
        def budget(directory,output):
            if (Path(directory).parent/'canonical.txt').exists():return 'rendered aggregate cap'
            if (Path(directory)/'canonical.txt').exists():return 'rendered aggregate cap'
            return original(directory,output)
        with patch.object(d,'budget_breach',side_effect=budget),self.assertRaises(d.DiagnosticError):self.replay()


if __name__=='__main__':unittest.main()
