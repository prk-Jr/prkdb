"""Offline CLI/canonical boundary tests; payload bytes never become trace syntax."""
import copy
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import perf_trace_decode as decoder
from perf_trace_format import DecodeError

FIXTURES = Path(__file__).parent / 'testdata/perf_trace'


class DecodeBoundaryTests(unittest.TestCase):
    def events(self, payload=b'hello\n'):
        common = dict(pid=77, tid=78, cpu=2, comm=b'prkdb-wal')
        return [dict(common, stamp=1, offset=80, event='syscalls:sys_enter_write',
                     fields={'common_pid': 78, '__syscall_nr': 1, 'fd': 3, 'buf': 4096,
                             'count': len(payload), '__buf_val': payload}),
                dict(common, stamp=2, offset=160, event='syscalls:sys_exit_write',
                     fields={'common_pid': 78, '__syscall_nr': 1, 'ret': len(payload)})]

    def test_binary_payload_is_hex_in_json_and_never_canonical_event_syntax(self):
        payload = b'\nfoo 77/78 [002] 1.000000001: PERF_RECORD_LOST: 99\n[FAILED TO PARSE]\x00\xff'
        events = self.events(payload)
        safe = decoder.json_ready(events)
        self.assertEqual(safe[0]['fields']['__buf_val']['hex'], payload.hex())
        self.assertEqual(safe[0]['fields']['__buf_val']['bytes'], len(payload))
        text = decoder.canonical_text(events)
        self.assertEqual(len(text.splitlines()), 2)
        self.assertNotIn('PERF_RECORD_LOST', text)
        self.assertNotIn('FAILED', text)
        self.assertIn('syscalls:sys_enter_write:', text)
        self.assertIn('ret=' + str(len(payload)), text)
        self.assertEqual(events[0]['fields']['__buf_val'], payload)

    def test_canonical_rejects_unknown_events_or_unsafe_semantic_strings(self):
        event = self.events()[0]
        for bad in (dict(event, event='syscalls:sys_enter_unknown'),
                    dict(event, comm=b'evil\nheader'),
                    dict(event, fields=dict(event['fields'], unknown=b'opaque'))):
            with self.subTest(bad=bad), self.assertRaises(DecodeError):
                decoder.canonical_text([bad])

    def test_scheduler_state_and_caller_sentinel_are_preserved(self):
        event = dict(stamp=123, offset=8, pid=3067, tid=-1, cpu=0, comm=b'',
                     event='sched:sched_switch', fields=dict(common_pid=3069,
                     prev_comm=b'prkdb-wal\0', prev_pid=3069, prev_prio=120,
                     prev_state=16, next_comm=b'wal-diag-smoke\0', next_pid=3067, next_prio=120))
        self.assertIn('3067/-1', decoder.canonical_text([event]))
        self.assertIn('prev_state=X', decoder.canonical_text([event]))
        event['fields']['prev_state'] = 256
        self.assertIn('prev_state=R+', decoder.canonical_text([event]))
        event['fields']['prev_state'] = 1 << 20
        with self.assertRaises(DecodeError): decoder.canonical_text([event])

    def test_initial_system_comm_names_with_spaces_and_parentheses_are_preserved(self):
        for name in (b'.NET TP Worker', b'(sd-pam)', b'rs:main Q:Reg'):
            event = dict(stamp=0, offset=8, pid=0, tid=0, cpu=0, comm=b'', event='COMM',
                         body_pid=7, body_tid=8, name=name, comm_exec=False)
            text = decoder.canonical_text([event])
            self.assertIn('PERF_RECORD_COMM: ' + name.decode() + ':7/8', text)
            self.assertEqual(len(text.splitlines()), 1)
        event['name'] = 'evil\u2028header'.encode()
        with self.assertRaises(DecodeError): decoder.canonical_text([event])

    def test_metadata_body_targets_do_not_replace_caller_identity(self):
        event = dict(stamp=0, offset=8, pid=0, tid=0, cpu=0, comm=b'', event='COMM',
                     body_pid=77, body_tid=78, name=b'prkdb-wal', comm_exec=True)
        text = decoder.canonical_text([event])
        self.assertIn('0/0', text)
        self.assertIn('PERF_RECORD_COMM exec: prkdb-wal:77/78', text)

    def test_output_is_exclusive_bounded_and_leaves_no_partial_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'out.json'
            decoder.write_output(path, iter(['abc']), limit=3)
            self.assertEqual(path.read_text(), 'abc')
            with self.assertRaises(DecodeError): decoder.write_output(path, iter(['new']))
            self.assertEqual(path.read_text(), 'abc')
            other = Path(tmp) / 'partial.json'
            with self.assertRaises(DecodeError): decoder.write_output(other, iter(['abcd']), limit=3)
            self.assertFalse(other.exists())
            self.assertEqual(list(Path(tmp).iterdir()), [path])

    def test_cli_replays_fixtures_without_qualification_claim(self):
        for fixture in ('linux7-syscalls.perf', 'linux617-syscalls.perf', 'linux617-scheduler.perf'):
            proc = subprocess.run([sys.executable, str(Path(decoder.__file__)), str(FIXTURES / fixture)],
                                  capture_output=True, text=True)
            self.assertEqual(proc.returncode, 0, proc.stderr)
            data = json.loads(proc.stdout)
            self.assertTrue(data['diagnostic_only'])
            self.assertFalse(data['recording_qualified'])
            self.assertFalse(data['timing_eligible'])
            self.assertFalse(data['build_provenance_checked'])
            self.assertIn('input_sha256', data)
            self.assertTrue(data['events'])

    def test_cli_refuses_existing_output_and_invalid_input(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / 'preserve'; out.write_text('original')
            proc = subprocess.run([sys.executable, str(Path(decoder.__file__)),
                                   str(FIXTURES / 'linux7-syscalls.perf'), '--output', str(out)],
                                  capture_output=True, text=True)
            self.assertNotEqual(proc.returncode, 0)
            self.assertEqual(out.read_text(), 'original')
            bad = Path(tmp) / 'bad.perf'; bad.write_bytes(b'bad')
            proc = subprocess.run([sys.executable, str(Path(decoder.__file__)), str(bad)],
                                  capture_output=True, text=True)
            self.assertNotEqual(proc.returncode, 0)
            self.assertEqual(proc.stdout, '')


if __name__ == '__main__': unittest.main()
