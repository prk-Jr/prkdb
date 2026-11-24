"""Offline record framing and typed-value regressions for retained recordings."""
import collections
import hashlib
import random
from pathlib import Path
import struct
import tempfile
import unittest

import perf_trace_records as records
from perf_trace_format import DecodeError

FIXTURES = Path(__file__).parent / 'testdata' / 'perf_trace'


def inventory(data):
    offset, size = struct.unpack_from('<QQ', data, 40)
    end = offset + size
    result = []
    while offset < end:
        kind, misc, length = struct.unpack_from('<IHH', data, offset)
        result.append((offset, kind, misc, length))
        offset += length
    return result


class RecordsTests(unittest.TestCase):
    def decode(self, name):
        return records.decode_file(FIXTURES / name)

    def reject_mutation(self, name, mutate):
        data = bytearray((FIXTURES / name).read_bytes())
        mutate(data)
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'bad.perf'
            path.write_bytes(data)
            with self.assertRaises(DecodeError):
                records.decode_file(path)

    def test_real_complete_inventories_and_order(self):
        expected = {
            'linux7-syscalls.perf': (114, {69:1,10:95,78:14,73:1,74:1,17:21,18:21,3:6,82:1,14:27,9:114,4:2,7:1,68:1}),
            'linux617-syscalls.perf': (120, {69:1,10:92,78:14,73:1,74:1,17:18,18:18,3:6,82:1,14:23,9:120,4:2,7:1,68:1}),
            'linux617-scheduler.perf': (36, {69:1,10:1874,78:7,73:1,74:1,17:18,18:18,7:255,3:259,82:1,15:904,68:2,9:36,4:2}),
        }
        for name, (samples, counts) in expected.items():
            with self.subTest(name=name):
                decoded = self.decode(name)
                data = (FIXTURES / name).read_bytes()
                self.assertEqual(decoded['input_sha256'], hashlib.sha256(data).hexdigest())
                self.assertEqual(decoded['sample_count'], samples)
                self.assertEqual(collections.Counter(row['type'] for row in decoded['records']), counts)
                self.assertEqual([(r['offset'], r['type'], r['misc'], r['size']) for r in decoded['records']], inventory(data))
                for row in decoded['records']:
                    self.assertEqual(row['sha256'], hashlib.sha256(data[row['offset']:row['offset']+row['size']]).hexdigest())
                keys = [(event['stamp'], event['offset']) for event in decoded['events']]
                self.assertEqual(keys, sorted(keys))

    def test_real_typed_samples_and_opaque_linux7_payload(self):
        for name, native in [('linux7-syscalls.perf',3188), ('linux617-syscalls.perf',3037)]:
            result = self.decode(name)
            samples = [e for e in result['events'] if e['kind'] == 'sample']
            self.assertTrue(all(e['pid'] == native for e in samples))
            exits = [e for e in samples if e['event'].startswith('syscalls:sys_exit_')]
            self.assertTrue(exits)
            self.assertTrue(all(isinstance(e['fields']['ret'], int) for e in exits))
            if native == 3188:
                writes = [e for e in samples if e['event'] == 'syscalls:sys_enter_write']
                payloads = [value for e in writes for value in e['fields'].values() if isinstance(value, bytes)]
                self.assertTrue(any(b'\n' in value for value in payloads))

    def test_signed_syscall_return_mutation(self):
        from perf_trace_format import load_file
        source = FIXTURES / 'linux7-syscalls.perf'
        perf = load_file(source)
        data = bytearray(perf.data)
        found = False
        for offset,kind,misc,size in inventory(data):
            if kind != 9:
                continue
            identity, = struct.unpack_from('<Q',data,offset+32)
            attr = perf.ids[identity]
            if attr.name.startswith('syscalls:sys_exit_'):
                field = next(f for f in perf.schemas[attr.config].fields if f.name == 'ret')
                struct.pack_into('<q',data,offset+60+field.offset,-13)
                found = True
                break
        self.assertTrue(found)
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)/'signed.perf'
            path.write_bytes(data)
            event = next(e for e in records.decode_file(path)['events'] if e['offset']==offset)
        self.assertEqual(event['fields']['ret'],-13)

    def test_scheduler_sentinel_is_not_raw_common_pid(self):
        event = next(e for e in self.decode('linux617-scheduler.perf')['events'] if e['offset'] == 338280)
        self.assertEqual((event['pid'],event['tid'],event['fields']['common_pid']), (3067,-1,3069))
        switches = [e for e in self.decode('linux617-scheduler.perf')['events'] if e['event'] == 'sched:sched_switch']
        self.assertTrue(all(isinstance(e['fields']['prev_state'], int) for e in switches))

    def test_body_target_and_trailer_caller_are_separate(self):
        result = self.decode('linux7-syscalls.perf')
        fork = next(e for e in result['events'] if e['kind'] == 'fork')
        self.assertEqual((fork['pid'],fork['tid']), (3188,3188))
        self.assertEqual((fork['body_pid'],fork['body_tid']), (3188,3190))
        self.assertEqual(fork['stamp'],fork['trailer']['stamp'])
        self.assertNotEqual(fork['stamp'],fork['body']['time'])
        initial = [e for e in result['events'] if e['kind']=='comm' and e['stamp']==0]
        self.assertTrue(initial)
        self.assertFalse(any(e.get('timed_identity',False) for e in initial))

    def test_rejects_unsupported_and_loss_record_types(self):
        for kind in (1,2,5,6,11,12,13,19,81,65535):
            with self.subTest(kind=kind):
                self.reject_mutation('linux7-syscalls.perf', lambda d: struct.pack_into('<I', d, 2792, kind))

    def test_rejects_record_framing(self):
        for length in (0,7,65535):
            self.reject_mutation('linux7-syscalls.perf',lambda d: struct.pack_into('<H',d,2798,length))
        self.reject_mutation('linux7-syscalls.perf',lambda d: struct.pack_into('<Q',d,48,32407))

    def test_rejects_sample_id_and_common_type_disagreement(self):
        self.reject_mutation('linux7-syscalls.perf',lambda d: struct.pack_into('<Q',d,15912+32,2**63))
        self.reject_mutation('linux7-syscalls.perf',lambda d: struct.pack_into('<H',d,15912+60,65535))
        self.reject_mutation('linux7-syscalls.perf',lambda d: struct.pack_into('<i',d,15912+64,1))

    def test_rejects_metadata_variants_reserved_and_trailer_ids(self):
        for offset, fmt, value in [(11648+8,'<Q',4),(12136+8,'<H',1),(2792+16,'<Q',999999),(15472+24,'<Q',999999),(15472+36,'<I',1)]:
            with self.subTest(offset=offset):
                self.reject_mutation('linux7-syscalls.perf',lambda d: struct.pack_into(fmt,d,offset,value))

    def test_payload_event_and_loss_text_stays_opaque(self):
        data = bytearray((FIXTURES / 'linux7-syscalls.perf').read_bytes())
        # This fixture's first dynamic write buffer is exactly 63 bytes at RAW+44.
        payload = b'\\nPERF_RECORD_LOST lost 99 events\\nfake 1/1 [000] 1.000: x: '
        payload = payload.replace(b'\\n', b'\n').ljust(63,b'x')
        self.assertEqual(len(payload),63)
        data[18280+60+44:18280+60+44+63] = payload
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'payload.perf'
            path.write_bytes(data)
            result = records.decode_file(path)
        self.assertEqual(result['sample_count'],114)
        self.assertEqual(len(result['records']),len(inventory(data)))
        event = next(e for e in result['events'] if e['offset']==18280)
        self.assertIn(payload,event['fields'].values())

    def test_rejects_sample_raw_extent_and_cpu_reserved(self):
        for offset, fmt, value in [(15912+56,'<I',65535),(15912+44,'<I',1),(15912+40,'<I',4096)]:
            with self.subTest(offset=offset):
                self.reject_mutation('linux7-syscalls.perf',lambda d: struct.pack_into(fmt,d,offset,value))

    def test_rejects_unknown_misc_flags(self):
        for kind in (3,4,7,9,10,14,17,18):
            data = (FIXTURES / 'linux7-syscalls.perf').read_bytes()
            offset = next(off for off,t,m,s in inventory(data) if t==kind)
            with self.subTest(kind=kind):
                self.reject_mutation('linux7-syscalls.perf', lambda d: struct.pack_into('<H', d, offset+4,32768))

    def test_real_filename_padding_is_retained_and_strings_are_bounded(self):
        result = self.decode('linux7-syscalls.perf')
        row = next(r for r in result['records'] if r['offset']==4968)
        self.assertEqual(row['body']['filename'],b'[x_tables]')
        self.assertEqual(row['body']['filename_padding'],b']\x00\x00\x00\x00')
        self.reject_mutation('linux7-syscalls.perf',lambda d: d.__setitem__(slice(4968+72,4968+120-32),b'x'*16))
        # An earlier NUL whose aligned end no longer reaches the body end is invalid.
        self.reject_mutation('linux7-syscalls.perf',lambda d: d.__setitem__(4968+72,0))

    def test_tracepoint_name_fields_require_nul(self):
        from perf_trace_format import load_file
        perf = load_file(FIXTURES/'linux7-syscalls.perf')
        data = bytearray(perf.data)
        offset = next(off for off,kind,misc,size in inventory(data)
                      if kind==9 and perf.ids[struct.unpack_from('<Q',data,off+32)[0]].name=='task:task_rename')
        data[offset+60+28:offset+60+44] = b'x'*16
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)/'bad-name.perf'
            path.write_bytes(data)
            with self.assertRaises(DecodeError): records.decode_file(path)

    def test_id_index_matches_configured_cpu_and_thread_maps(self):
        for offset,fmt,value in [(2792+24,'<Q',8191),(2792+32,'<Q',3),(2792+40,'<q',42)]:
            with self.subTest(offset=offset):
                self.reject_mutation('linux7-syscalls.perf',lambda d: struct.pack_into(fmt,d,offset,value))

    def test_rejects_dummy_scheduler_sample(self):
        from perf_trace_format import load_file
        perf = load_file(FIXTURES/'linux617-scheduler.perf')
        dummy = next(a for a in perf.attrs if a.event_type==1)
        self.reject_mutation('linux617-scheduler.perf',lambda d: struct.pack_into('<Q',d,329240+8,dummy.ids[0]))

    def test_rejects_known_metadata_body_variants(self):
        # Every mutation preserves the outer container and reaches record validation.
        mutations = [(4600+40,'<B',21), (4600+41,'<B',1),
                     (12152+20,'<H',0), (12152+22,'<H',2),
                     (12248+8,'<H',3), (12248+10,'<H',1),
                     (15472+6,'<H',48), (24744+6,'<H',56),
                     (2792+8,'<Q',57), (12096+8,'<Q',8193),
                     (11648+16,'<Q',67), (11648+28,'<H',1),
                     (12136+14,'<H',4)]
        for offset,fmt,value in mutations:
            with self.subTest(offset=offset):
                self.reject_mutation('linux7-syscalls.perf',lambda d: struct.pack_into(fmt,d,offset,value))
        self.reject_mutation('linux7-syscalls.perf',lambda d: d.__setitem__(slice(15400+16,15400+32),b'x'*16))

    def test_sample_and_trailer_cpu_matches_id_index(self):
        for name in ('linux7-syscalls.perf','linux617-syscalls.perf','linux617-scheduler.perf'):
            result = self.decode(name)
            configured = {entry['id']:entry['cpu'] for row in result['records'] if row['type']==69
                          for entry in row['body']['entries']}
            for row in result['records']:
                value = row.get('sample') or row.get('trailer')
                if value and not value.get('initialization'):
                    identity = value.get('id',value.get('identifier'))
                    self.assertEqual(value['cpu'],configured[identity])
        # Both changes stay inside CPU_MAP while disagreeing with the selected ID.
        for offset in (15912+40,15472+32):
            with self.subTest(offset=offset):
                self.reject_mutation('linux7-syscalls.perf',lambda d: struct.pack_into('<I',d,offset,1))

    def test_rejects_unreviewed_negative_callers(self):
        for offset,value in ((15912+16,-1),(15912+16,-2),(15912+20,-1),(15912+20,-2),
                             (15472+8,-2),(15472+12,-2)):
            with self.subTest(offset=offset,value=value):
                self.reject_mutation('linux7-syscalls.perf',lambda d: struct.pack_into('<i',d,offset,value))
        self.reject_mutation('linux617-scheduler.perf',lambda d: struct.pack_into('<i',d,338280+28,-2))

    def test_metadata_pid_ranges_preserve_only_reviewed_sentinels(self):
        # Body identities are independent of trailer caller identities.
        for offset in (15400+8,15400+12,16752+8,16752+12,16752+16,16752+20,
                       24744+8,24744+12,24744+16,24744+20):
            with self.subTest(offset=offset):
                self.reject_mutation('linux7-syscalls.perf',lambda d: struct.pack_into('<i',d,offset,-1))
        for offset in (4600+8,4600+12):
            with self.subTest(offset=offset):
                self.reject_mutation('linux7-syscalls.perf',lambda d: struct.pack_into('<i',d,offset,-2))
        self.reject_mutation('linux7-syscalls.perf',lambda d: struct.pack_into('<q',d,12096+16,2**31))
        result = self.decode('linux617-scheduler.perf')
        self.assertTrue(any(row['type']==10 and row['body']['pid']==-1 for row in result['records']))
        self.assertTrue(any(row['type']==15 and row['body']['next_prev_tid']==-1 for row in result['records']))
        self.assertTrue(any(row['type']==15 and row['trailer']['tid']==-1 for row in result['records']))

    def test_deterministic_malformed_input_has_bounded_outcomes(self):
        rng = random.Random(20261029)
        accepted = 0
        rejected = 0
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)/'mutated.perf'
            for name in ('linux7-syscalls.perf','linux617-syscalls.perf','linux617-scheduler.perf'):
                original = (FIXTURES/name).read_bytes()
                for case in range(100):
                    data = bytearray(original)
                    if case % 4 == 0:
                        del data[rng.randrange(len(data)):]
                    else:
                        for bit in rng.sample(range(len(data)*8),rng.randint(1,4)):
                            data[bit//8] ^= 1 << (bit%8)
                    path.write_bytes(data)
                    with self.subTest(name=name,case=case):
                        try:
                            result = records.decode_file(path)
                        except DecodeError:
                            rejected += 1
                            continue
                        # Any other exception is an actual test failure, never a
                        # successful refusal. Accepted mutations retain accounting.
                        accepted += 1
                        start,size = struct.unpack_from('<QQ',data,40)
                        cursor = start
                        counts = collections.Counter()
                        for row in result['records']:
                            self.assertEqual(row['offset'],cursor)
                            self.assertGreaterEqual(row['size'],8)
                            cursor += row['size']
                            counts[row['type']] += 1
                        self.assertEqual(cursor,start+size)
                        self.assertEqual(result['sample_count'],counts[9])
                        self.assertEqual(len(result['events']),sum(counts[k] for k in (3,4,7,9)))
                        self.assertEqual(result['event_counts'],dict(collections.Counter(e['event'] for e in result['events'])))
                        self.assertEqual(result['input_sha256'],hashlib.sha256(data).hexdigest())
        self.assertEqual(accepted+rejected,300)
        self.assertGreater(accepted,0)
        self.assertGreater(rejected,0)

    def test_record_caps_are_enforced(self):
        old = records.MAX_RECORDS
        records.MAX_RECORDS = 1
        try:
            with self.assertRaises(DecodeError): self.decode('linux7-syscalls.perf')
        finally:
            records.MAX_RECORDS = old
        old = records.MAX_EVENTS
        records.MAX_EVENTS = 1
        try:
            with self.assertRaises(DecodeError): self.decode('linux7-syscalls.perf')
        finally:
            records.MAX_EVENTS = old


if __name__ == '__main__':
    unittest.main()
