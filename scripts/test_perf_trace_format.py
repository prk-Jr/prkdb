"""Metadata and trace-schema regressions for the bounded offline decoder."""
import importlib
import pathlib
import os
import subprocess
import sys
import struct
import tempfile
import unittest
from unittest import mock

ROOT = pathlib.Path(__file__).parent / 'testdata' / 'perf_trace'
try:
    fmt = importlib.import_module('perf_trace_format')
except ModuleNotFoundError:
    fmt = None


def feature(data, wanted):
    end = sum(struct.unpack_from('<QQ', data, 40))
    flags = int.from_bytes(data[72:104], 'little')
    ids = [n for n in range(256) if flags >> n & 1]
    return struct.unpack_from('<QQ', data, end + ids.index(wanted) * 16)


class FormatTests(unittest.TestCase):
    def setUp(self):
        self.assertIsNotNone(fmt, 'framed metadata decoder is not implemented')

    def load_bytes(self, data):
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / 'input.perf'
            path.write_bytes(data)
            return fmt.load_file(path)

    def reject(self, data):
        with self.assertRaises(fmt.DecodeError):
            self.load_bytes(data)

    def test_real_old_and_new_metadata_identity(self):
        for name, count, size in [('linux7-syscalls.perf', 14, 144),
                                  ('linux617-syscalls.perf', 14, 136),
                                  ('linux617-scheduler.perf', 7, 136)]:
            with self.subTest(name=name):
                result = fmt.load_file(ROOT / name)
                self.assertEqual(len(result.attrs), count)
                self.assertEqual(len(result.ids), count * 4)
                self.assertEqual(len(result.schemas), count - (name == 'linux617-scheduler.perf'))
                self.assertEqual(result.metadata['arch'], 'x86_64')
                self.assertEqual(result.metadata['clockid'], 1)
                self.assertEqual(len(result.metadata['sha256']), 64)
                self.assertTrue(result.metadata['sections'])
                for attr in result.attrs:
                    self.assertEqual(len(attr.rawbytes), size)
                    self.assertEqual(attr.clockid, 1)
                    for identity in attr.ids:
                        self.assertIs(result.ids[identity], attr)
                    if attr.event_type == 2:
                        self.assertEqual(result.schemas[attr.config].name, attr.name)

    def test_real_dynamic_write_keeps_injected_text_as_bytes(self):
        result = fmt.load_file(ROOT / 'linux7-syscalls.perf')
        schema = next(s for s in result.schemas.values() if s.name == 'syscalls:sys_enter_write')
        payload = b'\nLOST 999 events\nworker 99 [001] 1.0: syscalls:sys_exit_write: ret=0\n'
        raw = bytearray(schema.fixed_end + len(payload))
        struct.pack_into('<H', raw, 0, schema.trace_id)
        locator = next(f for f in schema.fields if f.name == '__buf_val')
        struct.pack_into('<I', raw, locator.offset, len(payload) << 16 | schema.fixed_end)
        raw[schema.fixed_end:] = payload
        self.assertEqual(schema.decode(raw)['__buf_val'], payload)
        for target, length in [(0, 1), (len(raw), 1), (schema.fixed_end, 65535)]:
            struct.pack_into('<I', raw, locator.offset, length << 16 | target)
            with self.assertRaises(fmt.DecodeError):
                schema.decode(raw)

    def test_signed_returns_and_raw_common_type(self):
        result = fmt.load_file(ROOT / 'linux7-syscalls.perf')
        schema = next(s for s in result.schemas.values() if s.name == 'syscalls:sys_exit_write')
        raw = bytearray(schema.fixed_end)
        struct.pack_into('<H', raw, 0, schema.trace_id)
        struct.pack_into('<q', raw, 16, -13)
        self.assertEqual(schema.decode(raw)['ret'], -13)
        raw[0:2] = b'\0\0'
        with self.assertRaises(fmt.DecodeError):
            schema.decode(raw)
        with self.assertRaises(fmt.DecodeError):
            schema.decode(raw[:7])

    def test_header_bounds_features_and_independent_sections(self):
        original = (ROOT / 'linux7-syscalls.perf').read_bytes()
        for cut in (0, 8, 103, len(original)-1):
            self.reject(original[:cut])
        for position, value in [(8, 103), (16, 168), (32, 10), (48, 2**64-1), (64, 8)]:
            data = bytearray(original)
            struct.pack_into('<Q', data, position, value)
            self.reject(data)
        for byte, mask in [(76, 8), (75, 8)]:
            data = bytearray(original)
            data[byte] |= mask
            self.reject(data)
        data = bytearray(original)
        table = sum(struct.unpack_from('<QQ', data, 40))
        struct.pack_into('<Q', data, table, 0)
        self.reject(data)
        data = bytearray(original)
        attr_off = struct.unpack_from('<Q', data, 24)[0]
        struct.pack_into('<Q', data, attr_off + 144, 112)
        self.reject(data)

    def test_feature_table_reserved_descriptors_are_zero_and_bounded(self):
        original = (ROOT / 'linux7-syscalls.perf').read_bytes()
        result = fmt.load_file(ROOT / 'linux7-syscalls.perf')
        reserved = next(s for s in result.metadata['sections']
                        if s['name'] == 'feature_table_reserved')
        self.assertEqual(reserved['size'], 16)
        data = bytearray(original)
        data[reserved['offset']] = 1
        self.reject(data)
        # Manufacture another zero gap before attrs, rather than after the table.
        data = bytearray(original)
        attrs = struct.unpack_from('<Q', data, 24)[0]
        struct.pack_into('<Q', data, 24, attrs + 16)
        self.reject(data)
        # More empty descriptor slots than the supported feature bitmap allows.
        data = bytearray(original)
        table = sum(struct.unpack_from('<QQ', data, 40))
        first_payload, size = struct.unpack_from('<QQ', data, table)
        struct.pack_into('<Q', data, table, first_payload + 1024)
        self.reject(data)

    def test_attr_descriptor_schema_and_clock_disagreement(self):
        original = (ROOT / 'linux7-syscalls.perf').read_bytes()
        attr_off = struct.unpack_from('<Q', original, 24)[0]
        desc, _ = feature(original, 12)
        clock, _ = feature(original, 29)
        for position, value in [(attr_off + 8, 9999), (desc, 129),
                                (desc + 8, 99), (clock, 2), (clock + 4, 0)]:
            data = bytearray(original)
            struct.pack_into('<I', data, position, value)
            self.reject(data)
        data = bytearray(original)
        struct.pack_into('<Q', data, 112, struct.unpack_from('<Q', data, 104)[0])
        self.reject(data)
        for old, new in [(b'common_flags;\toffset:2;', b'common_flags;\toffset:0;'),
                         (b'size:2;\tsigned:0;', b'size:2;\tsigned:2;'),
                         (b'ID: 474', b'ID: 814'), (b'tracing0.6', b'tracing0.7')]:
            self.assertIn(old, original)
            self.reject(original.replace(old, new, 1))

    def test_file_size_cap_checked_before_read(self):
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / 'oversize.perf'
            with path.open('wb') as stream:
                stream.truncate(128 * 1024 * 1024 + 1)
            with self.assertRaises(fmt.DecodeError):
                fmt.load_file(path)

    def test_counts_and_aggregate_metadata_caps(self):
        original = (ROOT / 'linux7-syscalls.perf').read_bytes()
        for position, value in [(32, 129 * 160), (552 + 152, 8193 * 8)]:
            data = bytearray(original)
            struct.pack_into('<Q', data, position, value)
            self.reject(data)
        data = bytearray(original)
        data.extend(bytes(8 * 1024 * 1024))
        table = sum(struct.unpack_from('<QQ', data, 40))
        struct.pack_into('<Q', data, table + 8, 8 * 1024 * 1024 + 1)
        self.reject(data)

    def test_schema_bounds_and_opaque_print_format(self):
        prefix = (b'name: sample\nID: 17\nformat:\n'
                  b'field:unsigned short common_type; offset:0; size:2; signed:0;\n')
        suffix = b'\nprint fmt: opaque'
        for extra in [b'field:int bad; offset:65536; size:4; signed:1;\n',
                      b'field:int bad; offset:' + b'9' * 5000 + b'; size:4; signed:1;\n',
                      b'field:char bad[2]; offset:2; size:3; signed:0;\n',
                      b'field:__data_loc char[] bad; offset:4; size:8; signed:0;\n',
                      b'field:int common_type; offset:4; size:4; signed:1;\n']:
            with self.subTest(extra=extra[:80]):
                with self.assertRaises(fmt.DecodeError):
                    fmt._parse_schema(prefix + extra + suffix, 'test')
        schema = fmt._parse_schema(prefix + suffix + b'\xff\x00\nfield: this is opaque', 'test')
        self.assertEqual(schema.decode(b'\x11\0'), {'common_type': 17})
        fields = b''.join(('field:int f%d; offset:%d; size:1; signed:0;\n' % (i, i + 2)).encode()
                          for i in range(128))
        with self.assertRaises(fmt.DecodeError):
            fmt._parse_schema(prefix + fields + suffix, 'test')

    @unittest.skipUnless(hasattr(os, 'mkfifo'), 'requires POSIX FIFO support')
    def test_nonregular_file_refuses_without_waiting_for_a_writer(self):
        with tempfile.TemporaryDirectory() as directory:
            fifo = pathlib.Path(directory) / 'input.fifo'
            os.mkfifo(fifo)
            program = ('import sys; sys.path.insert(0, ' + repr(str(pathlib.Path(__file__).parent)) + '); '
                       'from perf_trace_format import load_file, DecodeError\n'
                       'try: load_file(' + repr(str(fifo)) + ')\n'
                       'except DecodeError: sys.exit(0)\n'
                       'sys.exit(1)')
            try:
                result = subprocess.run([sys.executable, '-c', program], timeout=2,
                                        capture_output=True, check=False)
            except subprocess.TimeoutExpired:
                self.fail('decoder blocked opening a nonregular input')
            self.assertEqual(result.returncode, 0, result.stderr)

    def test_oversized_declaration_refuses_before_name_search(self):
        # The old unanchored trailing-name search takes quadratic work on this
        # malformed suffix. Mocking the search proves the ordering without a
        # wall-clock threshold or waiting for the vulnerable expression.
        raw = (b'name: sample\nID: 17\nformat:\nfield:'
               + b'a' * (4 * 1024 * 1024) + b'?; offset:0; size:2; signed:0;'
               + b'\nprint fmt: opaque')
        with mock.patch.object(fmt.re, 'search', side_effect=AssertionError('unbounded name search')):
            with self.assertRaisesRegex(fmt.DecodeError, 'declaration.*cap'):
                fmt._parse_schema(raw, 'test')

    def test_declaration_and_field_name_boundaries(self):
        prefix = (b'name: sample\nID: 17\nformat:\n'
                  b'field:unsigned short common_type; offset:0; size:2; signed:0;\n')
        for declaration in (b'int ' + b'x' * 129, b't' * 255 + b' x'):
            with self.subTest(length=len(declaration)):
                raw = prefix + b'field:' + declaration + b'; offset:2; size:1; signed:0;\n\nprint fmt:'
                with self.assertRaises(fmt.DecodeError):
                    fmt._parse_schema(raw, 'test')
        for declaration in (b'int ' + b'x' * 128, b't' * 254 + b' x'):
            with self.subTest(length=len(declaration)):
                raw = prefix + b'field:' + declaration + b'; offset:2; size:1; signed:0;\n\nprint fmt:'
                schema = fmt._parse_schema(raw, 'test')
                self.assertEqual(schema.decode(b'\x11\0\x07')[schema.fields[-1].name], 7)

    def test_schema_relative_and_dynamic_overlaps(self):
        fields = (fmt.Field('common_type', 0, 2, False, 'numeric'),
                  fmt.Field('a', 4, 4, False, 'dynamic', True),
                  fmt.Field('b', 8, 4, False, 'dynamic'))
        schema = fmt.Schema('test:relative', 17, fields, 12)
        raw = bytearray(18)
        struct.pack_into('<H', raw, 0, 17)
        struct.pack_into('<I', raw, 4, 3 << 16 | 4)
        struct.pack_into('<I', raw, 8, 3 << 16 | 15)
        raw[12:] = b'abcdef'
        self.assertEqual(schema.decode(raw), {'common_type': 17, 'a': b'abc', 'b': b'def'})
        struct.pack_into('<I', raw, 8, 3 << 16 | 14)
        with self.assertRaises(fmt.DecodeError):
            schema.decode(raw)


if __name__ == '__main__':
    unittest.main()
