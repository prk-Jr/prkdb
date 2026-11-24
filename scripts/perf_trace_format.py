"""Strict, bounded metadata reader for the supported offline perf trace profile.

No print-format expression is interpreted. Known unrelated feature payloads are
inventoried by extent/hash only; this module does not claim to validate their ABI.
"""
from dataclasses import dataclass
import hashlib
import os
from pathlib import Path
import re
import stat
import struct

MAX_FILE_BYTES = 128 * 1024 * 1024
MAX_METADATA_BYTES = 8 * 1024 * 1024
MAX_ATTRS = 128
MAX_SCHEMAS = 128
MAX_FIELDS = 128
MAX_FIELD_DECLARATION_BYTES = 256
MAX_FIELD_NAME_BYTES = 128
MAX_IDS = 8192


class DecodeError(ValueError):
    """Input is malformed or outside the explicitly supported recording profile."""


def _require(condition, message):
    if not condition:
        raise DecodeError(message)


class _Reader:
    """A cursor whose reads cannot escape one declared metadata section."""
    def __init__(self, data, label):
        self.data = data
        self.pos = 0
        self.label = label

    @property
    def remaining(self):
        return len(self.data) - self.pos

    def take(self, size):
        _require(0 <= size <= self.remaining, f'{self.label}: truncated extent')
        start = self.pos
        self.pos += size
        return self.data[start:self.pos]

    def uint(self, width):
        return int.from_bytes(self.take(width), 'little')

    def count(self, maximum, minimum_size=1):
        count = self.uint(4)
        _require(count <= maximum and count <= self.remaining // minimum_size,
                 f'{self.label}: excessive or truncated count')
        return count

    def cstring(self):
        end = self.data.find(b'\0', self.pos)
        _require(end >= self.pos, f'{self.label}: unterminated string')
        return _text(self.take(end - self.pos + 1)[:-1])

    def padded_string(self):
        size = self.uint(4)
        _require(size > 0, f'{self.label}: empty string extent')
        raw = self.take(size)
        end = raw.find(b'\0')
        _require(end >= 0 and not any(raw[end:]), f'{self.label}: invalid string padding')
        return _text(raw[:end])

    def done(self):
        _require(self.remaining == 0, f'{self.label}: trailing bytes')


def _text(raw):
    try:
        return raw.decode('ascii')
    except UnicodeDecodeError as exc:
        raise DecodeError('non-ASCII metadata string') from exc


@dataclass(frozen=True)
class Attr:
    name: str
    event_type: int
    config: int
    sample_type: int
    flags: int
    clockid: int
    ids: tuple[int, ...]
    rawbytes: bytes


@dataclass(frozen=True)
class Field:
    name: str
    offset: int
    size: int
    signed: bool
    kind: str
    relative: bool = False


@dataclass(frozen=True)
class Schema:
    name: str
    trace_id: int
    fields: tuple[Field, ...]
    fixed_end: int

    def __post_init__(self):
        _require(0 < self.trace_id <= 65535, 'trace schema ID outside common_type range')
        _require(0 < len(self.fields) <= MAX_FIELDS, 'invalid schema field count')
        names = set()
        spans = []
        for field in self.fields:
            _require(len(field.name) <= MAX_FIELD_NAME_BYTES, 'field name exceeds cap')
            _require(field.name not in names and re.fullmatch(r'[A-Za-z_]\w*', field.name),
                     'duplicate or invalid field name')
            names.add(field.name)
            _require(0 <= field.offset < 65536 and 0 < field.size < 65536
                     and field.offset + field.size < 65536 and field.signed in (False, True),
                     'invalid field extent or signedness')
            _require(field.kind in ('numeric', 'fixed', 'dynamic'), 'unsupported field kind')
            if field.kind == 'numeric':
                _require(field.size in (1, 2, 4, 8), 'unsupported numeric width')
            if field.kind == 'dynamic':
                _require(field.size == 4 and not field.signed, 'unsupported dynamic locator')
            else:
                _require(not field.relative, 'relative non-locator field')
            spans.append((field.offset, field.offset + field.size))
        spans.sort()
        _require(all(a[1] <= b[0] for a, b in zip(spans, spans[1:])), 'overlapping schema fields')
        _require(self.fixed_end == spans[-1][1], 'schema fixed extent mismatch')
        common = next((f for f in self.fields if f.name == 'common_type'), None)
        _require(common == Field('common_type', 0, 2, False, 'numeric'),
                 'unsupported common_type layout')

    def decode(self, rawbytes):
        _require(len(rawbytes) >= self.fixed_end, f'{self.name}: truncated RAW fixed fields')
        values = {}
        dynamic = []
        for field in self.fields:
            raw = rawbytes[field.offset:field.offset + field.size]
            if field.kind == 'numeric':
                values[field.name] = int.from_bytes(raw, 'little', signed=bool(field.signed))
            elif field.kind == 'fixed':
                values[field.name] = bytes(raw)
            else:
                locator = int.from_bytes(raw, 'little')
                offset, size = locator & 0xffff, locator >> 16
                if field.relative:
                    offset += field.offset + 4
                _require(offset <= len(rawbytes) and size <= len(rawbytes) - offset,
                         f'{self.name}: dynamic extent escapes RAW')
                _require(size == 0 or offset >= self.fixed_end,
                         f'{self.name}: dynamic extent overlaps fixed fields')
                if size:
                    dynamic.append((offset, offset + size))
                values[field.name] = bytes(rawbytes[offset:offset + size])
        dynamic.sort()
        _require(all(a[1] <= b[0] for a, b in zip(dynamic, dynamic[1:])),
                 f'{self.name}: overlapping dynamic extents')
        _require(values['common_type'] == self.trace_id, f'{self.name}: RAW common_type mismatch')
        return values


@dataclass(frozen=True)
class PerfFile:
    data: bytes
    attrs: tuple[Attr, ...]
    ids: dict[int, Attr]
    schemas: dict[int, Schema]
    data_offset: int
    data_size: int
    metadata: dict


_FIELD = re.compile(r'\s*field:([^;]+);\s*offset:(\d{1,5});\s*size:(\d{1,5});\s*signed:([01]);\s*')
_IDENT = r'[A-Za-z_][A-Za-z_0-9]*'


def _parse_schema(raw, system):
    prefix, marker, _opaque_print = raw.partition(b'\nprint fmt:')
    _require(bool(marker), 'missing opaque print fmt')
    text = _text(prefix)
    header = re.match(r'name: (' + _IDENT + r')\nID: (\d{1,5})\nformat:\n', text)
    _require(header is not None, 'invalid trace format header')
    name, trace_id = header.group(1), int(header.group(2))
    body = text[header.end():]
    fields = []
    for line in body.splitlines():
        if not line.strip():
            continue
        match = _FIELD.fullmatch(line)
        _require(match is not None, 'invalid trace field declaration')
        declaration, offset, size, signed = match.groups()
        # Bound the input before the unanchored trailing-name search. Without
        # this guard an invalid suffix can cause quadratic regexp backtracking.
        _require(len(declaration) <= MAX_FIELD_DECLARATION_BYTES,
                 'field declaration exceeds cap')
        variable = re.search(r'(' + _IDENT + r')(\[\d{1,5}\])?\s*$', declaration)
        _require(variable is not None, 'unsupported trace field declaration')
        dynamic = declaration.startswith(('__data_loc ', '__rel_loc '))
        _require(not ('__data_loc' in declaration or '__rel_loc' in declaration) or dynamic,
                 'malformed dynamic declaration')
        if dynamic:
            _require(re.fullmatch(r'__(?:data|rel)_loc\s+char\[\]\s+' + _IDENT, declaration) is not None,
                     'unsupported dynamic field type')
            kind = 'dynamic'
        elif variable.group(2):
            _require(re.fullmatch(r'char\s+' + _IDENT + r'\[\d{1,5}\]', declaration) is not None,
                     'unsupported fixed array type')
            _require(int(variable.group(2)[1:-1]) == int(size), 'fixed array width mismatch')
            kind = 'fixed'
        else:
            # Recorded width/signedness are authoritative (syscall slots may
            # spell int while occupying unsigned eight-byte argument slots).
            _require(bool(declaration[:variable.start()].strip()), 'missing field type')
            kind = 'numeric'
        fields.append(Field(variable.group(1), int(offset), int(size), bool(int(signed)),
                            kind, declaration.startswith('__rel_loc ')))
        _require(len(fields) <= MAX_FIELDS, 'too many trace fields')
    return Schema(system + ':' + name, trace_id, tuple(fields),
                  max((f.offset + f.size for f in fields), default=0))


def _tracing(raw):
    reader = _Reader(raw, 'HEADER_TRACING_DATA')
    _require(reader.take(10) == b'\x17\x08Dtracing', 'unsupported tracing magic')
    _require(reader.cstring() == '0.6', 'unsupported tracing version')
    _require(reader.uint(1) == 0 and reader.uint(1) == 8, 'unsupported tracing endian/long size')
    _require(reader.uint(4) == 4096, 'unsupported tracing page size')
    for name in ('header_page', 'header_event'):
        _require(reader.cstring() == name, 'unexpected tracing header block')
        reader.take(reader.uint(8))
    schemas = {}
    names = set()

    def add(system):
        schema = _parse_schema(reader.take(reader.uint(8)), system)
        _require(schema.trace_id not in schemas and schema.name not in names,
                 'duplicate trace schema ID/name')
        _require(len(schemas) < MAX_SCHEMAS, 'too many trace schemas')
        schemas[schema.trace_id] = schema
        names.add(schema.name)

    for _ in range(reader.count(MAX_SCHEMAS, 8)):
        add('ftrace')
    systems = set()
    for _ in range(reader.count(MAX_SCHEMAS, 5)):
        system = reader.cstring()
        _require(re.fullmatch(_IDENT, system) is not None and system not in systems,
                 'invalid or duplicate trace system')
        systems.add(system)
        for _ in range(reader.count(MAX_SCHEMAS, 8)):
            add(system)
    for width in (4, 4, 8):  # kallsyms, printk formats, saved command lines
        reader.take(reader.uint(width))
    reader.done()
    return schemas


def _descriptions(raw, inner_size, count):
    reader = _Reader(raw, 'HEADER_EVENT_DESC')
    nr = reader.count(MAX_ATTRS, inner_size + 12)
    _require(nr == count and reader.uint(4) == inner_size, 'event description attr layout mismatch')
    descriptions = []
    total_ids = 0
    names = set()
    for _ in range(nr):
        attr = reader.take(inner_size)
        nr_ids = reader.count(MAX_IDS, 8)
        total_ids += nr_ids
        _require(total_ids <= MAX_IDS, 'too many described perf IDs')
        name = reader.padded_string()
        _require(name and name not in names, 'duplicate/empty event description name')
        names.add(name)
        ids = tuple(reader.uint(8) for _ in range(nr_ids))
        _require(len(set(ids)) == len(ids), 'duplicate described perf ID')
        descriptions.append((name, attr, ids))
    reader.done()
    return descriptions


def load_file(path):
    """Read a bounded regular file and validate its container/trace metadata."""
    path = Path(path)
    try:
        # O_NONBLOCK prevents a named pipe from blocking before fstat can
        # reject it. It does not change regular-file read semantics.
        descriptor = os.open(path, os.O_RDONLY | getattr(os, 'O_NONBLOCK', 0))
        with os.fdopen(descriptor, 'rb') as stream:
            before = os.fstat(stream.fileno())
            _require(stat.S_ISREG(before.st_mode), 'input must be a regular file')
            _require(104 <= before.st_size <= MAX_FILE_BYTES, 'unsupported file size')
            data = stream.read(before.st_size + 1)
            after = os.fstat(stream.fileno())
            _require(len(data) == before.st_size and before.st_size == after.st_size
                     and before.st_mtime_ns == after.st_mtime_ns, 'input changed while reading')
    except OSError as exc:
        raise DecodeError(f'cannot read perf input: {exc}') from exc
    _require(data[:8] == b'PERFILE2', 'unsupported perf magic/endian/pipe format')
    header_size, stride, attrs_off, attrs_size, data_off, data_size, types_off, types_size = struct.unpack_from('<8Q', data, 8)
    _require(header_size == 104 and stride in (152, 160), 'unsupported perf header/attr stride')
    _require(types_off == 0 and types_size == 0, 'legacy event_types unsupported')
    _require(attrs_size > 0 and attrs_size % stride == 0 and attrs_size // stride <= MAX_ATTRS,
             'invalid attr section size/count')
    bits = int.from_bytes(data[72:104], 'little')
    features = [i for i in range(256) if bits >> i & 1]
    _require(all(1 <= i <= 34 and i not in (15, 18, 19, 24, 27) for i in features),
             'unknown/unsupported feature (AUX/compression/group/stat)')
    _require({1, 6, 12, 23, 29}.issubset(features), 'missing required trace/identity/clock metadata')
    sections = []

    def section(name, offset, size, validation='structural'):
        _require(0 <= offset <= len(data) and 0 <= size <= len(data) - offset,
                 f'{name}: section escapes file')
        _require(size > 0, f'{name}: empty independent section')
        sections.append({'name': name, 'offset': offset, 'size': size,
                         'sha256': hashlib.sha256(memoryview(data)[offset:offset + size]).hexdigest(),
                         'validation': validation})
        return memoryview(data)[offset:offset + size]

    section('header', 0, 104)
    section('attrs', attrs_off, attrs_size)
    section('data', data_off, data_size, 'record decoder required')
    table = section('feature_table', data_off + data_size, 16 * len(features))
    payloads = {}
    parsed = {1, 6, 12, 23, 29, 33}
    metadata_bytes = attrs_size
    for index, feature_id in enumerate(features):
        offset, size = struct.unpack_from('<QQ', table, index * 16)
        view = section(f'feature:{feature_id}', offset, size,
                       'structural' if feature_id in parsed else 'opaque bounded bytes only')
        if feature_id in parsed:
            metadata_bytes += size
            _require(metadata_bytes <= MAX_METADATA_BYTES, 'parsed metadata exceeds cap')
            payloads[feature_id] = bytes(view)
    # perf_header__adds_write reserves the original bitmap's descriptor count;
    # failed feature writers may clear bits while leaving zero descriptor slots.
    table_end = data_off + data_size + 16 * len(features)
    first_payload = min(entry['offset'] for entry in sections
                        if entry['name'].startswith('feature:'))
    reserve = first_payload - table_end
    if reserve:
        _require(reserve > 0 and reserve % 16 == 0
                 and reserve <= (34 - len(features)) * 16,
                 'unsupported feature descriptor reservation')
        reserved = section('feature_table_reserved', table_end, reserve)
        _require(not any(reserved), 'nonzero reserved feature descriptors')
    count = attrs_size // stride
    outer = []
    total_ids = 0
    for index in range(count):
        position = attrs_off + index * stride
        raw = data[position:position + stride - 16]
        ids_off, ids_size = struct.unpack_from('<QQ', data, position + stride - 16)
        _require(ids_size % 8 == 0, 'misaligned perf ID extent')
        total_ids += ids_size // 8
        _require(total_ids <= MAX_IDS, 'too many perf IDs')
        metadata_bytes += ids_size
        _require(metadata_bytes <= MAX_METADATA_BYTES, 'parsed metadata exceeds cap')
        ids_raw = section(f'ids:{index}', ids_off, ids_size)
        ids = tuple(value[0] for value in struct.iter_unpack('<Q', ids_raw))
        outer.append((raw, ids))
    ordered = sorted(sections, key=lambda entry: entry['offset'])
    end = 0
    for entry in ordered:
        _require(entry['offset'] == end, 'overlapping sections or unaccounted file bytes')
        end += entry['size']
    _require(end == len(data), 'unaccounted trailing file bytes')
    arch_reader = _Reader(payloads[6], 'HEADER_ARCH')
    arch = arch_reader.padded_string()
    arch_reader.done()
    _require(arch == 'x86_64', 'unsupported architecture')
    _require(len(payloads[23]) == 8 and int.from_bytes(payloads[23], 'little') > 0,
             'invalid clock resolution')
    _require(len(payloads[29]) == 24, 'invalid clock data extent')
    version, clockid, wall_ns, clock_ns = struct.unpack('<IIQQ', payloads[29])
    _require(version == 1 and clockid == 1, 'unsupported clock data version/clockid')
    if 33 in payloads:
        _require(payloads[33] == struct.pack('<II', 62, 0), 'unsupported machine architecture/flags')
    schemas = _tracing(payloads[1])
    descriptions = _descriptions(payloads[12], stride - 16, count)
    attrs = []
    ids_map = {}
    used_schemas = set()
    for (raw, ids), (name, described_raw, described_ids) in zip(outer, descriptions):
        _require(raw == described_raw and set(ids) == set(described_ids),
                 'outer/described attr or ID mismatch')
        event_type, size, config, _period, sample_type = struct.unpack_from('<IIQQQ', raw)
        flags = struct.unpack_from('<Q', raw, 40)[0]
        attr_clockid = struct.unpack_from('<i', raw, 92)[0]
        _require(size == len(raw) and not any(raw[128:]), 'unsupported attr size/config3/config4')
        _require(flags & (1 << 18) and flags & (1 << 25) and attr_clockid == clockid,
                 'sample_id_all/use_clockid/CLOCK_MONOTONIC required')
        if event_type == 2:
            _require(sample_type in (1479, 66951), 'unsupported trace sample mask')
            _require(config in schemas and schemas[config].name == name and config not in used_schemas,
                     'attr config/schema ID/name mismatch')
            used_schemas.add(config)
        else:
            _require(event_type == 1 and config == 9 and sample_type == 65671 and name == 'dummy:u',
                     'unsupported nontracepoint attr')
        attr = Attr(name, event_type, config, sample_type, flags, attr_clockid, ids, raw)
        for identity in ids:
            _require(identity != 0 and identity not in ids_map, 'duplicate/zero perf ID')
            ids_map[identity] = attr
        attrs.append(attr)
    _require(used_schemas == set(schemas), 'unreferenced trace schemas')
    return PerfFile(data, tuple(attrs), ids_map, schemas, data_off, data_size,
                    {'sha256': hashlib.sha256(data).hexdigest(), 'bytes': len(data),
                     'arch': arch, 'clockid': clockid, 'clock_resolution_ns': int.from_bytes(payloads[23], 'little'),
                     'clock_data': {'version': version, 'wall_ns': wall_ns, 'clock_ns': clock_ns},
                     'features': features, 'sections': ordered,
                     'qualification': 'offline decoding only; no recorder/build/performance qualification'})
