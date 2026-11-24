"""Strict offline decoding of the reviewed little-endian perf record profiles.

Record sizes, attribute masks and trace schemas define framing. Payload bytes never
participate in record discovery or execute the recorded tracepoint print format.
"""
from collections import Counter
import hashlib
import struct

from perf_trace_format import DecodeError, load_file

MAX_RECORDS = 1_000_000
MAX_EVENTS = 250_000
_SAMPLE_MASKS = {1479, 66951}
_TRAILER_MASK = (1 << 1) | (1 << 2) | (1 << 6) | (1 << 7) | (1 << 9) | (1 << 16)
_ALLOWED_MISC = {3: {0, 8192}, 4: {0}, 7: {0, 8192},
                 9: {1, 2}, 10: {1, 2, 16385, 16386},
                 14: {0, 8192, 24576}, 15: {0, 8192, 24576},
                 17: {0}, 18: {0}}


def _require(condition, message):
    if not condition:
        raise DecodeError(message)


def _unpack(fmt, data, offset=0):
    _require(0 <= offset <= len(data) - struct.calcsize(fmt), 'truncated record field')
    return struct.unpack_from(fmt, data, offset)


def _cstring(data, label):
    end = data.find(b'\0')
    _require(end >= 0, f'{label}: missing NUL')
    return data[:end]


def _aligned_string(data, start, label):
    value = _cstring(data[start:], label)
    _require(len(data) == (start + len(value) + 1 + 7) // 8 * 8,
             f'{label}: invalid alignment/length')
    return value


def _cpu_range(data):
    _require(len(data) == 8, 'CPU range: unsupported length')
    kind, any_cpu, first, last = _unpack('<HHHH', data)
    _require(kind == 2 and any_cpu == 0 and first <= last < 4096,
             'CPU map: unsupported encoding/range')
    return {'first': first, 'last': last}


def _trailer(data, mask, ids):
    pos = 0
    result = {}
    if mask & (1 << 1):
        result['pid'], result['tid'] = _unpack('<ii', data, pos)
        pos += 8
    if mask & (1 << 2):
        result['stamp'], = _unpack('<Q', data, pos)
        pos += 8
    if mask & (1 << 6):
        result['id'], = _unpack('<Q', data, pos)
        pos += 8
    if mask & (1 << 9):
        result['stream_id'], = _unpack('<Q', data, pos)
        pos += 8
    if mask & (1 << 7):
        result['cpu'], reserved = _unpack('<II', data, pos)
        _require(reserved == 0, 'trailer CPU reserved bits')
        pos += 8
    if mask & (1 << 16):
        result['identifier'], = _unpack('<Q', data, pos)
        pos += 8
    _require(pos == len(data), 'unsupported sample trailer layout')
    _require(result['pid'] >= -1 and result['tid'] >= -1,
             'unsupported negative metadata caller PID/TID')
    identity = result.get('id', result.get('identifier'))
    if any(data):
        _require(identity in ids, 'unknown trailer event ID')
    else:
        result['initialization'] = True
    return result


def _sample(data, perf):
    # A sample ID is itself mask-dependent. Parse each supported recorded mask,
    # then require that the ID's own attribute selected precisely that layout.
    selected = []
    for mask in {attr.sample_type for attr in perf.attrs if attr.event_type == 2}:
        _require(mask in _SAMPLE_MASKS, 'unsupported tracepoint sample mask')
        pos = 8
        row = {}
        if mask & (1 << 16):
            row['identifier'], = _unpack('<Q', data, pos)
            pos += 8
        if mask & 1:
            row['ip'], = _unpack('<Q', data, pos)
            pos += 8
        if mask & (1 << 1):
            row['pid'], row['tid'] = _unpack('<ii', data, pos)
            pos += 8
        if mask & (1 << 2):
            row['stamp'], = _unpack('<Q', data, pos)
            pos += 8
        if mask & (1 << 6):
            row['id'], = _unpack('<Q', data, pos)
            pos += 8
        if mask & (1 << 7):
            row['cpu'], reserved = _unpack('<II', data, pos)
            _require(reserved == 0, 'sample CPU reserved bits')
            pos += 8
        if mask & (1 << 8):
            row['period'], = _unpack('<Q', data, pos)
            pos += 8
        identity = row.get('id', row.get('identifier'))
        attr = perf.ids.get(identity)
        if attr is not None and attr.event_type == 2 and attr.sample_type == mask:
            selected.append((attr, row, pos))
    _require(len(selected) == 1, 'sample has unknown/ambiguous/nontracepoint event ID')
    attr, row, pos = selected[0]
    scheduler_sentinel = (attr.sample_type == 66951 and
                          any(a.event_type == 1 and a.config == 9 and a.sample_type == 65671
                              for a in perf.attrs))
    _require(row['pid'] >= 0 and
             (row['tid'] >= 0 or (row['tid'] == -1 and scheduler_sentinel)),
             'unsupported negative sample caller PID/TID')
    raw_size, = _unpack('<I', data, pos)
    pos += 4
    _require(raw_size <= len(data) - pos, 'truncated RAW sample')
    raw = data[pos:pos + raw_size]
    end = pos + raw_size
    _require(len(data) == (end + 7) // 8 * 8 and not any(data[end:]),
             'invalid RAW sample padding')
    schema = perf.schemas.get(attr.config)
    _require(schema is not None, 'sample attribute has no trace schema')
    common_type, = _unpack('<H', raw)
    _require(common_type == attr.config, 'RAW common_type differs from event attribute')
    fields = schema.decode(raw)
    for name in ('comm', 'parent_comm', 'child_comm', 'prev_comm', 'next_comm',
                 'oldcomm', 'newcomm', 'filename'):
        if name in fields:
            _require(isinstance(fields[name], bytes) and b'\0' in fields[name],
                     f'{schema.name}: unterminated name field {name}')
    _require(row['tid'] < 0 or fields.get('common_pid') == row['tid'],
             'RAW common_pid differs from sample caller TID')
    if schema.name == 'sched:sched_process_fork':
        if 'parent_pid' in fields:
            fields['pid'] = fields['parent_pid']
        if 'parent_comm' in fields:
            fields['comm'] = fields['parent_comm']
    row.update(kind='sample', event=schema.name, fields=fields, raw_size=raw_size,
               attr_id=row.get('id', row.get('identifier')), comm=b'')
    return row


def _kernel_record(kind, data, misc):
    """Decode a known record body, excluding its sample_id_all trailer."""
    if kind == 3:
        _require(len(data) >= 24, 'short COMM')
        pid, tid = _unpack('<ii', data, 8)
        name = _aligned_string(data, 16, 'COMM name')
        return {'pid': pid, 'tid': tid, 'name': name, 'name_padding': data[17+len(name):],
                'comm_exec': bool(misc & 8192)}
    if kind in (4, 7):
        _require(len(data) == 32, 'invalid FORK/EXIT body size')
        pid, ppid, tid, ptid, stamp = _unpack('<iiiiQ', data, 8)
        return {'pid': pid, 'ppid': ppid, 'tid': tid, 'ptid': ptid, 'time': stamp}
    if kind == 10:
        _require(len(data) >= 80, 'short MMAP2')
        pid, tid, addr, length, pgoff = _unpack('<iiQQQ', data, 8)
        result = {'pid': pid, 'tid': tid, 'addr': addr, 'length': length, 'pgoff': pgoff}
        if misc & 16384:
            build_id_size, reserved1, reserved2 = _unpack('<BBH', data, 40)
            _require(build_id_size <= 20 and reserved1 == reserved2 == 0,
                     'invalid MMAP2 build ID descriptor')
            _require(not any(data[44 + build_id_size:64]), 'MMAP2 build ID padding')
            result['build_id'] = data[44:44 + build_id_size]
        else:
            result['maj'], result['min'], result['ino'], result['ino_generation'] = _unpack('<IIQQ', data, 40)
        result['prot'], result['flags'] = _unpack('<II', data, 64)
        result['filename'] = _aligned_string(data, 72, 'MMAP2 filename')
        result['filename_padding'] = data[73 + len(result['filename']):]
        return result
    if kind == 17:
        _require(len(data) >= 32, 'short KSYMBOL')
        addr, length, symbol_type, flags = _unpack('<QIHH', data, 8)
        _require(symbol_type in (1, 2) and flags in (0, 1), 'unsupported KSYMBOL variant')
        name = _aligned_string(data, 24, 'KSYMBOL name')
        return {'addr': addr, 'length': length, 'symbol_type': symbol_type,
                'flags': flags, 'name': name, 'name_padding': data[25 + len(name):]}
    if kind == 18:
        _require(len(data) == 24, 'invalid BPF_EVENT size')
        op, flags, program_id = _unpack('<HHI', data, 8)
        _require(op in (1, 2) and flags == 0, 'unsupported BPF_EVENT variant')
        return {'op': op, 'flags': flags, 'program_id': program_id, 'tag': data[16:24]}
    if kind == 14:
        _require(len(data) == 8, 'invalid SWITCH size')
        return {'switch_out': bool(misc & 8192), 'preempt': bool(misc & 16384)}
    if kind == 15:
        _require(len(data) == 16, 'invalid SWITCH_CPU_WIDE size')
        pid, tid = _unpack('<ii', data, 8)
        return {'next_prev_pid': pid, 'next_prev_tid': tid,
                'switch_out': bool(misc & 8192), 'preempt': bool(misc & 16384)}
    raise DecodeError(f'unsupported perf record type {kind}')


def _user_record(kind, data, ids):
    if kind in (68, 82):
        _require(len(data) == 8, 'invalid finished marker size')
        return {}
    if kind == 69:
        count, = _unpack('<Q', data, 8)
        _require(count <= 8192 and len(data) == 16 + count * 32,
                 'unsupported ID_INDEX length/entries2')
        entries = []
        seen = set()
        for index in range(count):
            identity, idx, cpu, tid = _unpack('<QQQq', data, 16 + index * 32)
            _require(identity in ids and identity not in seen, 'unknown/duplicate ID_INDEX ID')
            _require(idx < 8192 and cpu < 4096 and -1 <= tid < 2**31, 'invalid ID_INDEX bounds')
            seen.add(identity)
            entries.append({'id': identity, 'idx': idx, 'cpu': cpu, 'tid': tid})
        _require(seen == set(ids), 'ID_INDEX does not cover configured IDs')
        return {'entries': entries}
    if kind == 73:
        count, = _unpack('<Q', data, 8)
        _require(count <= 8192 and len(data) == 16 + count * 24, 'invalid THREAD_MAP size')
        entries = []
        seen = set()
        for index in range(count):
            tid, = _unpack('<q', data, 16 + index * 24)
            _require(-1 <= tid < 2**31 and tid not in seen, 'invalid/duplicate THREAD_MAP TID')
            seen.add(tid)
            name = _cstring(data[24 + index * 24:40 + index * 24], 'THREAD_MAP comm')
            entries.append({'tid': tid, 'comm': name})
        return {'entries': entries}
    if kind == 74:
        return {'cpus': _cpu_range(data[8:])}
    if kind == 78:
        _require(len(data) == 32, 'unsupported EVENT_UPDATE size')
        update_type, identity = _unpack('<QQ', data, 8)
        _require(update_type == 3 and identity in ids, 'unsupported EVENT_UPDATE type/ID')
        _require(identity == ids[identity].ids[0], 'EVENT_UPDATE must use attribute first ID')
        return {'update_type': update_type, 'id': identity, 'cpus': _cpu_range(data[24:])}
    raise DecodeError(f'unsupported user perf record type {kind}')


def decode_file(path):
    """Return typed semantic events and an inventory of every physical record."""
    perf = load_file(path)
    masks = {attr.sample_type & _TRAILER_MASK for attr in perf.attrs}
    _require(len(masks) == 1, 'inconsistent sample_id_all trailer masks')
    mask = masks.pop()
    trailer_size = mask.bit_count() * 8
    _require(trailer_size == 32, 'unsupported trailer size')
    events = []
    records = []
    pos = perf.data_offset
    end = pos + perf.data_size
    while pos < end:
        _require(len(records) < MAX_RECORDS, 'record count exceeds limit')
        _require(end - pos >= 8, 'truncated record header')
        kind, misc, size = _unpack('<IHH', perf.data, pos)
        _require(size >= 8 and size % 8 == 0 and size <= end - pos,
                 'invalid/truncated record size')
        if kind in _ALLOWED_MISC:
            _require(misc in _ALLOWED_MISC[kind], 'unsupported record misc flags')
        data = perf.data[pos:pos + size]
        record = {'offset': pos, 'size': size, 'type': kind, 'misc': misc,
                  'sha256': hashlib.sha256(data).hexdigest()}
        event = None
        if kind == 9:
            event = _sample(data, perf)
            record['sample'] = dict(event)
        elif kind in (3, 4, 7, 10, 14, 15, 17, 18):
            _require(size >= 8 + trailer_size, 'truncated sample_id_all trailer')
            trailer = _trailer(data[-trailer_size:], mask, perf.ids)
            body = _kernel_record(kind, data[:-trailer_size], misc)
            minimum_pid = 0 if kind in (3, 4, 7) else -1
            _require(all(value >= minimum_pid for key, value in body.items()
                         if key in ('pid', 'tid', 'ppid', 'ptid',
                                    'next_prev_pid', 'next_prev_tid')),
                     'unsupported negative metadata body PID/TID')
            record.update(body=body, trailer=trailer)
            if kind in (3, 4, 7):
                event = dict(trailer)
                event.update(kind={3:'comm',4:'exit',7:'fork'}[kind],
                             event={3:'COMM',4:'EXIT',7:'FORK'}[kind],
                             body=body, trailer=trailer, body_pid=body['pid'],
                             body_tid=body['tid'], fields={}, comm=b'',
                             timed_identity=trailer['stamp'] > 0)
                if kind == 3:
                    event.update(name=body['name'],comm_exec=body['comm_exec'])
                else:
                    event.update(parent_pid=body['ppid'],parent_tid=body['ptid'])
        elif kind in (68, 69, 73, 74, 78, 82):
            _require(misc == 0, 'user record reserved misc bits')
            record['body'] = _user_record(kind, data, perf.ids)
        else:
            raise DecodeError(f'unsupported/loss/throttle perf record type {kind} at {pos}')
        records.append(record)
        if event is not None:
            _require(len(events) < MAX_EVENTS, 'event count exceeds limit')
            event.update(offset=pos, record_type=kind, misc=misc)
            events.append(event)
        pos += size
    # Check actual configured CPU bounds after maps have been fully encountered.
    cpu_maps = [r['body']['cpus'] for r in records if r['type'] == 74]
    _require(len(cpu_maps) == 1, 'missing/duplicate CPU_MAP')
    cpus = cpu_maps[0]
    thread_maps = [r['body']['entries'] for r in records if r['type'] == 73]
    _require(len(thread_maps) == 1, 'missing/duplicate THREAD_MAP')
    threads = [entry['tid'] for entry in thread_maps[0]]
    cpu_count = cpus['last'] - cpus['first'] + 1
    expected_index = {}
    for attr in perf.attrs:
        _require(len(attr.ids) == cpu_count * len(threads),
                 'attribute IDs do not fit configured CPU/thread maps')
        for index, identity in enumerate(attr.ids):
            expected_index[identity] = (index, cpus['first'] + index % cpu_count,
                                        threads[index // cpu_count])
    def cpu_valid(cpu):
        return cpus['first'] <= cpu <= cpus['last']
    configured_cpus = {}
    for record in records:
        if record['type'] == 69:
            _require(all((e['idx'],e['cpu'],e['tid']) == expected_index[e['id']]
                         for e in record['body']['entries']),
                     'ID_INDEX entry differs from configured CPU/thread maps')
            configured_cpus.update((e['id'], e['cpu']) for e in record['body']['entries'])
        elif record['type'] == 78:
            _require(record['body']['cpus'] == cpus, 'EVENT_UPDATE CPU map mismatch')
    _require(set(configured_cpus) == set(perf.ids), 'missing configured ID_INDEX CPUs')
    for record in records:
        trailer = record.get('trailer')
        if trailer and not trailer.get('initialization'):
            identity = trailer.get('id', trailer.get('identifier'))
            _require(trailer['cpu'] == configured_cpus[identity],
                     'trailer CPU differs from event ID_INDEX CPU')
        if 'sample' in record:
            sample = record['sample']
            _require(sample['cpu'] == configured_cpus[sample['attr_id']],
                     'sample CPU differs from event ID_INDEX CPU')
    events.sort(key=lambda event: (event['stamp'], event['offset']))
    names = {}
    for event in events:
        if event['kind'] == 'sample':
            _require(cpu_valid(event['cpu']), 'sample CPU outside CPU_MAP')
        event['comm'] = names.get(event['tid'], b'')
        if event['kind'] == 'comm' and event['stamp'] > 0:
            names[event['body_tid']] = event['name']
            if event['tid'] == event['body_tid']:
                event['comm'] = event['name']
    return {'input_sha256': hashlib.sha256(perf.data).hexdigest(),
            'metadata': perf.metadata, 'sample_count': sum(e['kind']=='sample' for e in events),
            'event_counts': dict(sorted(Counter(e['event'] for e in events).items())),
            'events': events, 'records': records}
