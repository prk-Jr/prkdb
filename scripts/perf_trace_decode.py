#!/usr/bin/env python3
"""Offline record/schema decoding. Output never certifies recorder or DB timing."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import tempfile

from perf_trace_format import DecodeError
from perf_trace_records import decode_file

MAX_OUTPUT = 256 << 20
SYSCALLS = {'write', 'pwrite64', 'fsync', 'fdatasync', 'futex'}


def require(condition, message):
    if not condition:
        raise DecodeError(message)


def integer(value, label, low=0, high=(1 << 64) - 1):
    require(type(value) is int and low <= value <= high, f'invalid {label}')
    return value


def json_ready(value):
    """Preserve opaque bytes deterministically; JSON escaping owns framing."""
    if isinstance(value, bytes):
        return {'hex': value.hex(), 'bytes': len(value), 'sha256': hashlib.sha256(value).hexdigest()}
    if isinstance(value, dict):
        return {key: json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_ready(item) for item in value]
    return value


def safe_string(value, label, *, token=False, spaces=False):
    if isinstance(value, bytes):
        value = value.split(b'\0', 1)[0]
        try:
            value = value.decode('utf-8')
        except UnicodeDecodeError as error:
            raise DecodeError(f'{label}: canonical UTF-8 unavailable') from error
    require(isinstance(value, str) and value and '\0' not in value, f'invalid {label}')
    require(all(ord(ch) >= 32 and ord(ch) != 127 and ch not in '\x85\u2028\u2029' for ch in value), f'unsafe {label}')
    if token:
        require(re.fullmatch(r'[A-Za-z0-9_.:/+@=,%-]+', value) is not None, f'unsupported {label}')
    else:
        require(not any(ch.isspace() and (not spaces or ch != ' ') for ch in value), f'unsupported whitespace in {label}')
    return value


def task_name(value, label, *, spaces=False):
    name = safe_string(value, label, token=not spaces, spaces=spaces)
    require(len(name.encode()) <= 15, f'{label} exceeds native comm width')
    return name


def state_text(value):
    integer(value, 'scheduler state', high=511)
    bits = ((1, 'S'), (2, 'D'), (4, 'T'), (8, 't'), (16, 'X'), (32, 'Z'), (64, 'P'), (128, 'I'))
    state = '|'.join(name for bit, name in bits if value & bit) or 'R'
    return state + ('+' if value & 256 else '')


def canonical_detail(event, fields):
    """Serialize semantic scalars only; no print expression or payload text."""
    if event.startswith('syscalls:sys_'):
        match = re.fullmatch(r'syscalls:sys_(enter|exit)_(\w+)', event)
        require(match is not None and match[2] in SYSCALLS, 'unsupported syscall event')
        if match[1] == 'exit':
            return 'ret=' + str(integer(fields.get('ret'), 'return value', -(1 << 63), (1 << 63) - 1))
        pieces = []
        for key, value in fields.items():
            require(re.fullmatch(r'[A-Za-z_]\w*', key) is not None, 'invalid trace field name')
            if key.startswith('common_'):
                continue
            if key == '__buf_val' and match[2] in ('write', 'pwrite64'):
                require(isinstance(value, bytes), 'buffer payload must remain bytes')
                continue
            pieces.append(key + '=' + str(integer(value, key, -(1 << 63))))
        require(pieces, 'empty syscall entry')
        return ' '.join(pieces)
    if event == 'task:task_rename':
        parts = ['oldcomm=' + task_name(fields.get('oldcomm'), 'oldcomm'),
                 'newcomm=' + task_name(fields.get('newcomm'), 'newcomm')]
        if 'pid' in fields:
            parts.insert(0, 'pid=' + str(integer(fields['pid'], 'rename pid', high=(1 << 31) - 1)))
        return ' '.join(parts)
    if event == 'sched:sched_process_exec':
        return ' '.join(('filename=' + safe_string(fields.get('filename'), 'filename'),
                         'pid=' + str(integer(fields.get('pid'), 'exec pid', high=(1 << 31) - 1)),
                         'old_pid=' + str(integer(fields.get('old_pid'), 'old pid', high=(1 << 31) - 1))))
    if event == 'sched:sched_process_fork':
        parent = fields.get('parent_pid', fields.get('pid'))
        comm = fields.get('parent_comm', fields.get('comm'))
        return ' '.join(('comm=' + task_name(comm, 'parent comm'),
                         'pid=' + str(integer(parent, 'parent pid', high=(1 << 31) - 1)),
                         'child_comm=' + task_name(fields.get('child_comm'), 'child comm'),
                         'child_pid=' + str(integer(fields.get('child_pid'), 'child pid', high=(1 << 31) - 1))))
    if event == 'sched:sched_process_exit':
        return ' '.join(('comm=' + task_name(fields.get('comm'), 'exit comm'),
                         'pid=' + str(integer(fields.get('pid'), 'exit pid', high=(1 << 31) - 1)),
                         'prio=' + str(integer(fields.get('prio'), 'exit priority', high=(1 << 31) - 1))))
    if event == 'sched:sched_switch':
        parts = []
        for side in ('prev', 'next'):
            parts.extend((side + '_comm=' + task_name(fields.get(side + '_comm'), side + ' comm'),
                          side + '_pid=' + str(integer(fields.get(side + '_pid'), side + ' pid', high=(1 << 31) - 1)),
                          side + '_prio=' + str(integer(fields.get(side + '_prio'), side + ' priority', high=(1 << 31) - 1))))
            if side == 'prev':
                parts.extend(('prev_state=' + state_text(fields.get('prev_state')), '==>'))
        return ' '.join(parts)
    if event == 'sched:sched_wakeup':
        parts = ['comm=' + task_name(fields.get('comm'), 'wake comm'),
                 'pid=' + str(integer(fields.get('pid'), 'wake pid', high=(1 << 31) - 1)),
                 'prio=' + str(integer(fields.get('prio'), 'wake priority', high=(1 << 31) - 1))]
        if 'success' in fields:
            parts.append('success=' + str(integer(fields['success'], 'wake success', high=1)))
        parts.append('target_cpu=' + str(integer(fields.get('target_cpu'), 'target cpu', high=4095)))
        return ' '.join(parts)
    raise DecodeError('unsupported canonical event: ' + str(event))


def canonical_text(events):
    """An explicit adapter for unchanged legacy validators, never qualification."""
    lines = []
    previous = (-1, -1)
    for row in events:
        stamp = integer(row.get('stamp'), 'timestamp')
        offset = integer(row.get('offset'), 'record offset')
        require((stamp, offset) >= previous, 'canonical events are out of order')
        previous = (stamp, offset)
        pid = integer(row.get('pid'), 'caller pid', -1, (1 << 31) - 1)
        tid = integer(row.get('tid'), 'caller tid', -1, (1 << 31) - 1)
        cpu = integer(row.get('cpu'), 'CPU', high=4095)
        comm = row.get('comm') or b'decoded'
        prefix = f'{task_name(comm, "caller comm", spaces=True)} {pid}/{tid} [{cpu:03d}] {stamp // 10**9}.{stamp % 10**9:09d}: '
        event = row.get('event')
        if event == 'COMM':
            body = 'PERF_RECORD_COMM' + (' exec' if row.get('comm_exec') else '') + ': '
            body += task_name(row.get('name'), 'COMM name', spaces=True) + ':'
            body += str(integer(row.get('body_pid'), 'COMM body pid', high=(1 << 31) - 1)) + '/'
            body += str(integer(row.get('body_tid'), 'COMM body tid', high=(1 << 31) - 1))
        elif event in ('FORK', 'EXIT'):
            body = 'PERF_RECORD_' + event + '(' + str(integer(row.get('body_pid'), 'body pid'))
            body += ':' + str(integer(row.get('body_tid'), 'body tid')) + '):('
            body += str(integer(row.get('parent_pid'), 'parent pid')) + ':'
            body += str(integer(row.get('parent_tid'), 'parent tid')) + ')'
        else:
            body = str(event) + ': ' + canonical_detail(str(event), row.get('fields', {}))
        lines.append(prefix + body)
    return '\n'.join(lines) + ('\n' if lines else '')


def write_output(path, chunks, *, limit=MAX_OUTPUT):
    """Bounded sibling-temp + exclusive link: never overwrite evidence."""
    path = Path(path)
    require(not path.exists() and not path.is_symlink(), 'output already exists')
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(dir=path.parent, prefix='.perf-trace-', delete=False) as stream:
            temporary = Path(stream.name)
            size = 0
            for chunk in chunks:
                data = chunk.encode('utf-8')
                size += len(data)
                require(size <= limit, 'offline output exceeds byte cap')
                stream.write(data)
        try:
            os.link(temporary, path, follow_symlinks=False)
        except FileExistsError as error:
            raise DecodeError('output already exists') from error
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('input', type=Path)
    parser.add_argument('--output', type=Path)
    parser.add_argument('--format', choices=('json', 'canonical'), default='json')
    args = parser.parse_args(argv)
    try:
        document = decode_file(args.input)
        document.update(format_version=1, diagnostic_only=True, recording_qualified=False,
                        timing_eligible=False, build_provenance_checked=False)
        if args.format == 'canonical':
            chunks = iter([canonical_text(document['events'])])
        else:
            chunks = json.JSONEncoder(ensure_ascii=True, sort_keys=True, indent=2).iterencode(json_ready(document))
        if args.output:
            write_output(args.output, chunks)
        else:
            # Validate the complete output cap before printing any partial result.
            with tempfile.TemporaryDirectory() as tmp:
                target = Path(tmp) / 'result'
                write_output(target, chunks)
                with target.open() as stream:
                    while part := stream.read(65536):
                        sys.stdout.write(part)
        return 0
    except (DecodeError, OSError, ValueError) as error:
        print('offline decode refused: ' + str(error), file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
