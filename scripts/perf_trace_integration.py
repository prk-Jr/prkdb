#!/usr/bin/env python3
"""Bounded offline trace transport; successful decoding confers no qualification."""
import argparse
from collections import Counter
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import struct
import sys
import tempfile

from perf_trace_decode import canonical_text, integer, require, write_output
from perf_trace_format import DecodeError, MAX_FILE_BYTES, load_file
from perf_trace_records import MAX_EVENTS, MAX_RECORDS, decode_file

MAX_OUTPUT = 256 << 20
TOOLING = ('perf_trace_integration.py', 'perf_trace_decode.py',
           'perf_trace_records.py', 'perf_trace_format.py')
FLAGS = dict(diagnostic_only=True, recording_qualified=False,
             timing_eligible=False, build_provenance_checked=False)
BASE_KEYS = {'version', 'kind', 'category', 'input', 'tooling', *FLAGS}
ENVELOPE_KEYS = BASE_KEYS | {'canonical', 'sample_count', 'event_counts',
                             'records', 'cpu_map', 'thread_map'}
INVENTORY_KEYS = {'offset', 'size', 'type', 'misc', 'sha256'}
ALLOWED_MISC = {3:{0,8192},4:{0},7:{0,8192},9:{1,2},10:{1,2,16385,16386},
                14:{0,8192,24576},15:{0,8192,24576},17:{0},18:{0},
                68:{0},69:{0},73:{0},74:{0},78:{0},82:{0}}


def tooling_hashes():
    root=Path(__file__).resolve().parent
    return {name:hashlib.sha256((root/name).read_bytes()).hexdigest() for name in TOOLING}


def keys(value, expected, label):
    require(type(value) is dict and set(value)==expected, 'invalid '+label+' keys')


def sha(value):
    require(type(value) is str and re.fullmatch('[0-9a-f]{64}',value) is not None,'invalid SHA256')


def input_identity(perf):
    return dict(sha256=hashlib.sha256(perf.data).hexdigest(),bytes=len(perf.data),
                data_offset=perf.data_offset,data_size=perf.data_size)


def validate_base(doc,category,expected_tooling,kind):
    require(category in ('syscalls','scheduler'),'unsupported integration category')
    keys(doc, ENVELOPE_KEYS if kind=='trace' else BASE_KEYS,'protocol')
    require(type(doc['version']) is int and doc['version']==1,'unsupported protocol version')
    require(doc['kind']==kind and doc['category']==category,'wrong protocol category/kind')
    for name,value in FLAGS.items():
        require(type(doc[name]) is bool and doc[name] is value,'invalid qualification flag')
    keys(doc['tooling'],set(TOOLING),'tooling')
    for value in doc['tooling'].values():sha(value)
    require(doc['tooling']==expected_tooling,'integration tooling source changed')
    raw=doc['input'];keys(raw,{'sha256','bytes','data_offset','data_size'},'input');sha(raw['sha256'])
    integer(raw['bytes'],'input bytes',104,MAX_FILE_BYTES)
    integer(raw['data_offset'],'data offset',104,raw['bytes'])
    integer(raw['data_size'],'data bytes',8,raw['bytes']-raw['data_offset'])


def validate_map(value,kind,records):
    expected={'record_offset','record_hex','first','last'} if kind==74 else {'record_offset','record_hex','entries'}
    keys(value,expected,'attachment map')
    integer(value['record_offset'],'map offset')
    matches=[r for r in records if r['type']==kind]
    require(len(matches)==1 and matches[0]['offset']==value['record_offset'],'missing/duplicate/mismatched map identity')
    record=matches[0];encoded=value['record_hex']
    require(type(encoded) is str and len(encoded)==record['size']*2 and re.fullmatch('[0-9a-f]+',encoded) is not None,'invalid encoded map')
    raw=bytes.fromhex(encoded)
    require(hashlib.sha256(raw).hexdigest()==record['sha256'],'map record hash mismatch')
    require(struct.unpack_from('<IHH',raw)==(kind,record['misc'],record['size']),'map header mismatch')
    if kind==74:
        integer(value['first'],'first CPU',0,4095);integer(value['last'],'last CPU',value['first'],4095)
        require(len(raw)==16 and struct.unpack_from('<HHHH',raw,8)==(2,0,value['first'],value['last']),'CPU map differs from record')
    else:
        entries=value['entries'];require(type(entries) is list and 0<len(entries)<=8192,'invalid thread entries')
        require(len(raw)==16+24*len(entries) and struct.unpack_from('<Q',raw,8)[0]==len(entries),'thread map length mismatch')
        seen=set()
        for index,entry in enumerate(entries):
            keys(entry,{'tid','comm_hex'},'thread identity')
            tid=integer(entry['tid'],'thread ID',-1,(1<<31)-1)
            require(tid not in seen,'duplicate thread identity');seen.add(tid)
            comm=entry['comm_hex'];require(type(comm) is str and len(comm)<=30 and re.fullmatch('(?:[0-9a-f]{2})*',comm) is not None,'invalid thread name bytes')
            name=raw[24+24*index:40+24*index]
            require(b'\0' in name and struct.unpack_from('<q',raw,16+24*index)[0]==tid and name.split(b'\0',1)[0].hex()==comm,'thread map differs from record')


def validate_envelope(doc,category,expected_tooling):
    validate_base(doc,category,expected_tooling,'trace')
    canonical=doc['canonical'];keys(canonical,{'text','sha256'},'canonical');sha(canonical['sha256'])
    require(type(canonical['text']) is str and len(canonical['text'])<=MAX_OUTPUT,'canonical exceeds byte cap')
    try:encoded=canonical['text'].encode('utf-8')
    except UnicodeError as error:raise DecodeError('invalid canonical UTF8') from error
    require(len(encoded)<=MAX_OUTPUT and hashlib.sha256(encoded).hexdigest()==canonical['sha256'],'canonical hash/byte cap mismatch')
    records=doc['records'];require(type(records) is list and 0<len(records)<=MAX_RECORDS,'invalid record inventory')
    pos=doc['input']['data_offset'];counts=Counter();samples=0
    for record in records:
        keys(record,INVENTORY_KEYS,'record');sha(record['sha256'])
        integer(record['offset'],'record offset',pos,pos)
        size=integer(record['size'],'record size',8,65535)
        kind=integer(record['type'],'record type');misc=integer(record['misc'],'record misc',0,65535)
        require(kind in ALLOWED_MISC and misc in ALLOWED_MISC[kind] and size%8==0,'unsupported record inventory')
        pos+=size;require(pos<=doc['input']['data_offset']+doc['input']['data_size'],'record exceeds data bounds')
        if kind==9:samples+=1
        if kind in (3,4,7):counts[{3:'COMM',4:'EXIT',7:'FORK'}[kind]]+=1
    require(pos==doc['input']['data_offset']+doc['input']['data_size'],'incomplete record inventory')
    integer(doc['sample_count'],'sample count',0,MAX_EVENTS)
    require(doc['sample_count']==samples,'sample inventory count mismatch')
    event_counts=doc['event_counts'];require(type(event_counts) is dict and len(event_counts)<=128,'invalid event counts')
    for name,count in event_counts.items():
        require(type(name) is str and re.fullmatch(r'[A-Za-z0-9_:]+',name) is not None,'invalid event name')
        integer(count,'event count',1,MAX_EVENTS)
    require(sum(event_counts.values())==samples+sum(counts.values())<=MAX_EVENTS,'event inventory count mismatch')
    for name,count in counts.items():require(event_counts.get(name)==count,'metadata event count mismatch')
    # The canonical serializer emits exactly one physical line per event.
    actual=Counter()
    for line in canonical['text'].splitlines():
        match=re.fullmatch(r'.+ -?\d+/-?\d+ \[\d{3,4}\] \d+\.\d{9}: (.+)',line)
        require(match is not None,'invalid canonical event framing')
        body=match[1]
        if body.startswith('PERF_RECORD_COMM'):name='COMM'
        elif body.startswith('PERF_RECORD_FORK('):name='FORK'
        elif body.startswith('PERF_RECORD_EXIT('):name='EXIT'
        else:
            event=re.match(r'([A-Za-z0-9_]+:[A-Za-z0-9_]+): ',body)
            require(event is not None,'invalid canonical event label');name=event[1]
        actual[name]+=1
    require(dict(actual)==event_counts,'canonical event count mismatch')
    validate_map(doc['cpu_map'],74,records);validate_map(doc['thread_map'],73,records)
    enforce_json_cap(doc)
    return doc


def build_receipt(category,path):
    require(category in ('syscalls','scheduler'),'unsupported integration category')
    before=tooling_hashes();perf=load_file(path)
    doc=dict(version=1,kind='input-receipt',category=category,input=input_identity(perf),tooling=before,**FLAGS)
    require(before==tooling_hashes(),'tooling changed during verification')
    validate_base(doc,category,before,'input-receipt')
    return doc


def build_envelope(category,path):
    require(category in ('syscalls','scheduler'),'unsupported integration category')
    before=tooling_hashes();decoded=decode_file(path);perf=load_file(path)
    identity=input_identity(perf)
    require(decoded['input_sha256']==identity['sha256'],'input changed during decoding')
    # Bound accumulation before joining; one record cannot hide an unbounded
    # string behind a small event count. The decoder already sorts the events.
    lines=[];total=0
    for event in decoded['events']:
        line=canonical_text([event])
        total+=len(line.encode('utf-8'))
        require(total<=MAX_OUTPUT,'canonical output exceeds byte cap')
        lines.append(line)
    text=''.join(lines)
    records=[{key:r[key] for key in INVENTORY_KEYS} for r in decoded['records']]
    cpu=next(r for r in decoded['records'] if r['type']==74)
    thread=next(r for r in decoded['records'] if r['type']==73)
    def map_identity(r):
        return dict(record_offset=r['offset'],record_hex=perf.data[r['offset']:r['offset']+r['size']].hex())
    doc=dict(version=1,kind='trace',category=category,input=identity,tooling=before,**FLAGS,
        canonical=dict(text=text,sha256=hashlib.sha256(text.encode()).hexdigest()),
        sample_count=decoded['sample_count'],event_counts=decoded['event_counts'],records=records,
        cpu_map=dict(map_identity(cpu),**cpu['body']['cpus']),
        thread_map=dict(map_identity(thread),entries=[dict(tid=e['tid'],comm_hex=e['comm'].hex()) for e in thread['body']['entries']]))
    require(before==tooling_hashes(),'tooling changed during decoding')
    return validate_envelope(doc,category,before)


def load_json(path):
    def pairs(items):
        result={}
        for key,value in items:
            require(key not in result,'duplicate JSON key');result[key]=value
        return result
    def constant(value):raise DecodeError('nonfinite JSON constant')
    try:
        descriptor=os.open(path,os.O_RDONLY|getattr(os,'O_NONBLOCK',0))
        with os.fdopen(descriptor,'rb') as stream:
            before=os.fstat(stream.fileno())
            require(stat.S_ISREG(before.st_mode) and 0<before.st_size<=MAX_OUTPUT,'protocol exceeds byte cap or is not regular')
            data=stream.read(before.st_size+1);after=os.fstat(stream.fileno())
            require(len(data)==before.st_size==after.st_size and before.st_mtime_ns==after.st_mtime_ns,'protocol changed while reading')
        return json.loads(data.decode('utf-8'),object_pairs_hook=pairs,parse_constant=constant)
    except (OSError,UnicodeError,ValueError,RecursionError) as error:
        raise DecodeError('invalid integration JSON: '+str(error)) from error


def read_envelope(path,category,expected_tooling):
    return validate_envelope(load_json(path),category,expected_tooling)


def read_receipt(path,category,expected_tooling,identity):
    doc=load_json(path);validate_base(doc,category,expected_tooling,'input-receipt')
    require(doc['input']==identity,'raw input changed around attribute rendering')
    return doc


def enforce_json_cap(doc):
    # Account for JSON escaping before iterencode allocates a whole escaped
    # string. In particular, canonical text may otherwise expand sixfold.
    size=0;pending=[doc]
    while pending:
        value=pending.pop()
        if type(value) is str:
            size+=2
            for character in value:
                code=ord(character)
                size+=(2 if character in '\\"\b\f\n\r\t' else
                       6 if code<32 or 127<=code<=65535 else
                       12 if code>65535 else 1)
                require(size<=MAX_OUTPUT,'integration JSON exceeds byte cap')
        elif type(value) is dict:
            size+=2+max(0,len(value)-1)+len(value)
            for key,item in value.items():pending.extend((key,item))
        elif type(value) is list:
            size+=2+max(0,len(value)-1);pending.extend(value)
        else:size+=len(json.dumps(value,allow_nan=False,separators=(',',':')))
        require(size<=MAX_OUTPUT,'integration JSON exceeds byte cap')
    return size


def json_chunks(doc):
    enforce_json_cap(doc)
    return json.JSONEncoder(ensure_ascii=True,sort_keys=True,separators=(',',':')).iterencode(doc)


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('input',type=Path)
    parser.add_argument('--category',choices=('syscalls','scheduler'),required=True)
    parser.add_argument('--verify-input',action='store_true')
    args=parser.parse_args(argv)
    try:
        doc=(build_receipt if args.verify_input else build_envelope)(args.category,args.input)
        # Complete byte validation before a single stdout byte is emitted.
        with tempfile.TemporaryDirectory() as tmp:
            target=Path(tmp)/'protocol'
            write_output(target,json_chunks(doc),limit=MAX_OUTPUT)
            with target.open('rb') as stream:
                while chunk:=stream.read(65536):sys.stdout.buffer.write(chunk)
        return 0
    except (DecodeError,OSError,ValueError,RecursionError) as error:
        print('offline trace integration refused: '+str(error),file=sys.stderr)
        return 2


if __name__=='__main__':raise SystemExit(main())
