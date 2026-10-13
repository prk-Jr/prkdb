#!/usr/bin/env python3
"""One baseline-only invocation-envelope schedule; never performance acceptance."""
import argparse
import json
import math
import os
from pathlib import Path
import platform
import re
import shutil
import statistics
import subprocess
import sys
import time

import wal_stall_diagnostic as s
import wal_descriptor_diagnostic as d

verify_artifact = s.verify_artifact
CELLS = {'write': 'adapter_put/8w/64k', 'cold': 'stream_read/cold/64k'}
CYCLE_SECONDS = 2700
GIB = 1 << 30


def grid_order():
    writes = [(f'{kind}/{writers}w/{value}k',writers,value) for _ in range(3)
              for value in (1,64) for writers in (1,8,64)
              for kind in ('adapter_put','compaction_concurrent_put','model_memcpy_only','wal_durable','wal_fast')]
    streams = []
    for _ in range(3):
        streams += [(f'stream_append/{mode}/{writers}w/{value}k/b{batch}',writers,value)
                    for value in (1,64) for writers in (1,64)
                    for mode in ('durable','fast') for batch in (1,100)]
        streams += [('stream_append_retention/durable/64w/1k/b1',64,1),
                    ('stream_read/tail/1k',1,1),('stream_read/cold/64k',1,64)]
    return writes + streams


def schedule():
    return [dict(index=block*3+slot,block=block,slot=slot,kind=kind,side='base',timing_eligible=False)
            for block,order in enumerate((('full','write','cold'),('cold','full','write'),('write','cold','full')))
            for slot,kind in enumerate(order)]


def positive(value):
    result = float(value)
    d.require(math.isfinite(result) and result > 0,'invalid positive native value')
    return result


def parse_output(text, kind):
    d.require(kind in {'full',*CELLS},'unknown invocation kind')
    lines = text.splitlines()
    d.require(lines.count('# wal_write_path') == 1 and [line for line in lines if line.startswith('- warm-up')] == [s.CONFIG_HEADER], 'wrong native configuration')
    d.require(lines.count(s.WRITE_HEADER) == lines.count(s.STREAM_HEADER) == 1,'missing/duplicate native tables')
    d.require(lines.index(s.WRITE_HEADER) < lines.index(s.STREAM_HEADER),'reordered native tables')
    d.require('page cache could not be dropped' not in text,'target cold cache unqualified')
    patterns = [
        r'- disk ceiling, pwrite 1 MiB, no sync: ([\d.]+) MB/s \((\d+) writes/s\)',
        r'- disk ceiling, pwrite 1 MiB \+ sync_data: ([\d.]+) MB/s \((\d+) writes/s\)',
        r'- device: pread 1 MiB, page cache: ([\d.]+) MB/s',
        r'- device: pread 1 MiB, cold \(fadvise DONTNEED per pass\): ([\d.]+) MB/s',
        r'- 4 KiB write \+ sync_data \(F_FULLFSYNC on macOS\): p50 ([\d.]+) µs, p99 ([\d.]+) µs \((\d+) samples\)',
        r'- 4 KiB write \+ plain fsync\(2\): p50 ([\d.]+) µs, p99 ([\d.]+) µs \((\d+) samples\)',
    ]
    ceilings,rows,warnings = [],[],[]
    section = None
    for line in lines:
        if line in (s.WRITE_HEADER,s.STREAM_HEADER):
            section = 'stream' if line == s.STREAM_HEADER else 'write'
            continue
        if line.startswith('|---'):
            d.require(re.fullmatch(r'\|(?:---\|){9,10}',line),'malformed table separator')
            continue
        if line.startswith('|'):
            fields = [x.strip() for x in line.strip('|').split('|')]
            stream = fields[0].startswith('stream_')
            d.require(section == ('stream' if stream else 'write') and len(fields) == (10 if stream else 9),'malformed/misplaced cell')
            row = dict(cell=fields[0],writers=int(fields[1]),value_kib=int(fields[2].removesuffix(' KiB')))
            d.require(fields[2] == f"{row['value_kib']} KiB",'wrong value units')
            names = ('ops_s','mb_s','p50_us','p99_us','p999_us') + (('records_s',) if stream else ())
            row.update(zip(names,(positive(x) for x in fields[3:-1])))
            d.require(row['p50_us'] <= row['p99_us'] <= row['p999_us'],'unordered percentiles')
            loads = [float(x) for x in fields[-1].split()]
            d.require(len(loads) == 3 and all(math.isfinite(x) and x >= 0 for x in loads),'invalid load averages')
            row['load'] = loads
            rows.append(row)
            continue
        matches = [(index,re.fullmatch(pattern,line)) for index,pattern in enumerate(patterns)]
        found = [(index,match) for index,match in matches if match]
        if found:
            index,match = found[0]
            d.require(index == len(ceilings),'reordered/duplicate ceiling')
            metrics = [positive(x) for x in match.groups()]
            d.require(index < 4 or metrics[0] <= metrics[1],'unordered ceiling latencies')
            ceilings.append(dict(kind=index,metrics=metrics,raw=line))
        elif not line.strip() or line == '# wal_write_path' or line == s.CONFIG_HEADER or line.startswith('- load average at start:'):
            continue
        elif re.fullmatch(r'- compaction_concurrent_put/\d+w/\d+k: \d+ compaction runs during the cell',line) or re.fullmatch(r'- stream_append_retention/durable/64w/1k/b1: .*',line) or line.startswith('- T4 UNJUDGEABLE for stream_append_retention/durable/64w/1k/b1:'):
            warnings.append(line)
        else:
            raise d.DiagnosticError('unknown native output line')
    expected = grid_order() if kind == 'full' else [(CELLS[kind],8 if kind == 'write' else 1,64)]*3
    d.require([(x['cell'],x['writers'],x['value_kib']) for x in rows] == expected,'missing/extra/reordered native grid')
    d.require(len(ceilings) == (6 if kind == 'full' else 0),'missing/extra ceilings')
    return dict(rows=rows,ceilings=ceilings,non_target_commentary=warnings)


def summarize(runs):
    d.require(len(runs) == 9,'incomplete invocation schedule')
    for run,planned in zip(runs,schedule()):
        d.require(all(run.get(k) == v for k,v in planned.items()),'modified invocation schedule')
        d.require(run.get('exit_code') == 0 and run.get('binary_sha256') == s.PINNED['base']['binary_sha256'],'unqualified invocation')
        expected = grid_order() if run['kind'] == 'full' else [(CELLS[run['kind']],8 if run['kind']=='write' else 1,64)]*3
        d.require([(x['cell'],x['writers'],x['value_kib']) for x in run['rows']] == expected,'modified native row inventory')
    cells = {}
    for kind,cell in CELLS.items():
        result = {}
        for metric in ('ops_s','mb_s','p99_us','p999_us'):
            blocks = []
            for block in range(3):
                selected = {r['kind']:r for r in runs if r['block'] == block}
                grid = [r[metric] for r in selected['full']['rows'] if r['cell']==cell]
                isolated = [r[metric] for r in selected[kind]['rows']]
                ratio = statistics.median(grid)/statistics.median(isolated)
                blocks.append(dict(block=block,grid=grid,isolated=isolated,grid_over_isolated=ratio,
                                   grid_median=statistics.median(grid),isolated_median=statistics.median(isolated),
                                   grid_envelope=[min(grid),max(grid)],isolated_envelope=[min(isolated),max(isolated)]))
            greater = all(b['grid_over_isolated'] >= 1.20 and min(b['grid']) > max(b['isolated']) for b in blocks)
            smaller = all(b['grid_over_isolated'] <= .80 and max(b['grid']) < min(b['isolated']) for b in blocks)
            classification = 'consistent larger grid values' if greater else 'consistent smaller grid values' if smaller else 'no consistent >=20% envelope effect under this protocol'
            result[metric] = dict(classification=classification,blocks=blocks)
        cells[cell] = result
    return dict(diagnostic_only=True,timing_eligible=False,original_acceptance='FAILED, unchanged',cells=cells,
                limits='Whole invocation envelope, ordinal-balanced only; carryover/host/allocator/ceilings not uniquely isolated. Nonclassification is not equivalence or absence of history effects.')


def invocation_budget(kind, deadline, now):
    remaining = deadline-now
    d.require(remaining > 0,'aggregate cycle deadline exhausted')
    return min(900 if kind=='full' else 120,remaining)


def qualify_host(runtime):
    d.require(platform.system()=='Linux' and platform.machine()=='x86_64' and os.geteuid()!=0,'ordinary x86_64 Linux required')
    d.require(sorted(os.sched_getaffinity(0)) == [0,1,2,3],'four expected CPUs required')
    d.quiet()
    snapshot = d.host_snapshot(runtime)
    d.require(os.stat(runtime).st_dev == os.stat('/tmp').st_dev,'native TMPDIR is on a different device from /tmp')
    st = os.statvfs(runtime)
    d.require(st.f_bavail*st.f_frsize >= 20*GIB,'insufficient initial temporary space')
    snapshot.update(kernel=platform.release(),runtime_device=os.stat(runtime).st_dev,tmp_device=os.stat('/tmp').st_dev,
                    python=sys.version,ordinary=d.resource_proof())
    return snapshot


def execute(command, env, directory, output, timeout_s):
    directory = Path(directory)
    d.require(directory.is_dir() and not (directory/'exit.json').exists(),'fresh invocation required')
    d.require(0 < timeout_s <= 900,'invalid bounded invocation deadline')
    d.dump(directory/'command.json',dict(argv=command,environment=env,timeout_s=timeout_s,privileged=False))
    process,watch,code,error,timed_out = None,None,None,None,False
    begin = time.monotonic_ns()
    cleanup_errors = []
    try:
        with (directory/'stdout.txt').open('w') as stdout,(directory/'stderr.txt').open('w') as stderr:
            process = subprocess.Popen(command,env=env,stdout=stdout,stderr=stderr,start_new_session=True)
            watch = d.CaptureWatch(directory,output,lambda:d.kill_group(process.pid))
            watch.start()
            try:
                code = process.wait(timeout=timeout_s)
            except subprocess.TimeoutExpired:
                timed_out=True
    except Exception as failure:
        error = str(failure)
    finally:
        if process:
            try:
                d.kill_group(process.pid)
                code = process.wait(timeout=5)
            except Exception as failure:
                cleanup_errors.append(str(failure))
            if watch:
                try:watch.finish()
                except Exception as failure:error=error or str(failure)
        result = dict(exit_code=code,timed_out=timed_out,begin_ns=begin,end_ns=time.monotonic_ns(),
                      pid=None if process is None else process.pid,error=error,cleanup_errors=cleanup_errors)
        d.dump(directory/'exit.json',result)
    d.require(not error and not cleanup_errors and not timed_out and code==0,'native invocation failed or contained')
    d.require(not d.budget_breach(directory,output),'postexit evidence budget exceeded')
    return result


def run(artifact, output):
    output = Path(output).resolve()
    d.require(not output.exists(),'fresh output required; no retry')
    output.mkdir(parents=True)
    deadline = time.monotonic()+CYCLE_SECONDS
    result = dict(verdict='inconclusive',diagnostic_only=True,timing_eligible=False,original_acceptance='FAILED, unchanged')
    runs = []
    try:
        d.dump(output/'original-qualified.json',verify_artifact(artifact))
        runtime = output/'runtime';runtime.mkdir()
        host = qualify_host(runtime);d.dump(output/'preflight.json',host)
        ordinary = host['ordinary'];ordinary_path=output/'ordinary-resources.json';d.dump(ordinary_path,ordinary)
        live = d.proc_identity('self',output);d.validate_identity(live,ordinary)
        binary = output/'wal-diag-base'
        shutil.copyfile(Path(artifact)/'base-benchmark-executable',binary);binary.chmod(0o755)
        d.require(d.digest(binary)==s.PINNED['base']['binary_sha256'],'copied baseline hash differs')
        linked = subprocess.run(['ldd',str(binary)],capture_output=True,text=True,check=False)
        d.dump(output/'runtime-libraries.json',dict(exit_code=linked.returncode,stdout=linked.stdout,stderr=linked.stderr))
        d.require(linked.returncode==0 and 'not found' not in linked.stdout+linked.stderr,'native libraries unqualified')
        d.dump(output/'schedule.json',schedule())
        d.dump(output/'budget.json',dict(cycle_seconds=CYCLE_SECONDS,deadline_monotonic_s=deadline,full_seconds=900,isolated_seconds=120))
        for planned in schedule():
            d.quiet()
            st=os.statvfs(runtime);d.require(st.f_bavail*st.f_frsize>=10*GIB,'temporary space budget exhausted')
            d.require(os.stat(runtime).st_dev==host['runtime_device'],'native temporary device changed')
            d.require(d.digest(binary)==s.PINNED['base']['binary_sha256'],'native executable changed')
            env=s.fixed_environment(runtime);env['SPIKE_FILTER']='' if planned['kind']=='full' else CELLS[planned['kind']]
            directory=output/f"{planned['index']:02d}-{planned['kind']}";directory.mkdir()
            d.dump(directory/'host-before.json',d.host_snapshot(runtime))
            try:
                meta=execute(d.scoped_command(binary,directory,ordinary_path,s.PINNED['base']['binary_sha256']),env,directory,output,
                             invocation_budget(planned['kind'],deadline,time.monotonic()))
            finally:after=s.snapshot_after(directory,runtime)
            d.require('capture_error' not in after,'post-invocation host evidence missing')
            d.validate_native_evidence(directory,ordinary)
            d.require(d.digest(binary)==s.PINNED['base']['binary_sha256'],'native executable changed during invocation')
            parsed=parse_output((directory/'stdout.txt').read_text(),planned['kind'])
            runs.append(dict(planned,**parsed,exit_code=meta['exit_code'],binary_sha256=s.PINNED['base']['binary_sha256'],directory=directory.name))
            d.dump(output/'runs.json',runs)
            d.require(time.monotonic() <= deadline,'aggregate cycle deadline exhausted after invocation')
        d.require(time.monotonic() <= deadline,'aggregate cycle deadline exhausted before classification')
        summary=summarize(runs);d.dump(output/'summary.json',summary)
        d.require(time.monotonic() <= deadline,'aggregate cycle deadline exhausted during classification')
        result.update(verdict='complete fixed schedule; interpretation recorded',complete=True,invocations=9,cell_rows=459)
    except Exception as failure:
        result['error']=str(failure)
        raise d.DiagnosticError(str(failure)) from failure
    finally:
        d.dump(output/'runs.json',runs);d.dump(output/'result.json',result);d.save_inventory(output)
    print(json.dumps(result,indent=2))


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--artifact',required=True);parser.add_argument('--output',required=True)
    args=parser.parse_args()
    try:run(args.artifact,args.output)
    except d.DiagnosticError as failure:
        print(f'inconclusive: {failure}',file=sys.stderr);return 1
    return 0

if __name__=='__main__':sys.exit(main())
