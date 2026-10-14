#!/usr/bin/env python3
"""One fail-closed, qualified, finite Durable append diagnostic; never gate timing."""
import argparse
import bisect
import collections
from contextlib import contextmanager
import hashlib
import io
import json
import math
import os
from pathlib import Path
import re
import resource
import shutil
import signal
import struct
import subprocess
import sys
import threading
import tarfile
import time

import wal_descriptor_diagnostic as legacy
from wal_stall_diagnostic import DiagnosticError, LOSS, digest, dump, require, fixed_environment, host_snapshot as base_host_snapshot

SOURCES = {'A':'f2a798413b0575abd6416d23a16f417a45aab70b', 'B':'947ef54eac3ccf2b93d3f2e083edf9c9f6f95e9b'}
CELLS = ('stream_append/durable/1w/1k/b1','stream_append/durable/1w/1k/b100')
OVERLAY_FILES = ('crates/prkdb/benches/wal_write_path.rs','crates/prkdb/benches/support/stream_durable_diagnostic.rs')
RAW_LIMITS = {'cpu.data':64<<20,'syscalls.data':128<<20,'scheduler.data':128<<20}
RAW_TOTAL, RENDER_FILE, RENDER_TOTAL, OBSERVER_LIMIT = 1<<30, 512<<20, 2<<30, 256<<20
LIFECYCLE, RENAME = legacy.LIFECYCLE, legacy.RENAME
SYSCALLS = ('write','pwrite64','fsync','fdatasync','futex')
SELF = str(Path(__file__).resolve())
EVENT_FIELDS = ('kind','phase','operation_id','start_ns','append_start_ns','end_ns','tid','arg','amount','outcome','path_id')
BUILD_COMMAND = ['cargo','bench','-p','prkdb','--bench','wal_write_path','--no-run','--message-format=json']


def host_snapshot(runtime):
    data=base_host_snapshot(runtime)
    data['temporary_device']=os.stat(runtime).st_dev
    data['runner_identity']={k:os.environ.get(k) for k in ('RUNNER_NAME','RUNNER_OS','RUNNER_ARCH')}
    data['governors']={}
    for path in Path('/sys/devices/system/cpu').glob('cpu[0-9]*/cpufreq/scaling_governor'):
        try:data['governors'][str(path)]=path.read_text().strip()
        except OSError as e:data['governors'][str(path)]={'unavailable':str(e)}
    data['competing_processes']=[]
    for path in Path('/proc').glob('[0-9]*/comm'):
        try:data['competing_processes'].append(dict(pid=int(path.parent.name),comm=path.read_text().strip()))
        except OSError:pass
    try:data['cpuinfo']=Path('/proc/cpuinfo').read_text()
    except OSError as e:data['cpuinfo']={'unavailable':str(e)}
    return data


def sequence():
    return [dict(index=i,phase=phase,side=side,timing_eligible=False)
            for i,(phase,side) in enumerate([('original',side) for side in 'AABBABBA']+
               [('marked',side) for side in 'AB']+[('syscalls',side) for side in 'AB']+
               [('scheduler',side) for side in 'BA']+[('cpu',side) for side in 'AB'])]


def environment(runtime, planned, observations):
    env=dict(fixed_environment(runtime),SPIKE_FILTER=CELLS[0])
    if planned['phase']!='original':
        env.update(PRKDB_STREAM_DURABLE_DIAGNOSTIC_DIR=str(observations),
                   PRKDB_STREAM_DURABLE_DIAGNOSTIC_SOURCE=SOURCES[planned['side']])
    return env


def parse_native(text):
    from wal_stall_diagnostic import CONFIG_HEADER, WRITE_HEADER, STREAM_HEADER
    lines=text.splitlines()
    require([l for l in lines if l.startswith('- warm-up')]==[CONFIG_HEADER], 'native config differs')
    require(lines.count('# wal_write_path')==lines.count(WRITE_HEADER)==lines.count(STREAM_HEADER)==1,
            'native table inventory differs')
    require(not any('UNJUDGEABLE' in l for l in lines),'unjudgeable native output')
    rows=[]
    for line in lines:
        if not line.startswith('| ') or line in (WRITE_HEADER,STREAM_HEADER): continue
        values=[v.strip() for v in line.strip('|').split('|')]
        require(len(values)==10 and values[0]==CELLS[len(rows)%2] and len(rows)<6,
                'expected exactly ordered b1,b100 three times')
        require(values[1:3]==['1','1 KiB'],'native writer/payload differs')
        try:
            numbers=[float(v) for v in values[3:9]]
            loads=[float(v) for v in values[9].split()]
        except ValueError as e: raise DiagnosticError('invalid native metrics') from e
        require(len(numbers)==6 and all(math.isfinite(v) and v>0 for v in numbers),'invalid native metrics')
        require(len(loads)==3 and all(math.isfinite(v) and v>=0 for v in loads),'invalid native load triple')
        require(numbers[2]<=numbers[3]<=numbers[4],'unordered native percentiles')
        rows.append(dict(cell=values[0],repetition=len(rows)//2,load1=values[9],load_averages=loads,
                         **dict(zip(('ops_s','mb_s','p50_us','p99_us','p999_us','records_s'),numbers))))
    require(len(rows)==6,'missing native rows')
    return rows


class Deadline:
    def __init__(self, end_ns=None, cleanup_end_ns=None):
        self.end_ns=time.monotonic_ns()+1200*10**9 if end_ns is None else end_ns
        self.cleanup_end_ns=self.end_ns if cleanup_end_ns is None else cleanup_end_ns
    def remaining(self):
        remaining=(self.end_ns-time.monotonic_ns())/1e9
        require(remaining>0,'nonresetting aggregate deadline exhausted')
        return remaining
    def timeout(self): return min(120.0,self.remaining())


def observer_files(output):
    # os.walk pruning happens BEFORE descent, unlike rglob followed by filtering.
    root=Path(output)
    for directory,dirs,files in os.walk(root,followlinks=False):
        if Path(directory)==root:
            dirs[:]=[d for d in dirs if d!='runtime']
        dirs[:]=[d for d in dirs if not (Path(directory)/d).is_symlink()]
        for name in files:
            p=Path(directory)/name
            if not p.is_symlink(): yield p


def budget_breach(directory, output):
    raw=rendered=0
    observer=collections.Counter()
    for p in observer_files(output):
        size=p.stat().st_size
        if p.name in RAW_LIMITS:
            raw+=size
            if size>RAW_LIMITS[p.name]: return f'raw file cap: {p}'
        elif p.name not in ('wal-diag-A','wal-diag-B','wal-diag-smoke'):
            rendered+=size
            if size>RENDER_FILE: return f'rendered file cap: {p}'
        if p.name in ('events.bin','metadata.json'):
            call=p.parts[:p.parts.index('observations')] if 'observations' in p.parts else p.parts[:-1]
            observer[call]+=size
    if raw>RAW_TOTAL:return 'raw aggregate cap'
    if rendered>RENDER_TOTAL:return 'rendered aggregate cap'
    if any(n>OBSERVER_LIMIT for n in observer.values()):return 'observer process cap'
    return None


def inventory(output,deadline=None):
    rows=[]
    for p in observer_files(output):
        if p.name=='inventory.json':continue
        rows.append(dict(path=str(p.relative_to(output)),bytes=p.stat().st_size,sha256=checked_digest(p,deadline)))
    dump(Path(output)/'inventory.json',dict(runtime_pruned_before_traversal=True,files=rows))


def checked_digest(path,deadline=None):
    h=hashlib.sha256()
    with Path(path).open('rb') as source:
        for chunk in iter(lambda:source.read(1024*1024),b''):
            if deadline is not None:deadline.remaining()
            h.update(chunk)
    return h.hexdigest()


def run_deadlines():
    final=Deadline()
    return Deadline(final.end_ns-60*10**9,cleanup_end_ns=final.end_ns),final


def contain_owned_root(directory,deadline):
    directory=Path(directory)
    proof=dict(actual_group_known=False,qualified_cleanup=False,uncontained=True,signal=None,probe=None)
    try:
        group=legacy.read_owned_group(directory/'root-group.json')
        require(group is not None,'actual root group witness unavailable')
        proof.update(actual_group_known=True,actual_group=group)
        for name,flags in [('signal',['-KILL']),('probe',['-0'])]:
            argv=['sudo','-n','kill',*flags,'--',f'-{group}']
            timeout=min(5,deadline.remaining())
            result=subprocess.run(argv,capture_output=True,text=True,check=False,timeout=timeout,start_new_session=True)
            proof[name]=dict(argv=argv,exit_code=result.returncode,stdout=result.stdout,stderr=result.stderr,timeout_s=timeout)
        def absent(result):return result['exit_code']!=0 and 'no such process' in result['stderr'].lower()
        accepted=proof['signal']['exit_code']==0 or absent(proof['signal'])
        proof['qualified_cleanup']=accepted and absent(proof['probe'])
        proof['uncontained']=not proof['qualified_cleanup']
    except (DiagnosticError,OSError,ValueError,KeyError,subprocess.SubprocessError) as error:
        proof['error']=('cleanup timeout: ' if isinstance(error,subprocess.TimeoutExpired) else '')+str(error)
    finally:dump(directory/'root-containment.json',proof)
    require(proof['qualified_cleanup'],'bounded root cleanup unqualified; prefix and signal/probe/deadline evidence retained')
    return proof


def ownership_command(output,directory,deadline):
    return ['sudo','-n','env','-i','PATH='+os.environ.get('PATH','/usr/bin:/bin'),'LANG=C.UTF-8',
            'python3',SELF,'ownership-supervisor','--output',str(output),'--directory',str(directory),
            '--owner',f'{os.getuid()}:{os.getgid()}','--deadline-ns',str(deadline.end_ns)]


def ownership_cleanup(output,final_deadline):
    directory=Path(output)/'finalization';directory.mkdir()
    # Reserve 15 seconds for containment/reaping and final hashing/metadata.
    command_deadline=Deadline(final_deadline.end_ns-15*10**9)
    command=ownership_command(output,directory,command_deadline)
    dump(directory/'command.json',dict(argv=command,deadline_ns=command_deadline.end_ns))
    p=None;code=None;error=None;cleanup=[];root_contained=None
    try:
        with (directory/'stdout.txt').open('w') as stdout,(directory/'stderr.txt').open('w') as stderr:
            p=subprocess.Popen(command,stdout=stdout,stderr=stderr,start_new_session=True)
            code=p.wait(timeout=command_deadline.timeout())
    except (OSError,DiagnosticError,subprocess.SubprocessError) as failure:
        error=str(failure)
        if p is not None and (directory/'root-group.json').exists():
            try:
                proof=contain_owned_root(directory,Deadline(final_deadline.end_ns-10*10**9))
                root_contained=proof['qualified_cleanup']
            except (OSError,DiagnosticError,subprocess.SubprocessError) as failure:cleanup.append(str(failure))
        elif p is not None and command[:1]==['sudo']:
            cleanup.append('ownership actual root group unknown')
    finally:
        if p is not None:
            try:
                legacy.kill_group(p.pid);code=p.wait(timeout=min(5,final_deadline.remaining()))
            except (OSError,DiagnosticError,subprocess.SubprocessError) as failure:cleanup.append(str(failure))
        proof=dict(exit_code=code,error=error,cleanup_errors=cleanup,outer_cleanup_complete=not cleanup,
                   root_contained=root_contained,remaining_aggregate_s=max(0,(final_deadline.end_ns-time.monotonic_ns())/1e9))
        dump(directory/'exit.json',proof);dump(Path(output)/'ownership.json',proof)
    require(code==0 and error is None and not cleanup,'ownership repair failed; contained cleanup prefix retained')


def ownership_targets(output,owner):
    # Native runtime entries already belong to the ordinary native UID. Prune
    # their top-level directory before asking chown to recurse through observers.
    return ['chown','-R',owner,'--',*[str(p) for p in Path(output).iterdir() if p.name!='runtime']]


def ownership_supervisor(output,directory,owner,deadline):
    require(os.geteuid()==0,'ownership supervisor requires root')
    require(re.fullmatch(r'\d+:\d+',owner),'invalid ordinary ownership identity')
    directory=Path(directory)
    before=legacy.resource_proof();dump(directory/'root-before.json',before)
    code=legacy.supervise(ownership_targets(output,owner),directory/'root-group.json',timeout=deadline.timeout())
    after=legacy.resource_proof();dump(directory/'root-after.json',after)
    uid,gid=map(int,owner.split(':'));os.chown(directory/'root-after.json',uid,gid)
    require(before['nofile']==after['nofile'],'ownership supervisor changed root NOFILE')
    return code


def finalize_metadata(output,runs,result,deadline):
    failure=None
    try:
        deadline.remaining()
        dump(Path(output)/'runs.json',runs);dump(Path(output)/'result.json',result)
        inventory(output,deadline)
        breach=budget_breach(output,output)
        require(not breach,breach)
        deadline.remaining()
    except (OSError,DiagnosticError) as error:
        failure=error;result.update(complete=False,verdict='inconclusive',finalization_error=str(error))
        dump(Path(output)/'result.json',result)
        # Update the inventory to the final failure result if budget/time allows.
        try:
            result['post_write_cap_error']=budget_breach(output,output)
            dump(Path(output)/'result.json',result)
            inventory(output,deadline)
            require(not budget_breach(output,output),'final failure prefix remains over evidence caps')
        except (OSError,DiagnosticError) as nested:
            result['inventory_error']=str(nested);dump(Path(output)/'result.json',result)
    if failure:raise DiagnosticError(str(failure)) from failure


def git(root,*args):
    return subprocess.check_output(['git','-C',str(root),*args],text=True,timeout=120).strip()


def source_hashes(root):
    names=subprocess.check_output(['git','-C',str(root),'ls-files','-z']).decode().split('\0')
    return {name:digest(Path(root)/name) for name in names if name}


def snapshot_source(root,side,output):
    root=Path(root).resolve()
    require(git(root,'rev-parse','HEAD')==SOURCES[side],'exact source SHA differs')
    require(not git(root,'status','--porcelain','--untracked-files=all'),'source snapshot is dirty')
    dump(output,dict(side=side,root=str(root),source_sha=SOURCES[side],tree_sha=git(root,'rev-parse','HEAD^{tree}'),
                     source_file_hashes=source_hashes(root),clean=True))


def collect_manifest(args,record_runtime=False,deadline=None):
    evidence=Path(args.evidence).resolve();overlay=Path(args.overlay).resolve()
    files={str(p.relative_to(overlay)):digest(p) for p in overlay.rglob('*') if p.is_file()}
    require(set(files)==set(OVERLAY_FILES),'overlay must contain exactly the two benchmark files')
    manifest=dict(schema_version=1,evidence_root=str(evidence),source_paths={},sources={},overlay_root=str(overlay),overlay_files=files,members={},binaries={},builds={})
    def member(p):
        p=Path(p).absolute();require(p.is_file() and not p.is_symlink(),'missing or linked provenance member')
        manifest['members'][str(p)]=digest(p);return p
    for side in SOURCES:
        p=member(getattr(args,'source_'+side.lower()));source=json.loads(p.read_text());manifest['source_paths'][side]=str(p)
        require(source.get('source_sha')==SOURCES[side] and source.get('side')==side and source.get('clean') is True,
                'immutable source proof differs')
        root=Path(source['root'])
        require(git(root,'rev-parse','HEAD')==SOURCES[side] and git(root,'rev-parse','HEAD^{tree}')==source['tree_sha'],
                'source Git/tree identity differs')
        require(set(source['source_file_hashes'])==set(git(root,'ls-tree','-r','--name-only',SOURCES[side]).splitlines()),
                'immutable source inventory incomplete/extra')
        archive=subprocess.check_output(['git','-C',str(root),'archive','--format=tar',SOURCES[side]],
                                        timeout=120 if deadline is None else deadline.timeout())
        actual_hashes={}
        with tarfile.open(fileobj=io.BytesIO(archive),mode='r:') as tar:
            for item in tar:
                if item.isdir():continue
                require(item.isfile(),'unsupported linked source blob')
                actual_hashes[item.name]=hashlib.sha256(tar.extractfile(item).read()).hexdigest()
        require(actual_hashes==source['source_file_hashes'],'source blob inventory/hashes differ from immutable Git')
        if not record_runtime:
            # Builds are complete; both source worktrees must still contain only
            # the exact common benchmark overlay and immutable production tree.
            expected_current=dict(actual_hashes);expected_current.update(files)
            current=source_hashes(root)
            current.update({name:digest(root/name) for name in OVERLAY_FILES})
            require(current==expected_current,'post-build production/source tree changed')
        manifest['sources'][side]=source
    for p in overlay.rglob('*'):
        if p.is_file():member(p)
    for variant in ('original','marked'):
        for side in SOURCES:
            key=f'{variant}-{side.lower()}'
            paths={suffix:member(evidence/f'build-{key}.{suffix}') for suffix in ('jsonl','stderr','exit','proof.json')}
            require(paths['exit'].read_text().strip()=='0','unqualified build exit')
            proof=json.loads(paths['proof.json'].read_text())
            require(proof.get('command')==BUILD_COMMAND and proof.get('feature_args')==[],'build command/features differ')
            require('release: 1.98.1' in proof.get('rustc_vv','') and 'host: x86_64-unknown-linux-gnu' in proof['rustc_vv'],
                    'compiler identity differs')
            require(proof.get('cargo_v','').startswith('cargo 1.98.1'),'cargo identity differs')
            env=proof.get('environment',{})
            require(env.get('CARGO_INCREMENTAL')=='0' and env.get('RUSTFLAGS') is None,'build environment differs')
            require(Path(env.get('CARGO_TARGET_DIR','')).resolve()==Path(proof['source_root']).resolve()/'target',
                    'build cache is not isolated to source worktree')
            require(str(Path(proof['source_root']).resolve())==manifest['sources'][side]['root'],'wrong build source root')
            expected=dict(manifest['sources'][side]['source_file_hashes'])
            if variant=='marked':expected.update(files)
            require(proof.get('source_file_hashes')==expected,'production/source/overlay hash mismatch')
            messages=[json.loads(l) for l in paths['jsonl'].read_text().splitlines() if l.strip()]
            require(sum(m.get('reason')=='build-finished' and m.get('success') is True for m in messages)==1,
                    'missing successful compiler JSON')
            artifacts=[m for m in messages if m.get('reason')=='compiler-artifact' and m.get('target',{}).get('name')=='wal_write_path'
                       and 'bench' in m.get('target',{}).get('kind',[]) and m.get('executable')]
            require(len(artifacts)==1 and artifacts[0]['executable']==proof.get('cargo_executable'),'benchmark artifact identity differs')
            target=artifacts[0]['target']
            require(target.get('kind')==['bench'] and target.get('crate_types')==['bin'] and target.get('test') is False and
                    Path(target.get('src_path','')).resolve()==(Path(proof['source_root'])/OVERLAY_FILES[0]).resolve(),
                    'compiler benchmark target/source differs')
            profile=artifacts[0].get('profile',{})
            require(profile.get('opt_level')=='3' and profile.get('test') is True,
                    'compiler artifact is not the original optimized benchmark')
            binary=member(getattr(args,key.replace('-','_')))
            require(digest(binary)==proof.get('executable_sha256'),'compiled/copied executable differs')
            with binary.open('rb') as f:elf=f.read(20)
            require(elf[:7]==b'\x7fELF\x02\x01\x01' and elf[18:20]==b'\x3e\x00','ELF64 x86_64 required')
            libproof=evidence/f'{key}-runtime-libraries.json'
            if record_runtime:
                linked=subprocess.run(['ldd',str(binary)],capture_output=True,text=True,check=False,timeout=120)
                dump(libproof,dict(exit_code=linked.returncode,stdout=linked.stdout,stderr=linked.stderr))
            linked=json.loads(libproof.read_text())
            require(linked.get('exit_code')==0 and 'not found' not in linked.get('stdout','')+linked.get('stderr',''),
                    'runtime linkage unqualified')
            member(libproof)
            # Retain runtime dependency content, not just mutable ldd path strings.
            libraries={str(Path(p).resolve()):digest(p) for p in re.findall(r'(/[^\s()]+)',linked['stdout']) if Path(p).is_file()}
            require(libraries,'runtime libraries absent')
            manifest['binaries'][key]=dict(path=str(binary),sha256=digest(binary),runtime_libraries=libraries)
            manifest['builds'][key]=dict(proof,compiler_artifact=artifacts[0])
    profiles=[(p['compiler_artifact']['profile'],p['compiler_artifact']['features']) for p in manifest['builds'].values()]
    require(all(p==profiles[0] for p in profiles),'compiler profile/features differ')
    compilers={p['rustc_vv'] for p in manifest['builds'].values()}
    require(len(compilers)==1,'build compiler configuration differs')
    common_env=[]
    for p in manifest['builds'].values():
        env=dict(p['environment']);env.pop('CARGO_TARGET_DIR',None);common_env.append(env)
    require(all(e==common_env[0] for e in common_env),'build environment differs across sources/variants')
    require(len({item['path'] for item in manifest['binaries'].values()})==4,'four distinct retained executable paths required')
    return manifest


def build_manifest(args):
    manifest=collect_manifest(args,record_runtime=True)
    dump(args.output,manifest)
    verify_manifest(manifest)


def verify_manifest(manifest,deadline=None):
    try:
        require(manifest.get('schema_version')==1,'wrong manifest schema')
        require(set(manifest.get('binaries',{}))=={'original-a','original-b','marked-a','marked-b'},'four executables required')
        require(set(manifest.get('sources',{}))==set(SOURCES) and set(manifest.get('source_paths',{}))==set(SOURCES),
                'complete immutable source graph required')
        args=argparse.Namespace(evidence=manifest['evidence_root'],overlay=manifest['overlay_root'],
              source_a=manifest['source_paths']['A'],source_b=manifest['source_paths']['B'],
              **{key.replace('-','_'):item['path'] for key,item in manifest['binaries'].items()})
        expected=collect_manifest(args,record_runtime=False,deadline=deadline)
        require(expected==manifest,'manifest graph differs from complete retained provenance')
        return manifest
    except (OSError,ValueError,KeyError,TypeError,subprocess.SubprocessError) as error:
        raise DiagnosticError(f'incomplete/invalid provenance graph: {error}') from error


def seal_manifest(manifest):
    frozen=json.dumps(manifest,sort_keys=True,separators=(',',':')).encode()
    members=dict(manifest['members'])
    for item in manifest['binaries'].values():members.update(item['runtime_libraries'])
    return dict(graph_sha256=hashlib.sha256(frozen).hexdigest(),members=members)


def verify_seal(manifest,seal):
    frozen=json.dumps(manifest,sort_keys=True,separators=(',',':')).encode()
    require(hashlib.sha256(frozen).hexdigest()==seal['graph_sha256'],'qualified provenance graph changed')
    for path,sha in seal['members'].items():
        require(Path(path).is_file() and not Path(path).is_symlink() and digest(path)==sha,'qualified provenance member changed')


def profile_command(category,binary,directory,ordinary_path,sha256,arguments=()):
    directory=Path(directory)
    require(category in ('cpu','syscalls','scheduler'),'unknown recorder category')
    require(Path(binary).name.startswith('wal-diag-') and len(Path(binary).name.encode())<=15,'native alias exceeds comm')
    cmd=['perf','record','--clockid','mono','--sample-cpu','--timestamp','--switch-events','-m','8M','-o',str(directory/f'{category}.data')]
    if category=='cpu':cmd+=['--strict-freq','-e','cpu-clock:u','-F','99','--call-graph','dwarf,8192']
    elif category=='syscalls':
        cmd+=['-c','1']
        for name in SYSCALLS:
            for phase in ('enter','exit'):cmd+=['-e',f'syscalls:sys_{phase}_{name}']
    else:
        cmd+=['-a','-e','sched:sched_switch','--filter',legacy.filter_for(('prev_comm','next_comm')),
              '-e','sched:sched_wakeup','--filter',legacy.filter_for(('comm',))]
    for event in (*LIFECYCLE,RENAME):cmd+=['-e',event]
    return cmd+['--',*legacy.scoped_command(binary,directory,ordinary_path,sha256,arguments)]


def root_command(command,env,directory,ordinary_path,output,deadline):
    allowed={'PATH','LANG','TMPDIR','SPIKE_REPS','SPIKE_WARMUP_MS','SPIKE_MEASURE_MS','TOKIO_WORKER_THREADS',
             'SPIKE_FILTER','PRKDB_STREAM_DURABLE_DIAGNOSTIC_DIR','PRKDB_STREAM_DURABLE_DIAGNOSTIC_SOURCE'}
    require(set(env)<=allowed,'unexpected privileged environment')
    return ['sudo','-n','env','-i',*[f'{k}={v}' for k,v in sorted(env.items())],
            'python3',SELF,'supervisor','--proof',str(ordinary_path),'--directory',str(directory),
            '--output',str(output),'--deadline-ns',str(deadline.end_ns),'--',*command]


def execute(command,env,directory,output,deadline,privileged=False):
    with helper_scope():
        return _execute(command,env,directory,output,deadline,privileged)


def _execute(command,env,directory,output,deadline,privileged=False):
    directory=Path(directory);require(not (directory/'exit.json').exists(),'fresh invocation required')
    timeout=deadline.timeout();dump(directory/'command.json',dict(argv=command,environment=env,timeout_s=timeout,privileged=privileged))
    begin=time.monotonic_ns();p=None;watch=None;code=None;error=None;cleanup=[]
    try:
        with (directory/'stdout.txt').open('w') as stdout,(directory/'stderr.txt').open('w') as stderr:
            p=subprocess.Popen(command,env=env,stdout=stdout,stderr=stderr,start_new_session=True)
            if not privileged:
                watch=legacy.CaptureWatch(directory,output,lambda:legacy.kill_group(p.pid));watch.start()
            code=p.wait(timeout=timeout)
    except (DiagnosticError,OSError,ValueError,subprocess.SubprocessError) as e:
        error=str(e)
        if privileged and p is not None:
            try:contain_owned_root(directory,Deadline(min(deadline.end_ns+10*10**9,deadline.cleanup_end_ns)))
            except (DiagnosticError,OSError,subprocess.SubprocessError) as failure:cleanup.append(str(failure))
    finally:
        if p is not None:
            try:legacy.kill_group(p.pid);code=p.wait(timeout=5)
            except (DiagnosticError,OSError,subprocess.SubprocessError) as e:cleanup.append(str(e))
        if watch:
            try:watch.finish()
            except (DiagnosticError,OSError) as e:error=error or str(e)
        breach=budget_breach(directory,output)
        if breach:error=error or breach
        result=dict(exit_code=code,begin_ns=begin,end_ns=time.monotonic_ns(),error=error,cleanup_errors=cleanup,
                    outer_cleanup_complete=not cleanup,pid=None if p is None else p.pid)
        dump(directory/'exit.json',result)
    require(not error and not cleanup and code==0,f'invocation failed; preserved prefix: {error}, exit={code}, cleanup={cleanup}')
    deadline.remaining()
    return result


def native_lifetimes(rows,binary,pid):
    execs=[r for r in rows if r['event']==LIFECYCLE[0] and r['fields'].get('filename')==str(binary) and int(r['fields'].get('pid',-1))==pid]
    require(len(execs)==1,'missing exact native exec')
    begin=execs[0]['stamp']
    exits=[r for r in rows if r['event']==LIFECYCLE[2] and int(r['fields'].get('pid',-1))==pid and r['stamp']>begin]
    require(len(exits)==1,'missing native exit')
    end=exits[0]['stamp'];births={pid:begin};deaths={pid:end};names={pid:[(begin,Path(binary).name)]}
    relevant=[r for r in rows if begin<=r['stamp']<=end]
    for r in relevant:
        f=r.get('fields',{})
        if r['event']==LIFECYCLE[1] and int(f['pid']) in births:
            tid=int(f['child_pid']);require(tid not in births,'native thread reuse unsupported')
            births[tid]=r['stamp'];names[tid]=[(r['stamp'],f['child_comm'])]
        if r['event']==LIFECYCLE[2] and int(f['pid']) in births:deaths[int(f['pid'])]=r['stamp']
    require(set(births)==set(deaths),'missing native thread lifetime')
    for tid in births:
        if tid!=pid:
            forks=[r for r in rows if r['event']=='FORK' and r['body_tid']==tid and r['body_pid']==pid and r['parent_tid'] in births
                   and births[r['parent_tid']]<=r['stamp']<=deaths[tid]]
            require(len(forks)==1,'missing native timed FORK metadata')
        task_exits=[r for r in rows if r['event']=='EXIT' and r['body_tid']==tid and r['body_pid']==pid and r['stamp']>=births[tid]]
        require(len(task_exits)==1,'missing native timed EXIT metadata')
    comm_exec=[r for r in rows if r['event']=='COMM' and r['comm_exec'] and r['body_pid']==pid and r['body_tid']==pid
               and r['name']==Path(binary).name and 0<r['stamp']<=end]
    require(len(comm_exec)==1,'missing native timed COMM exec')
    renames=[r for r in relevant if r['event']==RENAME and
             (int(r['fields'].get('pid',-1)) in births or ('pid' not in r['fields'] and r['pid']==pid and r['tid'] in births))]
    consumed=set();brackets={tid:[] for tid in births};previous={tid:(births[tid],names[tid][0][1]) for tid in births}
    for r in rows:
        if r['event']!='COMM' or r['body_pid']!=pid:continue
        tid=r['body_tid'];require(tid in births,'COMM for native thread missing lifetime')
        if not births[tid]<=r['stamp']<=deaths[tid]:continue
        require(legacy.covered(r['name']),'uncovered native name')
        names[tid].append((r['stamp'],r['name']))
        if r['comm_exec']:previous[tid]=(r['stamp'],r['name']);continue
        old_stamp,old=previous[tid];matches=[]
        for i,rename in enumerate(renames):
            f=rename['fields'];target=int(f['pid'])==tid if 'pid' in f else rename['pid']==r['pid'] and rename['tid']==r['tid']
            if target and f.get('oldcomm')==old and f.get('newcomm')==r['name'] and old_stamp<=rename['stamp']<=r['stamp']:matches.append(i)
        require(len(matches)==1 and matches[0] not in consumed,'unpaired native timed rename')
        i=matches[0];consumed.add(i);brackets[tid].append(dict(begin_ns=renames[i]['stamp'],end_ns=r['stamp'],old=old,new=r['name']))
        previous[tid]=(r['stamp'],r['name'])
    require(len(consumed)==len(renames),'rename missing timed COMM')
    require(all(legacy.covered(n) for history in names.values() for _,n in history),'uncovered native thread name')
    return dict(window_ns=[begin,end],births=births,deaths=deaths,names=names,brackets=brackets)


def syscall_return(detail):
    match=re.fullmatch(r'(?:ret(?:=|:\s*))?(-?(?:0x[0-9a-fA-F]+|\d+))',detail.strip())
    require(match is not None,'missing/malformed syscall return status')
    token=match.group(1);value=int(token,0) if '0x' in token else int(token)
    require(-(1<<63)<=value<(1<<64),'syscall return exceeds qualified x86_64 ABI')
    if value>=(1<<63):value-=1<<64
    return value


def analyze_category(category,text,binary,pid):
    require(not LOSS.search(text),'lost/throttled trace')
    rows=[]
    for line in text.splitlines():
        if line.strip() and not line.startswith('#'):
            rows.extend(legacy.parse_records(line,True))
    require(rows,'empty category trace')
    # perf merges per-CPU streams; sort explicitly by the retained timestamps.
    rows.sort(key=lambda r:r['stamp'])
    life=native_lifetimes(rows,binary,pid)
    births,deaths=life['births'],life['deaths']
    def inside(r,tid):return tid in births and births[tid]<=r['stamp']<=deaths[tid]
    common={*LIFECYCLE,RENAME,'COMM','FORK','EXIT','PERF_RECORD_SWITCH','PERF_RECORD_SWITCH_CPU_WIDE'}
    category_events={'cpu':{'cpu-clock:u'},'syscalls':{f'syscalls:sys_{p}_{n}' for n in SYSCALLS for p in ('enter','exit')},
                     'scheduler':{'sched:sched_switch','sched:sched_wakeup'}}[category]
    # Deterministic fixtures may retain scheduler metadata in another category;
    # recording command itself remains strictly separate. Ignore such samples.
    allowed=common|category_events|{'sched:sched_switch','sched:sched_wakeup'}
    require(all(r['event'] in allowed for r in rows),'unexpected recorder event')
    result=dict(category=category,native_pid=pid,thread_ids=sorted(births),window_ns=life['window_ns'],
                thread_lifetimes_ns={str(t):[births[t],deaths[t]] for t in births},
                thread_names={str(t):list(dict.fromkeys(n for _,n in history)) for t,history in life['names'].items()},
                name_coverage_complete=True,timing_eligible=False)
    if category=='cpu':
        samples=[r for r in rows if r['event']=='cpu-clock:u' and r['pid']==pid and inside(r,r['tid'])]
        require(samples,'missing native CPU samples')
        result.update(cpu_sample_count=len(samples),samples=samples,
                      limits='Sampling identifies executed functions; absence or inlining does not prove zero cost.')
    elif category=='syscalls':
        pending={};pairs=[]
        # Pair whole recording FIRST; markers are applied only afterwards.
        for r in rows:
            if r['event'] not in category_events or r['pid']!=pid or not inside(r,r['tid']):continue
            phase,name=r['event'].removeprefix('syscalls:sys_').split('_',1);key=r['tid']
            if phase=='enter':
                require(key not in pending,'overlapping native syscall');pending[key]=(name,r)
            else:
                require(key in pending and pending[key][0]==name,'unpaired native syscall exit')
                _,entry=pending.pop(key)
                return_status=syscall_return(r['detail'])
                pairs.append(dict(name=name,tid=key,begin_ns=entry['stamp'],end_ns=r['stamp'],
                                  enter_detail=entry['detail'],exit_detail=r['detail'],return_value=return_status,**{'return':return_status}))
        require(not pending and pairs,'missing/unfinished native syscall pairing')
        result.update(pairs=pairs,limits='Pairs retained across markers. Separate captures cannot be summed as one operation critical path.')
    else:
        sleeping={};wake={};off=[];ready=[]
        def check_name(tid,stamp,name):
            valid=[n for when,n in life['names'][tid] if when<=stamp]
            bracket={n for b in life['brackets'][tid] if b['begin_ns']<=stamp<=b['end_ns'] for n in (b['old'],b['new'])}
            require(valid and (name==valid[-1] or name in bracket),'native scheduler name disagreement')
        for r in rows:
            f=r.get('fields',{});stamp=r['stamp']
            if r['event']=='sched:sched_wakeup' and f.get('success')!='0':
                tid=int(f['pid'])
                if inside(r,tid):check_name(tid,stamp,f['comm']);wake[tid]=stamp
            if r['event']=='sched:sched_switch':
                prev,nxt=int(f['prev_pid']),int(f['next_pid'])
                if inside(r,prev):
                    check_name(prev,stamp,f['prev_comm']);require(prev not in sleeping,'duplicate native switch-out')
                    sleeping[prev]=(stamp,f['prev_state'])
                if inside(r,nxt):
                    check_name(nxt,stamp,f['next_comm'])
                    if nxt in sleeping:
                        start,state=sleeping.pop(nxt);off.append(dict(tid=nxt,begin_ns=start,end_ns=stamp,state=state))
                    if nxt in wake:ready.append(dict(tid=nxt,begin_ns=wake.pop(nxt),end_ns=stamp))
        # Switch-out at thread exit has no next run and is explicitly bounded by exit.
        for tid,(start,state) in sleeping.items():
            off.append(dict(tid=tid,begin_ns=start,end_ns=deaths[tid],state=state,exit_bounded=True))
        require(off,'no native scheduler intervals')
        result.update(offcpu=off,wakeup_to_run=ready,limits='Off-CPU includes kernel/sync waiting; it overlaps VFS elapsed and must not be added to it.')
    return result


def select_intervals(intervals,window):
    begin,end=window
    return [dict(i,clipped_begin_ns=max(begin,i['begin_ns']),clipped_end_ns=min(end,i['end_ns']),
                 crosses_marker=i['begin_ns']<begin or i['end_ns']>end)
            for i in intervals if i['end_ns']>=begin and i['begin_ns']<=end]


def read_events(path):
    size=Path(path).stat().st_size
    require(size<=OBSERVER_LIMIT and size%88==0,'malformed/breached binary event prefix')
    result=[]
    with Path(path).open('rb') as f:
        while True:
            record=f.read(88)
            if not record:break
            result.append(dict(zip(EVENT_FIELDS,struct.unpack('<9QqQ',record))))
    return result


def validate_observations(metadata,events,source,repetition,pid):
    require(metadata.get('schema_version')==1 and metadata.get('record_bytes')==88 and metadata.get('byte_order')=='little',
            'observer schema mismatch')
    require(metadata.get('clock')=='CLOCK_MONOTONIC','observer clock is unqualified')
    witness=metadata.get('clock_witness',{})
    require(all(isinstance(witness.get(k),int) for k in ('before_ns','anchor_ns','after_ns','uncertainty_ns')),
            'missing monotonic conversion witness')
    require(0<witness['before_ns']<=witness['anchor_ns']<=witness['after_ns'] and
            witness['uncertainty_ns']>=witness['after_ns']-witness['before_ns'],'invalid monotonic witness')
    require(metadata.get('source')==source and metadata.get('cell')==CELLS[0] and metadata.get('repetition')==repetition
            and metadata.get('pid')==pid,'observer identity mismatch')
    config=dict(writers=1,value_bytes=1024,batch=1,sync_mode='Durable',segment_bytes=268435456,sync_interval_ms=10,warmup_ms=1000,measure_ms=3000)
    require(metadata.get('config')==config,'observer config mismatch')
    require(metadata.get('complete') is True and metadata.get('failed') is False and metadata.get('failure') is None,
            'incomplete/breached observer prefix')
    require(metadata.get('record_count')==len(events) and metadata.get('buffer_limit_bytes')==OBSERVER_LIMIT,'observer inventory mismatch')
    require(all(e['kind'] in (1,2,3,4,5,7) and e['phase'] in (0,1) and e['tid']>0 and e['start_ns']<=e['end_ns'] for e in events),
            'malformed/breached observation')
    measured=[e for e in events if e['phase']==1]
    begins=[e for e in measured if e['kind']==1];ends=[e for e in measured if e['kind']==2]
    require(len(begins)==len(ends)==1 and begins[0]['start_ns']<=ends[0]['end_ns'],'missing/ambiguous markers')
    begin,end=begins[0]['start_ns'],ends[0]['end_ns']
    ops=[e for e in measured if e['kind']==3]
    require(len(ops)==metadata['phase_totals']['measurement'] and ops,'operation total mismatch')
    require(sum(e['kind']==3 and e['phase']==0 for e in events)==metadata['phase_totals']['warmup'],'warmup total mismatch')
    require(len({e['operation_id'] for e in ops})==len(ops) and all(e['operation_id']>0 for e in ops),'duplicate/invalid operation ID')
    require(ends[0]['amount']==len(ops),'end marker total mismatch')
    paths=metadata.get('paths',[]);result=[]
    for op in ops:
        require(begin<=op['start_ns']<=op['append_start_ns']<=op['end_ns']<=end and op['outcome']==0 and op['amount']==1,
                'operation interval/ack/error mismatch')
        returns=[e for e in measured if e['kind']==7 and e['operation_id']==op['operation_id']]
        require(len(returns)==1 and returns[0]['start_ns']==returns[0]['end_ns']==op['end_ns'],'missing/mismatched return TID witness')
        vfs=[e for e in measured if e['kind'] in (4,5) and e['operation_id']==op['operation_id']]
        sync=[e for e in vfs if e['kind']==5];write=[e for e in vfs if e['kind']==4]
        complete=len(sync)==1 and bool(write) and all(op['append_start_ns']<=e['start_ns']<=e['end_ns']<=op['end_ns'] and
                    1<=e['path_id']<=len(paths) and e['outcome']==0 for e in vfs)
        ordered=sorted(vfs,key=lambda e:e['start_ns'])
        complete=complete and all(a['end_ns']<=b['start_ns'] for a,b in zip(ordered,ordered[1:]))
        sync_ns=sum(e['end_ns']-e['start_ns'] for e in sync) if complete else None
        write_ns=sum(e['end_ns']-e['start_ns'] for e in write) if complete else None
        result.append(dict(operation_id=op['operation_id'],tid=op['tid'],return_tid=returns[0]['tid'],begin_ns=op['start_ns'],append_start_ns=op['append_start_ns'],
                           end_ns=op['end_ns'],preparation_ns=op['append_start_ns']-op['start_ns'],
                           append_ns=op['end_ns']-op['append_start_ns'],sync_ns=sync_ns,write_ns=write_ns,
                           mixed_residual_ns=op['end_ns']-op['append_start_ns']-sync_ns-write_ns if complete else None,
                           attribution_complete=bool(complete),vfs=vfs))
    extras=[e for e in measured if e['kind'] in (4,5) and e['operation_id'] not in {op['operation_id'] for op in ops}]
    if extras:
        for op in result:op['attribution_complete']=False;op['mixed_residual_ns']=None
    return dict(window_ns=[begin,end],operations=result,unmatched_vfs=extras,clock_witness=witness,
                limits='Sync elapsed overlaps kernel/scheduler waits. Total minus write/sync is a mixed residual, never queue residence.')


def render_commands(category,directory):
    data=str(Path(directory)/f'{category}.data')
    fields='comm,pid,tid,cpu,time,event'+(',trace' if category!='cpu' else '')
    commands={'script':['perf','script','-i',data,'--ns','--show-task-events','--hide-call-graph','-F',fields],
              'attributes':['perf','evlist','-i',data,'-v'],
              'raw-events':['perf','script','-i',data,'-D']}
    if category=='cpu':
        commands['callchains']=['perf','script','-i',data,'--ns','-F','comm,pid,tid,cpu,time,event,ip,sym,dso']
    return commands


def cpu_quality(text,callchains):
    rows=[];unknown=0.0;active=False;sections=0
    for line in text.splitlines():
        event=re.search(r"of event ['\"]([^'\"]+)['\"]",line)
        if event:
            active=event.group(1)=='cpu-clock:u'
            sections+=active
        match=re.match(r'^\s*(\d+(?:\.\d+)?)%\s+',line)
        if not match or not active:continue
        weight=float(match.group(1));rows.append(dict(weight_pct=weight,row=line))
        if re.search(r'\[unknown\]|\[\.\]\s+0x[0-9a-f]+\b',line,re.I):unknown+=weight
    require(sections==1 and rows,'CPU-clock native symbol report has no unique sample section')
    active=False;readable=False;chain_samples=0
    for line in callchains.splitlines():
        header=legacy.PREFIX.fullmatch(line)
        if header:
            body=header.group(7);active=body.startswith('cpu-clock:u:')
            if active:chain_samples+=1
            candidate=body.removeprefix('cpu-clock:u:') if active else ''
        else:candidate=line if active else ''
        if re.search(r'^\s*[0-9a-f]+\s+(?!\[unknown\])\S+.*\(',candidate):readable=True
    require(readable and chain_samples>0,'no readable userspace CPU-clock callchains')
    return dict(symbol_rows=rows,unresolved_weight_pct=unknown,userspace_callchains_readable=True,callchain_sample_count=chain_samples,
                limits='Relative CPU-clock sampled weight; sparse/inlined hook samples cannot establish exact function duration.')


def cpu_window_commands(directory,pid,window):
    data=str(Path(directory)/'cpu.data')
    bounds=','.join(f'{n//10**9}.{n%10**9:09d}' for n in window)
    return dict(report=['perf','report','-i',data,'--stdio','--no-children','--sort','dso,symbol',
                        '--pid',str(pid),'--time',bounds,'--percentage','relative','--percent-limit','0'],
                callchains=['perf','script','-i',data,'--ns','--pid',str(pid),'--time',bounds,
                            '-F','comm,pid,tid,cpu,time,event,ip,sym,dso'])


def measured_cpu_quality(report,chains,count):
    if count==0:
        return dict(verdict='inconclusive',attribution_complete=False,symbol_rows=[],unresolved_weight_pct=None,
                    userspace_callchains_readable=False,error='No exact-b1 native CPU samples; whole-process symbols are contextual only.')
    try:
        quality=cpu_quality(report,chains)
        require(quality['callchain_sample_count']==count,'measured CPU callchain/sample inventory differs')
        return dict(quality,verdict='measured CPU evidence',attribution_complete=True)
    except DiagnosticError as error:
        return dict(verdict='inconclusive',attribution_complete=False,symbol_rows=[],unresolved_weight_pct=None,
                    userspace_callchains_readable=False,error=str(error))


def render_category(category,directory,output,ordinary_path,deadline,ordinary,observed=None):
    directory=Path(directory)
    def render_one(name,command):
        child=directory/f'render-{name}';child.mkdir()
        env={'PATH':os.environ.get('PATH','/usr/bin:/bin'),'LANG':'C.UTF-8'}
        execute(root_command(command,env,child,ordinary_path,output,deadline),env,child,output,deadline,True)
        for p in (child/'stdout.txt',child/'stderr.txt'):
            with p.open() as f:require(not any(LOSS.search(line) for line in f),'lost/throttled rendering')
        return (child/'stdout.txt').read_text()
    rendered={name:render_one(name,command) for name,command in render_commands(category,directory).items()}
    with (directory/'stderr.txt').open() as f:require(not any(LOSS.search(line) for line in f),'lost/throttled recording')
    legacy.validate_attributes(rendered['attributes'],require_inherit=category!='scheduler')
    if category=='scheduler':
        preflight=json.loads((output/'preflight.json').read_text())
        legacy.validate_system_attachment(rendered['raw-events'],preflight['host']['affinity'])
    launch=legacy.validate_native_evidence(directory,ordinary)
    summary=analyze_category(category,rendered['script'],launch['binary'],launch['pid'])
    if category=='cpu':
        commands=cpu_window_commands(directory,launch['pid'],summary['window_ns'])
        report=render_one('native-context-report',commands['report'])
        chains=render_one('native-context-callchains',commands['callchains'])
        summary['whole_process_cpu_context']=cpu_quality(report,chains)
        if observed is None:
            # Non-database smoke qualification proves the exact live CPU chain.
            summary.update(summary['whole_process_cpu_context'])
        else:
            summary['measured_cpu']=[]
            for repetition,rep in enumerate(observed):
                commands=cpu_window_commands(directory,launch['pid'],rep['window_ns'])
                report=render_one(f'b1-rep-{repetition}-report',commands['report'])
                chains=render_one(f'b1-rep-{repetition}-callchains',commands['callchains'])
                a,b=rep['window_ns'];count=sum(a<=r['stamp']<=b for r in summary['samples'])
                quality=measured_cpu_quality(report,chains,count)
                summary['measured_cpu'].append(dict(window_ns=rep['window_ns'],sample_count=count,**quality))
    dump(directory/'trace-quality.json',summary)
    return summary


@contextmanager
def helper_scope(deadline=None):
    # Mutations exist only while the donated watcher/supervisor actually runs.
    # Exceptions restore both symbols; importing or running this module never
    # changes the donor's public API for another diagnostic or its tests.
    previous_budget,previous_supervise=legacy.budget_breach,legacy.supervise
    try:
        legacy.budget_breach=budget_breach
        if deadline is not None:
            legacy.supervise=lambda command,witness,timeout:previous_supervise(command,witness,timeout=deadline.timeout())
        yield
    finally:
        legacy.budget_breach,legacy.supervise=previous_budget,previous_supervise


def qualified_supervisor(proof,directory,output,command,deadline):
    with helper_scope(deadline):
        deadline.remaining()
        return legacy.qualified_supervisor(proof,directory,output,command)


def qualify_recorders(output,runtime,ordinary_path,ordinary,deadline):
    smoke_binary=output/'wal-diag-smoke'
    python=shutil.which('python3');require(python,'missing python3; install python3')
    shutil.copyfile(Path(python).resolve(),smoke_binary);smoke_binary.chmod(0o755)
    dump(output/'smoke-binary.json',dict(source=str(Path(python).resolve()),sha256=digest(smoke_binary)))
    qualified={}
    for category in ('cpu','syscalls','scheduler'):
        directory=output/f'qualify-{category}';directory.mkdir()
        native_runtime=runtime/f'qualify-{category}';native_runtime.mkdir()
        env=fixed_environment(native_runtime)
        command=profile_command(category,smoke_binary,directory,ordinary_path,digest(smoke_binary),
                                [legacy.SELF,'smoke-target','--directory',str(directory)])
        dump(directory/'host-before.json',host_snapshot(native_runtime))
        try:execute(root_command(command,env,directory,ordinary_path,output,deadline),env,directory,output,deadline,True)
        finally:dump(directory/'host-after.json',host_snapshot(native_runtime))
        summary=render_category(category,directory,output,ordinary_path,deadline,ordinary)
        expected=json.loads((directory/'smoke-thread.json').read_text())
        tid=expected['tid']
        require(tid in summary['thread_ids'] and all(n in summary['thread_names'][str(tid)] for n in expected['names']),
                f'{category} synthetic native lifetime/name qualification failed')
        if category=='syscalls':
            require(all(any(p['name']==name for p in summary['pairs']) for name in SYSCALLS),'smoke syscall category lacks required pairs')
        if category=='scheduler':
            require(summary['offcpu'] and summary['wakeup_to_run'],'smoke scheduler category incomplete')
        qualified[category]=dict(qualified=True,native_pid=summary['native_pid'],benchmark_invocations=0,
                                 no_unsampled_thread_assumption=True)
        dump(output/'qualification.json',qualified)
    return qualified


def observations(directory,source,pid):
    root=Path(directory)/'observations'
    require(root.is_dir() and sorted(p.name for p in root.iterdir())==['rep-0','rep-1','rep-2'],'observer repetition inventory differs')
    results=[]
    for repetition in range(3):
        cell=root/f'rep-{repetition}'
        require(sorted(p.name for p in cell.iterdir())==['events.bin','metadata.json'],'observer file inventory differs')
        metadata=json.loads((cell/'metadata.json').read_text())
        results.append(validate_observations(metadata,read_events(cell/'events.bin'),source,repetition,pid))
    dump(Path(directory)/'observed-intervals.json',results)
    return results


def annotate_trace(summary,observed,deadline=None):
    # Each capture stays separate. Original records and pairs remain intact.
    if deadline is not None:deadline.remaining()
    pair_index={};sync_index=([],[],0)
    if summary['category']=='syscalls':
        grouped=collections.defaultdict(list)
        for pair in summary['pairs']:grouped[(pair['tid'],pair['name'])].append(pair)
        for key,pairs in grouped.items():
            pairs.sort(key=lambda p:p['begin_ns'])
            pair_index[key]=(pairs,[p['begin_ns'] for p in pairs])
        syncs=sorted((p for p in summary['pairs'] if p['name'] in {'fsync','fdatasync'}),key=lambda p:p['begin_ns'])
        sync_index=(syncs,[p['begin_ns'] for p in syncs],max((p['end_ns']-p['begin_ns'] for p in syncs),default=0))
    lifetimes={int(t):bounds for t,bounds in summary['thread_lifetimes_ns'].items()}
    for rep in observed:
        require(summary['window_ns'][0]<=rep['window_ns'][0]<=rep['window_ns'][1]<=summary['window_ns'][1],
                'measured marker outside qualified native exec/exit')
        for op in rep['operations']:
            if deadline is not None:deadline.remaining()
            witnesses=[(op['tid'],op['begin_ns']),(op['return_tid'],op['end_ns'])]
            witnesses.extend((e['tid'],stamp) for e in op['vfs'] for stamp in (e['start_ns'],e['end_ns']))
            require(all(t in lifetimes and lifetimes[t][0]<=stamp<=lifetimes[t][1] for t,stamp in witnesses),
                    'marked operation/VFS TID lacks qualified native lifetime')
    per_repetition=[]
    for rep in observed:
        item=dict(window_ns=rep['window_ns'],clock_uncertainty_ns=rep['clock_witness']['uncertainty_ns'])
        if summary['category']=='syscalls':
            item['pairs']=select_intervals(summary['pairs'],rep['window_ns'])
            item['operations']=[]
            for op in rep['operations']:
                if deadline is not None:deadline.remaining()
                matched=[];complete=op['attribution_complete'];fds=set()
                for v in op['vfs']:
                    names={'write','pwrite64'} if v['kind']==4 else {'fsync','fdatasync'}
                    pairs=[]
                    for name in names:
                        values,starts=pair_index.get((v['tid'],name),([],[]))
                        left=bisect.bisect_left(starts,v['start_ns']);right=bisect.bisect_right(starts,v['end_ns'])
                        pairs.extend(p for p in values[left:right] if p['end_ns']<=v['end_ns'])
                    pairs.sort(key=lambda p:p['begin_ns'])
                    good=bool(pairs)
                    for p in pairs:
                        fd=re.search(r'\bfd(?:=|:\s*)(0x[0-9a-fA-F]+|\d+)\b',p.get('enter_detail',''))
                        good=good and fd is not None
                        if fd:fds.add(int(fd.group(1),0) if '0x' in fd.group(1) else int(fd.group(1)))
                    if v['kind']==5:good=good and len(pairs)==1 and pairs[0]['return_value']==0
                    else:
                        good=good and all(p['return_value']>=0 for p in pairs) and sum(p['return_value'] for p in pairs)==v['amount']
                        pos=v['arg']
                        for p in pairs:
                            offset=re.search(r'\b(?:pos|offset)(?:=|:\s*)(0x[0-9a-fA-F]+|\d+)\b',p.get('enter_detail',''))
                            actual=None if offset is None else int(offset.group(1),0) if '0x' in offset.group(1) else int(offset.group(1))
                            good=good and p['name']=='pwrite64' and actual==pos
                            pos+=max(0,p['return_value'])
                    complete=complete and good;matched.extend(pairs)
                complete=complete and len(fds)==1
                syncs,starts,max_span=sync_index
                left=bisect.bisect_left(starts,op['append_start_ns']-max_span);right=bisect.bisect_right(starts,op['end_ns'])
                inop=[p for p in syncs[left:right] if p['end_ns']>=op['append_start_ns']]
                complete=complete and len(inop)==sum(p['name'] in {'fsync','fdatasync'} for p in matched)
                item['operations'].append(dict(operation_id=op['operation_id'],syscall_correspondence_complete=bool(complete),
                                               matched_pairs=matched,sync_ns=op['sync_ns'] if complete else None,
                                               limits='Exact captured TID/time/FD/offset/byte/return matching; ambiguous or extra sync stays inconclusive.'))
        elif summary['category']=='scheduler':
            item['offcpu']=select_intervals(summary['offcpu'],rep['window_ns'])
            item['wakeup_to_run']=select_intervals(summary['wakeup_to_run'],rep['window_ns'])
        else:
            a,b=rep['window_ns'];samples=[s for s in summary['samples'] if a<=s['stamp']<=b]
            measured=summary.get('measured_cpu',[])
            require(len(measured)==len(observed),'missing per-repetition measured CPU evidence')
            quality=measured[len(per_repetition)]
            require(quality['window_ns']==rep['window_ns'] and quality['sample_count']==len(samples),
                    'measured CPU window/sample inventory differs')
            item.update(quality,cpu_samples=samples,cpu_sample_count=len(samples))
        per_repetition.append(item)
    return dict(category=summary['category'],repetitions=per_repetition,
                limits='Marker bounds retain conversion uncertainty. Cross-marker pairs are explicitly retained. No cross-capture critical path addition.')


def validate_counter_rows(rows,observed):
    require(len(rows)==6 and len(observed)==3,'counter/row repetition inventory differs')
    for repetition,rep in enumerate(observed):
        row=rows[2*repetition]
        require(row['cell']==CELLS[0] and row['repetition']==repetition,'counter b1 row identity differs')
        count=len(rep['operations'])
        # The benchmark prints {:.0}; count/3 never lands on a half-integer.
        rounded=round(count/3)
        require(count>0 and row['ops_s']==row['records_s']==rounded,
                'measured operation/record totals disagree with rounded original native b1 rates')


def summarize_controls(runs):
    import statistics
    ordinary=[r for r in runs if r['phase']=='original']
    expected=sequence()[:8]
    require(len(ordinary)==8 and all(all(r.get(k)==v for k,v in p.items()) for r,p in zip(ordinary,expected)),
            'incomplete/changed original schedule')
    result={};observer_controls={}
    marked=[r for r in runs if r['phase']=='marked']
    require([(r['side'],r['index']) for r in marked]==[('A',8),('B',9)],'missing/changed marked control schedule')
    for cell in CELLS:
        byside={}
        for side in SOURCES:
            selected=[r for r in ordinary if r['side']==side]
            byside[side]=[dict(index=r['index'],ops_s=statistics.mean(row['ops_s'] for row in r['rows'] if row['cell']==cell),
                              mean_repetition_p99_us=statistics.mean(row['p99_us'] for row in r['rows'] if row['cell']==cell)) for r in selected]
        ratios={metric:statistics.mean(v[metric] for v in byside['B'])/statistics.mean(v[metric] for v in byside['A'])
                for metric in ('ops_s','mean_repetition_p99_us')}
        result[cell]=dict(by_source=byside,source_B_over_A=ratios)
        observer_controls[cell]={}
        for side in SOURCES:
            control=next(r for r in marked if r['side']==side)
            rows=[row for row in control['rows'] if row['cell']==cell]
            observer_controls[cell][side]=dict(
                ops_s_ratio=statistics.mean(row['ops_s'] for row in rows)/statistics.mean(v['ops_s'] for v in byside[side]),
                mean_repetition_p99_ratio=statistics.mean(row['p99_us'] for row in rows)/statistics.mean(v['mean_repetition_p99_us'] for v in byside[side]),
                limitations='One marked ordinary call per source relative to four untouched controls; observer/codegen sensitivity only.')
    return dict(cells=result,observer_controls=observer_controls,new_pass_threshold=None,original_gate_unchanged=True,
                limits='Balanced within-source/order evidence on a shared runner; source association is not sole-cause proof.')


def run(manifest_path,output,qualification_only=False):
    output=Path(output).resolve();require(not output.exists(),'fresh output required; no retries')
    output.mkdir(parents=True)
    deadline,final_deadline=run_deadlines()
    dump(output/'deadline.json',dict(begin_ns=time.monotonic_ns(),operation_deadline_ns=deadline.end_ns,
         deadline_ns=final_deadline.end_ns,aggregate_seconds=1200,operation_seconds=1140,finalization_reserve_seconds=60,per_command_seconds=120))
    result=dict(diagnostic_only=True,timing_eligible=False,verdict='inconclusive',complete=False,benchmark_invocations=0,
                original_acceptance='FAILED, unchanged',qualification_only=qualification_only)
    runs=[];failure=None
    try:
        manifest=verify_manifest(json.loads(Path(manifest_path).read_text()),deadline)
        seal=seal_manifest(manifest)
        dump(output/'manifest.json',manifest)
        runtime=output/'runtime';runtime.mkdir()
        dump(output/'preflight.json',legacy.preflight(runtime))
        ordinary=legacy.resource_proof();ordinary_path=output/'ordinary-resources.json';dump(ordinary_path,ordinary)
        proc_dir=output/'ordinary-live-proc';proc_dir.mkdir()
        actual=legacy.proc_identity('self',proc_dir);legacy.validate_identity(actual,ordinary);dump(proc_dir/'identity.json',actual)
        qualify_recorders(output,runtime,ordinary_path,ordinary,deadline)
        if qualification_only:
            result.update(complete=True,verdict='three actual recorder chains qualified; no benchmark timing')
        else:
            dump(output/'schedule.json',sequence())
            for planned in sequence():
                deadline.remaining();require(result['benchmark_invocations']<16,'benchmark invocation cap')
                legacy.quiet();verify_seal(manifest,seal)
                directory=output/f'{planned["index"]:02d}-{planned["phase"]}-{planned["side"]}';directory.mkdir()
                native_runtime=runtime/directory.name;native_runtime.mkdir()
                variant='original' if planned['phase']=='original' else 'marked'
                entry=manifest['binaries'][variant+'-'+planned['side'].lower()]
                alias=output/f'wal-diag-{planned["side"]}'
                shutil.copyfile(entry['path'],alias);alias.chmod(0o755)
                require(digest(alias)==entry['sha256'],'executable copy differs')
                env=environment(native_runtime,planned,directory/'observations')
                category=planned['phase'] if planned['phase'] in ('cpu','syscalls','scheduler') else None
                if category:
                    command=root_command(profile_command(category,alias,directory,ordinary_path,entry['sha256']),
                                         env,directory,ordinary_path,output,deadline)
                else:command=legacy.scoped_command(alias,directory,ordinary_path,entry['sha256'])
                result['benchmark_invocations']+=1
                dump(directory/'host-before.json',host_snapshot(native_runtime))
                try:exitproof=execute(command,env,directory,output,deadline,category is not None)
                finally:dump(directory/'host-after.json',host_snapshot(native_runtime))
                launch=legacy.validate_native_evidence(directory,ordinary)
                require(digest(alias)==entry['sha256'],'executable changed during native call')
                rows=parse_native((directory/'stdout.txt').read_text())
                item=dict(planned,rows=rows,binary_sha256=entry['sha256'],source_sha=SOURCES[planned['side']],
                          native_pid=launch['pid'],exit_code=exitproof['exit_code'],directory=directory.name)
                if variant=='marked':
                    spans=observations(directory,SOURCES[planned['side']],launch['pid'])
                    validate_counter_rows(rows,spans)
                    item['observer_attribution_complete']=all(op['attribution_complete'] for rep in spans for op in rep['operations'])
                    if category:
                        trace=render_category(category,directory,output,ordinary_path,deadline,ordinary,spans)
                        dump(directory/'measured-trace.json',annotate_trace(trace,spans,deadline))
                runs.append(item);dump(output/'runs.json',runs)
                require(not budget_breach(directory,output),'post-exit evidence cap')
            dump(output/'control-summary.json',summarize_controls(runs))
            result.update(complete=True,verdict='finite evidence collected; causal attribution pending independent review')
    except (DiagnosticError,OSError,ValueError,KeyError,subprocess.SubprocessError) as error:
        failure=error;result['error']=str(error)
    finally:
        # Prefix ownership has reserved time inside the original aggregate budget.
        try:
            if sys.platform.startswith('linux'):ownership_cleanup(output,final_deadline)
        except (DiagnosticError,OSError,subprocess.SubprocessError) as error:
            result.update(cleanup_error=str(error),complete=False,verdict='inconclusive');failure=failure or error
        try:finalize_metadata(output,runs,result,final_deadline)
        except (DiagnosticError,OSError) as error:failure=failure or error
    if failure:raise DiagnosticError(str(failure)) from failure
    return result


def main():
    parser=argparse.ArgumentParser(description=__doc__);sub=parser.add_subparsers(dest='operation',required=True)
    p=sub.add_parser('snapshot-source');p.add_argument('--root',required=True);p.add_argument('--side',choices=SOURCES,required=True);p.add_argument('--output',required=True)
    p=sub.add_parser('build-manifest')
    for name in ('evidence','source-a','source-b','overlay','original-a','original-b','marked-a','marked-b','output'):p.add_argument('--'+name,required=True)
    for operation in ('run','qualify'):
        p=sub.add_parser(operation);p.add_argument('--manifest',required=True);p.add_argument('--output',required=True)
    p=sub.add_parser('supervisor')
    for name in ('proof','directory','output'):p.add_argument('--'+name,required=True)
    p.add_argument('--deadline-ns',type=int,required=True);p.add_argument('command',nargs=argparse.REMAINDER)
    p=sub.add_parser('ownership-supervisor')
    for name in ('output','directory','owner'):p.add_argument('--'+name,required=True)
    p.add_argument('--deadline-ns',type=int,required=True)
    args=parser.parse_args()
    try:
        if args.operation=='snapshot-source':snapshot_source(args.root,args.side,args.output)
        elif args.operation=='build-manifest':build_manifest(args)
        elif args.operation=='ownership-supervisor':return ownership_supervisor(args.output,args.directory,args.owner,Deadline(args.deadline_ns))
        elif args.operation in ('run','qualify'):print(json.dumps(run(args.manifest,args.output,args.operation=='qualify'),indent=2))
        else:
            command=args.command[1:] if args.command[:1]==['--'] else args.command
            require(command,'missing supervisor command')
            return qualified_supervisor(args.proof,args.directory,args.output,command,Deadline(args.deadline_ns))
    except (DiagnosticError,OSError,ValueError,KeyError,subprocess.SubprocessError) as e:
        print(f'inconclusive: {e}',file=sys.stderr);return 1
    return 0

if __name__=='__main__':sys.exit(main())
