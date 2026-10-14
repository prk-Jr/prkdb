#!/usr/bin/env python3
"""Qualify live Linux resources and tracing without starting a database benchmark."""
import argparse
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys

import wal_descriptor_diagnostic as d


def retain_ownership(output):
    if platform.system() != 'Linux':
        return  # Portable unit controls; qualify preflight itself requires Linux.
    result = subprocess.run(['sudo', '-n', 'chown', '-R', f'{os.getuid()}:{os.getgid()}', '--', str(output)],
                            capture_output=True, text=True, check=False)
    d.dump(output / 'ownership.json', dict(exit_code=result.returncode, stderr=result.stderr))
    d.require(result.returncode == 0, 'cannot retain qualified trace ownership')


def qualify(output):
    output = Path(output).resolve()
    d.require(not output.exists(), 'fresh qualification output required')
    output.mkdir(parents=True)
    result = dict(qualified=False, benchmark_invocations=0, timing_eligible=False,
                  original_acceptance='FAILED, unchanged')
    failure = None
    try:
        runtime = output / 'runtime'
        runtime.mkdir()
        d.dump(output / 'preflight.json', d.preflight(runtime))
        ordinary = d.resource_proof()
        ordinary_path = output / 'ordinary-resources.json'
        d.dump(ordinary_path, ordinary)
        live_directory = output / 'ordinary-live-proc'
        live_directory.mkdir()
        live = d.proc_identity('self', live_directory)
        d.validate_identity(live, ordinary)
        d.dump(live_directory / 'identity.json', live)

        # No native benchmark, benchmark artifact, or descriptor.run call here.
        binary = output / 'wal-diag-smoke'
        python = shutil.which('python3')
        d.require(python is not None, 'missing host Python executable')
        shutil.copyfile(Path(python).resolve(), binary)
        binary.chmod(0o755)
        sha = d.digest(binary)
        d.dump(output / 'smoke-binary.json', dict(source=str(Path(python).resolve()),
                                                executable=str(binary), sha256=sha))
        smoke = output / 'trace-smoke'
        smoke.mkdir()
        env = d.fixed_environment(runtime)
        command = d.profile_command(binary, smoke, ordinary_path, sha,
                                    [d.SELF, 'smoke-target', '--directory', str(smoke)])
        d.execute(d.root_command(command, env, smoke, ordinary_path, output), env, smoke, output, privileged=True)
        d.validate_native_evidence(smoke, ordinary)
        summary = d.render(smoke, output, ordinary_path)
        d.qualify_smoke(smoke, summary)
        result.update(qualified=True, live_proc_identity=True, full_three_recorder_smoke=True,
                      cpu_symbol_attribution_complete=summary.get('cpu_symbol_attribution_complete', False))
    except (d.DiagnosticError, OSError, ValueError, KeyError, subprocess.SubprocessError) as error:
        failure = error
        result['error'] = str(error)
    finally:
        # Preserve the primary failure even if ownership repair also fails.
        try:
            retain_ownership(output)
        except (d.DiagnosticError, OSError, subprocess.SubprocessError) as error:
            result['ownership_error'] = str(error)
            result['qualified'] = False
            failure = failure or error
        d.dump(output / 'result.json', result)
        d.save_inventory(output)
    if failure is not None:
        raise d.DiagnosticError(str(failure)) from failure
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    try:
        result = qualify(args.output)
    except (d.DiagnosticError, OSError, ValueError, KeyError, subprocess.SubprocessError) as error:
        print(f'qualification failed: {error}', file=sys.stderr)
        return 1
    print(json.dumps(result, indent=2))
    return 0


if __name__ == '__main__':
    sys.exit(main())
