"""Benchmark fresh processes under a shared, explicitly reported memory budget."""
from pathlib import Path
import argparse
import csv
import ctypes
from ctypes import wintypes
import importlib.metadata
import json
import os
import platform
import subprocess
import sys
import time
import traceback
import psutil

ROOT = Path(__file__).resolve().parent
JOB = None


def apply_budget(mib):
    global JOB
    if not mib:
        return
    if os.name != 'nt':
        raise RuntimeError('The controlled budget is implemented for Windows. Use --budget-mib 0 elsewhere.')
    class Limits(ctypes.Structure):
        _fields_ = [('process_time', ctypes.c_int64), ('job_time', ctypes.c_int64),
                    ('flags', wintypes.DWORD), ('min_ws', ctypes.c_size_t), ('max_ws', ctypes.c_size_t),
                    ('active_processes', wintypes.DWORD), ('affinity', ctypes.c_size_t),
                    ('priority', wintypes.DWORD), ('scheduling', wintypes.DWORD)]
    class IO(ctypes.Structure):
        _fields_ = [(x, ctypes.c_uint64) for x in ('read_ops', 'write_ops', 'other_ops', 'read_bytes', 'write_bytes', 'other_bytes')]
    class Extended(ctypes.Structure):
        _fields_ = [('basic', Limits), ('io', IO), ('process_memory', ctypes.c_size_t),
                    ('job_memory', ctypes.c_size_t), ('peak_process', ctypes.c_size_t), ('peak_job', ctypes.c_size_t)]
    api = ctypes.WinDLL('kernel32', use_last_error=True)
    api.CreateJobObjectW.argtypes = [ctypes.c_void_p, wintypes.LPCWSTR]
    api.CreateJobObjectW.restype = wintypes.HANDLE
    api.SetInformationJobObject.argtypes = [wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p, wintypes.DWORD]
    api.SetInformationJobObject.restype = wintypes.BOOL
    api.GetCurrentProcess.restype = wintypes.HANDLE
    api.AssignProcessToJobObject.argtypes = [wintypes.HANDLE, wintypes.HANDLE]
    api.AssignProcessToJobObject.restype = wintypes.BOOL
    JOB = api.CreateJobObjectW(None, None)
    limit = Extended()
    limit.basic.flags = 0x100  # committed-memory limit, distinct from resident RAM
    limit.process_memory = int(mib * 1024**2)
    if not JOB or not api.SetInformationJobObject(JOB, 9, ctypes.byref(limit), ctypes.sizeof(limit)):
        raise ctypes.WinError(ctypes.get_last_error())
    if not api.AssignProcessToJobObject(JOB, api.GetCurrentProcess()):
        raise ctypes.WinError(ctypes.get_last_error())


def worker(args):
    from analysis import METHODS
    apply_budget(args.budget_mib)
    start = time.perf_counter()
    record = {'method': args.method, 'budget_mib': args.budget_mib, 'status': 'running'}
    try:
        record.update(METHODS[args.method](args.csv, threads=args.threads, block_mib=args.block_mib))
        record['status'] = 'ok'
    except (MemoryError, OSError) as error:
        record.update(status='memory_error', exception_type=type(error).__name__, message=str(error),
                      traceback=traceback.format_exc())
        traceback.print_exc()
    except Exception as error:
        record.update(status='error', exception_type=type(error).__name__, message=str(error),
                      traceback=traceback.format_exc())
        traceback.print_exc()
    record['processing_seconds'] = time.perf_counter() - start
    args.out.write_text(json.dumps(record, indent=2, allow_nan=False), encoding='utf-8')
    return 0 if record['status'] in ('ok', 'memory_error') else 1


def measured(args, method, repeat, output):
    label = f'{method}_{repeat}'
    destination = output / f'{label}.json'
    destination.unlink(missing_ok=True)  # never reuse a stale worker result
    command = [sys.executable, '-B', str(ROOT / 'benchmark.py'), '--method', method,
               '--csv', str(args.csv), '--out', str(destination), '--budget-mib', str(args.budget_mib),
               '--threads', str(args.threads), '--block-mib', str(args.block_mib)]
    started = time.perf_counter()
    measurements = []
    with (output / f'{label}.log').open('w', encoding='utf-8') as log:
        process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, cwd=ROOT)
        monitored = psutil.Process(process.pid)
        while process.poll() is None:
            try:
                family = [monitored]
                try:
                    family += monitored.children(recursive=True)
                except (psutil.Error, OSError):
                    pass
                memory = [member.memory_info() for member in family]
                measurements.append((time.perf_counter() - started,
                                     sum(item.rss for item in memory),
                                     sum(getattr(item, 'private', 0) for item in memory)))
            except (psutil.Error, OSError):
                pass
            time.sleep(0.1)
    with (output / f'{label}_memory.csv').open('w', newline='') as stream:
        writer = csv.writer(stream)
        writer.writerow(['seconds_since_launch', 'rss_bytes', 'private_bytes'])
        writer.writerows(measurements)
    record = json.loads(destination.read_text()) if destination.exists() else {'method': method, 'status': 'process_failure'}
    record.update(repeat=repeat, exit_code=process.returncode,
                  wall_seconds=time.perf_counter() - started,
                  peak_rss_bytes=max((r[1] for r in measurements), default=0),
                  peak_private_bytes=max((r[2] for r in measurements), default=0),
                  sample_interval_seconds=0.1, memory_samples=len(measurements))
    destination.write_text(json.dumps(record, indent=2, allow_nan=False), encoding='utf-8')
    print(f"{label}: {record['status']}, {record.get('processing_seconds', 0):.2f}s, RSS {record['peak_rss_bytes']/1024**2:.1f} MiB", flush=True)
    return record


def run(args):
    from analysis import verify
    output = args.out
    output.mkdir(parents=True, exist_ok=True)
    environment = {'python': sys.version, 'platform': platform.platform(),
                   'cpu_logical': psutil.cpu_count(), 'cpu_physical': psutil.cpu_count(logical=False),
                   'total_ram_bytes': psutil.virtual_memory().total,
                   'available_ram_at_start_bytes': psutil.virtual_memory().available,
                   'libraries': {n: importlib.metadata.version(n) for n in ['pandas', 'dask', 'psutil', 'matplotlib', 'pyarrow']},
                   'input_file': str(args.csv), 'input_bytes': args.csv.stat().st_size,
                   'budget_mib': args.budget_mib, 'threads': args.threads, 'block_mib': args.block_mib,
                   'chunk_rows': 100_000, 'repeats': args.repeats,
                   'memory_measurement': '100 ms sampled process-tree RSS and private bytes, including imports. RSS is distinct from the Job Object committed-memory cap. Shared memory may be double counted across launcher and worker.',
                   'timing': 'Processing time excludes startup/imports; includes CSV parsing, aggregation and lazy graph construction.',
                   'cache_note': 'No OS-cache flush. Method order rotated between repeats; identical input and schema.',
                   'parser_note': 'Full-load uses the Python parser to obtain a catchable MemoryError under the allocation cap. Chunking and Dask use the C parser; completed speed comparison is limited to those two methods.',
                   'run_utc': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}
    (output / 'environment.json').write_text(json.dumps(environment, indent=2), encoding='utf-8')
    rows = []
    order = ['eager', 'stream', 'lazy']
    for repeat in range(1, args.repeats + 1):
        rotated = order[(repeat-1)%3:] + order[:(repeat-1)%3]
        for method in rotated:
            rows.append(measured(args, method, repeat, output))
    successful = [r for r in rows if r['status'] == 'ok']
    reference = next(r['summary'] for r in successful if r['method'] == 'stream')
    for row in successful:
        verify(reference, row['summary'])
    manifest = ROOT / 'data' / 'manifest.json'
    full_input = args.csv.resolve() in ((ROOT / 'data' / 'train.csv').resolve(), (ROOT / 'train.csv').resolve())
    if full_input:
        expected = json.loads(manifest.read_text())['expected_rows']
        assert reference['rows'] == expected, 'Incomplete dataset'
        assert args.csv.stat().st_size > 5 * 1024**3, 'Input smaller than 5 GiB'
    assert all(r['status'] == 'ok' for r in rows if r['method'] in ('stream', 'lazy')), 'Out-of-core strategy failed'
    assert all(r['status'] in ('ok', 'memory_error') for r in rows), 'Unexpected failure'
    validation = {'passed': True, 'full_input': full_input, 'rows': reference['rows'],
                  'successful_runs_compared': len(successful), 'relative_tolerance': 1e-10, 'absolute_tolerance': 1e-6,
                  'integer_counts': 'exact equality', 'matching_fields': 'eight column counts, six numeric sums/means, all passenger fare groups'}
    (output / 'validation.json').write_text(json.dumps(validation, indent=2), encoding='utf-8')
    (output / 'summary.json').write_text(json.dumps({'runs': rows, 'validation': validation}, indent=2), encoding='utf-8')
    fields = ['method', 'repeat', 'status', 'processing_seconds', 'wall_seconds', 'peak_rss_bytes', 'peak_private_bytes']
    with (output / 'benchmark.csv').open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(rows)
    print('Validation passed: every successful method produced equivalent results.', flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--csv', type=Path, default=ROOT / 'data' / 'train.csv')
    parser.add_argument('--out', type=Path, default=ROOT / 'results')
    parser.add_argument('--repeats', type=int, default=3)
    parser.add_argument('--budget-mib', type=int, default=8192)
    parser.add_argument('--threads', type=int, default=4)
    parser.add_argument('--block-mib', type=int, default=16)
    parser.add_argument('--method', choices=['eager', 'stream', 'lazy'], help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.repeats < 1 or args.threads < 1 or args.block_mib < 1 or args.budget_mib < 0:
        parser.error('Invalid benchmark parameters')
    return worker(args) if args.method else run(args)


if __name__ == '__main__':
    sys.exit(main())
