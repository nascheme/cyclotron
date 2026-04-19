"""Sweep the benchmark kernel across a parameter grid for multiple Python
executables."""

import itertools
import json
import shlex
import subprocess
import sys
import time
from pathlib import Path

# Called as a standalone script by test interpreters that may not have
# cyclotron installed.
_CYCLES_SCRIPT = Path(__file__).parent / 'cycles.py'

CYCLE_SIZES_DEFAULT = [10, 100, 1_000]
EXTRA_BYTES_DEFAULT = [0, 10_000, 100_000, 300_000]
LIVE_OBJECTS_DEFAULT = [100, 10_000, 30_000]

if False:
    # fast run
    CYCLE_SIZES_DEFAULT = [10]
    EXTRA_BYTES_DEFAULT = [10_000]
    LIVE_OBJECTS_DEFAULT = [100, 10_000]


def add_args(p):
    p.add_argument(
        '--exec',
        dest='execs',
        action='append',
        default=[],
        metavar='PYTHON',
        help='Python executable to benchmark (repeat, paired with --output)',
    )
    p.add_argument(
        '--output',
        dest='outputs',
        action='append',
        default=[],
        metavar='FILE',
        help='JSON output file for the preceding --exec',
    )
    p.add_argument(
        '--cycle-sizes',
        type=int,
        nargs='+',
        default=CYCLE_SIZES_DEFAULT,
        metavar='N',
        help='cycle-size values to sweep',
    )
    p.add_argument(
        '--extra-bytes',
        type=int,
        nargs='+',
        default=EXTRA_BYTES_DEFAULT,
        metavar='N',
        help='extra-bytes values to sweep',
    )
    p.add_argument(
        '--live-objects',
        type=int,
        nargs='+',
        default=LIVE_OBJECTS_DEFAULT,
        metavar='N',
        help='live-objects values to sweep',
    )
    p.add_argument('--total-objects', type=int, default=10_000_000)
    p.add_argument('--report-interval', type=float, default=0.5)
    p.add_argument(
        '--timeout',
        type=float,
        default=900.0,
        help='per-run timeout in seconds',
    )
    p.add_argument('--dry-run', action='store_true')


def run_one(args, python_cmd, cycle_size, extra_bytes, live_objects):
    cmd = shlex.split(python_cmd) + [
        str(_CYCLES_SCRIPT),
        '--cycle-size',
        str(cycle_size),
        '--extra-bytes',
        str(extra_bytes),
        '--live-objects',
        str(live_objects),
        '--total-objects',
        str(args.total_objects),
        '--report-interval',
        str(args.report_interval),
    ]
    if args.dry_run:
        return {'cmd': cmd, 'skipped': True}

    t0 = time.perf_counter()
    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=args.timeout,
        )
    except subprocess.TimeoutExpired as e:
        return {
            'cmd': cmd,
            'error': 'timeout',
            'stdout': e.stdout or '',
            'stderr': e.stderr or '',
            'wall_time': time.perf_counter() - t0,
        }
    wall = time.perf_counter() - t0

    samples = []
    header = None
    for line in proc.stdout.splitlines():
        if not line:
            continue
        if header is None:
            header = line.split(',')
            continue
        parts = line.split(',')
        if len(parts) != len(header):
            continue
        sample = {}
        for k, v in zip(header, parts):
            try:
                sample[k] = float(v) if k in ('time_ms', 'pause_ms') else int(v)
            except ValueError:
                sample[k] = v
        samples.append(sample)

    summary = {}
    for line in proc.stderr.splitlines():
        if ':' in line:
            k, _, v = line.partition(':')
            summary[k.strip()] = v.strip()

    peaked_s = summary.get('GC peaked')
    peaked: bool | None = (
        True if peaked_s == 'yes' else False if peaked_s == 'no' else None
    )

    return {
        'cmd': cmd,
        'returncode': proc.returncode,
        'wall_time': wall,
        'samples': samples,
        'summary': summary,
        'peaked': peaked,
        'stderr_tail': proc.stderr[-2000:] if proc.returncode else '',
    }


def main(args=None):
    if args is None:
        import argparse

        p = argparse.ArgumentParser(description='Sweep benchmark across parameter grid')
        add_args(p)
        args = p.parse_args()

    if not args.execs:
        print(
            'error: at least one --exec/--output pair required',
            file=sys.stderr,
        )
        sys.exit(1)
    if len(args.execs) != len(args.outputs):
        print(
            'error: --exec and --output must be paired (equal count)',
            file=sys.stderr,
        )
        sys.exit(1)

    grid = list(
        itertools.product(
            args.cycle_sizes,
            args.extra_bytes,
            args.live_objects,
        )
    )
    print(
        f'{len(args.execs)} executable(s) × {len(grid)} grid points = '
        f'{len(args.execs) * len(grid)} total runs',
        file=sys.stderr,
    )

    for python_cmd, out_file in zip(args.execs, args.outputs):
        out_path = Path(out_file)
        out_path.parent.mkdir(parents=True, exist_ok=True)

        results = {
            'meta': {
                'python': python_cmd,
                'total_objects': args.total_objects,
                'report_interval': args.report_interval,
                'cycle_sizes': args.cycle_sizes,
                'extra_bytes': args.extra_bytes,
                'live_objects': args.live_objects,
                'started': time.strftime('%Y-%m-%dT%H:%M:%S'),
            },
            'runs': [],
        }

        for cs, eb, lo in grid:
            print(
                f'[run] python={python_cmd!r} cs={cs} eb={eb} lo={lo}',
                file=sys.stderr,
                flush=True,
            )
            r = run_one(args, python_cmd, cs, eb, lo)
            peaked_val = r.get('peaked')
            peaked_str = (
                'yes' if peaked_val is True else 'no' if peaked_val is False else '?'
            )
            print(f'[done] peaked={peaked_str}', file=sys.stderr, flush=True)
            results['runs'].append(
                {
                    'params': {
                        'python': python_cmd,
                        'cycle_size': cs,
                        'extra_bytes': eb,
                        'live_objects': lo,
                    },
                    **r,
                }
            )
            # Persist after every run so partial sweeps are recoverable.
            with open(out_path, 'w') as f:
                json.dump(results, f, indent=1)

        results['meta']['finished'] = time.strftime('%Y-%m-%dT%H:%M:%S')
        with open(out_path, 'w') as f:
            json.dump(results, f, indent=1)
        print(f'wrote {out_path}', file=sys.stderr)
