"""Sweep the benchmark kernel across a parameter grid for multiple Python
executables."""

import itertools
import json
import shlex
import subprocess
import sys
import time
from pathlib import Path

_SRC = Path(__file__).parent.parent

WORKLOADS_DEFAULT = ['chain']
CYCLE_SIZES_DEFAULT = [10, 100, 1_000]
EXTRA_BYTES_DEFAULT = [0, 10_000, 100_000, 300_000]
LIVE_OBJECTS_DEFAULT = [100, 10_000, 30_000]
CYCLIC_FRACTIONS_DEFAULT = [1.0]

if False:
    # fast run
    WORKLOADS_DEFAULT = ['chain']
    CYCLE_SIZES_DEFAULT = [100]
    EXTRA_BYTES_DEFAULT = [100_000]
    LIVE_OBJECTS_DEFAULT = [100, 10_000, 30_000]
    CYCLIC_FRACTIONS_DEFAULT = [1.0]


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
        '--workloads',
        nargs='+',
        choices=('chain', 'tree'),
        default=WORKLOADS_DEFAULT,
        metavar='WL',
        help='workload modes to sweep (chain, tree)',
    )
    p.add_argument(
        '--cycle-sizes',
        type=int,
        nargs='+',
        default=CYCLE_SIZES_DEFAULT,
        metavar='N',
        help='cycle-size values to sweep (records-per-tree in tree mode)',
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
    p.add_argument(
        '--cyclic-fractions',
        type=float,
        nargs='+',
        default=CYCLIC_FRACTIONS_DEFAULT,
        metavar='F',
        help='cyclic-fraction values to sweep (tree mode only)',
    )
    p.add_argument('--total-objects', type=int, default=1_000_000)
    p.add_argument('--report-interval', type=float, default=0.5)
    p.add_argument(
        '--sample-interval',
        type=float,
        default=0.5,
        help='seconds between trash-count samples for stability check',
    )
    p.add_argument(
        '--min-runtime',
        type=float,
        default=10.0,
        help='minimum wall-clock seconds before a run may exit (passed'
        ' through to cyclotron.cycles)',
    )
    p.add_argument(
        '--max-runtime',
        type=float,
        default=60.0,
        help='hard wall-clock cap per run (passed through to cyclotron.cycles)',
    )
    p.add_argument(
        '--timeout',
        type=float,
        default=300.0,
        help='per-run timeout in seconds',
    )
    p.add_argument('--dry-run', action='store_true')


def run_one(
    args,
    python_cmd,
    workload,
    cycle_size,
    extra_bytes,
    live_objects,
    cyclic_fraction,
):
    cmd = shlex.split(python_cmd) + [
        '-m',
        'cyclotron.cycles',
        '--workload',
        workload,
        '--cycle-size',
        str(cycle_size),
        '--extra-bytes',
        str(extra_bytes),
        '--live-objects',
        str(live_objects),
        '--cyclic-fraction',
        str(cyclic_fraction),
        '--total-objects',
        str(args.total_objects),
        '--report-interval',
        str(args.report_interval),
        '--sample-interval',
        str(args.sample_interval),
        '--min-runtime',
        str(args.min_runtime),
        '--max-runtime',
        str(args.max_runtime),
    ]
    if args.dry_run:
        return {'cmd': cmd, 'skipped': True}

    env = {'PYTHONPATH': _SRC}

    t0 = time.perf_counter()
    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=args.timeout,
            env=env,
        )
    except subprocess.TimeoutExpired as e:
        return {
            'cmd': cmd,
            'error': 'timeout',
            'stdout': e.stdout or '',
            'stderr': e.stderr or '',
            'wall_time': time.perf_counter() - t0,
        }
    if proc.returncode:
        print(f'cmd {cmd!r} failed')
        print(proc.stderr, file=sys.stderr)
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

    stable_s = summary.get('GC stable')
    stable: bool | None = (
        True if stable_s == 'yes' else False if stable_s == 'no' else None
    )

    return {
        'cmd': cmd,
        'returncode': proc.returncode,
        'wall_time': wall,
        'samples': samples,
        'summary': summary,
        'stable': stable,
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
            args.workloads,
            args.cycle_sizes,
            args.extra_bytes,
            args.live_objects,
            args.cyclic_fractions,
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
                'workloads': args.workloads,
                'cycle_sizes': args.cycle_sizes,
                'extra_bytes': args.extra_bytes,
                'live_objects': args.live_objects,
                'cyclic_fractions': args.cyclic_fractions,
                'started': time.strftime('%Y-%m-%dT%H:%M:%S'),
            },
            'runs': [],
        }

        for wl, cs, eb, lo, cf in grid:
            print(
                f'[run] python={python_cmd!r} wl={wl} cs={cs} eb={eb} lo={lo} cf={cf}',
                file=sys.stderr,
                flush=True,
            )
            r = run_one(args, python_cmd, wl, cs, eb, lo, cf)
            stable_val = r.get('stable')
            stable_str = (
                'yes' if stable_val is True else 'no' if stable_val is False else '?'
            )
            wall = r.get('wall_time', 0.0)
            print(
                f'[done] stable={stable_str} time={wall:.1f}s',
                file=sys.stderr,
                flush=True,
            )
            results['runs'].append(
                {
                    'params': {
                        'python': python_cmd,
                        'workload': wl,
                        'cycle_size': cs,
                        'extra_bytes': eb,
                        'live_objects': lo,
                        'cyclic_fraction': cf,
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
