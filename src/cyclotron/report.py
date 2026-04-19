"""Render text table reports from batch sweep JSON files."""

import json
import sys
from pathlib import Path


def to_float(s, default=0.0):
    if s is None:
        return default
    try:
        return float(str(s).split()[0])
    except ValueError, IndexError:
        return default


def to_int(s, default=0):
    return int(to_float(s, default))


def extract(run):
    p = run['params']
    samples = run.get('samples') or []
    summary = run.get('summary') or {}

    if samples:
        peak_rss = max(s.get('rss_kb', 0) for s in samples)
        avg_alive = sum(s.get('alive', 0) for s in samples) / len(samples)
        avg_trash = sum(s.get('trash', 0) for s in samples) / len(samples)
        max_trash_seen = max(s.get('trash', 0) for s in samples)
    else:
        peak_rss = avg_alive = avg_trash = max_trash_seen = 0

    if samples:
        max_pause_ms = max(to_float(s.get('pause_ms'), 0.0) for s in samples)
    else:
        max_pause_ms = 0.0
    max_pause_ms = max(to_float(summary.get('GC pause max ms')), max_pause_ms)

    return {
        'cycle_size': p['cycle_size'],
        'extra_bytes': p['extra_bytes'],
        'live_objects': p['live_objects'],
        'python': p['python'],
        'wall': run.get('wall_time', 0.0),
        'total_time': to_float(summary.get('Total time'), run.get('wall_time', 0.0)),
        'peak_rss_kb': max(to_int(summary.get('Peak RSS')), peak_rss),
        'max_trash': max(to_int(summary.get('Max trash')), max_trash_seen),
        'avg_alive': int(avg_alive),
        'avg_trash': int(avg_trash),
        'max_pause_ms': max_pause_ms,
        'peaked': run.get('peaked'),
        'rc': run.get('returncode', 0),
        'error': run.get('error'),
    }


def fmt_bytes(kb, precision=1):
    if kb >= 1024 * 1024:
        return f'{kb / 1024 / 1024:.{precision}f}G'
    if kb >= 1024:
        return f'{kb / 1024:.{precision}f}M'
    return f'{kb}K'


def fmt_bytes_int(kb):
    if kb >= 1024 * 1024:
        return f'{kb / 1024 / 1024:.1f}G'
    return fmt_bytes(kb, precision=0)


def fmt_count(n, precision=1):
    if n >= 1_000_000:
        return f'{n / 1_000_000:.{precision}f}M'
    if n >= 1_000:
        return f'{n / 1_000:.{precision}f}k'
    return str(n)


def fmt_count_int(n):
    return fmt_count(n, precision=0)


def _sort_rows(rows, sort_arg):
    sort_keys = sort_arg.split(',')
    rows.sort(key=lambda r: tuple(r.get(k, 0) for k in sort_keys))


def render_flat(rows):
    headers = [
        ('cycle', 8, lambda r: fmt_count(r['cycle_size'])),
        ('extra', 8, lambda r: fmt_count(r['extra_bytes'])),
        ('live', 8, lambda r: fmt_count(r['live_objects'])),
        ('time(s)', 9, lambda r: f"{r['total_time']:.2f}"),
        ('peakRSS', 9, lambda r: fmt_bytes(r['peak_rss_kb'])),
        ('maxTrash', 10, lambda r: fmt_count(r['max_trash'])),
        (
            'maxPause',
            10,
            lambda r: f"{r['max_pause_ms']:.2f}ms" if r['max_pause_ms'] else '-',
        ),
        ('avgAlive', 10, lambda r: fmt_count(r['avg_alive'])),
        ('avgTrash', 10, lambda r: fmt_count(r['avg_trash'])),
        (
            'peaked',
            7,
            lambda r: {True: 'yes', False: 'no', None: '?'}[r['peaked']],
        ),
        ('rc', 4, lambda r: str(r['rc']) if not r['error'] else r['error']),
    ]
    line = ' '.join(f'{h:>{w}}' for h, w, _ in headers)
    print(line)
    print('-' * len(line))
    for r in rows:
        print(' '.join(f'{fn(r):>{w}}' for _, w, fn in headers))


def render_compare(rows1, rows2, label1, label2):
    by_key1 = {(r['cycle_size'], r['extra_bytes'], r['live_objects']): r for r in rows1}
    by_key2 = {(r['cycle_size'], r['extra_bytes'], r['live_objects']): r for r in rows2}
    keys = sorted(set(by_key1) & set(by_key2))

    headers = [
        ('cycle', 8),
        ('extra', 8),
        ('live', 8),
        ('t(s)', 8),
        ('t%', 7),
        ('rss', 8),
        ('rss%', 7),
        ('trash', 9),
        ('trash%', 9),
        ('pause', 8),
        ('pause%', 8),
        ('peaked', 7),
    ]
    line = ' '.join(f'{h:>{w}}' for h, w in headers)
    print()
    print(f'base={label1}  vs  new={label2}')
    print(line)
    print('-' * len(line))
    for key in keys:
        r1, r2 = by_key1[key], by_key2[key]
        cs, eb, lo = key
        t1, t2 = r1['total_time'], r2['total_time']
        rss1, rss2 = r1['peak_rss_kb'], r2['peak_rss_kb']
        tr1, tr2 = r1['max_trash'], r2['max_trash']
        p1, p2 = r1['max_pause_ms'], r2['max_pause_ms']
        dt = (t2 - t1) / t1 * 100 if t1 else 0.0
        drss = (rss2 - rss1) / rss1 * 100 if rss1 else 0.0
        dtr = (tr2 - tr1) / tr1 * 100 if tr1 else 0.0
        dp = (p2 - p1) / p1 * 100 if p1 else 0.0
        cells = [
            fmt_count(cs),
            fmt_count(eb),
            fmt_count(lo),
            f'{t2:.2f}',
            f'{dt:+.1f}',
            fmt_bytes_int(rss2),
            f'{drss:+.0f}',
            fmt_count_int(tr2),
            f'{dtr:+.0f}',
            f'{p2:.2f}' if p2 else '-',
            f'{dp:+.0f}' if p1 and p2 else '-',
            {True: 'yes', False: 'no', None: '?'}[r2['peaked']],
        ]
        print(' '.join(f'{c:>{w}}' for c, (_, w) in zip(cells, headers)))


def _load(path_str):
    path = Path(path_str)
    if not path.exists():
        print(f'no such file: {path}', file=sys.stderr)
        sys.exit(1)
    return json.loads(path.read_text())


def report(args):
    data = _load(args.input)
    meta = data.get('meta', {})
    print(f'file: {args.input}')
    if meta:
        print(
            f"python={meta.get('python', '?')}  "
            f"total_objects={meta.get('total_objects')}  "
            f"started={meta.get('started')}  finished={meta.get('finished', '?')}"
        )
    runs = data.get('runs', [])
    print(f'runs: {len(runs)}')
    print()
    rows = [extract(r) for r in runs]
    _sort_rows(rows, getattr(args, 'sort', 'cycle_size,extra_bytes,live_objects'))
    render_flat(rows)
    _render_flat_legend()


def compare(args):
    base_data = _load(args.base)
    new_data = _load(args.new)
    label1 = base_data.get('meta', {}).get('python', args.base)
    label2 = new_data.get('meta', {}).get('python', args.new)

    rows1 = [extract(r) for r in base_data.get('runs', [])]
    rows2 = [extract(r) for r in new_data.get('runs', [])]
    sort = getattr(args, 'sort', 'cycle_size,extra_bytes,live_objects')
    _sort_rows(rows1, sort)
    _sort_rows(rows2, sort)

    render_compare(rows1, rows2, label1, label2)
    _render_compare_legend()


def _render_flat_legend():
    print()
    print('Legend:')
    print('  cycle      objects per reference cycle (--cycle-size)')
    print('  extra      extra bytes payload per cycle (--extra-bytes)')
    print('  live       live-object target before holder is cleared (--live-objects)')
    print('  time(s)    total wall time reported by the benchmark')
    print('  peakRSS    peak resident set size observed during the run')
    print('  maxTrash   max uncollected cyclic-garbage objects seen at once')
    print('  maxPause   max GC pause duration in ms observed during the run')
    print('  avgAlive   mean live object count across sample points')
    print('  avgTrash   mean uncollected garbage count across sample points')
    print('  peaked     yes if trash peaked during run; no = still rising')
    print('  rc         process return code (or error string on failure)')


def _render_compare_legend():
    print()
    print('Legend (base vs new, matched by cycle/extra/live):')
    print('  t(s)       total time for new build')
    print('  t%         percent change in time vs base, (new-base)/base*100')
    print('  rss        peak RSS for new build')
    print('  rss%       percent change in peak RSS vs base')
    print('  trash      max uncollected cyclic-garbage for new build')
    print('  trash%     percent change in max trash vs base')
    print('  pause      max GC pause (ms) for new build')
    print('  pause%     percent change in max GC pause vs base')
    print('  peaked     yes if new build RSS and trash peaked before final 25% of run')
