"""Render text table reports from batch sweep JSON files."""

import json
import sys
from pathlib import Path


def to_float(s, default=0.0):
    if s is None:
        return default
    try:
        return float(str(s).split()[0])
    except (ValueError, IndexError):
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
        'workload': p.get('workload', 'chain'),
        'cycle_size': p['cycle_size'],
        'extra_bytes': p['extra_bytes'],
        'live_objects': p['live_objects'],
        'cyclic_fraction': p.get('cyclic_fraction', 1.0),
        'python': p['python'],
        'wall': run.get('wall_time', 0.0),
        'total_time': (
            0.0
            if str(summary.get('Total time', '')).strip().startswith('n/a')
            else to_float(summary.get('Total time'), run.get('wall_time', 0.0))
        ),
        'peak_rss_kb': max(to_int(summary.get('Peak RSS')), peak_rss),
        'max_trash': max(to_int(summary.get('Max trash')), max_trash_seen),
        'avg_alive': int(avg_alive),
        'avg_trash': int(avg_trash),
        'max_pause_ms': max_pause_ms,
        'stable': run.get('stable'),
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
        ('wl', 5, lambda r: r['workload']),
        ('cycle', 8, lambda r: fmt_count(r['cycle_size'])),
        ('extra', 8, lambda r: fmt_count(r['extra_bytes'])),
        ('live', 8, lambda r: fmt_count(r['live_objects'])),
        ('cyc%', 6, lambda r: f"{r['cyclic_fraction'] * 100:.0f}"),
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
            'stable',
            7,
            lambda r: {True: 'yes', False: 'no', None: '?'}[r['stable']],
        ),
        ('rc', 4, lambda r: str(r['rc']) if not r['error'] else r['error']),
    ]
    line = ' '.join(f'{h:>{w}}' for h, w, _ in headers)
    print(line)
    print('-' * len(line))
    for r in rows:
        print(' '.join(f'{fn(r):>{w}}' for _, w, fn in headers))


def render_compare(rows1, rows2, label1, label2):
    def key(r):
        return (
            r['workload'],
            r['cycle_size'],
            r['extra_bytes'],
            r['live_objects'],
            r['cyclic_fraction'],
        )

    by_key1 = {key(r): r for r in rows1}
    by_key2 = {key(r): r for r in rows2}
    keys = sorted(set(by_key1) & set(by_key2))

    headers = [
        ('wl', 5),
        ('cycle', 8),
        ('extra', 8),
        ('live', 8),
        ('cyc%', 6),
        ('t(s)', 8),
        ('r-t', 6),
        ('rss', 8),
        ('r-rss', 6),
        ('trash', 9),
        ('r-trash', 8),
        ('pause', 8),
        ('r-pause', 8),
        ('stable', 7),
    ]
    line = ' '.join(f'{h:>{w}}' for h, w in headers)
    print()
    print(f'base={label1}  vs  new={label2}')
    print(line)
    print('-' * len(line))

    def ratio(new, base):
        return f'{new / base:.1f}' if base else '-'

    for k in keys:
        r1, r2 = by_key1[k], by_key2[k]
        wl, cs, eb, lo, cf = k
        t1, t2 = r1['total_time'], r2['total_time']
        rss1, rss2 = r1['peak_rss_kb'], r2['peak_rss_kb']
        tr1, tr2 = r1['max_trash'], r2['max_trash']
        p1, p2 = r1['max_pause_ms'], r2['max_pause_ms']
        cells = [
            wl,
            fmt_count(cs),
            fmt_count(eb),
            fmt_count(lo),
            f'{cf * 100:.0f}',
            f'{t2:.2f}',
            ratio(t2, t1),
            fmt_bytes_int(rss2),
            ratio(rss2, rss1),
            fmt_count_int(tr2),
            ratio(tr2, tr1),
            f'{p2:.2f}' if p2 else '-',
            ratio(p2, p1) if p1 and p2 else '-',
            {True: 'yes', False: 'no', None: '?'}[r2['stable']],
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
    print('  wl         workload mode (chain or tree)')
    print('  cycle      chain length (chain) or records per tree (tree)')
    print('  extra      bytes payload per cycle (chain) or max bytes per record (tree)')
    print('  live       live-object/record target before holder is cleared')
    print('  cyc%       fraction of allocation units made cyclic')
    print('  time(s)    total wall time reported by the benchmark')
    print('  peakRSS    peak resident set size observed during the run')
    print('  maxTrash   max uncollected cyclic-garbage objects seen at once')
    print('  maxPause   max GC pause duration in ms observed during the run')
    print('  avgAlive   mean live object count across sample points')
    print('  avgTrash   mean uncollected garbage count across sample points')
    print('  stable     yes if trash series was stable (non-rising) in tail window')
    print('  rc         process return code (or error string on failure)')


def _render_compare_legend():
    print()
    print('Legend (base vs new, matched by wl/cycle/extra/live/cyc%):')
    print('  wl         workload mode (chain or tree)')
    print('  cyc%       fraction of allocation units made cyclic')
    print('  t(s)       total time for new build')
    print('  r-t        ratio of new/base total time (1.0 = equal, 2.0 = 2x slower)')
    print('  rss        peak RSS for new build')
    print('  r-rss      ratio of new/base peak RSS')
    print('  trash      max uncollected cyclic-garbage for new build')
    print('  r-trash    ratio of new/base max trash')
    print('  pause      max GC pause (ms) for new build')
    print('  r-pause    ratio of new/base max GC pause')
    print('  stable     yes if new build trash count was stable (non-rising)')
