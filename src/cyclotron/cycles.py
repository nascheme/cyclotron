import gc
import statistics
import sys
import time


def get_memory_usage():
    result = {'peak': 0, 'rss': 0}
    with open('/proc/self/status') as status:
        for line in status:
            parts = line.split()
            key = parts[0][2:-1].lower()
            if key in result:
                result[key] = int(parts[1])
    return result


def get_rss_kb():
    return get_memory_usage().get('rss', 0)


class Node:
    __slots__ = ('next', 'payload')


class Stats:
    num_alive = 0
    num_trash = 0
    max_trash = 0
    peak_rss = 0


def make_cycle(cycle_size, extra_bytes):
    nodes = [Node() for _ in range(cycle_size)]
    for i, n in enumerate(nodes):
        n.next = nodes[(i + 1) % cycle_size]
        n.payload = None
    if extra_bytes:
        nodes[0].payload = bytes(extra_bytes)
    return nodes[0]


_gc_pause_start: float | None = None
_gc_pauses: list[float] = []
_interval_max_pause: float = 0.0

# Peaked detection: did trash stabilize by end of run?
# Track (count, hw, last) per generation; peaked = last <= max_of_all_previous.
# Keyed by whatever generation numbers the GC uses (handles free-threaded Python).
# RSS is not used: freed pages are rarely returned to the OS so RSS grows
# monotonically even for a stable workload.
_gc_counts: dict[int, int] = {}
_gc_trash_hw: dict[int, int] = {}
_gc_trash_last: dict[int, int] = {}


def gc_callback(phase, info):
    global _gc_pause_start, _interval_max_pause
    if phase == 'start':
        _gc_pause_start = time.perf_counter()
        gen = info.get('generation')
        if isinstance(gen, int):
            hw = _gc_trash_hw.get(gen, 0)
            last = _gc_trash_last.get(gen, 0)
            _gc_trash_hw[gen] = max(hw, last)
            _gc_trash_last[gen] = Stats.num_trash
            _gc_counts[gen] = _gc_counts.get(gen, 0) + 1
    elif phase == 'stop':
        if _gc_pause_start is not None:
            dur = time.perf_counter() - _gc_pause_start
            _gc_pauses.append(dur)
            if dur > _interval_max_pause:
                _interval_max_pause = dur
            _gc_pause_start = None
        collected = info.get('collected') or 0
        Stats.num_trash -= collected
        Stats.num_alive -= collected
        if Stats.num_trash < 0:
            Stats.num_trash = 0


def pause_percentiles(pauses):
    """Return (count, total_ms, mean_ms, p50_ms, p95_ms, p99_ms, max_ms)."""
    n = len(pauses)
    if n == 0:
        return None
    s = sorted(pauses)
    total_ms = sum(s) * 1000
    mean_ms = total_ms / n
    max_ms = s[-1] * 1000
    if n >= 2:
        qs = statistics.quantiles(s, n=100)  # 99 cut-points; index k → p(k+1)
        p50_ms = qs[49] * 1000
        p95_ms = qs[94] * 1000
        p99_ms = qs[98] * 1000
    else:
        p50_ms = p95_ms = p99_ms = max_ms
    return n, total_ms, mean_ms, p50_ms, p95_ms, p99_ms, max_ms


def add_args(p):
    p.add_argument(
        '--cycle-size',
        type=int,
        default=4,
        help='objects per cycle (ring length)',
    )
    p.add_argument(
        '--extra-bytes',
        type=int,
        default=0,
        help='extra bytes payload attached to each cycle',
    )
    p.add_argument(
        '--live-objects',
        type=int,
        default=10_000,
        help='approximate live object count before releasing',
    )
    p.add_argument(
        '--total-objects',
        type=int,
        default=1_000_000,
        help='exit after creating this many objects',
    )
    p.add_argument(
        '--report-interval',
        type=float,
        default=0.25,
        help='seconds between status lines',
    )


def main(args=None):
    if args is None:
        import argparse

        p = argparse.ArgumentParser(description='Cyclic GC benchmark')
        add_args(p)
        args = p.parse_args()

    gc.callbacks.append(gc_callback)

    holder = []
    created = 0
    cycle_size = args.cycle_size
    extra_bytes = args.extra_bytes
    live_target = args.live_objects
    total = args.total_objects
    interval = args.report_interval

    start = time.perf_counter()
    next_report = start

    print('time_ms,alive,trash,rss_kb,pause_ms')

    def report(now):
        global _interval_max_pause
        rss = get_rss_kb()
        if rss > Stats.peak_rss:
            Stats.peak_rss = rss
        elapsed_ms = (now - start) * 1000.0
        pause_ms = _interval_max_pause * 1000.0
        _interval_max_pause = 0.0
        print(
            f'{elapsed_ms:.1f},{Stats.num_alive},{Stats.num_trash},{rss},{pause_ms:.3f}'
        )

    while created < total:
        holder.append(make_cycle(cycle_size, extra_bytes))
        Stats.num_alive += cycle_size
        Stats.num_trash += cycle_size
        if Stats.num_trash > Stats.max_trash:
            Stats.max_trash = Stats.num_trash
        created += cycle_size

        if len(holder) * cycle_size >= live_target:
            holder.clear()

        now = time.perf_counter()
        if now >= next_report:
            report(now)
            next_report = now + interval

    report(time.perf_counter())
    end = time.perf_counter()
    print(f'Python: {sys.executable}', file=sys.stderr)
    print(f'Python version: {sys.version}', file=sys.stderr)
    print(f'Total time: {end - start:.3f}s', file=sys.stderr)
    print(f'Peak RSS: {Stats.peak_rss} KB', file=sys.stderr)
    print(f'Max trash: {Stats.max_trash}', file=sys.stderr)
    print(f'Created objects: {created}', file=sys.stderr)
    max_gen = max(_gc_counts) if _gc_counts else -1
    full_count = _gc_counts.get(max_gen, 0)
    print(f'GC full collections: {full_count}', file=sys.stderr)
    if full_count >= 2:
        peaked = _gc_trash_last[max_gen] <= _gc_trash_hw[max_gen]
    else:
        peaked = None
    if peaked is not None:
        print(f'GC peaked: {"yes" if peaked else "no"}', file=sys.stderr)
    ps = pause_percentiles(_gc_pauses)
    if ps:
        n, total_ms, mean_ms, p50_ms, p95_ms, p99_ms, max_ms = ps
        print(f'GC pause count: {n}', file=sys.stderr)
        print(f'GC pause total ms: {total_ms:.3f}', file=sys.stderr)
        print(f'GC pause mean ms: {mean_ms:.3f}', file=sys.stderr)
        print(f'GC pause p50 ms: {p50_ms:.3f}', file=sys.stderr)
        print(f'GC pause p95 ms: {p95_ms:.3f}', file=sys.stderr)
        print(f'GC pause p99 ms: {p99_ms:.3f}', file=sys.stderr)
        print(f'GC pause max ms: {max_ms:.3f}', file=sys.stderr)


if __name__ == '__main__':
    main()
