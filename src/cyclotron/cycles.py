import gc
import random
import statistics
import sys
import threading
import time
from statistics import median

from cyclotron import btree, util


def theil_sen_fit(xs, ys):
    """Robust line fit via median of pairwise slopes."""
    n = len(xs)
    slopes = []
    for i in range(n):
        for j in range(i + 1, n):
            dx = xs[j] - xs[i]
            if dx != 0:
                slopes.append((ys[j] - ys[i]) / dx)
    slope = median(slopes)
    intercept = median(y - slope * x for x, y in zip(xs, ys))
    return intercept, slope


def median_absolute_deviation(values):
    m = median(values)
    return median(abs(v - m) for v in values)


def block_reduce(values, block_size):
    """Group `values` into consecutive non-overlapping blocks and return
    (block_maxes, block_medians). A trailing partial block is dropped."""
    n_blocks = len(values) // block_size
    maxes = []
    medians = []
    for i in range(n_blocks):
        block = values[i * block_size : (i + 1) * block_size]
        maxes.append(max(block))
        medians.append(median(block))
    return maxes, medians


def gc_stability_check(
    trash_counts,
    window=20,
    abs_slope_limit=100.0,
    rel_slope_limit=0.05,
):
    """Decide whether a trash-count series has reached steady state.

    Fits a Theil-Sen slope over the last `window` samples and requires
    the slope (absolute and relative to median) to be below its limits.
    The check is signed, not abs-valued: a negative slope (trash
    decreasing) always passes — we only care that trash isn't rising.
    """
    if len(trash_counts) < window:
        return {'ok': False, 'reason': 'not enough samples'}

    ys = trash_counts[-window:]
    xs = list(range(len(ys)))
    _, slope = theil_sen_fit(xs, ys)
    med = median(ys)
    mad = median_absolute_deviation(ys)
    rel_slope = slope / med if med > 0 else 0.0
    rel_spread = mad / med if med > 0 else 0.0
    ok = slope <= abs_slope_limit and rel_slope <= rel_slope_limit
    return {
        'ok': ok,
        'window': window,
        'slope': slope,
        'rel_slope': rel_slope,
        'median': med,
        'mad': mad,
        'rel_spread': rel_spread,
    }


def get_rss_kb():
    return util.get_memory_usage()


class Node:
    __slots__ = ('next', 'payload', 'witness')


class Stats:
    num_alive = 0
    num_trash = 0
    max_trash = 0
    peak_rss = 0


class Witness:
    __slots__ = ('count',)

    def __init__(self, count):
        self.count = count

    def __del__(self):
        Stats.num_alive -= self.count
        Stats.num_trash -= self.count


def make_chain(cycle_size, extra_bytes, should_loop):
    first = node = Node()
    for _ in range(cycle_size - 1):
        node.next = Node()
        node = node.next
    if extra_bytes:
        first.payload = bytes(extra_bytes)
    first.witness = Witness(cycle_size)
    if should_loop:
        node.next = first
    return first


_GC_PAUSE_START: float | None = None
_GC_PAUSES: list[float] = []
_INTERVAL_PAUSE_TOTAL: float = 0.0
_INTERVAL_MAX_PAUSE: float = 0.0

# Stability detection: a background sampler thread snapshots Stats.num_trash
# on a fixed time interval. At report time the series is grouped into
# fixed-size blocks and robust slope tests are run on block-max and
# block-median. Stable = both flat. Time-based sampling is independent of
# GC implementation (matters for incremental collectors that never do a
# full cycle).
_GC_COUNTS: dict[int, int] = {}
_TRASH_SAMPLES: list[int] = []
_RSS_SAMPLES: list[int] = []
_SAMPLER_STOP = threading.Event()


def gc_callback(phase, info):
    global _GC_PAUSE_START, _INTERVAL_PAUSE_TOTAL, _INTERVAL_MAX_PAUSE
    if phase == 'start':
        _GC_PAUSE_START = time.perf_counter()
        gen = info.get('generation')
        if isinstance(gen, int):
            _GC_COUNTS[gen] = _GC_COUNTS.get(gen, 0) + 1
    elif phase == 'stop':
        if _GC_PAUSE_START is not None:
            dur = time.perf_counter() - _GC_PAUSE_START
            _GC_PAUSES.append(dur)
            _INTERVAL_PAUSE_TOTAL += dur
            if dur > _INTERVAL_MAX_PAUSE:
                _INTERVAL_MAX_PAUSE = dur
            _GC_PAUSE_START = None


def _trash_sampler(interval):
    while not _SAMPLER_STOP.wait(interval):
        _TRASH_SAMPLES.append(Stats.num_trash)
        _RSS_SAMPLES.append(get_rss_kb())


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
        '--workload',
        choices=('chain', 'tree'),
        default='chain',
        help='allocation pattern: chain of Node objects or b-tree of Records',
    )
    p.add_argument(
        '--cycle-size',
        type=int,
        default=4,
        help='chain: objects per cycle. tree: records per tree.',
    )
    p.add_argument(
        '--extra-bytes',
        type=int,
        default=0,
        help='chain: bytes payload per cycle. tree: max bytes per record.',
    )
    p.add_argument(
        '--live-objects',
        type=int,
        default=10_000,
        help='approximate live object/record count before releasing',
    )
    p.add_argument(
        '--total-objects',
        type=int,
        default=1_000_000,
        help='threshold of objects/records to produce before the run may'
        ' exit (see --min-runtime / --max-runtime for exit conditions)',
    )
    p.add_argument(
        '--cyclic-fraction',
        type=float,
        default=1.0,
        help='fraction of allocation units (chains or trees) made cyclic (0..1)',
    )
    p.add_argument(
        '--report-interval',
        type=float,
        default=0.25,
        help='seconds between status lines',
    )
    p.add_argument(
        '--sample-interval',
        type=float,
        default=0.25,
        help='seconds between trash-count samples taken by the background'
        ' sampler thread (feeds the stability check)',
    )
    p.add_argument(
        '--min-runtime',
        type=float,
        default=10.0,
        help='minimum wall-clock seconds before the run may exit. A run'
        ' exits once --total-objects has been produced, --min-runtime has'
        ' elapsed, and the trash-count series looks stable.',
    )
    p.add_argument(
        '--max-runtime',
        type=float,
        default=60.0,
        help='hard wall-clock cap. A run always exits at this point even'
        ' if the trash-count series has not stabilized.',
    )


def _make_tree(cycle_size, extra_bytes, should_loop):
    # We want to end up allocating 'cycle_size' container objects.  Using
    # a factor of 2x like this gets us close.  We don't need to be exact.
    tree_size = max(1, cycle_size // 2)
    records = list(btree.make_records(tree_size, extra_bytes))
    tree = btree.make_tree(tree_size, records)
    witness = Witness(cycle_size)
    tree.witness = witness
    if should_loop:
        tree.loop = tree
    return tree


def _calibrate_tree(cycle_size, cyclic_fraction, extra_bytes):
    """Return per_tree_total.

    Builds one canonical tree under controlled GC state and measures the
    object-count delta via gc.get_objects().
    """
    gc.collect()
    before = len(gc.get_objects())
    probe = _make_tree(cycle_size, extra_bytes, False)
    after = len(gc.get_objects())
    per_tree_total = after - before
    del probe
    gc.collect()
    return per_tree_total


def main(args=None):
    if args is None:
        import argparse

        p = argparse.ArgumentParser(description='Cyclic GC benchmark')
        add_args(p)
        args = p.parse_args()

    gc.callbacks.append(gc_callback)

    created = 0
    workload = args.workload
    cycle_size = args.cycle_size
    extra_bytes = args.extra_bytes
    live_target = args.live_objects
    total = args.total_objects
    interval = args.report_interval
    cyclic_fraction = args.cyclic_fraction
    min_runtime = args.min_runtime
    max_runtime = args.max_runtime

    sampler = threading.Thread(
        target=_trash_sampler, args=(args.sample_interval,), daemon=True
    )
    sampler.start()

    start = time.perf_counter()
    next_report = start

    print('time_ms,alive,trash,rss_kb,pause_ms,gc_time_ms')

    def report(now):
        global _INTERVAL_PAUSE_TOTAL, _INTERVAL_MAX_PAUSE
        rss = get_rss_kb()
        if rss > Stats.peak_rss:
            Stats.peak_rss = rss
        elapsed_ms = (now - start) * 1000.0
        gc_time_ms = _INTERVAL_PAUSE_TOTAL * 1000.0
        pause_ms = _INTERVAL_MAX_PAUSE * 1000.0
        _INTERVAL_PAUSE_TOTAL = 0.0
        _INTERVAL_MAX_PAUSE = 0.0
        print(
            f'{elapsed_ms:.1f},{Stats.num_alive},{Stats.num_trash},{rss},{pause_ms:.3f},{gc_time_ms:.3f}'
        )

    def _series_stable(samples, **kw):
        block_maxes, block_medians = block_reduce(samples, block_size=8)
        stab_max = gc_stability_check(block_maxes, window=5, **kw)
        stab_med = gc_stability_check(block_medians, window=5, **kw)
        return stab_max, stab_med

    def _stable_now():
        t_max, t_med = _series_stable(_TRASH_SAMPLES)
        for s in (t_max, t_med):
            if 'reason' in s:
                return False
        return t_max['ok'] and t_med['ok']

    stability_check_interval = 1.0
    next_stability_check = start + stability_check_interval
    last_stable = False

    def keep_going(now):
        nonlocal next_stability_check, last_stable
        elapsed = now - start
        if elapsed >= max_runtime:
            return False
        if created < total:
            return True
        if elapsed < min_runtime:
            return True
        if now >= next_stability_check:
            last_stable = _stable_now()
            next_stability_check = now + stability_check_interval
        return not last_stable

    rnd = random.Random(0)
    created_done = None  # perf_counter when `created` first reached `total`

    if workload == 'chain':
        holder = []
        while keep_going(time.perf_counter()):
            should_loop = rnd.random() <= cyclic_fraction
            holder.append(make_chain(cycle_size, extra_bytes, should_loop))
            Stats.num_alive += cycle_size
            created += cycle_size
            if created_done is None and created >= total:
                created_done = time.perf_counter()

            if len(holder) * cycle_size >= live_target:
                Stats.num_trash += len(holder) * cycle_size
                holder.clear()  # will free non cyclic trash, execute those witnesses
                if Stats.num_trash > Stats.max_trash:
                    Stats.max_trash = Stats.num_trash

            now = time.perf_counter()
            if now >= next_report:
                report(now)
                next_report = now + interval
    else:
        holder = []
        while keep_going(time.perf_counter()):
            should_loop = rnd.random() <= cyclic_fraction
            tree = _make_tree(cycle_size, extra_bytes, should_loop)
            holder.append(tree)
            Stats.num_alive += cycle_size
            created += cycle_size
            if created_done is None and created >= total:
                created_done = time.perf_counter()

            if len(holder) * cycle_size >= live_target:
                # Refcount-freed portion drops the moment the trees are
                # released; the witness's __del__ handles the cyclic portion.
                Stats.num_trash += len(holder) * cycle_size
                holder.clear()  # will free non cyclic trash, execute those witnesses
                if Stats.num_trash > Stats.max_trash:
                    Stats.max_trash = Stats.num_trash

            now = time.perf_counter()
            if now >= next_report:
                report(now)
                next_report = now + interval

    report(time.perf_counter())
    end = time.perf_counter()
    _SAMPLER_STOP.set()
    sampler.join(timeout=1.0)
    print(f'Python: {sys.executable}', file=sys.stderr)
    print(f'Python version: {sys.version}', file=sys.stderr)
    print(f'Workload: {workload}', file=sys.stderr)
    print(f'Cycle size: {cycle_size}', file=sys.stderr)
    print(f'Cyclic fraction: {cyclic_fraction}', file=sys.stderr)
    if workload == 'tree':
        per_tree_total = _calibrate_tree(
            cycle_size,
            cyclic_fraction,
            extra_bytes,
        )
        print(f'Per-tree count: {per_tree_total}', file=sys.stderr)
    if created_done is not None:
        print(f'Total time: {created_done - start:.3f}s', file=sys.stderr)
    else:
        print('Total time: n/a', file=sys.stderr)
    print(f'Wall time: {end - start:.3f}s', file=sys.stderr)
    print(f'Peak RSS: {Stats.peak_rss} KB', file=sys.stderr)
    print(f'Max trash: {Stats.max_trash}', file=sys.stderr)
    print(f'Created objects: {created}', file=sys.stderr)
    max_gen = max(_GC_COUNTS) if _GC_COUNTS else -1
    full_count = _GC_COUNTS.get(max_gen, 0)
    print(f'GC full collections: {full_count}', file=sys.stderr)
    t_max, t_med = _series_stable(_TRASH_SAMPLES)
    checks = [
        ('trash max', t_max),
        ('trash median', t_med),
    ]
    reasons = [s.get('reason') for _, s in checks if 'reason' in s]
    if reasons:
        stable = None
        print(f'GC stable: unknown ({reasons[0]})', file=sys.stderr)
    else:
        stable = all(s['ok'] for _, s in checks)
        print(f'GC stable: {"yes" if stable else "no"}', file=sys.stderr)
        for label, s in checks:
            print(
                f'GC stability ({label}): slope={s["slope"]:.3f} '
                f'rel_slope={s["rel_slope"]:.5f} '
                f'median={s["median"]:.0f} '
                f'mad={s["mad"]:.0f} '
                f'rel_spread={s["rel_spread"]:.4f} '
                f'window={s["window"]}',
                file=sys.stderr,
            )
    ps = pause_percentiles(_GC_PAUSES)
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
