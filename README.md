# cyclotron

Python GC benchmark and stress test tool.

An artificial benchmark that stresses the CPython garbage collector under
controlled conditions and measures total run time, peak RSS, GC pause
distribution, and maximum uncollected-garbage object count.

## Installation

Requires Python 3.14+. Install in editable mode with [uv](https://docs.astral.sh/uv/):

```
uv sync
```

All functionality is exposed through a single entry point:

```
python -m cyclotron <command> [options]
```

Equivalently, once installed: `cyclotron <command> [options]`.

## Commands

### `run` — single benchmark run

Creates reference cycles of configurable shape and payload, emits a CSV
stream of live/trash/rss/pause samples on stdout, and prints a summary to
stderr.

```
python -m cyclotron run [--workload {chain,tree}] [--cycle-size N] \
    [--extra-bytes N] [--live-objects N] [--total-objects N] \
    [--cyclic-fraction F] [--report-interval SEC]
```

Parameters:

- `--workload` — `chain` (chain of `Node` objects) or `tree`
  (b-tree of `Record`s, default).
- `--cycle-size` — chain: objects per cycle (chain length). tree: records
  per tree.
- `--extra-bytes` — chain: bytes payload attached to one node per cycle.
  tree: upper bound for the per-record bytes payload.
- `--live-objects` — approximate live object/record count before the
  holder is cleared.
- `--total-objects` — total objects/records created before exit.
- `--cyclic-fraction` — fraction of new objects to make into cyclic
  trash. 0.0 makes objects fully refcount-freeable; 1.0 makes all
  objects have cycles and need GC to free.  Defaults to 1.0.
- `--report-interval` — seconds between CSV sample lines.

### `run-queue` — queue based benchmark

This benchmark continously creates small reference cycles and adds them to a
fixed length dequeue. Stats are printed to stdout on each iteration.

### `batch` — sweep across a parameter grid

Runs `run` across the cartesian product of `cycle-size × extra-bytes ×
live-objects` for one or more Python executables, writing a JSON result file
per executable. Results are persisted after every run so interrupted sweeps are
recoverable.

```
python -m cyclotron batch \
    --exec ./python-base --output results/base.json \
    --exec ./python-inc  --output results/inc.json
```

`--exec` and `--output` are paired and may be repeated. Override the grid with
`--cycle-sizes`, `--extra-bytes`, `--live-objects`.

### `report` — show results for one file

```
python -m cyclotron report results/base.json
```

### `compare` — side-by-side comparison

```
python -m cyclotron compare results/base.json results/inc.json
```

Both reporting commands accept `--sort KEY1,KEY2,...` to order the rows.

## Design notes

The benchmark supports two workloads:

- **`chain`** — chain-linked `Node` objects (with `__slots__`)
  with an optional `bytes` payload.
- **`tree`** — b-tree of `Record`s adapted from the
  pyperformance `btree` benchmark. Each "allocation unit" is a full
  `BTree` (with branching factor 16) containing `--cycle-size` records,
  built in randomized key order with the bottom 20% of keys deleted and
  re-inserted to fragment in-memory layout.

GC pause times are captured via `gc.callbacks`. RSS is read from
`/proc/self/status` and is Linux-only.

A "stable" flag in the summary indicates whether the trash count has reached
steady state by the end of the run. A background sampler thread snapshots
`num_trash` at a fixed time interval (independent of any GC implementation
detail); the tail of that series is then grouped into blocks and a robust
Theil-Sen slope is fit on the per-block max and median. The run is flagged
stable if neither slope is rising beyond the configured limits. RSS is not
used for this check because it is less reliable of an indicator of stability.
