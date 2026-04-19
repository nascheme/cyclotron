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

Creates reference cycles of configurable size and payload, emits a CSV stream
of live/trash/rss/pause samples on stdout, and prints a summary to stderr.

```
python -m cyclotron run [--cycle-size N] [--extra-bytes N] \
    [--live-objects N] [--total-objects N] [--report-interval SEC]
```

Parameters:

- `--cycle-size` — objects per cycle (ring length)
- `--extra-bytes` — bytes payload attached to one node per cycle
- `--live-objects` — approximate live object count before the holder is cleared
- `--total-objects` — total objects created before exit
- `--report-interval` — seconds between CSV sample lines

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

The benchmark is intentionally artificial: it only creates ring-linked `Node`
objects (with `__slots__`) with an optional `bytes` payload. GC pause times
are captured via `gc.callbacks`. RSS is read from `/proc/self/status` and is
Linux-only.

A "peaked" flag in the summary indicates whether trash stabilized by the end
of the run: across full collections, is the last-seen trash count bounded by
its prior high-water mark? RSS is not used for this check because it is less
reliable of an indicator of stability.
