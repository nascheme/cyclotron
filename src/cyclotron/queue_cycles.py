# Tim Peters, Python GC stress test.
# Lightly modified by Neil Schemenauer
import gc
from collections import deque

from . import util


class Node:
    def __init__(self, nnodes=10_000):
        self.payload = bytes(nnodes)
        self.me = self


def add_args(p):
    pass


def main(args=None):
    gc.collect()

    lasttotalcoll = totalcoll = gen = 0

    def cb(phase, info):
        nonlocal totalcoll, gen
        if phase != "stop":
            return
        totalcoll += info["collected"]
        gen = info["generation"]

    gc.callbacks.append(cb)

    d = deque(maxlen=1000)
    c = 0
    gc.set_threshold(2000, 10)
    maxrss = 0
    maxat = 0
    lastdiap = 0
    while True:
        c += 1
        d.append(Node())
        rss = util.get_memory_usage()
        if rss > maxrss:
            maxrss = rss
            maxat = c
        if totalcoll == lasttotalcoll:
            continue
        print(
            f"i={c:_}",
            f"rss={rss / 1e6:_.1f}",
            f"rss%={rss / maxrss:7.2%}",
            f"maxrss={maxrss / 1e6:_.1f}",
            f"maxat={maxat:_}",
            f"gen={gen}",
            f"coll={totalcoll:_}",
            f"delta-coll={totalcoll - lasttotalcoll:_}",
            f"live={c - totalcoll:_}",
            f"delta-disp={c - lastdiap:_}",
        )
        lasttotalcoll = totalcoll
        lastdiap = c
