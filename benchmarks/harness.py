"""Timing harness for the suite's benchmarks.

It lives inside the package because the package is the thing that gets split out, and
a benchmark that cannot run in the public repo is not a benchmark. The host's own
benchmarks import it from here rather than keeping a copy — host code may depend on
package code, never the reverse, and one harness cannot drift from itself the way
``market_hours.py`` and ``market_hours.hpp`` can.

Two kinds of number come out of here, and they are gated differently on purpose:

- **Metrics** are wall-clock cost. They only mean anything against a baseline recorded
  on the same machine, so a metric is compared to ``baseline.json`` with a deliberately
  loose multiplier. A metric regression is a question ("what did we add?"), not a verdict.
- **Ratios** are one metric over another — what happens to cost when the input grows 4x.
  Those are machine-independent: a laptop and a server disagree about milliseconds but
  agree that a linear pass is linear. Each ratio carries a `limit` declared in the
  benchmark itself, and a breach is a real failure that needs no baseline to establish.

Repetition is calibrated rather than fixed: every measurement runs until it has spent
``MIN_TOTAL_S``, so a 2 us call and a 200 ms call both get a stable median without the
cheap one taking all day or the expensive one reporting a single noisy sample.
"""
import gc
import json
import platform
import statistics
import sys
import time

MIN_TOTAL_S = 0.25   # per measurement; the whole point is a stable median, not precision
MIN_REPS = 5
MAX_REPS = 5000


def _one(fn, setup) -> float:
    """One timed call. *setup* runs first and is deliberately outside the clock, so a
    benchmark that has to reset state (drop a cache, rebuild a DB) measures the call
    and not the reset."""
    if setup is not None:
        setup()
    t0 = time.perf_counter()
    fn()
    return time.perf_counter() - t0


def measure(name: str, fn, *, setup=None, note: str = "", items: int | None = None) -> dict:
    """Time *fn* and return a metric dict in milliseconds.

    GC is off for the duration: a collection landing inside one rep is the single
    largest source of spurious "regressions" in a run this short, and the code under
    test does not depend on when it happens.

    *items* is the unit of work per call (rows written, symbols ranked) — reported as
    throughput, which is the number a reader can actually compare against their own
    workload. It is informational; the gate is always the median.
    """
    gc.collect()
    gc.disable()
    try:
        first = _one(fn, setup)
        reps = MAX_REPS if first <= 0 else int(MIN_TOTAL_S / first)
        reps = max(MIN_REPS, min(MAX_REPS, reps))
        samples = [_one(fn, setup) for _ in range(reps)]
    finally:
        gc.enable()

    samples.sort()
    median = statistics.median(samples)
    metric = {
        "id": name,
        "unit": "ms",
        # The median is what gets gated; min is the least noise-contaminated estimate of
        # true cost and p90 says how noisy this machine was while measuring.
        "median": round(median * 1000, 4),
        "min": round(samples[0] * 1000, 4),
        "p90": round(samples[int(0.9 * (len(samples) - 1))] * 1000, 4),
        "reps": reps,
    }
    if items:
        metric["items_per_s"] = round(items / median) if median > 0 else None
        metric["items"] = items
    if note:
        metric["note"] = note
    return metric


def ratio(name: str, slow: dict, fast: dict, *, limit: float, note: str = "") -> dict:
    """How much more *slow* costs than *fast* — a scaling check.

    Give it two metrics from the same benchmark at different input sizes and a `limit`
    derived from the complexity you believe the code has. Unlike a metric this needs no
    baseline: it is a claim about the algorithm, and it holds on any machine.
    """
    value = slow["median"] / fast["median"] if fast["median"] else float("inf")
    return {
        "id": name,
        "value": round(value, 2),
        "limit": limit,
        "of": [slow["id"], fast["id"]],
        "note": note,
    }


def machine() -> dict:
    """Enough about where this ran to tell two baselines apart. A baseline recorded
    somewhere else is not wrong, it is just about a different computer — `report.py`
    says so rather than pretending the comparison is meaningful.

    Deliberately coarse. Baselines are committed, and the package's one is published, so
    this is a description of a *class* of machine, not a fingerprint of a particular
    one: `platform.platform()` would put an exact kernel build and glibc version in a
    public file, and neither of them moves a benchmark. The interpreter's minor version
    does, so that is the only version kept."""
    return {
        "python": ".".join(platform.python_version_tuple()[:2]),
        "platform": platform.system(),
        "processor": platform.machine(),
    }


def emit(suite: str, metrics: list, ratios=()) -> None:
    """Write the run to stdout as JSON. Benchmarks print nothing else — `bench.sh`
    captures this and hands it to `report.py`, which owns every human-readable word."""
    json.dump({"suite": suite, "machine": machine(),
               "metrics": metrics, "ratios": list(ratios)},
              sys.stdout, indent=1)
    sys.stdout.write("\n")
