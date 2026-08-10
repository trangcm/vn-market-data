"""Benchmark the store-first read path — the package's actual product.

The pitch is that a caller asks for candles as often as it likes and the network is not
involved, so the number that matters is what a *cache hit* costs and how it grows. Every
source here is a fake returning canned rows: no network, no clock dependence beyond
today's date, same numbers on any machine that runs it twice.

Three things are being watched, and each would be invisible in the test suite:

- **The warm read**, because it is on the hot path of every engine in the suite. Pattern
  geometry, RS ranking and the backtester each re-read the same series many times a pass.
- **How a read grows with series length**, because the store returns whole ranges and a
  consumer asking for five years pays for five years.
- **How a board read grows** — with the number of symbols (one query each, by design)
  and with how much board history has accumulated behind them. The second is the sneaky
  one: `md_board` is append-only, `latest_board` sorts a symbol's snapshots to take the
  newest, and `idx_md_board_symbol` covers the symbol but not the ordering. Nothing about
  that changes as it degrades — the reads stay correct and quietly get slower.

Run it directly (`python -m vn_market_data.benchmarks.bench_store`); it writes JSON to
stdout for `report.py` and nothing else.
"""
import sqlite3
import tempfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import vn_market_data as vmd
from vn_market_data.benchmarks.harness import emit, measure, ratio
from vn_market_data.sources.base import DataSource

TODAY = date.today()


def _business_days(back: int) -> list[str]:
    """ISO dates for the last *back* calendar days, weekends dropped — oldest first."""
    days = [TODAY - timedelta(days=i) for i in range(back, -1, -1)]
    return [d.isoformat() for d in days if d.weekday() < 5]


class _Canned(DataSource):
    """Answers instantly from memory. The point is to measure the store, so a source
    that did anything interesting would be measuring the wrong thing."""

    name = "canned"

    def __init__(self, rows: list[dict], board: dict):
        self._rows, self._board = rows, board

    def get_ohlcv(self, symbol, start, end, *, is_index=False):
        return [r for r in self._rows if start <= r["date"] <= end]

    def get_board(self, symbols):
        return {s: self._board for s in symbols}


def _candles(back: int) -> list[dict]:
    return [{"date": d, "open": 20000.0, "high": 21000.0, "low": 19500.0,
             "close": 20500.0, "volume": 1_000_000.0}
            for d in _business_days(back)]


# Column order is spelled out rather than taken from the dict's insertion order, so a
# reordered literal cannot silently write foreign buys into the ceiling column.
_BOARD_COLS = ("foreign_buy_value", "foreign_sell_value", "foreign_net_value",
               "ceiling", "floor", "ref_price", "close", "traded_value", "traded_volume")
_BOARD_ROW = {"foreign_buy_value": 1e9, "foreign_sell_value": 8e8, "foreign_net_value": 2e8,
              "ceiling": 22000.0, "floor": 19000.0, "ref_price": 20500.0, "close": 20500.0,
              "traded_value": 5e10, "traded_volume": 2.4e6}


def _seed_ohlcv(symbol: str, back: int) -> None:
    """Fill the store the way a real first run would — through the adapter, so the
    freshness metadata lands too and the next call is genuinely a warm read.

    Both the canned series *and* the seeding request reach back further than the
    lookback we then measure, and the second one is the subtle half: the adapter passes
    its own `today - lookback` down as the source's `start`, so seeding at `back` would
    filter the deeper candles out at the source and bank a history whose oldest row is
    the first *business* day on or after `today - back`. Whenever that date is a
    weekend, `get_ohlcv` would then see a stored history shallower than the request,
    refetch on every single call, and quietly turn the warm-read benchmark into a second
    measurement of the cold path — two days in seven, depending only on the calendar.
    """
    vmd.set_sources([_Canned(_candles(back + 60), _BOARD_ROW)])
    vmd.get_ohlcv(symbol, lookback_days=back + 60)


def _seed_board(symbols: list[str], snapshots_per_symbol: int) -> None:
    """Insert *snapshots_per_symbol* board rows for each symbol, the newest one fresh.

    Written straight to the table rather than through `insert_board`, which stamps every
    row with `now()` — the whole point here is a spread of timestamps, i.e. what the
    table looks like after weeks of 6-hourly snapshots rather than after one.
    """
    now = datetime.now(timezone.utc)
    values = tuple(_BOARD_ROW[c] for c in _BOARD_COLS)
    with vmd.connect() as conn:
        for sym in symbols:
            conn.executemany(
                f"""INSERT OR IGNORE INTO md_board(symbol, ts, {", ".join(_BOARD_COLS)}, source)
                    VALUES ({",".join("?" * (len(_BOARD_COLS) + 3))})""",
                [(sym,
                  # The newest is now, so it is inside the TTL and the read is warm; the
                  # rest march back 6 hours a step, the real snapshot cadence.
                  (now - timedelta(hours=6 * i)).isoformat(), *values, "bench")
                 for i in range(snapshots_per_symbol)])
        conn.commit()
    vmd.set_sources([_Canned([], _BOARD_ROW)])


def run() -> None:
    metrics, ratios = [], []

    # ── warm reads ────────────────────────────────────────────────────────────
    _seed_ohlcv("BENCH2Y", 730)
    _seed_ohlcv("BENCH10Y", 3650)

    warm_2y = measure("ohlcv.warm_read_2y",
                      lambda: vmd.get_ohlcv("BENCH2Y", lookback_days=730),
                      items=len(_business_days(730)),
                      note="store hit, no source touched — the hot path")
    warm_10y = measure("ohlcv.warm_read_10y",
                       lambda: vmd.get_ohlcv("BENCH10Y", lookback_days=3650),
                       items=len(_business_days(3650)))
    metrics += [warm_2y, warm_10y]
    ratios.append(ratio("ohlcv.scale_2y_to_10y", warm_10y, warm_2y, limit=8.0,
                        note="5x the rows; the read is a range scan plus a dict per row, "
                             "so anything past ~6x means per-row work crept in"))

    # ── cold path ─────────────────────────────────────────────────────────────
    # Fetch + write-through, with the source cost fixed at ~0. What is being measured is
    # the upsert, which is what a cold cache actually pays for.
    cold_rows = _candles(790)

    def _drop_cold():
        with vmd.connect() as conn:
            conn.execute("DELETE FROM md_ohlcv WHERE symbol='BENCHCOLD'")
            conn.execute("DELETE FROM md_fetch_meta WHERE symbol='BENCHCOLD'")
            conn.commit()
        vmd.set_sources([_Canned(cold_rows, _BOARD_ROW)])

    metrics.append(measure("ohlcv.cold_fetch_2y",
                           lambda: vmd.get_ohlcv("BENCHCOLD", lookback_days=730),
                           setup=_drop_cold, items=len(cold_rows),
                           note="cold cache: fetch (canned) + upsert + read back"))

    # ── board reads ───────────────────────────────────────────────────────────
    shallow = [f"S{i:04d}" for i in range(400)]
    _seed_board(shallow, 1)
    board_50 = measure("board.warm_read_50",
                       lambda: vmd.get_board(shallow[:50]),
                       items=50)
    board_400 = measure("board.warm_read_400",
                        lambda: vmd.get_board(shallow),
                        items=400,
                        note="one store query per symbol, by design")
    metrics += [board_50, board_400]
    ratios.append(ratio("board.scale_50_to_400", board_400, board_50, limit=12.0,
                        note="8x the symbols and one query each, so ~8x is the honest "
                             "cost; past 12x something is super-linear per symbol"))

    deep = [f"D{i:04d}" for i in range(400)]
    _seed_board(deep, 120)
    board_deep = measure("board.warm_read_400_deep_history",
                         lambda: vmd.get_board(deep),
                         items=400,
                         note="same read, 120 snapshots per symbol behind it")
    metrics.append(board_deep)
    ratios.append(ratio("board.history_depth_1_to_120", board_deep, board_400, limit=6.0,
                        note="reading the latest snapshot should not cost much more "
                             "because older ones exist; if this climbs, md_board wants "
                             "an index on (symbol, ts) or a retention sweep"))

    emit("vn-market-data", metrics, ratios)


def main() -> None:
    """Point the package at a throwaway database and run.

    `synchronous=OFF` because the DB is deleted seconds later: leaving it on would make
    every write benchmark a measurement of this disk's fsync latency, which is real but
    is not what changes when someone edits `store.py`.
    """
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "bench.db"

        def _connect():
            conn = sqlite3.connect(path, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA synchronous=OFF")
            return conn

        vmd.set_connection_factory(_connect)
        with _connect() as conn:
            vmd.init_schema(conn)
        try:
            run()
        finally:
            vmd.set_sources(None)


if __name__ == "__main__":
    main()
