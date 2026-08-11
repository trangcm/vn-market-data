"""Everything the library does, in one runnable file.

    pip install vn-market-data      # or vn-market-data[vci] for the board
    python examples/quickstart.py

No configuration: the first call creates `./vn_market_data.db` (override with
`$VN_MARKET_DATA_DB`) and backfills it, and every call after that reads from disk.
Needs network on the first run only.
"""
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Runs from a checkout without installing; from an installed package the guard is a
# no-op, since there is then no enclosing `vn_market_data/` directory to point at.
_root = Path(__file__).resolve().parents[2]
if (_root / "vn_market_data").is_dir():
    sys.path.insert(0, str(_root))

import vn_market_data as vmd  # noqa: E402

SYMBOL = "HPG"


def daily_candles():
    """Cache-first: the cold call fetches, the warm one does not touch the network.
    Timing both is the honest way to show it — the second is typically ~100× faster."""
    t0 = time.perf_counter()
    rows = vmd.get_ohlcv(SYMBOL, lookback_days=365)
    cold = time.perf_counter() - t0

    t0 = time.perf_counter()
    vmd.get_ohlcv(SYMBOL, lookback_days=365)
    warm = time.perf_counter() - t0

    print(f"\n{SYMBOL}: {len(rows)} daily bars   first call {cold * 1000:.0f} ms, "
          f"second {warm * 1000:.0f} ms (store)")
    for r in rows[-3:]:
        print(f"  {r['date']}  O {r['open']:>10,.0f}  H {r['high']:>10,.0f}  "
              f"L {r['low']:>10,.0f}  C {r['close']:>10,.0f}  vol {r['volume']:>12,.0f}")
    print("  values are full VND, whichever source answered")


def price_board():
    """Needs the `[vci]` extra. Without it *no* source implements the capability, and
    that raises `NotSupported` rather than returning an empty board — an empty board is
    a claim about the market, and this is a claim about your install."""
    try:
        board = vmd.get_board([SYMBOL, "VNM"])
    except vmd.NotSupported:
        print("\nboard: no source implements it — `pip install vn-market-data[vci]`")
        return
    if not board:
        print("\nboard: every source down and nothing recent enough in the store — "
              "fall back to candles knowingly")
        return
    for sym, b in board.items():
        # Every md_board column is nullable and the degraded path serves whatever the
        # last snapshot held, so nothing here can be formatted as a number unguarded.
        floor, ceiling = b.get("floor"), b.get("ceiling")
        band = f"{floor:,.0f} .. {ceiling:,.0f}" if floor and ceiling else "n/a"
        print(f"\n{sym} board:")
        print(f"  close {b.get('close') or 0:>12,.0f}   band {band}")
        print(f"  foreign net {b.get('foreign_net_value') or 0:>+15,.0f} VND"
              f"   traded {b.get('traded_value') or 0:>15,.0f} VND")


def market_money():
    """The exchange's own money traded — matched *plus* put-through. Summing a price
    board lands 15–20% short, because block deals never touch the order book. Uncached
    (the last row is the session in progress) and it propagates unavailability, so a
    caller with a narrower estimate knows to use it."""
    try:
        rows = vmd.get_market_turnover("VNINDEX", lookback_days=10)
    except vmd.SourceUnavailable as e:
        print(f"\nturnover: unavailable ({e}) — no silent zero")
        return
    print("\nVNINDEX money traded, last sessions:")
    for r in rows[-3:]:
        print(f"  {r['date']}  total {r['value'] / 1e12:>7.2f} T   "
              f"matched {(r.get('matched') or 0) / 1e12:>6.2f} T   "
              f"put-through {(r.get('put_through') or 0) / 1e12:>5.2f} T")


def session_clock():
    """Prior question to any TTL: could the number have moved at all? Outside the
    session the answer is no, however old your cache is."""
    now = datetime.now(timezone.utc)
    print(f"\nsession live right now: {vmd.session_live(now)}")
    print(f"  last close: {vmd.last_session_close(now):%Y-%m-%d %H:%M %Z}")
    print(f"  fetch worth making, never fetched : {vmd.fetch_due(None, now)}")
    print(f"  fetch worth making, fetched 1m ago: "
          f"{vmd.fetch_due(now - timedelta(minutes=1), now)}")


def main():
    db = os.getenv("VN_MARKET_DATA_DB", "vn_market_data.db")
    print(f"store: {Path(db).resolve()}")
    print("chain:", " → ".join(s.name for s in vmd.get_sources()))

    daily_candles()
    price_board()
    market_money()
    session_clock()

    print("\nrun it again — the candles now come off disk with no network at all")


if __name__ == "__main__":
    main()
