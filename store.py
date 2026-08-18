"""SQLite cache of raw market data — the ``md_*`` tables (see ``schema.sql``).

Plain functions over a caller-supplied connection: this module never opens one, so it
works the same whether the database is the package's own or the host's. The adapter is
the only caller and opens a short-lived connection per request (SQLite connect is
sub-millisecond). All figures are stored already-normalized (full VND; index unscaled)
— scaling is the source's job, so the store never has to know who answered.
"""
import json
from datetime import date, datetime, timezone


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _age_seconds(ts: str) -> float | None:
    try:
        return (datetime.now(timezone.utc) - datetime.fromisoformat(ts)).total_seconds()
    except (TypeError, ValueError):
        return None


# ── fetch-freshness markers (md_fetch_meta) ─────────────────────────────────
def meta_fresh(conn, symbol: str, kind: str, ttl_seconds: float) -> bool:
    """True if (symbol, kind) was fetched within the TTL — serve store, skip the source."""
    row = conn.execute(
        "SELECT fetched_at FROM md_fetch_meta WHERE symbol=? AND kind=?",
        (symbol, kind)).fetchone()
    if not row:
        return False
    age = _age_seconds(row["fetched_at"])
    return age is not None and age < ttl_seconds


def meta_floor(conn, symbol: str, kind: str) -> str | None:
    """The oldest date ever *asked* for (ohlcv), or None if never recorded.

    Distinct from the oldest date banked: a symbol listed eight months ago answers a
    two-year request with eight months of candles and that is the complete answer.
    Comparing the next request against the data floor would call that a miss forever.
    """
    row = conn.execute(
        "SELECT floor FROM md_fetch_meta WHERE symbol=? AND kind=?",
        (symbol, kind)).fetchone()
    return row["floor"] if row else None


def set_meta(conn, symbol: str, kind: str, source: str, floor: str | None = None) -> None:
    conn.execute(
        """INSERT INTO md_fetch_meta(symbol, kind, fetched_at, source, floor)
           VALUES (?,?,?,?,?)
           ON CONFLICT(symbol, kind) DO UPDATE SET
             fetched_at=excluded.fetched_at, source=excluded.source,
             floor=COALESCE(excluded.floor, md_fetch_meta.floor)""",
        (symbol, kind, _now(), source, floor))
    conn.commit()


# ── OHLCV (md_ohlcv) ────────────────────────────────────────────────────────
def ohlcv_bounds(conn, symbol: str) -> tuple[str, str] | None:
    """(min_date, max_date) for a symbol, or None when nothing is cached."""
    row = conn.execute(
        "SELECT MIN(date) lo, MAX(date) hi FROM md_ohlcv WHERE symbol=?",
        (symbol,)).fetchone()
    if not row or row["lo"] is None:
        return None
    return (row["lo"], row["hi"])


def get_ohlcv_range(conn, symbol: str, start: str, end: str) -> list[dict]:
    rows = conn.execute(
        """SELECT date, open, high, low, close, volume FROM md_ohlcv
           WHERE symbol=? AND date>=? AND date<=? ORDER BY date""",
        (symbol, start, end)).fetchall()
    return [{"date": r["date"], "open": r["open"], "high": r["high"], "low": r["low"],
             "close": r["close"], "volume": r["volume"] or 0.0} for r in rows]


def upsert_ohlcv(conn, symbol: str, rows: list[dict], source: str) -> None:
    conn.executemany(
        """INSERT INTO md_ohlcv(symbol, date, open, high, low, close, volume, source)
           VALUES (?,?,?,?,?,?,?,?)
           ON CONFLICT(symbol, date) DO UPDATE SET
             open=excluded.open, high=excluded.high, low=excluded.low,
             close=excluded.close, volume=excluded.volume, source=excluded.source""",
        [(symbol, r["date"], r.get("open"), r.get("high"), r.get("low"),
          r.get("close"), r.get("volume"), source) for r in rows])
    conn.commit()


# ── Corporate-action seams (md_ohlcv sanity) ────────────────────────────────
# Every VN exchange caps how far a price may move in one session — HOSE ±7%, HNX ±10%,
# UPCOM ±15%, with wider first-day / post-suspension bands above those. A move beyond
# the cap between two adjacent sessions was therefore never traded. It is the seam left
# when a source back-adjusts a symbol's whole history for a corporate action while the
# cache, which only ever tops up the tail, keeps the pre-adjustment bars in front of the
# post-adjustment ones. Nothing downstream can tell that apart from a crash: it prints a
# breakdown to pattern geometry, sinks the symbol's relative strength, and grades as a
# real prediction. See `find_price_seam` for the detector and the adapter for the repair.
_STD_BANDS = (0.07, 0.10, 0.15, 0.20, 0.30, 0.40)
# Prices are stored to the tick, so a ratio can land a hair outside its band on rounding
# alone. Small enough that the tightest real seam (a 5% cash dividend on HOSE) still
# clears it by a wide margin.
_SEAM_TOL = 0.005
# Fri→Tue across a Monday holiday. Past that the two rows are not adjacent sessions, and
# a week's move is not bounded by one session's band — so a gap is skipped, never flagged.
_SEAM_MAX_GAP_DAYS = 4


def price_band(conn, symbol: str) -> float | None:
    """The symbol's own daily price-limit band, as a fraction, or None if never boarded.

    Read off ceiling/floor against the reference price rather than mapped from an
    exchange, so the package needs no listing table and stays right for a symbol that
    changes exchange. Both edges are rounded to the tick *inside* the band, so the
    implied figure always undershoots — it is snapped up to the nearest published band
    and never used raw. An implied band wider than any of them is not a band at all
    (a malformed snapshot); None sends the caller to its own default.
    """
    row = conn.execute(
        """SELECT ceiling, floor, ref_price FROM md_board
           WHERE symbol=? AND ref_price>0 ORDER BY ts DESC LIMIT 1""",
        (symbol,)).fetchone()
    if not row:
        return None
    edges = [abs(v / row["ref_price"] - 1.0) for v in (row["ceiling"], row["floor"]) if v]
    if not edges:
        return None
    implied = max(edges)
    return next((b for b in _STD_BANDS if implied <= b + 1e-9), None)


def find_price_seams(conn, symbol: str, start: str, end: str, band: float) -> list[str]:
    """Every date in ``start..end`` whose close sits further from the previous session's
    than *band* allows, oldest first. Each is the later of the two dates — the first bar
    on the new scale, i.e. where a repair has to reach back past to be complete.

    All of them, not just the first: a symbol can carry a settled old move that no refetch
    will change (see ``md_ohlcv_seams``), and returning only the earliest would let that
    one mask every seam after it for good.
    """
    rows = conn.execute(
        """SELECT date, close FROM md_ohlcv
           WHERE symbol=? AND date>=? AND date<=? AND close>0 ORDER BY date""",
        (symbol, start, end)).fetchall()
    out, prev = [], None
    for row in rows:
        if prev is not None:
            gap = (date.fromisoformat(row["date"]) - date.fromisoformat(prev["date"])).days
            if (gap <= _SEAM_MAX_GAP_DAYS
                    and abs(row["close"] / prev["close"] - 1.0) > band + _SEAM_TOL):
                out.append(row["date"])
        prev = row
    return out


def seams_repaired(conn, symbol: str) -> set[str]:
    """Seam dates already refetched once for this symbol — never tried again."""
    return {r["seam_date"] for r in conn.execute(
        "SELECT seam_date FROM md_ohlcv_seams WHERE symbol=?", (symbol,))}


def mark_seams_repaired(conn, symbol: str, dates: list[str], band: float) -> None:
    """Record a repair attempt, whether or not it changed anything. A seam that survives
    the refetch was really traded, and this is what stops it being chased forever."""
    now = _now()
    conn.executemany(
        """INSERT INTO md_ohlcv_seams(symbol, seam_date, band, repaired_at)
           VALUES (?,?,?,?)
           ON CONFLICT(symbol, seam_date) DO UPDATE SET repaired_at=excluded.repaired_at""",
        [(symbol, d, band, now) for d in dates])
    conn.commit()


# ── Board (md_board) ─────────────────────────────────────────────────────────
_BOARD_FIELDS = ("foreign_buy_value", "foreign_sell_value", "foreign_net_value",
                 "ceiling", "floor", "ref_price", "close",
                 "traded_value", "traded_volume")


def latest_board(conn, symbol: str, ttl_seconds: float) -> dict | None:
    """Most recent board snapshot for a symbol if within the TTL, else None."""
    row = conn.execute(
        "SELECT * FROM md_board WHERE symbol=? ORDER BY ts DESC LIMIT 1",
        (symbol,)).fetchone()
    if not row:
        return None
    age = _age_seconds(row["ts"])
    if age is None or age >= ttl_seconds:
        return None
    return {f: row[f] for f in _BOARD_FIELDS}


def insert_board(conn, board: dict[str, dict], source: str) -> None:
    now = _now()
    for sym, b in board.items():
        conn.execute(
            """INSERT INTO md_board(symbol, ts, foreign_buy_value, foreign_sell_value,
                   foreign_net_value, ceiling, floor, ref_price, close,
                   traded_value, traded_volume, source)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(symbol, ts) DO NOTHING""",
            (sym, now, b.get("foreign_buy_value"), b.get("foreign_sell_value"),
             b.get("foreign_net_value"), b.get("ceiling"), b.get("floor"),
             b.get("ref_price"), b.get("close"),
             b.get("traded_value"), b.get("traded_volume"), source))
    conn.commit()


# ── Statements (md_statements) ────────────────────────────────────────────────
def get_statements(conn, symbol: str, period: str, ttl_seconds: float) -> tuple[bool, dict | None]:
    """(found, payload): found=True when a fresh row exists (payload may be None for a
    negative cache); found=False means cache miss — the adapter should fetch."""
    row = conn.execute(
        "SELECT payload, fetched_at FROM md_statements WHERE symbol=? AND period=?",
        (symbol, period)).fetchone()
    if not row:
        return (False, None)
    age = _age_seconds(row["fetched_at"])
    if age is None or age >= ttl_seconds:
        return (False, None)
    payload = json.loads(row["payload"]) if row["payload"] else None
    return (True, payload)


def upsert_statements(conn, symbol: str, period: str, payload: dict | None, source: str) -> None:
    conn.execute(
        """INSERT INTO md_statements(symbol, period, payload, source, fetched_at)
           VALUES (?,?,?,?,?)
           ON CONFLICT(symbol, period) DO UPDATE SET
             payload=excluded.payload, source=excluded.source, fetched_at=excluded.fetched_at""",
        (symbol, period, json.dumps(payload, ensure_ascii=False) if payload is not None else None,
         source, _now()))
    conn.commit()


# ── Events (md_events) ────────────────────────────────────────────────────────
def get_events(conn, symbol: str) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM md_events WHERE symbol=? ORDER BY ex_date", (symbol,)).fetchall()
    return [{"symbol": symbol, "type": r["type"], "ex_date": r["ex_date"],
             "record_date": r["record_date"], "pay_date": r["pay_date"],
             "value_per_share": r["value_per_share"], "ratio": r["ratio"],
             "title": r["title"], "event_code": r["event_code"]} for r in rows]


def replace_events(conn, symbol: str, events: list[dict], source: str) -> None:
    """Replace-all the cached events for a symbol (dividend calendars are revised, not appended)."""
    now = _now()
    conn.execute("DELETE FROM md_events WHERE symbol=?", (symbol,))
    for e in events:
        conn.execute(
            """INSERT INTO md_events(symbol, event_code, type, ex_date, record_date,
                   pay_date, value_per_share, ratio, title, source, fetched_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (symbol, e.get("event_code"), e.get("type"), e.get("ex_date"),
             e.get("record_date"), e.get("pay_date"), e.get("value_per_share"),
             e.get("ratio"), e.get("title"), source, now))
    conn.commit()


# ── Index constituents (md_index_members) ─────────────────────────────────────
def get_index_members(conn, group: str) -> list[str]:
    """Cached members of an index group, in the source's own order."""
    rows = conn.execute(
        "SELECT symbol FROM md_index_members WHERE grp=? ORDER BY ordinal", (group,)).fetchall()
    return [r["symbol"] for r in rows]


def replace_index_members(conn, group: str, symbols: list[str], source: str) -> None:
    """Replace-all the membership of a group (indices are rebalanced, not appended)."""
    now = _now()
    conn.execute("DELETE FROM md_index_members WHERE grp=?", (group,))
    conn.executemany(
        """INSERT INTO md_index_members(grp, symbol, ordinal, source, fetched_at)
           VALUES (?,?,?,?,?)""",
        [(group, s, i, source, now) for i, s in enumerate(symbols)])
    conn.commit()
