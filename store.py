"""SQLite cache of raw market data — the ``md_*`` tables (see ``schema.sql``).

Plain functions over a caller-supplied connection: this module never opens one, so it
works the same whether the database is the package's own or the host's. The adapter is
the only caller and opens a short-lived connection per request (SQLite connect is
sub-millisecond). All figures are stored already-normalized (full VND; index unscaled)
— scaling is the source's job, so the store never has to know who answered.
"""
import json
import logging
import statistics
from datetime import date, datetime, timezone

from .market_hours import ICT

log = logging.getLogger(__name__)


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


# ── Unsettled same-session bars (md_ohlcv sanity) ───────────────────────────
# A daily feed is supposed to publish a session only once it has closed — that is the
# assumption `session_date` is written around. A feed that instead answers with the day
# already in progress hands back a bar truncated to the first minutes of trading: the
# open is right, and high/low/close/volume are frozen near it. `get_index_live` refuses
# to store the index's half-formed candle for exactly this reason; the per-symbol series
# needs the same refusal, because nothing downstream can tell a truncated bar from a
# real one. On 2026-08-28 the primary source served one for the whole market at once —
# a median of 0.9% of each symbol's own recent volume, and closes off by up to 2.5% —
# which read as 100 simultaneous volume-dry-ups in the scanner and inverted the sign on
# the one symbol whose volume was genuinely abnormal (SHB traded 1.6x its baseline).
#
# The test is deliberately narrow, because low volume alone proves nothing: over 4460
# settled sessions in the live store, 0.8% of real bars sit below this fraction — thin
# symbols have thin days, and those bars are true. What is *not* safe is a bar for a
# session dated **today**, which the feed may still revise, that also looks truncated.
# So only the current day is ever withheld.
#
# What this cannot see, in both directions:
#   - A truncation that keeps the volume — a wrong close on a full-volume bar passes.
#   - A truncated bar on a symbol thin enough that the fragment still clears the
#     fraction. Measured against the second source on the day itself: of 225 symbols
#     served a truncated bar this withholds 199, and of the 25 it lets through at
#     least 19 were still truncated — all illiquid (BHN's fragment was 1,200 shares
#     against a 1,100 median, so 109% of a normal day, and still only a third of the
#     3,700 it really traded; one, NAV, had genuinely traded its 100 shares). A
#     per-symbol volume test cannot separate those from a real quiet day. A second
#     source can, which is why the cross-source check is the other half of this and
#     not a luxury — but it is metered, so it belongs on a scan, not on every write.
#     That is not a hypothetical: on the first live pass after this shipped, the feed
#     had consolidated for most of the market and this withheld the rest, yet TCB, STB
#     and SSB still banked fragments (6%, 5% and 8% of what they really traded) whose
#     volume cleared the fraction anyway. Twelve of the fifteen it admitted matched the
#     second source to the digit; three did not, and only the second source said which.
#   - A symbol with fewer than `_STUB_MIN_BASELINE` banked bars has nothing to be
#     measured against, so a new listing is always banked as given.
#   - It withholds a genuinely dead session's bar for one day. That is latency, never
#     loss: the date stops being "today", the tail top-up covers it, and it lands
#     settled on the next pass. Withholding is the cheap error here — the bar was
#     revisable anyway.
_STUB_VOLUME_FRAC = 0.10
_STUB_BASELINE_BARS = 20
_STUB_MIN_BASELINE = 10


def _unsettled_stub(conn, symbol: str, row: dict, rows: list[dict], today: str) -> bool:
    """Whether `row` is a truncated bar for a session that has not settled yet.

    Measured against the median of the symbol's own preceding bars — not the mean,
    which one spike drags far enough to hide a stub underneath it.
    """
    if row.get("date") != today:
        return False
    vol = row.get("volume")
    if vol is None:
        return False
    # Prefer what is already banked; fall back to the incoming rows so a cold
    # backfill, which banks nothing until this call returns, is still measurable.
    base = [r["volume"] for r in conn.execute(
        """SELECT volume FROM md_ohlcv WHERE symbol=? AND date<? AND volume IS NOT NULL
           ORDER BY date DESC LIMIT ?""", (symbol, today, _STUB_BASELINE_BARS))]
    if len(base) < _STUB_MIN_BASELINE:
        base = [r["volume"] for r in sorted(rows, key=lambda r: r["date"], reverse=True)
                if r.get("date", "") < today and r.get("volume") is not None
                ][:_STUB_BASELINE_BARS]
    if len(base) < _STUB_MIN_BASELINE:
        return False
    median = statistics.median(base)
    return median > 0 and vol < _STUB_VOLUME_FRAC * median


def upsert_ohlcv(conn, symbol: str, rows: list[dict], source: str) -> None:
    # Today's ICT calendar date, not `session_date()`: the source was still serving
    # the truncated bar an hour after the close, so "the session ended" is not the
    # point at which the day's bar can be trusted. A date in the past is settled.
    today = datetime.now(ICT).date().isoformat()
    keep = [r for r in rows if not _unsettled_stub(conn, symbol, r, rows, today)]
    if len(keep) != len(rows):
        log.warning("%s: withholding an unsettled %s bar from %s (volume far below the "
                    "symbol's own baseline) — it will be banked once it settles",
                    symbol, today, source)
    conn.executemany(
        """INSERT INTO md_ohlcv(symbol, date, open, high, low, close, volume, source)
           VALUES (?,?,?,?,?,?,?,?)
           ON CONFLICT(symbol, date) DO UPDATE SET
             open=excluded.open, high=excluded.high, low=excluded.low,
             close=excluded.close, volume=excluded.volume, source=excluded.source""",
        [(symbol, r["date"], r.get("open"), r.get("high"), r.get("low"),
          r.get("close"), r.get("volume"), source) for r in keep])
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


def rescaled_symbols(conn) -> dict[str, dict]:
    """Symbols whose banked history the repair actually *rewrote*, and when.

    ``md_ohlcv_seams`` records every repair **attempt**, which is not the same
    question: most attempts change nothing. Of 40 seams found in the first full
    sweep only 6 were corporate actions; the other 34 were moves genuinely traded
    under a wider band than the symbol carries today (a first session, a return from
    suspension, a symbol that has since changed exchange), and those survive the
    refetch by design — the ledger exists to stop them being chased forever.

    A repair that *worked* is visible in the data rather than in the ledger: the seam
    it was recorded against is no longer in the series. That is the test used here,
    which is why this needs no column and no backfill — it re-reads the answer rather
    than trusting a flag written at the time.

    Returns ``{symbol: {"rescaled_at": <latest repaired_at that resolved a seam>,
    "seam_date": <earliest resolved seam>, "seams": [...]}}``. Callers want both
    dates: ``rescaled_at`` is when the old price scale stopped existing, and
    ``seam_date`` is the first bar that was on the new one.

    Why the store owns this: anything derived from a series that was rescaled under it
    — a persisted price level, a graded outcome — is denominated in a scale the store
    no longer holds, and only the store knows that happened. It reports the fact; what
    to do about it belongs to whoever kept the derived rows.
    """
    ledger: dict[str, list] = {}
    for r in conn.execute(
            "SELECT symbol, seam_date, band, repaired_at FROM md_ohlcv_seams"):
        ledger.setdefault(r["symbol"], []).append(dict(r))

    out: dict[str, dict] = {}
    for symbol, entries in ledger.items():
        bounds = conn.execute(
            "SELECT MIN(date) lo, MAX(date) hi FROM md_ohlcv WHERE symbol=? AND close>0",
            (symbol,)).fetchone()
        if not bounds or not bounds["lo"]:
            continue                       # nothing banked any more; nothing to say
        # One scan per symbol, against the band each seam was judged under.
        still_there: set[str] = set()
        for band in {e["band"] for e in entries if e["band"]}:
            still_there |= set(find_price_seams(
                conn, symbol, bounds["lo"], bounds["hi"], band))
        resolved = [e for e in entries if e["seam_date"] not in still_there]
        if not resolved:
            continue
        out[symbol] = {
            "rescaled_at": max(e["repaired_at"] for e in resolved),
            "seam_date": min(e["seam_date"] for e in resolved),
            "seams": sorted(e["seam_date"] for e in resolved),
        }
    return out


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


# ── Statement archive (md_statement_periods) ──────────────────────────────────
_SECTIONS = ("income", "balance", "cashflow")


def _slice_period(payload: dict, label: str) -> dict:
    """The one labelled period's own line items, flattened out of the period-keyed
    payload: ``{income: {field: value}, balance: {...}, cashflow: {...}}``."""
    out = {}
    for sec in _SECTIONS:
        body = payload.get("statements", {}).get(sec) or {}
        vals = {f: series[label] for f, series in body.items()
                if isinstance(series, dict) and label in series}
        if vals:
            out[sec] = vals
    return out


def bank_statement_periods(conn, symbol: str, period: str,
                           payload: dict | None, source: str) -> int:
    """Archive each labelled period in ``payload`` as its own row. Returns the count
    written. A negative cache (payload None) banks nothing — absence is not a period.

    A re-fetch of the same label overwrites, so a restated or audited figure wins over
    the provisional one it replaces; that is why the cache stays the source of truth
    for *freshness* and this table only for *reach*.
    """
    if not payload:
        return 0
    now, kind, n = _now(), payload.get("kind"), 0
    for label in payload.get("periods") or []:
        body = _slice_period(payload, label)
        if not body:
            continue
        conn.execute(
            """INSERT INTO md_statement_periods(symbol, period, label, kind,
                   payload, source, fetched_at)
               VALUES (?,?,?,?,?,?,?)
               ON CONFLICT(symbol, period, label) DO UPDATE SET
                 kind=excluded.kind, payload=excluded.payload,
                 source=excluded.source, fetched_at=excluded.fetched_at""",
            (symbol, period, label, kind,
             json.dumps(body, ensure_ascii=False), source, now))
        n += 1
    conn.commit()
    return n


def banked_labels(conn, symbol: str, period: str) -> list[str]:
    """Every period label archived for this symbol, newest first. Labels sort
    lexically in chronological order for both cadences ('2026-Q2' > '2026-Q1')."""
    return [r["label"] for r in conn.execute(
        "SELECT label FROM md_statement_periods WHERE symbol=? AND period=? "
        "ORDER BY label DESC", (symbol, period))]


def banked_fetched_at(conn, symbol: str, period: str) -> str | None:
    """When this symbol's archive was last written, or None if nothing is banked.

    Distinct from any period *label*: it dates the write, not the content. A caller
    that has changed how a payload is normalized uses it to tell an archive built by
    the older code from one built by the current code, which the labels cannot say.
    """
    row = conn.execute(
        "SELECT MAX(fetched_at) FROM md_statement_periods WHERE symbol=? AND period=?",
        (symbol, period)).fetchone()
    return row[0] if row else None


def get_statement_history(conn, symbol: str, period: str,
                          limit: int | None = None) -> dict | None:
    """The archive rebuilt into the same shape ``get_statements`` returns, newest
    period first — or None when nothing is banked. ``ratio_extra`` is absent by
    construction (it is TTM, not per-period); callers wanting it merge the live
    payload's own.
    """
    sql = ("SELECT label, kind, payload FROM md_statement_periods "
           "WHERE symbol=? AND period=? ORDER BY label DESC")
    args: tuple = (symbol, period)
    if limit is not None:
        sql += " LIMIT ?"
        args += (limit,)
    rows = conn.execute(sql, args).fetchall()
    if not rows:
        return None
    labels = [r["label"] for r in rows]
    statements: dict[str, dict] = {sec: {} for sec in _SECTIONS}
    for r in rows:
        body = json.loads(r["payload"])
        for sec in _SECTIONS:
            for field, value in (body.get(sec) or {}).items():
                statements[sec].setdefault(field, {})[r["label"]] = value
    return {"kind": rows[0]["kind"], "periods": labels,
            "statements": statements, "ratio_extra": {}}


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
