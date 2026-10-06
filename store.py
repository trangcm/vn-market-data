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
from datetime import date, datetime, timedelta, timezone

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


def meta_fetched_at(conn, symbol: str, kind: str) -> tuple[datetime | None, str | None]:
    """(when (symbol, kind) was last fetched, tz-aware UTC; the date that fetch withheld),
    either None if never/unparseable/nothing withheld."""
    row = conn.execute(
        "SELECT fetched_at, withheld FROM md_fetch_meta WHERE symbol=? AND kind=?",
        (symbol, kind)).fetchone()
    if not row:
        return None, None
    try:
        return datetime.fromisoformat(row["fetched_at"]), row["withheld"]
    except (TypeError, ValueError):
        return None, row["withheld"]


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


def set_meta(conn, symbol: str, kind: str, source: str, floor: str | None = None,
             withheld: str | None = None) -> None:
    """`withheld` describes *this* fetch only, so it is overwritten, never kept."""
    conn.execute(
        """INSERT INTO md_fetch_meta(symbol, kind, fetched_at, source, floor, withheld)
           VALUES (?,?,?,?,?,?)
           ON CONFLICT(symbol, kind) DO UPDATE SET
             fetched_at=excluded.fetched_at, source=excluded.source,
             floor=COALESCE(excluded.floor, md_fetch_meta.floor),
             withheld=excluded.withheld""",
        (symbol, kind, _now(), source, floor, withheld))
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


def ohlcv_date_before(conn, symbol: str, day: str) -> str | None:
    """The newest banked date strictly before ``day``, or None."""
    row = conn.execute("SELECT MAX(date) d FROM md_ohlcv WHERE symbol=? AND date<?",
                       (symbol, day)).fetchone()
    return row["d"] if row else None


def ohlcv_date_after(conn, symbol: str, day: str) -> str | None:
    """The oldest banked date strictly after ``day``, or None."""
    row = conn.execute("SELECT MIN(date) d FROM md_ohlcv WHERE symbol=? AND date>?",
                       (symbol, day)).fetchone()
    return row["d"] if row else None


def get_ohlcv_range(conn, symbol: str, start: str, end: str) -> list[dict]:
    rows = conn.execute(
        """SELECT date, open, high, low, close, volume FROM md_ohlcv
           WHERE symbol=? AND date>=? AND date<=? ORDER BY date""",
        (symbol, start, end)).fetchall()
    return [{"date": r["date"], "open": r["open"], "high": r["high"], "low": r["low"],
             "close": r["close"], "volume": r["volume"] or 0.0} for r in rows]


# ── Truncated bars (md_ohlcv sanity) ────────────────────────────────────────
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
# The volume test below is the same wherever it runs. What differs is what failing it
# *means*, and that turns entirely on whether the bar is still today's:
#
#   - **Today** — the feed may still revise it, so there is nothing to gain by banking
#     it. The bar is withheld and the next pass carries the settled one.
#   - **A settled date** — the feed has said its last word, and the bar is already in the
#     window every consumer reads. Withholding would leave a hole where a session was, so
#     it is banked and then **adjudicated against a second source** (the adapter's
#     `_resolve_stubs`). It has to be a second source: low volume alone proves nothing —
#     over 4460 settled sessions in the live store, 0.8% of real bars sit below this
#     fraction, thin symbols have thin days, and every one of those bars is true. Only a
#     second reading separates a fragment from a quiet day.
#
# That second half was first left out on the reasoning that withholding today's bar costs
# "latency, never loss: the date stops being today, the tail top-up covers it, and it
# lands settled on the next pass". 2026-08-28 falsified that. Three days later the primary
# source was still serving the same truncated Friday bar (SHB at 8.2M against the 77.7M it
# really traded), so the withheld date merely stopped being today and banked anyway on the
# next pass — the guard bought one day, against a feed that never revised. Which is why a
# ruling has to be *recorded and enforced*: `md_ohlcv_stubs` keeps each adjudicated
# (symbol, date), and a source known to have served a fragment there is refused that date
# for good. Without the refusal the very next 4-hourly top-up overwrites the repair with
# the same fragment it was repaired from.
#
# What this cannot see, in both directions:
#   - A truncation that keeps the volume — a wrong close on a full-volume bar passes.
#   - A truncated bar on a symbol thin enough that its fragment still clears the
#     fraction: it is never offered to the second source at all. Of 225 symbols served a
#     truncated bar on 2026-08-28 the test flags 199, and of the 25 it lets through at
#     least 19 were still truncated — all illiquid (BHN's fragment was 1,200 shares
#     against a 1,100 median, so 109% of a normal day, and still only a third of the
#     3,700 it really traded; one, NAV, had genuinely traded its 100 shares).
#   - A symbol with fewer than `_STUB_MIN_BASELINE` banked bars has nothing to be
#     measured against, so a new listing is always banked as given.
#   - Anything older than `_STUB_RECENT_DAYS`, which is never adjudicated. A cold
#     backfill spans years; 0.8% of those bars trip the test, and paying a metered
#     second-source call for each would cost more than the backfill itself. Old history
#     is the seam detector's beat, not this one's.
_STUB_VOLUME_FRAC = 0.10
_STUB_BASELINE_BARS = 20
_STUB_MIN_BASELINE = 10
# How far back a settled bar is still worth a second reading. Long enough to reach across
# a Tết-length close (the market shuts for up to nine days, and the last settled bar has
# to stay reachable the whole time), short enough that a backfill is not a fetch storm.
_STUB_RECENT_DAYS = 14


def _looks_truncated(row: dict, banked: list[tuple[str, float]],
                     ordered: list[tuple[str, float]]) -> bool:
    """Whether `row`'s volume is far below what this symbol normally trades.

    Measured against the median of the bars preceding it — not the mean, which one
    spike drags far enough to hide a fragment underneath it.

    Both baselines arrive as `(date, volume)` newest-first, gathered once per write
    rather than once per candidate bar: `banked` is what the store already holds,
    `ordered` the incoming batch it falls back to. That is not a micro-optimisation. A
    cold backfill has nothing banked, so *every* candidate reaches the fallback, and
    re-scanning a multi-year fetch for each of them cost more than the entire rest of
    the write path — while a warm top-up, which has few incoming rows, showed nothing
    at all. That asymmetry is what found it: +46% on `ohlcv.cold_fetch_2y` beside an
    unmoved `ohlcv.warm_read_2y`.
    """
    vol, day = row.get("volume"), row.get("date")
    if vol is None or not day:
        return False
    # Prefer what is already banked; fall back to the incoming rows so a cold
    # backfill, which banks nothing until this call returns, is still measurable.
    base = [v for d, v in banked if d < day][:_STUB_BASELINE_BARS]
    if len(base) < _STUB_MIN_BASELINE:
        base = [v for d, v in ordered if d < day][:_STUB_BASELINE_BARS]
    if len(base) < _STUB_MIN_BASELINE:
        return False
    median = statistics.median(base)
    return median > 0 and vol < _STUB_VOLUME_FRAC * median


def stub_checked(conn, symbol: str) -> set[str]:
    """Dates already adjudicated for this symbol — asked about once, ever."""
    return {r["date"] for r in conn.execute(
        "SELECT date FROM md_ohlcv_stubs WHERE symbol=?", (symbol,))}


def fragment_dates(conn, symbol: str) -> dict[str, str]:
    """Dates ruled a fragment, mapped to the source that served it. That source is
    refused those dates from then on; every other source is unaffected."""
    return {r["date"]: r["source"] for r in conn.execute(
        "SELECT date, source FROM md_ohlcv_stubs WHERE symbol=? AND verdict='fragment'",
        (symbol,))}


def record_stub_check(conn, symbol: str, day: str, *, source: str,
                      suspect_volume: float | None, checked_against: str,
                      resolved_volume: float | None, fragment: bool) -> None:
    """Bank one adjudication. Both verdicts are recorded, not just the fragments: a bar
    confirmed true is what stops the same thin symbol being re-checked every TTL."""
    conn.execute(
        """INSERT INTO md_ohlcv_stubs(symbol, date, source, suspect_volume,
                                      checked_against, resolved_volume, verdict, checked_at)
           VALUES (?,?,?,?,?,?,?,?)
           ON CONFLICT(symbol, date) DO UPDATE SET
             source=excluded.source, suspect_volume=excluded.suspect_volume,
             checked_against=excluded.checked_against,
             resolved_volume=excluded.resolved_volume, verdict=excluded.verdict,
             checked_at=excluded.checked_at""",
        (symbol, day, source, suspect_volume, checked_against, resolved_volume,
         "fragment" if fragment else "true", _now()))
    conn.commit()


def clear_stub_checks(conn, symbol: str) -> None:
    """Drop every ruling for a symbol. Called when a corporate action sends the adapter
    back for the whole series: those rulings were made about bars on the old scale, and
    the refusal they carry would strand one date on it while everything around it moved."""
    conn.execute("DELETE FROM md_ohlcv_stubs WHERE symbol=?", (symbol,))
    conn.commit()


def upsert_ohlcv(conn, symbol: str, rows: list[dict], source: str) -> list[str]:
    """Bank `rows`, returning the settled dates that look truncated and have never been
    adjudicated — the adapter takes those to a second source."""
    # Today's ICT calendar date, not `session_date()`: the source was still serving
    # the truncated bar an hour after the close, so "the session ended" is not the
    # point at which the day's bar can be trusted. A date in the past is settled.
    now = datetime.now(ICT).date()
    today = now.isoformat()
    horizon = (now - timedelta(days=_STUB_RECENT_DAYS)).isoformat()
    refused = {d for d, s in fragment_dates(conn, symbol).items() if s == source}
    checked = stub_checked(conn, symbol)

    # Both baselines are gathered once, outside the loop, and both are cut to the same
    # depth — see `_looks_truncated`. That depth is the reason the cut is safe: a
    # candidate bar is at most `_STUB_RECENT_DAYS` calendar days old, so at most that
    # many sessions can sit above it, and 20 + that always spans the 20 bars preceding
    # *any* of them. Same window the per-row lookups asked for by date, read once.
    depth = _STUB_BASELINE_BARS + _STUB_RECENT_DAYS
    banked = [(r["date"], r["volume"]) for r in conn.execute(
        """SELECT date, volume FROM md_ohlcv WHERE symbol=? AND volume IS NOT NULL
           ORDER BY date DESC LIMIT ?""", (symbol, depth))]
    ordered = sorted(((r.get("date", ""), r["volume"]) for r in rows
                      if r.get("volume") is not None), reverse=True)[:depth]
    keep, withheld, suspect = [], [], []
    for row in rows:
        day = row.get("date")
        if day in refused:
            continue                      # known fragment-server for this date
        # The date gate comes first because only the recent tail can be withheld or
        # adjudicated at all, so a multi-year backfill runs the volume test on the
        # handful of bars at its end and not on the five hundred behind them.
        if day and day >= horizon and _looks_truncated(row, banked, ordered):
            if day == today:
                withheld.append(day)
                continue
            if day not in checked:
                suspect.append(day)
        keep.append(row)

    if withheld:
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
    return sorted(suspect)


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


# A re-served bar whose four prices all moved by one common factor is a rescale, not a
# correction. Adjusted prices are rounded to the tick, so the four ratios agree only to
# about a tick over the price (10 VND on 14,000 is 0.07%); the floor on the factor
# keeps rounding drift on an unadjusted bar from reading as a corporate action.
_RESCALE_MIN = 0.01
_RESCALE_SPREAD = 0.003


def find_rescale(conn, symbol: str, rows: list[dict]) -> tuple[str, float] | None:
    """``(date, factor)`` when a bar the source re-serves in ``rows`` is one already
    banked, rescaled — the *latest* such date, i.e. the last banked bar still on the old
    scale — else ``None``. Call it *before* ``upsert_ohlcv`` overwrites the banked copy.

    ``find_price_seams`` can only see a rescale bigger than the band, and most are
    not: a 1,000 VND dividend on a 14,000 stock is 7%, inside UPCOM's ±15% and even
    HOSE's ±7%. A tail top-up re-reads the last banked bar, and once the ex-date is
    known the source serves that bar already adjusted — so the overwrite put DRI's
    09-21 bar on the new scale in front of 09-18 on the old one, and a Double Top drew
    itself across the join. The overlap bar is where the evidence exists, and only
    until the upsert: afterwards the old scale is gone from this bar.

    What this proves and what it cannot. Open, high, low and close moving together is
    the signature of adjustment; a bar banked mid-session and settled later keeps its
    open, so a partial day is never read as one. It sees only bars the source re-serves
    — a tail top-up's one-bar overlap — so a rescale announced *after* the bar was
    banked and re-read is caught on the next top-up at the latest, never earlier. A
    false positive costs one full refetch, not data.
    """
    by_date = {r["date"]: r for r in rows if r.get("date")}
    if not by_date:
        return None
    marks = ",".join("?" * len(by_date))
    banked = conn.execute(
        f"""SELECT date, open, high, low, close FROM md_ohlcv
            WHERE symbol=? AND date IN ({marks}) ORDER BY date""",
        (symbol, *by_date)).fetchall()
    for old in reversed(banked):
        new = by_date[old["date"]]
        try:
            ratios = [new[k] / old[k] for k in ("open", "high", "low", "close")]
        except (KeyError, TypeError, ZeroDivisionError):
            continue
        f = sum(ratios) / 4
        if abs(f - 1.0) >= _RESCALE_MIN and max(ratios) - min(ratios) <= _RESCALE_SPREAD * f:
            return old["date"], f
    return None


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


def seam_repaired_at(conn, symbol: str, seam_date: str) -> str | None:
    """When this seam date was last refetched (ISO-8601 UTC), or None if never."""
    row = conn.execute(
        "SELECT repaired_at FROM md_ohlcv_seams WHERE symbol=? AND seam_date=?",
        (symbol, seam_date)).fetchone()
    return row["repaired_at"] if row else None


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
                 "traded_value", "traded_volume", "vwap")


def latest_board(conn, symbol: str, ttl_seconds: float) -> dict | None:
    """Most recent board snapshot for a symbol if within the TTL, else None.

    Carries ``read_at`` (ISO-8601 UTC): when the snapshot was actually read from the
    source. A cached or stale-fallback answer can be minutes to hours older than the
    call that returned it, and a caller banking session-to-date counters must be able
    to say which moment of the session they describe."""
    row = conn.execute(
        "SELECT * FROM md_board WHERE symbol=? ORDER BY ts DESC LIMIT 1",
        (symbol,)).fetchone()
    if not row:
        return None
    age = _age_seconds(row["ts"])
    if age is None or age >= ttl_seconds:
        return None
    return {**{f: row[f] for f in _BOARD_FIELDS}, **_depth_out(row["depth"]),
            "read_at": row["ts"]}


def newest_board(conn, symbol: str) -> dict | None:
    """The most recent board snapshot for a symbol at any age (``read_at`` says how
    old), or None if none was ever banked. For a reader that is *told* the answer
    is old — outside the session the last read is the settled one — never for a
    caller that would pass it off as live."""
    return latest_board(conn, symbol, float("inf"))


def _depth_out(raw) -> dict:
    """``{bids, asks}`` from the stored ``depth`` JSON; both None for a row banked
    before depth was kept (or by a source that has none) — "not read", never empty."""
    try:
        d = json.loads(raw) if raw else None
    except (TypeError, ValueError):
        d = None
    d = d if isinstance(d, dict) else {}
    return {"bids": d.get("bids"), "asks": d.get("asks")}


def _depth_in(b: dict) -> str | None:
    if b.get("bids") is None and b.get("asks") is None:
        return None
    return json.dumps({"bids": b.get("bids"), "asks": b.get("asks")}, separators=(",", ":"))


def insert_board(conn, board: dict[str, dict], source: str) -> str:
    """Bank one board read; returns its timestamp (the rows' ``ts``, i.e. ``read_at``)."""
    now = _now()
    for sym, b in board.items():
        conn.execute(
            """INSERT INTO md_board(symbol, ts, foreign_buy_value, foreign_sell_value,
                   foreign_net_value, ceiling, floor, ref_price, close,
                   traded_value, traded_volume, vwap, depth, source)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(symbol, ts) DO NOTHING""",
            (sym, now, b.get("foreign_buy_value"), b.get("foreign_sell_value"),
             b.get("foreign_net_value"), b.get("ceiling"), b.get("floor"),
             b.get("ref_price"), b.get("close"),
             b.get("traded_value"), b.get("traded_volume"),
             b.get("vwap"), _depth_in(b), source))
    conn.commit()
    return now


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
    construction (the source re-serves the whole dated series each fetch); callers
    wanting it merge the live payload's own.
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
    """Cached events, oldest ex-date first — announced-but-undated ones lead.

    `status` is derived on read rather than stored: it is a restatement of whether
    `ex_date` is set, and a column would be free to disagree with the column it
    describes. Deriving it here also gives it to rows banked before the field
    existed, and to any source that does not emit it.
    """
    rows = conn.execute(
        "SELECT * FROM md_events WHERE symbol=? ORDER BY ex_date", (symbol,)).fetchall()
    return [{"symbol": symbol, "type": r["type"], "ex_date": r["ex_date"] or "",
             "status": "confirmed" if r["ex_date"] else "announced",
             "record_date": r["record_date"], "pay_date": r["pay_date"],
             "announced_date": r["announced_date"] or "",
             "value_per_share": r["value_per_share"], "ratio": r["ratio"],
             "title": r["title"], "event_code": r["event_code"]} for r in rows]


def replace_events(conn, symbol: str, events: list[dict], source: str) -> None:
    """Replace-all the cached events for a symbol (dividend calendars are revised, not appended)."""
    now = _now()
    conn.execute("DELETE FROM md_events WHERE symbol=?", (symbol,))
    for e in events:
        conn.execute(
            """INSERT INTO md_events(symbol, event_code, type, ex_date, record_date,
                   pay_date, announced_date, value_per_share, ratio, title,
                   source, fetched_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (symbol, e.get("event_code"), e.get("type"), e.get("ex_date"),
             e.get("record_date"), e.get("pay_date"), e.get("announced_date"),
             e.get("value_per_share"), e.get("ratio"), e.get("title"), source, now))
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
