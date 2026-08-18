"""Store-first data adapter — the package's single entry point.

For each capability it serves from the local SQLite store first and only reaches a
source on a cache miss / stale tail, then writes through. Sources are tried head-first
down the chain; a source that raises ``NotSupported`` or ``SourceUnavailable`` falls
through to the next, but a genuinely-empty answer (``[]``/``None``) is **authoritative
and stops the chain** — "this symbol has no dividends" is a real answer, not a failure.
If *every* source is unavailable the ``SourceUnavailable`` propagates, so a caller can
tell "nothing to report" apart from "nobody answered" — except where the store already
holds an answer worth serving, which is the point of the layer: candles, the board and
the index constituents degrade to what is banked, and only a cold cache raises. See the
degradation table in the README.

The whole layer is synchronous: the VCI backend (vnstock) is blocking, and SQLite reads
are sub-millisecond. Run it under a threadpool if you need concurrency — a future async
source can use ``httpx.Client`` (sync) under the same one.
"""
import logging
from contextlib import closing
from datetime import date, timedelta

from vn_market_data.db import connect
from vn_market_data import store
from vn_market_data.sources.base import NotSupported, SourceUnavailable
from vn_market_data.sources.registry import build_sources

log = logging.getLogger(__name__)

# Default freshness TTLs, in seconds. Every one is also a keyword argument on the call
# it governs, so a caller who needs a different cadence overrides it per call rather
# than reaching in here. OHLCV matches a 4-hourly candle refresh; statements are
# quarterly data; events and index membership change slowly; the board is a snapshot.
OHLCV_TTL_S      = 4 * 3600
BOARD_TTL_S      = 15 * 60
EVENTS_TTL_S     = 24 * 3600
STATEMENTS_TTL_S = 7 * 24 * 3600
MEMBERS_TTL_S    = 24 * 3600   # index membership only moves at a quarterly review
# How stale a board snapshot may be when the source is *down* — one trading session, so
# an outage anywhere in the day still answers with this session's own numbers.
BOARD_STALE_S    = 6 * 3600

# The band to test a candle series against when the symbol has never been boarded and its
# own limit is unknown — the widest ordinary VN band (UPCOM). Deliberately the loosest of
# them: a missed seam is repaired the next time the symbol *is* boarded, while a false one
# refetches a whole history that was never wrong.
DEFAULT_PRICE_BAND = 0.15
# How far *before* the oldest candle held a seam repair starts. A source asked for a
# multi-year window can answer from the day after the one requested (DNSE does), which
# would leave the single oldest bar on the pre-adjustment scale and the seam merely moved
# to the front of the series. A week of lead is free — the rows are refetched anyway.
SEAM_REPAIR_LEAD_DAYS = 7

_sources = None


def set_sources(sources) -> None:
    """Replace the source chain with *sources* (an iterable of :class:`DataSource`).

    The chain is tried head-first per capability, so order is priority. Pass ``None`` to
    fall back to the built-in chain on the next call. Use this to add your own backend,
    to drop one you don't want, or — in tests — to install fakes and touch no network::

        set_sources([MyBrokerSource(), *build_sources()])
    """
    global _sources
    _sources = list(sources) if sources is not None else None


def get_sources() -> list:
    """The live source chain, building the default one if none has been set."""
    return list(_src_chain())


def _src_chain():
    global _sources
    if _sources is None:
        _sources = build_sources()
    return _sources


def _query(method: str, *args, **kwargs):
    """Try each source for one capability. Returns (source_name, result).
    Falls through on NotSupported / SourceUnavailable; raises the last
    SourceUnavailable if every source was unavailable."""
    last_unavailable = None
    for src in _src_chain():
        try:
            return src.name, getattr(src, method)(*args, **kwargs)
        except NotSupported:
            continue
        except SourceUnavailable as e:
            last_unavailable = e
            log.warning("%s unavailable for %s%s", src.name, method, args)
            continue
    if last_unavailable is not None:
        raise last_unavailable
    raise NotSupported(method)


# ── OHLCV ────────────────────────────────────────────────────────────────────
def get_ohlcv(symbol: str, lookback_days: int = 730, *,
              is_index: bool = False, ttl_s: float = OHLCV_TTL_S) -> list[dict]:
    """Daily OHLCV for one symbol, oldest-first, normalized to full VND (index unscaled).
    Served from the store; only a cold cache, a stale tail (new trading day) or a
    corporate-action seam in the banked series hits a source."""
    symbol = symbol.strip().upper()
    if not symbol:
        return []
    today = date.today()
    start = (today - timedelta(days=lookback_days)).isoformat()
    end = today.isoformat()

    with closing(connect()) as conn:
        bounds = store.ohlcv_bounds(conn, symbol)
        fetch_from = None
        floor = None
        band, repair = None, []
        if bounds is None:
            fetch_from = start                                   # cold cache → full backfill
        else:
            min_d, max_d = bounds
            # Against the deepest window ever *asked* for, not the oldest candle banked.
            # A symbol listed eight months ago answers a two-year request with eight
            # months, and that is the whole answer — comparing against the data floor
            # would read it as a miss and refetch its entire history on every call, for
            # every recent listing, forever. A cache with no floor recorded yet falls
            # back to the data floor, i.e. probes once and then records what it learned.
            floor = store.meta_floor(conn, symbol, "ohlcv") or min_d
            if start < floor:
                fetch_from = start                               # need deeper history → refetch
            elif not store.meta_fresh(conn, symbol, "ohlcv", ttl_s):
                if end > max_d and today.weekday() < 5:
                    # Stale tail on a weekday → top up. All three conditions matter:
                    # without the TTL every call re-fetches, without `end > max_d` a
                    # symbol whose last candle is already today re-fetches all day, and
                    # without the weekday test every weekend call chases a session that
                    # will never print.
                    fetch_from = max_d
                # …but a tail top-up is exactly what a corporate action defeats. When one
                # lands, the source rescales the symbol's *whole* history; appending the
                # new bars in front of the old ones leaves a fall no exchange would have
                # allowed, and it never heals, because every later call is a tail top-up
                # too. So re-read what is banked and look for that seam: finding one means
                # refetching everything held, not just the tail. Indices are exempt —
                # they have no corporate actions and no price band to test against.
                if not is_index:
                    band = store.price_band(conn, symbol) or DEFAULT_PRICE_BAND
                    seams = store.find_price_seams(conn, symbol, min_d, end, band)
                    # Each seam is repaired at most once. Some moves beyond *today's*
                    # band were genuinely traded — a symbol that has since changed
                    # exchange met a wider band at the time — and those survive the
                    # refetch, so without the ledger they would be chased every TTL for
                    # as long as the symbol is cached.
                    repair = [d for d in seams if d not in store.seams_repaired(conn, symbol)]
                    if repair:
                        oldest = (date.fromisoformat(min_d)
                                  - timedelta(days=SEAM_REPAIR_LEAD_DAYS)).isoformat()
                        fetch_from = min(start, oldest)
                        log.warning("%s: price seam at %s beyond the ±%.0f%% band — "
                                    "refetching %s..%s (corporate action?)",
                                    symbol, ", ".join(repair), band * 100, fetch_from, end)

        if fetch_from is not None:
            try:
                src, rows = _query("get_ohlcv", symbol, fetch_from, end, is_index=is_index)
            except SourceUnavailable as e:
                # Nobody answered, but the store is holding real history — serve it. The
                # alternative fails a whole pipeline pass over a tail that is at most one
                # session short. A cold cache is the one case with nothing to fall back
                # on, and there "nobody answered" is the only honest answer. Freshness is
                # deliberately *not* stamped, so the next call retries rather than
                # sitting out the TTL on the strength of a failure.
                if bounds is None:
                    raise
                log.warning("ohlcv unavailable for %s (%s) — serving %s..%s from the store",
                            symbol, e, *bounds)
            else:
                if rows:
                    store.upsert_ohlcv(conn, symbol, rows, src)
                # A tail top-up starts at max_d and must not raise the floor with it.
                store.set_meta(conn, symbol, "ohlcv", src,
                               floor=min(fetch_from, floor) if floor else fetch_from)
                if repair:
                    store.mark_seams_repaired(conn, symbol, repair, band)

        return store.get_ohlcv_range(conn, symbol, start, end)


def get_index_live(symbol: str) -> dict | None:
    """The in-progress session's index candle, or None when none is under way.

    Deliberately **uncached and never stored**: it is a live quote, and writing a
    half-formed candle into ``md_ohlcv`` would hand every consumer that reads the store
    (moving averages, pattern geometry, backtests) a partial bar that later changes
    underneath them. Callers who want the *market right now* overlay it on the daily
    series themselves. A source that can't answer degrades to None rather than raising:
    a stale-but-honest last close beats failing the whole page.
    """
    symbol = symbol.strip().upper()
    if not symbol:
        return None
    try:
        _, row = _query("get_index_live", symbol)
    except (SourceUnavailable, NotSupported):
        log.warning("no live index quote for %s", symbol)
        return None
    return row or None


def get_market_turnover(index: str = "VNINDEX", lookback_days: int = 420) -> list[dict]:
    """The exchange's own money traded per session, oldest-first, in full VND:
    ``[{date, value, matched, put_through, volume}]`` where ``value`` = matched +
    put-through — the "GTGD" figure, which no price board can produce.

    **Uncached**, for the same reason as ``get_index_live``: the last row is the session
    in progress and would be frozen half-formed in the store. Unlike the live index this
    is cheap to re-read whole — one request answers the entire history — so there is
    nothing to gain by keeping it. Unavailability propagates rather than degrading: a
    caller who has a narrower way to estimate the money needs to know to use it.
    """
    index = index.strip().upper()
    if not index:
        return []
    today = date.today()
    _, rows = _query("get_market_turnover", index,
                     (today - timedelta(days=lookback_days)).isoformat(), today.isoformat())
    return rows or []


# ── Price board ────────────────────────────────────────────────────────────────
def get_board(symbols: list[str], *, ttl_s: float = BOARD_TTL_S,
              stale_ttl_s: float = BOARD_STALE_S) -> dict[str, dict]:
    """Price-board snapshot per symbol. Fresh snapshots are served from the store;
    only symbols past ``ttl_s`` trigger a (single) source call for that batch.

    A source that can't answer **degrades to the last stored snapshot** (up to
    ``stale_ttl_s``) instead of to nothing. The only fallback a caller has is the daily
    candle, i.e. *yesterday's* close with no foreign flow and no traded value at all —
    so a board an hour old is strictly the better answer, and one transient
    ConnectionError must not blank a market page for a whole job cycle (2026-07-29: it
    did). Past ``stale_ttl_s`` the symbol is simply absent, so a caller falls back to
    candles knowingly rather than being handed a day-old board as live.

    If no installed source implements the capability at all — a base install without the
    ``[vci]`` extra — this raises ``NotSupported``. That is a fact about the install, not
    about the market, and an empty board would state the second while meaning the first.
    """
    symbols = [s.strip().upper() for s in symbols if s.strip()]
    if not symbols:
        return {}
    with closing(connect()) as conn:
        out: dict[str, dict] = {}
        need: list[str] = []
        for s in symbols:
            cached = store.latest_board(conn, s, ttl_s)
            if cached is not None:
                out[s] = cached
            else:
                need.append(s)
        if need:
            try:
                src, fresh = _query("get_board", need)
            except SourceUnavailable as e:
                stale = {s: b for s in need
                         if (b := store.latest_board(conn, s, stale_ttl_s)) is not None}
                log.warning("board unavailable (%s) — serving %d/%d "
                            "stale snapshots", e, len(stale), len(need))
                out.update(stale)
                return out
            if fresh:
                store.insert_board(conn, fresh, src)
                out.update(fresh)
        return out


# ── Statements ────────────────────────────────────────────────────────────────
def get_statements(symbol: str, period: str = "year", *,
                   ttl_s: float = STATEMENTS_TTL_S) -> dict | None:
    """Raw, source-parsed statements ``{kind, periods, statements, ratio_extra}`` (or None).

    Line items only — no ratios are computed here, so the field→alias map is the only
    per-source knowledge and the metric math stays with whoever needs it. ``kind``
    distinguishes a bank's statements from a general company's, since they share almost
    no line items. A symbol with no statements is cached *negatively*, so a listing
    that will never have them isn't re-fetched every pass.
    """
    symbol = symbol.strip().upper()
    if not symbol:
        return None
    with closing(connect()) as conn:
        found, payload = store.get_statements(conn, symbol, period, ttl_s)
        if found:
            return payload
        src, payload = _query("get_statements", symbol, period)
        store.upsert_statements(conn, symbol, period, payload, src)  # None → negative cache
        return payload


# ── Events ────────────────────────────────────────────────────────────────────
def get_events(symbol: str, *, ttl_s: float = EVENTS_TTL_S) -> list[dict]:
    """Dividend/corporate-action events for one symbol, served store-first."""
    symbol = symbol.strip().upper()
    if not symbol:
        return []
    with closing(connect()) as conn:
        if store.meta_fresh(conn, symbol, "events", ttl_s):
            return store.get_events(conn, symbol)
        src, events = _query("get_events", symbol)
        store.replace_events(conn, symbol, events, src)
        store.set_meta(conn, symbol, "events", src)
        return store.get_events(conn, symbol)


# ── Index constituents ────────────────────────────────────────────────────────
def get_index_constituents(group: str = "VN30", *,
                           ttl_s: float = MEMBERS_TTL_S) -> list[str]:
    """Members of an index group, served store-first (membership only changes at a
    quarterly review, so a stale-but-cached list is always better than an empty one).
    A source that is unavailable — or answers empty — falls back to the last cached
    membership rather than blanking the page."""
    group = group.strip().upper()
    if not group:
        return []
    with closing(connect()) as conn:
        cached = store.get_index_members(conn, group)
        if cached and store.meta_fresh(conn, group, "members", ttl_s):
            return cached
        try:
            src, members = _query("get_index_constituents", group)
        except (SourceUnavailable, NotSupported):
            log.warning("no source for %s constituents — serving cache", group)
            return cached
        if members:
            store.replace_index_members(conn, group, members, src)
            store.set_meta(conn, group, "members", src)
            return members
        return cached
