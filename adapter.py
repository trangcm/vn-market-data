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

The whole layer is synchronous: the sources use blocking ``httpx``, and SQLite reads
are sub-millisecond. Run it under a threadpool if you need concurrency — a future async
source can use ``httpx.Client`` (sync) under the same one.
"""
import logging
import time
from contextlib import closing, contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone

from vn_market_data.db import connect
from vn_market_data import market_hours, store
from vn_market_data.sources.base import (RATIO_LABELS, NotSupported, SourceUnavailable,
                                         SourceUnreachable, _take_units)
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
# How many suspect-thin bars one pass will pay a second-source call for. They come in
# ones — a feed either served a fragment for a date or it did not — and the cap is only
# so that a symbol which somehow accumulates them cannot turn one refresh into a storm.
STUB_RESOLVE_MAX = 3
# How much bigger the second reading has to be before the first is called a fragment.
# Two honest feeds agree on a *settled* session to the digit (DNSE and VCI matched
# exactly on every 2026-08-24..27 bar checked), while the fragments ran 5-16x short.
# Anything in between is left as it was rather than guessed at.
STUB_FRAGMENT_FACTOR = 2.0

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


@dataclass(frozen=True)
class SourceCall:
    """One request the adapter put to a source: which, for what, and how it ended.

    ``outcome`` is ``"ok"`` (the source answered — an empty answer included),
    ``"unavailable"`` (it raised :class:`SourceUnavailable`: down, or out of quota),
    ``"unreachable"`` (it raised :class:`SourceUnreachable`: the network, not a refusal) or
    ``"error"`` (anything else, which propagates). ``NotSupported`` is not recorded — it
    is a capability refusal decided before any request is made. ``symbol`` is the symbol
    (or index group) the call named, ``None`` for a multi-symbol call like the board.
    ``at`` is ``time.monotonic()`` when the request returned — meaningful only relative to
    the other calls in the same process, which is what a rate limit is measured in."""
    source: str
    method: str
    outcome: str
    symbol: str | None = None
    at: float = field(default_factory=time.monotonic, compare=False)
    #: The source's own metering units for this request, when it reported them
    #: (:func:`~vn_market_data.sources.base.report_units`); ``None`` = not reported.
    units: int | None = None


# Every recorder open in this context, innermost last. A tuple, so an inner recorder
# extends the outer one rather than shadowing it: a per-symbol count inside a per-pass one
# still leaves the pass with the whole total.
_recorders: ContextVar[tuple] = ContextVar("vn_market_data_recorders", default=())


@contextmanager
def record_source_calls():
    """Record every source request made inside the block; yields the list it fills.

    The store answers most reads without a request, so what a pass actually *cost* a
    rate-limited source can't be read off what it asked for — only off what reached the
    source. That is what this counts: one :class:`SourceCall` per request, cache hits
    excluded, so ``len(calls)`` is the pass's real bill and a trailing ``"unavailable"``
    marks where a quota tripped::

        with record_source_calls() as calls:
            for s in symbols:
                get_statements(s)
        tripped = [c for c in calls if c.outcome == "unavailable"]

    Scoped by :mod:`contextvars`, so it sees only this context's requests — a concurrent
    task's are not mixed in, and ``asyncio.to_thread`` carries it into the worker thread.
    It counts *requests*; a source's own metering units ride on ``SourceCall.units`` only
    where the source reports them (VCI's statements: four for a bank, three otherwise,
    since only a bank's ratio series is fetched), and quota spent outside this context —
    by another task in the same process, or another process — is invisible from here.
    """
    calls: list[SourceCall] = []
    token = _recorders.set(_recorders.get() + (calls,))
    try:
        yield calls
    finally:
        _recorders.reset(token)


def _record(source: str, method: str, outcome: str, args: tuple) -> None:
    units = _take_units()     # always taken, so a stale count cannot leak to the next call
    open_ = _recorders.get()
    if not open_:
        return
    symbol = args[0] if args and isinstance(args[0], str) else None
    call = SourceCall(source, method, outcome, symbol, units=units)
    for calls in open_:
        calls.append(call)


def _query(method: str, *args, **kwargs):
    """Try each source for one capability. Returns (source_name, result).
    Falls through on NotSupported / SourceUnavailable; raises the last
    SourceUnavailable if every source was unavailable."""
    last_unavailable = None
    for src in _src_chain():
        _take_units()
        try:
            result = getattr(src, method)(*args, **kwargs)
        except NotSupported:
            continue
        except SourceUnavailable as e:
            _record(src.name, method,
                    "unreachable" if isinstance(e, SourceUnreachable) else "unavailable", args)
            last_unavailable = e
            log.warning("%s unavailable for %s%s", src.name, method, args)
            continue
        except Exception:
            _record(src.name, method, "error", args)
            raise
        _record(src.name, method, "ok", args)
        return src.name, result
    if last_unavailable is not None:
        raise last_unavailable
    raise NotSupported(method)


# ── OHLCV ────────────────────────────────────────────────────────────────────
def _resolve_stubs(conn, symbol: str, dates: list[str], banked_source: str,
                   is_index: bool, limit: int | None = STUB_RESOLVE_MAX) -> None:
    """Take each suspect-thin bar to a *different* source and record the ruling.

    This is the other half of `store.upsert_ohlcv`'s volume test, and not a luxury: that
    test can say a bar is far below what the symbol normally trades, and no amount of the
    symbol's own history can say whether that is a fragment of an unsettled session or a
    genuinely quiet day — 0.8% of settled bars in the live store are that thin and every
    one of them is true. Only a second reading separates them, so a store with one source
    answering can defer a fragment by a day and no more.

    Metered on purpose: `limit` dates a pass, newest first, each date asked about once
    ever (`md_ohlcv_stubs`), and only for the recent tail the store already narrowed to.
    A caller repairing a known list passes `limit=None` — the cap is there to keep an
    incidental refresh from turning into a fetch storm, not to ration a deliberate scan.
    A source with no bar for the date is not a second opinion and is passed over: the
    ruling waits rather than being made on silence.
    """
    for day in sorted(dates)[-limit:] if limit else sorted(dates):
        banked = store.get_ohlcv_range(conn, symbol, day, day)
        suspect_vol = banked[0]["volume"] if banked else None
        for other in _src_chain():
            if other.name == banked_source:
                continue
            try:
                served = other.get_ohlcv(symbol, day, day, is_index=is_index)
            except NotSupported:
                continue
            except SourceUnavailable:
                _record(other.name, "get_ohlcv", "unavailable", (symbol,))
                continue
            _record(other.name, "get_ohlcv", "ok", (symbol,))
            alt = next((r for r in served or [] if r.get("date") == day
                        and r.get("volume") is not None), None)
            if alt is None:
                continue
            fragment = (suspect_vol is not None
                        and alt["volume"] > STUB_FRAGMENT_FACTOR * suspect_vol)
            if fragment:
                log.warning("%s: %s served a fragment for %s — %.0f against %.0f from "
                            "%s. Re-banking from %s, and %s is refused that date.",
                            symbol, banked_source, day, suspect_vol, alt["volume"],
                            other.name, other.name, banked_source)
                store.upsert_ohlcv(conn, symbol, [alt], other.name)
            store.record_stub_check(conn, symbol, day, source=banked_source,
                                    suspect_volume=suspect_vol,
                                    checked_against=other.name,
                                    resolved_volume=alt["volume"], fragment=fragment)
            break


# How long after the padded close (15:15 ICT) a daily feed has published the session's
# bar. Measured 2026-09-24: every symbol read at 15:30 carried it, every one read by
# 15:15 did not.
OHLCV_PUBLISH_LAG = timedelta(minutes=30)
# How often a catch-up is retried while the feed keeps serving the session's bar
# truncated (withheld by `store.upsert_ohlcv`). On 2026-08-28 it still was an hour
# after the close; the retry ends once the bar banks, or once the date is no longer
# today and the store banks it settled (then adjudicates it).
OHLCV_WITHHELD_RETRY = timedelta(minutes=15)


def _missed_close(conn, symbol: str, max_d: str) -> bool:
    """True when the last fetch predates the newest closed session's bar being
    published and the store still stops short of that session.

    The TTL alone cannot see this. A read during the session (or in the minutes after
    the close) finds no bar for it yet, and counts as fresh for four hours — so on
    2026-09-24 the 97 symbols a pipeline happened to read between 13:04 and 15:15 ICT
    served the previous session to every post-close job (patterns 16:31, scenarios,
    analogues, plan_odds), while the 318 read at 15:30 had the day. This is the
    catch-up pass `market_hours` promises: one per closed stretch, since the fetch it
    triggers is stamped after the close. A symbol that did not trade that session
    makes that one fetch and then falls back to the TTL.

    One exception: a catch-up that was *served* the session's bar but withheld it as
    truncated has not caught up, and is retried every `OHLCV_WITHHELD_RETRY` —
    otherwise that one read spends the pass and the TTL holds the previous bar until
    ~20:00, the exact failure this exists for.
    """
    fetched, withheld = store.meta_fetched_at(conn, symbol, "ohlcv")
    if fetched is None:
        return False
    close = market_hours.last_session_close()
    session, published = close.date().isoformat(), close + OHLCV_PUBLISH_LAG
    now = datetime.now(timezone.utc)
    if max_d >= session or now < published:
        return False
    if fetched < published:
        return True
    return withheld == session and now - fetched >= OHLCV_WITHHELD_RETRY


def _withheld_today(conn, symbol: str, rows: list[dict]) -> str | None:
    """Today's (ICT) date when the source served a bar for it that the store did not
    bank — `upsert_ohlcv` withholds a truncated one — else None."""
    today = datetime.now(market_hours.ICT).date().isoformat()
    if not any(r.get("date") == today for r in rows):
        return None
    bounds = store.ohlcv_bounds(conn, symbol)
    return today if bounds is None or bounds[1] < today else None


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
        band, repair, tail = None, [], False
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
            elif (missed := _missed_close(conn, symbol, max_d)) \
                    or not store.meta_fresh(conn, symbol, "ohlcv", ttl_s):
                if end > max_d and (today.weekday() < 5 or missed):
                    # Stale tail on a weekday → top up. All three conditions matter:
                    # without the TTL every call re-fetches, without `end > max_d` a
                    # symbol whose last candle is already today re-fetches all day, and
                    # without the weekday test every weekend call chases a session that
                    # will never print. `missed` overrides the TTL and the weekday test
                    # both: see _missed_close.
                    #
                    # It re-reads one banked bar *before* max_d as well. That overlap is
                    # the only place a sub-band rescale shows (`store.find_rescale`), and
                    # max_d alone may have been banked mid-session, so its settled
                    # re-read differs for a reason of its own; every bar before it was
                    # already re-read settled by the previous top-up.
                    fetch_from = store.ohlcv_date_before(conn, symbol, max_d) or max_d
                    tail = True
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
                        # Every fragment ruling this symbol carries was made about a bar
                        # on the *old* scale, and each one refuses a source a date. Keep
                        # them across a rescale and the refused date stays behind on the
                        # old scale while everything around it moves — a seam of our own
                        # making. Dropping them costs one re-adjudication, since the
                        # refetched bar is tested again on the way in.
                        store.clear_stub_checks(conn, symbol)
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
                rescale = (store.find_rescale(conn, symbol, rows)
                           if tail and not repair and not is_index else None)
                if rescale:
                    # The source has adjusted history the store holds on the old scale,
                    # by a factor too small for the band test. Banking the tail would
                    # join the two scales — so bank nothing from it, and refetch all of
                    # it through the same repair as a band seam, ledger included, so the
                    # derived rows priced on the old scale are found (`rescaled_symbols`).
                    day, factor = rescale
                    fetch_from = min(start, (date.fromisoformat(min_d)
                                             - timedelta(days=SEAM_REPAIR_LEAD_DAYS)).isoformat())
                    repair = [day]
                    store.clear_stub_checks(conn, symbol)
                    log.warning("%s: re-served %s bar rescaled ×%.4f (inside the ±%.0f%% "
                                "band) — refetching %s..%s (corporate action?)",
                                symbol, day, factor, band * 100, fetch_from, end)
                    try:
                        src, rows = _query("get_ohlcv", symbol, fetch_from, end,
                                           is_index=is_index)
                    except SourceUnavailable as e:
                        # Neither half is safe to bank; the next call retries.
                        log.warning("ohlcv refetch unavailable for %s (%s) — serving "
                                    "%s..%s from the store", symbol, e, *bounds)
                        return store.get_ohlcv_range(conn, symbol, start, end)
                withheld = None
                if rows:
                    suspect = store.upsert_ohlcv(conn, symbol, rows, src)
                    if suspect:
                        _resolve_stubs(conn, symbol, suspect, src, is_index)
                    withheld = _withheld_today(conn, symbol, rows)
                # A tail top-up starts at max_d and must not raise the floor with it.
                store.set_meta(conn, symbol, "ohlcv", src,
                               floor=min(fetch_from, floor) if floor else fetch_from,
                               withheld=withheld)
                if repair:
                    store.mark_seams_repaired(conn, symbol, repair, band)

        return store.get_ohlcv_range(conn, symbol, start, end)


def rescan_rescale(symbol: str, lookback_days: int = 45) -> str | None:
    """Re-read a symbol's recent window from the source and repair the whole banked
    history if any bar in it comes back rescaled (`store.find_rescale`); returns the
    rescaled date, or None when the window agrees with the store.

    `get_ohlcv` checks only a tail top-up's two-bar overlap, so this is how a caller
    reaches a rescale the store already absorbed — banked before that check existed,
    or on a pass that overlapped nothing. Costs one source read per symbol, two when
    it repairs.

    A date already in the seam ledger is repaired again, at most once a day. The
    ledger only says a refetch happened, not that it worked. If the source had not
    adjusted yet, that refetch re-banked the old scale, and `find_rescale` reaching
    this line proves the store still disagrees. Skipping on the ledger alone made
    such a repair permanent (VPI, 2026-09-08; review 2026-09 N5). The daily limit
    bounds the cost when a source keeps serving both scales.
    """
    symbol = symbol.strip().upper()
    today = date.today()
    with closing(connect()) as conn:
        bounds = store.ohlcv_bounds(conn, symbol)
        if bounds is None:
            return None
        start = max(bounds[0], (today - timedelta(days=lookback_days)).isoformat())
        _, rows = _query("get_ohlcv", symbol, start, today.isoformat())
        # Today's bar is still printing; only settled days can prove a rescale.
        hit = store.find_rescale(conn, symbol,
                                 [r for r in rows if r.get("date", "") < today.isoformat()])
        if not hit:
            return None
        # The seam is the join: the first banked bar the source already agrees with,
        # after the last one still on the old scale (`seam_void` reads it that way).
        day = store.ohlcv_date_after(conn, symbol, hit[0]) or hit[0]
        factor = hit[1]
        last = store.seam_repaired_at(conn, symbol, day)
        if last and last[:10] == datetime.now(timezone.utc).date().isoformat():
            return None
        fetch_from = (date.fromisoformat(bounds[0])
                      - timedelta(days=SEAM_REPAIR_LEAD_DAYS)).isoformat()
        log.warning("%s: banked %s bar rescaled ×%.4f at source — refetching %s..%s",
                    symbol, day, factor, fetch_from, today.isoformat())
        store.clear_stub_checks(conn, symbol)
        src, rows = _query("get_ohlcv", symbol, fetch_from, today.isoformat())
        withheld = None
        if rows:
            suspect = store.upsert_ohlcv(conn, symbol, rows, src)
            if suspect:
                _resolve_stubs(conn, symbol, suspect, src, False)
            withheld = _withheld_today(conn, symbol, rows)
        floor = store.meta_floor(conn, symbol, "ohlcv")
        # `withheld` is per fetch: leaving it out would clear a post-close catch-up's
        # retry marker while this refetch withheld the same truncated bar.
        store.set_meta(conn, symbol, "ohlcv", src,
                       floor=min(fetch_from, floor) if floor else fetch_from,
                       withheld=withheld)
        store.mark_seams_repaired(conn, symbol, [day],
                                  store.price_band(conn, symbol) or DEFAULT_PRICE_BAND)
        return day


def adjudicate_ohlcv(symbol: str, dates: list[str], *, is_index: bool = False) -> None:
    """Take specific *banked* bars to a second source and rule on them, as the write path
    does for the suspect bars it sees (`_resolve_stubs`).

    The write path only ever sees the bars it is banking, so bars written before the check
    existed — or before a second source was reachable — are never adjudicated. This is how
    a caller reaches those: a scan finds the suspects, this rules on them, and the ruling
    is what stops the source that got it wrong from re-serving them. Each date is asked
    about once, ever; a date already ruled on is a no-op, so this is safe to re-run.
    """
    with closing(connect()) as conn:
        checked = store.stub_checked(conn, symbol)
        pending = [d for d in dates if d not in checked]
        if not pending:
            return
        # Group by the source that actually banked each bar: the ruling names it, and a
        # second opinion has to come from someone else.
        by_source: dict[str, list[str]] = {}
        for row in conn.execute(
                f"""SELECT date, source FROM md_ohlcv WHERE symbol=? AND date IN
                    ({",".join("?" * len(pending))})""", (symbol, *pending)):
            by_source.setdefault(row["source"], []).append(row["date"])
        for src, days in by_source.items():
            _resolve_stubs(conn, symbol, days, src, is_index, limit=None)


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


# ── Settled foreign flow ───────────────────────────────────────────────────────
def get_foreign_history(symbol: str, start: str, end: str | None = None) -> tuple[str, list[dict]]:
    """One symbol's **settled** foreign buy/sell per session over ``[start, end]``,
    oldest-first, in full VND — ``(source_name, rows)``.

    **Uncached**, deliberately. The board's foreign columns are the live reading and
    the caller already keeps a per-session table of them; this is the post-close
    figure that table is reconciled *against*, so a second copy in this package would
    be a cache of a cache. The source name rides along so the caller can stamp which
    reading a row holds — a settled row and a board row of the same session are not
    interchangeable, and a writer that cannot tell them apart will let the busier one
    win.

    Unavailability propagates (``SourceUnavailable``): "nobody answered" must not be
    banked as "the exchange printed nothing".
    """
    symbol = symbol.strip().upper()
    if not symbol:
        return "", []
    end = end or date.today().isoformat()
    src, rows = _query("get_foreign_history", symbol, start, end)
    return src, rows or []


def get_foreign_archive(symbol: str, start: str, end: str) -> tuple[str, list[dict]]:
    """One symbol's foreign buy/sell per session over a **multi-year** window —
    ``(source_name, rows)``, the ``get_foreign_history`` shape.

    For backfilling history older than the settled source keeps, never for settling:
    a source here may read a whole session as zero, which only the cross-section
    shows, so the caller drops such dates before banking (see ``sources.vndirect``).
    Uncached, like ``get_foreign_history``.
    """
    symbol = symbol.strip().upper()
    if not symbol:
        return "", []
    src, rows = _query("get_foreign_archive", symbol, start, end)
    return src, rows or []


def get_trade_tape(symbol: str) -> tuple[str, dict | None]:
    """One symbol's matched-trade tape for the current or last closed session —
    ``(source_name, tape)``, the ``get_trade_tape`` shape, ``None`` when the symbol
    has not traded.

    **Uncached**, and there is nothing behind it to fall back on: the source serves
    one session only, so a caller that wants history has to bank each session
    itself, and must check ``tape["date"]`` is the session it means — the endpoint
    rolls over on its own clock, not this package's. Unavailability propagates
    (``SourceUnavailable``), and so does a tape that fails its own volume chain.
    """
    symbol = symbol.strip().upper()
    if not symbol:
        return "", None
    return _query("get_trade_tape", symbol)


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

    Every entry carries ``read_at`` (ISO-8601 UTC), when that snapshot was read from the
    source — so a cached or stale answer says how old it is instead of passing as live.
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
                read_at = store.insert_board(conn, fresh, src)
                # Depth defaults to "not read", as the same row reads back from the
                # store — a source without it must not answer differently fresh vs cached.
                out.update({s: {"vwap": None, "bids": None, "asks": None, **b,
                                "read_at": read_at} for s, b in fresh.items()})
        return out


def newest_board(symbol: str) -> dict | None:
    """The newest banked board snapshot for one symbol, at any age — a store-only
    read that calls no source. ``read_at`` says when it was read; outside the session
    that is the settled answer, and a caller showing it must show its age too."""
    symbol = symbol.strip().upper()
    if not symbol:
        return None
    with closing(connect()) as conn:
        return store.newest_board(conn, symbol)


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
            return _with_labelled_ratios(payload)
        src, payload = _query("get_statements", symbol, period)
        store.upsert_statements(conn, symbol, period, payload, src)  # None → negative cache
        store.bank_statement_periods(conn, symbol, period, payload, src)
        return _with_labelled_ratios(payload)


def labelled_ratios(payload: dict | None) -> dict:
    """A statements payload's ``ratio_extra`` if a source dated it, else ``{}``.

    Only a payload stamped ``ratio_labels`` = :data:`~vn_market_data.sources.base.RATIO_LABELS`
    has ratios keyed by the period they belong to. An unstamped one is a cache row
    written before that was true — VCI's 2018 figures relabelled as the newest
    periods — and would pass every shape check, so it is dropped rather than read.
    It heals at the row's next refresh."""
    if not payload or payload.get("ratio_labels") != RATIO_LABELS:
        return {}
    return payload.get("ratio_extra") or {}


def _with_labelled_ratios(payload: dict | None) -> dict | None:
    if payload is None or payload.get("ratio_labels") == RATIO_LABELS:
        return payload
    return {**payload, "ratio_extra": {}}


def banked_periods(symbol: str, period: str = "year") -> list[str]:
    """Period labels already archived for this symbol, newest first — a store-only
    read that touches no source. Lets a caller decide whether a fetch could even
    return something it does not already have."""
    symbol = symbol.strip().upper()
    if not symbol:
        return []
    with closing(connect()) as conn:
        return store.banked_labels(conn, symbol, period)


def banked_fetched_at(symbol: str, period: str = "year") -> str | None:
    """When this symbol's statement archive was last written — a store-only read that
    touches no source. See :func:`vn_market_data.store.banked_fetched_at`."""
    symbol = symbol.strip().upper()
    if not symbol:
        return None
    with closing(connect()) as conn:
        return store.banked_fetched_at(conn, symbol, period)


def get_banked_statements(symbol: str, period: str = "year", *,
                          max_periods: int | None = None) -> dict | None:
    """The archive alone, newest period first — **no source is ever contacted**.

    :func:`get_statement_history` refreshes through the cache before answering, which
    means it can spend the source's quota when a symbol has no cache row yet. A caller
    that only wants what is already banked — and whose freshness is governed elsewhere,
    the way the quarterly archive's is by its own gated banking pass — needs a read
    that cannot fetch at all, so an outage or a tripped quota degrades it to "nothing
    banked yet" rather than to an error.

    ``ratio_extra`` is carried from the *cached* payload rather than the archive,
    which holds none: the source serves the whole dated series on every fetch (VCI's
    runs back to 2018), so banking it per period would add nothing. Reading it from
    the cache keeps this fetch-free — an absent, expired or pre-stamp cache row simply
    yields ``{}``.
    """
    symbol = symbol.strip().upper()
    if not symbol:
        return None
    with closing(connect()) as conn:
        history = store.get_statement_history(conn, symbol, period, max_periods)
        if history is None:
            return None
        # ttl_s=inf: accept whatever is cached at any age, and never fetch. Freshness
        # here is the banking pass's responsibility, not this read's.
        _, cached = store.get_statements(conn, symbol, period, float("inf"))
        history["ratio_extra"] = labelled_ratios(cached)
        return history


def get_statement_history(symbol: str, period: str = "year", *,
                          max_periods: int | None = None,
                          ttl_s: float = STATEMENTS_TTL_S) -> dict | None:
    """Statements over every period ever banked for this symbol, newest first.

    Same shape as :func:`get_statements`, but reaching past the fixed window a source
    returns: VCI serves only the latest four periods, so a quarterly series read from
    it alone can never contain its own year-ago comparable no matter how often it is
    fetched. This refreshes through the normal cache first — so the current period is
    as fresh as ``get_statements`` would give — then answers from the archive that
    every fetch has been filling.

    Falls back to the live payload when nothing is banked yet (a cold archive is
    narrower, never empty), and carries the live ``ratio_extra`` through, since the
    archive deliberately holds no ratios (the source re-serves the whole series).
    """
    symbol = symbol.strip().upper()
    if not symbol:
        return None
    live = get_statements(symbol, period, ttl_s=ttl_s)
    with closing(connect()) as conn:
        history = store.get_statement_history(conn, symbol, period, max_periods)
    if history is None:
        return live
    if live:
        history["kind"] = live.get("kind") or history["kind"]
        history["ratio_extra"] = labelled_ratios(live)
    return history


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
