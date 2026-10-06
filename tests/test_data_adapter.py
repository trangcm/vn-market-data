"""DA-U-01..03 — the vn_market_data adapter (Tier 0, no network). [G5]

The store-first cache, the head-first source fallback chain, and the source
contract — all exercised with a temp SQLite DB and fake sources, so nothing here
touches DNSE/VCI or any real database.

These tests go with the package if it is ever split out, so they reach it only through
its public surface: ``set_sources`` and ``set_connection_factory``, never a monkeypatched
module global. ``test_package_boundary.py`` enforces the other half of that.
"""
import logging
import json
import sqlite3
from contextlib import closing
from datetime import date, datetime, timedelta, timezone

import pytest

import vn_market_data as vmd
from vn_market_data import adapter, store
from vn_market_data.market_hours import ICT
from vn_market_data.sources.base import (RATIO_LABELS, DataSource, NotSupported,
                                         SourceUnavailable)
from vn_market_data.sources.registry import build_sources


# ── DA-U-02: registry (ordered chain; TCBS rejected) ─────────────────────────

def test_registry_is_dnse_then_vietcap_then_vndirect_then_cafef_then_kbs():
    names = [s.name for s in build_sources()]
    # OHLCV primary, board, market turnover, settled foreign flow, trade tape — each of
    # the last four answers one capability nothing before it has, so order only matters
    # for the capabilities they share.
    assert names == ["dnse", "vietcap", "vndirect", "cafef", "kbs"]
    assert "tcbs" not in names          # TCBS public API is dead (rejected)


def test_the_default_chain_does_not_need_vnstock(monkeypatch):
    """Board, statements and events used to ride an optional install and vanish
    without it. The chain is the same everywhere now, and building it must not so
    much as look for the package."""
    import builtins
    real = builtins.__import__

    def no_vnstock(name, *a, **kw):
        if name.split(".")[0] in ("vnstock", "vnai", "pandas"):
            raise ImportError(name)
        return real(name, *a, **kw)

    monkeypatch.setattr(builtins, "__import__", no_vnstock)
    chain = build_sources()
    assert [s.name for s in chain][1] == "vietcap"
    assert "vci" not in [s.name for s in chain]


# ── DA-U-02: _query fallback semantics ───────────────────────────────────────

class _Src(DataSource):
    def __init__(self, name, *, ohlcv=None, raises=None):
        self.name = name
        self._ohlcv = ohlcv
        self._raises = raises
        self.calls = 0

    def get_ohlcv(self, symbol, start, end, *, is_index=False):
        self.calls += 1
        if self._raises:
            raise self._raises
        return self._ohlcv


@pytest.fixture
def restore_sources():
    """Undo any set_sources() a test does; None restores the built-in chain."""
    yield
    vmd.set_sources(None)


def test_query_falls_through_not_supported(restore_sources):
    a = _Src("a", raises=NotSupported("get_ohlcv"))
    b = _Src("b", ohlcv=[{"date": "2024-01-01"}])
    vmd.set_sources([a, b])
    name, rows = adapter._query("get_ohlcv", "HPG", "s", "e", is_index=False)
    assert name == "b" and rows == [{"date": "2024-01-01"}]


def test_query_falls_through_source_unavailable(restore_sources):
    a = _Src("a", raises=SourceUnavailable("rate limited"))
    b = _Src("b", ohlcv=[])
    vmd.set_sources([a, b])
    name, rows = adapter._query("get_ohlcv", "HPG", "s", "e")
    assert name == "b" and rows == []      # empty from b is authoritative


def test_query_raises_when_all_unavailable(restore_sources):
    vmd.set_sources([_Src("a", raises=SourceUnavailable("x")),
                     _Src("b", raises=SourceUnavailable("y"))])
    with pytest.raises(SourceUnavailable):
        adapter._query("get_ohlcv", "HPG", "s", "e")


def test_query_empty_result_stops_chain(restore_sources):
    # A genuinely-empty answer from the head source is authoritative — the tail is
    # never consulted.
    a = _Src("a", ohlcv=[])
    b = _Src("b", ohlcv=[{"date": "x"}])
    vmd.set_sources([a, b])
    name, rows = adapter._query("get_ohlcv", "HPG", "s", "e")
    assert name == "a" and rows == []
    assert b.calls == 0


def test_set_sources_none_restores_the_builtin_chain(restore_sources):
    vmd.set_sources([_Src("only", ohlcv=[])])
    assert [s.name for s in vmd.get_sources()] == ["only"]
    vmd.set_sources(None)
    assert [s.name for s in vmd.get_sources()] == ["dnse", "vietcap", "vndirect", "cafef", "kbs"]


# ── DA-U-03: source contract ─────────────────────────────────────────────────

def test_base_source_raises_not_supported_for_each_capability():
    s = DataSource()
    for call in (lambda: s.get_ohlcv("HPG", "s", "e"),
                 lambda: s.get_board(["HPG"]),
                 lambda: s.get_statements("HPG"),
                 lambda: s.get_events("HPG"),
                 lambda: s.get_market_turnover("VNINDEX", "s", "e")):
        with pytest.raises(NotSupported):
            call()


def test_vci_board_resolves_the_match_price_not_the_auction_one():
    """The bug this guards: VCI's board carries `match_price_ato` / `match_price_atc`
    *before* `match_price`, so a substring lookup for "match_price" answers with the ATO
    price — every stock frozen at its open (VRE read +0.23% on a +6.81% session).

    Column names in the real order vnstock hands them over."""
    from vn_market_data.sources.vci import pick_col

    cols = {c: c for c in [
        "listing/symbol", "listing/ceiling", "listing/floor", "listing/ref_price",
        "listing/mapping_symbol", "match/accumulated_value", "match/accumulated_volume",
        "match/accumulated_value_g1", "match/match_price_ato", "match/match_price_atc",
        "match/avg_match_price", "match/foreign_buy_value", "match/foreign_sell_value",
        "match/match_price", "match/open_price", "match/ceiling_price",
        "match/floor_price", "match/reference_price"]}

    assert pick_col(cols, "match", "match_price") == "match/match_price"
    assert pick_col(cols, "symbol") == "listing/symbol"
    assert pick_col(cols, "ref_price") == "listing/ref_price"
    assert pick_col(cols, "ceiling") == "listing/ceiling"
    assert pick_col(cols, "floor") == "listing/floor"
    assert pick_col(cols, "accumulated_value") == "match/accumulated_value"
    # No exact leaf → the substring pass still answers, which is how renamed columns
    # keep resolving across vnstock versions.
    assert pick_col(cols, "foreign_buy") == "match/foreign_buy_value"
    assert pick_col(cols, "match", "close_price") is None


class _Frame:
    """The bit of a pandas frame `get_events` reads on its fallback path: an emptiness
    flag and `to_dict("records")`. Kept local so the package's tests stay free of a
    pandas import the source itself does not make."""
    def __init__(self, rows):
        self._rows, self.empty = rows, not rows

    def to_dict(self, _orient):
        return list(self._rows)


_ELC_EVENTS = [
    {"eventTitleVi": "Phát hành cổ phiếu - Trả Cổ tức bằng Cổ phiếu tỉ lệ 5.0%",
     "exrightDate": "2026-09-10T00:00:00", "recordDate": "2026-09-11T00:00:00",
     "exerciseRatio": 0.05, "eventCode": "ISS"},
    {"eventTitleVi": "Phát hành cổ phiếu - Cổ phiếu thưởng tỉ lệ 2.0%",
     "exrightDate": "2026-09-10T00:00:00", "recordDate": "2026-09-11T00:00:00",
     "exerciseRatio": 0.02, "eventCode": "ISS"},
    {"eventTitleVi": "ELC - Tổ chức ĐHĐCĐ thường niên 2026",   # excluded: AGM
     "exrightDate": "2026-03-17T00:00:00", "exerciseRatio": None},
]


def _serve_events(monkeypatch, *, filtered=None, frame=None):
    """Stand in for vnstock. `filtered` answers the DIV,ISS request the source prefers;
    `None` there makes that call fail the way a moved private method would, so the
    public-frame fallback is what gets exercised."""
    import sys, types
    seen = {}

    class _Provider:
        def _fetch_events(self, event_codes=None):
            seen["event_codes"] = event_codes
            if filtered is None:
                raise AttributeError("no such method in this vnstock")
            return list(filtered)

    class _Company:
        def __init__(self, symbol, source):
            self._provider = _Provider()

        def events(self):
            seen["fell_back"] = True
            return _Frame(frame or [])

    mod = types.ModuleType("vnstock")
    mod.Company = _Company
    monkeypatch.setitem(sys.modules, "vnstock", mod)
    return seen


def test_vci_events_survive_a_symbol_that_has_never_paid_cash(monkeypatch):
    """The bug this guards: VCI omits a field entirely when no row in the answer carries
    it. ELC has only ever issued shares, so its answer has no `value_per_share`/
    `payout_date` — and requiring those columns dropped every event the symbol has,
    including the 5% stock dividend three days from its ex-date. SM's Upcoming Dividends
    card read empty for a dividend that was actually coming."""
    from vn_market_data.sources.vci import VCISource

    seen = _serve_events(monkeypatch, filtered=_ELC_EVENTS)
    events = VCISource().get_events("ELC")

    assert seen["event_codes"] == "DIV,ISS"   # not the everything-page
    assert [(e["type"], e["ex_date"], e["ratio"]) for e in events] == [
        ("STOCK", "2026-09-10", 0.05), ("STOCK", "2026-09-10", 0.02)]
    # The field the answer does not have reads as no cash, not as a missing symbol.
    assert all(e["value_per_share"] == 0.0 and e["pay_date"] == "" for e in events)


def test_vci_events_fall_back_to_the_public_frame(monkeypatch):
    """`_fetch_events` is vnstock-private. A version that moves it must cost the code
    filter, not the feed — the frame's rows are already snake_case."""
    from vn_market_data.sources.vci import VCISource, _snake

    frame = [{_snake(k): v for k, v in r.items()} for r in _ELC_EVENTS]
    seen = _serve_events(monkeypatch, filtered=None, frame=frame)
    events = VCISource().get_events("ELC")

    assert seen["fell_back"] is True
    assert [(e["type"], e["ratio"]) for e in events] == [("STOCK", 0.05), ("STOCK", 0.02)]


def test_vci_events_publish_an_announcement_with_no_date_yet(monkeypatch):
    """The guard that was **dropped** on 2026-09-16, and why.

    `exright_date` used to be required, so an answer carrying none returned `[]`. That
    was the right shape for an upstream rename and the wrong one for the ordinary case
    it could not tell apart: VCI publishes an entitlement's rate the day the board
    resolves it and fills the date in only when the issuer files the record date. DRI
    declared 1,000 VND/share on 2026-09-11 and VPB a 26.04104% bonus on the 15th, and
    both were invisible — not late, not flagged, *absent* — while their rates sat in
    the feed. A dividend that has been declared must not read as no dividend.

    So a missing date is now a status. The rename it used to guard against is still
    visible, but as "every row on every symbol reads announced", which
    `/api/monitor.announced_events` warns on — a loud wrong state instead of a silent
    empty one.
    """
    from vn_market_data.sources.vci import VCISource

    _serve_events(monkeypatch, filtered=[
        {"eventTitleVi": "Trả cổ tức bằng tiền mặt - Cả năm 2025 - 1,000 VND",
         "publicDate": "2026-09-11T00:00:00", "valuePerShare": 1000.0,
         "exerciseRatio": 0.1, "eventCode": "DIV"}])
    [ev] = VCISource().get_events("DRI")

    assert ev["status"] == "announced"
    # Empty string, never None: every downstream guard is a truthiness or a string
    # comparison (`e.get("ex_date", "") >= cutoff` in scrapers/financials.py would
    # raise TypeError on a None), and an empty sorts first under `ORDER BY ex_date`.
    assert ev["ex_date"] == "" and ev["type"] == "CASH"
    assert ev["value_per_share"] == 1000.0
    # The one date it has. Without it, an entitlement announced this morning and one
    # announced in April and never dated are the same row.
    assert ev["announced_date"] == "2026-09-11"


def test_vci_events_mark_a_dated_event_confirmed(monkeypatch):
    """The other half of the same field: a row VCI has dated must not be softened into
    an announcement, or the card would stop telling the two apart in the direction that
    matters — one of these adjusts the price on a known day."""
    from vn_market_data.sources.vci import VCISource

    _serve_events(monkeypatch, filtered=_ELC_EVENTS)
    events = VCISource().get_events("ELC")
    assert [e["status"] for e in events] == ["confirmed", "confirmed"]


def test_vci_events_refuse_an_answer_that_cannot_be_classified(monkeypatch):
    """The guard that stays. `event_title_vi` is the only field the classifier reads to
    decide cash-vs-stock-vs-not-a-dividend; without it nothing can be published at all,
    and an answer missing it is an upstream change rather than rows to emit."""
    from vn_market_data.sources.vci import VCISource

    _serve_events(monkeypatch, filtered=[{"exrightDate": "2026-09-10T00:00:00",
                                          "exerciseRatio": 0.05}])
    assert VCISource().get_events("ELC") == []


# ── DA-U-01: store-first cache (temp DB, fake source) ────────────────────────

@pytest.fixture
def temp_db(tmp_path, restore_sources):
    """Hand the package a fresh temp SQLite DB the way a host application does — through
    the public connection factory, not a monkeypatched module global. Whatever factory
    was installed (a host installs its own on import) goes back on teardown."""
    path = tmp_path / "test.db"

    def _connect():
        conn = sqlite3.connect(path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA synchronous=OFF")   # throwaway DB; don't pay fsync per test
        return conn

    saved = vmd.get_connection_factory()
    vmd.set_connection_factory(_connect)
    with _connect() as conn:
        vmd.init_schema(conn)
    yield
    vmd.set_connection_factory(saved)


def _recent_rows(n=5):
    base = date.today() - timedelta(days=n)
    rows = []
    for i in range(n):
        px = 100 + i
        rows.append({"date": (base + timedelta(days=i)).isoformat(),
                     "open": px, "high": px + 1, "low": px - 1,
                     "close": px, "volume": 1000 + i})
    return rows


def test_get_ohlcv_is_store_first_and_writes_through(temp_db):
    # 30 days of stored history; a 3-day lookback is fully covered, so the second
    # call has no missing history / stale tail and must serve purely from the store.
    src = _Src("fake", ohlcv=_recent_rows(30))
    vmd.set_sources([src])

    first = adapter.get_ohlcv("HPG", lookback_days=3)
    assert first, "cold cache should backfill from the source"
    assert src.calls == 1

    second = adapter.get_ohlcv("HPG", lookback_days=3)
    assert [r["close"] for r in second] == [r["close"] for r in first]
    assert src.calls == 1, "served from the store — source must not be hit again"


def test_get_ohlcv_blank_symbol_returns_empty(temp_db):
    assert adapter.get_ohlcv("   ") == []


# ── DA-U-01: the incremental top-up branch ───────────────────────────────────
# Three ANDed conditions decide whether a stale symbol tops up (adapter.py). Each is
# silent when wrong — the wrong one just means "fetches nobody notices", until the
# provider quota does.

def _pin_today(monkeypatch, day: date) -> None:
    """Freeze `date.today()` inside the adapter. The top-up branch reads the calendar
    three different ways, so a test that can't move the date can't reach it."""
    class _Date(date):
        @classmethod
        def today(cls):
            return day
    monkeypatch.setattr(adapter, "date", _Date)


def _rows_between(d0: date, d1: date) -> list[dict]:
    rows, d = [], d0
    while d <= d1:
        rows.append({"date": d.isoformat(), "open": 100.0, "high": 101.0,
                     "low": 99.0, "close": 100.0, "volume": 1000})
        d += timedelta(days=1)
    return rows


class _RangeSrc(DataSource):
    """Answers within the requested window and records every window it was asked for —
    the only way to tell a tail top-up apart from a full refetch."""
    name = "range"

    def __init__(self, rows):
        self._rows, self.calls = rows, []

    def get_ohlcv(self, symbol, start, end, *, is_index=False):
        self.calls.append((start, end))
        return [r for r in self._rows if start <= r["date"] <= end]


# A Wednesday, so ±1 day stays inside the trading week.
_WED = date(2026, 7, 15)


def test_stale_tail_tops_up_from_the_last_stored_candle(temp_db, monkeypatch):
    src = _RangeSrc(_rows_between(_WED - timedelta(days=60), _WED + timedelta(days=5)))
    vmd.set_sources([src])

    _pin_today(monkeypatch, _WED)
    adapter.get_ohlcv("HPG", lookback_days=30)
    assert src.calls == [((_WED - timedelta(days=30)).isoformat(), _WED.isoformat())]

    # Next weekday, meta stale: fetch again — but only from the tail we hold, not the
    # whole 30-day window again. One settled bar before max_d is re-read with it: that
    # overlap is where a sub-band rescale shows (see DA-U-01b).
    _pin_today(monkeypatch, _WED + timedelta(days=1))
    adapter.get_ohlcv("HPG", lookback_days=30, ttl_s=0)
    assert len(src.calls) == 2
    assert src.calls[1][0] == (_WED - timedelta(days=1)).isoformat(), (
        "top-up must start one banked bar before max_d, not at start")


def test_no_top_up_when_the_last_candle_is_already_today(temp_db, monkeypatch):
    """Without the `end > max_d` condition a symbol whose tail is current re-fetches on
    every call for the rest of the day, TTL or no TTL."""
    src = _RangeSrc(_rows_between(_WED - timedelta(days=60), _WED))
    vmd.set_sources([src])
    _pin_today(monkeypatch, _WED)

    adapter.get_ohlcv("HPG", lookback_days=30)
    adapter.get_ohlcv("HPG", lookback_days=30, ttl_s=0)
    assert len(src.calls) == 1


def test_no_top_up_at_the_weekend(temp_db, monkeypatch):
    """The exchange will never print a Saturday session, so a stale tail on a weekend is
    not stale — it is final."""
    src = _RangeSrc(_rows_between(_WED - timedelta(days=60), _WED + timedelta(days=5)))
    vmd.set_sources([src])

    friday = _WED + timedelta(days=2)
    _pin_today(monkeypatch, friday)
    adapter.get_ohlcv("HPG", lookback_days=30)
    assert len(src.calls) == 1

    _pin_today(monkeypatch, friday + timedelta(days=1))   # Saturday
    adapter.get_ohlcv("HPG", lookback_days=30, ttl_s=0)
    assert len(src.calls) == 1, "weekend must not chase a session that cannot print"


def test_deeper_lookback_refetches_rather_than_tops_up(temp_db, monkeypatch):
    """A caller asking for more history than is banked needs the *front* filled; topping
    up the tail would silently answer with the shorter series it already had."""
    src = _RangeSrc(_rows_between(_WED - timedelta(days=200), _WED))
    vmd.set_sources([src])
    _pin_today(monkeypatch, _WED)

    adapter.get_ohlcv("HPG", lookback_days=30)
    rows = adapter.get_ohlcv("HPG", lookback_days=180)
    assert src.calls[1][0] == (_WED - timedelta(days=180)).isoformat()
    assert len(rows) > 150


def test_short_history_is_not_refetched_on_every_call(temp_db, monkeypatch):
    """A symbol listed after the lookback window begins answers with everything it has,
    and that is the complete answer. Measured against the oldest candle *banked* it looks
    like a permanent cache miss, so every recent listing would refetch its whole history
    on every pass — the exact traffic this package exists to remove."""
    listed = _WED - timedelta(days=40)              # 40 days of history, 180 requested
    src = _RangeSrc(_rows_between(listed, _WED))
    vmd.set_sources([src])
    _pin_today(monkeypatch, _WED)

    first = adapter.get_ohlcv("NEW", lookback_days=180)
    assert len(src.calls) == 1
    assert len(first) == len(_rows_between(listed, _WED))

    # Same window again, and again with the TTL stale — the store already holds every
    # candle that exists and the tail is current, so neither call reaches the source.
    again = adapter.get_ohlcv("NEW", lookback_days=180)
    adapter.get_ohlcv("NEW", lookback_days=180, ttl_s=0)
    assert len(src.calls) == 1, "a short history is the whole answer, not a cache miss"
    assert len(again) == len(first)

    # Next weekday with the TTL stale: the tail is genuinely one session behind, so this
    # does fetch — but only from the last candle held, not from the front again.
    _pin_today(monkeypatch, _WED + timedelta(days=1))
    adapter.get_ohlcv("NEW", lookback_days=180, ttl_s=0)
    assert len(src.calls) == 2
    assert src.calls[-1][0] == (_WED - timedelta(days=1)).isoformat(), (
        "top-up must start one banked bar before max_d, not at start")

    # But a genuinely deeper request is still a miss.
    adapter.get_ohlcv("NEW", lookback_days=365)
    assert src.calls[-1][0] == (_WED + timedelta(days=1) - timedelta(days=365)).isoformat()


def _pin_close(monkeypatch, close: datetime) -> None:
    monkeypatch.setattr(adapter.market_hours, "last_session_close", lambda now=None: close)


def _stamp_fetch(symbol: str, at: datetime) -> None:
    with closing(adapter.connect()) as conn:
        conn.execute("UPDATE md_fetch_meta SET fetched_at=? WHERE symbol=? AND kind='ohlcv'",
                     (at.astimezone(timezone.utc).isoformat(), symbol))
        conn.commit()


def test_a_pre_close_read_is_caught_up_once_after_the_close(temp_db, monkeypatch):
    """2026-09-24: 97 symbols read between 13:04 and 15:15 ICT had no bar for the
    session, and the 4-h TTL called them fresh until ~17:00 — so every post-close job
    ran a session behind. A read made before the session's bar was published must not
    count as fresh once it has been; the catch-up is stamped after the close, so it
    happens exactly once."""
    close = (datetime.now(ICT) - timedelta(hours=2)).replace(microsecond=0)
    session = close.date()
    _pin_close(monkeypatch, close)
    _pin_today(monkeypatch, session)
    src = _RangeSrc(_rows_between(session - timedelta(days=60), session - timedelta(days=1)))
    vmd.set_sources([src])
    adapter.get_ohlcv("DRI", lookback_days=30)
    _stamp_fetch("DRI", close - timedelta(minutes=30))       # read mid-session, inside the TTL

    src._rows = _rows_between(session - timedelta(days=60), session)   # bar published
    rows = adapter.get_ohlcv("DRI", lookback_days=30)        # default 4-h TTL: "fresh"
    assert len(src.calls) == 2, "a pre-close read must not stand in for the close"
    assert rows[-1]["date"] == session.isoformat()

    adapter.get_ohlcv("DRI", lookback_days=30)
    assert len(src.calls) == 2, "caught up — the TTL governs again"


def test_a_session_that_printed_nothing_costs_one_catch_up(temp_db, monkeypatch):
    """A symbol that did not trade gets no bar; the catch-up is stamped after the close
    all the same, so it does not chase the missing bar on every call."""
    close = (datetime.now(ICT) - timedelta(hours=2)).replace(microsecond=0)
    session = close.date()
    _pin_close(monkeypatch, close)
    _pin_today(monkeypatch, session)
    src = _RangeSrc(_rows_between(session - timedelta(days=60), session - timedelta(days=1)))
    vmd.set_sources([src])
    adapter.get_ohlcv("ILQ", lookback_days=30)
    _stamp_fetch("ILQ", close - timedelta(minutes=30))

    adapter.get_ohlcv("ILQ", lookback_days=30)
    adapter.get_ohlcv("ILQ", lookback_days=30)
    assert len(src.calls) == 2


def test_a_catch_up_served_a_truncated_bar_is_retried(temp_db, monkeypatch):
    """The feed can still serve the session's bar truncated after the close (2026-08-28,
    an hour on). `upsert_ohlcv` withholds it — and if the catch-up then counted as done,
    the TTL would hold the previous bar until ~20:00. So it retries, at most every
    OHLCV_WITHHELD_RETRY, until the settled bar banks."""
    now = datetime.now(ICT)
    close = (now - timedelta(hours=2)).replace(microsecond=0)
    session = close.date()
    if session != now.date():
        pytest.skip("needs a close earlier today (ICT) — the withhold keys on today")
    _pin_close(monkeypatch, close)
    _pin_today(monkeypatch, session)
    src = _RangeSrc(_rows_between(session - timedelta(days=60), session - timedelta(days=1)))
    vmd.set_sources([src])
    adapter.get_ohlcv("DRI", lookback_days=30)
    _stamp_fetch("DRI", close - timedelta(minutes=30))

    fragment = dict(_rows_between(session, session)[0], volume=5)     # 0.5% of normal
    src._rows = _rows_between(session - timedelta(days=60), session - timedelta(days=1)) + [fragment]
    rows = adapter.get_ohlcv("DRI", lookback_days=30)                  # the catch-up
    assert len(src.calls) == 2 and rows[-1]["date"] < session.isoformat(), "withheld"

    adapter.get_ohlcv("DRI", lookback_days=30)
    assert len(src.calls) == 2, "not before the retry interval"

    _stamp_fetch("DRI", datetime.now(ICT) - adapter.OHLCV_WITHHELD_RETRY)
    src._rows[-1] = _rows_between(session, session)[0]                 # settled
    rows = adapter.get_ohlcv("DRI", lookback_days=30)
    assert len(src.calls) == 3 and rows[-1]["date"] == session.isoformat()

    _stamp_fetch("DRI", datetime.now(ICT) - adapter.OHLCV_WITHHELD_RETRY)
    adapter.get_ohlcv("DRI", lookback_days=30)
    assert len(src.calls) == 3, "banked — the retry stops"


def test_no_catch_up_before_the_bar_can_be_published(temp_db, monkeypatch):
    """Minutes after the close the feed has not published the bar yet; a catch-up then
    would be stamped post-close with nothing in it, and spend the one pass."""
    close = (datetime.now(ICT) - timedelta(minutes=5)).replace(microsecond=0)
    session = close.date()
    _pin_close(monkeypatch, close)
    _pin_today(monkeypatch, session)
    src = _RangeSrc(_rows_between(session - timedelta(days=60), session - timedelta(days=1)))
    vmd.set_sources([src])
    adapter.get_ohlcv("DRI", lookback_days=30)
    _stamp_fetch("DRI", close - timedelta(hours=2))

    adapter.get_ohlcv("DRI", lookback_days=30)
    assert len(src.calls) == 1


# ── DA-U-01: the corporate-action seam detector ──────────────────────────────
# A tail top-up is blind to a source rescaling the history behind it: the old bars stay
# on the old scale and the join prints a fall no exchange would have allowed. It never
# heals on its own, because every later call tops up the tail too.


def _seamed(d0, d1, *, scale, at):
    """A flat run of candles carrying two different scales — the shape a
    back-adjustment leaves behind when only the tail was refetched."""
    rows = _rows_between(d0, d1)
    for r in rows:
        if r["date"] >= at:
            for k in ("open", "high", "low", "close"):
                r[k] = round(r[k] * scale, 2)
    return rows


def _bank(symbol, rows, board=None):
    """Seed the store directly — these tests are about what the adapter does with
    candles that are *already* banked, so nothing here should reach a source."""
    with closing(vmd.connect()) as conn:
        store.upsert_ohlcv(conn, symbol, rows, "seeded")
        if board is not None:
            store.insert_board(conn, {symbol: board}, "fake")


def test_price_band_snaps_up_to_the_published_band(temp_db):
    with closing(vmd.connect()) as conn:
        # Ceiling and floor are rounded to the tick *inside* the band, so a HOSE symbol
        # implies 6.8%, not 7% — taken raw it would flag every legal limit-up move.
        store.insert_board(conn, {"MBB": {"ceiling": 21250.0, "floor": 18550.0,
                                          "ref_price": 19900.0}}, "fake")
        assert store.price_band(conn, "MBB") == 0.07
        assert store.price_band(conn, "NEVER_BOARDED") is None


def test_price_band_rejects_a_malformed_snapshot(temp_db):
    with closing(vmd.connect()) as conn:
        store.insert_board(conn, {"X": {"ceiling": 900.0, "floor": 10.0,
                                        "ref_price": 100.0}}, "fake")
        assert store.price_band(conn, "X") is None   # 800% is not a band → caller's default


def test_find_price_seam_ignores_a_gap_in_the_series(temp_db):
    """Two rows a fortnight apart are not adjacent sessions, and a fortnight's move is
    not bounded by one session's band. Flagging it would refetch on missing data."""
    rows = [{"date": "2026-07-01", "open": 100, "high": 100, "low": 100,
             "close": 100.0, "volume": 1},
            {"date": "2026-07-20", "open": 50, "high": 50, "low": 50,
             "close": 50.0, "volume": 1}]
    _bank("GAPPY", rows)
    with closing(vmd.connect()) as conn:
        assert store.find_price_seams(conn, "GAPPY", "2026-01-01", "2026-12-31", 0.07) == []


def test_corporate_action_seam_forces_a_full_refetch(temp_db, monkeypatch, caplog):
    """MBB, 2026-08-07: a 15% stock dividend plus a 10:1 rights issue rescaled the whole
    history at source. The cache held 23,900 in front of 20,120 — a 15.8% fall on a
    ±7% board. The repair has to reach every candle held, not the tail."""
    _pin_today(monkeypatch, _WED)
    _bank("MBB",
          _seamed(_WED - timedelta(days=40), _WED - timedelta(days=1),
                  scale=0.8, at=(_WED - timedelta(days=10)).isoformat()),
          board={"ceiling": 107.0, "floor": 93.0, "ref_price": 100.0})

    src = _RangeSrc(_seamed(_WED - timedelta(days=60), _WED,
                            scale=0.8, at="1900-01-01"))     # source is fully adjusted
    vmd.set_sources([src])

    with caplog.at_level("WARNING"):
        rows = adapter.get_ohlcv("MBB", lookback_days=30, ttl_s=0)

    assert src.calls, "a seam must reach a source"
    assert src.calls[-1][0] == (_WED - timedelta(days=40 + adapter.SEAM_REPAIR_LEAD_DAYS)
                                ).isoformat(), (
        "the refetch must start before the oldest candle held — a tail top-up, or even "
        "the requested window, would leave earlier bars on the pre-adjustment scale, and "
        "a source that answers from the day *after* the one asked for would leave the "
        "oldest bar itself")
    assert "price seam" in caplog.text
    closes = [r["close"] for r in rows]
    assert max(closes) / min(closes) < 1.07, "the seam must be gone after the repair"


def test_a_seam_that_survives_the_refetch_is_never_chased_again(temp_db, monkeypatch):
    """9% of the cached universe carries a move beyond *today's* band that was really
    traded — a bank that has since moved from UPCOM to HOSE met ±15% at the time. The
    source serves it back unchanged, so without a ledger of what has already been tried
    the detector refetches those whole histories once per TTL, forever."""
    _pin_today(monkeypatch, _WED)
    banked = _seamed(_WED - timedelta(days=40), _WED,
                     scale=0.8, at=(_WED - timedelta(days=10)).isoformat())
    _bank("VAB", banked, board={"ceiling": 107.0, "floor": 93.0, "ref_price": 100.0})

    src = _RangeSrc(banked)                       # the source agrees: this really traded
    vmd.set_sources([src])

    adapter.get_ohlcv("VAB", lookback_days=30, ttl_s=0)
    assert len(src.calls) == 1, "the first sighting is worth one refetch"
    adapter.get_ohlcv("VAB", lookback_days=30, ttl_s=0)
    adapter.get_ohlcv("VAB", lookback_days=30, ttl_s=0)
    assert len(src.calls) == 1, "and only one — the refetch already answered the question"


def test_a_settled_seam_does_not_mask_a_later_one(temp_db, monkeypatch):
    """The dangerous shape: a symbol carrying an old move no refetch will change, which
    then has a real corporate action. Reporting only the earliest seam would leave the
    new one permanently invisible behind the old one."""
    _pin_today(monkeypatch, _WED)
    banked = _seamed(_WED - timedelta(days=40), _WED,
                     scale=0.8, at=(_WED - timedelta(days=30)).isoformat())
    _bank("SHB", banked, board={"ceiling": 107.0, "floor": 93.0, "ref_price": 100.0})
    src = _RangeSrc(banked)
    vmd.set_sources([src])
    adapter.get_ohlcv("SHB", lookback_days=30, ttl_s=0)     # old seam: tried, survives
    assert len(src.calls) == 1

    # Now a real action rescales the tail — a second seam, ten days back.
    fresh = _seamed(_WED - timedelta(days=40), _WED,
                    scale=0.5, at=(_WED - timedelta(days=10)).isoformat())
    _bank("SHB", [r for r in fresh if r["date"] >= (_WED - timedelta(days=10)).isoformat()])
    adapter.get_ohlcv("SHB", lookback_days=30, ttl_s=0)
    assert len(src.calls) == 2, "the new seam must still be seen behind the settled one"


def test_a_clean_series_is_not_refetched(temp_db, monkeypatch):
    """The detector's whole cost falls on symbols that were never wrong, so it must be
    silent on an ordinary series — including one that limit-moves every session."""
    _pin_today(monkeypatch, _WED)
    rows = _rows_between(_WED - timedelta(days=40), _WED)
    px = 100.0
    for r in rows:                              # +6.9% a day: legal on HOSE, every day
        px *= 1.069
        for k in ("open", "high", "low", "close"):
            r[k] = round(px, 2)
    _bank("RUNNER", rows,
          board={"ceiling": 107.0, "floor": 93.0, "ref_price": 100.0})

    src = _RangeSrc(rows)
    vmd.set_sources([src])
    adapter.get_ohlcv("RUNNER", lookback_days=30, ttl_s=0)
    assert src.calls == [], "no missing history, no stale tail, no seam — no fetch"


def test_index_series_is_never_seam_checked(temp_db, monkeypatch):
    """VNINDEX has no corporate actions and no price band; a crash in the index is a
    crash, and refetching two years of it would be a permanent cost for nothing."""
    _pin_today(monkeypatch, _WED)
    banked = _seamed(_WED - timedelta(days=40), _WED,
                     scale=0.5, at=(_WED - timedelta(days=10)).isoformat())
    _bank("VNINDEX", banked)
    src = _RangeSrc(banked)
    vmd.set_sources([src])
    adapter.get_ohlcv("VNINDEX", lookback_days=30, is_index=True, ttl_s=0)
    assert src.calls == []


# ── DA-U-01b: which repairs actually rewrote history ─────────────────────────
# The ledger above records every *attempt*, and most attempts change nothing. Anything
# derived from a rescaled series — a persisted pivot, a graded outcome — is denominated
# in a scale that no longer exists, so the two cases have to be told apart after the
# fact, from the data rather than from the flag.


def test_a_repair_that_rescaled_the_history_is_reported(temp_db, monkeypatch):
    _pin_today(monkeypatch, _WED)
    _bank("MBB",
          _seamed(_WED - timedelta(days=40), _WED - timedelta(days=1),
                  scale=0.8, at=(_WED - timedelta(days=10)).isoformat()),
          board={"ceiling": 107.0, "floor": 93.0, "ref_price": 100.0})
    vmd.set_sources([_RangeSrc(_seamed(_WED - timedelta(days=60), _WED,
                                       scale=0.8, at="1900-01-01"))])
    adapter.get_ohlcv("MBB", lookback_days=30, ttl_s=0)

    with closing(vmd.connect()) as conn:
        got = store.rescaled_symbols(conn)
    assert set(got) == {"MBB"}
    assert got["MBB"]["seam_date"] == (_WED - timedelta(days=10)).isoformat(), (
        "the reported date is the first bar on the new scale — what a holder of old "
        "levels needs in order to know which of its rows are dead")
    assert got["MBB"]["rescaled_at"]        # and when the scale changed under them


def test_a_seam_that_really_traded_is_not_reported_as_a_rescale(temp_db, monkeypatch):
    """The common case, and the one that makes this a data test rather than a flag
    read: the ledger row looks identical to the one above. Only the series can say
    the repair changed nothing, and here it didn't — so nothing derived is stale."""
    _pin_today(monkeypatch, _WED)
    banked = _seamed(_WED - timedelta(days=40), _WED,
                     scale=0.8, at=(_WED - timedelta(days=10)).isoformat())
    _bank("VAB", banked, board={"ceiling": 107.0, "floor": 93.0, "ref_price": 100.0})
    vmd.set_sources([_RangeSrc(banked)])          # the source agrees: this really traded
    adapter.get_ohlcv("VAB", lookback_days=30, ttl_s=0)

    with closing(vmd.connect()) as conn:
        assert store.seams_repaired(conn, "VAB"), "the attempt was made and recorded"
        assert store.rescaled_symbols(conn) == {}, "but it rewrote nothing"


def test_a_settled_seam_does_not_hide_a_later_rescale(temp_db, monkeypatch):
    """A symbol can carry both: an old move no refetch will change, and a real action
    after it. The surviving seam must not suppress the resolved one, or the symbol
    whose levels are actually dead is the one that goes unreported."""
    _pin_today(monkeypatch, _WED)
    old_seam = (_WED - timedelta(days=30)).isoformat()
    banked = _seamed(_WED - timedelta(days=40), _WED, scale=0.8, at=old_seam)
    _bank("SHB", banked, board={"ceiling": 107.0, "floor": 93.0, "ref_price": 100.0})
    vmd.set_sources([_RangeSrc(banked)])
    adapter.get_ohlcv("SHB", lookback_days=30, ttl_s=0)          # survives

    # A real action rescales the tail. The source carries the adjustment — and still
    # carries the settled move, because that one really happened.
    _bank("SHB", [r for r in _seamed(_WED - timedelta(days=40), _WED, scale=0.5,
                                     at=(_WED - timedelta(days=10)).isoformat())
                  if r["date"] >= (_WED - timedelta(days=10)).isoformat()])
    vmd.set_sources([_RangeSrc(_seamed(_WED - timedelta(days=60), _WED,
                                       scale=0.8, at=old_seam))])
    adapter.get_ohlcv("SHB", lookback_days=30, ttl_s=0)

    with closing(vmd.connect()) as conn:
        got = store.rescaled_symbols(conn)
    assert set(got) == {"SHB"}
    assert got["SHB"]["seams"] == [(_WED - timedelta(days=10)).isoformat()], (
        "only the seam that went away — the settled one is still in the series")


# ── DA-U-01b: a rescale inside the band ──────────────────────────────────────
# Most corporate actions move the price by less than the band: DRI's 1,000 VND dividend
# on 14,900 is 6.7%, on a ±15% board. The seam test above cannot see it, and the tail
# top-up that re-serves the last banked bar already adjusted writes the join itself.


def test_a_sub_band_rescale_on_the_overlap_forces_a_full_refetch(temp_db, monkeypatch,
                                                                caplog):
    """DRI, 2026-09-22: the re-served 09-21 bar came back ×0.93 over 09-18 left on the
    old scale, and the pattern engine drew a confirmed Double Top across the join."""
    _pin_today(monkeypatch, _WED)
    _bank("DRI", _rows_between(_WED - timedelta(days=40), _WED - timedelta(days=1)),
          board={"ceiling": 115.0, "floor": 85.0, "ref_price": 100.0})
    src = _RangeSrc(_seamed(_WED - timedelta(days=60), _WED,
                            scale=0.93, at="1900-01-01"))   # source is fully adjusted
    vmd.set_sources([src])

    with caplog.at_level("WARNING"):
        rows = adapter.get_ohlcv("DRI", lookback_days=30, ttl_s=0)

    assert len(src.calls) == 2, "the tail read, then the repair"
    assert src.calls[-1][0] == (_WED - timedelta(days=40 + adapter.SEAM_REPAIR_LEAD_DAYS)
                                ).isoformat(), "the repair reaches every candle held"
    assert "rescaled" in caplog.text
    closes = [r["close"] for r in rows]
    assert max(closes) / min(closes) < 1.01, "one scale after the repair, not two"
    with closing(vmd.connect()) as conn:
        got = store.rescaled_symbols(conn)
    assert set(got) == {"DRI"}, (
        "the repair must reach the ledger, or the levels priced on the old scale are "
        "never voided")


def test_a_partial_bar_settling_is_not_a_rescale(temp_db, monkeypatch):
    """The common re-read: the last bar was banked mid-session and the source now serves
    the settled day. High, low and close move; the open does not — so no refetch."""
    _pin_today(monkeypatch, _WED)
    banked = _rows_between(_WED - timedelta(days=40), _WED - timedelta(days=1))
    banked[-1].update(high=100.5, low=99.5, close=100.2)
    _bank("HPG", banked, board={"ceiling": 107.0, "floor": 93.0, "ref_price": 100.0})
    src = _RangeSrc(_rows_between(_WED - timedelta(days=60), _WED))
    vmd.set_sources([src])

    adapter.get_ohlcv("HPG", lookback_days=30, ttl_s=0)
    assert len(src.calls) == 1, "a settling bar is not a corporate action"
    with closing(vmd.connect()) as conn:
        assert store.seams_repaired(conn, "HPG") == set()


def test_find_rescale_needs_all_four_prices_to_move_together(temp_db):
    _bank("X", _rows_between(_WED - timedelta(days=3), _WED))
    day = _WED.isoformat()
    scaled = {"date": day, "open": 93.0, "high": 93.93, "low": 92.07, "close": 93.0}
    with closing(vmd.connect()) as conn:
        got = store.find_rescale(conn, "X", [scaled])
        assert got and got[0] == day and abs(got[1] - 0.93) < 1e-3
        # A real move on the day: close alone off by 7% is trading, not adjustment.
        moved = {"date": day, "open": 100.0, "high": 101.0, "low": 92.0, "close": 93.0}
        assert store.find_rescale(conn, "X", [moved]) is None
        # A bar never banked has nothing to compare against.
        new = {**scaled, "date": (_WED + timedelta(days=1)).isoformat()}
        assert store.find_rescale(conn, "X", [new]) is None


def test_rescan_repairs_a_rescale_the_store_already_absorbed(temp_db, monkeypatch):
    """The four symbols the tail check came too late for: the join is already banked,
    old scale behind new. The rescan finds it, repairs once, and dates the ledger at the
    join — the first banked bar on the new scale, which is what `seam_void` measures."""
    _pin_today(monkeypatch, _WED)
    join = (_WED - timedelta(days=5)).isoformat()
    _bank("ELC", _seamed(_WED - timedelta(days=40), _WED - timedelta(days=1),
                         scale=0.93, at=join),
          board={"ceiling": 115.0, "floor": 85.0, "ref_price": 100.0})
    src = _RangeSrc(_seamed(_WED - timedelta(days=60), _WED, scale=0.93, at="1900-01-01"))
    vmd.set_sources([src])

    assert adapter.rescan_rescale("ELC") == join
    assert src.calls[-1][0] == (_WED - timedelta(days=40 + adapter.SEAM_REPAIR_LEAD_DAYS)
                                ).isoformat()
    with closing(vmd.connect()) as conn:
        closes = [r["close"] for r in store.get_ohlcv_range(
            conn, "ELC", "2000-01-01", _WED.isoformat())]
        assert max(closes) / min(closes) < 1.01
        assert store.rescaled_symbols(conn)["ELC"]["seam_date"] == join
    n = len(src.calls)
    assert adapter.rescan_rescale("ELC") is None, "repaired — the window now agrees"
    assert len(src.calls) == n + 1, "one read to say so, no second repair"


def test_rescan_repairs_again_when_an_earlier_repair_did_not_take(temp_db, monkeypatch):
    """N5 (VPI): a repair that refetched before the source had adjusted re-banked the
    old scale and still wrote the ledger. The store disagreeing with the source now
    outranks that entry, at most once a day."""
    _pin_today(monkeypatch, _WED)
    join = (_WED - timedelta(days=5)).isoformat()
    _bank("ELC", _seamed(_WED - timedelta(days=40), _WED - timedelta(days=1),
                         scale=0.93, at=join),
          board={"ceiling": 115.0, "floor": 85.0, "ref_price": 100.0})
    src = _RangeSrc(_seamed(_WED - timedelta(days=60), _WED, scale=0.93, at="1900-01-01"))
    vmd.set_sources([src])
    with closing(vmd.connect()) as conn:
        conn.execute("INSERT INTO md_ohlcv_seams(symbol, seam_date, band, repaired_at) "
                     "VALUES ('ELC', ?, 0.15, '2000-01-01T02:02:00+00:00')", (join,))
        conn.commit()

    assert adapter.rescan_rescale("ELC") == join
    with closing(vmd.connect()) as conn:
        closes = [r["close"] for r in store.get_ohlcv_range(
            conn, "ELC", "2000-01-01", _WED.isoformat())]
        assert max(closes) / min(closes) < 1.01


def test_rescan_repairs_a_seam_at_most_once_a_day(temp_db, monkeypatch):
    """A source serving both scales must not buy a full refetch on every call."""
    _pin_today(monkeypatch, _WED)
    join = (_WED - timedelta(days=5)).isoformat()
    _bank("ELC", _seamed(_WED - timedelta(days=40), _WED - timedelta(days=1),
                         scale=0.93, at=join),
          board={"ceiling": 115.0, "floor": 85.0, "ref_price": 100.0})
    src = _RangeSrc(_seamed(_WED - timedelta(days=60), _WED, scale=0.93, at="1900-01-01"))
    vmd.set_sources([src])
    with closing(vmd.connect()) as conn:
        store.mark_seams_repaired(conn, "ELC", [join], 0.15)       # stamped now

    assert adapter.rescan_rescale("ELC") is None
    assert len(src.calls) == 1, "the window read only, no repair refetch"


def test_rescan_keeps_the_withheld_marker_of_its_own_refetch(temp_db, monkeypatch):
    """A rescan's repair refetch reaches today too. If the feed is still serving today's
    bar truncated, the store withholds it — and the fetch record must say so, or it
    wipes a post-close catch-up's retry marker and the TTL serves yesterday till ~20:00."""
    today = datetime.now(ICT).date()
    _pin_today(monkeypatch, today)
    join = (today - timedelta(days=5)).isoformat()
    _bank("ELC", _seamed(today - timedelta(days=40), today - timedelta(days=1),
                         scale=0.93, at=join),
          board={"ceiling": 115.0, "floor": 85.0, "ref_price": 100.0})
    rows = _seamed(today - timedelta(days=60), today, scale=0.93, at="1900-01-01")
    rows[-1]["volume"] = 5                                   # today's bar, truncated
    vmd.set_sources([_RangeSrc(rows)])

    assert adapter.rescan_rescale("ELC") == join
    with closing(vmd.connect()) as conn:
        assert store.meta_fetched_at(conn, "ELC", "ohlcv")[1] == today.isoformat()
        assert store.ohlcv_bounds(conn, "ELC")[1] < today.isoformat()


def test_no_ledger_means_nothing_to_report(temp_db):
    """A store that has never repaired anything must answer empty, not scan."""
    _bank("QUIET", _rows_between(_WED - timedelta(days=10), _WED))
    with closing(vmd.connect()) as conn:
        assert store.rescaled_symbols(conn) == {}


def test_ohlcv_degrades_to_banked_candles_when_every_source_is_down(temp_db, monkeypatch,
                                                                   caplog):
    """A warm store outlives an outage. Raising here would fail a whole pipeline pass —
    patterns, RS, backtests all read candles — over a tail one session short."""
    src = _RangeSrc(_rows_between(_WED - timedelta(days=60), _WED))
    vmd.set_sources([src])
    _pin_today(monkeypatch, _WED)
    adapter.get_ohlcv("HPG", lookback_days=30)

    vmd.set_sources([_Src("down", raises=SourceUnavailable("ConnectionError"))])
    _pin_today(monkeypatch, _WED + timedelta(days=1))
    with caplog.at_level(logging.WARNING):
        rows = adapter.get_ohlcv("HPG", lookback_days=30, ttl_s=0)
    assert len(rows) > 15
    assert "unavailable" in caplog.text

    # Freshness must not have been re-stamped on the strength of a failure, or the TTL
    # would sit the next call out too and the outage would outlive itself. The meta row
    # still names the last source that actually answered.
    with vmd.connect() as conn:
        meta = conn.execute("SELECT source FROM md_fetch_meta "
                            "WHERE symbol='HPG' AND kind='ohlcv'").fetchone()
    assert meta["source"] == "range", "a failed fetch must not count as a fresh one"


def test_ohlcv_raises_when_every_source_is_down_and_nothing_is_banked(temp_db, monkeypatch):
    """The one case with nothing to serve: "nobody answered" must not read as "no data"."""
    vmd.set_sources([_Src("down", raises=SourceUnavailable("ConnectionError"))])
    _pin_today(monkeypatch, _WED)
    with pytest.raises(SourceUnavailable):
        adapter.get_ohlcv("HPG", lookback_days=30)


# ── DA-U-01: get_index_live — uncached, and never stored ─────────────────────

class _LiveSrc(DataSource):
    def __init__(self, name, *, row=None, raises=None):
        self.name, self._row, self._raises = name, row, raises
        self.calls = 0

    def get_index_live(self, symbol):
        self.calls += 1
        if self._raises:
            raise self._raises
        return self._row


def _stored_candles(symbol: str) -> int:
    conn = vmd.connect()
    try:
        return conn.execute("SELECT COUNT(*) FROM md_ohlcv WHERE symbol = ?",
                            (symbol,)).fetchone()[0]
    finally:
        conn.close()


def test_index_live_is_never_written_to_the_store(temp_db):
    """The invariant this guards: `md_ohlcv` holds settled candles only. A half-formed
    bar written here later changes underneath every consumer that read it — moving
    averages, pattern geometry, backtests — and nothing downstream can detect that."""
    src = _LiveSrc("live", row={"date": date.today().isoformat(), "open": 1290.0,
                                "high": 1301.0, "low": 1288.0, "close": 1299.0,
                                "volume": 5.1e8})
    vmd.set_sources([src])

    assert adapter.get_index_live("VNINDEX")["close"] == 1299.0
    assert _stored_candles("VNINDEX") == 0


def test_index_live_is_uncached(temp_db):
    src = _LiveSrc("live", row={"date": "2026-07-15", "close": 1299.0})
    vmd.set_sources([src])
    adapter.get_index_live("VNINDEX")
    adapter.get_index_live("VNINDEX")
    assert src.calls == 2, "a live quote served from a cache is not a live quote"


def test_index_live_degrades_to_none_when_no_source_answers(temp_db):
    """Unavailability here must not raise: the daily candles are still a truthful (if
    stale) answer, and failing the whole page to avoid showing them is the worse trade."""
    for src in (_LiveSrc("down", raises=SourceUnavailable("ConnectionError")),
                _LiveSrc("abstains", raises=NotSupported("get_index_live"))):
        vmd.set_sources([src])
        assert adapter.get_index_live("VNINDEX") is None


def test_index_live_normalizes_empty_and_blank(temp_db):
    vmd.set_sources([_LiveSrc("empty", row={})])
    assert adapter.get_index_live("VNINDEX") is None      # {} is not a candle

    src = _LiveSrc("unused", row={"close": 1.0})
    vmd.set_sources([src])
    assert adapter.get_index_live("  ") is None
    assert src.calls == 0


class _BoardSrc(DataSource):
    def __init__(self, name, *, board=None, raises=None):
        self.name, self._board, self._raises = name, board, raises

    def get_board(self, symbols):
        if self._raises:
            raise self._raises
        return {s: dict(self._board or {}) for s in symbols}


def test_board_degrades_to_the_last_stored_snapshot(temp_db):
    """The bug this guards (2026-07-29): one ConnectionError inside `price_board` blanked
    the whole market page for a 15-minute job cycle — no foreign flow, no traded value,
    every quote back at yesterday's close. Stale board > no board."""
    row = {"foreign_net_value": 1.4e11, "close": 21650.0, "ref_price": 21000.0,
           "traded_value": 7.7e11, "traded_volume": 3.6e7,
           "ceiling": 22470.0, "floor": 19530.0,
           "foreign_buy_value": 1.9e11, "foreign_sell_value": 5.0e10}
    vmd.set_sources([_BoardSrc("live", board=row)])
    assert adapter.get_board(["HPG"])["HPG"]["foreign_net_value"] == 1.4e11

    # Source goes down and the stored snapshot is past its 15-min freshness TTL.
    vmd.set_sources([_BoardSrc("down", raises=SourceUnavailable("ConnectionError"))])
    served = adapter.get_board(["HPG"], ttl_s=0)
    assert served["HPG"]["foreign_net_value"] == 1.4e11
    assert served["HPG"]["close"] == 21650.0

    # …but only within the staleness bound; past it the caller gets nothing and falls
    # back to candles knowingly, rather than being handed a day-old board as live.
    assert adapter.get_board(["HPG"], ttl_s=0, stale_ttl_s=0) == {}


def test_board_entries_carry_when_they_were_read(temp_db):
    """A board answer can be the cached or stale snapshot, not a read made for this call,
    so each entry says when it was read — and a cache hit reports the original read, not
    the moment it was served (2026-09-18: a 12:56 VPB row banked as the session's flow
    read positive while the live board at 13:27 read −21.5 bn, and nothing said which
    moment either number described)."""
    vmd.set_sources([_BoardSrc("live", board={"foreign_net_value": 1.0})])
    first = adapter.get_board(["HPG"])["HPG"]["read_at"]
    assert datetime.fromisoformat(first).tzinfo is not None

    vmd.set_sources([_BoardSrc("down", raises=SourceUnavailable("ConnectionError"))])
    assert adapter.get_board(["HPG"])["HPG"]["read_at"] == first            # cache hit
    assert adapter.get_board(["HPG"], ttl_s=0)["HPG"]["read_at"] == first   # stale fallback


# ── DA-U-08: the statement archive ───────────────────────────────────────────

class _StmtSrc(DataSource):
    """A source with VCI's defining limitation: it answers with a fixed-width window
    of the most recent periods and nothing earlier, however often it is asked."""

    def __init__(self, window, *, kind="general", name="fake", labelled=True):
        self.name = name
        self.window = window          # {label: revenue}
        self.labelled = labelled      # False = a payload cached before the relabel
        self.calls = 0

    def get_statements(self, symbol, period="year"):
        self.calls += 1
        labels = sorted(self.window, reverse=True)
        out = {"kind": "general", "periods": labels,
               "statements": {"income": {"net_sales": dict(self.window)},
                              "balance": {}, "cashflow": {}},
               "ratio_extra": {"NPL (%)": {"2018": 0.02, labels[0]: 0.01}}}
        if self.labelled:
            out["ratio_labels"] = RATIO_LABELS
        return out


def test_statement_archive_outgrows_the_fixed_source_window(temp_db):
    """The bug this exists to prevent: md_statements is keyed (symbol, period), so a
    4-period payload *replaces* the previous four. Fetch every day for a year and the
    store still holds four — a quarterly series can never reach its own year-ago
    comparable, which is what forces a YoY question to be answered as QoQ."""
    src = _StmtSrc({"2025-Q3": 30, "2025-Q4": 40, "2026-Q1": 50, "2026-Q2": 60})
    vmd.set_sources([src])
    adapter.get_statements("DPR", "quarter")

    # Two quarters later the source has moved its window on and dropped the oldest two.
    src.window = {"2026-Q1": 50, "2026-Q2": 60, "2026-Q3": 70, "2026-Q4": 80}
    adapter.get_statements("DPR", "quarter", ttl_s=0)

    assert adapter.get_statements("DPR", "quarter")["periods"] == \
        ["2026-Q4", "2026-Q3", "2026-Q2", "2026-Q1"], "the cache still mirrors the source"

    history = adapter.get_statement_history("DPR", "quarter", ttl_s=1e9)
    assert history["periods"] == ["2026-Q4", "2026-Q3", "2026-Q2", "2026-Q1",
                                  "2025-Q4", "2025-Q3"]
    revenue = history["statements"]["income"]["net_sales"]
    assert revenue["2025-Q4"] == 40, "a period the source no longer serves survives"
    assert revenue["2026-Q4"] == 80
    # ...which is the whole point: Q4 now has the year-ago comparable it needs.
    assert revenue["2026-Q4"] / revenue["2025-Q4"] == 2.0


def test_statement_archive_prefers_the_restated_figure(temp_db):
    """An audited annual supersedes the provisional one it restates: same label, later
    fetch, new number. Banking must overwrite, not keep the first answer seen."""
    src = _StmtSrc({"2025": 100})
    vmd.set_sources([src])
    adapter.get_statements("DPR", "year")
    src.window = {"2025": 118}
    adapter.get_statements("DPR", "year", ttl_s=0)

    history = adapter.get_statement_history("DPR", "year", ttl_s=1e9)
    assert history["periods"] == ["2025"]
    assert history["statements"]["income"]["net_sales"]["2025"] == 118


def test_statement_archive_carries_live_ratio_extra_but_banks_none_of_it(temp_db):
    """ratio_extra arrives whole on every fetch — the source serves the full series back
    to 2018 — so archiving it per period would bank nothing the next fetch lacks."""
    vmd.set_sources([_StmtSrc({"2025": 100})])
    adapter.get_statements("DPR", "year")
    with closing(vmd.connect()) as conn:
        banked = json.loads(conn.execute(
            "SELECT payload FROM md_statement_periods WHERE symbol='DPR'").fetchone()[0])
    assert set(banked) == {"income"} and "ratio_extra" not in banked
    assert adapter.get_statement_history("DPR", "year", ttl_s=1e9)["ratio_extra"] == \
        {"NPL (%)": {"2018": 0.02, "2025": 0.01}}


def test_unlabelled_ratio_payload_reads_as_cannot_say(temp_db):
    """A payload cached before the ratios were keyed by the source's own labels carries
    2018 readings under this decade's labels. It must read as no ratios at all — on the
    fresh path, the cached path and the archive path alike — never as a number."""
    vmd.set_sources([_StmtSrc({"2025": 100}, labelled=False)])
    assert adapter.get_statements("DPR", "year")["ratio_extra"] == {}
    assert adapter.get_statements("DPR", "year")["ratio_extra"] == {}      # cached
    assert adapter.get_statement_history("DPR", "year", ttl_s=1e9)["ratio_extra"] == {}
    assert adapter.get_banked_statements("DPR", "year")["ratio_extra"] == {}
    # The statements themselves are untouched.
    assert adapter.get_statements("DPR", "year")["statements"]["income"]["net_sales"] == \
        {"2025": 100}


def test_labelled_ratios_needs_the_current_stamp():
    series = {"CASA Ratio": {"2026-Q2": 0.13}}
    assert adapter.labelled_ratios({"ratio_extra": series,
                                    "ratio_labels": RATIO_LABELS}) == series
    assert adapter.labelled_ratios({"ratio_extra": series}) == {}
    assert adapter.labelled_ratios({"ratio_extra": series, "ratio_labels": "old"}) == {}
    assert adapter.labelled_ratios({}) == {}


def _vci_ratio_frame():
    import pandas as pd
    return pd.DataFrame([
        # VCI's statistics-financial rows, oldest first, as the raw report returns them.
        {"yearReport": 2018, "quarter": 1, "ratioType": "RATIO_TTM", "npl": 0.035,
         "casaRatio": 0.11, "car": 0, "netInterestMargin": 0.09},
        {"yearReport": 2018, "quarter": 5, "ratioType": "RATIO_YEAR", "npl": 0.035,
         "casaRatio": 0.12, "car": 0.12, "netInterestMargin": 0.092},
        {"yearReport": 2025, "quarter": 5, "ratioType": "RATIO_YEAR", "npl": 0.033,
         "casaRatio": 0.1445, "car": 0.1435, "netInterestMargin": 0.0557},
        {"yearReport": 2026, "quarter": 2, "ratioType": "RATIO_TTM", "npl": 0.0328,
         "casaRatio": 0.1275, "car": 0, "netInterestMargin": 0.0524},
        {"yearReport": 2026, "quarter": 3, "ratioType": "OTHER", "npl": 0.5,
         "casaRatio": 0.5, "car": 0.5, "netInterestMargin": 0.5},
    ])


def test_vci_ratio_series_is_keyed_by_the_rows_own_period():
    """The fault this replaced: vnstock's ratio() keeps the *oldest* four rows and the
    old parse relabelled them with the newest periods, so every bank published 2018."""
    pytest.importorskip("pandas")
    from vn_market_data.sources.vci import VCISource
    s = VCISource._ratio_series(_vci_ratio_frame())
    assert s["NPL (%)"] == {"2018-Q1": 0.035, "2018": 0.035, "2025": 0.033,
                            "2026-Q2": 0.0328}
    assert s["Net Interest Margin"]["2026-Q2"] == 0.0524
    assert s["CASA Ratio"]["2025"] == 0.1445
    # VCI fills an unreported CAR with 0: absent, not zero.
    assert s["CAR"] == {"2018": 0.12, "2025": 0.1435}
    assert all("2026-Q3" not in v for v in s.values())      # unknown ratioType skipped


def test_vci_ratio_series_degrades_to_empty():
    from vn_market_data.sources.vci import VCISource
    assert VCISource._ratio_series(None) == {}
    assert VCISource._ratio_series(object()) == {}


def test_no_statements_banks_nothing(temp_db):
    """A negative cache records that a symbol has no statements. Absence is not a
    period, so it must not become an archive row that later reads as real."""
    class _None(DataSource):
        name = "fake"
        def get_statements(self, symbol, period="year"):
            return None

    vmd.set_sources([_None()])
    assert adapter.get_statements("XXX", "year") is None
    assert adapter.banked_periods("XXX", "year") == []
    assert adapter.get_statement_history("XXX", "year") is None


def test_banked_periods_never_touches_a_source(temp_db):
    """The quarterly gate asks this before deciding to spend a fetch, so it has to be
    answerable from the store alone — otherwise the gate costs what it saves."""
    src = _StmtSrc({"2026-Q1": 50, "2026-Q2": 60})
    vmd.set_sources([src])
    adapter.get_statements("DPR", "quarter")
    calls = src.calls
    assert adapter.banked_periods("DPR", "quarter") == ["2026-Q2", "2026-Q1"]
    assert adapter.banked_periods("DPR", "year") == []
    assert src.calls == calls


def test_a_live_cache_is_backfilled_into_the_archive_once(temp_db):
    """A store that has been running for months is already holding periods the source
    may have stopped serving, and the next TTL expiry overwrites them. They cost no
    fetch to keep, so init_schema banks them — once per (symbol, period), never
    re-deriving a pair the normal path has already banked."""
    payload = {"kind": "general", "periods": ["2023", "2022"],
               "statements": {"income": {"net_sales": {"2023": 10, "2022": 9}},
                              "balance": {}, "cashflow": {}},
               "ratio_extra": {}}
    with closing(vmd.connect()) as conn:
        conn.execute("INSERT INTO md_statements(symbol, period, payload, source, fetched_at) "
                     "VALUES ('DPR','year',?, 'vci', '2026-01-01T00:00:00Z')",
                     (json.dumps(payload),))
        conn.execute("INSERT INTO md_statements(symbol, period, payload, source, fetched_at) "
                     "VALUES ('XXX','year', NULL, 'vci', '2026-01-01T00:00:00Z')")
        conn.commit()
        vmd.init_schema(conn)
        assert store.banked_labels(conn, "DPR", "year") == ["2023", "2022"]
        assert store.banked_labels(conn, "XXX", "year") == [], \
            "a negative cache carries no period to bank"

        # A pair the normal path has since moved on is not dragged back to the cache's
        # copy on the next startup.
        conn.execute("UPDATE md_statement_periods SET payload=? WHERE label='2023'",
                     (json.dumps({"income": {"net_sales": 11}}),))
        conn.commit()
        vmd.init_schema(conn)
        assert json.loads(conn.execute(
            "SELECT payload FROM md_statement_periods WHERE label='2023'"
        ).fetchone()[0])["income"]["net_sales"] == 11


# ── DA-U-04: unsettled same-session bars ─────────────────────────────────────
# A daily feed that answers with the day still in progress returns a bar truncated to
# the first minutes of trading. `upsert_ohlcv` withholds those rather than banking a
# figure that later changes underneath every consumer of the store.

def _settled_rows(n, *, volume=1_000_000, end_offset=1):
    """`n` bars ending `end_offset` days before today — settled history by definition."""
    first = date.today() - timedelta(days=n + end_offset - 1)
    return [{"date": (first + timedelta(days=i)).isoformat(),
             "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.0,
             "volume": float(volume)} for i in range(n)]


def _today_row(volume, close=100.0):
    return {"date": date.today().isoformat(), "open": 100.0, "high": 100.5,
            "low": 99.5, "close": close, "volume": float(volume)}


def _banked(symbol):
    with closing(vmd.connect()) as conn:
        return {r["date"]: r["volume"] for r in conn.execute(
            "SELECT date, volume FROM md_ohlcv WHERE symbol=?", (symbol,))}


def test_upsert_withholds_a_truncated_bar_for_today(temp_db, caplog):
    """The 2026-08-28 shape: ~1% of the symbol's own recent volume, dated today."""
    rows = _settled_rows(20) + [_today_row(9_000)]     # 0.9% of a 1,000,000 median
    with closing(vmd.connect()) as conn, caplog.at_level(logging.WARNING):
        store.upsert_ohlcv(conn, "SHB", rows, "fake")

    banked = _banked("SHB")
    assert date.today().isoformat() not in banked, "a truncated bar must not be banked"
    assert len(banked) == 20, "the settled history around it is banked as given"
    assert "unsettled" in caplog.text.lower()


def test_upsert_banks_a_normal_bar_for_today(temp_db):
    """The guard is about truncation, not about today — a full session banks normally."""
    rows = _settled_rows(20) + [_today_row(800_000)]
    with closing(vmd.connect()) as conn:
        store.upsert_ohlcv(conn, "HPG", rows, "fake")
    assert _banked("HPG")[date.today().isoformat()] == 800_000


def test_upsert_banks_a_thin_settled_session_and_reports_it(temp_db):
    """Low volume alone proves nothing: 0.8% of real settled bars in the live store sit
    below the threshold, and those are true. So a settled bar is never withheld — it is
    banked, and handed back for a second source to rule on."""
    thin = _settled_rows(1, volume=9_000, end_offset=1)     # yesterday, 0.9% of median
    rows = _settled_rows(20, end_offset=2) + thin
    with closing(vmd.connect()) as conn:
        suspect = store.upsert_ohlcv(conn, "SCR", rows, "fake")
    assert _banked("SCR")[thin[0]["date"]] == 9_000, "a hole is worse than a thin bar"
    assert suspect == [thin[0]["date"]]


def test_upsert_banks_todays_bar_when_there_is_no_baseline(temp_db):
    """A new listing has nothing to be measured against, so it is banked as given —
    the check states what it cannot see rather than guessing."""
    rows = _settled_rows(4) + [_today_row(9_000)]
    with closing(vmd.connect()) as conn:
        store.upsert_ohlcv(conn, "NEWCO", rows, "fake")
    assert _banked("NEWCO")[date.today().isoformat()] == 9_000


def test_a_withheld_bar_is_banked_once_the_feed_settles_it(temp_db):
    """Withholding costs latency, never data: the next pass carries the real bar."""
    today = date.today().isoformat()
    with closing(vmd.connect()) as conn:
        store.upsert_ohlcv(conn, "VIX", _settled_rows(20) + [_today_row(9_000)], "fake")
        assert today not in _banked("VIX")
        store.upsert_ohlcv(conn, "VIX", [_today_row(1_200_000)], "fake")
    assert _banked("VIX")[today] == 1_200_000


def test_a_cold_backfill_measures_against_its_own_rows(temp_db):
    """Nothing is banked yet on a first fetch, so the baseline has to come from the
    payload itself — otherwise the very first write is the one that gets through."""
    src = _Src("fake", ohlcv=_settled_rows(30) + [_today_row(9_000)])
    vmd.set_sources([src])
    served = adapter.get_ohlcv("LPB", lookback_days=60)
    assert date.today().isoformat() not in {r["date"] for r in served}
    assert len(served) == 30


# ── DA-U-05: a settled fragment is adjudicated against a second source ───────
# The 2026-08-28 feed never revised the bar it truncated: three days on it still served
# SHB at 8.2M against the 77.7M really traded. So withholding today's bar only defers a
# fragment — the date stops being today and it banks anyway. A settled bar this thin has
# to be ruled on by a second reading, and the ruling has to hold against the source that
# got it wrong.

def _yesterday():
    return (date.today() - timedelta(days=1)).isoformat()


def _fragment_history(fragment_volume=9_000, real_volume=1_600_000):
    """20 normal sessions, then yesterday served two ways: a fragment and the real bar."""
    history = _settled_rows(20, end_offset=2)
    def bar(volume, close):
        return {"date": _yesterday(), "open": 100.0, "high": 100.5, "low": 99.5,
                "close": close, "volume": float(volume)}
    return history, bar(fragment_volume, 100.0), bar(real_volume, 103.0)


def _ruling(symbol, day):
    with closing(vmd.connect()) as conn:
        row = conn.execute("SELECT * FROM md_ohlcv_stubs WHERE symbol=? AND date=?",
                           (symbol, day)).fetchone()
    return dict(row) if row else None


def test_a_settled_fragment_is_re_banked_from_the_second_source(temp_db):
    """The primary's fragment is banked (a hole would be worse), then overwritten by the
    source that has the real session — close and all, not just the volume."""
    history, fragment, real = _fragment_history()
    primary = _Src("primary", ohlcv=history + [fragment])
    backup = _Src("backup", ohlcv=[real])
    vmd.set_sources([primary, backup])

    adapter.get_ohlcv("SHB", lookback_days=60)

    with closing(vmd.connect()) as conn:
        banked = conn.execute("SELECT volume, close, source FROM md_ohlcv "
                              "WHERE symbol='SHB' AND date=?", (_yesterday(),)).fetchone()
    assert banked["volume"] == 1_600_000 and banked["close"] == 103.0
    assert banked["source"] == "backup", "provenance follows the reading that was kept"
    assert _ruling("SHB", _yesterday())["verdict"] == "fragment"


def test_the_source_that_served_a_fragment_is_refused_that_date(temp_db):
    """The whole point of recording the ruling. The primary keeps serving its fragment
    on every 4-hourly top-up; without the refusal each one overwrites the repair."""
    history, fragment, real = _fragment_history()
    vmd.set_sources([_Src("primary", ohlcv=history + [fragment]),
                     _Src("backup", ohlcv=[real])])
    adapter.get_ohlcv("SHB", lookback_days=60)

    with closing(vmd.connect()) as conn:
        store.upsert_ohlcv(conn, "SHB", [fragment], "primary")
        assert _banked("SHB")[_yesterday()] == 1_600_000, "the repair must survive"
        # Only that source, and only that date: anyone else may still write it.
        store.upsert_ohlcv(conn, "SHB", [dict(fragment, volume=1_700_000.0)], "backup")
    assert _banked("SHB")[_yesterday()] == 1_700_000


def test_a_thin_day_the_second_source_confirms_is_kept_and_not_re_asked(temp_db):
    """Two feeds agreeing on a thin session means the session was thin. The bar stands,
    and the ruling is banked so the same quiet symbol is not re-checked every TTL."""
    history, fragment, _ = _fragment_history()
    backup = _Src("backup", ohlcv=[dict(fragment)])
    vmd.set_sources([_Src("primary", ohlcv=history + [fragment]), backup])

    adapter.get_ohlcv("SCR", lookback_days=60)
    assert _banked("SCR")[_yesterday()] == 9_000
    assert _ruling("SCR", _yesterday())["verdict"] == "true"

    asked = backup.calls
    with closing(vmd.connect()) as conn:
        assert store.upsert_ohlcv(conn, "SCR", [fragment], "primary") == [], \
            "a date already ruled on is never offered again"
    assert backup.calls == asked


def test_an_old_thin_bar_is_never_adjudicated(temp_db):
    """A backfill spans years and 0.8% of its bars are legitimately this thin. Paying a
    metered call for each would cost more than the backfill. Old history belongs to the
    seam detector, not to this check."""
    old_thin = _settled_rows(1, volume=9_000, end_offset=40)
    rows = _settled_rows(20, end_offset=41) + old_thin
    backup = _Src("backup", ohlcv=[])
    vmd.set_sources([_Src("primary", ohlcv=rows), backup])

    adapter.get_ohlcv("DPM", lookback_days=200)
    assert _banked("DPM")[old_thin[0]["date"]] == 9_000
    assert backup.calls == 0 and _ruling("DPM", old_thin[0]["date"]) is None


def test_a_rescale_drops_the_rulings_it_would_strand(temp_db):
    """A refusal is a statement about one bar on one scale. After a corporate action the
    whole series is refetched on a new one, and keeping the refusal would hold that date
    back on the old scale — a seam of our own making."""
    history, fragment, real = _fragment_history()
    vmd.set_sources([_Src("primary", ohlcv=history + [fragment]),
                     _Src("backup", ohlcv=[real])])
    adapter.get_ohlcv("VIX", lookback_days=60)

    with closing(vmd.connect()) as conn:
        store.clear_stub_checks(conn, "VIX")
        assert store.fragment_dates(conn, "VIX") == {}
        store.upsert_ohlcv(conn, "VIX", [fragment], "primary")
    assert _banked("VIX")[_yesterday()] == 9_000, "re-adjudicated, not refused outright"


# ── DA-U-09: the source-call recorder ────────────────────────────────────────

class _QuotaSrc(DataSource):
    """Statements from a source with a quota: it answers `budget` requests, then trips."""

    def __init__(self, budget, *, name="vci"):
        self.name = name
        self.budget = budget

    def get_statements(self, symbol, period="year"):
        if self.budget <= 0:
            raise SourceUnavailable("quota")
        self.budget -= 1
        return {"kind": "general", "periods": ["2025"],
                "statements": {"income": {"net_sales": {"2025": 1}},
                               "balance": {}, "cashflow": {}}, "ratio_extra": {}}


def test_recorder_counts_requests_not_reads(temp_db):
    """The point of it: the store answers a repeat read, and that read costs the source
    nothing, so it must not be on the bill."""
    vmd.set_sources([_QuotaSrc(10)])
    with vmd.record_source_calls() as calls:
        adapter.get_statements("HPG", "year")
        adapter.get_statements("HPG", "year")          # cache hit — no request
        adapter.get_statements("VNM", "year")
    assert calls == [vmd.SourceCall("vci", "get_statements", "ok", "HPG"),
                     vmd.SourceCall("vci", "get_statements", "ok", "VNM")]


def test_recorder_marks_where_the_quota_tripped(temp_db):
    """A trip is on the record as the request it happened on, not inferred from a pass
    that came up short."""
    vmd.set_sources([_QuotaSrc(2)])
    with vmd.record_source_calls() as calls:
        for s in ("AAA", "BBB", "CCC"):
            try:
                adapter.get_statements(s, "year")
            except SourceUnavailable:
                break
    assert [(c.symbol, c.outcome) for c in calls] == [
        ("AAA", "ok"), ("BBB", "ok"), ("CCC", "unavailable")]


def test_recorder_sees_each_source_the_chain_tried(temp_db):
    """A fallthrough is two requests to two sources — the head one spent its quota too."""
    vmd.set_sources([_QuotaSrc(0, name="head"), _QuotaSrc(5, name="tail")])
    with vmd.record_source_calls() as calls:
        adapter.get_statements("HPG", "year")
    assert [(c.source, c.outcome) for c in calls] == [("head", "unavailable"),
                                                      ("tail", "ok")]


def test_recorder_ignores_not_supported_and_records_nothing_when_closed(restore_sources):
    """A capability refusal is decided before any request; and with no recorder open the
    hot path must not accumulate anything."""
    vmd.set_sources([_Src("a", raises=NotSupported("get_ohlcv")), _Src("b", ohlcv=[])])
    adapter._query("get_ohlcv", "HPG", "s", "e")      # no recorder: nowhere to go
    with vmd.record_source_calls() as calls:
        adapter._query("get_ohlcv", "HPG", "s", "e")
    assert [(c.source, c.outcome) for c in calls] == [("b", "ok")]


def test_recorders_nest_and_the_outer_keeps_the_total(restore_sources):
    vmd.set_sources([_Src("a", ohlcv=[])])
    with vmd.record_source_calls() as outer:
        adapter._query("get_ohlcv", "AAA", "s", "e")
        with vmd.record_source_calls() as inner:
            adapter._query("get_ohlcv", "BBB", "s", "e")
    assert [c.symbol for c in inner] == ["BBB"]
    assert [c.symbol for c in outer] == ["AAA", "BBB"]


def test_recorder_follows_the_call_into_a_worker_thread(restore_sources):
    """The host runs this blocking layer under `asyncio.to_thread`; a recorder opened in
    the coroutine has to see the worker's requests or it undercounts every real pass."""
    import asyncio
    vmd.set_sources([_Src("a", ohlcv=[])])

    async def _pass():
        with vmd.record_source_calls() as calls:
            await asyncio.gather(*(asyncio.to_thread(adapter._query, "get_ohlcv", s, "s", "e")
                                   for s in ("AAA", "BBB")))
        return calls

    assert sorted(c.symbol for c in asyncio.run(_pass())) == ["AAA", "BBB"]


# ── announced-but-undated events, through the store ──────────────────────────

def test_store_derives_status_and_keeps_an_undated_row(temp_db):
    """A cached announcement survives the round trip and comes back labelled.

    `status` is derived on read rather than stored, for two reasons. A column would be
    free to disagree with the column it describes — `status="confirmed"` beside an empty
    `ex_date` is a state nothing could act on. And deriving it hands the label to rows
    banked before the field existed and to any source that never emits one, which is
    what SM's card and import job read to decide whether an event has a calendar
    position at all.
    """
    from vn_market_data import store

    with vmd.connect() as conn:
        store.replace_events(conn, "DRI", [
            {"type": "CASH", "ex_date": "2026-09-22", "record_date": "2026-09-23",
             "pay_date": "2026-10-15", "announced_date": "2026-08-01",
             "value_per_share": 1000.0, "ratio": 0.0, "title": "dated",
             "event_code": "DIV"},
            {"type": "STOCK", "ex_date": "", "record_date": "", "pay_date": "",
             "announced_date": "2026-09-11", "value_per_share": 0.0, "ratio": 0.2,
             "title": "announced", "event_code": "ISS"},
        ], "fake")
        rows = store.get_events(conn, "DRI")

    # Undated first: `ORDER BY ex_date` sorts "" ahead of every real date, which is the
    # order a reader wants anyway — an announcement needs a lookup, a date does not.
    assert [r["status"] for r in rows] == ["announced", "confirmed"]
    assert rows[0]["ex_date"] == "" and rows[0]["announced_date"] == "2026-09-11"
    assert rows[1]["ex_date"] == "2026-09-22"


def test_store_reports_a_missing_announcement_date_as_empty_not_null(temp_db):
    """Rows banked before `announced_date` existed read `""`, like every other absent
    date in this shape. A None would reach the frontend as `null` and compare against
    an ISO window without raising — `null >= "2026-08-17"` is just false in JS, so the
    row would vanish silently rather than be recognised as unaged."""
    from vn_market_data import store

    with vmd.connect() as conn:
        store.replace_events(conn, "SHB", [
            {"type": "STOCK", "ex_date": "", "ratio": 0.1, "title": "no date at all",
             "event_code": "ISS"}], "fake")
        [row] = store.get_events(conn, "SHB")
    assert row["announced_date"] == "" and row["status"] == "announced"


# ── Board depth + VWAP (todo 42, display only) ───────────────────────────────

def _serve_board(monkeypatch, frame):
    import sys, types

    class _Trading:
        def __init__(self, source):
            pass

        def price_board(self, symbols):
            return frame

    mod = types.ModuleType("vnstock")
    mod.Trading = _Trading
    monkeypatch.setitem(sys.modules, "vnstock", mod)


def test_vci_board_reads_vwap_and_three_levels_per_side(monkeypatch):
    """Shapes measured on the live board 2026-09-24: HOSE sends three levels, HNX/UPCOM
    ten (cut to three so depth means one thing everywhere); an empty level is NaN and
    ends the side; `avg_match_price` is the session VWAP in full VND, 0 before any match."""
    pd = pytest.importorskip("pandas")
    from vn_market_data.sources.vci import VCISource

    nan = float("nan")
    cols = pd.MultiIndex.from_tuples(
        [("listing", "symbol"), ("match", "match_price"), ("match", "avg_match_price")]
        + [("bid_ask", f"{side}_{i}_{f}") for side in ("bid", "ask")
           for i in range(1, 5) for f in ("price", "volume")])
    rows = [
        # HNX-like: four levels served, three kept
        ["PVS", 33000, 33086.87,
         32900, 17100, 32800, 79300, 32700, 57900, 32600, 66500,
         33000, 7300, 33100, 20800, 33200, 35400, 33300, 32400],
        # Limit-up lock: no asks at all; bids stop at a hole; no match yet → no VWAP
        ["XYZ", 0, 0,
         10700, 500000, nan, nan, 10600, 900, nan, nan,
         nan, nan, nan, nan, nan, nan, nan, nan],
    ]
    _serve_board(monkeypatch, pd.DataFrame(rows, columns=cols))
    board = VCISource().get_board(["PVS", "XYZ"])

    assert board["PVS"]["vwap"] == 33086.87
    assert board["PVS"]["bids"] == [[32900, 17100], [32800, 79300], [32700, 57900]]
    assert board["PVS"]["asks"] == [[33000, 7300], [33100, 20800], [33200, 35400]]
    assert board["XYZ"]["vwap"] is None
    assert board["XYZ"]["bids"] == [[10700, 500000]]     # nothing past the hole
    assert board["XYZ"]["asks"] == []                    # empty side, not "not read"


def test_an_auction_price_as_text_reads_as_not_read_not_as_empty(monkeypatch):
    """During ATO/ATC a level-1 price can be the text "ATO"/"ATC". That side is
    unreadable, not empty: `[]` is what a limit lock looks like, and the close's
    snapshot banked from it would carry a book with no orders on one side."""
    pd = pytest.importorskip("pandas")
    from vn_market_data.sources.vci import VCISource

    cols = pd.MultiIndex.from_tuples(
        [("listing", "symbol"), ("match", "match_price"), ("match", "avg_match_price")]
        + [("bid_ask", f"{side}_{i}_{f}") for side in ("bid", "ask")
           for i in range(1, 4) for f in ("price", "volume")])
    rows = [["MBB", 19950, 19946.0,
             "ATC", 120000, 19900, 665900, 19850, 978400,
             19950, 298800, 20000, 431900, 20050, 140800]]
    _serve_board(monkeypatch, pd.DataFrame(rows, columns=cols, dtype=object))
    board = VCISource().get_board(["MBB"])

    assert board["MBB"]["bids"] is None
    assert board["MBB"]["asks"] == [[19950, 298800], [20000, 431900], [20050, 140800]]


def test_board_depth_round_trips_through_the_store(temp_db):
    vmd.set_sources([_BoardSrc("live", board={
        "close": 20000.0, "vwap": 19950.0,
        "bids": [[19950.0, 1000.0]], "asks": [[20000.0, 500.0], [20050.0, 200.0]]})])
    adapter.get_board(["MBB"])
    vmd.set_sources([_BoardSrc("down", raises=SourceUnavailable("ConnectionError"))])
    got = adapter.get_board(["MBB"])["MBB"]                     # cache hit, from the store
    assert got["vwap"] == 19950.0
    assert got["bids"] == [[19950.0, 1000.0]]
    assert got["asks"] == [[20000.0, 500.0], [20050.0, 200.0]]

    newest = vmd.newest_board("mbb")                             # any age, no source call
    assert newest["read_at"] == got["read_at"] and newest["vwap"] == 19950.0
    assert vmd.newest_board("NONE") is None


def test_a_board_without_depth_reads_as_not_read(temp_db):
    """A row banked before depth was kept (or by a source with none) must not come
    back as an empty book — `[]` means no resting orders, None means nobody looked."""
    vmd.set_sources([_BoardSrc("live", board={"close": 20000.0})])
    got = adapter.get_board(["MBB"])["MBB"]
    assert got["bids"] is None and got["asks"] is None and got["vwap"] is None
