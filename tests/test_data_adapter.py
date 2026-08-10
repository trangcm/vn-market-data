"""DA-U-01..03 — the vn_market_data adapter (Tier 0, no network). [G5]

The store-first cache, the head-first source fallback chain, and the source
contract — all exercised with a temp SQLite DB and fake sources, so nothing here
touches DNSE/VCI or the real /data/news.db.

These tests go with the package if it is ever split out, so they reach it only through
its public surface: ``set_sources`` and ``set_connection_factory``, never a monkeypatched
module global. ``test_package_boundary.py`` enforces the other half of that.
"""
import logging
import sqlite3
from datetime import date, timedelta

import pytest

import vn_market_data as vmd
from vn_market_data import adapter
from vn_market_data.sources.base import DataSource, NotSupported, SourceUnavailable
from vn_market_data.sources.registry import build_sources


# ── DA-U-02: registry (ordered chain; TCBS rejected) ─────────────────────────

def _expected_chain():
    """The default chain for *this* environment. VCI rides the optional `[vci]` extra,
    so it is present only where vnstock is — in the container, not necessarily on a
    contributor's laptop."""
    return ["dnse", "vci", "vndirect"] if vmd.vnstock_installed() else ["dnse", "vndirect"]


def test_registry_is_dnse_then_vci_then_vndirect():
    names = [s.name for s in build_sources()]
    assert names == _expected_chain()   # OHLCV primary, board, market turnover
    assert "tcbs" not in names          # TCBS public API is dead (rejected)


def test_registry_drops_vci_when_vnstock_is_absent(monkeypatch, caplog):
    """Without the extra the chain must still build — losing a capability, not raising.
    An ImportError escaping mid-fetch instead would look like a data outage."""
    monkeypatch.setattr("vn_market_data.sources.registry.vnstock_installed", lambda: False)
    with caplog.at_level("WARNING"):
        names = [s.name for s in build_sources()]
    assert names == ["dnse", "vndirect"]
    assert "vnstock" in caplog.text and "[vci]" in caplog.text   # says how to fix it


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
    assert [s.name for s in vmd.get_sources()] == _expected_chain()


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


# ── DA-U-01: store-first cache (temp DB, fake source) ────────────────────────

@pytest.fixture
def temp_db(tmp_path, restore_sources):
    """Hand the package a fresh temp SQLite DB the way a host application does — through
    the public connection factory, not a monkeypatched module global. Whatever factory
    was installed (market-intel installs its own on import) goes back on teardown."""
    path = tmp_path / "news.db"

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

    # Next weekday, meta stale: fetch again — but only from the last candle we hold,
    # not the whole 30-day window again.
    _pin_today(monkeypatch, _WED + timedelta(days=1))
    adapter.get_ohlcv("HPG", lookback_days=30, ttl_s=0)
    assert len(src.calls) == 2
    assert src.calls[1][0] == _WED.isoformat(), "top-up must start at max_d, not at start"


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
    assert src.calls[-1][0] == _WED.isoformat(), "top-up must start at max_d, not at start"

    # But a genuinely deeper request is still a miss.
    adapter.get_ohlcv("NEW", lookback_days=365)
    assert src.calls[-1][0] == (_WED + timedelta(days=1) - timedelta(days=365)).isoformat()


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
