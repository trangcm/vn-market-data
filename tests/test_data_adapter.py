"""DA-U-01..03 — the vn_market_data adapter (Tier 0, no network). [G5]

The store-first cache, the head-first source fallback chain, and the source
contract — all exercised with a temp SQLite DB and fake sources, so nothing here
touches DNSE/VCI or any real database.

These tests go with the package if it is ever split out, so they reach it only through
its public surface: ``set_sources`` and ``set_connection_factory``, never a monkeypatched
module global. ``test_package_boundary.py`` enforces the other half of that.
"""
import logging
import sqlite3
from contextlib import closing
from datetime import date, timedelta

import pytest

import vn_market_data as vmd
from vn_market_data import adapter, store
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
