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


# ── DA-U-08: the statement archive ───────────────────────────────────────────

class _StmtSrc(DataSource):
    """A source with VCI's defining limitation: it answers with a fixed-width window
    of the most recent periods and nothing earlier, however often it is asked."""

    def __init__(self, window, *, kind="general", name="fake"):
        self.name = name
        self.window = window          # {label: revenue}
        self.calls = 0

    def get_statements(self, symbol, period="year"):
        self.calls += 1
        labels = sorted(self.window, reverse=True)
        return {"kind": "general", "periods": labels,
                "statements": {"income": {"net_sales": dict(self.window)},
                               "balance": {}, "cashflow": {}},
                "ratio_extra": {"P/E": 1.0}}


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
    """ratio_extra is trailing-twelve-month, not a property of any labelled period, so
    archiving it per period would date-stamp a figure that has no date."""
    vmd.set_sources([_StmtSrc({"2025": 100})])
    adapter.get_statements("DPR", "year")
    with closing(vmd.connect()) as conn:
        banked = json.loads(conn.execute(
            "SELECT payload FROM md_statement_periods WHERE symbol='DPR'").fetchone()[0])
    assert set(banked) == {"income"} and "ratio_extra" not in banked
    assert adapter.get_statement_history("DPR", "year", ttl_s=1e9)["ratio_extra"] == {"P/E": 1.0}


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


def test_upsert_banks_a_thin_settled_session(temp_db):
    """Low volume alone proves nothing: 0.8% of real settled bars in the live store sit
    below the threshold, and those are true. Only *today* is ever withheld."""
    thin = _settled_rows(1, volume=9_000, end_offset=1)     # yesterday, 0.9% of median
    rows = _settled_rows(20, end_offset=2) + thin
    with closing(vmd.connect()) as conn:
        store.upsert_ohlcv(conn, "SCR", rows, "fake")
    assert _banked("SCR")[thin[0]["date"]] == 9_000


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
