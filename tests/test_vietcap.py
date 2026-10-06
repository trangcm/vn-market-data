"""The direct Vietcap source: the wire shapes it reads and what it does when they change.

The payloads below are cut down from live answers (2026-10-06) — the same endpoints
``VCISource`` reaches through ``vnstock``. What is pinned here is what ``vnstock`` used
to do for us on the way through, and what the contract in ``sources/base.py`` asks of a
source at each failure.

Offline: both HTTP helpers are substituted, so nothing here opens a socket.
"""
import json
from datetime import datetime, timezone

import httpx
import pytest

from vn_market_data.sources import http as vmd_http, vietcap
from vn_market_data.sources.base import (NotSupported, SourceUnavailable,
                                         SourceUnreachable, _take_units)
from vn_market_data.sources.vietcap import VietcapSource


def _ts(day: str) -> str:
    """Midnight UTC of *day* as the string of unix seconds the chart endpoint sends."""
    return str(int(datetime.fromisoformat(day).replace(tzinfo=timezone.utc).timestamp()))


class _Wire:
    """Answers each request by the first route whose fragment is in its URL."""

    def __init__(self, monkeypatch):
        self.routes: list[tuple[str, object, int]] = []
        self.calls: list[tuple[str, dict]] = []
        monkeypatch.setattr(vietcap, "get_capped", self._get)
        monkeypatch.setattr(vietcap, "post_capped", self._post)
        monkeypatch.setattr(vietcap._pacer, "_interval", 0.0)

    def on(self, fragment: str, answer, status: int = 200):
        self.routes.append((fragment, answer, status))
        return self

    def _answer(self, url, sent):
        self.calls.append((url, sent))
        for fragment, answer, status in self.routes:
            if fragment in url:
                if isinstance(answer, Exception):
                    raise answer
                if callable(answer):
                    answer = answer(sent)
                body = answer if isinstance(answer, bytes) else json.dumps(answer).encode()
                return status, body if status == 200 else b""
        raise AssertionError(f"unexpected request: {url}")

    def _get(self, url, *, params=None, **_):
        return self._answer(url, params or {})

    def _post(self, url, *, json=None, **_):
        return self._answer(url, json or {})


@pytest.fixture
def wire(monkeypatch):
    return _Wire(monkeypatch)


def _iq(data):
    return {"status": 200, "code": 0, "msg": "Success", "successful": True, "data": data}


# ── candles ──────────────────────────────────────────────────────────────────

def test_candles_are_full_dong_and_keep_to_the_window(wire):
    """``vnstock`` divides by 1000 and ``VCISource`` multiplies back; the wire is already
    full VND, so a scale here would be ×1000. It also hands back bars from before
    ``start`` — the endpoint counts back from ``to`` — which a source must not."""
    wire.on("gap-chart", [{"symbol": "FPT",
                           "t": [_ts("2026-09-30"), _ts("2026-10-01"), _ts("2026-10-02")],
                           "o": [61000, 62700.5, 62200], "h": [61500, 63200, 62800],
                           "l": [60900, 62100, 61900], "c": [61200, 62100, 61900],
                           "v": [100, 3199600, 2513400]}])
    rows = VietcapSource().get_ohlcv("fpt", "2026-10-01", "2026-10-02")
    assert rows == [
        {"date": "2026-10-01", "open": 62700.5, "high": 63200.0, "low": 62100.0,
         "close": 62100.0, "volume": 3199600.0},
        {"date": "2026-10-02", "open": 62200.0, "high": 62800.0, "low": 61900.0,
         "close": 61900.0, "volume": 2513400.0},
    ]
    sent = wire.calls[0][1]
    assert sent["symbols"] == ["FPT"] and sent["timeFrame"] == "ONE_DAY"
    assert sent["countBack"] == 3          # two weekdays in the window, plus one


def test_a_stock_bar_missing_a_price_is_dropped_but_an_index_bar_is_kept(wire):
    answer = [{"t": [_ts("2026-10-01"), _ts("2026-10-02")], "o": [None, 1650.1],
               "h": [None, 1660], "l": [None, 1640], "c": [1655.5, 1652.2], "v": [0, 9]}]
    wire.on("gap-chart", answer)
    src = VietcapSource()
    assert [r["date"] for r in src.get_ohlcv("FPT", "2026-10-01", "2026-10-02")] == ["2026-10-02"]
    index = src.get_ohlcv("VNINDEX", "2026-10-01", "2026-10-02", is_index=True)
    assert [(r["date"], r["close"]) for r in index] == [("2026-10-01", 1655.5), ("2026-10-02", 1652.2)]


@pytest.mark.parametrize("body", [[], [{}], [{"t": {"a": 1}, "c": "x"}], {"error": "x"},
                                  "a string", None, b"not json at all"])
def test_candles_survive_an_answer_of_the_wrong_shape(wire, body):
    """``[]`` is how an unknown symbol answers; anything else that is not candles is
    no-data too, never an exception in the caller's stack."""
    wire.on("gap-chart", body)
    assert VietcapSource().get_ohlcv("NOSUCH", "2026-10-01", "2026-10-02") == []


@pytest.mark.parametrize("status, raised", [(500, SourceUnreachable), (503, SourceUnreachable),
                                            (429, SourceUnavailable), (403, SourceUnavailable),
                                            (418, SourceUnavailable)])
def test_an_outage_or_a_refusal_is_never_an_empty_answer(wire, status, raised):
    wire.on("gap-chart", b"", status=status)
    with pytest.raises(raised):
        VietcapSource().get_ohlcv("FPT", "2026-10-01", "2026-10-02")


def test_a_network_failure_is_unreachable(wire):
    wire.on("gap-chart", httpx.ConnectTimeout("timed out"))
    with pytest.raises(SourceUnreachable):
        VietcapSource().get_ohlcv("FPT", "2026-10-01", "2026-10-02")


def test_a_request_left_hanging_is_asked_again_but_a_status_is_not(wire):
    """The edge drops about one request in ten; a healthy one answers in under a
    second. A 5xx is an answer, and is not hammered."""
    hangs = iter([httpx.ReadTimeout("hung"), httpx.ConnectTimeout("hung")])

    def flaky(sent):
        for e in hangs:
            raise e
        return [{"t": [_ts("2026-10-01")], "o": [1], "h": [1], "l": [1], "c": [1], "v": [1]}]

    wire.on("gap-chart", flaky)
    assert len(VietcapSource().get_ohlcv("FPT", "2026-10-01", "2026-10-02")) == 1
    assert len(wire.calls) == 3

    wire.routes.clear(); wire.calls.clear()
    wire.on("gap-chart", b"", status=503)
    with pytest.raises(SourceUnreachable):
        VietcapSource().get_ohlcv("FPT", "2026-10-01", "2026-10-02")
    assert len(wire.calls) == 1


def test_any_other_4xx_is_no_data(wire):
    wire.on("gap-chart", b"", status=404)
    assert VietcapSource().get_ohlcv("FPT", "2026-10-01", "2026-10-02") == []


# ── the price board ──────────────────────────────────────────────────────────

def _board_item(sym, **match):
    return {"listingInfo": {"symbol": sym, "ceiling": 66300, "floor": 57700, "refPrice": 62000},
            "bidAsk": {"bidPrices": [{"price": 60500, "volume": 1200}, {"price": 60400, "volume": 800},
                                     {"price": 60300, "volume": 50}, {"price": 60200, "volume": 7}],
                       "askPrices": [{"price": 60600, "volume": 300}, {"price": 0, "volume": 0},
                                     {"price": 60800, "volume": 10}]},
            "matchPrice": {"matchPrice": 60500, "accumulatedValue": 243512.5,
                           "accumulatedVolume": 3973500, "avgMatchPrice": 61284.1,
                           "foreignBuyValue": 1.5e9, "foreignSellValue": 4.0e9, **match}}


def test_the_board_row_is_normalized_like_the_vnstock_one(wire):
    placeholder = {"listingInfo": None, "bidAsk": {"symbol": ""}, "matchPrice": {"symbol": ""}}
    wire.on("getList", [_board_item("FPT"), placeholder])
    board = VietcapSource().get_board(["fpt", "NOSUCH"])
    assert set(board) == {"FPT"}           # an unknown symbol's placeholder is no row
    row = board["FPT"]
    assert (row["ceiling"], row["floor"], row["ref_price"], row["close"]) == (66300, 57700, 62000, 60500)
    assert row["traded_value"] == 243512.5 * 1_000_000   # the one field sent in millions
    assert row["traded_volume"] == 3973500 and row["vwap"] == 61284.1
    assert row["bids"] == [[60500, 1200], [60400, 800], [60300, 50]]   # cut to three levels
    assert row["asks"] == [[60600, 300]]   # a hole ends the side
    assert wire.calls[0][1] == {"symbols": ["FPT", "NOSUCH"]}


def test_an_auction_price_makes_the_side_unreadable_not_empty(wire):
    item = _board_item("FPT")
    item["bidAsk"]["bidPrices"][0]["price"] = "ATO"
    wire.on("getList", [item])
    assert VietcapSource().get_board(["FPT"])["FPT"]["bids"] is None


@pytest.mark.parametrize("body", [{"error": "x"}, None, "text", b"<html>", [{"listingInfo": {}}], [7]])
def test_a_board_that_cannot_be_read_is_unavailable_never_empty(wire, body):
    """``{}`` stops the chain and leaves every caller with no quotes at all."""
    wire.on("getList", body)
    with pytest.raises(SourceUnavailable):
        VietcapSource().get_board(["FPT"])


def test_a_board_4xx_is_unavailable_too(wire):
    wire.on("getList", b"", status=400)
    with pytest.raises(SourceUnavailable):
        VietcapSource().get_board(["FPT"])


def test_an_empty_request_asks_nothing(wire):
    assert VietcapSource().get_board([]) == {} and wire.calls == []


# ── index and exchange members ────────────────────────────────────────────────

_LISTING = [
    {"symbol": "FPT", "type": "STOCK", "board": "HSX"},
    {"symbol": "E1VFVN30", "type": "ETF", "board": "HSX"},
    {"symbol": "FUCVREIT", "type": "UNIT_TRUST", "board": "HSX"},
    {"symbol": "CFPT2601", "type": "CW", "board": "HSX"},
    {"symbol": "PVS", "type": "STOCK", "board": "HNX"},
    {"symbol": "OLD", "type": "STOCK", "board": "DELISTED"},
    {"symbol": "fpt", "type": "STOCK", "board": "HSX"},
]


def test_an_exchange_is_its_listed_stocks_not_everything_trading_there(wire):
    """Vietcap's ``HOSE`` group carries the ETFs and fund certificates as well (430
    against 406 stocks on 2026-10-06); the broad tier means the stocks."""
    wire.on("getAll", _LISTING)
    src = VietcapSource()
    assert src.get_index_constituents("HOSE") == ["FPT"]
    assert src.get_index_constituents("hnx") == ["PVS"]


@pytest.mark.parametrize("body", [[], {"error": "x"}, None, [{"symbol": "PVS", "type": "STOCK", "board": "HNX"}]])
def test_an_exchange_with_no_stocks_is_unavailability(wire, body):
    wire.on("getAll", body)
    with pytest.raises(SourceUnavailable):
        VietcapSource().get_index_constituents("HOSE")


def test_an_index_is_read_from_its_group_under_vietcaps_name(wire):
    wire.on("getByGroup", [{"symbol": "ACB"}, {"symbol": "acb"}, {"symbol": ""}, {"symbol": "FPT"}, 7])
    src = VietcapSource()
    assert src.get_index_constituents("VN30") == ["ACB", "FPT"]
    assert src.get_index_constituents("VNMID") == ["ACB", "FPT"]
    assert [c[1] for c in wire.calls] == [{"group": "VN30"}, {"group": "VNMIDCAP"}]


def test_an_unknown_group_has_no_members(wire):
    wire.on("getByGroup", [])
    assert VietcapSource().get_index_constituents("NOSUCHGROUP") == []


# ── statements ───────────────────────────────────────────────────────────────

def _rows(prefix, years, **fields):
    """One annual row per year (oldest first, as served) and the 2026 quarters."""
    def row(year, length, k):
        return {"ticker": "X", "yearReport": year, "lengthReport": length,
                **{f"{prefix}{n}": v * k for n, v in fields.items()}}
    return {"years": [row(y, 5, y - 2018) for y in years],
            "quarters": [row(2026, q, q) for q in (1, 2)]}


def _statement_wire(wire, *, income_titles, ratio=None):
    metrics = {"INCOME_STATEMENT": [{"field": f"isa{n}", "titleEn": t} for n, t in income_titles.items()],
               "BALANCE_SHEET": [{"field": "bsa1", "titleEn": vietcap._BALANCE["equity"]},
                                 {"field": "bsa2", "titleEn": "TOTAL ASSETS"}],
               "CASH_FLOW": [{"field": "cfa1", "titleEn": vietcap._CASHFLOW["capex"]}],
               "NOTE": None}
    years = range(2019, 2026)
    sections = {"INCOME_STATEMENT": _rows("isa", years, **{str(n): 10.0 * n for n in income_titles}),
                "BALANCE_SHEET": _rows("bsa", years, **{"1": 5.0, "2": 9.0}),
                "CASH_FLOW": _rows("cfa", years, **{"1": -1.0})}
    wire.on("/metrics", _iq(metrics))
    wire.on("/financial-statement", lambda sent: _iq(sections[sent["section"]]))
    wire.on("/statistics-financial", _iq(ratio if ratio is not None else []))
    return wire


def test_statements_keep_the_newest_four_periods_under_their_labels(wire):
    """The rows arrive oldest first; ``head(4)`` of them is the symbol's 2019."""
    _statement_wire(wire, income_titles={1: vietcap._INCOME["net_sales"]})
    _take_units()
    got = VietcapSource().get_statements("x", "year")
    assert got["kind"] == "general" and got["ratio_labels"] == "source"
    assert got["periods"] == ["2025", "2024", "2023", "2022"]
    assert got["statements"]["income"]["net_sales"] == {"2025": 70.0, "2024": 60.0, "2023": 50.0, "2022": 40.0}
    assert got["statements"]["balance"]["equity"]["2025"] == 35.0
    assert got["statements"]["cashflow"]["capex"]["2025"] == -7.0
    assert got["statements"]["income"]["net_profit"] == {}      # no such line filed
    assert got["ratio_extra"] == {}
    assert not any("statistics-financial" in url for url, _ in wire.calls)   # general: not asked
    assert _take_units() == 4              # three statements and their line-item names


def test_quarters_are_labelled_by_year_and_quarter(wire):
    _statement_wire(wire, income_titles={1: vietcap._INCOME["net_sales"]})
    got = VietcapSource().get_statements("X", "quarter")
    assert got["periods"] == ["2026-Q2", "2026-Q1"]
    assert got["statements"]["income"]["net_sales"] == {"2026-Q2": 20.0, "2026-Q1": 10.0}


def test_a_bank_is_read_with_the_bank_maps_and_its_ratio_series(wire):
    ratio = [{"year": "2025", "quarter": 5, "yearReport": 2025, "ratioType": "RATIO_YEAR",
              "npl": 0.011, "casaRatio": 0.35, "car": 0, "netInterestMargin": None},
             {"year": "2026", "quarter": 2, "yearReport": 2026, "ratioType": "RATIO_TTM", "npl": 0.012}]
    _statement_wire(wire, income_titles={1: vietcap._BANK_MARKER}, ratio=ratio)
    _take_units()
    got = VietcapSource().get_statements("VCB", "year")
    assert got["kind"] == "bank"
    assert got["statements"]["income"]["net_interest_income"]["2025"] == 70.0
    # A zero or a null reads as missing, so a ratio nobody filed is absent, not 0.
    assert got["ratio_extra"] == {"NPL (%)": {"2025": 0.011, "2026-Q2": 0.012},
                                  "CASA Ratio": {"2025": 0.35}}
    assert _take_units() == 5


def test_no_such_listing_has_no_statements(wire):
    wire.on("/financial-statement", _iq(None))
    assert VietcapSource().get_statements("NOSUCH") is None
    assert len(wire.calls) == 1            # and nothing more was asked


@pytest.mark.parametrize("answer", [_iq({"rows": []}), _iq([1, 2]), {"data": None, "successful": False},
                                    {"error": "x"}, [1]])
def test_a_statement_answer_of_another_shape_is_not_no_statements(wire, answer):
    """None is cached negatively for a week; a changed payload must not buy that."""
    wire.on("/financial-statement", answer)
    with pytest.raises(SourceUnavailable):
        VietcapSource().get_statements("FPT")


def test_an_outage_mid_statement_is_unreachable_not_a_partial_answer(wire):
    _statement_wire(wire, income_titles={1: vietcap._INCOME["net_sales"]})
    wire.routes.insert(0, ("section", None, 0))    # never matches; keeps the order explicit
    wire.routes.insert(0, ("/metrics", httpx.ReadTimeout("slow"), 200))
    with pytest.raises(SourceUnreachable):
        VietcapSource().get_statements("FPT")


# ── corporate actions ────────────────────────────────────────────────────────

def test_events_are_read_across_pages_and_classified_by_the_shared_rule(wire):
    cash = {"eventCode": "DIV", "eventTitleVi": "VNM - Trả cổ tức bằng tiền mặt đợt 1 năm 2026",
            "exrightDate": "2026-09-15T00:00:00", "recordDate": "2026-09-16T00:00:00",
            "payoutDate": "2026-09-30T00:00:00", "publicDate": "2026-08-20T00:00:00",
            "valuePerShare": 1850.0}
    stock = {"eventCode": "ISS", "eventTitleVi": "VNM - Trả cổ tức bằng cổ phiếu, tỷ lệ 10%",
             "exrightDate": "2020-09-29T00:00:00", "exerciseRatio": 0.1}
    pages = {0: {"content": [cash], "totalPages": 2}, 1: {"content": [stock], "totalPages": 2}}
    wire.on("/v1/events", lambda sent: _iq(pages[sent["page"]]))
    _take_units()
    events = VietcapSource().get_events("vnm")
    assert len(events) == 2 and [c[1]["page"] for c in wire.calls] == [0, 1]
    assert _take_units() == 2              # one request a page, billed as such
    assert wire.calls[0][1]["ticker"] == "VNM" and wire.calls[0][1]["eventCode"] == "DIV,ISS"
    by_date = {e["ex_date"]: e for e in events}
    assert set(by_date) == {"2026-09-15", "2020-09-29"}
    assert 1850.0 in by_date["2026-09-15"].values()


def test_no_events_is_an_answer(wire):
    wire.on("/v1/events", _iq({"content": [], "totalPages": 0}))
    assert VietcapSource().get_events("NOSUCH") == []


def test_a_later_page_that_cannot_be_read_is_not_a_short_list(wire):
    row = {"eventCode": "DIV", "eventTitleVi": "Trả cổ tức bằng tiền mặt",
           "exrightDate": "2026-09-15T00:00:00", "valuePerShare": 500.0}
    pages = {0: {"content": [row], "totalPages": 3}, 1: None}
    wire.on("/v1/events", lambda sent: _iq(pages[sent["page"]]))
    with pytest.raises(SourceUnavailable):
        VietcapSource().get_events("VNM")


# ── listing and classification ────────────────────────────────────────────────

def test_the_listing_carries_board_name_and_kind(wire):
    wire.on("getAll", [
        {"symbol": "FPT", "type": "STOCK", "board": "HSX", "organShortName": "FPT Corp",
         "organName": "Công ty Cổ phần FPT"},
        {"symbol": "E1VFVN30", "type": "ETF", "board": "HSX", "organShortName": None,
         "organName": "Quỹ ETF DCVFMVN30"},
        {"symbol": "", "type": "STOCK", "board": "HSX"}, "junk"])
    assert VietcapSource().get_listing() == {
        "FPT": {"exchange": "HSX", "name": "FPT Corp", "type": "STOCK"},
        "E1VFVN30": {"exchange": "HSX", "name": "Quỹ ETF DCVFMVN30", "type": "ETF"}}


@pytest.mark.parametrize("body", [[], {"error": "x"}, [{"type": "STOCK"}]])
def test_a_listing_with_no_symbol_is_unavailable(wire, body):
    wire.on("getAll", body)
    with pytest.raises(SourceUnavailable):
        VietcapSource().get_listing()


def test_a_company_is_its_english_sector_and_short_name(wire):
    wire.on("/v1/company/details", _iq({"ticker": "FPT", "sector": "Technology",
                                        "viOrganShortName": "FPT Corp", "icbCodeLv2": "9500"}))
    assert VietcapSource().get_company("fpt") == {"sector": "Technology", "name": "FPT Corp"}
    assert wire.calls[-1][1] == {"ticker": "FPT"}


def test_an_unknown_company_is_none_and_a_refusal_raises(wire):
    wire.on("/v1/company/details", _iq(None))
    assert VietcapSource().get_company("NOSUCH") is None
    wire.routes.clear()
    wire.on("/v1/company/details", {}, status=418)
    with pytest.raises(SourceUnavailable):
        VietcapSource().get_company("FPT")


_SEARCH_BAR = [
    {"code": "HPG", "icbLv1": {"code": "1000", "name": "Vật liệu cơ bản"},
     "icbLv2": {"code": "1700", "name": "Tài nguyên Cơ bản"}},
    {"code": "A+ Fund", "icbLv2": {"code": "8700", "name": "Dịch vụ tài chính"}},
    {"code": "XXX", "icbLv2": {"code": "", "name": ""}},      # unclassified: left out
    {"code": "YYY", "icbLv2": None},
]


def test_icb_sectors_are_read_at_the_level_asked_for(wire):
    wire.on("/v2/company/search-bar", _iq(_SEARCH_BAR))
    src = VietcapSource()
    assert src.get_icb_sectors() == {
        "HPG": {"sector": "Tài nguyên Cơ bản", "icb_code": "1700"},
        "A+ FUND": {"sector": "Dịch vụ tài chính", "icb_code": "8700"}}
    assert src.get_icb_sectors(1) == {"HPG": {"sector": "Vật liệu cơ bản", "icb_code": "1000"}}


@pytest.mark.parametrize("data", [[], None, {"rows": []}, [{"code": "HPG", "icb2": {}}]])
def test_an_icb_answer_with_no_classified_row_is_unavailable(wire, data):
    # A renamed level field would otherwise read as a market with no sectors, and the
    # stored map would be replaced by nothing.
    wire.on("/v2/company/search-bar", _iq(data))
    with pytest.raises(SourceUnavailable):
        VietcapSource().get_icb_sectors()


# ── what it does not answer, and the POST helper it added ─────────────────────

def test_capabilities_it_does_not_have_fall_through():
    with pytest.raises(NotSupported):
        VietcapSource().get_index_live("VNINDEX")


def test_post_capped_is_capped_like_get(monkeypatch):
    class _Fake:
        status_code = 200

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def iter_bytes(self):
            yield from [b"x" * 64] * 100

    seen = {}

    def stream(method, url, **kw):
        seen.update(method=method, url=url, **kw)
        return _Fake()

    monkeypatch.setattr(vmd_http.httpx, "stream", stream)
    with pytest.raises(SourceUnavailable):
        vmd_http.post_capped("https://example.test", json={"a": 1}, max_bytes=128, what="big")
    assert seen["method"] == "POST" and seen["json"] == {"a": 1}
