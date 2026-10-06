"""CafeF settled foreign-flow history — parsing, paging and the trust boundary.

Offline: the page fetch is substituted, so nothing here opens a socket. The fixture
rows are the shape the endpoint served on 2026-09-18 (VPB, verbatim field names).
"""
import json

import httpx
import pytest

from vn_market_data.sources import cafef
from vn_market_data.sources.base import NotSupported, SourceUnavailable
from vn_market_data.sources.cafef import CafeFSource, parse_page


def _row(day: str, net: float, buy: float = 1.0, sell: float = 1.0) -> dict:
    return {"Symbol": "VPB", "Ngay": day, "KLGDRong": -1733600, "GTDGRong": net,
            "ThayDoi": "28,20 (+2,17%)", "KLMua": 7249100, "GtMua": buy,
            "KLBan": 8982700, "GtBan": sell, "RoomConLai": 568289760, "DangSoHuu": 22.84}


def _page(rows, total=63) -> bytes:
    return json.dumps({"Data": {"TotalCount": total, "Index": "VNINDEX", "Data": rows},
                       "Message": None, "Success": True}).encode()


# ── parsing ──────────────────────────────────────────────────────────────────

def test_a_row_is_normalized_to_the_package_shape():
    rows, total = parse_page(_page([_row("17/09/2026", -48069360000, 203563065000, 251632425000)]))
    assert total == 63
    assert rows == [{"date": "2026-09-17", "buy_value": 203563065000.0,
                     "sell_value": 251632425000.0, "net_value": -48069360000.0,
                     "buy_volume": 7249100.0, "sell_volume": 8982700.0,
                     "net_volume": -1733600.0}]


@pytest.mark.parametrize("body", [b"<html>captive portal</html>", b"null", b"[]",
                                  b'{"Data": "nope"}', b'{"Data": {"Data": {}}}',
                                  b'{"Data": {"Data": [1, "x", null]}}'])
def test_garbage_degrades_to_no_rows_not_an_exception(body):
    rows, total = parse_page(body)
    assert rows == [] and total is None


def test_a_row_without_a_readable_date_or_net_is_dropped_not_guessed():
    bad_date = _row("2026-09-17", -1.0)            # ISO where dd/MM/yyyy is expected
    bad_net = dict(_row("16/09/2026", 0.0), GTDGRong="n/a")
    rows, _ = parse_page(_page([bad_date, bad_net, _row("15/09/2026", 5.0)]))
    assert [r["date"] for r in rows] == ["2026-09-15"]


# ── paging ───────────────────────────────────────────────────────────────────

def _serve(pages: dict[int, list[dict]], total=63):
    """A fake `_fetch_page` that records which pages were asked for."""
    asked = []

    def fetch(self, symbol, page):
        asked.append(page)
        return parse_page(_page(pages.get(page, []), total))
    return fetch, asked


def test_paging_stops_once_the_window_is_covered(monkeypatch):
    """Twenty rows a page, newest first; a daily top-up wanting the last three sessions
    must cost one request, not the whole archive."""
    page1 = [_row(f"{d:02d}/09/2026", float(d)) for d in range(18, 0, -1)]   # 18 rows... pad to 20
    page1 += [_row("29/08/2026", 29.0), _row("28/08/2026", 28.0)]
    fetch, asked = _serve({1: page1, 2: [_row("27/08/2026", 27.0)] * 20})
    monkeypatch.setattr(CafeFSource, "_fetch_page", fetch)
    rows = CafeFSource().get_foreign_history("vpb", "2026-09-15", "2026-09-17")
    assert asked == [1]
    assert [r["date"] for r in rows] == ["2026-09-15", "2026-09-16", "2026-09-17"]
    assert [r["net_value"] for r in rows] == [15.0, 16.0, 17.0]      # oldest-first


def test_paging_walks_the_archive_for_a_backfill_and_stops_on_a_short_page(monkeypatch):
    page1 = [_row(f"{d:02d}/09/2026", float(d)) for d in range(20, 0, -1)]
    page2 = [_row(f"{d:02d}/08/2026", float(d)) for d in range(31, 11, -1)]
    page3 = [_row(f"{d:02d}/08/2026", float(d)) for d in range(11, 8, -1)]   # short
    fetch, asked = _serve({1: page1, 2: page2, 3: page3}, total=43)
    monkeypatch.setattr(CafeFSource, "_fetch_page", fetch)
    rows = CafeFSource().get_foreign_history("VPB", "2026-01-01", "2026-12-31")
    assert asked == [1, 2, 3]
    assert len(rows) == 43 and rows[0]["date"] == "2026-08-09" and rows[-1]["date"] == "2026-09-20"


def test_paging_is_capped_even_if_the_payload_promises_forever(monkeypatch):
    """`TotalCount` comes off the wire; a value that never runs out must not turn one
    symbol into an unbounded crawl."""
    fetch, asked = _serve({p: [_row("01/01/2026", 1.0)] * 20 for p in range(1, 50)}, total=10**9)
    monkeypatch.setattr(CafeFSource, "_fetch_page", fetch)
    CafeFSource().get_foreign_history("VPB", "2025-01-01", "2026-12-31")
    assert len(asked) == cafef._MAX_PAGES


def test_a_duplicate_date_across_pages_keeps_the_newest_copy(monkeypatch):
    """The archive can shift between two page reads (a session settles mid-walk), so
    the same date can appear on both; the first — newer-read — copy wins."""
    fetch, _ = _serve({1: [_row("17/09/2026", 1.0)] * 20, 2: [_row("17/09/2026", 2.0)] * 5})
    monkeypatch.setattr(CafeFSource, "_fetch_page", fetch)
    rows = CafeFSource().get_foreign_history("VPB", "2026-09-01", "2026-09-30")
    assert rows == [dict(rows[0], net_value=1.0)]


def test_bad_dates_are_the_callers_fault_not_the_sources():
    with pytest.raises(ValueError):
        CafeFSource().get_foreign_history("VPB", "17/09/2026", "2026-09-18")


# ── the trust boundary ───────────────────────────────────────────────────────

def test_a_non_200_is_unavailability_not_no_history(monkeypatch):
    monkeypatch.setattr(cafef, "get_capped", lambda *a, **k: (503, b""))
    with pytest.raises(SourceUnavailable):
        CafeFSource().get_foreign_history("VPB", "2026-09-01", "2026-09-18")


def test_a_network_error_is_unavailability(monkeypatch):
    def boom(*a, **k):
        raise httpx.ConnectError("no route")
    monkeypatch.setattr(cafef, "get_capped", boom)
    with pytest.raises(SourceUnavailable):
        CafeFSource().get_foreign_history("VPB", "2026-09-01", "2026-09-18")


def test_every_page_of_a_crawl_rides_one_keep_alive_client(monkeypatch):
    """A fresh connection per page is what CafeF's edge throttles (one of its two
    addresses drops ~20% of new connections under a crawl, each costing the whole
    connect timeout); the pages of a pass must share one client."""
    seen = []

    def fake(url, *, client=None, **k):
        seen.append(client)
        page = int(k["params"]["PageIndex"])
        return 200, _page([_row(f"{18 - page:02d}/09/2026", 1.0)] * _PAGE_SIZE_ROWS)

    monkeypatch.setattr(cafef, "get_capped", fake)
    src = CafeFSource()
    src.get_foreign_history("VPB", "2026-09-10", "2026-09-18")
    src.get_foreign_history("HPG", "2026-09-10", "2026-09-18")
    assert len(seen) >= 2
    assert all(c is not None for c in seen)
    assert len({id(c) for c in seen}) == 1


_PAGE_SIZE_ROWS = cafef._PAGE_SIZE


def test_the_source_answers_nothing_else():
    with pytest.raises(NotSupported):
        CafeFSource().get_board(["VPB"])
