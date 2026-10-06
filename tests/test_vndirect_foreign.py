"""VNDirect foreign-flow archive — parsing, paging, and staying out of the settle chain.

Offline: the page fetch is substituted. Fixture rows are the shape ``v4/foreigns``
served on 2026-09-18 (HPG, verbatim field names).
"""
import json

import httpx
import pytest

from vn_market_data.sources import vndirect
from vn_market_data.sources.base import NotSupported, SourceUnavailable
from vn_market_data.sources.vndirect import VNDirectSource, parse_foreign_page


def _row(day: str, net: float, code: str = "HPG") -> dict:
    return {"code": code, "type": "STOCK", "tradingDate": day, "floor": "HOSE",
            "buyVal": 58409618000.0, "sellVal": 167523766500.0, "netVal": net,
            "buyVol": 1483800.0, "sellVol": 4250860.0, "netVol": -2767060.0,
            "totalRoom": 1040714511.0, "currentRoom": 211820856.0}


def _page(rows, pages=1) -> bytes:
    return json.dumps({"data": rows, "currentPage": 1, "size": 3000,
                       "totalElements": len(rows), "totalPages": pages}).encode()


def test_a_row_is_normalized_to_the_foreign_history_shape():
    rows, pages = parse_foreign_page(_page([_row("2018-08-30", -109114148500.0)]), "HPG")
    assert pages == 1
    assert rows == [{"date": "2018-08-30", "buy_value": 58409618000.0,
                     "sell_value": 167523766500.0, "net_value": -109114148500.0,
                     "buy_volume": 1483800.0, "sell_volume": 4250860.0,
                     "net_volume": -2767060.0}]


@pytest.mark.parametrize("body", [b"<html>captive portal</html>", b"null", b"[]",
                                  b'{"data": "nope"}', b'{"data": [1, "x", null]}'])
def test_garbage_degrades_to_no_rows_not_an_exception(body):
    rows, _ = parse_foreign_page(body, "HPG")
    assert rows == []


def test_rows_for_another_code_or_without_date_or_net_are_dropped():
    bad_net = dict(_row("2020-01-03", 0.0), netVal=None)
    rows, _ = parse_foreign_page(_page([_row("2020-01-02", 1.0, code="VNM"), bad_net,
                                        dict(_row("x", 1.0)),
                                        _row("2020-01-06", 5.0)]), "HPG")
    assert [r["date"] for r in rows] == ["2020-01-06"]


def test_a_real_zero_is_kept_because_only_the_cross_section_can_judge_it():
    rows, _ = parse_foreign_page(_page([dict(_row("2026-08-28", 0.0), buyVal=0.0,
                                             sellVal=0.0)]), "HPG")
    assert rows[0]["net_value"] == 0.0


def test_pages_are_followed_windowed_and_returned_oldest_first(monkeypatch):
    asked = []
    served = {1: [_row("2019-01-03", 1.0), _row("2019-01-02", 2.0)],
              2: [_row("2019-01-04", 3.0), _row("2030-01-01", 9.0)]}

    def page(self, symbol, start, end, n):
        asked.append(n)
        return parse_foreign_page(_page(served[n], pages=2), symbol)

    monkeypatch.setattr(VNDirectSource, "_foreign_page", page)
    rows = VNDirectSource().get_foreign_archive("hpg", "2019-01-01", "2019-12-31")
    assert asked == [1, 2]
    assert [r["date"] for r in rows] == ["2019-01-02", "2019-01-03", "2019-01-04"]


def test_a_network_error_is_unavailability(monkeypatch):
    def boom(*a, **k):
        raise httpx.ConnectError("down")

    monkeypatch.setattr(vndirect, "get_capped", boom)
    with pytest.raises(SourceUnavailable):
        VNDirectSource().get_foreign_archive("HPG", "2019-01-01", "2019-12-31")


def test_the_archive_is_not_a_settle_source():
    """The settle job reads ``get_foreign_history``; a source that can read a whole
    session as zero must never answer it, not even as a fallback."""
    with pytest.raises(NotSupported):
        VNDirectSource().get_foreign_history("HPG", "2026-01-01", "2026-09-17")
