"""KBS trade tape: parsing, the AVO chain, paging."""
import json

import pytest

from vn_market_data.sources import kbs
from vn_market_data.sources.base import SourceUnavailable


def _row(ft, price, vol, avo, side="B", td="24/09/2026"):
    return {"TD": td, "SB": "MBB", "FT": ft, "LC": side, "FMP": price, "FCV": 0,
            "FV": vol, "AVO": avo, "AVA": 0}


def _page(rows):
    return json.dumps({"data": rows}).encode()


def test_parse_and_chain_oldest_first():
    rows = [_row("14:45:00", 20100, 300, 600, side=""),   # newest first, as served
            _row("09:16:00", 20000, 200, 300, side="S"),
            _row("09:15:00", 20000, 100, 100, side="B")]
    tape = kbs.chain(kbs.parse_page(_page(rows)))
    assert [t["time"] for t in tape] == ["09:15:00", "09:16:00", "14:45:00"]
    assert [t["side"] for t in tape] == ["buy", "sell", None]
    assert tape[0]["date"] == "2026-09-24"


def test_chain_gap_raises():
    rows = [_row("09:16:00", 20000, 200, 400), _row("09:15:00", 20000, 100, 100)]
    with pytest.raises(SourceUnavailable, match="sum to 300"):
        kbs.chain(kbs.parse_page(_page(rows)))


def test_two_sessions_raise():
    rows = [_row("09:15:00", 20000, 100, 200, td="25/09/2026"),
            _row("09:15:00", 20000, 100, 100)]
    with pytest.raises(SourceUnavailable, match="two sessions"):
        kbs.chain(kbs.parse_page(_page(rows)))


@pytest.mark.parametrize("body", [b"not json", b'{"x": 1}', b'{"data": [1]}',
                                  _page([_row("09:15:00", None, 100, 100)])])
def test_malformed_raises(body):
    with pytest.raises(SourceUnavailable):
        kbs.parse_page(body)


def test_pages_until_short(monkeypatch):
    monkeypatch.setattr(kbs, "_PAGE_SIZE", 2)
    # Five prints of 100, newest first, served two per page.
    allrows = [_row(f"09:1{i}:00", 20000, 100, 100 * (i + 1)) for i in reversed(range(5))]
    calls = []

    def fake(self, symbol, page):
        calls.append(page)
        return kbs.parse_page(_page(allrows[(page - 1) * 2: page * 2]))

    monkeypatch.setattr(kbs.KBSSource, "_fetch_page", fake)
    out = kbs.KBSSource().get_trade_tape("mbb")
    assert calls == [1, 2, 3]
    assert out["symbol"] == "MBB" and out["date"] == "2026-09-24"
    assert [t["accumulated_volume"] for t in out["trades"]] == [100, 200, 300, 400, 500]


def test_never_short_refuses(monkeypatch):
    monkeypatch.setattr(kbs, "_PAGE_SIZE", 1)
    monkeypatch.setattr(kbs, "_MAX_PAGES", 3)
    monkeypatch.setattr(kbs.KBSSource, "_fetch_page",
                        lambda self, s, p: kbs.parse_page(_page([_row("09:15:00", 1, 1, 1)])))
    with pytest.raises(SourceUnavailable, match="still full"):
        kbs.KBSSource().get_trade_tape("MBB")


def test_empty_symbol_is_none(monkeypatch):
    monkeypatch.setattr(kbs.KBSSource, "_fetch_page", lambda self, s, p: [])
    assert kbs.KBSSource().get_trade_tape("ZZZ") is None


def test_bad_ticker_never_reaches_the_url():
    with pytest.raises(ValueError):
        kbs.KBSSource().get_trade_tape("../x")


def test_same_second_prints_are_ordered_on_accumulated_volume():
    # HPG 2026-09-24 09:15:45, as served: the two halves of one sweep, out of order.
    rows = [_row("09:15:45", 21100, 600, 700, side="S"),
            _row("09:15:45", 21050, 600, 1300, side="S"),
            _row("09:15:40", 21050, 100, 100, side="S")]
    tape = kbs.chain(kbs.parse_page(_page(rows)))
    assert [t["accumulated_volume"] for t in tape] == [100, 700, 1300]


def test_repeated_print_still_breaks():
    rows = [_row("09:15:01", 20000, 100, 200), _row("09:15:01", 20000, 100, 200),
            _row("09:15:00", 20000, 100, 100)]
    with pytest.raises(SourceUnavailable):
        kbs.chain(kbs.parse_page(_page(rows)))
