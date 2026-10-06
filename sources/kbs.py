"""KBS — one symbol's **matched-trade tape** for the current session, with the side
that initiated each trade.

The price board says how much traded; it cannot say *who was in a hurry*. Each
matched trade on HOSE/HNX/UPCOM is initiated by an order crossing the spread, and
KBS's public trade-history endpoint labels every print with that side — the
"Mua CĐ / Bán CĐ" split retail apps show. It is the only source in this package
that does: VCI's intraday endpoint also carries a side but serves at most 100
prints per call (measured 2026-09-24; vnstock 4.0.4's wrapper cannot reach it at
all), and no source serves a *past* session's tape.

Endpoint::

    GET https://kbbuddywts.kbsec.com.vn/iis-server/investment/trade/history/MBB
        ?page=1&limit=5000

No token, no cookie. The response is ``{"data": [row, …]}``, rows **newest-first**::

    {"t": "2026-09-24 10:00:52:79", "TD": "24/09/2026", "SB": "MBB",
     "FT": "10:00:52", "LC": "B", "FMP": 20000, "FCV": 0, "FV": 100,
     "AVO": 1276900, "AVA": 25507425000}

``TD`` is the session, ``FT`` the match time (ICT), ``LC`` the initiating side
(``B``/``S``; empty on the ATO/ATC auction prints, which have no initiator),
``FMP`` the price in full VND, ``FV`` the matched volume and ``AVO`` the session's
**accumulated** matched volume after that print. Measured 2026-09-24 (MBB, PVS,
ACV): the buy + sell + auction volume summed to the board's
``match_accumulated_volume`` exactly, and on HPG two 200-row pages chained without
a gap (``AVO`` of one page's oldest print minus its ``FV`` equals the next page's
newest ``AVO``). An unknown symbol answers ``{"data": []}`` with HTTP 200.

**Only the session in progress, or the last one closed, is served** — there is no
date parameter. What this package does not bank the day it happens is gone; the
caller owns that. ``TD`` is returned so the caller can refuse a tape that belongs
to a different session than it thinks (the endpoint rolls over at some point before
the next open, and the hour is not measured).

``AVO`` is what makes the tape self-checking: ordered on it (the served order is
only by time, and ties are arbitrary), every print must advance it by exactly its
own volume. A tape that does not chain — a page boundary that shifted
under a live session, a truncated body — raises ``SourceUnavailable`` rather than
returning a short tape, because a short tape reads as a quieter session and would
be banked as one.

This source implements *only* ``get_trade_tape``.
"""
import json
import logging
import re
from datetime import datetime

import httpx

from vn_market_data.sources.base import DataSource, SourceUnavailable
from vn_market_data.sources.http import get_capped

log = logging.getLogger(__name__)

_URL = "https://kbbuddywts.kbsec.com.vn/iis-server/investment/trade/history/"
_TIMEOUT = 10.0
# ~150 bytes of JSON per print, so a page is ~0.75 MB — well under the 8 MiB read cap.
# The busiest names print tens of thousands of times a session; paging keeps each
# body bounded rather than trusting one response with the whole day.
_PAGE_SIZE = 5000
# 60 pages = 300,000 prints, several times the busiest session on record. A payload
# that never comes back short cannot turn one symbol into an unbounded crawl.
_MAX_PAGES = 60
_HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json, text/plain, */*"}
_SIDES = {"B": "buy", "S": "sell"}
# The symbol goes into the URL *path*, so it is held to what a ticker can be.
_SYMBOL_RE = re.compile(r"^[A-Z0-9]{1,10}$")


def _int(v) -> int | None:
    if isinstance(v, bool):
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, float) and v == v and v.is_integer():
        return int(v)
    return None


def parse_page(body: bytes) -> list[dict]:
    """One page's prints, newest-first as served, in this package's shape.

    Malformed JSON or a payload of the wrong shape raises ``SourceUnavailable`` — an
    unreadable page is not an empty session. A single print missing a field it
    cannot do without is also refused whole: dropping it would break the ``AVO``
    chain anyway, and a clear error beats a chain error two frames later."""
    try:
        payload = json.loads(body)
    except ValueError as e:
        raise SourceUnavailable(f"kbs trade tape: unparseable response — {e}") from None
    rows = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(rows, list):
        raise SourceUnavailable("kbs trade tape: response has no `data` list")
    out = []
    for row in rows:
        if not isinstance(row, dict):
            raise SourceUnavailable("kbs trade tape: a print is not an object")
        try:
            session = datetime.strptime(str(row.get("TD", "")).strip(), "%d/%m/%Y").date()
        except ValueError:
            raise SourceUnavailable(f"kbs trade tape: bad session date {row.get('TD')!r}") from None
        price, vol, acc = _int(row.get("FMP")), _int(row.get("FV")), _int(row.get("AVO"))
        tm = row.get("FT")
        if price is None or vol is None or acc is None or not isinstance(tm, str) or vol <= 0:
            raise SourceUnavailable(f"kbs trade tape: incomplete print {row!r}"[:200])
        out.append({"date": session.isoformat(), "time": tm.strip(), "price": price,
                    "volume": vol, "side": _SIDES.get(str(row.get("LC") or "").strip().upper()),
                    "accumulated_volume": acc})
    return out


def chain(prints: list[dict]) -> list[dict]:
    """Prints from consecutive pages → oldest-first, verified.

    Ordered on ``accumulated_volume``, not as served: prints sharing a timestamp —
    one order sweeping two price levels — arrive in either order (HPG 2026-09-24
    09:15:45: 600 @ 21,050 listed before 600 @ 21,100 but accumulated after it).
    Then every print must carry the same session and advance
    ``accumulated_volume`` by exactly its own volume, starting from zero, so a
    missing or repeated print still breaks the chain. Raises ``SourceUnavailable``
    on the first break, naming where."""
    tape = sorted(prints, key=lambda p: p["accumulated_volume"])
    if not tape:
        return []
    session, acc = tape[0]["date"], 0
    for i, p in enumerate(tape):
        if p["date"] != session:
            raise SourceUnavailable(f"kbs trade tape: two sessions in one tape "
                                    f"({session}, {p['date']})")
        acc += p["volume"]
        if p["accumulated_volume"] != acc:
            raise SourceUnavailable(
                f"kbs trade tape: print {i} at {p['time']} reads accumulated "
                f"{p['accumulated_volume']}, the prints before it sum to {acc}")
    return tape


class KBSSource(DataSource):
    name = "kbs"

    def get_trade_tape(self, symbol: str) -> dict | None:
        symbol = symbol.strip().upper()
        if not symbol:
            return None
        if not _SYMBOL_RE.match(symbol):
            raise ValueError(f"not a ticker: {symbol!r}")
        prints: list[dict] = []
        for page in range(1, _MAX_PAGES + 1):
            rows = self._fetch_page(symbol, page)
            prints.extend(rows)
            if len(rows) < _PAGE_SIZE:
                break
        else:
            raise SourceUnavailable(f"kbs trade tape {symbol}: still full after "
                                    f"{_MAX_PAGES} pages — refusing to crawl further")
        tape = chain(prints)
        if not tape:
            return None
        return {"symbol": symbol, "date": tape[0]["date"], "trades": tape}

    def _client(self) -> httpx.Client:
        """One keep-alive connection for the whole pass (see ``cafef._client``: a
        pass is ~400 symbols against one host)."""
        client = getattr(self, "_http", None)
        if client is None:
            client = self._http = httpx.Client(headers=_HEADERS, timeout=_TIMEOUT)
        return client

    def _fetch_page(self, symbol: str, page: int) -> list[dict]:
        try:
            status, body = get_capped(_URL + symbol, params={"page": page, "limit": _PAGE_SIZE},
                                      headers=_HEADERS, timeout=_TIMEOUT,
                                      what=f"kbs trade tape {symbol}", client=self._client())
        except httpx.HTTPError as e:
            raise SourceUnavailable(f"kbs trade tape {symbol}: {e}") from None
        if status != 200:
            raise SourceUnavailable(f"kbs trade tape {symbol}: HTTP {status}")
        return parse_page(body)
