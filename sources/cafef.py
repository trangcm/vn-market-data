"""CafeF — per-symbol **settled** foreign (khối ngoại) buy/sell history.

The price board carries each symbol's foreign buy/sell value as a *session-to-date
accumulator*: right after the close, and only then. A series banked from board reads
is therefore only as good as the pass that happened to run last — a day whose last
pass fell at 10:47 records two hours of a session as its final word, and nothing in
the row says so. What that needs is a second reading taken after the exchange
settled the day, and CafeF publishes exactly that: one row per session per symbol,
in full VND, from the exchange's own post-close foreign-trading report.

Endpoint::

    GET https://cafef.vn/du-lieu/Ajax/PageNew/DataHistory/GDKhoiNgoai.ashx
        ?Symbol=VNM&StartDate=&EndDate=&PageIndex=1&PageSize=20

No token, no cookie — a browser ``User-Agent`` is all it wants. The response is
``{"Data": {"TotalCount": 63, "Data": [row, …]}, "Success": true}`` with rows
newest-first and dates as ``dd/MM/yyyy``. Measured 2026-09-18: ``TotalCount`` is
**63 sessions for every symbol regardless of the date parameters** (~3 months), the
page size caps at 20, and the current session's row appears only after the close —
mid-session the newest row is the previous session's. ``StartDate``/``EndDate`` are
sent but not honoured, so the window is applied here, and paging stops as soon as a
page reaches past ``start``.

Values are full VND (``GtMua``/``GtBan``/``GTDGRong``) and shares
(``KLMua``/``KLBan``/``KLGDRong``); the buy and sell totals sit a little *below* the
board's post-close accumulators on some sessions (the board also counts the two
sides of a foreign-to-foreign put-through), while the net agrees to rounding — which
is why a writer keyed on "the busier reading wins" must not be the one that banks
these.

This source implements *only* ``get_foreign_history``.
"""
import json
import logging
from datetime import date, datetime

import httpx

from vn_market_data.sources.base import DataSource, SourceUnavailable
from vn_market_data.sources.http import get_capped

log = logging.getLogger(__name__)

_URL = "https://cafef.vn/du-lieu/Ajax/PageNew/DataHistory/GDKhoiNgoai.ashx"
_TIMEOUT = 10.0
_PAGE_SIZE = 20          # the endpoint's own maximum
# The endpoint serves ~63 sessions however far back you ask, so four pages is the
# whole archive; the cap is there so a payload that lies about `TotalCount` cannot
# turn one symbol into an unbounded crawl.
_MAX_PAGES = 8
_HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json, text/plain, */*",
            "Referer": "https://cafef.vn/"}


def _num(v) -> float | None:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if f == f and abs(f) != float("inf") else None


def _iso(v) -> str | None:
    """``dd/MM/yyyy`` → ISO; anything else → ``None`` (the row is dropped)."""
    if not isinstance(v, str):
        return None
    try:
        return datetime.strptime(v.strip(), "%d/%m/%Y").date().isoformat()
    except ValueError:
        return None


def parse_page(body: bytes) -> tuple[list[dict], int | None]:
    """One page's rows in this package's shape, plus the advertised ``TotalCount``.

    Malformed JSON, a payload of the wrong shape, or rows missing their date or net
    all degrade to *fewer rows*, never to an exception: a source is a trust boundary
    and what comes off the wire is not owed a shape. ``TotalCount`` is ``None`` when
    the payload did not carry a usable one, and the caller then pages until a page
    comes back short.
    """
    try:
        payload = json.loads(body)
    except ValueError as e:
        log.warning("cafef: unparseable foreign-history response — %s", e)
        return [], None
    outer = payload.get("Data") if isinstance(payload, dict) else None
    if not isinstance(outer, dict):
        return [], None
    rows = outer.get("Data")
    total = outer.get("TotalCount")
    total = int(total) if isinstance(total, (int, float)) and total >= 0 else None
    out = []
    for row in rows if isinstance(rows, list) else []:
        if not isinstance(row, dict):
            continue
        d = _iso(row.get("Ngay"))
        net = _num(row.get("GTDGRong"))
        if d is None or net is None:
            continue
        out.append({
            "date":        d,
            "buy_value":   _num(row.get("GtMua")),
            "sell_value":  _num(row.get("GtBan")),
            "net_value":   net,
            "buy_volume":  _num(row.get("KLMua")),
            "sell_volume": _num(row.get("KLBan")),
            "net_volume":  _num(row.get("KLGDRong")),
        })
    return out, total


class CafeFSource(DataSource):
    name = "cafef"

    def get_foreign_history(self, symbol: str, start: str, end: str) -> list[dict]:
        symbol = symbol.strip().upper()
        if not symbol:
            return []
        try:
            lo, hi = date.fromisoformat(start), date.fromisoformat(end)
        except (TypeError, ValueError):
            raise ValueError(f"start/end must be ISO dates, got {start!r}/{end!r}")
        seen: dict[str, dict] = {}
        for page in range(1, _MAX_PAGES + 1):
            rows, total = self._fetch_page(symbol, page)
            if not rows:
                break
            oldest = None
            for r in rows:
                d = date.fromisoformat(r["date"])
                oldest = d if oldest is None or d < oldest else oldest
                if lo <= d <= hi:
                    seen.setdefault(r["date"], r)     # newest-first: first copy wins
            if oldest is not None and oldest <= lo:   # the window is fully covered
                break
            if len(rows) < _PAGE_SIZE:                # a short page is the last page
                break
            if total is not None and page * _PAGE_SIZE >= total:
                break
        return [seen[d] for d in sorted(seen)]

    def _client(self) -> httpx.Client:
        """One keep-alive connection for the whole crawl, opened on first use.

        A settle pass is ~400 symbols × up to 4 pages against one host. Measured
        2026-09-18 with a fresh connection per page: one of CafeF's two addresses
        dropped ~20% of new connections, each costing a full connect timeout before
        the other address answered — 14 s per symbol, 2 h per pass. The same 40
        requests over one connection took 17 s with no stall. The connection is kept
        for the source's lifetime; httpx reopens it transparently if the host closes
        it between passes.
        """
        client = getattr(self, "_http", None)
        if client is None:
            client = self._http = httpx.Client(headers=_HEADERS, timeout=_TIMEOUT)
        return client

    def _fetch_page(self, symbol: str, page: int) -> tuple[list[dict], int | None]:
        params = {"Symbol": symbol, "StartDate": "", "EndDate": "",
                  "PageIndex": str(page), "PageSize": str(_PAGE_SIZE)}
        try:
            status, body = get_capped(_URL, params=params, headers=_HEADERS,
                                      timeout=_TIMEOUT, what=f"cafef foreign history {symbol}",
                                      client=self._client())
        except httpx.HTTPError as e:
            raise SourceUnavailable(f"cafef foreign history {symbol}: {e}") from None
        if status != 200:                             # no 'unknown symbol' status exists
            raise SourceUnavailable(f"cafef foreign history {symbol}: HTTP {status}")
        return parse_page(body)
