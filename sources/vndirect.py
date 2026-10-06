"""VNDirect finfo — the exchange's own market-wide turnover, matched **and** put-through.

The one thing no price board can give: what a session's money actually was. A board
carries each stock's accumulated *match* value, so summing it over the exchange lands
~15-20% short of the "GTGD" figure every terminal quotes, because block deals agreed
off the order book (thỏa thuận / put-through) never touch it. VNDirect republishes
HOSE's own daily aggregate — matched, put-through and their total — for the whole
history in a single call, so a headline number and its chart can both come from the
exchange rather than from an estimate assembled downstream.

Values arrive in full VND already. Today's row is live: it is present and rising from
the open, so the current session can be read straight off the tail.

It also serves ``get_foreign_archive``: each symbol's foreign (khối ngoại) buy/sell
per session from 2018-08 on, in one request (``v4/foreigns``). Measured 2026-09-18
against CafeF's settled figures over 30 symbols × 63 sessions: the **net** agrees
within 0.5% of the day's gross on 97% of sessions with one sign disagreement and no
missing dates either way, but the **gross** buy/sell runs 2–14× CafeF's on some days —
the two sides of a foreign-to-foreign put-through, counted like the board counts them.
And a whole session can read zero for every symbol (2026-08-28 did, where CafeF has
real figures), which no per-symbol reader can tell from a quiet day. Both are why
this is an *archive* capability and never ``get_foreign_history``: the settle job
must not fall through to it and write a zero day over a board row. The caller that
banks it drops dates where the cross-section reads zero
(``scripts/backfill_foreign_flow_archive.py``).

Everything else falls through to the next source in the chain.
"""
import json
import logging
from datetime import date

import httpx

from vn_market_data.sources.base import DataSource, SourceUnavailable
from vn_market_data.sources.http import get_capped

log = logging.getLogger(__name__)

_URL = "https://api-finfo.vndirect.com.vn/v4/vnmarket_prices"
_FOREIGN_URL = "https://api-finfo.vndirect.com.vn/v4/foreigns"
# One symbol's whole archive (~2,000 sessions since 2018-08) fits one page of this;
# more pages are followed, capped so a payload that lies cannot become a crawl.
_FOREIGN_PAGE = 3000
_FOREIGN_MAX_PAGES = 4
_TIMEOUT = 20.0
# The API pages; one session is one row, so this covers ~4 years in a single request.
_MAX_ROWS = 1000
_HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}


def _num(v) -> float | None:
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


class VNDirectSource(DataSource):
    name = "vndirect"

    def get_market_turnover(self, index: str, start: str, end: str) -> list[dict]:
        params = {"q": f"code:{index}~date:gte:{start}~date:lte:{end}",
                  "size": str(_MAX_ROWS), "sort": "date"}
        try:
            status, body = get_capped(_URL, params=params, headers=_HEADERS,
                                      timeout=_TIMEOUT, what="vndirect market turnover")
        except httpx.HTTPError as e:
            raise SourceUnavailable(f"vndirect market turnover: {e}") from None
        if status != 200:                             # this endpoint has no 'no data' status
            raise SourceUnavailable(f"vndirect market turnover: HTTP {status}")
        try:
            payload = json.loads(body)
        except ValueError as e:                       # malformed JSON
            log.warning("vndirect: unparseable turnover response — %s", e)
            return []
        rows = (payload or {}).get("data") if isinstance(payload, dict) else None
        rows = rows if isinstance(rows, list) else []

        out = []
        for row in rows:
            if not isinstance(row, dict):             # not this API's shape — skip the row
                continue
            d, total = row.get("date"), _num(row.get("accumulatedVal"))
            if not d or not total:
                continue
            out.append({
                "date":        str(d)[:10],
                "value":       total,                          # matched + put-through
                "matched":     _num(row.get("nmValue")),       # khớp lệnh
                "put_through": _num(row.get("ptValue")),       # thỏa thuận
                "volume":      _num(row.get("accumulatedVol")),
            })
        out.sort(key=lambda r: r["date"])              # the API answers newest-first
        return out

    def get_foreign_archive(self, symbol: str, start: str, end: str) -> list[dict]:
        symbol = symbol.strip().upper()
        if not symbol:
            return []
        seen: dict[str, dict] = {}
        for page in range(1, _FOREIGN_MAX_PAGES + 1):
            rows, pages = self._foreign_page(symbol, start, end, page)
            for r in rows:
                if start <= r["date"] <= end:
                    seen.setdefault(r["date"], r)
            if not rows or pages is None or page >= pages:
                break
        return [seen[d] for d in sorted(seen)]

    def _client(self) -> httpx.Client:
        """One keep-alive connection for a backfill's ~400 requests."""
        client = getattr(self, "_http", None)
        if client is None:
            client = self._http = httpx.Client(headers=_HEADERS, timeout=_TIMEOUT)
        return client

    def _foreign_page(self, symbol: str, start: str, end: str,
                      page: int) -> tuple[list[dict], int | None]:
        params = {"q": f"code:{symbol}~tradingDate:gte:{start}~tradingDate:lte:{end}",
                  "sort": "tradingDate:asc", "size": str(_FOREIGN_PAGE), "page": str(page)}
        try:
            status, body = get_capped(_FOREIGN_URL, params=params, headers=_HEADERS,
                                      timeout=_TIMEOUT, what=f"vndirect foreigns {symbol}",
                                      client=self._client())
        except httpx.HTTPError as e:
            raise SourceUnavailable(f"vndirect foreigns {symbol}: {e}") from None
        if status != 200:                    # an unknown symbol is 200 with no rows
            raise SourceUnavailable(f"vndirect foreigns {symbol}: HTTP {status}")
        return parse_foreign_page(body, symbol)


def parse_foreign_page(body: bytes, symbol: str) -> tuple[list[dict], int | None]:
    """One ``v4/foreigns`` page in the package's foreign-flow shape, plus
    ``totalPages``. Degrades to fewer rows, never to an exception; a row for another
    code (the query is a filter the server could ignore) is dropped."""
    try:
        payload = json.loads(body)
    except ValueError as e:
        log.warning("vndirect: unparseable foreigns response — %s", e)
        return [], None
    rows = payload.get("data") if isinstance(payload, dict) else None
    pages = payload.get("totalPages") if isinstance(payload, dict) else None
    pages = int(pages) if isinstance(pages, (int, float)) and pages >= 0 else None
    out = []
    for row in rows if isinstance(rows, list) else []:
        if not isinstance(row, dict) or row.get("code") != symbol:
            continue
        d, net = row.get("tradingDate"), _num(row.get("netVal"))
        if not isinstance(d, str) or len(d) < 10 or net is None:
            continue
        try:
            d = date.fromisoformat(d[:10]).isoformat()
        except ValueError:
            continue
        out.append({
            "date":        d,
            "buy_value":   _num(row.get("buyVal")),
            "sell_value":  _num(row.get("sellVal")),
            "net_value":   net,
            "buy_volume":  _num(row.get("buyVol")),
            "sell_volume": _num(row.get("sellVol")),
            "net_volume":  _num(row.get("netVol")),
        })
    return out, pages
