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

This source implements *only* ``get_market_turnover`` — everything else falls through
to the next source in the chain.
"""
import json
import logging

import httpx

from vn_market_data.sources.base import DataSource, SourceUnavailable
from vn_market_data.sources.http import get_capped

log = logging.getLogger(__name__)

_URL = "https://api-finfo.vndirect.com.vn/v4/vnmarket_prices"
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
