"""DNSE Entrade source — open, unauthenticated OHLCV.

DNSE's Entrade chart API (``services.entrade.com.vn``) serves daily OHLCV with no auth,
generous rate limits (~30/s observed, vs vnstock's ~20/min) and the **same unit
convention as VCI** — stock prices in thousands (×1000 to full VND), the index unscaled
— so the two are interchangeable for candles. Cross-checked 2026-06-24: HPG close and
volume matched VCI to the digit.

OHLCV **only** (daily, plus the intraday-aggregated live index candle). DNSE has no
board / financials / corporate actions, so those methods inherit ``NotSupported`` from
the base class and the adapter's per-capability chain falls through to VCI. That is why
this source sits at the head of the chain and still costs nothing: it answers the one
capability it has, quickly, and abstains from the rest.

Pure ``httpx``, no pandas — which is what keeps the base install light.
"""
import logging
import math
from datetime import datetime, timezone, timedelta

import httpx

from vn_market_data.sources.base import DataSource, SourceUnavailable

log = logging.getLogger(__name__)

_BASE = "https://services.entrade.com.vn/chart-api/v2/ohlcs"
_HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
_PRICE_SCALE = 1000.0          # stock prices come in thousands of VND (like VCI Quote.history)
_VN_TZ = timezone(timedelta(hours=7))  # label each bar with its Vietnam trading date
_TIMEOUT = 15.0


def _f(v):
    """Coerce to float, or None when missing/NaN."""
    try:
        x = float(v)
        return None if math.isnan(x) else x
    except (TypeError, ValueError):
        return None


def _epoch(iso: str, fallback_days: int) -> int:
    """ISO 'YYYY-MM-DD' → Unix seconds (UTC midnight); fallback N days ago on parse error."""
    try:
        return int(datetime.fromisoformat(iso).replace(tzinfo=timezone.utc).timestamp())
    except (TypeError, ValueError):
        return int((datetime.now(timezone.utc) - timedelta(days=fallback_days)).timestamp())


class DNSESource(DataSource):
    name = "dnse"

    def get_ohlcv(self, symbol, start, end, *, is_index=False):
        symbol = symbol.strip().upper()
        if not symbol:
            return []
        kind = "index" if is_index else "stock"
        frm = _epoch(start, 730)
        to = _epoch(end, 0) + 86400          # +1 day so the end date is inclusive
        params = {"from": frm, "to": to, "symbol": symbol, "resolution": "1D"}

        try:
            r = httpx.get(f"{_BASE}/{kind}", params=params, headers=_HEADERS, timeout=_TIMEOUT)
        except httpx.HTTPError as e:           # timeout / connect / read error → transient
            log.warning("dnse: %s OHLCV network error — %s", symbol, e)
            raise SourceUnavailable(symbol) from None

        if r.status_code == 429 or r.status_code >= 500:
            log.warning("dnse: %s OHLCV HTTP %s — unavailable", symbol, r.status_code)
            raise SourceUnavailable(symbol)
        if r.status_code != 200:               # 400 invalid symbol etc. — genuine no-data
            log.warning("dnse: %s OHLCV HTTP %s — no data", symbol, r.status_code)
            return []
        try:
            j = r.json()
        except ValueError:
            log.warning("dnse: %s OHLCV non-JSON body", symbol)
            return []

        t = j.get("t") or []
        c = j.get("c") or []
        if not t or not c:
            return []
        o, h, l, v = j.get("o", []), j.get("h", []), j.get("l", []), j.get("v", [])
        scale = 1.0 if is_index else _PRICE_SCALE

        out: list[dict] = []
        for i in range(len(t)):
            ci = _f(c[i]) if i < len(c) else None
            oi = _f(o[i]) if i < len(o) else None
            hi = _f(h[i]) if i < len(h) else None
            li = _f(l[i]) if i < len(l) else None
            date = datetime.fromtimestamp(t[i], tz=_VN_TZ).date().isoformat()
            if is_index:
                # Index keeps rows with a close even if o/h/l are missing (unscaled).
                if ci is None:
                    continue
                out.append({"date": date, "open": oi, "high": hi, "low": li,
                            "close": ci, "volume": (_f(v[i]) if i < len(v) else None) or 0.0})
            else:
                if None in (oi, hi, li, ci):
                    continue
                out.append({"date": date, "open": oi * scale, "high": hi * scale,
                            "low": li * scale, "close": ci * scale,
                            "volume": (_f(v[i]) if i < len(v) else None) or 0.0})
        out.sort(key=lambda r: r["date"])
        return out

    def get_index_live(self, symbol):
        """Today's *forming* index candle, aggregated from 1-minute bars.

        The ``1D`` feed only publishes a session once it has closed, so mid-session
        every daily consumer is a session behind — fine for the engines (a half-formed
        candle would poison SMAs, patterns and backtests) but wrong for a board that
        claims to show where the index is *now*. Minute bars are the same series the
        daily one is built from, so open/high/low/close/Σvolume over today's bars is
        the daily candle as far as it has been written.

        Returns ``None`` when no bar carries today's Vietnam date — before the open,
        on a weekend/holiday, or once the daily feed has caught up.
        """
        symbol = symbol.strip().upper()
        if not symbol:
            return None
        now = datetime.now(tz=_VN_TZ)
        today = now.date().isoformat()
        params = {"from": int((now - timedelta(days=2)).timestamp()),
                  "to": int(now.timestamp()) + 60,
                  "symbol": symbol, "resolution": "1"}
        try:
            r = httpx.get(f"{_BASE}/index", params=params, headers=_HEADERS, timeout=_TIMEOUT)
        except httpx.HTTPError as e:
            log.warning("dnse: %s intraday network error — %s", symbol, e)
            raise SourceUnavailable(symbol) from None

        if r.status_code == 429 or r.status_code >= 500:
            raise SourceUnavailable(symbol)
        if r.status_code != 200:
            return None
        try:
            j = r.json()
        except ValueError:
            return None

        t, c = j.get("t") or [], j.get("c") or []
        o, h, l, v = j.get("o", []), j.get("h", []), j.get("l", []), j.get("v", [])
        bars = [i for i in range(min(len(t), len(c)))
                if datetime.fromtimestamp(t[i], tz=_VN_TZ).date().isoformat() == today
                and _f(c[i]) is not None]
        if not bars:
            return None

        highs = [x for x in (_f(h[i]) for i in bars if i < len(h)) if x is not None]
        lows  = [x for x in (_f(l[i]) for i in bars if i < len(l)) if x is not None]
        first, last = bars[0], bars[-1]
        close = _f(c[last])
        return {
            "date":   today,
            "open":   (_f(o[first]) if first < len(o) else None) or close,
            "high":   max(highs) if highs else close,
            "low":    min(lows) if lows else close,
            "close":  close,
            "volume": sum((_f(v[i]) or 0.0) for i in bars if i < len(v)),
        }
