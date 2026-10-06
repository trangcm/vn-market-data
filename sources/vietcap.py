"""Vietcap source — the same endpoints the VCI source reads, without ``vnstock``.

``vnstock``'s VCI backend is a client for Vietcap's public read endpoints
(``trading.vietcap.com.vn`` for candles, the price board and index groups;
``iq.vietcap.com.vn`` for statements, the ratio series and corporate actions). They
take no key and no cookie (probed 2026-10-06), so this source calls them directly:
pure ``httpx``, no pandas, and no dependency on a package PyPI quarantined on
2026-09-24.

It answers the capabilities :class:`~vn_market_data.sources.vci.VCISource` answers and
normalizes to the same shapes. What the two share is imported from ``vci`` rather than
copied — the statement line-item maps, the ratio fields, the dividend-title
classification, the unpopulated-foreign-column rule — so a rule changed there changes
here. What differs is only what ``vnstock`` did to the wire format on the way through:

- Candles arrive in **full VND** (``vnstock`` divides by 1000 and ``VCISource``
  multiplies back), so nothing is scaled. The index is unscaled on both. ``vnstock``
  also rounds to 10 VND on the way, so an adjusted close here can differ from
  ``VCISource``'s by up to 5 VND; and only bars inside ``[start, end]`` are returned,
  where ``VCISource`` passes on the few weeks before ``start`` that ``vnstock`` gets.
- An exchange's members come from the listing (stocks only). ``VCISource`` gets the
  same list from KBS — ``vnstock``'s ``Listing()`` defaults to it.
- Corporate actions are read across pages; ``vnstock`` reads the first 50.
- Statements arrive as one row per period, oldest first, keyed by field code
  (``isa1``, ``bsa53``); the labels the line-item maps match on come from the
  per-symbol ``…/financial-statement/metrics`` call. The newest
  :data:`_PERIOD_WINDOW` periods are kept — the window ``VCISource`` has always
  served, which the statement archive and the LLM payload are sized to.
- ``vnai``'s ~20 calls/min cap and its ``SystemExit`` do not exist here. Vietcap's own
  limit is **not known**; requests are spaced :data:`_MIN_INTERVAL_S` apart per
  process, and a 403/418/429 is ``SourceUnavailable``.

In the default chain since 2026-10-06, in the place ``VCISource`` held, after its output
was compared with ``VCISource``'s capability by capability, live. Beyond the ``DataSource``
capabilities it reads the listing, one company's profile and the ICB sector map —
metadata no chain serves, called by the application directly.
"""
import json
import logging
import threading
import time
from datetime import date, datetime, timedelta, timezone

import httpx

from vn_market_data.sources.base import (RATIO_LABELS, DataSource, SourceUnavailable,
                                         SourceUnreachable, report_units)
from vn_market_data.sources.http import get_capped, post_capped
from vn_market_data.sources.vci import (_BALANCE, _BANK_BALANCE, _BANK_INCOME,
                                        _CASHFLOW, _EVENT_CODES, _INCOME, BOOK_LEVELS,
                                        VCISource, _blank_unpopulated_foreign, _num,
                                        _snake)

log = logging.getLogger(__name__)

_TRADING = "https://trading.vietcap.com.vn/api"
_IQ = "https://iq.vietcap.com.vn/api/iq-insight-service"
# The endpoints answer a bare client too; these are what the site's own pages send.
_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Referer": "https://trading.vietcap.com.vn/",
    "Origin": "https://trading.vietcap.com.vn/",
}
# Both hosts sit behind an edge that now and then leaves a request hanging — about one
# in ten on 2026-10-06, in bursts, from a lone client and from `vnstock` alike — while
# a healthy answer takes well under a second. So: a short timeout and a few tries, or
# a statements read (five requests) would fail more often than not.
_TIMEOUT = 6.0
_ATTEMPTS = 3
_VN_TZ = timezone(timedelta(hours=7))

#: Seconds between two requests from this process. Vietcap publishes no limit and none
#: was hit while probing; this is manners, not a measured ceiling.
_MIN_INTERVAL_S = 0.25

#: Symbols per board request, as in ``VCISource``.
_BOARD_BATCH = 400

#: Newest periods kept per statement — the window ``VCISource`` serves.
_PERIOD_WINDOW = 4

#: Corporate actions are asked for this far back, the reach ``vnstock`` defaults to.
_EVENT_YEARS = 10
_EVENT_PAGE = 50
#: Pages followed per symbol. One page is 50 DIV/ISS events, which no listing has
#: reached in ten years; the cap only bounds a response that lies about its length.
_EVENT_MAX_PAGES = 5

#: The label that marks a bank's income statement.
_BANK_MARKER = _BANK_INCOME["net_interest_income"]

# An exchange is answered from the listing, not the group endpoint: Vietcap's ``HOSE``
# group also carries the 24 ETFs and fund certificates trading there, and the callers
# (the broad tier, the market page's turnover universe) mean listed stocks. Caller's
# name → the listing's ``board`` code.
_EXCHANGE_BOARDS = {"HOSE": "HSX", "HNX": "HNX", "UPCOM": "UPCOM"}

# Names Vietcap's group endpoint files an index under, where they differ from the
# name callers use. The indices not listed pass through under their own name.
_GROUP_ALIASES = {
    "VNI": "VNINDEX",
    "VNMID": "VNMIDCAP",
    "VNSML": "VNSMALLCAP",
    "VNALL": "VNALLSHARE",
}
# ...and the ones its chart endpoint files an index candle under.
_INDEX_SYMBOLS = {
    "VNI": "VNINDEX",
    "HNX": "HNXIndex",
    "HNXINDEX": "HNXIndex",
    "UPCOM": "HNXUpcomIndex",
    "UPCOMINDEX": "HNXUpcomIndex",
}


class _Pacer:
    """Spaces this process's requests at least ``interval`` seconds apart."""

    def __init__(self, interval: float):
        self._interval = interval
        self._lock = threading.Lock()
        self._next = 0.0

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            due = max(now, self._next)
            self._next = due + self._interval
        if due > now:
            time.sleep(due - now)


_pacer = _Pacer(_MIN_INTERVAL_S)


def _bar_date(v) -> str | None:
    """Unix seconds (served as a string) → that bar's Vietnam date, or None."""
    try:
        return datetime.fromtimestamp(int(v), tz=_VN_TZ).date().isoformat()
    except (TypeError, ValueError, OverflowError, OSError):
        return None


def _iso(v: str) -> date | None:
    try:
        return date.fromisoformat(str(v)[:10])
    except (TypeError, ValueError):
        return None


def _period_label(row: dict, period: str) -> str | None:
    """The period a statement row names for itself: ``2025``, or ``2026-Q2`` for a
    quarter row. ``lengthReport`` is 1–4 for a quarter and 5 for a fiscal year."""
    try:
        year, length = int(row.get("yearReport")), int(row.get("lengthReport"))
    except (TypeError, ValueError):
        return None
    if period == "quarter":
        return f"{year}-Q{length}" if 1 <= length <= 4 else None
    return str(year)


def _book_side(levels) -> list[list[float]] | None:
    """One side of the book as ``[[price, volume], …]`` best first, cut to
    :data:`BOOK_LEVELS`. Same reading as ``vci._book_side``: an empty level ends the
    side, and a level whose price is text (an auction's "ATO"/"ATC") makes the whole
    side unreadable — None, not ``[]``, which would claim no orders rest there."""
    if not isinstance(levels, list):
        return None
    out: list[list[float]] = []
    for level in levels[:BOOK_LEVELS]:
        if not isinstance(level, dict):
            return None
        raw = level.get("price")
        if isinstance(raw, str) and raw.strip() and _num(raw) is None:
            return None
        price, vol = _num(raw), _num(level.get("volume"))
        if not price or price <= 0 or vol is None or vol < 0:
            break
        out.append([price, vol])
    return out


class VietcapSource(DataSource):
    name = "vietcap"

    #: As ``VCISource.ratio_for_general``: off, the ratio series is read for banks only.
    ratio_for_general = False

    def __init__(self):
        self._client: httpx.Client | None = None
        self._client_lock = threading.Lock()

    # ── Transport ──────────────────────────────────────────────────────────
    def _http(self) -> httpx.Client:
        # One keep-alive connection per source: a statements read is five requests to
        # one host, a board two.
        with self._client_lock:
            if self._client is None:
                self._client = httpx.Client(headers=_HEADERS)
            return self._client

    def _fetch(self, method: str, url: str, *, what: str, params=None, body=None):
        """→ the decoded JSON, or None when Vietcap answered with something that is not
        an answer to read (a 4xx, a non-JSON body). The network, a 5xx and a refusal
        raise — they say nothing about the symbol."""
        for attempt in range(1, _ATTEMPTS + 1):
            _pacer.wait()
            try:
                if method == "POST":
                    status, raw = post_capped(url, json=body, timeout=_TIMEOUT, what=what,
                                              client=self._http())
                else:
                    status, raw = get_capped(url, params=params, timeout=_TIMEOUT, what=what,
                                             client=self._http())
                break
            except httpx.TransportError as e:
                # Only a request that never got an answer is asked again; a status is
                # an answer and is read once.
                if attempt == _ATTEMPTS:
                    log.warning("vietcap: %s network error after %d tries — %s", what, attempt, e)
                    raise SourceUnreachable(what) from None
                log.info("vietcap: %s try %d failed — %s", what, attempt, e)
            except httpx.HTTPError as e:
                log.warning("vietcap: %s network error — %s", what, e)
                raise SourceUnreachable(what) from None
        if status >= 500:
            log.warning("vietcap: %s HTTP %s", what, status)
            raise SourceUnreachable(what)
        if status in (403, 418, 429):     # refused, not answered (418 seen 2026-10-06)
            log.warning("vietcap: %s HTTP %s — refused", what, status)
            raise SourceUnavailable(what)
        if status != 200:
            log.warning("vietcap: %s HTTP %s — no data", what, status)
            return None
        try:
            return json.loads(raw)
        except ValueError:
            log.warning("vietcap: %s non-JSON body", what)
            return None

    def _iq(self, path: str, *, what: str, params=None):
        """The ``data`` of an IQ answer; None when it is null (how the service says
        "no such listing") or there was no answer to read. An envelope that reports
        failure is unavailability — it is not a statement about the symbol."""
        j = self._fetch("GET", f"{_IQ}{path}", what=what, params=params)
        if j is None:
            return None
        if not isinstance(j, dict) or "data" not in j:
            raise SourceUnavailable(f"{what}: unexpected envelope")
        if j.get("successful") is False:
            raise SourceUnavailable(f"{what}: {j.get('msg') or j.get('code')}")
        return j["data"]

    # ── OHLCV ──────────────────────────────────────────────────────────────
    def get_ohlcv(self, symbol, start, end, *, is_index=False):
        symbol = symbol.strip().upper()
        if not symbol:
            return []
        today = datetime.now(tz=_VN_TZ).date()
        d0 = _iso(start) or today - timedelta(days=730)
        d1 = _iso(end) or today
        if d0 > d1:
            return []
        # The endpoint counts bars back from `to`; weekdays in the window is the most
        # sessions it can hold, so asking for that many (+1) always reaches `start`.
        days = (d1 - d0).days + 1
        weekdays = sum(1 for i in range(days) if (d0 + timedelta(days=i)).weekday() < 5)
        to = int(datetime(d1.year, d1.month, d1.day, tzinfo=_VN_TZ).timestamp()) + 86400
        wire = _INDEX_SYMBOLS.get(symbol, symbol) if is_index else symbol
        j = self._fetch("POST", f"{_TRADING}/chart/OHLCChart/gap-chart",
                        what=f"{symbol} OHLCV",
                        body={"timeFrame": "ONE_DAY", "symbols": [wire], "to": to,
                              "countBack": weekdays + 1})
        if not isinstance(j, list) or not j or not isinstance(j[0], dict):
            return []                      # `[]` is how an unknown symbol answers

        def arr(key):
            val = j[0].get(key)
            return val if isinstance(val, list) else []

        t, o, h, l, c, v = (arr(k) for k in "tohlcv")
        lo, hi = d0.isoformat(), d1.isoformat()
        out: list[dict] = []
        for i in range(len(t)):
            bar = _bar_date(t[i])
            if bar is None or not lo <= bar <= hi:
                continue
            oi, hi_, li, ci = (_num(a[i]) if i < len(a) else None for a in (o, h, l, c))
            vol = (_num(v[i]) if i < len(v) else None) or 0.0
            if is_index:
                # The index keeps rows with a close even if o/h/l are missing.
                if ci is None:
                    continue
            elif None in (oi, hi_, li, ci):
                continue
            out.append({"date": bar, "open": oi, "high": hi_, "low": li, "close": ci,
                        "volume": vol})
        out.sort(key=lambda r: r["date"])
        return out

    # ── Price board ────────────────────────────────────────────────────────
    def get_board(self, symbols):
        symbols = [str(s).strip().upper() for s in symbols if str(s).strip()]
        if not symbols:
            return {}
        out: dict[str, dict] = {}
        for i in range(0, len(symbols), _BOARD_BATCH):
            out.update(self._board_batch(symbols[i:i + _BOARD_BATCH]))
        # Decided over the whole board: a trailing chunk can be too small to judge.
        return _blank_unpopulated_foreign(out)

    def _board_batch(self, symbols: list[str]) -> dict[str, dict]:
        j = self._fetch("POST", f"{_TRADING}/price/symbols/getList", what="price_board",
                        body={"symbols": symbols})
        # Unavailability, never an empty board: `{}` is an authoritative answer that
        # stops the chain and leaves every caller with no quotes at all.
        if not isinstance(j, list):
            raise SourceUnavailable("price_board: no board in the answer")
        out: dict[str, dict] = {}
        unreadable = 0
        for item in j:
            if not isinstance(item, dict):
                unreadable += 1
                continue
            listing = item.get("listingInfo")
            if listing is None:            # the placeholder an unknown symbol gets
                continue
            match, book = item.get("matchPrice"), item.get("bidAsk")
            sym = str(listing.get("symbol") or "").upper() if isinstance(listing, dict) else ""
            if not sym or not isinstance(match, dict):
                unreadable += 1
                continue
            book = book if isinstance(book, dict) else {}
            fbv, fsv = _num(match.get("foreignBuyValue")), _num(match.get("foreignSellValue"))
            net = (fbv or 0) - (fsv or 0) if (fbv is not None or fsv is not None) else None
            tval = _num(match.get("accumulatedValue"))
            out[sym] = {
                "foreign_buy_value":  fbv,
                "foreign_sell_value": fsv,
                "foreign_net_value":  net,
                # Full VND as served. `accumulatedValue` alone is in MILLIONS of VND
                # (see VCISource.get_board) and is scaled up like everything else.
                "ceiling":   _num(listing.get("ceiling")) or 0,
                "floor":     _num(listing.get("floor")) or 0,
                "ref_price": _num(listing.get("refPrice")) or 0,
                "close":     _num(match.get("matchPrice")) or 0,
                "traded_value":  tval * 1_000_000.0 if tval is not None else None,
                "traded_volume": _num(match.get("accumulatedVolume")),
                "vwap":  _num(match.get("avgMatchPrice")) or None,
                "bids":  _book_side(book.get("bidPrices")),
                "asks":  _book_side(book.get("askPrices")),
            }
        if unreadable and not out:
            # Rows, none of them readable = the shape changed under us. A caller must
            # not read that as "these symbols don't trade".
            raise SourceUnavailable("price_board: no readable row")
        return out

    # ── Index constituents ──────────────────────────────────────────────────
    def get_index_constituents(self, group):
        """Members of an index (``VN30``, ``VN100``) in Vietcap's own order, or the
        stocks listed on an exchange (``HOSE``/``HNX``/``UPCOM``). A name it does not
        know answers ``[]``."""
        name = str(group).strip().upper()
        if not name:
            return []
        board = _EXCHANGE_BOARDS.get(name)
        if board is not None:
            members = [m for m in self._listing_rows(f"listing {name}")
                       if m.get("board") == board and m.get("type") == "STOCK"]
            if not members:
                raise SourceUnavailable(f"listing {name}: no stock on board {board}")
        else:
            j = self._fetch("GET", f"{_TRADING}/price/symbols/getByGroup",
                            what=f"group {name}",
                            params={"group": _GROUP_ALIASES.get(name, name)})
            members = j if isinstance(j, list) else []
        out, seen = [], set()
        for m in members:
            sym = str(m.get("symbol") or "").strip().upper() if isinstance(m, dict) else ""
            if sym and sym not in seen:
                seen.add(sym)
                out.append(sym)
        return out

    # ── Listing and classification (beyond the DataSource contract) ─────────
    # Reference data the chain has no capability for: asked of this source by name,
    # never through the adapter, and stored by whoever asks.
    def _listing_rows(self, what: str) -> list[dict]:
        j = self._fetch("GET", f"{_TRADING}/price/symbols/getAll", what=what)
        # The whole listing or nothing: an empty market is not an answer Vietcap can
        # give, and `[]` here would empty the broad tier.
        if not isinstance(j, list) or not j:
            raise SourceUnavailable(f"{what}: no listing in the answer")
        return [m for m in j if isinstance(m, dict)]

    def get_listing(self) -> dict[str, dict]:
        """Every listed instrument: ``{symbol: {"exchange", "name", "type"}}``.
        ``exchange`` is Vietcap's board code (``HSX``/``HNX``/``UPCOM``, also
        ``DELISTED`` and ``BOND``), ``type`` its instrument kind (``STOCK``, ``ETF``,
        ``CW``, …), ``name`` the Vietnamese short name."""
        out: dict[str, dict] = {}
        for m in self._listing_rows("listing"):
            sym = str(m.get("symbol") or "").strip().upper()
            if sym:
                out[sym] = {"exchange": str(m.get("board") or ""),
                            "name": str(m.get("organShortName") or m.get("organName") or ""),
                            "type": str(m.get("type") or "")}
        if not out:
            raise SourceUnavailable("listing: no symbol in the answer")
        return out

    def get_company(self, symbol) -> dict | None:
        """One company's ``{"sector", "name"}`` — Vietcap's own English industry label
        ("Technology") and Vietnamese short name. None for a symbol it does not know.
        Not ICB: see :meth:`get_icb_sectors`."""
        sym = str(symbol).strip().upper()
        d = self._iq("/v1/company/details", what=f"{sym} company", params={"ticker": sym})
        if d is None:
            return None
        if not isinstance(d, dict):
            raise SourceUnavailable(f"{sym} company: details is not an object")
        return {"sector": str(d.get("sector") or ""),
                "name": str(d.get("viOrganShortName") or d.get("viOrganName") or "")}

    def get_icb_sectors(self, level: int = 2) -> dict[str, dict]:
        """ICB classification of every company Vietcap lists, at one level (1–4):
        ``{symbol: {"sector", "icb_code"}}``, ``sector`` being ICB's Vietnamese name.
        A company with no name at that level is left out. One request."""
        rows = self._iq("/v2/company/search-bar", what="icb sectors", params={"language": 1})
        if not isinstance(rows, list) or not rows:
            raise SourceUnavailable("icb sectors: no company in the answer")
        key, out = f"icbLv{int(level)}", {}
        for r in rows:
            if not isinstance(r, dict) or not isinstance(r.get(key), dict):
                continue
            sym = str(r.get("code") or "").strip().upper()
            name = str(r[key].get("name") or "").strip()
            if sym and name:
                out[sym] = {"sector": name, "icb_code": str(r[key].get("code") or "").strip()}
        if not out:
            # Every row missing the level is a renamed field, not an unclassified market.
            raise SourceUnavailable(f"icb sectors: no row carries {key}")
        return out

    # ── Financial statements ────────────────────────────────────────────────
    def get_statements(self, symbol, period="year"):
        symbol = symbol.strip().upper()
        if not symbol or period not in ("year", "quarter"):
            return None
        income = self._section(symbol, "INCOME_STATEMENT", period)
        if not income:
            return None                    # no such listing, or nothing filed
        periods = list(income)             # newest first
        titles = self._titles(symbol)
        balance = self._section(symbol, "BALANCE_SHEET", period) or {}
        cashflow = self._section(symbol, "CASH_FLOW", period) or {}

        # Detected off the income statement, as in VCISource, so the ratio request is
        # only made when its rows will be read.
        is_bank = self._field(income, titles, _BANK_MARKER) is not None
        want_ratio = is_bank or self.ratio_for_general
        ratio = self._ratio_rows(symbol) if want_ratio else []
        report_units(5 if want_ratio else 4)

        if not is_bank and self._field(balance, titles, _BANK_BALANCE["loans"]) is not None:
            log.warning("vietcap: %s balance sheet carries bank loans but no Net Interest "
                        "Income line — read as general", symbol)
        return {
            "kind": "bank" if is_bank else "general",
            "periods": periods,
            "statements": {
                "income":   self._lines(income, titles, _BANK_INCOME if is_bank else _INCOME, periods),
                "balance":  self._lines(balance, titles, _BANK_BALANCE if is_bank else _BALANCE, periods),
                "cashflow": self._lines(cashflow, titles, _CASHFLOW, periods),
            },
            "ratio_extra": VCISource._ratio_series_records(ratio),
            "ratio_labels": RATIO_LABELS,
        }

    def _section(self, symbol: str, section: str, period: str) -> dict[str, dict] | None:
        """One statement as ``{period_label: row}``, newest first, cut to the window.
        None when the listing has no such statement. A payload that is there but is
        not statement rows raises: None is cached as "no statements" for a week, and
        a changed shape is not that."""
        what = f"{symbol} {section.lower()}"
        data = self._iq(f"/v1/company/{symbol}/financial-statement", what=what,
                        params={"section": section})
        if data is None:
            return None
        rows = data.get("years" if period == "year" else "quarters") if isinstance(data, dict) else None
        if not isinstance(rows, list):
            raise SourceUnavailable(f"{what}: no {period} rows in the answer")
        labelled: dict[str, dict] = {}
        for row in rows:
            label = _period_label(row, period) if isinstance(row, dict) else None
            if label is not None and label not in labelled:
                labelled[label] = row
        newest = sorted(labelled, reverse=True)[:_PERIOD_WINDOW]
        return {p: labelled[p] for p in newest}

    def _titles(self, symbol: str) -> dict[str, str]:
        """``{field code: English label}`` for this symbol's statement template."""
        what = f"{symbol} statement metrics"
        data = self._iq(f"/v1/company/{symbol}/financial-statement/metrics", what=what)
        if not isinstance(data, dict):
            raise SourceUnavailable(f"{what}: no line-item names in the answer")
        out: dict[str, str] = {}
        for items in data.values():
            for it in items if isinstance(items, list) else ():
                if isinstance(it, dict) and it.get("field") and it.get("titleEn"):
                    out.setdefault(str(it["field"]), str(it["titleEn"]))
        return out

    def _ratio_rows(self, symbol: str) -> list[dict]:
        """The whole ratio series as raw rows; ``[]`` when it cannot be read. The
        network and a refusal are left to the caller, as in VCISource."""
        data = self._iq(f"/v1/company/{symbol}/statistics-financial", what=f"{symbol} ratios")
        if not isinstance(data, list):
            if data is not None:
                log.warning("vietcap: %s ratio series unreadable", symbol)
            return []
        return [r for r in data if isinstance(r, dict)]

    @staticmethod
    def _field(section: dict[str, dict], titles: dict[str, str], label: str) -> str | None:
        """The field code filed under *label* in this statement; first match wins, in
        the order the rows carry their fields."""
        for row in section.values():
            for key in row:
                if titles.get(key) == label:
                    return key
            break                          # every row of a statement has the same fields
        return None

    @classmethod
    def _lines(cls, section, titles, mapping: dict[str, str], periods: list[str]) -> dict[str, dict]:
        """``{alias: {period: value}}`` for the mapped line items."""
        out: dict[str, dict] = {}
        for alias, label in mapping.items():
            field = cls._field(section, titles, label)
            out[alias] = ({p: _num(section[p].get(field)) if p in section else None
                           for p in periods} if field is not None else {})
        return out

    # ── Dividend / corporate-action events ──────────────────────────────────
    def get_events(self, symbol):
        symbol = symbol.strip().upper()
        if not symbol:
            return []
        today = datetime.now(tz=_VN_TZ).date()
        params = {"ticker": symbol,
                  "fromDate": (today - timedelta(days=365 * _EVENT_YEARS)).strftime("%Y%m%d"),
                  "toDate": today.strftime("%Y%m%d"),
                  "eventCode": _EVENT_CODES, "size": _EVENT_PAGE}
        rows: list[dict] = []
        for page in range(_EVENT_MAX_PAGES):
            data = self._iq("/v1/events", what=f"{symbol} events", params={**params, "page": page})
            content = data.get("content") if isinstance(data, dict) else None
            if not isinstance(content, list):
                if page == 0:
                    return []
                # A later page that cannot be read leaves the list short, and a short
                # list reads as events that do not exist.
                raise SourceUnavailable(f"{symbol} events: page {page} unreadable")
            rows.extend({_snake(k): v for k, v in r.items()} for r in content if isinstance(r, dict))
            try:
                pages = int(data.get("totalPages") or 1)
            except (TypeError, ValueError):
                pages = 1
            if page + 1 >= pages or not content:
                break
        report_units(page + 1)
        return VCISource._events_from_rows(symbol, rows)
