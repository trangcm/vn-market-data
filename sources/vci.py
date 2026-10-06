"""VCI source — the ``vnstock`` (VCI) backend.

Out of the default chain since 2026-10-06: ``VietcapSource`` reads the same Vietcap
endpoints over plain HTTP and holds this source's place. This class stays as an opt-in
(``adapter.set_sources``) and as the other side of the comparison between the two; the
line-item maps, ratio fields and dividend-title rules defined here are the ones
``VietcapSource`` imports. It needs the optional ``vnstock`` dependency
(``pip install vn-market-data[vci]``), which pulls pandas.

Everything vnstock-specific is quarantined in this file: the stdout/stderr silencing
(vnstock prints banners), the ``SystemExit`` quota signal (vnai caps calls at ~20/min
and raises ``SystemExit`` rather than an exception) mapped to ``SourceUnavailable``,
the ×1000 equity price scaling, the statement line-item maps and the ratio series
(read raw, since ``ratio()`` truncates it to 2018), and the Vietnamese dividend-title classification. ``vnstock`` itself
is imported lazily inside each call, so importing this module costs nothing.
"""
import contextlib
import importlib.util
import io
import logging
import math
import re

from vn_market_data.sources.base import (RATIO_LABELS, DataSource, SourceUnavailable,
                                         SourceUnreachable, report_units)

log = logging.getLogger(__name__)


def vnstock_installed() -> bool:
    """Is the optional ``vnstock`` dependency importable? Checked by the registry,
    which drops this source from the default chain rather than letting an ImportError
    surface from the middle of a fetch."""
    return importlib.util.find_spec("vnstock") is not None

# vnstock VCI quotes equity prices in THOUSANDS of VND; scale to full VND so the stored
# series lines up with every other source's. The index level is NOT scaled.
_PRICE_SCALE = 1000.0

# ...and the price board's accumulated (traded) value in MILLIONS of VND.
_VALUE_SCALE = 1_000_000.0

#: Row cap for the raw ratio read — well past the ~45 rows (quarters + fiscal years
#: since 2018) the endpoint serves, so nothing current is cut off.
_RATIO_ROW_CAP = 400

#: The bank ratios read off VCI's series: the four with no statement-derived
#: equivalent. ``Net Interest Margin`` is VCI's own — NII over average *earning*
#: assets — which the statements cannot rebuild (no earning-asset line is mapped).
_RATIO_FIELDS = {
    "NPL (%)":             "npl",
    "CASA Ratio":          "casaRatio",
    "CAR":                 "car",
    "Net Interest Margin": "netInterestMargin",
}

#: Smallest board on which "every symbol reads zero" is evidence about the *feed*
#: rather than about the symbols. Well under a full HOSE listing (~410) and well
#: over any hand-picked watchlist a caller might ask about.
_FOREIGN_QUORUM = 20


def _blank_unpopulated_foreign(board: dict[str, dict]) -> dict[str, dict]:
    """Null the foreign columns when the whole board reads zero.

    The board's foreign values are session-to-date counters, and the feed serves
    them as a literal ``0`` both when a symbol genuinely had no foreign trade and
    when the column has not been populated at all — before the open, or when the
    upstream simply does not fill it. One symbol at zero is a fact; every symbol on
    the exchange at zero on both sides is not a market, it is a column that is not
    answering yet, and the difference matters because ``0`` banks as a measurement
    while ``None`` is skipped.

    Only decided over a board wide enough for the claim to mean something — a
    caller asking about three symbols may legitimately get three zeros. Prices are
    left alone: they are populated on the same board that has not filled this one.
    """
    if len(board) < _FOREIGN_QUORUM:
        return board
    if any(b.get("foreign_buy_value") or b.get("foreign_sell_value") for b in board.values()):
        return board
    for b in board.values():
        b["foreign_buy_value"] = b["foreign_sell_value"] = b["foreign_net_value"] = None
    return board


# Symbols per price_board call. The market page asks for a whole exchange (~400
# names) in one go; VCI answers 400 at a time, so the request is chunked.
_BOARD_BATCH = 400

# Non-numeric metadata columns present on every statement / ratio DataFrame.
_META_COLS = ("item", "item_en", "item_id")

# ── Statement line items (exact English labels from VCI), keyed by a short alias ──
# Sign conventions are VCI's own, passed through unchanged: costs, expenses, tax and
# capex come back NEGATIVE; depreciation is the POSITIVE indirect-method add-back.
# Callers take abs() where they want a magnitude.
_INCOME = {
    "net_sales":         "Net sales",
    "cost_of_sales":     "Cost of sales",
    "gross_profit":      "Gross Profit",
    "interest_expense":  "Interest expenses",
    "operating_profit":  "Operating profit/(loss)",
    "pretax_profit":     "Net accounting profit/(loss) before tax",
    "tax":               "Corporate income tax expenses",
    "net_profit":        "Net profit/(loss) after tax",
    "net_profit_parent": "Attributable to parent company",
    "eps":               "EPS basic (VND)",
}
_BALANCE = {
    "total_assets":   "Total Assets",
    "liabilities":    "Liabilities",
    "equity":         "Owner's Equity",
    "st_borrowings":  "Short-term borrowings",
    "lt_borrowings":  "Long-term borrowings",
    "inventories":    "Inventories, Net",
    "receivables":    "Accounts receivable",
    "payables":       "Trade accounts payable",
    # Fixed assets vs construction in progress: for a company mid-build the split
    # says how much of the asset base is already earning. VCI also carries a legacy
    # "Construction in progress (before 2015)" row that is all zeros — the live one
    # is the unsuffixed label.
    "fixed_assets":   "Fixed assets",
    "cip":            "Construction in progress",
    "cash":           "Cash and cash equivalents",
    "st_investments": "Short-term investments",
    # Par-value share capital, in VND. Divided by the 10,000 VND par that every
    # listed Vietnamese issuer carries, it is a **point-in-time share count** —
    # which the statements otherwise do not give, since the EPS line is per-period
    # and blank for many issuers. "Common shares" rather than "Paid-in capital"
    # because the two are equal wherever both are filed and only this one excludes
    # preferred stock.
    "share_capital":  "Common shares",
}
_CASHFLOW = {
    "operating_cash": "Net cash inflows/(outflows) from operating activities",
    "capex":          "Purchases of fixed assets and other long term assets",
    "depreciation":   "Depreciation and amortization",
    "dividends_paid": "Dividends paid",
}
# Bank variants — different statement structure entirely.
_BANK_INCOME = {
    "net_interest_income":    "Net Interest Income",
    "net_fee_income":         "Net Fee and Commission Income",
    "total_operating_income": "Total Operating Income",
    "operating_expenses":     "General and Admin Expenses",
    "pre_provision_profit":   "Net Operating Profit Before Allowance for Credit Loss",
    "provisions":             "Provision for Credit Losses",
    "net_profit":             "Net profit/(loss) after tax",
    "net_profit_parent":      "Attributable to parent company",
    "eps":                    "EPS basic (VND)",
}
_BANK_BALANCE = {
    "total_assets": "TOTAL ASSETS",
    "equity":       "OWNER'S EQUITY",
    "loans":        "Loans and advances to customers, net",
    "deposits":     "Deposits from customers",
    # Same figure under the name the bank template files it as (see _BALANCE).
    "share_capital": "Charter capital",
}

_CAMEL_RE = re.compile(r"(?<=[a-z0-9])([A-Z])")

# vnstock event titles are Vietnamese. Classify on lowercase substrings.
_EXCLUDE = ("đhđcđ", "giao dịch nội bộ", "cbcnv", "esop")  # AGM, insider, employee issues
_CASH_HINTS = ("tiền mặt", "cổ tức bằng tiền")
_STOCK_HINTS = ("cổ tức bằng cổ phiếu", "cổ phiếu thưởng", "thưởng cổ phiếu")

# The only event codes this feed publishes: DIV = cash dividend, ISS = share issue.
_EVENT_CODES = "DIV,ISS"


def _snake(name: str) -> str:
    """`exrightDate` → `exright_date`, matching the names vnstock's own frame uses."""
    return _CAMEL_RE.sub(r"_\1", name).lower()


def _event_rows(symbol: str) -> list[dict]:
    """One symbol's corporate actions, newest first, as plain dicts.

    Asks VCI for the DIV/ISS codes rather than taking `Company.events()`, whose frame is
    one page of the 50 most recent events of *every* kind — director deals, AGMs,
    listings. On a name traded heavily by insiders that page can hold no corporate
    action at all: MBB's 50 newest events were all director deals, so its 15% stock
    dividend was not in them and the symbol read as one that pays nothing. The filtered
    request costs the same one call and reaches ~10 years back. `_fetch_events` is
    vnstock-private, so a version that moves it falls back to the public frame, which is
    wrong only in the way described above — never in a way that invents an event.
    """
    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        from vnstock import Company
        company = Company(symbol=symbol, source="VCI")
        try:
            raw = company._provider._fetch_events(event_codes=_EVENT_CODES)
        except SystemExit:
            raise
        except Exception as e:
            if _unreachable(e) or _refused(e):       # the public frame would fail alike
                raise
            log.info("vci: %s falling back to the unfiltered event frame — %s", symbol, e)
            df = company.events()
            return [] if df is None or df.empty else df.to_dict("records")
    return [{_snake(k): v for k, v in row.items()} for row in raw]


def _num(v):
    """Coerce a pandas/NaN cell to a float, or None when missing."""
    try:
        f = float(v)
        return None if math.isnan(f) else f
    except (TypeError, ValueError):
        return None


def _num0(v) -> float:
    """Coerce a pandas/NaN cell to a float, 0.0 when missing (dividend values)."""
    n = _num(v)
    return n if n is not None else 0.0


def _iso_date(v) -> str:
    """Trim '2026-05-28T00:00:00' → '2026-05-28'; '' when missing/NaN."""
    if v is None:
        return ""
    s = str(v)
    if not s or s.lower() in ("nan", "nat"):
        return ""
    return s[:10]


def _flatten(col) -> str:
    return "/".join(str(x) for x in col) if isinstance(col, tuple) else str(col)


def pick_col(cols: dict[str, object], *needles: str):
    """Resolve a board column by name fragments, exact leaf name first.

    VCI's board carries families of near-identical names — ``match_price``,
    ``match_price_ato``, ``match_price_atc`` — and the auction ones come *first* in the
    frame. A plain substring search therefore answers "match_price" with the ATO price,
    i.e. every stock frozen at its open (found 2026-07-29: VRE read +0.23% on a +6.81%
    session). So match the leaf name exactly first, and only then fall back to substrings,
    which is what the columns vnstock renames between versions still need.
    """
    for key, real in cols.items():
        if key.rsplit("/", 1)[-1] == needles[-1] and all(n in key for n in needles):
            return real
    for key, real in cols.items():
        if all(n in key for n in needles):
            return real
    return None


#: Resting-order levels kept per side. HOSE publishes three; HNX/UPCOM send ten
#: (verified 2026-09-24, PVS/ACV), cut to three so the depth means the same thing on
#: every exchange.
BOOK_LEVELS = 3


def _book_side(row, cols) -> list[list[float]] | None:
    """``[[price, volume], …]`` best-first for one side of the book, or None when the
    side cannot be read. An empty level (price 0/NaN — no resting order there) ends the
    side: levels are contiguous, so nothing past a hole is real. A level whose price is
    *text* (an auction's "ATO"/"ATC" in place of a number) is not a hole but a reading
    we cannot make, so the whole side is None — ``[]`` would claim no orders rest there,
    which is what a limit lock looks like."""
    if all(pc is None for pc, _ in cols):
        return None
    out: list[list[float]] = []
    for pc, vc in cols:
        raw = row[pc] if pc is not None else None
        if isinstance(raw, str) and raw.strip() and _num(raw) is None:
            return None
        price = _num(raw)
        vol = _num(row[vc]) if vc is not None else None
        if not price or price <= 0 or vol is None or vol < 0:
            break
        out.append([price, vol])
    return out


# vnstock's send_request_direct turns every non-200 into a bare built-in
# ConnectionError carrying only this message — no response object to read.
_HTTP_STATUS = re.compile(r"Failed to fetch data: (\d{3})\b")


# Statuses Vietcap answers when it will not serve the caller, as opposed to having
# nothing for the symbol: 418 is what its edge sent mid-pass on 2026-10-06.
_REFUSAL_STATUSES = (403, 418, 429)


def _io_error(e: BaseException) -> BaseException | None:
    """The I/O error behind a vnstock failure (tenacity's RetryError unwrapped), or
    None when the failure is not I/O at all."""
    last = getattr(e, "last_attempt", None)          # tenacity.RetryError
    if last is not None:
        try:
            e = last.exception() or e
        except Exception:
            pass
    return e if isinstance(e, OSError) else None     # requests' errors are IOErrors


def _http_status(e: BaseException) -> int | None:
    """The HTTP status an I/O error carries, or None for a failure below HTTP."""
    m = _HTTP_STATUS.search(str(e))
    if m:
        return int(m.group(1))
    for x in (e, e.__cause__):                       # a real requests.HTTPError
        status = getattr(getattr(x, "response", None), "status_code", None)
        if status is not None:
            return status
    return None


def _unreachable(e: BaseException) -> bool:
    """Whether a vnstock failure is the network rather than the answer: a connection
    error or timeout, a 5xx, or either behind tenacity's RetryError. A 4xx is an
    answer (an unknown listing, or a refusal — see ``_refused``), and so is anything
    that is not I/O at all."""
    e = _io_error(e)
    if e is None:
        return False
    status = _http_status(e)
    return status is None or status >= 500


def _refused(e: BaseException) -> bool:
    """Whether Vietcap declined to serve the request (a block or a rate limit). That
    says nothing about the symbol, so it must not be cached as "no data"."""
    e = _io_error(e)
    return e is not None and _http_status(e) in _REFUSAL_STATUSES


class VCISource(DataSource):
    name = "vci"

    #: Fetch the ratio series for non-bank symbols too. Off: only a bank's ratios are
    #: read (NPL/CASA/CAR/NIM), and the request is a quarter of a statements call's
    #: quota. A class attribute so a caller can flip it back without a new chain.
    ratio_for_general = False

    # ── OHLCV ──────────────────────────────────────────────────────────────
    def get_ohlcv(self, symbol, start, end, *, is_index=False):
        try:
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                from vnstock import Quote
                df = Quote(symbol=symbol, source="VCI").history(
                    start=start, end=end, interval="1D")
        except SystemExit:  # vnai raises SystemExit (BaseException) when the quota trips
            log.warning("vci: %s OHLCV rate-limited by vnstock quota", symbol)
            raise SourceUnavailable(symbol) from None
        except Exception as e:  # unknown symbol, schema drift, VCI error — no data
            log.warning("vci: %s OHLCV fetch failed — %s", symbol, e)
            return []
        if df is None or len(df) == 0:
            return []

        scale = 1.0 if is_index else _PRICE_SCALE
        out: list[dict] = []
        for _, row in df.iterrows():
            o, h, l, c = (_num(row.get("open")), _num(row.get("high")),
                          _num(row.get("low")), _num(row.get("close")))
            if is_index:
                # The index keeps rows with a close even if o/h/l are missing (unscaled).
                if c is None:
                    continue
                out.append({"date": str(row.get("time"))[:10], "open": o, "high": h,
                            "low": l, "close": c, "volume": _num(row.get("volume")) or 0.0})
            else:
                if None in (o, h, l, c):
                    continue
                out.append({"date": str(row.get("time"))[:10], "open": o * scale,
                            "high": h * scale, "low": l * scale, "close": c * scale,
                            "volume": _num(row.get("volume")) or 0.0})
        out.sort(key=lambda r: r["date"])
        return out

    # ── Price board ────────────────────────────────────────────────────────
    def get_board(self, symbols):
        symbols = list(symbols)
        if len(symbols) > _BOARD_BATCH:
            out: dict[str, dict] = {}
            for i in range(0, len(symbols), _BOARD_BATCH):
                out.update(self.get_board(symbols[i:i + _BOARD_BATCH]))
            # Re-decided over the whole board: a trailing chunk can be too small to
            # judge on its own, and the merged answer is the one the caller acts on.
            return _blank_unpopulated_foreign(out)
        try:
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                from vnstock import Trading
                pb = Trading(source="VCI").price_board(symbols)
        except SystemExit:
            raise SourceUnavailable("price_board") from None
        except Exception as e:
            # Unavailability, never an empty board: `{}` is an authoritative answer that
            # stops the chain and leaves every caller with no quotes at all.
            raise SourceUnavailable(f"price_board: {e}") from None

        cols = {_flatten(c).lower(): c for c in pb.columns}

        def col(*needles):
            return pick_col(cols, *needles)

        c_sym = col("symbol")
        c_fbv = col("foreign_buy_value")
        c_fsv = col("foreign_sell_value")
        c_ceil = col("ceiling")
        c_flr = col("floor")
        c_ref = col("ref_price")
        c_close = col("match", "close_price") or col("match", "match_price") or col("close_price")
        c_tval = col("accumulated_value")
        c_tvol = col("accumulated_volume")
        c_vwap = col("avg_match_price")
        c_depth = {side: [(col(f"{side}_{i}_price"), col(f"{side}_{i}_volume"))
                          for i in range(1, BOOK_LEVELS + 1)]
                   for side in ("bid", "ask")}
        if c_sym is None:
            # Rows but no symbol column = the frame's shape changed under us. Also
            # unavailability: a caller must not read that as "these symbols don't trade".
            if len(pb):
                raise SourceUnavailable("price_board: no symbol column")
            return {}

        out: dict[str, dict] = {}
        for _, r in pb.iterrows():
            sym = str(r[c_sym]).upper()
            fbv = _num(r[c_fbv]) if c_fbv is not None else None
            fsv = _num(r[c_fsv]) if c_fsv is not None else None
            net = (fbv or 0) - (fsv or 0) if (fbv is not None or fsv is not None) else None
            # NOTE: unlike Quote.history (thousands), VCI's price_board already returns
            # prices in FULL VND (verified 2026-06-24: HPG ceil=24,900, ref=23,300), so
            # ceiling/floor/ref/close are NOT scaled. Foreign values are full VND too.
            # accumulated_value is the exception: it is in MILLIONS of VND — verified
            # 2026-07-29 against accumulated_volume × avg_match_price for HPG/VCB/SSI/FPT
            # (match to the cent) — so it is scaled up to full VND like everything else.
            tval = _num(r[c_tval]) if c_tval is not None else None
            out[sym] = {
                "foreign_buy_value":  fbv,
                "foreign_sell_value": fsv,
                "foreign_net_value":  net,
                "ceiling":   (_num(r[c_ceil]) or 0) if c_ceil is not None else None,
                "floor":     (_num(r[c_flr]) or 0) if c_flr is not None else None,
                "ref_price": (_num(r[c_ref]) or 0) if c_ref is not None else None,
                "close":     (_num(r[c_close]) or 0) if c_close is not None else None,
                "traded_value":  tval * _VALUE_SCALE if tval is not None else None,
                "traded_volume": _num(r[c_tvol]) if c_tvol is not None else None,
                # Session VWAP, full VND (verified 2026-09-24: MBB 19,946.98 =
                # accumulated value ÷ volume). 0 before the first match → None.
                "vwap":  (_num(r[c_vwap]) or None) if c_vwap is not None else None,
                "bids":  _book_side(r, c_depth["bid"]),
                "asks":  _book_side(r, c_depth["ask"]),
            }
        return _blank_unpopulated_foreign(out)

    # ── Index constituents ──────────────────────────────────────────────────
    def get_index_constituents(self, group):
        """Members of an index group, in vnstock's own order.

        ``group`` is any name vnstock knows: an index (``VN30``, ``VN100``) or a whole
        exchange (``HOSE`` → the ~400 listed stocks, which is how the market page gets
        its turnover universe). Note ``HOSE``, not the ``HSX`` code the listing uses."""
        try:
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                from vnstock import Listing
                members = Listing().symbols_by_group(group)
        except SystemExit:
            raise SourceUnavailable(f"symbols_by_group({group})") from None
        except Exception as e:
            log.warning("vci: symbols_by_group(%s) failed — %s", group, e)
            return []
        if members is None:
            return []
        out, seen = [], set()
        for m in list(members):
            sym = str(m).strip().upper()
            if sym and sym not in seen:
                seen.add(sym)
                out.append(sym)
        return out

    # ── Financial statements ────────────────────────────────────────────────
    def get_statements(self, symbol, period="year"):
        try:
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                from vnstock import Finance
                fin = Finance(source="VCI", symbol=symbol, period=period)
                income = fin.income_statement(period=period, lang="en")
                balance = fin.balance_sheet(period=period, lang="en")
                cashflow = fin.cash_flow(period=period, lang="en")
                # Detected off the income statement, which is fetched first, so the
                # ratio request — one of four quota units — is only paid when its
                # rows will be read: nothing reads a general symbol's ratios.
                is_bank = "Net Interest Income" in income["item_en"].tolist()
                want_ratio = is_bank or self.ratio_for_general
                ratio = self._ratio_rows(symbol, period) if want_ratio else None
        except SystemExit:
            log.warning("vci: %s statements rate-limited by vnstock quota", symbol)
            raise SourceUnavailable(symbol) from None
        except Exception as e:
            # None means "this listing has no statements" and is cached negatively for
            # STATEMENTS_TTL_S, so a failure to reach VCI must not return it: a dropped
            # connection on 2026-09-28 blanked six symbols with 8 banked quarters each
            # (VND, VIB, VPB, EIB, MDG, S4A) for a week.
            if _unreachable(e):
                log.warning("vci: %s statements unreachable — %s", symbol, e)
                raise SourceUnreachable(symbol) from None
            if _refused(e):
                log.warning("vci: %s statements refused — %s", symbol, e)
                raise SourceUnavailable(symbol) from None
            log.warning("vci: %s statements fetch failed — %s", symbol, e)
            return None
        report_units(4 if want_ratio else 3)

        periods = self._periods(income)
        if not periods:
            return None
        periods = periods[:5]  # keep the LLM payload small; 5 periods shows the trend

        kind = "bank" if is_bank else "general"
        if not is_bank and _BANK_BALANCE["loans"] in balance["item_en"].tolist():
            # A misread bank loses NPL/CASA/CAR silently rather than showing a wrong
            # number, so the one shape that says it happened gets a line.
            log.warning("vci: %s balance sheet carries bank loans but no Net Interest "
                        "Income line — read as general", symbol)
        inc = self._lines(income,  _BANK_INCOME  if is_bank else _INCOME,  periods)
        bal = self._lines(balance, _BANK_BALANCE if is_bank else _BALANCE, periods)
        cf  = self._lines(cashflow, _CASHFLOW, periods)
        return {
            "kind": kind,
            "periods": periods,
            "statements": {"income": inc, "balance": bal, "cashflow": cf},
            "ratio_extra": self._ratio_series(ratio),
            "ratio_labels": RATIO_LABELS,
        }

    @staticmethod
    def _ratio_rows(symbol, period):
        """VCI's whole ratio series as raw rows, or None when it cannot be read.

        ``Finance.ratio()`` cannot be used: the endpoint returns every quarter since
        2018 **oldest first** and vnstock 4.0.4 keeps ``head(4)`` of it, so what it
        hands back is the symbol's 2018 — under column labels that say so, which the
        old positional parse discarded and replaced with the statements' newest
        periods. Raw mode with no row cap is the only way to reach the current rows.
        A vnstock that no longer has this entry point yields None → no ratios, never
        the truncated frame. ``SystemExit`` (the quota) and a network failure are left
        to the caller."""
        try:
            from vnstock.explorer.vci.financial import Finance as _VCIFinance
            fin = _VCIFinance(symbol=symbol, period=period)
            return fin._get_report(report_type="ratio", mode="raw",
                                   limit=_RATIO_ROW_CAP, period=period)
        except SystemExit:
            raise
        except Exception as e:
            if _unreachable(e) or _refused(e):   # the caller's to raise, not "no ratios"
                raise
            log.warning("vci: %s ratio series unreadable — %s", symbol, e)
            return None

    @staticmethod
    def _periods(df) -> list[str]:
        """Period columns (e.g. ['2025','2024',...]) newest-first."""
        cols = [c for c in df.columns if c not in _META_COLS]
        return sorted((str(c) for c in cols), reverse=True)

    @staticmethod
    def _lines(df, mapping: dict[str, str], periods: list[str]) -> dict[str, dict]:
        """{alias: {period: value}} for the mapped line items; first match wins."""
        out: dict[str, dict] = {}
        for alias, label in mapping.items():
            rows = df[df["item_en"] == label]
            out[alias] = {p: _num(rows.iloc[0].get(p)) for p in periods} if len(rows) else {}
        return out

    @staticmethod
    def _ratio_series(raw) -> dict[str, dict]:
        """``{ratio_name: {period: value}}`` keyed by the period **each row names for
        itself** — ``yearReport`` + ``quarter`` — never by position.

        Quarter rows (``RATIO_TTM``, quarter 1–4) label as ``2026-Q2``; the fiscal-year
        rows (``RATIO_YEAR``, quarter 5) as ``2025``, the statements' own annual label.
        So one map serves both bases, and a period the series does not carry is simply
        absent rather than borrowed from its neighbour. VCI fills an unreported cell
        with ``0`` (CAR is published on fiscal-year and Q2 rows only), and none of these
        four can truly be zero for an operating bank, so zero is read as missing."""
        try:
            records = raw.to_dict("records")
        except Exception:        # None, or not a frame — nothing readable
            return {}
        return VCISource._ratio_series_records(records)

    @staticmethod
    def _ratio_series_records(records: list[dict]) -> dict[str, dict]:
        """:meth:`_ratio_series` over plain row dicts — the form the endpoint itself
        serves, and what a source reading it without a frame hands in."""
        out: dict[str, dict] = {name: {} for name in _RATIO_FIELDS}
        for r in records:
            year, q, kind = r.get("yearReport"), r.get("quarter"), r.get("ratioType")
            try:
                year, q = int(year), int(q)
            except (TypeError, ValueError):
                continue
            if kind == "RATIO_YEAR" and q == 5:
                label = str(year)
            elif kind == "RATIO_TTM" and 1 <= q <= 4:
                label = f"{year}-Q{q}"
            else:
                continue
            for name, col in _RATIO_FIELDS.items():
                v = _num(r.get(col))
                if v is not None and v != 0:
                    out[name][label] = v
        return {k: v for k, v in out.items() if v}

    # ── Dividend / corporate-action events ──────────────────────────────────
    def get_events(self, symbol):
        try:
            rows = _event_rows(symbol)
        except SystemExit:
            log.warning("vci: %s events rate-limited by vnstock quota", symbol)
            raise SourceUnavailable(symbol) from None
        except Exception as e:
            # [] means "this symbol has no corporate actions" and is cached for a day.
            if _unreachable(e):
                log.warning("vci: %s events unreachable — %s", symbol, e)
                raise SourceUnreachable(symbol) from None
            if _refused(e):
                log.warning("vci: %s events refused — %s", symbol, e)
                raise SourceUnavailable(symbol) from None
            log.warning("vci: %s events fetch failed — %s", symbol, e)
            return []
        return self._events_from_rows(symbol, rows)

    @classmethod
    def _events_from_rows(cls, symbol: str, rows: list[dict]) -> list[dict]:
        """Raw event rows (snake_case keys) → the normalized events. Split from the
        fetch so every reader of this endpoint classifies by the same rules."""
        # VCI omits a field entirely when no row in the answer carries it, so the set of
        # keys is per-symbol, not a schema. Only `event_title_vi` can be *required*:
        # ELC has never paid cash, so nothing in its answer carries
        # `value_per_share`/`payout_date` at all, and requiring those dropped every
        # event the symbol has — including the 5% stock dividend going ex in three days.
        # The value fields are per-row facts already handled per row (a missing one
        # reads 0 and `_classify` drops that row), so an absent one costs the rows that
        # needed it and nothing else. Still logged: the other reason a field goes
        # missing is an upstream rename.
        #
        # `exright_date` was required here too until 2026-09-16 and is not any more,
        # because a missing one is now a *status* rather than a drop (below). The
        # rename it guarded against is still worth knowing about, so it is logged —
        # but a rename now presents as every event on every symbol reading
        # `announced`, which `/api/monitor.announced_events` counts and flags out
        # loud. Bailing to `[]` instead would present as the dividend calendar being
        # empty, which is the failure this whole change is about: the feed cannot
        # tell "no event" from "nothing came through" and neither can its readers.
        keys = set().union(*(r.keys() for r in rows)) if rows else set()
        if rows and "event_title_vi" not in keys:
            log.warning("vci: %s events carry no event_title_vi — nothing can classify",
                        symbol)
            return []
        if rows and "exright_date" not in keys:
            log.warning("vci: %s events carry no exright_date field at all — every "
                        "event reads as announced-without-a-date", symbol)
        absent = {"value_per_share", "exercise_ratio", "record_date", "payout_date"} - keys
        if rows and absent:
            log.info("vci: %s events carry no %s field — those rows cannot classify",
                     symbol, "/".join(sorted(absent)))

        # An entitlement is announced before its dates are set. VCI carries the rate
        # from the announcement and fills `exright_date` only once the issuer files
        # the record date, so for the days in between there is a row that is a real,
        # classifiable entitlement with nowhere to sit on a calendar. Those rows used
        # to be dropped here, silently and uncounted — which cost the two live cases
        # that found this: DRI's 1,000 VND/share (announced 2026-09-11, ex 2026-09-22)
        # and VPB's 26.04104% bonus (announced 2026-09-10, ex 2026-09-24) were both
        # missing from the feed while VCI had carried their rates for days.
        #
        # They now come through with `ex_date: ""` and `status: "announced"`. The
        # empty string is what every reader downstream already treats as "no date" —
        # it is falsy, it sorts before every real date, and it compares False against
        # a cutoff instead of raising — so relaxing this cannot make an undated row
        # act like a dated one anywhere. `status` is the field to key on to tell
        # "not dated yet" from "no such event", and the one consumer that must not
        # act on an undated row (stock-manager's staging job) keys on the date it
        # would book at, which stays empty.
        out: list[dict] = []
        for r in rows:
            ex_date = _iso_date(r.get("exright_date")) or ""
            title = str(r.get("event_title_vi") or "")
            kind = cls._classify(title, _num0(r.get("value_per_share")),
                                 _num0(r.get("exercise_ratio")))
            if not kind:
                continue
            div_type, vps, ratio = kind
            out.append({
                "symbol":          symbol,
                "type":            div_type,
                "ex_date":         ex_date,
                "status":          "confirmed" if ex_date else "announced",
                "record_date":     _iso_date(r.get("record_date")),
                "pay_date":        _iso_date(r.get("payout_date")),
                # The one date an announced row has. VCI fills it on every event,
                # dated or not, which is what lets an undated one be aged: SHB's
                # 10% stock dividend has read announced since 2026-04-24 without
                # ever being dated, and it must not sit on a two-week calendar
                # beside one declared yesterday.
                "announced_date":  _iso_date(r.get("public_date")) or "",
                "value_per_share": vps,
                "ratio":           ratio,
                "title":           title[:160],
                "event_code":      str(r.get("event_code") or ""),
            })
        return out

    @staticmethod
    def _classify(title: str, value_per_share: float, ratio: float):
        """Return ('CASH'|'STOCK', value_per_share, ratio) or None to drop the row."""
        t = (title or "").casefold()
        if any(x in t for x in _EXCLUDE):
            return None
        if any(x in t for x in _CASH_HINTS):
            return ("CASH", value_per_share, 0.0) if value_per_share > 0 else None
        if any(x in t for x in _STOCK_HINTS):
            return ("STOCK", 0.0, ratio) if ratio > 0 else None
        return None
