"""VCI source — the ``vnstock`` (VCI) backend.

The broadest of the three: the only source here for the price board, financial
statements and corporate actions, and the OHLCV fallback behind DNSE. It needs the
optional ``vnstock`` dependency (``pip install vn-market-data[vci]``), which pulls
pandas — hence optional; DNSE and VNDirect are pure ``httpx``.

Everything vnstock-specific is quarantined in this file: the stdout/stderr silencing
(vnstock prints banners), the ``SystemExit`` quota signal (vnai caps calls at ~20/min
and raises ``SystemExit`` rather than an exception) mapped to ``SourceUnavailable``,
the ×1000 equity price scaling, the statement line-item maps and the awkward positional
``ratio()`` parse, and the Vietnamese dividend-title classification. ``vnstock`` itself
is imported lazily inside each call, so importing this module costs nothing.
"""
import contextlib
import importlib.util
import io
import logging
import math

from vn_market_data.sources.base import DataSource, SourceUnavailable

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

# Symbols per price_board call. The market page asks for a whole exchange (~400
# names) in one go; VCI answers 400 at a time, so the request is chunked.
_BOARD_BATCH = 400

# Non-numeric metadata columns present on every statement / ratio DataFrame.
_META_COLS = ("item", "item_en", "item_id")

# ── Statement line items (exact English labels from VCI), keyed by a short alias ──
_INCOME = {
    "net_sales":         "Net sales",
    "gross_profit":      "Gross Profit",
    "operating_profit":  "Operating profit/(loss)",
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
    "cash":           "Cash and cash equivalents",
}
_CASHFLOW = {
    "operating_cash": "Net cash inflows/(outflows) from operating activities",
    "capex":          "Purchases of fixed assets and other long term assets",
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
}

# vnstock event titles are Vietnamese. Classify on lowercase substrings.
_EXCLUDE = ("đhđcđ", "giao dịch nội bộ", "cbcnv", "esop")  # AGM, insider, employee issues
_CASH_HINTS = ("tiền mặt", "cổ tức bằng tiền")
_STOCK_HINTS = ("cổ tức bằng cổ phiếu", "cổ phiếu thưởng", "thưởng cổ phiếu")


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


class VCISource(DataSource):
    name = "vci"

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
            return out
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
            }
        return out

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
                ratio = fin.ratio(period=period, lang="en")
        except SystemExit:
            log.warning("vci: %s statements rate-limited by vnstock quota", symbol)
            raise SourceUnavailable(symbol) from None
        except Exception as e:
            log.warning("vci: %s statements fetch failed — %s", symbol, e)
            return None

        periods = self._periods(income)
        if not periods:
            return None
        periods = periods[:5]  # keep the LLM payload small; 5 periods shows the trend

        is_bank = "Net Interest Income" in income["item_en"].tolist()
        kind = "bank" if is_bank else "general"
        inc = self._lines(income,  _BANK_INCOME  if is_bank else _INCOME,  periods)
        bal = self._lines(balance, _BANK_BALANCE if is_bank else _BALANCE, periods)
        cf  = self._lines(cashflow, _CASHFLOW, periods)
        ratio_extra = self._ratio_map(ratio, periods)
        return {
            "kind": kind,
            "periods": periods,
            "statements": {"income": inc, "balance": bal, "cashflow": cf},
            "ratio_extra": ratio_extra,
        }

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
    def _ratio_map(ratio_df, periods: list[str]) -> dict[str, dict]:
        """{ratio_name: {period: value}} parsed positionally from vnstock's ratio().

        The period columns are mislabeled and the distinct block repeats, so the block
        size is detected from a stable numeric row. The block is ordered oldest-first
        (verified), so it is reversed to align with the statements' newest-first
        `periods`: newest period ↔ last block column."""
        names = ratio_df["item_en"].tolist()

        def row_vals(name):
            return list(ratio_df.iloc[names.index(name), 3:].values) if name in names else None

        probe = next((row_vals(k) for k in ("P/E", "NPL (%)", "ROE (%)")
                      if row_vals(k) is not None), None)
        if not probe:
            return {}
        n = len(probe)
        block = next((k for k in range(1, n) if probe[k] == probe[0]), n)
        cols = min(block, len(periods))

        out: dict[str, dict] = {}
        for i, name in enumerate(names):
            if name in out:  # first occurrence wins
                continue
            vals = list(ratio_df.iloc[i, 3:3 + block].values)  # oldest → newest
            out[name] = {periods[j]: _num(vals[block - 1 - j]) for j in range(cols)}
        return out

    # ── Dividend / corporate-action events ──────────────────────────────────
    def get_events(self, symbol):
        try:
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                from vnstock import Company
                df = Company(symbol=symbol, source="VCI").events()
        except SystemExit:
            log.warning("vci: %s events rate-limited by vnstock quota", symbol)
            raise SourceUnavailable(symbol) from None
        except Exception as e:
            log.warning("vci: %s events fetch failed — %s", symbol, e)
            return []

        cols = set(df.columns)
        required = {"exright_date", "event_title_vi", "value_per_share", "exercise_ratio"}
        if not required.issubset(cols):
            log.warning("vci: %s events missing expected columns (%s)",
                        symbol, sorted(required - cols))
            return []

        out: list[dict] = []
        for _, r in df.iterrows():
            ex_date = _iso_date(r.get("exright_date"))
            if not ex_date:  # the field we exist to provide — skip rows without it
                continue
            title = str(r.get("event_title_vi") or "")
            cls = self._classify(title, _num0(r.get("value_per_share")),
                                 _num0(r.get("exercise_ratio")))
            if not cls:
                continue
            div_type, vps, ratio = cls
            out.append({
                "symbol":          symbol,
                "type":            div_type,
                "ex_date":         ex_date,
                "record_date":     _iso_date(r.get("record_date")),
                "pay_date":        _iso_date(r.get("payout_date")),
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
