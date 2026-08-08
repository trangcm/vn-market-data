"""DataSource — the contract every market-data backend implements.

A source is a *pure fetcher*: it talks to one external provider, normalizes the
result to full VND (the index is left unscaled), and knows nothing about the cache.
The adapter owns the store and the fallback chain.

Return-shape contract — every source normalizes to exactly these shapes, which is what
makes them interchangeable:
- ``get_ohlcv``      → oldest-first ``[{date,open,high,low,close,volume}]``; ``[]`` = no candles.
- ``get_board``      → ``{sym: {foreign_buy_value, foreign_sell_value, foreign_net_value,
                       ceiling, floor, ref_price, close, traded_value, traded_volume}}``
                       (prices and values full VND; traded_* = today's accumulated match).
- ``get_statements`` → ``{kind, periods, statements:{income,balance,cashflow}, ratio_extra}``
                       or ``None``. Raw, source-parsed line items — no ratio math here, so
                       the field→alias map is the only per-source knowledge.
- ``get_events``     → ``[{symbol,type,ex_date,record_date,pay_date,value_per_share,
                       ratio,title,event_code}]``; ``[]`` = none.
- ``get_index_constituents`` → ordered ``["ACB", "BID", …]`` for an index group
                       (e.g. ``"VN30"``); ``[]`` = the group is unknown/empty.
- ``get_market_turnover`` → oldest-first ``[{date,value,matched,put_through,volume}]``
                       for an index (``"VNINDEX"``): the exchange's own money traded per
                       session, in full VND. ``value`` is the **total** — matched plus
                       put-through — which is what "GTGD" means and what summing a price
                       board cannot produce. The last row is the live session.
- ``get_index_live``  → the **in-progress** session's index candle
                       ``{date,open,high,low,close,volume}`` (unscaled), or ``None``
                       when no session is under way. Daily feeds only publish a
                       session's candle after the close, so this is the only way to
                       show the index where it actually is right now.

A genuinely-empty result returns ``[]``/``None`` and must **not** raise. Only a
transient failure (rate-limit / network / 5xx) raises ``SourceUnavailable``, which
tells the adapter to try the next source; a capability a source doesn't implement
raises ``NotSupported`` (the default below) so the chain falls through per-capability.

**This distinction is the one thing to get right when writing a source.** An empty list
stops the chain, because "no dividends" is an answer. A failed fetch that returns ``{}``
instead of raising is therefore not a degraded answer — it is a wrong one, cached and
served as fact.
"""


class NotSupported(Exception):
    """This source does not implement this capability — adapter tries the next source."""


class SourceUnavailable(Exception):
    """Transient unavailability (rate-limited / network / 5xx). The adapter tries the
    next source; if *every* source is unavailable it propagates and the scraper shim
    maps it to its existing ``FeedUnavailable`` (→ HTTP 503 / scheduler stop-and-resume)."""


class DataSource:
    """Base class — implement the capabilities a backend supports; leave the rest
    raising ``NotSupported`` so the adapter falls through to the next source."""

    name: str = "base"

    def get_ohlcv(self, symbol: str, start: str, end: str, *, is_index: bool = False) -> list[dict]:
        raise NotSupported("get_ohlcv")

    def get_board(self, symbols: list[str]) -> dict[str, dict]:
        raise NotSupported("get_board")

    def get_statements(self, symbol: str, period: str = "year") -> dict | None:
        raise NotSupported("get_statements")

    def get_events(self, symbol: str) -> list[dict]:
        raise NotSupported("get_events")

    def get_index_constituents(self, group: str) -> list[str]:
        raise NotSupported("get_index_constituents")

    def get_index_live(self, symbol: str) -> dict | None:
        raise NotSupported("get_index_live")

    def get_market_turnover(self, index: str, start: str, end: str) -> list[dict]:
        raise NotSupported("get_market_turnover")
