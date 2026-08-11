"""vn-market-data — a cache-first, source-abstracted market-data layer for Vietnam.

Vietnamese market data comes from a handful of free endpoints that are individually
unreliable: one rate-limits, one goes down, one silently renames a column, and none of
them covers everything. This package puts a local SQLite cache in front of them and an
ordered fallback chain behind, so a caller asks once and gets an answer:

    from vn_market_data import get_ohlcv, get_board

    candles = get_ohlcv("HPG", lookback_days=365)   # cached; tops up the tail on a new day
    board   = get_board(["HPG", "VNM"])             # last snapshot if every source is down

Three design commitments, because they are what you are actually buying:

- **Cache-first, not cache-aside.** Reads are served from SQLite and only a cold cache
  or a genuinely stale tail reaches a source. Rate limits stop being the binding
  constraint on how often you can compute something.
- **Degrade, don't blank.** A source outage falls through to the next source, then to
  the last stored snapshot within a bounded staleness window, and only then to nothing.
  A page rendering hour-old numbers beats a page rendering none.
- **Empty is an answer.** ``[]`` from a source stops the fallback chain; only a
  *transient failure* raises and falls through. Blurring those two is how a failed fetch
  gets cached as fact.

Figures are normalized to **full VND** across every source (the index is left unscaled),
so consumers never have to know which backend answered.

Sources today: **DNSE** (OHLCV, open and fast), **VCI** via ``vnstock`` (board,
statements, corporate actions; the OHLCV fallback — needs the ``[vci]`` extra), and
**VNDirect** (market-wide traded value including put-through deals, which no price board
can produce). Add your own with :func:`set_sources`.

Also here: :mod:`vn_market_data.market_hours`, the VN session clock. Gate a polling loop
on :func:`fetch_due` and it stops asking the exchange for a number that cannot have
changed since the close — a different question from "is my cache stale?", and the one
that decides whether a fetch is worth making at all.

Storage: by default a SQLite file at ``$VN_MARKET_DATA_DB`` (or ``./vn_market_data.db``),
created on first use. If you already have a database, hand over a connection factory with
:func:`set_connection_factory` and call :func:`init_schema` once — the package will keep
its ``md_*`` tables inside yours rather than opening a second file beside it.

Not a goal: replacing ``vnstock``. This layers on it (and on the raw HTTP endpoints),
adding caching, fallback and one normalized return shape. If you want a single
one-off fetch in a notebook, use vnstock directly — that is what it is good at.
"""
from vn_market_data.adapter import (
    BOARD_STALE_S,
    BOARD_TTL_S,
    EVENTS_TTL_S,
    MEMBERS_TTL_S,
    OHLCV_TTL_S,
    STATEMENTS_TTL_S,
    get_board,
    get_events,
    get_index_constituents,
    get_index_live,
    get_market_turnover,
    get_ohlcv,
    get_sources,
    get_statements,
    set_sources,
)
from vn_market_data.db import (
    connect,
    get_connection_factory,
    init_schema,
    set_connection_factory,
)
from vn_market_data.market_hours import (
    CLOSE,
    ICT,
    OPEN,
    fetch_due,
    last_session_close,
    session_live,
)
from vn_market_data.sources.base import DataSource, NotSupported, SourceUnavailable
from vn_market_data.sources.dnse import DNSESource
from vn_market_data.sources.registry import build_sources
from vn_market_data.sources.vci import VCISource, vnstock_installed
from vn_market_data.sources.vndirect import VNDirectSource

__version__ = "0.1.0"

__all__ = [
    # reads
    "get_ohlcv",
    "get_index_live",
    "get_market_turnover",
    "get_board",
    "get_statements",
    "get_events",
    "get_index_constituents",
    # storage
    "set_connection_factory",
    "get_connection_factory",
    "init_schema",
    "connect",
    # sources
    "DataSource",
    "NotSupported",
    "SourceUnavailable",
    "build_sources",
    "set_sources",
    "get_sources",
    "DNSESource",
    "VCISource",
    "VNDirectSource",
    "vnstock_installed",
    # session clock — "could the number have moved at all?"
    "session_live",
    "fetch_due",
    "last_session_close",
    "OPEN",
    "CLOSE",
    "ICT",
    # default TTLs (each is also a keyword argument on the call it governs)
    "OHLCV_TTL_S",
    "BOARD_TTL_S",
    "BOARD_STALE_S",
    "EVENTS_TTL_S",
    "STATEMENTS_TTL_S",
    "MEMBERS_TTL_S",
    "__version__",
]
