"""The default source chain.

The chain is tried head-first *per capability*, so ordering is priority and a source
only needs to implement what it can actually answer. Adding a backend is one import
plus one list entry here — or, without touching the package at all,
``adapter.set_sources([MySource(), *build_sources()])``.
"""
import logging

from vn_market_data.sources.base import DataSource
from vn_market_data.sources.dnse import DNSESource
from vn_market_data.sources.vci import VCISource, vnstock_installed
from vn_market_data.sources.vndirect import VNDirectSource

log = logging.getLogger(__name__)


def build_sources() -> list[DataSource]:
    """The built-in chain: ``[DNSE, VCI, VNDirect]``.

    DNSE's Entrade endpoint is open and fast, so it serves OHLCV first; it implements
    only ``get_ohlcv``, so board / statements / events fall through to VCI, which is
    also the OHLCV fallback. VNDirect sits on the tail because it answers exactly one
    capability nothing else can: the exchange's own market-wide traded value,
    put-through deals included.

    VCI is dropped when the optional ``vnstock`` dependency is missing. The chain still
    serves OHLCV and market turnover, but board / statements / events then have **no
    source at all** and raise ``NotSupported`` — so the omission is logged at WARNING
    rather than swallowed, because losing three capabilities silently reads downstream
    as an empty market rather than a missing install.
    """
    chain: list[DataSource] = [DNSESource()]
    if vnstock_installed():
        chain.append(VCISource())
    else:
        log.warning("VCISource disabled — vnstock is not installed, so the price board, "
                    "financial statements and corporate actions have no source. "
                    "Install it with: pip install vn-market-data[vci]")
    chain.append(VNDirectSource())
    return chain
