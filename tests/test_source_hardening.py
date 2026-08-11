"""DA-U-04 — what a source does when the upstream answer is not the expected one. [G5]

A source is a trust boundary. These endpoints are unauthenticated, which means we cannot
authenticate *them* either: a hijacked host, a captive-portal page, a proxy error or a
provider mid-incident can all put arbitrary bytes where a candle array should be. The
contract in `sources/base.py` says a source returns normalized rows, an empty result, or
`SourceUnavailable` — never an arbitrary exception in the caller's stack, and never an
unbounded read.

Offline: the HTTP layer is substituted, so nothing here opens a socket.
"""
import json

import httpx
import pytest

from vn_market_data.sources import dnse, http as vmd_http
from vn_market_data.sources.base import SourceUnavailable


# ── the timestamp guard ──────────────────────────────────────────────────────

@pytest.mark.parametrize("bad", ["1720000000", None, {}, 1e30, -1e30, float("nan")])
def test_bar_date_rejects_anything_that_is_not_a_timestamp(bad):
    """Every price field was coerced through `_f`; the timestamp was not, and it is read
    outside the try that covers the fetch. A string, a null or a value outside `time_t`
    raised TypeError/OverflowError straight past `get_ohlcv`'s contract."""
    assert dnse._bar_date(bad) is None


def test_bar_date_labels_a_bar_with_its_vietnam_date():
    # 2026-06-28 23:30 UTC is already the 29th in Vietnam (UTC+7) — the trading date is
    # the local one, which is the whole reason this is not `utcfromtimestamp`.
    assert dnse._bar_date(1782689400) == "2026-06-29"


def _payload(**kw) -> bytes:
    return json.dumps(kw).encode()


def _serve(monkeypatch, body: bytes, status: int = 200):
    """Answer the next fetch with exactly these bytes, without a socket."""
    monkeypatch.setattr(dnse, "get_capped", lambda *a, **k: (status, body))


def test_ohlcv_drops_unreadable_bars_instead_of_raising(monkeypatch):
    """One poisoned timestamp used to take down the whole call — and with it every
    consumer, since the exception escaped the source contract entirely."""
    _serve(monkeypatch, _payload(t=[1782689400, "nope", 1e30, 1782775800],
                                 o=[1, 1, 1, 2], h=[1, 1, 1, 2],
                                 l=[1, 1, 1, 2], c=[1, 1, 1, 2], v=[9, 9, 9, 9]))
    rows = dnse.DNSESource().get_ohlcv("HPG", "2026-06-01", "2026-06-30")
    assert [r["date"] for r in rows] == ["2026-06-29", "2026-06-30"]  # the two readable bars


@pytest.mark.parametrize("body", [b"[]", b'"a string"', b"null", b"not json at all"])
def test_ohlcv_survives_a_payload_of_the_wrong_shape(monkeypatch, body):
    """A JSON body that parses but is not this API's object — an error envelope, a bare
    list — is no-data, not a crash and not a cache-poisoning empty candle set."""
    _serve(monkeypatch, body)
    assert dnse.DNSESource().get_ohlcv("HPG", "2026-06-01", "2026-06-30") == []


def test_ohlcv_survives_arrays_that_are_not_arrays(monkeypatch):
    """`j.get("t") or []` accepts any truthy value; a dict there used to be indexed."""
    _serve(monkeypatch, _payload(t={"a": 1}, c={"a": 1}))
    assert dnse.DNSESource().get_ohlcv("HPG", "2026-06-01", "2026-06-30") == []


def test_index_live_survives_the_same_garbage(monkeypatch):
    _serve(monkeypatch, _payload(t=["nope", None], c=[1, 2]))
    assert dnse.DNSESource().get_index_live("VNINDEX") is None


def test_a_5xx_is_still_unavailability_not_no_data(monkeypatch):
    """The distinction the whole fallback chain rests on: transient failure raises so the
    next source is tried; `[]` would stop the chain and be cached as fact."""
    _serve(monkeypatch, b"", status=503)
    with pytest.raises(SourceUnavailable):
        dnse.DNSESource().get_ohlcv("HPG", "2026-06-01", "2026-06-30")


# ── the response cap ─────────────────────────────────────────────────────────

class _FakeStream:
    """Stands in for httpx's streaming response context manager."""

    def __init__(self, status, chunks):
        self.status_code, self._chunks = status, chunks
        self.consumed = 0

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def iter_bytes(self):
        for c in self._chunks:
            self.consumed += len(c)
            yield c


def _stream(monkeypatch, status, chunks) -> _FakeStream:
    fake = _FakeStream(status, chunks)
    monkeypatch.setattr(vmd_http.httpx, "stream", lambda *a, **k: fake)
    return fake


def test_a_body_under_the_cap_comes_back_whole(monkeypatch):
    _stream(monkeypatch, 200, [b"abc", b"def"])
    assert vmd_http.get_capped("https://example.test", max_bytes=100) == (200, b"abcdef")


def test_an_oversized_body_is_unavailability_not_a_partial_answer(monkeypatch):
    """Truncating would hand the parser half a JSON document, which is a different and
    worse failure. The read stops and the chain falls through to the next source."""
    fake = _stream(monkeypatch, 200, [b"x" * 64] * 100)
    with pytest.raises(SourceUnavailable):
        vmd_http.get_capped("https://example.test", max_bytes=128, what="oversized")


def test_the_cap_stops_reading_rather_than_reading_it_all_first(monkeypatch):
    """The point is memory, so the guard has to be inside the loop: a hostile body must
    cost the cap, not its own size."""
    fake = _stream(monkeypatch, 200, [b"x" * 1024] * 10_000)
    with pytest.raises(SourceUnavailable):
        vmd_http.get_capped("https://example.test", max_bytes=4096)
    assert fake.consumed < 16 * 1024        # a handful of chunks, not all 10 MiB


def test_a_non_200_is_not_read_at_all(monkeypatch):
    """Callers here decide on the status alone; nobody parses an error page, so there is
    no reason to pull one into memory."""
    fake = _stream(monkeypatch, 404, [b"a very long error page" * 1000])
    assert vmd_http.get_capped("https://example.test") == (404, b"")
    assert fake.consumed == 0


def test_network_errors_still_surface_as_httpx_errors(monkeypatch):
    """Sources catch `httpx.HTTPError` and convert; the wrapper must not swallow it or
    change its type, or every source's error handling silently stops matching."""
    def boom(*a, **k):
        raise httpx.ConnectError("no route to host")

    monkeypatch.setattr(vmd_http.httpx, "stream", boom)
    with pytest.raises(httpx.HTTPError):
        vmd_http.get_capped("https://example.test")
