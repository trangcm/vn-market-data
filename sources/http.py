"""A GET whose response body cannot grow without bound.

``httpx.get`` buffers the whole body before you can look at any of it, so a source that
calls it is trusting the upstream with the process's memory. None of the endpoints here
authenticate, which cuts both ways: they don't know who we are, and *we don't know who
they are either*. A hijacked host, a captive-portal splash page, a proxy's error page or
a provider having a very bad afternoon are all as likely as the real answer, and none of
them are obliged to be small.

So the read streams and stops at a cap. The cap is on **decoded** bytes — httpx undoes
``Content-Encoding`` as it iterates — so a compressed body cannot slip past it.

An overrun raises ``SourceUnavailable``, deliberately: it is the same class of event as
a timeout (the source did not give us a usable answer), so the chain falls through to
the next source. Returning ``[]`` instead would mean "this symbol has no candles", which
is a fact, and it would be cached and served as one.
"""
import httpx

from vn_market_data.sources.base import SourceUnavailable

# 8 MiB. The largest honest response in this package is ~4,000 daily bars of JSON, which
# is well under 1 MiB; ten years of minute bars, the worst case anything here asks for,
# does not reach 3. The cap is set where nothing legitimate can hit it and no accident
# can cost real memory.
MAX_BYTES = 8 * 1024 * 1024


def get_capped(url: str, *, params=None, headers=None, timeout=None,
               max_bytes: int = MAX_BYTES, what: str = "") -> tuple[int, bytes]:
    """``GET url`` → ``(status_code, body)``, reading at most *max_bytes* of body.

    Network errors propagate as ``httpx.HTTPError``, exactly as ``httpx.get`` raises
    them, so callers keep their existing handling. A non-200 returns its status with an
    empty body — every caller here decides on the status alone and none of them parse an
    error page. *what* names the fetch in the overrun message.
    """
    with httpx.stream("GET", url, params=params, headers=headers, timeout=timeout) as r:
        if r.status_code != 200:
            return r.status_code, b""
        buf = bytearray()
        for chunk in r.iter_bytes():
            buf += chunk
            if len(buf) > max_bytes:
                raise SourceUnavailable(
                    f"{what or url}: response exceeded {max_bytes} bytes — refusing to buffer it")
        return r.status_code, bytes(buf)
