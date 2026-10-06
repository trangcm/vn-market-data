# vn-market-data

Cache-first, source-abstracted market data for the Vietnamese stock market
(HOSE / HNX / UPCOM).

```python
from vn_market_data import get_ohlcv, get_board

candles = get_ohlcv("HPG", lookback_days=365)   # [{date, open, high, low, close, volume}, ...]
board   = get_board(["HPG", "VNM"])             # foreign flow, limit bands, traded value
```

No setup. The first call creates a SQLite file (`./vn_market_data.db`) and backfills it;
every call after that is served from disk and only the new session's tail is fetched.

Not on PyPI — install from the repository, at a tag:

```
pip install "vn-market-data @ git+https://github.com/trangcm/vn-market-data@v0.3.0"
```

Pin the tag. Without `@v0.3.0` pip takes whatever the default branch happens to be at
that moment, so the same command means something different tomorrow and an install
cannot be reproduced. Releases never move a tag once it is pushed, so a pinned install
is the one that stays what you tested against.

The base install is the whole default chain — DNSE (OHLCV), Vietcap (price board,
statements, corporate actions, the OHLCV fallback), VNDirect (turnover), CafeF (settled
foreign flow) — and is pure `httpx`. The `vci` extra adds only `VCISource`, an opt-in
reader of the same Vietcap endpoints through `vnstock` (and therefore pandas); nothing
in the default chain uses it.

## Why this exists

The free Vietnamese market-data endpoints are individually unreliable. One rate-limits
at ~20 requests a minute. One goes down for a few minutes at a time. One renames a
column between versions. None of them covers everything. Anything you build on top of a
single one of them spends most of its code on that, and still blanks out when the source
has a bad afternoon.

So this package is really three behaviours, and they are the whole point:

**Cache-first, not cache-aside.** Reads come from local SQLite; a source is touched only
on a cold cache or a genuinely stale tail. Once history is banked, recomputing something
over 400 symbols costs no network at all — the provider quota stops deciding how often
you are allowed to think.

**Degrade, don't blank.** A source that fails falls through to the next source, then to
the last stored snapshot inside a bounded staleness window, and only then to nothing.
This is not theoretical: on 2026-07-29 a single `ConnectionError` inside one price-board
call blanked a whole market page — no foreign flow, no traded value, every quote back at
yesterday's close — for a full job cycle. A board an hour old was available the entire
time and was strictly the better answer.

**Empty is an answer.** `[]` from a source *stops* the fallback chain, because "this
symbol has no dividends" is a fact, not a failure. Only a transient failure raises
`SourceUnavailable` and falls through. Blurring those two is how a failed fetch gets
cached and served as truth, and it is the single easiest mistake to make when writing a
new source.

## Failure modes, and what you actually get

| When | You get |
|---|---|
| Cache is warm | Local read. No network. |
| Cache is cold | Full backfill from the head source, written through. |
| New trading day, weekday | Tail top-up only (not a refetch). |
| A corporate action rescaled the history | Detected as a session move beyond the symbol's own price-limit band (read off the last board snapshot), and repaired by refetching every candle held — a tail top-up alone would leave the pre-adjustment bars in place forever. Each seam is tried **once**: a move that survives the refetch was really traded, and is remembered rather than chased. |
| Deeper `lookback_days` than before | Refetch from the new floor, once. Asking *shallower* again is a local read. |
| Listed after the window starts | Everything that exists, and that is the whole answer — not re-probed on every call. |
| Weekend / public holiday | Nothing is fetched — the session will never print. Closures are a measured table (`market_hours.EXCHANGE_HOLIDAYS`), not a rule: VN's are part lunar and part decreed yearly. Past `CALENDAR_THROUGH` the module reverts to treating every weekday as open — ask `calendar_covers(d)` before trusting a session count that far out. |
| Pre-open on a trading day, board **counters** | `board_session()` returns `None`. Prices there are the settled close and fine to read, but the session-to-date accumulators (foreign buy/sell, traded value) have already reset for a day that has not opened — so a caller banking them must not use `session_date()`, which still names yesterday. |
| Head source down | Next source in the chain, transparently. |
| **Every** source down, board | The last stored snapshot, up to 6 h old. Every board entry carries `read_at` (ISO-8601 UTC, when that snapshot was read), so a cached or stale answer says how old it is. |
| Board's foreign columns unpopulated | `None`, not `0` — a board over quorum reading zero on every symbol is a column that is not answering yet, and `0` would bank as a measurement. Per-symbol zeros on a live board are left alone: those are facts. |
| Every source down, past 6 h | The symbol is **absent** from the result — so you fall back to candles knowingly, rather than being handed a day-old board as live. |
| Every source down, OHLCV | Whatever is banked, plus a logged warning. `SourceUnavailable` propagates only if there was nothing at all. |
| Symbol genuinely has no data | `[]` / `None`, cached negatively so it is not re-hammered. |
| No source implements the call | `NotSupported` — e.g. `get_board` on a base install. A fact about your install, not about the market, so it is not reported as an empty board. |

Two calls deliberately opt **out** of the cache: `get_index_live()` and
`get_market_turnover()`. Both return the session *in progress*, and writing a
half-formed bar into the store would hand every downstream consumer — moving averages,
pattern geometry, backtests — a candle that silently changes underneath them later.
`get_foreign_history()` is uncached for the opposite reason: it is the settled reading
the caller's own per-session table is reconciled against, and a copy of it here would be
a cache of a cache. `get_trade_tape()` is uncached because there is nothing to cache
*against*: the source serves the current (or last closed) session only, so history is
whatever the caller banks each day — check `tape["date"]` before banking it.

## Units

Everything is normalized to **full VND** regardless of which source answered. VCI quotes
equities in thousands and traded value in millions; DNSE matches VCI; index levels are
left unscaled. You should never have to know who answered — that is the contract, and
the `source` column on every stored row is there so you can audit it after the fact.

## API

```python
get_ohlcv(symbol, lookback_days=730, *, is_index=False, ttl_s=...) -> list[dict]
get_index_live(symbol) -> dict | None            # in-progress index candle; uncached
get_market_turnover(index="VNINDEX", lookback_days=420) -> list[dict]
get_foreign_history(symbol, start, end=None) -> (source, list[dict])   # settled khối ngoại; uncached
get_trade_tape(symbol) -> (source, dict | None)  # today's matched prints + initiating side (KBS); uncached, one session only
get_board(symbols, *, ttl_s=..., stale_ttl_s=...) -> dict[str, dict]
get_statements(symbol, period="year", *, ttl_s=...) -> dict | None
get_statement_history(symbol, period="year", *, max_periods=None, ttl_s=...) -> dict | None
banked_periods(symbol, period="year") -> list[str]   # store-only; touches no source
get_events(symbol, *, ttl_s=...) -> list[dict]   # dividends / corporate actions
get_index_constituents(group="VN30", *, ttl_s=...) -> list[str]
record_source_calls()                            # context manager → list[SourceCall]
```

Every TTL is a keyword argument with the module default, so a caller who needs a
different cadence overrides it per call rather than reaching into the module.

`get_market_turnover` is worth singling out: it returns the exchange's own money traded
per session — matched **plus** put-through (thoả thuận) — which is the "GTGD" figure
every terminal quotes. Summing a price board over the whole exchange lands 15–20% short
of it, because block deals agreed off the order book never touch the board.

`get_statement_history` is the other one to know about, because it does something the
source cannot. VCI answers `get_statements` with the latest **four** periods and no more,
and the cache holds exactly what the source returned — so fetching quarterly statements
every week for a year still leaves you with four quarters, and a quarterly series that
can never contain its own year-ago comparable. Every fetch therefore also banks each
period it saw as its own row, and `get_statement_history` reads across those: the window
grows with the number of passes rather than staying fixed at the source's. It refreshes
through the normal cache first, so the current period is exactly as fresh as
`get_statements` would give, and falls back to the live payload while the archive is
still cold. Two things it deliberately does not do: `ratio_extra` is not archived (the
source re-serves the whole dated series — VCI's back to 2018 — on every fetch, keyed by
the period each value belongs to, so a per-period copy would add nothing), and nothing
is back-filled from before you started
— history only reachable forward has to be started before it is needed.

`record_source_calls` answers the question the cache makes hard: *what did that pass
actually cost the source?* Most reads never leave SQLite, so a loop over 90 symbols may
make 5 requests or 360, and against a rate-limited source (VCI's guest tier) that is the
number that decides whether the pass finishes. Inside the block every request that
reaches a source is appended as a `SourceCall(source, method, outcome, symbol)` — cache
hits are not — with `outcome` one of `"ok"`, `"unavailable"` (down or out of quota; the
chain moved on) or `"error"`. So a trip is not inferred from a short pass, it is the
first `"unavailable"` in the list, and the symbol it happened on is on the record.
Where a source meters one call as several requests it says so on `SourceCall.units`
(`None` when it reported nothing): VCI's statements are **3** requests for a non-bank and
**4** for a bank, because the ratio series — read only for a bank's NPL/CASA/CAR/NIM — is
fetched for banks only, so a non-bank payload's `ratio_extra` is `{}`. Set
`VCISource.ratio_for_general = True` to fetch it for every symbol:

```python
from vn_market_data import get_statements, record_source_calls

with record_source_calls() as calls:
    for s in symbols:
        get_statements(s)
tripped = next((c for c in calls if c.outcome == "unavailable"), None)
```

It counts requests, not a source's metering units (one Vietcap statements read is
several requests), and it sees only this context — quota spent by another task or another
process against the same budget is invisible to it. Scoped with `contextvars`, so concurrent tasks don't mix and
`asyncio.to_thread` carries it into the worker; recorders nest, the outer one keeping
the total.

## Sources

| Source | Capabilities | Notes |
|---|---|---|
| `DNSESource` | OHLCV, live index | DNSE Entrade. Open, no auth, ~30 req/s. Head of the chain. |
| `VietcapSource` | board, statements, events, OHLCV, index members | Vietcap's public endpoints over plain HTTP. No auth; rate limit not published, so requests are spaced 0.25 s apart and a 403/418/429 is a refusal. Also reads the listing, a company profile and the ICB sector map. |
| `VCISource` | board, statements, events, OHLCV | The same endpoints via `vnstock`. **Not in the default chain** — opt in with `set_sources`; needs the `[vci]` extra. ~20 req/min. |
| `VNDirectSource` | market turnover | The only source for matched + put-through. |
| `CafeFSource` | foreign history | Settled per-session foreign buy/sell, ~63 sessions deep. The board only ever shows the live accumulator. Pages ride one keep-alive connection: its edge drops new connections under a crawl. |

The chain is tried head-first *per capability*, so a source implements only what it can
answer and abstains from the rest with `NotSupported`. Add your own without touching the
package:

```python
from vn_market_data import DataSource, build_sources, set_sources

class MyBroker(DataSource):
    name = "mybroker"
    def get_ohlcv(self, symbol, start, end, *, is_index=False):
        ...   # [] means "no candles"; raise SourceUnavailable if the fetch failed

set_sources([MyBroker(), *build_sources()])
```

## Storage

By default: a SQLite file at `$VN_MARKET_DATA_DB`, or `./vn_market_data.db`, with the
schema created on first use. The tables are namespaced `md_*` precisely so they can live
inside a database you already own:

```python
from vn_market_data import set_connection_factory, init_schema

set_connection_factory(my_app.connect)   # zero-arg callable → sqlite3.Connection
with my_app.connect() as conn:
    init_schema(conn)                    # idempotent; call once at startup
```

`init_schema` also does one piece of data repair, not just schema: any `(symbol, period)`
already in the statements cache but absent from the statement archive is banked from what
the cache is holding. That is a one-shot per pair and costs no fetch — an existing cache
carries periods the source may already have stopped serving, and they are lost the first
time their TTL expires.

The package borrows a connection, uses it, and closes it — your application keeps
ownership of the path, the pragmas and the lifecycle. This matters more than it sounds:
opening a second database beside yours splits the cache in half and doubles the WAL.

## What a cached read costs

The pitch is that you can ask for the same candles as often as you like and the network
stays out of it, so the honest thing to publish is what the cache hit costs. Measured by
`benchmarks/bench_store.py` (fake sources, temp SQLite, `synchronous=OFF`) on a Linux
x86_64 box, CPython 3.10 — treat these as an order of magnitude, not a promise:

| Read | Rows | Median |
|---|---|---|
| `get_ohlcv`, 2 years, warm | 522 | **0.98 ms** |
| `get_ohlcv`, 10 years, warm | 2,608 | **3.9 ms** |
| `get_ohlcv`, 2 years, cold (fetch + upsert + read back) | 565 | 2.4 ms |
| `get_board`, 50 symbols, warm | 50 | 0.75 ms |
| `get_board`, 400 symbols (≈ the HOSE listing), warm | 400 | **4.8 ms** |

Roughly 2 µs per candle and 12 µs per symbol of board, both linear in what you asked for:
5× the rows costs 4.0×, 8× the symbols costs 6.4×. The board is one query per symbol by
design — batching it would trade a clear failure mode for a fast one.

The number worth watching over time is the last row of the benchmark: `md_board` is
append-only, and reading the *latest* snapshot for 400 symbols with 120 snapshots each
behind them costs 1.3× reading it with one each. That is the shape you want, and it is
benchmarked rather than assumed because it would degrade silently — the reads stay
correct and just get slower. If it ever climbs, `md_board` wants an index on
`(symbol, ts)` or a retention sweep.

Run it yourself: `python -m vn_market_data.benchmarks.bench_store` (writes JSON to stdout).

## Non-goals

- **Not a client library.** This layers on the providers' raw HTTP endpoints, adding
  caching, fallback and one normalized return shape. For a single one-off fetch in a
  notebook, a client library is the simpler tool.
- **No intraday tick or order-book data.** Daily bars, one live index candle, and a
  price-board snapshot.
- **No indicators, signals or backtesting.** This layer fetches and caches; what you
  compute on top is yours.
- **No redistribution rights.** The package is MIT; the *data* it fetches is not the
  author's to license. What each provider permits is between you and them.
- **Vietnam only.** The unit conventions, limit bands and session model are not
  generalizable, and pretending otherwise would make all three worse.

## Development

```bash
pip install -e ".[dev]"
pytest tests/ -q                  # offline: fake sources, temp SQLite, no network
python examples/quickstart.py     # hits the real endpoints
python examples/diagnose_sources.py HPG   # which source answers, and do they agree?
python -m vn_market_data.benchmarks.bench_store   # timings above, on your machine
```

The editable install is required rather than convenient: this repository's root *is* the
package directory, so a bare `pytest` in a fresh clone reports
`ModuleNotFoundError: vn_market_data`. The suite passes without `vnstock`: the
few tests of the opt-in `VCISource` that need pandas skip on a base install.

If something is wrong, `diagnose_sources.py` is the first thing to run and the most
useful thing to paste into an issue: it fetches the same symbol from every source in the
chain and prints where they disagree.

Patches are welcome, with one caveat worth knowing before you write one: this repository
is a **one-way mirror** of a directory in a larger application, so a pull request opened
here cannot be merged here — the change gets applied upstream with you credited, and
ships in the next release. [`CONTRIBUTING.md`](CONTRIBUTING.md) explains what to do
instead.

## License

MIT. See [LICENSE](LICENSE).
