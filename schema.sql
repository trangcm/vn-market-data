-- ── vn-market-data: the md_* cache tables ───────────────────────────────────
-- Owned by this package and created by vn_market_data.init_schema(). They are
-- namespaced `md_` precisely so they can live inside a host application's own
-- database without colliding with its tables.
--
-- The adapter serves reads from here and only tops up the tail from a source on a
-- cache miss, so a provider's per-symbol quota (~20/min for vnstock) stops being
-- the binding constraint on how often you can compute something.
--
-- Figures are stored NORMALIZED (full VND; index unscaled) — scaling is each
-- source's job, so the store and every consumer see one consistent unit. The
-- `source` column records provenance, which is what makes cross-source
-- consistency audits possible after the fact.
CREATE TABLE IF NOT EXISTS md_ohlcv (
    symbol  TEXT NOT NULL,
    date    TEXT NOT NULL,                  -- YYYY-MM-DD
    open    REAL, high REAL, low REAL, close REAL, volume REAL,
    source  TEXT NOT NULL,
    PRIMARY KEY (symbol, date)              -- write-through is an idempotent upsert
);
CREATE INDEX IF NOT EXISTS idx_md_ohlcv_symbol ON md_ohlcv(symbol);

-- Board is a point-in-time snapshot; history is kept so a foreign-flow series accrues.
CREATE TABLE IF NOT EXISTS md_board (
    symbol TEXT NOT NULL,
    ts     TEXT NOT NULL,                   -- snapshot time (UTC ISO8601)
    foreign_buy_value REAL, foreign_sell_value REAL, foreign_net_value REAL,
    ceiling REAL, floor REAL, ref_price REAL, close REAL,
    traded_value REAL, traded_volume REAL,  -- today's accumulated match (full VND / shares)
    source TEXT NOT NULL,
    PRIMARY KEY (symbol, ts)
);
CREATE INDEX IF NOT EXISTS idx_md_board_symbol ON md_board(symbol);

-- Index membership (VN30 today), replace-all per group on refresh; `ordinal` keeps
-- the source's own order. `grp` (not `group`) — GROUP is a SQL keyword.
CREATE TABLE IF NOT EXISTS md_index_members (
    grp        TEXT    NOT NULL,
    symbol     TEXT    NOT NULL,
    ordinal    INTEGER NOT NULL,
    source     TEXT    NOT NULL,
    fetched_at TEXT    NOT NULL,
    PRIMARY KEY (grp, symbol)
);

-- One cached statement set per (symbol, period); payload is the raw, source-parsed
-- line items + ratio_extra (full VND). NULL payload = a negative cache (fetched, no
-- statements) so no-statement symbols aren't re-hammered. No ratio math is done
-- here — only the field→alias mapping is per-source.
CREATE TABLE IF NOT EXISTS md_statements (
    symbol TEXT NOT NULL, period TEXT NOT NULL,   -- 'year' | 'quarter'
    payload TEXT,                                 -- JSON {kind,periods,statements,ratio_extra}
    source TEXT NOT NULL, fetched_at TEXT NOT NULL,
    PRIMARY KEY (symbol, period)
);

-- Dividend/corporate-action events, replace-all per symbol on refresh.
CREATE TABLE IF NOT EXISTS md_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL, event_code TEXT,
    type TEXT, ex_date TEXT, record_date TEXT, pay_date TEXT,
    value_per_share REAL, ratio REAL, title TEXT,
    source TEXT NOT NULL, fetched_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_md_events_symbol ON md_events(symbol);

-- Per-(symbol,kind) fetch freshness for the row-keyed caches (ohlcv/events/board)
-- where the data rows can't carry their own fetched_at and an empty result is
-- indistinguishable from "never fetched". A fresh marker → serve store, zero source calls.
CREATE TABLE IF NOT EXISTS md_fetch_meta (
    symbol     TEXT NOT NULL,
    kind       TEXT NOT NULL,               -- 'ohlcv' | 'events' | 'board'
    fetched_at TEXT NOT NULL,
    source     TEXT NOT NULL,
    floor      TEXT,                        -- oldest date ever *asked* for (ohlcv only)
    PRIMARY KEY (symbol, kind)
);
