"""VN cash-equity session clock — when a price-bearing fetch is worth making.

The exchanges publish nothing between one close and the next open, so a board or
quote fetched in that gap re-reads a number that cannot have changed. Gate a polling
loop on :func:`fetch_due` and it polls freely during the session, makes exactly one
catch-up pass per closed stretch to bank the settled close, then goes quiet.

This is separate from the adapter's TTLs, which answer "is my cache stale?". This
answers the prior question: "could the number have moved at all?" — and outside the
session the answer is no, regardless of how old the cache is.

Times are ICT (UTC+7, no DST), computed against a fixed offset rather than local time
so a machine with no TZ set behaves identically to one in Hanoi.

If you mirror this rule in another language elsewhere in your system, keep the two
windows *and the closure calendar* in step — nothing here can enforce that.
"""
from datetime import date, datetime, time, timedelta, timezone

ICT = timezone(timedelta(hours=7))

# HOSE/HNX match 09:00–14:45 (ATO opens 09:00, ATC runs 14:30–14:45) and settle
# put-through deals until 15:00. Padded 15 min either side so the board is warm
# before the first match and the settled close is picked up after the last one.
OPEN  = time(8, 45)
CLOSE = time(15, 15)

# --------------------------------------------------------------------------- #
# the closure calendar
# --------------------------------------------------------------------------- #
#
# Dates HOSE/HNX did not trade. Written down rather than computed, because most of
# it is not computable: Tết moves with the lunar calendar, and the bridge days
# around Tết, Hùng Kings, Apr 30 – May 1 and Sep 2 are set by decree each year, not
# by a rule.
#
# So it is *measured*, not remembered. Every date below was read back out of bars
# the exchange actually printed, and only where two independently-sourced copies of
# that history agreed a weekday was blank — a bar missing from one feed is that
# feed's gap, not a closed exchange, and writing a feed gap in here as a holiday
# would be the worse error: it teaches the freshness scan that a dead day is normal.
# Regenerate with `scripts/derive_market_holidays.py`, which states which years rest
# on a single source.
EXCHANGE_HOLIDAYS: frozenset[date] = frozenset(date.fromisoformat(d) for d in (
    # 2018
    "2018-09-03", "2018-12-31",
    # 2019
    "2019-01-01", "2019-02-04", "2019-02-05", "2019-02-06", "2019-02-07", "2019-02-08",
    "2019-04-15", "2019-04-29", "2019-04-30", "2019-05-01", "2019-09-02",
    # 2020
    "2020-01-01", "2020-01-23", "2020-01-24", "2020-01-27", "2020-01-28", "2020-01-29",
    "2020-04-02", "2020-04-30", "2020-05-01", "2020-09-02",
    # 2021
    "2021-01-01", "2021-02-10", "2021-02-11", "2021-02-12", "2021-02-15", "2021-02-16",
    "2021-04-21", "2021-04-30", "2021-05-03", "2021-09-02", "2021-09-03",
    # 2022
    "2022-01-03", "2022-01-31", "2022-02-01", "2022-02-02", "2022-02-03", "2022-02-04",
    "2022-04-11", "2022-05-02", "2022-05-03", "2022-09-01", "2022-09-02",
    # 2023
    "2023-01-02", "2023-01-20", "2023-01-23", "2023-01-24", "2023-01-25", "2023-01-26",
    "2023-05-01", "2023-05-02", "2023-05-03", "2023-09-01", "2023-09-04",
    # 2024
    "2024-01-01", "2024-02-08", "2024-02-09", "2024-02-12", "2024-02-13", "2024-02-14",
    "2024-04-18", "2024-04-29", "2024-04-30", "2024-05-01", "2024-09-02", "2024-09-03",
    # 2025
    "2025-01-01", "2025-01-27", "2025-01-28", "2025-01-29", "2025-01-30", "2025-01-31",
    "2025-04-07", "2025-04-30", "2025-05-01", "2025-05-02", "2025-09-01", "2025-09-02",
    # 2026
    "2026-01-01", "2026-01-02", "2026-02-16", "2026-02-17", "2026-02-18", "2026-02-19",
    "2026-02-20", "2026-04-27", "2026-04-30", "2026-05-01", "2026-08-31", "2026-09-01",
    "2026-09-02",
))

#: The window the table can answer for. Below `CALENDAR_FROM` there were no banked
#: bars to measure; above `CALENDAR_THROUGH` the decrees have not been published, so
#: the table cannot know. 2026 closes at year end because VN's statutory holidays run
#: Jan 1 · Tết · Hùng Kings · Apr 30 · May 1 · Sep 2 and the last of those has passed,
#: so nothing further can land in 2026 — but 2027 needs a new run of the script.
#:
#: Outside this window the helpers below fall back to weekdays-are-open, which is
#: what this module did everywhere before the table existed. That fallback is a
#: guess, not an answer, and `calendar_covers()` is how a caller finds out which one
#: it got. Nothing here should ever report an unmodelled holiday as a trading day
#: *silently* — the freshness scan reads exactly that distinction.
CALENDAR_FROM    = date(2018, 6, 1)
CALENDAR_THROUGH = date(2026, 12, 31)

#: Longest run of consecutive non-trading days the table contains (Tết 2019, 9 days
#: with its flanking weekends). Walk-backs are sized off this with headroom, because
#: a loop that gives up mid-Tết reports no last close at all.
MAX_CLOSED_RUN_DAYS = 9


def calendar_covers(d: date) -> bool:
    """Whether the closure table can answer for `d`, rather than guessing at it."""
    return CALENDAR_FROM <= d <= CALENDAR_THROUGH


def is_trading_day(d: date) -> bool:
    """Whether the exchanges opened (or will open) on `d`.

    Weekday **and** not a modelled closure. Past the calendar's range this answers on
    the weekday alone and so reads an unknown holiday as a trading day; that is the
    safe direction for the freshness scan — it over-counts sessions, which makes a
    healthy store look slightly staler, never a stopped one look alive.
    """
    if d.weekday() >= 5:
        return False
    return not (calendar_covers(d) and d in EXCHANGE_HOLIDAYS)


def sessions_between(start: date, end: date) -> int:
    """Trading days strictly after `start`, up to and including `end`.

    The unit anything price-bearing is measured in: HOSE prints nothing over a
    weekend or a holiday, so counting calendar days flags every Monday and counting
    weekdays flags every Tết.
    """
    n, d = 0, start
    while d < end:
        d += timedelta(days=1)
        if is_trading_day(d):
            n += 1
    return n


def session_live(now: datetime | None = None) -> bool:
    """True while the exchanges can still print a new price.

    Trading days only: weekends, and the public holidays the calendar above covers.
    Past `CALENDAR_THROUGH` an unmodelled holiday still costs a day of polling, which
    wastes calls but never correctness — the feeds serve the last close and every
    write is idempotent.
    """
    t = (now or datetime.now(timezone.utc)).astimezone(ICT)
    return is_trading_day(t.date()) and OPEN <= t.time() < CLOSE


def last_session_close(now: datetime | None = None) -> datetime:
    """The most recent session close that has already passed (tz-aware, ICT).

    Anything fetched after this has already seen the final prints of the last
    session; anything older may be stale.
    """
    t = (now or datetime.now(timezone.utc)).astimezone(ICT)
    # Walk back to the nearest trading day whose close is behind us. A weekend needs
    # three days; Tết needs ten, which is why the window is sized off
    # MAX_CLOSED_RUN_DAYS rather than off the weekend that used to be the worst case.
    for back in range(MAX_CLOSED_RUN_DAYS + 7):
        d = t.date() - timedelta(days=back)
        if is_trading_day(d) and (back > 0 or t.time() >= CLOSE):
            return datetime.combine(d, CLOSE, tzinfo=ICT)
    raise AssertionError(
        "no trading day within {} days of {} — the closure calendar claims a "
        "shutdown longer than any on record".format(MAX_CLOSED_RUN_DAYS + 7, t.date()))


def session_date(now: datetime | None = None) -> str:
    """The trading session a *live* read (board, quote, intraday bar) belongs to,
    as an ISO date.

    Inside the session that is today; outside it, the last session that closed.
    This is what dates a board snapshot, and it is deliberately **not** the same
    as the newest date in a daily OHLCV feed: daily feeds only publish a session
    after it closes, so during and just after a session the freshest candle is
    still the *previous* one. Stamp each with its own date rather than assuming
    they agree.
    """
    now = now or datetime.now(timezone.utc)
    if session_live(now):
        return now.astimezone(ICT).date().isoformat()
    return last_session_close(now).date().isoformat()


def board_session(now: datetime | None = None) -> str | None:
    """The session a live *board* snapshot may be **banked** under — or ``None``
    when it belongs to no settled session and must not be banked at all.

    :func:`session_date` answers "which session is this read *about*?", and outside
    the session it answers "the last one that closed". For a **price** that is right
    at every hour: the board keeps serving the settled close until the next open.
    For the board's **accumulators** — foreign buy/sell value, traded value and
    volume, counters that all run from zero at the open — it is right after the
    close and wrong before the next one. The exchange resets them for the coming
    session while `session_date` still names the previous one, so a read taken in
    that gap is a row of zeros wearing yesterday's date, and banking it overwrites a
    complete session with nothing.

    Not hypothetical: on 2026-09-04 a 6-hourly job landed at 08:37 ICT, eight
    minutes before the padded open, and zeroed all 414 symbols of the 2026-09-03
    session — one the 15:54 pass had already banked correctly.

    So this refuses the pre-open stretch of a trading day and defers to
    `session_date` everywhere else. The asymmetry is the whole point: refusing costs
    one skipped write of a number already banked, accepting costs the session.

    Stock-manager has no counterpart to mirror. It banks prices and candles, which a
    pre-open read serves at their settled value; only a counter reads zero there.
    """
    now = now or datetime.now(timezone.utc)
    t = now.astimezone(ICT)
    if is_trading_day(t.date()) and t.time() < OPEN:
        return None
    return session_date(now)


def fetch_due(last_ok: datetime | None, now: datetime | None = None) -> bool:
    """Whether a price-bearing fetch is worth making.

    Always inside the session; outside it, only when the last success predates
    the last close — one catch-up pass per closed stretch (which is also what
    fills a cold cache on an overnight restart), then silence until the open.

    ``last_ok`` of ``None`` means "never fetched", which always fetches: a cold
    cache is worth one call at any hour. There is no minimum-interval argument —
    pacing inside the session belongs to the caller's own scheduler.
    """
    now = now or datetime.now(timezone.utc)
    if session_live(now):
        return True
    if last_ok is None:
        return True
    return last_ok.astimezone(ICT) < last_session_close(now)
