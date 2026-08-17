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
windows in step — nothing here can enforce that.
"""
from datetime import datetime, time, timedelta, timezone

ICT = timezone(timedelta(hours=7))

# HOSE/HNX match 09:00–14:45 (ATO opens 09:00, ATC runs 14:30–14:45) and settle
# put-through deals until 15:00. Padded 15 min either side so the board is warm
# before the first match and the settled close is picked up after the last one.
OPEN  = time(8, 45)
CLOSE = time(15, 15)


def session_live(now: datetime | None = None) -> bool:
    """True while the exchanges can still print a new price.

    Weekdays only — VN public holidays (Tết above all) are not modelled, so a
    holiday still costs a day of polling. That only wastes calls, never
    correctness: the feeds serve the last close and every write is idempotent.
    """
    t = (now or datetime.now(timezone.utc)).astimezone(ICT)
    return t.weekday() < 5 and OPEN <= t.time() < CLOSE


def last_session_close(now: datetime | None = None) -> datetime:
    """The most recent session close that has already passed (tz-aware, ICT).

    Anything fetched after this has already seen the final prints of the last
    session; anything older may be stale.
    """
    t = (now or datetime.now(timezone.utc)).astimezone(ICT)
    # Walk back to the nearest weekday whose close is behind us — never more than
    # three days (Monday pre-open reaches back to Friday).
    for back in range(8):
        d = t.date() - timedelta(days=back)
        if d.weekday() < 5 and (back > 0 or t.time() >= CLOSE):
            return datetime.combine(d, CLOSE, tzinfo=ICT)
    raise AssertionError("unreachable: any 8-day window contains a weekday")


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
