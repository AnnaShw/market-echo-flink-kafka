from datetime import datetime, timedelta, time as dtime
from zoneinfo import ZoneInfo
import holidays

# Shared by both producers so prices and news always agree on which day
# they describe: live "today" while NYSE is open, otherwise the last
# trading day (so a closed market still shows a full, joinable day).
MARKET_TZ    = ZoneInfo("America/New_York")
MARKET_OPEN  = dtime(9, 30)
MARKET_CLOSE = dtime(16, 0)
NYSE_HOLIDAYS = holidays.financial_holidays("NYSE")


def is_trading_day(d):
    return d.weekday() < 5 and d not in NYSE_HOLIDAYS


def previous_trading_day(d):
    prev = d - timedelta(days=1)
    while not is_trading_day(prev):
        prev -= timedelta(days=1)
    return prev


def is_market_open(now=None):
    now = now or datetime.now(MARKET_TZ)
    return is_trading_day(now.date()) and MARKET_OPEN <= now.time() < MARKET_CLOSE


def reference_date(now=None):
    now = now or datetime.now(MARKET_TZ)
    today = now.date()
    # Weekend, or trading hasn't started yet today — use the last full trading day.
    # After 16:00 on a trading day, today's session is complete, so keep today.
    if not is_trading_day(today) or now.time() < MARKET_OPEN:
        return previous_trading_day(today)
    return today


def seconds_until_close(now=None):
    now = now or datetime.now(MARKET_TZ)
    close = datetime.combine(now.date(), MARKET_CLOSE, tzinfo=MARKET_TZ)
    return max(0.0, (close - now).total_seconds())


def day_bounds(d, start=dtime(0, 0), end=None):
    """Epoch seconds [from, to) for date d between two ET wall-clock times."""
    lo = datetime.combine(d, start, tzinfo=MARKET_TZ)
    hi = (datetime.combine(d, end, tzinfo=MARKET_TZ) if end
          else datetime.combine(d + timedelta(days=1), dtime(0, 0), tzinfo=MARKET_TZ))
    return int(lo.timestamp()), int(hi.timestamp())
