from datetime import time

from trading_calendars import TradingCalendar


class BinanceExchangeCalendar(TradingCalendar):
    """A TradingCalendar for an exchange that's open every minute of every day.
    """
    name = 'Binance'
    tz = 'UTC'
    weekmask = '1111111'
    open_times = (
        (None, time(0)),
    )
    close_times = (
        (None, time(23, 59)),
    )
