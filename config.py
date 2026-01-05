# config.py
from typing import Dict

DISPLAY_ORDER = [
    "QQQ", "SPY", "DIA",
    "BTC", "ETH",
    "纳指", "SPX", "道指", "罗素2000",
    "恒科", "恒生", "上证", "日经",
]

MARKETS: Dict[str, Dict] = {
    "纳指": {"name": "纳指", "type": "index", "symbol": "^IXIC", "source": "yf", "calendar": "XNYS"},
    "SPX": {"name": "SPX", "type": "index", "symbol": "^GSPC", "source": "yf", "calendar": "XNYS"},
    "道指": {"name": "道指", "type": "index", "symbol": "^DJI", "source": "yf", "calendar": "XNYS"},
    "罗素2000": {"name": "罗素2000", "type": "index", "symbol": "^RUT", "source": "yf", "calendar": "XNYS"},
    "日经": {"name": "日经", "type": "index", "ak_symbol": "日经225", "source": "ak", "calendar": "XTKS"},
    "恒科": {"name": "恒科", "type": "index", "ak_symbol": "HSTECH", "source": "ak_hk", "calendar": "XHKG"},
    "恒生": {"name": "恒生", "type": "index", "ak_symbol": "恒生指数", "source": "ak", "calendar": "XHKG"},
    "上证": {"name": "上证", "type": "index", "symbol": "000001.SS", "source": "yf", "calendar": "XSHG"},

    "SPY": {"name": "SPY", "type": "stock", "symbol": "SPY", "source": "yf", "calendar": "XNYS"},
    "QQQ": {"name": "QQQ", "type": "stock", "symbol": "QQQ", "source": "yf", "calendar": "XNYS"},
    "DIA": {"name": "DIA", "type": "stock", "symbol": "DIA", "source": "yf", "calendar": "XNYS"},

    "BTC": {"name": "BTC", "type": "crypto", "symbol": "BTC/USDT", "source": "binance"},
    "ETH": {"name": "ETH", "type": "crypto", "symbol": "ETH/USDT", "source": "binance"},
}

CALENDAR_TIMEZONES = {
    "XNYS": "America/New_York",
    "XHKG": "Asia/Hong_Kong",
    "XSHG": "Asia/Shanghai",
    "XTKS": "Asia/Tokyo",
}

CLOSE_BUFFER_MINUTES = {
    "yf": 20,
    "ak": 20,
    "ak_hk": 20,
    "binance": 0,
}