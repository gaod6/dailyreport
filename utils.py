# utils.py
import pandas as pd
import pytz
import time as time_module
from datetime import date, datetime, time
import exchange_calendars as xcals
from typing import Optional

from config import CALENDAR_TIMEZONES, CLOSE_BUFFER_MINUTES

CALENDARS = {
    "XNYS": xcals.get_calendar("XNYS"),
    "XHKG": xcals.get_calendar("XHKG"),
    "XSHG": xcals.get_calendar("XSHG"),
    "XTKS": xcals.get_calendar("XTKS"),
}

def format_date_display(d: date) -> str:
    weekdays = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]
    return f"{d.strftime('%Y-%m-%d')}（{weekdays[d.weekday()]}）"

def get_local_now(calendar_name: Optional[str]) -> pd.Timestamp:
    tz = CALENDAR_TIMEZONES.get(calendar_name, "UTC")
    return pd.Timestamp.now(tz=tz)

def get_latest_completed_session(calendar_name: Optional[str], source: str) -> Optional[date]:
    if not calendar_name or calendar_name not in CALENDARS:
        return (pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=1)).date()

    cal = CALENDARS[calendar_name]
    local_now = get_local_now(calendar_name)
    now_utc_aware = local_now.tz_convert("UTC")
    now_utc = now_utc_aware.tz_localize(None)

    sessions = cal.sessions
    idx = sessions.searchsorted(now_utc, side="right") - 1
    if idx < 0:
        return None

    session = sessions[idx]
    schedule = cal.schedule.loc[session]
    close_time = schedule.get("market_close") or schedule.get("close")

    if close_time.tzinfo is None:
        close_time = close_time.tz_localize(CALENDAR_TIMEZONES.get(calendar_name, "UTC"))

    close_time_utc = close_time.tz_convert("UTC")
    buffer = pd.Timedelta(minutes=CLOSE_BUFFER_MINUTES.get(source, 30))

    if now_utc_aware >= close_time_utc + buffer:
        return session.date()

    prev = cal.previous_session(session)
    return prev.date() if prev else None

def retry_fetch(func, *args, success_msg: str = "获取成功", max_retries: int = 5, **kwargs):
    delays = [2, 1, 3, 4, 5, 2, 4]
    for attempt in range(1, max_retries + 1):
        try:
            result = func(*args, **kwargs)
            if result is not None and (
                (isinstance(result, pd.DataFrame) and len(result) >= 2) or
                isinstance(result, tuple)
            ):
                print(f"→ {success_msg}（第 {attempt} 次尝试）")
                return result
        except Exception as e:
            print(f"  → 重试第 {attempt} 次失败: {type(e).__name__}: {e}")

        if attempt < max_retries:
            sleep_time = delays[min(attempt - 1, len(delays) - 1)]
            print(f"    等待 {sleep_time} 秒后重试...")
            time_module.sleep(sleep_time)

    print(f"→ 获取彻底失败（已重试 {max_retries} 次）")
    return None