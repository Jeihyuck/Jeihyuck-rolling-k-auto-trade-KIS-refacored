# FILE: trader/utils/timeutil.py
from __future__ import annotations
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo

KST = ZoneInfo("Asia/Seoul")


def now_kst() -> datetime:
    return datetime.now(KST)


def hhmm(s: str) -> dtime:
    h, m = s.split(":")
    return dtime(int(h), int(m))


def is_market_open(dt: datetime) -> bool:
    if dt.weekday() >= 5:
        return False
    t = dt.time()
    return dtime(9, 0) <= t <= dtime(15, 30)
