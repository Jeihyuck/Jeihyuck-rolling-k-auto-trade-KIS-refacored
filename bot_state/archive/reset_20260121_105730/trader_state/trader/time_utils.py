"""거래일/거래 가능 시간 헬퍼."""

from __future__ import annotations

import os
import logging
from datetime import datetime, time, timedelta, timezone

logger = logging.getLogger(__name__)

KST = timezone(timedelta(hours=9))
MARKET_OPEN = time(9, 0)
MARKET_CLOSE = time(15, 20)


def now_kst() -> datetime:
    """현재 KST 시각을 반환."""
    return datetime.now(tz=KST)


def is_trading_day(ts: datetime | None = None) -> bool:
    """주말을 제외한 기본 거래일 여부를 판정.
    FORCE_TRADING_DAY=1 이면 강제로 True 반환 (테스트용)
    """

    ts = ts or now_kst()

    # 🔥 강제 거래일 테스트 모드
    if os.getenv("FORCE_TRADING_DAY") == "1":
        logger.warning(
            "[TIME_UTILS] FORCE_TRADING_DAY=1 → 비거래일 체크 우회 (%s)",
            ts.date(),
        )
        return True

    return ts.weekday() < 5


def is_trading_window(ts: datetime | None = None) -> bool:
    """당일 장중(09:00~15:20) 여부."""

    ts = ts or now_kst()

    # 거래일 여부도 동일하게 FORCE_TRADING_DAY 영향 받음
    if not is_trading_day(ts):
        return False

    return MARKET_OPEN <= ts.time() <= MARKET_CLOSE
