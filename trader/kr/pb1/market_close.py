from __future__ import annotations

import logging
from datetime import datetime

logger = logging.getLogger(__name__)


def resolve_market_close(*, now_kst: datetime, market_close_time: str | None, close_auction_end: str | None) -> tuple[datetime, str]:
    fallback_raw = "15:30"
    source = "default"
    raw = ""
    try:
        market_close_env = str(market_close_time or "").strip()
        close_auction_env = str(close_auction_end or "").strip()
        if market_close_env:
            raw = market_close_env
            source = "MARKET_CLOSE_TIME"
        elif close_auction_env:
            raw = close_auction_env
            source = "CLOSE_AUCTION_END"
        else:
            raw = fallback_raw
        try:
            close_time = datetime.strptime(raw, "%H:%M").time()
        except ValueError:
            logger.warning(
                "[PB1][MARKET_CLOSE][INVALID] raw=%s source=%s fallback=%s",
                raw,
                source,
                fallback_raw,
            )
            raw = fallback_raw
            source = "default"
            close_time = datetime.strptime(fallback_raw, "%H:%M").time()
        close_dt = datetime.combine(now_kst.date(), close_time, tzinfo=now_kst.tzinfo)
        logger.info(
            "[PB1][MARKET_CLOSE][RESOLVE] raw=%s source=%s close=%s",
            raw,
            source,
            close_dt.isoformat(),
        )
        return close_dt, raw
    except Exception as exc:
        close_time = datetime.strptime(fallback_raw, "%H:%M").time()
        close_dt = datetime.combine(now_kst.date(), close_time, tzinfo=now_kst.tzinfo)
        logger.warning(
            "[PB1][MARKET_CLOSE][RESOLVE_FAIL] raw=%s source=%s err=%s fallback=%s",
            raw,
            source,
            exc,
            fallback_raw,
        )
        logger.info(
            "[PB1][MARKET_CLOSE][RESOLVE] raw=%s source=%s close=%s",
            fallback_raw,
            "default",
            close_dt.isoformat(),
        )
        return close_dt, fallback_raw
