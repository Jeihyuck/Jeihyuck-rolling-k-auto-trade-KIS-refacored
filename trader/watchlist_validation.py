"""Watchlist validation helpers for PB1 trading."""
from __future__ import annotations

import logging
import os
from typing import List, Dict, Any

logger = logging.getLogger(__name__)


def validate_final30_strict(
    watchlist: List[Dict[str, Any]],
    intended_live: bool,
) -> None:
    """
    Final 30 엄격 검증 - Trade 단계에서만 30개 강제.
    
    Args:
        watchlist: 후보군 리스트
        intended_live: LIVE 모드 의도 여부
    
    Raises:
        RuntimeError: watchlist가 정확히 30개가 아닐 경우
    """
    size = len(watchlist)
    
    # LIVE 모드에서는 무조건 30개 강제
    if intended_live:
        if size != 30:
            logger.error(
                "[WATCHLIST][FINAL30][FAIL] LIVE mode requires exactly 30 stocks, got %s. "
                "This is a critical safety check to prevent trading with incomplete data. "
                "Run candidate pool builder with final30 pipeline first.",
                size
            )
            raise RuntimeError(f"WATCHLIST_SIZE_INVALID: expected 30, got {size} (LIVE mode)")
    
    # PAPER 모드에서는 경고만
    else:
        if size != 30:
            logger.warning(
                "[WATCHLIST][FINAL30][WARN] PAPER mode watchlist size=%s (expected 30). "
                "This is OK for testing, but LIVE mode will reject this.",
                size
            )


def check_live_fallback_allowed(
    intended_live: bool,
    watchlist_as_of: str,
    requested_as_of: str,
) -> None:
    """
    LIVE 모드에서 fallback 사용 차단.
    
    Args:
        intended_live: LIVE 모드 의도 여부
        watchlist_as_of: watchlist 기준일
        requested_as_of: 요청한 기준일
    
    Raises:
        RuntimeError: LIVE 모드에서 fallback을 사용하려는 경우
    """
    if not intended_live:
        return  # PAPER 모드는 fallback 허용
    
    if watchlist_as_of != requested_as_of:
        logger.error(
            "[WATCHLIST][FALLBACK][BLOCKED] LIVE mode cannot use fallback data. "
            "requested_as_of=%s but watchlist_as_of=%s. "
            "This prevents trading with stale data in production. "
            "Run candidate pool builder for today's date first.",
            requested_as_of,
            watchlist_as_of
        )
        raise RuntimeError(
            f"LIVE_FALLBACK_FORBIDDEN: requested={requested_as_of} actual={watchlist_as_of}"
        )
    
    logger.info(
        "[WATCHLIST][FALLBACK][OK] LIVE mode using exact date match: %s",
        watchlist_as_of
    )
