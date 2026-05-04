# -*- coding: utf-8 -*-
"""US Fills 조회.

당일 체결 내역 조회 및 파싱.
"""
from __future__ import annotations

import logging
import os
from typing import Any

logger = logging.getLogger(__name__)


def get_fills_today(provider: Any | None = None, signal_only: bool = False) -> dict:
    """당일 체결 내역 반환.

    Args:
        provider: USDataProvider 인스턴스 (None이면 offline 기본 provider 사용)
        signal_only: True이면 KIS API 호출 없이 DB 데이터만 사용

    Returns:
        {"status": "OK"|"ERROR", "fills": [...], "error": str|None}
        - status="OK": 성공 (fills는 체결 목록, 빈 리스트도 OK)
        - status="ERROR": 오류 발생 (error에 오류 메시지)
    """
    if provider is None:
        from trader.us.data_provider import USDataProvider
        provider = USDataProvider(offline=True)

    # signal_only 모드: KIS 호출 차단, DB 데이터만 사용
    if signal_only or os.getenv("US_SIGNAL_ONLY") == "1":
        logger.info("[US_FILLS][SIGNAL_ONLY] skipping KIS fill query, using DB-only")
        try:
            from trader.us.db.repos import load_today_fills
            db_fills = load_today_fills()
            logger.info("[US_FILLS][DB_ONLY] count=%d", len(db_fills))
            return {"status": "OK", "fills": db_fills, "error": None}
        except Exception as exc:
            logger.warning("[US_FILLS][DB_ONLY][WARN] %s", exc)
            return {"status": "ERROR", "fills": [], "error": str(exc)}

    if getattr(provider, "_offline", False):
        logger.debug("[US_FILLS][OFFLINE] returning stub fills")
        return {"status": "OK", "fills": [], "error": None}

    try:
        client = provider._get_client()
        raw = client.get_us_fills_today()
        fills = []
        for row in raw:
            fills.append({
                "symbol": row.get("pdno", ""),
                "exchange": row.get("ovrs_excg_cd", ""),
                "side": "BUY" if row.get("sll_buy_dvsn_cd") == "02" else "SELL",
                "qty": int(row.get("ft_ccld_qty", 0) or 0),
                "price": float(row.get("ft_ccld_unpr3", 0) or 0),
                "filled_at": row.get("ord_dt", ""),
                "order_no": row.get("odno", ""),
                "raw": row,
            })
        logger.info("[US_FILLS][OK] count=%d", len(fills))
        return {"status": "OK", "fills": fills, "error": None}
    except Exception as exc:
        from trader.us.execution.kis_us_client import KisUSTemporaryError
        
        error_msg = str(exc)
        if isinstance(exc, KisUSTemporaryError):
            logger.error("[US_FILLS][ERROR][TEMPORARY] %s", error_msg)
        else:
            logger.error("[US_FILLS][ERROR] %s", error_msg)
        
        return {"status": "ERROR", "fills": [], "error": error_msg}
