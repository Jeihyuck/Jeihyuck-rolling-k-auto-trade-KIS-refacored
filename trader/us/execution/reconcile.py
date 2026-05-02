# -*- coding: utf-8 -*-
"""US Positions Reconcile.

KIS 잔고와 로컬 DB 잔고를 비교 검증.
"""
from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


def reconcile_positions(provider: Any | None = None) -> dict:
    """잔고 조회 및 reconcile.

    Args:
        provider: USDataProvider 인스턴스

    Returns:
        {"status": "OK"|"MISMATCH"|"ERROR", "positions": [...], ...}
    """
    if provider is None:
        from trader.us.data_provider import USDataProvider
        provider = USDataProvider(offline=True)

    try:
        balance = provider.get_balance()
    except Exception as exc:
        logger.error("[US_RECONCILE][ERROR] balance fetch failed: %s", exc)
        return {"status": "ERROR", "error": str(exc)}

    positions = balance.get("positions", [])
    total_pvs = balance.get("total_pvs", "0")

    logger.info("[US_RECONCILE][OK] position_count=%d total_pvs=%s", len(positions), total_pvs)

    # TODO: DB 기반 포지션과 비교 (US_RECONCILE_MISMATCH 감지)
    return {
        "status": "OK",
        "position_count": len(positions),
        "total_pvs": total_pvs,
        "positions": positions,
    }
