from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from trader.kis_wrapper import KisAPI
from trader.config import KST

logger = logging.getLogger(__name__)


def evaluate_setup(code: str, kis: Optional[KisAPI], health: Dict[str, Any], state: Dict[str, Any]) -> Dict[str, Any]:
    health = health or {}
    reasons: List[str] = []
    missing: List[str] = []
    setup_ok = True

    if not health.get("ok", False):
        setup_ok = False
        health_reasons = health.get("reasons") or []
        joined = ",".join(str(r) for r in health_reasons)
        reasons.append(f"DATA_HEALTH_BAD:{joined}" if joined else "DATA_HEALTH_BAD")

    positions = (state or {}).get("positions") if isinstance(state, dict) else {}
    pos = (positions or {}).get(str(code).zfill(6), {})
    daily_ctx: Dict[str, Any] = {}
    intra_ctx: Dict[str, Any] = {}

    if isinstance(pos, dict):
        daily_ctx = pos.get("data_health") or {}
        intra_ctx = pos.get("setup") or {}

    if setup_ok and not reasons:
        reasons = ["OK"]
        logger.warning(
            "[SETUP-REASON-GUARD] code=%s setup_ok=%s reasons_was_empty -> injected=%s",
            code,
            setup_ok,
            reasons,
        )
    if setup_ok is False and not reasons:
        reasons = ["UNKNOWN_SETUP_FAIL"]
        logger.warning(
            "[SETUP-REASON-GUARD] code=%s setup_ok=%s reasons_was_empty -> injected=%s",
            code,
            setup_ok,
            reasons,
        )
    if setup_ok is False and not missing:
        missing.append("UNKNOWN_MISSING")

    payload = {
        "ts": datetime.now(KST).isoformat(),
        "setup_ok": setup_ok,
        "missing": missing,
        "reasons": reasons if setup_ok is False or reasons else ["OK"],
        "daily": daily_ctx,
        "intra": intra_ctx,
    }
    return payload
