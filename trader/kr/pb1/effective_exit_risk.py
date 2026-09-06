from __future__ import annotations

import json
import logging
import os
from typing import Any

from trader.config import PB1_EFFECTIVE_STOP_CAP_KOSPI_PCT, PB1_EFFECTIVE_STOP_CAP_KOSDAQ_PCT

logger = logging.getLogger(__name__)


def resolve_effective_exit_risk_for_pos(pos: dict[str, Any]) -> dict[str, Any]:
    eff_exit_enabled = os.getenv("PB1_EXISTING_POSITION_EFFECTIVE_EXIT_ENABLED", "1") != "0"
    stop_cap_enabled = os.getenv("PB1_EFFECTIVE_STOP_CAP_ENABLED", "1") != "0"

    meta = pos.get("position_meta") or {}
    if isinstance(meta, str):
        try:
            meta = json.loads(meta)
        except Exception:
            meta = {}

    entry_price = float(
        pos.get("entry_price")
        or pos.get("avg_buy_price")
        or pos.get("avg")
        or 0.0
    )
    raw_stop = float(
        meta.get("initial_stop_price")
        or pos.get("stop_price_at_entry")
        or pos.get("stop_price")
        or pos.get("initial_stop")
        or 0.0
    )

    if entry_price <= 0:
        return {
            "entry_price": entry_price,
            "raw_stop_price": raw_stop,
            "effective_stop_price": raw_stop,
            "raw_r_value": None,
            "effective_r_value": None,
            "stop_cap_price": None,
            "stop_cap_pct": None,
            "effective_applied": False,
            "market": "",
            "code": str(pos.get("code") or ""),
            "reason": "invalid_entry_price",
        }

    raw_r = entry_price - raw_stop if raw_stop > 0 else None

    market = str(pos.get("market") or pos.get("market_code") or "").upper()
    code = str(pos.get("code") or pos.get("pdno") or "").zfill(6)

    kospi_cap = float(os.getenv("PB1_EFFECTIVE_STOP_CAP_KOSPI_PCT", str(PB1_EFFECTIVE_STOP_CAP_KOSPI_PCT)))
    kosdaq_cap = float(os.getenv("PB1_EFFECTIVE_STOP_CAP_KOSDAQ_PCT", str(PB1_EFFECTIVE_STOP_CAP_KOSDAQ_PCT)))

    stop_cap_pct = kospi_cap
    if market in {"KQ", "KOSDAQ", "Q"}:
        stop_cap_pct = kosdaq_cap

    stop_cap_price = entry_price * (1.0 - stop_cap_pct / 100.0)
    effective_stop = raw_stop
    effective_applied = False

    if eff_exit_enabled and stop_cap_enabled:
        if raw_stop <= 0:
            effective_stop = stop_cap_price
            effective_applied = True
        else:
            effective_stop = max(raw_stop, stop_cap_price)
            effective_applied = effective_stop != raw_stop

    effective_r = entry_price - effective_stop if effective_stop > 0 else None
    if effective_r is not None and effective_r <= 0:
        effective_r = raw_r

    logger.info(
        "[EXIT][EFFECTIVE_RISK] code=%s entry=%.2f raw_stop=%s effective_stop=%s "
        "raw_r=%s effective_r=%s cap_pct=%s applied=%s",
        code,
        entry_price,
        raw_stop,
        round(effective_stop, 2) if effective_stop else None,
        round(raw_r, 2) if raw_r is not None else None,
        round(effective_r, 2) if effective_r is not None else None,
        stop_cap_pct,
        int(effective_applied),
    )

    return {
        "entry_price": entry_price,
        "raw_stop_price": raw_stop,
        "effective_stop_price": effective_stop,
        "raw_r_value": raw_r,
        "effective_r_value": effective_r,
        "stop_cap_price": stop_cap_price,
        "stop_cap_pct": stop_cap_pct,
        "effective_applied": effective_applied,
        "market": market,
        "code": code,
        "reason": "ok",
    }
