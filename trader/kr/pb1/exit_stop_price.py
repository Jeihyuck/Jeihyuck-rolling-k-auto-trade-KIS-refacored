from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from trader.config import ATR_MULT, INITIAL_STOP_MODE
from trader.positioning.minervini_risk import calc_initial_stop

logger = logging.getLogger(__name__)


def resolve_exit_stop_price(
    *,
    code: str,
    close_px: float | None,
    atr_val: float | None,
    ma20: float | None,
    ma50: float | None,
    stop_price_at_entry: float | None,
    pivot_val: float | None,
    tight_low: float | None,
    entry_style_selected: str,
    order_px: float,
) -> tuple[float | None, str | None, int, str | None]:
    if stop_price_at_entry is not None and stop_price_at_entry > 0 and stop_price_at_entry < order_px:
        logger.info(
            "[STOP][BUILD] code=%s source=entry_metadata close=%s atr=%s stop=%s degraded=0",
            code,
            close_px,
            atr_val,
            float(stop_price_at_entry),
        )
        return float(stop_price_at_entry), "entry_metadata", 0, None

    if pivot_val is not None or tight_low is not None:
        calc_stop = calc_initial_stop(
            pivot=float(pivot_val) if pivot_val is not None else float("nan"),
            tight_low=float(tight_low) if tight_low is not None else None,
            atr=float(atr_val) if atr_val is not None else None,
            mode=INITIAL_STOP_MODE,
            entry=order_px,
            atr_mult=ATR_MULT,
        )
        if calc_stop is not None and pd.notna(calc_stop) and float(calc_stop) > 0 and float(calc_stop) < order_px:
            logger.info(
                "[STOP][BUILD] code=%s source=calc_initial_stop close=%s atr=%s stop=%s degraded=0",
                code,
                close_px,
                atr_val,
                float(calc_stop),
            )
            return float(calc_stop), "calc_initial_stop", 0, None

    if close_px is not None and atr_val is not None and atr_val > 0:
        atr_mult = max(ATR_MULT, 2.2) if entry_style_selected == "MOMENTUM" else ATR_MULT
        stop_price = min(float(close_px - (atr_val * atr_mult)), order_px * 0.99)
        if stop_price > 0:
            logger.info(
                "[STOP][BUILD] code=%s source=atr_fallback close=%s atr=%s stop=%s degraded=0",
                code,
                close_px,
                atr_val,
                stop_price,
            )
            return stop_price, "atr_fallback", 0, None

    ma_candidates = [value for value in (ma20, ma50) if value is not None and value > 0]
    if close_px is not None and close_px > 0 and ma_candidates:
        stop_price = min(min(ma_candidates), float(close_px) * 0.97, order_px * 0.99)
        if stop_price > 0:
            logger.info(
                "[STOP][BUILD] code=%s source=ma_fallback close=%s atr=%s stop=%s degraded=0",
                code,
                close_px,
                atr_val,
                stop_price,
            )
            return stop_price, "ma_fallback", 0, None

    if close_px is not None and close_px > 0:
        stop_price = min(float(close_px) * 0.90, order_px * 0.99)
        if stop_price > 0:
            logger.info(
                "[STOP][BUILD] code=%s source=hard_fallback close=%s atr=%s ma20=%s ma50=%s stop=%s degraded=1 reason=missing_all_primary_inputs",
                code,
                close_px,
                atr_val,
                ma20,
                ma50,
                stop_price,
            )
            return stop_price, "hard_fallback", 1, "missing_all_primary_inputs"

    return None, None, 1, "missing_all_primary_inputs"
