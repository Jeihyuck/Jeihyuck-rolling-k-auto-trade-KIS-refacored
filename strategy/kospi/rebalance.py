from __future__ import annotations

import logging
from typing import Dict, List

from rolling_k_auto_trade_api.adjust_price_to_tick import adjust_price_to_tick
from rolling_k_auto_trade_api.kis_api import get_price_quote
from .universe import kospi_universe

logger = logging.getLogger(__name__)


DEFAULT_TOP_N = 50


def build_target_allocations(total_capital: float, top_n: int = DEFAULT_TOP_N) -> List[Dict[str, float]]:
    universe = kospi_universe(top_n)
    if not universe:
        logger.warning("[KOSPI_CORE] universe empty")
        return []
    weight = 1.0 / len(universe)
    targets: List[Dict[str, float]] = []
    for item in universe:
        code = item.get("code")
        try:
            quote = get_price_quote(code)
            price = float(quote.get("stck_prpr") or 0)
        except Exception:
            logger.exception("[KOSPI_CORE] quote fail for %s", code)
            price = 0.0
        target_val = total_capital * weight
        targets.append(
            {
                "code": code,
                "name": item.get("name") or "",
                "weight": weight,
                "target_value": target_val,
                "last_price": adjust_price_to_tick(price) if price else 0.0,
            }
        )
    return targets
