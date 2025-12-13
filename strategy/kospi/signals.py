from __future__ import annotations

import logging
from typing import Dict, List

from rolling_k_auto_trade_api.adjust_price_to_tick import adjust_price_to_tick
from rolling_k_auto_trade_api.kis_api import inquire_balance, send_order, get_price_quote

logger = logging.getLogger(__name__)


def _current_positions() -> Dict[str, Dict[str, float]]:
    pos: Dict[str, Dict[str, float]] = {}
    for row in inquire_balance():
        code = str(row.get("pdno") or row.get("code") or "").zfill(6)
        qty = int(float(row.get("hldg_qty") or row.get("qty") or 0))
        price = float(row.get("pchs_avg_pric") or row.get("avg_price") or 0)
        pos[code] = {"qty": qty, "avg_price": price}
    return pos


def execute_rebalance(targets: List[Dict[str, float]], cash: float, tag: str) -> List[Dict[str, str]]:
    fills: List[Dict[str, str]] = []
    positions = _current_positions()
    for target in targets:
        code = str(target.get("code") or "").zfill(6)
        weight = float(target.get("weight") or 0)
        last_price = float(target.get("last_price") or 0)
        if not code or last_price <= 0 or weight <= 0:
            logger.warning("%s skip target %s", tag, target)
            continue
        target_value = float(target.get("target_value") or 0)
        current = positions.get(code, {})
        current_qty = int(current.get("qty") or 0)
        try:
            quote = get_price_quote(code)
            mkt_price = float(quote.get("askp1") or quote.get("stck_prpr") or last_price)
        except Exception:
            logger.exception("%s quote fail for %s", tag, code)
            mkt_price = last_price
        mkt_price = adjust_price_to_tick(mkt_price)
        target_qty = int(target_value // mkt_price) if mkt_price > 0 else 0
        delta_qty = target_qty - current_qty
        if delta_qty == 0:
            continue
        side = "buy" if delta_qty > 0 else "sell"
        qty = abs(delta_qty)
        try:
            res = send_order(code, qty=qty, price=None, side=side, order_type="market")
            fills.append({"code": code, "side": side, "qty": qty, "resp": str(res)})
            logger.info("%s %s %s qty=%s price=%s", tag, side.upper(), code, qty, mkt_price)
        except Exception:
            logger.exception("%s order fail %s qty=%s", tag, code, qty)
    return fills
