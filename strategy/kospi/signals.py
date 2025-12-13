from __future__ import annotations

import logging
from datetime import datetime, time, timedelta, timezone
from typing import Dict, List

from rolling_k_auto_trade_api.adjust_price_to_tick import adjust_price_to_tick
from rolling_k_auto_trade_api.kis_api import (
    inquire_balance,
    inquire_cash_balance,
    send_order,
    get_price_quote,
)

logger = logging.getLogger(__name__)

KST = timezone(timedelta(hours=9))


def _current_positions() -> Dict[str, Dict[str, float]]:
    pos: Dict[str, Dict[str, float]] = {}
    for row in inquire_balance():
        code = str(row.get("pdno") or row.get("code") or "").zfill(6)
        qty = int(float(row.get("hldg_qty") or row.get("qty") or 0))
        price = float(row.get("pchs_avg_pric") or row.get("avg_price") or 0)
        pos[code] = {"qty": qty, "avg_price": price}
    return pos


def _buy_window_open() -> bool:
    now = datetime.now(tz=KST).time()
    return now >= time(9, 30)


def execute_rebalance(
    targets: List[Dict[str, float]],
    cash: float,
    tag: str,
    *,
    allow_buys: bool = True,
) -> List[Dict[str, str]]:
    fills: List[Dict[str, str]] = []
    positions = _current_positions()
    target_map = {str(t.get("code") or "").zfill(6): t for t in targets}
    all_codes = set(positions.keys()) | set(target_map.keys())
    buys = sells = 0
    available_cash = min(float(cash), float(inquire_cash_balance() or 0))

    before_snapshot = {code: data.get("qty", 0) for code, data in positions.items() if data.get("qty")}
    target_snapshot = {
        code: int(float(payload.get("target_qty") or 0))
        for code, payload in target_map.items()
        if float(payload.get("target_qty") or 0) > 0
    }
    delta_snapshot = {
        code: target_snapshot.get(code, 0) - before_snapshot.get(code, 0)
        for code in sorted(all_codes)
        if before_snapshot.get(code, 0) != target_snapshot.get(code, 0)
    }
    logger.info(
        "%s[KOSPI_CORE][DIFF] before=%s targets=%s delta=%s",
        tag + " " if tag else "",
        before_snapshot,
        target_snapshot,
        delta_snapshot,
    )

    for code in sorted(all_codes):
        current = positions.get(code, {})
        current_qty = int(current.get("qty") or 0)
        target = target_map.get(code)
        target_qty = int(target.get("target_qty") or 0) if target else 0
        weight = float(target.get("weight") or 0) if target else 0.0
        if not target:
            weight = 0.0
        try:
            quote = get_price_quote(code)
            mkt_price = float(quote.get("askp1") or quote.get("stck_prpr") or target.get("last_price") if target else 0)
        except Exception:
            logger.exception("%s quote fail for %s", tag, code)
            mkt_price = float(target.get("last_price") or 0 if target else 0)
        mkt_price = adjust_price_to_tick(mkt_price) if mkt_price else 0.0

        if weight <= 0 or target_qty <= 0:
            delta_qty = -current_qty
        else:
            delta_qty = target_qty - current_qty

        if delta_qty == 0:
            continue
        side = "buy" if delta_qty > 0 else "sell"
        qty = abs(delta_qty)
        if side == "buy":
            if not allow_buys:
                logger.info("%s skip buy %s (buys disabled)", tag, code)
                continue
            if not _buy_window_open():
                logger.info("%s skip buy %s (pre-open window)", tag, code)
                continue
            if mkt_price <= 0:
                logger.info("%s skip buy %s (no price)", tag, code)
                continue
            affordable = int(available_cash // mkt_price)
            if affordable <= 0:
                logger.info("%s skip buy %s (insufficient cash)", tag, code)
                continue
            qty = min(qty, affordable)
        try:
            res = send_order(code, qty=qty, price=None, side=side, order_type="market")
            fills.append({"code": code, "side": side, "qty": qty, "resp": str(res)})
            if side == "buy":
                buys += 1
            else:
                sells += 1
            logger.info("%s %s %s qty=%s price=%s", tag, side.upper(), code, qty, mkt_price)
            if side == "buy":
                available_cash = max(0.0, available_cash - qty * mkt_price)
        except Exception:
            logger.exception("%s order fail %s qty=%s", tag, code, qty)
    invested = sum(float(t.get("target_value") or 0) for t in targets)
    cash_left = max(float(available_cash), 0.0)
    logger.info("%s[KOSPI_CORE][REBALANCE] BUY=%s SELL=%s", tag + " " if tag else "", buys, sells)
    logger.info("%s[KOSPI_CORE][PORTFOLIO] invested=%.0f cash=%.0f", tag + " " if tag else "", invested, cash_left)
    return fills
