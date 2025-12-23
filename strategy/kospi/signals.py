from __future__ import annotations

import logging
from datetime import datetime, time, timedelta, timezone
from typing import Dict, List, Tuple

from rolling_k_auto_trade_api.adjust_price_to_tick import adjust_price_to_tick
from rolling_k_auto_trade_api.kis_api import (
    inquire_balance,
    inquire_cash_balance,
    send_order,
    get_price_quote,
)
from trader.code_utils import normalize_code

logger = logging.getLogger(__name__)

KST = timezone(timedelta(hours=9))


def _current_positions() -> Dict[str, Dict[str, float | str | None]]:
    pos: Dict[str, Dict[str, float | str | None]] = {}
    for row in inquire_balance():
        code = normalize_code(row.get("pdno") or row.get("code") or "")
        if not code:
            continue
        qty = int(float(row.get("hldg_qty") or row.get("qty") or 0))
        price = float(row.get("pchs_avg_pric") or row.get("avg_price") or 0)
        market = row.get("market") or row.get("mkt") or row.get("market_div")
        pos[code] = {"qty": qty, "avg_price": price, "market": market}
    return pos


def _buy_window_open() -> bool:
    now = datetime.now(tz=KST).time()
    return now >= time(9, 30)


def _split_positions_for_kospi(
    positions: Dict[str, Dict[str, float | str | None]],
    target_map: Dict[str, Dict[str, float]],
) -> Tuple[Dict[str, Dict[str, float | str | None]], List[str]]:
    kospi_positions: Dict[str, Dict[str, float | str | None]] = {}
    excluded: List[str] = []
    for code, payload in positions.items():
        code_key = normalize_code(code)
        if not code_key:
            continue
        market = str(payload.get("market") or "").upper()
        if market in {"KOSPI", "KSE"}:
            kospi_positions[code_key] = payload
            continue
        if market in {"KOSDAQ", "KSQ"}:
            excluded.append(code_key)
            continue
        if code_key in target_map:
            kospi_positions[code_key] = payload
            continue
        excluded.append(code_key)
    return kospi_positions, excluded


def execute_rebalance(
    targets: List[Dict[str, float]],
    cash: float,
    tag: str,
    *,
    allow_buys: bool = True,
) -> List[Dict[str, str]]:
    fills: List[Dict[str, str]] = []
    positions = _current_positions()
    target_map = {normalize_code(t.get("code") or ""): t for t in targets if normalize_code(t.get("code") or "")}
    kospi_positions, excluded_positions = _split_positions_for_kospi(
        positions, target_map
    )
    all_codes = set(kospi_positions.keys()) | set(target_map.keys())
    buys = sells = 0
    available_cash = min(float(cash), float(inquire_cash_balance() or 0))

    before_snapshot = {
        code: data.get("qty", 0)
        for code, data in kospi_positions.items()
        if data.get("qty")
    }
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
    if excluded_positions:
        logger.warning(
            "%s[KOSPI_CORE][UNKNOWN_HOLDINGS] excluded=%s (targets_fallback=%s)",
            tag + " " if tag else "",
            sorted(excluded_positions),
            sorted(set(excluded_positions) & set(target_map.keys())),
        )
    logger.info(
        "%s[KOSPI_CORE][DIFF] before=%s targets=%s delta=%s",
        tag + " " if tag else "",
        before_snapshot,
        target_snapshot,
        delta_snapshot,
    )

    orders: List[Dict[str, float]] = []
    for code in sorted(all_codes):
        current = kospi_positions.get(code, {})
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
        orders.append(
            {
                "code": code,
                "side": side,
                "qty": qty,
                "mkt_price": mkt_price,
                "allow_weight": weight,
            }
        )

    # Execute sells first to release cash before any new buys.
    for order in [o for o in orders if o["side"] == "sell"]:
        code, qty, mkt_price = order["code"], order["qty"], order["mkt_price"]
        try:
            res = send_order(code, qty=qty, price=None, side="sell", order_type="market")
            fills.append({"code": code, "side": "sell", "qty": qty, "resp": str(res)})
            sells += 1
            logger.info("%s SELL %s qty=%s price=%s", tag, code, qty, mkt_price)
            if mkt_price > 0:
                available_cash += qty * mkt_price
        except Exception:
            logger.exception("%s order fail %s qty=%s", tag, code, qty)

    for order in [o for o in orders if o["side"] == "buy"]:
        code, qty, mkt_price, weight = (
            order["code"],
            order["qty"],
            order["mkt_price"],
            order["allow_weight"],
        )
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
            res = send_order(code, qty=qty, price=None, side="buy", order_type="market")
            fills.append({"code": code, "side": "buy", "qty": qty, "resp": str(res)})
            buys += 1
            logger.info("%s BUY %s qty=%s price=%s", tag, code, qty, mkt_price)
            available_cash = max(0.0, available_cash - qty * mkt_price)
        except Exception:
            logger.exception("%s order fail %s qty=%s", tag, code, qty)
    invested = sum(float(t.get("target_value") or 0) for t in targets)
    cash_left = max(float(available_cash), 0.0)
    logger.info("%s[KOSPI_CORE][REBALANCE] BUY=%s SELL=%s", tag + " " if tag else "", buys, sells)
    logger.info("%s[KOSPI_CORE][PORTFOLIO] invested=%.0f cash=%.0f", tag + " " if tag else "", invested, cash_left)
    return fills
