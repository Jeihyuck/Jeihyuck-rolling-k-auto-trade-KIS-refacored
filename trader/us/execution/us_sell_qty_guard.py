# -*- coding: utf-8 -*-
"""US SELL Quantity Guard.

KIS 해외잔고 기반 매도가능수량(orderable_qty)을 초과하는 SELL 주문을 방지한다.

원칙 (한국장 qty_to_close 원칙 이식):
- sell_qty = min(intent_qty, holding_qty, orderable_qty)
- sell_qty <= 0이면 주문 금지 (BLOCKED)
- 절대 orderable_qty보다 큰 SELL intent를 KIS에 보내지 않는다.

금지:
- trader.db.repos import
- trader.kis_wrapper import
- 특정 종목 하드코딩 (CRDO, LITE, SOXX, VRT 등)
"""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def resolve_sell_qty(
    intent: dict,
    position: dict | None = None,
) -> tuple[int, dict]:
    """SELL 주문 수량을 orderable_qty 이하로 제한한다.

    Args:
        intent: order intent dict (qty, symbol 등 포함)
        position: 현재 보유 포지션 dict (holding_qty, orderable_qty, sellable_qty 등)

    Returns:
        (sell_qty, meta)
        sell_qty: 최종 SELL 수량 (0이면 주문 금지)
        meta: {
            "intent_qty": int,
            "holding_qty": int,
            "orderable_qty": int,
            "sell_qty": int,
            "clamped": bool,
            "reason": str,  # "ok" | "sell_qty_clamped_to_orderable" | "no_orderable_qty"
        }
    """
    symbol = intent.get("symbol", "")
    intent_qty = int(intent.get("qty", 0))

    # position이 None이거나 빈 dict이면 포지션 정보가 없는 것으로 간주
    # → 가드 없이 intent_qty 그대로 반환 (DB 조회는 호출자 책임)
    if not position:
        meta = {
            "intent_qty": intent_qty,
            "holding_qty": 0,
            "orderable_qty": 0,
            "sell_qty": intent_qty,
            "clamped": False,
            "reason": "ok",
        }
        logger.info(
            "[US_SELL_QTY][RESOLVE] symbol=%s intent_qty=%d"
            " holding_qty=N/A orderable_qty=N/A sell_qty=%d clamped=0 reason=no_position_data",
            symbol, intent_qty, intent_qty,
        )
        return intent_qty, meta

    # orderable_qty/holding_qty 추출 — None과 0을 구분해야 하므로 or 대신 명시적 None 체크
    def _pick_first_not_none(*keys, default=None):
        for k in keys:
            v = position.get(k)
            if v is not None:
                return v
        return default

    raw_holding = _pick_first_not_none("holding_qty", "qty")
    holding_qty = int(raw_holding) if raw_holding is not None else 0

    raw_orderable = _pick_first_not_none("orderable_qty", "sellable_qty")
    if raw_orderable is not None:
        orderable_qty = int(raw_orderable)
    else:
        # orderable_qty 정보 없으면 holding_qty를 fallback으로 사용
        orderable_qty = holding_qty

    if holding_qty <= 0 and orderable_qty <= 0:
        logger.warning(
            "[US_SELL_QTY][RESOLVE] symbol=%s intent_qty=%d"
            " holding_qty=%d orderable_qty=%d sell_qty=0 clamped=1 reason=no_orderable_qty",
            symbol,
            intent_qty,
            holding_qty,
            orderable_qty,
        )
        meta = {
            "intent_qty": intent_qty,
            "holding_qty": holding_qty,
            "orderable_qty": orderable_qty,
            "sell_qty": 0,
            "clamped": True,
            "reason": "no_orderable_qty",
        }
        return 0, meta

    # orderable_qty=0이면 (holding은 있지만 매도불가) → BLOCKED
    if orderable_qty <= 0:
        logger.warning(
            "[US_SELL_QTY][RESOLVE] symbol=%s intent_qty=%d"
            " holding_qty=%d orderable_qty=0 sell_qty=0 clamped=1 reason=no_orderable_qty",
            symbol, intent_qty, holding_qty,
        )
        meta = {
            "intent_qty": intent_qty,
            "holding_qty": holding_qty,
            "orderable_qty": orderable_qty,
            "sell_qty": 0,
            "clamped": True,
            "reason": "no_orderable_qty",
        }
        return 0, meta

    sell_qty = min(intent_qty, holding_qty, orderable_qty) if holding_qty > 0 else min(intent_qty, orderable_qty)
    clamped = sell_qty < intent_qty
    reason = "sell_qty_clamped_to_orderable" if clamped else "ok"

    logger.info(
        "[US_SELL_QTY][RESOLVE] symbol=%s intent_qty=%d"
        " holding_qty=%d orderable_qty=%d sell_qty=%d clamped=%d reason=%s",
        symbol,
        intent_qty,
        holding_qty,
        orderable_qty,
        sell_qty,
        int(clamped),
        reason,
    )

    meta = {
        "intent_qty": intent_qty,
        "holding_qty": holding_qty,
        "orderable_qty": orderable_qty,
        "sell_qty": sell_qty,
        "clamped": clamped,
        "reason": reason,
    }
    return sell_qty, meta
