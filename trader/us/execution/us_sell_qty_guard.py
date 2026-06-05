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
import os

logger = logging.getLogger(__name__)


def resolve_sell_qty(
    intent: dict,
    position: dict | None = None,
) -> tuple[int, dict]:
    """SELL 주문 수량을 orderable_qty 이하로 제한하고 partial exit 정책을 적용한다.

    partial_exit_allowed 정책:
    - partial_exit_allowed=False (기본): 전략적 partial sell 금지
      - holding_qty 전량 매도가 기본
      - orderable_qty < holding_qty 이면 PARTIAL_DUE_TO_ORDERABLE_QTY_CLAMP (브로커 제약)
    - partial_exit_allowed=True: intent_qty대로 partial 허용

    Args:
        intent: order intent dict (qty, symbol, partial_exit_allowed 등 포함)
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
            "strategic_partial": bool,
            "broker_constrained": bool,
            "reason": str,
        }
    """
    symbol = intent.get("symbol", "")
    intent_qty = int(intent.get("qty", 0))

    # partial_exit_allowed 확인: intent > meta > env 순
    _meta = intent.get("meta") if isinstance(intent.get("meta"), dict) else {}
    _allowed_raw = (
        intent.get("partial_exit_allowed")
        if intent.get("partial_exit_allowed") is not None
        else _meta.get("partial_exit_allowed")
    )
    if _allowed_raw is None:
        partial_exit_allowed = os.getenv("US_SELL_PARTIAL_ALLOWED", "1") == "1"
    else:
        partial_exit_allowed = bool(_allowed_raw)

    logger.info(
        "[US_EXIT][PARTIAL_POLICY] symbol=%s partial_allowed=%d requested=%s",
        symbol, int(partial_exit_allowed),
        "partial" if intent_qty > 0 else "full_exit",
    )

    # position이 None이거나 빈 dict이면 포지션 정보 없음
    if not position:
        meta = {
            "intent_qty": intent_qty,
            "holding_qty": 0,
            "orderable_qty": 0,
            "sell_qty": intent_qty,
            "clamped": False,
            "strategic_partial": False,
            "broker_constrained": False,
            "reason": "ok",
        }
        logger.info(
            "[US_SELL_QTY][RESOLVE] symbol=%s intent_qty=%d"
            " holding_qty=N/A orderable_qty=N/A sell_qty=%d clamped=0 reason=no_position_data",
            symbol, intent_qty, intent_qty,
        )
        return intent_qty, meta

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
        orderable_qty = holding_qty

    if holding_qty <= 0 and orderable_qty <= 0:
        logger.warning(
            "[US_SELL_QTY][RESOLVE] symbol=%s intent_qty=%d"
            " holding_qty=%d orderable_qty=%d sell_qty=0 clamped=1 reason=no_orderable_qty",
            symbol, intent_qty, holding_qty, orderable_qty,
        )
        meta = {
            "intent_qty": intent_qty,
            "holding_qty": holding_qty,
            "orderable_qty": orderable_qty,
            "sell_qty": 0,
            "clamped": True,
            "strategic_partial": False,
            "broker_constrained": False,
            "reason": "no_orderable_qty",
        }
        return 0, meta

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
            "strategic_partial": False,
            "broker_constrained": False,
            "reason": "no_orderable_qty",
        }
        return 0, meta

    # partial_exit_allowed=False: 전략적 partial sell 금지
    # → 전량(holding_qty) 매도가 기본; orderable_qty 부족이면 broker constraint
    if not partial_exit_allowed:
        # full exit 시도: orderable_qty 이하로 clamp
        sell_qty = min(holding_qty, orderable_qty) if holding_qty > 0 else orderable_qty
        broker_constrained = sell_qty < holding_qty
        if broker_constrained:
            logger.warning(
                "[US_ORDER][SELL_QTY_CLAMP] symbol=%s reason=broker_orderable_qty_less_than_holding"
                " holding_qty=%d orderable_qty=%d sell_qty=%d",
                symbol, holding_qty, orderable_qty, sell_qty,
            )
            logger.info(
                "[US_ORDER][PARTIAL_DUE_TO_ORDERABLE_QTY_CLAMP] symbol=%s"
                " strategic_partial=0 broker_constraint=1"
                " holding_qty=%d orderable_qty=%d",
                symbol, holding_qty, orderable_qty,
            )
        logger.info(
            "[US_SELL_QTY][RESOLVE] symbol=%s intent_qty=%d"
            " holding_qty=%d orderable_qty=%d sell_qty=%d"
            " clamped=%d broker_constrained=%d reason=%s",
            symbol, intent_qty, holding_qty, orderable_qty, sell_qty,
            int(broker_constrained), int(broker_constrained),
            "broker_orderable_qty_clamp" if broker_constrained else "ok",
        )
        meta = {
            "intent_qty": intent_qty,
            "holding_qty": holding_qty,
            "orderable_qty": orderable_qty,
            "sell_qty": sell_qty,
            "clamped": broker_constrained,
            "strategic_partial": False,
            "broker_constrained": broker_constrained,
            "reason": "broker_orderable_qty_clamp" if broker_constrained else "ok",
        }
        return sell_qty, meta

    # partial_exit_allowed=True: intent_qty대로 처리 (orderable_qty 이하로 clamp)
    sell_qty = min(intent_qty, holding_qty, orderable_qty) if holding_qty > 0 else min(intent_qty, orderable_qty)
    clamped = sell_qty < intent_qty
    strategic_partial = intent_qty < holding_qty  # 의도적 partial sell
    reason = "sell_qty_clamped_to_orderable" if clamped else "ok"

    logger.info(
        "[US_SELL_QTY][RESOLVE] symbol=%s intent_qty=%d"
        " holding_qty=%d orderable_qty=%d sell_qty=%d"
        " clamped=%d strategic_partial=%d reason=%s",
        symbol, intent_qty, holding_qty, orderable_qty, sell_qty,
        int(clamped), int(strategic_partial), reason,
    )

    meta = {
        "intent_qty": intent_qty,
        "holding_qty": holding_qty,
        "orderable_qty": orderable_qty,
        "sell_qty": sell_qty,
        "clamped": clamped,
        "strategic_partial": strategic_partial,
        "broker_constrained": clamped and not strategic_partial,
        "reason": reason,
    }
    return sell_qty, meta
