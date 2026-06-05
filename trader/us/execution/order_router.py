# -*- coding: utf-8 -*-
"""US Order Router.

intent를 risk gate에 통과시키고 DB에 저장한 후
DRY_RUN 또는 KIS 모의주문으로 라우팅한다.

처리 순서:
1. intent normalize
2. save_order_intent()
3. DB 기반 + memory 기반 중복 key 합산
4. risk gate 실행
5. BLOCK → mark_order_intent_blocked, return BLOCKED
6. DRY_RUN=1 → save_dry_run_order, mark_order_intent_sent, return DRY_RUN
7. DRY_RUN=0 → KIS 주문 → save_order_ack, mark_order_intent_sent, return ACK
8. KIS REJECT → save_order_reject, mark_order_intent_rejected, return REJECT
"""
from __future__ import annotations

import logging
import os
from typing import Any

from trader.us import config as us_cfg
from trader.us.execution.risk_gate import RiskGateBlocked, assert_order_allowed
from trader.us.db.repos import (  # test patch surface
    load_today_order_keys,
    mark_order_intent_blocked,
    mark_order_intent_dry_run,
    save_dry_run_order,
    save_order_intent,
)

logger = logging.getLogger(__name__)

# in-process 중복 키 (DB fallback 없을 때도 동일 process 내 중복 차단)
_SENT_ORDER_KEYS: set[str] = set()


def resolve_dry_run_for_us_order() -> bool:
    """US order DRY_RUN 여부를 resolve하고 runtime guard 검증.
    
    Rules:
    - KIS_ENV=practice + DRY_RUN=0: ALLOWED (practice 주문)
    - KIS_ENV!=practice + DRY_RUN=0: FORBIDDEN (즉시 RuntimeError)
    - DRY_RUN=1: dry-run mode
    
    Returns:
        True: DRY_RUN mode
        False: Real order mode (practice orders are allowed)
        
    Raises:
        RuntimeError: KIS_ENV!=practice에서 DRY_RUN=0 시도 시
    """
    from trader.utils.env import env_bool
    
    kis_env = str(os.getenv("KIS_ENV", "")).strip().lower()
    dry_run_raw = os.getenv("DRY_RUN", "1").strip()
    
    dry_run = env_bool("DRY_RUN", default=True)
    
    logger.info(
        "[US_ORDER][DRY_RUN_RESOLVE] kis_env=%s raw=%s resolved=%d",
        kis_env, dry_run_raw, int(dry_run)
    )
    
    # Runtime safety: KIS_ENV != practice에서 DRY_RUN=0은 허용하지 않음
    if kis_env not in ("practice", "vps") and not dry_run:
        msg = (
            f"[US_ORDER][DRY_RUN_RESOLVE][FORBIDDEN] "
            f"DRY_RUN=0 is only allowed when KIS_ENV=practice, got KIS_ENV={kis_env}"
        )
        logger.error(msg)
        raise RuntimeError(msg)
    
    return dry_run


def route_order(
    intent: dict,
    *,
    current_daily_notional_usd: float = 0.0,
    current_position_count: int = 0,
    total_portfolio_usd: float = 1000.0,
    available_cash_usd: float = 1000.0,
    kis_client: Any | None = None,
    signal_only: bool = False,
    kis_order_allowed: bool = True,
    allowed_symbols: "set[str] | None" = None,
    current_position_symbols: "set[str] | None" = None,
) -> dict:
    """Order intent를 라우팅한다.

    Args:
        kis_order_allowed: If False, return ORDER_DISABLED status
        allowed_symbols: BUY 허용 심볼 집합 (locked watchlist)
        current_position_symbols: SELL universe (현재 보유 포지션 심볼 집합)

    Returns:
        {"status": "DRY_RUN"|"ACK"|"BLOCKED"|"REJECT"|"SIGNAL_ONLY"|"ORDER_DISABLED", ...}
    """
    from trader.us.db.repos import (
        save_order_intent, save_dry_run_order, save_order_ack, save_order_reject,
        mark_order_intent_sent, mark_order_intent_blocked, mark_order_intent_rejected,
        mark_order_intent_dry_run,
        load_today_order_keys,
    )
    from trader.utils.env import env_bool

    symbol = intent.get("symbol", "")
    side = intent.get("side", "BUY")
    qty = int(intent.get("qty", 0))
    price = float(intent.get("limit_price", 0.0))
    exchange = intent.get("exchange", "NASDAQ")
    order_key = intent.get("client_order_key") or intent.get("order_key", "")

    logger.info(
        "[US_ORDER][INTENT] symbol=%s side=%s qty=%s notional_usd=%.2f key=%s",
        symbol, side, qty, float(intent.get("notional_usd", 0)), order_key,
    )

    # KIS order disabled
    if not kis_order_allowed:
        logger.info(
            "[US_ORDER][DISABLED] symbol=%s side=%s qty=%s reason=kis_order_allowed_false",
            symbol, side, qty,
        )
        return {
            "status": "ORDER_DISABLED",
            "reason": "kis_order_allowed_false",
            "symbol": symbol,
            "side": side,
            "qty": qty,
            "intent": intent,
        }

    # Signal-only mode: 신호만 생성, KIS 주문 차단
    if signal_only:
        logger.info(
            "[US_ORDER][SIGNAL_ONLY] symbol=%s side=%s qty=%s reason=KIS_ORDER_DISABLED_SIGNAL_ONLY",
            symbol, side, qty,
        )
        return {
            "status": "SIGNAL_ONLY",
            "reason": "KIS_ORDER_DISABLED_SIGNAL_ONLY",
            "symbol": symbol,
            "side": side,
            "qty": qty,
            "intent": intent,
        }

    # 1. intent DB 저장
    save_order_intent(intent)

    # 2. 중복 key: DB + in-memory 합산
    try:
        db_keys = load_today_order_keys()
    except Exception:
        db_keys = set()
    existing_keys = _SENT_ORDER_KEYS | db_keys

    # 3. Risk Gate
    gate_intent = {**intent, "client_order_key": order_key}
    try:
        assert_order_allowed(
            gate_intent,
            current_daily_notional_usd=current_daily_notional_usd,
            current_position_count=current_position_count,
            total_portfolio_usd=total_portfolio_usd,
            available_cash_usd=available_cash_usd,
            existing_order_keys=existing_keys,
            allowed_symbols=allowed_symbols,
            current_position_symbols=current_position_symbols,
        )
    except RiskGateBlocked as exc:
        logger.warning("[US_ORDER][BLOCKED] %s", exc)
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # A안: notional_exceeds_order_limit이면 qty 축소 후 1회 재시도
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        exc_str = str(exc)
        if "notional_exceeds_order_limit" in exc_str:
            logger.warning(
                "[US_ORDER][RESIZE_RETRY] attempting qty resize symbol=%s old_qty=%d",
                symbol, qty,
            )
            
            # US_MAX_ORDER_USD 기준으로 새 qty 계산
            order_cap_usd = float(os.getenv("US_MAX_ORDER_USD", "2500"))
            new_qty = int(order_cap_usd // price) if price > 0 else 0
            
            if new_qty > 0 and new_qty < qty:
                # 축소된 qty로 재시도
                resized_intent = {**intent}
                resized_intent["qty"] = new_qty
                resized_intent["notional_usd"] = new_qty * price
                
                logger.info(
                    "[US_ORDER][RESIZE_RETRY] symbol=%s old_qty=%d new_qty=%d cap=%.2f",
                    symbol, qty, new_qty, order_cap_usd,
                )
                
                # risk gate 재시도
                resized_gate_intent = {**resized_intent, "client_order_key": order_key}
                try:
                    assert_order_allowed(
                        resized_gate_intent,
                        current_daily_notional_usd=current_daily_notional_usd,
                        current_position_count=current_position_count,
                        total_portfolio_usd=total_portfolio_usd,
                        available_cash_usd=available_cash_usd,
                        existing_order_keys=existing_keys,
                        allowed_symbols=allowed_symbols,
                        current_position_symbols=current_position_symbols,
                    )
                    
                    # 재시도 성공: 축소된 intent로 계속 진행
                    logger.info(
                        "[US_ORDER][RESIZE_RETRY][SUCCESS] symbol=%s new_qty=%d new_notional=%.2f",
                        symbol, new_qty, new_qty * price,
                    )
                    intent = resized_intent  # 원래 intent를 축소된 것으로 교체
                    qty = new_qty  # 로컬 변수도 업데이트
                    
                except RiskGateBlocked as exc2:
                    # 재시도도 실패
                    logger.warning(
                        "[US_ORDER][RESIZE_RETRY][BLOCKED] symbol=%s new_qty=%d reason=%s",
                        symbol, new_qty, exc2,
                    )
                    if order_key:
                        mark_order_intent_blocked(order_key, reason=f"resize_retry_blocked: {exc2}")
                    return {"status": "BLOCKED", "reason": f"resize_retry_blocked: {exc2}", "intent": resized_intent}
            else:
                # 축소해도 qty가 0 이하거나 원래 qty와 같음
                logger.warning(
                    "[US_ORDER][RESIZE_RETRY][SKIP] symbol=%s new_qty=%d old_qty=%d price=%.2f cap=%.2f",
                    symbol, new_qty, qty, price, order_cap_usd,
                )
                if order_key:
                    mark_order_intent_blocked(order_key, reason=str(exc))
                return {"status": "BLOCKED", "reason": str(exc), "intent": intent}
        else:
            # notional_exceeds_order_limit가 아닌 다른 block reason
            if order_key:
                mark_order_intent_blocked(order_key, reason=str(exc))
            return {"status": "BLOCKED", "reason": str(exc), "intent": intent}

    # 4. DRY_RUN resolve with runtime guard
    dry_run_resolved = resolve_dry_run_for_us_order()
    
    if dry_run_resolved:
        logger.info("[US_ORDER][DRY_RUN] symbol=%s side=%s qty=%s", symbol, side, qty)
        dry_intent = {**intent, "client_order_key": order_key}
        save_dry_run_order(dry_intent)
        if order_key:
            # Mark as DRY_RUN instead of SENT to distinguish from real orders
            mark_order_intent_dry_run(order_key)
            _SENT_ORDER_KEYS.add(order_key)
        return {
            "status": "DRY_RUN",
            "symbol": symbol,
            "side": side,
            "qty": qty,
            "intent": intent,
        }

    # 5. Paper order via KIS
    if kis_client is None:
        from trader.us.execution.kis_us_client import KisUSClient
        kis_client = KisUSClient(env="practice")

    # SELL 직전 balance-match guard: orderable_qty 초과 주문 방지
    if side == "SELL":
        from trader.us.execution.us_sell_qty_guard import resolve_sell_qty

        # intent 또는 us_positions에서 holding_qty/orderable_qty 확보
        _pos_for_guard = {
            "holding_qty": intent.get("holding_qty")
                           or intent.get("available_qty")
                           or (intent.get("meta") or {}).get("holding_qty"),
            "orderable_qty": intent.get("orderable_qty")
                             or (intent.get("meta") or {}).get("orderable_qty"),
            "sellable_qty": intent.get("sellable_qty")
                            or (intent.get("meta") or {}).get("sellable_qty"),
        }
        # DB fallback: intent에 orderable_qty가 없으면 us_positions 조회
        if not _pos_for_guard["orderable_qty"] and symbol:
            try:
                from trader.us.db.repos import load_us_positions_by_symbols
                _db_positions = load_us_positions_by_symbols([symbol])
                _db_pos = _db_positions.get(symbol, {})
                if _db_pos:
                    _pos_for_guard["holding_qty"] = _pos_for_guard["holding_qty"] or _db_pos.get("holding_qty") or _db_pos.get("qty")
                    _pos_for_guard["orderable_qty"] = _db_pos.get("orderable_qty") or _db_pos.get("qty")
                    _pos_for_guard["sellable_qty"] = _db_pos.get("sellable_qty") or _pos_for_guard["orderable_qty"]
            except Exception as _db_exc:
                logger.warning("[US_ORDER][BALANCE_MATCH][WARN] db fallback failed: %s", _db_exc)

        sell_qty, guard_meta = resolve_sell_qty(intent, _pos_for_guard)

        logger.info(
            "[US_ORDER][BALANCE_MATCH] symbol=%s intent_qty=%s holding_qty=%s"
            " orderable_qty=%s sell_qty=%s",
            symbol,
            qty,
            guard_meta.get("holding_qty"),
            guard_meta.get("orderable_qty"),
            sell_qty,
        )

        if sell_qty <= 0:
            logger.error(
                "[US_ORDER][SELL_BLOCKED] symbol=%s reason=no_orderable_qty"
                " holding_qty=%s orderable_qty=%s",
                symbol,
                guard_meta.get("holding_qty"),
                guard_meta.get("orderable_qty"),
            )
            if order_key:
                mark_order_intent_blocked(order_key, reason="no_orderable_qty")
            return {
                "status": "BLOCKED",
                "reason": "no_orderable_qty",
                "symbol": symbol,
                "side": side,
                "qty": qty,
                "intent": intent,
            }

        if sell_qty < qty:
            logger.warning(
                "[US_ORDER][SELL_QTY_CLAMP] symbol=%s old_qty=%d new_qty=%d"
                " holding_qty=%s orderable_qty=%s",
                symbol,
                qty,
                sell_qty,
                guard_meta.get("holding_qty"),
                guard_meta.get("orderable_qty"),
            )
            qty = sell_qty
            intent = {**intent, "qty": sell_qty, "notional_usd": sell_qty * price}
            intent.setdefault("meta", {})
            if isinstance(intent.get("meta"), dict):
                intent["meta"]["sell_qty_clamped"] = True

    logger.info("[US_ORDER][SEND] symbol=%s side=%s qty=%s price=%.4f", symbol, side, qty, price)

    # ── KIS 주문 호출 (KIS ACK) ───────────────────────────────────────────
    # 중요: KIS ACK과 DB ACK을 반드시 분리한다.
    # KIS 주문 성공 후 DB 저장 실패는 REJECT가 아니라 ACK_DB_FAILED이다.
    order_no: str | None = None
    resp: Any = None
    try:
        if side == "BUY":
            resp = kis_client.place_us_buy_order(symbol, exchange, qty, price)
        else:
            resp = kis_client.place_us_sell_order(symbol, exchange, qty, price)

        from trader.us.execution.kis_us_response_parser import extract_order_no
        order_no = extract_order_no(resp)
        logger.info("[US_ORDER][KIS_ACK] symbol=%s side=%s order_no=%s", symbol, side, order_no)

    except Exception as exc:
        # KIS API 자체 실패 → REJECT
        logger.error("[US_ORDER][REJECT] symbol=%s side=%s error=%s", symbol, side, exc)
        reject_result = {
            "client_order_key": order_key,
            "symbol": symbol,
            "exchange": exchange,
            "side": side,
            "qty": qty,
            "reason": str(exc),
        }
        save_order_reject(reject_result)
        if order_key:
            mark_order_intent_rejected(order_key, reason=str(exc))
        return {
            "status": "REJECT",
            "reason": str(exc),
            "kis_ack": False,
            "ack_db_saved": False,
            "requires_reconcile": False,
            "intent": intent,
        }

    # ── DB ACK 저장 (KIS 성공 이후 별도 try) ─────────────────────────────
    # KIS 주문이 성공했으므로 어떤 경우에도 REJECT로 기록하면 안 된다.
    ack_result = {
        "client_order_key": order_key,
        "symbol": symbol,
        "exchange": exchange,
        "side": side,
        "qty_requested": qty,
        "qty_filled": 0,
        "avg_price_usd": price or None,
        "order_no": order_no,
        "status": "ACK",
        "dry_run": False,
        "meta": {"raw_response": resp},
    }

    ack_db_saved = False
    try:
        save_order_ack(ack_result)
        ack_db_saved = True
        logger.info("[US_ORDER][ACK_DB_SAVE][OK] symbol=%s order_no=%s", symbol, order_no)
    except Exception as db_exc:
        logger.error(
            "[US_ORDER][ACK_DB_FAILED] symbol=%s order_no=%s error=%s",
            symbol, order_no, db_exc,
        )

    # ── intent 상태 업데이트 ───────────────────────────────────────────────
    try:
        if order_key:
            mark_order_intent_sent(order_key)
            _SENT_ORDER_KEYS.add(order_key)
    except Exception as mark_exc:
        logger.error(
            "[US_ORDER][INTENT_MARK_SENT_FAILED] symbol=%s order_no=%s error=%s",
            symbol, order_no, mark_exc,
        )

    # ── 최종 반환 ──────────────────────────────────────────────────────────
    if ack_db_saved:
        logger.info("[US_ORDER][ACK] symbol=%s order_no=%s", symbol, order_no)
        return {
            "status": "ACK",
            "symbol": symbol,
            "side": side,
            "qty": qty,
            "order_no": order_no,
            "response": resp,
            "intent": intent,
            "kis_ack": True,
            "ack_db_saved": True,
            "requires_reconcile": False,
        }
    else:
        logger.error(
            "[US_ORDER][RECONCILE_REQUIRED] symbol=%s order_no=%s "
            "reason=ack_db_failed_after_kis_success",
            symbol, order_no,
        )
        return {
            "status": "ACK_DB_FAILED",
            "symbol": symbol,
            "side": side,
            "qty": qty,
            "order_no": order_no,
            "response": resp,
            "intent": intent,
            "kis_ack": True,
            "ack_db_saved": False,
            "requires_reconcile": True,
        }


def clear_sent_order_keys() -> None:
    """테스트 cleanup 용. in-memory주문 스토어도 함께 초기화."""
    _SENT_ORDER_KEYS.clear()
    try:
        from trader.us.db.repos import reset_memory_stores
        reset_memory_stores()
    except Exception:
        pass
