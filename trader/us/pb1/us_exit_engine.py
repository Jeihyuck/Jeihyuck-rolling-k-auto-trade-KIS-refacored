# -*- coding: utf-8 -*-
"""US PB1 Exit Engine.

한국장 PB1의 청산 전략을 미국장용으로 이식.

청산 종류:
- hard_stop:      손절 한도 초과
- trailing_stop:  고점 대비 하락
- profit_protect: 목표 수익 근접 시 이익 보호
- giveback:       수익 반납 비율 초과
- time_stop:      보유 기간 초과
- risk_off:       시장 전체 위험 off

정책:
- 주문 ACK와 fill 분리 (US_ORDER_ACCEPTED_IS_NOT_FILLED=1)
- fill confirm 전까지 포지션 반영 금지
- same-day soft exit 차단 (US_BLOCK_REBUY_AFTER_SELL_SAME_DAY)
"""
from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Any

# US Explanation System
from trader.us.pb1.us_explain import (
    build_us_exit_explanation,
    log_us_exit_decision,
    validate_explanations_batch,
)

logger = logging.getLogger(__name__)

# 설정값
_HARD_STOP_PCT = float(os.getenv("US_HARD_STOP_PCT", "0.07"))       # 7% 손절
_TRAILING_STOP_PCT = float(os.getenv("US_TRAILING_STOP_PCT", "0.05")) # 5% trailing
_PROFIT_PROTECT_PCT = float(os.getenv("US_PROFIT_PROTECT_PCT", "0.15")) # 15% 수익 보호
_GIVEBACK_PCT = float(os.getenv("US_GIVEBACK_PCT", "0.33"))           # 최고 수익 33% 반납
_TIME_STOP_DAYS = int(os.getenv("US_TIME_STOP_DAYS", "20"))           # 20일 보유


def _reload_env() -> dict:
    """환경변수 최신값 로드."""
    return {
        "hard_stop": float(os.getenv("US_HARD_STOP_PCT", "0.07")),
        "trailing_stop": float(os.getenv("US_TRAILING_STOP_PCT", "0.05")),
        "profit_protect": float(os.getenv("US_PROFIT_PROTECT_PCT", "0.15")),
        "giveback": float(os.getenv("US_GIVEBACK_PCT", "0.33")),
        "time_stop_days": int(os.getenv("US_TIME_STOP_DAYS", "20")),
    }


def evaluate_exit(
    position: dict,
    current_price: float,
    now: datetime | None = None,
) -> dict | None:
    """단일 포지션 청산 조건 평가.

    Args:
        position: {symbol, exchange, qty, entry_price, entry_date, max_price, ...}
        current_price: 현재가 (USD)
        now: 현재 시각

    Returns:
        청산 intent dict 또는 None (청산 불필요)
    """
    cfg = _reload_env()

    symbol = position.get("symbol", "")
    exchange = position.get("exchange", "NASDAQ")
    qty = int(position.get("qty", 0))

    # entry_price 방어적 fallback (주된 보강은 resolver에서)
    entry_price: float = 0.0
    for field in ("entry_price", "avg_price_usd", "avg_cost", "average_price", "avg_buy_price"):
        v = position.get(field)
        if v is not None:
            try:
                fv = float(v)
                if fv > 0:
                    entry_price = fv
                    break
            except (TypeError, ValueError):
                pass
    # buy_amount_usd / qty 마지막 방어
    if entry_price <= 0:
        buy_amount = position.get("buy_amount_usd")
        if buy_amount is not None and qty > 0:
            try:
                ep = float(buy_amount) / qty
                if ep > 0:
                    entry_price = ep
            except (TypeError, ValueError, ZeroDivisionError):
                pass

    # max_price fallback — None이어도 crash 방지
    _raw_max = position.get("max_price") or position.get("high_watermark")
    if _raw_max:
        try:
            max_price = float(_raw_max)
        except (TypeError, ValueError):
            max_price = max(entry_price, current_price) if entry_price > 0 else current_price
    else:
        max_price = max(entry_price, current_price) if entry_price > 0 else current_price

    # ── qty guard ────────────────────────────────────────────────────────────
    if qty <= 0:
        return None

    # ── orderable_qty clamp ──────────────────────────────────────────────────
    # SELL qty는 orderable_qty를 초과할 수 없다. (한국장 qty_to_close 원칙 이식)
    raw_qty = qty
    orderable_qty = int(
        position.get("orderable_qty")
        or position.get("sellable_qty")
        or position.get("holding_qty")
        or qty
    )
    exit_qty = min(raw_qty, orderable_qty) if orderable_qty > 0 else raw_qty

    logger.info(
        "[US_EXIT][SELL_QTY] symbol=%s holding_qty=%d orderable_qty=%d exit_qty=%d",
        symbol,
        raw_qty,
        orderable_qty,
        exit_qty,
    )

    if exit_qty <= 0:
        logger.warning(
            "[US_EXIT][SELL_QTY][SKIP] symbol=%s exit_qty=%d orderable_qty=%d",
            symbol, exit_qty, orderable_qty,
        )
        return None

    # exit_qty로 교체
    qty = exit_qty

    # ── current_price guard ──────────────────────────────────────────────────
    if current_price <= 0:
        logger.warning(
            "[US_EXIT][PRICE_MISSING] symbol=%s qty=%s current_price=%s",
            symbol, qty, current_price,
        )
        return None

    # ── entry_price guard: PNL_MISSING fail-closed ────────────────────────────
    if entry_price <= 0:
        logger.error(
            "[US_EXIT][PNL_MISSING] symbol=%s qty=%s current_price=%.4f "
            "entry_price=%s avg_price_usd=%s avg_cost=%s buy_amount_usd=%s pnl_rate=%s source=%s",
            symbol,
            qty,
            current_price,
            position.get("entry_price"),
            position.get("avg_price_usd"),
            position.get("avg_cost"),
            position.get("buy_amount_usd"),
            position.get("pnl_rate"),
            position.get("entry_price_source"),
        )
        fail_closed = os.getenv("US_EXIT_FAIL_CLOSED_ON_PNL_MISSING", "1") not in {
            "0", "false", "False", "NO", "no",
        }
        if fail_closed:
            return _make_exit_intent(
                symbol=symbol,
                exchange=exchange,
                qty=qty,
                current_price=current_price,
                entry_price=current_price,
                exit_type="pnl_missing_fail_closed",
                reason="PNL_MISSING_ENTRY_PRICE_FAIL_CLOSED",
                unrealized_pnl_usd=0.0,
                pnl_pct=-999.0,
                holding_qty=raw_qty,
                orderable_qty=orderable_qty,
            )
        return None

    pnl_pct = (current_price - entry_price) / entry_price
    unrealized_pnl_usd = (current_price - entry_price) * qty

    # ── POSITION_INPUT 로그 ──────────────────────────────────────────────────
    logger.info(
        "[US_EXIT][POSITION_INPUT] symbol=%s qty=%s entry_price=%.4f current_price=%.4f source=%s",
        symbol,
        qty,
        entry_price,
        current_price,
        position.get("entry_price_source", "unknown"),
    )

    # ── PnL CHECK 로그 ────────────────────────────────────────────────────────
    logger.info(
        "[US_EXIT][CHECK] symbol=%s qty=%s entry_price=%.4f current_price=%.4f "
        "pnl_pct=%.4f hard_stop=%.4f trailing_stop=%.4f",
        symbol,
        qty,
        entry_price,
        current_price,
        pnl_pct,
        cfg["hard_stop"],
        cfg["trailing_stop"],
    )

    # ── hard stop ─────────────────────────────────────────────────────────────
    if pnl_pct <= -cfg["hard_stop"]:
        return _make_exit_intent(
            symbol=symbol, exchange=exchange, qty=qty,
            current_price=current_price, entry_price=entry_price,
            exit_type="hard_stop",
            reason=f"pnl_pct={pnl_pct:.3f} <= -{cfg['hard_stop']}",
            unrealized_pnl_usd=unrealized_pnl_usd,
            pnl_pct=pnl_pct,
            holding_qty=raw_qty,
            orderable_qty=orderable_qty,
        )

    # ── trailing stop ─────────────────────────────────────────────────────────
    if max_price > 0 and current_price < max_price * (1 - cfg["trailing_stop"]):
        trail_pct = (max_price - current_price) / max_price
        return _make_exit_intent(
            symbol=symbol, exchange=exchange, qty=qty,
            current_price=current_price, entry_price=entry_price,
            exit_type="trailing_stop",
            reason=f"trail_pct={trail_pct:.3f} > {cfg['trailing_stop']}",
            unrealized_pnl_usd=unrealized_pnl_usd,
            pnl_pct=pnl_pct,
            holding_qty=raw_qty,
            orderable_qty=orderable_qty,
        )

    # ── profit protect ────────────────────────────────────────────────────────
    if pnl_pct >= cfg["profit_protect"]:
        # 수익 보호: 고점 대비 X% 빠지면 청산
        protect_trail = 0.03
        if max_price > 0 and current_price < max_price * (1 - protect_trail):
            return _make_exit_intent(
                symbol=symbol, exchange=exchange, qty=qty,
                current_price=current_price, entry_price=entry_price,
                exit_type="profit_protect",
                reason=f"pnl_pct={pnl_pct:.3f} high but pulling back",
                unrealized_pnl_usd=unrealized_pnl_usd,
                pnl_pct=pnl_pct,
                holding_qty=raw_qty,
                orderable_qty=orderable_qty,
            )

    # ── giveback ──────────────────────────────────────────────────────────────
    if max_price > entry_price:
        max_gain = (max_price - entry_price) / entry_price
        current_gain = pnl_pct
        if max_gain > 0.05:  # 최소 5% 수익 있을 때만 giveback 적용
            giveback_ratio = (max_gain - current_gain) / max_gain
            if giveback_ratio >= cfg["giveback"]:
                return _make_exit_intent(
                    symbol=symbol, exchange=exchange, qty=qty,
                    current_price=current_price, entry_price=entry_price,
                    exit_type="giveback",
                    reason=f"giveback_ratio={giveback_ratio:.3f} >= {cfg['giveback']}",
                    unrealized_pnl_usd=unrealized_pnl_usd,
                    pnl_pct=pnl_pct,
                    holding_qty=raw_qty,
                    orderable_qty=orderable_qty,
                )

    return None


def _make_exit_intent(
    symbol: str,
    exchange: str,
    qty: int,
    current_price: float,
    entry_price: float,
    exit_type: str,
    reason: str,
    unrealized_pnl_usd: float,
    pnl_pct: float,
    holding_qty: int = 0,
    orderable_qty: int = 0,
) -> dict:
    """Exit order intent 생성."""
    import hashlib
    from datetime import date
    today = date.today().strftime("%Y%m%d")
    key_raw = f"{symbol}_{today}_SELL_{exit_type}"
    client_order_key = hashlib.sha256(key_raw.encode()).hexdigest()[:24]

    logger.info(
        "[US_EXIT][SIGNAL] symbol=%s exit_type=%s reason=%s pnl_pct=%.3f",
        symbol, exit_type, reason, pnl_pct,
    )

    _holding = holding_qty or qty
    _orderable = orderable_qty or _holding

    return {
        "symbol": symbol,
        "exchange": exchange,
        "side": "SELL",
        "qty": qty,
        "available_qty": qty,   # risk gate SELL qty <= available_qty 확인용
        "limit_price": round(current_price * 0.998, 4),  # 0.2% 슬리피지 허용
        "notional_usd": round(current_price * qty, 4),
        "exit_type": exit_type,
        "reason": reason,
        "unrealized_pnl_usd": round(unrealized_pnl_usd, 4),
        "unrealized_pnl_pct": round(pnl_pct, 4),
        "client_order_key": client_order_key,
        "strategy": "us_pb1_exit",
        "meta": {
            "holding_qty": _holding,
            "orderable_qty": _orderable,
            "sellable_qty": _orderable,
            "qty_source": "orderable_qty_clamp" if qty < _holding else "holding_qty",
        },
    }


def generate_exit_intents(
    positions: list[dict],
    provider: Any,
    now: datetime | None = None,
) -> list[dict]:
    """보유 포지션 전체에 대해 청산 조건 평가.

    Args:
        positions: 보유 포지션 목록
        provider: USDataProvider
        now: 현재 시각

    Returns:
        청산 intent 목록
    """
    from trader.us.symbols import normalize_us_exchange
    
    intents: list[dict] = []
    sell_explanations: list[dict] = []
    hold_explanations: list[dict] = []

    if not positions:
        return intents

    for pos in positions:
        symbol = pos.get("symbol", "")
        raw_exchange = pos.get("exchange", "NASDAQ")
        
        # Normalize exchange for price lookup (NASD → NASDAQ, etc.)
        try:
            exchange = normalize_us_exchange(raw_exchange)
        except ValueError as exc:
            logger.warning(
                "[US_EXIT][EXCHANGE_NORMALIZE_FAILED] symbol=%s raw_exchange=%s error=%s, defaulting to NASDAQ",
                symbol, raw_exchange, exc
            )
            exchange = "NASDAQ"

        try:
            price_data = provider.get_current_price(symbol, exchange)
            current_price = float(price_data.get("last", 0))
        except Exception as exc:
            logger.warning("[US_EXIT][WARN] price fetch failed symbol=%s exchange=%s error=%s", symbol, exchange, exc)
            continue

        if current_price <= 0:
            continue

        intent = evaluate_exit(position=pos, current_price=current_price, now=now)
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # Build exit explanation
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        if intent is not None:
            # SELL decision
            exit_explanation = build_us_exit_explanation(
                symbol=symbol,
                position=pos,
                exit_intent=intent,
                current_price=current_price,
            )
            sell_explanations.append(exit_explanation)
            
            # Log WHY_SELL
            log_us_exit_decision(symbol, "SELL", exit_explanation)
            
            # Add explanation fields to intent
            intent["exit_style"] = exit_explanation.get("exit_style", "unknown")
            intent["exit_trigger"] = exit_explanation.get("exit_trigger")
            intent["explanation_quality"] = exit_explanation.get("explanation_quality", "FULL")
            
            intents.append(intent)
        else:
            # HOLD decision (NO_EXIT_SIGNAL)
            hold_explanation = build_us_exit_explanation(
                symbol=symbol,
                position=pos,
                exit_intent=None,
                current_price=current_price,
            )
            hold_explanations.append(hold_explanation)
            
            # Log WHY_HOLD
            log_us_exit_decision(symbol, "HOLD", hold_explanation)

    # ─────────────────────────────────────────────────────────────────────────
    # Explanation Quality Validation
    # ─────────────────────────────────────────────────────────────────────────
    all_explanations = sell_explanations + hold_explanations
    if all_explanations:
        quality_report = validate_explanations_batch(all_explanations)
        logger.info(
            "[US_EXIT][EXPLANATION_QUALITY] total=%d full=%d partial=%d minimal=%d missing=%d summary=%s warning=%s",
            quality_report["total_count"],
            quality_report["full_count"],
            quality_report["partial_count"],
            quality_report["minimal_count"],
            quality_report["missing_count"],
            quality_report["quality_summary"],
            quality_report["quality_warning"],
        )
        
        if quality_report["quality_warning"]:
            logger.warning(
                "[US_EXIT][EXPLANATION_QUALITY_WARNING] %s",
                quality_report["quality_summary"],
            )
    else:
        logger.warning("[US_EXIT][EXPLANATION_QUALITY] no explanations generated")

    return intents
