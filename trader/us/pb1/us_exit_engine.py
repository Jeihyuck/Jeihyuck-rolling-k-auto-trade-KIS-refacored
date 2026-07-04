# -*- coding: utf-8 -*-
"""US PB1 Exit Engine.

한국장 PB1의 청산 전략을 미국장용으로 이식.

청산 종류:
- hard_stop_loss:      -8% 하드 손절
- soft_stop_loss:      -5% 소프트 손절(확인/부분매도)
- profit_trailing_stop: 수익 경험 후 고점 대비 하락
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
EXIT_HARD_STOP_LOSS = "hard_stop_loss"
EXIT_SOFT_STOP_LOSS = "soft_stop_loss"
EXIT_PROFIT_TRAILING_STOP = "profit_trailing_stop"
EXIT_TIME_OR_MOMENTUM_EXIT = "time_or_momentum_exit"

_HARD_STOP_PCT = float(os.getenv("US_HARD_STOP_PCT", "0.08"))       # 8% 하드 손절
_SOFT_STOP_PCT = float(os.getenv("US_SOFT_STOP_LOSS_PCT", "0.05"))  # 5% 소프트 손절
_TRAILING_ACTIVATION_PROFIT_PCT = float(os.getenv("US_TRAILING_ACTIVATION_PROFIT_PCT", "0.03"))
_TRAILING_STOP_PCT = float(os.getenv("US_TRAILING_STOP_PCT", "0.05")) # 수익 경험 후 5% trailing
_PROFIT_PROTECT_PCT = float(os.getenv("US_PROFIT_PROTECT_PCT", "0.15")) # 15% 수익 보호
_GIVEBACK_PCT = float(os.getenv("US_GIVEBACK_PCT", "0.33"))           # 최고 수익 33% 반납
_TIME_STOP_DAYS = int(os.getenv("US_TIME_STOP_DAYS", "20"))           # 20일 보유


def _reload_env() -> dict:
    """환경변수 최신값 로드."""
    return {
        "hard_stop": float(os.getenv("US_HARD_STOP_PCT", "0.08")),
        "soft_stop": float(os.getenv("US_SOFT_STOP_LOSS_PCT", "0.05")),
        "trailing_activation_profit": float(os.getenv("US_TRAILING_ACTIVATION_PROFIT_PCT", "0.03")),
        "trailing_stop": float(os.getenv("US_TRAILING_STOP_PCT", "0.05")),
        "profit_protect": float(os.getenv("US_PROFIT_PROTECT_PCT", "0.15")),
        "giveback": float(os.getenv("US_GIVEBACK_PCT", "0.33")),
        "time_stop_days": int(os.getenv("US_TIME_STOP_DAYS", "20")),
    }


def is_open_vol_guard_window(now: datetime | None) -> bool:
    """Return True during the regular-open whipsaw guard window (09:30-10:00 ET)."""
    if os.getenv("US_OPEN_VOL_GUARD_ENABLED", "1") not in {"1", "true", "True", "yes", "YES"}:
        return False
    from zoneinfo import ZoneInfo
    from datetime import time
    ny = ZoneInfo("America/New_York")
    dt = (now or datetime.now(tz=ny)).astimezone(ny)
    return time(9, 30) <= dt.time() < time(10, 0)


def _soft_stop_confirmed(position: dict, default_required: int = 2) -> tuple[bool, int, int]:
    raw_required = position.get("soft_stop_confirm_ticks") or os.getenv("US_SOFT_STOP_CONFIRM_TICKS", str(default_required))
    required = max(1, int(raw_required))
    count = int(position.get("soft_stop_breach_count") or position.get("risk_state", {}).get("soft_stop_breach_count") or 0)
    return count >= required, count, required


def _apply_sell_ratio(qty: int, ratio: float) -> int:
    return max(1, min(qty, int(qty * ratio)))


def _canonical_exit_reason(exit_type: str) -> tuple[str, str]:
    exit_reason_detail = exit_type
    if exit_type == EXIT_PROFIT_TRAILING_STOP:
        return "trailing_stop", exit_reason_detail
    if exit_type == EXIT_HARD_STOP_LOSS:
        # legacy contract marker: return "hard_stop", exit_reason_detail
        return "hard_stop_full_exit", exit_reason_detail
    if exit_type == "persistent_soft_stop_full_exit":
        return "persistent_soft_stop_full_exit", exit_reason_detail
    return exit_type, exit_reason_detail


def evaluate_exit(
    position: dict,
    current_price: float,
    now: datetime | None = None,
    trail_high_price: float | None = None,
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
                now=now,
                trail_high_price=current_price,
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
        "pnl_pct=%.4f hard_stop=%.4f soft_stop=%.4f trailing_activation=%.4f trailing_stop=%.4f",
        symbol,
        qty,
        entry_price,
        current_price,
        pnl_pct,
        cfg["hard_stop"],
        cfg["soft_stop"],
        cfg["trailing_activation_profit"],
        cfg["trailing_stop"],
    )

    # ── hard stop ─────────────────────────────────────────────────────────────
    if pnl_pct <= -cfg["hard_stop"]:
        return _make_exit_intent(
            symbol=symbol, exchange=exchange, qty=qty,
            current_price=current_price, entry_price=entry_price,
            exit_type=EXIT_HARD_STOP_LOSS,
            reason=f"pnl_pct={pnl_pct:.3f} <= -{cfg['hard_stop']}",
            unrealized_pnl_usd=unrealized_pnl_usd,
            pnl_pct=pnl_pct,
            holding_qty=raw_qty,
            orderable_qty=orderable_qty,
            now=now,
            trail_high_price=max_price,
        )

    # ── persistent soft stop: first partial 이후에도 -5%가 지속되면 전량 청산 ─────────
    if pnl_pct <= -cfg["soft_stop"]:
        confirmed, breach_count, required_ticks = _soft_stop_confirmed(position)
        persistent_required = max(required_ticks + 1, int(os.getenv("US_PERSISTENT_SOFT_STOP_TICKS", "3") or 3))
        already_reduced = bool(position.get("soft_stop_partial_done") or position.get("partial_soft_stop_done") or position.get("last_exit_type") == EXIT_SOFT_STOP_LOSS)
        if already_reduced and breach_count >= persistent_required:
            return _make_exit_intent(
                symbol=symbol, exchange=exchange, qty=qty,
                current_price=current_price, entry_price=entry_price,
                exit_type="persistent_soft_stop_full_exit",
                reason=f"persistent soft stop pnl_pct={pnl_pct:.3f} <= -{cfg['soft_stop']} ticks={breach_count}/{persistent_required}",
                unrealized_pnl_usd=unrealized_pnl_usd,
                pnl_pct=pnl_pct,
                holding_qty=raw_qty,
                orderable_qty=orderable_qty,
                now=now,
                trail_high_price=max_price,
            )

    # ── profit trailing stop: 수익 경험 후에만 활성화 ────────────────────────
    max_unrealized_profit_pct = ((max_price - entry_price) / entry_price) if entry_price > 0 and max_price > 0 else 0.0
    if (
        max_price > 0
        and max_unrealized_profit_pct >= cfg["trailing_activation_profit"]
        and current_price <= max_price * (1 - cfg["trailing_stop"])
    ):
        trail_pct = (max_price - current_price) / max_price
        sell_qty = _apply_sell_ratio(qty, float(os.getenv("US_PROFIT_TRAILING_SELL_RATIO", "0.5")))
        return _make_exit_intent(
            symbol=symbol, exchange=exchange, qty=sell_qty,
            current_price=current_price, entry_price=entry_price,
            exit_type=EXIT_PROFIT_TRAILING_STOP,
            reason=f"max_profit={max_unrealized_profit_pct:.3f} trail_pct={trail_pct:.3f} > {cfg['trailing_stop']}",
            unrealized_pnl_usd=(current_price - entry_price) * sell_qty,
            pnl_pct=pnl_pct,
            holding_qty=raw_qty,
            orderable_qty=orderable_qty,
            now=now,
            trail_high_price=max_price,
        )

    # ── soft stop: 단일 -5% 틱 전량매도 금지, 확인 후 부분매도 ──────────────
    if pnl_pct <= -cfg["soft_stop"]:
        confirmed, breach_count, required_ticks = _soft_stop_confirmed(position)
        if is_open_vol_guard_window(now):
            return _make_hold_intent(
                symbol=symbol,
                exit_type=EXIT_SOFT_STOP_LOSS,
                reason="open_vol_guard_blocks_soft_exit",
                pnl_pct=pnl_pct,
                breach_count=breach_count,
                required_ticks=required_ticks,
                now=now,
            )
        if not confirmed:
            return _make_hold_intent(
                symbol=symbol,
                exit_type=EXIT_SOFT_STOP_LOSS,
                reason="soft_stop_wait_confirm",
                pnl_pct=pnl_pct,
                breach_count=breach_count,
                required_ticks=required_ticks,
                now=now,
            )
        sell_qty = _apply_sell_ratio(qty, float(os.getenv("US_SOFT_STOP_SELL_RATIO", "0.5")))
        return _make_exit_intent(
            symbol=symbol, exchange=exchange, qty=sell_qty,
            current_price=current_price, entry_price=entry_price,
            exit_type=EXIT_SOFT_STOP_LOSS,
            reason=f"pnl_pct={pnl_pct:.3f} <= -{cfg['soft_stop']} confirmed_ticks={breach_count}/{required_ticks}",
            unrealized_pnl_usd=(current_price - entry_price) * sell_qty,
            pnl_pct=pnl_pct,
            holding_qty=raw_qty,
            orderable_qty=orderable_qty,
            now=now,
            trail_high_price=max_price,
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
                now=now,
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
                    now=now,
                    trail_high_price=max_price,
                )

    return None


def resolve_us_trade_date_key(now: datetime | None = None) -> str:
    from zoneinfo import ZoneInfo
    ny = ZoneInfo("America/New_York")
    dt = (now or datetime.now(tz=ny)).astimezone(ny)
    return dt.strftime("%Y%m%d")


def _trade_date_from_key(trade_date_key: str) -> str:
    return f"{trade_date_key[:4]}-{trade_date_key[4:6]}-{trade_date_key[6:]}"


def should_skip_exit_due_to_pending_sell(symbol: str, trade_date: str) -> tuple[bool, str | None, dict | None]:
    """Return true when a same-symbol SELL is already pending/ACK-like."""
    statuses = {"SUBMITTED", "ACK", "PENDING", "PARTIALLY_FILLED", "RECONCILE_PENDING", "ACK_DB_FAILED"}
    try:
        from trader.us.db.repos import has_pending_order_for_symbol_side, find_recent_sell_ack
        recent_ack = find_recent_sell_ack(symbol=symbol, trade_date=trade_date)
        if recent_ack:
            return True, "recent_sell_ack_exists", recent_ack
        if has_pending_order_for_symbol_side(symbol=symbol, side="SELL", trade_date=trade_date, include_statuses=statuses):
            return True, "pending_sell_order_exists", None
    except Exception as exc:
        logger.warning("[US_EXIT][PENDING_SELL_CHECK_WARN] symbol=%s trade_date=%s err=%s", symbol, trade_date, exc)
    return False, None, None


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
    now: datetime | None = None,
    trail_high_price: float | None = None,
) -> dict | None:
    """Exit order intent 생성."""
    import hashlib
    trade_date_key = resolve_us_trade_date_key(now)
    trade_date = _trade_date_from_key(trade_date_key)
    skip, skip_reason, recent_ack = should_skip_exit_due_to_pending_sell(symbol, trade_date)
    if skip:
        logger.info(
            "[US_EXIT][SKIP_PENDING_SELL] symbol=%s trade_date=%s reason=%s order_no=%s action=RECONCILE_ONLY",
            symbol, trade_date, skip_reason, (recent_ack or {}).get("order_no", ""),
        )
        return None
    leg_no = 2 if exit_type == "persistent_soft_stop_full_exit" else 1
    position_snapshot_qty = int(holding_qty or qty or 0)
    key_raw = f"{symbol}_SELL_{trade_date_key}_{exit_type}_{leg_no}_{qty}_{position_snapshot_qty}"
    client_order_key = hashlib.sha256(key_raw.encode()).hexdigest()[:24]

    exit_reason, exit_reason_detail = _canonical_exit_reason(exit_type)

    logger.info(
        "[US_EXIT][SIGNAL] symbol=%s exit_type=%s exit_reason=%s exit_reason_detail=%s reason=%s pnl_pct=%.3f",
        symbol, exit_type, exit_reason, exit_reason_detail, reason, pnl_pct,
    )

    _holding = holding_qty or qty
    _orderable = orderable_qty or _holding
    trail_high = float(trail_high_price or current_price or 0.0)
    trail_drawdown_pct = ((trail_high - current_price) / trail_high) if trail_high > 0 else 0.0
    cfg = _reload_env()
    stop_type = _canonical_exit_reason(exit_type)[0]
    threshold = cfg["hard_stop"] if exit_type in {"hard_stop", EXIT_HARD_STOP_LOSS} else cfg["soft_stop"] if exit_type == EXIT_SOFT_STOP_LOSS else cfg["trailing_stop"] if exit_type in {"trailing_stop", EXIT_PROFIT_TRAILING_STOP, "profit_protect"} else cfg.get("giveback", 0.0)
    try:
        from zoneinfo import ZoneInfo
        decision_ts_et = (now or datetime.now(tz=ZoneInfo("America/New_York"))).astimezone(ZoneInfo("America/New_York")).isoformat()
    except Exception:
        decision_ts_et = datetime.utcnow().isoformat() + "Z"
    logger.info(
        "[US_EXIT][DECISION_AUDIT] symbol=%s stop_type=%s entry=%.4f current=%.4f pnl_pct=%.4f trail_high=%.4f trail_dd_pct=%.4f threshold=%.4f",
        symbol, stop_type, entry_price, current_price, pnl_pct, trail_high, trail_drawdown_pct, threshold,
    )

    return {
        "symbol": symbol,
        "exchange": exchange,
        "side": "SELL",
        "qty": qty,
        "available_qty": qty,   # risk gate SELL qty <= available_qty 확인용
        "limit_price": round(current_price * 0.998, 4),  # 0.2% 슬리피지 허용
        "notional_usd": round(current_price * qty, 4),
        "exit_type": exit_type,
        "exit_reason": exit_reason,
        "exit_reason_detail": exit_reason_detail,
        "exit_policy": "US_SWING_DEFAULT",
        "reason": reason,
        "unrealized_pnl_usd": round(unrealized_pnl_usd, 4),
        "unrealized_pnl_pct": round(pnl_pct, 4),
        "client_order_key": client_order_key,
        "strategy": "us_pb1_exit",
        "partial_allowed": False if exit_type == EXIT_HARD_STOP_LOSS else (exit_type not in {"persistent_soft_stop_full_exit"}),
        "leg_no": leg_no,
        "trade_date": trade_date,
        "meta": {
            "holding_qty": _holding,
            "orderable_qty": _orderable,
            "sellable_qty": _orderable,
            "qty_source": "orderable_qty_clamp" if qty < _holding else "holding_qty",
            "sell_reason": reason,
            "partial_allowed": False if exit_type == EXIT_HARD_STOP_LOSS else (exit_type not in {"persistent_soft_stop_full_exit"}),
            "leg_no": leg_no,
            "position_snapshot_qty": position_snapshot_qty,
            "stop_type": stop_type,
            "exit_reason": exit_reason,
            "exit_reason_detail": exit_reason_detail,
            "exit_policy": "US_SWING_DEFAULT",
            "entry_price": entry_price,
            "avg_cost": entry_price,
            "current_price": current_price,
            "pnl_pct_from_avg_cost": round(pnl_pct, 6),
            "hard_stop_threshold_pct": cfg["hard_stop"],
            "trail_high_price": trail_high,
            "trail_drawdown_pct": round(trail_drawdown_pct, 6),
            "trailing_stop_threshold_pct": cfg["trailing_stop"],
            "soft_stop_threshold_pct": cfg["soft_stop"],
            "trailing_activation_profit_pct": cfg["trailing_activation_profit"],
            "price_source": "provider_current_price",
            "qty": qty,
            "decision_ts_et": decision_ts_et,
        },
    }


def _make_hold_intent(
    symbol: str,
    exit_type: str,
    reason: str,
    pnl_pct: float,
    breach_count: int,
    required_ticks: int,
    now: datetime | None = None,
) -> dict:
    try:
        from zoneinfo import ZoneInfo
        decision_ts_et = (now or datetime.now(tz=ZoneInfo("America/New_York"))).astimezone(ZoneInfo("America/New_York")).isoformat()
    except Exception:
        decision_ts_et = datetime.utcnow().isoformat() + "Z"
    exit_reason, exit_reason_detail = _canonical_exit_reason(exit_type)
    logger.info(
        "[US_EXIT][HOLD] symbol=%s exit_type=%s exit_reason=%s exit_reason_detail=%s reason=%s pnl_pct=%.4f soft_stop_breach_count=%d required_ticks=%d",
        symbol, exit_type, exit_reason, exit_reason_detail, reason, pnl_pct, breach_count, required_ticks,
    )
    return {
        "symbol": symbol,
        "side": "HOLD",
        "action": "PARTIAL_SOFT_STOP_WAIT",
        "qty": 0,
        "exit_type": exit_type,
        "exit_reason": exit_reason,
        "exit_reason_detail": exit_reason_detail,
        "exit_policy": "US_SWING_DEFAULT",
        "reason": reason,
        "unrealized_pnl_pct": round(pnl_pct, 4),
        "meta": {
            "full_sell": False,
            "soft_stop_breach_count": breach_count,
            "soft_stop_required_ticks": required_ticks,
            "decision_ts_et": decision_ts_et,
        },
    }


def _resolve_position_entry_price(position: dict) -> float:
    for field in ("entry_price", "avg_price_usd", "avg_cost", "average_price", "avg_buy_price"):
        v = position.get(field)
        if v is not None:
            try:
                fv = float(v)
                if fv > 0:
                    return fv
            except (TypeError, ValueError):
                pass
    qty = int(position.get("qty") or position.get("holding_qty") or 0)
    buy_amount = position.get("buy_amount_usd")
    if buy_amount is not None and qty > 0:
        try:
            ep = float(buy_amount) / qty
            if ep > 0:
                return ep
        except (TypeError, ValueError, ZeroDivisionError):
            pass
    return 0.0


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
        logger.info("[US_EXIT][NO_SIGNAL] positions=0")
        return intents

    logger.info("[US_EXIT][EVAL][START] positions=%d", len(positions))

    skipped_price_fetch_failed: int = 0
    skipped_price_nonpositive: int = 0
    hold_count: int = 0

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
            skipped_price_fetch_failed += 1
            continue

        if current_price <= 0:
            logger.debug("[US_EXIT][SKIP] symbol=%s reason=price_nonpositive price=%.4f", symbol, current_price)
            skipped_price_nonpositive += 1
            continue

        entry_price_for_state = _resolve_position_entry_price(pos)
        if entry_price_for_state > 0 and current_price > 0:
            try:
                from trader.us.db.repos import update_us_soft_stop_risk_state
                pnl_pct_for_state = (current_price - entry_price_for_state) / entry_price_for_state
                risk_state = update_us_soft_stop_risk_state(
                    symbol=symbol,
                    trade_date=_trade_date_from_key(resolve_us_trade_date_key(now)),
                    pnl_pct=pnl_pct_for_state,
                    current_price=current_price,
                    now=now or datetime.now(),
                    soft_stop_pct=float(os.getenv("US_SOFT_STOP_LOSS_PCT", "0.05")),
                )
                pos["risk_state"] = risk_state
                pos["soft_stop_breach_count"] = int(risk_state.get("soft_stop_breach_count") or 0)
            except Exception as exc:
                logger.warning("[US_RISK_STATE][UPDATE_WARN] symbol=%s err=%s", symbol, exc)

        # book/horizon 기반 router 사용 — SWING vs DAY 분리
        # fallback: meta 없으면 SWING_BOOK (기본값)
        from trader.us.pb1.us_exit_router import route_exit_by_book_horizon
        intent = route_exit_by_book_horizon(position=pos, current_price=current_price, now=now)
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # Build exit explanation
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        if intent is not None and intent.get("side") == "SELL":
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
        elif intent is not None and intent.get("side") == "HOLD":
            hold_explanation = build_us_exit_explanation(
                symbol=symbol,
                position=pos,
                exit_intent=None,
                current_price=current_price,
            )
            hold_explanations.append(hold_explanation)
            log_us_exit_decision(symbol, "HOLD", hold_explanation)
            hold_count += 1
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
            hold_count += 1

    # ─────────────────────────────────────────────────────────────────────────
    # Skip/Hold Summary for Observability
    # ─────────────────────────────────────────────────────────────────────────
    exit_intents_count = len(intents)
    logger.info(
        "[US_EXIT][SKIP_SUMMARY] "
        "positions_total=%d evaluated=%d exit_intents=%d hold=%d "
        "skipped_price_fetch_failed=%d skipped_price_nonpositive=%d",
        len(positions),
        len(positions) - skipped_price_fetch_failed - skipped_price_nonpositive,
        exit_intents_count,
        hold_count,
        skipped_price_fetch_failed,
        skipped_price_nonpositive,
    )
    if exit_intents_count == 0:
        logger.info("[US_EXIT][NO_SIGNAL] no exit intents generated positions=%d", len(positions))
    logger.info("[US_EXIT][INTENTS] count=%d", exit_intents_count)

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
