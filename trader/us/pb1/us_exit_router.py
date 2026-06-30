# -*- coding: utf-8 -*-
"""US Exit Router — book/horizon 기반 청산 전략 라우터.

SWING_BOOK / DAY_BOOK 분리 원칙:
- SWING_BOOK + SWING_CARRY: 당일 soft exit 차단 (min_hold 경과 전)
  - hard_stop, pnl_missing_fail_closed, risk_off_exit, manual_force_exit 항상 허용
- DAY_BOOK + DAY_TRADE: 당일 수익보호/close flatten 허용
- fallback: meta 없으면 SWING_BOOK

중요:
- book / horizon은 "왜 샀는가"(entry_signal_type)와 다르다.
- pullback으로 샀어도 SWING_BOOK으로 관리할 수 있다.
- SWING_BOOK이 +1~2% 수익에서 팔리지 않는 것이 핵심 목적이다.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)


# ── book / horizon 정규화 ──────────────────────────────────────────────────

_SWING_ALIASES = {"swing_book", "swing", "swing-book", "swing_carry", "swing-carry", "overnight"}
_DAY_ALIASES = {"day_book", "day", "day-book", "day_trade", "day-trade", "intraday"}


def normalize_book(raw: str | None) -> str:
    """book 값을 SWING_BOOK 또는 DAY_BOOK으로 정규화.

    미설정/인식불가 → 환경변수 US_DEFAULT_ENTRY_BOOK (기본 SWING_BOOK)
    """
    if not raw:
        return os.getenv("US_DEFAULT_ENTRY_BOOK", "SWING_BOOK")
    n = raw.strip().lower()
    if n in _DAY_ALIASES:
        return "DAY_BOOK"
    if n in _SWING_ALIASES:
        return "SWING_BOOK"
    logger.warning("[US_EXIT_ROUTER][NORMALIZE_BOOK][FALLBACK] raw=%r → SWING_BOOK", raw)
    return "SWING_BOOK"


def normalize_horizon(raw: str | None) -> str:
    """horizon 값을 SWING_CARRY 또는 DAY_TRADE로 정규화."""
    if not raw:
        return os.getenv("US_DEFAULT_ENTRY_HORIZON", "SWING_CARRY")
    n = raw.strip().lower()
    if n in {"day_trade", "day-trade", "intraday", "day_book", "day"}:
        return "DAY_TRADE"
    if n in {"swing_carry", "swing-carry", "swing_book", "swing", "overnight"}:
        return "SWING_CARRY"
    logger.warning("[US_EXIT_ROUTER][NORMALIZE_HORIZON][FALLBACK] raw=%r → SWING_CARRY", raw)
    return "SWING_CARRY"


# ── exit type 분류 ─────────────────────────────────────────────────────────

# soft exit: 수익 보호 목적 — SWING_BOOK 당일 차단 대상
_SOFT_EXIT_TYPES: frozenset[str] = frozenset({
    "profit_protect",
    "trailing_stop",
    "soft_stop_loss",
    "profit_trailing_stop",
    "giveback",
    "time_stop",
    "weak_momentum_exit",
    "day_profit_take",
    "day_trailing",
    "day_close_flatten",
})

# hard exit: 손실 방어/위험 off — 당일이어도 항상 허용
_HARD_EXIT_TYPES: frozenset[str] = frozenset({
    "hard_stop",
    "hard_stop_loss",
    "intraday_catastrophic_stop",
    "pnl_missing_fail_closed",
    "risk_off_exit",
    "manual_force_exit",
})


def _is_soft_exit(exit_type: str) -> bool:
    return exit_type in _SOFT_EXIT_TYPES


def _is_hard_exit(exit_type: str) -> bool:
    return exit_type in _HARD_EXIT_TYPES


# ── min_hold guard ─────────────────────────────────────────────────────────

def _min_hold_elapsed(position: dict, now: datetime | None) -> tuple[bool, int, int]:
    """min_hold_minutes 경과 여부 반환.

    Returns:
        (elapsed: bool, held_minutes: int, required_minutes: int)
    """
    required = int(
        position.get("min_hold_minutes")
        or os.getenv("US_SWING_MIN_HOLD_MINUTES", "390")
    )
    # 차단 기능 꺼져 있으면 바로 허용
    if os.getenv("US_SWING_BLOCK_SAME_DAY_SOFT_EXIT", "1") not in {"1", "true", "True", "yes"}:
        return True, 0, required

    entry_time_raw = (
        position.get("entry_time")
        or position.get("created_at")
        or position.get("entry_at")
    )
    if not entry_time_raw:
        return True, 0, required  # 시간 모르면 허용

    if now is None:
        now = datetime.now(timezone.utc)

    try:
        if isinstance(entry_time_raw, str):
            et = datetime.fromisoformat(entry_time_raw.replace("Z", "+00:00"))
        elif isinstance(entry_time_raw, datetime):
            et = entry_time_raw
        else:
            return True, 0, required

        if et.tzinfo is None:
            et = et.replace(tzinfo=timezone.utc)
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)

        held = int((now - et).total_seconds() / 60)
        return held >= required, held, required
    except Exception as exc:
        logger.warning("[US_EXIT_ROUTER][MIN_HOLD_CALC][WARN] %s", exc)
        return True, 0, required


# ── SWING exit ─────────────────────────────────────────────────────────────

def evaluate_swing_exit(
    position: dict,
    current_price: float,
    now: datetime | None = None,
) -> dict | None:
    """SWING_BOOK 포지션 청산 평가.

    same-day soft exit 차단:
    - hard/risk exit 타입은 당일이어도 항상 허용
    - soft exit는 min_hold_minutes 경과 후만 허용
    - +1~2% 수익에서 당일 팔리지 않는 것이 목적
    """
    from trader.us.pb1.us_exit_engine import evaluate_exit

    intent = evaluate_exit(position=position, current_price=current_price, now=now)
    if intent is None:
        return None

    exit_type = intent.get("exit_type", "")

    # hard exit: 항상 즉시 허용
    if _is_hard_exit(exit_type):
        logger.info(
            "[US_EXIT][SWING_GUARD][ALLOW_HARD_EXIT] symbol=%s exit_type=%s pnl_pct=%.4f",
            position.get("symbol"), exit_type,
            intent.get("unrealized_pnl_pct", 0),
        )
        return intent

    # soft exit: min_hold 경과 여부 확인
    if _is_soft_exit(exit_type):
        elapsed, held, required = _min_hold_elapsed(position, now)
        if not elapsed:
            if exit_type == "soft_stop_loss":
                original_qty = int(intent.get("qty") or 0)
                capped_qty = max(1, min(original_qty, int((position.get("qty") or original_qty) * 0.5))) if original_qty > 0 else 0
                intent["qty"] = capped_qty
                intent["available_qty"] = capped_qty
                intent["notional_usd"] = round(float(intent.get("limit_price") or current_price) * capped_qty, 4)
                intent.setdefault("meta", {})["min_hold_blocks_full_soft_exit"] = True
                intent["reason"] = f"{intent.get('reason', '')}; min_hold_blocks_full_soft_exit"
                logger.info(
                    "[US_EXIT][SWING_GUARD][ALLOW_PARTIAL_SOFT_EXIT] symbol=%s exit_type=%s "
                    "reason=min_hold_blocks_full_soft_exit held_minutes=%d required_minutes=%d qty=%d",
                    position.get("symbol"), exit_type, held, required, capped_qty,
                )
                return intent
            logger.info(
                "[US_EXIT][SWING_GUARD][BLOCK_SOFT_EXIT] symbol=%s exit_type=%s "
                "reason=same_day_min_hold book=SWING_BOOK horizon=SWING_CARRY "
                "held_minutes=%d required_minutes=%d",
                position.get("symbol"), exit_type, held, required,
            )
            return None
        logger.info(
            "[US_EXIT][SWING_GUARD][ALLOW_SOFT_EXIT] symbol=%s exit_type=%s "
            "held_minutes=%d required_minutes=%d",
            position.get("symbol"), exit_type, held, required,
        )

    return intent


# ── DAY exit ───────────────────────────────────────────────────────────────

def _get_entry_price(position: dict) -> float:
    for field in ("entry_price", "avg_price_usd", "avg_cost", "average_price", "avg_buy_price"):
        v = position.get(field)
        if v is not None:
            try:
                fv = float(v)
                if fv > 0:
                    return fv
            except (TypeError, ValueError):
                pass
    qty = int(position.get("qty") or position.get("holding_qty") or 1)
    ba = position.get("buy_amount_usd")
    if ba and qty > 0:
        try:
            ep = float(ba) / qty
            if ep > 0:
                return ep
        except (TypeError, ValueError, ZeroDivisionError):
            pass
    return 0.0


def _make_day_exit_intent(
    symbol: str, exchange: str, qty: int,
    current_price: float, entry_price: float,
    exit_type: str, reason: str, pnl_pct: float,
) -> dict:
    import hashlib
    from datetime import date
    key_raw = f"{symbol}_{date.today():%Y%m%d}_SELL_{exit_type}"
    client_order_key = hashlib.sha256(key_raw.encode()).hexdigest()[:24]
    unrealized = (current_price - entry_price) * qty
    logger.info(
        "[US_EXIT][DAY_CHECK] symbol=%s pnl_pct=%.4f exit_type=%s reason=%s",
        symbol, pnl_pct, exit_type, reason,
    )
    return {
        "symbol": symbol,
        "exchange": exchange,
        "side": "SELL",
        "qty": qty,
        "available_qty": qty,
        "limit_price": round(current_price * 0.998, 4),
        "notional_usd": round(current_price * qty, 4),
        "exit_type": exit_type,
        "reason": reason,
        "unrealized_pnl_usd": round(unrealized, 4),
        "unrealized_pnl_pct": round(pnl_pct, 4),
        "client_order_key": client_order_key,
        "strategy": "us_pb1_exit",
        "exit_policy": "DAY_BOOK",
        "book": "DAY_BOOK",
        "horizon": "DAY_TRADE",
        "meta": {"holding_qty": qty, "orderable_qty": qty, "sellable_qty": qty, "qty_source": "holding_qty"},
    }


def evaluate_day_exit(
    position: dict,
    current_price: float,
    now: datetime | None = None,
) -> dict | None:
    """DAY_BOOK 포지션 청산 평가.

    - hard_stop: 기본 -3% (US_DAY_HARD_STOP_PCT)
    - day_profit_take: +2.5% (US_DAY_PROFIT_TAKE_PCT)
    - day_trailing: 고점 대비 -2.5% (US_DAY_TRAILING_STOP_PCT)
    - day_close_flatten: 장마감 N분 전 (US_DAY_CLOSE_FLATTEN_AFTER_ET)
    """
    from trader.us.pb1.us_exit_engine import evaluate_exit

    # hard_stop 먼저 base evaluate_exit로 확인
    base_intent = evaluate_exit(position=position, current_price=current_price, now=now)
    if base_intent and _is_hard_exit(base_intent.get("exit_type", "")):
        base_intent["exit_policy"] = "DAY_BOOK"
        base_intent["book"] = "DAY_BOOK"
        return base_intent

    symbol = position.get("symbol", "")
    exchange = position.get("exchange", "NASDAQ")
    entry_price = _get_entry_price(position)
    qty = int(position.get("qty") or position.get("holding_qty") or 0)

    if entry_price <= 0 or current_price <= 0 or qty <= 0:
        return None

    pnl_pct = (current_price - entry_price) / entry_price

    # day_hard_stop: 데이 전용 타이트 스탑
    day_hard_stop = float(os.getenv("US_DAY_HARD_STOP_PCT", "0.03"))
    if pnl_pct <= -day_hard_stop:
        return _make_day_exit_intent(
            symbol, exchange, qty, current_price, entry_price,
            "day_hard_stop",
            f"pnl_pct={pnl_pct:.4f} <= -{day_hard_stop}",
            pnl_pct,
        )

    # day_profit_take: 당일 익절
    day_profit_take = float(os.getenv("US_DAY_PROFIT_TAKE_PCT", "0.025"))
    if pnl_pct >= day_profit_take:
        return _make_day_exit_intent(
            symbol, exchange, qty, current_price, entry_price,
            "day_profit_take",
            f"pnl_pct={pnl_pct:.4f} >= day_profit_take={day_profit_take}",
            pnl_pct,
        )

    # day_trailing: 당일 고점 대비 하락
    max_price = float(position.get("max_price") or current_price)
    day_trailing = float(os.getenv("US_DAY_TRAILING_STOP_PCT", "0.025"))
    if max_price > 0 and current_price < max_price * (1 - day_trailing):
        trail = (max_price - current_price) / max_price
        return _make_day_exit_intent(
            symbol, exchange, qty, current_price, entry_price,
            "day_trailing",
            f"trail_pct={trail:.4f} >= day_trailing={day_trailing}",
            pnl_pct,
        )

    # day_close_flatten: 장마감 전 강제 청산
    if now is not None:
        flatten_after_et = os.getenv("US_DAY_CLOSE_FLATTEN_AFTER_ET", "15:45")
        try:
            from zoneinfo import ZoneInfo
            ny = ZoneInfo("America/New_York")
            now_et = now.astimezone(ny)
            hh, mm = map(int, flatten_after_et.split(":"))
            flatten_dt = now_et.replace(hour=hh, minute=mm, second=0, microsecond=0)
            if now_et >= flatten_dt:
                logger.info(
                    "[US_EXIT][DAY_CLOSE_FLATTEN] symbol=%s reason=day_trade_close_flatten "
                    "now_et=%s flatten_after=%s",
                    symbol, now_et.strftime("%H:%M:%S"), flatten_after_et,
                )
                return _make_day_exit_intent(
                    symbol, exchange, qty, current_price, entry_price,
                    "day_close_flatten",
                    f"close_flatten after {flatten_after_et} ET",
                    pnl_pct,
                )
        except Exception as exc:
            logger.warning("[US_EXIT_ROUTER][DAY][CLOSE_FLATTEN_CALC][WARN] %s", exc)

    return None


# ── 메인 라우터 ────────────────────────────────────────────────────────────

def route_exit_by_book_horizon(
    position: dict,
    current_price: float,
    now: datetime | None = None,
) -> dict | None:
    """book/horizon 기반 청산 평가 라우터.

    position.book / position.meta.book 순으로 확인한다.
    없으면 SWING_BOOK fallback.

    Args:
        position: 포지션 dict
        current_price: 현재가 (USD)
        now: 현재 시각

    Returns:
        exit intent dict 또는 None
    """
    meta = position.get("meta") if isinstance(position.get("meta"), dict) else {}
    raw_book = position.get("book") or meta.get("book")
    raw_horizon = position.get("horizon") or meta.get("horizon")

    book = normalize_book(raw_book)
    horizon = normalize_horizon(raw_horizon)

    logger.info(
        "[US_EXIT][ROUTER] symbol=%s book=%s horizon=%s router=%s",
        position.get("symbol"), book, horizon,
        "DAY_EXIT_ROUTER" if (book == "DAY_BOOK" or horizon == "DAY_TRADE") else "SWING_EXIT_ROUTER",
    )

    if book == "DAY_BOOK" or horizon == "DAY_TRADE":
        intent = evaluate_day_exit(position=position, current_price=current_price, now=now)
        if intent:
            intent.setdefault("exit_policy", "DAY_BOOK")
        return intent

    # SWING_BOOK (기본)
    intent = evaluate_swing_exit(position=position, current_price=current_price, now=now)
    if intent:
        intent.setdefault("exit_policy", "US_SWING_DEFAULT")
        intent.setdefault("book", "SWING_BOOK")
        intent.setdefault("horizon", "SWING_CARRY")
    return intent
