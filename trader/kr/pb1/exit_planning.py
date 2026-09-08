from __future__ import annotations

import json
import logging
import os
from typing import Any

from trader.config import (
    PB1_ABS_TP1_PROFIT_PCT,
    PB1_ABS_TP1_SELL_PCT,
    PB1_CORE_TP1_PROFIT_PCT,
    PB1_CORE_TP1_SELL_PCT,
    PB1_PROFIT_PROTECT_ENABLED,
    PB1_PROFIT_PROTECT_PCT,
    PB1_PROFIT_PROTECT_SELL_PCT,
    PB1_SWING_TP1_PROFIT_PCT,
    PB1_SWING_TP2_PROFIT_PCT,
    PB1_SWING_GIVEBACK_SELL_PCT,
    PB1_TIME_STOP_DAYS,
)

from trader.kr.pb1.effective_exit_risk import resolve_effective_exit_risk_for_pos
from trader.kr.pb1.horizon_utils import calculate_exit_qty
from trader.kr.pb1.exit_policy import resolve_exit_policy

logger = logging.getLogger(__name__)


def resolve_day_protect_exit(
    pos: dict[str, Any],
    mark: float,
    now_hhmm: int,
    *,
    ret_pct: float,
    max_pnl_pct: float,
    stop_hit: bool,
) -> dict[str, Any]:
    """DAY_PROTECT 당일 수익 보호 매도 정책."""
    enabled = os.getenv("PB1_DAY_PROTECT_ENABLED", "1") == "1"
    if not enabled:
        return {"exit_ok": False, "reason": "DAY_PROTECT_DISABLED"}

    stop_loss_pct = float(os.getenv("PB1_DAY_STOP_LOSS_PCT", "2.0"))
    profit_arm_pct = float(os.getenv("PB1_DAY_PROFIT_ARM_PCT", "1.5"))
    breakeven_pct = float(os.getenv("PB1_DAY_BREAKEVEN_PROTECT_PCT", "0.2"))
    trail_arm_pct = float(os.getenv("PB1_DAY_TRAIL_ARM_PCT", "2.0"))
    trail_drop_pct = float(os.getenv("PB1_DAY_TRAIL_DROP_PCT", "1.0"))
    take_profit_pct = float(os.getenv("PB1_DAY_TAKE_PROFIT_PCT", "4.0"))
    take_profit_sell = float(os.getenv("PB1_DAY_TAKE_PROFIT_SELL_PCT", "0.50"))
    close_protect_hhmm = int(os.getenv("PB1_DAY_CLOSE_PROTECT_TIME", "15:05").replace(":", ""))
    force_exit_hhmm = int(os.getenv("PB1_DAY_FORCE_EXIT_TIME", "15:20").replace(":", ""))

    meta = pos.get("position_meta") or {}
    if isinstance(meta, str):
        try:
            meta = json.loads(meta)
        except Exception:
            meta = {}
    tp1_done = bool(meta.get("tp1_done", False))
    orderable_qty = int(pos.get("orderable_qty") or pos.get("qty") or 0)
    drawdown_from_high = max_pnl_pct - ret_pct

    if stop_hit or ret_pct <= -stop_loss_pct:
        qty = calculate_exit_qty(orderable_qty, orderable_qty, None)
        return {"exit_ok": True, "reason": "EXIT_DAY_STOP_LOSS", "qty": qty, "sell_pct": None}

    if now_hhmm >= force_exit_hhmm:
        qty = calculate_exit_qty(orderable_qty, orderable_qty, None)
        return {"exit_ok": True, "reason": "EXIT_DAY_FORCE_CLOSE", "qty": qty, "sell_pct": None}

    if now_hhmm >= close_protect_hhmm and ret_pct <= -1.0:
        qty = calculate_exit_qty(orderable_qty, orderable_qty, None)
        return {"exit_ok": True, "reason": "EXIT_DAY_CLOSE_LOSS_CUT", "qty": qty, "sell_pct": None}

    if now_hhmm >= close_protect_hhmm and ret_pct > 0:
        qty = calculate_exit_qty(orderable_qty, orderable_qty, None)
        return {"exit_ok": True, "reason": "EXIT_DAY_CLOSE_PROFIT_PROTECT", "qty": qty, "sell_pct": None}

    if max_pnl_pct >= profit_arm_pct and ret_pct <= breakeven_pct:
        qty = calculate_exit_qty(orderable_qty, orderable_qty, None)
        return {"exit_ok": True, "reason": "EXIT_DAY_BREAKEVEN_PROTECT", "qty": qty, "sell_pct": None}

    if max_pnl_pct >= trail_arm_pct and drawdown_from_high >= trail_drop_pct:
        qty = calculate_exit_qty(orderable_qty, orderable_qty, None)
        return {"exit_ok": True, "reason": "EXIT_DAY_TRAIL_PROTECT", "qty": qty, "sell_pct": None}

    if ret_pct >= take_profit_pct and not tp1_done:
        qty = calculate_exit_qty(orderable_qty, orderable_qty, take_profit_sell)
        return {
            "exit_ok": True,
            "reason": "EXIT_DAY_TAKE_PROFIT_50",
            "qty": qty,
            "sell_pct": take_profit_sell,
            "update_meta": {"tp1_done": True},
        }

    return {"exit_ok": False, "reason": "DAY_HOLD_PROFIT_OK"}


def resolve_swing_staged_exit(
    pos: dict[str, Any],
    mark: float,
    ma20: float | None,
    *,
    ma50: float | None = None,
    features: dict[str, Any] | None = None,
    regime: str = "",
    ret_pct: float,
    days_held: int,
    stop_hit: bool,
    trail_hit: bool = False,
    trail_stop_price: float | None = None,
    highest_ret_pct: float | None = None,
) -> dict[str, Any]:
    """SWING_CARRY R-multiple 단계별 매도 정책 (Multi-Layer Exit Router 통합)."""
    enabled = os.getenv("PB1_SWING_STAGED_EXIT_ENABLED", "1") == "1"
    if not enabled:
        return {"exit_ok": False, "reason": "SWING_STAGED_EXIT_DISABLED"}

    meta = pos.get("position_meta") or {}
    if isinstance(meta, str):
        try:
            meta = json.loads(meta)
        except Exception:
            meta = {}

    avg = float(pos.get("avg_buy_price") or pos.get("avg") or pos.get("entry_price") or 0.0)
    orderable_qty = int(pos.get("orderable_qty") or pos.get("qty") or 0)
    code_for_log = str(pos.get("code") or pos.get("stock_code") or "UNKNOWN")

    risk_ctx = resolve_effective_exit_risk_for_pos(pos)
    effective_stop = float(risk_ctx["effective_stop_price"] or 0.0)
    effective_r = risk_ctx["effective_r_value"]

    if effective_r is None or effective_r <= 0:
        raw_stop = float(
            meta.get("initial_stop_price")
            or pos.get("stop_price_at_entry")
            or pos.get("stop_price")
            or pos.get("initial_stop")
            or 0.0
        )
        effective_r = avg - raw_stop if avg > raw_stop > 0 else 0.0
        effective_stop = raw_stop

    risk_per_share = float(effective_r) if effective_r and effective_r > 0 else 0.0
    current_r = (mark - avg) / risk_per_share if risk_per_share > 0 else 0.0
    _highest_ret = float(highest_ret_pct) if highest_ret_pct is not None else ret_pct

    logger.info(
        "[EXIT][SWING][R_CTX] code=%s avg=%.2f stop=%.2f mark=%.2f risk_per_share=%.2f "
        "current_r=%.3f highest_ret=%.2f days_held=%s effective_applied=%s",
        code_for_log, avg, effective_stop, mark, risk_per_share, current_r,
        _highest_ret, days_held, int(risk_ctx.get("effective_applied", False)),
    )

    calendar_days_held = int(pos.get("calendar_days_held") or days_held)
    trading_days_held = int(pos.get("trading_days_held") or days_held)
    holding_bars = int(pos.get("holding_bars") or days_held)
    legacy_time_stop_hit = bool(trading_days_held >= int(PB1_TIME_STOP_DAYS) and ret_pct < 2.0)

    router_enabled = os.getenv("PB1_EXIT_ROUTER_ENABLED", "1") == "1"
    if router_enabled:
        from trader.exit_policy.router import (
            apply_swing_exit_decision,
            resolve_exit_policy_for_position,
        )
        policy = resolve_exit_policy_for_position(
            pos=pos,
            features=features or {},
            holding_ctx={
                "days_held": trading_days_held,
                "calendar_days_held": calendar_days_held,
                "trading_days_held": trading_days_held,
                "holding_bars": holding_bars,
                "legacy_time_stop_hit": legacy_time_stop_hit,
                "current_return_pct": ret_pct,
                "current_r": current_r,
                "highest_return_pct": _highest_ret,
                "mark": mark,
            },
            market_ctx={"ma20": ma20, "ma50": ma50, "regime": regime},
        )
        return apply_swing_exit_decision(
            pos, mark, policy,
            ret_pct=ret_pct,
            current_r=current_r,
            highest_ret_pct=_highest_ret,
            days_held=trading_days_held,
            stop_hit=stop_hit,
            trail_hit=trail_hit,
            trail_stop_price=trail_stop_price,
            ma20=ma20,
            effective_stop=effective_stop,
            effective_r=float(effective_r) if effective_r else 0.0,
            risk_ctx=risk_ctx,
        )

    tp1_r = float(os.getenv("PB1_SWING_TP1_R", str(PB1_SWING_TP1_PROFIT_PCT)))
    tp1_sell_pct = float(os.getenv("PB1_SWING_TP1_SELL_PCT", str(PB1_SWING_GIVEBACK_SELL_PCT)))
    tp2_r = float(os.getenv("PB1_SWING_TP2_R", str(PB1_SWING_TP2_PROFIT_PCT)))
    tp2_sell_pct = float(os.getenv("PB1_SWING_TP2_SELL_PCT", str(PB1_SWING_GIVEBACK_SELL_PCT)))
    time_stop_days = int(os.getenv("PB1_SWING_TIME_STOP_DAYS", "10"))

    tp1_done = bool(meta.get("tp1_done", False))
    tp2_done = bool(meta.get("tp2_done", False))
    profit_protect_done = bool(meta.get("profit_protect_done", False))
    abs_tp1_done = bool(meta.get("abs_tp1_done", False))
    max_r = float(meta.get("max_r_since_entry") or 0.0)

    if stop_hit or (effective_stop > 0 and mark <= effective_stop):
        qty = calculate_exit_qty(orderable_qty, orderable_qty, None)
        logger.info(
            "[EXIT][SWING][TP_CHECK] code=%s check=STOP_HIT result=EXIT "
            "reason=STOP_HIT_EFFECTIVE mark=%.2f effective_stop=%.2f",
            code_for_log, mark, effective_stop,
        )
        effective_meta_update = {
            "effective_stop_price": effective_stop,
            "effective_r_value": float(effective_r) if effective_r else None,
            "raw_stop_price": float(risk_ctx["raw_stop_price"]),
            "raw_r_value": float(risk_ctx["raw_r_value"]) if risk_ctx["raw_r_value"] else None,
            "effective_stop_cap_pct": float(risk_ctx["stop_cap_pct"] or 0),
            "effective_exit_policy_version": "2026-04-29-effective-risk-v1",
        }
        return {
            "exit_ok": True,
            "reason": "STOP_HIT_EFFECTIVE",
            "qty": qty,
            "sell_pct": None,
            "update_meta": effective_meta_update,
        }

    profit_protect_pct = float(os.getenv("PB1_PROFIT_PROTECT_PCT", str(PB1_PROFIT_PROTECT_PCT)))
    profit_protect_sell_pct = float(os.getenv("PB1_PROFIT_PROTECT_SELL_PCT", str(PB1_PROFIT_PROTECT_SELL_PCT)))
    profit_protect_enabled = os.getenv("PB1_PROFIT_PROTECT_ENABLED", "1") == "1" and PB1_PROFIT_PROTECT_ENABLED

    if profit_protect_enabled and ret_pct >= profit_protect_pct and not profit_protect_done:
        qty = calculate_exit_qty(orderable_qty, orderable_qty, profit_protect_sell_pct)
        logger.info(
            "[EXIT][PROFIT_PROTECT] code=%s ret_pct=%.2f threshold=%.1f "
            "qty=%s sell_qty=%s reason=PROFIT_PROTECT_8PCT",
            code_for_log, ret_pct, profit_protect_pct, orderable_qty, qty,
        )
        return {
            "exit_ok": True,
            "reason": "PROFIT_PROTECT_8PCT",
            "qty": qty,
            "sell_pct": profit_protect_sell_pct,
            "update_meta": {
                "profit_protect_done": True,
                "profit_protect_price": mark,
                "profit_protect_ret_pct": ret_pct,
                "effective_stop_price": effective_stop,
                "effective_r_value": float(effective_r) if effective_r else None,
                "raw_stop_price": float(risk_ctx["raw_stop_price"]),
                "effective_exit_policy_version": "2026-04-29-effective-risk-v1",
            },
        }

    abs_tp1_pct = float(os.getenv("PB1_ABS_TP1_PROFIT_PCT", str(PB1_ABS_TP1_PROFIT_PCT)))
    abs_tp1_sell_pct = float(os.getenv("PB1_ABS_TP1_SELL_PCT", str(PB1_ABS_TP1_SELL_PCT)))
    abs_tp1_enabled = os.getenv("PB1_ABS_TP1_ENABLED", "1") == "1" and PB1_ABS_TP1_ENABLED

    if abs_tp1_enabled and ret_pct >= abs_tp1_pct and not abs_tp1_done:
        qty = calculate_exit_qty(orderable_qty, orderable_qty, abs_tp1_sell_pct)
        logger.info(
            "[EXIT][ABS_TP1] code=%s ret_pct=%.2f threshold=%.1f "
            "sell_qty=%s reason=ABS_TP1_10PCT",
            code_for_log, ret_pct, abs_tp1_pct, qty,
        )
        return {
            "exit_ok": True,
            "reason": "ABS_TP1_10PCT",
            "qty": qty,
            "sell_pct": abs_tp1_sell_pct,
            "update_meta": {
                "abs_tp1_done": True,
                "abs_tp1_price": mark,
                "abs_tp1_ret_pct": ret_pct,
                "effective_stop_price": effective_stop,
                "effective_r_value": float(effective_r) if effective_r else None,
                "raw_stop_price": float(risk_ctx["raw_stop_price"]),
                "effective_exit_policy_version": "2026-04-29-effective-risk-v1",
            },
        }

    logger.info(
        "[EXIT][SWING][TP_CHECK] code=%s check=TP1 current_r=%.3f tp1_r=%.1f tp1_done=%s",
        code_for_log, current_r, tp1_r, tp1_done,
    )
    if current_r >= tp1_r and not tp1_done:
        qty = calculate_exit_qty(orderable_qty, orderable_qty, tp1_sell_pct)
        new_stop = max(float(meta.get("current_stop_price") or effective_stop or avg), avg)
        logger.info(
            "[EXIT][SWING][TP_CHECK] code=%s result=TP1_HIT qty=%s sell_pct=%.2f new_stop=%.2f",
            code_for_log, qty, tp1_sell_pct, new_stop,
        )
        return {
            "exit_ok": True,
            "reason": "EXIT_SWING_TP1",
            "qty": qty,
            "sell_pct": tp1_sell_pct,
            "update_meta": {
                "tp1_done": True,
                "tp1_price": mark,
                "tp1_qty": qty,
                "current_stop_price": new_stop,
                "runner_qty": orderable_qty - qty,
                "effective_stop_price": effective_stop,
                "effective_r_value": float(effective_r) if effective_r else None,
                "effective_exit_policy_version": "2026-04-29-effective-risk-v1",
            },
        }

    logger.info(
        "[EXIT][SWING][TP_CHECK] code=%s check=TP2 current_r=%.3f tp2_r=%.1f tp2_done=%s",
        code_for_log, current_r, tp2_r, tp2_done,
    )
    if current_r >= tp2_r and not tp2_done:
        qty = calculate_exit_qty(orderable_qty, orderable_qty, tp2_sell_pct)
        logger.info(
            "[EXIT][SWING][TP_CHECK] code=%s result=TP2_HIT qty=%s sell_pct=%.2f",
            code_for_log, qty, tp2_sell_pct,
        )
        return {
            "exit_ok": True,
            "reason": "EXIT_SWING_TP2",
            "qty": qty,
            "sell_pct": tp2_sell_pct,
            "update_meta": {
                "tp2_done": True,
                "tp2_price": mark,
                "tp2_qty": qty,
                "runner_qty": orderable_qty - qty,
                "trail_policy": "MA20_RUNNER",
                "effective_exit_policy_version": "2026-04-29-effective-risk-v1",
            },
        }

    if tp1_done and ma20 is not None and mark < ma20:
        qty = calculate_exit_qty(orderable_qty, orderable_qty, None)
        logger.info(
            "[EXIT][SWING][TP_CHECK] code=%s check=MA20_RUNNER result=EXIT mark=%.2f ma20=%.2f",
            code_for_log, mark, ma20,
        )
        return {"exit_ok": True, "reason": "EXIT_SWING_RUNNER_MA20_BREAK", "qty": qty, "sell_pct": None}

    if days_held >= time_stop_days and current_r < 1.0:
        qty = calculate_exit_qty(orderable_qty, orderable_qty, None)
        logger.info(
            "[EXIT][SWING][TP_CHECK] code=%s check=TIME_STOP result=EXIT "
            "days_held=%s time_stop_days=%s current_r=%.3f",
            code_for_log, days_held, time_stop_days, current_r,
        )
        return {"exit_ok": True, "reason": "EXIT_SWING_TIME_STOP", "qty": qty, "sell_pct": None}

    logger.info(
        "[EXIT][SWING][TP_CHECK] code=%s result=HOLD current_r=%.3f tp1_done=%s tp2_done=%s",
        code_for_log, current_r, tp1_done, tp2_done,
    )
    return {"exit_ok": False, "reason": "SWING_HOLD_TREND_OK"}


def resolve_core_trend_follow_exit(
    pos: dict[str, Any],
    mark: float,
    ma20: float | None,
    ma50: float | None,
    *,
    ret_pct: float,
    days_held: int,
    regime: str,
) -> dict[str, Any]:
    """CORE_CARRY 중기 추세 보유 정책."""
    enabled = os.getenv("PB1_CORE_EXIT_ENABLED", "1") == "1"
    if not enabled:
        return {"exit_ok": False, "reason": "CORE_EXIT_DISABLED"}

    hard_stop_pct = float(os.getenv("PB1_CORE_HARD_STOP_PCT", "8.0"))
    tp1_r = float(os.getenv("PB1_CORE_TP1_R", str(PB1_CORE_TP1_PROFIT_PCT)))
    tp1_sell_pct = float(os.getenv("PB1_CORE_TP1_SELL_PCT", str(PB1_CORE_TP1_SELL_PCT)))
    time_stop_days = int(os.getenv("PB1_CORE_TIME_STOP_DAYS", "20"))

    meta = pos.get("position_meta") or {}
    if isinstance(meta, str):
        try:
            meta = json.loads(meta)
        except Exception:
            meta = {}
    core_tp1_done = bool(meta.get("core_tp1_done") or meta.get("tp1_done", False))

    avg = float(pos.get("avg_buy_price") or pos.get("avg") or pos.get("entry_price") or 0.0)
    initial_stop = float(
        meta.get("initial_stop_price")
        or pos.get("stop_price_at_entry")
        or pos.get("stop_price")
        or 0.0
    )
    orderable_qty = int(pos.get("orderable_qty") or pos.get("qty") or 0)
    risk_per_share = avg - initial_stop if avg > initial_stop > 0 else 0.0
    current_r = (mark - avg) / risk_per_share if risk_per_share > 0 else 0.0

    if ret_pct <= -hard_stop_pct:
        qty = calculate_exit_qty(orderable_qty, orderable_qty, None)
        return {"exit_ok": True, "reason": "EXIT_CORE_HARD_STOP", "qty": qty, "sell_pct": None}

    if current_r >= tp1_r and not core_tp1_done:
        qty = calculate_exit_qty(orderable_qty, orderable_qty, tp1_sell_pct)
        return {
            "exit_ok": True,
            "reason": "EXIT_CORE_TP1",
            "qty": qty,
            "sell_pct": tp1_sell_pct,
            "update_meta": {"core_tp1_done": True, "tp1_done": True, "tp1_price": mark, "tp1_qty": qty},
        }

    bear_regime = str(regime or "").upper() in {"BEAR", "DOWNTREND", "RISK_OFF"}
    if bear_regime and ma20 is not None and ma50 is not None and mark < ma20 and mark < ma50:
        qty = calculate_exit_qty(orderable_qty, orderable_qty, None)
        return {"exit_ok": True, "reason": "EXIT_CORE_RISK_OFF", "qty": qty, "sell_pct": None}

    if ma50 is not None and mark < ma50:
        qty = calculate_exit_qty(orderable_qty, orderable_qty, None)
        return {"exit_ok": True, "reason": "EXIT_CORE_MA50_BREAK", "qty": qty, "sell_pct": None}

    return {"exit_ok": False, "reason": "CORE_HOLD_TREND_OK"}


def resolve_exit_policy_wrapper(
    *,
    days_held: int,
    holding_bars: int,
    stop_hit: bool,
    trail_stop_price: float | None,
    mark: float,
    ma20: float | None,
    ma50: float | None,
    time_stop_hit: bool,
    risk_off_signal: bool,
) -> dict[str, Any]:
    return resolve_exit_policy(
        days_held=days_held,
        holding_bars=holding_bars,
        stop_hit=stop_hit,
        trail_stop_price=trail_stop_price,
        mark=mark,
        ma20=ma20,
        ma50=ma50,
        time_stop_hit=time_stop_hit,
        risk_off_signal=risk_off_signal,
    )
