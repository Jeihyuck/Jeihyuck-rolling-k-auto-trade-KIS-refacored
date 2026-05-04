"""trader/exit_policy/router.py

Multi-Layer Exit Router.

포지션의 매수 유형(entry_style / trade_horizon / exit_family)에 따라
exit policy를 결정하고 매도 결정을 내린다.

핵심 원칙:
- R은 손절, 리스크, 포지션 사이징, 돌파 실패 판단에 계속 사용한다.
- 익절은 R 단독이 아니라 수익률%, 고점 대비 반납, 추세 훼손, 보유일수, 매수 유형을 함께 사용한다.
- 매수 유형이 다르면 매도 기준도 달라야 한다.
- 삼성E&A 같은 특정 종목을 팔기 위해 임의의 수익률을 하드코딩하지 않는다.

Exit Family:
  INTRADAY_PROFIT_PROTECT  ← DAY_PROTECT / ENTRY_BREAKOUT / ENTRY_MOMENTUM / ENTRY_OPEN_PUSH
  SWING_STAGED_EXIT        ← SWING_CARRY / ENTRY_PULLBACK / ENTRY_VCP / ENTRY_MINERVINI (default)
  CORE_TREND_FOLLOW        ← CORE_CARRY / score_final >= 85 + trend + low ATR
"""
from __future__ import annotations

import logging
import os
from typing import Any

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────
# 내부 헬퍼
# ─────────────────────────────────────────────────────────────────────

def _ef(key: str, default: float) -> float:
    """os.getenv 기반 float 읽기 (monkeypatch 즉시 반영)."""
    try:
        return float(os.getenv(key, str(default)))
    except (ValueError, TypeError):
        return default


def _eb(key: str, default: bool = True) -> bool:
    """os.getenv 기반 bool 읽기."""
    val = os.getenv(key)
    if val is None:
        return default
    return val.strip() not in {"0", "false", "False", "FALSE", "no", "No", "NO"}


def _calculate_exit_qty(orderable_qty: int, sell_pct: float | None) -> int:
    orderable = max(0, int(orderable_qty or 0))
    if sell_pct is None:
        return orderable
    qty = int(orderable * float(sell_pct))
    if qty < 1 and orderable > 0:
        qty = 1
    return min(qty, orderable)


def _parse_meta(pos: dict[str, Any]) -> dict[str, Any]:
    meta = pos.get("position_meta") or {}
    if isinstance(meta, str):
        try:
            import json as _json
            meta = _json.loads(meta)
        except Exception:
            meta = {}
    return meta


# ─────────────────────────────────────────────────────────────────────
# 공개 API 1: Policy 결정
# ─────────────────────────────────────────────────────────────────────

def resolve_exit_policy_for_position(
    pos: dict[str, Any],
    features: dict[str, Any],
    holding_ctx: dict[str, Any],
    market_ctx: dict[str, Any],
) -> dict[str, Any]:
    """포지션의 매수 유형과 시장 상태를 기준으로 exit policy를 반환한다.

    입력:
        pos: 포지션 dict (entry_style_selected, entry_reason, trade_horizon,
                          exit_policy_family, avg_buy_price, ...)
        features: 스코어/특성 dict (market, atr_pct, score_final, rs_percentile,
                                    vcp_score, ...)
        holding_ctx: 보유 컨텍스트 (days_held, same_day, current_return_pct,
                                     current_r, highest_return_pct,
                                     drawdown_from_peak_pct, mark, position_meta)
        market_ctx: 시장 컨텍스트 (ma20, ma50, regime, atr_pct)

    출력:
        exit_family: 적용 exit family 이름
        hard_stop_enabled: bool
        r_take_profit_enabled: bool  ← R은 신호로 유지, 단독 판단 금지
        percent_take_profit_enabled: bool
        profit_protect_enabled: bool  ← giveback 기반
        trend_follow_enabled: bool
        time_stop_enabled: bool
        max_hold_days: int
        partial_sell_rules: list[dict]
        full_exit_rules: list[dict]
        router_enabled: bool
    """
    enabled = _eb("PB1_EXIT_ROUTER_ENABLED", default=True)

    # ── entry style / exit family 결정 ──────────────────────────────
    entry_style = str(
        pos.get("entry_style_selected")
        or pos.get("entry_reason")
        or features.get("entry_style_selected")
        or features.get("entry_reason")
        or "ENTRY_UNKNOWN"
    ).upper()

    exit_family = str(
        pos.get("exit_policy_family")
        or features.get("exit_policy_family")
        or ""
    ).strip()

    trade_horizon = str(
        pos.get("trade_horizon")
        or features.get("trade_horizon")
        or ""
    ).strip()

    if not exit_family:
        if entry_style in {"ENTRY_BREAKOUT", "ENTRY_MOMENTUM", "ENTRY_OPEN_PUSH"}:
            exit_family = "INTRADAY_PROFIT_PROTECT"
        elif entry_style in {"ENTRY_PULLBACK", "ENTRY_VCP", "ENTRY_MINERVINI"}:
            exit_family = "SWING_STAGED_EXIT"
        elif trade_horizon == "DAY_PROTECT":
            exit_family = "INTRADAY_PROFIT_PROTECT"
        elif trade_horizon == "CORE_CARRY":
            exit_family = "CORE_TREND_FOLLOW"
        else:
            exit_family = "SWING_STAGED_EXIT"

    # ── 보유 컨텍스트 ────────────────────────────────────────────────
    calendar_days_held = int(holding_ctx.get("calendar_days_held") or holding_ctx.get("days_held") or 0)
    trading_days_held = int(holding_ctx.get("trading_days_held") or holding_ctx.get("days_held") or 0)
    holding_bars = int(holding_ctx.get("holding_bars") or 0)
    legacy_time_stop_hit = holding_ctx.get("legacy_time_stop_hit")
    days_held = trading_days_held
    same_day = bool(holding_ctx.get("same_day", trading_days_held == 0))
    current_return_pct = float(holding_ctx.get("current_return_pct") or 0.0)
    current_r = float(holding_ctx.get("current_r") or 0.0)
    highest_return_pct = float(
        holding_ctx.get("highest_return_pct") or current_return_pct
    )
    drawdown_from_peak_pct = float(
        holding_ctx.get("drawdown_from_peak_pct")
        or max(0.0, highest_return_pct - current_return_pct)
    )

    # ── 시장 컨텍스트 ────────────────────────────────────────────────
    ma20 = market_ctx.get("ma20")
    ma50 = market_ctx.get("ma50")
    regime = str(market_ctx.get("regime") or "").upper()
    atr_pct = float(
        market_ctx.get("atr_pct") or features.get("atr_pct") or 2.0
    )

    mark = float(holding_ctx.get("mark") or holding_ctx.get("current_price") or 0.0)
    trend_ok = (
        (ma20 is None or mark <= 0 or mark >= float(ma20))
        and regime not in {"BEAR", "RISK_OFF", "DOWNTREND"}
    )
    trend_strong = trend_ok and (ma50 is None or mark <= 0 or mark >= float(ma50))

    score_final = float(features.get("score_final") or features.get("score") or 0.0)

    code_for_log = str(pos.get("code") or pos.get("pdno") or "UNKNOWN")

    logger.info(
        "[EXIT][ROUTER][CTX] code=%s entry_style=%s exit_family=%s trade_horizon=%s "
        "ret_pct=%.2f current_r=%.2f highest_ret=%.2f drawdown=%.2f "
        "calendar_days_held=%d trading_days_held=%d holding_bars=%d trend_ok=%s trend_strong=%s regime=%s router=%s",
        code_for_log, entry_style, exit_family, trade_horizon,
        current_return_pct, current_r, highest_return_pct, drawdown_from_peak_pct,
        calendar_days_held, trading_days_held, holding_bars, int(trend_ok), int(trend_strong), regime, int(enabled),
    )

    # ── router 비활성화 → minimal fallback ──────────────────────────
    if not enabled:
        return {
            "exit_family": exit_family,
            "entry_style": entry_style,
            "trade_horizon": trade_horizon,
            "hard_stop_enabled": True,
            "r_take_profit_enabled": True,
            "percent_take_profit_enabled": False,
            "profit_protect_enabled": False,
            "trend_follow_enabled": False,
            "time_stop_enabled": True,
            "max_hold_days": 10,
            "partial_sell_rules": [],
            "full_exit_rules": [],
            "trend_ok": trend_ok,
            "trend_strong": trend_strong,
            "router_enabled": False,
        }

    # ── family별 policy ──────────────────────────────────────────────
    if exit_family == "INTRADAY_PROFIT_PROTECT":
        policy = _policy_intraday(
            entry_style=entry_style,
            same_day=same_day,
            days_held=days_held,
            trend_ok=trend_ok,
        )
    elif exit_family == "CORE_TREND_FOLLOW":
        policy = _policy_core(
            score_final=score_final,
            trend_strong=trend_strong,
            atr_pct=atr_pct,
            days_held=days_held,
        )
    else:  # SWING_STAGED_EXIT (default)
        policy = _policy_swing(
            entry_style=entry_style,
            trend_ok=trend_ok,
            trend_strong=trend_strong,
            days_held=days_held,
        )

    policy.update({
        "exit_family": exit_family,
        "entry_style": entry_style,
        "trade_horizon": trade_horizon,
        "trend_ok": trend_ok,
        "trend_strong": trend_strong,
        "router_enabled": True,
        "time_stop_basis": "trading_days",
    })

    logger.info(
        "[EXIT][ROUTER][POLICY] code=%s exit_family=%s entry_style=%s "
        "r_tp=%s pct_tp=%s protect=%s trend=%s time=%s max_hold=%s",
        code_for_log, exit_family, entry_style,
        int(policy.get("r_take_profit_enabled", False)),
        int(policy.get("percent_take_profit_enabled", False)),
        int(policy.get("profit_protect_enabled", False)),
        int(policy.get("trend_follow_enabled", False)),
        int(policy.get("time_stop_enabled", False)),
        policy.get("max_hold_days"),
    )

    return policy


# ─────────────────────────────────────────────────────────────────────
# 내부: Family별 policy 빌더
# ─────────────────────────────────────────────────────────────────────

def _policy_intraday(
    *, entry_style: str, same_day: bool, days_held: int, trend_ok: bool
) -> dict[str, Any]:
    """INTRADAY_PROFIT_PROTECT: 당일 수익 보호 정책."""
    return {
        "hard_stop_enabled": True,
        "r_take_profit_enabled": True,
        "percent_take_profit_enabled": True,
        "profit_protect_enabled": True,
        "trend_follow_enabled": False,
        "time_stop_enabled": True,
        "max_hold_days": 1,
        "partial_sell_rules": [
            {
                "trigger": "giveback",
                "activate_pct": _ef("PB1_MOMENTUM_PROFIT_ACTIVATE_PCT", 5.0),
                "giveback_pct": _ef("PB1_MOMENTUM_PROFIT_GIVEBACK_PCT", 2.0),
                "floor_pct": 0.0,
                "sell_pct": 0.50,
            },
            {
                "trigger": "r_hybrid",
                "r": _ef("PB1_MOMENTUM_TP1_R", 1.5),
                "sell_pct": 0.33,
                "meta_flag": "tp1_done",
                "block_if_trend_strong": False,
            },
        ],
        "full_exit_rules": [
            {"trigger": "pivot_break"},
            {
                "trigger": "r_hybrid",
                "r": _ef("PB1_MOMENTUM_TP2_R", 2.5),
                "sell_pct": None,
                "meta_flag": "tp2_done",
                "block_if_trend_strong": False,
            },
        ],
    }


def _policy_swing(
    *, entry_style: str, trend_ok: bool, trend_strong: bool, days_held: int
) -> dict[str, Any]:
    """SWING_STAGED_EXIT: R + % + giveback 복합 정책.

    - 강한 추세에서는 R 도달만으로 전량 매도하지 않는다.
    - giveback 기반 보호익절: peak 대비 일정 %p 이상 반납 시 부분 매도.
    - 수익률 기준 TP: ret >= 12%면 부분 매도.
    - MA20 추세 훼손 + TP1 이후: 잔여 청산.
    """
    return {
        "hard_stop_enabled": True,
        "r_take_profit_enabled": True,      # R은 신호; 단독 판단 금지
        "percent_take_profit_enabled": True,
        "profit_protect_enabled": True,
        "trend_follow_enabled": True,
        "time_stop_enabled": True,
        "max_hold_days": int(os.getenv("PB1_SWING_TIME_STOP_DAYS", "10")),
        "trend_strong": trend_strong,
        "trend_ok": trend_ok,
        "partial_sell_rules": [
            # [A] giveback 보호: peak >= activate AND 반납 >= giveback AND current >= floor
            {
                "trigger": "giveback",
                "activate_pct": _ef("PB1_SWING_PROFIT_ACTIVATE_PCT", 8.0),
                "giveback_pct": _ef("PB1_SWING_PROFIT_GIVEBACK_PCT", 3.0),
                "floor_pct": _ef("PB1_SWING_PROFIT_FLOOR_PCT", 5.0),
                "sell_pct": _ef("PB1_SWING_TP1_SELL_PCT", 0.33),
                "meta_flag": "giveback_protect_done",
            },
            # [B] 수익률 기준 TP1: ret >= 12%
            {
                "trigger": "percent",
                "profit_pct": _ef("PB1_SWING_TP1_PROFIT_PCT", 12.0),
                "sell_pct": _ef("PB1_SWING_TP1_SELL_PCT", 0.33),
                "meta_flag": "tp1_done",
            },
            # [C] R 기준 TP1: trend 강하면 억제 (partial hold runner)
            {
                "trigger": "r_hybrid",
                "r": _ef("PB1_SWING_TP1_R", 2.0),
                "sell_pct": _ef("PB1_SWING_TP1_SELL_PCT", 0.33),
                "meta_flag": "tp1_done",
                "block_if_trend_strong": True,
            },
            # [D] 수익률 기준 TP2: ret >= 18%
            {
                "trigger": "percent",
                "profit_pct": _ef("PB1_SWING_TP2_PROFIT_PCT", 18.0),
                "sell_pct": _ef("PB1_SWING_TP2_SELL_PCT", 0.33),
                "meta_flag": "tp2_done",
            },
            # [E] R 기준 TP2: trend 강하면 억제
            {
                "trigger": "r_hybrid",
                "r": _ef("PB1_SWING_TP2_R", 3.0),
                "sell_pct": _ef("PB1_SWING_TP2_SELL_PCT", 0.33),
                "meta_flag": "tp2_done",
                "block_if_trend_strong": True,
            },
        ],
        "full_exit_rules": [
            {"trigger": "ma20_break_after_tp1"},
            {"trigger": "time_stop", "min_r": 1.0},
        ],
    }


def _policy_core(
    *, score_final: float, trend_strong: bool, atr_pct: float, days_held: int
) -> dict[str, Any]:
    """CORE_TREND_FOLLOW: 중기 추세 보유 정책. 조기 익절 금지."""
    return {
        "hard_stop_enabled": True,
        "r_take_profit_enabled": False,      # R 단독 TP 금지
        "percent_take_profit_enabled": True,
        "profit_protect_enabled": False,     # 단기 반납에 반응하지 않음
        "trend_follow_enabled": True,
        "time_stop_enabled": True,
        "max_hold_days": int(os.getenv("PB1_CORE_TIME_STOP_DAYS", "20")),
        "partial_sell_rules": [
            # 20% 이상에서만 부분 익절
            {
                "trigger": "percent",
                "profit_pct": _ef("PB1_CORE_TP1_PROFIT_PCT", 20.0),
                "sell_pct": _ef("PB1_CORE_TP1_SELL_PCT", 0.25),
                "meta_flag": "tp1_done",
            },
            {
                "trigger": "percent",
                "profit_pct": _ef("PB1_CORE_TP2_PROFIT_PCT", 30.0),
                "sell_pct": _ef("PB1_CORE_TP2_SELL_PCT", 0.25),
                "meta_flag": "tp2_done",
            },
        ],
        "full_exit_rules": [
            {"trigger": "ma20_break_after_tp1"},
            {"trigger": "ma50_break"},
            {"trigger": "risk_off_bear"},
        ],
    }


# ─────────────────────────────────────────────────────────────────────
# 공개 API 2: SWING 매도 결정 적용
# ─────────────────────────────────────────────────────────────────────

def apply_swing_exit_decision(
    pos: dict[str, Any],
    mark: float,
    policy: dict[str, Any],
    *,
    ret_pct: float,
    current_r: float,
    highest_ret_pct: float,
    days_held: int,
    stop_hit: bool,
    ma20: float | None = None,
    effective_stop: float = 0.0,
    effective_r: float = 0.0,
    risk_ctx: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """SWING_STAGED_EXIT policy 적용 → exit 결정 반환.

    R 기반 신호와 % 기반 신호를 혼합하여 판단한다.
    강한 추세에서는 R 도달만으로 전량 매도하지 않는다.

    수익률 계산:
        return_pct = (current_price - avg_buy_price) / avg_buy_price * 100
    """
    meta = _parse_meta(pos)

    tp1_done = bool(meta.get("tp1_done", False))
    tp2_done = bool(meta.get("tp2_done", False))
    giveback_protect_done = bool(meta.get("giveback_protect_done", False))
    calendar_days_held = int(pos.get("calendar_days_held") or days_held)
    trading_days_held = int(pos.get("trading_days_held") or days_held)
    holding_bars = int(pos.get("holding_bars") or 0)
    legacy_time_stop_hit = pos.get("legacy_time_stop_hit")
    if legacy_time_stop_hit is None:
        legacy_time_stop_hit = trading_days_held >= int(policy.get("max_hold_days") or 10) and current_r < 1.0

    orderable_qty = int(pos.get("orderable_qty") or pos.get("qty") or 0)
    code_for_log = str(pos.get("code") or pos.get("stock_code") or "UNKNOWN")
    avg = float(pos.get("avg_buy_price") or pos.get("avg") or pos.get("entry_price") or 0.0)

    drawdown_from_peak = max(0.0, highest_ret_pct - ret_pct)
    trend_strong = bool(policy.get("trend_strong", False))

    # effective stop 기반 meta update 공통 필드
    _eff_meta: dict[str, Any] = {}
    if risk_ctx:
        _eff_meta = {
            "effective_stop_price": effective_stop,
            "effective_r_value": float(effective_r) if effective_r else None,
            "raw_stop_price": float(risk_ctx.get("raw_stop_price") or 0),
            "effective_exit_policy_version": "2026-router-v1",
        }

    partial_sell_rules = policy.get("partial_sell_rules") or []

    # ── 1. Hard stop (R/effective stop 기반; 정책과 무관하게 항상 작동) ──
    if stop_hit or (effective_stop > 0 and mark <= effective_stop):
        qty = _calculate_exit_qty(orderable_qty, None)
        logger.info(
            "[EXIT][ROUTER][DECISION] code=%s exit_ok=1 reason=STOP_HIT_EFFECTIVE "
            "mark=%.2f stop=%.2f",
            code_for_log, mark, effective_stop,
        )
        return {
            "exit_ok": True,
            "reason": "STOP_HIT_EFFECTIVE",
            "qty": qty,
            "sell_pct": None,
            "update_meta": _eff_meta,
        }

    # ── 2. Giveback protection (peak 대비 반납) ───────────────────────
    gb_rule = next(
        (r for r in partial_sell_rules if r.get("trigger") == "giveback"),
        None,
    )
    if (
        policy.get("profit_protect_enabled")
        and gb_rule
        and not giveback_protect_done
    ):
        activate_pct = float(gb_rule.get("activate_pct", 8.0))
        giveback_pct = float(gb_rule.get("giveback_pct", 3.0))
        floor_pct = float(gb_rule.get("floor_pct", 5.0))

        giveback_hit = (
            highest_ret_pct >= activate_pct
            and drawdown_from_peak >= giveback_pct
            and ret_pct >= floor_pct
        )

        logger.info(
            "[EXIT][PERCENT][CTX] code=%s check=GIVEBACK highest_ret=%.2f "
            "activate=%.1f drawdown=%.2f giveback_threshold=%.1f floor=%.1f hit=%s",
            code_for_log, highest_ret_pct, activate_pct,
            drawdown_from_peak, giveback_pct, floor_pct, int(giveback_hit),
        )

        if giveback_hit:
            # [2026-04-30] PB1_PROFIT_PROTECT_FULL_EXIT=1 이면 전량매도
            full_exit = _eb("PB1_PROFIT_PROTECT_FULL_EXIT", default=False) or _eb("PB1_GIVEBACK_EXIT_FULL_SELL", default=False)
            if full_exit:
                sell_pct_used = 1.0
                qty = _calculate_exit_qty(orderable_qty, None)
                logger.info(
                    "[EXIT][FULL_SELL_POLICY] code=%s reason=SWING_PROFIT_PROTECT_GIVEBACK "
                    "full_exit=1 orderable_qty=%s sell_qty=%s sell_pct=1.0",
                    code_for_log, orderable_qty, qty,
                )
            else:
                sell_pct_used = float(os.getenv("PB1_SWING_GIVEBACK_SELL_PCT", str(gb_rule.get("sell_pct", 0.33))))
                qty = _calculate_exit_qty(orderable_qty, sell_pct_used)
            logger.info(
                "[EXIT][ROUTER][DECISION] code=%s exit_ok=1 "
                "reason=SWING_PROFIT_PROTECT_GIVEBACK qty=%s full_exit=%s",
                code_for_log, qty, int(full_exit),
            )
            return {
                "exit_ok": True,
                "reason": "SWING_PROFIT_PROTECT_GIVEBACK",
                "qty": qty,
                "sell_pct": sell_pct_used,
                "full_exit": full_exit,
                "update_meta": {
                    **_eff_meta,
                    "giveback_protect_done": True,
                    "giveback_protect_ret_pct": ret_pct,
                    "giveback_protect_peak_pct": highest_ret_pct,
                },
            }

    # ── 3. 수익률 기준 TP1 (ret >= swing_tp1_profit_pct) ─────────────
    pct_tp1_rule = next(
        (
            r for r in partial_sell_rules
            if r.get("trigger") == "percent" and r.get("meta_flag") == "tp1_done"
        ),
        None,
    )
    if (
        policy.get("percent_take_profit_enabled")
        and pct_tp1_rule
        and not tp1_done
        and ret_pct >= pct_tp1_rule["profit_pct"]
    ):
        sell_pct = float(pct_tp1_rule["sell_pct"])
        qty = _calculate_exit_qty(orderable_qty, sell_pct)
        logger.info(
            "[EXIT][ROUTER][DECISION] code=%s exit_ok=1 reason=SWING_PCT_TP1 "
            "ret=%.2f threshold=%.1f qty=%s",
            code_for_log, ret_pct, pct_tp1_rule["profit_pct"], qty,
        )
        return {
            "exit_ok": True,
            "reason": "SWING_PCT_TP1",
            "qty": qty,
            "sell_pct": sell_pct,
            "update_meta": {
                **_eff_meta,
                "tp1_done": True,
                "tp1_price": mark,
                "current_stop_price": max(effective_stop, avg),
            },
        }

    # ── 4. R 기준 TP1 (trend 강하면 runner 유지) ──────────────────────
    r_tp1_rule = next(
        (
            r for r in partial_sell_rules
            if r.get("trigger") == "r_hybrid" and r.get("meta_flag") == "tp1_done"
        ),
        None,
    )
    if (
        policy.get("r_take_profit_enabled")
        and r_tp1_rule
        and not tp1_done
        and current_r >= r_tp1_rule["r"]
    ):
        block_if_strong = bool(r_tp1_rule.get("block_if_trend_strong", True))
        if block_if_strong and trend_strong:
            logger.info(
                "[EXIT][TREND][CTX] code=%s check=R_TP1 r=%.2f r_threshold=%.1f "
                "trend_strong=True → runner 유지 (R 신호 억제)",
                code_for_log, current_r, r_tp1_rule["r"],
            )
            # trend 강함: partial sell 억제, runner 유지
        else:
            sell_pct = float(r_tp1_rule["sell_pct"])
            qty = _calculate_exit_qty(orderable_qty, sell_pct)
            logger.info(
                "[EXIT][ROUTER][DECISION] code=%s exit_ok=1 reason=EXIT_SWING_TP1 "
                "r=%.2f trend_strong=%s qty=%s",
                code_for_log, current_r, int(trend_strong), qty,
            )
            return {
                "exit_ok": True,
                "reason": "EXIT_SWING_TP1",
                "qty": qty,
                "sell_pct": sell_pct,
                "update_meta": {
                    **_eff_meta,
                    "tp1_done": True,
                    "tp1_price": mark,
                    "current_stop_price": max(effective_stop, avg),
                    "runner_qty": orderable_qty - qty,
                },
            }

    # ── 5. 수익률 기준 TP2 (ret >= swing_tp2_profit_pct) ─────────────
    pct_tp2_rule = next(
        (
            r for r in partial_sell_rules
            if r.get("trigger") == "percent" and r.get("meta_flag") == "tp2_done"
        ),
        None,
    )
    if (
        policy.get("percent_take_profit_enabled")
        and pct_tp2_rule
        and not tp2_done
        and ret_pct >= pct_tp2_rule["profit_pct"]
    ):
        sell_pct = float(pct_tp2_rule["sell_pct"])
        qty = _calculate_exit_qty(orderable_qty, sell_pct)
        logger.info(
            "[EXIT][ROUTER][DECISION] code=%s exit_ok=1 reason=SWING_PCT_TP2 "
            "ret=%.2f threshold=%.1f qty=%s",
            code_for_log, ret_pct, pct_tp2_rule["profit_pct"], qty,
        )
        return {
            "exit_ok": True,
            "reason": "SWING_PCT_TP2",
            "qty": qty,
            "sell_pct": sell_pct,
            "update_meta": {
                **_eff_meta,
                "tp2_done": True,
                "tp2_price": mark,
            },
        }

    # ── 6. R 기준 TP2 (trend 강하면 runner 유지) ──────────────────────
    r_tp2_rule = next(
        (
            r for r in partial_sell_rules
            if r.get("trigger") == "r_hybrid" and r.get("meta_flag") == "tp2_done"
        ),
        None,
    )
    if (
        policy.get("r_take_profit_enabled")
        and r_tp2_rule
        and not tp2_done
        and current_r >= r_tp2_rule["r"]
    ):
        block_if_strong = bool(r_tp2_rule.get("block_if_trend_strong", True))
        if block_if_strong and trend_strong:
            logger.info(
                "[EXIT][TREND][CTX] code=%s check=R_TP2 r=%.2f trend_strong=True → runner 유지",
                code_for_log, current_r,
            )
        else:
            sell_pct = float(r_tp2_rule["sell_pct"])
            qty = _calculate_exit_qty(orderable_qty, sell_pct)
            logger.info(
                "[EXIT][ROUTER][DECISION] code=%s exit_ok=1 reason=EXIT_SWING_TP2 "
                "r=%.2f qty=%s",
                code_for_log, current_r, qty,
            )
            return {
                "exit_ok": True,
                "reason": "EXIT_SWING_TP2",
                "qty": qty,
                "sell_pct": sell_pct,
                "update_meta": {
                    **_eff_meta,
                    "tp2_done": True,
                    "tp2_price": mark,
                    "runner_qty": orderable_qty - qty,
                    "trail_policy": "MA20_RUNNER",
                },
            }

    # ── 7. MA20 runner (TP1 이후) ──────────────────────────────────
    if policy.get("trend_follow_enabled") and tp1_done and ma20 is not None and mark < ma20:
        qty = _calculate_exit_qty(orderable_qty, None)
        logger.info(
            "[EXIT][TREND][CTX] code=%s ma20_break mark=%.2f ma20=%.2f "
            "→ EXIT_SWING_RUNNER_MA20_BREAK",
            code_for_log, mark, float(ma20),
        )
        logger.info(
            "[EXIT][ROUTER][DECISION] code=%s exit_ok=1 reason=EXIT_SWING_RUNNER_MA20_BREAK",
            code_for_log,
        )
        return {
            "exit_ok": True,
            "reason": "EXIT_SWING_RUNNER_MA20_BREAK",
            "qty": qty,
            "sell_pct": None,
        }

    # ── 8. Time stop ──────────────────────────────────────────────────
    if policy.get("time_stop_enabled"):
        max_hold = int(policy.get("max_hold_days") or 10)
        router_time_stop_hit = bool(trading_days_held >= max_hold and current_r < 1.0)
        logger.info(
            "[EXIT][TIME_STOP][BASIS] basis=trading_days calendar_days=%s trading_days=%s max_hold=%s hit=%s",
            calendar_days_held,
            trading_days_held,
            max_hold,
            int(router_time_stop_hit),
        )
        if router_time_stop_hit and legacy_time_stop_hit is False:
            logger.info("[EXIT][ROUTER][CONSISTENCY] legacy_time_stop_hit=0 router_time_stop_hit=0")
            return {"exit_ok": False, "reason": "TIME_STOP_RULE_ROUTER_MISMATCH", "qty": 0, "sell_pct": None}
        if router_time_stop_hit and trading_days_held <= 0:
            logger.info("[EXIT][ROUTER][CONSISTENCY] legacy_time_stop_hit=%s router_time_stop_hit=0", int(bool(legacy_time_stop_hit)))
            return {"exit_ok": False, "reason": "TIME_STOP_NO_TRADING_DAYS", "qty": 0, "sell_pct": None}
        if router_time_stop_hit and trading_days_held < max_hold:
            logger.info("[EXIT][ROUTER][CONSISTENCY] legacy_time_stop_hit=%s router_time_stop_hit=0", int(bool(legacy_time_stop_hit)))
            return {"exit_ok": False, "reason": "TIME_STOP_TRADING_DAYS_NOT_REACHED", "qty": 0, "sell_pct": None}
        logger.info(
            "[EXIT][ROUTER][CONSISTENCY] legacy_time_stop_hit=%s router_time_stop_hit=%s",
            int(bool(legacy_time_stop_hit)) if legacy_time_stop_hit is not None else -1,
            int(router_time_stop_hit),
        )
        if router_time_stop_hit:
            qty = _calculate_exit_qty(orderable_qty, None)
            logger.info(
                "[EXIT][ROUTER][DECISION] code=%s exit_ok=1 reason=EXIT_SWING_TIME_STOP "
                "calendar_days=%s trading_days=%s max=%s r=%.2f",
                code_for_log, calendar_days_held, trading_days_held, max_hold, current_r,
            )
            return {
                "exit_ok": True,
                "reason": "EXIT_SWING_TIME_STOP",
                "qty": qty,
                "sell_pct": None,
            }

    logger.info(
        "[EXIT][ROUTER][DECISION] code=%s exit_ok=0 "
        "reason=SWING_HOLD_PROFIT_NOT_YET_PROTECTED "
        "ret=%.2f highest=%.2f r=%.2f tp1=%s tp2=%s",
        code_for_log, ret_pct, highest_ret_pct, current_r, tp1_done, tp2_done,
    )
    return {"exit_ok": False, "reason": "SWING_HOLD_PROFIT_NOT_YET_PROTECTED"}
