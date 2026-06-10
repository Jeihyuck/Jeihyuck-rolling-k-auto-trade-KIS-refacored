from __future__ import annotations

import contextlib
import json
import logging
import os
import time
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo
from typing import Any, Dict, Iterable, List

import numpy as np
import pandas as pd
import sqlalchemy as sa
from sqlalchemy import inspect

from trader.runtime_paths import close_entry_orders_path, runtime_path
from trader.path_contract import resolve_repo_root
from trader.logging_utils import append_jsonl
from trader.config import (
    CAP_CAP,
    LEDGER_BASE_DIR,
    LEDGER_LOOKBACK_DAYS,
    PB1_REQUIRE_VOLUME,
    PB1_MIN_CANDLES,
    PB1_MAX_POSITIONS,
    PB1_MIN_SCORE_BASE,
    PB1_MIN_SCORE_FLOOR,
    PB1_MIN_SCORE_STEP,
    PB1_MIN_BUYABLE,
    PB1_USE_MINERVINI_AS_RANK_ONLY,
    PB1_MINERVINI_HARD_GATE,
    PB1_EMERGENCY_ORDER_ENABLED,
    PB1_EMERGENCY_DIAG_ONLY,
    PB1_BOOTSTRAP_ENABLE,
    BOOTSTRAP_MINERVINI_RS_MIN_PCTILE,
    BOOTSTRAP_MINERVINI_VCP_MIN_SCORE,
    BOOTSTRAP_RELAX_PASSES,
    BOOTSTRAP_KEEP_TREND_TEMPLATE_ALWAYS,
    BOOTSTRAP_PB1_VOL_MAX,
    BOOTSTRAP_PB1_VOLU_MAX,
    BOOTSTRAP_PB1_PULLBACK_MIN,
    BOOTSTRAP_PB1_PULLBACK_MAX,
    BOOTSTRAP_PB1_REQUIRE_BOTH_CONTRACTIONS,
    BOOTSTRAP_PB1_MIN_SCORE_BASE,
    BOOTSTRAP_PB1_MIN_SCORE_FLOOR,
    BOOTSTRAP_PB1_MIN_SCORE_STEP,
    BOOTSTRAP_SCORE_CUT_KEEP_TOPN,
    BOOTSTRAP_FORCE_MIN_1_SHARE,
    BOOTSTRAP_MIN1_TOPN,
    PB1_MAX_ATR_PCT,
    PB1_MAX_ATR_PCT_RAW,
    PB1_MIN_VALUE20,
    PB1_FAILMODE_SOFT,
    PB1_MIN_CANDIDATES,
    PB1_RELAX_MAX_PASSES,
    PB1_SPREAD_HARD_MAX_PCT,
    PB1_GAP_HARD_MAX_PCT,
    PB1_CAPITAL_MODE,
    PB1_ENTRY_CAPITAL_KRW,
    PB1_CASH_RESERVE_PCT,
    PB1_ENTRY_BUDGET_PCT_PER_TICK,
    PB1_MAX_POS_PCT,
    PB1_TARGET_NEW_POSITIONS,
    PAPER_MAX_CAPITAL_KRW,
    PB1_ENTRY_WINDOW_START,
    PB1_ENTRY_OPEN_END,
    PB1_ENTRY_WINDOW_END,
    PB1_EXIT_WINDOW_START,
    PB1_EXIT_WINDOW_END,
    PB1_TIME_STOP_DAYS,
    PB1_ALLOW_ADD_TO_EXISTING,
    PB1_PREOPEN_ORDER_TYPE,
    PB1_PREOPEN_LIMIT_BUFFER_PCT,
    PB1_VOL_MAX,
    PB1_VOLU_MAX,
    PB1_VOLU_MAX_INTRADAY,
    PB1_PULLBACK_MIN,
    PB1_PULLBACK_MAX,
    PB1_RELAX_MA_FILTER,
    PB1_RELAX_MA20_SLOPE,
    PB1_ENTRY_MODE,
    PB1_REQUIRE_BOTH,
    PB1_REQUIRE_BOTH_CONTRACTIONS,
    ENTRY_COND_MODE,
    PB1_LOG_ENTRY_GATE,
    PB1_LOG_DROP_REASONS_TOPN,
    PB1_OHLCV_DAYS_BASE,
    PB1_MAX_DAILY_FETCH_PER_TICK,
    PB1_MAX_PRICE_FETCH_PER_TICK,
    PB1_EARLY_STOP_ENABLED,
    MIN_ORDER_KRW,
    ALLOW_SINGLE_SHARE_OVERRIDE,
    MIN_REMAINING_CASH_KRW,
    BUY_PRICE_BUFFER_PCT,
    BUDGET_FLEX_PCT,
    MIN_TRAIL_BARS,
    MIN_EXIT_BARS,
    SIZING_ALLOW_MIN_1_SHARE,
    SIZING_MIN_1_SHARE_TOPN,
    PRICE_SLIPPAGE_PCT_BUY,
    PRICE_USE_ASK_IF_AVAILABLE,
    MINERVINI_ADD_ON_R,
    MINERVINI_BREAKOUT_VOL_MULT,
    MINERVINI_HEAVY_VOL_MULT,
    MINERVINI_INITIAL_STOP_PCT,
    MINERVINI_MAX_EXTENSION_PIVOT,
    MINERVINI_MAX_PYRAMID,
    MINERVINI_RS_MIN,
    MINERVINI_TIME_STOP_DAYS,
    ATR_WINDOW,
    ATR_MULT,
    BREAKOUT_VOL_MULT,
    ENTRY_MODE,
    FAILED_BREAKOUT_EXIT_DAYS,
    INITIAL_STOP_MODE,
    MAX_GAP_UP_PCT,
    MAX_INTRADAY_RANGE_PCT,
    MAX_SPREAD_PROXY_BPS,
    MIN_AVG_VALUE_KRW,
    REENTRY_COOLDOWN_DAYS,
    REGIME_INDEX,
    REGIME_MA_FAST,
    REGIME_MA_SLOW,
    REGIME_MAX_RISK,
    REGIME_MID_RISK,
    REGIME_MIN_RISK,
    REGIME_MODE,
    RISK_PER_TRADE_PCT,
    RS_BENCHMARK,
    RS_COMPOSITE_W1,
    RS_COMPOSITE_W2,
    RS_LOOKBACK_DAYS,
    RS_LOOKBACK2_DAYS,
    RS_MIN_PCTILE,
    TAKE_PROFIT_R1,
    TAKE_PROFIT_R2,
    TP1_SELL_PCT,
    TP2_SELL_PCT,
    TRAIL_MODE,
    TRAIL_STEP_AFTER_R,
    UNIVERSE_POOL_SIZE,
    VCP_LOOKBACK,
    VCP_MIN_SCORE,
    FORCE_MIN1_OVERRIDE_POSITION_CAP,
    FORCE_MIN1_OVERRIDE_TOPN,
    resolve_market_window,
    # [2026-04-29] Effective Exit Policy
    PB1_EXISTING_POSITION_EFFECTIVE_EXIT_ENABLED,
    PB1_EFFECTIVE_STOP_CAP_ENABLED,
    PB1_EFFECTIVE_STOP_CAP_KOSPI_PCT,
    PB1_EFFECTIVE_STOP_CAP_KOSDAQ_PCT,
    PB1_PROFIT_PROTECT_ENABLED,
    PB1_PROFIT_PROTECT_PCT,
    PB1_PROFIT_PROTECT_SELL_PCT,
    PB1_ABS_TP1_ENABLED,
    PB1_ABS_TP1_PROFIT_PCT,
    PB1_ABS_TP1_SELL_PCT,
    # [2026-05-18] PB1 KR-only adaptive entry filter
    PB1_KR_ADAPTIVE_ENTRY_FILTER,
    PB1_KR_PULLBACK_VOL_HARD_FAIL,
    PB1_KR_PULLBACK_VOLU_HARD_FAIL,
    PB1_KR_PULLBACK_VOL_PENALTY,
    PB1_KR_PULLBACK_VOLU_PENALTY,
    PB1_KR_MOMENTUM_ALLOW_VOL_EXPANSION,
    PB1_KR_BREAKOUT_ALLOW_VOL_EXPANSION,
    PB1_KR_SCORE_MODE,
    PB1_KR_ADAPTIVE_RANK_TOPN,
    PB1_KR_ADAPTIVE_SCORE_MIN_FLOOR,
    PB1_KR_ENABLE_RESCUE_CANDIDATES,
    PB1_KR_RESCUE_TOPN,
    PB1_KR_RESCUE_SOURCE,
    PB1_KR_MARKET_STRESS_GUARD,
    PB1_KR_STRESS_VOL_FAIL_RATIO,
    PB1_KR_STRESS_MA20_FAIL_RATIO,
    PB1_KR_STRESS_MAX_NEW_POSITIONS,
    PB1_KR_STRESS_TICK_BUDGET_PCT,
    PB1_KR_STRESS_REQUIRE_STRONG_RS,
    PB1_KR_STRESS_MIN_RS_PCTILE,
    PB1_KR_LOG_FILTER_MATRIX,
    PB1_KR_LOG_RESCUE_DECISION,
)
from trader.constants import FLOW_OPTIONAL_COLS, REQUIRED_FINAL30_SCORED_COLS
from trader.db.engine import dispose_engine_safely
from trader.db.repos import (
    DerivedMinerviniRepo,
    FillsRepo,
    LedgerEventsRepo,
    OrdersRepo,
    PositionsRepo,
    ScoredWatchlistInvalidError,
    ScoredWatchlistNotFoundError,
    UniverseRepo,
    WatchlistRepo,
    load_final30_scored_db_only,
    load_price_daily_bulk,
)
from trader.data.ohlcv_provider import ChainOHLCVProvider, KISOHLCVProvider, KRXOHLCVProvider
from trader.kis_wrapper import KisAPI, KISBlockedError, extract_order_no, is_order_accepted
from trader.ledger.store import LedgerStore
from trader.universe.validation import validate_tradeable
from trader.factors.liquidity_risk import gap_filter, liquidity_filter, range_filter, spread_proxy_filter
from trader.factors.regime import get_regime, risk_multiplier
from trader.factors.rs_rank import rank_rs
from trader.positioning.minervini_risk import calc_initial_stop, calc_position_size, update_exits
from trader.minervini.report import run_minervini_report
from trader.setups.vcp_pro import PriceTightRules, VolContractRules, find_pivot, is_vcp_ready, score_vcp
from trader.strategies.pb1_minervini_v2 import (
    MinerviniConfig,
    compute_features,
    compute_pivot,
    detect_vcp,
    entry_trigger,
    evaluate_filters as _strategy_evaluate_filters,
    initial_stop,
    risk_position_size,
    score_setup,
    update_trailing_stop,
)
from trader.time_utils import now_kst, week_monday, prev_business_day
from trader.position_age import calc_position_age, normalize_ohlcv_dates, to_kst_date
from trader.core_utils import _round_to_tick
from trader.decision_schema import build_entry_evaluation, build_exit_evaluation
from trader.reasons import ReasonCode
from trader.eventlog import emit_event
from trader.utils.env import env_bool, parse_bool_any
from trader.utils.json_sanitize import to_jsonable
from trader.window_router import WindowDecision
from trader.diagnostics.spool import spool_event
from trader.watchlist_builder import load_today_watchlist_with_fallback
from trader.trade_plan import build_entry_exit_plan, classify_close_action_from_plan, parse_plan_bool
from rolling_k_auto_trade_api.best_k_meta_strategy import run_rebalance
from trader.final_list_store import get_as_of_date
from trader.final30_quality import validate_trade_ready
from trader.minervini_filter import compute_minervini_signals, select_buyable_with_relax, minervini_filter
from trader.entry_signals import breakout_signal, pullback_signal, momentum_signal
from trader.minervini_store import write_minervini_signals
from trader.strategies.pb1_pullback_close import (
    compute_features as compute_pb1_features,
    evaluate_setup as evaluate_pb1_setup,
    classify_pb1_near_miss,
)

logger = logging.getLogger(__name__)


class PB1StageTimeout(RuntimeError):
    pass


def _safe_flow_optional_missing(columns: list[str]) -> list[str]:
    try:
        flow_optional_cols = globals().get("FLOW_OPTIONAL_COLS", [])
        return [c for c in flow_optional_cols if c not in set(columns or [])]
    except Exception as e:
        logger.warning(
            "[FINAL30][FLOW_CHECK_GUARD] optional flow check failed err=%s",
            e,
        )
        return []

ALTERNATIVE_REQUIRED_SCORED_COLS = [("close", "last_close")]

# Minervini feature calc requires MA200 slope + VCP; force long window.
MINERVINI_OHLCV_DAYS_MIN = int(os.getenv("MINERVINI_OHLCV_DAYS", "520"))
MA200_SLOPE_LOOKBACK = int(os.getenv("MA200_SLOPE_LOOKBACK", "20"))
KST = ZoneInfo("Asia/Seoul")
BUYABLE_GATE_TODAY_BUY_EVENT_TYPES = {"BUY_FILL"}
BUYABLE_GATE_COOLDOWN_RULE = "filled_trade_required"
LEDGER_BUY_EXECUTION_EVENT_TYPES = {"BUY_FILLED", "ORDER_FILLED_BUY", "BUY_EXECUTED", "EXECUTED_BUY"}
LEDGER_BUY_SUBMISSION_EVENT_TYPES = {"ORDER_INTENT", "ORDER_SUBMITTED", "ORDER_ACCEPTED_BUY", "ORDER_ACCEPTED"}
LEDGER_BUY_SUBMISSION_EVENT_TYPES = {"ORDER_INTENT", "ORDER_SUBMIT_ATTEMPT", "ORDER_SUBMIT_ACCEPTED", "ORDER_SUBMIT_ACCEPTED_CONFIRMED"}
LEDGER_SKIP_EVENT_TYPES = {"ORDER_SKIP"}
LEDGER_RECONCILE_EVENT_TYPES = {"RECONCILE"}

EXIT_REASON_PRIORITY = [
    "EXIT_RISK_OFF",
    "EXIT_STOP_LOSS",
    "EXIT_TRAILING_STOP",
    "EXIT_MA50_BREAK",
    "EXIT_MA20_BREAK",
    "EXIT_TIME_STOP",
]


@dataclass
class HoldingContext:
    code: str
    name: str
    holding_qty: int
    orderable_qty: int
    avg_price: float
    last_price: float
    market_value: float
    unrealized_pnl: float
    unrealized_pct: float
    source: str = "kis_balance"
    market: str | None = None
    mode: int = 1
    sid: int = 1
    entry_date: str | None = None
    days_held: int = 0
    calendar_days_held: int = 0
    trading_days_held: int = 0
    holding_bars: int = 0
    last_fill_at: str | None = None
    position_meta: dict[str, Any] = field(default_factory=dict)

    def to_position_dict(self) -> dict[str, Any]:
        payload = dict(self.position_meta)
        total_cost = self.position_meta.get("total_cost")
        if total_cost in (None, "", 0, 0.0) and self.avg_price and self.holding_qty:
            total_cost = float(self.avg_price) * int(self.holding_qty)
        payload.update(
            {
                "code": self.code,
                "name": self.name,
                "qty": self.holding_qty,
                "kis_qty": self.holding_qty,
                "orderable_qty": self.orderable_qty,
                "avg_buy_price": self.avg_price or None,
                "last_price": self.last_price or None,
                "market_value": self.market_value,
                "unrealized_pnl": self.unrealized_pnl,
                "unrealized_pct": self.unrealized_pct,
                "total_cost": total_cost or 0.0,
                "market": self.market,
                "mode": self.mode,
                "sid": self.sid,
                "entry_date": self.entry_date,
                "holding_days": self.days_held,
                "calendar_days_held": self.calendar_days_held,
                "trading_days_held": self.trading_days_held or self.days_held,
                "holding_bars": self.holding_bars,
                "last_fill_at": self.last_fill_at,
                "holding_source": self.source,
            }
        )
        return payload


@dataclass
class ExitEvaluation:
    code: str
    holding_qty: int
    avg_price: float
    last_price: float
    pnl_pct: float
    days_held: int
    stop_hit: bool = False
    trail_hit: bool = False
    ma20_break: bool = False
    ma50_break: bool = False
    time_stop_hit: bool = False
    risk_off_hit: bool = False
    exit_ok: bool = False
    family: str = "SKIP"
    primary_reason: str = "NO_EXIT_SIGNAL"
    secondary_reasons: list[str] = field(default_factory=list)


@dataclass
class UnifiedGateDecision:
    ok: bool
    reason_codes: list[str]
    blocking_stage: str
    context: dict[str, Any]


def _minervini_ohlcv_days() -> int:
    base = 200 + MA200_SLOPE_LOOKBACK + 60
    need_days = max(MINERVINI_OHLCV_DAYS_MIN, base)
    if os.getenv("MODE") == "trade" and need_days >= 200:
        raise RuntimeError("TRADE_TICK_FORBIDS_LONG_MINERVINI_FETCH")
    return need_days

_OUTPUT2_LIST_NORMALIZED_LOGGED = False
_OUTPUT2_UNEXPECTED_TYPE_LOGGED = False

UNREALIZED_KEYS = (
    "evlu_pfls_amt",
    "evlu_pfls_smtl_amt",
)
RETURN_PCT_KEYS = (
    "asst_icdc_erng_rt",
    "evlu_pfls_rt",
)
COST_KEYS = (
    "pchs_amt_smtl_amt",
    "pchs_amt",
)
EVAL_KEYS = (
    "scts_evlu_amt",
    "evlu_amt_smtl_amt",
    "tot_evlu_amt",
    "nass_amt",
)
CASH_KEYS = (
    "dnca_tot_amt",
    "nxdy_excc_amt",
    "ord_psbl_cash",
    "evlu_amt_sbst_amt",
)

_ENTRY_BLOCK_REASON_MAP = {
    "SIZING_CAP_BELOW_ONE_SHARE": "SIZING_CAP_BELOW_ONE_SHARE",
    "SIZING_MIN_ORDER_NOTIONAL_FAIL": "SIZING_MIN_ORDER_NOTIONAL_FAIL",
    "SIZING_QTY_ZERO": "SIZING_QTY_ZERO",
    "cap_below_min_order": "MIN_ORDER_KRW",
    "min_order_krw": "MIN_ORDER_KRW",
    "cap_below_one_share": "MIN_ORDER_KRW",
    "planned_qty_zero_or_min_order": "MIN_ORDER_KRW",
    "unaffordable_min1share": "MIN_ORDER_KRW",
    "order_price_missing": "PRICE_MISSING",
    "entry_cutoff": "CUTOFF",
    "entry_disabled": "ENTRY_DISABLED",
    "available_cash_zero": "NO_CASH",
    "insufficient_cash": "NO_CASH",
    "entry_capital_zero": "NO_CASH",
    "tick_budget_zero": "NO_CASH",
    "entry_cap_exceeded": "ENTRY_CAP_LIMIT",
    "tick_budget_below_min_order": "MIN_ORDER_KRW",
    "max_positions": "MAX_POSITIONS",
    "target_new_positions_limit": "MAX_POSITIONS",
    "target_new_positions_zero": "TARGET_NEW_POSITIONS_ZERO",
    "BUYABLE_EXISTING_HOLDING": "BUYABLE_EXISTING_HOLDING",
    "BUYABLE_OPEN_ORDER": "BUYABLE_OPEN_ORDER",
    "BUYABLE_TODAY_BUY_EXISTS": "BUYABLE_TODAY_BUY_EXISTS",
    "BUYABLE_TODAY_SELL_REBUY_BLOCKED": "BUYABLE_TODAY_SELL_REBUY_BLOCKED",
    "BUYABLE_COOLDOWN": "BUYABLE_COOLDOWN",
    "BUYABLE_DUPLICATE": "BUYABLE_DUPLICATE",
    "BUYABLE_WINDOW_BLOCK": "BUYABLE_WINDOW_BLOCK",
    "open_order": "RATE_LIMIT",
    "today_buy_exists": "RATE_LIMIT",
    "duplicate_order": "DUPLICATE",
    "rate_limit": "RATE_LIMIT",
    "holding_position": "EXISTING_POSITION",
    "order_value_zero": "ORDER_VALUE_ZERO",
    "qty_zero": "QTY_ZERO",
    "universe_empty": "UNIVERSE_EMPTY",
}

_ORDER_SKIP_REASON_MAP = {
    "SIZING_CAP_BELOW_ONE_SHARE": "ORDER_SKIP_SIZING_CAP_BELOW_ONE_SHARE",
    "SIZING_MIN_ORDER_NOTIONAL_FAIL": "ORDER_SKIP_SIZING_MIN_ORDER_NOTIONAL_FAIL",
    "SIZING_QTY_ZERO": "ORDER_SKIP_SIZING_QTY_ZERO",
    "cap_below_min_order": "ORDER_SKIP_MIN_ORDER",
    "min_order_krw": "ORDER_SKIP_MIN_ORDER",
    "cap_below_one_share": "ORDER_SKIP_MIN_ORDER",
    "planned_qty_zero_or_min_order": "ORDER_SKIP_MIN_ORDER",
    "unaffordable_min1share": "ORDER_SKIP_MIN_ORDER",
    "order_price_missing": "ORDER_SKIP_PRICE_MISSING",
    "available_cash_zero": "ORDER_SKIP_NO_CASH",
    "insufficient_cash": "ORDER_SKIP_NO_CASH",
    "entry_capital_zero": "ORDER_SKIP_NO_CASH",
    "tick_budget_zero": "ORDER_SKIP_NO_CASH",
    "entry_cap_exceeded": "ORDER_SKIP_NO_CASH",
    "BUYABLE_EXISTING_HOLDING": "ORDER_SKIP_BUYABLE_EXISTING_HOLDING",
    "BUYABLE_OPEN_ORDER": "ORDER_SKIP_BUYABLE_OPEN_ORDER",
    "BUYABLE_TODAY_BUY_EXISTS": "ORDER_SKIP_BUYABLE_TODAY_BUY_EXISTS",
    "BUYABLE_TODAY_SELL_REBUY_BLOCKED": "ORDER_SKIP_BUYABLE_TODAY_SELL_REBUY_BLOCKED",
    "BUYABLE_COOLDOWN": "ORDER_SKIP_BUYABLE_COOLDOWN",
    "BUYABLE_DUPLICATE": "ORDER_SKIP_BUYABLE_DUPLICATE",
    "BUYABLE_WINDOW_BLOCK": "ORDER_SKIP_BUYABLE_WINDOW_BLOCK",
    "open_order": "ORDER_SKIP_RATE_LIMIT",
    "today_buy_exists": "ORDER_SKIP_RATE_LIMIT",
    "duplicate_order": "ORDER_SKIP_DUPLICATE",
    "rate_limit": "ORDER_SKIP_RATE_LIMIT",
    "entry_cutoff": "ORDER_SKIP_CUTOFF",
    "entry_disabled": "ORDER_SKIP_DISABLED",
}


def _as_first_dict(v: Any) -> Dict[str, Any]:
    """
    KIS 응답에서 output2가 list([dict])로 오는 케이스가 많음.
    dict / list / None 등 어떤 형태든 dict로 정규화.
    """
    global _OUTPUT2_LIST_NORMALIZED_LOGGED
    global _OUTPUT2_UNEXPECTED_TYPE_LOGGED

    if v is None:
        return {}
    if isinstance(v, dict):
        return v
    if isinstance(v, list):
        if not _OUTPUT2_LIST_NORMALIZED_LOGGED:
            logger.info("[BALANCE] output2 list->dict normalized")
            _OUTPUT2_LIST_NORMALIZED_LOGGED = True
        for item in v:
            if isinstance(item, dict):
                return item
        return {}
    if not _OUTPUT2_UNEXPECTED_TYPE_LOGGED:
        logger.warning("[BALANCE] output2 unexpected type=%s using rows fallback", type(v).__name__)
        _OUTPUT2_UNEXPECTED_TYPE_LOGGED = True
    return {}


def _extract_output2_keys(summary_raw: Any) -> list[str]:
    if isinstance(summary_raw, list) and summary_raw:
        row = summary_raw[0]
        if isinstance(row, dict):
            return list(row.keys())
        return [f"type:{type(row).__name__}"]
    if isinstance(summary_raw, dict):
        return list(summary_raw.keys())
    if summary_raw is None:
        return []
    return [f"type:{type(summary_raw).__name__}"]


def _normalize_entry_block_reasons(reasons: Iterable[str] | None) -> Counter[str]:
    counter: Counter[str] = Counter()
    for reason in reasons or []:
        mapped = _ENTRY_BLOCK_REASON_MAP.get(reason, reason.upper())
        counter[mapped] += 1
    return counter


def _normalize_entry_block_counts(reason_counts: Counter[str]) -> Counter[str]:
    counter: Counter[str] = Counter()
    for reason, count in reason_counts.items():
        mapped = _ENTRY_BLOCK_REASON_MAP.get(reason, reason.upper())
        counter[mapped] += count
    return counter


def _format_reason_counts(counter: Counter[str]) -> str:
    if not counter:
        return "none"
    parts = [f"{key}:{count}" for key, count in counter.most_common()]
    return ",".join(parts)


NO_TRADE_REASON_PRIORITY = (
    "BUYABLE_TODAY_BUY_EXISTS",
    "BUYABLE_TODAY_SELL_REBUY_BLOCKED",
    "BUYABLE_COOLDOWN",
    "ATR_PCT_TOO_HIGH",
    "atr_pct_too_high",
    "SIZING_CAP_BELOW_ONE_SHARE",
    "MIN_ORDER_KRW",
)


def _summarize_blocked_reasons(counter: Counter[str] | dict[str, int] | None) -> Counter[str]:
    normalized_input = Counter(counter or {})
    return _normalize_entry_block_counts(normalized_input)


def _primary_no_trade_reason(
    counter: Counter[str] | dict[str, int] | None,
    *,
    ok_count: int,
    order_candidates: int,
) -> str:
    if order_candidates > 0:
        return "ORDERS_PRESENT"
    normalized = _summarize_blocked_reasons(counter)
    for reason in NO_TRADE_REASON_PRIORITY:
        if normalized.get(reason, 0) > 0:
            return reason
    if normalized:
        return sorted(normalized.items(), key=lambda item: (-item[1], item[0]))[0][0]
    if ok_count > 0:
        return "NO_ORDERABLE_CANDIDATES"
    return "NO_CANDIDATES_AFTER_RELAX"
    
def _classify_no_candidate_result() -> tuple[str, str]:
    return "OK_NO_TRADE", "NO_CANDIDATES_AFTER_RELAX"


def _log_balance_snapshot_shape(snapshot: Any, *, label: str) -> None:
    if not isinstance(snapshot, dict):
        logger.info("[BALANCE][SHAPE] label=%s type=%s", label, type(snapshot).__name__)
        return
    keys = list(snapshot.keys())
    output2 = snapshot.get("output2")
    output2_type = type(output2).__name__
    output2_len = len(output2) if isinstance(output2, list) else None
    output2_first_type = None
    output2_first_keys = None
    if isinstance(output2, list) and output2:
        output2_first_type = type(output2[0]).__name__
        if isinstance(output2[0], dict):
            output2_first_keys = list(output2[0].keys())
    elif isinstance(output2, dict):
        output2_first_type = "dict"
        output2_first_keys = list(output2.keys())
    logger.info(
        "[BALANCE][SHAPE] label=%s type=dict keys=%s output2_type=%s output2_len=%s output2_first_type=%s output2_first_keys=%s",
        label,
        keys,
        output2_type,
        output2_len,
        output2_first_type,
        output2_first_keys,
    )


def _is_sanitized_balance_snapshot(snapshot: dict) -> bool:
    output2 = snapshot.get("output2")
    if isinstance(output2, list):
        if not output2:
            return False
        first = output2[0]
    elif isinstance(output2, dict):
        first = output2
    else:
        return False
    if not isinstance(first, dict):
        return False
    if len(first.keys()) == 0:
        return True
    values = [value for value in first.values() if value is not None]
    if not values:
        return False
    return all(isinstance(value, str) and value == "****" for value in values)


def _extract_dnca_tot_amt(balance_resp: dict) -> int | None:
    if not isinstance(balance_resp, dict):
        return None
    out2 = balance_resp.get("output2")
    if isinstance(out2, list) and out2:
        row = out2[0]
        if isinstance(row, dict) and row:
            value = row.get("dnca_tot_amt")
            if value is not None and str(value).strip() != "":
                return int(float(str(value).replace(",", "")))
        if isinstance(row, dict) and not row:
            return None
    if isinstance(out2, dict) and out2:
        value = out2.get("dnca_tot_amt")
        if value is not None and str(value).strip() != "":
            return int(float(str(value).replace(",", "")))
    return None


@dataclass
class CandidateFeature:
    code: str
    market: str
    features: Dict[str, float]
    setup_ok: bool
    reasons: List[str]
    mode: int
    mode_reasons: List[str]
    client_order_key: str | None = None
    planned_qty: int = 0
    planned_value: float = 0.0
    score: float | None = None
    sizing_reason: str | None = None  # 명확한 reason 코드 (BUDGET_INSUFFICIENT_FOR_1_SHARE, MIN_ORDER_NOTIONAL_FAIL 등)
    sizing_details: Dict[str, Any] | None = None  # 수치 정보: buy_budget, price, qty, shortfall 등


def _normalize_sizing_failure_reason(raw_reason: str | None) -> str:
    if raw_reason in {
        "ORDER_PX_ABOVE_TICK_BUDGET",
        "ORDER_PX_ABOVE_POSITION_CAP",
        "ORDER_PX_ABOVE_USABLE_CASH",
        "INSUFFICIENT_CASH_FOR_ONE_SHARE",
        "MIN_REMAINING_CASH_VIOLATION",
        "INSUFFICIENT_BUDGET_AND_OVERRIDE_DISABLED",
        "QUANTITY_ZERO_AFTER_BUDGET_CHECK",
    }:
        return raw_reason
    if raw_reason == "MIN_ORDER_KRW_NOT_MET":
        return raw_reason
    if raw_reason in {"FORCE_MIN1_APPLIED", "FORCE_MIN1_NOT_ELIGIBLE"}:
        return raw_reason
    if raw_reason == "BUDGET_INSUFFICIENT_FOR_1_SHARE":
        return "ORDER_PX_ABOVE_POSITION_CAP"
    if raw_reason == "MIN_ORDER_NOTIONAL_FAIL":
        return "MIN_ORDER_KRW_NOT_MET"
    if raw_reason == "FORCE_MIN1_TOPN":
        return "FORCE_MIN1_APPLIED"
    if raw_reason == "OK":
        return "SIZING_OK"
    return "FORCE_MIN1_NOT_ELIGIBLE"


def _coerce_timestamp(value: Any) -> pd.Timestamp | None:
    if value in (None, "", 0, 0.0):
        return None
    try:
        ts = pd.Timestamp(value)
    except Exception:
        return None
    if ts.tzinfo is None:
        try:
            return ts.tz_localize(KST)
        except Exception:
            return ts
    try:
        return ts.tz_convert(KST)
    except Exception:
        return ts


def _calendar_days_held(entry_ts: Any, trade_date: date) -> int:
    entry_date = to_kst_date(entry_ts)
    if entry_date is None:
        return 0
    return max(0, (trade_date - entry_date).days)


def _is_kr_stock_code(code: str | None) -> bool:
    """한국장 6자리 숫자 종목 코드 판별"""
    text = str(code or "").strip()
    return len(text) == 6 and text.isdigit()


def _is_kis_balance_authoritative_empty(balance_snapshot: dict | None) -> bool:
    """
    KIS balance가 정상 조회되었고 output1=[]이면 한국장 보유는 0개.
    이 경우 ledger_reconstruct 등 fallback을 금지한다.
    """
    if not isinstance(balance_snapshot, dict):
        return False

    rt_cd = str(balance_snapshot.get("rt_cd") or "0").strip()
    if rt_cd not in {"", "0"}:
        return False

    output1 = balance_snapshot.get("output1")
    output2 = balance_snapshot.get("output2")

    if isinstance(output1, dict):
        output1_rows = [output1] if output1 else []
    elif isinstance(output1, list):
        output1_rows = output1
    else:
        output1_rows = []

    if output1_rows:
        return False

    def _to_int(v):
        try:
            return int(float(str(v or "0").replace(",", "")))
        except Exception:
            return 0

    out2 = output2
    if isinstance(out2, list):
        out2 = out2[0] if out2 and isinstance(out2[0], dict) else {}
    if not isinstance(out2, dict):
        out2 = {}

    scts_evlu_amt = _to_int(out2.get("scts_evlu_amt"))
    pchs_amt_smtl_amt = _to_int(out2.get("pchs_amt_smtl_amt"))
    evlu_amt_smtl_amt = _to_int(out2.get("evlu_amt_smtl_amt"))

    if scts_evlu_amt == 0 and pchs_amt_smtl_amt == 0 and evlu_amt_smtl_amt == 0:
        return True

    return False


def _compute_highest_since_entry(df: pd.DataFrame, entry_ts: Any, entry_price: float) -> tuple[float, int]:
    safe_entry_price = float(entry_price or 0.0)
    entry_trade_date = to_kst_date(entry_ts)
    if safe_entry_price <= 0:
        safe_entry_price = 0.0
    if df is None or df.empty or "high" not in df.columns or entry_trade_date is None:
        return safe_entry_price, 0

    df_norm = normalize_ohlcv_dates(df)
    date_series: pd.Series | None = None
    if "date" in df_norm.columns:
        date_series = pd.Series(df_norm["date"], index=df_norm.index)
    elif isinstance(df_norm.index, pd.Index):
        date_series = pd.Series(df_norm.index, index=df_norm.index)

    if date_series is None:
        return safe_entry_price, 0

    post_entry_mask = date_series.apply(lambda value: value is not None and value > entry_trade_date)
    post_entry_bars = df_norm.loc[post_entry_mask.fillna(False)]
    if post_entry_bars.empty:
        return safe_entry_price, 0

    post_entry_high = pd.to_numeric(post_entry_bars["high"], errors="coerce").max()
    if pd.isna(post_entry_high):
        return safe_entry_price, int(len(post_entry_bars))
    return max(safe_entry_price, float(post_entry_high)), int(len(post_entry_bars))


# ============================================================
# Trade Horizon Classification & Exit Policy Router
# ============================================================

def _classify_trade_horizon(features: dict[str, Any]) -> str:
    """매수 후보의 특성으로 trade_horizon을 결정한다.

    Returns: "DAY_PROTECT" | "SWING_CARRY" | "CORE_CARRY"
    """
    entry_style = str(
        features.get("entry_style_selected") or features.get("entry_reason") or ""
    ).upper()
    score_final = float(features.get("score_final") or features.get("score") or 0)
    atr_pct = float(features.get("atr_pct") or 0)
    breakout = bool(features.get("breakout_signal") or features.get("pivot_breakout"))
    vcp_score = float(features.get("vcp_score") or 0)
    trend_ok = bool(features.get("trend_template_ok") or features.get("minervini_ok"))

    # 1. 당일 보호형
    if entry_style in {"ENTRY_BREAKOUT", "ENTRY_MOMENTUM", "ENTRY_OPEN_PUSH"}:
        return "DAY_PROTECT"
    if breakout and atr_pct >= 5.0:
        return "DAY_PROTECT"

    # 2. 핵심 중기형 (스윙보다 먼저 체크)
    if score_final >= 85 and trend_ok and atr_pct <= 4.0:
        return "CORE_CARRY"

    # 3. 스윙형
    if entry_style in {"ENTRY_PULLBACK", "ENTRY_VCP", "ENTRY_MINERVINI"}:
        return "SWING_CARRY"
    if trend_ok and vcp_score >= 45:
        return "SWING_CARRY"

    return "SWING_CARRY"


def _horizon_to_exit_family(horizon: str) -> str:
    return {
        "DAY_PROTECT": "INTRADAY_PROFIT_PROTECT",
        "SWING_CARRY": "SWING_STAGED_EXIT",
        "CORE_CARRY": "CORE_TREND_FOLLOW",
    }.get(horizon, "SWING_STAGED_EXIT")


def _resolve_position_horizon(pos: dict[str, Any], now_kst_date: Any | None = None) -> str:
    """포지션 dict에서 trade_horizon을 결정한다.

    우선순위:
    1. position_meta.trade_horizon
    2. entry_meta_json.trade_horizon
    3. entry_date == today -> DAY_PROTECT
    4. SWING_CARRY
    """
    meta = pos.get("position_meta") or {}
    if isinstance(meta, str):
        try:
            import json as _json
            meta = _json.loads(meta)
        except Exception:
            meta = {}
    horizon = str(meta.get("trade_horizon") or "").strip()
    if horizon in {"DAY_PROTECT", "SWING_CARRY", "CORE_CARRY"}:
        return horizon

    entry_meta = pos.get("entry_meta_json") or {}
    if isinstance(entry_meta, str):
        try:
            import json as _json
            entry_meta = _json.loads(entry_meta)
        except Exception:
            entry_meta = {}
    horizon = str(entry_meta.get("trade_horizon") or "").strip()
    if horizon in {"DAY_PROTECT", "SWING_CARRY", "CORE_CARRY"}:
        return horizon

    # entry_date 기반 fallback
    if now_kst_date is not None:
        entry_date_raw = pos.get("entry_date") or pos.get("last_fill_at") or pos.get("entry_ts")
        if entry_date_raw:
            try:
                entry_d = pd.Timestamp(entry_date_raw).date()
                if entry_d == now_kst_date:
                    return "DAY_PROTECT"
            except Exception:
                pass

    return "SWING_CARRY"


def _resolve_position_book(pos: dict[str, Any]) -> str:
    """포지션 dict에서 book(SWING_BOOK / DAY_BOOK / CORE_BOOK)을 결정한다.

    우선순위:
    1. entry_meta_json.book
    2. position_meta.book
    3. trade_horizon → 매핑
    4. fallback: SWING_BOOK
    """
    import json as _json
    entry_meta = pos.get("entry_meta_json") or {}
    if isinstance(entry_meta, str):
        try:
            entry_meta = _json.loads(entry_meta)
        except Exception:
            entry_meta = {}
    book = str(entry_meta.get("book") or "").strip()
    if book in {"SWING_BOOK", "DAY_BOOK", "CORE_BOOK"}:
        return book

    meta = pos.get("position_meta") or {}
    if isinstance(meta, str):
        try:
            meta = _json.loads(meta)
        except Exception:
            meta = {}
    book = str(meta.get("book") or "").strip()
    if book in {"SWING_BOOK", "DAY_BOOK", "CORE_BOOK"}:
        return book

    # trade_horizon 기반 매핑
    horizon = str(entry_meta.get("trade_horizon") or meta.get("trade_horizon") or "").strip()
    _horizon_to_book = {
        "DAY_PROTECT": "DAY_BOOK",
        "SWING_CARRY": "SWING_BOOK",
        "CORE_CARRY": "CORE_BOOK",
    }
    if horizon in _horizon_to_book:
        return _horizon_to_book[horizon]

    return "SWING_BOOK"


def _apply_swing_same_day_guard(
    *,
    code: str,
    book: str,
    same_day: bool,
    holding_minutes: float,
    pnl_pct: float,
    hard_stop_hit: bool,
    emergency_stop_hit: bool,
    candidate_exit_reason: str,
    holding_qty: int,
    sell_pct: float | None,
) -> dict[str, Any]:
    """스윙 종목 당일 매도 가드 (Section 7/9 spec).

    Returns:
        {
          "allowed": bool,
          "blocked_reason": str | None,
          "route": str,   # SWING_EXIT_ROUTER | INTRADAY_EXIT_ROUTER | SWING_SAFE_EXIT_ROUTER
        }
    """
    import os as _os
    import logging as _logging
    _log = _logging.getLogger(__name__)

    swing_min_hold = int(_os.getenv("PB1_SWING_MIN_HOLD_MINUTES", "60"))
    swing_exception_pct = float(_os.getenv("PB1_SWING_SAME_DAY_EXCEPTION_PROFIT_PCT", "5.0"))

    # 긴급 손절/하드 스탑은 항상 허용
    if hard_stop_hit or emergency_stop_hit:
        _log.info(
            "[EXIT][SAME_DAY_GUARD] code=%s book=%s same_day=%s holding_minutes=%.1f pnl_pct=%.2f "
            "hard_stop=%s action=ALLOW reason=HARD_STOP_OVERRIDE",
            code, book, int(same_day), holding_minutes, pnl_pct, int(hard_stop_hit),
        )
        return {"allowed": True, "blocked_reason": None, "route": "SWING_EXIT_ROUTER"}

    if book == "SWING_BOOK":
        route = "SWING_EXIT_ROUTER"
        # 1. 최소 보유 시간 미달
        if same_day and holding_minutes < swing_min_hold:
            # 급등 예외: pnl >= exception_pct AND holding_minutes >= 30 AND qty >= 2
            if pnl_pct >= swing_exception_pct and holding_minutes >= 30 and holding_qty >= 2:
                _log.info(
                    "[EXIT][SAME_DAY_GUARD] code=%s book=SWING_BOOK same_day=1 holding_minutes=%.1f "
                    "pnl_pct=%.2f holding_qty=%s action=ALLOW reason=SWING_SAME_DAY_EXCEPTION_PROFIT",
                    code, holding_minutes, pnl_pct, holding_qty,
                )
                return {"allowed": True, "blocked_reason": None, "route": route}
            _log.info(
                "[EXIT][SAME_DAY_GUARD] code=%s book=SWING_BOOK same_day=1 holding_minutes=%.1f "
                "pnl_pct=%.2f hard_stop=0 action=BLOCK reason=SWING_SAME_DAY_MIN_HOLD_BLOCK",
                code, holding_minutes, pnl_pct,
            )
            return {"allowed": False, "blocked_reason": "SWING_SAME_DAY_MIN_HOLD_BLOCK", "route": route}

        # 2. DAY_PROTECT 계열 exit reason 차단
        _day_protect_reasons = {
            "EXIT_DAY_TRAIL_PROTECT",
            "EXIT_DAY_TAKE_PROFIT_50",
            "INTRADAY_PROFIT_PROTECT",
            "DAY_PROTECT",
            "EXIT_DAY_BREAKEVEN_PROTECT",
            "EXIT_DAY_CLOSE_PROFIT_PROTECT",
            "EXIT_DAY_CLOSE_LOSS_CUT",
            "EXIT_DAY_FORCE_CLOSE",
        }
        if candidate_exit_reason in _day_protect_reasons:
            _log.info(
                "[EXIT][DAY_PROTECT][BLOCK] code=%s book=SWING_BOOK reason=SWING_DAY_PROTECT_DISABLED "
                "candidate_reason=%s",
                code, candidate_exit_reason,
            )
            return {"allowed": False, "blocked_reason": "SWING_DAY_PROTECT_DISABLED", "route": route}

        # 3. 1주 포지션 partial exit 금지
        if holding_qty < 2 and sell_pct is not None:
            _log.info(
                "[EXIT][PARTIAL][BLOCK] code=%s holding_qty=%s sell_pct=%s reason=PARTIAL_EXIT_QTY_TOO_SMALL",
                code, holding_qty, sell_pct,
            )
            return {"allowed": False, "blocked_reason": "PARTIAL_EXIT_QTY_TOO_SMALL", "route": route}

        return {"allowed": True, "blocked_reason": None, "route": route}

    elif book == "DAY_BOOK":
        # DAY_BOOK은 당일 익절 허용
        route = "INTRADAY_EXIT_ROUTER"
        day_min_hold = int(_os.getenv("PB1_DAY_MIN_HOLD_MINUTES", "5"))
        if same_day and holding_minutes < day_min_hold:
            # DAY_BOOK도 최소 보유시간 5분 미만이면 hard_stop만 허용
            _log.info(
                "[EXIT][SAME_DAY_GUARD] code=%s book=DAY_BOOK same_day=1 holding_minutes=%.1f "
                "action=BLOCK reason=DAY_SAME_DAY_MIN_HOLD_BLOCK",
                code, holding_minutes,
            )
            return {"allowed": False, "blocked_reason": "DAY_SAME_DAY_MIN_HOLD_BLOCK", "route": route}
        # DAY_BOOK 1주 partial exit 차단
        if holding_qty < 2 and sell_pct is not None:
            day_allow_single = _os.getenv("PB1_DAY_ALLOW_SINGLE_SHARE_FULL_EXIT", "0") == "1"
            if not day_allow_single:
                _log.info(
                    "[EXIT][PARTIAL][BLOCK] code=%s book=DAY_BOOK holding_qty=%s sell_pct=%s "
                    "reason=PARTIAL_EXIT_QTY_TOO_SMALL",
                    code, holding_qty, sell_pct,
                )
                return {"allowed": False, "blocked_reason": "PARTIAL_EXIT_QTY_TOO_SMALL", "route": route}
        return {"allowed": True, "blocked_reason": None, "route": route}

    else:
        # meta 없거나 알 수 없는 book → SWING_SAFE_EXIT_ROUTER
        route = "SWING_SAFE_EXIT_ROUTER"
        # SWING_SAFE: hard_stop/emergency만 허용, 익절성 당일 매도 차단
        if same_day:
            _day_protect_reasons = {
                "EXIT_DAY_TRAIL_PROTECT",
                "EXIT_DAY_TAKE_PROFIT_50",
                "INTRADAY_PROFIT_PROTECT",
                "DAY_PROTECT",
                "EXIT_DAY_BREAKEVEN_PROTECT",
                "EXIT_DAY_CLOSE_PROFIT_PROTECT",
            }
            if candidate_exit_reason in _day_protect_reasons:
                _log.info(
                    "[EXIT][ROUTER][META_MISSING] code=%s route=SWING_SAFE_EXIT_ROUTER "
                    "candidate_reason=%s action=BLOCK",
                    code, candidate_exit_reason,
                )
                return {"allowed": False, "blocked_reason": "META_MISSING_SWING_SAFE", "route": route}
        return {"allowed": True, "blocked_reason": None, "route": route}


def _compute_kr_per_position_budget(
    *,
    tick_budget: float,
    orderable_count: int,
    slots_remaining: int,
    session_kind: str = "am",
) -> tuple[float, dict[str, Any]]:
    """KR 전용 per-position budget 계산 (Section 11 spec).

    Returns:
        (per_position_budget, sizing_debug)
    """
    import os as _os
    import logging as _logging
    _log = _logging.getLogger(__name__)

    if session_kind in {"am", "morning"}:
        target_key = "PB1_KR_AM_TARGET_POSITIONS"
        default_target = 6
    else:
        target_key = "PB1_KR_PM_TARGET_POSITIONS"
        default_target = 6

    target_positions_cfg = int(_os.getenv(target_key, str(default_target)))
    max_per_tick = int(_os.getenv("PB1_KR_MAX_NEW_POSITIONS_PER_TICK", "4"))
    min_krw = float(_os.getenv("PB1_KR_MIN_POSITION_KRW", "2000000"))
    max_krw = float(_os.getenv("PB1_KR_MAX_POSITION_KRW", "5000000"))

    actual_target = min(
        max(orderable_count, 1),
        max(slots_remaining, 1),
        target_positions_cfg,
        max_per_tick,
    )

    raw_budget = (tick_budget / actual_target) if actual_target > 0 else min_krw
    per_position_budget = min(max_krw, max(min_krw, raw_budget))

    debug = {
        "orderable_count": orderable_count,
        "slots_remaining": slots_remaining,
        "target_positions_cfg": target_positions_cfg,
        "max_per_tick": max_per_tick,
        "actual_target": actual_target,
        "tick_budget": tick_budget,
        "raw_budget": raw_budget,
        "per_position_budget": per_position_budget,
        "min_krw": min_krw,
        "max_krw": max_krw,
    }
    _log.info(
        "[SIZING][KR] orderable_count=%s target_positions=%s tick_budget=%.0f "
        "per_position_budget=%.0f min=%.0f max=%.0f",
        orderable_count,
        actual_target,
        tick_budget,
        per_position_budget,
        min_krw,
        max_krw,
    )
    return per_position_budget, debug


def _calculate_exit_qty(holding_qty: int, orderable_qty: int, sell_pct: float | None) -> int:
    """exit 수량을 계산한다.

    sell_pct=None -> 전량 매도
    """
    orderable = max(0, int(orderable_qty or 0))
    if sell_pct is None:
        return orderable
    qty = int(orderable * float(sell_pct))
    if qty < 1 and orderable > 0:
        qty = 1
    return min(qty, orderable)


def _resolve_effective_exit_risk_for_pos(pos: dict[str, Any]) -> dict[str, Any]:
    """기존 보유 포지션 포함 모든 포지션의 exit 평가용 effective stop/R을 계산한다.

    DB에 저장된 기존 stop_price_at_entry가 너무 깊어도 최신 정책 기준(7%/8% 캡)으로 보정한다.
    수정 방향: effective_stop = max(raw_stop, entry_price * (1 - cap_pct))
    long 기준으로 higher stop = tighter stop이므로 max()를 사용한다.

    Note: os.getenv를 직접 사용하여 테스트 시 monkeypatch가 즉시 반영되도록 한다.
    """
    import os as _os

    eff_exit_enabled = _os.getenv("PB1_EXISTING_POSITION_EFFECTIVE_EXIT_ENABLED", "1") != "0"
    stop_cap_enabled = _os.getenv("PB1_EFFECTIVE_STOP_CAP_ENABLED", "1") != "0"

    meta = pos.get("position_meta") or {}
    if isinstance(meta, str):
        try:
            import json as _json
            meta = _json.loads(meta)
        except Exception:
            meta = {}

    entry_price = float(
        pos.get("entry_price")
        or pos.get("avg_buy_price")
        or pos.get("avg")
        or 0.0
    )
    raw_stop = float(
        meta.get("initial_stop_price")
        or pos.get("stop_price_at_entry")
        or pos.get("stop_price")
        or pos.get("initial_stop")
        or 0.0
    )

    if entry_price <= 0:
        return {
            "entry_price": entry_price,
            "raw_stop_price": raw_stop,
            "effective_stop_price": raw_stop,
            "raw_r_value": None,
            "effective_r_value": None,
            "stop_cap_price": None,
            "stop_cap_pct": None,
            "effective_applied": False,
            "market": "",
            "code": str(pos.get("code") or ""),
            "reason": "invalid_entry_price",
        }

    raw_r = entry_price - raw_stop if raw_stop > 0 else None

    market = str(pos.get("market") or pos.get("market_code") or "").upper()
    code = str(pos.get("code") or pos.get("pdno") or "").zfill(6)

    _kospi_cap = float(_os.getenv("PB1_EFFECTIVE_STOP_CAP_KOSPI_PCT", str(PB1_EFFECTIVE_STOP_CAP_KOSPI_PCT)))
    _kosdaq_cap = float(_os.getenv("PB1_EFFECTIVE_STOP_CAP_KOSDAQ_PCT", str(PB1_EFFECTIVE_STOP_CAP_KOSDAQ_PCT)))

    stop_cap_pct = _kospi_cap
    if market in {"KQ", "KOSDAQ", "Q"}:
        stop_cap_pct = _kosdaq_cap

    stop_cap_price = entry_price * (1.0 - stop_cap_pct / 100.0)
    effective_stop = raw_stop
    effective_applied = False

    if eff_exit_enabled and stop_cap_enabled:
        if raw_stop <= 0:
            effective_stop = stop_cap_price
            effective_applied = True
        else:
            # long 기준: 높은 stop이 더 타이트 → max()로 캡 적용
            effective_stop = max(raw_stop, stop_cap_price)
            effective_applied = effective_stop != raw_stop

    effective_r = entry_price - effective_stop if effective_stop > 0 else None
    if effective_r is not None and effective_r <= 0:
        effective_r = raw_r

    logger.info(
        "[EXIT][EFFECTIVE_RISK] code=%s entry=%.2f raw_stop=%s effective_stop=%s "
        "raw_r=%s effective_r=%s cap_pct=%s applied=%s",
        code,
        entry_price,
        raw_stop,
        round(effective_stop, 2) if effective_stop else None,
        round(raw_r, 2) if raw_r is not None else None,
        round(effective_r, 2) if effective_r is not None else None,
        stop_cap_pct,
        int(effective_applied),
    )

    return {
        "entry_price": entry_price,
        "raw_stop_price": raw_stop,
        "effective_stop_price": effective_stop,
        "raw_r_value": raw_r,
        "effective_r_value": effective_r,
        "stop_cap_price": stop_cap_price,
        "stop_cap_pct": stop_cap_pct,
        "effective_applied": effective_applied,
        "market": market,
        "code": code,
        "reason": "ok",
    }






def _resolve_day_protect_exit(
    pos: dict[str, Any],
    mark: float,
    now_hhmm: int,
    *,
    ret_pct: float,
    max_pnl_pct: float,
    stop_hit: bool,
) -> dict[str, Any]:
    """DAY_PROTECT 당일 수익 보호 매도 정책."""
    import os as _os
    enabled = _os.getenv("PB1_DAY_PROTECT_ENABLED", "1") == "1"
    if not enabled:
        return {"exit_ok": False, "reason": "DAY_PROTECT_DISABLED"}

    stop_loss_pct = float(_os.getenv("PB1_DAY_STOP_LOSS_PCT", "2.0"))
    profit_arm_pct = float(_os.getenv("PB1_DAY_PROFIT_ARM_PCT", "1.5"))
    breakeven_pct = float(_os.getenv("PB1_DAY_BREAKEVEN_PROTECT_PCT", "0.2"))
    trail_arm_pct = float(_os.getenv("PB1_DAY_TRAIL_ARM_PCT", "2.0"))
    trail_drop_pct = float(_os.getenv("PB1_DAY_TRAIL_DROP_PCT", "1.0"))
    take_profit_pct = float(_os.getenv("PB1_DAY_TAKE_PROFIT_PCT", "4.0"))
    take_profit_sell = float(_os.getenv("PB1_DAY_TAKE_PROFIT_SELL_PCT", "0.50"))
    close_protect_hhmm = int(_os.getenv("PB1_DAY_CLOSE_PROTECT_TIME", "15:05").replace(":", ""))
    force_exit_hhmm = int(_os.getenv("PB1_DAY_FORCE_EXIT_TIME", "15:20").replace(":", ""))

    meta = pos.get("position_meta") or {}
    if isinstance(meta, str):
        try:
            import json as _json
            meta = _json.loads(meta)
        except Exception:
            meta = {}
    tp1_done = bool(meta.get("tp1_done", False))
    orderable_qty = int(pos.get("orderable_qty") or pos.get("qty") or 0)
    drawdown_from_high = max_pnl_pct - ret_pct

    # 1. 하드 손절
    if stop_hit or ret_pct <= -stop_loss_pct:
        qty = _calculate_exit_qty(orderable_qty, orderable_qty, None)
        return {"exit_ok": True, "reason": "EXIT_DAY_STOP_LOSS", "qty": qty, "sell_pct": None}

    # 2. 15:20 강제 청산
    if now_hhmm >= force_exit_hhmm:
        qty = _calculate_exit_qty(orderable_qty, orderable_qty, None)
        return {"exit_ok": True, "reason": "EXIT_DAY_FORCE_CLOSE", "qty": qty, "sell_pct": None}

    # 3. 장마감 손실 축소 (15:05 이후, 손실)
    if now_hhmm >= close_protect_hhmm and ret_pct <= -1.0:
        qty = _calculate_exit_qty(orderable_qty, orderable_qty, None)
        return {"exit_ok": True, "reason": "EXIT_DAY_CLOSE_LOSS_CUT", "qty": qty, "sell_pct": None}

    # 4. 장마감 수익 보호 (15:05 이후, 수익)
    if now_hhmm >= close_protect_hhmm and ret_pct > 0:
        qty = _calculate_exit_qty(orderable_qty, orderable_qty, None)
        return {"exit_ok": True, "reason": "EXIT_DAY_CLOSE_PROFIT_PROTECT", "qty": qty, "sell_pct": None}

    # 5. 본전 보호 (수익 발생 후 돌아옴)
    if max_pnl_pct >= profit_arm_pct and ret_pct <= breakeven_pct:
        qty = _calculate_exit_qty(orderable_qty, orderable_qty, None)
        return {"exit_ok": True, "reason": "EXIT_DAY_BREAKEVEN_PROTECT", "qty": qty, "sell_pct": None}

    # 6. 고점 대비 하락 보호
    if max_pnl_pct >= trail_arm_pct and drawdown_from_high >= trail_drop_pct:
        qty = _calculate_exit_qty(orderable_qty, orderable_qty, None)
        return {"exit_ok": True, "reason": "EXIT_DAY_TRAIL_PROTECT", "qty": qty, "sell_pct": None}

    # 7. 당일 목표 수익 부분익절
    if ret_pct >= take_profit_pct and not tp1_done:
        qty = _calculate_exit_qty(orderable_qty, orderable_qty, take_profit_sell)
        return {
            "exit_ok": True,
            "reason": "EXIT_DAY_TAKE_PROFIT_50",
            "qty": qty,
            "sell_pct": take_profit_sell,
            "update_meta": {"tp1_done": True},
        }

    return {"exit_ok": False, "reason": "DAY_HOLD_PROFIT_OK"}


def _resolve_swing_staged_exit(
    pos: dict[str, Any],
    mark: float,
    ma20: float | None,
    *,
    ret_pct: float,
    days_held: int,
    stop_hit: bool,
    trail_hit: bool = False,
    trail_stop_price: float | None = None,
    highest_ret_pct: float | None = None,
) -> dict[str, Any]:
    """SWING_CARRY R-multiple 단계별 매도 정책 (Multi-Layer Exit Router 통합).

    PB1_EXIT_ROUTER_ENABLED=1 (기본값):
        trader/exit_policy/router.py apply_swing_exit_decision() 사용.
        R + 수익률% + giveback + 추세 복합 판단.
        강한 추세에서는 R 도달만으로 전량 매도하지 않는다.

    PB1_EXIT_ROUTER_ENABLED=0 (레거시 fallback):
        기존 R-based TP + profit_protect(8%) + abs_tp1(10%) 로직 사용.
    """
    import os as _os
    enabled = _os.getenv("PB1_SWING_STAGED_EXIT_ENABLED", "1") == "1"
    if not enabled:
        return {"exit_ok": False, "reason": "SWING_STAGED_EXIT_DISABLED"}

    # ──────────────────────────────────────────────────────────────
    # Effective stop/R 계산: 기존 손절가 캡 기준으로 보정 (공통)
    # ──────────────────────────────────────────────────────────────
    meta = pos.get("position_meta") or {}
    if isinstance(meta, str):
        try:
            import json as _json
            meta = _json.loads(meta)
        except Exception:
            meta = {}

    avg = float(pos.get("avg_buy_price") or pos.get("avg") or pos.get("entry_price") or 0.0)
    orderable_qty = int(pos.get("orderable_qty") or pos.get("qty") or 0)
    code_for_log = str(pos.get("code") or pos.get("stock_code") or "UNKNOWN")

    risk_ctx = _resolve_effective_exit_risk_for_pos(pos)
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

    # ──────────────────────────────────────────────────────────────
    # Exit Router 분기: PB1_EXIT_ROUTER_ENABLED=1이면 router 사용
    # ──────────────────────────────────────────────────────────────
    _router_enabled = _os.getenv("PB1_EXIT_ROUTER_ENABLED", "1") == "1"
    if _router_enabled:
        from trader.exit_policy.router import (
            resolve_exit_policy_for_position,
            apply_swing_exit_decision,
        )
        _policy = resolve_exit_policy_for_position(
            pos=pos,
            features={},
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
            market_ctx={"ma20": ma20},
        )
        return apply_swing_exit_decision(
            pos, mark, _policy,
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

    # ──────────────────────────────────────────────────────────────
    # 레거시 fallback (PB1_EXIT_ROUTER_ENABLED=0)
    # ──────────────────────────────────────────────────────────────
    tp1_r = float(_os.getenv("PB1_SWING_TP1_R", "2.0"))
    tp1_sell_pct = float(_os.getenv("PB1_SWING_TP1_SELL_PCT", "0.33"))
    tp2_r = float(_os.getenv("PB1_SWING_TP2_R", "3.0"))
    tp2_sell_pct = float(_os.getenv("PB1_SWING_TP2_SELL_PCT", "0.33"))
    time_stop_days = int(_os.getenv("PB1_SWING_TIME_STOP_DAYS", "10"))

    tp1_done = bool(meta.get("tp1_done", False))
    tp2_done = bool(meta.get("tp2_done", False))
    profit_protect_done = bool(meta.get("profit_protect_done", False))
    abs_tp1_done = bool(meta.get("abs_tp1_done", False))
    max_r = float(meta.get("max_r_since_entry") or 0.0)

    # ──────────────────────────────────────────────────────────────
    # 1. effective hard stop (기존 raw stop 대신 effective stop 사용)
    # ──────────────────────────────────────────────────────────────
    if stop_hit or (effective_stop > 0 and mark <= effective_stop):
        qty = _calculate_exit_qty(orderable_qty, orderable_qty, None)
        logger.info(
            "[EXIT][SWING][TP_CHECK] code=%s check=STOP_HIT result=EXIT "
            "reason=STOP_HIT_EFFECTIVE mark=%.2f effective_stop=%.2f",
            code_for_log, mark, effective_stop,
        )
        _effective_meta_update = {
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
            "update_meta": _effective_meta_update,
        }

    # ──────────────────────────────────────────────────────────────
    # 2. +8% 보호익절 (profit_protect, 중복 방지)
    # ──────────────────────────────────────────────────────────────
    _profit_protect_pct = float(_os.getenv("PB1_PROFIT_PROTECT_PCT", str(PB1_PROFIT_PROTECT_PCT)))
    _profit_protect_sell_pct = float(_os.getenv("PB1_PROFIT_PROTECT_SELL_PCT", str(PB1_PROFIT_PROTECT_SELL_PCT)))
    _profit_protect_enabled = _os.getenv("PB1_PROFIT_PROTECT_ENABLED", "1") == "1" and PB1_PROFIT_PROTECT_ENABLED

    if _profit_protect_enabled and ret_pct >= _profit_protect_pct and not profit_protect_done:
        qty = _calculate_exit_qty(orderable_qty, orderable_qty, _profit_protect_sell_pct)
        logger.info(
            "[EXIT][PROFIT_PROTECT] code=%s ret_pct=%.2f threshold=%.1f "
            "qty=%s sell_qty=%s reason=PROFIT_PROTECT_8PCT",
            code_for_log, ret_pct, _profit_protect_pct, orderable_qty, qty,
        )
        return {
            "exit_ok": True,
            "reason": "PROFIT_PROTECT_8PCT",
            "qty": qty,
            "sell_pct": _profit_protect_sell_pct,
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

    # ──────────────────────────────────────────────────────────────
    # 3. +10% 절대수익률 TP1 (abs_tp1, 중복 방지)
    # ──────────────────────────────────────────────────────────────
    _abs_tp1_pct = float(_os.getenv("PB1_ABS_TP1_PROFIT_PCT", str(PB1_ABS_TP1_PROFIT_PCT)))
    _abs_tp1_sell_pct = float(_os.getenv("PB1_ABS_TP1_SELL_PCT", str(PB1_ABS_TP1_SELL_PCT)))
    _abs_tp1_enabled = _os.getenv("PB1_ABS_TP1_ENABLED", "1") == "1" and PB1_ABS_TP1_ENABLED

    if _abs_tp1_enabled and ret_pct >= _abs_tp1_pct and not abs_tp1_done:
        qty = _calculate_exit_qty(orderable_qty, orderable_qty, _abs_tp1_sell_pct)
        logger.info(
            "[EXIT][ABS_TP1] code=%s ret_pct=%.2f threshold=%.1f "
            "sell_qty=%s reason=ABS_TP1_10PCT",
            code_for_log, ret_pct, _abs_tp1_pct, qty,
        )
        return {
            "exit_ok": True,
            "reason": "ABS_TP1_10PCT",
            "qty": qty,
            "sell_pct": _abs_tp1_sell_pct,
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

    # ──────────────────────────────────────────────────────────────
    # 4. R-based TP1 (중복 방지)
    # ──────────────────────────────────────────────────────────────
    logger.info(
        "[EXIT][SWING][TP_CHECK] code=%s check=TP1 current_r=%.3f tp1_r=%.1f tp1_done=%s",
        code_for_log, current_r, tp1_r, tp1_done,
    )
    if current_r >= tp1_r and not tp1_done:
        qty = _calculate_exit_qty(orderable_qty, orderable_qty, tp1_sell_pct)
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

    # ──────────────────────────────────────────────────────────────
    # 5. R-based TP2 (중복 방지)
    # ──────────────────────────────────────────────────────────────
    logger.info(
        "[EXIT][SWING][TP_CHECK] code=%s check=TP2 current_r=%.3f tp2_r=%.1f tp2_done=%s",
        code_for_log, current_r, tp2_r, tp2_done,
    )
    if current_r >= tp2_r and not tp2_done:
        qty = _calculate_exit_qty(orderable_qty, orderable_qty, tp2_sell_pct)
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

    # ──────────────────────────────────────────────────────────────
    # 6. Runner MA20 이탈 (TP1 이후)
    # ──────────────────────────────────────────────────────────────
    if tp1_done and ma20 is not None and mark < ma20:
        qty = _calculate_exit_qty(orderable_qty, orderable_qty, None)
        logger.info(
            "[EXIT][SWING][TP_CHECK] code=%s check=MA20_RUNNER result=EXIT mark=%.2f ma20=%.2f",
            code_for_log, mark, ma20,
        )
        return {"exit_ok": True, "reason": "EXIT_SWING_RUNNER_MA20_BREAK", "qty": qty, "sell_pct": None}

    # ──────────────────────────────────────────────────────────────
    # 7. time stop
    # ──────────────────────────────────────────────────────────────
    if days_held >= time_stop_days and current_r < 1.0:
        qty = _calculate_exit_qty(orderable_qty, orderable_qty, None)
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




def _resolve_core_trend_follow_exit(
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
    import os as _os
    enabled = _os.getenv("PB1_CORE_EXIT_ENABLED", "1") == "1"
    if not enabled:
        return {"exit_ok": False, "reason": "CORE_EXIT_DISABLED"}

    hard_stop_pct = float(_os.getenv("PB1_CORE_HARD_STOP_PCT", "8.0"))
    tp1_r = float(_os.getenv("PB1_CORE_TP1_R", "3.0"))
    tp1_sell_pct = float(_os.getenv("PB1_CORE_TP1_SELL_PCT", "0.25"))
    time_stop_days = int(_os.getenv("PB1_CORE_TIME_STOP_DAYS", "20"))

    meta = pos.get("position_meta") or {}
    if isinstance(meta, str):
        try:
            import json as _json
            meta = _json.loads(meta)
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

    # 1. hard stop
    if ret_pct <= -hard_stop_pct:
        qty = _calculate_exit_qty(orderable_qty, orderable_qty, None)
        return {"exit_ok": True, "reason": "EXIT_CORE_HARD_STOP", "qty": qty, "sell_pct": None}

    # 2. core TP1 (25% 부분익절)
    if current_r >= tp1_r and not core_tp1_done:
        qty = _calculate_exit_qty(orderable_qty, orderable_qty, tp1_sell_pct)
        return {
            "exit_ok": True,
            "reason": "EXIT_CORE_TP1",
            "qty": qty,
            "sell_pct": tp1_sell_pct,
            "update_meta": {"core_tp1_done": True, "tp1_done": True, "tp1_price": mark, "tp1_qty": qty},
        }

    # 3. risk-off: bear + MA20 & MA50 동시 이탈 (MA50 단순 이탈보다 우선)
    bear_regime = str(regime or "").upper() in {"BEAR", "DOWNTREND", "RISK_OFF"}
    if bear_regime and ma20 is not None and ma50 is not None and mark < ma20 and mark < ma50:
        qty = _calculate_exit_qty(orderable_qty, orderable_qty, None)
        return {"exit_ok": True, "reason": "EXIT_CORE_RISK_OFF", "qty": qty, "sell_pct": None}

    # 4. MA50 이탈
    if ma50 is not None and mark < ma50:
        qty = _calculate_exit_qty(orderable_qty, orderable_qty, None)
        return {"exit_ok": True, "reason": "EXIT_CORE_MA50_BREAK", "qty": qty, "sell_pct": None}

    return {"exit_ok": False, "reason": "CORE_HOLD_TREND_OK"}


def _resolve_exit_policy(
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
    same_day_entry = int(days_held or 0) == 0
    trail_eligible = int(days_held or 0) >= 1 and int(holding_bars or 0) >= int(MIN_TRAIL_BARS)
    soft_exit_eligible = int(days_held or 0) >= 1 and int(holding_bars or 0) >= int(MIN_EXIT_BARS)
    trail_hit = bool(trail_eligible and trail_stop_price is not None and mark <= float(trail_stop_price))
    ma20_break = bool(soft_exit_eligible and ma20 is not None and mark < ma20)
    ma50_break = bool(soft_exit_eligible and ma50 is not None and mark < ma50)
    soft_exit_hit = bool(soft_exit_eligible and (risk_off_signal or ma50_break or ma20_break))

    triggered: list[str] = []
    final_reason = "NO_EXIT_SIGNAL"
    family = "SKIP"
    exit_ok = False
    if stop_hit:
        triggered.append("EXIT_HARD_STOP")
        final_reason = "EXIT_HARD_STOP"
        family = "EXIT_STOP"
        exit_ok = True
    elif trail_hit:
        triggered.append("EXIT_TRAIL")
        final_reason = "EXIT_TRAIL"
        family = "EXIT_TRAIL"
        exit_ok = True
    elif soft_exit_hit:
        triggered.append("EXIT_SOFT_RISK_OFF")
        final_reason = "EXIT_SOFT_RISK_OFF"
        family = "EXIT_RISK_OFF"
        exit_ok = True
    elif time_stop_hit:
        triggered.append("EXIT_TIME_BASED")
        final_reason = "EXIT_TIME_BASED"
        family = "EXIT_TIME"
        exit_ok = True

    return {
        "same_day_entry": same_day_entry,
        "trail_eligible": trail_eligible,
        "soft_exit_eligible": soft_exit_eligible,
        "trail_hit": trail_hit,
        "ma20_break": ma20_break,
        "ma50_break": ma50_break,
        "risk_off_hit": bool(soft_exit_eligible and risk_off_signal),
        "soft_exit_hit": soft_exit_hit,
        "triggered": triggered,
        "final_reason": final_reason,
        "family": family,
        "exit_ok": exit_ok,
    }


def _compute_affordable_buy_qty(
    *,
    target_budget: float,
    buy_ref_price: float,
    cash_available: float,
    min_remaining_cash_krw: float,
    allow_single_share_override: bool,
    budget_flex_pct: float,
) -> tuple[int, dict[str, Any]]:
    budget = float(target_budget or 0.0)
    price = float(buy_ref_price or 0.0)
    cash = float(cash_available or 0.0)
    remaining_cash_floor = float(min_remaining_cash_krw or 0.0)
    effective_budget = round(budget * max(float(budget_flex_pct or 0.0), 0.0), 4)
    qty_by_budget = int(effective_budget // price) if price > 0 else 0
    one_share_cost = round(price * (1.0 + float(BUY_PRICE_BUFFER_PCT)), 4) if price > 0 else 0.0
    override = bool(allow_single_share_override)
    skip_reason = ""
    buy_mode = "budget"
    final_qty = qty_by_budget

    if price <= 0:
        final_qty = 0
        skip_reason = "quantity_zero_after_budget_check"
        buy_mode = "invalid_price"
    elif final_qty < 1:
        if not override:
            skip_reason = "insufficient_budget_and_override_disabled"
            buy_mode = "insufficient_budget"
            final_qty = 0
        elif cash < one_share_cost:
            skip_reason = "insufficient_cash_for_one_share"
            buy_mode = "insufficient_budget"
            final_qty = 0
        elif cash < one_share_cost + remaining_cash_floor:
            skip_reason = "min_remaining_cash_violation"
            buy_mode = "insufficient_budget"
            final_qty = 0
        else:
            final_qty = 1
            buy_mode = "single_share_override"

    details = {
        "cash": cash,
        "target_budget": budget,
        "effective_budget": effective_budget,
        "buy_ref_price": price,
        "qty_by_budget": qty_by_budget,
        "one_share_cost": one_share_cost,
        "single_share_override": override,
        "buy_mode": buy_mode,
        "final_qty": final_qty,
        "skip_reason": skip_reason,
    }
    return final_qty, details


def _should_allow_single_share_position_cap_override(
    *,
    rank: int,
    final_qty: int,
    afford_details: dict[str, Any],
    force_min1_topn: int,
    force_min1_override_position_cap: bool,
    cash_available: float,
    min_remaining_cash_krw: float,
    order_possible_cash: float,
) -> bool:
    if not bool(force_min1_override_position_cap):
        return False
    if int(final_qty or 0) != 1:
        return False
    if int(rank or 0) > max(1, int(force_min1_topn or 1)):
        return False
    if str(afford_details.get("buy_mode") or "").strip().lower() != "single_share_override":
        return False

    one_share_cost = float(afford_details.get("one_share_cost") or 0.0)
    cash_floor = float(one_share_cost + float(min_remaining_cash_krw or 0.0))
    if float(cash_available or 0.0) < cash_floor:
        return False
    if float(order_possible_cash or 0.0) > 0 and float(order_possible_cash or 0.0) < one_share_cost:
        return False
    return True


def _resolve_session_window_name(*, session_kind: str | None, raw_window_name: str | None) -> str:
    normalized_session = str(session_kind or "").strip().lower()
    normalized_window = str(raw_window_name or "").strip().lower()
    if normalized_session == "am" and normalized_window in {"am", "morning", "intraday", "day", "preopen", "session", "open"}:
        return "morning"
    if normalized_window in {"morning", "preopen", "close", "intraday"}:
        return "intraday"
    if normalized_window == "after":
        return "after"
    return "day"


def build_stage_label(*, session_kind: str | None, window: str | None = None, phase: str | None = None) -> str:
    """[2026-04-30] 올바른 stage 레이블 생성 (AM entry ≠ PB1-CLOSE).

    Rules:
        session_kind=am, phase=entry  → PB1-AM-ENTRY
        session_kind=afternoon/pm, phase=entry → PB1-AFTERNOON-ENTRY
        phase=exit or close           → PB1-CLOSE-EXIT
        fallback                      → PB1-{SESSION}-{PHASE}
    """
    sess = str(session_kind or "").strip().lower()
    ph = str(phase or "").strip().lower()
    if ph in {"exit", "close"}:
        return "PB1-CLOSE-EXIT"
    if ph == "entry":
        if sess == "am":
            return "PB1-AM-ENTRY"
        if sess in {"pm", "afternoon"}:
            return "PB1-AFTERNOON-ENTRY"
    sess_label = (sess or "unknown").upper()
    ph_label = (ph or "unknown").upper()
    return f"PB1-{sess_label}-{ph_label}"


def _extract_cooldown_source_details(ledger_rows: Iterable[dict[str, Any]] | None) -> dict[str, Any]:
    risk_off_exit_reasons = {"EXIT_RISK_OFF", "EXIT_SOFT_RISK_OFF", "BUG_RECOVERY_EXIT"}
    for row in ledger_rows or []:
        event_type = str((row or {}).get("event_type") or "").strip().upper()
        side = str((row or {}).get("side") or "").strip().upper()
        reasons = [str(item).strip().upper() for item in ((row or {}).get("reasons") or []) if str(item).strip()]
        payload = (row or {}).get("payload_json") if isinstance((row or {}).get("payload_json"), dict) else {}
        exit_reason = str(payload.get("exit_reason") or payload.get("reason") or (reasons[0] if reasons else "")).strip().upper()
        if not exit_reason:
            continue
        if side != "SELL" and not event_type.startswith("EXIT"):
            continue
        if exit_reason in risk_off_exit_reasons:
            return {
                "source": "risk_off_same_day_only",
                "recent_valid_exit_event": False,
                "recent_exit_reason": exit_reason,
                "evidence_count": 1,
            }
        return {
            "source": "completed_trade_cooldown",
            "recent_valid_exit_event": True,
            "recent_exit_reason": exit_reason,
            "evidence_count": 1,
        }
    return {
        "source": "none",
        "recent_valid_exit_event": False,
        "recent_exit_reason": None,
        "evidence_count": 0,
    }

def _fill_reconcile_warn_needed(filled_price: float, avg_price_from_balance: float) -> bool:
    if filled_price <= 0 or avg_price_from_balance <= 0:
        return False
    return abs(filled_price - avg_price_from_balance) / avg_price_from_balance > 0.01


@dataclass
class RunResult:
    status: str
    notes: str | None = None
    balance_api_calls: int = 0
    balance_cache_hits: int = 0
    balance_tick_cache_hits: int = 0
    terminal_state: str | None = None
    warning_counts: dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True)
class UniverseContext:
    as_of_date: str | None
    members: list[dict]
    selected_path: str | None = None
    meta: dict | None = None
    is_empty: bool = False


@dataclass(frozen=True)
class FilterThresholds:
    vol_contraction_max: float
    volu_contraction_max: float
    pullback_min: float
    pullback_max: float
    require_both_contractions: bool

    def with_overrides(self, **kwargs: float | bool) -> "FilterThresholds":
        data = {
            "vol_contraction_max": self.vol_contraction_max,
            "volu_contraction_max": self.volu_contraction_max,
            "pullback_min": self.pullback_min,
            "pullback_max": self.pullback_max,
            "require_both_contractions": self.require_both_contractions,
        }
        data.update(kwargs)
        return FilterThresholds(**data)


def evaluate_filters(
    features: Dict[str, Any],
    market: str | MinerviniConfig,
    thresholds: FilterThresholds | None = None,
    *,
    require_volume: bool | None = None,
):
    """Backward-compatible PB1 filter adapter."""
    if isinstance(market, MinerviniConfig):
        return _strategy_evaluate_filters(features, market)

    reasons: list[str] = []
    def _num(value: Any) -> float | None:
        try:
            if value in (None, ""):
                return None
            return float(value)
        except Exception:
            return None

    close_value = _num(features.get("close"))
    ma20_value = _num(features.get("ma20"))
    ma50_value = _num(features.get("ma50"))
    ma20_slope = _num(features.get("ma20_slope"))
    pullback_pct = _num(features.get("pullback_pct"))
    vol_contraction = _num(features.get("vol_contraction"))
    volu_contraction = _num(features.get("volu_contraction"))
    volume_missing = bool(features.get("volume_missing"))

    if close_value is None or ma20_value is None or close_value <= ma20_value:
        reasons.append("close_below_ma20")
    if ma20_value is None or ma50_value is None or ma20_value <= ma50_value:
        reasons.append("ma20_below_ma50")
    if ma20_slope is None or ma20_slope <= 0:
        reasons.append("ma20_slope_fail")

    pullback_min = float(thresholds.pullback_min) if thresholds is not None else float(PB1_PULLBACK_MIN)
    pullback_max = float(thresholds.pullback_max) if thresholds is not None else float(PB1_PULLBACK_MAX)
    if pullback_pct is not None and pullback_pct > 1.0:
        pullback_pct = pullback_pct / 100.0
    if pullback_pct is None or pullback_pct < pullback_min or pullback_pct > pullback_max:
        reasons.append("pullback_range_fail")

    vol_limit = float(thresholds.vol_contraction_max) if thresholds is not None else float(PB1_VOL_MAX)
    volu_limit = float(thresholds.volu_contraction_max) if thresholds is not None else float(PB1_VOLU_MAX)
    if vol_contraction is None or vol_contraction > vol_limit:
        reasons.append("vol_contraction_fail")
    if require_volume if require_volume is not None else PB1_REQUIRE_VOLUME:
        if volume_missing:
            reasons.append("volume_missing")
        elif volu_contraction is None or volu_contraction > volu_limit:
            reasons.append("volu_contraction_fail")

    require_both = bool(thresholds.require_both_contractions) if thresholds is not None else bool(PB1_REQUIRE_BOTH_CONTRACTIONS)
    if not require_both:
        contraction_failures = {"vol_contraction_fail", "volu_contraction_fail"}
        if any(reason not in contraction_failures for reason in reasons):
            reasons = [reason for reason in reasons if reason not in contraction_failures]

    return len(reasons) == 0, reasons


def resolve_pb1_phase(
    now: datetime,
    trading_day: bool,
    force_phase_env: str | None = None,
    force_entry_window_override: bool = False,
    forced_trade_session: str | None = None,
) -> tuple[str, str, str]:
    force_raw = (force_phase_env or "").strip().lower()
    window = resolve_market_window(now, trading_day)
    force_market_window = (os.getenv("FORCE_MARKET_WINDOW") or "").strip().lower()
    close_cutoff_raw = (os.getenv("PB1_PM_SESSION_END") or os.getenv("PM_SESSION_END") or "15:10").strip()
    try:
        close_cutoff = datetime.strptime(close_cutoff_raw, "%H:%M").time()
    except ValueError:
        close_cutoff = datetime.strptime("15:10", "%H:%M").time()
    if force_market_window == "afternoon" and os.getenv("PB1_EXIT_ONLY_MODE", "0") == "1":
        return "exit", "exit_only_mode", window
    if force_market_window == "afternoon":
        if trading_day and now.time() < close_cutoff:
            logger.info(
                "[PB1][PHASE] force_market_window=afternoon phase=pm_entry entry_enabled=True reason=afternoon_entry_allowed"
            )
            return "pm_entry", "afternoon_entry_allowed", window
        return "close", "close_window", window
    if force_raw:
        if force_raw in {"entry", "pm_entry", "exit", "close", "verify", "manage", "idle"}:
            if force_raw == "close":
                return "close", "force", window
            return force_raw, "force", window
        logger.warning("[PB1][PHASE] invalid force phase=%s -> auto", force_raw)
    if not trading_day:
        return "idle", "auto_non_trading_day", window
    if force_entry_window_override and str(forced_trade_session or "").strip().lower() in {"am", "pm"}:
        return "entry", "force_entry_window_override", "day"
    entry_start = datetime.combine(now.date(), datetime.strptime(PB1_ENTRY_WINDOW_START, "%H:%M").time(), now.tzinfo)
    entry_open_end = datetime.combine(now.date(), datetime.strptime(PB1_ENTRY_OPEN_END, "%H:%M").time(), now.tzinfo)
    entry_end = datetime.combine(now.date(), datetime.strptime(PB1_ENTRY_WINDOW_END, "%H:%M").time(), now.tzinfo)
    exit_start = datetime.combine(now.date(), datetime.strptime(PB1_EXIT_WINDOW_START, "%H:%M").time(), now.tzinfo)
    exit_end = datetime.combine(now.date(), datetime.strptime(PB1_EXIT_WINDOW_END, "%H:%M").time(), now.tzinfo)
    if entry_start <= now < entry_open_end:
        return "entry", "auto_opening", window
    if entry_open_end <= now < entry_end:
        return "entry", "auto_day", window
    if exit_start <= now <= exit_end:
        return "exit", "auto_close", window
    return "manage", "auto_manage", window


def compute_window(now_kst: datetime) -> str:
    if now_kst.tzinfo is None:
        now_kst = now_kst.replace(tzinfo=ZoneInfo("Asia/Seoul"))
    trading_day = now_kst.weekday() < 5
    return resolve_market_window(now_kst, trading_day)


def round_to_tick(price: float) -> int:
    """KRX 호가단위로 올림(ceiling) 처리"""
    return _round_to_tick(price, mode="up")


def _extract_px_from_snapshot(snapshot: dict) -> tuple[float | None, float | None, float | None]:
    """
    Extract (ask, bid, prpr) from snapshot dict.
    Returns (ask, bid, prpr) as floats or None.
    """
    def _to_float(x):
        try:
            return float(x) if x not in (None, "", {}) else None
        except Exception:
            return None

    # snapshot may have keys: ask, bid, last, prpr, stck_prpr, raw, etc.
    ask = snapshot.get("ask") or snapshot.get("askp") or snapshot.get("ask_prc") or snapshot.get("askp1")
    bid = snapshot.get("bid") or snapshot.get("bidp") or snapshot.get("bid_prc") or snapshot.get("bidp1")
    prpr = snapshot.get("prpr") or snapshot.get("stck_prpr") or snapshot.get("last")
    
    # If raw is present, try to extract from raw too
    if "raw" in snapshot and isinstance(snapshot["raw"], dict):
        raw = snapshot["raw"]
        if ask is None:
            ask = raw.get("askp1") or raw.get("askp") or raw.get("ask")
        if bid is None:
            bid = raw.get("bidp1") or raw.get("bidp") or raw.get("bid")
        if prpr is None:
            prpr = raw.get("stck_prpr") or raw.get("prpr") or raw.get("last")

    return _to_float(ask), _to_float(bid), _to_float(prpr)


class PB1Engine:
    STRATEGY_NAME = "pb1_pullback_close"
    UNIVERSE_STRATEGY = "best_k_meta"

    def __init__(
        self,
        *,
        universe_repo: UniverseRepo,
        orders_repo: OrdersRepo,
        fills_repo: FillsRepo,
        positions_repo: PositionsRepo,
        ledger_repo: LedgerEventsRepo,
        kis: KisAPI | None,
        dry_run: bool,
        env: str,
        run_id: str,
        window: WindowDecision | str | None = None,
        window_label: str = "",
        phase: str | None = None,
        intended_live: bool = False,
        strategy: str | None = None,
        now_kst_value: datetime | None = None,
        balance_snapshot: dict | None = None,
        balance_source: str | None = None,
        calc_allowed: bool = True,  # ✅ 계산 허용
        price_allowed: bool = True,  # ✅ 가격조회 허용
        order_allowed: bool = True,  # ✅ 주문 허용
        entry_block_reason: str | None = None,
        minervini_only: bool = False,  # ✅ MINERVINI_ONLY 모드
        preopen_max_new_positions: int = 0,
        universe_context: UniverseContext | None = None,
        diag_full_exec: bool = False,
        precomputed_final30_df: pd.DataFrame | None = None,
        precomputed_derived_df: pd.DataFrame | None = None,
        precomputed_universe_df: pd.DataFrame | None = None,
        trade_use_precomputed_features: bool = False,
        as_of: str | None = None,
        trade_date: str | date | None = None,
        run_ctx: Any = None,
        derived_as_of: str | None = None,
        final30_df: pd.DataFrame | None = None,
        final30_source: str | None = None,
        final30_locked: bool = False,
        watchlist_final_df: pd.DataFrame | None = None,
        precomputed_features_df: pd.DataFrame | None = None,
        market_window_name: str | None = None,
        compute_only_full_run: bool = False,
        force_block_live: bool = False,
        trading_day: bool | None = None,
        phase_name: str | None = None,
        window_name: str | None = None,
        entry_allowed_this_tick: bool | None = None,
        force_entry_window_override: bool = False,
        session_recovery_continue: bool = False,
        forced_trade_session: str | None = None,
        phase_guard_classification: str | None = None,
        scanner_context: dict | None = None,
    ) -> None:
        self._as_of = None
        self._trade_date = None
        self._as_of_source = None
        self._run_ctx = None
        self.universe_repo = universe_repo
        self.orders_repo = orders_repo
        self.fills_repo = fills_repo
        self.positions_repo = positions_repo
        self.ledger_repo = ledger_repo
        self.kis = kis
        raw_window = ""
        if isinstance(window, str):
            raw_window = window
        elif window is not None:
            raw_window = str(getattr(window, "name", "") or "")
        raw_window = raw_window or str(window_label or "")
        raw_phase_name = str(phase_name or "").strip().lower()
        raw_window_name = str(window_name or raw_window or "").strip().lower()
        raw_phase = str(phase or "").strip().lower()
        session_kind = str(os.getenv("PB1_SESSION_KIND") or "").strip().lower()
        inferred_phase_name = str(getattr(window, "phase", "") or "").strip().lower()
        resolved_phase_name = raw_phase_name or inferred_phase_name or "entry"
        if raw_phase:
            resolved_phase_name = raw_phase_name or raw_phase or inferred_phase_name or "entry"
        if resolved_phase_name not in {"entry", "exit", "manage"}:
            resolved_phase_name = "entry"
        resolved_window_name = _resolve_session_window_name(session_kind=session_kind, raw_window_name=raw_window_name)
        self.phase_name = resolved_phase_name
        self.window_name = resolved_window_name
        self.session_kind = session_kind  # ✅ _safe_session_kind() 첫 번째 fallback용 인스턴스 저장
        logger.info(
            "[ENGINE][INIT_CTX] phase_name=%s window_name=%s phase_arg=%s window_arg=%s",
            self.phase_name,
            self.window_name,
            phase_name,
            window_name,
        )
        if phase_name is None and window_name is None and not raw_phase and not raw_window:
            logger.warning(
                "[ENGINE][INIT_DEFAULT] missing phase/window -> fallback phase=%s window=%s",
                self.phase_name,
                self.window_name,
            )
        self.market_window_name = _resolve_session_window_name(
            session_kind=session_kind,
            raw_window_name=(market_window_name or self.window_name or "day").strip().lower(),
        )
        self.window = WindowDecision(name=self.window_name, phase=self.phase_name)
        self.window_label = self.window_name
        self.phase = self.phase_name
        
        # ✅ CRITICAL: dry_run may come as bool/int/str. Never use bool("0")!
        # parse_bool_any handles all cases: bool(True/False), int(0/1), str("0"/"1"/"yes"/"no"/etc)
        self.dry_run = parse_bool_any(dry_run, default=True)
        
        self.env = env
        self.run_id = run_id
        self.scanner_context: dict = scanner_context or {}  # [2026-05-18] KR adaptive entry filter
        self.strategy = strategy or "best_k_meta"  # [FIX] watchlist 버그 수정
        self.diag_full_exec = diag_full_exec  # ✅ DIAG 풀패스 플래그
        self.engine = getattr(orders_repo, "engine", None)
        
        # ✅ DEFENSIVE GUARD: Auto-correct intended_live vs dry_run mismatch instead of crashing
        if intended_live and self.dry_run:
            logger.error(
                "[PB1][ENGINE] intended_live=True but dry_run=True detected; "
                "downgrading intended_live to False to prevent fatal crash. "
                "DRY_RUN(env)=%s dry_run(input)=%s dry_run(parsed)=%s type=%s",
                os.getenv('DRY_RUN'), dry_run, self.dry_run, type(dry_run).__name__
            )
            intended_live = False  # Auto-correct to prevent crash
        
        self.intended_live = intended_live
        
        # ✅ DRY_RUN verification log in engine (critical)
        logger.info(
            "[DRY_RUN][ENGINE] dry_run=%s (type=%s) intended_live=%s phase=%s",
            self.dry_run,
            type(self.dry_run).__name__,
            self.intended_live,
            self.phase,
        )
        self.balance_api_calls = 0
        self.balance_cache_hits = 0
        self.balance_tick_cache_hits = 0
        self.require_volume = env_bool("PB1_REQUIRE_VOLUME", PB1_REQUIRE_VOLUME)
        self.min_candles = int(PB1_MIN_CANDLES)
        self.minervini_config = MinerviniConfig(
            rs_min_percentile=RS_MIN_PCTILE / 100.0,
            min_dollar_vol_50d=PB1_MIN_VALUE20,
            breakout_vol_mult_20=BREAKOUT_VOL_MULT,
            max_extension_from_pivot=MINERVINI_MAX_EXTENSION_PIVOT,
            initial_stop_pct=MINERVINI_INITIAL_STOP_PCT,
            risk_pct_of_equity=RISK_PER_TRADE_PCT / 100.0,
            max_pyramid_levels=MINERVINI_MAX_PYRAMID,
            add_on_R=MINERVINI_ADD_ON_R,
            heavy_volume_mult=MINERVINI_HEAVY_VOL_MULT,
        )
        providers = []
        if kis:
            providers.append(KISOHLCVProvider(kis))
        providers.append(KRXOHLCVProvider())
        self.ohlcv_provider = ChainOHLCVProvider(providers, env=env)
        self._now_kst = now_kst_value or now_kst()
        self._today = self._now_kst.date().isoformat()
        derived_default = None
        if universe_context and getattr(universe_context, "as_of_date", None):
            derived_default = str(universe_context.as_of_date)
        self.derived_as_of = str(derived_as_of or derived_default or self._today)
        self.selection_as_of = self.derived_as_of
        self.execution_date_kst = self._today
        self.run_ctx_as_of = self.derived_as_of
        self._init_run_context_state(
            as_of=as_of,
            trade_date=trade_date,
            run_ctx=run_ctx,
            derived_as_of=self.derived_as_of,
        )
        self.final30_df = final30_df if final30_df is not None else precomputed_final30_df
        source_default = None
        if universe_context and getattr(universe_context, "meta", None):
            source_default = str((universe_context.meta or {}).get("source") or "")
        self.final30_source = str(final30_source or source_default or "none")
        self.final30_locked = bool(final30_locked)
        self.watchlist_final_df = watchlist_final_df
        self.precomputed_features_df = precomputed_features_df
        self.compute_only_full_run = bool(compute_only_full_run)
        self.force_block_live = bool(force_block_live)
        self.trading_day = bool(self._now_kst.weekday() < 5) if trading_day is None else bool(trading_day)
        self.strategy_mode = str(os.getenv("STRATEGY_MODE") or "").strip().upper()
        self.precomputed_final30_df = precomputed_final30_df if precomputed_final30_df is not None else self.final30_df
        self.final30_source = self._normalize_trade_input_source(self.final30_df)
        self.precomputed_derived_df = precomputed_derived_df if precomputed_derived_df is not None else precomputed_features_df
        self.precomputed_universe_df = precomputed_universe_df
        self.trade_use_precomputed_features = bool(trade_use_precomputed_features)
        self._precomputed_final30_map: dict[str, dict[str, Any]] = {}
        self._precomputed_derived_map: dict[str, dict[str, Any]] = {}
        self._precomputed_universe_map: dict[str, dict[str, Any]] = {}
        self._data_metrics: dict[str, int] = {
            "precomputed_hits": 0,
            "short_fetch_count": 0,
            "long_fetch_blocked_count": 0,
            "kis_trade_daily_fetch_count_trade": 0,
            "kis_trade_daily_blocked_count_trade": 0,
            "kis_daily_fetch_count_trade": 0,
            "kis_daily_fetch_blocked_count_trade": 0,
        }
        if self.precomputed_final30_df is not None and not self.precomputed_final30_df.empty and "code" in self.precomputed_final30_df.columns:
            for row in self.precomputed_final30_df.to_dict(orient="records"):
                code_key = str((row or {}).get("code") or "").zfill(6)
                if code_key:
                    self._precomputed_final30_map[code_key] = dict(row or {})
        if self.precomputed_derived_df is not None and not self.precomputed_derived_df.empty:
            for row in self.precomputed_derived_df.to_dict(orient="records"):
                code_key = str((row or {}).get("symbol") or (row or {}).get("code") or "").zfill(6)
                if code_key:
                    self._precomputed_derived_map[code_key] = dict(row or {})
        if self.precomputed_universe_df is not None and not self.precomputed_universe_df.empty:
            for row in self.precomputed_universe_df.to_dict(orient="records"):
                code_key = str((row or {}).get("symbol") or (row or {}).get("code") or "").zfill(6)
                if code_key:
                    self._precomputed_universe_map[code_key] = dict(row or {})
        self._setup_reason_counter: Counter[str] = Counter()
        self.reject_reason_counts: dict[str, int] = {}
        self.reject_reason_samples: dict[str, list[str]] = {}
        self._reject_summary_candidates: list[CandidateFeature] = []
        self.total_candidates = 0
        self.ok_count = 0
        self.daily_fetch_count = 0
        self.price_fetch_count = 0
        self._current_price_cache: dict[str, tuple[float, datetime]] = {}
        self._current_price_cache_at: dict[str, datetime] = {}
        self._current_price_fetch_count = 0
        self._relax_bridge_summary: dict[str, Any] = {"enabled": False, "activated": False, "reason": "not_checked"}
        self.entry_mode = PB1_ENTRY_MODE
        self.entry_require_both = bool(PB1_REQUIRE_BOTH)
        self.entry_cond_mode = ENTRY_COND_MODE
        self.log_entry_gate = bool(PB1_LOG_ENTRY_GATE)
        self.drop_reasons_topn = int(PB1_LOG_DROP_REASONS_TOPN)
        self.debug = env_bool("DEBUG", False)
        self._universe_as_of = None
        self._universe_path: str | None = None
        self._universe_context = universe_context
        self._warned_keys: set[str] = set()
        self._balance_price_map: Dict[str, float] = {}
        self._balance_cost: float | None = None
        self._balance_snapshot: dict | None = balance_snapshot
        self._balance_snapshot_source: str | None = balance_source
        if balance_snapshot is not None:
            if balance_source == "api":
                self.balance_api_calls += 1
            elif balance_source is not None:
                self.balance_cache_hits += 1
        self._holdings_summary: Dict[str, Any] = {}
        self._code_name_map: Dict[str, str] = {}
        self._entry_eval_by_code: Dict[str, dict[str, Any]] = {}
        self._entry_evaluations: list[dict[str, Any]] = []
        self._exit_evaluations: list[dict[str, Any]] = []
        self._exit_summary_payload: dict[str, Any] = {}
        self._exit_holdings_meta: dict[str, Any] = {
            "source": "empty",
            "snapshot_ts": self._now_kst.isoformat(),
            "freshness": "empty",
            "reconstructed": False,
        }
        self._scanner_summary: dict[str, Any] = {}
        self._prep_summary_payload: dict[str, Any] = {}
        self.current_code: str | None = None
        self.top_candidates: list[dict[str, Any]] = []
        self.calc_allowed = bool(calc_allowed)  # ✅ 계산 게이트
        self.price_allowed = bool(price_allowed)  # ✅ 가격 게이트
        self.order_allowed = bool(order_allowed)  # ✅ 주문 게이트
        self.minervini_only = bool(minervini_only)  # ✅ MINERVINI_ONLY 모드
        self.entry_enabled = bool(order_allowed)  # 하위호환용
        self.entry_block_reason = entry_block_reason
        self.force_entry_window_override = bool(force_entry_window_override)
        self.session_recovery_continue = bool(session_recovery_continue)
        self.forced_trade_session = str(forced_trade_session or "").strip().lower()
        self.phase_guard_classification = str(phase_guard_classification or "").strip()
        self.preopen_max_new_positions = int(preopen_max_new_positions or 0)
        self.window_internal = self._resolve_window_internal()
        self.bootstrap_enabled = bool(self._bool_env("PB1_BOOTSTRAP_ENABLED", PB1_BOOTSTRAP_ENABLE))
        self.bootstrap_config = self._resolve_bootstrap_config()
        self.force_min1_enabled = bool(self._bool_env("PB1_FORCE_MIN1", self.bootstrap_config["force_min1_enabled"]))
        self.force_min1_topn = max(1, int(self._int_env("PB1_FORCE_MIN1_TOPN", int(self.bootstrap_config["force_min1_topn"]))))
        self.order_candidate_mode = (os.getenv("PB1_ORDER_CANDIDATE_MODE") or "normal").strip().lower() or "normal"
        logger.info(
            "[PB1][BOOTSTRAP][INIT] enabled=%s force_min1=%s force_min1_topn=%s phase=%s window=%s force_entry_window_override=%s session_recovery_continue=%s forced_trade_session=%s phase_guard_classification=%s",
            self.bootstrap_enabled,
            self.force_min1_enabled,
            self.force_min1_topn,
            self.phase_name,
            self.window_name,
            int(self.force_entry_window_override),
            int(self.session_recovery_continue),
            self.forced_trade_session or "none",
            self.phase_guard_classification or "none",
        )
        self.effective_entry_filters = self._resolve_effective_entry_filters()
        self.filter_thresholds = self._resolve_filter_thresholds()
        self.minervini_config.rs_min_percentile = float(self.effective_entry_filters["rs_min_pctile"]) / 100.0
        self.entry_capital_krw: float | None = None
        self.entry_usable_krw: float | None = None
        self.entry_tick_budget_krw: float | None = None
        self.entry_reserve_krw: float | None = None
        self.target_new_positions: int | None = None
        self._budget_plan_meta: dict[str, Any] | None = None
        self._touched_files: list[Path] = []
        self.order_possible_cash_krw: float = 0.0
        self._debug_score_cut_codes: list[str] = []
        self._debug_risk_ok_codes: list[str] = []
        self._debug_sizing_ok_codes: list[str] = []
        self._debug_sizing_fail_items: list[dict[str, Any]] = []
        self._debug_summary: dict[str, Any] = {}
        self._tick_price_cache: dict[tuple[str, str], dict[str, Any]] = {}
        self._tick_price_cache_hits = 0
        self._tick_price_api_calls = 0
        self._warning_counts: Counter[str] = Counter()
        self._tick_warning_counts: Counter[str] = Counter()
        self._tick_db_cache: dict[str, Any] = {}
        self._last_stage = "engine.init"
        os.environ["PB1_LAST_STAGE"] = self._last_stage

        logger.info(
            "[PB1][ASOF][USE] component=engine value=%s source=run_ctx",
            self.derived_as_of,
        )

    @contextlib.contextmanager
    def _stage_timer(self, stage_name: str):
        started = time.perf_counter()
        self._last_stage = stage_name
        os.environ["PB1_LAST_STAGE"] = stage_name
        logger.info("[PB1][STAGE][START] stage=%s", stage_name)
        try:
            yield
        except Exception as exc:
            elapsed = time.perf_counter() - started
            logger.warning(
                "[PB1][STAGE][EXCEPTION] stage=%s elapsed=%.2f err_type=%s err=%s",
                stage_name,
                elapsed,
                type(exc).__name__,
                exc,
            )
            raise
        finally:
            elapsed = time.perf_counter() - started
            logger.info("[PB1][STAGE][END] stage=%s elapsed=%.2f", stage_name, elapsed)

    @contextlib.contextmanager
    def _stage_timer_with_timeout(self, stage_name: str, max_sec: float):
        started = time.perf_counter()
        self._last_stage = stage_name
        os.environ["PB1_LAST_STAGE"] = stage_name
        logger.info("[PB1][STAGE][START] stage=%s max_sec=%.2f", stage_name, max_sec)
        error: Exception | None = None
        try:
            yield
        except Exception as exc:
            error = exc
            raise
        finally:
            elapsed = time.perf_counter() - started
            # [2026-04-30] tick 잔여 시간 업데이트 (non-critical DB skip 판단용)
            if max_sec > 0:
                remaining = max(0.0, max_sec - elapsed)
                self._tick_remaining_sec = remaining
                if remaining < int(os.getenv("PB1_SKIP_NONCRITICAL_DB_UPDATE_WHEN_REMAINING_SEC_LT", "5")):
                    logger.info(
                        "[STAGE_BUDGET][LOW] stage=%s remaining_sec=%.1f action=skip_noncritical_update",
                        stage_name, remaining,
                    )
            logger.info("[PB1][STAGE][END] stage=%s elapsed=%.2f", stage_name, elapsed)
            if error is None and max_sec > 0 and elapsed > max_sec:
                logger.warning("[WARN][PB1][STAGE_TIMEOUT] stage=%s elapsed=%.2f max_sec=%.2f", stage_name, elapsed, max_sec)
                raise PB1StageTimeout(f"{stage_name}:{elapsed:.2f}>{max_sec:.2f}")

    def _is_kis_kr_context(self) -> bool:
        """
        한국장 KIS context 판별.
        미국장/해외주식에서는 절대 true가 되면 안 된다.
        """
        env = str(getattr(self, "env", "") or "").lower()
        if env not in {"practice", "real", "live"}:
            return False

        phase = str(getattr(self, "phase", "") or "").lower()
        window_label = str(getattr(self, "window_label", "") or "").lower()

        if phase in {"trade", "entry", "exit"}:
            return True

        if window_label in {"am", "afternoon", "day", "morning"}:
            return True

        return False

    def _bump_warning(self, name: str, amount: int = 1) -> None:
        key = str(name or "").strip()
        if not key or amount <= 0:
            return
        self._warning_counts[key] += int(amount)
        self._tick_warning_counts[key] += int(amount)

    def _warning_counts_dict(self) -> dict[str, int]:
        return {key: int(value) for key, value in dict(self._warning_counts).items()}

    def _reset_tick_warning_counts(self) -> None:
        self._tick_warning_counts = Counter()
        self._tick_db_cache = {}
        logger.info("[PB1][TICK][CACHE_RESET] tick_id=%s", getattr(self, "_last_stage", "unknown"))

    def _emit_tick_warning_summary(self, *, code: str | None = None) -> None:
        counts = {key: int(value) for key, value in dict(self._tick_warning_counts).items()}
        logger.info(
            "[PB1][TICK][WARNINGS] code=%s timeout=%s db_fail_open=%s degraded=%s ledger_fail_first=%s duplicate_skip=%s details=%s",
            str(code or "tick"),
            counts.get("timeout_count", 0),
            counts.get("db_read_fail_open_count", 0),
            counts.get("degraded_stage_count", 0),
            counts.get("ledger_fail_first_count", 0),
            counts.get("duplicate_skip_count", 0),
            counts,
        )

    def _resolve_terminal_state(self, *, status: str, notes: str | None = None) -> str:
        normalized_status = str(status or "UNKNOWN").strip().upper()
        normalized_notes = str(notes or "").strip().lower()
        warning_counts = self._warning_counts_dict()
        warnings_total = sum(int(value) for value in warning_counts.values())
        if normalized_status in {"FATAL_RUNTIME", "FATAL_POSTPROCESS"}:
            return "SESSION_END_FATAL"
        if normalized_status.startswith("SKIP"):
            return "SESSION_END_SKIPPED"
        if normalized_status in {"OK_DEGRADED", "DEGRADED_POSTPROCESS"} or warning_counts.get("degraded_stage_count", 0) > 0:
            return "SESSION_END_OK_DEGRADED"
        if warnings_total > 0 or normalized_status in {"WARN_FAIL_OPEN", "OK_WITH_WARNINGS"} or "degraded" in normalized_notes:
            return "SESSION_END_OK_WITH_WARNINGS"
        return "SESSION_END_OK"

    def _finalize_run_result(self, *, status: str, notes: str | None) -> RunResult:
        self._emit_tick_warning_summary()
        warning_counts = self._warning_counts_dict()
        logger.info(
            "[PB1][SESSION][WARNINGS] timeout_count=%s db_read_fail_open_count=%s ledger_fail_first_count=%s degraded_stage_count=%s duplicate_skip_count=%s db_engine_dispose_count=%s",
            warning_counts.get("timeout_count", 0),
            warning_counts.get("db_read_fail_open_count", 0),
            warning_counts.get("ledger_fail_first_count", 0),
            warning_counts.get("degraded_stage_count", 0),
            warning_counts.get("duplicate_skip_count", 0),
            warning_counts.get("db_engine_dispose_count", 0),
        )
        return RunResult(
            status=status,
            notes=notes,
            balance_api_calls=self.balance_api_calls,
            balance_cache_hits=self.balance_cache_hits,
            balance_tick_cache_hits=self.balance_tick_cache_hits,
            terminal_state=self._resolve_terminal_state(status=status, notes=notes),
            warning_counts=warning_counts,
        )

    def _resolve_entry_identity_from_mapping(self, source: dict[str, Any] | None) -> dict[str, str]:
        payload = source if isinstance(source, dict) else {}
        entry_reason = self._normalize_entry_reason(
            payload.get("entry_reason")
            or payload.get("entry_style_selected")
            or payload.get("entry_signal")
        )
        raw_style = payload.get("entry_style_selected") or payload.get("entry_signal") or entry_reason
        entry_style_selected = self._normalize_entry_reason(raw_style)
        if entry_style_selected == "ENTRY_GENERIC":
            entry_style_selected = entry_reason
        entry_decision_family = str(payload.get("entry_decision_family") or entry_reason).strip().upper() or entry_reason
        _, exit_policy_family = self._resolve_exit_family(entry_reason, entry_style_selected)
        return {
            "entry_reason": entry_reason,
            "entry_style_selected": entry_style_selected,
            "entry_decision_family": entry_decision_family,
            "exit_policy_family": exit_policy_family,
        }

    def _resolve_entry_identity_for_candidate(self, cf: CandidateFeature) -> dict[str, str]:
        return self._resolve_entry_identity_from_mapping(getattr(cf, "features", {}) or {})

    # =========================================================================
    # [2026-05-18] KR 전용 adaptive entry filter 메서드들
    # 이 메서드들은 반드시 _is_kr_equity_context() == True일 때만 적용됨
    # =========================================================================

    def _is_kr_equity_context(self) -> bool:
        """한국장(KRX/KOSPI/KOSDAQ) PB1 context인지 판정한다.
        
        해외장(미국장 포함), crypto, 비국내시장에는 False를 반환하므로
        이 메서드가 False이면 PB1_KR_* 로직이 절대 적용되지 않는다.
        """
        env = str(getattr(self, "env", "") or "").lower()
        session_kind = str(getattr(self, "session_kind", "") or "").lower()
        window_name = str(getattr(self, "window_name", "") or "").lower()
        market_window = str(getattr(self, "market_window_name", "") or "").lower()
        final30_source = str(getattr(self, "final30_source", "") or "").lower()

        return (
            env in {"practice", "real", "live"}
            and session_kind in {"am", "pm", "afternoon", "close", ""}
            and window_name in {"morning", "afternoon", "close", "am", "pm", "day", ""}
            and market_window in {"morning", "afternoon", "close", "am", "pm", "day", ""}
            and (
                "pb1_watchlist_final_scored" in final30_source
                or "final30" in final30_source
                or final30_source in {"none", ""}
            )
        )

    @staticmethod
    def _classify_kr_reason_by_style(reason: str, entry_style: str) -> str:
        """한국장 전용 reason severity classifier.
        
        return: HARD | SOFT | IGNORE
        한국장에서만 사용. 기존 글로벌 로직에 영향 없음.
        """
        r = str(reason).lower()
        style = str(entry_style or "").upper()

        is_pullback = "PULLBACK" in style
        is_momentum = "MOMENTUM" in style
        is_breakout = "BREAKOUT" in style
        is_vcp = "VCP" in style or "MINERVINI" in style

        # 유동성/MA 필수 데이터 실패: 항상 HARD
        if r in {"missing_ma", "illiquid", "liquidity_fail", "price_scale_outlier"}:
            return "HARD"
        if r in {"spread_fail", "gap_fail", "range_fail"}:
            return "HARD"

        if r == "vol_contraction_fail":
            if is_pullback and not PB1_KR_PULLBACK_VOL_HARD_FAIL:
                return "SOFT"
            if is_momentum and PB1_KR_MOMENTUM_ALLOW_VOL_EXPANSION:
                return "IGNORE"
            if is_breakout and PB1_KR_BREAKOUT_ALLOW_VOL_EXPANSION:
                return "IGNORE"
            if is_vcp:
                return "HARD"
            return "SOFT"

        if r == "volu_contraction_fail":
            if is_pullback and not PB1_KR_PULLBACK_VOLU_HARD_FAIL:
                return "SOFT"
            if is_momentum and PB1_KR_MOMENTUM_ALLOW_VOL_EXPANSION:
                return "IGNORE"
            if is_breakout and PB1_KR_BREAKOUT_ALLOW_VOL_EXPANSION:
                return "IGNORE"
            if is_vcp:
                return "HARD"
            return "SOFT"

        if r in {"close_below_ma20", "ma20_slope_hard_fail"}:
            return "HARD"

        if r.startswith("soft:"):
            return "SOFT"

        return "SOFT"

    def _kr_adaptive_rank_candidates(
        self,
        candidates: list,
        *,
        topn: int | None = None,
    ) -> list:
        """한국장 전용 adaptive rank 후보 선정.
        
        PB1_KR_SCORE_MODE=ADAPTIVE_RANK일 때만 적용.
        글로벌 score 기준을 바꾸지 않음.
        """
        if not (self._is_kr_equity_context() and PB1_KR_SCORE_MODE == "ADAPTIVE_RANK"):
            return []

        usable = []
        for c in candidates:
            features = getattr(c, "features", {}) or {}
            hard_reasons = set(features.get("hard_reasons") or [])

            fatal = {
                "missing_ma",
                "illiquid",
                "liquidity_fail",
                "price_scale_outlier",
                "spread_fail",
                "gap_fail",
                "range_fail",
            }

            if hard_reasons & fatal:
                continue

            score = float(
                features.get("kr_adjusted_score")
                or features.get("score")
                or getattr(c, "score", 0.0)
                or 0.0
            )
            if score < PB1_KR_ADAPTIVE_SCORE_MIN_FLOOR:
                continue

            usable.append(c)

        ordered = sorted(
            usable,
            key=lambda c: (
                float(
                    (getattr(c, "features", {}) or {}).get("kr_adjusted_score")
                    or (getattr(c, "features", {}) or {}).get("score")
                    or getattr(c, "score", 0.0)
                    or 0.0
                ),
                float((getattr(c, "features", {}) or {}).get("rs_percentile") or 0.0),
                float((getattr(c, "features", {}) or {}).get("pullback_score") or 0.0),
                float((getattr(c, "features", {}) or {}).get("momentum_score") or 0.0),
            ),
            reverse=True,
        )

        n = int(topn or PB1_KR_ADAPTIVE_RANK_TOPN)
        return ordered[: max(1, n)]

    def _detect_kr_market_stress_from_filter_stats(
        self,
        *,
        scanned: int,
        reason_counts: dict,
        raw_signal_setup_ok: int = 0,
        scanner_passed: int = 0,
    ) -> tuple[bool, dict]:
        """한국장 급변동(stress) 상태 감지.
        
        _is_kr_equity_context() == False이면 항상 (False, {}) 반환.
        """
        if not (self._is_kr_equity_context() and PB1_KR_MARKET_STRESS_GUARD):
            return False, {}

        scanned = max(1, int(scanned or 0))

        vol_fail_ratio = float(reason_counts.get("vol_contraction_fail", 0)) / scanned
        ma20_fail_ratio = float(reason_counts.get("close_below_ma20", 0)) / scanned
        ma20_slope_fail_ratio = float(reason_counts.get("ma20_slope_hard_fail", 0)) / scanned

        stress = False
        reasons = []

        if vol_fail_ratio >= PB1_KR_STRESS_VOL_FAIL_RATIO:
            stress = True
            reasons.append("kr_vol_fail_ratio_high")

        if ma20_fail_ratio >= PB1_KR_STRESS_MA20_FAIL_RATIO:
            stress = True
            reasons.append("kr_ma20_fail_ratio_high")

        scanner_pb1_divergence = (
            raw_signal_setup_ok >= 10
            and scanner_passed >= 5
            and vol_fail_ratio >= PB1_KR_STRESS_VOL_FAIL_RATIO
        )
        if scanner_pb1_divergence:
            stress = True
            reasons.append("kr_scanner_pb1_divergence_under_high_vol")

        detail = {
            "vol_fail_ratio": vol_fail_ratio,
            "ma20_fail_ratio": ma20_fail_ratio,
            "ma20_slope_fail_ratio": ma20_slope_fail_ratio,
            "raw_signal_setup_ok": raw_signal_setup_ok,
            "scanner_passed": scanner_passed,
            "reasons": reasons,
        }

        logger.info(
            "[PB1][KR_MARKET_STRESS] stress=%s scanned=%s vol_fail_ratio=%.2f ma20_fail_ratio=%.2f raw_setup=%s scanner_passed=%s reasons=%s",
            int(stress),
            scanned,
            vol_fail_ratio,
            ma20_fail_ratio,
            raw_signal_setup_ok,
            scanner_passed,
            reasons,
        )

        return stress, detail

    def _build_kr_rescue_candidates(
        self,
        candidates: list,
        *,
        scanner_passed_codes: "set[str] | None" = None,
        minervini_buyable_codes: "set[str] | None" = None,
        market_stress: bool = False,
    ) -> list:
        """한국장 setup_ok=0일 때 scanner/minervini 후보를 rescue path로 넘긴다.
        
        반드시 기존 risk -> sizing -> buyable -> order_candidates 경로를 통과해야 한다.
        rescue 후보가 바로 주문되지 않는다.
        """
        if not (self._is_kr_equity_context() and PB1_KR_ENABLE_RESCUE_CANDIDATES):
            return []

        scanner_passed_codes = scanner_passed_codes or set()
        minervini_buyable_codes = minervini_buyable_codes or set()

        rescued = []

        for cf in candidates:
            features = getattr(cf, "features", {}) or {}
            code = getattr(cf, "code", None) or features.get("code")

            if not code:
                continue

            hard_reasons = set(features.get("hard_reasons") or [])

            fatal = {
                "missing_ma",
                "illiquid",
                "liquidity_fail",
                "price_scale_outlier",
                "spread_fail",
                "gap_fail",
                "range_fail",
            }

            if hard_reasons & fatal:
                continue

            # 한국장 급변동 중 MA20 아래 + 기울기 실패 조합은 rescue 금지
            if "close_below_ma20" in hard_reasons and "ma20_slope_hard_fail" in hard_reasons:
                continue

            source_ok = False
            source_tags = []

            if PB1_KR_RESCUE_SOURCE in {"SCANNER", "SCANNER_OR_MINERVINI"} and code in scanner_passed_codes:
                source_ok = True
                source_tags.append("scanner")

            if PB1_KR_RESCUE_SOURCE in {"MINERVINI", "SCANNER_OR_MINERVINI"} and code in minervini_buyable_codes:
                source_ok = True
                source_tags.append("minervini_relax")

            if not source_ok:
                continue

            rs = float(
                features.get("rs_percentile")
                or features.get("rs_pctile")
                or 0.0
            )

            # stress 모드에서는 강한 RS만 허용
            if market_stress and PB1_KR_STRESS_REQUIRE_STRONG_RS and rs < PB1_KR_STRESS_MIN_RS_PCTILE * 100:
                continue

            clone = self._clone_candidate(cf) if hasattr(self, "_clone_candidate") else cf
            clone.setup_ok = True
            clone.reasons = []

            clone.features["kr_rescue_candidate"] = True
            clone.features["kr_rescue_source"] = source_tags
            clone.features["kr_rescue_reason"] = "kr_setup_zero_but_scanner_or_minervini_positive"

            rescued.append(clone)

        ordered = sorted(
            rescued,
            key=lambda c: (
                float(
                    (getattr(c, "features", {}) or {}).get("kr_adjusted_score")
                    or (getattr(c, "features", {}) or {}).get("score")
                    or getattr(c, "score", 0.0)
                    or 0.0
                ),
                float((getattr(c, "features", {}) or {}).get("rs_percentile") or 0.0),
                float((getattr(c, "features", {}) or {}).get("pullback_score") or 0.0),
            ),
            reverse=True,
        )

        topn = max(1, int(PB1_KR_RESCUE_TOPN))

        # stress 모드에서는 최대 1개
        if market_stress and PB1_KR_MARKET_STRESS_GUARD:
            topn = min(topn, PB1_KR_STRESS_MAX_NEW_POSITIONS)

        selected = ordered[:topn]

        if PB1_KR_LOG_RESCUE_DECISION:
            logger.info(
                "[PB1][KR_RESCUE][RESULT] enabled=1 market_stress=%s source=%s selected=%s codes=%s",
                int(bool(market_stress)),
                PB1_KR_RESCUE_SOURCE,
                len(selected),
                [getattr(c, "code", None) for c in selected],
            )

        return selected

    def _stage_timeout_sec(self, env_name: str, default: float) -> float:
        raw = os.getenv(env_name)
        if raw is None:
            return float(default)
        try:
            return max(1.0, float(raw))
        except Exception:
            return float(default)

    def _exit_pass_timeout_sec(self) -> float:
        default = 300.0
        return self._stage_timeout_sec("PB1_EXIT_PASS_TIMEOUT_SEC", default)

    def _exit_pass_timeout_fail_open_enabled(self) -> bool:
        raw = os.getenv("PB1_EXIT_PASS_TIMEOUT_FAIL_OPEN")
        if raw is not None:
            return str(raw).strip().lower() in {"1", "true", "yes", "on"}
        return str(self.env or "").strip().lower() == "practice"

    def _prewarm_exit_holdings_data(self, holdings: list[dict]) -> None:
        """exit_pass 직전 보유종목 OHLCV/현재가 캐시를 미리 채운다."""
        enabled = str(os.getenv("PB1_EXIT_PREWARM_ENABLED", "1")).lower() in {"1", "true", "yes", "on"}
        if not enabled:
            logger.info("[PB1][EXIT_PREWARM] skipped reason=disabled")
            return

        codes = []
        for h in holdings or []:
            code = str(h.get("code") or h.get("pdno") or "").zfill(6)
            if code and code != "000000":
                codes.append(code)
        codes = sorted(set(codes))

        if not codes:
            logger.info("[PB1][EXIT_PREWARM] skipped reason=no_holdings")
            return

        session_kind = str(os.getenv("PB1_SESSION_KIND") or "").strip().lower()
        logger.info("[PB1][EXIT_PREWARM][START] session=%s count=%s codes=%s", session_kind, len(codes), codes)

        ok = 0
        failed = 0
        for code in codes:
            try:
                self._fetch_exit_ohlcv(code)
                self._get_price_snapshot_cached(code)
                ok += 1
            except Exception as exc:
                failed += 1
                logger.warning(
                    "[PB1][EXIT_PREWARM][FAIL] session=%s code=%s err_type=%s err=%s",
                    session_kind,
                    code,
                    type(exc).__name__,
                    exc,
                )

        logger.info(
            "[PB1][EXIT_PREWARM][DONE] session=%s count=%s ok=%s failed=%s",
            session_kind,
            len(codes),
            ok,
            failed,
        )

    def _consume_order_lookup_fail_open(self, op_name: str) -> bool:
        consume = getattr(self.orders_repo, "consume_fail_open_marker", None)
        if callable(consume):
            return bool(consume(op_name))
        marker = getattr(self.orders_repo, "_last_read_fail_open_op", None)
        if marker != op_name:
            return False
        setattr(self.orders_repo, "_last_read_fail_open_op", None)
        return True

    def _consume_fill_lookup_fail_open(self, op_name: str) -> bool:
        consume = getattr(self.fills_repo, "consume_fail_open_marker", None)
        if callable(consume):
            return bool(consume(op_name))
        marker = getattr(self.fills_repo, "_last_read_fail_open_op", None)
        if marker != op_name:
            return False
        setattr(self.fills_repo, "_last_read_fail_open_op", None)
        return True

    def _safe_get_open_orders(self) -> list[dict]:
        cached = self._tick_db_cache.get("open_orders")
        if cached is not None:
            return [dict(row) for row in cached]
        logger.info("[PB1][STAGE][START] stage=orders.lookup_open")
        rows = self.orders_repo.get_open_orders(self.env)
        if self._consume_order_lookup_fail_open("orders.get_open_orders"):
            self._bump_warning("db_read_fail_open_count")
            logger.warning("[FAIL_OPEN][PB1][ORDER_LOOKUP] op=open_orders")
        self._tick_db_cache["open_orders"] = [dict(row) for row in rows]
        logger.info("[PB1][STAGE][END] stage=orders.lookup_open rows=%s", len(rows))
        return rows

    def _get_sold_codes_today(self) -> set[str]:
        """
        오늘 매도된 종목 코드 리스트 추출 (일반화).
        
        Returns:
            오늘 매도된 종목 코드 set
        """
        sold_codes = set()
        
        # 1. 오늘 SELL fill에서 추출
        try:
            sell_fills = self._safe_list_today_fills(side="SELL")
            for row in sell_fills:
                code = str(row.get("code") or "").zfill(6)
                if code and code != "000000":
                    sold_codes.add(code)
        except Exception as exc:
            logger.warning("[SOLD_CODES][FILLS_LOOKUP_FAIL] err=%s", exc)
        
        # 2. 오늘 SELL accepted/submitted order에서 추출
        try:
            sell_orders = self._safe_list_today_orders(side="SELL")
            for row in sell_orders:
                status = str(row.get("status") or "").upper()
                if status in {"SUBMITTED", "ACCEPTED", "FILLED", "PARTIAL_FILLED"}:
                    code = str(row.get("code") or "").zfill(6)
                    if code and code != "000000":
                        sold_codes.add(code)
        except Exception as exc:
            logger.warning("[SOLD_CODES][ORDERS_LOOKUP_FAIL] err=%s", exc)
        
        # 3. 현재 tick의 exit summary에서 추출
        payload = dict(getattr(self, "_exit_summary_payload", {}) or {})
        exit_evaluations = getattr(self, "_exit_evaluations", []) or []
        for evaluation in exit_evaluations:
            if int(evaluation.get("submitted") or 0) > 0:
                code = str(evaluation.get("code") or "").zfill(6)
                if code and code != "000000":
                    sold_codes.add(code)
        
        return sold_codes

    def _safe_list_today_orders(self, *, code: str | None = None, side: str | None = None) -> list[dict]:
        cache_key = ("today_orders", str(code or ""), str(side or ""))
        cached = self._tick_db_cache.get(cache_key)
        if cached is not None:
            return [dict(row) for row in cached]
        logger.info("[PB1][STAGE][START] stage=orders.lookup_today code=%s side=%s", code, side)
        rows = self.orders_repo.list_today_orders(self.env, code=code, side=side)
        if self._consume_order_lookup_fail_open("orders.list_today_orders"):
            self._bump_warning("db_read_fail_open_count")
            logger.warning("[FAIL_OPEN][PB1][ORDER_LOOKUP] op=today_orders")
        self._tick_db_cache[cache_key] = [dict(row) for row in rows]
        logger.info("[PB1][STAGE][END] stage=orders.lookup_today rows=%s code=%s side=%s", len(rows), code, side)
        return rows

    def _safe_has_blocking_order_today(
        self,
        *,
        code: str,
        side: str,
        stage: str | None = None,
        trade_date: str | None = None,
    ) -> tuple[bool, dict | None]:
        cache_key = ("blocking_order", str(code or "").zfill(6), str(side or ""), str(stage or ""), str(trade_date or ""))
        cached = self._tick_db_cache.get(cache_key)
        if cached is not None:
            return cached
        return self.orders_repo.has_blocking_order_today(
            env=self.env,
            code=code,
            side=side,
            stage=stage,
            trade_date=trade_date,
        )

    def _safe_list_today_fills(self, *, code: str | None = None, side: str | None = None) -> list[dict]:
        cache_key = ("today_fills", str(code or ""), str(side or ""))
        cached = self._tick_db_cache.get(cache_key)
        if cached is not None:
            logger.info("[PB1][DB_CACHE][HIT] key=%s", cache_key)
            return [dict(row) for row in cached]
        with self._stage_timer("entry.today_buy_fills_bulk_lookup"):
            try:
                rows = self.fills_repo.list_today_fills(self.env, code=code, side=side)
                if self._consume_fill_lookup_fail_open("fills.list_today_fills"):
                    self._bump_warning("db_read_fail_open_count")
                    logger.warning("[FAIL_OPEN][PB1][ORDER_LOOKUP] op=today_fills code=%s side=%s", code, side)
            except Exception as exc:
                self._bump_warning("db_read_fail_open_count")
                dispose_engine_safely(self.engine, reason=f"fills.list_today_fills:{type(exc).__name__}")
                self._bump_warning("db_engine_dispose_count")
                logger.warning(
                    "[PB1][DB_LOOKUP][FAIL_OPEN] op=fills.list_today_fills code=%s side=%s err_type=%s err=%s",
                    code,
                    side,
                    type(exc).__name__,
                    exc,
                )
                rows = []
        self._tick_db_cache[cache_key] = [dict(row) for row in rows]
        return rows

    def _resolve_buy_cooldown_state(
        self,
        *,
        code: str,
        cooldown_until,
        holding_qty: int = 0,
        today_buy_exists: bool,
        today_fill_exists: bool,
        cooldown_source_events_count: int,
        last_fill_event_at,
        cooldown_source: str | None = None,
        recent_valid_exit_event: bool = False,
        recent_exit_reason: str | None = None,
    ) -> dict:
        cooldown_active = False
        stale_ignored = False
        final_cooldown_policy = "none"
        resolved_source = str(cooldown_source or "none")
        risk_off_same_day_only = str(recent_exit_reason or "").strip().upper() in {"EXIT_RISK_OFF", "EXIT_SOFT_RISK_OFF", "BUG_RECOVERY_EXIT"}

        cooldown_until_value = str(cooldown_until).strip() if cooldown_until is not None else ""
        if cooldown_until_value and cooldown_until_value >= self._today:
            if bool(today_buy_exists):
                cooldown_active = True
                final_cooldown_policy = "same_day_only"
                resolved_source = "same_day_duplicate_prevention"
            elif bool(recent_valid_exit_event) and not risk_off_same_day_only:
                cooldown_active = True
                final_cooldown_policy = "multi_day"
                if resolved_source == "none":
                    resolved_source = "completed_trade_cooldown"
            elif int(holding_qty or 0) <= 0 and not bool(today_fill_exists):
                stale_ignored = True
                final_cooldown_policy = "stale_ignored"
                if resolved_source == "none":
                    resolved_source = "stale_residue"
            elif bool(today_fill_exists) or risk_off_same_day_only:
                final_cooldown_policy = "same_day_only"
                resolved_source = "same_day_duplicate_prevention"
            else:
                cooldown_active = int(cooldown_source_events_count or 0) > 0 or bool(last_fill_event_at)
                final_cooldown_policy = "multi_day" if cooldown_active else "none"

        logger.info(
            "[PB1][BUYABLE_GATE][COOLDOWN_SRC] code=%s source=%s active=%s stale=%s today_buy_exists=%s today_fill_exists=%s last_fill_event_at=%s cooldown_until=%s",
            code,
            resolved_source,
            int(cooldown_active),
            int(stale_ignored),
            int(bool(today_buy_exists)),
            int(bool(today_fill_exists)),
            last_fill_event_at,
            cooldown_until_value or None,
        )
        if stale_ignored:
            logger.warning(
                "[PB1][BUYABLE_GATE][STALE_COOLDOWN] code=%s cooldown_until=%s ignored=1 reason=no_fill_evidence",
                code,
                cooldown_until_value or None,
            )

        return {
            "cooldown_active": cooldown_active,
            "stale_ignored": stale_ignored,
            "cooldown_source": resolved_source,
            "final_cooldown_policy": final_cooldown_policy,
        }

    def _get_price_snapshot_cached(self, code: str, market: str = "J") -> dict:
        norm_code = str(code).zfill(6)
        key = (norm_code, market)

        if key in self._tick_price_cache:
            self._tick_price_cache_hits += 1
            logger.info("[PB1][PRICE_CACHE][HIT] code=%s market=%s", norm_code, market)
            return self._tick_price_cache[key]

        logger.info("[PB1][PRICE_CACHE][MISS] code=%s market=%s", norm_code, market)

        snapshot = {}
        if getattr(self, "kis", None):
            snapshot = self.kis.get_price_snapshot(norm_code, market=market) or {}
        elif getattr(self, "kis_api", None):
            snapshot = self.kis_api.get_price_snapshot(norm_code, market=market) or {}

        self._tick_price_api_calls += 1
        self._tick_price_cache[key] = snapshot
        logger.info("[PB1][PRICE_CACHE][STORE] code=%s market=%s ok=%s", norm_code, market, int(bool(snapshot)))
        return snapshot

    def _log_tick_price_cache_summary(self) -> None:
        logger.info(
            "[PB1][PRICE_CACHE][SUMMARY] api_calls=%s cache_hits=%s keys=%s",
            self._tick_price_api_calls,
            self._tick_price_cache_hits,
            len(self._tick_price_cache),
        )

    def _init_run_context_state(
        self,
        *,
        as_of: str | None,
        trade_date: str | date | None,
        run_ctx: Any,
        derived_as_of: str | None,
    ) -> None:
        self._run_ctx = run_ctx
        run_ctx_as_of = None
        if isinstance(run_ctx, dict):
            run_ctx_as_of = run_ctx.get("derived_as_of") or run_ctx.get("as_of")
        else:
            run_ctx_as_of = getattr(run_ctx, "derived_as_of", None) or getattr(run_ctx, "as_of", None)
        resolved_as_of = str(as_of or run_ctx_as_of or derived_as_of or self._today)
        resolved_trade_date = trade_date
        if resolved_trade_date is None:
            if isinstance(run_ctx, dict):
                resolved_trade_date = run_ctx.get("trade_date")
            else:
                resolved_trade_date = getattr(run_ctx, "trade_date", None)
        self._as_of = resolved_as_of
        self._trade_date = str(resolved_trade_date or self._today)
        if as_of:
            self._as_of_source = "explicit_as_of"
        elif run_ctx_as_of:
            self._as_of_source = "run_ctx"
        elif derived_as_of:
            self._as_of_source = "derived_as_of"
        else:
            self._as_of_source = "fallback_resolver"
        logger.info(
            "[PB1][ASOF][INIT] as_of=%s trade_date=%s source=%s",
            self._as_of,
            self._trade_date,
            self._as_of_source,
        )

    def get_as_of(self) -> str:
        if self._as_of:
            return str(self._as_of)
        run_ctx_as_of = None
        if isinstance(self._run_ctx, dict):
            run_ctx_as_of = self._run_ctx.get("derived_as_of") or self._run_ctx.get("as_of")
        elif self._run_ctx is not None:
            run_ctx_as_of = getattr(self._run_ctx, "derived_as_of", None) or getattr(self._run_ctx, "as_of", None)
        backfill_value = str(run_ctx_as_of or getattr(self, "derived_as_of", None) or self._today or "")
        if backfill_value:
            self._as_of = backfill_value
            if not self._trade_date:
                self._trade_date = str(self._today)
            if not self._as_of_source:
                self._as_of_source = "backfill"
            logger.warning(
                "[PB1][ASOF][BACKFILL] as_of=%s trade_date=%s source=%s",
                self._as_of,
                self._trade_date,
                self._as_of_source,
            )
            return str(self._as_of)
        raise RuntimeError("engine_as_of_missing")

    def _resolve_window_internal(self) -> str:
        internal = compute_window(self._now_kst)
        normalized = (self.window_label or "").strip().lower()
        label_map = {"preopen": "morning", "morning": "morning", "day": "day", "close": "close"}
        if normalized in label_map:
            forced = label_map[normalized]
            if internal != forced:
                self._warn_once(
                    "window_mismatch",
                    "[PB1][WINDOW][WARN] market_window=%s mismatch window=%s -> forcing %s",
                    normalized,
                    internal,
                    forced,
                )
            return forced
        return internal

    @staticmethod
    def _float_env(name: str, default: float) -> float:
        raw = os.getenv(name)
        if raw is None:
            return default
        try:
            return float(raw)
        except ValueError:
            logger.warning("[PB1][ENV] invalid %s=%s fallback=%s", name, raw, default)
            return default

    @staticmethod
    def _int_env(name: str, default: int) -> int:
        raw = os.getenv(name)
        if raw is None:
            return default
        try:
            return int(raw)
        except ValueError:
            logger.warning("[PB1][ENV] invalid %s=%s fallback=%s", name, raw, default)
            return default

    @staticmethod
    def _bool_env(name: str, default: bool) -> bool:
        raw = os.getenv(name)
        if raw is None:
            return bool(default)
        return parse_bool_any(raw, default=bool(default))

    def _resolve_bootstrap_config(self) -> dict[str, Any]:
        return {
            "rs_min_pctile": float(self._float_env("PB1_BOOTSTRAP_RS_MIN", BOOTSTRAP_MINERVINI_RS_MIN_PCTILE)),
            "vcp_min_score": float(self._float_env("PB1_BOOTSTRAP_VCP_MIN", BOOTSTRAP_MINERVINI_VCP_MIN_SCORE)),
            "relax_passes": int(self._int_env("PB1_BOOTSTRAP_RELAX_PASSES", int(BOOTSTRAP_RELAX_PASSES))),
            "vol_max": float(self._float_env("PB1_BOOTSTRAP_VOL_MAX", BOOTSTRAP_PB1_VOL_MAX)),
            "volu_max": float(self._float_env("PB1_BOOTSTRAP_VOLU_MAX", BOOTSTRAP_PB1_VOLU_MAX)),
            "pullback_min": float(self._float_env("PB1_BOOTSTRAP_PULLBACK_MIN", BOOTSTRAP_PB1_PULLBACK_MIN)),
            "pullback_max": float(self._float_env("PB1_BOOTSTRAP_PULLBACK_MAX", BOOTSTRAP_PB1_PULLBACK_MAX)),
            "require_both_contractions": bool(
                self._bool_env("PB1_BOOTSTRAP_REQUIRE_BOTH", BOOTSTRAP_PB1_REQUIRE_BOTH_CONTRACTIONS)
            ),
            "min_score_base": float(self._float_env("PB1_BOOTSTRAP_MIN_SCORE_BASE", BOOTSTRAP_PB1_MIN_SCORE_BASE)),
            "min_score_floor": float(self._float_env("PB1_BOOTSTRAP_MIN_SCORE_FLOOR", BOOTSTRAP_PB1_MIN_SCORE_FLOOR)),
            "min_score_step": float(self._float_env("PB1_BOOTSTRAP_MIN_SCORE_STEP", BOOTSTRAP_PB1_MIN_SCORE_STEP)),
            "score_keep_topn": int(self._int_env("PB1_SCORE_KEEP_TOPN", int(BOOTSTRAP_SCORE_CUT_KEEP_TOPN))),
            "force_min1_enabled": bool(self._bool_env("PB1_FORCE_MIN1", BOOTSTRAP_FORCE_MIN_1_SHARE)),
            "force_min1_topn": int(self._int_env("PB1_FORCE_MIN1_TOPN", int(BOOTSTRAP_MIN1_TOPN))),
            "min_buyable": max(1, int(self._int_env("PB1_MIN_BUYABLE", self._int_env("MIN_BUYABLE", PB1_MIN_BUYABLE)))),
        }

    def _resolve_effective_entry_filters(self) -> dict[str, Any]:
        def _first_float(names: Iterable[str], default: float) -> tuple[float, str]:
            for name in names:
                raw = os.getenv(name)
                if raw is None or str(raw).strip() == "":
                    continue
                try:
                    return float(str(raw).strip()), name
                except Exception:
                    logger.warning("[PB1][THRESHOLDS][ENV_INVALID] name=%s value=%s default=%s", name, raw, default)
                    break
            return float(default), "default"

        defaults = {
            "rs_min_pctile": float(RS_MIN_PCTILE),
            "vcp_min_score": float(VCP_MIN_SCORE),
            "vol_max": float(PB1_VOL_MAX),
            "volu_max": float(PB1_VOLU_MAX),
            "volu_max_intraday": float(PB1_VOLU_MAX_INTRADAY),
            "pullback_min": float(PB1_PULLBACK_MIN),
            "pullback_max": float(PB1_PULLBACK_MAX),
            "require_both_contractions": bool(PB1_REQUIRE_BOTH_CONTRACTIONS),
            "min_score_base": float(PB1_MIN_SCORE_BASE),
            "min_score_floor": float(PB1_MIN_SCORE_FLOOR),
            "min_score_step": float(PB1_MIN_SCORE_STEP),
            "relax_passes": int(PB1_RELAX_MAX_PASSES),
            "min_buyable": max(1, int(self._int_env("PB1_MIN_BUYABLE", self._int_env("MIN_BUYABLE", PB1_MIN_BUYABLE)))),
            "score_keep_topn": max(1, int(self._int_env("PB1_SCORE_KEEP_TOPN", int(BOOTSTRAP_SCORE_CUT_KEEP_TOPN)))),
        }
        effective = dict(defaults)
        if self.bootstrap_enabled and self.phase in {"entry", "pm_entry"}:
            # Profile defaults are deliberately applied before explicit env overrides.
            # A profile must never silently replace PB1_VOL_MAX/PB1_VOLU_MAX=1.25 with 1.15.
            effective.update(
                {
                    "rs_min_pctile": 60.0,
                    "vcp_min_score": 45.0,
                    "vol_max": 1.15,
                    "volu_max": 1.15,
                    "volu_max_intraday": 1.15,
                    "pullback_min": 0.02,
                    "pullback_max": 0.25,
                    "require_both_contractions": False,
                    "min_score_base": 55.0,
                    "min_score_floor": 45.0,
                    "min_score_step": 5.0,
                    "relax_passes": 5,
                    "min_buyable": 1,
                    "score_keep_topn": 3,
                }
            )

        vol_max, vol_src = _first_float(
            ("PB1_ENTRY_VOL_MAX", "PB1_VOL_MAX", "PB1_KR_VOL_MAX", "PB1_BOOTSTRAP_VOL_MAX", "BOOTSTRAP_PB1_VOL_MAX"),
            effective["vol_max"],
        )
        volu_max, volu_src = _first_float(
            ("PB1_ENTRY_VOLU_MAX", "PB1_VOLU_MAX", "PB1_KR_VOLU_MAX", "PB1_BOOTSTRAP_VOLU_MAX", "BOOTSTRAP_PB1_VOLU_MAX"),
            effective["volu_max"],
        )
        volu_intraday, volu_intraday_src = _first_float(
            ("PB1_VOLU_MAX_INTRADAY", "PB1_ENTRY_VOLU_MAX", "PB1_VOLU_MAX", "PB1_KR_VOLU_MAX_INTRADAY", "PB1_BOOTSTRAP_VOLU_MAX_INTRADAY", "PB1_BOOTSTRAP_VOLU_MAX"),
            effective["volu_max_intraday"],
        )
        effective.update(
            {
                "rs_min_pctile": float(self._float_env("PB1_BOOTSTRAP_RS_MIN", effective["rs_min_pctile"])),
                "vcp_min_score": float(self._float_env("PB1_BOOTSTRAP_VCP_MIN", effective["vcp_min_score"])),
                "vol_max": vol_max,
                "volu_max": volu_max,
                "volu_max_intraday": volu_intraday,
                "pullback_min": float(self._float_env("PB1_BOOTSTRAP_PULLBACK_MIN", effective["pullback_min"])),
                "pullback_max": float(self._float_env("PB1_BOOTSTRAP_PULLBACK_MAX", effective["pullback_max"])),
                "require_both_contractions": bool(self._bool_env("PB1_BOOTSTRAP_REQUIRE_BOTH", effective["require_both_contractions"])),
                "min_score_base": float(self._float_env("PB1_BOOTSTRAP_MIN_SCORE_BASE", effective["min_score_base"])),
                "min_score_floor": float(self._float_env("PB1_BOOTSTRAP_MIN_SCORE_FLOOR", effective["min_score_floor"])),
                "min_score_step": float(self._float_env("PB1_BOOTSTRAP_MIN_SCORE_STEP", effective["min_score_step"])),
                "relax_passes": max(1, int(self._int_env("PB1_BOOTSTRAP_RELAX_PASSES", effective["relax_passes"]))),
                "min_buyable": max(1, int(self._int_env("PB1_MIN_BUYABLE", self._int_env("MIN_BUYABLE", effective["min_buyable"])))),
                "score_keep_topn": max(1, int(self._int_env("PB1_SCORE_KEEP_TOPN", effective["score_keep_topn"]))),
            }
        )
        logger.info(
            "[PB1][THRESHOLDS][FINAL] phase=%s window=%s vol_max=%.2f volu_max=%.2f volu_max_intraday=%.2f source=env_overrides_applied vol_src=%s volu_src=%s volu_intraday_src=%s",
            self.phase_name,
            self.window_name,
            effective["vol_max"],
            effective["volu_max"],
            effective["volu_max_intraday"],
            vol_src,
            volu_src,
            volu_intraday_src,
        )
        logger.info(
            "[PB1][EFFECTIVE_FILTERS] phase=%s window=%s filters=%s",
            self.phase_name,
            self.window_name,
            effective,
        )
        return effective

    def _record_simulated_order(
        self,
        code: str,
        side: str,
        qty: int,
        limit_price: float,
        reason: str,
        mode: str,
    ) -> None:
        """DIAG 모드에서 시뮬레이션 주문 DB 기록."""
        try:
            with self.engine.begin() as conn:
                from trader.db.schema import ORDERS
                conn.execute(
                    sa.insert(ORDERS).values(
                        run_id=self.run_id,
                        code=code,
                        side=side,
                        qty=qty,
                        limit_price=limit_price,
                        status="SIMULATED",
                        reason=reason,
                        mode=mode,
                        created_at=sa.func.now(),
                    )
                )
                logger.info("[SIM_ORDER][DB_OK] code=%s side=%s qty=%s limit=%.0f", code, side, qty, limit_price)
        except Exception as exc:
            logger.warning("[SIM_ORDER][DB_FAIL] code=%s err=%s", code, exc)

    def _resolve_entry_cutoff(self) -> tuple[datetime, str]:
        raw = (os.getenv("ENTRY_CUTOFF_TIME") or PB1_ENTRY_WINDOW_END or "").strip()
        if not raw:
            raw = "15:15"
        try:
            cutoff_time = datetime.strptime(raw, "%H:%M").time()
        except ValueError:
            logger.warning("[PB1][ENV] invalid ENTRY_CUTOFF_TIME=%s fallback=%s", raw, PB1_ENTRY_WINDOW_END)
            cutoff_time = datetime.strptime(PB1_ENTRY_WINDOW_END, "%H:%M").time()
            raw = PB1_ENTRY_WINDOW_END
        cutoff = datetime.combine(self._now_kst.date(), cutoff_time, tzinfo=self._now_kst.tzinfo)
        return cutoff, raw

    def _resolve_market_close(self) -> tuple[datetime, str]:
        fallback_raw = "15:30"
        source = "default"
        raw = ""
        try:
            market_close_env = str(os.getenv("MARKET_CLOSE_TIME") or "").strip()
            close_auction_env = str(os.getenv("CLOSE_AUCTION_END") or "").strip()
            if market_close_env:
                raw = market_close_env
                source = "MARKET_CLOSE_TIME"
            elif close_auction_env:
                raw = close_auction_env
                source = "CLOSE_AUCTION_END"
            else:
                raw = fallback_raw
            try:
                close_time = datetime.strptime(raw, "%H:%M").time()
            except ValueError:
                logger.warning(
                    "[PB1][MARKET_CLOSE][INVALID] raw=%s source=%s fallback=%s",
                    raw,
                    source,
                    fallback_raw,
                )
                raw = fallback_raw
                source = "default"
                close_time = datetime.strptime(fallback_raw, "%H:%M").time()
            close_dt = datetime.combine(self._now_kst.date(), close_time, tzinfo=self._now_kst.tzinfo)
            logger.info(
                "[PB1][MARKET_CLOSE][RESOLVE] raw=%s source=%s close=%s",
                raw,
                source,
                close_dt.isoformat(),
            )
            return close_dt, raw
        except Exception as exc:
            close_time = datetime.strptime(fallback_raw, "%H:%M").time()
            close_dt = datetime.combine(self._now_kst.date(), close_time, tzinfo=self._now_kst.tzinfo)
            logger.warning(
                "[PB1][MARKET_CLOSE][RESOLVE_FAIL] raw=%s source=%s err=%s fallback=%s",
                raw,
                source,
                exc,
                fallback_raw,
            )
            logger.info(
                "[PB1][MARKET_CLOSE][RESOLVE] raw=%s source=%s close=%s",
                fallback_raw,
                "default",
                close_dt.isoformat(),
            )
            return close_dt, fallback_raw

    def _is_buy_allowed_now(self, now: datetime | None = None) -> tuple[bool, str, datetime, datetime]:
        current_now = now or now_kst()
        entry_cutoff_dt, _ = self._resolve_entry_cutoff()
        market_close_dt, _ = self._resolve_market_close()
        if current_now >= market_close_dt:
            return False, "MARKET_CLOSED", entry_cutoff_dt, market_close_dt
        if current_now >= entry_cutoff_dt:
            return False, "ENTRY_CUTOFF_PASSED", entry_cutoff_dt, market_close_dt
        return True, "TIME_WINDOW_OK", entry_cutoff_dt, market_close_dt

    def _should_block_entry_after_exit(self) -> tuple[bool, dict]:
        """SELL 제출 이후 신규 BUY를 차단할지 판단한다.

        PB1_BLOCK_ENTRY_AFTER_EXIT=1 (default 0)이고 이번 tick에 SELL 제출이 있었을 때만 True.
        """
        enabled = str(os.getenv("PB1_BLOCK_ENTRY_AFTER_EXIT", "0")).strip() == "1"
        if not enabled:
            return False, {"exit_submit_attempt_count": 0, "accepted_sell_count": 0}
        payload = dict(getattr(self, "_exit_summary_payload", {}) or {})
        submit_attempt_count = int(payload.get("submit_attempt_count") or 0)
        accepted_sell_count = int(payload.get("accepted_sell_count") or 0)
        metrics = {
            "exit_submit_attempt_count": submit_attempt_count,
            "accepted_sell_count": accepted_sell_count,
        }
        blocked = submit_attempt_count > 0 or accepted_sell_count > 0
        return blocked, metrics

    def _warn_once(self, key: str, message: str, *args: object) -> None:
        if key in self._warned_keys:
            return
        self._warned_keys.add(key)
        logger.warning(message, *args)

    def _is_intraday_threshold_window(self) -> bool:
        if self.phase not in {"prep", "entry", "pm_entry"}:
            return False
        if self.window_internal not in {"morning", "day"}:
            return False
        try:
            entry_end = datetime.strptime(PB1_ENTRY_WINDOW_END, "%H:%M").time()
            return self._now_kst.time() <= entry_end
        except ValueError:
            return False

    def _resolve_filter_thresholds(self) -> FilterThresholds:
        intraday = self._is_intraday_threshold_window()
        filters = getattr(self, "effective_entry_filters", {}) or {}
        volu_max = float(filters.get("volu_max_intraday" if intraday else "volu_max", PB1_VOLU_MAX_INTRADAY if intraday else PB1_VOLU_MAX))
        thresholds = FilterThresholds(
            vol_contraction_max=float(filters.get("vol_max", PB1_VOL_MAX)),
            volu_contraction_max=volu_max,
            pullback_min=float(filters.get("pullback_min", PB1_PULLBACK_MIN)),
            pullback_max=float(filters.get("pullback_max", PB1_PULLBACK_MAX)),
            require_both_contractions=bool(filters.get("require_both_contractions", PB1_REQUIRE_BOTH_CONTRACTIONS)),
        )
        logger.info(
            "[PB1][THRESHOLDS] intraday=%s phase=%s window=%s thresholds={vol_max:%.2f volu_max:%.2f pullback_min:%.3f pullback_max:%.3f require_both:%s}",
            int(intraday),
            self.phase,
            self.window_internal,
            thresholds.vol_contraction_max,
            thresholds.volu_contraction_max,
            thresholds.pullback_min,
            thresholds.pullback_max,
            thresholds.require_both_contractions,
        )
        return thresholds

    def _resolve_strict_thresholds(self) -> FilterThresholds:
        return self.filter_thresholds

    def _record_setup_reasons(self, reasons: Iterable[str], code: str | None = None) -> None:
        for reason in reasons:
            if not reason:
                continue
            self._setup_reason_counter[reason] += 1
            if reason not in self.reject_reason_counts:
                self.reject_reason_counts[reason] = 0
                self.reject_reason_samples[reason] = []
            self.reject_reason_counts[reason] += 1
            if code and len(self.reject_reason_samples[reason]) < 5:
                self.reject_reason_samples[reason].append(code)

    def _log_reason_summary(self, note: str | None = None) -> None:
        total_rejected = sum(self.reject_reason_counts.values())
        setup_counter_total = getattr(getattr(self, "_setup_reason_counter", None), "total", lambda: 0)()
        total_candidates = total_rejected + (setup_counter_total - total_rejected)  # legacy diagnostic only
        # Actually, track total candidates separately
        # For now, assume we have total from somewhere
        # Wait, in the code, we need to count total
        # Let's add self.total_candidates = 0
        # In __init__, self.total_candidates = 0
        # In _log_setup, self.total_candidates += 1
        # Then use it.

        # For simplicity, use sum of ok and rejected
        # But to match the example, [PB1][REJECT_SUMMARY] total=198 ok=10 rejected=188
        # So need total and ok count.

        # Add self.ok_count = 0
        # In _log_setup, if cf.setup_ok: self.ok_count += 1

        if not self.reject_reason_counts and self.total_candidates > 0 and self.ok_count == 0:
            fallback_counter: Counter[str] = Counter()
            fallback_samples: dict[str, list[str]] = {}
            for cf in list(getattr(self, "_reject_summary_candidates", []) or []):
                features = getattr(cf, "features", {}) or {}
                collected: list[str] = []
                for key in ("setup_loose_reasons", "setup_strict_reasons", "filters_failed", "quality_flags"):
                    raw_items = features.get(key)
                    if raw_items is None:
                        continue
                    if isinstance(raw_items, str):
                        raw_iter = [x.strip() for x in raw_items.replace(";", ",").split(",") if x.strip()]
                    elif isinstance(raw_items, (list, tuple, set)):
                        raw_iter = list(raw_items)
                    else:
                        raw_iter = [raw_items]
                    collected.extend(str(item).strip() for item in raw_iter if str(item).strip())
                if not collected:
                    collected.extend(str(item).strip() for item in (getattr(cf, "reasons", None) or []) if str(item).strip())
                for reason in collected or ["unspecified_fail"]:
                    fallback_counter[str(reason)] += 1
                    sample_list = fallback_samples.setdefault(str(reason), [])
                    if getattr(cf, "code", None) and len(sample_list) < 5:
                        sample_list.append(str(cf.code))
            if fallback_counter:
                self.reject_reason_counts.update(dict(fallback_counter))
                for reason, samples in fallback_samples.items():
                    self.reject_reason_samples.setdefault(reason, samples)
                logger.info(
                    "[ENTRY][REJECT_SUMMARY][FALLBACK] total=%s source=candidate_features reasons=%s",
                    self.total_candidates,
                    fallback_counter.most_common(10),
                )
        if not self.reject_reason_counts:
            fallback_text = "UNKNOWN:1" if self.total_candidates > 0 and self.ok_count == 0 else "none"
            logger.info("[ENTRY][REJECT_SUMMARY] total=%s top_blockers=%s", self.total_candidates, fallback_text)
            if fallback_text != "none":
                logger.info("[RUN_SUMMARY][NO_BUY] reason=NO_FINAL_SETUPS top_blockers=%s", fallback_text)
            try:
                artifact_dir = Path("artifacts")
                artifact_dir.mkdir(parents=True, exist_ok=True)
                path = artifact_dir / "entry_reject_summary.json"
                path.write_text(json.dumps({
                    "total_candidates": int(getattr(self, "total_candidates", 0) or 0),
                    "setup_ok": int(getattr(self, "ok_count", 0) or 0),
                    "top_blockers": [] if fallback_text == "none" else [{"reason": "UNKNOWN", "count": 1, "samples": []}],
                    "thresholds": {},
                    "source_policy": {"authoritative": "pb1_engine", "scanner": "diagnostic_only", "minervini": "diagnostic_only"},
                    "bridge": {"enabled": False, "activated": False, "reason": "not_checked"},
                }, ensure_ascii=False, indent=2), encoding="utf-8")
                logger.info("[ENTRY][REJECT_SUMMARY][JSON] path=%s", path)
            except Exception as exc:
                logger.warning("[ENTRY][REJECT_SUMMARY][JSON][FAIL] err=%s", exc)
            return
        sorted_reasons = sorted(self.reject_reason_counts.items(), key=lambda x: x[1], reverse=True)
        top_blockers = ",".join(f"{str(reason).upper()}:{count}" for reason, count in sorted_reasons[:10])
        logger.info("[PB1][REJECT_SUMMARY] total=%s ok=%s rejected=%s", self.total_candidates, self.ok_count, total_rejected)
        logger.info("[ENTRY][REJECT_SUMMARY] total=%s top_blockers=%s", self.total_candidates, top_blockers or "none")
        if self.ok_count == 0 and (top_blockers or self.reject_reason_counts):
            logger.info("[RUN_SUMMARY][NO_BUY] reason=NO_FINAL_SETUPS top_blockers=%s", top_blockers or "unknown")
        for reason, count in sorted_reasons:
            samples = self.reject_reason_samples.get(reason, [])
            sample_str = f" sample={samples}" if samples else ""
            logger.info("[PB1][REJECT_REASON] %s=%s%s", reason, count, sample_str)
        try:
            artifact_dir = Path("artifacts")
            artifact_dir.mkdir(parents=True, exist_ok=True)
            filters = getattr(self, "effective_entry_filters", {}) or {}
            bridge_summary = getattr(self, "_relax_bridge_summary", {}) or {}
            payload = {
                "total_candidates": int(getattr(self, "total_candidates", 0) or 0),
                "setup_ok": int(getattr(self, "ok_count", 0) or 0),
                "top_blockers": [
                    {
                        "reason": str(reason).upper(),
                        "count": int(count),
                        "samples": list((getattr(self, "reject_reason_samples", {}) or {}).get(reason, []) or []),
                    }
                    for reason, count in sorted_reasons[:10]
                ],
                "thresholds": {
                    "vol_max": filters.get("vol_max"),
                    "volu_max": filters.get("volu_max"),
                    "volu_max_intraday": filters.get("volu_max_intraday"),
                },
                "source_policy": {
                    "authoritative": "pb1_engine",
                    "scanner": "diagnostic_only",
                    "minervini": "diagnostic_only",
                },
                "bridge": {
                    "enabled": bool(bridge_summary.get("enabled", False)),
                    "activated": bool(bridge_summary.get("activated", False)),
                    "reason": str(bridge_summary.get("reason") or "not_checked"),
                },
            }
            path = artifact_dir / "entry_reject_summary.json"
            path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
            logger.info("[ENTRY][REJECT_SUMMARY][JSON] path=%s", path)
        except Exception as exc:
            logger.warning("[ENTRY][REJECT_SUMMARY][JSON][FAIL] err=%s", exc)

    @staticmethod
    def _to_float(value: Any) -> float | None:
        try:
            if value is None:
                return None
            if isinstance(value, str):
                value = value.replace(",", "").strip()
            fval = float(value)
            if fval != fval:  # NaN guard
                return None
            return fval
        except Exception:
            return None

    @staticmethod
    def _value_missing(value: Any) -> bool:
        if value is None:
            return True
        if isinstance(value, str):
            return value.strip() == ""
        if isinstance(value, (float, np.floating)):
            return not np.isfinite(float(value))
        return False

    @classmethod
    def _has_numeric_value(cls, value: Any) -> bool:
        if cls._value_missing(value):
            return False
        try:
            return np.isfinite(float(value))
        except Exception:
            return False

    @classmethod
    def _pick_first_present(cls, row: dict[str, Any] | None, *keys: str) -> Any:
        source = row or {}
        for key in keys:
            value = source.get(key)
            if not cls._value_missing(value):
                return value
        return None

    @classmethod
    def _pick_first_float(cls, row: dict[str, Any] | None, *keys: str) -> float | None:
        return cls._to_float(cls._pick_first_present(row, *keys))

    @classmethod
    def _normalize_pullback_pct(cls, value: Any) -> float | None:
        pullback_pct = cls._to_float(value)
        if pullback_pct is None:
            return None
        if 0.0 < pullback_pct <= 1.0:
            return float(pullback_pct) * 100.0
        return float(pullback_pct)

    @classmethod
    def _zero_like_missing(cls, key: str, value: Any, *, zero_missing_keys: set[str] | None = None) -> bool:
        if key not in (zero_missing_keys or set()):
            return False
        try:
            return float(value) == 0.0
        except Exception:
            return False

    @classmethod
    def _merge_non_missing(
        cls,
        target: dict[str, Any],
        source: dict[str, Any] | None,
        *,
        zero_missing_keys: set[str] | None = None,
    ) -> None:
        for key, value in (source or {}).items():
            if key in target and not cls._value_missing(target.get(key)) and not cls._zero_like_missing(
                key,
                target.get(key),
                zero_missing_keys=zero_missing_keys,
            ):
                continue
            if cls._value_missing(value):
                continue
            target[key] = value

    def _map_precomputed_candidate_row(self, code: str) -> tuple[dict[str, Any], dict[str, int], list[str], bool, bool]:
        pre_row = dict(self._precomputed_final30_map.get(code) or {})
        derived_row = dict(self._precomputed_derived_map.get(code) or {})
        universe_row = dict(self._precomputed_universe_map.get(code) or {})
        derived_extra = derived_row.get("features_json") if isinstance(derived_row.get("features_json"), dict) else {}
        zero_fill_keys = {
            "atr_pct",
            "high20",
            "ma10",
            "ma20",
            "ma50",
            "ma150",
            "ma20_slope",
            "pullback_pct",
            "tr_range_pct",
            "trend_strength",
            "value20",
            "vol_contraction",
            "volu_contraction",
        }

        mapped: dict[str, Any] = {
            "close": self._pick_first_float(pre_row, "close", "last_close"),
            "current_price": self._pick_first_float(pre_row, "current_price", "intraday_last", "last", "stck_prpr"),
            "ma20": self._pick_first_float(pre_row, "ma20"),
            "ma50": self._pick_first_float(pre_row, "ma50"),
            "ma150": self._pick_first_float(pre_row, "ma150"),
            "breakout_score": self._pick_first_float(pre_row, "breakout_score"),
            "pullback_score": self._pick_first_float(pre_row, "pullback_score"),
            "momentum_score": self._pick_first_float(pre_row, "momentum_score"),
            "rs_percentile": self._pick_first_float(pre_row, "rs_percentile", "rs_pctile", "rs_score"),
            "vcp_score": self._pick_first_float(pre_row, "vcp_score"),
            "trend_score": self._pick_first_float(pre_row, "trend_score"),
            "entry_style_selected": self._pick_first_present(pre_row, "entry_style_selected"),
            "pullback_pct": self._normalize_pullback_pct(self._pick_first_present(pre_row, "pullback_pct")),
            "atr_pct": self._pick_first_float(pre_row, "atr_pct"),
            "score_final": self._pick_first_float(pre_row, "score_final", "final_score"),
            "tech_score": self._pick_first_float(pre_row, "tech_score", "score_tech"),
        }
        self._merge_non_missing(mapped, pre_row, zero_missing_keys=zero_fill_keys)
        self._merge_non_missing(mapped, derived_extra, zero_missing_keys=zero_fill_keys)
        self._merge_non_missing(
            mapped,
            {
                "close": self._pick_first_float(derived_row, "close", "last_close"),
                "current_price": self._pick_first_float(derived_row, "current_price", "intraday_last", "last", "stck_prpr"),
                "ma20": self._pick_first_float(derived_row, "ma20"),
                "ma50": self._pick_first_float(derived_row, "ma50"),
                "ma150": self._pick_first_float(derived_row, "ma150"),
                "breakout_score": self._pick_first_float(derived_row, "breakout_score"),
                "pullback_score": self._pick_first_float(derived_row, "pullback_score"),
                "momentum_score": self._pick_first_float(derived_row, "momentum_score"),
                "rs_percentile": self._pick_first_float(derived_row, "rs_percentile", "rs_pctile", "rs_score"),
                "vcp_score": self._pick_first_float(derived_row, "vcp_score"),
                "trend_score": self._pick_first_float(derived_row, "trend_score"),
                "entry_style_selected": self._pick_first_present(derived_row, "entry_style_selected"),
                "pullback_pct": self._normalize_pullback_pct(self._pick_first_present(derived_row, "pullback_pct")),
                "atr_pct": self._pick_first_float(derived_row, "atr_pct"),
                "score_final": self._pick_first_float(derived_row, "score_final", "final_score"),
                "tech_score": self._pick_first_float(derived_row, "tech_score", "score_tech"),
            },
            zero_missing_keys=zero_fill_keys,
        )
        self._merge_non_missing(mapped, derived_row, zero_missing_keys=zero_fill_keys)
        self._merge_non_missing(
            mapped,
            {
                "close": self._pick_first_float(universe_row, "close", "last_close"),
                "ma20": self._pick_first_float(universe_row, "ma20"),
                "ma50": self._pick_first_float(universe_row, "ma50"),
                "ma150": self._pick_first_float(universe_row, "ma150"),
                "breakout_score": self._pick_first_float(universe_row, "breakout_score"),
                "pullback_score": self._pick_first_float(universe_row, "pullback_score"),
                "momentum_score": self._pick_first_float(universe_row, "momentum_score"),
                "rs_percentile": self._pick_first_float(universe_row, "rs_percentile", "rs_pctile", "rs_score"),
                "vcp_score": self._pick_first_float(universe_row, "vcp_score"),
                "trend_score": self._pick_first_float(universe_row, "trend_score"),
                "entry_style_selected": self._pick_first_present(universe_row, "entry_style_selected"),
                "pullback_pct": self._normalize_pullback_pct(self._pick_first_present(universe_row, "pullback_pct")),
                "atr_pct": self._pick_first_float(universe_row, "atr_pct"),
                "score_final": self._pick_first_float(universe_row, "score_final", "final_score"),
                "tech_score": self._pick_first_float(universe_row, "tech_score", "score_tech"),
            },
            zero_missing_keys=zero_fill_keys,
        )
        self._merge_non_missing(mapped, universe_row, zero_missing_keys=zero_fill_keys)

        checks = {
            "has_close": int(self._has_numeric_value(mapped.get("close"))),
            "has_breakout_score": int(self._has_numeric_value(mapped.get("breakout_score"))),
            "has_pullback_score": int(self._has_numeric_value(mapped.get("pullback_score"))),
            "has_momentum_score": int(self._has_numeric_value(mapped.get("momentum_score"))),
            "has_rs_percentile": int(self._has_numeric_value(mapped.get("rs_percentile"))),
            "has_vcp_score": int(self._has_numeric_value(mapped.get("vcp_score"))),
            "has_ma20": int(self._has_numeric_value(mapped.get("ma20"))),
            "has_ma50": int(self._has_numeric_value(mapped.get("ma50"))),
            "has_ma150": int(self._has_numeric_value(mapped.get("ma150"))),
            "has_entry_style_selected": int(not self._value_missing(mapped.get("entry_style_selected"))),
            "has_price_context": int(
                any(
                    self._has_numeric_value(mapped.get(key))
                    for key in ("pullback_pct", "atr_pct", "high20", "tr_range_pct", "vol_contraction", "volu_contraction")
                )
            ),
        }
        reasons: list[str] = []
        if not (pre_row or derived_row or universe_row):
            reasons.append("schema_not_mapped")
        if not checks["has_close"]:
            reasons.append("missing_close")
        if not checks["has_breakout_score"]:
            reasons.append("missing_breakout_score")
        if not checks["has_pullback_score"]:
            reasons.append("missing_pullback_score")
        if not checks["has_momentum_score"]:
            reasons.append("missing_momentum_score")
        if not checks["has_rs_percentile"]:
            reasons.append("missing_rs_percentile")
        if not checks["has_vcp_score"]:
            reasons.append("missing_vcp_score")
        if not checks["has_ma20"]:
            reasons.append("missing_ma20")
        if not checks["has_ma50"]:
            reasons.append("missing_ma50")
        if not checks["has_ma150"]:
            reasons.append("missing_ma150")
        if not checks["has_entry_style_selected"]:
            reasons.append("missing_entry_style_selected")
        if not checks["has_price_context"]:
            reasons.append("missing_price_context")

        usable_precomputed_row = all(
            checks[key]
            for key in (
                "has_close",
                "has_breakout_score",
                "has_pullback_score",
                "has_momentum_score",
                "has_rs_percentile",
                "has_vcp_score",
                "has_ma20",
                "has_ma50",
                "has_ma150",
                "has_entry_style_selected",
                "has_price_context",
            )
        )
        precomputed_data_ok = all(
            checks[key]
            for key in (
                "has_close",
                "has_breakout_score",
                "has_pullback_score",
                "has_momentum_score",
                "has_rs_percentile",
                "has_ma20",
                "has_ma50",
                "has_ma150",
            )
        )
        checks["usable_precomputed_row"] = int(usable_precomputed_row)
        return mapped, checks, reasons, usable_precomputed_row, precomputed_data_ok

    def _extract_holdings_prices(self, holdings_rows: Iterable[dict]) -> Dict[str, float]:
        prices: Dict[str, float] = {}
        for row in holdings_rows or []:
            try:
                code = str(row.get("pdno") or row.get("code") or "").zfill(6)
                px = self._to_float(row.get("prpr") or row.get("stck_prpr"))
                if code and px is not None:
                    prices[code] = px
            except Exception:
                continue
        return prices

    def _sum_cost_from_rows(self, rows: Iterable[dict]) -> float:
        total = 0.0
        for row in rows or []:
            val = self._to_float(row.get("pchs_amt"))
            if val:
                total += val
        return total

    def _extract_holdings_cost(self, holdings_rows: Iterable[dict], holdings_summary: dict | None) -> float | None:
        summary = _as_first_dict(holdings_summary)
        for key in COST_KEYS:
            cost = self._to_float(summary.get(key))
            if cost and cost > 0:
                return cost
        total = self._sum_cost_from_rows(holdings_rows)
        return total if total > 0 else None

    def _extract_available_cash(self, holdings_summary: dict | None) -> float | None:
        summary = _as_first_dict(holdings_summary)
        for key in CASH_KEYS:
            cash = self._to_float(summary.get(key))
            if cash is not None:
                return cash
        return None

    def _parse_available_cash_snapshot(self, snapshot: dict) -> tuple[int | None, dict]:
        summary_raw = snapshot.get("output2")
        summary = _as_first_dict(summary_raw)
        selected_key = None
        cash_value = None
        
        for key in ("ord_psbl_cash", "nxdy_excc_amt", "dnca_tot_amt", "prvs_rcdl_excc_amt"):
            if key in summary:
                val = self._to_float(summary.get(key))
                if val is not None and val > 0:
                    selected_key = key
                    cash_value = val
                    break
        
        if cash_value is None:
            tot_evlu = self._to_float(summary.get("tot_evlu_amt"))
            scts_evlu = self._to_float(summary.get("scts_evlu_amt"))
            if tot_evlu is not None and scts_evlu is not None:
                estimated = tot_evlu - scts_evlu
                if estimated >= 0:
                    selected_key = "tot_evlu_minus_scts_evlu"
                    cash_value = estimated
        output2_keys = _extract_output2_keys(summary_raw)
        meta = {
            "selected_key": selected_key,
            "output2_keys": output2_keys,
        }
        if cash_value is None:
            return None, meta
        return int(cash_value), meta

    def _resolve_holdings_snapshot_with_cash(self, snapshot: dict) -> tuple[dict, int, dict]:
        if isinstance(snapshot, dict) and snapshot.get("output2") is None and self.kis:
            self._warn_once(
                "balance_output2_none",
                "[BALANCE][CACHE][INVALID] reason=output2_none -> refetch",
            )
            try:
                snapshot = self.kis.get_balance_cached(force=True)
            except Exception as exc:
                raise RuntimeError("Balance refetch failed after output2 None") from exc
        _log_balance_snapshot_shape(snapshot, label="input")
        if _is_sanitized_balance_snapshot(snapshot):
            logger.warning("[PB1][CASH][SANITIZED] detected -> force refetch raw")
            if self.kis:
                try:
                    refreshed_snapshot, _source = self.kis.get_balance_cached(force=True, return_source=True)
                    snapshot = refreshed_snapshot
                except Exception as exc:
                    raise RuntimeError("Balance refresh failed after sanitized snapshot") from exc

        orderable = None
        if self.kis:
            try:
                orderable = self.kis.get_orderable_cash_krw(force=False)
            except Exception:
                orderable = None

        if isinstance(orderable, (int, float)) and orderable > 0:
            available_cash_krw = int(orderable)
            return snapshot, available_cash_krw, {"source": "orderable_cash", "selected_key": "ord_psbl_cash"}

        balance_resp = snapshot
        cash, meta = self._parse_available_cash_snapshot(balance_resp)
        if cash is not None and cash > 0:
            return snapshot, int(cash), {**meta, "source": "balance_snapshot"}

        if self.kis:
            try:
                balance_resp = self.kis.get_balance_cached(force=True)
                _log_balance_snapshot_shape(balance_resp, label="force_refresh")
                cash, meta = self._parse_available_cash_snapshot(balance_resp)
            except Exception:
                cash = None

        if cash is None or cash <= 0:
            output2 = balance_resp.get("output2") if isinstance(balance_resp, dict) else None
            output2_type = type(output2).__name__
            output2_len = len(output2) if isinstance(output2, list) else None
            output2_keys = _extract_output2_keys(output2)
            raise RuntimeError(
                "Balance parse failed: cannot locate usable cash fields "
                f"(cache object shape type(output2)={output2_type} len={output2_len} keys(output2[0])={output2_keys})"
            )

        return balance_resp, int(cash), {**meta, "source": "balance_snapshot"}

    def _resolve_entry_capital(
        self,
        *,
        base_cash_krw: int | None = None,
        available_cash_krw: int | None = None,
        override_capital: float | None,
        reserve_pct: float,
    ) -> tuple[int, int, dict]:
        if base_cash_krw is None:
            base_cash_krw = int(available_cash_krw or 0)
        raw_base_cash_krw = int(base_cash_krw or 0)
        effective_base_cash_krw = raw_base_cash_krw
        clamp_meta = {}
        if (self.env or "").strip().lower() in {"paper", "practice"}:
            cap = int(PAPER_MAX_CAPITAL_KRW)
            if cap > 0 and effective_base_cash_krw > cap:
                before = effective_base_cash_krw
                effective_base_cash_krw = cap
                clamp_meta = {"before": before, "cap": cap, "after": effective_base_cash_krw}
                logger.info(
                    "[PB1][CAPITAL][CLAMP] before=%s cap=%s after=%s reason=paper_limit",
                    before,
                    cap,
                    effective_base_cash_krw,
                )
        use_override = override_capital is not None and int(override_capital) > 0
        usable = max(int(effective_base_cash_krw * (1 - reserve_pct)), 0)
        entry_capital = usable
        if use_override:
            entry_capital = min(entry_capital, int(override_capital))
        cap_limit = None
        cap_applied = False
        if self.intended_live and CAP_CAP and CAP_CAP > 0:
            cap_limit = int(effective_base_cash_krw * CAP_CAP) if CAP_CAP <= 1 else int(CAP_CAP)
            if cap_limit > 0 and entry_capital > cap_limit:
                entry_capital = cap_limit
                cap_applied = True
        meta = {
            "use_override": use_override,
            "reserve_pct": reserve_pct,
            "cap_limit": cap_limit,
            "cap_applied": cap_applied,
            "clamp": clamp_meta,
            "base_cash": effective_base_cash_krw,
            "raw_base_cash": raw_base_cash_krw,
        }
        return entry_capital, usable, meta

    def _parse_kis_holdings(self, holdings_rows: Iterable[dict]) -> dict[str, dict]:
        holdings: dict[str, dict] = {}
        for row in holdings_rows or []:
            code = str(row.get("pdno") or row.get("code") or "").zfill(6)
            qty_raw = row.get("qty") if "qty" in row else row.get("hldg_qty") or row.get("ord_psbl_qty")
            try:
                qty = int(float(qty_raw or 0))
            except Exception:
                qty = 0
            if not code or qty <= 0:
                continue
            avg = self._to_float(row.get("avg_price") or row.get("pchs_avg_pric") or row.get("pchs_avg_price"))
            holdings[code] = {
                "code": code,
                "qty": qty,
                "avg_buy_price": avg,
                "market": row.get("market") or row.get("prdt_type_cd") or row.get("mket_gb"),
            }
        return holdings

    def _build_positions_from_kis(
        self,
        holdings_rows: Iterable[dict],
        positions_rows: Iterable[dict] | None = None,
    ) -> list[dict]:
        kis_holdings = self._parse_kis_holdings(holdings_rows)
        positions_by_code = {
            str(row.get("code") or "").zfill(6): row
            for row in positions_rows or []
            if row.get("code")
        }
        ledger_store = LedgerStore(LEDGER_BASE_DIR, env=self.env, run_id=self.run_id)
        ledger_positions = ledger_store.rebuild_positions_average_cost(lookback_days=LEDGER_LOOKBACK_DAYS)
        api_codes = set(kis_holdings.keys())
        for (code, sid, mode), state in ledger_positions.items():
            if sid != 1:
                continue
            total_qty = int(state.get("total_qty") or 0)
            if total_qty <= 0 or code in api_codes:
                continue
            logger.warning(
                "[RECONCILE][ORPHAN] code=%s reason=ledger_only ledger_qty=%s",
                code,
                total_qty,
            )
            append_event = getattr(self.ledger_repo, "add_event", None) or getattr(self.ledger_repo, "append_event", None)
            if append_event is not None:
                append_event(
                    env=self.env,
                    run_id=self.run_id,
                    strategy=self.STRATEGY_NAME,
                    event_type="POSITION_ORPHANED",
                    code=code,
                    market=state.get("market"),
                    sid=sid,
                    qty=total_qty,
                    ok=True,
                    payload_json={"ledger_total_qty": total_qty},
                )
        ledger_by_code: dict[str, dict] = {}
        for (code, sid, mode), state in ledger_positions.items():
            if sid != 1:
                continue
            existing = ledger_by_code.get(code)
            if not existing or int(state.get("total_qty") or 0) > int(existing.get("total_qty") or 0):
                ledger_by_code[code] = state

        positions: list[dict] = []
        for code, holding in kis_holdings.items():
            ledger_state = ledger_by_code.get(code, {})
            pos_state = positions_by_code.get(code, {})
            qty = holding.get("qty") or 0
            avg = holding.get("avg_buy_price") or ledger_state.get("avg_buy_price") or 0.0
            positions.append(
                {
                    "mode": int(ledger_state.get("mode") or 1),
                    "qty": qty,
                    "kis_qty": qty,
                    "avg_buy_price": avg or None,
                    "market": holding.get("market") or ledger_state.get("market"),
                    "holding_days": ledger_state.get("holding_days") or 0,
                    "first_buy_ts": ledger_state.get("first_buy_ts"),
                    "total_cost": float(ledger_state.get("total_cost") or 0.0) or (avg * qty if avg else 0.0),
                    "realized_pnl": ledger_state.get("realized_pnl") or 0.0,
                    "meta_source": "kis",
                    "entry_ts": pos_state.get("entry_ts"),
                    "entry_reason": pos_state.get("entry_reason"),
                    "entry_style_selected": pos_state.get("entry_style_selected"),
                    "entry_decision_family": pos_state.get("entry_decision_family"),
                    "entry_rule_version": pos_state.get("entry_rule_version"),
                    "entry_meta_json": pos_state.get("entry_meta_json") or {},
                    "stop_price_at_entry": pos_state.get("stop_price_at_entry"),
                    "pivot_price_at_entry": pos_state.get("pivot_price_at_entry"),
                    "initial_stop": pos_state.get("initial_stop"),
                    "stop_price": pos_state.get("stop_price"),
                    "max_price": pos_state.get("max_price"),
                    "pyramid_level": pos_state.get("pyramid_level"),
                    "pivot": pos_state.get("pivot"),
                    "last_add_price": pos_state.get("last_add_price"),
                    "last_stop_update_ts": pos_state.get("last_stop_update_ts"),
                    "partial_exit_level": pos_state.get("partial_exit_level"),
                    "base_id": pos_state.get("base_id"),
                    "setup_id": pos_state.get("setup_id"),
                    "tight_low": pos_state.get("tight_low"),
                    "base_high": pos_state.get("base_high"),
                    "entry_price": pos_state.get("entry_price"),
                    "r_value": pos_state.get("r_value"),
                    "tp1_done": pos_state.get("tp1_done"),
                    "tp2_done": pos_state.get("tp2_done"),
                    "trail_mode": pos_state.get("trail_mode"),
                    "last_trail_stop": pos_state.get("last_trail_stop"),
                    "cooldown_until": pos_state.get("cooldown_until"),
                    "regime_at_entry": pos_state.get("regime_at_entry"),
                    "risk_mult_at_entry": pos_state.get("risk_mult_at_entry"),
                    "exit_policy_family": pos_state.get("exit_policy_family"),
                    "last_exit_eval_json": pos_state.get("last_exit_eval_json") or {},
                }
            )
        return positions

    def _build_holding_contexts_from_balance_rows(
        self,
        balance_rows: Iterable[dict],
        ledger_positions: Iterable[dict] | None = None,
    ) -> list[HoldingContext]:
        positions_by_code = {
            str((row or {}).get("code") or "").zfill(6): dict(row or {})
            for row in (ledger_positions or [])
            if str((row or {}).get("code") or "").strip()
        }
        codes = [
            str((row or {}).get("pdno") or (row or {}).get("code") or "").zfill(6)
            for row in (balance_rows or [])
            if str((row or {}).get("pdno") or (row or {}).get("code") or "").strip()
        ]
        latest_buy_fills = self.fills_repo.list_latest_buy_fills_by_codes(self.env, codes)
        today_kst = self._now_kst.date()
        holdings: list[HoldingContext] = []
        for row in balance_rows or []:
            code = str(row.get("pdno") or row.get("code") or "").zfill(6)
            if not code:
                continue
            try:
                holding_qty = int(float(row.get("hldg_qty") or row.get("qty") or 0))
            except Exception:
                holding_qty = 0
            if holding_qty <= 0:
                continue
            try:
                orderable_qty = int(float(row.get("ord_psbl_qty") or row.get("qty") or 0))
            except Exception:
                orderable_qty = 0
            avg_price = float(row.get("avg_price") or row.get("pchs_avg_pric") or row.get("pchs_avg_price") or 0.0)
            last_price = float(row.get("last_price") or row.get("prpr") or row.get("stck_prpr") or row.get("now_pric") or 0.0)
            if last_price <= 0:
                last_price = self._to_float(self._balance_price_map.get(code)) or avg_price
            market_value = float(row.get("evlu_amt") or row.get("evlu_pfls_amt") or (last_price * holding_qty))
            unrealized_pnl = float(row.get("evlu_pfls_amt") or row.get("unrealized_pnl") or ((last_price - avg_price) * holding_qty))
            unrealized_pct = float(
                row.get("evlu_pfls_rt")
                or row.get("unrealized_pct")
                or ((((last_price - avg_price) / avg_price) * 100.0) if avg_price > 0 else 0.0)
            )
            pos_meta = dict(positions_by_code.get(code) or {})
            latest_buy_fill = latest_buy_fills.get(code) or {}
            entry_ts_raw = latest_buy_fill.get("filled_at") or pos_meta.get("entry_ts") or pos_meta.get("last_trade_at")
            pos_age = calc_position_age(entry_ts_raw, today_kst)
            entry_date = pos_age.entry_date_kst
            trading_days_held = pos_age.days_held if entry_ts_raw else int(pos_meta.get("trading_days_held") or pos_meta.get("holding_days") or 0)
            calendar_days_held = _calendar_days_held(entry_ts_raw, today_kst) if entry_ts_raw else int(pos_meta.get("calendar_days_held") or trading_days_held)
            holdings.append(
                HoldingContext(
                    code=code,
                    name=str(row.get("prdt_name") or row.get("name") or self._code_name_map.get(code, "")),
                    holding_qty=holding_qty,
                    orderable_qty=max(0, orderable_qty),
                    avg_price=avg_price,
                    last_price=last_price,
                    market_value=market_value,
                    unrealized_pnl=unrealized_pnl,
                    unrealized_pct=unrealized_pct,
                    source="kis_balance",
                    market=pos_meta.get("market") or row.get("prdt_type_cd") or row.get("mket_gb"),
                    mode=int(pos_meta.get("mode") or 1),
                    sid=int(pos_meta.get("sid") or 1),
                    entry_date=entry_date,
                    days_held=trading_days_held,
                    calendar_days_held=calendar_days_held,
                    trading_days_held=trading_days_held,
                    holding_bars=pos_age.holding_bars,
                    last_fill_at=str(entry_ts_raw) if entry_ts_raw else None,
                    position_meta=pos_meta,
                )
            )
        return holdings

    def _build_holding_contexts_from_position_rows(
        self,
        ledger_positions: Iterable[dict] | None = None,
    ) -> list[HoldingContext]:
        positions = [dict(row or {}) for row in (ledger_positions or [])]
        codes = [str((row or {}).get("code") or "").zfill(6) for row in positions if str((row or {}).get("code") or "").strip()]
        latest_buy_fills = {}
        if codes and hasattr(self.fills_repo, "list_latest_buy_fills_by_codes"):
            try:
                latest_buy_fills = self.fills_repo.list_latest_buy_fills_by_codes(self.env, codes)
            except Exception:
                latest_buy_fills = {}
        today_kst = self._now_kst.date()
        holdings: list[HoldingContext] = []
        for row in positions:
            code = str(row.get("code") or "").zfill(6)
            if not code:
                continue
            qty = int(float(row.get("qty") or 0) or 0)
            if qty <= 0:
                continue
            avg_price = float(row.get("avg_buy_price") or row.get("entry_price") or 0.0)
            last_price = float(row.get("last_price") or self._balance_price_map.get(code) or avg_price or 0.0)
            latest_buy_fill = latest_buy_fills.get(code) or {}
            entry_ts_raw = latest_buy_fill.get("filled_at") or row.get("entry_ts") or row.get("last_trade_at")
            entry_date = None
            days_held = int(row.get("holding_days") or 0)
            if entry_ts_raw:
                try:
                    entry_dt = pd.Timestamp(entry_ts_raw)
                    entry_date = entry_dt.date().isoformat()
                    days_held = max(0, (today_kst - entry_dt.date()).days)
                except Exception:
                    entry_date = str(entry_ts_raw)
            total_cost = float(row.get("total_cost") or (avg_price * qty) or 0.0)
            market_value = float(last_price * qty)
            unrealized_pnl = ((last_price - avg_price) * qty) if avg_price > 0 else 0.0
            unrealized_pct = (((last_price - avg_price) / avg_price) * 100.0) if avg_price > 0 else 0.0
            pos_meta = dict(row)
            pos_meta.update(
                {
                    "avg_buy_price": avg_price,
                    "total_cost": total_cost,
                    "entry_date": entry_date,
                    "last_fill_at": str(entry_ts_raw) if entry_ts_raw else None,
                    "entry_reason": pos_meta.get("entry_reason") or (pos_meta.get("entry_meta_json") or {}).get("entry_reason"),
                    "entry_style_selected": pos_meta.get("entry_style_selected") or (pos_meta.get("entry_meta_json") or {}).get("entry_style_selected"),
                    "stop_price_at_entry": pos_meta.get("stop_price_at_entry") or (pos_meta.get("entry_meta_json") or {}).get("stop_price_at_entry"),
                    "pivot_price_at_entry": pos_meta.get("pivot_price_at_entry") or (pos_meta.get("entry_meta_json") or {}).get("pivot_price_at_entry"),
                }
            )
            holdings.append(
                HoldingContext(
                    code=code,
                    name=str(row.get("name") or self._code_name_map.get(code, "")),
                    holding_qty=qty,
                    orderable_qty=qty,
                    avg_price=avg_price,
                    last_price=last_price,
                    market_value=market_value,
                    unrealized_pnl=unrealized_pnl,
                    unrealized_pct=unrealized_pct,
                    source="db_positions",
                    market=row.get("market"),
                    mode=int(row.get("mode") or 1),
                    sid=int(row.get("sid") or 1),
                    entry_date=entry_date,
                    days_held=days_held,
                    last_fill_at=str(entry_ts_raw) if entry_ts_raw else None,
                    position_meta=pos_meta,
                )
            )
        return holdings

    def _build_holding_contexts_from_fill_reconstruction(
        self,
        ledger_positions: Iterable[dict] | None = None,
    ) -> list[HoldingContext]:
        if not hasattr(self.fills_repo, "list_fills_in_window"):
            return []
        lookback_days = max(30, int(os.getenv("PB1_EXIT_LEDGER_LOOKBACK_DAYS", "365") or "365"))
        start_at = self._now_kst - pd.Timedelta(days=lookback_days)
        try:
            fills = self.fills_repo.list_fills_in_window(
                self.env,
                start_at=start_at,
                end_at=self._now_kst + pd.Timedelta(seconds=1),
            )
        except Exception:
            return []
        if not fills:
            return []
        positions_by_code = {
            str((row or {}).get("code") or "").zfill(6): dict(row or {})
            for row in (ledger_positions or [])
            if str((row or {}).get("code") or "").strip()
        }
        state_by_code: dict[str, dict[str, Any]] = {}
        sorted_fills = sorted([dict(fill or {}) for fill in fills], key=lambda item: str(item.get("filled_at") or ""))
        for fill in sorted_fills:
            code = str(fill.get("code") or "").zfill(6)
            if not code:
                continue
            side = str(fill.get("side") or "").upper()
            qty = int(float(fill.get("qty") or 0) or 0)
            price = float(fill.get("price") or 0.0)
            if qty <= 0:
                continue
            state = state_by_code.setdefault(
                code,
                {
                    "qty": 0,
                    "total_cost": 0.0,
                    "avg_price": 0.0,
                    "last_fill_at": None,
                    "entry_ts": None,
                    "entry_reason": None,
                    "entry_style_selected": None,
                    "stop_price_at_entry": None,
                    "pivot_price_at_entry": None,
                    "market": (positions_by_code.get(code) or {}).get("market"),
                },
            )
            if side == "BUY":
                state["total_cost"] += float(price) * qty
                state["qty"] += qty
                state["avg_price"] = (state["total_cost"] / state["qty"]) if state["qty"] > 0 else 0.0
                state["entry_ts"] = state.get("entry_ts") or fill.get("filled_at")
                state["last_fill_at"] = fill.get("filled_at")
                fill_meta = dict(fill.get("fill_meta_json") or fill.get("entry_meta_json") or {})
                if not state.get("entry_reason"):
                    state["entry_reason"] = fill.get("entry_reason") or fill_meta.get("entry_reason")
                if not state.get("entry_style_selected"):
                    state["entry_style_selected"] = fill.get("entry_style_selected") or fill_meta.get("entry_style_selected")
                if not state.get("stop_price_at_entry"):
                    state["stop_price_at_entry"] = fill.get("stop_price_at_entry") or fill_meta.get("stop_price_at_entry")
                if not state.get("pivot_price_at_entry"):
                    state["pivot_price_at_entry"] = fill.get("pivot_price_at_entry") or fill_meta.get("pivot_price_at_entry")
            elif side == "SELL":
                close_qty = min(state["qty"], qty)
                avg_price = float(state.get("avg_price") or 0.0)
                state["qty"] = max(0, int(state["qty"] - close_qty))
                state["total_cost"] = max(0.0, float(state["total_cost"] or 0.0) - (avg_price * close_qty))
                state["avg_price"] = (state["total_cost"] / state["qty"]) if state["qty"] > 0 else 0.0
                state["last_fill_at"] = fill.get("filled_at")
        holdings: list[HoldingContext] = []
        today_kst = self._now_kst.date()
        for code, state in state_by_code.items():
            qty = int(state.get("qty") or 0)
            if qty <= 0:
                continue
            pos_meta = dict(positions_by_code.get(code) or {})
            entry_ts_raw = state.get("entry_ts") or pos_meta.get("entry_ts") or state.get("last_fill_at")
            entry_date = None
            days_held = int(pos_meta.get("holding_days") or 0)
            if entry_ts_raw:
                try:
                    entry_dt = pd.Timestamp(entry_ts_raw)
                    entry_date = entry_dt.date().isoformat()
                    days_held = max(0, (today_kst - entry_dt.date()).days)
                except Exception:
                    entry_date = str(entry_ts_raw)
            avg_price = float(state.get("avg_price") or pos_meta.get("avg_buy_price") or 0.0)
            last_price = float(pos_meta.get("last_price") or self._balance_price_map.get(code) or avg_price or 0.0)
            total_cost = float(state.get("total_cost") or pos_meta.get("total_cost") or (avg_price * qty) or 0.0)
            unrealized_pnl = ((last_price - avg_price) * qty) if avg_price > 0 else 0.0
            unrealized_pct = (((last_price - avg_price) / avg_price) * 100.0) if avg_price > 0 else 0.0
            pos_meta.update(
                {
                    "avg_buy_price": avg_price,
                    "total_cost": total_cost,
                    "entry_date": entry_date,
                    "last_fill_at": str(state.get("last_fill_at") or "") or None,
                    "entry_reason": pos_meta.get("entry_reason") or state.get("entry_reason") or (pos_meta.get("entry_meta_json") or {}).get("entry_reason"),
                    "entry_style_selected": pos_meta.get("entry_style_selected") or state.get("entry_style_selected") or (pos_meta.get("entry_meta_json") or {}).get("entry_style_selected"),
                    "stop_price_at_entry": pos_meta.get("stop_price_at_entry") or state.get("stop_price_at_entry") or (pos_meta.get("entry_meta_json") or {}).get("stop_price_at_entry"),
                    "pivot_price_at_entry": pos_meta.get("pivot_price_at_entry") or state.get("pivot_price_at_entry") or (pos_meta.get("entry_meta_json") or {}).get("pivot_price_at_entry"),
                }
            )
            holdings.append(
                HoldingContext(
                    code=code,
                    name=str(pos_meta.get("name") or self._code_name_map.get(code, "")),
                    holding_qty=qty,
                    orderable_qty=qty,
                    avg_price=avg_price,
                    last_price=last_price,
                    market_value=float(last_price * qty),
                    unrealized_pnl=unrealized_pnl,
                    unrealized_pct=unrealized_pct,
                    source="ledger_reconstruct",
                    market=pos_meta.get("market") or state.get("market"),
                    mode=int(pos_meta.get("mode") or 1),
                    sid=int(pos_meta.get("sid") or 1),
                    entry_date=entry_date,
                    days_held=days_held,
                    last_fill_at=str(state.get("last_fill_at") or "") or None,
                    position_meta=pos_meta,
                )
            )
        return holdings

    def _load_exit_test_holdings_rows(self) -> tuple[list[dict[str, Any]], str | None]:
        json_raw = (os.getenv("PB1_EXIT_TEST_HOLDINGS_JSON") or "").strip()
        path_raw = (os.getenv("PB1_EXIT_TEST_HOLDINGS_PATH") or "").strip()
        source = None
        payload: Any = []
        if json_raw:
            source = "env_json"
            try:
                payload = json.loads(json_raw)
            except Exception:
                logger.warning("[EXIT][TEST_HOLDINGS][LOAD_FAIL] source=env_json")
                return [], None
        elif path_raw:
            source = "env_path"
            try:
                payload = json.loads(Path(path_raw).read_text(encoding="utf-8"))
            except Exception:
                logger.warning("[EXIT][TEST_HOLDINGS][LOAD_FAIL] source=env_path path=%s", path_raw)
                return [], None
        if not isinstance(payload, list):
            return [], None
        rows = [dict(item or {}) for item in payload if isinstance(item, dict)]
        if rows:
            logger.info("[EXIT][TEST_HOLDINGS][LOAD] count=%s source=%s", len(rows), source)
        return rows, source

    def _build_holding_contexts_from_test_rows(
        self,
        test_rows: Iterable[dict[str, Any]],
        ledger_positions: Iterable[dict] | None = None,
    ) -> list[HoldingContext]:
        positions_by_code = {
            str((row or {}).get("code") or "").zfill(6): dict(row or {})
            for row in (ledger_positions or [])
            if str((row or {}).get("code") or "").strip()
        }
        today_kst = self._now_kst.date()
        holdings: list[HoldingContext] = []
        for row in test_rows or []:
            code = str(row.get("code") or "").zfill(6)
            if not code:
                continue
            qty = int(float(row.get("qty") or 0) or 0)
            if qty <= 0:
                continue
            avg_price = float(row.get("avg_price") or 0.0)
            last_price = float(row.get("last_price") or row.get("close") or avg_price or 0.0)
            bought_at = row.get("bought_at")
            entry_date = None
            days_held = 0
            if bought_at:
                try:
                    entry_dt = pd.Timestamp(bought_at)
                    entry_date = entry_dt.date().isoformat()
                    days_held = max(0, (today_kst - entry_dt.date()).days)
                except Exception:
                    entry_date = str(bought_at)
            pos_meta = dict(positions_by_code.get(code) or {})
            holdings.append(
                HoldingContext(
                    code=code,
                    name=str(row.get("name") or self._code_name_map.get(code, "")),
                    holding_qty=qty,
                    orderable_qty=qty,
                    avg_price=avg_price,
                    last_price=last_price,
                    market_value=float(last_price * qty),
                    unrealized_pnl=((last_price - avg_price) * qty) if avg_price > 0 else 0.0,
                    unrealized_pct=(((last_price - avg_price) / avg_price) * 100.0) if avg_price > 0 else 0.0,
                    source="injected_test_holdings",
                    market=pos_meta.get("market") or row.get("market"),
                    mode=int(pos_meta.get("mode") or 1),
                    sid=int(pos_meta.get("sid") or 1),
                    entry_date=entry_date,
                    days_held=days_held,
                    last_fill_at=str(bought_at) if bought_at else None,
                    position_meta=pos_meta,
                )
            )
        return holdings

    def load_effective_holdings_for_exit(
        self,
        balance_rows: Iterable[dict],
        ledger_positions: Iterable[dict] | None = None,
        *,
        as_of: str | None = None,
        env: str | None = None,
        intended_live: bool | None = None,
        allow_http: bool | None = None,
        balance_snapshot: dict | None = None,
    ) -> tuple[list[HoldingContext], dict[str, Any]]:
        del as_of, env, intended_live, allow_http

        # 🔥 KIS empty guard: KIS balance가 정상 조회되었고 output1=[]이면 ledger_reconstruct 금지
        authoritative_kis = os.getenv("KR_AUTHORITATIVE_KIS_BALANCE_FOR_EXIT", "1") == "1"
        block_when_kis_empty = os.getenv("KR_BLOCK_LEDGER_RECONSTRUCT_WHEN_KIS_EMPTY", "1") == "1"

        if (
            authoritative_kis
            and block_when_kis_empty
            and self._is_kis_kr_context()
            and _is_kis_balance_authoritative_empty(balance_snapshot)
        ):
            logger.warning(
                "[EXIT][HOLDINGS][KIS_AUTHORITATIVE_EMPTY] "
                "source=kis output1_empty=1 action=skip_db_ledger_reconstruct"
            )

            # stale ledger positions 기록
            if ledger_positions:
                stale_codes = [
                    str((row or {}).get("code") or "").zfill(6)
                    for row in ledger_positions
                    if int((row or {}).get("qty") or 0) > 0
                ]
                if stale_codes:
                    logger.warning(
                        "[EXIT][STALE_LEDGER_POSITIONS] count=%s codes=%s reason=kis_empty_authoritative",
                        len(stale_codes),
                        stale_codes,
                    )
                    for code in stale_codes:
                        self._append_ledger_event(
                            event_type="STALE_LEDGER_POSITION",
                            code=code,
                            market=None,
                            mode=1,
                            qty=0,
                            price=None,
                            client_order_key=None,
                            side="NONE",
                            ok=True,
                            reasons=["KIS_EMPTY_AUTHORITATIVE", "NO_KIS_HOLDING"],
                            stage="exit_holdings_prep",
                            payload_json={
                                "source": "ledger_positions",
                                "kis_qty": 0,
                                "action": "skip_exit_order",
                            },
                        )

            return [], {
                "source": "kis_empty_authoritative",
                "count": 0,
                "fallback_blocked": True,
                "reason": "KIS_BALANCE_EMPTY_NO_LEDGER_RECONSTRUCT",
            }

        balance_holdings = self._build_holding_contexts_from_balance_rows(balance_rows, ledger_positions)
        if balance_holdings:
            meta = {
                "source": "kis_balance",
                "snapshot_ts": self._now_kst.isoformat(),
                "freshness": "live",
                "reconstructed": False,
            }
            logger.info("[EXIT][HOLDINGS][SOURCE] source=%s count=%s", meta["source"], len(balance_holdings))
            return balance_holdings, meta

        # 🔥 ledger_reconstruct 허용 조건 제한
        allow_ledger_fallback = os.getenv("KR_ALLOW_LEDGER_RECONSTRUCT_WITHOUT_KIS", "0") == "1"
        if (
            self._is_kis_kr_context()
            and not allow_ledger_fallback
            and balance_snapshot is not None
        ):
            logger.warning(
                "[EXIT][HOLDINGS][BLOCK_LEDGER_RECONSTRUCT] "
                "kis_context=1 balance_empty=1 allow_fallback=0 action=skip_ledger_reconstruct"
            )
            return [], {
                "source": "empty_blocked_ledger_reconstruct",
                "count": 0,
                "fallback_blocked": True,
                "reason": "KIS_BALANCE_EMPTY_NO_LEDGER_RECONSTRUCT",
            }

        position_holdings = self._build_holding_contexts_from_position_rows(ledger_positions)
        if position_holdings:
            meta = {
                "source": "db_positions",
                "snapshot_ts": self._now_kst.isoformat(),
                "freshness": "db_snapshot",
                "reconstructed": False,
            }
            logger.info("[EXIT][HOLDINGS][SOURCE] source=%s count=%s", meta["source"], len(position_holdings))
            return position_holdings, meta

        reconstructed_holdings = self._build_holding_contexts_from_fill_reconstruction(ledger_positions)
        if reconstructed_holdings:
            meta = {
                "source": "ledger_reconstruct",
                "snapshot_ts": self._now_kst.isoformat(),
                "freshness": "reconstructed",
                "reconstructed": True,
            }
            logger.info("[EXIT][HOLDINGS][SOURCE] source=%s count=%s", meta["source"], len(reconstructed_holdings))
            return reconstructed_holdings, meta

        test_rows, test_source = self._load_exit_test_holdings_rows()
        test_holdings = self._build_holding_contexts_from_test_rows(test_rows, ledger_positions)
        if test_holdings:
            meta = {
                "source": "injected_test_holdings",
                "snapshot_ts": self._now_kst.isoformat(),
                "freshness": test_source or "test",
                "reconstructed": True,
            }
            logger.info("[EXIT][TEST_HOLDINGS][APPLIED] count=%s", len(test_holdings))
            logger.info("[EXIT][HOLDINGS][SOURCE] source=%s count=%s", meta["source"], len(test_holdings))
            return test_holdings, meta

        meta = {
            "source": "empty",
            "snapshot_ts": self._now_kst.isoformat(),
            "freshness": "empty",
            "reconstructed": False,
        }
        logger.info("[EXIT][HOLDINGS][SOURCE] source=%s count=0", meta["source"])
        return [], meta

    def load_current_holdings_for_exit(
        self,
        balance_rows: Iterable[dict],
        ledger_positions: Iterable[dict] | None = None,
    ) -> list[HoldingContext]:
        holdings, meta = self.load_effective_holdings_for_exit(
            balance_rows,
            ledger_positions,
            as_of=self.get_as_of(),
            env=self.env,
            intended_live=self.intended_live,
            allow_http=bool(self.kis),
            balance_snapshot=self._balance_snapshot,
        )
        self._exit_holdings_meta = meta
        return holdings

    @staticmethod
    def _record_drop(
        counter: Counter[str],
        examples: Dict[str, list[str]],
        reason: str,
        code: str,
        *,
        limit: int = 3,
    ) -> None:
        if not reason:
            return
        counter[reason] += 1
        sample_list = examples.setdefault(reason, [])
        if len(sample_list) < limit:
            sample_list.append(code)

    def _log_order_skip(self, cf: CandidateFeature, reasons: list[str], stage: str) -> None:
        reason_codes = [_ORDER_SKIP_REASON_MAP.get(reason, f"ORDER_SKIP_{reason.upper()}") for reason in reasons]
        logger.info(
            "[PB1][ORDER][SKIP] code=%s reason_code=%s reasons=%s",
            self._display_code(cf.code),
            reason_codes,
            reasons,
        )
        try:
            self._append_ledger_event(
                event_type="ORDER_SKIP",
                code=cf.code,
                market=cf.market,
                mode=cf.mode,
                side="BUY",
                qty=cf.planned_qty,
                price=float(cf.features.get("close") or 0.0),
                client_order_key=cf.client_order_key,
                ok=False,
                reasons=reasons,
                stage=stage,
                payload_json={"features": cf.features},
            )
        except Exception:
            logger.exception("[PB1][LEDGER][SKIP_FAIL] code=%s", cf.code)

    @staticmethod
    def _is_retryable_entry_order_status(status: Any) -> bool:
        return str(status or "").upper() in {"", "CREATED", "INTENT", "ERROR", "REJECTED", "CANCELLED", "SKIP"}

    @staticmethod
    def _is_open_entry_order_status(status: Any) -> bool:
        return str(status or "").upper() in {"SUBMITTED", "ACKED", "ACCEPTED", "PARTIAL_FILLED", "FILLED"}

    def _build_unified_gate_context(
        self,
        *,
        code: str,
        qty: int,
        gate_snapshot: dict[str, Any] | None,
        open_order_exists: bool,
        duplicate_intent_exists: bool,
        duplicate_intent_status: str | None,
        blocking_duplicate_exists: bool,
    ) -> dict[str, Any]:
        snapshot = dict(gate_snapshot or {})
        return {
            "code": str(code or "").zfill(6),
            "dry_run": bool(self.dry_run),
            "intended_live": bool(self.intended_live),
            "order_allowed": bool(self.order_allowed),
            "qty": int(qty or 0),
            "holding_qty": int(snapshot.get("holding_qty") or 0),
            "kis_holding_qty": int(snapshot.get("kis_holding_qty") or snapshot.get("holding_qty") or 0),
            "today_buy_exists": bool(snapshot.get("today_buy_exists")),
            "today_submit_exists": bool(snapshot.get("today_submit_exists")),
            "today_fill_exists": bool(snapshot.get("today_fill_exists")),
            "today_sell_exists": bool(snapshot.get("today_sell_exists")),
            "open_order_exists": bool(open_order_exists or snapshot.get("open_order_exists")),
            "cooldown_active": bool(snapshot.get("cooldown_active")),
            "duplicate_intent_exists": bool(duplicate_intent_exists),
            "duplicate_intent_status": str(duplicate_intent_status or ""),
            "blocking_duplicate_exists": bool(blocking_duplicate_exists),
            "last_buy_event_at": snapshot.get("last_buy_event_at"),
            "last_fill_event_at": snapshot.get("last_fill_event_at"),
            "last_order_submit_at": snapshot.get("last_order_submit_at"),
            "phase": self.phase_name,
            "window": self.window_internal,
            "trade_date": self._today,
        }

    def _evaluate_unified_buyable_gate(
        self,
        *,
        code: str,
        gate_context: dict[str, Any],
        allow_add_to_existing: bool,
    ) -> UnifiedGateDecision:
        reason_codes: list[str] = []
        kis_holding_qty = int(gate_context.get("kis_holding_qty") or 0)
        if not allow_add_to_existing and kis_holding_qty > 0:
            reason_codes.append("BUYABLE_EXISTING_HOLDING_KIS")
            logger.info(
                "[BUYABLE_GATE][KIS_HOLDING] code=%s kis_qty=%s ok=0 reason=BUYABLE_EXISTING_HOLDING_KIS",
                self._display_code(code),
                kis_holding_qty,
            )
        if bool(gate_context.get("open_order_exists")):
            reason_codes.append("BUYABLE_OPEN_ORDER")
        if bool(gate_context.get("today_buy_exists")):
            reason_codes.append("BUYABLE_TODAY_BUY_EXISTS")
        # [2026-04-30] 당일 매도 후 재매수 차단
        if bool(gate_context.get("today_sell_exists")):
            block_rebuy = os.getenv("PB1_BLOCK_REBUY_AFTER_SELL_SAME_DAY", "1") not in {"0", "false", "False"}
            allow_override = os.getenv("PB1_ALLOW_SAME_DAY_REBUY_AFTER_SELL", "0") in {"1", "true", "True"}
            if block_rebuy and not allow_override:
                reason_codes.append("BUYABLE_TODAY_SELL_REBUY_BLOCKED")
                logger.info(
                    "[PB1][BUYABLE_GATE][TODAY_SELL_SRC] code=%s today_sell_exists=1 last_sell_at=%s",
                    self._display_code(code),
                    gate_context.get("last_sell_event_at"),
                )
        if bool(gate_context.get("cooldown_active")):
            reason_codes.append("BUYABLE_COOLDOWN")
        if bool(gate_context.get("blocking_duplicate_exists")):
            reason_codes.append("BUYABLE_DUPLICATE")
        ok = not reason_codes
        return UnifiedGateDecision(
            ok=ok,
            reason_codes=reason_codes or ["ok"],
            blocking_stage="shared",
            context=gate_context,
        )

    def _log_buyable_gate_unified(self, *, code: str, decision: UnifiedGateDecision) -> None:
        display_code = self._display_code(code)
        logger.info(
            "[PB1][BUYABLE_GATE][UNIFIED] code=%s ok=%s reasons=%s",
            display_code,
            int(bool(decision.ok)),
            decision.reason_codes,
        )
        logger.info(
            "[PB1][BUYABLE_GATE][CTX] code=%s holding_qty=%s kis_holding_qty=%s today_submit_exists=%s today_fill_exists=%s today_sell_exists=%s open_order_exists=%s cooldown_active=%s duplicate_intent_exists=%s last_order_submit_at=%s",
            display_code,
            decision.context.get("holding_qty", 0),
            decision.context.get("kis_holding_qty", 0),
            int(bool(decision.context.get("today_submit_exists"))),
            int(bool(decision.context.get("today_fill_exists"))),
            int(bool(decision.context.get("today_sell_exists"))),
            int(bool(decision.context.get("open_order_exists"))),
            int(bool(decision.context.get("cooldown_active"))),
            int(bool(decision.context.get("duplicate_intent_exists"))),
            decision.context.get("last_order_submit_at"),
        )

    def _resolve_entry_pre_submit(
        self,
        *,
        cf: CandidateFeature,
        stage: str,
        order_price: float,
        order_type: str,
        allow_add_to_existing: bool = False,
    ) -> UnifiedGateDecision:
        gate_snapshot = getattr(self, "_buyable_gate_context", {}).get(str(cf.code or "").zfill(6), {}) if hasattr(self, "_buyable_gate_context") else {}
        existing_order = None
        if hasattr(self.orders_repo, "get_order_by_client_order_key") and cf.client_order_key:
            existing_order = self.orders_repo.get_order_by_client_order_key(self.env, cf.client_order_key)
        existing_status = str((existing_order or {}).get("status") or "").upper()
        open_order_exists = bool(gate_snapshot.get("open_order_exists")) or self._is_open_entry_order_status(existing_status)
        duplicate_intent_exists = bool(existing_order)
        blocking_duplicate_exists = bool(open_order_exists)
        gate_context = self._build_unified_gate_context(
            code=cf.code,
            qty=int(cf.planned_qty or 0),
            gate_snapshot=gate_snapshot,
            open_order_exists=open_order_exists,
            duplicate_intent_exists=duplicate_intent_exists,
            duplicate_intent_status=existing_status,
            blocking_duplicate_exists=blocking_duplicate_exists,
        )
        shared_decision = self._evaluate_unified_buyable_gate(
            code=cf.code,
            gate_context=gate_context,
            allow_add_to_existing=allow_add_to_existing,
        )
        buyable_ok = bool(getattr(cf, "features", {}).get("buyable_ok", False))
        matched = int((not buyable_ok and not shared_decision.ok) or (buyable_ok == shared_decision.ok))
        logger.info(
            "[ORDER][PRE_SUBMIT][UNIFIED_MATCH] code=%s buyable_ok=%s submit_ok=%s matched=%s",
            self._display_code(cf.code),
            int(bool(buyable_ok)),
            int(bool(shared_decision.ok)),
            matched,
        )
        logger.info(
            "[ORDER][PRE_SUBMIT][CTX] code=%s dry_run=%s intended_live=%s order_allowed=%s qty=%s holding_qty=%s today_submit_exists=%s today_fill_exists=%s open_order_exists=%s cooldown_active=%s duplicate_intent_exists=%s",
            self._display_code(cf.code),
            int(bool(gate_context.get("dry_run"))),
            int(bool(gate_context.get("intended_live"))),
            int(bool(gate_context.get("order_allowed"))),
            gate_context.get("qty", 0),
            gate_context.get("holding_qty", 0),
            int(bool(gate_context.get("today_submit_exists"))),
            int(bool(gate_context.get("today_fill_exists"))),
            int(bool(gate_context.get("open_order_exists"))),
            int(bool(gate_context.get("cooldown_active"))),
            int(bool(gate_context.get("duplicate_intent_exists"))),
        )
        reason_codes = [] if shared_decision.ok else list(shared_decision.reason_codes)
        if not reason_codes:
            if int(cf.planned_qty or 0) <= 0:
                reason_codes.append("API_PAYLOAD_INVALID_QTY")
            if order_price <= 0:
                reason_codes.append("API_PAYLOAD_INVALID_PRICE")
        # [2026-04-30] tick budget 부족 시 주문 차단
        _block_order_lt = int(os.getenv("PB1_BLOCK_NEW_ORDER_WHEN_REMAINING_SEC_LT", "8"))
        _tick_remaining = getattr(self, "_tick_remaining_sec", None)
        if _tick_remaining is not None and float(_tick_remaining) < _block_order_lt:
            logger.warning(
                "[ORDER][BLOCK][LOW_TICK_BUDGET] code=%s remaining_sec=%.1f threshold=%s",
                self._display_code(cf.code), float(_tick_remaining), _block_order_lt,
            )
            reason_codes.append("LOW_TICK_BUDGET")
        gate_reasons = self._order_precheck_gate_reasons(side="BUY", stage=stage)
        for reason in gate_reasons:
            mapped = "LIVE_GATE_BLOCKED" if reason == "live_gate_blocked" else f"PRECHECK_{str(reason).upper()}"
            if mapped not in reason_codes:
                reason_codes.append(mapped)
        ok = not reason_codes
        logger.info(
            "[ORDER][PRE_SUBMIT][CHECK] code=%s ok=%s reasons=%s",
            self._display_code(cf.code),
            int(bool(ok)),
            reason_codes or ["ok"],
        )
        return UnifiedGateDecision(
            ok=ok,
            reason_codes=reason_codes or ["ok"],
            blocking_stage="shared" if not gate_reasons else "submit",
            context=gate_context,
        )

    def _log_final_skip(self, *, cf: CandidateFeature, reason_code: str, reason_detail: str, stage: str, price: float) -> None:
        logger.info(
            "[ORDER][FINAL_SKIP] code=%s reason_code=%s reason_detail=%s",
            self._display_code(cf.code),
            reason_code,
            reason_detail,
        )
        self._append_ledger_event(
            event_type="ORDER_SKIP",
            code=cf.code,
            market=cf.market,
            mode=cf.mode,
            side="BUY",
            qty=cf.planned_qty,
            price=price,
            client_order_key=cf.client_order_key,
            ok=False,
            reasons=[reason_code],
            stage=stage,
            payload_json={"reason_detail": reason_detail, "features": cf.features},
        )

    def _next_retry_client_order_key(self, client_order_key: str) -> str:
        if not client_order_key:
            return client_order_key
        suffix = 1
        candidate = client_order_key
        while hasattr(self.orders_repo, "has_client_order_key") and self.orders_repo.has_client_order_key(self.env, candidate):
            suffix += 1
            candidate = f"{client_order_key}:retry{suffix}"
        return candidate

    def _safe_score(self, cf: CandidateFeature) -> float:
        score_val = (
            getattr(cf, "score", None)
            or getattr(cf, "rank_score", None)
            or getattr(cf, "total_score", None)
            or cf.features.get("score")
        )
        if score_val is None:
            meta = getattr(cf, "meta", None)
            if isinstance(meta, dict):
                score_val = meta.get("score")
        if score_val is None:
            logger.warning("[PB1][BUY][SCORE_MISSING] code=%s -> fallback to 0.0", cf.code)
            return 0.0
        try:
            return float(score_val)
        except Exception:
            logger.warning(
                "[PB1][BUY][SCORE_INVALID] code=%s raw=%s -> fallback to 0.0",
                cf.code,
                score_val,
            )
            return 0.0

    def _emit_buy_decision(
        self,
        cf: CandidateFeature,
        *,
        order_value: float,
        reasons: list[str],
        entry_allowed: bool,
        entry_reason: str,
        stage: str | None = None,
    ) -> None:
        def _to_float(value: object) -> float:
            try:
                return float(value)
            except Exception:
                return 0.0

        price = cf.features.get("cap_price") or cf.features.get("close") or 0.0
        qty = int(cf.planned_qty or 0)
        if qty < 1:
            logger.warning(
                "[PB1][BUY][SKIP] code=%s reason=qty_zero qty=%s cap=%.0f",
                self._display_code(cf.code),
                qty,
                float(order_value or 0.0),
            )
            return
        buyable = entry_allowed and not reasons
        reasons_out = reasons if reasons else (["ok"] if entry_allowed else [entry_reason])
        payload = to_jsonable(
            {
                "code": cf.code,
                "market": cf.market,
                "score": self._safe_score(cf),
                "qty": qty,
                "price": _to_float(price),
                "notional": _to_float(order_value),
                "buyable": buyable,
                "reasons": reasons_out,
            }
        )
        ok, exc = emit_event(
            as_of=self._today,
            event="PB1_BUY_DECISION",
            **payload,
        )
        if not ok:
            logger.error(
                "[PB1][BUY][EVENT_FAIL] code=%s qty=%s cap=%.0f payload_keys=%s exc=%r",
                cf.code,
                qty,
                float(order_value or 0.0),
                sorted(payload.keys()),
                exc,
            )
        if not buyable:
            logger.info(
                "[PB1][BUY][SKIP] code=%s reasons=%s stage=%s",
                self._display_code(cf.code),
                reasons_out,
                stage or build_stage_label(session_kind=os.getenv("PB1_SESSION_KIND"), phase="entry"),
            )
        else:
            logger.info(
                "[PB1][BUY][INTENT] code=%s qty=%s price=%s reason=%s stage=%s",
                self._display_code(cf.code),
                qty,
                _to_float(price),
                "entry_ok",
                stage or build_stage_label(session_kind=os.getenv("PB1_SESSION_KIND"), phase="entry"),
            )

    def _fetch_holdings_snapshot(self) -> dict:
        # ✅ DIAG bypass: skip balance check when account params invalid
        skip_balance = os.getenv("SKIP_BALANCE_CHECK", "0") == "1"
        if skip_balance:
            logger.info("[BALANCE][SKIP] SKIP_BALANCE_CHECK=1 -> return empty snapshot")
            return {
                "output1": [],
                "output2": [{"dnca_tot_amt": "0", "scts_evlu_amt": "0"}],
            }
        
        if self._balance_snapshot is not None:
            self.balance_tick_cache_hits += 1
            source = self._balance_snapshot_source or "tick_cache"
            logger.info("[ENGINE][BALANCE_CACHE] hit=True source=%s", source)
            self._balance_snapshot_source = "tick_cache"
            return self._balance_snapshot
        if not self.kis:
            return {}
        snap, source = self.kis.get_balance_cached(return_source=True)
        if source == "api":
            self.balance_api_calls += 1
        else:
            self.balance_cache_hits += 1
        logger.info("[ENGINE][BALANCE_CACHE] hit=%s source=%s", source != "api", source)
        self._balance_snapshot = snap
        return self._balance_snapshot

    def _client_order_key(self, code: str, mode: int, side: str, window_tag: str, stage: str) -> str:
        """
        Dedupe key with full separation between EXIT/ENTRY passes.
        
        Format: {env}:{strategy}:{date}:{code}:{ACTION}:{SIDE}:{stage}:{window}:{mode}
        
        Example EXIT: live:pb1_pullback_close:2026-01-30:323280:EXIT:SELL:TP1:day:1
        Example ENTRY: live:pb1_pullback_close:2026-01-30:005930:ENTRY:BUY:PB1:day:1
        """
        # Determine action type from side and stage
        action = "EXIT" if side.upper() == "SELL" else "ENTRY"
        return f"{self.env}:{self.STRATEGY_NAME}:{self._today}:{code}:{action}:{side.upper()}:{stage}:{window_tag}:{mode}"

    def _name_for_code(self, code: str | None) -> str | None:
        if not code:
            return None
        return self._code_name_map.get(str(code).zfill(6))

    def _display_code(self, code: str | None) -> str:
        if not code:
            return ""
        name = self._name_for_code(code)
        return f"{name}({code})" if name else str(code)

    def _masked_account(self) -> str:
        account = str(os.getenv("CANO") or "").strip()
        if len(account) >= 4:
            return f"***{account[-4:]}"
        return "unknown"

    def _reconcile_positions_from_kis_balance(self, holdings_rows: list[dict], positions: list[dict]) -> list[dict]:
        if str(os.getenv("ACCOUNT_SANITY_RECONCILE_FROM_KIS", "0") or "0").strip() != "1":
            return positions
        logger.info(
            "[POSITIONS][RECONCILE][START] source=kis_balance env=%s account=%s",
            self.env,
            self._masked_account(),
        )
        db_by_code = {str((row or {}).get("code") or "").zfill(6): dict(row or {}) for row in (positions or [])}
        corrected_count = 0
        for row in holdings_rows or []:
            code = str(row.get("pdno") or row.get("code") or "").zfill(6)
            if not code:
                continue
            kis_qty = int(float(row.get("hldg_qty") or row.get("qty") or 0) or 0)
            db_row = db_by_code.get(code) or {}
            db_qty = int(db_row.get("qty") or 0)
            stock_name = str(row.get("prdt_name") or row.get("name") or self._name_for_code(code) or code)
            if kis_qty != db_qty:
                logger.warning(
                    "[POSITIONS][RECONCILE][MISMATCH] code=%s name=%s kis_qty=%s db_qty=%s action=upsert_from_kis",
                    code,
                    stock_name,
                    kis_qty,
                    db_qty,
                )
                avg_price = float(row.get("pchs_avg_pric") or row.get("pchs_avg_price") or row.get("avg_price") or 0.0)
                total_cost = float(row.get("pchs_amt") or row.get("total_cost") or (avg_price * kis_qty) or 0.0)
                market = row.get("prdt_type_cd") or row.get("market") or row.get("mket_gb")
                if db_row:
                    self.positions_repo.update_position_fields(
                        env=self.env,
                        strategy=self.STRATEGY_NAME,
                        sid=int(db_row.get("sid") or 1),
                        mode=int(db_row.get("mode") or 1),
                        code=code,
                        fields={
                            "qty": kis_qty,
                            "avg_buy_price": avg_price or None,
                            "total_cost": total_cost or 0.0,
                            "market": market,
                            "last_reconciled_at": now_kst(),
                        },
                    )
                elif kis_qty > 0:
                    self.positions_repo.bootstrap_from_kis_holdings(
                        env=self.env,
                        strategy=self.STRATEGY_NAME,
                        sid=1,
                        mode=1,
                        holdings=[row],
                    )
                corrected_count += 1
        refreshed = self.positions_repo.list_positions(self.env, self.STRATEGY_NAME)
        logger.info(
            "[POSITIONS][RECONCILE][OK] holdings=%s corrected=%s",
            len(holdings_rows or []),
            corrected_count,
        )
        return refreshed

    def _with_name_reason(self, reasons: list[str] | None, code: str | None) -> list[str]:
        enriched = list(reasons or [])
        name = self._name_for_code(code)
        if name:
            enriched.append(f"name:{name}")
        return enriched

    def _coerce_kst_datetime(self, value: Any) -> datetime | None:
        if value is None or value == "":
            return None
        if isinstance(value, datetime):
            return value.astimezone(KST) if value.tzinfo else value.replace(tzinfo=KST)
        if isinstance(value, date):
            return datetime.combine(value, datetime.min.time(), tzinfo=KST)
        text = str(value).strip()
        if not text:
            return None
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
        return parsed.astimezone(KST) if parsed.tzinfo else parsed.replace(tzinfo=KST)

    def _format_kst_datetime(self, value: Any) -> str | None:
        dt_value = self._coerce_kst_datetime(value)
        return dt_value.isoformat() if dt_value else None

    def _parse_cooldown_until(self, value: Any) -> datetime | None:
        if value is None or value == "":
            return None
        if isinstance(value, str):
            raw = value.strip()
            if not raw:
                return None
            if len(raw) == 10:
                try:
                    day_value = date.fromisoformat(raw)
                except ValueError:
                    return None
                return datetime.combine(day_value + timedelta(days=1), datetime.min.time(), tzinfo=KST)
        return self._coerce_kst_datetime(value)

    def _classify_ledger_event(self, event: dict[str, Any]) -> dict[str, bool]:
        event_type = str(event.get("event_type") or "").upper()
        side = str(event.get("side") or "").upper()
        payload = event.get("payload_json") if isinstance(event.get("payload_json"), dict) else {}
        filled_qty = int(payload.get("filled_qty") or payload.get("executed_qty") or event.get("qty") or 0)
        is_buy_execution_event = bool(side == "BUY" and event_type in LEDGER_BUY_EXECUTION_EVENT_TYPES and filled_qty > 0)
        is_buy_submission_event = bool(side == "BUY" and event_type in LEDGER_BUY_SUBMISSION_EVENT_TYPES and not is_buy_execution_event)
        is_skip_event = event_type in LEDGER_SKIP_EVENT_TYPES
        is_reconcile_event = event_type in LEDGER_RECONCILE_EVENT_TYPES
        starts_cooldown = bool(is_buy_execution_event and filled_qty > 0)
        logger.info(
            "[LEDGER][CLASSIFY] event_type=%s is_buy_execution=%s is_buy_submission=%s is_skip=%s is_reconcile=%s starts_cooldown=%s",
            event_type or "UNKNOWN",
            int(is_buy_execution_event),
            int(is_buy_submission_event),
            int(is_skip_event),
            int(is_reconcile_event),
            int(starts_cooldown),
        )
        return {
            "is_buy_execution_event": is_buy_execution_event,
            "is_buy_submission_event": is_buy_submission_event,
            "is_skip_event": is_skip_event,
            "is_reconcile_event": is_reconcile_event,
            "starts_cooldown": starts_cooldown,
        }

    def _normalize_today_buy_event(self, fill_row: dict[str, Any]) -> dict[str, Any]:
        created_at = self._coerce_kst_datetime(fill_row.get("filled_at") or fill_row.get("created_at"))
        return {
            "event_id": fill_row.get("fill_id") or fill_row.get("trade_id") or fill_row.get("broker_fill_id"),
            "event_type": next(iter(BUYABLE_GATE_TODAY_BUY_EVENT_TYPES)),
            "code": str(fill_row.get("code") or "").zfill(6),
            "side": str(fill_row.get("side") or "BUY").upper(),
            "status": "FILLED",
            "qty": int(fill_row.get("qty") or 0),
            "filled_qty": int(fill_row.get("qty") or 0),
            "created_at": created_at.isoformat() if created_at else None,
            "event_date_kst": created_at.date().isoformat() if created_at else None,
            "trade_date_ref": self.execution_date_kst,
            "run_id": fill_row.get("run_id"),
        }

    def _normalize_cooldown_event(self, fill_row: dict[str, Any], *, cooldown_until: str | None, matched_now: bool) -> dict[str, Any]:
        created_at = self._coerce_kst_datetime(fill_row.get("filled_at") or fill_row.get("created_at"))
        return {
            "event_id": fill_row.get("fill_id") or fill_row.get("trade_id") or fill_row.get("broker_fill_id"),
            "event_type": f"{str(fill_row.get('side') or 'TRADE').upper()}_FILL",
            "code": str(fill_row.get("code") or "").zfill(6),
            "created_at": created_at.isoformat() if created_at else None,
            "cooldown_until": cooldown_until,
            "cooldown_days": int(REENTRY_COOLDOWN_DAYS),
            "source_rule": BUYABLE_GATE_COOLDOWN_RULE,
            "matched_now": int(bool(matched_now)),
            "run_id": fill_row.get("run_id"),
        }

    def _buyable_gate_fail_open_enabled(self) -> bool:
        if self.strategy_mode == "DIAG":
            return True
        if env_bool("NO_TRADE", False):
            return True
        if str(os.getenv("MANUAL_MODE") or "").strip():
            return True
        if str(os.getenv("GITHUB_EVENT_NAME") or "").strip().lower() == "workflow_dispatch":
            return True
        workflow_name = str(os.getenv("GITHUB_WORKFLOW") or "").strip().lower()
        return "manual" in workflow_name or "dispatch" in workflow_name

    def _buyable_gate_max_sec(self) -> float:
        raw = str(os.getenv("PB1_BUYABLE_GATE_MAX_SEC") or "20").strip()
        try:
            return max(float(raw), 0.0)
        except ValueError:
            return 20.0

    def _buyable_gate_timeout_exceeded(self, *, code: str, started: float, max_sec: float) -> bool:
        if max_sec <= 0:
            return False
        elapsed = time.perf_counter() - started
        if elapsed <= max_sec:
            return False
        logger.warning(
            "[PB1][BUYABLE_GATE][TIMEOUT] code=%s elapsed=%.2f max_sec=%s -> skip_candidate",
            self._display_code(code),
            elapsed,
            max_sec,
        )
        return True

    def _build_candidate_buyable_gate_snapshot(
        self,
        *,
        code: str,
        position: dict[str, Any] | None,
        today_fills: list[dict[str, Any]] | None,
        prior_order: dict[str, Any] | None,
    ) -> dict[str, Any]:
        code_key = str(code or "").zfill(6)
        pos = dict(position or {})
        fill_rows = [dict(row) for row in (today_fills or [])]
        fill_rows_sorted = sorted(
            fill_rows,
            key=lambda row: self._coerce_kst_datetime(row.get("filled_at") or row.get("created_at")) or datetime.min.replace(tzinfo=KST),
            reverse=True,
        )
        today_buy_events = [
            self._normalize_today_buy_event(row)
            for row in fill_rows_sorted
            if str(row.get("side") or "BUY").upper() == "BUY"
        ]
        last_buy_fill = next(
            (row for row in fill_rows_sorted if str(row.get("side") or "BUY").upper() == "BUY"),
            None,
        )
        last_fill_event = fill_rows_sorted[0] if fill_rows_sorted else None
        cooldown_until_raw = str(pos.get("cooldown_until") or "") or None
        cooldown_until_dt = self._parse_cooldown_until(cooldown_until_raw)
        cooldown_until_value = cooldown_until_dt.isoformat() if cooldown_until_dt else cooldown_until_raw
        last_fill_event_at = self._format_kst_datetime((last_fill_event or {}).get("filled_at") if last_fill_event else None)
        cooldown_source_meta = _extract_cooldown_source_details([])
        cooldown_events = [
            self._normalize_cooldown_event(
                row,
                cooldown_until=cooldown_until_value,
                matched_now=False,
            )
            for row in fill_rows_sorted[:5]
        ] if cooldown_until_raw else []
        cooldown_state = self._resolve_buy_cooldown_state(
            code=code_key,
            cooldown_until=cooldown_until_value,
            holding_qty=int(pos.get("qty") or 0),
            today_buy_exists=bool(today_buy_events),
            today_fill_exists=bool(fill_rows_sorted),
            cooldown_source_events_count=len(cooldown_events),
            last_fill_event_at=last_fill_event_at,
            cooldown_source=cooldown_source_meta["source"],
            recent_valid_exit_event=bool(cooldown_source_meta["recent_valid_exit_event"]),
            recent_exit_reason=cooldown_source_meta["recent_exit_reason"],
        )
        cooldown_active = bool(cooldown_state["cooldown_active"])
        for event in cooldown_events:
            event["matched_now"] = cooldown_active
        return {
            "holding_qty": int(pos.get("qty") or 0),
            "today_buy_exists": bool(today_buy_events),
            "today_submit_exists": bool(prior_order),
            "today_fill_exists": bool(fill_rows_sorted),
            "open_order_exists": False,
            "cooldown_active": cooldown_active,
            "stale_cooldown_ignored": bool(cooldown_state["stale_ignored"]),
            "today_buy_events": today_buy_events,
            "cooldown_events": cooldown_events,
            "today_buy_source_events_count": len(today_buy_events),
            "cooldown_source_events_count": len(cooldown_events),
            "last_buy_event_at": self._format_kst_datetime((last_buy_fill or {}).get("filled_at") if last_buy_fill else None),
            "last_fill_event_at": last_fill_event_at,
            "last_order_submit_at": self._format_kst_datetime(
                (prior_order or {}).get("submitted_at")
                or (prior_order or {}).get("acked_at")
                or (prior_order or {}).get("created_at")
            ),
            "cooldown_until": cooldown_until_value,
            "cooldown_rule_name": BUYABLE_GATE_COOLDOWN_RULE,
            "cooldown_source": cooldown_state["cooldown_source"],
            "final_cooldown_policy": cooldown_state["final_cooldown_policy"],
            "code": code_key,
        }

    def _build_buyable_gate_context(self, *, codes: Iterable[str], positions: Iterable[dict[str, Any]]) -> dict[str, dict[str, Any]]:
        code_list = [str(code).zfill(6) for code in codes if str(code or "").strip()]
        if not code_list:
            logger.info("[PB1][BUYABLE_GATE][SUMMARY] total=0 today_buy_exists=0 cooldown_active=0")
            return {}

        today_start = self._now_kst.replace(hour=0, minute=0, second=0, microsecond=0)
        tomorrow_start = today_start + timedelta(days=1)
        lookback_days = max(int(REENTRY_COOLDOWN_DAYS), 1) + 7
        lookback_start = today_start - timedelta(days=lookback_days)

        today_buy_fills = self.fills_repo.list_fills_in_window(
            self.env,
            start_at=today_start,
            end_at=tomorrow_start,
            side="BUY",
            codes=code_list,
        )
        # [2026-04-30] 당일 SELL fills 조회 (same-day sell rebuy block)
        today_sell_fills = self.fills_repo.list_fills_in_window(
            self.env,
            start_at=today_start,
            end_at=tomorrow_start,
            side="SELL",
            codes=code_list,
        ) if os.getenv("PB1_BLOCK_REBUY_AFTER_SELL_SAME_DAY", "1") not in {"0", "false"} else []
        today_sell_fills_by_code: dict[str, list[dict]] = {}
        for row in today_sell_fills:
            c = str(row.get("code") or "").zfill(6)
            today_sell_fills_by_code.setdefault(c, []).append(row)
        recent_fill_rows = self.fills_repo.list_fills_in_window(
            self.env,
            start_at=lookback_start,
            end_at=tomorrow_start,
            codes=code_list,
        )
        recent_order_rows = self.orders_repo.list_orders_in_window(
            self.env,
            start_at=lookback_start,
            end_at=tomorrow_start,
            codes=code_list,
        )
        recent_ledger_rows = self.ledger_repo.list_events_in_window(
            self.env,
            start_at=lookback_start,
            end_at=tomorrow_start,
            codes=code_list,
        )

        position_by_code = {str(row.get("code") or "").zfill(6): dict(row) for row in positions if row.get("code")}
        kis_holding_qty_by_code: dict[str, int] = {}
        for row in (self._balance_snapshot.get("output1", []) if isinstance(self._balance_snapshot, dict) else []):
            code_key = str((row or {}).get("pdno") or (row or {}).get("code") or "").zfill(6)
            if not code_key:
                continue
            try:
                kis_holding_qty_by_code[code_key] = int(float((row or {}).get("hldg_qty") or (row or {}).get("qty") or 0))
            except Exception:
                continue
        today_buy_by_code: dict[str, list[dict[str, Any]]] = {}
        recent_fills_by_code: dict[str, list[dict[str, Any]]] = {}
        recent_orders_by_code: dict[str, list[dict[str, Any]]] = {}
        recent_ledger_by_code: dict[str, list[dict[str, Any]]] = {}

        for row in today_buy_fills:
            code_key = str(row.get("code") or "").zfill(6)
            today_buy_by_code.setdefault(code_key, []).append(dict(row))
        for row in recent_fill_rows:
            code_key = str(row.get("code") or "").zfill(6)
            recent_fills_by_code.setdefault(code_key, []).append(dict(row))
        for row in recent_order_rows:
            code_key = str(row.get("code") or "").zfill(6)
            recent_orders_by_code.setdefault(code_key, []).append(dict(row))
        for row in recent_ledger_rows:
            code_key = str(row.get("code") or "").zfill(6)
            recent_ledger_by_code.setdefault(code_key, []).append(dict(row))

        snapshots: dict[str, dict[str, Any]] = {}
        today_buy_count = 0
        cooldown_active_count = 0
        for code_key in code_list:
            pos = position_by_code.get(code_key, {})
            today_buy_events = [self._normalize_today_buy_event(row) for row in today_buy_by_code.get(code_key, [])]
            recent_fill_events = sorted(
                recent_fills_by_code.get(code_key, []),
                key=lambda row: self._coerce_kst_datetime(row.get("filled_at") or row.get("created_at")) or datetime.min.replace(tzinfo=KST),
                reverse=True,
            )
            recent_order_events = sorted(
                recent_orders_by_code.get(code_key, []),
                key=lambda row: self._coerce_kst_datetime(row.get("submitted_at") or row.get("acked_at") or row.get("created_at")) or datetime.min.replace(tzinfo=KST),
                reverse=True,
            )
            recent_ledger_events = sorted(
                recent_ledger_by_code.get(code_key, []),
                key=lambda row: self._coerce_kst_datetime(row.get("ts") or row.get("created_at")) or datetime.min.replace(tzinfo=KST),
                reverse=True,
            )
            for ledger_event in recent_ledger_events[:5]:
                self._classify_ledger_event(ledger_event)

            last_buy_fill = next((row for row in recent_fill_events if str(row.get("side") or "").upper() == "BUY"), None)
            last_fill_event = recent_fill_events[0] if recent_fill_events else None
            last_order_event = recent_order_events[0] if recent_order_events else None
            cooldown_until_raw = str(pos.get("cooldown_until") or "") or None
            cooldown_until_dt = self._parse_cooldown_until(cooldown_until_raw)
            last_fill_event_at = self._format_kst_datetime((last_fill_event or {}).get("filled_at") if last_fill_event else None)
            cooldown_source_meta = _extract_cooldown_source_details(recent_ledger_events[:5])
            cooldown_events = [
                self._normalize_cooldown_event(row, cooldown_until=cooldown_until_dt.isoformat() if cooldown_until_dt else cooldown_until_raw, matched_now=False)
                for row in recent_fill_events[:5]
            ] if cooldown_until_raw else []
            cooldown_state = self._resolve_buy_cooldown_state(
                code=code_key,
                cooldown_until=cooldown_until_dt.isoformat() if cooldown_until_dt else cooldown_until_raw,
                holding_qty=int(pos.get("qty") or 0),
                today_buy_exists=bool(today_buy_events),
                today_fill_exists=bool(recent_fill_events),
                cooldown_source_events_count=len(cooldown_events),
                last_fill_event_at=last_fill_event_at,
                cooldown_source=cooldown_source_meta["source"],
                recent_valid_exit_event=bool(cooldown_source_meta["recent_valid_exit_event"]),
                recent_exit_reason=cooldown_source_meta["recent_exit_reason"],
            )
            cooldown_active = bool(cooldown_state["cooldown_active"])
            for event in cooldown_events:
                event["matched_now"] = cooldown_active
            # [2026-04-30] 당일 매도 정보
            today_sell_rows = today_sell_fills_by_code.get(code_key, [])
            today_sell_exists = bool(today_sell_rows)
            last_sell_event_at = self._format_kst_datetime(
                today_sell_rows[0].get("filled_at") if today_sell_rows else None
            )
            snapshot = {
                "holding_qty": int(pos.get("qty") or 0),
                "kis_holding_qty": int(kis_holding_qty_by_code.get(code_key) or pos.get("qty") or 0),
                "today_buy_exists": bool(today_buy_events),
                "today_submit_exists": bool(recent_order_events),
                "today_fill_exists": bool(recent_fill_events),
                "today_sell_exists": today_sell_exists,
                "last_sell_event_at": last_sell_event_at,
                "open_order_exists": False,
                "cooldown_active": cooldown_active,
                "stale_cooldown_ignored": bool(cooldown_state["stale_ignored"]),
                "today_buy_events": today_buy_events,
                "cooldown_events": cooldown_events,
                "today_buy_source_events_count": len(today_buy_events),
                "cooldown_source_events_count": len(cooldown_events),
                "last_buy_event_at": self._format_kst_datetime((last_buy_fill or {}).get("filled_at") if last_buy_fill else None),
                "last_fill_event_at": last_fill_event_at,
                "last_order_submit_at": self._format_kst_datetime(
                    (last_order_event or {}).get("submitted_at")
                    or (last_order_event or {}).get("acked_at")
                    or (last_order_event or {}).get("created_at")
                    if last_order_event
                    else None
                ),
                "cooldown_until": cooldown_until_dt.isoformat() if cooldown_until_dt else cooldown_until_raw,
                "cooldown_rule_name": BUYABLE_GATE_COOLDOWN_RULE,
                "cooldown_source": cooldown_state["cooldown_source"],
                "final_cooldown_policy": cooldown_state["final_cooldown_policy"],
            }
            snapshots[code_key] = snapshot
            today_buy_count += int(snapshot["today_buy_exists"])
            cooldown_active_count += int(snapshot["cooldown_active"])

        logger.info(
            "[PB1][BUYABLE_GATE][SUMMARY] total=%s today_buy_exists=%s cooldown_active=%s",
            len(code_list),
            today_buy_count,
            cooldown_active_count,
        )
        return snapshots

    def _log_buyable_gate_trace(self, *, code: str, entry_allowed: bool, snapshot: dict[str, Any]) -> None:
        code_key = str(code or "").zfill(6)
        logger.info(
            "[BUYABLE_GATE][COMMON_PATH] func_name=%s code=%s window=%s phase=%s mode=%s intended_live=%s",
            "_build_buyable_gate_context",
            self._display_code(code_key),
            self.window_internal,
            self.phase_name,
            self.strategy,
            int(bool(self.intended_live)),
        )
        logger.info(
            "[PB1][BUYABLE_GATE][TRACE] code=%s env=%s now_kst=%s trade_date=%s derived_as_of=%s run_ctx_as_of=%s strategy_mode=%s window=%s phase=%s intended_live=%s entry_allowed=%s holding_qty=%s today_buy_exists=%s cooldown_active=%s today_buy_source_events_count=%s cooldown_source_events_count=%s last_buy_event_at=%s last_fill_event_at=%s last_order_submit_at=%s cooldown_until=%s cooldown_rule=%s",
            self._display_code(code_key),
            self.env,
            self._now_kst.isoformat(),
            self.execution_date_kst,
            self.selection_as_of,
            self.run_ctx_as_of,
            self.strategy,
            self.window_internal,
            self.phase_name,
            int(bool(self.intended_live)),
            int(bool(entry_allowed)),
            snapshot.get("holding_qty", 0),
            int(bool(snapshot.get("today_buy_exists"))),
            int(bool(snapshot.get("cooldown_active"))),
            snapshot.get("today_buy_source_events_count", 0),
            snapshot.get("cooldown_source_events_count", 0),
            snapshot.get("last_buy_event_at"),
            snapshot.get("last_fill_event_at"),
            snapshot.get("last_order_submit_at"),
            snapshot.get("cooldown_until"),
            snapshot.get("cooldown_rule_name"),
        )
        if self.debug and snapshot.get("today_buy_exists"):
            for event in snapshot.get("today_buy_events", []):
                logger.info(
                    "[PB1][BUYABLE_GATE][TODAY_BUY_EVENTS] event_id=%s event_type=%s code=%s side=%s status=%s qty=%s filled_qty=%s created_at=%s event_date_kst=%s trade_date_ref=%s run_id=%s",
                    event.get("event_id"),
                    event.get("event_type"),
                    event.get("code"),
                    event.get("side"),
                    event.get("status"),
                    event.get("qty"),
                    event.get("filled_qty"),
                    event.get("created_at"),
                    event.get("event_date_kst"),
                    event.get("trade_date_ref"),
                    event.get("run_id"),
                )
        if self.debug and snapshot.get("cooldown_active"):
            for event in snapshot.get("cooldown_events", []):
                logger.info(
                    "[PB1][BUYABLE_GATE][COOLDOWN_EVENTS] event_id=%s event_type=%s code=%s created_at=%s cooldown_until=%s cooldown_days=%s source_rule=%s matched_now=%s run_id=%s",
                    event.get("event_id"),
                    event.get("event_type"),
                    event.get("code"),
                    event.get("created_at"),
                    event.get("cooldown_until"),
                    event.get("cooldown_days"),
                    event.get("source_rule"),
                    event.get("matched_now"),
                    event.get("run_id"),
                )

    def _entry_gate(self, *, setup_filters_ok: bool, breakout_trigger_ok: bool) -> tuple[bool, list[str], str]:
        mode = (self.entry_cond_mode or "OR").strip().upper()
        reasons: list[str] = []

        if mode == "SETUP_ONLY":
            entry_ok = bool(setup_filters_ok)
            if not entry_ok:
                reasons.append("setup_filters_fail")
            return entry_ok, reasons, mode

        if mode == "TRIGGER_ONLY":
            entry_ok = bool(breakout_trigger_ok)
            if not entry_ok:
                reasons.append("breakout_trigger_fail")
            return entry_ok, reasons, mode

        if mode == "AND":
            entry_ok = bool(setup_filters_ok and breakout_trigger_ok)
            if not setup_filters_ok:
                reasons.append("setup_filters_fail")
            if not breakout_trigger_ok:
                reasons.append("breakout_trigger_fail")
            if not entry_ok:
                reasons.append("require_and_fail")
            return entry_ok, reasons, mode

        entry_ok = bool(setup_filters_ok or breakout_trigger_ok)
        if not entry_ok:
            if not setup_filters_ok:
                reasons.append("setup_filters_fail")
            if not breakout_trigger_ok:
                reasons.append("breakout_trigger_fail")
            reasons.append("require_or_fail")
        return entry_ok, reasons, mode

    @staticmethod
    def _resolve_entry_trigger_policy(
        *,
        trigger_ok: bool,
        entry_ok: bool,
        setup_filters_ok: bool,
        decision_family: str | None,
    ) -> str:
        if trigger_ok:
            return "BREAKOUT_CONFIRMED"
        if entry_ok and setup_filters_ok:
            family = str(decision_family or "").strip().upper()
            if family.endswith("PULLBACK_OVERRIDE"):
                return "PULLBACK_OVERRIDE"
            if family.endswith("MOMENTUM_CONTINUATION"):
                return "MOMENTUM_CONTINUATION"
            if family.endswith("SCORE_OVERRIDE"):
                return "SCORE_OVERRIDE"
            return "SETUP_OVERRIDE"
        return "NONE"

    @staticmethod
    def _enforce_explicit_trigger_bypass(
        *,
        entry_ok: bool,
        trigger_ok: bool,
        trigger_policy: str,
        reasons: list[str] | None,
    ) -> tuple[bool, list[str]]:
        resolved_reasons = list(reasons or [])
        if os.getenv("PB1_REQUIRE_EXPLICIT_TRIGGER_BYPASS", "0") != "1":
            return entry_ok, resolved_reasons
        if trigger_ok or not entry_ok:
            return entry_ok, resolved_reasons
        if str(trigger_policy or "NONE").strip().upper() == "NONE":
            resolved_reasons.append("explicit_trigger_bypass_required")
            return False, resolved_reasons
        return entry_ok, resolved_reasons

    @staticmethod
    def _is_precomputed_order_context_usable(cf: Any) -> bool:
        """precomputed feature + stop 계산이 충분해 trade 판단 가능한지 확인.

        필수: close/last_close/current_price, atr/atr_pct, ma20, ma50, ma150,
              rs_percentile/rs_pctile, entry_style_selected, qty>0/planned_qty>0
        """
        features = getattr(cf, "features", None) or {}

        def _has(key: str) -> bool:
            v = features.get(key)
            return v is not None and str(v).strip() not in ("", "None", "nan")

        has_close = _has("close") or _has("last_close") or _has("current_price")
        has_atr = _has("atr") or _has("atr14") or _has("atr_pct")
        has_ma = _has("ma20") and _has("ma50") and _has("ma150")
        has_rs = _has("rs_percentile") or _has("rs_pctile")
        has_style = _has("entry_style_selected")
        has_qty = (
            int(getattr(cf, "planned_qty", 0) or 0) > 0
            or int(features.get("qty") or 0) > 0
        )
        return all([has_close, has_atr, has_ma, has_rs, has_style, has_qty])

    @staticmethod
    def _is_kr_pb1_precomputed_trade_ok(
        cf: Any,
        ohlcv_df: "pd.DataFrame | None",
    ) -> bool:
        """KR PB1 final30 precomputed 경로 신규 BUY에서 OHLCV 단기 완화 허용 여부.

        조건을 모두 만족해야 True:
        1. KR 6자리 숫자 종목코드
        2. mode_reasons에 'pb1_from_final30' 포함 (final30 locked source 경로)
        3. ohlcv rows >= 60 (신규상장 방어 최솟값)
        4. 필수 precomputed feature 11개 모두 존재하고 비어있지 않음
        5. close > 0, atr_pct > 0, ma20/ma50/ma150 > 0

        미국장 / 해외장 / US PB1 / 일반 전략에는 절대 적용하지 않음.
        risk_gate / sizing_gate / buyable_gate는 이 함수가 우회하지 않음.
        """
        if cf is None:
            return False

        # 1. KR 종목코드 판별 (6자리 숫자)
        code = str(getattr(cf, "code", "") or "")
        if not _is_kr_stock_code(code):
            return False

        # 2. final30 경로 확인 (pb1_from_final30 mode_reason)
        mode_reasons = list(getattr(cf, "mode_reasons", None) or [])
        if "pb1_from_final30" not in mode_reasons:
            return False

        # 3. OHLCV rows >= 60 (신규상장 방어)
        if ohlcv_df is None or len(ohlcv_df) < 60:
            return False

        # 4. 필수 precomputed feature 존재 및 비어있지 않음
        features = getattr(cf, "features", None) or {}
        _REQUIRED_PRECOMPUTED = (
            "close",
            "ma20",
            "ma50",
            "ma150",
            "atr_pct",
            "rs_percentile",
            "vcp_score",
            "breakout_score",
            "pullback_score",
            "momentum_score",
            "entry_style_selected",
        )
        for col in _REQUIRED_PRECOMPUTED:
            v = features.get(col)
            if v is None or str(v).strip() in ("", "None", "nan"):
                return False

        # 5. 핵심 수치 유효성 검사
        try:
            close_val = float(features.get("close") or 0)
            atr_pct_val = float(features.get("atr_pct") or 0)
            ma20_val = float(features.get("ma20") or 0)
            ma50_val = float(features.get("ma50") or 0)
            ma150_val = float(features.get("ma150") or 0)
        except (ValueError, TypeError):
            return False

        if close_val <= 0 or atr_pct_val <= 0:
            return False
        if ma20_val <= 0 or ma50_val <= 0 or ma150_val <= 0:
            return False

        return True

    @staticmethod
    def _entry_ohlcv_block_reason(
        *,
        df: "pd.DataFrame",
        meta: "dict[str, Any] | None",
        cf: Any = None,
        stop0: "float | None" = None,
        order_price: float = 0.0,
    ) -> "str | None":
        """OHLCV 부족 시 주문 차단 여부 결정.

        새 정책 (Fix 2/3):
        - PB1_BLOCK_INSUFFICIENT_OHLCV=0 (기본) → 기존처럼 차단 안함.
        - PB1_BLOCK_INSUFFICIENT_OHLCV=1 이지만 precomputed_ok=1 + stop_ready=1 이면
          PB1_BLOCK_INSUFFICIENT_OHLCV_WHEN_PRECOMPUTED_OK=0 (기본) 시 차단 안함.
        - 신규상장 수준 (rows < PB1_MIN_OHLCV_ROWS_FOR_ENTRY=60) 이면 항상 차단.

        KR PB1 final30 precomputed 완화:
        - rows >= 60 + long_fetch_blocked + final30 경로 + 필수 feature 모두 OK →
          insufficient_ohlcv hard block 제거, warning/degraded 로 완화.
        """
        block_enabled = os.getenv("PB1_BLOCK_INSUFFICIENT_OHLCV", "0") == "1"
        if not block_enabled:
            return None

        source = str((meta or {}).get("source") or "").strip().lower()
        # 신규상장 수준: 절대 최소 rows (기본 60)
        abs_min_rows = int(os.getenv("PB1_MIN_OHLCV_ROWS_FOR_ENTRY", "60") or "60")
        actual_rows = len(df) if (df is not None and not df.empty) else 0

        # 신규상장 수준이면 precomputed 여부와 무관하게 차단 (rows < 60)
        if actual_rows < abs_min_rows:
            return "insufficient_ohlcv"

        # ── [KR][PB1] final30 precomputed trade bypass ──────────────────────
        # 한국장 PB1 trade-am/trade-afternoon: final30 precomputed 피처가 충분하면
        # db_short_only / long_fetch_blocked 상태여도 주문 후보 생성 허용.
        # 미국장 / 해외장 / US PB1 / 일반 전략에는 절대 적용하지 않음.
        # risk_gate / sizing_gate / buyable_gate는 우회하지 않음.
        _long_blocked_meta = bool((meta or {}).get("long_fetch_blocked", 0) or 0)
        if _long_blocked_meta and PB1Engine._is_kr_pb1_precomputed_trade_ok(cf=cf, ohlcv_df=df):
            _code_val = str(getattr(cf, "code", "") or "")
            _requested_days = int(os.getenv("PB1_OHLCV_DAYS_BASE", "200") or "200")
            logger.info(
                "[KR][PB1][ORDER][OHLCV_SHORT_BUT_PRECOMPUTED_OK] "
                "code=%s rows=%s requested=%s action=continue",
                _code_val,
                actual_rows,
                _requested_days,
            )
            return None

        # Legacy bypass: MODE + PB1_SESSION_KIND 환경변수 기반 (하위호환)
        if cf is not None:
            _code_val_leg = str(getattr(cf, "code", "") or "")
            _session = str(os.getenv("PB1_SESSION_KIND", "")).strip().lower()
            _mode = str(os.getenv("MODE", "")).strip().lower()
            _is_kr_trade_precomputed = (
                _mode == "trade"
                and _is_kr_stock_code(_code_val_leg)
                and _session in ("am", "afternoon", "pm")
                and _long_blocked_meta
            )
            if _is_kr_trade_precomputed:
                _precomputed_ok = PB1Engine._is_precomputed_order_context_usable(cf)
                if _precomputed_ok:
                    logger.info(
                        "[KR][PB1][ORDER][OHLCV_SHORT_BUT_PRECOMPUTED_OK] code=%s rows=%s requested=%s action=continue reason=legacy_env_bypass",
                        _code_val_leg,
                        actual_rows,
                        abs_min_rows,
                    )
                    return None

        # db source가 명시적 부족/차단 상태이면 차단
        if source.startswith("db_insufficient") or source in {"db_short_only", "db_blocked"}:
            return "insufficient_ohlcv"

        # long_fetch_blocked=True 이거나 required_rows 미달인 경우
        long_blocked = bool((meta or {}).get("long_fetch_blocked", 0) or 0)
        required_rows = max(int(PB1_MIN_CANDLES or 0), 120)
        rows_short = (df is None or df.empty or len(df) < required_rows) or long_blocked

        if not rows_short:
            return None

        # PB1_BLOCK_INSUFFICIENT_OHLCV_WHEN_PRECOMPUTED_OK=1 이면 기존처럼 무조건 차단
        block_when_precomputed_ok = (
            os.getenv("PB1_BLOCK_INSUFFICIENT_OHLCV_WHEN_PRECOMPUTED_OK", "0") == "1"
        )
        if block_when_precomputed_ok:
            return "insufficient_ohlcv"

        # precomputed_ok + stop_ready 이면 경고만 하고 차단하지 않음
        precomputed_ok = False
        stop_ready = False
        if cf is not None:
            precomputed_ok = PB1Engine._is_precomputed_order_context_usable(cf)
            stop_ready = (
                stop0 is not None
                and float(stop0) > 0
                and order_price > 0
                and float(stop0) < float(order_price)
            )
        if precomputed_ok and stop_ready:
            logger.warning(
                "[PB1][ORDER][OHLCV_SHORT_BUT_PRECOMPUTED_OK] code=%s rows=%s full_required=%s action=continue",
                getattr(cf, "code", "unknown"),
                actual_rows,
                required_rows,
            )
            return None

        return "insufficient_ohlcv"

    def _log_entry_gate(
        self,
        *,
        code: str,
        setup_filters_ok: bool,
        breakout_trigger_ok: bool,
        entry_ok: bool,
        entry_mode: str,
        setup_metrics: dict,
        trigger_metrics: dict,
    ) -> None:
        if not self.log_entry_gate:
            return
        prefix = "[PB1][ENTRY][SETUP-OK]" if entry_ok else "[PB1][ENTRY][SETUP-BAD]"
        logger.info(
            "%s code=%s setup_filters_ok=%s breakout_trigger_ok=%s entry_cond_mode=%s should_buy=%s setup_def=%s trigger_def=%s setup_metrics=%s trigger_metrics=%s",
            prefix,
            self._display_code(code),
            int(bool(setup_filters_ok)),
            int(bool(breakout_trigger_ok)),
            entry_mode,
            int(bool(entry_ok)),
            "trend_template+rs+vcp+liquidity+score_or_pullback_override",
            "pivot_breakout+volume",
            setup_metrics,
            trigger_metrics,
        )

    def _log_buyable_gate(self, *, code: str, ok: bool, reasons: list[str]) -> None:
        snapshot = getattr(self, "_buyable_gate_context", {}).get(str(code).zfill(6), {}) if hasattr(self, "_buyable_gate_context") else {}
        logger.info(
            "[PB1][BUYABLE_GATE] code=%s ok=%s reasons=%s cooldown_until=%s",
            self._display_code(code),
            int(bool(ok)),
            reasons or ["ok"],
            snapshot.get("cooldown_until"),
        )
        logger.info(
            "[PB1][BUYABLE_GATE][FINAL] code=%s ok=%s reasons=%s final_cooldown_policy=%s",
            self._display_code(code),
            int(bool(ok)),
            reasons or ["ok"],
            snapshot.get("final_cooldown_policy", "none"),
        )

    def _store_entry_evaluation(
        self,
        cf: CandidateFeature,
        *,
        setup_ok: bool,
        score_ok: bool,
        risk_ok: bool,
        sizing_ok: bool,
        buyable_ok: bool,
        trigger_ok: bool,
        order_ready: bool,
        reasons: list[str] | None,
        decision_reason: str | None = None,
    ) -> dict[str, Any]:
        evaluation = build_entry_evaluation(
            code=cf.code,
            as_of=self.get_as_of(),
            trade_date=self._trade_date,
            input_source="final30_locked" if self.final30_locked else (self.final30_source or "watchlist"),
            setup_ok=setup_ok,
            score_ok=score_ok,
            risk_ok=risk_ok,
            sizing_ok=sizing_ok,
            buyable_ok=buyable_ok,
            trigger_ok=trigger_ok,
            order_ready=order_ready,
            reasons=reasons,
            setup_family=cf.features.get("entry_setup_family") or cf.features.get("entry_reason") or cf.features.get("entry_style_selected"),
            decision_reason=decision_reason,
            features=cf.features,
        )
        self._entry_eval_by_code[cf.code] = evaluation
        self._entry_evaluations = list(self._entry_eval_by_code.values())
        logger.info(
            "[ENTRY][EVAL] code=%s setup_ok=%s score_ok=%s risk_ok=%s sizing_ok=%s buyable_ok=%s trigger_ok=%s order_ready=%s family=%s reason=%s",
            self._display_code(cf.code),
            int(bool(evaluation.get("setup_ok"))),
            int(bool(evaluation.get("score_ok"))),
            int(bool(evaluation.get("risk_ok"))),
            int(bool(evaluation.get("sizing_ok"))),
            int(bool(evaluation.get("buyable_ok"))),
            int(bool(evaluation.get("trigger_ok"))),
            int(bool(evaluation.get("order_ready"))),
            evaluation.get("decision_family"),
            evaluation.get("decision_reason"),
        )
        if order_ready:
            logger.info(
                "[ENTRY][ORDER_READY] code=%s family=%s trigger_policy=%s reason=%s",
                self._display_code(cf.code),
                evaluation.get("decision_family"),
                evaluation.get("entry_trigger_policy"),
                evaluation.get("decision_reason"),
            )
        else:
            logger.info(
                "[ENTRY][SKIP] code=%s family=%s reason=%s",
                self._display_code(cf.code),
                evaluation.get("decision_family"),
                evaluation.get("decision_reason"),
            )
        return evaluation

    def _resolve_entry_setup_family(self, cf: CandidateFeature) -> str:
        selected_family = self._normalize_entry_reason(cf.features.get("entry_style_selected") or cf.features.get("entry_signal"))
        score_lookup = {
            "ENTRY_BREAKOUT": float(cf.features.get("breakout_score") or 0.0),
            "ENTRY_PULLBACK": float(cf.features.get("pullback_score") or 0.0),
            "ENTRY_MOMENTUM": float(cf.features.get("momentum_score") or 0.0),
        }
        if selected_family in score_lookup and score_lookup[selected_family] > 0:
            return selected_family
        strongest_family = max(score_lookup.items(), key=lambda item: item[1])[0]
        if score_lookup[strongest_family] > 0:
            return strongest_family
        return "ENTRY_GENERIC"

    @staticmethod
    def _normalize_entry_reason(value: Any) -> str:
        raw = str(value or "").strip().upper()
        if raw in {"ENTRY_BREAKOUT", "BREAKOUT", "ENTRY_BREAKOUT_CONFIRMED"}:
            return "ENTRY_BREAKOUT"
        if raw in {"ENTRY_PULLBACK", "PULLBACK", "ENTRY_PULLBACK_OVERRIDE"}:
            return "ENTRY_PULLBACK"
        if raw in {"ENTRY_MOMENTUM", "MOMENTUM", "ENTRY_MOMENTUM_CONTINUATION"}:
            return "ENTRY_MOMENTUM"
        return "ENTRY_GENERIC"

    @classmethod
    def _resolve_entry_decision_family(
        cls,
        *,
        entry_reason: str,
        setup_filters_ok: bool,
        breakout_trigger_ok: bool,
        trigger_reason: Any = None,
    ) -> str:
        normalized_reason = cls._normalize_entry_reason(entry_reason)
        trigger_reason_s = str(trigger_reason or "").strip().lower()
        if breakout_trigger_ok:
            return "ENTRY_BREAKOUT_CONFIRMED"
        if normalized_reason == "ENTRY_PULLBACK" and setup_filters_ok:
            return "ENTRY_PULLBACK_OVERRIDE"
        if normalized_reason == "ENTRY_MOMENTUM" and setup_filters_ok:
            return "ENTRY_MOMENTUM_CONTINUATION"
        if setup_filters_ok:
            if "score" in trigger_reason_s:
                return "ENTRY_SCORE_OVERRIDE"
            return "ENTRY_SETUP_OVERRIDE"
        if "score" in trigger_reason_s:
            return "ENTRY_SCORE_OVERRIDE"
        return "ENTRY_SETUP_OVERRIDE"

    @classmethod
    def _resolve_exit_family(cls, entry_reason: Any, entry_style_selected: Any) -> tuple[str, str]:
        normalized_reason = cls._normalize_entry_reason(entry_reason or entry_style_selected)
        if normalized_reason == "ENTRY_BREAKOUT":
            return normalized_reason, "BREAKOUT_EXIT"
        if normalized_reason == "ENTRY_PULLBACK":
            return normalized_reason, "PULLBACK_EXIT"
        if normalized_reason == "ENTRY_MOMENTUM":
            return normalized_reason, "MOMENTUM_EXIT"
        return normalized_reason, "GENERIC_EXIT"

    def _build_entry_metadata(
        self,
        cf: CandidateFeature,
        *,
        entry_price_planned: float,
        entry_price_filled: float | None = None,
    ) -> dict[str, Any]:
        identity = self._resolve_entry_identity_for_candidate(cf)
        entry_reason = identity["entry_reason"]
        score_lookup = {
            "ENTRY_BREAKOUT": cf.features.get("breakout_score"),
            "ENTRY_PULLBACK": cf.features.get("pullback_score"),
            "ENTRY_MOMENTUM": cf.features.get("momentum_score"),
        }
        normalized_entry_style = identity["entry_style_selected"]
        resolved_entry_reason = identity["entry_reason"]
        exit_family = identity["exit_policy_family"]
        trace_id = str(cf.features.get("trace_id") or f"{self.run_id or 'NORUN'}:{self._today}:{cf.code}:{cf.mode}")
        missing_reference_fields = [
            key
            for key in ("hi_52w", "vol20", "ma200")
            if cf.features.get(key) in (None, "", 0, 0.0)
        ]
        return to_jsonable(
            {
                "trace_id": trace_id,
                "entry_reason": resolved_entry_reason,
                "entry_style_selected": normalized_entry_style,
                "entry_decision_family": identity["entry_decision_family"],
                "entry_component": cf.features.get("entry_component") or cf.features.get("entry_signal") or normalized_entry_style,
                "entry_signal_score": score_lookup.get(resolved_entry_reason),
                "score_final_at_entry": cf.features.get("score_final") or cf.features.get("final_score") or cf.features.get("score"),
                "breakout_score_at_entry": cf.features.get("breakout_score"),
                "pullback_score_at_entry": cf.features.get("pullback_score"),
                "momentum_score_at_entry": cf.features.get("momentum_score"),
                "rs_percentile_at_entry": cf.features.get("rs_percentile") or cf.features.get("rs_percentile"),
                "vcp_score_at_entry": cf.features.get("vcp_score"),
                "atr_pct_at_entry": cf.features.get("atr_pct"),
                "entry_price_planned": entry_price_planned,
                "entry_price_filled": entry_price_filled,
                "stop_price_at_entry": cf.features.get("stop_price") or cf.features.get("initial_stop"),
                "pivot_price_at_entry": cf.features.get("pivot_triggered") or cf.features.get("pivot"),
                "pullback_pct_at_entry": cf.features.get("pullback_pct"),
                "ma20_at_entry": cf.features.get("ma20"),
                "ma50_at_entry": cf.features.get("ma50"),
                "ma150_at_entry": cf.features.get("ma150"),
                "derived_as_of": cf.features.get("derived_as_of") or self.get_as_of(),
                "trade_date": self._today,
                "setup_snapshot_json": cf.features.get("setup_snapshot_json") or {},
                "trigger_snapshot_json": cf.features.get("trigger_snapshot_json") or {},
                "entry_rule_version": cf.features.get("entry_rule_version") or "pb1_entry_reason_v1",
                "exit_policy_family": _horizon_to_exit_family(
                    _classify_trade_horizon(dict(cf.features))
                ),
                "trade_horizon": _classify_trade_horizon(dict(cf.features)),
                "initial_stop_price": cf.features.get("stop_price") or cf.features.get("initial_stop"),
                "entry_reason_source": "candidate_features",
                "setup_snapshot_missing_fields": missing_reference_fields,
            }
        )


    def _prepare_entry_exit_plan(self, cf: CandidateFeature, *, entry_price_for_plan: float) -> tuple[dict[str, Any], dict[str, Any]] | None:
        identity = self._resolve_entry_identity_for_candidate(cf)
        entry_style_selected = (
            cf.features.get("entry_style_selected")
            or cf.features.get("entry_setup_family")
            or cf.features.get("decision_family")
            or identity.get("entry_style_selected")
            or identity.get("entry_reason")
        )
        entry_reason = (
            cf.features.get("entry_reason")
            or cf.features.get("decision_reason")
            or identity.get("entry_reason")
            or entry_style_selected
        )
        try:
            plan = build_entry_exit_plan(
                code=cf.code,
                market=cf.market,
                entry_style_selected=str(entry_style_selected or ""),
                entry_reason=str(entry_reason or entry_style_selected or ""),
                entry_price=float(entry_price_for_plan or 0.0),
                features=cf.features,
            )
            plan_dict = plan.to_dict()
        except Exception as exc:
            logger.warning(
                "[PB1][ENTRY_PLAN][MISSING_OR_INVALID] code=%s entry_style=%s entry_reason=%s err=%s action=skip_buy",
                cf.code, entry_style_selected, entry_reason, exc,
            )
            self._append_ledger_event(
                event_type="ORDER_SKIP",
                code=cf.code,
                market=cf.market,
                mode=cf.mode,
                side="BUY",
                qty=cf.planned_qty,
                price=float(entry_price_for_plan or 0.0),
                client_order_key=cf.client_order_key,
                ok=False,
                reasons=["ENTRY_EXIT_PLAN_MISSING_OR_INVALID"],
                stage="ENTRY_PLAN_VALIDATE",
                payload_json={
                    "features": cf.features,
                    "entry_style_selected": entry_style_selected,
                    "entry_reason": entry_reason,
                    "error": str(exc),
                },
            )
            return None
        risk = plan_dict.get("risk_plan") or {}
        time_plan = plan_dict.get("time_plan") or {}
        entry_meta = {
            "entry_thesis": plan_dict.get("entry_thesis"),
            "entry_style_selected": plan_dict.get("entry_style_selected"),
            "entry_reason": plan_dict.get("entry_reason"),
            "trade_horizon": plan_dict.get("trade_horizon"),
            "exit_policy_family": plan_dict.get("exit_policy_family"),
            "eod_action": plan_dict.get("eod_action"),
            "force_eod_close": plan_dict.get("force_eod_close"),
            "initial_stop_price": risk.get("initial_stop"),
            "initial_risk_r": risk.get("risk_R"),
            "max_trading_days": time_plan.get("max_trading_days"),
            "policy_source": plan_dict.get("policy_source"),
            "policy_version": plan_dict.get("policy_version"),
        }
        logger.info(
            "[PB1][ENTRY_PLAN][READY] code=%s thesis=%s style=%s horizon=%s exit_family=%s eod_action=%s force_eod=%s stop=%.2f risk_R=%.2f max_days=%s version=%s",
            cf.code, plan_dict.get("entry_thesis"), plan_dict.get("entry_style_selected"),
            plan_dict.get("trade_horizon"), plan_dict.get("exit_policy_family"), plan_dict.get("eod_action"),
            int(bool(plan_dict.get("force_eod_close"))), float(risk.get("initial_stop") or 0.0),
            float(risk.get("risk_R") or 0.0), time_plan.get("max_trading_days"), plan_dict.get("policy_version"),
        )
        return plan_dict, entry_meta

    def _portfolio_risk_diag_settings(self) -> dict[str, Any]:
        return {
            "sector_max_positions": self._int_env("PB1_SECTOR_MAX_POSITIONS", 0),
            "theme_max_weight": self._float_env("PB1_THEME_MAX_WEIGHT", 0.0),
            "daily_new_entry_limit_by_regime": self._int_env("PB1_DAILY_NEW_ENTRY_LIMIT_BY_REGIME", 0),
            "account_drawdown_new_entry_block": self._float_env("PB1_ACCOUNT_DRAWDOWN_NEW_ENTRY_BLOCK", 0.0),
            "weekly_loss_limit": self._float_env("PB1_WEEKLY_LOSS_LIMIT", 0.0),
        }

    def _log_portfolio_risk_diagnostics(
        self,
        *,
        existing_positions: list[dict[str, Any]],
        total_cash_krw: float,
        available_cash_krw: float,
        target_new_positions: int,
    ) -> None:
        settings = self._portfolio_risk_diag_settings()
        sector_counts: Counter[str] = Counter()
        theme_weights: Counter[str] = Counter()
        total_position_cost = 0.0
        for row in existing_positions or []:
            meta = row.get("entry_meta_json") if isinstance(row.get("entry_meta_json"), dict) else {}
            sector = str(meta.get("sector") or row.get("sector") or "").strip()
            theme = str(meta.get("theme") or row.get("theme") or "").strip()
            cost = float(row.get("total_cost") or 0.0)
            total_position_cost += cost
            if sector:
                sector_counts[sector] += 1
            if theme:
                theme_weights[theme] += cost
        denominator = max(float(total_cash_krw) + float(total_position_cost), 1.0)
        normalized_theme_weights = {
            key: round(float(value) / denominator, 4)
            for key, value in theme_weights.items()
        }
        logger.info(
            "[RISK][DIAG] sector_max_positions=%s theme_max_weight=%s daily_new_entry_limit_by_regime=%s account_drawdown_new_entry_block=%s weekly_loss_limit=%s enforced=0",
            settings["sector_max_positions"],
            settings["theme_max_weight"],
            settings["daily_new_entry_limit_by_regime"],
            settings["account_drawdown_new_entry_block"],
            settings["weekly_loss_limit"],
        )
        logger.info(
            "[RISK][DIAG][PORTFOLIO] positions=%s total_cash_krw=%s available_cash_krw=%s target_new_positions=%s sector_counts=%s theme_weights=%s",
            len(existing_positions or []),
            int(total_cash_krw or 0),
            int(available_cash_krw or 0),
            int(target_new_positions or 0),
            dict(sector_counts),
            normalized_theme_weights,
        )

    def _resolve_force_exit_simulation(self, *, code: str, orderable_qty: int, exit_policy_family: str) -> dict[str, Any] | None:
        if not env_bool("FORCE_EXIT_SIMULATION", False):
            return None
        requested_code = str(os.getenv("FORCE_EXIT_CODE") or "").strip().zfill(6)
        if requested_code and requested_code != str(code or "").zfill(6):
            return None
        requested_reason = str(os.getenv("FORCE_EXIT_REASON") or "STOP_HIT").strip().upper() or "STOP_HIT"
        valid_reasons = {"STOP_HIT", "FAILED_BREAKOUT", "TP1", "TP2", "TRAIL_STOP"}
        if requested_reason not in valid_reasons:
            requested_reason = "STOP_HIT"
        qty = int(orderable_qty or 0)
        if requested_reason == "TP1":
            qty = max(1, int(np.ceil(float(orderable_qty or 0) * float(TP1_SELL_PCT))))
        elif requested_reason == "TP2":
            qty = max(1, int(np.ceil(float(orderable_qty or 0) * float(TP2_SELL_PCT))))
        family_hint = exit_policy_family or "GENERIC_EXIT"
        if requested_reason == "FAILED_BREAKOUT":
            family_hint = "PULLBACK_EXIT"
        elif requested_reason in {"TP1", "TP2", "TRAIL_STOP"}:
            family_hint = "MOMENTUM_EXIT"
        return {
            "reason": requested_reason,
            "qty": min(max(1, qty), max(1, int(orderable_qty or 0))),
            "stage": requested_reason,
            "exit_policy_family": family_hint,
        }

    @staticmethod
    def _entry_meta_position_fields(entry_meta: dict[str, Any]) -> dict[str, Any]:
        return {
            "entry_reason": entry_meta.get("entry_reason"),
            "entry_style_selected": entry_meta.get("entry_style_selected"),
            "entry_decision_family": entry_meta.get("entry_decision_family"),
            "entry_rule_version": entry_meta.get("entry_rule_version"),
            "entry_meta_json": entry_meta,
            "stop_price_at_entry": entry_meta.get("stop_price_at_entry"),
            "pivot_price_at_entry": entry_meta.get("pivot_price_at_entry"),
            "exit_policy_family": entry_meta.get("exit_policy_family"),
        }

    def _append_ledger_event(
        self,
        *,
        event_type: str,
        code: str | None,
        market: str | None,
        mode: int | None,
        side: str | None,
        qty: int | None,
        price: float | None,
        client_order_key: str | None,
        ok: bool,
        reasons: list[str] | None,
        stage: str | None,
        payload_json: dict | None = None,
    ) -> None:
        payload = dict(payload_json or {})
        payload.setdefault("run_id", self.run_id)
        payload.setdefault("derived_as_of", self.get_as_of())
        payload.setdefault("trade_date", self._trade_date)
        payload.setdefault("code", code)
        payload.setdefault("qty", qty)
        payload.setdefault("price", price)
        try:
            last_exc: Exception | None = None
            for attempt in range(2):
                try:
                    self.ledger_repo.append_event(
                        env=self.env,
                        run_id=self.run_id,
                        strategy=self.STRATEGY_NAME,
                        event_type=event_type,
                        ts=now_kst(),
                        code=code,
                        market=market,
                        sid=1,
                        mode=mode,
                        side=side,
                        qty=qty,
                        price=price,
                        client_order_key=client_order_key,
                        ok=ok,
                        reasons=self._with_name_reason(reasons, code),
                        stage=stage,
                        payload_json=payload,
                    )
                    if attempt > 0:
                        self._bump_warning("ledger_fail_first_count")
                        logger.warning(
                            "[WARN][PB1][LEDGER][RETRY_OK] event=%s code=%s attempt=%s",
                            event_type,
                            self._display_code(code),
                            attempt + 1,
                        )
                    return
                except Exception as exc:
                    last_exc = exc
                    if attempt == 0:
                        logger.warning(
                            "[WARN][PB1][LEDGER][RETRY] event=%s code=%s err=%s",
                            event_type,
                            self._display_code(code),
                            repr(exc),
                        )
                        continue
                    raise
        except Exception as exc:
            self._bump_warning("degraded_stage_count")
            logger.warning(
                "[DEGRADED][PB1][LEDGER][FAIL] event=%s code=%s err=%s",
                event_type,
                self._display_code(code),
                repr(exc),
            )
            if self.debug:
                logger.exception("[PB1][LEDGER][FAIL_TRACE]")

    def _spool_db_fail(self, kind: str, payload: dict) -> None:
        try:
            spool_event(kind, payload)
        except Exception as exc:
            logger.exception("[PB1][DB_FAIL][SPOOL_FAIL] kind=%s err=%s", kind, exc)

    @staticmethod
    def _format_order_result_reason(resp: dict | None) -> str:
        if not isinstance(resp, dict):
            return "ORDER_FAIL_API(no_response)"
        if resp.get("status") == "SKIPPED":
            skip_reason = resp.get("skip_reason") or "SKIPPED"
            return f"ORDER_SKIP_{skip_reason}"
        rt_cd = resp.get("rt_cd")
        if str(rt_cd) == "0":
            return "ORDER_OK"
        msg_cd = resp.get("msg_cd")
        msg1 = resp.get("msg1")
        if msg_cd:
            return f"ORDER_FAIL_BIZ_{msg_cd}"
        return f"ORDER_FAIL_API(rt_cd={rt_cd},msg_cd={msg_cd},msg1={msg1})"

    @staticmethod
    def _empty_order_status() -> dict[str, Any]:
        return {
            "candidate_built": 1,
            "submit_attempted": 0,
            "api_submitted": 0,
            "accepted": 0,
            "filled": 0,
            "failed": 0,
            "skipped": 0,
            "rejected": 0,
            "broker_submit_called": 0,
            "submit_terminal_status": "NO_API_CALL",
            "skipped_reason": "",
            "broker_order_no": None,
            "broker_response_code": None,
            "broker_message": None,
            "submitted": 0,
        }

    @staticmethod
    def _classify_submit_terminal_status(
        *,
        api_submitted: int,
        accepted: int,
        skipped_reason: str | None,
        response: dict | None,
    ) -> str:
        if skipped_reason:
            return "SKIPPED_BY_POLICY"
        if api_submitted <= 0:
            return "NO_API_CALL"
        if not isinstance(response, dict):
            return "API_CALLED_NO_RESPONSE"
        if accepted > 0:
            return "ACCEPTED_PENDING_FILL"
        return "BROKER_REJECTED"

    def _log_setup(self, cf: CandidateFeature) -> None:
        self.total_candidates += 1
        if cf.setup_ok:
            self.ok_count += 1
        prefix = "[PB1][SETUP-OK]" if cf.setup_ok else "[PB1][SETUP-BAD]"
        if not cf.setup_ok:
            self._record_setup_reasons(cf.reasons or ["unspecified_fail"], cf.code)
        if not cf.setup_ok and not env_bool("PB1_VERBOSE", False):
            return
        logger.info(
            "%s code=%s market=%s mode=%s reasons=%s features=%s",
            prefix,
            cf.code,
            cf.market,
            cf.mode,
            cf.reasons or ["n/a"],
            {
                k: cf.features.get(k)
                for k in [
                    "close",
                    "ma50",
                    "ma150",
                    "ma200",
                    "ma200_slope",
                    "hi_52w",
                    "lo_52w",
                    "dollar_vol_50",
                    "pivot",
                    "rs_percentile",
                ]
            },
        )

    def _prefilter_members_fast(self, members: List[dict]) -> List[dict]:
        """
        빠른 프리필터: 최근 30일 거래대금 기준으로 상위 N개만 스캔.
        - OHLCV가 없으면 즉시 스킵(프리필터 점수 0)
        - 30일만 가져오므로 200일 풀 계산보다 훨씬 빠름
        """
        import os
        limit = int(os.getenv("PB1_UNIVERSE_SCAN_LIMIT", "50"))
        lb = int(os.getenv("PB1_PREFILTER_LOOKBACK_DAYS", "30"))
        
        scored: List[tuple[float, dict]] = []
        for member in members:
            code = str(member.get("code") or "").zfill(6)
            try:
                df, _ = self._fetch_daily(code, count=lb)
                if df is None or len(df) < max(10, lb // 2):
                    continue
                # 거래대금 proxy: close * volume
                if "close" not in df.columns or "volume" not in df.columns:
                    continue
                close_series = df["close"].astype(float)
                vol_series = df["volume"].astype(float)
                tv = float((close_series * vol_series).tail(20).mean())
                scored.append((tv, member))
            except Exception:
                # 프리필터에서 죽으면 안 됨: 그냥 스킵
                continue
        
        scored.sort(reverse=True, key=lambda x: x[0])
        top = [m for _, m in scored[:limit]]
        
        # benchmark 포함 보장 (예: 229200)
        benchmark_codes = [RS_BENCHMARK, "229200", "005930"]
        for bcode in benchmark_codes:
            if any(str(m.get("code") or "").zfill(6) == bcode for m in members):
                if not any(str(m.get("code") or "").zfill(6) == bcode for m in top):
                    bench_member = next((m for m in members if str(m.get("code") or "").zfill(6) == bcode), None)
                    if bench_member:
                        top = [bench_member] + top
        
        logger.info(
            "[PB1][PREFILTER] total=%s limit=%s selected=%s lookback_days=%s",
            len(members), limit, len(top), lb
        )
        return top

    def _metric_add(self, key: str, delta: int | float) -> None:
        if not hasattr(self, "_data_metrics") or self._data_metrics is None:
            self._data_metrics = {}
        self._data_metrics[key] = self._data_metrics.get(key, 0) + delta

    def _fetch_daily(self, code: str, count: int | None = None, days: int | None = None) -> tuple[pd.DataFrame, Dict]:
        """OHLCV 로딩 (기본 PB1_OHLCV_DAYS_BASE일 윈도우로 안정화)
        
        Args:
            code: 종목코드
            count: 요청할 캔들 수 (days와 상호 호환)
            days: count의 별칭 (candidate_pool_builder 호환용)
        """
        # days와 count는 같은 의미 (하위 호환성)
        if count is None:
            count = days if days is not None else int(PB1_OHLCV_DAYS_BASE)
        
        # ✅ 레짐/벤치마크 심볼 판단 (trade-tick 긴 조회 예외 허용)
        purpose = None
        if code == str(REGIME_INDEX).zfill(6) or code == REGIME_INDEX:
            purpose = "regime"
        elif code == str(RS_BENCHMARK).zfill(6) or code == RS_BENCHMARK:
            purpose = "regime"

        trade_input = (os.getenv("TRADE_INPUT") or "final30").strip().lower() or "final30"
        trade_precomputed_only = bool(
            self.phase in {"entry", "pm_entry"}
            and self.trade_use_precomputed_features
            and bool(self._precomputed_final30_map)
            and (self.window_internal in {"morning", "day", "intraday", "after"})
            and trade_input == "final30"
        )
        usage_context = "trade" if (self.env == "trade" or os.getenv("MODE") == "trade") else None
        allow_long_fetch = True
        if trade_precomputed_only and count > 60 and purpose != "regime":
            allow_long_fetch = False
        if purpose == "regime":
            logger.info(
                "[OHLCV][TRADE][REGIME_EXCEPTION] symbol=%s days=%s db_first=1 long_fetch_allowed=1",
                code,
                count,
            )
        
        self.daily_fetch_count += 1
        try:
            result = self.ohlcv_provider.get_ohlcv(
                code,
                count,
                purpose=purpose,
                usage_context=usage_context,
                allow_long_fetch=allow_long_fetch,
            )
        except Exception:
            logger.exception("[PB1][DATA][FAIL] code=%s", code)
            return pd.DataFrame(), {"volume_missing": True, "source": "error", "mapped": {}}
        if not result or result.df is None or result.df.empty:
            return pd.DataFrame(), (result.meta if result else {"volume_missing": True, "source": "none"})
        df_norm = result.df.sort_values("date").tail(count)
        meta = result.meta or {}
        meta.setdefault("volume_missing", df_norm["volume"].isna().all() if "volume" in df_norm.columns else True)
        # 데이터 품질 로그: 252일(정확한 52주) 또는 120일(fallback) 여부 표시
        has_full_52w = 1 if len(df_norm) >= 252 else 0
        has_fallback = 1 if len(df_norm) >= 120 else 0
        self._metric_add("long_fetch_blocked_count", int(meta.get("long_fetch_blocked", 0) or 0))
        fetch_delta = int(meta.get("kis_trade_daily_fetch", 0) or 0)
        self._metric_add("kis_trade_daily_fetch_count_trade", fetch_delta)
        # Backward-compat metric key kept for existing summary and dashboards.
        self._metric_add("kis_daily_fetch_count_trade", fetch_delta)
        blocked_delta = int(meta.get("kis_trade_daily_blocked", 0) or 0)
        self._metric_add("kis_trade_daily_blocked_count_trade", blocked_delta)
        self._metric_add("kis_daily_fetch_blocked_count_trade", blocked_delta)
        if count <= 60:
            self._metric_add("short_fetch_count", 1)
        logger.info("[PB1][OHLCV][WINDOW] code=%s days=%d rows=%d hi_52w_full=%d fallback_120d=%d purpose=%s",
                    code, count, len(df_norm), has_full_52w, has_fallback, purpose or "universe")
        return df_norm, meta

    def _fetch_exit_ohlcv(self, code: str) -> tuple[pd.DataFrame, dict[str, Any]]:
        result = None
        try:
            result = self.ohlcv_provider.get_ohlcv(
                code,
                200,
                purpose="exit",
                usage_context="trade",
                allow_long_fetch=True,
            )
        except Exception:
            logger.exception("[EXIT][OHLCV][FAIL] code=%s days=200", self._display_code(code))
        df = pd.DataFrame()
        meta: dict[str, Any] = {}
        if result and result.df is not None and not result.df.empty:
            df = result.df.sort_values("date")
            meta = dict(result.meta or {})
        if len(df) < 50:
            try:
                fallback = self.ohlcv_provider.get_ohlcv(
                    code,
                    60,
                    purpose="exit",
                    usage_context="trade",
                    allow_long_fetch=True,
                )
                if fallback and fallback.df is not None and len(fallback.df) > len(df):
                    df = fallback.df.sort_values("date")
                    meta = dict(fallback.meta or {})
            except Exception:
                logger.exception("[EXIT][OHLCV][FAIL] code=%s days=60", self._display_code(code))
        logger.info(
            "[EXIT][OHLCV][SOURCE] code=%s source=%s rows=%s",
            self._display_code(code),
            str(meta.get("source") or "none"),
            len(df),
        )
        if len(df) < 50:
            logger.info(
                "[EXIT][MA_CTX][DEGRADED] code=%s reason=insufficient_history rows=%s",
                self._display_code(code),
                len(df),
            )
        return df, meta

    def _compute_candidates(self, members: Iterable[dict]) -> List[CandidateFeature]:
        # ✅ 타이머 및 시간 예산 설정
        t0 = time.monotonic()
        t_minervini_start = time.monotonic()  # ✅ Minervini 전용 타이머
        deadline = t0 + 3600.0  # 후보 스캔 타임아웃: 3600초 (1시간)
        reason = "OK"
        candidates: List[CandidateFeature] = []
        members_list = list(members)  # Iterable → list 변환
        scan_count = len(members_list)
        
        # ✅ CRITICAL: UnboundLocalError 방지 - 모든 변수 초기화
        trade_mode = os.getenv("MODE") == "trade"
        checked_count = 0
        ohlcv_missing_count = 0
        
        # ✅ GUARD: 스캔 대상이 150 초과 시 경고 (성능 저하 우려)
        # Note: 이 로그는 _compute_candidates 호출 후가 아닌, 입력 members_list 기준
        from trader.config import CANDIDATE_POOL_ENABLED
        pb1_candidate_only = os.getenv("PB1_CANDIDATE_ONLY", "0") == "1"
        
        if scan_count > 150:
            if pb1_candidate_only:
                logger.error(
                    "[SCAN][GUARD] CRITICAL: scan_count=%d exceeds 150 in candidate-only mode (pool too large)",
                    scan_count
                )
            elif CANDIDATE_POOL_ENABLED:
                logger.warning(
                    "[SCAN][GUARD] scan_count=%d exceeds 150 (should have been reduced by candidate pool, check pool build)",
                    scan_count
                )
            else:
                logger.info(
                    "[SCAN][GUARD] scan_count=%d exceeds 150 (using full universe, watchlist disabled)",
                    scan_count
                )
        
        # ✅ 프리필터 적용: watchlist인 경우 프리필터 스킵 (후보군 무력화 방지)
        from trader.config import CANDIDATE_POOL_ENABLED
        is_watchlist = CANDIDATE_POOL_ENABLED and scan_count <= 150
        
        if not is_watchlist and os.getenv("PB1_UNIVERSE_PREFILTER", "1") == "1":
            members_list = self._prefilter_members_fast(members_list)
            logger.info("[PB1][CANDIDATES][PREFILTER] scan_count=%s -> filtered=%s", scan_count, len(members_list))
        elif is_watchlist:
            logger.info("[PB1][CANDIDATES][PREFILTER] SKIP (using watchlist, scan_count=%s)", scan_count)
        
        try:
            # ✅ holdings/prev_top 필터: watchlist인 경우 스킵 (후보군 무력화 방지)
            if not is_watchlist:
                holdings_codes = set(str(row.get("pdno") or "").zfill(6) for row in self._holdings_summary.get("output1", []))
                top_candidates_path = runtime_path("top_candidates.json")
                prev_top_codes = set()
                if top_candidates_path.exists():
                    try:
                        with open(top_candidates_path) as f:
                            prev_top_candidates = json.load(f)
                        prev_top_codes = set(c.get("code") for c in prev_top_candidates if c.get("code"))
                    except Exception:
                        logger.warning("[PB1][TOP_CANDIDATES][LOAD_FAIL] %s", top_candidates_path)
                relevant_codes = holdings_codes | prev_top_codes
                if relevant_codes:
                    members_list = [m for m in members_list if str(m.get("code") or "").zfill(6) in relevant_codes]
                    logger.info("[PB1][CANDIDATES][LIMITED] holdings=%s prev_top=%s total_members=%s", len(holdings_codes), len(prev_top_codes), len(members_list))
                else:
                    logger.info("[PB1][CANDIDATES][FULL] no holdings/top_candidates -> full universe")
            else:
                logger.info("[PB1][CANDIDATES][WATCHLIST] SKIP holdings/prev_top filter (scan_count=%s)", len(members_list))
            
            # 최소 캔들 수 조건 완화: 120일 또는 200일 데이터만으로도 후보 선정 가능
            required_candles = self.min_candles
            if trade_mode:
                required_candles = min(required_candles, 30)
            
            derived_map: dict[str, dict] = {}
            if trade_mode:
                derived_repo = DerivedMinerviniRepo(self.engine)
                today_val = self._today
                if isinstance(today_val, datetime):
                    today_val = today_val.date()
                elif isinstance(today_val, str):
                    today_val = date.fromisoformat(today_val.split("T")[0])
                symbol_list = [str(m.get("code") or "").zfill(6) for m in members_list]
                
                # ✅ CRITICAL: fallback 지원 버전 사용 (최대 7일 이내)
                derived_rows, actual_as_of = derived_repo.load_for_as_of_with_fallback(
                    env=self.env,
                    as_of=today_val,
                    symbols=symbol_list,
                    ttl_days=7,
                )
                
                if not derived_rows:
                    logger.warning(
                        "[MINERVINI][TRADE_SKIP] derived_minervini missing requested=%s actual=%s",
                        today_val.isoformat(),
                        actual_as_of.isoformat() if actual_as_of else "N/A",
                    )
                    return []
                
                if actual_as_of and actual_as_of != today_val:
                    logger.info(
                        "[MINERVINI][DERIVED][FALLBACK] requested=%s actual=%s age=%d rows=%d",
                        today_val.isoformat(),
                        actual_as_of.isoformat(),
                        (today_val - actual_as_of).days,
                        len(derived_rows),
                    )
                
                derived_map = {str(row.get("symbol") or "").zfill(6): row for row in derived_rows}
            bench_df = pd.DataFrame()
            bench_close = pd.Series(dtype=float)
            debug_mode = os.getenv("MINERVINI_DEBUG") == "1"
            degraded_ok = os.getenv("MINERVINI_DEGRADED_OK", "0") == "1"
            bench_insufficient = False
            min_bench_required = max(RS_LOOKBACK_DAYS, RS_LOOKBACK2_DAYS)

            if not trade_mode:
                bench_df, _ = self._fetch_daily(RS_BENCHMARK)
                bench_close = bench_df["close"] if not bench_df.empty else pd.Series(dtype=float)

                # [MINERVINI] 벤치마크 데이터 부족 감지
                bench_insufficient = len(bench_df) < min_bench_required
                if bench_insufficient:
                    logger.warning(
                        "[MINERVINI][SKIP] reason=insufficient_benchmark benchmark_rows=%s min_required=%s pass_through=%s",
                        len(bench_df),
                        min_bench_required,
                        degraded_ok,
                    )
                    if not degraded_ok:
                        # STRICT 모드: 벤치마크 데이터 부족 시 후보 비우기
                        logger.error(
                            "[MINERVINI][STRICT] benchmark data insufficient -> clear candidates (set MINERVINI_DEGRADED_OK=1 to allow)"
                        )
                        # 빈 후보 리스트 반환
                        for cf in candidates:
                            cf.setup_ok = False
                            cf.reasons = (cf.reasons or []) + ["minervini_benchmark_insufficient"]
                        return candidates

                need_days = _minervini_ohlcv_days()
                logger.info(
                    "[MINERVINI][OHLCV_DAYS] need_days=%s (env MINERVINI_OHLCV_DAYS=%s, slope_lb=%s)",
                    need_days,
                    os.getenv("MINERVINI_OHLCV_DAYS"),
                    MA200_SLOPE_LOOKBACK,
                )
            else:
                need_days = 30
            
            rs_prices: dict[str, pd.Series] = {}
            # checked_count는 함수 시작 시 이미 초기화됨

            
            # ✅ 조기 종료 설정 (config 기반, 기본값 0=비활성화)
            early_stop = PB1_EARLY_STOP_ENABLED
            early_n = int(os.getenv("PB1_EARLY_STOP_CANDIDATES", "12"))
            
            for m in members_list:
                # ✅ 조기 종료 체크 (충분한 후보 확보 시) - PB1_EARLY_STOP_ENABLED=0이면 절대 실행 안 됨
                if early_stop and len(candidates) >= max(early_n, int(PB1_MIN_CANDIDATES or 1)):
                    reason = "EARLY_STOP_CANDIDATES_SUFFICIENT"
                    logger.info("[PB1][CANDIDATES][EARLY_STOP] candidates=%s early_n=%s", len(candidates), early_n)
                    break
                
                # ✅ 시간 예산 체크 (10개마다)
                checked_count += 1
                if checked_count % 10 == 0 and time.monotonic() > deadline:
                    reason = "TIME_BUDGET_EXCEEDED"
                    logger.warning(
                        "[ENTRY][CANDIDATES][TIMEOUT] checked=%s/%s elapsed=%.1fs deadline_exceeded=True",
                        checked_count, scan_count, time.monotonic() - t0
                    )
                    break
                
                code = str(m.get("code") or "").zfill(6)
                market = m.get("market") or ""
                try:
                    # ✅ OHLCV 결측 즉시 스킵 (보장 모드)
                    derived_row = derived_map.get(code) if trade_mode else None
                    if trade_mode and not derived_row:
                        continue

                    df, meta = self._fetch_daily(code, days=need_days)
                    if df is None or df.empty or len(df) < required_candles:
                        if df is None or df.empty:
                            skip_reason = "data_empty"
                        else:
                            skip_reason = "insufficient_data"
                        logger.debug(
                            "[PB1][CANDIDATES][SKIP] code=%s reason=%s rows=%s",
                            code,
                            skip_reason,
                            len(df) if df is not None else 0,
                        )
                        continue

                    if trade_mode:
                        features = {
                            "close": derived_row.get("close") or float(df["close"].iloc[-1]),
                            "ma50": derived_row.get("ma50"),
                            "ma150": derived_row.get("ma150"),
                            "ma200": derived_row.get("ma200"),
                            "ma200_slope": derived_row.get("ma200_slope"),
                            "dollar_vol_50": derived_row.get("dollar_vol_50"),
                            "atr14": derived_row.get("atr"),
                            "atr_pct": derived_row.get("atr_pct"),
                            "rs_percentile": derived_row.get("rs_percentile"),
                            "vcp_score": derived_row.get("vcp_score"),
                            "vcp_ok": derived_row.get("vcp_ok"),
                            "pivot": derived_row.get("pivot"),
                            "score": derived_row.get("minervini_score"),
                        }
                        extra = derived_row.get("features_json") or {}
                        if isinstance(extra, dict):
                            features.update(extra)
                    else:
                        try:
                            features = compute_features(df)
                        except ValueError:
                            reasons = ["insufficient_candles"]
                            cf = CandidateFeature(
                                code=code,
                                market=market,
                                features={"reasons": reasons, "count": len(df), "data_ok": False},
                                setup_ok=False,
                                reasons=reasons,
                                mode=1,
                                mode_reasons=["default_day_mode"],
                            )
                            candidates.append(cf)
                            continue
                    ma200_slope = features.get("ma200_slope")
                    if debug_mode and (ma200_slope is None or (isinstance(ma200_slope, float) and not np.isfinite(ma200_slope))):
                        close_series = df["close"] if "close" in df.columns else pd.Series(dtype=float)
                        close_nan = int(close_series.isna().sum()) if not close_series.empty else 0
                        if "date" in df.columns:
                            first_date = str(df["date"].iloc[0])
                            last_date = str(df["date"].iloc[-1])
                        else:
                            first_date = str(df.index[0]) if len(df.index) else "n/a"
                            last_date = str(df.index[-1]) if len(df.index) else "n/a"
                        ma200_series = close_series.rolling(200).mean() if not close_series.empty else pd.Series(dtype=float)
                        ma200_valid = int(ma200_series.notna().sum()) if not ma200_series.empty else 0
                        logger.info(
                            "[MINERVINI][MA200_SLOPE_DEBUG] code=%s rows=%s first=%s last=%s close_nan=%s ma200_valid=%s ma200=%s slope=%s",
                            code,
                            len(df),
                            first_date,
                            last_date,
                            close_nan,
                            ma200_valid,
                            features.get("ma200"),
                            ma200_slope,
                        )
                    close_val = features.get("close")
                    ma200_val = features.get("ma200")
                    scale_ratio = None
                    if close_val and ma200_val and np.isfinite(close_val) and np.isfinite(ma200_val) and ma200_val != 0:
                        scale_ratio = float(close_val) / float(ma200_val)
                    if scale_ratio is not None and (scale_ratio > 3.5 or scale_ratio < 0.3):
                        reasons = ["price_scale_outlier"]
                        cf = CandidateFeature(
                            code=code,
                            market=market,
                            features={
                                "reasons": reasons,
                                "data_ok": False,
                                "scale_ratio": scale_ratio,
                                "close": close_val,
                                "ma200": ma200_val,
                            },
                            setup_ok=False,
                            reasons=reasons,
                            mode=1,
                            mode_reasons=["default_day_mode"],
                        )
                        candidates.append(cf)
                        continue
                    gap_pct = None
                    if len(df) >= 2:
                        prev_close = float(df["close"].iloc[-2])
                        open_price = float(df["open"].iloc[-1])
                        if prev_close > 0:
                            gap_pct = (open_price - prev_close) / prev_close * 100.0
                    spread_pct = None
                    range_pct = None
                    if len(df) >= 1:
                        spread_proxy = (df["high"] - df["low"]) / df["close"].replace(0, np.nan) * 100.0
                        spread_pct = float(spread_proxy.tail(20).mean()) if len(spread_proxy) else None
                        range_proxy = (df["high"] - df["low"]) / df["close"] * 100.0
                        range_pct = float(range_proxy.tail(20).mean()) if len(range_proxy) else None
                    if trade_mode:
                        pivot_val = features.get("pivot")
                        features["market"] = market
                        features["volume_missing"] = bool(meta.get("volume_missing"))
                        features["data_ok"] = True
                        features["vcp_ok"] = bool(features.get("vcp_ok"))
                        features["vcp_score"] = float(features.get("vcp_score") or 0.0)
                        features["pivot"] = float(pivot_val) if pivot_val and np.isfinite(pivot_val) else float("nan")
                        features["pivot_scan"] = features["pivot"]
                        features.setdefault("pivot_valid", bool(pivot_val))
                    else:
                        vcp_score = score_vcp(
                            df,
                            VCP_LOOKBACK,
                            VolContractRules(),
                            PriceTightRules(),
                        )
                        pivot_info = find_pivot(df)
                        pivot = pivot_info.get("pivot_price")
                        vcp_info = detect_vcp(df, self.minervini_config)
                        features["market"] = market
                        features["volume_missing"] = bool(meta.get("volume_missing"))
                        features["data_ok"] = True
                        features["vcp_ok"] = bool(vcp_info.get("vcp_ok") or is_vcp_ready(vcp_score, VCP_MIN_SCORE))
                        features["vcp_score"] = float(vcp_score)
                        features["vcp_contractions"] = vcp_info.get("contractions")
                        pivot_val = float(pivot) if pivot and np.isfinite(pivot) else float("nan")
                        features["pivot"] = pivot_val
                        features["pivot_scan"] = pivot_val
                        features["pivot_age"] = pivot_info.get("pivot_date")
                        features["pivot_valid"] = bool(pivot and np.isfinite(pivot))
                        features["tight_low"] = pivot_info.get("tight_low")
                        features["base_high"] = pivot_info.get("base_high")
                    features["gap_pct"] = gap_pct
                    features["spread_pct"] = spread_pct
                    features["range_pct"] = range_pct
                    features["liq_ok"] = liquidity_filter(df, MIN_AVG_VALUE_KRW)
                    features["gap_ok"] = gap_filter(df, MAX_GAP_UP_PCT)
                    features["spread_ok"] = spread_proxy_filter(df, MAX_SPREAD_PROXY_BPS)
                    features["range_ok"] = range_filter(df, MAX_INTRADAY_RANGE_PCT)
                    cf = CandidateFeature(
                        code=code,
                        market=market,
                        features=features,
                        setup_ok=False,
                        reasons=[],
                        mode=1,
                        mode_reasons=["minervini_default"],
                    )
                    candidates.append(cf)
                    if not trade_mode:
                        rs_prices[code] = df["close"].reset_index(drop=True)
                except Exception as exc:
                    # ✅ 개별 종목 예외로 전체 런이 죽지 않게
                    logger.exception("[PB1][CANDIDATES][COMPUTE_FAILED] code=%s error=%s -> skip", code, exc)
                    continue
            
            if not candidates:
                reason = "NO_CANDIDATES_AFTER_OHLCV"
                return candidates
            
            if trade_mode:
                rs_rank = pd.DataFrame()
                rs_map = {}
            else:
                rs_rank = rank_rs(
                    rs_prices,
                    bench_close,
                    lookback_days=RS_LOOKBACK_DAYS,
                    lookback2_days=RS_LOOKBACK2_DAYS,
                    w1=RS_COMPOSITE_W1,
                    w2=RS_COMPOSITE_W2,
                )
                rs_map = {row["ticker"]: row for row in rs_rank.to_dict(orient="records")}
            
            # [MINERVINI] RS/VCP 필터 적용 전 카운트
            before_minervini = len([cf for cf in candidates if cf.features.get("data_ok")])
            debug_mode = os.getenv("MINERVINI_DEBUG") == "1"
            t_minervini_enter = time.monotonic()
            
            rs_fail_count = 0
            vcp_fail_count = 0
            
            for i, cf in enumerate(candidates):
                if trade_mode:
                    rs_p = float(cf.features.get("rs_percentile") or 0.0)
                    rs_row = {}
                else:
                    rs_row = rs_map.get(cf.code, {})
                    rs_p = float(rs_row.get("pctile") or 0.0)
                cf.features["rs_percentile"] = rs_p
                cf.features["rs_pctile"] = rs_p * 100.0
                cf.features["rs_comp"] = rs_row.get("composite")
                vcp_info = {
                    "score": cf.features.get("vcp_score"),
                    "vcp_ok": cf.features.get("vcp_ok"),
                    "contractions": cf.features.get("vcp_contractions"),
                }
                
                # RS/VCP 필터 체크
                if cf.features.get("data_ok"):
                    if rs_p < self.minervini_config.rs_min_percentile:
                        rs_fail_count += 1
                    if not cf.features.get("vcp_ok"):
                        vcp_fail_count += 1
                
                cf.score = score_setup(cf.features, rs_percentile=rs_p, vcp_info=vcp_info, cfg=self.minervini_config)
                cf.features["score"] = cf.score
            
            # [MINERVINI] 필터링 후 통과 종목 수
            after_rs = before_minervini - rs_fail_count
            after_vcp = before_minervini - vcp_fail_count
            both_pass = len([cf for cf in candidates if cf.features.get("data_ok") and 
                             cf.features.get("rs_percentile", 0) >= self.minervini_config.rs_min_percentile and
                             cf.features.get("vcp_ok")])
            
            sample_codes = [cf.code for cf in sorted(candidates, key=lambda x: x.score or 0, reverse=True)[:5]]
            
            skipped_count = len([cf for cf in candidates if not cf.features.get("data_ok")])
            degraded_count = 1 if bench_insufficient and degraded_ok else 0
            
            dt_minervini_total = time.monotonic() - t_minervini_enter
        
            logger.info(
                "[MINERVINI][APPLY] universe=%s rs_min_pctile=%.0f vcp_lookback=%s lookback_days=%s/%s",
                before_minervini,
                self.minervini_config.rs_min_percentile * 100,
                VCP_LOOKBACK,
                RS_LOOKBACK_DAYS,
                RS_LOOKBACK2_DAYS,
            )
            logger.info(
                "[MINERVINI][RESULT] before=%s after_rs=%s dropped_rs=%s after_vcp=%s dropped_vcp=%s both_pass=%s skipped=%s degraded=%s sample=%s",
                before_minervini,
                after_rs,
                rs_fail_count,
                after_vcp,
                vcp_fail_count,
                both_pass,
                skipped_count,
                degraded_count,
                sample_codes,
            )
            logger.info(
                "[MINERVINI][EXIT] passed=%s failed=%s dt=%.2f",
                both_pass,
                before_minervini - both_pass,
                dt_minervini_total,
            )
            
            if debug_mode:
                # 샘플 종목 상세 정보
                for code in sample_codes[:3]:
                    cf = next((c for c in candidates if c.code == code), None)
                    if cf and cf.features.get("data_ok"):
                        logger.info(
                            "[MINERVINI][SAMPLE] code=%s rs_pct=%.1f vcp_ok=%s vcp_score=%.1f score=%.1f",
                            code,
                            cf.features.get("rs_pctile", 0),
                            cf.features.get("vcp_ok"),
                            cf.features.get("vcp_score", 0),
                            cf.score or 0,
                        )
            return candidates
        
        except Exception as exc:
            reason = f"EXCEPTION:{type(exc).__name__}"
            logger.exception("[ENTRY][PIPE][ERROR] exception in _compute_candidates: %s", exc)
            
            # ✅ failmode_soft 처리
            failmode_soft = int(os.getenv("PB1_FAILMODE_SOFT", str(PB1_FAILMODE_SOFT)))
            if failmode_soft:
                logger.warning(
                    "[ENTRY][PIPE][SOFT_FAIL] failmode_soft=1 -> return empty candidates (no crash)"
                )
                return []
            else:
                # hard 모드: 예외 전파
                raise
        
        finally:
            # ✅ 무조건 요약 로그 출력 (성공/실패 모두)
            total_dt = time.monotonic() - t0
            candidates_count = len(candidates)
            
            # [NEW] B. 후보 0명일 때 "왜 0인지" 상세 로그
            if candidates_count == 0:
                ohlcv_missing_count = checked_count - len(candidates)
                ohlcv_ok_count = len([cf for cf in candidates if cf.features.get("data_ok")])
                regime_pass_count = len([cf for cf in candidates if cf.features.get("liq_ok") and cf.features.get("gap_ok")])
                vcp_pass_count = len([cf for cf in candidates if cf.features.get("vcp_ok")])
                rs_pass_count = len([cf for cf in candidates if cf.features.get("rs_percentile", 0) >= self.minervini_config.rs_min_percentile])
                final_count = len([cf for cf in candidates if cf.setup_ok])
                
                logger.warning(
                    "[PB1][CANDIDATES=0][BREAKDOWN] scan_count=%s checked=%s ohlcv_missing=%s ohlcv_ok=%s "
                    "regime_pass=%s vcp_pass=%s rs_pass=%s final=%s reason=%s",
                    scan_count,
                    checked_count,
                    ohlcv_missing_count,
                    ohlcv_ok_count,
                    regime_pass_count,
                    vcp_pass_count,
                    rs_pass_count,
                    final_count,
                    reason,
                )
                
                # OHLCV 결측 샘플 출력 (최대 5개)
                skip_samples = [m.get("code") for m in members_list[:5] if m.get("code") not in [cf.code for cf in candidates]]
                if skip_samples:
                    logger.warning("[PB1][CANDIDATES=0][OHLCV_SKIP_SAMPLE] codes=%s", skip_samples)
            
            logger.info(
                "[ENTRY][PIPE][END] trace=compute_candidates total_dt=%.2fs reason=%s scan_count=%s candidates=%s",
                total_dt,
                reason,
                scan_count,
                candidates_count,
            )

    @staticmethod
    def _clone_candidate(cf: CandidateFeature) -> CandidateFeature:
        return CandidateFeature(
            code=cf.code,
            market=cf.market,
            features=dict(cf.features),
            setup_ok=cf.setup_ok,
            reasons=list(cf.reasons),
            mode=cf.mode,
            mode_reasons=list(cf.mode_reasons),
            client_order_key=cf.client_order_key,
            planned_qty=cf.planned_qty,
            score=cf.score,
        )

    def _apply_thresholds(
        self,
        candidates: List[CandidateFeature],
        thresholds: FilterThresholds,
        *,
        min_score: float,
        log_results: bool,
        soft_mode: bool,
        spread_hard_max_pct: float,
        gap_hard_max_pct: float,
    ) -> List[CandidateFeature]:
        evaluated: List[CandidateFeature] = []
        cfg = self.minervini_config
        rs_min_pctile = float(self.effective_entry_filters.get("rs_min_pctile", RS_MIN_PCTILE))
        vcp_min_score = float(self.effective_entry_filters.get("vcp_min_score", VCP_MIN_SCORE))
        for cf in candidates:
            clone = self._clone_candidate(cf)
            data_ok = bool(clone.features.get("data_ok"))
            if not data_ok:
                if log_results:
                    self._log_setup(clone)
                evaluated.append(clone)
                continue
            ok, reasons = _strategy_evaluate_filters(clone.features, cfg)
            
            # [MINERVINI] 필터 스킵/무력화 감지
            debug_mode = os.getenv("MINERVINI_DEBUG") == "1"
            if debug_mode and not ok:
                logger.info(
                    "[MINERVINI][FILTER_FAIL] code=%s reasons=%s rs_pct=%.1f vcp_ok=%s",
                    clone.code,
                    reasons,
                    clone.features.get("rs_pctile", 0),
                    clone.features.get("vcp_ok"),
                )
            
            hard_reasons: list[str] = []
            soft_reasons: list[str] = []

            pullback = self._to_float(clone.features.get("pullback_pct"))
            vol_c = self._to_float(clone.features.get("vol_contraction"))
            volu_c = self._to_float(clone.features.get("volu_contraction"))
            rs_pctile = self._to_float(clone.features.get("rs_percentile"))
            vcp_score = self._to_float(clone.features.get("vcp_score"))

            if pullback is None or not (float(thresholds.pullback_min) <= pullback <= float(thresholds.pullback_max)):
                hard_reasons.append("pullback_out_of_band")

            vol_fail = vol_c is None or float(vol_c) > float(thresholds.vol_contraction_max)
            volu_fail = (not bool(clone.features.get("volume_missing", False))) and (
                volu_c is None or float(volu_c) > float(thresholds.volu_contraction_max)
            )
            if bool(thresholds.require_both_contractions):
                if vol_fail:
                    hard_reasons.append("vol_contraction_fail")
                if volu_fail:
                    hard_reasons.append("volu_contraction_fail")
            else:
                if vol_fail and volu_fail:
                    hard_reasons.append("contraction_fail")

            if rs_pctile is None or float(rs_pctile) < rs_min_pctile:
                hard_reasons.append("rs_below_min")
            if vcp_score is None or float(vcp_score) < vcp_min_score:
                hard_reasons.append("vcp_fail")

            if not clone.features.get("liq_ok", True):
                reasons.append("liquidity_fail")
            if not clone.features.get("spread_ok", True):
                reasons.append("spread_fail")
            if not clone.features.get("range_ok", True):
                reasons.append("range_fail")
            if not clone.features.get("gap_ok", True):
                reasons.append("gap_fail")

            hard_reason_set = {"missing_ma", "illiquid", "liquidity_fail", "price_scale_outlier"}
            for reason in reasons:
                if reason in hard_reason_set:
                    hard_reasons.append(reason)

            spread_pct = self._to_float(clone.features.get("spread_pct"))
            gap_pct = self._to_float(clone.features.get("gap_pct"))
            if spread_hard_max_pct and spread_pct is not None and spread_pct > spread_hard_max_pct:
                hard_reasons.append("spread_fail")
            if gap_hard_max_pct and gap_pct is not None and gap_pct > gap_hard_max_pct:
                hard_reasons.append("gap_fail")

            soft_reasons.extend([r for r in reasons if r not in hard_reason_set])

            base_score = 0.0
            try:
                rs_p = float(clone.features.get("rs_percentile") or 0.0)
                vcp_info = {
                    "score": clone.features.get("vcp_score"),
                    "vcp_ok": clone.features.get("vcp_ok"),
                    "contractions": clone.features.get("vcp_contractions"),
                }
                base_score = float(score_setup(clone.features, rs_percentile=rs_p, vcp_info=vcp_info, cfg=cfg))
            except Exception:
                base_score = 0.0
            normalized_score = self._normalize_setup_score(base_score)
            adjusted_score = self._apply_soft_penalties(normalized_score, soft_reasons)
            clone.features["score_raw"] = normalized_score
            clone.features["score"] = adjusted_score
            clone.score = adjusted_score

            score_ok = adjusted_score >= float(min_score)
            if not score_ok:
                soft_reasons.append("score_below_min")

            # [2026-05-18] KR 전용 adaptive entry filter
            # _is_kr_equity_context()==False이면 기존 글로벌 로직과 동일
            if self._is_kr_equity_context() and PB1_KR_ADAPTIVE_ENTRY_FILTER:
                entry_style = str(
                    clone.features.get("entry_style_selected")
                    or clone.features.get("entry_signal")
                    or clone.features.get("entry_reason")
                    or ""
                ).upper()

                # 기존 hard_reasons를 severity로 재분류
                reclassified_hard: list[str] = []
                reclassified_soft: list[str] = []
                kr_penalty = 0.0

                for r in hard_reasons:
                    sev = self._classify_kr_reason_by_style(r, entry_style)
                    if sev == "HARD":
                        reclassified_hard.append(r)
                    elif sev == "SOFT":
                        reclassified_soft.append(r)
                        if r == "vol_contraction_fail":
                            kr_penalty += PB1_KR_PULLBACK_VOL_PENALTY
                        elif r == "volu_contraction_fail":
                            kr_penalty += PB1_KR_PULLBACK_VOLU_PENALTY
                    # IGNORE: 버림

                kr_adjusted_score = adjusted_score - kr_penalty
                clone.features["kr_adjusted_score"] = kr_adjusted_score
                clone.features["kr_reclassified_hard"] = reclassified_hard
                clone.features["kr_reclassified_soft"] = reclassified_soft

                if PB1_KR_LOG_FILTER_MATRIX:
                    logger.info(
                        "[PB1][KR_FILTER_MATRIX] code=%s style=%s score=%.1f kr_adj=%.1f hard=%s→%s soft=%s→%s penalty=%.1f",
                        getattr(clone, "code", "?"),
                        entry_style or "?",
                        adjusted_score,
                        kr_adjusted_score,
                        hard_reasons,
                        reclassified_hard,
                        soft_reasons,
                        reclassified_soft,
                        kr_penalty,
                    )

                kr_score_ok = kr_adjusted_score >= PB1_KR_ADAPTIVE_SCORE_MIN_FLOOR
                must_fail = (
                    bool(reclassified_hard)
                    or not kr_score_ok
                    or (not soft_mode and (not ok or bool(reclassified_soft + soft_reasons)))
                )

                if not must_fail:
                    clone.features["kr_score_ok"] = True
                else:
                    clone.features["kr_score_ok"] = False
                    if not kr_score_ok:
                        reclassified_soft.append("kr_score_below_floor")
                    hard_reasons = reclassified_hard
                    soft_reasons = reclassified_soft + [r for r in soft_reasons if r != "score_below_min"]
            else:
                # 기존 글로벌 로직 그대로
                must_fail = bool(hard_reasons) or not score_ok or (not soft_mode and (not ok or bool(soft_reasons)))
            if must_fail:
                clone.setup_ok = False
                clone.reasons = hard_reasons + soft_reasons if soft_reasons or hard_reasons else ["unspecified_fail"]
            else:
                clone.setup_ok = True
                clone.reasons = []
                if soft_reasons:
                    clone.features["soft_flags"] = soft_reasons

            if log_results:
                self._log_setup(clone)
            evaluated.append(clone)
        return evaluated

    @staticmethod
    def _collect_reason_counts(candidates: Iterable[CandidateFeature]) -> Counter[str]:
        counts: Counter[str] = Counter()
        for cf in candidates:
            if cf.setup_ok:
                continue
            reasons = cf.reasons or ["unspecified_fail"]
            for reason in reasons:
                counts[reason] += 1
        return counts

    @staticmethod
    def _map_minervini_reasons(reasons: Iterable[str] | None) -> list[str]:
        mapping = {
            "rs_below_min": "RS_BELOW",
            "trend_template_fail": "TREND_FAIL",
            "ma200_not_rising": "MA200_NOT_RISING",
            "illiquid": "LIQ_FAIL",
            "liquidity_fail": "LIQ_FAIL",
            "gap_fail": "GAP_FAIL",
            "spread_fail": "SPREAD_FAIL",
            "range_fail": "RANGE_FAIL",
            "vcp_fail": "VCP_SCORE_LOW",
            "score_below_min": "SCORE_BELOW_MIN",
            "score_below_cut": "SCORE_BELOW_CUT",
            "price_scale_outlier": "PRICE_SCALE_OUTLIER",
            "insufficient_candles": "INSUFFICIENT_CANDLES",
            "data_empty": "DATA_MISSING",
            "planned_qty_zero_or_min_order": "MIN_ORDER_FAIL",
            "atr_pct_too_high": "ATR_TOO_HIGH",
            "liquidity_too_low": "LIQ_TOO_LOW",
        }
        mapped = []
        for reason in reasons or []:
            mapped.append(mapping.get(reason, str(reason).upper()))
        if not mapped:
            mapped = ["UNSPECIFIED_FAIL"]
        return mapped

    @staticmethod
    def _exit_reason_code(reason: str) -> str:
        mapping = {
            "EXIT_STOP_LOSS": ReasonCode.EXIT_STOP_LOSS,
            "EXIT_TRAILING_STOP": ReasonCode.EXIT_TRAIL,
            "EXIT_MA50_BREAK": ReasonCode.EXIT_REGIME,
            "EXIT_MA20_BREAK": ReasonCode.EXIT_TRAIL,
            "EXIT_TIME_STOP": ReasonCode.EXIT_TIME,
            "EXIT_RISK_OFF": ReasonCode.EXIT_REGIME,
            "STOP_HIT": ReasonCode.EXIT_STOP_LOSS,
            "FAILED_BREAKOUT": ReasonCode.EXIT_STOP_LOSS,
            "TP1": ReasonCode.EXIT_TP_PARTIAL,
            "TP2": ReasonCode.EXIT_TP_PARTIAL,
            "climax_partial": ReasonCode.EXIT_TP_PARTIAL,
            "time_stop": ReasonCode.EXIT_TIME,
            "ma50_break_heavy_volume": ReasonCode.EXIT_REGIME,
        }
        return mapping.get(reason, ReasonCode.EXIT_TRAIL)


    @staticmethod
    def _normalize_setup_score(raw_score: float) -> float:
        try:
            return float(min(100.0, max(0.0, raw_score)))
        except Exception:
            return 0.0

    def _apply_soft_penalties(self, base_score: float, reasons: Iterable[str]) -> float:
        penalties = {
            "trend_template_fail": 12.0,
            "ma200_not_rising": 8.0,
            "rs_below_min": 15.0,
            "vcp_fail": 8.0,
            "spread_fail": 10.0,
            "gap_fail": 6.0,
            "range_fail": 6.0,
        }
        penalty = 0.0
        for reason in reasons or []:
            penalty += penalties.get(reason, 0.0)
        return max(0.0, base_score - penalty)

    def _log_fail_reason_breakdown(self, candidates: Iterable[CandidateFeature], note: str | None = None) -> None:
        counts: Counter[str] = Counter()
        combos: Counter[str] = Counter()
        for cf in candidates:
            if cf.setup_ok:
                continue
            reasons = cf.reasons or ["unspecified_fail"]
            for reason in reasons:
                counts[reason] += 1
            combo = "+".join(sorted(set(reasons)))
            if combo:
                combos[combo] += 1
        if not counts:
            return
        logger.info(
            "[PB1][FAIL_REASONS] counts=%s top_combos=%s%s",
            counts.most_common(10),
            combos.most_common(5),
            f" note={note}" if note else "",
        )

    def _select_candidates_with_fallback(
        self,
        candidates: List[CandidateFeature],
    ) -> tuple[List[CandidateFeature], str, FilterThresholds, Counter[str], list[str], int, float, bool]:
        filters = self.effective_entry_filters
        strict_thresholds = self._resolve_strict_thresholds()
        if self.bootstrap_enabled:
            medium_thresholds = self.filter_thresholds.with_overrides(
                vol_contraction_max=min(float(filters["vol_max"]), 1.10),
                volu_contraction_max=min(float(filters["volu_max"]), 1.10),
                pullback_min=max(float(filters["pullback_min"]), 0.01),
                pullback_max=min(float(filters["pullback_max"]), 0.22),
                require_both_contractions=False,
            )
            loose_thresholds = self.filter_thresholds.with_overrides(
                vol_contraction_max=float(filters["vol_max"]),
                volu_contraction_max=float(filters["volu_max"]),
                pullback_min=float(filters["pullback_min"]),
                pullback_max=float(filters["pullback_max"]),
                require_both_contractions=bool(filters["require_both_contractions"]),
            )
        else:
            medium_thresholds = self.filter_thresholds.with_overrides(
                vol_contraction_max=1.00,
                volu_contraction_max=1.00,
                pullback_min=0.03,
                pullback_max=0.15,
                require_both_contractions=True,
            )
            loose_thresholds = self.filter_thresholds.with_overrides(
                vol_contraction_max=1.10,
                volu_contraction_max=1.10,
                pullback_min=0.02,
                pullback_max=0.18,
                require_both_contractions=False,
            )
        tiers = [
            ("tier1", strict_thresholds),
            ("tier2", medium_thresholds),
            ("tier3", loose_thresholds),
        ]
        selected_candidates: List[CandidateFeature] = []
        selected_tier = tiers[-1][0]
        selected_thresholds = tiers[-1][1]
        tiers_tried: list[str] = []
        all_reason_counts: Counter[str] = Counter()
        relax_passes_used = 0
        applied_min_score = float(filters["min_score_base"])
        applied_require_both = bool(filters["require_both_contractions"])

        min_score_values: list[float] = []
        current = float(filters["min_score_base"])
        floor = float(filters["min_score_floor"])
        step = max(float(filters["min_score_step"]), 1.0)
        max_passes = max(1, int(filters["relax_passes"]))
        while current >= floor and len(min_score_values) < max_passes:
            min_score_values.append(current)
            current -= step
        if not min_score_values:
            min_score_values = [float(applied_min_score)]

        relax_passes: list[tuple[float, bool, str]] = [
            (
                score,
                bool(filters["require_both_contractions"]),
                "score_relax",
            )
            for score in min_score_values
        ]
        if (not self.bootstrap_enabled) and bool(filters["require_both_contractions"]) and len(relax_passes) < max_passes + 1:
            relax_passes.append((min_score_values[-1], False, "require_both_off"))

        for min_score, require_both, relax_label in relax_passes:
            relax_passes_used += 1
            applied_min_score = min_score
            applied_require_both = require_both
            for tier_name, thresholds in tiers:
                tiers_tried.append(f"{tier_name}:{relax_label}")
                adjusted = thresholds.with_overrides(require_both_contractions=require_both)
                evaluated = self._apply_thresholds(
                    candidates,
                    adjusted,
                    min_score=min_score,
                    log_results=False,
                    soft_mode=PB1_FAILMODE_SOFT,
                    spread_hard_max_pct=float(PB1_SPREAD_HARD_MAX_PCT),
                    gap_hard_max_pct=float(PB1_GAP_HARD_MAX_PCT),
                )
                ok_count = len([c for c in evaluated if c.setup_ok])
                reason_counts = self._collect_reason_counts(evaluated)
                all_reason_counts.update(reason_counts)
                logger.info(
                    "[PB1][CANDIDATES] tier=%s pass=%s ok=%s total=%s thresholds={vol_max:%.2f volu_max:%.2f pullback_min:%.3f pullback_max:%.3f require_both:%s min_score:%.1f}",
                    tier_name,
                    relax_label,
                    ok_count,
                    len(evaluated),
                    adjusted.vol_contraction_max,
                    adjusted.volu_contraction_max,
                    adjusted.pullback_min,
                    adjusted.pullback_max,
                    adjusted.require_both_contractions,
                    min_score,
                )
                if ok_count > 0:
                    selected_candidates = self._apply_thresholds(
                        candidates,
                        adjusted,
                        min_score=min_score,
                        log_results=True,
                        soft_mode=PB1_FAILMODE_SOFT,
                        spread_hard_max_pct=float(PB1_SPREAD_HARD_MAX_PCT),
                        gap_hard_max_pct=float(PB1_GAP_HARD_MAX_PCT),
                    )
                    selected_tier = tier_name
                    selected_thresholds = adjusted
                    break
                selected_candidates = evaluated
                selected_tier = tier_name
                selected_thresholds = adjusted
            if selected_candidates and any(c.setup_ok for c in selected_candidates):
                break

        if selected_candidates and all(c.setup_ok is False for c in selected_candidates):
            selected_candidates = self._apply_thresholds(
                candidates,
                selected_thresholds,
                min_score=applied_min_score,
                log_results=True,
                soft_mode=PB1_FAILMODE_SOFT,
                spread_hard_max_pct=float(PB1_SPREAD_HARD_MAX_PCT),
                gap_hard_max_pct=float(PB1_GAP_HARD_MAX_PCT),
            )
        return (
            selected_candidates,
            selected_tier,
            selected_thresholds,
            all_reason_counts,
            tiers_tried,
            relax_passes_used,
            applied_min_score,
            applied_require_both,
        )

    def _apply_score_fallback(self, candidates: List[CandidateFeature]) -> int:
        scored: list[CandidateFeature] = []
        for cf in candidates:
            if not cf.features.get("data_ok"):
                continue
            try:
                rs_p = float(cf.features.get("rs_percentile") or 0.0)
                vcp_info = {
                    "score": cf.features.get("vcp_score"),
                    "vcp_ok": cf.features.get("vcp_ok"),
                    "contractions": cf.features.get("vcp_contractions"),
                }
                score = float(score_setup(cf.features, rs_percentile=rs_p, vcp_info=vcp_info, cfg=self.minervini_config))
            except Exception:
                score = 0.0
            cf.features["score"] = score
            cf.score = score
            scored.append(cf)

        if not scored:
            return 0

        scored.sort(key=lambda c: float(c.features.get("score") or 0.0), reverse=True)
        max_n = max(1, int(PB1_MAX_POSITIONS))
        min_n = max(1, int(PB1_MIN_CANDIDATES))
        selected = scored[: min(max_n, max(min_n, 1))]
        selected_codes = {c.code for c in selected}
        for cf in candidates:
            if cf.code in selected_codes:
                cf.setup_ok = True
                cf.features["score_fallback"] = True
                cf.reasons = list(cf.reasons or []) + ["score_fallback"]
        logger.info(
            "[PB1][CANDIDATES][FALLBACK] mode=score_based selected=%s total=%s",
            len(selected_codes),
            len(candidates),
        )
        return len(selected_codes)

    def _apply_adaptive_score_cut(
        self,
        candidates: List[CandidateFeature],
        target_new_positions: int,
    ) -> tuple[float, set[str]]:
        filters = self.effective_entry_filters
        ok_setups = [c for c in candidates if c.setup_ok]
        if not ok_setups:
            base_default = float(filters["min_score_base"])
            return base_default, set()
        target = min(max(1, target_new_positions), len(ok_setups)) if target_new_positions > 0 else min(1, len(ok_setups))
        base_cut = float(filters["min_score_base"])
        floor_cut = float(filters["min_score_floor"])
        step = max(float(filters["min_score_step"]), 1.0)
        applied_cut = base_cut
        selected: list[CandidateFeature] = []
        current_cut = base_cut
        while current_cut >= floor_cut:
            current = [
                cf
                for cf in ok_setups
                if float(cf.features.get("score") or 0.0) >= current_cut or cf.features.get("score_fallback")
            ]
            applied_cut = current_cut
            selected = current
            if len(current) >= target:
                break
            current_cut -= step
        if len(selected) < target:
            ordered = sorted(ok_setups, key=lambda c: float(c.features.get("score") or 0.0), reverse=True)
            selected = ordered[:target]
        if self.bootstrap_enabled and len(selected) <= 1:
            keep_n = max(1, int(filters["score_keep_topn"]))
            ordered = sorted(ok_setups, key=lambda c: float(c.features.get("score") or 0.0), reverse=True)
            selected = ordered[: min(keep_n, len(ordered))]
            kept_codes = [c.code for c in selected]
            logger.warning(
                "[PB1][BOOTSTRAP][SCORE_CUT_BYPASS] before=%s after=%s keep_topn=%s codes=%s",
                len(ok_setups),
                len(kept_codes),
                keep_n,
                kept_codes,
            )
        selected_codes = {c.code for c in selected}
        logger.info(
            "[PB1][SCORE_CUT] base=%.1f floor=%.1f step=%.1f target=%s ok_setups=%s after_cut=%s applied=%.1f",
            base_cut,
            floor_cut,
            step,
            target,
            len(ok_setups),
            len(selected_codes),
            applied_cut,
        )
        logger.info(
            "[PB1][SCORE_CUT][RESULT] applied_cut=%.1f after_count=%s codes=%s",
            applied_cut,
            len(selected_codes),
            sorted(selected_codes),
        )
        return applied_cut, selected_codes

    def _size_positions(self, candidates: List[CandidateFeature]) -> List[CandidateFeature]:
        ok_list = [c for c in candidates if c.setup_ok]
        if not ok_list:
            return candidates

        # 1) 점수 계산 + Adaptive score cut + ATR/유동성 컷
        for cf in ok_list:
            try:
                rs_p = float(cf.features.get("rs_percentile") or 0.0)
                vcp_info = {
                    "score": cf.features.get("vcp_score"),
                    "vcp_ok": cf.features.get("vcp_ok"),
                    "contractions": cf.features.get("vcp_contractions"),
                }
                score = float(score_setup(cf.features, rs_percentile=rs_p, vcp_info=vcp_info, cfg=self.minervini_config))
            except Exception:
                score = 0.0
            cf.features["score"] = score
            cf.score = score
        applied_cut, selected_codes = self._apply_adaptive_score_cut(
            ok_list,
            target_new_positions=int(self.target_new_positions or 0),
        )
        score_cut_codes = sorted(selected_codes)
        self._debug_score_cut_codes = score_cut_codes
        logger.info(
            "[PB1][SCORE_CUT][CODES] count=%s codes=%s",
            len(score_cut_codes),
            score_cut_codes,
        )
        for cf in ok_list:
            score_fallback = bool(cf.features.get("score_fallback"))
            if cf.code not in selected_codes and not score_fallback:
                cf.reasons.append("score_below_cut")
                cf.features["score_below_cut"] = True
        filtered: List[CandidateFeature] = []
        # ATR% 상한 단위 가드 (config가 6, 7, 8 등으로 오면 0.06, 0.07, 0.08로 교정)
        atr_max_ratio = PB1_MAX_ATR_PCT
        if atr_max_ratio > 1.0:
            atr_max_ratio = atr_max_ratio / 100.0
        for cf in ok_list:
            if not cf.setup_ok:
                continue
            atr_ratio = cf.features.get("atr_pct")  # ratio (0~1)
            value20 = cf.features.get("value20")
            risk_reasons: list[str] = []
            atr_missing = atr_ratio is None or (isinstance(atr_ratio, float) and atr_ratio != atr_ratio)
            value_missing = value20 is None or (isinstance(value20, float) and value20 != value20)
            
            # ATR 스케일 방어 (데이터 이상 감지)
            if not atr_missing:
                atr_val = float(atr_ratio)
                close_val = cf.features.get("close", 0.0)
                atr_abs = cf.features.get("atr14", 0.0)
                
                # 스케일 이상: ratio > 0.5 (50%) 같은 비정상 값
                if atr_val > 0.5:
                    logger.warning(
                        "[PB1][SCALE_SUSPECT] code=%s atr_ratio=%.4f (%.2f%%) atr=%.1f close=%.1f - 단위 혼선 의심",
                        self._display_code(cf.code), atr_val, atr_val * 100, atr_abs, close_val
                    )
                    risk_reasons.append("invalid_atr")
                elif close_val <= 0:
                    risk_reasons.append("invalid_close")
                elif atr_abs <= 0:
                    risk_reasons.append("invalid_atr")
            
            if atr_missing:
                risk_reasons.append("atr_pct_missing")
            elif float(atr_ratio) > float(atr_max_ratio):
                risk_reasons.append("atr_pct_too_high")
            if value_missing:
                risk_reasons.append("value20_missing")
            elif float(value20) < float(PB1_MIN_VALUE20):
                risk_reasons.append("liquidity_too_low")

            # 로그: atr, close, ratio, pct 모두 표시
            atr_abs = cf.features.get("atr14", 0.0)
            close_val = cf.features.get("close", 0.0)
            logger.info(
                "[PB1][RISK_GATE] code=%s ok=%s reasons=%s atr=%.1f close=%.1f atr_ratio=%.4f atr_pct=%.2f%% atr_max=%.2f%%",
                self._display_code(cf.code),
                int(not risk_reasons),
                risk_reasons or ["ok"],
                atr_abs,
                close_val,
                float(atr_ratio) if atr_ratio is not None else 0.0,
                float(atr_ratio) * 100 if atr_ratio is not None else 0.0,
                float(atr_max_ratio) * 100,
            )

            if risk_reasons:
                cf.setup_ok = False
                cf.reasons.extend(risk_reasons)
                continue

            filtered.append(cf)

        risk_ok_codes = [c.code for c in filtered]
        risk_fail_items = [
            (c.code, list(c.reasons or []))
            for c in ok_list
            if not c.setup_ok
        ]
        self._debug_risk_ok_codes = risk_ok_codes
        logger.info("[PB1][RISK_GATE][PASS] count=%s codes=%s", len(risk_ok_codes), risk_ok_codes)
        logger.info("[PB1][RISK_GATE][FAIL] count=%s sample=%s", len(risk_fail_items), risk_fail_items[:10])

        if not filtered:
            return candidates

        # 2) 점수 내림차순 정렬
        filtered.sort(key=lambda c: float(c.features.get("score") or 0.0), reverse=True)
        ranked = filtered

        # 3) 사이징: tick budget 분할 + 1주 가능 필터
        tick_budget = float(self.entry_tick_budget_krw or 0.0)
        if tick_budget <= 0:
            tick_budget = float(self.entry_usable_krw or 0.0)
        if tick_budget <= 0:
            logger.warning("[PB1][SIZE] skip sizing: tick_budget=0 applied_score_cut=%.1f", applied_cut)
            return candidates

        min_order_krw = float(MIN_ORDER_KRW)
        max_pos_krw = tick_budget * float(PB1_MAX_POS_PCT)
        for cf in ranked:
            daily_close = self._to_float(cf.features.get("close"))
            # ✅ 단일 스냅샷 조회 (중복 호출 방지)
            snapshot = self._get_price_snapshot_cached(cf.code, market="J") if self.kis else {}
            ask, bid, prpr = _extract_px_from_snapshot(snapshot)
            
            # 호가 없으면 prpr로 처리
            fallback_used = None
            if ask is None or bid is None:
                fallback_used = "prpr_only"
                logger.info(
                    "[PB1][PRICE][FALLBACK] code=%s used=prpr_only prpr=%s",
                    cf.code,
                    prpr,
                )
            
            # quote dict 형식으로 변환 (기존 코드와 호환)
            quote = snapshot
            
            order_px, source = self._calc_order_price(cf.code, quote, daily_close)
            if order_px is None:
                cf.setup_ok = False
                cf.reasons.append("order_price_missing")
                continue
            last_price = self._to_float(quote.get("last") or quote.get("prpr") or quote.get("stck_prpr"))
            last_volume = self._to_float(quote.get("acml_vol") or quote.get("stck_vol") or quote.get("stck_trqu") or quote.get("volume"))
            if last_price:
                cf.features["last_price"] = float(last_price)
            if last_volume:
                cf.features["last_volume"] = float(last_volume)
            if source and source != "ask":
                logger.info("[PB1][PRICE][FALLBACK] code=%s used=%s px=%.2f", cf.code, source, order_px)
            cf.features["order_price"] = float(order_px)
            cf.features["order_price_source"] = source or "unknown"

        equity_krw = float(getattr(self, "_equity_krw", 0.0) or 0.0)
        if equity_krw <= 0:
            equity_krw = float(self.entry_usable_krw or 0.0)
        risk_mult = float(getattr(self, "_regime_risk_mult", 1.0))
        risk_krw = equity_krw * (float(RISK_PER_TRADE_PCT) / 100.0) * risk_mult
        target_new_positions = max(1, int(self.target_new_positions or len(ranked) or 1))
        per_position_budget = min(max_pos_krw, tick_budget / target_new_positions) if tick_budget > 0 else max_pos_krw
        self._budget_plan_meta = {
            "risk_krw": risk_krw,
            "per_position_budget": per_position_budget,
            "tick_budget": tick_budget,
            "target_new_positions": target_new_positions,
            "max_pos_krw": max_pos_krw,
            "entry_capital": float(self.entry_capital_krw or 0.0),
        }
        force_min1_enabled = bool(self.force_min1_enabled)
        force_min1_topn = max(1, int(self._int_env("FORCE_MIN1_OVERRIDE_TOPN", FORCE_MIN1_OVERRIDE_TOPN or self.force_min1_topn or 2)))
        force_min1_override_position_cap = self._bool_env("FORCE_MIN1_OVERRIDE_POSITION_CAP", FORCE_MIN1_OVERRIDE_POSITION_CAP)
        order_possible_cash = float(self.order_possible_cash_krw or 0.0)
        allocated_slots = 0
        for cf in ranked:
            if not cf.setup_ok:
                continue
            order_px = self._to_float(cf.features.get("order_price")) or 0.0
            if order_px <= 0:
                cf.setup_ok = False
                cf.reasons.append("order_price_missing")
                continue
            stop0, stop_source, stop_degraded, stop_reason = self._build_stop_price(cf, order_px)
            if stop0 is None:
                cf.setup_ok = False
                cf.reasons.append("stop_calc_fail")
                if stop_reason:
                    logger.warning("[STOP][BUILD][FAIL] code=%s reason=%s", cf.code, stop_reason)
                continue
            cf.features["stop_source"] = stop_source
            cf.features["stop_degraded"] = int(stop_degraded)
            per_share_risk = order_px - stop0
            if per_share_risk <= 0:
                cf.setup_ok = False
                cf.reasons.append("risk_invalid")
                continue
            budget_cap = min(per_position_budget, max_pos_krw)
            raw_risk_qty = calc_position_size(
                equity=equity_krw,
                risk_pct=float(RISK_PER_TRADE_PCT),
                entry=order_px,
                stop=stop0,
                risk_mult=risk_mult,
            )
            qty = raw_risk_qty
            buy_ref_price = float(order_px) * (1.0 + float(BUY_PRICE_BUFFER_PCT))
            usable_cash = float(locals().get("usable_cash", getattr(self, "entry_usable_krw", 0.0)) or 0.0)
            order_possible_cash = float(locals().get("order_possible_cash", getattr(self, "_order_possible_cash", 0.0)) or 0.0)
            cash_available_for_order = float(order_possible_cash if order_possible_cash > 0 else usable_cash)
            logger.info("[PB1][CASH][ORDERABLE] usable_cash=%.0f order_possible_cash=%.0f cash_available_for_order=%.0f", usable_cash, order_possible_cash, cash_available_for_order)
            affordable_qty, afford_details = _compute_affordable_buy_qty(
                target_budget=budget_cap,
                buy_ref_price=buy_ref_price,
                cash_available=cash_available_for_order,
                min_remaining_cash_krw=MIN_REMAINING_CASH_KRW,
                allow_single_share_override=ALLOW_SINGLE_SHARE_OVERRIDE,
                budget_flex_pct=BUDGET_FLEX_PCT,
            )
            if raw_risk_qty <= 0:
                qty = 0
            elif budget_cap > 0:
                qty = min(raw_risk_qty, affordable_qty) if affordable_qty > 0 else 0
            logger.info(
                "[BUY][AFFORD] code=%s cash=%.0f target_budget=%.0f effective_budget=%.0f buy_ref_price=%.2f qty_by_budget=%s one_share_cost=%.2f single_share_override=%s final_qty=%s skip_reason=%s",
                self._display_code(cf.code),
                cash_available_for_order,
                afford_details["target_budget"],
                afford_details["effective_budget"],
                afford_details["buy_ref_price"],
                afford_details["qty_by_budget"],
                afford_details["one_share_cost"],
                int(bool(afford_details["single_share_override"])),
                afford_details["final_qty"],
                afford_details["skip_reason"] or "none",
            )
            allow_single_share_position_cap_override = _should_allow_single_share_position_cap_override(
                rank=ranked.index(cf) + 1 if cf in ranked else 999,
                final_qty=int(afford_details.get("final_qty") or 0),
                afford_details=afford_details,
                force_min1_topn=force_min1_topn,
                force_min1_override_position_cap=force_min1_override_position_cap,
                cash_available=cash_available_for_order,
                min_remaining_cash_krw=MIN_REMAINING_CASH_KRW,
                order_possible_cash=order_possible_cash,
            )
            
            # ===== New: 명확한 sizing_reason 계산 =====
            # ⚠️ CRITICAL: reserve는 _resolve_entry_capital()에서 이미 적용됨
            # usable = base_cash * (1 - reserve_pct)
            # 따라서 여기서 reserve_krw는 "정보용"이지, 재차감하지 않음!
            reserve_krw = float(self.entry_reserve_krw or 0.0)
            usable_cash = float(self.entry_usable_krw or 0.0)
            sizing_reason: str | None = None
            sizing_details: dict[str, Any] = {
                "tick_budget": tick_budget,
                "position_cap": budget_cap,
                "usable_cash": usable_cash,
                "order_px": order_px,
                "buy_ref_price": buy_ref_price,
                "effective_budget": afford_details["effective_budget"],
                "qty_by_budget": afford_details["qty_by_budget"],
                "one_share_cost": afford_details["one_share_cost"],
                "buy_mode": afford_details["buy_mode"],
                "binding_constraint": None,
            }
            
            # rank 계산 (최소 1주 보장용)
            rank = ranked.index(cf) + 1 if cf in ranked else 999
            
            # 1단계: qty=0 여부 체크 (예산 부족)
            raw_position_qty = (float(afford_details["effective_budget"]) / float(buy_ref_price)) if buy_ref_price > 0 and budget_cap > 0 else 0.0
            floor_position_qty = int(raw_position_qty) if raw_position_qty > 0 else 0
            position_cap_shortfall = float(buy_ref_price - float(afford_details["effective_budget"])) if budget_cap > 0 else float("inf")
            edge_qty_ok = int(buy_ref_price > 0 and floor_position_qty >= 1 and position_cap_shortfall <= 0)
            logger.info(
                "[SIZING][EDGE_CHECK] code=%s position_cap=%s order_px=%s raw_qty=%.3f floor_qty=%s final_ok=%s shortfall=%s",
                self._display_code(cf.code),
                budget_cap,
                order_px,
                raw_position_qty,
                floor_position_qty,
                edge_qty_ok,
                position_cap_shortfall,
            )
            if qty <= 0 and edge_qty_ok:
                qty = max(qty, floor_position_qty)

            if qty <= 0 or (order_px > 0 and qty * order_px < order_px):  # qty=0 또는 부분 삭감
                skip_reason = str(afford_details.get("skip_reason") or "").strip().upper()
                if skip_reason == "INSUFFICIENT_CASH_FOR_ONE_SHARE":
                    sizing_reason = "INSUFFICIENT_CASH_FOR_ONE_SHARE"
                    sizing_details["binding_constraint"] = "cash_available"
                elif skip_reason == "MIN_REMAINING_CASH_VIOLATION":
                    sizing_reason = "MIN_REMAINING_CASH_VIOLATION"
                    sizing_details["binding_constraint"] = "min_remaining_cash"
                elif skip_reason == "INSUFFICIENT_BUDGET_AND_OVERRIDE_DISABLED":
                    sizing_reason = "INSUFFICIENT_BUDGET_AND_OVERRIDE_DISABLED"
                    sizing_details["binding_constraint"] = "override_disabled"
                elif skip_reason == "QUANTITY_ZERO_AFTER_BUDGET_CHECK":
                    sizing_reason = "QUANTITY_ZERO_AFTER_BUDGET_CHECK"
                    sizing_details["binding_constraint"] = "budget_check"
                elif order_px > tick_budget > 0:
                    sizing_reason = "ORDER_PX_ABOVE_TICK_BUDGET"
                    sizing_details["binding_constraint"] = "tick_budget"
                elif order_px > budget_cap > 0 and floor_position_qty < 1:
                    sizing_reason = "ORDER_PX_ABOVE_POSITION_CAP"
                    sizing_details["binding_constraint"] = "position_cap"
                elif order_px > usable_cash > 0:
                    sizing_reason = "ORDER_PX_ABOVE_USABLE_CASH"
                    sizing_details["binding_constraint"] = "usable_cash"
                else:
                    sizing_reason = "ORDER_PX_ABOVE_POSITION_CAP"
                    sizing_details["binding_constraint"] = "position_cap"
                if allow_single_share_position_cap_override and sizing_reason == "ORDER_PX_ABOVE_POSITION_CAP":
                    qty = 1
                    sizing_reason = "OK_SINGLE_SHARE_POSITION_CAP_OVERRIDE"
                    sizing_details.update(
                        {
                            "rank": rank,
                            "position_cap_override": 1,
                            "single_share_override": 1,
                            "cash_ok": 1,
                            "forced_qty": 1,
                            "binding_constraint": "position_cap_override",
                        }
                    )
                    logger.info(
                        "[PB1][SIZING][OVERRIDE] code=%s rank=%s single_share_override=1 position_cap_override=1 cash_ok=1 final_qty=1 reason=OK_SINGLE_SHARE_POSITION_CAP_OVERRIDE",
                        self._display_code(cf.code),
                        rank,
                    )

                # 최소 1주 보장 옵션 체크
                remaining_slots = max(0, int(target_new_positions) - int(allocated_slots))
                min_1_share_allowed = (
                    qty <= 0
                    and
                    force_min1_enabled
                    and rank <= force_min1_topn
                    and order_px >= min_order_krw
                    and usable_cash >= order_px
                    and (force_min1_override_position_cap or budget_cap <= 0 or order_px <= budget_cap)
                    and (order_possible_cash <= 0 or order_possible_cash >= order_px)
                    and remaining_slots >= 1
                )

                logger.info(
                    "[PB1][SIZING][OVERRIDE_CHECK] code=%s eligible=%s position_cap_block=%s tick_budget_ok=%s allow_override=%s",
                    self._display_code(cf.code),
                    int(force_min1_enabled and rank <= force_min1_topn and qty <= 0),
                    int(sizing_reason == "ORDER_PX_ABOVE_POSITION_CAP"),
                    int(order_px <= tick_budget if tick_budget > 0 else 0),
                    int(min_1_share_allowed),
                )

                if qty <= 0 and min_1_share_allowed:
                    qty = 1
                    sizing_reason = "FORCE_MIN1_APPLIED"
                    sizing_details.update(
                        {
                            "rank": rank,
                            "topn": force_min1_topn,
                            "price": order_px,
                            "order_possible_cash": order_possible_cash,
                            "forced_qty": 1,
                            "force_min1_override_position_cap": int(force_min1_override_position_cap),
                        }
                    )
                    logger.info(
                        "[PB1][SIZING][OVERRIDE_APPLIED] code=%s qty=1",
                        self._display_code(cf.code),
                    )
                    logger.info(
                        "[PB1][SIZING][FORCE_MIN1] code=%s rank=%s price=%s usable_cash=%s qty=1 reason=FORCE_MIN1_APPLIED override_position_cap=%s",
                        self._display_code(cf.code),
                        rank,
                        order_px,
                        usable_cash,
                        int(force_min1_override_position_cap),
                    )
                elif qty <= 0:
                    # ❌ 예산 부족: 1주도 못 삼
                    qty = 0
                    sizing_details.update(
                        {
                            "rank": rank,
                            "buy_budget_after_reserve": tick_budget,
                            "target_budget": afford_details["target_budget"],
                            "effective_budget": afford_details["effective_budget"],
                            "price": order_px,
                            "required_for_1_share": afford_details["one_share_cost"],
                            "shortfall": max(0, order_px - min(x for x in [tick_budget, budget_cap, usable_cash] if x > 0) if any(x > 0 for x in [tick_budget, budget_cap, usable_cash]) else order_px),
                            "reserve_applied_at_capital_level": reserve_krw,
                            "budget_cap": budget_cap,
                            "order_possible_cash": order_possible_cash,
                            "cash_available": cash_available_for_order,
                            "skip_reason": afford_details["skip_reason"],
                            "force_min1_enabled": int(force_min1_enabled),
                            "force_min1_topn": force_min1_topn,
                            "force_min1_override_position_cap": int(force_min1_override_position_cap),
                        }
                    )
            else:
                # 2단계: qty>=1인데 최소주문금액 체크
                planned_notional = float(qty * order_px)
                if min_order_krw > 0 and planned_notional < min_order_krw:
                    qty = 0
                    sizing_reason = "MIN_ORDER_KRW_NOT_MET"
                    sizing_details.update(
                        {
                            "rank": rank,
                            "qty": qty,
                            "price": order_px,
                            "planned_notional": planned_notional,
                            "min_order_krw": min_order_krw,
                            "shortfall": max(0, min_order_krw - planned_notional),
                            "binding_constraint": "min_order_krw",
                        }
                    )
                # 3단계: qty>=1이고 최소주문 조건도 충족하면 OK
            
            # 기존 sizing_reasons 로직 (하위호환성)
            sizing_reasons: list[str] = []
            if qty <= 0:
                if tick_budget <= 0:
                    sizing_reasons.append("tick_budget_zero")
                if min_order_krw > 0 and tick_budget > 0 and min_order_krw > tick_budget:
                    sizing_reasons.append("min_order_krw_gt_tick_budget")
                if order_px > 0 and tick_budget > 0 and order_px > tick_budget:
                    sizing_reasons.append("price_gt_tick_budget")
                if min_order_krw > 0 and order_px > 0 and order_px > min_order_krw:
                    sizing_reasons.append("price_gt_min_order")
                if reserve_krw > 0:
                    sizing_reasons.append("reserve_applied")
                if not sizing_reasons:
                    sizing_reasons.append(sizing_reason or "FORCE_MIN1_NOT_ELIGIBLE")
            
            logger.info(
                "[PB1][SIZING] code=%s rank=%s cash_usable=%.0f tick_budget=%.0f reserve=%.0f px=%.0f qty=%s min_order_krw=%.0f ok=%s reason=%s detail=%s",
                self._display_code(cf.code),
                rank,
                usable_cash,
                tick_budget,
                reserve_krw,
                order_px,
                qty,
                min_order_krw,
                int(qty > 0),
                sizing_reason or "OK",
                sizing_details or {},
            )
            if qty <= 0:
                cf.setup_ok = False
                cf.sizing_reason = _normalize_sizing_failure_reason(sizing_reason)
                cf.reasons.append(cf.sizing_reason)
                cf.sizing_details = sizing_details
                continue
            logger.info(
                "[PB1][SIZING][QTY] code=%s price=%.0f per_position_budget=%.0f tick_budget_remaining=%.0f qty_raw=%.3f qty_final=%s",
                self._display_code(cf.code),
                order_px,
                per_position_budget,
                max(0.0, float(tick_budget) - float(getattr(self, "_planned_entry_spent_krw", 0.0) or 0.0)),
                float(raw_risk_qty or 0.0),
                qty,
            )
            if qty == 1:
                one_share_reason = "budget_limited" if int(affordable_qty or 0) <= 1 else "risk_limited"
                logger.info(
                    "[PB1][SIZING][ONE_SHARE_ONLY] code=%s reason=%s",
                    self._display_code(cf.code),
                    one_share_reason,
                )
            cf.planned_qty = qty
            cf.planned_value = float(qty * order_px)
            cf.features["planned_cap"] = float(budget_cap)
            cf.features["planned_value"] = cf.planned_value
            cf.features["initial_stop"] = float(stop0)
            cf.features["stop_price"] = float(stop0)
            cf.sizing_reason = "SIZING_OK"
            cf.sizing_details = {"qty": qty, "price": order_px, "notional": cf.planned_value}
            cf.client_order_key = self._client_order_key(cf.code, cf.mode, "BUY", "close", "PB1")
            allocated_slots += 1
            logger.info(
                "[PB1][RANK] code=%s score=%.1f cap=%.0f qty=%s value=%.0f atr_pct=%.2f%% value20=%s tick_budget=%.0f",
                cf.code,
                float(cf.features.get("score") or 0.0),
                float(cf.features.get("planned_cap") or 0.0),
                cf.planned_qty,
                cf.planned_value,
                float(cf.features.get("atr_pct") or 0.0) * 100,  # ratio -> %
                cf.features.get("value20"),
                tick_budget,
            )

        sized_ok = [c.code for c in candidates if (c.planned_qty or 0) > 0 and c.setup_ok]
        sized_fail = [
            {
                "code": c.code,
                "reason": _normalize_sizing_failure_reason(getattr(c, "sizing_reason", None)),
                "detail": getattr(c, "sizing_details", None) or {"binding_constraint": "unknown"},
            }
            for c in candidates
            if bool(getattr(c, "sizing_reason", None)) and getattr(c, "sizing_reason", None) not in {"SIZING_OK"} and not ((c.planned_qty or 0) > 0 and c.setup_ok)
        ]
        self._debug_sizing_ok_codes = sized_ok
        self._debug_sizing_fail_items = sized_fail
        logger.info("[PB1][SIZING][PASS] count=%s codes=%s", len(sized_ok), sized_ok)
        logger.info("[PB1][SIZING][FAIL] count=%s sample=%s", len(sized_fail), sized_fail[:10])

        return candidates

    def _mark_price(self, code: str) -> float | None:
        # Tick 내 최대 조회 수 제한 (안전장치)
        if self.price_fetch_count >= PB1_MAX_PRICE_FETCH_PER_TICK:
            self._warn_once(
                "price_probe_limit",
                "[PB1][PRICE][LIMIT] price_fetch_count=%s >= max=%s -> skip_price_probe",
                self.price_fetch_count,
                PB1_MAX_PRICE_FETCH_PER_TICK,
            )
            return None
        
        if self.kis:
            # ✅ 서킷 브레이커 체크
            try:
                from trader.kis_wrapper import _price_cache
                if _price_cache.is_circuit_open():
                    self._warn_once(
                        "price_circuit_open",
                        "[PB1][PRICE][CIRCUIT_OPEN] skip price fetch until circuit closes (until=%.0f)",
                        _price_cache.circuit_until,
                    )
                    return None
            except Exception as e:
                logger.debug("[PB1][PRICE][CIRCUIT_CHECK_FAIL] %s", e)
            
            try:
                self.price_fetch_count += 1
                
                # ✅ 단일 스냅샷 조회 (중복 호출 방지)
                snapshot = self._get_price_snapshot_cached(code, market="J")
                ask, bid, prpr = _extract_px_from_snapshot(snapshot)
                
                # ask/bid 없으면 prpr로 처리 (재호출 금지)
                if ask is None or bid is None:
                    self.askbid_fail_count += 1
                    if prpr is None:
                        logger.warning("[PB1][PRICE][SKIP] code=%s no ask/bid/prpr snapshot=%s", code, snapshot)
                        return None
                    # prpr만 있으면 prpr 사용
                    logger.info("[PB1][PRICE][FALLBACK_PRPR] code=%s prpr=%.0f (no ask/bid)", code, prpr)
                    return prpr
                
                # ask가 있으면 ask 사용
                if ask and ask > 0:
                    return ask
                
                # fallback to prpr
                if prpr and prpr > 0:
                    return prpr
                
                logger.warning("[PB1][PRICE][WARN] code=%s no valid price ask=%s bid=%s prpr=%s", code, ask, bid, prpr)
                return None
                
            except Exception as exc:
                # ✅ RATE_LIMIT 감지 시 서킷 오픈
                exc_str = str(exc).lower()
                if "rate_limit" in exc_str or "egw002" in exc_str or "초당" in exc_str:
                    try:
                        from trader.kis_wrapper import _price_cache
                        _price_cache.open_circuit()
                        logger.warning(
                            "[PB1][PRICE][RATE_LIMIT] code=%s opened circuit for %ss err=%s",
                            code,
                            _price_cache.circuit_sec,
                            repr(exc),
                        )
                    except Exception as e2:
                        logger.debug("[PB1][PRICE][CIRCUIT_OPEN_FAIL] %s", e2)
                self._warn_once(f"quote_fail:{code}", "[PB1][PRICE][FAIL] code=%s err=%s", code, repr(exc))
        return None

    def _resolve_price_with_fallback(self, code: str, *, ohlcv_close: float | None = None) -> tuple[float | None, str | None]:
        # [2026-04-30] PB1_USE_BALANCE_PRPR_FIRST=1: balance prpr를 quote보다 먼저 사용
        use_balance_first = os.getenv("PB1_USE_BALANCE_PRPR_FIRST", "1") not in {"0", "false", "False"}
        if use_balance_first and code in self._balance_price_map:
            fallback_price = self._balance_price_map.get(code)
            if fallback_price:
                logger.info("[PRICE][SOURCE] code=%s source=kis_balance_prpr price=%s", code, fallback_price)
                return float(fallback_price), "kis_balance_prpr"
        price = self._mark_price(code)
        if price is not None:
            return price, "quote"
        if code in self._balance_price_map:
            fallback_price = self._balance_price_map.get(code)
            if fallback_price:
                logger.info("[PRICE][SOURCE] code=%s source=kis_balance_prpr price=%s", code, fallback_price)
                return float(fallback_price), "kis_balance_prpr"
        if ohlcv_close is not None and ohlcv_close > 0:
            logger.info("[PRICE][SOURCE] code=%s source=ohlcv_close price=%s", code, ohlcv_close)
            return float(ohlcv_close), "ohlcv_close"
        logger.info("[PB1][PRICE][UNAVAILABLE] code=%s", code)
        return None, None

    def _run_price_probe(self) -> None:
        """가격 API 진단용 프로브: LIVE에서 주문 없이 가격 호출만 검증."""
        if not self.kis:
            return
        probe_codes = []
        # 보유종목 우선
        if hasattr(self, '_balance_positions') and self._balance_positions:
            probe_codes.extend(list(self._balance_positions.keys())[:2])
        # 유니버스에서 추가
        if hasattr(self, '_universe') and self._universe:
            universe_codes = [u.get('code') for u in self._universe if u.get('code')][:3]
            probe_codes.extend(universe_codes[:3 - len(probe_codes)])
        # 중복 제거
        probe_codes = list(dict.fromkeys(probe_codes))[:3]
        
        if not probe_codes:
            logger.info("[PB1][PRICE_PROBE] no codes to probe")
            return
        
        logger.info("[PB1][PRICE_PROBE] probing codes=%s", probe_codes)
        for code in probe_codes:
            try:
                # ✅ 단일 스냅샷 조회
                snapshot = self._get_price_snapshot_cached(code, market="J")
                ask, bid, prpr = _extract_px_from_snapshot(snapshot)
                
                if ask is None or bid is None:
                    logger.error(
                        "[PB1][PRICE_PROBE][FAIL] code=%s ask=%s bid=%s prpr=%s",
                        code, ask, bid, prpr
                    )
                else:
                    logger.info("[PB1][PRICE_PROBE][OK] code=%s ask=%s bid=%s", code, ask, bid)
            except Exception as exc:
                logger.error("[PB1][PRICE_PROBE][EXCEPTION] code=%s err=%s", code, repr(exc))

    def _calc_order_price(
        self,
        code: str,
        quote: dict | None,
        daily_close: float | None,
    ) -> tuple[float | None, str | None]:
        ask = self._to_float((quote or {}).get("ask"))
        prpr = self._to_float((quote or {}).get("prpr") or (quote or {}).get("stck_prpr") or (quote or {}).get("last"))
        close = self._to_float(daily_close)
        if ask and ask > 0:
            return ask, "ask"
        if prpr and prpr > 0:
            raw_slippage = os.getenv("PB1_ORDER_SLIPPAGE_BPS") or str(PRICE_SLIPPAGE_PCT_BUY)
            fraction, percent_for_log = self._normalize_slippage(raw_slippage)
            order_price = prpr * (1 + fraction)
            logger.info(
                "[PB1][PRICE][FALLBACK] code=%s raw=%s fraction=%.6f percent=%.2f%% base=%.0f final=%.0f",
                code,
                raw_slippage,
                fraction,
                percent_for_log,
                prpr,
                order_price,
            )
            return order_price, "prpr_slippage"
        if close and close > 0:
            return close, "daily_close"
        logger.info("[PB1][PRICE][UNAVAILABLE] code=%s", code)
        return None, None

    @staticmethod
    def _normalize_slippage(raw_value: Any) -> tuple[float, float]:
        raw_text = str(raw_value).strip() if raw_value is not None else ""
        try:
            numeric = float(raw_value)
        except (TypeError, ValueError):
            numeric = float(PRICE_SLIPPAGE_PCT_BUY)
            raw_text = str(numeric)
        if raw_text and raw_text.replace("-", "", 1).isdigit() and numeric >= 1.0:
            fraction = numeric / 10000.0
        elif numeric >= 1.0:
            fraction = numeric / 100.0
        else:
            fraction = numeric
        fraction = max(0.0, fraction)
        return fraction, fraction * 100.0

    def _build_budget_plan(
        self,
        ranked: list[CandidateFeature],
        *,
        tick_budget: float,
        target_new_positions: int,
        min_order_krw: float,
        max_pos_krw: float,
    ) -> tuple[list[CandidateFeature], dict[str, Any], Counter[str]]:
        affordable: list[CandidateFeature] = []
        drop_reasons: Counter[str] = Counter()
        for cf in ranked:
            px = self._to_float(cf.features.get("order_price"))
            if not px or px <= 0:
                cf.setup_ok = False
                cf.reasons.append("order_price_missing")
                drop_reasons["order_price_missing"] += 1
                continue
            need = max(px, min_order_krw)
            if need <= tick_budget:
                affordable.append(cf)
            else:
                cf.setup_ok = False
                cf.reasons.append("unaffordable_min1share")
                drop_reasons["unaffordable_min1share"] += 1

        if not affordable:
            meta = {"reason": "no_affordable_candidates"}
            logger.info(
                "[PB1][BUDGET_PLAN] effective_target=0 cap=0 affordable=0 drop_reasons=%s",
                drop_reasons.most_common(3),
            )
            return [], meta, drop_reasons

        effective_target = min(max(target_new_positions, 1), len(affordable))
        buyables: list[CandidateFeature] = []
        while effective_target >= 1:
            cap = tick_budget / effective_target
            cap = max(cap, min_order_krw)
            upper = max_pos_krw if max_pos_krw > 0 else tick_budget
            cap = min(cap, upper)
            buyables = []
            planned_drop: Counter[str] = Counter()
            for cf in affordable:
                px = self._to_float(cf.features.get("order_price")) or 0.0
                qty = int(cap // px) if px > 0 else 0
                planned_value = float(qty * px)
                if qty >= 1 and planned_value >= min_order_krw:
                    cf.planned_qty = qty
                    cf.planned_value = planned_value
                    cf.features["planned_cap"] = float(cap)
                    cf.features["planned_value"] = planned_value
                    buyables.append(cf)
                else:
                    cf.setup_ok = False
                    cf.reasons.append("planned_qty_zero_or_min_order")
                    planned_drop["planned_qty_zero_or_min_order"] += 1
            if buyables:
                meta = {"effective_target": effective_target, "cap": cap}
                drop_reasons.update(planned_drop)
                logger.info(
                    "[PB1][BUDGET_PLAN] effective_target=%s cap=%.0f affordable=%s buyable=%s drop_reasons=%s",
                    effective_target,
                    cap,
                    len(affordable),
                    len(buyables),
                    drop_reasons.most_common(3),
                )
                return buyables, meta, drop_reasons
            drop_reasons.update(planned_drop)
            effective_target -= 1

        meta = {"reason": "cannot_make_valid_qty"}
        logger.info(
            "[PB1][BUDGET_PLAN] effective_target=0 cap=0 affordable=%s drop_reasons=%s",
            len(affordable),
            drop_reasons.most_common(3),
        )
        return [], meta, drop_reasons

    def _fetch_marks(self, codes: Iterable[str], fallback: Dict[str, float]) -> Dict[str, float]:
        marks: Dict[str, float] = {}
        for code in codes:
            px, source = self._resolve_price_with_fallback(code, ohlcv_close=fallback.get(code))
            if px is not None:
                marks[code] = px
        return marks

    def _should_block_order(
        self,
        client_order_key: str,
        code: str | None = None,
        side: str = "BUY",
        stage: str | None = None,
    ) -> tuple[bool, dict | None]:
        """
        중복 주문 차단 여부 판정.
        - 성공/접수된 주문만 차단 (SUBMITTED, ACCEPTED, FILLED, PARTIAL_FILLED)
        - 실패/거절/스킵된 주문은 재시도 허용
        
        Returns:
            (should_block: bool, prior_order_info: dict | None)
        """
        if not client_order_key or not code:
            return False, None
        
        # 오늘 같은 종목/사이드/스테이지에 블록 상태의 주문이 있는지 확인
        is_blocked, prior = self._safe_has_blocking_order_today(
            code=code,
            side=side,
            stage=stage,
            trade_date=self._today,
        )
        
        return is_blocked, prior

    def _pretrade_check(
        self,
        *,
        code: str,
        market: str | None,
        mode: int | None,
        side: str,
        qty: int | None,
        price: float | None,
        client_order_key: str | None,
        stage: str,
    ) -> bool:
        gate_reasons = self._order_precheck_gate_reasons(side=side, stage=stage)
        if gate_reasons:
            logger.info(
                "[ORDER_PRECHECK][SKIP] side=%s code=%s reasons=%s",
                side.upper(),
                self._display_code(code),
                gate_reasons,
            )
            return False
        if not self.kis:
            return False
        ok, reason = validate_tradeable(self.kis, code)
        if ok:
            return True
        display_code = self._display_code(code)
        logger.warning("[PB1][PRETRADE][SKIP] code=%s reason=%s stage=%s", display_code, reason, stage)
        try:
            self._append_ledger_event(
                event_type="ORDER_SKIP",
                code=code,
                market=market,
                mode=mode,
                side=side,
                qty=qty,
                price=price,
                client_order_key=client_order_key,
                ok=False,
                reasons=[f"pretrade:{reason}"],
                stage=stage,
            )
        except Exception:
            logger.exception("[PB1][LEDGER][PRETRADE_SKIP_FAIL] code=%s", display_code)
        return False

    def _order_precheck_gate_reasons(self, *, side: str, stage: str) -> list[str]:
        del stage
        reasons: list[str] = []
        if not self.trading_day:
            reasons.append("nontrading_day")
        if not bool(self.order_allowed):
            reasons.append("order_blocked")
        session_recovery_continue = bool(getattr(self, "session_recovery_continue", False) or getattr(self, "am_recovery_continue", False))
        if self.market_window_name == "after" and not (self.force_entry_window_override or session_recovery_continue):
            reasons.append("window_blocked")
        if self.force_block_live or not self.intended_live or self.strategy_mode == "DIAG":
            reasons.append("live_gate_blocked")
        deduped: list[str] = []
        for reason in reasons:
            if reason not in deduped:
                deduped.append(reason)
        return deduped

    def _resolve_exit_submit_gate_reasons(self, *, code: str) -> list[str]:
        reasons = self._order_precheck_gate_reasons(side="SELL", stage="PB1-EXIT")
        logger.info(
            "[EXIT][SUBMIT_GATE] code=%s order_allowed=%s trading_day=%s force_block_live=%s source=%s action=%s",
            self._display_code(code),
            int(bool(self.order_allowed)),
            int(bool(self.trading_day)),
            int(bool(self.force_block_live)),
            str((self._exit_holdings_meta or {}).get("source") or "unknown"),
            "skip_before_precheck" if reasons else "allow",
        )
        return reasons

    def _build_stop_price(self, cf: CandidateFeature, order_px: float) -> tuple[float | None, str | None, int, str | None]:
        features = cf.features or {}
        close_px = self._to_float(features.get("close") or features.get("last_price") or order_px)
        atr_val = self._to_float(features.get("atr14") or features.get("atr"))
        ma20 = self._to_float(features.get("ma20"))
        ma50 = self._to_float(features.get("ma50"))
        stop_price_at_entry = self._to_float(features.get("stop_price_at_entry") or features.get("stop_price"))
        pivot_val = features.get("pivot_price_at_entry")
        if pivot_val is None:
            pivot_val = features.get("pivot")
        tight_low = features.get("tight_low")
        entry_style_selected = str(features.get("entry_style_selected") or "").strip().upper()

        if stop_price_at_entry is not None and stop_price_at_entry > 0 and stop_price_at_entry < order_px:
            logger.info(
                "[STOP][BUILD] code=%s source=entry_metadata close=%s atr=%s stop=%s degraded=0",
                cf.code,
                close_px,
                atr_val,
                float(stop_price_at_entry),
            )
            return float(stop_price_at_entry), "entry_metadata", 0, None

        if pivot_val is not None or tight_low is not None:
            calc_stop = calc_initial_stop(
                pivot=float(pivot_val) if pivot_val is not None else float("nan"),
                tight_low=float(tight_low) if tight_low is not None else None,
                atr=float(atr_val) if atr_val is not None else None,
                mode=INITIAL_STOP_MODE,
                entry=order_px,
                atr_mult=ATR_MULT,
            )
            if calc_stop is not None and pd.notna(calc_stop) and float(calc_stop) > 0 and float(calc_stop) < order_px:
                logger.info(
                    "[STOP][BUILD] code=%s source=calc_initial_stop close=%s atr=%s stop=%s degraded=0",
                    cf.code,
                    close_px,
                    atr_val,
                    float(calc_stop),
                )
                return float(calc_stop), "calc_initial_stop", 0, None

        if close_px is not None and atr_val is not None and atr_val > 0:
            atr_mult = max(ATR_MULT, 2.2) if entry_style_selected == "MOMENTUM" else ATR_MULT
            stop_price = min(float(close_px - (atr_val * atr_mult)), order_px * 0.99)
            if stop_price > 0:
                logger.info(
                    "[STOP][BUILD] code=%s source=atr_fallback close=%s atr=%s stop=%s degraded=0",
                    cf.code,
                    close_px,
                    atr_val,
                    stop_price,
                )
                return stop_price, "atr_fallback", 0, None

        ma_candidates = [value for value in (ma20, ma50) if value is not None and value > 0]
        if close_px is not None and close_px > 0 and ma_candidates:
            stop_price = min(min(ma_candidates), float(close_px) * 0.97, order_px * 0.99)
            if stop_price > 0:
                logger.info(
                    "[STOP][BUILD] code=%s source=ma_fallback close=%s atr=%s stop=%s degraded=0",
                    cf.code,
                    close_px,
                    atr_val,
                    stop_price,
                )
                return stop_price, "ma_fallback", 0, None

        if close_px is not None and close_px > 0:
            stop_price = min(float(close_px) * 0.90, order_px * 0.99)
            if stop_price > 0:
                logger.info(
                    "[STOP][BUILD] code=%s source=hard_fallback close=%s atr=%s ma20=%s ma50=%s stop=%s degraded=1 reason=missing_all_primary_inputs",
                    cf.code,
                    close_px,
                    atr_val,
                    ma20,
                    ma50,
                    stop_price,
                )
                return stop_price, "hard_fallback", 1, "missing_all_primary_inputs"

        return None, None, 1, "missing_all_primary_inputs"

    def _submit_force_buy_order(self, code: str, qty: int) -> None:
        """
        FORCE_BUY 스모크 모드: 주문 endpoint까지 도달하는지 검증용.
        모의투자 전용. 시장가 또는 최우선 매수호가로 1주 강제 주문.
        """
        try:
            # 가격 조회
            quote = self._get_price_snapshot_cached(code, market="J")
            ask = quote.get("ask")
            prpr = quote.get("prpr") or quote.get("last")
            
            # ask 있으면 ask로, 없으면 prpr + 1틱
            if ask and ask > 0:
                price = float(ask)
            elif prpr and prpr > 0:
                tick = self.kis_api._get_tick_size(float(prpr))
                price = float(prpr) + tick
            else:
                logger.error("[FORCE_BUY][FAIL] code=%s no_price", code)
                return
            
            logger.warning(
                "[FORCE_BUY][SUBMIT] code=%s qty=%s price=%.0f mode=SMOKE_TEST",
                code, qty, price
            )
            
            # 주문 제출 (시장가 선호, 없으면 지정가)
            resp = self.kis_api.buy_market(code, qty) if hasattr(self.kis_api, 'buy_market') else self.kis_api.buy(code, price, qty)
            
            logger.warning(
                "[FORCE_BUY][RESULT] code=%s resp=%s",
                code, resp
            )
        except Exception as exc:
            logger.exception("[FORCE_BUY][ERROR] code=%s error=%s", code, exc)

    def _place_entry(self, cf: CandidateFeature) -> dict[str, int | str]:
        status: dict[str, Any] = self._empty_order_status()
        stock_name = str(self._name_for_code(cf.code) or cf.features.get("name") or cf.code)
        # ✅ 최종 방어선: intended_live=True인데 dry_run=True면 Fatal
        if self.intended_live and self.dry_run:
            raise RuntimeError(
                f"FATAL: order path reached with dry_run=True while intended_live=True. "
                f"code={cf.code} intended_live={self.intended_live} dry_run={self.dry_run}"
            )
        
        # NO_TRADE 모드: 주문 전송 스킵, 로그만 출력
        no_trade = os.getenv("NO_TRADE", "0") == "1"
        
        display_code = self._display_code(cf.code)
        identity = self._resolve_entry_identity_for_candidate(cf)
        decision_reason = identity["entry_reason"]
        logger.info(
            "[TRADE][DECISION][BUY] code=%s name=%s family=%s decision_reason=%s score=%.1f entry=%.2f stop=%.2f risk_pct=%.2f qty=%s budget=%.0f no_trade=%s",
            display_code,
            self._code_name_map.get(cf.code),
            identity["entry_style_selected"],
            decision_reason,
            float(cf.features.get("score") or 0.0),
            float(cf.features.get("entry_price") or cf.features.get("close") or 0.0),
            float(cf.features.get("stop_price") or 0.0),
            float(RISK_PER_TRADE_PCT),
            cf.planned_qty,
            float(cf.features.get("planned_cap") or 0.0),
            no_trade,
        )
        
        # ✅ DIAG 모드: 주문 생성만 하고 전송은 스킵 (SIM_ORDER 로그 및 DB 기록)
        if no_trade:
            # DIAG 모드인지 확인
            strategy_mode = os.getenv("STRATEGY_MODE", "").upper()
            if strategy_mode == "DIAG":
                # SIM_ORDER 로그 남기기
                logger.info(
                    "[SIM_ORDER][BUY] code=%s qty=%s limit=%.0f reason=DIAG_NO_HTTP mode=%s",
                    display_code,
                    cf.planned_qty,
                    float(cf.features.get("entry_price") or cf.features.get("close") or 0.0),
                    strategy_mode,
                )
                # ✅ DB에 SIMULATED 주문 기록
                try:
                    self._record_simulated_order(
                        code=display_code,
                        side="BUY",
                        qty=cf.planned_qty,
                        limit_price=float(cf.features.get("entry_price") or cf.features.get("close") or 0.0),
                        reason="DIAG_NO_HTTP",
                        mode=strategy_mode,
                    )
                except Exception as exc:
                    logger.warning("[SIM_ORDER][DB_FAIL] code=%s err=%s", display_code, exc)
                # ✅ 계속 실행하지 않고 return (주문 전송 스킵)
                status["skipped"] = 1
                status["skipped_reason"] = "diag_no_http"
                status["submit_terminal_status"] = "SKIPPED_BY_POLICY"
                return status
            else:
                logger.info(
                    "[TRADE][SKIP][NO_TRADE] code=%s qty=%s reason=NO_TRADE_MODE",
                    display_code,
                    cf.planned_qty,
                )
                status["skipped"] = 1
                status["skipped_reason"] = "no_trade_mode"
                status["submit_terminal_status"] = "SKIPPED_BY_POLICY"
                return status
        
        reasons = cf.reasons or []
        features_snapshot = {
            k: cf.features.get(k)
            for k in [
                "close",
                "ma50",
                "ma150",
                "ma200",
                "ma200_slope",
                "hi_52w",
                "lo_52w",
                "dollar_vol_50",
                "pivot",
                "rs_percentile",
                "score",
            ]
        }
        logger.info(
            "[PB1][ENTRY][WHY] code=%s selected_family=%s trigger_policy=%s reason_codes=%s reason_text=%s features_snapshot=%s stage=%s",
            display_code,
            identity["entry_style_selected"],
            cf.features.get("entry_trigger_policy") or "NONE",
            reasons,
            ", ".join(reasons),
            features_snapshot,
            "PB1-CLOSE",
        )
        order_type = "LIMIT"
        entry_price = float(cf.features.get("entry_price") or cf.features.get("close") or 0.0)
        limit_price = round_to_tick(entry_price * 1.003) if entry_price > 0 else cf.features.get("close")
        if (self.window_label or "").lower() == "preopen" and PB1_PREOPEN_ORDER_TYPE == "LIMIT":
            base_price, _source = self._resolve_price_with_fallback(
                cf.code,
                ohlcv_close=self._to_float(cf.features.get("close")),
            )
            if not base_price:
                logger.info("[PB1][ENTRY][SKIP] code=%s reason=price_unavailable", display_code)
                status["skipped_reason"] = "price_unavailable"
                return status
            buffer_pct = max(float(PB1_PREOPEN_LIMIT_BUFFER_PCT), 0.0)
            limit_price = round_to_tick(float(base_price) * (1 + buffer_pct / 100))
            order_type = "LIMIT"
        record_price = float(limit_price or entry_price or 0.0)
        pre_submit = self._resolve_entry_pre_submit(
            cf=cf,
            stage="PB1-CLOSE",
            order_price=record_price,
            order_type=order_type,
            allow_add_to_existing=False,
        )
        if not pre_submit.ok:
            final_reason = next((reason for reason in pre_submit.reason_codes if reason != "ok"), "PRE_SUBMIT_BLOCKED")
            self._log_final_skip(
                cf=cf,
                reason_code=final_reason,
                reason_detail=",".join(pre_submit.reason_codes),
                stage="PB1-CLOSE",
                price=record_price,
            )
            status["skipped"] = 1
            status["skipped_reason"] = final_reason
            status["submit_terminal_status"] = "SKIPPED_BY_POLICY"
            status["terminal_event"] = "FINAL_SKIP"
            return status
        plan_prepared = self._prepare_entry_exit_plan(cf, entry_price_for_plan=float(cf.features.get("entry_price") or limit_price or cf.features.get("close") or record_price or 0.0))
        if plan_prepared is None:
            status["skipped"] = 1
            status["skipped_reason"] = "ENTRY_EXIT_PLAN_MISSING_OR_INVALID"
            status["submit_terminal_status"] = "SKIPPED_BY_POLICY"
            status["terminal_event"] = "FINAL_SKIP"
            return status
        entry_exit_plan_dict, plan_entry_meta = plan_prepared
        entry_meta = self._build_entry_metadata(cf, entry_price_planned=record_price)
        entry_meta.update(plan_entry_meta)
        # ── [ENTRY][HORIZON] / [ENTRY][RISK_UNIT] 태깅 ───────────────────────
        _eh_horizon = entry_meta.get("trade_horizon") or "SWING_CARRY"
        _eh_book = {"DAY_PROTECT": "DAY_BOOK", "SWING_CARRY": "SWING_BOOK", "CORE_CARRY": "CORE_BOOK"}.get(_eh_horizon, "SWING_BOOK")
        _eh_exit_fam = entry_meta.get("exit_policy_family") or "SWING_STAGED_EXIT"
        _eh_entry_px = float(cf.features.get("entry_price") or record_price or 0.0)
        _eh_stop_px = float(cf.features.get("stop_price") or 0.0)
        _eh_r = max(_eh_entry_px - _eh_stop_px, _eh_entry_px * 0.02) if (_eh_stop_px > 0 and _eh_entry_px > 0) else 0.0
        _eh_tp1 = round(_eh_entry_px + _eh_r * 2.0, 2) if _eh_r > 0 else 0.0
        _eh_tp2 = round(_eh_entry_px + _eh_r * 3.0, 2) if _eh_r > 0 else 0.0
        logger.info(
            "[ENTRY][HORIZON] code=%s style=%s horizon=%s book=%s exit_policy=%s",
            display_code, identity.get("entry_style_selected"), _eh_horizon, _eh_book, _eh_exit_fam,
        )
        logger.info(
            "[ENTRY][RISK_UNIT] code=%s entry=%.2f stop=%.2f r=%.2f tp1=%.2f tp2=%.2f",
            display_code, _eh_entry_px, _eh_stop_px, _eh_r, _eh_tp1, _eh_tp2,
        )
        entry_meta.update({
            "position_book": _eh_book,
            "initial_stop_price": _eh_stop_px,
            "r_value": _eh_r,
            "planned_tp1_price": _eh_tp1,
            "planned_tp2_price": _eh_tp2,
            "tp1_done": False,
            "tp2_done": False,
            "max_r_since_entry": 0.0,
            "max_pnl_pct_since_entry": 0.0,
            "current_stop_price": _eh_stop_px,
        })
        # ─────────────────────────────────────────────────────────────────────
        request_payload = {"features": cf.features, "reasons": cf.reasons, "entry_meta": entry_meta, "entry_exit_plan": entry_exit_plan_dict}
        effective_client_order_key = cf.client_order_key or ""
        existing_order = self.orders_repo.get_order_by_client_order_key(self.env, effective_client_order_key) if hasattr(self.orders_repo, "get_order_by_client_order_key") and effective_client_order_key else None
        existing_status = str((existing_order or {}).get("status") or "").upper()
        if existing_order and self._is_retryable_entry_order_status(existing_status):
            effective_client_order_key = self._next_retry_client_order_key(effective_client_order_key)
            logger.info(
                "[ORDER][PRE_SUBMIT][RETRY_KEY] code=%s old_key=%s new_key=%s prior_status=%s",
                display_code,
                cf.client_order_key,
                effective_client_order_key,
                existing_status,
            )
        try:
            order_id, created = self.orders_repo.create_intent_idempotent(
                env=self.env,
                run_id=self.run_id,
                strategy=self.STRATEGY_NAME,
                sid=1,
                mode=cf.mode,
                code=cf.code,
                market=cf.market,
                side="BUY",
                ord_type=order_type,
                qty=cf.planned_qty,
                limit_price=limit_price,
                stage="PB1-CLOSE",
                client_order_key=effective_client_order_key,
                request_json=request_payload,
                status="CREATED",
                entry_meta_json=entry_meta,
            )
        except Exception as exc:
            policy = os.getenv("DB_FAIL_POLICY", "halt").lower()
            logger.exception("[PB1][ENTRY][DB_FAIL] code=%s policy=%s err=%s", display_code, policy, exc)
            if policy == "best_effort":
                self._spool_db_fail(
                    "entry_intent",
                    {
                        "env": self.env,
                        "run_id": self.run_id,
                        "strategy": self.STRATEGY_NAME,
                        "sid": 1,
                        "mode": cf.mode,
                        "code": cf.code,
                        "market": cf.market,
                        "side": "BUY",
                        "ord_type": order_type,
                        "qty": cf.planned_qty,
                        "limit_price": limit_price,
                        "stage": "PB1-CLOSE",
                        "client_order_key": effective_client_order_key,
                        "request_json": request_payload,
                    },
                )
                order_id = effective_client_order_key or "DB_FAIL"
                created = True
            else:
                raise
        if not created:
            self._log_final_skip(
                cf=cf,
                reason_code="DUPLICATE_ORDER_EXISTS",
                reason_detail=f"client_order_key={effective_client_order_key}",
                stage="PB1-CLOSE",
                price=record_price,
            )
            status["skipped_reason"] = "DUPLICATE_ORDER_EXISTS"
            status["skipped"] = 1
            status["submit_terminal_status"] = "SKIPPED_BY_POLICY"
            status["terminal_event"] = "FINAL_SKIP"
            return status
        cf.client_order_key = effective_client_order_key
        if not hasattr(self, "_intents_created"):
            self._intents_created = []
        self._intents_created.append(cf.code)
        logger.info(
            "[ENTRY][META][SAVE] code=%s entry_reason=%s decision_family=%s stop=%s pivot=%s score=%s",
            display_code,
            entry_meta.get("entry_reason"),
            entry_meta.get("entry_decision_family"),
            entry_meta.get("stop_price_at_entry"),
            entry_meta.get("pivot_price_at_entry"),
            entry_meta.get("score_final_at_entry"),
        )
        try:
            self._append_ledger_event(
                event_type="ORDER_INTENT",
                code=cf.code,
                market=cf.market,
                mode=cf.mode,
                side="BUY",
                qty=cf.planned_qty,
                price=record_price,
                client_order_key=effective_client_order_key,
                ok=True,
                reasons=["entry"] + (cf.reasons or []),
                stage="PB1-CLOSE",
                payload_json={"features": cf.features, "entry_meta": entry_meta, "entry_exit_plan": entry_exit_plan_dict, "trace_id": entry_meta.get("trace_id")},
            )
        except Exception:
            logger.exception("[PB1][LEDGER][INTENT_FAIL] code=%s", display_code)
            if not self.dry_run:
                raise
            status["skipped_reason"] = "intent_log_fail"
            return status
        if self.dry_run:
            logger.info("[PB1][ENTRY-DRY] code=%s qty=%s key=%s order_id=%s", display_code, cf.planned_qty, cf.client_order_key, order_id)
            logger.info(
                "[TRADE][ORDER][BUY] code=%s name=%s oid=%s qty=%s price=%.2f result=DRY_RUN",
                cf.code,
                stock_name,
                order_id,
                cf.planned_qty,
                float(limit_price or entry_price or 0.0),
            )
            status["submit_attempted"] = 1
            status["api_submitted"] = 0
            status["skipped"] = 1
            status["accepted"] = 0
            status["submit_terminal_status"] = "SKIPPED_BY_POLICY"
            status["terminal_event"] = "FINAL_SKIP"
            return status
        
        # ✅ 라이브 주문 직전 최종 확인
        if self.intended_live and self.dry_run:
            raise RuntimeError(
                f"FATAL: About to send live order but dry_run=True. "
                f"code={display_code} intended_live={self.intended_live} dry_run={self.dry_run}"
            )
        
        if not self.kis:
            logger.warning("[PB1][ENTRY][SKIP] KIS missing code=%s", display_code)
            self._log_final_skip(
                cf=cf,
                reason_code="KIS_MISSING",
                reason_detail="kis client unavailable",
                stage="PB1-CLOSE",
                price=record_price,
            )
            status["skipped"] = 1
            status["skipped_reason"] = "KIS_MISSING"
            status["submit_terminal_status"] = "SKIPPED_BY_POLICY"
            status["terminal_event"] = "FINAL_SKIP"
            return status
        if not self._pretrade_check(
            code=cf.code,
            market=cf.market,
            mode=cf.mode,
            side="BUY",
            qty=cf.planned_qty,
            price=record_price,
            client_order_key=effective_client_order_key,
            stage="PB1-CLOSE",
        ):
            self._log_final_skip(
                cf=cf,
                reason_code="PRETRADE_CHECK_FAILED",
                reason_detail="validate_tradeable returned false",
                stage="PB1-CLOSE",
                price=record_price,
            )
            status["skipped"] = 1
            status["skipped_reason"] = "PRETRADE_CHECK_FAILED"
            status["submit_terminal_status"] = "SKIPPED_BY_POLICY"
            status["terminal_event"] = "FINAL_SKIP"
            return status
        status["submit_attempted"] = 1
        logger.info(
            "[ORDER][API_REQUEST] code=%s name=%s qty=%s price=%s order_type=%s",
            cf.code,
            stock_name,
            cf.planned_qty,
            record_price,
            order_type,
        )
        emit_event(
            as_of=self._today,
            event="ORDER_SUBMIT",
            side="BUY",
            code=cf.code,
            qty=cf.planned_qty,
            price=float(limit_price or 0.0),
            order_type=order_type,
            client_order_key=effective_client_order_key,
        )
        resp = None
        kis_odno = None
        try:
            if order_type == "LIMIT":
                resp = self.kis.buy_stock_limit(cf.code, cf.planned_qty, float(limit_price))
            else:
                resp = self.kis.buy_stock_market(cf.code, cf.planned_qty)
            kis_odno = extract_order_no(resp)
            status["broker_submit_called"] = 1
            status["api_submitted"] = 1
        except Exception:
            logger.exception("[PB1][ENTRY][FAIL] code=%s", display_code)
            status["failed"] = 1
        self.orders_repo.mark_submitted(
            self.env,
            effective_client_order_key or "",
            kis_odno,
            resp if isinstance(resp, dict) else {"resp": resp},
            entry_meta_json=entry_meta,
        )
        status["submitted"] = int(status.get("api_submitted", 0) or 0)
        status["broker_order_no"] = kis_odno
        self._append_ledger_event(
            event_type="ORDER_SUBMIT_ATTEMPT",
            code=cf.code,
            market=cf.market,
            mode=cf.mode,
            side="BUY",
            qty=cf.planned_qty,
            price=record_price,
            client_order_key=effective_client_order_key,
            ok=bool(status.get("api_submitted")),
            reasons=["submit"],
            stage="PB1-CLOSE",
            payload_json={"entry_meta": entry_meta, "entry_exit_plan": entry_exit_plan_dict, "trace_id": entry_meta.get("trace_id"), "kis_odno": kis_odno},
        )
        ok = bool(is_order_accepted(resp, kis_env=self.env))
        rt_cd = resp.get("rt_cd") if isinstance(resp, dict) else None
        msg_cd = resp.get("msg_cd") if isinstance(resp, dict) else None
        msg1 = resp.get("msg1") if isinstance(resp, dict) else None
        status["broker_response_code"] = msg_cd or rt_cd
        status["broker_message"] = msg1
        logger.info(
            "[ORDER][API_RESULT] code=%s name=%s rt_cd=%s msg_cd=%s accepted=%s rejected=%s",
            cf.code,
            stock_name,
            rt_cd,
            msg_cd,
            int(bool(ok)),
            int(not bool(ok)),
        )
        emit_event(
            as_of=self._today,
            event="ORDER_RESULT",
            side="BUY",
            code=cf.code,
            ok=ok,
            rt_cd=rt_cd,
            msg_cd=msg_cd,
            msg1=msg1,
            kis_odno=kis_odno,
        )
        reason_code = self._format_order_result_reason(resp if isinstance(resp, dict) else None)
        self._append_ledger_event(
            event_type="ORDER_SUBMIT_ACCEPTED" if ok else "ORDER_SUBMIT_REJECTED",
            code=cf.code,
            market=cf.market,
            mode=cf.mode,
            side="BUY",
            qty=cf.planned_qty,
            price=record_price,
            client_order_key=effective_client_order_key,
            ok=ok,
            reasons=[reason_code],
            stage=build_stage_label(session_kind=os.getenv("PB1_SESSION_KIND"), phase="entry"),
            payload_json={"entry_meta": entry_meta, "entry_exit_plan": entry_exit_plan_dict, "trace_id": entry_meta.get("trace_id"), "rt_cd": rt_cd, "msg_cd": msg_cd, "msg1": msg1},
        )
        logger.info(
            "[PB1][ORDER][RESULT] side=BUY code=%s ok=%s reason=%s rt_cd=%s msg_cd=%s msg1=%s",
            display_code,
            int(ok),
            reason_code,
            rt_cd,
            msg_cd,
            msg1,
        )
        logger.info(
            "[TRADE][ORDER][BUY] code=%s name=%s oid=%s qty=%s price=%.2f result=%s",
            cf.code,
            stock_name,
            kis_odno or order_id,
            cf.planned_qty,
            float(limit_price or entry_price or 0.0),
            "ACCEPTED" if ok else "REJECTED",
        )
        if ok:
            # [2026-05-21] KIS 주문 성공 후 DB ACK 소프트 실패 처리
            # accepted=1은 먼저 설정 (KIS 주문은 이미 완료)
            status["accepted"] = 1
            status["submit_terminal_status"] = "ACCEPTED_PENDING_FILL"
            try:
                self.orders_repo.mark_acked(self.env, kis_odno, resp, entry_meta_json=entry_meta)
                logger.info("[ORDER][DB_ACK][OK] code=%s kis_odno=%s", cf.code, kis_odno)
            except Exception as _ack_exc:
                _soft_ack = os.getenv("PB1_ORDER_DB_ACK_FAIL_SOFT", "1") == "1"
                logger.warning(
                    "[ORDER][DB_ACK][TIMEOUT] code=%s kis_odno=%s err=%s action=ACK_PENDING_RECONCILE",
                    cf.code, kis_odno, _ack_exc,
                )
                status["db_ack_timeout"] = 1
                status["submit_terminal_status"] = "ACK_PENDING_RECONCILE"
                if not _soft_ack:
                    raise
            # [2026-04-30] ORDER_SUBMIT_ACCEPTED 직후 BUY_FILL/positions update 금지.
            # 실제 체결은 reconcile_kis.py에서 KIS balance 확인 후 BUY_FILL_CONFIRMED로 생성한다.
            logger.info(
                "[ORDER][ACCEPTED] side=BUY code=%s name=%s odno=%s fill_status=pending",
                cf.code,
                stock_name,
                kis_odno or order_id,
            )
            logger.info("[POSITION][META][SKIP_ACCEPTED_ONLY] code=%s", display_code)
            logger.info(
                "[TRADE][ORDER][BUY] code=%s name=%s oid=%s qty=%s result=ACCEPTED",
                cf.code,
                stock_name,
                kis_odno or order_id,
                cf.planned_qty,
            )
            status["filled"] = 0  # fill은 reconcile 후에만 1
            status["terminal_event"] = "API_RESULT"
        else:
            self.orders_repo.mark_error(self.env, effective_client_order_key or "", resp if isinstance(resp, dict) else {"resp": resp})
            status["rejected"] = 1
            status["failed"] = int(status.get("failed", 0) or 0) + 1
            status["submit_terminal_status"] = self._classify_submit_terminal_status(
                api_submitted=int(status.get("api_submitted", 0) or 0),
                accepted=int(status.get("accepted", 0) or 0),
                skipped_reason=str(status.get("skipped_reason") or ""),
                response=resp if isinstance(resp, dict) else None,
            )
            status["terminal_event"] = "API_RESULT"
        return status

    def _place_add_on(self, pos: dict, *, qty: int, price: float) -> None:
        # ✅ 최종 방어선: intended_live=True인데 dry_run=True면 Fatal
        if self.intended_live and self.dry_run:
            raise RuntimeError(
                f"FATAL: add_on order path reached with dry_run=True while intended_live=True. "
                f"intended_live={self.intended_live} dry_run={self.dry_run}"
            )
        
        code = pos.get("code")
        if not code or qty <= 0:
            return
        display_code = self._display_code(code)
        stock_name = str(self._name_for_code(code) or pos.get("name") or code)
        logger.info(
            "[TRADE][DECISION][BUY] code=%s name=%s reason=%s price=%.2f qty=%s",
            display_code,
            self._code_name_map.get(code),
            ReasonCode.ENTRY_PYRAMID,
            float(price or 0.0),
            qty,
        )
        mode = int(pos.get("mode") or 1)
        pyramid_level = int(pos.get("pyramid_level") or 0)
        client_key = self._client_order_key(code, mode, "BUY", f"add{pyramid_level + 1}", "PB1")
        limit_price = round_to_tick(price * 1.003) if price > 0 else price
        fill_price = float(limit_price or price or 0.0)
        try:
            order_id, created = self.orders_repo.create_intent_idempotent(
                env=self.env,
                run_id=self.run_id,
                strategy=self.STRATEGY_NAME,
                sid=1,
                mode=mode,
                code=code,
                market=pos.get("market"),
                side="BUY",
                ord_type="LIMIT",
                qty=qty,
                limit_price=limit_price,
                stage="PB1-ADD",
                client_order_key=client_key,
                request_json={"reasons": ["pyramid_add"], "price": price, "level": pyramid_level + 1},
                status="CREATED",
            )
        except Exception:
            logger.exception("[PB1][ADD][DB_FAIL] code=%s", display_code)
            if not self.dry_run:
                raise
            return
        if not created:
            logger.info("[PB1][ADD][SKIP] code=%s reason=duplicate_order", display_code)
            return
        if self.dry_run:
            logger.info("[PB1][ADD-DRY] code=%s qty=%s key=%s order_id=%s", display_code, qty, client_key, order_id)
            logger.info(
                "[TRADE][ORDER][BUY] code=%s name=%s oid=%s qty=%s price=%.2f result=DRY_RUN",
                code,
                stock_name,
                order_id,
                qty,
                float(limit_price or price or 0.0),
            )
            return
        
        # ✅ 라이브 주문 직전 최종 확인 (ADD_ON)
        if self.intended_live and self.dry_run:
            raise RuntimeError(
                f"FATAL: About to send live ADD_ON order but dry_run=True. "
                f"code={display_code} intended_live={self.intended_live} dry_run={self.dry_run}"
            )
        
        if not self.kis:
            logger.warning("[PB1][ADD][SKIP] KIS missing code=%s", display_code)
            return
        if not self._pretrade_check(
            code=code,
            market=pos.get("market"),
            mode=mode,
            side="BUY",
            qty=qty,
            price=fill_price,
            client_order_key=client_key,
            stage="PB1-ADD",
        ):
            return
        emit_event(
            as_of=self._today,
            event="ORDER_SUBMIT",
            side="BUY",
            code=code,
            qty=qty,
            price=float(limit_price),
            order_type="LIMIT",
            client_order_key=client_key,
        )
        resp = None
        kis_odno = None
        try:
            resp = self.kis.buy_stock_limit(code, qty, float(limit_price))
            kis_odno = (resp.get("output") or {}).get("ODNO") if isinstance(resp, dict) else None
        except Exception:
            logger.exception("[PB1][ADD][FAIL] code=%s", display_code)
        self.orders_repo.mark_submitted(self.env, client_key, kis_odno, resp if isinstance(resp, dict) else {"resp": resp})
        ok = bool(resp and isinstance(resp, dict) and resp.get("rt_cd") == "0")
        logger.info(
            "[TRADE][ORDER][BUY] code=%s name=%s oid=%s qty=%s price=%.2f result=%s",
            code,
            stock_name,
            kis_odno or order_id,
            qty,
            float(limit_price or price or 0.0),
            "ACCEPTED" if ok else "REJECTED",
        )
        if ok:
            try:
                self.orders_repo.mark_acked(self.env, kis_odno, resp)
                logger.info("[ORDER][DB_ACK][OK][ADD_BUY] code=%s kis_odno=%s", code, kis_odno)
            except Exception as _add_ack_exc:
                _soft_ack = os.getenv("PB1_ORDER_DB_ACK_FAIL_SOFT", "1") == "1"
                logger.warning(
                    "[ORDER][DB_ACK][TIMEOUT][ADD_BUY] code=%s kis_odno=%s err=%s",
                    code, kis_odno, _add_ack_exc,
                )
                if not _soft_ack:
                    raise
            filled_at = now_kst()
            self.fills_repo.upsert_fill(
                env=self.env,
                run_id=self.run_id,
                order_id=order_id,
                kis_odno=kis_odno,
                trade_id=None,
                code=code,
                market=pos.get("market"),
                side="BUY",
                qty=qty,
                price=fill_price,
                fee=0.0,
                tax=0.0,
                filled_at=filled_at,
            )
            logger.info(
                "[POSITIONS][UPSERT_AFTER_FILL] code=%s name=%s side=%s qty=%s price=%s source=order_fill",
                code,
                stock_name,
                "BUY",
                qty,
                fill_price,
            )
            updated_level = pyramid_level + 1
            entry_price = float(pos.get("avg_buy_price") or fill_price)
            stop_price = float(pos.get("stop_price") or pos.get("initial_stop") or 0.0)
            if entry_price > 0:
                stop_price = max(stop_price, entry_price * 0.995)
            self.positions_repo.update_position_fields(
                env=self.env,
                strategy=self.STRATEGY_NAME,
                sid=1,
                mode=mode,
                code=code,
                fields={
                    "pyramid_level": updated_level,
                    "last_add_price": fill_price,
                    "stop_price": stop_price if stop_price > 0 else None,
                    "last_stop_update_ts": filled_at.isoformat(),
                },
            )
            self._log_fill_reconcile(
                code=code,
                sid=1,
                mode=mode,
                submitted_price=float(limit_price or price or 0.0),
                filled_price=float(fill_price or 0.0),
            )
            logger.info(
                "[TRADE][FILL][BUY] code=%s name=%s oid=%s fill_qty=%s fill_px=%.2f",
                code,
                stock_name,
                kis_odno or order_id,
                qty,
                float(fill_price or 0.0),
            )
        else:
            self.orders_repo.mark_error(self.env, client_key, resp if isinstance(resp, dict) else {"resp": resp})

    def _append_close_entry_record(self, payload: dict) -> None:
        path = close_entry_orders_path(self._today)
        payload = to_jsonable(payload)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, ensure_ascii=False) + "\n")

    def _log_fill_reconcile(
        self,
        *,
        code: str,
        sid: int,
        mode: int,
        submitted_price: float,
        filled_price: float,
    ) -> None:
        pos_state = self.positions_repo.get_position(
            env=self.env,
            strategy=self.STRATEGY_NAME,
            sid=sid,
            mode=mode,
            code=code,
        ) or {}
        avg_price_from_balance = self._to_float(pos_state.get("avg_buy_price")) or 0.0
        logger.info(
            "[TRADE][FILL][RECONCILE] code=%s submitted_price=%.2f filled_price=%.2f avg_price_from_balance=%.2f",
            self._display_code(code),
            float(submitted_price or 0.0),
            float(filled_price or 0.0),
            float(avg_price_from_balance or 0.0),
        )
        if _fill_reconcile_warn_needed(float(filled_price or 0.0), float(avg_price_from_balance or 0.0)):
            logger.warning(
                "[FILL_RECONCILE_WARN] code=%s submitted=%.2f filled=%.2f avg_from_balance=%.2f",
                self._display_code(code),
                float(submitted_price or 0.0),
                float(filled_price or 0.0),
                float(avg_price_from_balance or 0.0),
            )

    def _place_entry_close(self, cf: CandidateFeature) -> dict[str, int | str]:
        status: dict[str, Any] = self._empty_order_status()
        # NO_TRADE 모드: 주문 전송 스킵, 로그만 출력
        no_trade = os.getenv("NO_TRADE", "0") == "1"
        
        display_code = self._display_code(cf.code)
        identity = self._resolve_entry_identity_for_candidate(cf)
        decision_reason = identity["entry_reason"]
        logger.info(
            "[TRADE][DECISION][BUY] code=%s name=%s family=%s decision_reason=%s score=%.1f entry=%.2f stop=%.2f qty=%s no_trade=%s",
            display_code,
            self._code_name_map.get(cf.code),
            identity["entry_style_selected"],
            decision_reason,
            float(cf.features.get("score") or 0.0),
            float(cf.features.get("entry_price") or cf.features.get("close") or 0.0),
            float(cf.features.get("stop_price") or 0.0),
            cf.planned_qty,
            no_trade,
        )
        
        # ✅ DIAG 모드: 주문 생성만 하고 전송은 스킵 (SIM_ORDER 로그 및 DB 기록)
        if no_trade:
            # DIAG 모드인지 확인
            strategy_mode = os.getenv("STRATEGY_MODE", "").upper()
            if strategy_mode == "DIAG":
                # SIM_ORDER 로그 남기기
                logger.info(
                    "[SIM_ORDER][BUY] code=%s qty=%s limit=%.0f reason=DIAG_NO_HTTP mode=%s",
                    display_code,
                    cf.planned_qty,
                    float(cf.features.get("entry_price") or cf.features.get("close") or 0.0),
                    strategy_mode,
                )
                # ✅ DB에 SIMULATED 주문 기록
                try:
                    self._record_simulated_order(
                        code=display_code,
                        side="BUY",
                        qty=cf.planned_qty,
                        limit_price=float(cf.features.get("entry_price") or cf.features.get("close") or 0.0),
                        reason="DIAG_NO_HTTP",
                        mode=strategy_mode,
                    )
                except Exception as exc:
                    logger.warning("[SIM_ORDER][DB_FAIL] code=%s err=%s", display_code, exc)
                # ✅ 계속 실행하지 않고 return (주문 전송 스킵)
                status["skipped_reason"] = "diag_no_http"
                status["skipped"] = 1
                status["terminal_event"] = "FINAL_SKIP"
                return status
            else:
                logger.info(
                    "[TRADE][SKIP][NO_TRADE] code=%s qty=%s reason=NO_TRADE_MODE",
                    display_code,
                    cf.planned_qty,
                )
                status["skipped_reason"] = "no_trade_mode"
                status["skipped"] = 1
                status["terminal_event"] = "FINAL_SKIP"
                return status
        
        cap_buffer_pct = self._float_env("PB1_CLOSE_ENTRY_CAP_BUFFER_PCT", 1.0)
        ref_daily_close = cf.features.get("close")
        snap = self.kis.get_quote_snapshot(cf.code) if self.kis else {}
        ap = snap.get("ap") if isinstance(snap, dict) else None
        tp = snap.get("tp") if isinstance(snap, dict) else None
        base_from = None
        base = None
        if ap:
            base_from = "ap"
            base = float(ap)
        elif tp:
            base_from = "tp"
            base = float(tp)
        reasons = ["close_entry"] + (cf.reasons or [])
        if base is None:
            logger.info(
                "[PB1][CLOSE_ENTRY][WHY] code=%s base_from=%s base=%s cap=%s cap_buffer_pct=%.2f ref_daily_close=%s reasons=%s",
                display_code,
                base_from or "none",
                base,
                None,
                cap_buffer_pct,
                ref_daily_close,
                reasons + ["missing_quote_base"],
            )
            logger.warning("[PB1][CLOSE_ENTRY][SKIP] code=%s reason=missing_quote_base", display_code)
            status["skipped_reason"] = "missing_quote_base"
            return status
        cap = round_to_tick(base * (1 + cap_buffer_pct / 100.0))
        logger.info(
            "[PB1][CLOSE_ENTRY][WHY] code=%s selected_family=%s trigger_policy=%s base_from=%s base=%.2f cap=%s cap_buffer_pct=%.2f ref_daily_close=%s reasons=%s",
            display_code,
            cf.features.get("entry_style_selected") or cf.features.get("entry_signal") or decision_reason,
            cf.features.get("entry_trigger_policy") or "NONE",
            base_from,
            base,
            cap,
            cap_buffer_pct,
            ref_daily_close,
            reasons,
        )
        pre_submit = self._resolve_entry_pre_submit(
            cf=cf,
            stage="PB1-CLOSE",
            order_price=float(cap or 0.0),
            order_type="LIMIT",
            allow_add_to_existing=False,
        )
        if not pre_submit.ok:
            final_reason = next((reason for reason in pre_submit.reason_codes if reason != "ok"), "PRE_SUBMIT_BLOCKED")
            self._log_final_skip(
                cf=cf,
                reason_code=final_reason,
                reason_detail=",".join(pre_submit.reason_codes),
                stage="PB1-CLOSE",
                price=float(cap or 0.0),
            )
            status["skipped"] = 1
            status["skipped_reason"] = final_reason
            status["submit_terminal_status"] = "SKIPPED_BY_POLICY"
            status["terminal_event"] = "FINAL_SKIP"
            return status
        plan_prepared = self._prepare_entry_exit_plan(cf, entry_price_for_plan=float(cf.features.get("entry_price") or cap or cf.features.get("close") or 0.0))
        if plan_prepared is None:
            status["skipped"] = 1
            status["skipped_reason"] = "ENTRY_EXIT_PLAN_MISSING_OR_INVALID"
            status["submit_terminal_status"] = "SKIPPED_BY_POLICY"
            status["terminal_event"] = "FINAL_SKIP"
            return status
        entry_exit_plan_dict, plan_entry_meta = plan_prepared
        entry_meta = self._build_entry_metadata(cf, entry_price_planned=float(cap or 0.0))
        entry_meta.update(plan_entry_meta)
        # ── [ENTRY][HORIZON] / [ENTRY][RISK_UNIT] 태깅 ───────────────────────
        _eh_horizon = entry_meta.get("trade_horizon") or "SWING_CARRY"
        _eh_book = {"DAY_PROTECT": "DAY_BOOK", "SWING_CARRY": "SWING_BOOK", "CORE_CARRY": "CORE_BOOK"}.get(_eh_horizon, "SWING_BOOK")
        _eh_exit_fam = entry_meta.get("exit_policy_family") or "SWING_STAGED_EXIT"
        _eh_entry_px = float(cf.features.get("entry_price") or cap or 0.0)
        _eh_stop_px = float(cf.features.get("stop_price") or 0.0)
        _eh_r = max(_eh_entry_px - _eh_stop_px, _eh_entry_px * 0.02) if (_eh_stop_px > 0 and _eh_entry_px > 0) else 0.0
        _eh_tp1 = round(_eh_entry_px + _eh_r * 2.0, 2) if _eh_r > 0 else 0.0
        _eh_tp2 = round(_eh_entry_px + _eh_r * 3.0, 2) if _eh_r > 0 else 0.0
        logger.info(
            "[ENTRY][HORIZON] code=%s style=%s horizon=%s book=%s exit_policy=%s",
            display_code, identity.get("entry_style_selected"), _eh_horizon, _eh_book, _eh_exit_fam,
        )
        logger.info(
            "[ENTRY][RISK_UNIT] code=%s entry=%.2f stop=%.2f r=%.2f tp1=%.2f tp2=%.2f",
            display_code, _eh_entry_px, _eh_stop_px, _eh_r, _eh_tp1, _eh_tp2,
        )
        entry_meta.update({
            "position_book": _eh_book,
            "initial_stop_price": _eh_stop_px,
            "r_value": _eh_r,
            "planned_tp1_price": _eh_tp1,
            "planned_tp2_price": _eh_tp2,
            "tp1_done": False,
            "tp2_done": False,
            "max_r_since_entry": 0.0,
            "max_pnl_pct_since_entry": 0.0,
            "current_stop_price": _eh_stop_px,
        })
        # ─────────────────────────────────────────────────────────────────────
        effective_client_order_key = cf.client_order_key or ""
        existing_order = self.orders_repo.get_order_by_client_order_key(self.env, effective_client_order_key) if hasattr(self.orders_repo, "get_order_by_client_order_key") and effective_client_order_key else None
        existing_status = str((existing_order or {}).get("status") or "").upper()
        if existing_order and self._is_retryable_entry_order_status(existing_status):
            effective_client_order_key = self._next_retry_client_order_key(effective_client_order_key)
            logger.info(
                "[ORDER][PRE_SUBMIT][RETRY_KEY] code=%s old_key=%s new_key=%s prior_status=%s",
                display_code,
                cf.client_order_key,
                effective_client_order_key,
                existing_status,
            )
        try:
            order_id, created = self.orders_repo.create_intent_idempotent(
                env=self.env,
                run_id=self.run_id,
                strategy=self.STRATEGY_NAME,
                sid=1,
                mode=cf.mode,
                code=cf.code,
                market=cf.market,
                side="BUY",
                ord_type="LIMIT",
                qty=cf.planned_qty,
                limit_price=cap,
                stage="PB1-CLOSE",
                client_order_key=effective_client_order_key,
                request_json={
                    "features": cf.features,
                    "reasons": reasons,
                    "base_from": base_from,
                    "base": base,
                    "cap_buffer_pct": cap_buffer_pct,
                    "entry_meta": entry_meta,
                    "entry_exit_plan": entry_exit_plan_dict,
                },
                status="CREATED",
                entry_meta_json=entry_meta,
            )
        except Exception:
            logger.exception("[PB1][CLOSE_ENTRY][DB_FAIL] code=%s", display_code)
            if not self.dry_run:
                raise
            status["skipped_reason"] = "db_fail"
            status["skipped"] = 1
            status["terminal_event"] = "FINAL_SKIP"
            return status
        if not created:
            self._log_final_skip(
                cf=cf,
                reason_code="DUPLICATE_ORDER_EXISTS",
                reason_detail=f"client_order_key={effective_client_order_key}",
                stage="PB1-CLOSE",
                price=float(cap or 0.0),
            )
            status["skipped_reason"] = "DUPLICATE_ORDER_EXISTS"
            status["skipped"] = 1
            status["terminal_event"] = "FINAL_SKIP"
            return status
        cf.client_order_key = effective_client_order_key
        logger.info(
            "[ENTRY][META][SAVE] code=%s entry_reason=%s decision_family=%s stop=%s pivot=%s score=%s",
            display_code,
            entry_meta.get("entry_reason"),
            entry_meta.get("entry_decision_family"),
            entry_meta.get("stop_price_at_entry"),
            entry_meta.get("pivot_price_at_entry"),
            entry_meta.get("score_final_at_entry"),
        )
        if self.dry_run:
            logger.info(
                "[PB1][CLOSE_ENTRY-DRY] code=%s qty=%s cap=%s key=%s order_id=%s",
                display_code,
                cf.planned_qty,
                cap,
                effective_client_order_key,
                order_id,
            )
            logger.info(
                "[TRADE][ORDER][BUY] code=%s name=%s oid=%s qty=%s price=%.2f result=DRY_RUN",
                cf.code,
                str(self._name_for_code(cf.code) or cf.features.get("name") or cf.code),
                order_id,
                cf.planned_qty,
                float(cap or 0.0),
            )
            status["skipped"] = 1
            status["submit_attempted"] = 1
            status["submit_terminal_status"] = "SKIPPED_BY_POLICY"
            status["terminal_event"] = "FINAL_SKIP"
            return status
        if not self.kis:
            logger.warning("[PB1][CLOSE_ENTRY][SKIP] KIS missing code=%s", display_code)
            self._log_final_skip(
                cf=cf,
                reason_code="KIS_MISSING",
                reason_detail="kis client unavailable",
                stage="PB1-CLOSE",
                price=float(cap or 0.0),
            )
            status["skipped"] = 1
            status["skipped_reason"] = "KIS_MISSING"
            status["submit_terminal_status"] = "SKIPPED_BY_POLICY"
            status["terminal_event"] = "FINAL_SKIP"
            return status
        if not self._pretrade_check(
            code=cf.code,
            market=cf.market,
            mode=cf.mode,
            side="BUY",
            qty=cf.planned_qty,
            price=float(cap or 0.0),
            client_order_key=effective_client_order_key,
            stage="PB1-CLOSE",
        ):
            self._log_final_skip(
                cf=cf,
                reason_code="PRETRADE_CHECK_FAILED",
                reason_detail="validate_tradeable returned false",
                stage="PB1-CLOSE",
                price=float(cap or 0.0),
            )
            status["skipped"] = 1
            status["skipped_reason"] = "PRETRADE_CHECK_FAILED"
            status["submit_terminal_status"] = "SKIPPED_BY_POLICY"
            status["terminal_event"] = "FINAL_SKIP"
            return status
        self._append_ledger_event(
            event_type="ORDER_INTENT",
            code=cf.code,
            market=cf.market,
            mode=cf.mode,
            side="BUY",
            qty=cf.planned_qty,
            price=float(cap or 0.0),
            client_order_key=effective_client_order_key,
            ok=True,
            reasons=reasons,
            stage="PB1-CLOSE",
            payload_json={"entry_meta": entry_meta, "trace_id": entry_meta.get("trace_id")},
        )
        emit_event(
            as_of=self._today,
            event="ORDER_SUBMIT",
            side="BUY",
            code=cf.code,
            qty=cf.planned_qty,
            price=float(cap),
            order_type="LIMIT",
            client_order_key=effective_client_order_key,
        )
        resp = None
        kis_odno = None
        try:
            status["submit_attempted"] = 1
            logger.info("[ORDER][API_REQUEST] code=%s name=%s qty=%s price=%s order_type=LIMIT", cf.code, str(self._name_for_code(cf.code) or cf.features.get("name") or cf.code), cf.planned_qty, float(cap or 0.0))
            resp = self.kis.buy_stock_limit(cf.code, cf.planned_qty, cap)
            kis_odno = extract_order_no(resp)
            status["broker_submit_called"] = 1
            status["api_submitted"] = 1
        except Exception:
            logger.exception("[PB1][CLOSE_ENTRY][FAIL] code=%s", display_code)
            status["failed"] = 1
        self.orders_repo.mark_submitted(
            self.env,
            effective_client_order_key or "",
            kis_odno,
            resp if isinstance(resp, dict) else {"resp": resp},
            entry_meta_json=entry_meta,
        )
        status["submitted"] = int(status.get("api_submitted", 0) or 0)
        status["broker_order_no"] = kis_odno
        self._append_ledger_event(
            event_type="ORDER_SUBMIT_ATTEMPT",
            code=cf.code,
            market=cf.market,
            mode=cf.mode,
            side="BUY",
            qty=cf.planned_qty,
            price=float(cap or 0.0),
            client_order_key=effective_client_order_key,
            ok=bool(status.get("api_submitted")),
            reasons=["submit"],
            stage="PB1-CLOSE",
            payload_json={"entry_meta": entry_meta, "entry_exit_plan": entry_exit_plan_dict, "trace_id": entry_meta.get("trace_id"), "kis_odno": kis_odno},
        )
        ok = bool(is_order_accepted(resp, kis_env=self.env))
        rt_cd = resp.get("rt_cd") if isinstance(resp, dict) else None
        msg_cd = resp.get("msg_cd") if isinstance(resp, dict) else None
        msg1 = resp.get("msg1") if isinstance(resp, dict) else None
        status["broker_response_code"] = msg_cd or rt_cd
        status["broker_message"] = msg1
        logger.info(
            "[ORDER][API_RESULT] code=%s name=%s rt_cd=%s msg_cd=%s accepted=%s rejected=%s",
            cf.code,
            str(self._name_for_code(cf.code) or cf.features.get("name") or cf.code),
            rt_cd,
            msg_cd,
            int(bool(ok)),
            int(not bool(ok)),
        )
        emit_event(
            as_of=self._today,
            event="ORDER_RESULT",
            side="BUY",
            code=cf.code,
            ok=ok,
            rt_cd=rt_cd,
            msg_cd=msg_cd,
            msg1=msg1,
            kis_odno=kis_odno,
        )
        reason_code = self._format_order_result_reason(resp if isinstance(resp, dict) else None)
        self._append_ledger_event(
            event_type="ORDER_SUBMIT_ACCEPTED" if ok else "ORDER_SUBMIT_REJECTED",
            code=cf.code,
            market=cf.market,
            mode=cf.mode,
            side="BUY",
            qty=cf.planned_qty,
            price=float(cap or 0.0),
            client_order_key=effective_client_order_key,
            ok=ok,
            reasons=[reason_code],
            stage="PB1-CLOSE",
            payload_json={"entry_meta": entry_meta, "entry_exit_plan": entry_exit_plan_dict, "trace_id": entry_meta.get("trace_id"), "rt_cd": rt_cd, "msg_cd": msg_cd, "msg1": msg1},
        )
        logger.info(
            "[PB1][ORDER][RESULT] side=BUY code=%s ok=%s reason=%s rt_cd=%s msg_cd=%s msg1=%s",
            display_code,
            int(ok),
            reason_code,
            rt_cd,
            msg_cd,
            msg1,
        )
        logger.info(
            "[TRADE][ORDER][BUY] code=%s name=%s oid=%s qty=%s price=%.2f result=%s",
            cf.code,
            str(self._name_for_code(cf.code) or cf.features.get("name") or cf.code),
            kis_odno or order_id,
            cf.planned_qty,
            float(cap or 0.0),
            "ACCEPTED" if ok else "REJECTED",
        )
        if ok:
            # [2026-05-21] KIS 주문 성공 후 DB ACK 소프트 실패 처리
            status["accepted"] = 1
            status["submit_terminal_status"] = "ACCEPTED_PENDING_FILL"
            try:
                self.orders_repo.mark_acked(self.env, kis_odno, resp, entry_meta_json=entry_meta)
                logger.info("[ORDER][DB_ACK][OK][CLOSE_ENTRY] code=%s kis_odno=%s", cf.code, kis_odno)
            except Exception as _ack_exc:
                _soft_ack = os.getenv("PB1_ORDER_DB_ACK_FAIL_SOFT", "1") == "1"
                logger.warning(
                    "[ORDER][DB_ACK][TIMEOUT][CLOSE_ENTRY] code=%s kis_odno=%s err=%s",
                    cf.code, kis_odno, _ack_exc,
                )
                status["db_ack_timeout"] = 1
                status["submit_terminal_status"] = "ACK_PENDING_RECONCILE"
                if not _soft_ack:
                    raise
            self._append_close_entry_record(
                {
                    "order_id": order_id,
                    "code": cf.code,
                    "qty": cf.planned_qty,
                    "cap_price": cap,
                    "client_order_key": effective_client_order_key,
                    "kis_odno": kis_odno,
                    "created_at": now_kst().isoformat(),
                }
            )
            status["terminal_event"] = "API_RESULT"
        else:
            self.orders_repo.mark_error(self.env, effective_client_order_key or "", resp if isinstance(resp, dict) else {"resp": resp})
            status["rejected"] = 1
            status["failed"] = int(status.get("failed", 0) or 0) + 1
            status["submit_terminal_status"] = self._classify_submit_terminal_status(
                api_submitted=int(status.get("api_submitted", 0) or 0),
                accepted=int(status.get("accepted", 0) or 0),
                skipped_reason=str(status.get("skipped_reason") or ""),
                response=resp if isinstance(resp, dict) else None,
            )
            status["terminal_event"] = "API_RESULT"
        return status

    def _plan_exit_event(self, pos: Dict, features: Dict[str, float], df: pd.DataFrame, window_tag: str) -> dict[str, Any] | None:
        avg = self._to_float(pos.get("avg_buy_price"))
        if not avg:
            return None
        code = str(pos.get("code") or "").zfill(6)
        display_code = self._display_code(code)
        market = pos.get("market")
        mode = int(pos.get("mode") or 1)
        sid = int(pos.get("sid") or 0)
        qty = int(pos.get("qty") or 0)
        if sid != 1 or qty <= 0:
            return None

        mark, _source = self._resolve_price_with_fallback(
            code,
            ohlcv_close=self._to_float(features.get("close") or pos.get("last_price") or avg),
        )
        if mark is None:
            mark = self._to_float(pos.get("last_price")) or avg
        ret_pct = ((mark - avg) / avg) * 100 if avg else 0.0
        client_key = self._client_order_key(code, mode, "SELL", window_tag, "exit")
        stop_price = pos.get("stop_price")
        if stop_price is None:
            stop_price = pos.get("initial_stop")
        stop_price = self._to_float(stop_price)

        # [2026-04-29] effective stop 계산: 기존 손절가가 너무 깊으면 7%/8% 캡으로 보정
        _eff_risk = _resolve_effective_exit_risk_for_pos({
            **pos,
            "avg_buy_price": avg,
        })
        _eff_stop = _eff_risk["effective_stop_price"]
        if _eff_stop and _eff_stop > 0:
            stop_price = _eff_stop

        max_price = self._to_float(pos.get("max_price")) or 0.0
        new_max = max(max_price, float(mark or 0.0))
        update_fields: dict[str, Any] = {"max_price": new_max}
        if stop_price is not None and pos.get("stop_price") is None:
            update_fields["stop_price"] = stop_price
        if update_fields:
            self.positions_repo.update_position_fields(
                env=self.env,
                strategy=self.STRATEGY_NAME,
                sid=sid,
                mode=mode,
                code=code,
                fields=update_fields,
            )

        entry_reason_value = pos.get("entry_reason") or (pos.get("entry_meta_json") or {}).get("entry_reason")
        entry_style_selected = pos.get("entry_style_selected") or (pos.get("entry_meta_json") or {}).get("entry_style_selected")
        entry_reason_normalized, resolved_exit_family = self._resolve_exit_family(entry_reason_value, entry_style_selected)
        exit_policy_family = str(
            pos.get("exit_policy_family")
            or (pos.get("entry_meta_json") or {}).get("exit_policy_family")
            or resolved_exit_family
        )
        logger.info(
            "[EXIT][POSITION_META] code=%s entry_reason=%s entry_style_selected=%s stop_price_at_entry=%s pivot_price_at_entry=%s meta_ok=%s",
            display_code,
            entry_reason_value,
            entry_style_selected,
            pos.get("stop_price_at_entry"),
            pos.get("pivot_price_at_entry"),
            int(bool(entry_reason_value or entry_style_selected)),
        )
        entry_ts_value = pos.get("last_fill_at") or pos.get("entry_date") or pos.get("entry_ts")
        entry_ts = None
        days_held = int(pos.get("holding_days") or 0)
        calendar_days_held = int(pos.get("calendar_days_held") or days_held)
        trading_days_held = int(pos.get("trading_days_held") or days_held)
        holding_bars = int(pos.get("holding_bars") or 0)
        if entry_ts_value:
            try:
                entry_ts = pd.Timestamp(entry_ts_value)
                pos_age = calc_position_age(entry_ts, self._now_kst.date(), df)
                trading_days_held = pos_age.days_held
                days_held = trading_days_held
                calendar_days_held = _calendar_days_held(entry_ts, self._now_kst.date())
                holding_bars = max(holding_bars, int(pos_age.holding_bars or 0))
            except Exception:
                entry_ts = None
        logger.info(
            "[EXIT][HOLDING_META] code=%s entry_date=%s calendar_days_held=%s trading_days_held=%s holding_bars=%s last_fill_at=%s",
            display_code,
            entry_ts.date().isoformat() if entry_ts is not None else pos.get("entry_date"),
            calendar_days_held,
            trading_days_held,
            holding_bars,
            pos.get("last_fill_at"),
        )

        close_px = self._to_float(features.get("close") or mark)
        ma20 = self._to_float(features.get("ma20"))
        ma50 = self._to_float(features.get("ma50"))
        if ma20 is None and not df.empty and "close" in df.columns and len(df) >= 20:
            ma20 = self._to_float(df["close"].tail(20).mean())
        if ma50 is None and not df.empty and "close" in df.columns and len(df) >= 50:
            ma50 = self._to_float(df["close"].tail(50).mean())
        atr_current = self._to_float(features.get("atr14") or features.get("atr"))
        last_volume = self._to_float(features.get("last_volume"))
        vol50 = self._to_float(features.get("vol50"))
        heavy_volume = bool(last_volume and vol50 and vol50 > 0 and last_volume >= vol50 * self.minervini_config.heavy_volume_mult)

        failed_breakout = False
        pivot = self._to_float(pos.get("pivot") or pos.get("pivot_price_at_entry"))
        logger.info(
            "[EXIT][POLICY] code=%s entry_reason=%s entry_style_selected=%s exit_policy_family=%s stop_price_at_entry=%s pivot_price_at_entry=%s",
            display_code,
            entry_reason_normalized,
            entry_style_selected,
            exit_policy_family,
            pos.get("stop_price_at_entry") or stop_price,
            pos.get("pivot_price_at_entry") or pivot,
        )
        if pivot and close_px is not None and entry_ts is not None:
            failed_breakout = (self._now_kst.date() - entry_ts.date()).days <= FAILED_BREAKOUT_EXIT_DAYS and close_px < pivot

        highest_since_entry = float(avg or 0.0)
        trail_stop_price = None
        trail_source = "entry_price"
        post_entry_rows = 0
        if not df.empty and "high" in df.columns:
            highest_since_entry, post_entry_rows = _compute_highest_since_entry(df, entry_ts, float(avg or 0.0))
            trail_source = "post_entry_bars" if post_entry_rows > 0 else "entry_price_fallback"
        holding_bars = max(int(holding_bars or 0), int(post_entry_rows or 0))
        logger.info(
            "[EXIT][HIGHEST] code=%s entry_ts=%s entry_price=%s post_entry_rows=%s highest_since_entry=%s",
            display_code,
            entry_ts.isoformat() if entry_ts is not None else None,
            avg,
            post_entry_rows,
            highest_since_entry,
        )
        if entry_ts is not None and calendar_days_held > 0 and post_entry_rows <= 0:
            logger.warning(
                "[EXIT][HIGHEST][WARN] code=%s entry_ts=%s calendar_days_held=%s trading_days_held=%s post_entry_rows=%s source=%s",
                display_code,
                entry_ts.isoformat(),
                calendar_days_held,
                trading_days_held,
                post_entry_rows,
                trail_source,
            )
        if highest_since_entry and atr_current and atr_current > 0:
            trail_stop_price = highest_since_entry - (atr_current * ATR_MULT)
        logger.info(
            "[EXIT][TRAIL_CTX] code=%s highest_since_entry=%s atr=%s trail_stop=%s source=%s",
            display_code,
            highest_since_entry,
            atr_current,
            trail_stop_price,
            trail_source,
        )
        logger.info(
            "[EXIT][MA_CTX] code=%s ma20=%s ma50=%s source=%s ma_mode=%s last=%s",
            display_code,
            ma20,
            ma50,
            features.get("_exit_ohlcv_source") or "unknown",
            "prev_close_ref",
            mark,
        )
        if (ma20 is None or ma50 is None) and len(df) < 50:
            logger.info(
                "[EXIT][MA_CTX][DEGRADED] code=%s reason=insufficient_history rows=%s",
                display_code,
                len(df),
            )

        stop_hit = bool(stop_price is not None and mark <= float(stop_price))
        time_stop_hit = bool(trading_days_held >= int(PB1_TIME_STOP_DAYS) and ret_pct < 2.0)
        logger.info(
            "[EXIT][TIME_STOP][BASIS] code=%s basis=trading_days calendar_days=%s trading_days=%s max_hold=%s hit=%s",
            display_code,
            calendar_days_held,
            trading_days_held,
            int(PB1_TIME_STOP_DAYS),
            int(time_stop_hit),
        )
        raw_ma20_break = bool(ma20 is not None and mark < ma20)
        raw_ma50_break = bool(ma50 is not None and mark < ma50)
        risk_off_signal = bool(failed_breakout or (heavy_volume and raw_ma50_break))
        exit_policy = _resolve_exit_policy(
            days_held=trading_days_held,
            holding_bars=holding_bars,
            stop_hit=stop_hit,
            trail_stop_price=trail_stop_price,
            mark=float(mark or 0.0),
            ma20=ma20,
            ma50=ma50,
            time_stop_hit=time_stop_hit,
            risk_off_signal=risk_off_signal,
        )
        trail_hit = bool(exit_policy["trail_hit"])
        ma20_break = bool(exit_policy["ma20_break"])
        ma50_break = bool(exit_policy["ma50_break"])
        risk_off_hit = bool(exit_policy["risk_off_hit"])
        take_profit_threshold = float(TAKE_PROFIT_R1)
        take_profit_hit = bool((ret_pct / 100.0) >= take_profit_threshold)
        tp2_threshold = float(TAKE_PROFIT_R2)
        tp1_hit = bool((ret_pct / 100.0) >= take_profit_threshold)
        tp2_hit = bool((ret_pct / 100.0) >= tp2_threshold)

        triggered: list[str] = []
        if stop_hit:
            triggered.append("EXIT_HARD_STOP")
        if trail_hit:
            triggered.append("EXIT_TRAIL")
        if risk_off_hit:
            triggered.append("EXIT_SOFT_RISK_OFF")
        if ma50_break:
            triggered.append("EXIT_MA50_BREAK")
        if ma20_break:
            triggered.append("EXIT_MA20_BREAK")
        if time_stop_hit:
            triggered.append("EXIT_TIME_BASED")
        final_reason = str(exit_policy["final_reason"])
        ordered_reasons = [final_reason] + [reason for reason in triggered if reason != final_reason] if final_reason != "NO_EXIT_SIGNAL" else []

        logger.info(
            "[EXIT][CHECK] code=%s qty=%s avg=%s last=%s pnl_pct=%.2f days_held=%s",
            display_code,
            qty,
            avg,
            mark,
            ret_pct,
            days_held,
        )
        logger.info(
            "[EXIT][RULES] code=%s stop_hit=%s trail_hit=%s ma20_break=%s ma50_break=%s time_stop_hit=%s risk_off_hit=%s",
            display_code,
            int(stop_hit),
            int(trail_hit),
            int(ma20_break),
            int(ma50_break),
            int(time_stop_hit),
            int(risk_off_hit),
        )
        trail_drawdown = None
        trail_threshold = None
        if highest_since_entry and highest_since_entry > 0 and mark is not None:
            trail_drawdown = float((highest_since_entry - float(mark)) / highest_since_entry)
        if highest_since_entry and highest_since_entry > 0 and trail_stop_price is not None:
            trail_threshold = float((highest_since_entry - float(trail_stop_price)) / highest_since_entry)
        conds = {
            "stop_loss_pct": {
                "hit": bool(stop_hit),
                "threshold": float(((float(stop_price) - avg) / avg) if (stop_price is not None and avg) else 0.0),
                "actual": float(ret_pct / 100.0),
            },
            "take_profit_pct": {
                "hit": bool(take_profit_hit),
                "threshold": take_profit_threshold,
                "actual": float(ret_pct / 100.0),
            },
            "trail_stop": {
                "hit": bool(trail_hit),
                "peak": float(((highest_since_entry - avg) / avg) if (highest_since_entry is not None and avg) else 0.0),
                "drawdown_from_peak": trail_drawdown,
                "threshold": trail_threshold,
                "eligible": bool(exit_policy["trail_eligible"]),
                "holding_bars": holding_bars,
            },
            "ma20_break": {
                "hit": bool(ma20_break),
                "close": close_px,
                "ma20": ma20,
                "raw_hit": bool(raw_ma20_break),
                "eligible": bool(exit_policy["soft_exit_eligible"]),
            },
            "ma50_break": {
                "hit": bool(ma50_break),
                "close": close_px,
                "ma50": ma50,
                "raw_hit": bool(raw_ma50_break),
                "eligible": bool(exit_policy["soft_exit_eligible"]),
            },
            "time_stop": {
                "hit": bool(time_stop_hit),
                "holding_days": trading_days_held,
                "calendar_days_held": calendar_days_held,
                "trading_days_held": trading_days_held,
                "holding_bars": holding_bars,
                "time_stop_basis": "trading_days",
                "threshold": int(PB1_TIME_STOP_DAYS),
            },
            "regime_exit": {
                "hit": bool(risk_off_hit),
                "heavy_volume": heavy_volume,
                "failed_breakout": failed_breakout,
                "eligible": bool(exit_policy["soft_exit_eligible"]),
                "same_day_blocked": bool(exit_policy["same_day_entry"] and risk_off_signal),
            },
        }
        logger.info("[EXIT][COND] code=%s conds=%s", display_code, conds)
        if exit_policy_family == "PULLBACK_EXIT":
            rule_checks = [
                ("failed_breakout", failed_breakout, {"days_held": trading_days_held, "pivot": pivot, "close": close_px}),
                ("time_stop", time_stop_hit, {"calendar_days_held": calendar_days_held, "trading_days_held": trading_days_held, "threshold": int(PB1_TIME_STOP_DAYS), "pnl_pct": ret_pct}),
                ("hard_stop", stop_hit, {"mark": mark, "stop_price": stop_price}),
            ]
        elif exit_policy_family == "MOMENTUM_EXIT":
            rule_checks = [
                ("trail_stop", trail_hit, {"mark": mark, "trail_stop_price": trail_stop_price, "holding_bars": holding_bars}),
                ("TP1", tp1_hit, {"actual_r": float(ret_pct / 100.0), "threshold_r": take_profit_threshold}),
                ("TP2", tp2_hit, {"actual_r": float(ret_pct / 100.0), "threshold_r": tp2_threshold}),
                ("hard_stop", stop_hit, {"mark": mark, "stop_price": stop_price}),
            ]
        else:
            rule_checks = [
                ("hard_stop", stop_hit, {"mark": mark, "stop_price": stop_price}),
                ("trail_stop", trail_hit, {"mark": mark, "trail_stop_price": trail_stop_price, "holding_bars": holding_bars}),
                ("failed_breakout", failed_breakout, {"days_held": trading_days_held, "pivot": pivot, "close": close_px}),
                ("time_stop", time_stop_hit, {"calendar_days_held": calendar_days_held, "trading_days_held": trading_days_held, "threshold": int(PB1_TIME_STOP_DAYS), "pnl_pct": ret_pct}),
            ]
        for rule_name, rule_hit, details in rule_checks:
            logger.info(
                "[EXIT][RULE_CHECK] code=%s family=%s rule=%s hit=%s details=%s",
                display_code,
                exit_policy_family,
                rule_name,
                int(bool(rule_hit)),
                details,
            )
        logger.info(
            "[EXIT][ROUTER][CONSISTENCY] code=%s legacy_time_stop_hit=%s router_time_stop_hit=%s",
            display_code,
            int(time_stop_hit),
            int(str(final_reason) == "EXIT_SWING_TIME_STOP"),
        )
        logger.info(
            "[EXIT][DECISION] code=%s name=%s qty=%s pnl_pct=%.2f days_held=%s same_day=%s holding_bars=%s hard_stop=%s trail_hit=%s trail_eligible=%s ma50_break=%s regime_exit=%s final_reason=%s",
            code,
            str(pos.get("name") or self._name_for_code(code) or code),
            qty,
            ret_pct,
            trading_days_held,
            int(exit_policy["same_day_entry"]),
            holding_bars,
            int(stop_hit),
            int(trail_hit),
            int(exit_policy["trail_eligible"]),
            int(ma50_break),
            int(risk_off_hit),
            final_reason,
        )

        # ------------------------------------------------------------------
        # [2026-05-21] Book/Horizon 기반 exit router + same-day guard
        # ------------------------------------------------------------------
        trade_horizon = _resolve_position_horizon(pos, now_kst_date=self._now_kst.date())
        position_book = _resolve_position_book(pos)
        horizon_exit_family = _horizon_to_exit_family(trade_horizon)
        horizon_result: dict[str, Any] | None = None
        _now_hhmm = int(self._now_kst.strftime("%H%M"))
        _max_pnl = float((pos.get("position_meta") or {}).get("max_pnl_pct_since_entry") or 0.0)
        _max_pnl = max(_max_pnl, ret_pct)
        regime_str = str(getattr(self, "_regime", None) or "")

        # holding_minutes 계산 (당일 보유 시간)
        _holding_minutes: float = 0.0
        _same_day: bool = bool(exit_policy.get("same_day_entry", False))
        if entry_ts is not None:
            try:
                _holding_minutes = float((self._now_kst - entry_ts).total_seconds() / 60.0)
            except Exception:
                _holding_minutes = 0.0
        if _same_day and _holding_minutes <= 0:
            _holding_minutes = float(holding_bars * 1.5)  # bar 수 기반 추정

        logger.info(
            "[EXIT][ROUTER] code=%s book=%s horizon=%s exit_policy_family=%s "
            "route=%s same_day=%s holding_minutes=%.1f pnl_pct=%.2f",
            display_code,
            position_book,
            trade_horizon,
            exit_policy_family,
            "SWING_EXIT_ROUTER" if position_book == "SWING_BOOK" else
            ("INTRADAY_EXIT_ROUTER" if position_book == "DAY_BOOK" else "SWING_SAFE_EXIT_ROUTER"),
            int(_same_day),
            _holding_minutes,
            ret_pct,
        )

        # same-day guard: SWING_BOOK / DAY_BOOK / unknown 분기
        _guard_result = _apply_swing_same_day_guard(
            code=display_code,
            book=position_book,
            same_day=_same_day,
            holding_minutes=_holding_minutes,
            pnl_pct=ret_pct,
            hard_stop_hit=stop_hit,
            emergency_stop_hit=bool(os.getenv("EMERGENCY_GLOBAL_SELL", "0") not in {"0", "false", "False"}),
            candidate_exit_reason=final_reason,
            holding_qty=qty,
            sell_pct=None,  # 전체 exit 여부 판단용; partial 판단은 아래에서
        )
        if not _guard_result["allowed"] and exit_policy.get("exit_ok", False):
            _blocked_reason = _guard_result["blocked_reason"]
            logger.info(
                "[EXIT][SAME_DAY_GUARD] code=%s book=%s same_day=%s holding_minutes=%.1f "
                "pnl_pct=%.2f hard_stop=%s action=BLOCK reason=%s",
                display_code, position_book, int(_same_day), _holding_minutes,
                ret_pct, int(stop_hit), _blocked_reason,
            )
            # guard가 block했으면 exit 취소
            exit_policy = {**exit_policy, "exit_ok": False, "final_reason": _blocked_reason}
            final_reason = _blocked_reason or "SWING_SAME_DAY_GUARD_BLOCK"
            ordered_reasons = [final_reason]

        _router_reason = final_reason
        if exit_policy_family in {
            "INTRADAY_PROFIT_PROTECT",
            "SWING_STAGED_EXIT",
            "CORE_TREND_FOLLOW",
        } or horizon_exit_family in {
            "INTRADAY_PROFIT_PROTECT",
            "SWING_STAGED_EXIT",
            "CORE_TREND_FOLLOW",
        }:
            _active_family = exit_policy_family if exit_policy_family in {
                "INTRADAY_PROFIT_PROTECT", "SWING_STAGED_EXIT", "CORE_TREND_FOLLOW"
            } else horizon_exit_family

            if _active_family == "INTRADAY_PROFIT_PROTECT":
                # SWING_BOOK인데 INTRADAY_PROFIT_PROTECT family가 배정되면 guard가 block했어도 여기서 재확인
                if position_book in {"SWING_BOOK", "CORE_BOOK"} and _same_day and not stop_hit:
                    logger.info(
                        "[EXIT][ROUTER] code=%s book=%s INTRADAY_PROTECT → SWING_STAGED_EXIT (same_day override)",
                        display_code, position_book,
                    )
                    _active_family = "SWING_STAGED_EXIT"
                    horizon_result = _resolve_swing_staged_exit(
                        pos, float(mark or 0.0), ma20,
                        ret_pct=ret_pct,
                        days_held=trading_days_held,
                        stop_hit=stop_hit,
                        trail_hit=trail_hit,
                        trail_stop_price=trail_stop_price,
                        highest_ret_pct=_max_pnl,
                    )
                else:
                    horizon_result = _resolve_day_protect_exit(
                        pos, float(mark or 0.0), _now_hhmm,
                        ret_pct=ret_pct,
                        max_pnl_pct=_max_pnl,
                        stop_hit=stop_hit,
                    )
            elif _active_family == "SWING_STAGED_EXIT":
                horizon_result = _resolve_swing_staged_exit(
                    pos, float(mark or 0.0), ma20,
                    ret_pct=ret_pct,
                    days_held=trading_days_held,
                    stop_hit=stop_hit,
                    trail_hit=trail_hit,
                    trail_stop_price=trail_stop_price,
                    highest_ret_pct=_max_pnl,
                )
            elif _active_family == "CORE_TREND_FOLLOW":
                horizon_result = _resolve_core_trend_follow_exit(
                    pos, float(mark or 0.0), ma20, ma50,
                    ret_pct=ret_pct,
                    days_held=trading_days_held,
                    regime=regime_str,
                )

            if horizon_result is not None:
                _router_qty = int(horizon_result.get("qty", 0))
                _router_full_exit = bool(horizon_result.get("full_exit", False))
                _router_sell_pct = horizon_result.get("sell_pct")
                _router_reason = str(horizon_result.get("reason") or final_reason)
                logger.info(
                    "[EXIT][HORIZON] code=%s horizon=%s family=%s exit_ok=%s reason=%s qty=%s full_exit=%s sell_pct=%s holding_qty=%s",
                    display_code,
                    trade_horizon,
                    _active_family,
                    int(bool(horizon_result.get("exit_ok", False))),
                    horizon_result.get("reason", ""),
                    _router_qty,
                    int(_router_full_exit),
                    _router_sell_pct,
                    qty,
                )
                # position_meta 업데이트 (tp1_done 등)
                _meta_update = horizon_result.get("update_meta") or {}
                _meta_update["max_pnl_pct_since_entry"] = _max_pnl
                self.positions_repo.update_position_fields(
                    env=self.env,
                    strategy=self.STRATEGY_NAME,
                    sid=sid,
                    mode=mode,
                    code=code,
                    fields={"position_meta": {
                        **(pos.get("position_meta") or {}),
                        **_meta_update,
                    }},
                )
                # 기존 exit_policy/final_reason 오버라이드
                if horizon_result.get("exit_ok", False):
                    stop_hit = stop_hit or (horizon_result.get("reason", "") == "EXIT_DAY_STOP_LOSS")
                    final_reason = str(horizon_result.get("reason") or final_reason)
                    ordered_reasons = [final_reason]
                    exit_policy = {**exit_policy, "exit_ok": True, "final_reason": final_reason}
            else:
                # No router result → will use legacy full qty
                _router_qty = None
                _router_full_exit = False
                _router_sell_pct = None

        signal_hit = bool(stop_hit or trail_hit or ma20_break or ma50_break or time_stop_hit or risk_off_hit)
        eval_reason = final_reason if signal_hit else "NO_EXIT_SIGNAL"

        entry_exit_plan = pos.get("entry_exit_plan_json")
        if isinstance(entry_exit_plan, str):
            try:
                entry_exit_plan = json.loads(entry_exit_plan)
            except Exception:
                entry_exit_plan = {}
        if not isinstance(entry_exit_plan, dict) or not entry_exit_plan:
            meta = pos.get("position_meta") or {}
            if isinstance(meta, str):
                try:
                    meta = json.loads(meta)
                except Exception:
                    meta = {}
            entry_exit_plan = (meta or {}).get("entry_exit_plan") or {}
        close_action, close_reason = classify_close_action_from_plan(entry_exit_plan if isinstance(entry_exit_plan, dict) else {})
        logger.info(
            "[PB1][CLOSE_PLAN] code=%s qty=%s close_action=%s reason=%s thesis=%s horizon=%s exit_family=%s eod_action=%s force_eod=%s",
            code, qty, close_action, close_reason,
            (entry_exit_plan or {}).get("entry_thesis") or pos.get("entry_thesis"),
            (entry_exit_plan or {}).get("trade_horizon") or pos.get("trade_horizon"),
            (entry_exit_plan or {}).get("exit_policy_family") or pos.get("exit_policy_family"),
            (entry_exit_plan or {}).get("eod_action") or pos.get("eod_action"),
            int(parse_plan_bool((entry_exit_plan or {}).get("force_eod_close"), default=parse_plan_bool(pos.get("force_eod_close"), default=False))),
        )
        if str(window_tag).lower() == "close":
            if close_action == "FORCE_SELL":
                final_reason = close_reason
                ordered_reasons = [final_reason]
                signal_hit = True
                eval_reason = final_reason
                exit_policy = {**exit_policy, "exit_ok": True, "final_reason": final_reason}
                _router_qty = int(pos.get("orderable_qty") or qty)
                _router_full_exit = True
                _router_sell_pct = None
            elif close_action == "CARRY" and not signal_hit:
                logger.info("[PB1][CLOSE_PLAN][CARRY] code=%s reason=%s no_exit_signal=1", code, close_reason)
                exit_policy = {**exit_policy, "exit_ok": False, "final_reason": close_reason}
                final_reason = close_reason
                ordered_reasons = [close_reason]
                eval_reason = "NO_EXIT_SIGNAL"
            elif close_action == "SKIP":
                logger.info("[PB1][CLOSE_PLAN][SKIP] code=%s reason=%s action=no_force_sell", code, close_reason)
                exit_policy = {**exit_policy, "exit_ok": False, "final_reason": close_reason}
                final_reason = close_reason
                ordered_reasons = [close_reason]
                eval_reason = "NO_EXIT_SIGNAL"

        exit_eval = ExitEvaluation(
            code=code,
            holding_qty=qty,
            avg_price=float(avg or 0.0),
            last_price=float(mark or 0.0),
            pnl_pct=float(ret_pct),
            days_held=trading_days_held,
            stop_hit=stop_hit,
            trail_hit=trail_hit,
            ma20_break=ma20_break,
            ma50_break=ma50_break,
            time_stop_hit=time_stop_hit,
            risk_off_hit=risk_off_hit,
            exit_ok=bool(exit_policy["exit_ok"]),
            family=str(exit_policy["family"]),
            primary_reason=final_reason,
            secondary_reasons=ordered_reasons[1:],
        )

        exit_eval_payload = build_exit_evaluation(
            code=code,
            as_of=self.get_as_of(),
            trade_date=self._trade_date,
            holding_qty=qty,
            avg_price=float(avg or 0.0),
            last_price=float(mark or 0.0),
            entry_date=entry_ts.isoformat() if entry_ts is not None else pos.get("entry_date"),
            days_held=trading_days_held,
            stop_loss_hit=exit_eval.stop_hit,
            trailing_stop_hit=exit_eval.trail_hit,
            ma20_break=exit_eval.ma20_break,
            ma50_break=exit_eval.ma50_break,
            time_stop_hit=exit_eval.time_stop_hit,
            risk_off_hit=exit_eval.risk_off_hit,
            exit_ok=exit_eval.exit_ok,
            reasons=[eval_reason] + list(exit_eval.secondary_reasons),
            decision_reason=eval_reason,
            secondary_reasons=exit_eval.secondary_reasons,
        )
        exit_eval_payload.update(
            {
                "signal_hit": signal_hit,
                "eval_reason": eval_reason,
                "router_reason": _router_reason,
                "entry_reason": entry_reason_normalized,
                "entry_style_selected": entry_style_selected,
                "exit_family": exit_eval.family,
                "exit_policy_family": exit_policy_family,
                "entry_exit_plan": entry_exit_plan if isinstance(entry_exit_plan, dict) else {},
                "close_action": close_action,
                "close_reason": close_reason,
                "entry_thesis": (entry_exit_plan or {}).get("entry_thesis") or pos.get("entry_thesis"),
                "trade_horizon": (entry_exit_plan or {}).get("trade_horizon") or pos.get("trade_horizon"),
                "eod_action": (entry_exit_plan or {}).get("eod_action") or pos.get("eod_action"),
                "force_eod_close": parse_plan_bool((entry_exit_plan or {}).get("force_eod_close"), default=parse_plan_bool(pos.get("force_eod_close"), default=False)),
                "entry_exit_plan_status": "OK" if isinstance(entry_exit_plan, dict) and entry_exit_plan else "POLICY_MISSING",
                "entry_price": float(pos.get("entry_price") or avg),
                "current_price": mark,
                "stop_price_at_entry": pos.get("stop_price_at_entry") or stop_price,
                "pivot_price_at_entry": pos.get("pivot_price_at_entry") or pivot,
                "holding_days": trading_days_held,
                "calendar_days_held": calendar_days_held,
                "trading_days_held": trading_days_held,
                "holding_bars": holding_bars,
                "time_stop_basis": "trading_days",
                "legacy_time_stop_hit": bool(time_stop_hit),
                "router_time_stop_hit": bool(str(exit_eval.primary_reason) == "EXIT_SWING_TIME_STOP"),
                "pnl_pct": ret_pct,
                "orderable_qty": int(pos.get("orderable_qty") or qty),
                "router_qty": int(_router_qty) if _router_qty is not None else None,
                "router_full_exit": bool(_router_full_exit),
                "router_sell_pct": _router_sell_pct,
                "trigger_metrics": {
                    "close": close_px,
                    "ma20": ma20,
                    "ma50": ma50,
                    "heavy_volume": heavy_volume,
                    "failed_breakout": failed_breakout,
                    "highest_since_entry": highest_since_entry,
                    "post_entry_rows": post_entry_rows,
                    "atr_current": atr_current,
                    "trail_stop_price": trail_stop_price,
                },
                "exit_rule_version": "pb1_exit_reason_v2",
                "submit_attempted": 0,
                "submitted": 0,
                "order_result": "ORDER_SKIPPED_NO_SIGNAL" if not signal_hit else "ORDER_SIGNAL_ONLY",
                "order_skip_reasons": [],
                "holdings_source": str((self._exit_holdings_meta or {}).get("source") or "unknown"),
                "condition_details": conds,
            }
        )
        # [2026-04-30] last_exit_eval_json은 non-critical: 남은 tick budget이 부족하면 skip
        _noncritical_timeout = int(os.getenv("PB1_NONCRITICAL_DB_UPDATE_TIMEOUT_SEC", "2"))
        _skip_threshold = int(os.getenv("PB1_SKIP_NONCRITICAL_DB_UPDATE_WHEN_REMAINING_SEC_LT", "5"))
        _tick_remaining = getattr(self, "_tick_remaining_sec", None)
        _should_skip_noncritical = (
            _tick_remaining is not None and float(_tick_remaining) < _skip_threshold
        )
        if _should_skip_noncritical:
            logger.info(
                "[POSITIONS][UPDATE][SKIP_NONCRITICAL] code=%s field=last_exit_eval_json reason=low_tick_budget remaining_sec=%.1f",
                code, float(_tick_remaining or 0),
            )
        else:
            try:
                import signal as _signal
                def _timeout_handler(signum, frame):
                    raise TimeoutError("noncritical_db_update_timeout")
                _signal.signal(_signal.SIGALRM, _timeout_handler)
                _signal.alarm(_noncritical_timeout)
                try:
                    self.positions_repo.update_position_fields(
                        env=self.env,
                        strategy=self.STRATEGY_NAME,
                        sid=sid,
                        mode=mode,
                        code=code,
                        fields={"exit_policy_family": exit_policy_family, "last_exit_eval_json": exit_eval_payload},
                    )
                finally:
                    _signal.alarm(0)
            except Exception as _upd_exc:
                logger.warning(
                    "[POSITIONS][UPDATE][FAIL_SOFT] code=%s field=last_exit_eval_json err=%s",
                    code, repr(_upd_exc),
                )

        logger.info(
            "[EXIT][EVAL] code=%s qty=%s avg=%s last=%s pnl_pct=%.2f stop_loss_hit=%s take_profit_hit=%s trailing_stop_hit=%s close_below_ma20=%s close_below_ma50=%s time_stop_hit=%s signal_hit=%s orderable=%s reason=%s",
            display_code,
            qty,
            avg,
            mark,
            ret_pct,
            int(stop_hit),
            int(take_profit_hit),
            int(trail_hit),
            int(ma20_break),
            int(ma50_break),
            int(time_stop_hit),
            int(exit_eval.exit_ok),
            int(int(pos.get("orderable_qty") or qty) > 0),
            exit_eval.primary_reason,
        )

        emit_event(
            as_of=self._today,
            event="PB1_SELL_DECISION",
            code=str(code),
            qty=int(qty),
            avg_price=float(avg),
            mark=float(mark),
            pnl_pct=float(ret_pct),
            should_sell=bool(exit_eval.exit_ok),
            reasons=[exit_eval.primary_reason] + list(exit_eval.secondary_reasons),
        )
        if not exit_eval.exit_ok:
            forced_simulation = self._resolve_force_exit_simulation(
                code=code,
                orderable_qty=int(pos.get("orderable_qty") or qty),
                exit_policy_family=exit_policy_family,
            )
            if forced_simulation is None:
                logger.info("[EXIT][SKIP] code=%s reason=%s", display_code, exit_eval.primary_reason)
                if str(close_action) == "SKIP":
                    self._append_ledger_event(
                        event_type="EXIT_SKIP", code=code, market=market, mode=mode, side="SELL", qty=qty,
                        price=float(mark or 0.0), client_order_key=client_key, ok=False,
                        reasons=[close_reason], stage="CLOSE_PLAN",
                        payload_json={"entry_exit_plan": entry_exit_plan if isinstance(entry_exit_plan, dict) else {}, "close_action": close_action, "close_reason": close_reason},
                    )
                return exit_eval_payload
        else:
            forced_simulation = self._resolve_force_exit_simulation(
                code=code,
                orderable_qty=int(pos.get("orderable_qty") or qty),
                exit_policy_family=exit_policy_family,
            )

        signal_reason_labels = {
            "EXIT_HARD_STOP": "hard_stop",
            "EXIT_TRAIL": "trail_stop",
            "EXIT_MA20_BREAK": "ma20_break",
            "EXIT_MA50_BREAK": "ma50_break",
            "EXIT_TIME_BASED": "time_stop",
            "EXIT_SOFT_RISK_OFF": "regime_exit",
        }
        logger.info(
            "[EXIT][SIGNAL_HIT] code=%s reasons=%s",
            display_code,
            [signal_reason_labels.get(reason, reason.lower()) for reason in ([exit_eval.primary_reason] + list(exit_eval.secondary_reasons))],
        )

        # [2026-05-07] Router qty 우선 사용: partial exit (TP1, TP2, giveback) 수량 존중
        if _router_qty is not None and _router_qty > 0 and not _router_full_exit:
            orderable_qty = int(_router_qty)
            logger.info(
                "[EXIT][ROUTER][QTY] code=%s source=router_qty router_qty=%s full_exit=0 holding_qty=%s reason=%s",
                display_code, orderable_qty, qty, exit_eval.primary_reason,
            )
        elif _router_full_exit or _router_qty is None:
            orderable_qty = int(pos.get("orderable_qty") or qty)
            if _router_full_exit:
                logger.info(
                    "[EXIT][ROUTER][QTY] code=%s source=router_full_exit router_qty=%s full_exit=1 holding_qty=%s reason=%s",
                    display_code, orderable_qty, int(_router_qty or 0), qty, exit_eval.primary_reason,
                )
            else:
                logger.info(
                    "[EXIT][LEGACY][QTY] code=%s source=legacy_full_qty orderable_qty=%s holding_qty=%s reason=%s",
                    display_code, orderable_qty, qty, exit_eval.primary_reason,
                )
        else:
            # _router_qty <= 0: skip order
            orderable_qty = 0
            logger.warning(
                "[EXIT][ROUTER][QTY][ZERO] code=%s router_qty=%s holding_qty=%s reason=%s → skip order",
                display_code, int(_router_qty or 0), qty, exit_eval.primary_reason,
            )

        if orderable_qty <= 0:
            exit_eval_payload["order_skip_reasons"] = ["orderable_qty_zero"]
            exit_eval_payload["order_result"] = "ORDER_SKIPPED_ROUTER_QTY_ZERO"
            logger.info("[EXIT][ORDER_SKIP] code=%s reasons=%s", display_code, exit_eval_payload["order_skip_reasons"])
            return exit_eval_payload

        holding_qty = max(0, int(qty or 0))
        orderable_balance_qty = max(0, int(pos.get("orderable_qty") or 0))
        strategy_qty = max(0, int(orderable_qty or 0))
        sell_qty = min(strategy_qty, holding_qty, orderable_balance_qty)
        logger.info(
            "[SELLABLE][CHECK] code=%s holding_qty=%s orderable_qty=%s strategy_qty=%s sell_qty=%s",
            display_code,
            holding_qty,
            orderable_balance_qty,
            strategy_qty,
            sell_qty,
        )
        if sell_qty <= 0:
            exit_eval_payload["order_skip_reasons"] = ["NO_SELLABLE_QTY"]
            exit_eval_payload["order_result"] = "ORDER_SKIPPED_NO_SELLABLE_QTY"
            logger.info("[EXIT][ORDER_SKIP] code=%s reason=NO_SELLABLE_QTY", display_code)
            return exit_eval_payload
        orderable_qty = sell_qty

        stage = exit_eval.primary_reason
        simulated_client_key = None
        if forced_simulation is not None:
            stage = str(forced_simulation["stage"])
            orderable_qty = int(forced_simulation["qty"])
            simulated_client_key = self._client_order_key(code, mode, "SELL", window_tag, stage)
            simulated_payload = {
                "code": code,
                "market": market,
                "side": "SELL",
                "ord_type": "MARKET",
                "qty": orderable_qty,
                "limit_price": mark,
                "stage": stage,
                "client_order_key": simulated_client_key,
                "reason": forced_simulation["reason"],
                "exit_policy_family": forced_simulation["exit_policy_family"],
            }
            exit_eval_payload.update(
                {
                    "exit_ok": True,
                    "primary_reason": forced_simulation["reason"],
                    "reasons": [forced_simulation["reason"]],
                    "decision_reason": forced_simulation["reason"],
                    "secondary_reasons": [],
                    "orderable_qty": orderable_qty,
                    "stage": stage,
                    "client_order_key": simulated_client_key,
                    "simulated_order_payload": simulated_payload,
                    "force_exit_simulation": True,
                    "order_skip_reasons": ["force_exit_simulation"],
                }
            )
            logger.info(
                "[EXIT][SIMULATION] code=%s requested_reason=%s qty=%s stage=%s client_order_key=%s order_allowed=%s payload=%s",
                display_code,
                forced_simulation["reason"],
                orderable_qty,
                stage,
                simulated_client_key,
                int(bool(self.order_allowed)),
                simulated_payload,
            )
            return exit_eval_payload
        stock_name = str(self._name_for_code(code) or pos.get("name") or code)
        logger.info(
            "[EXIT][ORDER_READY] code=%s name=%s qty=%s family=%s reason=%s",
            code,
            stock_name,
            orderable_qty,
            exit_policy_family,
            stage,
        )

        submit_block_reasons = self._resolve_exit_submit_gate_reasons(code=code)
        if submit_block_reasons:
            exit_eval_payload["submit_attempted"] = 0
            exit_eval_payload["submitted"] = 0
            exit_eval_payload["api_called"] = 0
            exit_eval_payload["order_skip_reasons"] = submit_block_reasons
            logger.info("[EXIT][ORDER_SKIP] code=%s reasons=%s", display_code, submit_block_reasons)
            return exit_eval_payload

        cooldown_until: str | None = None
        if exit_eval.primary_reason in {"EXIT_HARD_STOP", "EXIT_SOFT_RISK_OFF"}:
            if REENTRY_COOLDOWN_DAYS <= 0:
                cooldown_until = self._now_kst.date().isoformat()
            else:
                cooldown_until = (self._now_kst + pd.Timedelta(days=REENTRY_COOLDOWN_DAYS)).date().isoformat()

        if self._should_block_order(client_key, code=code, side="SELL", stage="PB1-EXIT")[0]:
            exit_eval_payload["submit_attempted"] = 1
            exit_eval_payload["order_skip_reasons"] = ["duplicate_order"]
            logger.info("[EXIT][ORDER_SKIP] code=%s reasons=%s", display_code, exit_eval_payload["order_skip_reasons"])
            return exit_eval_payload

        exit_meta = {
            "exit_reason": exit_eval.primary_reason,
            "exit_stage": stage,
            "close_action": exit_eval_payload.get("close_action"),
            "close_reason": exit_eval_payload.get("close_reason"),
            "entry_exit_plan": exit_eval_payload.get("entry_exit_plan") or {},
            "position_trade_horizon": pos.get("trade_horizon") or exit_eval_payload.get("trade_horizon"),
            "position_exit_policy_family": pos.get("exit_policy_family") or exit_policy_family,
            "position_eod_action": pos.get("eod_action") or exit_eval_payload.get("eod_action"),
            "policy_version": pos.get("policy_version") or (exit_eval_payload.get("entry_exit_plan") or {}).get("policy_version"),
        }
        try:
            order_id, _created = self.orders_repo.create_intent_idempotent(
                env=self.env,
                run_id=self.run_id,
                strategy=self.STRATEGY_NAME,
                sid=1,
                mode=mode,
                code=code,
                market=market,
                side="SELL",
                ord_type="MARKET",
                qty=orderable_qty,
                limit_price=mark,
                stage=stage,
                client_order_key=client_key,
                request_json={"reasons": [exit_eval.primary_reason] + list(exit_eval.secondary_reasons), "ret_pct": ret_pct, "exit_meta": exit_meta, "entry_exit_plan": exit_meta.get("entry_exit_plan") or {}},
                status="CREATED",
            )
        except Exception:
            logger.exception("[PB1][EXIT][DB_FAIL] code=%s", display_code)
            if not self.dry_run:
                raise
            return exit_eval_payload

        logger.info(
            "[EXIT][SUBMIT] code=%s name=%s qty=%s order_type=market family=%s reason=%s",
            code,
            stock_name,
            orderable_qty,
            exit_policy_family,
            stage,
        )
        logger.info(
            "[PB1][EXIT][WHY] code=%s qty=%s mark=%s avg=%s ret_pct=%.2f exit_reason_codes=%s exit_reason_text=%s holding_days=%s stage=%s",
            display_code,
            orderable_qty,
            mark,
            avg,
            ret_pct,
            [exit_eval.primary_reason] + list(exit_eval.secondary_reasons),
            ", ".join([exit_eval.primary_reason] + list(exit_eval.secondary_reasons)),
            days_held,
            stage,
        )
        if self.dry_run:
            logger.info("[PB1][EXIT-DRY] code=%s qty=%s key=%s order_id=%s", display_code, orderable_qty, client_key, order_id)
            return exit_eval_payload

        if not self.kis:
            logger.warning("[PB1][EXIT][SKIP] kis missing code=%s", display_code)
            return exit_eval_payload
        if not self._pretrade_check(
            code=code,
            market=market,
            mode=mode,
            side="SELL",
            qty=orderable_qty,
            price=float(mark or 0.0),
            client_order_key=client_key,
            stage=stage,
        ):
            if not exit_eval_payload.get("order_skip_reasons"):
                exit_eval_payload["order_skip_reasons"] = ["pretrade_blocked"]
                logger.info("[EXIT][ORDER_SKIP] code=%s reasons=%s", display_code, exit_eval_payload["order_skip_reasons"])
            return exit_eval_payload

        exit_eval_payload["submit_attempted"] = 1
        emit_event(
            as_of=self._today,
            event="ORDER_SUBMIT",
            side="SELL",
            code=code,
            qty=orderable_qty,
            price=float(mark),
            order_type="MARKET",
            client_order_key=client_key,
        )
        resp = None
        kis_odno = None
        try:
            resp = self.kis.sell_stock_market(code, orderable_qty)
            kis_odno = (resp.get("output") or {}).get("ODNO") if isinstance(resp, dict) else None
        except Exception:
            logger.exception("[PB1][EXIT][FAIL] code=%s", display_code)
        self.orders_repo.mark_submitted(self.env, client_key, kis_odno, resp if isinstance(resp, dict) else {"resp": resp})
        ok = bool(resp and isinstance(resp, dict) and resp.get("rt_cd") == "0")
        rt_cd = resp.get("rt_cd") if isinstance(resp, dict) else None
        msg_cd = resp.get("msg_cd") if isinstance(resp, dict) else None
        msg1 = resp.get("msg1") if isinstance(resp, dict) else None
        emit_event(
            as_of=self._today,
            event="ORDER_RESULT",
            side="SELL",
            code=code,
            ok=ok,
            rt_cd=rt_cd,
            msg_cd=msg_cd,
            msg1=msg1,
            kis_odno=kis_odno,
        )
        logger.info(
            "[PB1][ORDER][RESULT] side=SELL code=%s name=%s ok=%s reason=%s rt_cd=%s msg_cd=%s msg1=%s",
            code,
            stock_name,
            int(ok),
            self._format_order_result_reason(resp if isinstance(resp, dict) else None),
            rt_cd,
            msg_cd,
            msg1,
        )
        exit_eval_payload["order_result"] = self._format_order_result_reason(resp if isinstance(resp, dict) else None)
        logger.info(
            "[TRADE][ORDER][SELL] code=%s name=%s oid=%s qty=%s price=%.2f result=%s",
            code,
            stock_name,
            kis_odno or order_id,
            orderable_qty,
            float(mark or 0.0),
            "ACCEPTED" if ok else "REJECTED",
        )
        if ok:
            exit_eval_payload["submitted"] = 1
            try:
                self.orders_repo.mark_acked(self.env, kis_odno, resp)
                logger.info("[ORDER][DB_ACK][OK][SELL] code=%s kis_odno=%s", code, kis_odno)
            except Exception as _sell_ack_exc:
                _soft_ack = os.getenv("PB1_ORDER_DB_ACK_FAIL_SOFT", "1") == "1"
                logger.warning(
                    "[ORDER][DB_ACK][TIMEOUT][SELL] code=%s kis_odno=%s err=%s",
                    code, kis_odno, _sell_ack_exc,
                )
                if not _soft_ack:
                    raise
            self.orders_repo.mark_filled(self.env, kis_odno=kis_odno, client_order_key=client_key)
            filled_at = now_kst()
            avg_buy_at_sell = float(pos.get("avg_buy_price") or avg or 0.0)
            position_qty_before_sell = int(pos.get("qty") or 0)
            sold_qty = int(orderable_qty or 0)
            cost_basis_at_sell = avg_buy_at_sell * sold_qty if avg_buy_at_sell > 0 else 0.0
            realized_pnl_at_sell = ((float(mark or 0.0) - avg_buy_at_sell) * sold_qty) if avg_buy_at_sell > 0 else 0.0
            realized_pnl_pct_at_sell = ((float(mark or 0.0) - avg_buy_at_sell) / avg_buy_at_sell * 100.0) if avg_buy_at_sell > 0 else 0.0
            self.fills_repo.upsert_fill(
                env=self.env,
                run_id=self.run_id,
                order_id=order_id,
                kis_odno=kis_odno,
                trade_id=None,
                code=code,
                market=market,
                side="SELL",
                qty=orderable_qty,
                price=mark,
                fee=0.0,
                tax=0.0,
                filled_at=filled_at,
                raw_json={"kis_response": resp, "exit_meta": exit_meta, "entry_exit_plan": exit_meta.get("entry_exit_plan") or {}},
                fill_meta_json={
                    "avg_buy_at_sell": avg_buy_at_sell,
                    "cost_basis_at_sell": cost_basis_at_sell,
                    "position_qty_before_sell": position_qty_before_sell,
                    "entry_date": pos.get("entry_date") or pos.get("entry_ts") or pos.get("last_fill_at"),
                    "realized_pnl": realized_pnl_at_sell,
                    "realized_pnl_pct": realized_pnl_pct_at_sell,
                    "exit_reason": exit_eval.primary_reason,
                    "exit_meta": exit_meta,
                },
            )
            self.positions_repo.apply_fill(
                env=self.env,
                strategy=self.STRATEGY_NAME,
                sid=1,
                mode=mode,
                code=code,
                market=market,
                side="SELL",
                qty=orderable_qty,
                price=mark,
                fee=0.0,
                tax=0.0,
                filled_at=filled_at,
                entry_meta_json=exit_meta,
            )
            logger.info(
                "[POSITIONS][UPSERT_AFTER_FILL] code=%s name=%s side=%s qty=%s price=%s source=order_fill",
                code,
                stock_name,
                "SELL",
                orderable_qty,
                mark,
            )
            logger.info(
                "[TRADE][FILL][SELL] code=%s name=%s oid=%s fill_qty=%s fill_px=%.2f",
                code,
                stock_name,
                kis_odno or order_id,
                orderable_qty,
                float(mark or 0.0),
            )
            if cooldown_until:
                self.positions_repo.update_position_fields(
                    env=self.env,
                    strategy=self.STRATEGY_NAME,
                    sid=sid,
                    mode=mode,
                    code=code,
                    fields={"cooldown_until": cooldown_until},
                )
        else:
            self.orders_repo.mark_error(self.env, client_key, resp if isinstance(resp, dict) else {"resp": resp})
        self.positions_repo.update_position_fields(
            env=self.env,
            strategy=self.STRATEGY_NAME,
            sid=sid,
            mode=mode,
            code=code,
            fields={"last_exit_eval_json": exit_eval_payload, "last_exit_plan_eval_json": exit_meta if 'exit_meta' in locals() else exit_eval_payload},
        )
        return exit_eval_payload

    def _positions_with_meta(self, positions: Iterable[Dict]) -> List[Dict]:
        enriched: List[Dict] = []
        for state in positions:
            if state.get("sid") != 1:
                continue
            enriched.append(
                {
                    "code": state.get("code"),
                    "sid": state.get("sid"),
                    "mode": state.get("mode"),
                    "qty": state.get("qty") or 0,
                    "kis_qty": state.get("kis_qty") or state.get("qty") or 0,
                    "avg_buy_price": state.get("avg_buy_price"),
                    "market": state.get("market"),
                    "holding_days": state.get("holding_days") or 0,
                    "first_buy_ts": state.get("first_buy_ts"),
                    "total_cost": state.get("total_cost") or 0.0,
                    "realized_pnl": state.get("realized_pnl") or 0.0,
                    "meta_source": state.get("meta_source"),
                    "entry_ts": state.get("entry_ts"),
                    "entry_reason": state.get("entry_reason"),
                    "entry_style_selected": state.get("entry_style_selected"),
                    "entry_decision_family": state.get("entry_decision_family"),
                    "entry_rule_version": state.get("entry_rule_version"),
                    "entry_meta_json": state.get("entry_meta_json") or {},
                    "stop_price_at_entry": state.get("stop_price_at_entry"),
                    "pivot_price_at_entry": state.get("pivot_price_at_entry"),
                    "initial_stop": state.get("initial_stop"),
                    "stop_price": state.get("stop_price"),
                    "max_price": state.get("max_price"),
                    "pyramid_level": state.get("pyramid_level"),
                    "pivot": state.get("pivot"),
                    "last_add_price": state.get("last_add_price"),
                    "last_stop_update_ts": state.get("last_stop_update_ts"),
                    "partial_exit_level": state.get("partial_exit_level"),
                    "base_id": state.get("base_id"),
                    "setup_id": state.get("setup_id"),
                    "tight_low": state.get("tight_low"),
                    "base_high": state.get("base_high"),
                    "entry_price": state.get("entry_price"),
                    "r_value": state.get("r_value"),
                    "tp1_done": state.get("tp1_done"),
                    "tp2_done": state.get("tp2_done"),
                    "trail_mode": state.get("trail_mode"),
                    "last_trail_stop": state.get("last_trail_stop"),
                    "cooldown_until": state.get("cooldown_until"),
                    "regime_at_entry": state.get("regime_at_entry"),
                    "risk_mult_at_entry": state.get("risk_mult_at_entry"),
                    "entry_thesis": state.get("entry_thesis"),
                    "trade_horizon": state.get("trade_horizon"),
                    "exit_policy_family": state.get("exit_policy_family"),
                    "eod_action": state.get("eod_action"),
                    "force_eod_close": state.get("force_eod_close"),
                    "max_trading_days": state.get("max_trading_days"),
                    "initial_stop_price": state.get("initial_stop_price"),
                    "initial_risk_r": state.get("initial_risk_r"),
                    "entry_exit_plan_json": state.get("entry_exit_plan_json") or {},
                    "last_exit_plan_eval_json": state.get("last_exit_plan_eval_json") or {},
                    "policy_source": state.get("policy_source"),
                    "policy_version": state.get("policy_version"),
                    "last_exit_eval_json": state.get("last_exit_eval_json") or {},
                }
            )
        return enriched

    def _run_exit_always(
        self,
        *,
        holdings_for_exit: list[HoldingContext],
        marks_fallback: dict[str, float],
    ) -> list[dict]:
        holdings_raw = list(holdings_for_exit or [])
        holdings_source = str((self._exit_holdings_meta or {}).get("source") or "unknown")
        logger.info("[EXIT][LOAD] holdings_raw=%s codes=%s source=%s", len(holdings_raw), [holding.code for holding in holdings_raw], holdings_source)
        holdings_exit_scope = [holding for holding in holdings_raw if int(holding.holding_qty or 0) > 0]
        logger.info("[EXIT][SCOPE] holdings_exit_scope=%s codes=%s", len(holdings_exit_scope), [holding.code for holding in holdings_exit_scope])
        existing_positions_count = len([holding for holding in holdings_raw if int(holding.holding_qty or 0) > 0])
        if existing_positions_count > 0 and len(holdings_exit_scope) == 0:
            logger.error(
                "[EXIT][INCONSISTENT_HOLDINGS] existing_positions=%s holdings_exit_scope=%s",
                existing_positions_count,
                len(holdings_exit_scope),
            )

        pos_list = [holding.to_position_dict() for holding in holdings_exit_scope]
        exit_evaluations: list[dict[str, Any]] = []
        for pos in pos_list:
            display_code = self._display_code(pos.get("code"))
            logger.info(
                "[PB1][POST_CAPITAL][EXIT_PASS][HOLDING] code=%s step=start qty=%s holding_days=%s",
                display_code,
                int(pos.get("qty") or 0),
                int(pos.get("holding_days") or 0),
            )
            if int(pos.get("holding_days") or 0) <= 0:
                df = pd.DataFrame()
                meta = {"source": "same_day_holdings_skip"}
            else:
                df, meta = self._fetch_exit_ohlcv(pos["code"])
            features: dict[str, Any] = {}
            if not df.empty:
                try:
                    features = compute_features(df)
                except ValueError:
                    features = {}
            features["_exit_ohlcv_source"] = str((meta or {}).get("source") or "none")
            features["_exit_ohlcv_rows"] = len(df)
            features["market"] = pos.get("market") or ""
            features["close"] = features.get("close") or pos.get("last_price") or pos.get("avg_buy_price") or 0.0
            marks_fallback[pos["code"]] = features.get("close") or pos.get("avg_buy_price") or 0.0
            evaluation = self._plan_exit_event(pos, features, df, "close" if self.window_internal == "close" else "manage")
            if isinstance(evaluation, dict):
                exit_evaluations.append(evaluation)
                logger.info(
                    "[PB1][POST_CAPITAL][EXIT_PASS][HOLDING_DONE] code=%s exit_ok=%s orderable_qty=%s submitted=%s primary_reason=%s",
                    display_code,
                    int(bool(evaluation.get("exit_ok"))),
                    int(evaluation.get("orderable_qty") or 0),
                    int(evaluation.get("submitted") or 0),
                    evaluation.get("primary_reason") or "none",
                )

        holdings_count = len(holdings_exit_scope)
        checked_count = len(exit_evaluations)
        signal_hit_count = len([evaluation for evaluation in exit_evaluations if bool(evaluation.get("exit_ok"))])
        orderable_count = len([
            evaluation
            for evaluation in exit_evaluations
            if bool(evaluation.get("exit_ok")) and int(evaluation.get("orderable_qty") or 0) > 0
        ])
        submit_attempt_count = len([evaluation for evaluation in exit_evaluations if int(evaluation.get("submit_attempted") or 0) > 0])
        submitted_count = len([evaluation for evaluation in exit_evaluations if int(evaluation.get("submitted") or 0) > 0])
        accepted_sell_count = submitted_count
        fill_confirmed_sell_count = submitted_count
        no_exit_count = len([evaluation for evaluation in exit_evaluations if not bool(evaluation.get("exit_ok"))])
        blocked_count = len([
            evaluation
            for evaluation in exit_evaluations
            if bool(evaluation.get("exit_ok")) and not int(evaluation.get("submitted") or 0)
        ])
        if checked_count != len(holdings_exit_scope):
            logger.error(
                "[CONSISTENCY][EXIT_HOLDINGS] existing_positions=%s holdings_exit_scope=%s checked=%s",
                existing_positions_count,
                len(holdings_exit_scope),
                checked_count,
            )
        self._exit_evaluations = exit_evaluations
        self._exit_summary_payload = {
            "holdings": holdings_count,
            "checked": checked_count,
            "signal_hit": signal_hit_count,
            "orderable": orderable_count,
            "evaluated_count": checked_count,
            "signal_hit_count": signal_hit_count,
            "orderable_count": orderable_count,
            "submit_attempt_count": submit_attempt_count,
            "accepted_sell_count": accepted_sell_count,
            "fill_confirmed_sell_count": fill_confirmed_sell_count,
            "no_exit_count": no_exit_count,
            "blocked_count": blocked_count,
            "submitted": submitted_count,
        }
        logger.info(
            "[EXIT][FUNNEL] holdings=%s checked=%s signal_hit=%s orderable=%s submit_attempted=%s submitted=%s accepted_sells=%s fill_confirmed_sells=%s blocked=%s no_exit=%s",
            holdings_count,
            checked_count,
            signal_hit_count,
            orderable_count,
            submit_attempt_count,
            submitted_count,
            accepted_sell_count,
            fill_confirmed_sell_count,
            blocked_count,
            no_exit_count,
        )
        if checked_count > signal_hit_count:
            logger.info("[EXIT][FUNNEL][DROP] stage=signal count=%s reasons=%s", checked_count - signal_hit_count, ["NO_EXIT_SIGNAL"])
        submit_drop_reasons = sorted(
            {
                reason
                for evaluation in exit_evaluations
                for reason in (evaluation.get("order_skip_reasons") or [])
                if evaluation.get("exit_ok") and not int(evaluation.get("submitted") or 0)
            }
        )
        submit_drop_count = len(
            [evaluation for evaluation in exit_evaluations if evaluation.get("exit_ok") and not int(evaluation.get("submitted") or 0)]
        )
        if submit_drop_count > 0:
            logger.info(
                "[EXIT][FUNNEL][DROP] stage=submit count=%s reasons=%s",
                submit_drop_count,
                submit_drop_reasons or ["ORDER_NOT_SUBMITTED"],
            )
        family_counter = Counter(
            str((evaluation.get("exit_policy_family") or evaluation.get("exit_family") or "UNKNOWN"))
            for evaluation in exit_evaluations
        )
        eval_reason_counter = Counter(
            str(evaluation.get("eval_reason") or (evaluation.get("primary_reason") if evaluation.get("signal_hit") else "NO_EXIT_SIGNAL") or "NO_EXIT_SIGNAL")
            for evaluation in exit_evaluations
        )
        router_reason_counter = Counter(
            str(evaluation.get("router_reason") or evaluation.get("decision_reason") or evaluation.get("primary_reason") or "NO_EXIT_SIGNAL")
            for evaluation in exit_evaluations
        )
        order_reason_counter = Counter(
            str(evaluation.get("order_result") or ("ORDER_ACCEPTED" if int(evaluation.get("submitted") or 0) > 0 else "ORDER_SKIPPED_NO_SIGNAL"))
            for evaluation in exit_evaluations
        )
        logger.info("[EXIT][SUMMARY_BY_FAMILY] counts=%s", dict(family_counter))
        logger.info("[EXIT][SUMMARY_BY_REASON] counts=%s", dict(eval_reason_counter))
        logger.info("[EXIT][SUMMARY][EVAL_REASON] counts=%s", dict(eval_reason_counter))
        logger.info("[EXIT][SUMMARY][ROUTER_REASON] counts=%s", dict(router_reason_counter))
        logger.info("[EXIT][SUMMARY][ORDER_RESULT] counts=%s", dict(order_reason_counter))
        self._exit_summary_payload.update(
            {
                "eval_reason_summary": dict(eval_reason_counter),
                "router_reason_summary": dict(router_reason_counter),
                "order_result_summary": dict(order_reason_counter),
            }
        )
        return pos_list

    def _load_close_entry_orders(self) -> list[dict]:
        path = close_entry_orders_path(self._today)
        if not path.exists():
            logger.info("[PB1][CLOSE_CANCEL][WHY] reason_codes=%s reason_text=%s stage=%s", ["close_entry_file_missing"], "close_entry_file_missing", "PB1-CLOSE")
            return []
        orders: list[dict] = []
        try:
            for line in path.read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                orders.append(json.loads(line))
        except Exception:
            logger.exception("[PB1][CLOSE_CANCEL][FAIL] invalid close_entry file path=%s", path)
            return []
        return orders

    def run_close_cancel(self) -> RunResult:
        open_orders = self._safe_get_open_orders()
        tracked = self._load_close_entry_orders()
        tracked_keys = {row.get("client_order_key") for row in tracked if row.get("client_order_key")}
        target_orders = []
        for row in open_orders:
            if str(row.get("side") or "").upper() != "BUY":
                continue
            if row.get("stage") != "PB1-CLOSE":
                continue
            if tracked_keys and row.get("client_order_key") not in tracked_keys:
                continue
            target_orders.append(row)
        for row in target_orders:
            code = row.get("code")
            qty = row.get("qty") or 0
            client_key = row.get("client_order_key")
            logger.info(
                "[PB1][CLOSE_CANCEL][WHY] code=%s qty=%s reason_codes=%s reason_text=%s stage=%s",
                self._display_code(code),
                qty,
                ["close_cancel"],
                "close_cancel",
                row.get("stage") or "PB1-CLOSE",
            )
            self.orders_repo.mark_cancelled(self.env, client_key, response_json={"reason": "close_cancel"})
        return RunResult(
            status="OK",
            notes="close_cancel_only",
            balance_api_calls=self.balance_api_calls,
            balance_cache_hits=self.balance_cache_hits,
            balance_tick_cache_hits=self.balance_tick_cache_hits,
        )

    def _load_universe(self) -> list[dict]:
        if self._universe_context is not None:
            members = list(self._universe_context.members or [])
            self._universe_as_of = self._universe_context.as_of_date or (
                members[0].get("as_of_date") if members else None
            )
            self._universe_path = self._universe_context.selected_path
        else:
            members = self.universe_repo.get_current_universe_members(self.env, self.UNIVERSE_STRATEGY)
            self._universe_as_of = members[0].get("as_of_date") if members else None
        self._code_name_map = {
            str(m.get("code") or "").zfill(6): (m.get("name") or (m.get("meta_json") or {}).get("name"))
            for m in members or []
            if m.get("code")
        }
        return members

    def _write_entry_scorecard_jsonl(self, candidates: list[CandidateFeature] | None = None) -> None:
        """
        각 종목별 스코어카드를 JSONL로 기록
        파일: runtime/signals/pb1_entry/{YYYY-MM-DD}/run_{trace}.jsonl
        """
        if not candidates:
            candidates = []
        
        try:
            today = self._today
            scorecard_dir = runtime_path("runtime", "signals", "pb1_entry", today)
            scorecard_dir.mkdir(parents=True, exist_ok=True)
            
            scorecard_path = scorecard_dir / f"run_{self.run_id}.jsonl"
            
            for cf in candidates:
                code = str(cf.code or "").zfill(6)
                features = cf.features or {}
                sizing_reason = getattr(cf, 'sizing_reason', None)
                sizing_details = getattr(cf, 'sizing_details', {}) or {}
                detail_reasons = getattr(cf, 'detail_reasons', None)
                
                scorecard_record = {
                    "run_id": str(self.run_id),
                    "trace": str(self.run_id),
                    "env": self.env,
                    "as_of_used": self._universe_as_of,
                    "code": code,
                    "setup_ok": bool(cf.setup_ok),
                    "reasons": cf.reasons or [],
                    # PB1 주요 수치
                    "pb1": {
                        "pullback_pct": features.get("pullback_pct"),
                        "vol_contraction": features.get("vol_contraction"),
                        "volu_contraction": features.get("volu_contraction"),
                        "close_vs_ma": features.get("close_vs_ma"),
                        "score": cf.score,
                    },
                    # Minervini 평가
                    "minervini": {
                        "buyable": cf.setup_ok,
                        "detail_reasons": detail_reasons,
                        "rs_pctile": features.get("rs_pctile"),
                        "vcp_score": features.get("vcp_score"),
                        "regime_pass": features.get("regime_pass"),
                    },
                    # Risk 평가
                    "risk": {
                        "atr_pct": features.get("atr_pct"),
                        "atr14": features.get("atr14"),
                        "ok": not any("atr" in r for r in (cf.reasons or [])),
                    },
                    # Sizing 정보
                    "sizing": {
                        "qty": cf.planned_qty,
                        "price": features.get("order_price"),
                        "reason": sizing_reason,
                        "details": sizing_details,
                    },
                    # Final 결정
                    "final": {
                        "selected": bool(cf.setup_ok and cf.planned_qty > 0),
                        "drop_reasons": cf.reasons or [],
                    },
                }
                
                append_jsonl(str(scorecard_path), scorecard_record)
            
            logger.info(
                "[PB1][SCORECARD][JSONL] written count=%s path=%s",
                len(candidates),
                scorecard_path,
            )
        except Exception as e:
            logger.warning(
                "[PB1][SCORECARD][FAIL] error=%s",
                type(e).__name__,
                exc_info=False,
            )

    def get_touched_files(self) -> list[Path]:
        return list(self._touched_files)

    def _build_universe(self) -> list[dict]:
        payload = run_rebalance(str(get_as_of_date()), return_by_market=True)
        selected = payload.get("selected") if isinstance(payload, dict) else []
        members: list[dict] = []
        for item in selected or []:
            code = str((item or {}).get("code") or "").zfill(6)
            if not code:
                continue
            members.append(
                {
                    "code": code,
                    "market": (item or {}).get("market") or "",
                    "name": (item or {}).get("name") or self._code_name_map.get(code, ""),
                }
            )
        return members

    @staticmethod
    def _final30_sort_key(cf: CandidateFeature) -> tuple[float, float, float, float]:
        feats = cf.features or {}
        pullback = float(feats.get("pullback_pct") or float("inf"))
        vol_c = float(feats.get("vol_contraction") or float("inf"))
        volu_raw = feats.get("volu_contraction")
        try:
            volu_v = float(volu_raw)
        except (TypeError, ValueError):
            volu_v = float("nan")
        volu_c = volu_v if np.isfinite(volu_v) else float("inf")
        trend_strength = float(feats.get("trend_strength") or 0.0)
        return (pullback, vol_c, volu_c, -trend_strength)

    def _kr_pb1_batch_preload_ohlcv(self, codes: list[str], days: int = 60) -> None:
        """한국장 PB1 전용 OHLCV batch preload.

        final30 30개 종목의 60일 OHLCV를 단일 DB 커넥션으로 한 번에 조회해
        daily_cache에 저장한다. 이후 _compute_candidates_from_codes에서
        종목별 DB 연결 반복을 방지해 PB1_FILTER 속도를 개선한다.

        해외장/공통 OHLCV 캐시와 분리된다 – KR 6자리 코드만 처리.
        """
        from trader.cache_ttl import daily_cache, DAILY_BAR_TTL_SEC
        from trader.data.ohlcv_provider import OHLCVResult
        from trader.utils.ohlcv import normalize_ohlcv
        from trader.db.engine import make_engine
        from trader.time_utils import now_kst

        kr_codes = [c for c in (codes or []) if _is_kr_stock_code(c)]
        if not kr_codes:
            return

        session_kind = str(os.getenv("PB1_SESSION_KIND", "")).strip().lower()
        logger.info(
            "[KR][PB1][OHLCV][BATCH_PRELOAD][START] codes=%s days=%s session=%s",
            len(kr_codes),
            days,
            session_kind,
        )
        t_start = time.monotonic()

        end_date = now_kst().date()
        from datetime import timedelta
        start_date = end_date - timedelta(days=max(days, 90))

        try:
            engine = make_engine()
            bulk_map = load_price_daily_bulk(engine, kr_codes, start_date, end_date)
        except Exception as exc:
            logger.warning("[KR][PB1][OHLCV][BATCH_PRELOAD][ERROR] err=%s", exc)
            return

        hit = 0
        miss = 0
        for code in kr_codes:
            candles = bulk_map.get(code) or []
            cache_key = ("daily", code, days)
            if daily_cache.get(cache_key) is not None:
                # 이미 캐시에 있으면 건너뜀
                hit += 1
                continue
            if not candles:
                miss += 1
                continue
            try:
                df_raw = pd.DataFrame(candles)
                df_norm, meta_norm = normalize_ohlcv(df_raw)
                df_norm = df_norm.sort_values("date").tail(days)
                meta_norm.update(
                    {
                        "provider": "kis",
                        "source": "db",
                        "rows": len(df_norm),
                        "stale_ok": True,
                        "refresh_failed": False,
                        "batch_preloaded": True,
                    }
                )
                result = OHLCVResult(df=df_norm, meta=meta_norm)
                daily_cache.set(cache_key, result, DAILY_BAR_TTL_SEC)
                logger.debug(
                    "[KR][PB1][OHLCV][CACHE][HIT] code=%s days=%s source=batch_preload rows=%s",
                    code,
                    days,
                    len(df_norm),
                )
                hit += 1
            except Exception as exc:
                logger.debug("[KR][PB1][OHLCV][BATCH_PRELOAD][SKIP] code=%s err=%s", code, exc)
                miss += 1

        elapsed = time.monotonic() - t_start
        logger.info(
            "[KR][PB1][OHLCV][BATCH_PRELOAD][DONE] hit=%s miss=%s elapsed=%.2f",
            hit,
            miss,
            elapsed,
        )

    def _parse_price_ts(self, value: Any) -> datetime | None:
        if value in (None, ""):
            return None
        if isinstance(value, datetime):
            _tz = getattr(getattr(self, "_now_kst", None), "tzinfo", ZoneInfo("Asia/Seoul"))
            return value if value.tzinfo is not None else value.replace(tzinfo=_tz)
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            _tz = getattr(getattr(self, "_now_kst", None), "tzinfo", ZoneInfo("Asia/Seoul"))
            return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=_tz)
        except Exception:
            return None

    def _fresh_current_price_from_features(self, code: str, features: dict[str, Any]) -> float | None:
        max_age = int(os.getenv("PB1_CURRENT_PRICE_MAX_AGE_SEC", "180") or "180")
        for key in ("current_price", "intraday_last", "last", "stck_prpr"):
            price = self._to_float(features.get(key))
            if price is None or price <= 0:
                continue
            ts = PB1Engine._parse_price_ts(
                self,
                features.get(f"{key}_ts")
                or features.get(f"{key}_at")
                or features.get("current_price_ts")
                or features.get("intraday_ts")
            )
            age_sec = 0.0
            if ts is not None:
                _now = getattr(self, "_now_kst", now_kst())
                age_sec = max(0.0, (_now - ts.astimezone(_now.tzinfo)).total_seconds())
                if age_sec > max_age:
                    logger.info(
                        "[ENTRY][CURRENT_PRICE][STALE] code=%s price=%s age_sec=%.1f max_age=%s",
                        code,
                        price,
                        age_sec,
                        max_age,
                    )
                    continue
            logger.info(
                "[ENTRY][CURRENT_PRICE][SOURCE] code=%s source=%s price=%s age_sec=%.1f",
                code,
                key,
                price,
                age_sec,
            )
            return price
        return None

    def _get_current_price_for_entry(self, code: str, features: dict[str, Any] | None = None) -> float | None:
        norm_code = str(code).zfill(6)
        features = features or {}
        try:
            feature_price = self._fresh_current_price_from_features(norm_code, features)
        except (TypeError, AttributeError):
            feature_price = PB1Engine._fresh_current_price_from_features(self, norm_code, features)
        if feature_price is not None:
            if hasattr(self, "_current_price_cache"):
                self._current_price_cache[norm_code] = (feature_price, getattr(self, "_now_kst", now_kst()))
            return feature_price

        ttl = int(os.getenv("PB1_CURRENT_PRICE_CACHE_TTL_SEC", "30") or "30")
        cache = getattr(self, "_current_price_cache", {})
        cached = cache.get(norm_code)
        if cached:
            price, cached_at = cached
            age_sec = max(0.0, (getattr(self, "_now_kst", now_kst()) - cached_at).total_seconds())
            if age_sec <= ttl:
                logger.info(
                    "[ENTRY][CURRENT_PRICE][SOURCE] code=%s source=tick_cache price=%s age_sec=%.1f",
                    norm_code,
                    price,
                    age_sec,
                )
                return price

        balance_price = self._to_float(getattr(self, "_balance_price_map", {}).get(norm_code))
        if balance_price is not None and balance_price > 0:
            logger.info(
                "[ENTRY][CURRENT_PRICE][SOURCE] code=%s source=kis_balance_prpr price=%s age_sec=0.0",
                norm_code,
                balance_price,
            )
            if hasattr(self, "_current_price_cache"):
                self._current_price_cache[norm_code] = (balance_price, getattr(self, "_now_kst", now_kst()))
            return balance_price

        max_fetch = int(os.getenv("PB1_FETCH_CURRENT_PRICE_MAX_PER_TICK", "30") or "30")
        if int(getattr(self, "_current_price_fetch_count", 0)) >= max_fetch:
            logger.info("[ENTRY][CURRENT_PRICE][MISSING] code=%s reason=fetch_limit_reached", norm_code)
            return None
        if not bool(getattr(self, "price_allowed", True)):
            logger.info("[ENTRY][CURRENT_PRICE][MISSING] code=%s reason=price_not_allowed", norm_code)
            return None
        self._current_price_fetch_count = int(getattr(self, "_current_price_fetch_count", 0)) + 1
        quote_price = self._mark_price(norm_code)
        if quote_price is not None and quote_price > 0:
            logger.info(
                "[ENTRY][CURRENT_PRICE][SOURCE] code=%s source=kis_quote price=%s age_sec=0.0",
                norm_code,
                quote_price,
            )
            if hasattr(self, "_current_price_cache"):
                self._current_price_cache[norm_code] = (quote_price, getattr(self, "_now_kst", now_kst()))
            return quote_price
        logger.info("[ENTRY][CURRENT_PRICE][MISSING] code=%s reason=no_intraday_price_available", norm_code)
        return None

    def _resolve_intraday_current_price_for_reclaim(
        self,
        code: str,
        *,
        current_price: float | None,
        last_close: float | None,
        ma20_value: float | None,
        features: dict[str, Any] | None = None,
    ) -> float | None:
        if current_price is not None and current_price > 0:
            return current_price
        if not (self.phase in {"entry", "pm_entry"} and last_close is not None and ma20_value is not None and last_close < ma20_value):
            return current_price
        get_price = getattr(self, "_get_current_price_for_entry", None)
        if callable(get_price):
            resolved = get_price(code, features or {})
        else:
            resolved = PB1Engine._get_current_price_for_entry(self, code, features or {})
        if resolved is None:
            logger.info(
                "[ENTRY][INTRADAY_RECLAIM][SKIP] code=%s reason=current_price_missing last_close=%s ma20=%s",
                code,
                last_close,
                ma20_value,
            )
        return resolved

    def _relax_bridge_enabled(self) -> tuple[bool, str]:
        raw = os.getenv("PB1_ENABLE_RELAX_BRIDGE")
        env_name = str(self.env or os.getenv("STRATEGY_ENV") or "practice").strip().lower()
        default_enabled = env_name == "practice"
        enabled = parse_bool_any(raw, default=default_enabled)
        if not enabled:
            return False, "disabled"
        if env_name == "real" and parse_bool_any(os.getenv("PB1_REAL_ALLOW_RELAX_BRIDGE"), default=False) is not True:
            logger.info("[ENTRY][RELAX_BRIDGE][DISABLED] reason=real_requires_explicit_opt_in")
            return False, "real_requires_explicit_opt_in"
        allowed = {x.strip() for x in str(os.getenv("PB1_RELAX_BRIDGE_ALLOWED_PHASES") or "entry,pm_entry").split(",") if x.strip()}
        if self.phase not in allowed:
            return False, "phase_not_allowed"
        return True, "enabled"

    def _activate_relax_bridge_candidates(
        self,
        candidates: list[CandidateFeature],
        *,
        setup_ok_count: int,
        scanner_passed_codes: set[str],
        minervini_passed_codes: set[str],
        order_allowed: bool,
    ) -> list[CandidateFeature]:
        enabled, disabled_reason = self._relax_bridge_enabled()
        scanner_count = len(scanner_passed_codes or set())
        minervini_count = len(minervini_passed_codes or set())
        logger.info(
            "[ENTRY][RELAX_BRIDGE][CHECK] enabled=%s pb1_setup_ok=%s scanner_pass=%s minervini_pass=%s",
            int(enabled),
            setup_ok_count,
            scanner_count,
            minervini_count,
        )
        self._relax_bridge_summary = {
            "enabled": bool(enabled),
            "activated": False,
            "reason": disabled_reason,
            "scanner_pass": scanner_count,
            "minervini_pass": minervini_count,
        }
        if not enabled:
            logger.info("[ENTRY][RELAX_BRIDGE][SKIP] reason=%s", disabled_reason)
            return []
        if self.phase not in {"entry", "pm_entry"}:
            logger.info("[ENTRY][RELAX_BRIDGE][SKIP] reason=phase_not_entry")
            self._relax_bridge_summary["reason"] = "phase_not_entry"
            return []
        if setup_ok_count > 0:
            logger.info("[ENTRY][RELAX_BRIDGE][SKIP] reason=pb1_setup_exists")
            self._relax_bridge_summary["reason"] = "pb1_setup_exists"
            return []
        if not order_allowed:
            logger.info("[ENTRY][RELAX_BRIDGE][SKIP] reason=order_not_allowed")
            self._relax_bridge_summary["reason"] = "order_not_allowed"
            return []
        if not bool(self.final30_locked or self._precomputed_final30_map):
            logger.info("[ENTRY][RELAX_BRIDGE][SKIP] reason=final30_missing")
            self._relax_bridge_summary["reason"] = "final30_missing"
            return []
        source_codes = set(scanner_passed_codes or set()) | set(minervini_passed_codes or set())
        if not source_codes:
            logger.info("[ENTRY][RELAX_BRIDGE][SKIP] reason=no_scanner_or_minervini_candidates")
            self._relax_bridge_summary["reason"] = "no_scanner_or_minervini_candidates"
            return []

        max_candidates = max(1, int(os.getenv("PB1_RELAX_BRIDGE_MAX_CANDIDATES", "3") or "3"))
        min_rs = float(os.getenv("PB1_RELAX_BRIDGE_MIN_RS", "70") or "70")
        min_score = float(os.getenv("PB1_RELAX_BRIDGE_MIN_SCORE", "70") or "70")
        require_price = parse_bool_any(os.getenv("PB1_RELAX_BRIDGE_REQUIRE_CURRENT_PRICE"), default=True)
        require_reclaim = parse_bool_any(os.getenv("PB1_RELAX_BRIDGE_REQUIRE_MA20_RECLAIM"), default=True)
        require_liquidity = parse_bool_any(os.getenv("PB1_RELAX_BRIDGE_REQUIRE_LIQUIDITY"), default=True)
        selected: list[CandidateFeature] = []
        for cf in sorted(candidates or [], key=lambda c: float((c.features or {}).get("score_final") or (c.features or {}).get("score") or 0), reverse=True):
            if cf.code not in source_codes or len(selected) >= max_candidates:
                continue
            features = cf.features or {}
            price = self._get_current_price_for_entry(cf.code, features)
            ma20 = self._to_float(features.get("ma20"))
            rs = self._to_float(features.get("rs_percentile") or features.get("rs_pctile")) or 0.0
            score = self._to_float(features.get("score_final") or features.get("score") or features.get("final_score")) or 0.0
            atr_pct = self._to_float(features.get("atr_pct")) or 0.0
            value20 = self._to_float(features.get("value20"))
            if require_price and (price is None or price <= 0):
                logger.info("[ENTRY][RELAX_BRIDGE][SKIP] code=%s reason=current_price_missing", cf.code)
                continue
            reclaim_ok = bool(price is not None and ma20 is not None and price >= ma20 * 1.001)
            if require_reclaim and not reclaim_ok:
                logger.info("[ENTRY][RELAX_BRIDGE][SKIP] code=%s reason=ma20_reclaim_missing", cf.code)
                continue
            if require_liquidity and value20 is not None and value20 < float(PB1_MIN_VALUE20):
                logger.info("[ENTRY][RELAX_BRIDGE][SKIP] code=%s reason=liquidity_fail", cf.code)
                continue
            if atr_pct > float(PB1_MAX_ATR_PCT_RAW):
                logger.info("[ENTRY][RELAX_BRIDGE][SKIP] code=%s reason=atr_too_high atr_pct=%s", cf.code, atr_pct)
                continue
            if rs < min_rs and score < min_score:
                logger.info("[ENTRY][RELAX_BRIDGE][SKIP] code=%s reason=score_rs_below_min score=%s rs=%s", cf.code, score, rs)
                continue
            cf.setup_ok = True
            cf.reasons = [r for r in (cf.reasons or []) if r not in {"close_below_ma20", "close_below_ma"}]
            features["setup_loose_ok"] = True
            features["entry_reason"] = "ENTRY_RELAX_BRIDGE"
            features["decision_family"] = "ENTRY_RELAX_BRIDGE_INTRADAY_RECLAIM"
            flags = set(list(features.get("quality_flags") or []))
            flags.add("RELAX_BRIDGE")
            if reclaim_ok:
                flags.add("INTRADAY_RECLAIM_MA20")
            features["quality_flags"] = sorted(flags)
            features["current_price"] = price
            logger.info(
                "[ENTRY][RELAX_BRIDGE][CANDIDATE] code=%s score=%s rs=%s current_price=%s ma20=%s reason=ENTRY_RELAX_BRIDGE",
                cf.code,
                score,
                rs,
                price,
                ma20,
            )
            selected.append(cf)
        if not selected:
            logger.info("[ENTRY][RELAX_BRIDGE][SKIP] reason=no_bridge_candidates")
            self._relax_bridge_summary.update({"reason": "no_bridge_candidates", "count": 0})
            return []
        if scanner_passed_codes and minervini_passed_codes:
            source = "combined"
        elif scanner_passed_codes:
            source = "scanner"
        else:
            source = "minervini"
        logger.info(
            "[ENTRY][RELAX_BRIDGE][ACTIVATED] source=%s count=%s max=%s",
            source,
            len(selected),
            max_candidates,
        )
        logger.info("[ENTRY][RELAX_BRIDGE][TO_RISK] count=%s", len(selected))
        self._relax_bridge_summary.update({"activated": True, "reason": "activated", "count": len(selected), "source": source, "max_candidates": max_candidates})
        return selected

    def _compute_candidates_from_codes(self, codes: list[str]) -> list[CandidateFeature]:
        # ── [KR][PB1] scope guard + batch OHLCV preload ─────────────────────
        # 한국장 PB1 trade 모드일 때 final30 코드 전체를 단일 DB 커넥션으로
        # 미리 캐시에 로드해 종목별 반복 커넥션을 방지한다.
        _session = str(os.getenv("PB1_SESSION_KIND", "")).strip().lower()
        _mode = str(os.getenv("MODE", "")).strip().lower()
        _kr_pb1_scope = (
            _mode == "trade"
            and _session in ("am", "afternoon", "pm")
            and bool(self._precomputed_final30_map)
        )
        if _kr_pb1_scope:
            _final30_source = "db_pb1_watchlist_final_scored"
            logger.info(
                "[KR][PB1][SCOPE] enabled=1 market=KR source=%s session=%s",
                _final30_source,
                _session,
            )
            self._kr_pb1_batch_preload_ohlcv(list(codes or []), days=60)

        code_market = {
            str(m.get("code") or "").zfill(6): (m.get("market") or "")
            for m in (self._load_universe() or [])
            if m.get("code")
        }
        candidates: list[CandidateFeature] = []
        for raw_code in codes or []:
            code = str(raw_code or "").zfill(6)
            if not code:
                continue
            market = code_market.get(code, "")
            try:
                merged_features, usable_checks, usable_reasons, usable_precomputed_row, precomputed_data_ok = self._map_precomputed_candidate_row(code)

                b_ok = usable_checks["has_breakout_score"] == 1
                p_ok = usable_checks["has_pullback_score"] == 1
                m_ok = usable_checks["has_momentum_score"] == 1
                ma_ok = all(usable_checks[key] == 1 for key in ("has_ma20", "has_ma50", "has_ma150"))
                rs_ok = usable_checks["has_rs_percentile"] == 1
                vcp_ok = usable_checks["has_vcp_score"] == 1

                source_mode = "precomputed"
                if self._precomputed_final30_map.get(code) or self._precomputed_derived_map.get(code):
                    self._metric_add("precomputed_hits", 1)
                    logger.info(
                        "[PB1][PRECOMPUTED][USABLE_CHECK] code=%s has_close=%s has_breakout_score=%s has_pullback_score=%s has_momentum_score=%s has_rs_percentile=%s has_vcp_score=%s has_ma20=%s has_ma50=%s has_ma150=%s has_entry_style_selected=%s has_price_context=%s usable=%s reasons=%s",
                        code,
                        usable_checks["has_close"],
                        usable_checks["has_breakout_score"],
                        usable_checks["has_pullback_score"],
                        usable_checks["has_momentum_score"],
                        usable_checks["has_rs_percentile"],
                        usable_checks["has_vcp_score"],
                        usable_checks["has_ma20"],
                        usable_checks["has_ma50"],
                        usable_checks["has_ma150"],
                        usable_checks["has_entry_style_selected"],
                        usable_checks["has_price_context"],
                        int(usable_precomputed_row),
                        usable_reasons,
                    )
                    logger.info(
                        "[PB1][FEATURE_SOURCE] code=%s breakout=%s pullback=%s momentum=%s ma=%s rs=%s vcp=%s source=precomputed",
                        code,
                        b_ok,
                        p_ok,
                        m_ok,
                        ma_ok,
                        rs_ok,
                        vcp_ok,
                    )
                else:
                    source_mode = "short_ohlcv"

                df = pd.DataFrame()
                meta: dict[str, Any] = {}
                need_short_ohlcv_fill = not precomputed_data_ok or any(
                    self._value_missing(merged_features.get(key))
                    for key in ("ma20_slope", "vol_contraction", "volu_contraction", "high20", "tr_range_pct", "trend_strength", "ma10", "value20")
                )
                if need_short_ohlcv_fill:
                    df, meta = self._fetch_daily(code, days=60)
                    if df is None or df.empty:
                        if not precomputed_data_ok:
                            logger.info(
                                "[PB1][PRECOMPUTED][DATA_OK_FAIL] code=%s reasons=%s source=%s",
                                code,
                                usable_reasons,
                                source_mode,
                            )
                            continue
                    else:
                        fresh_features = compute_pb1_features(df, min_candles=min(self.min_candles, max(20, len(df))))
                        self._merge_non_missing(
                            merged_features,
                            fresh_features,
                            zero_missing_keys={
                                "atr_pct",
                                "high20",
                                "ma10",
                                "ma20",
                                "ma50",
                                "ma150",
                                "ma20_slope",
                                "pullback_pct",
                                "tr_range_pct",
                                "trend_strength",
                                "value20",
                                "vol_contraction",
                                "volu_contraction",
                            },
                        )
                        if source_mode == "precomputed":
                            source_mode = "precomputed+short_ohlcv_fill"

                features = {
                    "close": float(merged_features.get("close") or (float(df["close"].iloc[-1]) if not df.empty else 0.0)),
                    "current_price": self._to_float(merged_features.get("current_price") or merged_features.get("intraday_last")),
                    "ma20": float(merged_features.get("ma20") or 0.0),
                    "ma50": float(merged_features.get("ma50") or 0.0),
                    "ma10": float(merged_features.get("ma10") or 0.0),
                    "atr14": float(merged_features.get("atr14") or merged_features.get("atr") or 0.0),
                    "atr_pct": float(merged_features.get("atr_pct") or 0.0),
                    "vol_contraction": float(merged_features.get("vol_contraction") or 0.9),
                    "volu_contraction": float(merged_features.get("volu_contraction") or 0.9),
                    "ma20_slope": merged_features.get("ma20_slope"),
                    "high20": float(merged_features.get("high20") or 0.0),
                    "pullback_pct": float(merged_features.get("pullback_pct") or 0.0),
                    "tr_range_pct": float(merged_features.get("tr_range_pct") or 0.0),
                    "trend_strength": float(merged_features.get("trend_strength") or 1.0),
                    "value20": merged_features.get("value20"),
                    "volume_missing": bool(merged_features.get("volume_missing", False)),
                    "breakout_score": float(merged_features.get("breakout_score") or 0.0),
                    "pullback_score": float(merged_features.get("pullback_score") or 0.0),
                    "momentum_score": float(merged_features.get("momentum_score") or 0.0),
                    "entry_style_selected": merged_features.get("entry_style_selected"),
                    "rs_percentile": float(merged_features.get("rs_percentile") or 0.0),
                    "vcp_score": float(merged_features.get("vcp_score") or 0.0),
                    "trend_score": float(merged_features.get("trend_score") or 0.0),
                    "ma150": float(merged_features.get("ma150") or 0.0),
                    "score_final": float(merged_features.get("score_final") or 0.0),
                    "tech_score": float(merged_features.get("tech_score") or 0.0),
                    "source_mode": source_mode,
                    "precomputed_usable_row": bool(usable_precomputed_row),
                    "precomputed_usable_reasons": list(usable_reasons),
                }
                features["market"] = market
                features["_pb1_vol_max"] = float(self.filter_thresholds.vol_contraction_max)
                features["_pb1_volu_max"] = float(self.filter_thresholds.volu_contraction_max)
                features["volume_missing"] = bool(meta.get("volume_missing")) if isinstance(meta, dict) else bool(merged_features.get("volume_missing", False))
                features["data_ok"] = bool(precomputed_data_ok or not df.empty)

                last_close = self._to_float(features.get("close"))
                current_price = self._resolve_intraday_current_price_for_reclaim(
                    code,
                    current_price=self._to_float(features.get("current_price")),
                    last_close=last_close,
                    ma20_value=self._to_float(features.get("ma20")),
                    features=features,
                )
                if current_price is not None and current_price > 0:
                    features["current_price"] = current_price
                ma20_value = self._to_float(features.get("ma20"))
                if (
                    self.phase in {"entry", "pm_entry"}
                    and last_close is not None
                    and current_price is not None
                    and ma20_value is not None
                    and last_close < ma20_value
                    and current_price >= ma20_value * 1.001
                ):
                    features["last_close"] = last_close
                    features["close"] = current_price
                    features["intraday_reclaim_ma20"] = True
                    features["quality_flags"] = list(set(list(features.get("quality_flags") or []) + ["INTRADAY_RECLAIM_MA20"]))
                    logger.info(
                        "[ENTRY][INTRADAY_RECLAIM] code=%s last_close=%s current_price=%s ma20=%s action=soften_close_below_ma20",
                        code,
                        last_close,
                        current_price,
                        ma20_value,
                    )
                elif (
                    self.phase in {"entry", "pm_entry"}
                    and last_close is not None
                    and current_price is not None
                    and ma20_value is not None
                    and last_close < ma20_value
                ):
                    logger.info(
                        "[ENTRY][INTRADAY_RECLAIM][NO] code=%s current_price=%s ma20=%s required=%s",
                        code,
                        current_price,
                        ma20_value,
                        ma20_value * 1.001,
                    )
                loose_ok, loose_reasons = evaluate_pb1_setup(
                    features,
                    market=market,
                    require_volume=self.require_volume,
                    mode="relaxed",
                    relax_ma_filter=PB1_RELAX_MA_FILTER,
                    relax_ma20_slope=PB1_RELAX_MA20_SLOPE,
                )
                strict_ok, strict_reasons = evaluate_pb1_setup(
                    features,
                    market=market,
                    require_volume=self.require_volume,
                    mode="strict",
                    relax_ma_filter=False,
                    relax_ma20_slope=False,
                )
                features["setup_loose_ok"] = bool(loose_ok)
                features["setup_strict_ok"] = bool(strict_ok)
                features["setup_loose_reasons"] = list(loose_reasons or [])
                features["setup_strict_reasons"] = list(strict_reasons or [])
                cf = CandidateFeature(
                    code=code,
                    market=market,
                    features=features,
                    setup_ok=bool(loose_ok),
                    reasons=list(loose_reasons or []),
                    mode=1,
                    mode_reasons=["pb1_from_final30"],
                )
                candidates.append(cf)
            except Exception as exc:
                logger.debug("[PB1][FINAL30][CAND_FAIL] code=%s err=%s", code, exc)
                continue
        return candidates

    def _select_final30_codes(self, candidates: list[CandidateFeature]) -> list[str]:
        setup_ok = [cf for cf in candidates if cf.setup_ok]
        ranked = sorted(setup_ok, key=self._final30_sort_key)
        return [cf.code for cf in ranked[:30]]

    @staticmethod
    def _normalize_final30_rows(rows: list[dict]) -> tuple[list[dict], list[str]]:
        required_defaults: dict[str, Any] = {
            "code": "",
            "name": "",
            "score_final": 0.0,
            "tech_score": 0.0,
            "flow_score": 0.0,
            "rs_percentile": 0.0,
            "vcp_score": 0.0,
            "breakout_score": 0.0,
            "pullback_score": 0.0,
            "momentum_score": 0.0,
            "entry_style_selected": "unknown",
        }
        normalized: list[dict] = []
        missing_cols: set[str] = set()
        for row in rows or []:
            item = dict(row or {})
            for key, default in required_defaults.items():
                if key not in item:
                    missing_cols.add(key)
                    item[key] = default
            item["code"] = str(item.get("code") or "").zfill(6)
            normalized.append(item)
        return normalized, sorted(missing_cols)

    @staticmethod
    def _scored_missing_cols(columns: list[str]) -> list[str]:
        colset = {str(c) for c in (columns or [])}
        missing = [c for c in REQUIRED_FINAL30_SCORED_COLS if c not in colset]
        for primary, alternative in ALTERNATIVE_REQUIRED_SCORED_COLS:
            if primary in missing and alternative in colset:
                missing.remove(primary)
        return missing

    def _write_final30_input_reject_debug(
        self,
        *,
        source: str,
        as_of: str,
        rows: int,
        columns: list[str],
        missing_scored_cols: list[str],
    ) -> None:
        diag_dir = Path("runtime") / "diagnostics" / str(getattr(self, "_today", None) or as_of)
        diag_dir.mkdir(parents=True, exist_ok=True)
        path = diag_dir / "final30_input_reject.json"
        payload = {
            "source": source,
            "as_of": as_of,
            "rows": int(rows),
            "columns": list(columns or []),
            "missing_scored_cols": list(missing_scored_cols or []),
        }
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def _normalize_trade_input_source(self, df: pd.DataFrame | None = None) -> str:
        source = str(getattr(self, "final30_source", "none") or "none")
        frame = df if isinstance(df, pd.DataFrame) else (self.final30_df if isinstance(self.final30_df, pd.DataFrame) else pd.DataFrame())
        cols = {str(col) for col in frame.columns.tolist()}
        plain_universe_cols = {
            "as_of_date",
            "code",
            "env",
            "market",
            "market_cap",
            "name",
            "provider",
            "rank",
            "reason",
            "strategy",
        }
        if source in {"db_pb1_watchlist_final_scored", "final30_locked", "watchlist_env", "candidate_pool_only"}:
            return source
        if source in {"db", "db_only", "best_k_meta", "universe", "plain_universe", "db_plain_universe"}:
            return "db_plain_universe"
        if plain_universe_cols.issubset(cols):
            return "db_plain_universe"
        return source

    def _classify_locked_final30_abort_reason(self, df: pd.DataFrame | None) -> tuple[str, list[str]]:
        frame = df if isinstance(df, pd.DataFrame) else pd.DataFrame()
        rows = len(frame)
        cols = [str(col) for col in frame.columns.tolist()]
        missing_cols = self._scored_missing_cols(cols)
        normalized_source = self._normalize_trade_input_source(frame)
        universe_meta = dict(getattr(getattr(self, "_universe_context", None), "meta", {}) or {})
        locked_rows = list(universe_meta.get("locked_final30_rows") or [])
        locked_has_contract = len(locked_rows) == 30
        if normalized_source == "db_plain_universe":
            return "stripped_members_contamination", missing_cols
        if normalized_source not in {"db_pb1_watchlist_final_scored", "final30_locked"}:
            if locked_has_contract:
                return "locked_final30_source_mismatch", missing_cols
            return "missing_db_exact_scored_final30", missing_cols
        if rows == 0:
            if locked_has_contract:
                return "stripped_members_contamination", missing_cols
            return "missing_db_exact_scored_final30", missing_cols
        if rows != 30:
            if locked_has_contract:
                return "stripped_members_contamination", missing_cols
            return "invalid_db_exact_scored_final30_contract", missing_cols
        if missing_cols:
            if locked_has_contract:
                return "stripped_members_contamination", missing_cols
            return "invalid_db_exact_scored_final30_contract", missing_cols
        return "missing_db_exact_scored_final30", missing_cols

    def _abort_locked_final30(self, *, as_of: str, reason: str, rows: int, missing_cols: list[str]) -> None:
        normalized_source = self._normalize_trade_input_source()
        columns = [str(col) for col in (self.final30_df.columns.tolist() if isinstance(self.final30_df, pd.DataFrame) else [])]
        logger.info(
            "[FINAL30][SOURCE_SUMMARY] source=%s rows=%s locked=0 usable=0",
            normalized_source,
            rows,
        )
        logger.error("[FINAL30][ABORT] reason=%s", reason)
        if reason in {"invalid_db_exact_scored_final30_contract", "stripped_members_contamination"}:
            logger.error(
                "[PB1][ENTRY][INPUT_REJECT] reason=plain_universe_contamination rows=%s cols=%s",
                rows,
                columns,
            )
        if reason == "locked_final30_source_mismatch":
            logger.error(
                "[PB1][ENTRY][INPUT_REJECT] reason=locked_final30_source_mismatch final30_source=%s rows=%s cols=%s",
                str(getattr(self, "final30_source", "none") or "none"),
                rows,
                columns,
            )
        logger.error("[PB1][ENTRY][GUARD] db_locked_final30_scored_missing -> abort as_of=%s env=%s", as_of, self.env)
        logger.error("[PB1][ENTRY][ABORT] source=%s require_scored=1", normalized_source)
        logger.error("[PB1][ENTRY][ABORT] reason=%s", reason)
        self._write_final30_input_reject_debug(
            source=normalized_source,
            as_of=as_of,
            rows=rows,
            columns=columns,
            missing_scored_cols=missing_cols,
        )
        raise SystemExit(2)

    def _load_entry_final30_or_abort(self, as_of: str) -> list[str]:
        """
        Entry final input은 locked DB final30 scored rows만 허용한다.
        """
        require_scored = (os.getenv("TRADE_REQUIRE_PREP_FINAL30_SCORED", "1") == "1")
        locked_as_of = str(getattr(self, "derived_as_of", None) or as_of)
        if os.getenv("PB1_SKIP_NEW_ENTRIES", "0") == "1":
            logger.warning("[ENTRY][SKIP_NEW] reason=missing_scored_final30")
            return []

        locked_df = self.final30_df if isinstance(self.final30_df, pd.DataFrame) else pd.DataFrame()
        rows = len(locked_df)
        reason, missing_cols = self._classify_locked_final30_abort_reason(locked_df)
        normalized_source = self._normalize_trade_input_source(locked_df)
        usable_locked = bool(
            self.final30_locked
            and normalized_source in {"db_pb1_watchlist_final_scored", "final30_locked"}
            and rows == 30
            and not missing_cols
        )
        logger.info(
            "[PB1][ENTRY][INPUT_CHECK] locked=%s source=%s as_of=%s usable=%s",
            int(bool(self.final30_locked)),
            normalized_source,
            locked_as_of,
            int(usable_locked),
        )
        if not usable_locked:
            if self._universe_context and self._universe_context.members:
                logger.info(
                    "[SCAN_UNIVERSE][LOCKED_FINAL30_REQUIRED] scan_source=%s rows=%s",
                    str((self._universe_context.meta or {}).get("source") or "unknown"),
                    len(self._universe_context.members or []),
                )
            self._abort_locked_final30(as_of=as_of, reason=reason, rows=rows, missing_cols=missing_cols)

        logger.info(
            "[FINAL30][SOURCE_SUMMARY] source=db_pb1_watchlist_final_scored rows=%s as_of=%s locked=1 usable=1",
            rows,
            locked_as_of,
        )
        try:
            validate_trade_ready(locked_df)
        except RuntimeError:
            reason, missing_cols = self._classify_locked_final30_abort_reason(locked_df)
            self._abort_locked_final30(as_of=as_of, reason=reason, rows=rows, missing_cols=missing_cols)

        if require_scored and normalized_source not in {"db_pb1_watchlist_final_scored", "final30_locked"}:
            self._abort_locked_final30(as_of=as_of, reason="missing_db_exact_scored_final30", rows=rows, missing_cols=missing_cols)

        logger.info(
            "[TRADE][READY][OK] source=%s as_of=%s rows=%s",
            self.final30_source,
            locked_as_of,
            rows,
        )
        order_rows = [dict(x or {}) for x in locked_df.to_dict(orient="records")]
        if "rank_final30" in locked_df.columns:
            order_rows.sort(key=lambda x: (float(x.get("rank_final30") or 999999), str(x.get("code") or "")))
        elif "score_final" in locked_df.columns:
            order_rows.sort(key=lambda x: (-float(x.get("score_final") or 0.0), str(x.get("code") or "")))
        else:
            order_rows.sort(key=lambda x: str(x.get("code") or ""))
        logger.info(
            "[PB1][FINAL30][USE_LOCKED] source=%s as_of=%s rows=%s",
            self.final30_source,
            locked_as_of,
            len(order_rows),
        )
        return [str(m.get("code") or "").zfill(6) for m in order_rows if m.get("code")]

    def _resolve_scan_members_for_entry(self, universe_members: list[dict], watchlist_members: list[dict], watchlist_reason: str) -> tuple[list[dict], str]:
        """
        Entry scan target must be deterministic:
        - if watchlist(candidate_pool) has members -> scan ONLY watchlist
        - else -> scan universe
        Returns: (scan_members, scan_source)
        """
        wl = list(watchlist_members or [])
        uni = list(universe_members or [])

        watchlist_enabled = os.getenv("PB1_WATCHLIST_ENABLED", "1") == "1"
        
        if watchlist_enabled and len(wl) > 0:
            # watchlist가 존재하면 무조건 watchlist만 스캔
            return wl, watchlist_reason or "candidate_pool"
        
        # watchlist가 없거나 비활성화된 경우에만 universe 사용
        return uni, "universe"

    def _load_today_watchlist_members(self) -> tuple[list[dict], str]:
        """
        ✅ NEW: 후보군(Candidate Pool) 우선 사용 로직.
        
        1. 후보군이 유효하면 → 후보군 사용
        2. 후보군 없음/만료/작음 + ENABLED=1 → 가벼운 스캔으로 후보군 생성 후 사용
        3. 최종 fallback → 195 유니버스 사용 (EMERGENCY)
        """
        from trader.candidate_pool_builder import (
            candidate_pool_scan_only_meta,
            load_candidate_pool,
            build_and_save_candidate_pool,
            CandidatePoolBuilder,
        )
        from trader.config import (
            CANDIDATE_POOL_ENABLED,
            CANDIDATE_POOL_FORCE_REBUILD,
            CANDIDATE_POOL_MIN_SIZE,
            CANDIDATE_POOL_SIZE,
            CANDIDATE_POOL_MIN_PRICE,
        )
        
        # ✅ 후보군 시스템 비활성화 시 기존 워치리스트 로직 사용
        if not CANDIDATE_POOL_ENABLED:
            logger.info("[CANDIDATE_POOL] disabled -> fallback to legacy watchlist logic")
            return self._load_legacy_watchlist_members()
        
        today = self._today
        
        # normalize today to date
        if isinstance(today, str):
            today = today.split("T")[0]
            today = date.fromisoformat(today)
        elif isinstance(today, datetime):
            today = today.date()
        
        # ✅ CRITICAL: derived_as_of 사용 (전일 종가 기준)
        # trade-tick에서 watchlist는 전일 빌드된 것을 사용해야 함
        from trader.time_utils import resolve_derived_as_of
        derived_as_of = resolve_derived_as_of(self._now_kst)
        
        # ✅ FIX: watchlist/candidate는 STRATEGY_ENV namespace를 사용
        import os
        pool_env = os.getenv("STRATEGY_ENV", self.env)

        # ✅ trade 모드: watchlist_final only (no universe/candidate fallback)
        trade_mode = (os.getenv("MODE") or "").strip().lower() == "trade"
        if trade_mode:
            locked_df = self.final30_df if isinstance(self.final30_df, pd.DataFrame) else pd.DataFrame()
            locked_source = str(getattr(self, "final30_source", "none") or "none")
            locked_cols = [str(col) for col in locked_df.columns.tolist()]
            locked_missing_scored = self._scored_missing_cols(locked_cols)

            if (
                bool(getattr(self, "final30_locked", False))
                and locked_source in {"db_pb1_watchlist_final_scored", "final30_locked"}
                and len(locked_df) == 30
                and not locked_missing_scored
            ):
                rows = [dict(item or {}) for item in locked_df.to_dict(orient="records")]
                top10_codes = [str(item.get("code") or "").zfill(6) for item in rows[:10] if item.get("code")]

                logger.info(
                    "[PB1][WATCHLIST][LOCK_REUSE] env=%s source=%s as_of=%s rows=%s",
                    pool_env,
                    locked_source,
                    getattr(self, "derived_as_of", None) or self.get_as_of(),
                    len(rows),
                )
                logger.info(
                    "[TRADE][WATCHLIST_FINAL][TOP10] source=%s rank_basis=locked_final30 codes=%s",
                    locked_source,
                    top10_codes,
                )

                members = []
                for row in rows:
                    member = dict(row)
                    code = str(member.get("code") or "").zfill(6)
                    member["code"] = code
                    member.setdefault("name", self._code_name_map.get(code, ""))
                    members.append(member)

                return members, "locked_final30_reuse"

            pool_strategy = os.getenv("WATCHLIST_FINAL_SCORED_STRATEGY_KEY", "pb1_watchlist_final_scored").strip().lower()
            request_as_of = derived_as_of
            try:
                df = load_final30_scored_db_only(
                    self.engine,
                    env=pool_env,
                    strategy=pool_strategy,
                    as_of=request_as_of,
                    require_exact_rows=30,
                    fail_if_missing=True,
                )
            except ScoredWatchlistNotFoundError:
                logger.error(
                    "[WATCHLIST][TRADE][MISS] requested=%s env=%s strategy=%s max_back_days=%d",
                    request_as_of,
                    pool_env,
                    pool_strategy,
                    0,
                )
                return [], "db_exact_scored_final30_missing"
            except ScoredWatchlistInvalidError as exc:
                logger.error(
                    "[WATCHLIST][TRADE][INVALID] requested=%s env=%s strategy=%s source=trade_scored_reload reason=%s",
                    request_as_of,
                    pool_env,
                    pool_strategy,
                    exc.reason,
                )
                raise RuntimeError("db_exact_scored_final30_invalid") from exc

            rows = [dict(item or {}) for item in df.to_dict(orient="records")]

            top10_codes = [str(item.get("code") or "").zfill(6) for item in rows[:10] if item.get("code")]
            logger.info(
                "[TRADE][WATCHLIST_FINAL][LOCK] env=%s strategy=%s requested_as_of=%s actual_as_of=%s n=%d",
                pool_env,
                pool_strategy,
                request_as_of,
                request_as_of,
                len(rows),
            )
            logger.info(
                "[TRADE][WATCHLIST_FINAL][TOP10] source=db_pb1_watchlist_final_scored rank_basis=stored_rank codes=%s",
                top10_codes,
            )

            members = []
            for row in rows:
                member = dict(row)
                code = str(member.get("code") or "").zfill(6)
                member["code"] = code
                member.setdefault("name", self._code_name_map.get(code, ""))
                members.append(member)
            return members, "db_exact_scored_final30"

        full_members = self._load_universe()

        pool_codes, pool_as_of, pool_reason = load_candidate_pool(
            engine=self.engine,
            env=pool_env,
            today=today,
        )

        if pool_reason == "hit" and pool_codes:
            age_info = f" (latest={pool_as_of})" if pool_as_of != today else " (today)"
            logger.info(
                "[CANDIDATE_POOL][USAGE] candidates_universe_size=%s as_of=%s%s (NOT 195 universe)",
                len(pool_codes), pool_as_of, age_info
            )
            if os.getenv("MINERVINI_ONLY") == "1":
                print(
                    f"[MINERVINI_ONLY][POOL] loaded={len(pool_codes)} as_of={pool_as_of}{age_info}"
                )
            members = [
                {
                    "code": code,
                    "name": self._code_name_map.get(code, ""),
                    **candidate_pool_scan_only_meta(),
                }
                for code in pool_codes
            ]
            return members, f"candidate_pool_hit"

        logger.warning(
            "[CANDIDATE_POOL][MISS] reason=%s -> rebuild_light_scan",
            pool_reason
        )

        try:
            force_rebuild = CANDIDATE_POOL_FORCE_REBUILD

            def _ohlcv_wrapper(code: str, days: int = 100):
                df, meta = self._fetch_daily(code, count=days)
                return df

            pool_codes = build_and_save_candidate_pool(
                engine=self.engine,
                env=pool_env,
                as_of=today,
                members=full_members,
                ohlcv_provider=_ohlcv_wrapper,
                force_rebuild=force_rebuild,
            )

            if pool_codes and len(pool_codes) >= CANDIDATE_POOL_MIN_SIZE:
                logger.info(
                    "[CANDIDATE_POOL][BUILD][SUCCESS] generated=%s saved to DB",
                    len(pool_codes)
                )
                members = [
                    {
                        "code": code,
                        "name": self._code_name_map.get(code, ""),
                        **candidate_pool_scan_only_meta(),
                    }
                    for code in pool_codes
                ]
                return members, f"candidate_pool_autobuilt"
            logger.warning(
                "[CANDIDATE_POOL][BUILD][TOO_SMALL] generated=%s min=%s -> fallback to universe",
                len(pool_codes) if pool_codes else 0,
                CANDIDATE_POOL_MIN_SIZE
            )
        except Exception as exc:
            logger.error(
                "[CANDIDATE_POOL][BUILD][FAIL] err=%s -> fallback to universe",
                exc, exc_info=True
            )
        
        # ✅ STEP 4: 최종 fallback → 가벼운 프리필터 후 제한된 유니버스 사용 (EMERGENCY)
        logger.error(
            "[CANDIDATE_POOL][EMERGENCY] fallback to filtered universe - applying lightweight filter to avoid timeout"
        )
        
        # timeout-safe: 195 전체가 아니라 상위 80~100개만 사용 (최소 프리필터)
        try:
            # ✅ 호환 래퍼: days 파라미터를 count로 변환
            def _ohlcv_wrapper(code: str, days: int = 100):
                df, meta = self._fetch_daily(code, count=days)
                return df
            
            builder = CandidatePoolBuilder(
                ohlcv_provider=_ohlcv_wrapper,
                target_size=min(CANDIDATE_POOL_SIZE, 100),  # 최대 100개로 제한
                min_price=CANDIDATE_POOL_MIN_PRICE,
                liq_days=30,  # 30일로 단축 (timeout 방지)
                min_rows=30,
            )
            emergency_codes = builder.build_light_scan(members=full_members, as_of=today)
            if emergency_codes:
                logger.warning(
                    "[CANDIDATE_POOL][EMERGENCY] filtered universe: %s (from 195)",
                    len(emergency_codes)
                )
                members = [
                    {
                        "code": code,
                        "name": self._code_name_map.get(code, ""),
                        **candidate_pool_scan_only_meta(),
                    }
                    for code in emergency_codes
                ]
                return members, "universe_emergency_filtered"
        except Exception as emergency_exc:
            logger.error(
                "[CANDIDATE_POOL][EMERGENCY][FAIL] emergency filter failed: %s",
                emergency_exc, exc_info=True
            )
        
        # 최종 최후의 수단: 195 전체 (timeout 위험 있음)
        logger.critical(
            "[CANDIDATE_POOL][CRITICAL] all fallbacks failed -> using full 195 universe (TIMEOUT RISK!)"
        )
        return full_members, "universe_emergency_fallback"
    
    def _load_legacy_watchlist_members(self) -> tuple[list[dict], str]:
        """
        기존 워치리스트 로직 (CANDIDATE_POOL_ENABLED=0일 때 사용).
        """
        watchlist_enabled = os.getenv("PB1_WATCHLIST_ENABLED", "1") == "1"
        watchlist_strict = os.getenv("PB1_WATCHLIST_STRICT", "0") == "1"
        
        if not watchlist_enabled:
            logger.info("[PB1][WATCHLIST] disabled -> use full universe")
            members = self._load_universe()
            return members, "universe_full"
        
        today = self._today
        
        if isinstance(today, str):
            today = today.split("T")[0]
            today = date.fromisoformat(today)
        elif isinstance(today, datetime):
            today = today.date()
        
        as_of = week_monday(today)
        
        full_members = self._load_universe()
        
        try:
            watchlist, source = load_today_watchlist_with_fallback(
                engine=self.engine,
                env=self.env,
                strategy=self.strategy,
                today=as_of,
                members=full_members,
                ohlcv_provider=self._fetch_daily,
                minervini_config=self.minervini_config,
            )
        except Exception as exc:
            error_msg = f"[PB1][WATCHLIST][LOAD_FAIL] as_of={as_of} err={exc}"
            logger.exception(error_msg)
            
            if watchlist_strict:
                logger.error("[PB1][WATCHLIST][STRICT] strict=1 -> exit on watchlist failure")
                raise RuntimeError(f"Watchlist load failed in strict mode: {exc}") from exc
            
            logger.warning("[PB1][WATCHLIST] strict=0 -> fallback to full universe on exception")
            return full_members, "universe_fallback_on_error"
        
        if not watchlist:
            warning_msg = f"[PB1][WATCHLIST] empty watchlist as_of={as_of}"
            logger.warning(warning_msg)
            
            if watchlist_strict:
                logger.error("[PB1][WATCHLIST][STRICT] strict=1 -> exit on empty watchlist")
                raise RuntimeError("Watchlist is empty in strict mode")
            
            logger.warning("[PB1][WATCHLIST] strict=0 -> fallback to full universe on empty")
            return full_members, "universe_fallback_empty"
        
        members = [
            {
                "code": w["code"],
                "rank": w.get("rank"),
                "name": self._code_name_map.get(w["code"], ""),
                "meta_json": w.get("meta"),
            }
            for w in watchlist
        ]
        
        logger.info(
            "[PB1][WATCHLIST] size=%s source=%s as_of=%s (today=%s) strict=%s",
            len(members), source, as_of, today, watchlist_strict
        )
        return members, source

    def _pnl_snapshot(self, positions: List[Dict]) -> Dict[str, float]:
        fallback: Dict[str, float] = {p["code"]: p.get("avg_buy_price") or 0.0 for p in positions}
        marks = self._fetch_marks([p["code"] for p in positions], fallback)
        totals: Dict[str, float] = {"market_value": 0.0, "cost": 0.0, "unrealized": 0.0, "realized": 0.0}
        for pos in positions:
            qty = pos.get("qty") or 0
            mark = marks.get(pos["code"]) or self._balance_price_map.get(pos["code"]) or pos.get("avg_buy_price") or 0.0
            market_value = float(mark) * qty
            cost = float(pos.get("total_cost") or 0.0)
            totals["market_value"] += market_value
            totals["cost"] += cost
            totals["realized"] += float(pos.get("realized_pnl") or 0.0)
            totals["unrealized"] += market_value - cost

        summary_mv = None
        for key in EVAL_KEYS:
            summary_mv = self._to_float(self._holdings_summary.get(key))
            if summary_mv is not None:
                break
        summary_cash = None
        for key in CASH_KEYS:
            summary_cash = self._to_float(self._holdings_summary.get(key))
            if summary_cash is not None:
                break
        if not positions and summary_mv is not None:
            totals["market_value"] = summary_mv
            totals["unrealized"] = 0.0
        if summary_cash is not None:
            totals["cash"] = summary_cash

        cost_source = "positions"
        summary_cost = self._extract_holdings_cost([], self._holdings_summary)
        cost_base = self._balance_cost if self._balance_cost is not None else totals["cost"] or summary_cost or summary_mv
        if self._balance_cost is not None:
            cost_source = "kis_balance"
            totals["cost"] = self._balance_cost
        elif summary_cost:
            cost_source = "balance_summary"
            totals["cost"] = summary_cost

        balance_unrealized = None
        for row in self._balance_snapshot.get("output1", []) if self._balance_snapshot else []:
            for key in UNREALIZED_KEYS:
                val = self._to_float(row.get(key))
                if val is not None:
                    balance_unrealized = (balance_unrealized or 0.0) + val
        summary_unrealized = None
        for key in UNREALIZED_KEYS:
            summary_unrealized = self._to_float(self._holdings_summary.get(key))
            if summary_unrealized is not None:
                break
        unrealized_source = "positions"
        if balance_unrealized is not None:
            totals["unrealized"] = balance_unrealized
            unrealized_source = "kis_balance_rows"
        elif summary_unrealized is not None:
            totals["unrealized"] = summary_unrealized
            unrealized_source = "kis_balance_summary"

        return_pct_source = "computed"
        balance_return_pct = None
        for key in RETURN_PCT_KEYS:
            balance_return_pct = self._to_float(self._holdings_summary.get(key))
            if balance_return_pct is not None:
                return_pct_source = f"kis_balance:{key}"
                break

        portfolio_return_pct = balance_return_pct
        if portfolio_return_pct is None:
            if cost_base and cost_base > 0:
                portfolio_return_pct = (totals["market_value"] - cost_base + totals["realized"]) / cost_base * 100
            else:
                self._warn_once("pnl_zero_cost", "[PNL][SNAPSHOT][WARN] zero_or_missing_cost -> return_pct=N/A")

        invested_return_pct = None
        invested_pnl = self._to_float(self._holdings_summary.get("evlu_pfls_smtl_amt"))
        invested_cost = self._to_float(self._holdings_summary.get("pchs_amt_smtl_amt"))
        if invested_pnl is not None and invested_cost and invested_cost > 0:
            invested_return_pct = (invested_pnl / invested_cost) * 100

        total_asset_return_pct = self._to_float(self._holdings_summary.get("asst_icdc_erng_rt"))

        realized_source = "ledger" if totals["realized"] != 0.0 else "none"

        logger.info(
            "[PNL][SNAPSHOT] universe_as_of=%s market_value=%.2f cost=%.2f cost_source=%s unrealized=%.2f unrealized_source=%s realized=%.2f realized_source=%s return_pct=%s return_pct_source=%s invested_return_pct=%s total_asset_return_pct=%s",
            self._universe_as_of or "none",
            totals["market_value"],
            totals["cost"],
            cost_source,
            totals["unrealized"],
            unrealized_source,
            totals["realized"],
            realized_source,
            f"{portfolio_return_pct:.2f}" if portfolio_return_pct is not None else "N/A",
            return_pct_source,
            f"{invested_return_pct:.2f}" if invested_return_pct is not None else "N/A",
            f"{total_asset_return_pct:.2f}" if total_asset_return_pct is not None else "N/A",
        )
        totals["return_pct"] = portfolio_return_pct if portfolio_return_pct is not None else 0.0
        return totals

    def _safe_session_kind(self) -> str:
        """환경변수와 인스턴스 속성에서 session kind를 안전하게 읽는다.
        어떤 환경에서도 예외를 던지지 않으며, 최소 "unknown"을 반환한다."""
        raw = (
            getattr(self, "session_kind", None)
            or getattr(self, "_session_kind", None)
            or getattr(getattr(self, "run_ctx", None), "session_kind", None)
            or os.getenv("PB1_SESSION_KIND")
            or os.getenv("PB1_FORCE_TRADE_SESSION")
            or os.getenv("FORCE_MARKET_WINDOW")
            or getattr(self, "window_name", None)
            or getattr(self, "window", None)
            or getattr(self, "market_window", None)
            or "unknown"
        )

        raw = str(raw or "unknown").strip().lower()

        if raw in {"pm", "close", "trade-pm", "trade-close", "day", "intraday"}:
            return "afternoon"
        if raw in {"afternoon", "trade-afternoon"}:
            return "afternoon"
        if raw in {"am", "morning", "trade-am"}:
            return "am"

        return raw

    def run(self) -> RunResult:
        # IMPORTANT:
        # Do NOT create a separate after-hours / weekend / smoke trading engine.
        # The same intraday trade path must be reused for market hours, after-hours compute-only,
        # and non-trading-day validation. Only order submission gates may differ.
        # 중요:
        # 장중 공통 매매 로직을 훼손하지 않는다.
        # 장마감 후/비거래일 검증도 동일한 trade 경로를 사용하며,
        # 달라질 수 있는 것은 주문 제출 허용 여부뿐이다.

        # ✅ [SESSION_KIND] run() 스코프 내 session_kind 안전 정의 — NameError 방지
        session_kind = self._safe_session_kind()
        logger.info(
            "[PB1][SESSION_KIND][ENGINE] session_kind=%s phase=%s window=%s window_name=%s market_window=%s",
            session_kind,
            getattr(self, "phase", None),
            getattr(self, "window", None),
            getattr(self, "window_name", None),
            getattr(self, "market_window_name", None),
        )

        self._warned_keys.clear()
        self._setup_reason_counter.clear()
        self.current_code = None
        self.top_candidates = []
        self._tick_price_cache = {}
        self._tick_price_cache_hits = 0
        self._tick_price_api_calls = 0
        self._tick_remaining_sec: float | None = None  # [2026-04-30] non-critical DB skip용
        self._tick_start_time: float = time.monotonic()
        logger.info("[PB1][PRICE_CACHE][TICK_INIT]")
        self._data_metrics = {
            "precomputed_hits": 0,
            "short_fetch_count": 0,
            "long_fetch_blocked_count": 0,
            "kis_trade_daily_fetch_count_trade": 0,
            "kis_trade_daily_blocked_count_trade": 0,
            "kis_daily_fetch_count_trade": 0,
            "kis_daily_fetch_blocked_count_trade": 0,
        }
        self.askbid_fail_count = 0  # [PATCH] 회로차단기용 실패 카운트
        engine_asof = self.get_as_of()
        logger.info(
            "[ENGINE][ASOF][VERIFY] as_of=%s trade_date=%s source=%s",
            engine_asof,
            self._trade_date,
            self._as_of_source,
        )
        final_status = "OK"
        final_notes: str | None = None
        engine_dry_run = bool(getattr(self, "dry_run", False))
        engine_intended_live = bool(getattr(self, "intended_live", False))
        engine_phase = getattr(self, "phase", None)
        engine_force_block_live = bool(getattr(self, "force_block_live", False))
        engine_live_trading_enabled = bool(getattr(self, "live_trading_enabled", False))
        engine_run_mode = getattr(self, "run_mode", None)
        engine_compute_only = bool(getattr(self, "compute_only_full_run", False))
        logger.info(
            "[PB1][ENGINE_STATE] dry_run=%s intended_live=%s phase=%s has_flags=%s force_block_live=%s",
            bool(getattr(self, "dry_run", False)),
            bool(getattr(self, "intended_live", False)),
            getattr(self, "phase", None),
            hasattr(self, "flags"),
            bool(getattr(self, "force_block_live", False)),
        )
        logger.info(
            "[PB1][ENGINE_STATE] final30_source=%s as_of=%s rows=%s immutable=%s file_mirror_present=%s",
            getattr(self, "final30_source", "none"),
            engine_asof,
            len(getattr(self, "final30_df", pd.DataFrame()) if getattr(self, "final30_df", None) is not None else pd.DataFrame()),
            int(bool(getattr(self, "final30_locked", False))),
            int(bool((((self._universe_context.meta or {}) if getattr(self, "_universe_context", None) else {}).get("file_mirror_present")))),
        )
        
        # ✅ [GATE] calc_allowed, order_allowed 분리
        calc_allowed = self.calc_allowed  # 계산 허용
        order_allowed = self.order_allowed  # 주문 허용
        minervini_only = self.minervini_only  # MINERVINI_ONLY 모드
        entry_allowed = bool(self.entry_enabled and order_allowed)  # 하위호환용
        entry_reason = self.entry_block_reason or ("entry_disabled" if not entry_allowed else "ok")
        
        # ✅ MINERVINI_ONLY 모드에서는 계산만 허용, 주문은 금지
        if minervini_only:
            calc_allowed = True
            order_allowed = False
            logger.info(
                "[MINERVINI_ONLY] calc_allowed=1 order_allowed=0 (bypass cutoff/window for analytics only)"
            )

        diag_compute_only = (
            (os.getenv("STRATEGY_MODE") or "").strip().upper() == "DIAG"
            and (
                env_bool("FORCE_COMPUTE_ON_CUTOFF", False)
                or env_bool("BYPASS_ENTRY_CUTOFF_COMPUTE_ONLY", False)
                or env_bool("FORCE_COMPUTE_WHEN_CUTOFF", False)
                or env_bool("BYPASS_ENTRY_CUTOFF_FOR_COMPUTE", False)
            )
        )
        compute_only_full_run = (
            (env_bool("DRY_RUN", False) or env_bool("DRYRUN", False) or env_bool("FORCE_BLOCK_LIVE", False) or env_bool("DISABLE_LIVE_TRADING", False))
            and (env_bool("FORCE_COMPUTE_ON_CUTOFF", False) or env_bool("FORCE_COMPUTE_WHEN_CUTOFF", False))
            and (env_bool("BYPASS_ENTRY_CUTOFF_COMPUTE_ONLY", False) or env_bool("BYPASS_ENTRY_CUTOFF_FOR_COMPUTE", False))
        )
        if compute_only_full_run and (self.window_label or "").lower() == "after":
            logger.info("[AFTER_COMPUTE_ONLY][ROUTE] using existing live pipeline path")
        if diag_compute_only:
            order_allowed = False
            logger.info("[DIAG][ENTRY_COMPUTE] enabled=1 submit_orders=0")
        
        # ✅ DIAG_FULL_EXEC: entry gate 우회
        diag_full_exec = bool(getattr(self, "diag_full_exec", False))
        
        # ✅ 서킷 브레이커 체크: EGW002 발생 시 신규진입 중단
        if self.kis and hasattr(self.kis, '_price_cache'):
            from trader.kis_wrapper import _price_cache
            if _price_cache.is_circuit_open():
                logger.warning("[PB1][DEGRADED] price circuit open -> skip new entries this tick")
                order_allowed = False
                entry_reason = "price_circuit_open"
        
        entry_summary_emitted = False
        entry_decision_emitted = False
        entry_decision_result: str | None = None
        entry_decision_reason: str | None = None
        entry_cutoff_dt, entry_cutoff_raw = self._resolve_entry_cutoff()
        entry_phase = self.phase in {"prep", "entry", "pm_entry"}
        max_positions = int(PB1_MAX_POSITIONS)
        target_new_positions_raw = self._int_env("PB1_TARGET_NEW_POSITIONS", PB1_TARGET_NEW_POSITIONS)
        min_order_krw = float(MIN_ORDER_KRW)
        entry_capital_krw = 0.0
        skip_entry_scan = False
        forced_entry_disabled_reason = str(os.getenv("FORCE_ENTRY_DISABLED_REASON") or "").strip()
        if forced_entry_disabled_reason == "PM_LATE_START_NO_NEW_BUY":
            logger.warning("[ENTRY][LEGACY_POLICY_IGNORED] reason=%s action=allow_before_cutoff", forced_entry_disabled_reason)
        elif forced_entry_disabled_reason in {"ENTRY_CUTOFF_PASSED"}:
            calc_allowed = False
            order_allowed = False
            entry_allowed = False
            entry_reason = "ENTRY_CUTOFF_PASSED"
            skip_entry_scan = True
            logger.info("[ENTRY][DISABLED] reason=%s action=skip_entry_scan", forced_entry_disabled_reason)
            logger.info("[EXIT][ENABLED] reason=%s", "entry_cutoff_exit_only")
        if self.preopen_max_new_positions > 0 and (self.window_label or "").lower() == "preopen":
            target_new_positions_raw = min(target_new_positions_raw, self.preopen_max_new_positions)
        if not order_allowed and not minervini_only:
            # ✅ DIAG_FULL_EXEC: entry gate 우회
            if diag_full_exec and entry_reason in ("entry_cutoff", "window_blocked", "phase_manage", "entry_disabled"):
                logger.warning(
                    "[PB1][DIAG_FULL_EXEC] override entry gate reason=%s -> allow entry pipeline (dry_run=%s)",
                    entry_reason,
                    self.dry_run
                )
                order_allowed = True
                calc_allowed = True
                entry_reason = "diag_full_exec_override"
            else:
                logger.warning(
                    "[PB1][ORDER_DISABLED][REASON] order_allowed=0 reason=%s -> skip new orders",
                    entry_reason
                )
                logger.info("[PB1][BUY][SKIP] reason=%s details={'order_allowed': False}", entry_reason)
                # 주문만 차단, 계산은 계속 허용
        if self.phase == "verify":
            if diag_full_exec:
                logger.warning("[PB1][DIAG_FULL_EXEC] override phase=verify -> allow entry")
            else:
                order_allowed = False
                entry_reason = "phase_verify"
                # 주문만 차단, 계산은 계속 허용
        if self.phase in {"manage", "exit", "idle"}:
            if diag_full_exec:
                logger.warning("[PB1][DIAG_FULL_EXEC] override phase=%s -> allow entry", self.phase)
            else:
                order_allowed = False
                entry_reason = f"phase_{self.phase}"
                # 주문만 차단, 계산은 계속 허용
        # ✅ FATAL 가드: intended_live=True인데 dry_run=True면 즉시 종료
        if engine_intended_live and engine_dry_run:
            raise RuntimeError(
                "FATAL: intended_live=True but pb1_engine received dry_run=True. "
                "This would block live orders. Fix dry_run propagation."
            )
        
        logger.info(
            "[PB1][RUN] window=%s window_internal=%s phase=%s dry_run=%s intended_live=%s env=%s",
            self.market_window_name,
            self.window_internal,
            self.phase_name,
            self.dry_run,
            self.intended_live,
            self.env,
        )
        trade_input = (os.getenv("TRADE_INPUT") or "final30").strip().lower() or "final30"
        window_name = (self.market_window_name or self.window_internal or self.window_label or "").strip().lower()
        precomputed_mode_reason = "default"
        force_precomputed_mode = bool(
            self.compute_only_full_run
            or self.force_block_live
            or (not self.trading_day)
            or window_name == "after"
        )
        if force_precomputed_mode:
            precomputed_mode_reason = "locked_final30_compute_only"
        self.trade_precomputed_only = bool(
            self.phase_name in {"entry", "pm_entry"}
            and self.final30_locked
            and bool(self._precomputed_final30_map)
            and self.trade_use_precomputed_features
            and window_name in {"morning", "day", "intraday", "after"}
            and trade_input == "final30"
            and force_precomputed_mode
        )
        logger.info(
            "[PB1][DATA_MODE] precomputed_only=%s phase=%s trade_input=%s window=%s reason=%s",
            self.trade_precomputed_only,
            self.phase_name,
            trade_input,
            window_name,
            precomputed_mode_reason,
        )
        # 타입 검증: orders 테이블의 시간 컬럼 타입 확인
        try:
            cols = {c["name"] for c in inspect(self.engine).get_columns("orders")}
            required = {"created_at","updated_at","submitted_at","acked_at"}
            ok = required.issubset(cols)
            cols_missing = required - cols
            logger.info("[PB1][SCHEMA_CHECK] orders columns check: ok=%s, missing=%s", ok, cols_missing)
        except Exception as e:
            logger.warning("[PB1][SCHEMA_CHECK][FAIL] Failed to check schema via self.engine: %s. Available alternatives: universe_repo.engine=%s, orders_repo.engine=%s", 
                           str(e), hasattr(self.universe_repo, 'engine'), hasattr(self.orders_repo, 'engine'))
        regime = {"regime": "UNKNOWN"}
        risk_mult = float(REGIME_MIN_RISK)
        regime_df, _ = self._fetch_daily(REGIME_INDEX, count=max(REGIME_MA_SLOW + 5, 260))
        if not regime_df.empty:
            regime = get_regime(regime_df["close"], REGIME_MA_FAST, REGIME_MA_SLOW)
            risk_mult = risk_multiplier(
                regime,
                REGIME_MODE,
                max_risk=REGIME_MAX_RISK,
                mid_risk=REGIME_MID_RISK,
                min_risk=REGIME_MIN_RISK,
            )
        self._regime = regime
        self._regime_risk_mult = risk_mult
        if risk_mult <= 0.0 and REGIME_MODE.upper() == "STRICT":
            entry_allowed = False
            entry_reason = "regime_risk_off"
        emit_event(
            as_of=self._today,
            event="PB1_RUN_START",
            env=self.env,
            window=self.window_label,
            phase=self.phase,
            dry_run=self.dry_run,
            entry_enabled=self.entry_enabled,
            entry_block_reason=entry_reason if not entry_allowed else None,
        )

        # Price probe hook for LIVE mode diagnostics
        if os.getenv("PB1_PRICE_PROBE", "0") == "1" and self.phase in {"prep", "entry", "pm_entry"} and not self.dry_run:
            self._run_price_probe()

        # ✅ NEW: decide scan universe
        # Philosophy: if candidate pool exists, it IS the universe for DIAG candidate scan.
        pb1_candidate_only = os.getenv("PB1_CANDIDATE_ONLY", "0") == "1"
        watchlist_enabled = os.getenv("PB1_WATCHLIST_ENABLED", "1") == "1"
        skip_new_entries = os.getenv("PB1_SKIP_NEW_ENTRIES", "0") == "1"
        
        # 유니버스 로드 (호환성 유지, 하지만 스캔에는 안 씀)
        universe_members = self._load_universe()
        universe_count = len(universe_members)
        
        scan_codes = []
        scan_source = "universe"
        watchlist_count = 0
        
        if skip_new_entries:
            logger.error("[ENTRY][BLOCK] missing scored final30 contract")
            logger.warning("[ENTRY][SKIP_NEW] reason=missing_scored_final30")
            scan_codes = []
            scan_source = "blocked_missing_scored_final30"
        elif watchlist_enabled and self.phase in {"prep", "entry", "pm_entry"}:
            logger.info("[PB1][WATCHLIST] enabled -> load today watchlist")
            # Watchlist로 members 대체
            watchlist_members, watchlist_source = self._load_today_watchlist_members()
            logger.info(
                "[PB1][WATCHLIST][RESULT] phase=%s source=%s rows=%s",
                self.phase,
                watchlist_source,
                len(watchlist_members or []),
            )
            watchlist_count = len(watchlist_members)
            logger.info(
                "[PB1][WATCHLIST] loaded=%s source=%s",
                watchlist_count, watchlist_source
            )
            
            # ✅ E-3: candidate_pool_hit이면 universe_count를 watchlist로 교체
            if "candidate_pool_hit" in watchlist_source:
                universe_members = watchlist_members
                universe_count = len(watchlist_members)
                logger.info(
                    "[E-3][CANDIDATE_POOL_HIT] universe_count fixed: %s -> %s",
                    len(self._load_universe()), universe_count
                )
            
            # candidate pool이 있으면 항상 우선 사용
            if watchlist_members:
                if pb1_candidate_only:
                    scan_codes = watchlist_members
                    scan_source = "candidate_pool"
                else:
                    # 일반 모드에서도 watchlist 우선
                    scan_codes = watchlist_members
                    scan_source = "candidate_pool"
            else:
                # watchlist 없으면 universe 사용
                scan_codes = universe_members
                scan_source = "universe"
        else:
            # watchlist disabled: 전체 유니버스 사용
            scan_codes = universe_members
            scan_source = "universe"
        
        # members를 scan_codes로 설정 (하위 로직 호환)
        members = scan_codes
        
        logger.info(
            "[ENTRY][SCAN_UNIVERSE] source=%s scan_count=%s (universe_count=%s watchlist_count=%s)",
            scan_source,
            len(scan_codes),
            universe_count,
            watchlist_count,
        )
        
        if self.phase in {"prep", "entry", "pm_entry"} and self._now_kst >= entry_cutoff_dt:
            force_compute_when_cutoff = (
                env_bool("FORCE_COMPUTE_ON_CUTOFF", False)
                or env_bool("BYPASS_ENTRY_CUTOFF_COMPUTE_ONLY", False)
                or env_bool("FORCE_COMPUTE_WHEN_CUTOFF", False)
                or env_bool("BYPASS_ENTRY_CUTOFF_FOR_COMPUTE", False)
            )
            entry_allowed = False
            order_allowed = False
            entry_reason = "ENTRY_CUTOFF_PASSED"
            logger.info(
                "[PB1][SKIP_ENTRY] reason=ENTRY_CUTOFF_PASSED now=%s cutoff=%s",
                self._now_kst.isoformat(),
                entry_cutoff_dt.isoformat(),
            )
            if force_compute_when_cutoff:
                calc_allowed = True
                logger.info("[PB1][CUTOFF_OVERRIDE] compute_only=1 order_allowed=0")
            if not calc_allowed and self.phase in {"prep", "entry", "pm_entry"}:
                skip_entry_scan = True
                final_status = "SKIPPED"
                final_notes = "ENTRY_CUTOFF_PASSED"

        logger.info(
            "[GATE] compute_allowed=%s order_allowed=%s reason=%s",
            int(calc_allowed),
            int(order_allowed),
            entry_reason or "ok",
        )
        candidate_allowed = bool(calc_allowed and self.phase in {"prep", "entry", "pm_entry"} and not skip_entry_scan)
        rank_allowed = bool(calc_allowed and self.phase in {"prep", "entry", "pm_entry"})
        logger.info(
            "[PB1][GATE] calc_allowed=%s candidate_allowed=%s rank_allowed=%s order_allowed=%s",
            int(calc_allowed),
            int(candidate_allowed),
            int(rank_allowed),
            int(order_allowed),
        )

        if not entry_allowed:
            logger.info("[PB1][ENTRY_BLOCKED] reason=%s entry_allowed=0", entry_reason)

        def _emit_entry_decision(
            result: str,
            *,
            reason: str | None,
            ok_setups: int,
            blocked_by: Counter[str],
            orders: int = 0,
            total_krw: float = 0.0,
        ) -> None:
            nonlocal entry_decision_emitted
            nonlocal entry_decision_result
            nonlocal entry_decision_reason
            if entry_decision_emitted:
                return
            blocked_text = _format_reason_counts(blocked_by)
            if result == "PLACE":
                logger.info(
                    "ENTRY_DECISION result=PLACE orders=%s total_krw=%.0f ok_setups=%s blocked_by=%s",
                    orders,
                    total_krw,
                    ok_setups,
                    blocked_text,
                )
            else:
                logger.info(
                    "ENTRY_DECISION result=SKIP reason=%s ok_setups=%s blocked_by=%s",
                    reason or "UNKNOWN",
                    ok_setups,
                    blocked_text,
                )
            entry_decision_result = result
            entry_decision_reason = reason
            entry_decision_emitted = True

        def _emit_entry_summary(
            setup_ok_codes: list[str] | None,
            orderable_candidates: list[CandidateFeature] | None,
            drop_reason_counter: Counter[str] | None,
        ) -> None:
            nonlocal entry_summary_emitted
            if entry_summary_emitted:
                return
            drop_reason_counter = drop_reason_counter or Counter()
            emit_event(
                as_of=self._today,
                event="PB1_ENTRY_SUMMARY",
                entry_allowed=entry_allowed,
                entry_block_reason=entry_reason,
                setup_ok_count=len(setup_ok_codes or []),
                selected_count=len(orderable_candidates or []),
                top_drop_reasons=drop_reason_counter.most_common(self.drop_reasons_topn),
            )
            entry_summary_emitted = True

        def _write_relax_debug_report(payload: dict[str, Any]) -> Path | None:
            try:
                report_path = runtime_path("runtime", "reports", "minervini", self._today, "relax_debug.json")
                report_path.parent.mkdir(parents=True, exist_ok=True)
                with report_path.open("w", encoding="utf-8") as fp:
                    json.dump(to_jsonable(payload), fp, ensure_ascii=False, indent=2)
                self._touched_files.append(report_path)
                logger.info("[MINERVINI][RELAX_DEBUG][SAVE] path=%s", report_path)
                return report_path
            except Exception as exc:
                logger.warning("[MINERVINI][RELAX_DEBUG][SAVE_FAIL] err=%s", exc)
                return None

        def _set_run_summary_payload(
            *,
            scanned: int,
            setup_ok: int,
            relax_ok: int,
            score_ok: int,
            risk_ok: int,
            sized_ok: int,
            buyable_ok: int,
            order_candidates: int,
            submitted: int,
            blocked_reasons_counter: Counter[str] | dict[str, int] | None,
            no_trade_reason: str | None,
        ) -> None:
            blocked_counter = _summarize_blocked_reasons(blocked_reasons_counter)
            entry_skipped = bool(no_trade_reason in {"PORTFOLIO_FULL", "PHASE_VERIFY", "phase_verify"} or submitted == 0 and scanned == 0 and no_trade_reason)
            payload = {
                "scanned": int(scanned),
                "setup_ok": int(setup_ok),
                "relax_ok": int(relax_ok),
                "score_ok": int(score_ok),
                "risk_ok": int(risk_ok),
                "sized_ok": int(sized_ok),
                "buyable_ok": int(buyable_ok),
                "order_candidates": int(order_candidates),
                "submitted": int(submitted),
                "blocked_reasons_counter": {key: int(value) for key, value in blocked_counter.items()},
                "blocked_by": _format_reason_counts(blocked_counter),
                "no_trade_reason": no_trade_reason,
                "entry_decision_result": entry_decision_result,
                "entry_decision_reason": entry_decision_reason,
                "entry_skipped": entry_skipped,
                "entry_skip_reason": no_trade_reason if entry_skipped else None,
                "existing_positions": existing_positions_count,
                "max_positions": max_positions,
                "slots_remaining": slots_remaining,
                "session_kind": session_kind,
            }
            self._run_summary_payload = payload
            self._debug_summary = {
                "scanned_count": payload["scanned"],
                "setup_ok_count": payload["setup_ok"],
                "after_relax_count": payload["relax_ok"],
                "after_score_cut_count": payload["score_ok"],
                "after_risk_count": payload["risk_ok"],
                "after_sizing_count": payload["sized_ok"],
                "after_buyable_count": payload["buyable_ok"],
                "order_candidate_count": payload["order_candidates"],
                "submit_success_count": payload["submitted"],
                "blocked_reasons_counter": payload["blocked_reasons_counter"],
                "skip_reason_top": payload["no_trade_reason"] or "none",
                "entry_decision_result": payload["entry_decision_result"],
            }

        holdings_snapshot = self._fetch_holdings_snapshot()
        holdings_snapshot, available_cash_krw, cash_meta = self._resolve_holdings_snapshot_with_cash(holdings_snapshot)
        self._balance_snapshot = holdings_snapshot
        holdings_rows = holdings_snapshot.get("output1") or []
        holdings_summary_raw = holdings_snapshot.get("output2")
        holdings_summary = _as_first_dict(holdings_summary_raw)
        self._holdings_summary = holdings_summary
        self._balance_price_map = self._extract_holdings_prices(holdings_rows)
        self._balance_cost = self._extract_holdings_cost(holdings_rows, holdings_summary)
        total_cash_krw = int(available_cash_krw)
        order_possible_cash_krw = int(available_cash_krw)
        if self.kis and entry_phase:
            try:
                cash_summary = self.kis.get_cash_summary()
                total_cash_krw = int(cash_summary.get("total_cash_krw") or 0)
                order_possible_cash_krw = int(cash_summary.get("order_possible_cash_krw") or 0)
            except Exception as exc:
                logger.warning("[PB1][CASH][SUMMARY_FAIL] err=%s", exc)
        if order_possible_cash_krw > 0:
            available_cash_krw = order_possible_cash_krw
        self.order_possible_cash_krw = float(order_possible_cash_krw)
        if PB1_CAPITAL_MODE == "CASH":
            base_cash_krw = int(order_possible_cash_krw or total_cash_krw or available_cash_krw)
        else:
            positive_cash_values = [value for value in (total_cash_krw, order_possible_cash_krw) if int(value or 0) > 0]
            base_cash_krw = min(positive_cash_values) if positive_cash_values else int(available_cash_krw)
        self.entry_base_cash_krw = float(base_cash_krw)
        if entry_phase:
            reserve_pct = min(max(float(PB1_CASH_RESERVE_PCT), 0.0), 1.0)
            override_capital = PB1_ENTRY_CAPITAL_KRW
            entry_capital_krw, entry_usable_krw, capital_meta = self._resolve_entry_capital(
                base_cash_krw=base_cash_krw,
                override_capital=override_capital,
                reserve_pct=reserve_pct,
            )
            self.entry_capital_krw = float(entry_capital_krw)
            self.entry_usable_krw = float(entry_usable_krw)
            self.entry_reserve_krw = max(0.0, float(base_cash_krw) - float(entry_usable_krw))
            budget_pct = min(max(float(PB1_ENTRY_BUDGET_PCT_PER_TICK), 0.0), 1.0)
            tick_budget_krw = int(entry_capital_krw * budget_pct)
            self.entry_tick_budget_krw = float(tick_budget_krw)
            logger.info(
                "[PB1][CAPITAL] mode=%s total_cash=%s order_possible_cash=%s reserve_pct=%.2f usable=%s base_cash=%s override=%s use_override=%s entry_capital=%s cap_limit=%s tick_budget=%s tick_budget_pct=%.2f source=%s",
                PB1_CAPITAL_MODE,
                total_cash_krw,
                order_possible_cash_krw,
                reserve_pct,
                entry_usable_krw,
                base_cash_krw,
                override_capital,
                int(capital_meta.get("use_override") or 0),
                entry_capital_krw,
                capital_meta.get("cap_limit"),
                tick_budget_krw,
                budget_pct,
                cash_meta.get("selected_key") or cash_meta.get("source") or "unknown",
            )
            if tick_budget_krw < min_order_krw:
                logger.warning(
                    "[PB1][BUDGET_PLAN][WARN] tick_budget_below_min_order tick_budget=%s min_order=%s",
                    tick_budget_krw,
                    min_order_krw,
                )
                logger.info(
                    "[PB1][BUY][SKIP] reason=tick_budget_too_small details={'tick_budget': %s, 'min_order': %s}",
                    tick_budget_krw,
                    min_order_krw,
                )
        else:
            self.entry_capital_krw = 0.0
            self.entry_usable_krw = float(available_cash_krw)
            self.entry_tick_budget_krw = 0.0
            self.entry_reserve_krw = 0.0
            tick_budget_krw = 0.0
        logger.info("[PB1][POST_CAPITAL][START] phase=%s window=%s", self.phase, self.window_internal)
        with self._stage_timer("exit.positions_lookup"):
            try:
                pos_cache_key = ("positions", self.env, self.STRATEGY_NAME)
                if pos_cache_key in self._tick_db_cache:
                    logger.info("[PB1][DB_CACHE][HIT] key=%s", pos_cache_key)
                    positions = list(self._tick_db_cache[pos_cache_key])
                else:
                    positions = self.positions_repo.list_positions(self.env, self.STRATEGY_NAME)
                    self._tick_db_cache[pos_cache_key] = list(positions)
            except Exception as _pos_exc:
                _krx_pos = str(os.getenv("PB1_MARKET_SCOPE") or os.getenv("EXCHANGE") or "").upper() in {"KRX", "KR"}
                _pos_fail_open = _krx_pos and os.getenv("PB1_FAIL_OPEN_ON_POSITION_LOOKUP_TIMEOUT", "0") in {"1", "true", "TRUE", "yes", "on"}
                self._bump_warning("db_read_fail_open_count")
                dispose_engine_safely(self.engine, reason=f"positions_lookup:{type(_pos_exc).__name__}")
                self._bump_warning("db_engine_dispose_count")
                # traceback_seen 방지: logger.exception() 대신 logger.error() 사용
                logger.error(
                    "[PB1][EXIT][POSITIONS_LOOKUP][FAIL] env=%s krx=%s fail_open=%s err_type=%s err=%s",
                    self.env,
                    int(bool(_krx_pos)),
                    int(bool(_pos_fail_open)),
                    type(_pos_exc).__name__,
                    _pos_exc,
                )
                if _pos_fail_open:
                    logger.warning("[PB1][EXIT][POSITIONS_LOOKUP][FAIL_OPEN] positions=[] continue_entry=1")
                    positions = []
                else:
                    logger.warning(
                        "[PB1][POSITIONS][FAIL_OPEN] err_type=%s err=%s",
                        type(_pos_exc).__name__,
                        _pos_exc,
                    )
                    positions = []
            else:
                logger.info(
                    "[PB1][EXIT][POSITIONS_LOOKUP][RESULT] rows=%s source=db continue_entry=1",
                    len(positions),
                )
        positions = self._reconcile_positions_from_kis_balance(holdings_rows, positions)
        if not positions and holdings_rows:
            bootstrapped = self.positions_repo.bootstrap_from_kis_holdings(
                env=self.env,
                strategy=self.STRATEGY_NAME,
                sid=1,
                mode=1,
                holdings=holdings_rows,
            )
            logger.info("[PB1][BOOTSTRAP] holdings_count=%s inserted=%s", len(holdings_rows), bootstrapped)
            with self._stage_timer("exit.positions_lookup"):
                try:
                    positions = self.positions_repo.list_positions(self.env, self.STRATEGY_NAME)
                    self._tick_db_cache[("positions", self.env, self.STRATEGY_NAME)] = list(positions)
                except Exception as _pos_exc2:
                    self._bump_warning("db_read_fail_open_count")
                    dispose_engine_safely(self.engine, reason=f"positions_lookup_bootstrap:{type(_pos_exc2).__name__}")
                    self._bump_warning("db_engine_dispose_count")
                    # traceback_seen 방지: logger.error() 사용
                    logger.error(
                        "[PB1][EXIT][POSITIONS_LOOKUP][FAIL_OPEN] bootstrap_retry err_type=%s err=%s",
                        type(_pos_exc2).__name__,
                        _pos_exc2,
                    )
                    positions = []
        cooldown_map: dict[str, str] = {
            str(p.get("code") or "").zfill(6): str(p.get("cooldown_until"))
            for p in positions
            if p.get("cooldown_until")
        }
        logger.info("[PB1][POST_CAPITAL][HOLDINGS_ROWS] rows=%s", len(holdings_rows or []))
        logger.info("[PB1][POST_CAPITAL][EXIT_HOLDINGS_PREP][START]")
        with self._stage_timer_with_timeout(
            "post_capital.exit_holdings_prep",
            self._stage_timeout_sec("PB1_POST_CAPITAL_TIMEOUT_SEC", 20.0),
        ):
            holdings_for_exit, holdings_meta = self.load_effective_holdings_for_exit(
                holdings_rows,
                positions,
                as_of=self.get_as_of(),
                env=self.env,
                intended_live=self.intended_live,
                allow_http=bool(self.kis),
                balance_snapshot=self._balance_snapshot,
            )
        logger.info(
            "[PB1][POST_CAPITAL][EXIT_HOLDINGS_PREP][DONE] holdings=%s source=%s",
            len(holdings_for_exit or []),
            (holdings_meta or {}).get("source"),
        )
        self._exit_holdings_meta = holdings_meta
        positions_for_exit = [holding.to_position_dict() for holding in holdings_for_exit]
        existing_positions = [p for p in positions_for_exit if int(p.get("qty") or 0) > 0]
        existing_positions_count = len(existing_positions)
        positions_cost = sum(float(p.get("total_cost") or 0.0) for p in positions_for_exit)
        self._equity_krw = float(available_cash_krw) + positions_cost
        slots_remaining = max(0, max_positions - existing_positions_count)
        target_new_positions = min(target_new_positions_raw, slots_remaining)
        if slots_remaining > 0:
            target_new_positions = max(1, target_new_positions)
        else:
            target_new_positions = 0
        self.target_new_positions = target_new_positions
        allow_add_to_existing = PB1_ALLOW_ADD_TO_EXISTING
        logger.info(
            "[PB1][POSITION_LIMIT] session=%s max_positions=%s existing_positions=%s slots_remaining=%s "
            "target_new_positions_raw=%s target_new_positions=%s allow_add_to_existing=%s",
            session_kind,
            max_positions,
            existing_positions_count,
            slots_remaining,
            target_new_positions_raw,
            target_new_positions,
            allow_add_to_existing,
        )
        self._log_portfolio_risk_diagnostics(
            existing_positions=existing_positions,
            total_cash_krw=float(total_cash_krw),
            available_cash_krw=float(available_cash_krw),
            target_new_positions=int(target_new_positions),
        )
        logger.info(
            "[PB1][RUN-START] EXISTING_POSITIONS_COUNT=%s PB1_TARGET_NEW_POSITIONS=%s PB1_MAX_POSITIONS=%s PB1_ENTRY_CAPITAL_KRW=%.0f MIN_ORDER_KRW=%.0f ENTRY_CUTOFF_TIME=%s AVAILABLE_CASH_KRW=%s TICK_BUDGET_KRW=%s ADD_TO_EXISTING=%s",
            existing_positions_count,
            target_new_positions,
            max_positions,
            entry_capital_krw,
            min_order_krw,
            entry_cutoff_raw,
            available_cash_krw,
            tick_budget_krw,
            allow_add_to_existing,
        )
        # ATR% 상한 검증 로그 (raw=config에서 읽은 원본, used=가드 후 실제 사용값)
        try:
            atr_pct_max_raw = float(PB1_MAX_ATR_PCT_RAW)
        except (TypeError, ValueError):
            atr_pct_max_raw = float(PB1_MAX_ATR_PCT)
        atr_pct_max_used = float(PB1_MAX_ATR_PCT)
        atr_pct_max_raw_display = atr_pct_max_raw * 100 if atr_pct_max_raw <= 1.0 else atr_pct_max_raw
        logger.info(
            "[PB1][ATR_MAX] raw=%.2f used=%.2f pct=%.2f%%",
            atr_pct_max_raw_display,
            atr_pct_max_used,
            atr_pct_max_used * 100,
        )
        holdings = list(holdings_rows or [])
        if not holdings and self.kis:
            logger.info("[PB1][HOLDINGS] empty_balance_snapshot -> skip extra fetch")
        marks_fallback: Dict[str, float] = {}
        exit_order_submit_allowed = bool(self.trading_day and self.intended_live and not self.dry_run and not self.force_block_live)
        logger.info(
            "[EXIT][EVAL_ALLOWED] nontrading_day=%s order_submit=%s eval=1",
            int(not self.trading_day),
            int(exit_order_submit_allowed),
        )
        
        # ========== EXIT PASS (완전 독립 실행) ==========
        exit_pass_ok = True
        exit_pass_sells = 0
        exit_pass_skipped = 0
        exit_pass_timeout_sec = self._exit_pass_timeout_sec()
        exit_pass_fail_open = self._exit_pass_timeout_fail_open_enabled()
        exit_pass_started = time.perf_counter()
        # exit prewarm: exit_pass 직전에 OHLCV/현재가 캐시를 미리 채운다
        self._prewarm_exit_holdings_data([h.to_dict() if hasattr(h, "to_dict") else (h if isinstance(h, dict) else getattr(h, "__dict__", {})) for h in (holdings_for_exit or [])])
        logger.info(
            "[PASS][EXIT][START] phase=%s existing_positions=%s holdings_source=%s",
            self.phase,
            existing_positions_count,
            holdings_meta.get("source"),
        )
        try:
            logger.info("[PB1][POST_CAPITAL][EXIT_PASS_ENTER]")
            logger.info(
                "[PB1][POST_CAPITAL][EXIT_PASS][START] holdings=%s timeout_sec=%.2f env=%s",
                len(holdings_for_exit or []),
                exit_pass_timeout_sec,
                self.env,
            )
            with self._stage_timer_with_timeout(
                "post_capital.exit_pass",
                exit_pass_timeout_sec,
            ):
                positions_for_exit = self._run_exit_always(holdings_for_exit=holdings_for_exit, marks_fallback=marks_fallback)
            logger.info("[PB1][POST_CAPITAL][EXIT_PASS_DONE] evals=%s", len(getattr(self, "_exit_evaluations", []) or []))
            
            # ========== Exit sells 카운팅 수정 (submitted_count 사용) ==========
            exit_summary_payload = dict(getattr(self, "_exit_summary_payload", {}) or {})
            exit_submitted_count = int(exit_summary_payload.get("submitted") or 0)
            exit_accepted_sells = int(exit_summary_payload.get("accepted_sell_count") or 0)
            exit_fill_confirmed = int(exit_summary_payload.get("fill_confirmed_sell_count") or 0)
            sold_codes_count = len(self._get_sold_codes_today())
            
            # 우선순위: fill > accepted > submitted
            exit_reported_sells = exit_fill_confirmed or exit_accepted_sells or exit_submitted_count
            
            logger.info(
                "[PB1][POST_CAPITAL][EXIT_PASS][DONE] sells=%s sold_codes_count=%s skipped=%s evals=%s elapsed=%.2f",
                exit_reported_sells,
                sold_codes_count,
                exit_pass_skipped,
                len(getattr(self, "_exit_evaluations", []) or []),
                time.perf_counter() - exit_pass_started,
            )
            logger.info(
                "[PASS][EXIT][END] sells=%s sold_codes_count=%s skipped_dup=%s",
                exit_reported_sells,
                sold_codes_count,
                exit_pass_skipped,
            )
        except PB1StageTimeout as exit_timeout:
            exit_pass_ok = False
            elapsed = time.perf_counter() - exit_pass_started
            logger.error(
                "[PB1][POST_CAPITAL][EXIT_PASS][TIMEOUT] env=%s elapsed=%.2f max_sec=%.2f holdings=%s fail_open=%s reason=%s",
                self.env,
                elapsed,
                exit_pass_timeout_sec,
                len(holdings_for_exit or []),
                int(bool(exit_pass_fail_open)),
                exit_timeout,
            )
            self._bump_warning("timeout_count")
            if exit_pass_fail_open:
                final_status = "OK_WITH_WARNINGS"
                final_notes = "EXIT_PASS_TIMEOUT_DEGRADED"
                self._bump_warning("degraded_stage_count")
                logger.warning(
                    "[DEGRADED][PB1][POST_CAPITAL][EXIT_PASS] env=%s action=continue phase=%s",
                    self.env,
                    self.phase,
                )
            else:
                raise
        except Exception as exit_exc:
            exit_pass_ok = False
            logger.exception("[PASS][EXIT][FAIL] err=%s -> continue to ENTRY", exit_exc)
        
        if self.phase == "exit":
            logger.info("[PB1][EXIT] entry_skipped=1")
            self._log_tick_price_cache_summary()
            return self._finalize_run_result(status=final_status, notes=final_notes or "exit_phase")
        logger.info("[PB1][POST_CAPITAL][OPEN_ORDERS_LOOKUP]")
        open_orders = self._safe_get_open_orders()
        if open_orders:
            logger.info("[PB1][ORDERS][OPEN] count=%s", len(open_orders))
        self._pnl_snapshot(self._positions_with_meta(positions_for_exit))
        if self.phase == "verify":
            final_status = "OK"
            final_notes = "verify_only"
            self._log_reason_summary(final_notes)
            _emit_entry_summary([], [], Counter())
            _emit_entry_decision(
                "SKIP",
                reason="PHASE_VERIFY",
                ok_setups=0,
                blocked_by=_normalize_entry_block_reasons(["phase_verify"]),
            )
            _set_run_summary_payload(
                scanned=0,
                setup_ok=0,
                relax_ok=0,
                score_ok=0,
                risk_ok=0,
                sized_ok=0,
                buyable_ok=0,
                order_candidates=0,
                submitted=0,
                blocked_reasons_counter=Counter({"PHASE_VERIFY": 1}),
                no_trade_reason="PHASE_VERIFY",
            )
            self._log_tick_price_cache_summary()
            return self._finalize_run_result(status=final_status, notes=final_notes)

        # ========== 당일 매도 종목 추적 (전체 차단이 아닌 종목별 필터링) ==========
        sold_codes_this_tick = set()
        payload = dict(getattr(self, "_exit_summary_payload", {}) or {})
        accepted_sell_count = int(payload.get("accepted_sell_count") or 0)
        exit_evaluations = getattr(self, "_exit_evaluations", []) or []
        
        # 이번 tick에서 매도된 종목 추출
        for evaluation in exit_evaluations:
            if int(evaluation.get("submitted") or 0) > 0:
                code = str(evaluation.get("code") or "").zfill(6)
                if code and code != "000000":
                    sold_codes_this_tick.add(code)
        
        # 오늘 전체 매도 종목 추출 (당일 재매수 금지)
        sold_codes_today = self._get_sold_codes_today()
        
        logger.info(
            "[ENTRY][AFTER_EXIT][CODE_BLOCK] sold_this_tick=%s sold_today=%s action=block_sold_codes_only",
            len(sold_codes_this_tick),
            len(sold_codes_today),
        )
        if sold_codes_this_tick:
            logger.info("[ENTRY][AFTER_EXIT][THIS_TICK] codes=%s", sorted(sold_codes_this_tick))
        if sold_codes_today:
            logger.info("[ENTRY][AFTER_EXIT][TODAY] codes=%s", sorted(sold_codes_today))

        # ========== ENTRY PASS (EXIT와 완전 독립) ==========
        logger.info("[PB1][POST_CAPITAL][ENTRY_PIPE_ENTER]")

        # Portfolio full guard: existing_positions >= max_positions이면 신규 매수 scan 불필요
        if self.phase in {"prep", "entry", "pm_entry"} and (existing_positions_count >= max_positions or target_new_positions <= 0):
            logger.info(
                "[ENTRY][SKIP_PORTFOLIO_FULL] session=%s existing_positions=%s max_positions=%s "
                "slots_remaining=%s target_new_positions=%s reason=PORTFOLIO_FULL",
                session_kind,
                existing_positions_count,
                max_positions,
                slots_remaining,
                target_new_positions,
            )
            _emit_entry_summary([], [], Counter())
            _emit_entry_decision(
                "SKIP",
                reason="PORTFOLIO_FULL",
                ok_setups=0,
                blocked_by=_normalize_entry_block_reasons(["portfolio_full"]),
            )
            _set_run_summary_payload(
                scanned=0,
                setup_ok=0,
                relax_ok=0,
                score_ok=0,
                risk_ok=0,
                sized_ok=0,
                buyable_ok=0,
                order_candidates=0,
                submitted=0,
                blocked_reasons_counter=Counter({"PORTFOLIO_FULL": 1}),
                no_trade_reason="PORTFOLIO_FULL",
            )
            self._log_tick_price_cache_summary()
            return self._finalize_run_result(status="OK_NO_TRADE", notes="PORTFOLIO_FULL")

        entry_pass_ok = True
        entry_pass_buys = 0
        entry_pass_skipped = 0

        # Entry capacity 로그 (entry pipeline 직전)
        logger.info(
            "[ENTRY][CAPACITY_CHECK] session=%s existing_positions=%s max_positions=%s slots_remaining=%s "
            "target_new_positions=%s tick_budget=%s available_cash=%s",
            session_kind,
            existing_positions_count,
            max_positions,
            slots_remaining,
            target_new_positions,
            tick_budget_krw,
            available_cash_krw,
        )

        # 계측 변수 초기화
        trace_id = f"{self.run_id or 'NORUN'}:{self._today}:{int(time.time() * 1000) % 100000}"
        t0_entry = time.monotonic()
        dt_minervini = 0.0
        dt_pb1_filter = 0.0
        dt_rank_pick = 0.0
        dt_order_build = 0.0
        dt_order_submit = 0.0
        
        # ✅ universe_members는 보유/리포트/정산용으로만 로드
        universe_members = self._load_universe()
        
        # ✅ FIX B: actual_as_of를 universe_context에서 추출 (trade 모드에서 watchlist lock된 as_of)
        # 리포트용 today() 날짜가 아니라, 실제 데이터 기반 as_of를 사용
        if not self.get_as_of():
            raise RuntimeError("ASOF_LOCK_MISSING")
        as_of_final = str(engine_asof)
        reason = "run_ctx_lock"
        if self._universe_context and self._universe_context.as_of_date:
            universe_as_of = str(self._universe_context.as_of_date)
            if universe_as_of != as_of_final:
                logger.error(
                    "[PB1][INVARIANT][VIOLATION] derived_as_of_mismatch start=%s universe=%s",
                    as_of_final,
                    universe_as_of,
                )
                raise RuntimeError("ASOF_INVARIANT_VIOLATION")
        
        logger.info(
            "[PB1][ASOF][CONTEXT] as_of_final=%s reason=%s universe_context_available=%s",
            as_of_final,
            reason,
            bool(self._universe_context),
        )
        
        final30_codes: list[str] = []

        if self.phase == "prep":
            if isinstance(self.final30_df, pd.DataFrame) and not self.final30_df.empty:
                final30_codes = [str(code).zfill(6) for code in self.final30_df.get("code", pd.Series(dtype=str)).tolist() if str(code).strip()]
            if env_bool("DEBUG_EXPORT_RUNTIME", False) and final30_codes:
                logger.warning("[FINAL30][DEBUG_EXPORT_ONLY] as_of=%s count=%s", as_of_final, len(final30_codes))

        if self.phase in {"entry", "pm_entry"}:
            final30_codes = self._load_entry_final30_or_abort(as_of_final)
            trade_input = (os.getenv("TRADE_INPUT") or "final30").strip().lower() or "final30"
            logger.info("[PB1][ENTRY][INPUT] TRADE_INPUT=%s FINAL_LIST_NAME=%s", trade_input, os.getenv("FINAL_LIST_NAME", "final30"))
        
        # ✅ scan_members: 후보군 우선, 단일 함수로 결정 (이후 절대 덮어쓰지 않음)
        watchlist_members = []
        watchlist_reason = ""
        
        if self.phase == "prep":
            watchlist_members, watchlist_reason = self._load_today_watchlist_members()
        
        # ✅ CRITICAL: entry는 final30만 입력으로 사용 (유니버스/후보 재생성 금지)
        if self.phase in {"entry", "pm_entry"}:
            code_market = {
                str(m.get("code") or "").zfill(6): (m.get("market") or "")
                for m in universe_members
                if m.get("code")
            }
            source_meta = (self._universe_context.meta or {}) if self._universe_context else {}
            source_name = str(source_meta.get("source") or "final30")
            scored_rows = list(self._universe_context.members or []) if self._universe_context else []
            has_rank_final30 = int(any("rank_final30" in (row or {}) for row in scored_rows))
            canonical_codes = list(final30_codes)
            if scored_rows:
                if has_rank_final30:
                    scored_rows = sorted(
                        scored_rows,
                        key=lambda x: (float((x or {}).get("rank_final30") or 999999), str((x or {}).get("code") or "")),
                    )
                elif any("score_final" in (row or {}) for row in scored_rows):
                    scored_rows = sorted(
                        scored_rows,
                        key=lambda x: (-float((x or {}).get("score_final") or 0.0), str((x or {}).get("code") or "")),
                    )
                else:
                    scored_rows = sorted(scored_rows, key=lambda x: str((x or {}).get("code") or ""))
                canonical_codes = [str((row or {}).get("code") or "").zfill(6) for row in scored_rows if (row or {}).get("code")]
            canonical_order_match = int(list(final30_codes) == list(canonical_codes))
            logger.info(
                "[ENTRY][SCAN_INPUT][ORDER] source=%s has_rank_final30=%s canonical_order_match=%s",
                source_name,
                has_rank_final30,
                canonical_order_match,
            )
            scan_members = [{"code": c, "market": code_market.get(c, "")} for c in canonical_codes]
            scan_source = "final30"
            if len(final30_codes) > 0 and len(scan_members) != len(final30_codes):
                logger.error(
                    "[PB1][INVARIANT][VIOLATION] final30_rows=%s engine_input_rows=%s",
                    len(final30_codes),
                    len(scan_members),
                )
                raise RuntimeError("FINAL30_INPUT_ROWS_MISMATCH")
        else:
            scan_members, scan_source = self._resolve_scan_members_for_entry(
                universe_members=universe_members,
                watchlist_members=watchlist_members,
                watchlist_reason=watchlist_reason,
            )
        
        # ✅ 디버그 로그: 원인 추적용 (재발 방지)
        logger.info(
            "[DEBUG][SCAN_MEMBERS] source=%s scan_len=%d watchlist_len=%d universe_len=%d",
            scan_source, len(scan_members), len(watchlist_members or []), len(universe_members or [])
        )
        logger.info(
            "[ENTRY][SCAN_INPUT] source=%s count=%s codes=%s",
            scan_source,
            len(scan_members),
            [str(m.get("code") or "").zfill(6) for m in scan_members if m.get("code")],
        )
        
        buy_allowed_now, buy_allowed_reason, runtime_cutoff_dt, _market_close_dt = self._is_buy_allowed_now(self._now_kst)
        if self.phase in {"prep", "entry", "pm_entry"} and not buy_allowed_now:
            logger.warning(
                "[ENTRY][PIPE][SKIP] reason=%s now=%s cutoff=%s",
                buy_allowed_reason,
                self._now_kst.isoformat(),
                runtime_cutoff_dt.isoformat(),
            )
            _emit_entry_summary([], [], Counter({buy_allowed_reason: 1}))
            _emit_entry_decision(
                "SKIP",
                reason=buy_allowed_reason,
                ok_setups=0,
                blocked_by=Counter({buy_allowed_reason: 1}),
            )
            _set_run_summary_payload(
                scanned=len(scan_members),
                setup_ok=0,
                relax_ok=0,
                score_ok=0,
                risk_ok=0,
                sized_ok=0,
                buyable_ok=0,
                order_candidates=0,
                submitted=0,
                blocked_reasons_counter=Counter({buy_allowed_reason: 1}),
                no_trade_reason=buy_allowed_reason,
            )
            self._log_tick_price_cache_summary()
            return self._finalize_run_result(status="OK_EXIT_ONLY_AFTER_CUTOFF", notes=buy_allowed_reason)

        logger.info(
            "[ENTRY][PIPE][START] trace=%s scan_count=%d source=%s slots=%d tick_budget=%.0f entry_allowed=%s reason=%s",
            trace_id,
            len(scan_members),
            scan_source,
            slots_remaining,
            tick_budget_krw,
            entry_allowed,
            buy_allowed_reason if buy_allowed_now else entry_reason,
        )
        logger.info("[PASS][ENTRY][START] phase=%s entry_allowed=%s slots_remaining=%s", self.phase, entry_allowed, slots_remaining)
        
        emit_event(
            as_of=self._today,
            event="PB1_UNIVERSE_STATUS",
            ok=bool(scan_members),
            universe_members=len(universe_members),
            scan_members=len(scan_members),
            scan_source=scan_source,
            universe_as_of=self._universe_as_of,
        )
        if not scan_members:
            note = "scan_members_empty"
            logger.error(
                "[PB1][SCAN][EMPTY] env=%s strategy=%s scan_source=%s scan_count=%s action_required=universe_or_candidate_pool_build",
                self.env,
                self.UNIVERSE_STRATEGY,
                scan_source,
                len(scan_members),
            )
            self._log_reason_summary("scan_members_empty")
            entry_reason = "scan_members_empty"
            entry_allowed = False
            logger.info("[PB1][ENTRY_BLOCKED] reason=%s entry_allowed=0", entry_reason)
            if self.phase in {"prep", "entry", "pm_entry"}:
                _emit_entry_summary([], [], Counter())
                _emit_entry_decision(
                    "SKIP",
                    reason="SCAN_MEMBERS_EMPTY",
                    ok_setups=0,
                    blocked_by=_normalize_entry_block_reasons([entry_reason]),
                )
                self._bump_warning("duplicate_skip_count")
                return self._finalize_run_result(status="SKIPPED", notes=note)
            skip_entry_scan = True

        code_market = {m.get("code"): m.get("market") for m in scan_members}
        candidates: List[CandidateFeature] = []
        selected_tier = "tier1"
        selected_thresholds = self.filter_thresholds
        all_reason_counts: Counter[str] = Counter()
        tiers_tried: list[str] = []
        relax_passes_used = 0
        applied_min_score = float(self.effective_entry_filters.get("min_score_base", PB1_MIN_SCORE_BASE))
        applied_require_both = bool(self.effective_entry_filters.get("require_both_contractions", PB1_REQUIRE_BOTH_CONTRACTIONS))
        setup_ok_codes: list[str] = []
        strict_setup_ok_codes: list[str] = []
        drop_reason_counter: Counter[str] = Counter()
        drop_examples: Dict[str, list[str]] = {}
        after_risk_check_count = 0
        after_buyable_check_count = 0
        after_dedup_count = 0
        buyable_ok_codes: list[str] = []
        buyable_stage_counter: Counter[str] = Counter()
        order_stage_counter: Counter[str] = Counter()
        orderable_candidates: list[CandidateFeature] = []
        submit_attempt_count = 0
        submit_success_count = 0
        minervini_report_path: str | None = None
        buyable_codes: set[str] = set()
        buyable_report: dict[str, Any] = {}
        relax_debug_payload: dict[str, Any] = {
            "trade_date": self._today,
            "phase": self.phase,
            "env": self.env,
            "bootstrap_enabled": int(self.bootstrap_enabled),
            "final30_input_codes": list(final30_codes),
            "pass_codes": {},
            "score_cut_codes": [],
            "risk_gate_codes": [],
            "sizing_ok_codes": [],
            "sizing_fail_items": [],
            "final_orderable_codes": [],
            "drop_reasons_by_code": {},
            "drop_reason_examples": {},
        }
        minervini_rank_only = bool(self._bool_env("PB1_USE_MINERVINI_AS_RANK_ONLY", PB1_USE_MINERVINI_AS_RANK_ONLY))
        minervini_hard_gate = bool(self._bool_env("PB1_MINERVINI_HARD_GATE", PB1_MINERVINI_HARD_GATE))
        emergency_order_enabled = bool(self._bool_env("PB1_EMERGENCY_ORDER_ENABLED", PB1_EMERGENCY_ORDER_ENABLED))
        emergency_diag_only = bool(self._bool_env("PB1_EMERGENCY_DIAG_ONLY", PB1_EMERGENCY_DIAG_ONLY))
        if self.phase in {"prep", "entry", "pm_entry"} and not skip_entry_scan:
            if self.phase in {"entry", "pm_entry"}:
                t_pb1_start = time.monotonic()
                scan_input_codes = [str(m.get("code") or "").zfill(6) for m in scan_members if m.get("code")]
                candidates = self._compute_candidates_from_codes(scan_input_codes)
                self._reject_summary_candidates = list(candidates or [])
                dt_pb1_filter = time.monotonic() - t_pb1_start
                dt_minervini = 0.0
                data_ok_count = len([c for c in candidates if bool(c.features.get("data_ok"))])
                logger.info(
                    "[ENTRY][CANDIDATES] trace=%s start scan_count=%s source=%s data_ok=%s dt_minervini=%.2f",
                    trace_id,
                    len(scan_members),
                    scan_source,
                    data_ok_count,
                    dt_minervini,
                )
                logger.info(
                    "[ENTRY][PB1_FILTER][LOOSE] trace=%s before=%s after=%s dt=%.2f tier=%s relax_passes=%s",
                    trace_id,
                    data_ok_count,
                    len([c for c in candidates if bool(c.features.get("data_ok")) and bool(c.features.get("setup_loose_ok", c.setup_ok))]),
                    dt_pb1_filter,
                    "final30_pb1",
                    0,
                )
                strict_setup_ok_codes = [c.code for c in candidates if bool(c.features.get("data_ok")) and bool(c.features.get("setup_strict_ok", False))]
                logger.info(
                    "[ENTRY][PB1_FILTER][STRICT] trace=%s before=%s after=%s",
                    trace_id,
                    len([c for c in candidates if bool(c.features.get("data_ok")) and bool(c.features.get("setup_loose_ok", c.setup_ok))]),
                    len(strict_setup_ok_codes),
                )
                setup_ok_codes = [c.code for c in candidates if bool(c.features.get("data_ok")) and bool(c.features.get("setup_loose_ok", c.setup_ok))]
                logger.info("[ENTRY][SETUP_OK] count=%s codes=%s", len(setup_ok_codes), setup_ok_codes)
                logger.info("[ENTRY][SOURCE_POLICY] authoritative=pb1_engine scanner=diagnostic_only minervini=diagnostic_only")

                # === 한국장 PB1 consistency check: raw signal vs pb1 filter ===
                pb1_filter_setup_ok_count = len(setup_ok_codes)
                data_ok_candidates = [c for c in candidates if bool(c.features.get("data_ok"))]
                
                # raw_signal_setup_ok는 data_ok인 후보 중에서 setup fail이 아닌 것으로 근사
                # (prep에서 계산된 setup_ok는 final30에 저장되어 있지만, 여기서는 근사치 사용)
                raw_signal_setup_ok_count = len([c for c in data_ok_candidates if not any(
                    r in (c.reasons or []) 
                    for r in ["missing_ma", "volume_missing", "pullback_missing", "ma20_slope_missing"]
                )])

                if raw_signal_setup_ok_count > 0 and pb1_filter_setup_ok_count == 0:
                    mismatch_count = raw_signal_setup_ok_count
                    logger.warning(
                        "[CONSISTENCY][RAW_VS_PB1][SUMMARY] raw_ok=%s pb1_ok=%s mismatch=%s",
                        raw_signal_setup_ok_count,
                        pb1_filter_setup_ok_count,
                        mismatch_count,
                    )
                    
                    # 상위 10개 종목에 대해 상세 diff 로그 출력
                    mismatch_candidates = [c for c in data_ok_candidates if c.code not in setup_ok_codes][:10]
                    for mc in mismatch_candidates:
                        feat = mc.features
                        reasons_str = ", ".join(mc.reasons or [])
                        logger.warning(
                            "[CONSISTENCY][RAW_VS_PB1][DETAIL] code=%s raw_ok=1 pb1_ok=0 "
                            "reasons=[%s] vol=%.2f volu=%.2f close=%.0f ma20=%.0f ma50=%.0f "
                            "ma20_slope=%.4f rs_pct=%.1f atr_pct=%.3f",
                            mc.code,
                            reasons_str,
                            float(feat.get("vol_contraction") or 0),
                            float(feat.get("volu_contraction") or 0),
                            float(feat.get("close") or 0),
                            float(feat.get("ma20") or 0),
                            float(feat.get("ma50") or 0),
                            float(feat.get("ma20_slope") or 0),
                            float(feat.get("rs_percentile") or 0),
                            float(feat.get("atr_pct") or 0),
                        )

                # === PB1 relax pass: near-miss 후보 복구 ===
                pb1_near_miss_candidates: list[CandidateFeature] = []
                pb1_relax_passes_executed = 0
                
                if pb1_filter_setup_ok_count == 0 and self.bootstrap_enabled:
                    logger.info(
                        "[PB1][RELAX_PASS][START] reason=empty_after_pb1_filter input=%s",
                        len(data_ok_candidates),
                    )
                    
                    # Pass 1: vol_contraction_fail 단독
                    for c in data_ok_candidates:
                        if _is_kr_stock_code(c.code):
                            reasons_set = set(c.reasons or [])
                            # vol_contraction_fail 단독 (soft 제외)
                            hard_reasons = [r for r in reasons_set if not str(r).startswith("soft:")]
                            if hard_reasons == ["vol_contraction_fail"]:
                                near_ok, near_reasons = classify_pb1_near_miss(
                                    c.features,
                                    list(reasons_set),
                                    market=c.market,
                                    rs_percentile=c.features.get("rs_percentile"),
                                    atr_max_pct=0.10,
                                )
                                if near_ok:
                                    pb1_near_miss_candidates.append(c)
                                    logger.info(
                                        "[PB1][RELAX_PASS][CANDIDATE] pass=1 code=%s reasons=%s action=near_miss",
                                        c.code,
                                        hard_reasons,
                                    )
                    
                    pb1_relax_passes_executed = 1
                    if len(pb1_near_miss_candidates) > 0:
                        logger.info(
                            "[PB1][RELAX_PASS][RESULT] pass=1 selected=%s",
                            len(pb1_near_miss_candidates),
                        )
                    
                    # Pass 2: vol + volu contraction fail 둘 다
                    if len(pb1_near_miss_candidates) == 0:
                        for c in data_ok_candidates:
                            if _is_kr_stock_code(c.code):
                                reasons_set = set(c.reasons or [])
                                hard_reasons = [r for r in reasons_set if not str(r).startswith("soft:")]
                                contraction_only = all(
                                    r in {"vol_contraction_fail", "volu_contraction_fail"}
                                    for r in hard_reasons
                                )
                                if contraction_only and len(hard_reasons) > 0:
                                    near_ok, near_reasons = classify_pb1_near_miss(
                                        c.features,
                                        list(reasons_set),
                                        market=c.market,
                                        rs_percentile=c.features.get("rs_percentile"),
                                        atr_max_pct=0.10,
                                    )
                                    if near_ok and float(c.features.get("rs_percentile") or 0) >= 80:
                                        pb1_near_miss_candidates.append(c)
                                        logger.info(
                                            "[PB1][RELAX_PASS][CANDIDATE] pass=2 code=%s reasons=%s action=near_miss",
                                            c.code,
                                            hard_reasons,
                                        )
                        
                        pb1_relax_passes_executed = 2
                        if len(pb1_near_miss_candidates) > 0:
                            logger.info(
                                "[PB1][RELAX_PASS][RESULT] pass=2 selected=%s",
                                len(pb1_near_miss_candidates),
                            )
                    
                    # Near-miss 후보를 setup_ok로 복구
                    for c in pb1_near_miss_candidates:
                        c.setup_ok = True
                        c.features["setup_loose_ok"] = True
                        c.features["pb1_near_miss_recovered"] = True
                        if c.code not in setup_ok_codes:
                            setup_ok_codes.append(c.code)
                    
                    logger.info(
                        "[PB1][RELAX_PASS][FINAL] selected=%s relax_passes=%s",
                        len(pb1_near_miss_candidates),
                        pb1_relax_passes_executed,
                    )

                minervini_input_codes = list(setup_ok_codes) or [c.code for c in candidates if bool(c.features.get("data_ok"))]
                t_minervini_start = time.monotonic()
                signals = compute_minervini_signals(
                    self,
                    as_of=as_of_final,
                    symbols=minervini_input_codes,
                    benchmark=str(os.getenv("RS_BENCHMARK", "229200")),
                )
                signals["final30"] = list(scan_input_codes)
                relax_min_buyable = int(self.effective_entry_filters.get("min_buyable", 1))
                relax_passes = int(self.effective_entry_filters.get("relax_passes", 3))
                relax_rs_step = int(os.getenv("RS_PCTILE_STEP", "5") or 5)
                relax_vcp_step = int(os.getenv("VCP_SCORE_STEP", "5") or 5)
                relax_keep_trend = (os.getenv("KEEP_TREND_TEMPLATE_ALWAYS", "1") == "1")
                logger.info(
                    "[MINERVINI][CFG] min_buyable=%s rank_only=%s hard_gate=%s",
                    relax_min_buyable,
                    int(minervini_rank_only),
                    int(minervini_hard_gate),
                )
                if self.bootstrap_enabled:
                    relax_min_buyable = int(self.effective_entry_filters.get("min_buyable", 1))
                    relax_passes = int(self.effective_entry_filters.get("relax_passes", 5))
                    relax_rs_step = 5
                    relax_vcp_step = 5
                    relax_keep_trend = bool(BOOTSTRAP_KEEP_TREND_TEMPLATE_ALWAYS)
                    logger.warning(
                        "[PB1][BOOTSTRAP][MINERVINI] enabled=1 min_buyable=%s relax_passes=%s rs_step=%s vcp_step=%s keep_trend=%s rs_min=%s vcp_min=%s",
                        relax_min_buyable,
                        relax_passes,
                        relax_rs_step,
                        relax_vcp_step,
                        int(relax_keep_trend),
                        BOOTSTRAP_MINERVINI_RS_MIN_PCTILE,
                        BOOTSTRAP_MINERVINI_VCP_MIN_SCORE,
                    )
                buyable_codes_list, buyable_report = select_buyable_with_relax(
                    signals=signals,
                    min_buyable=relax_min_buyable,
                    relax_passes=relax_passes,
                    rs_step=relax_rs_step,
                    vcp_step=relax_vcp_step,
                    keep_trend=relax_keep_trend,
                )
                dt_minervini = time.monotonic() - t_minervini_start
                buyable_codes = set(buyable_codes_list)
                signal_item_map = {
                    str(item.get("code") or "").zfill(6): item
                    for item in (signals.get("items") or [])
                    if item.get("code")
                }

                pass_counts = buyable_report.get("pass_counts") or {}
                logger.info(
                    "[MINERVINI][RELAX] target=%s pass0=%s pass1=%s pass2=%s pass3=%s used=%s rs_cut=%s vcp_cut=%s",
                    relax_min_buyable,
                    pass_counts.get("pass0", 0),
                    pass_counts.get("pass1", 0),
                    pass_counts.get("pass2", 0),
                    pass_counts.get("pass3", 0),
                    buyable_report.get("relax_level_used"),
                    buyable_report.get("rs_cut_used"),
                    buyable_report.get("vcp_cut_used"),
                )
                pass_codes = buyable_report.get("pass_codes") or {}
                ordered_pass_keys = sorted(
                    pass_codes.keys(),
                    key=lambda key: int(str(key).replace("pass", "") or 0),
                )
                for key in ordered_pass_keys:
                    pass_idx = key.replace("pass", "")
                    logger.info("[MINERVINI][RELAX][PASS%s] count=%s", pass_idx, len(pass_codes.get(key, [])))
                    logger.info("[MINERVINI][RELAX][PASS%s][CODES] codes=%s", pass_idx, pass_codes.get(key, []))
                    logger.info(
                        "[MINERVINI][RELAX][CODES] %s count=%s codes=%s",
                        key,
                        len(pass_codes.get(key, [])),
                        pass_codes.get(key, []),
                    )
                logger.info(
                    "[MINERVINI][RELAX][FINAL] used=%s rs_cut=%s vcp_cut=%s count=%s codes=%s",
                    buyable_report.get("relax_level_used"),
                    buyable_report.get("rs_cut_used"),
                    buyable_report.get("vcp_cut_used"),
                    len(buyable_report.get("final_buyable_codes", [])),
                    buyable_report.get("final_buyable_codes", []),
                )

                # [2026-05-18] KR rescue source 로그: minervini relax codes를 rescue source로 표기
                if self._is_kr_equity_context() and PB1_KR_ENABLE_RESCUE_CANDIDATES and buyable_codes:
                    logger.info(
                        "[MINERVINI][KR_RESCUE_SOURCE][CODES] count=%s codes=%s"
                        " note=not_order_candidate_until_risk_sizing_buyable_pass",
                        len(buyable_codes),
                        sorted(buyable_codes)[:20],
                    )
                
                # === Minervini relax bridge: PB1 setup 0개일 때 Minervini relax 후보 연결 ===
                minervini_bridge_candidates: list[CandidateFeature] = []
                pb1_setup_ok_before_bridge = len(setup_ok_codes)
                
                if pb1_setup_ok_before_bridge == 0 and len(buyable_codes) > 0:
                    if minervini_rank_only and not minervini_hard_gate:
                        logger.info(
                            "[MINERVINI][BRIDGE][START] pb1_setup_ok=%s minervini_relax_count=%s",
                            pb1_setup_ok_before_bridge,
                            len(buyable_codes),
                        )
                        
                        for c in candidates:
                            if c.code in buyable_codes and _is_kr_stock_code(c.code):
                                # Minervini relax 후보를 bridge로 표시 (setup_ok는 여전히 False)
                                c.features["setup_source"] = "minervini_relax_bridge"
                                c.features["minervini_bridge_candidate"] = True
                                minervini_bridge_candidates.append(c)
                                logger.info(
                                    "[MINERVINI][BRIDGE][CANDIDATE] code=%s source=minervini_relax_bridge",
                                    c.code,
                                )
                        
                        logger.info(
                            "[MINERVINI][BRIDGE][DONE] bridged=%s note=bridge_candidates_will_go_through_risk_sizing_buyable_gates",
                            len(minervini_bridge_candidates),
                        )

                relax_debug_payload["pass_codes"] = pass_codes
                relax_debug_payload["final_buyable_codes"] = list(buyable_report.get("final_buyable_codes", []))
                relax_debug_payload["relax_level_used"] = buyable_report.get("relax_level_used")
                relax_debug_payload["rs_cut_used"] = buyable_report.get("rs_cut_used")
                relax_debug_payload["vcp_cut_used"] = buyable_report.get("vcp_cut_used")
                relax_debug_payload["base_rs"] = buyable_report.get("base_rs")
                relax_debug_payload["base_vcp"] = buyable_report.get("base_vcp")

                _scanner_ctx_for_bridge = getattr(self, "scanner_context", {}) or {}
                bridge_candidates = self._activate_relax_bridge_candidates(
                    candidates,
                    setup_ok_count=len([c for c in candidates if c.setup_ok]),
                    scanner_passed_codes={str(x).zfill(6) for x in (_scanner_ctx_for_bridge.get("scanner_passed_codes", []) or [])},
                    minervini_passed_codes={str(x).zfill(6) for x in (buyable_codes or set())},
                    order_allowed=bool(order_allowed),
                )
                if bridge_candidates:
                    for _bc in bridge_candidates:
                        if _bc.code not in setup_ok_codes:
                            setup_ok_codes.append(_bc.code)
                    logger.info(
                        "[ENTRY][RELAX_BRIDGE][ORDERABLE] count=%s codes=%s note=pre_risk_sizing_buyable",
                        0,
                        [],
                    )

                minervini_path = write_minervini_signals(
                    base_dir=Path("bot_state"),
                    env=self.env,
                    run_id=str(self.run_id),
                    signals=signals,
                    buyable_report=buyable_report,
                )
                self._touched_files.append(minervini_path)
                logger.info("[MINERVINI][STORE] path=%s", minervini_path)

                for cf in candidates:
                    cf.features["minervini_buyable"] = cf.code in buyable_codes
                    signal_item = signal_item_map.get(cf.code) or {}
                    if signal_item.get("pivot") is not None:
                        cf.features["pivot"] = signal_item.get("pivot")
                    if (not minervini_rank_only) and minervini_hard_gate and cf.setup_ok and cf.code not in buyable_codes:
                        cf.setup_ok = False
                        cf.reasons = list(cf.reasons or []) + ["minervini_not_buyable"]

                if minervini_rank_only or not minervini_hard_gate:
                    minervini_ranked_count = len([
                        item for item in (signals.get("items") or [])
                        if bool(item.get("data_ok"))
                    ])
                    logger.info(
                        "[MINERVINI][RANK_ONLY] input=%s ranked=%s",
                        len(minervini_input_codes),
                        minervini_ranked_count,
                    )

                # === [2026-05-18] KR 전용 adaptive rescue path ===
                # _is_kr_equity_context()==True이고 setup_ok==0인 경우만 실행
                kr_rescue_applied = False
                if self._is_kr_equity_context() and PB1_KR_ENABLE_RESCUE_CANDIDATES:
                    current_setup_ok = [c for c in candidates if c.setup_ok]
                    if len(current_setup_ok) == 0:
                        # filter reason 통계 수집
                        _reason_counts: dict[str, int] = {}
                        for _c in candidates:
                            for _r in (_c.reasons or []):
                                _reason_counts[_r] = _reason_counts.get(_r, 0) + 1

                        _scanned = len([c for c in candidates if c.features.get("data_ok")])

                        # Market stress 판정
                        _scanner_ctx = getattr(self, "scanner_context", {}) or {}
                        _scanner_passed = len(_scanner_ctx.get("scanner_passed_codes", []))
                        kr_stress, kr_stress_detail = self._detect_kr_market_stress_from_filter_stats(
                            scanned=_scanned,
                            reason_counts=_reason_counts,
                            raw_signal_setup_ok=raw_signal_setup_ok_count,
                            scanner_passed=_scanner_passed,
                        )

                        # stress guard: 최대 허용 포지션 수 적용
                        _rescue_budget = (
                            PB1_KR_STRESS_MAX_NEW_POSITIONS
                            if kr_stress and PB1_KR_MARKET_STRESS_GUARD
                            else PB1_KR_RESCUE_TOPN
                        )

                        if PB1_KR_LOG_FILTER_MATRIX:
                            logger.info(
                                "[PB1][KR_FILTER_MATRIX][SUMMARY] scanned=%s vol_fail=%s ma20_fail=%s stress=%s rescue_budget=%s",
                                _scanned,
                                _reason_counts.get("vol_contraction_fail", 0),
                                _reason_counts.get("close_below_ma20", 0),
                                int(kr_stress),
                                _rescue_budget,
                            )

                        _scanner_passed_codes = set(_scanner_ctx.get("scanner_passed_codes", []))
                        _minervini_buyable_codes = buyable_codes

                        rescued_candidates = self._build_kr_rescue_candidates(
                            candidates,
                            scanner_passed_codes=_scanner_passed_codes,
                            minervini_buyable_codes=_minervini_buyable_codes,
                            market_stress=kr_stress,
                        )

                        if rescued_candidates:
                            for _rc in rescued_candidates:
                                if _rc not in candidates:
                                    candidates = list(candidates) + [_rc]
                                else:
                                    # 이미 있으면 setup_ok만 갱신
                                    _rc.setup_ok = True
                            kr_rescue_applied = True

                            if PB1_KR_LOG_RESCUE_DECISION:
                                logger.info(
                                    "[PB1][KR_RESCUE][APPLIED] count=%s stress=%s codes=%s",
                                    len(rescued_candidates),
                                    int(kr_stress),
                                    [getattr(c, "code", None) for c in rescued_candidates],
                                )
                        else:
                            logger.info(
                                "[PB1][KR_NO_TRADE_EXPLAIN] setup_ok=0 rescue=0 "
                                "reason=no_qualified_rescue_candidates "
                                "scanned=%s vol_fail_ratio=%.2f stress=%s scanner_passed=%s minervini_buyable=%s",
                                _scanned,
                                float(_reason_counts.get("vol_contraction_fail", 0)) / max(1, _scanned),
                                int(kr_stress),
                                len(_scanner_passed_codes),
                                len(_minervini_buyable_codes),
                            )

                candidates = self._size_positions(candidates)
                relax_debug_payload["score_cut_codes"] = list(self._debug_score_cut_codes)
                relax_debug_payload["risk_gate_codes"] = list(self._debug_risk_ok_codes)
                relax_debug_payload["sizing_ok_codes"] = list(self._debug_sizing_ok_codes)
                relax_debug_payload["sizing_fail_items"] = list(self._debug_sizing_fail_items)
                if minervini_rank_only or not minervini_hard_gate:
                    ok_after_risk = sorted(
                        [c for c in candidates if c.setup_ok],
                        key=lambda c: (
                            1 if c.code in buyable_codes else 0,
                            float(c.features.get("score") or 0.0),
                        ),
                        reverse=True,
                    )
                else:
                    ok_after_risk = sorted(
                        [c for c in candidates if c.setup_ok and c.code in buyable_codes],
                        key=lambda c: float(c.features.get("score") or 0.0),
                        reverse=True,
                    )
            else:
                t_minervini_start = time.monotonic()
                candidates = self._compute_candidates(scan_members)
                dt_minervini = time.monotonic() - t_minervini_start
                data_ok_count = len([cf for cf in candidates if cf.features.get("data_ok")])
                logger.info(
                    "[ENTRY][CANDIDATES] trace=%s start scan_count=%s source=%s data_ok=%s dt_minervini=%.2f",
                    trace_id,
                    len(scan_members),
                    scan_source,
                    data_ok_count,
                    dt_minervini,
                )

                # ✅ [HEDGE_FUND] Entry Signals 적용 (breakout/pullback/momentum)
                entry_signals_enabled = os.getenv("ENTRY_SIGNALS_ENABLED", "1") == "1"
                if entry_signals_enabled and candidates:
                    logger.info("[ENTRY_SCAN] start")
                    t_entry_start = time.monotonic()
                    breakout_count = 0
                    pullback_count = 0
                    momentum_count = 0
                    no_signal_count = 0
                    precomputed_style_counts: Counter[str] = Counter()
                    scanner_style_counts: Counter[str] = Counter()
                    
                    # OHLCV 데이터 필요량 (signals 체크용)
                    signal_ohlcv_days = int(os.getenv("ENTRY_SIGNAL_OHLCV_DAYS", "260"))
                    if self.trade_precomputed_only and signal_ohlcv_days > 60:
                        logger.warning(
                            "[ENTRY_SCAN][LONG_OHLCV_BLOCKED] code=ALL requested_days=%s source=trade_precomputed_only",
                            signal_ohlcv_days,
                        )
                        self._metric_add("long_fetch_blocked_count", 1)
                        signal_ohlcv_days = 60
                    
                    for cf in candidates:
                        if not cf.features.get("data_ok"):
                            continue
                        precomputed_style = str(
                            cf.features.get("entry_style_selected")
                            or cf.features.get("entry_style")
                            or ""
                        ).strip().upper()
                        if precomputed_style:
                            precomputed_style_counts[precomputed_style] += 1
                        
                        # OHLCV 데이터 가져오기
                        try:
                            df, meta = self._fetch_daily(cf.code, days=signal_ohlcv_days)
                            if df is None or df.empty or len(df) < 51:
                                cf.features["entry_signal"] = "no_data"
                                no_signal_count += 1
                                continue
                            
                            # Entry signal 체크 (우선순위: breakout > pullback > momentum)
                            if breakout_signal(df):
                                cf.features["entry_signal"] = "breakout"
                                breakout_count += 1
                                scanner_style_counts["BREAKOUT"] += 1
                                if precomputed_style and precomputed_style != "BREAKOUT":
                                    logger.info(
                                        "[ENTRY_SCAN][STYLE_SOURCE] code=%s precomputed_entry_style=%s scanner_strategy=BREAKOUT reason=signal_scan_priority",
                                        cf.code,
                                        precomputed_style,
                                    )
                            elif pullback_signal(df):
                                cf.features["entry_signal"] = "pullback"
                                pullback_count += 1
                                scanner_style_counts["PULLBACK"] += 1
                                if precomputed_style and precomputed_style != "PULLBACK":
                                    logger.info(
                                        "[ENTRY_SCAN][STYLE_SOURCE] code=%s precomputed_entry_style=%s scanner_strategy=PULLBACK reason=signal_scan_priority",
                                        cf.code,
                                        precomputed_style,
                                    )
                            elif momentum_signal(df):
                                cf.features["entry_signal"] = "momentum"
                                momentum_count += 1
                                scanner_style_counts["MOMENTUM"] += 1
                                if precomputed_style and precomputed_style != "MOMENTUM":
                                    logger.info(
                                        "[ENTRY_SCAN][STYLE_SOURCE] code=%s precomputed_entry_style=%s scanner_strategy=MOMENTUM reason=signal_scan_priority",
                                        cf.code,
                                        precomputed_style,
                                    )
                            else:
                                cf.features["entry_signal"] = "none"
                                no_signal_count += 1
                        
                        except Exception as exc:
                            logger.debug("[ENTRY][SIGNAL][ERROR] code=%s error=%s", cf.code, exc)
                            cf.features["entry_signal"] = "error"
                            no_signal_count += 1
                    
                    dt_entry_signals = time.monotonic() - t_entry_start
                    
                    logger.info(
                        "[ENTRY][SIGNAL] breakout=%s pullback=%s momentum=%s no_signal=%s dt=%.2f",
                        breakout_count,
                        pullback_count,
                        momentum_count,
                        no_signal_count,
                        dt_entry_signals,
                    )
                    filter_no_signal = os.getenv("ENTRY_SIGNAL_REQUIRED", "0") == "1"
                    scanner_pass_count = breakout_count + pullback_count + momentum_count
                    if filter_no_signal:
                        logger.info(
                            "[ENTRY][SCANNER][GATE] passed=%s no_signal=%s required=1",
                            scanner_pass_count,
                            no_signal_count,
                        )
                    else:
                        logger.info(
                            "[ENTRY][SCANNER][DIAG_ONLY] passed=%s no_signal=%s required=0 note=scanner_is_not_authoritative_order_gate",
                            scanner_pass_count,
                            no_signal_count,
                        )
                    logger.info("[ENTRY_SCAN] breakout signals=%s", breakout_count)
                    logger.info("[ENTRY_SCAN] pullback signals=%s", pullback_count)
                    logger.info("[ENTRY_SCAN] momentum signals=%s", momentum_count)
                    logger.info(
                        "[ENTRY_SCAN][STYLE_COMPARE] precomputed=%s scanner=%s",
                        dict(precomputed_style_counts),
                        dict(scanner_style_counts),
                    )
                    
                    # Entry signal이 없는 종목 필터링 (옵션)
                    if filter_no_signal:
                        before_filter = len([c for c in candidates if c.features.get("data_ok")])
                        for cf in candidates:
                            signal = cf.features.get("entry_signal")
                            if signal in ("none", "no_data", "error"):
                                if cf.setup_ok:
                                    cf.setup_ok = False
                                    cf.reasons = list(cf.reasons or []) + ["no_entry_signal"]
                        after_filter = len([c for c in candidates if c.setup_ok])
                        logger.info(
                            "[ENTRY][SIGNAL][FILTER] before=%s after=%s filtered=%s",
                            before_filter,
                            after_filter,
                            before_filter - after_filter,
                        )
                else:
                    logger.debug("[ENTRY][SIGNAL] disabled (ENTRY_SIGNALS_ENABLED=%s)", os.getenv("ENTRY_SIGNALS_ENABLED", "1"))

                t_pb1_start = time.monotonic()
                (
                    candidates,
                    selected_tier,
                    selected_thresholds,
                    all_reason_counts,
                    tiers_tried,
                    relax_passes_used,
                    applied_min_score,
                    applied_require_both,
                ) = self._select_candidates_with_fallback(candidates)
                dt_pb1_filter = time.monotonic() - t_pb1_start

                logger.info(
                    "[ENTRY][PB1_FILTER] trace=%s before=%s after=%s dt=%.2f tier=%s relax_passes=%s",
                    trace_id,
                    data_ok_count,
                    len([c for c in candidates if c.setup_ok]),
                    dt_pb1_filter,
                    selected_tier,
                    relax_passes_used,
                )
                if not any(c.setup_ok for c in candidates):
                    self._apply_score_fallback(candidates)
                setup_ok_codes = [c.code for c in candidates if c.setup_ok]
                logger.info("[ENTRY][SETUP_OK] count=%s codes=%s", len(setup_ok_codes), setup_ok_codes)
                candidates = self._size_positions(candidates)
                relax_debug_payload["score_cut_codes"] = list(self._debug_score_cut_codes)
                relax_debug_payload["risk_gate_codes"] = list(self._debug_risk_ok_codes)
                relax_debug_payload["sizing_ok_codes"] = list(self._debug_sizing_ok_codes)
                relax_debug_payload["sizing_fail_items"] = list(self._debug_sizing_fail_items)
                ok_after_risk = sorted(
                    [c for c in candidates if c.setup_ok],
                    key=lambda c: float(c.features.get("score") or 0.0),
                    reverse=True,
                )
            after_risk_check_count = len(ok_after_risk)
            candidate_codes = [cf.code for cf in ok_after_risk]
            logger.info(
                "[MINERVINI][CANDIDATES] n=%s codes=%s",
                len(candidate_codes),
                ", ".join(candidate_codes) if candidate_codes else "(no candidates)",
            )
            # ✅ [NEW] MINERVINI_ONLY 검증용 로그
            logger.info(
                "[MINERVINI][DONE] ranked=%d dt=%.2f",
                len(candidate_codes),
                dt_minervini,
            )
            
            # ✅ MINERVINI_ONLY 모드: 계산 완료 후 결과 저장하고 조기 종료
            if minervini_only:
                minervini_pass_count = len([c for c in candidates if c.setup_ok])
                minervini_fail_count = len(candidates) - minervini_pass_count
                topk_target = int(os.getenv("PB1_WATCHLIST_TOPK", "50"))
                finaln_target = int(os.getenv("PB1_WATCHLIST_FINALN", "30"))
                scan_codes = [m.get("code") for m in (scan_members or []) if m.get("code")]
                topk_selected = scan_codes[:topk_target]
                finaln_selected = candidate_codes[:finaln_target]
                logger.info(
                    "[MINERVINI_ONLY][SUMMARY] candidate_loaded=%s",
                    len(scan_members),
                )
                logger.info(
                    "[MINERVINI_ONLY][SUMMARY] watchlist_topk=%s selected=%s sample=%s",
                    topk_target,
                    len(topk_selected),
                    ",".join(topk_selected[:5]) if topk_selected else "(none)",
                )
                logger.info(
                    "[MINERVINI_ONLY][SUMMARY] watchlist_finaln=%s selected=%s sample=%s",
                    finaln_target,
                    len(finaln_selected),
                    ",".join(finaln_selected[:5]) if finaln_selected else "(none)",
                )
                logger.info(
                    "[MINERVINI_ONLY][SUMMARY] minervini_pass_count=%s fail_count=%s",
                    minervini_pass_count,
                    minervini_fail_count,
                )
                self._log_reason_summary("minervini_only")
                minervini_top_10 = candidate_codes[:10] if len(candidate_codes) >= 10 else candidate_codes
                
                print(
                    "[MINERVINI_ONLY][RESULT] passed=%s top10=%s"
                    % (minervini_pass_count, ",".join(minervini_top_10) if minervini_top_10 else "(none)")
                )
                logger.info(
                    "[MINERVINI_ONLY][RESULT] minervini_dt=%.2f candidates_ok=%s minervini_top_summary=%s",
                    dt_minervini,
                    minervini_pass_count,
                    ",".join(minervini_top_10) if minervini_top_10 else "(none)",
                )
                
                # top candidates 저장
                top_candidates_path = runtime_path("top_candidates.json")
                try:
                    with open(top_candidates_path, 'w') as f:
                        json.dump(self.top_candidates, f)
                    logger.info(
                        "[MINERVINI_ONLY][TOP_CANDIDATES] saved=%s path=%s",
                        len(self.top_candidates),
                        top_candidates_path
                    )
                except Exception as exc:
                    logger.warning("[MINERVINI_ONLY][TOP_SAVE_FAIL] %s", exc)
                
                # 조기 종료
                logger.info(
                    "[MINERVINI_ONLY][EXIT] order_allowed=0 minervini_dt=%.2f candidates=%s -> DONE_ANALYTICS",
                    dt_minervini,
                    minervini_pass_count,
                )
                self._log_tick_price_cache_summary()
                return self._finalize_run_result(
                    status="DONE_ANALYTICS",
                    notes=f"minervini_only_complete_{minervini_pass_count}_candidates",
                )
            
            with self._stage_timer("entry.minervini_report"):
                minervini_report_path = run_minervini_report(
                    scan_members,
                    self.minervini_config,
                    as_of=self._today,
                    candidates=candidates,
                )
            logger.info("[PB1][STAGE][CHECKPOINT] stage=minervini_report_saved")
            self.top_candidates = [
                {
                    "code": cf.code,
                    "cap": float(cf.features.get("planned_cap") or 0.0),
                    "qty": int(cf.planned_qty or 0),
                }
                for cf in ok_after_risk
            ]
            if not ok_after_risk:
                logger.info(
                    "[TRADE][SKIP] reason=NO_CANDIDATES report_path=%s",
                    minervini_report_path or "none",
                )
            dropped_after_risk = {c.code for c in candidates if c.code in setup_ok_codes and not c.setup_ok}
            for cf in candidates:
                if cf.code in dropped_after_risk:
                    for reason in cf.reasons or ["unspecified_fail"]:
                        self._record_drop(drop_reason_counter, drop_examples, reason, cf.code)

            new_position_limit = min(target_new_positions, len(ok_after_risk)) if ok_after_risk else target_new_positions
            if isinstance(self._budget_plan_meta, dict) and self._budget_plan_meta.get("effective_target"):
                new_position_limit = min(new_position_limit, int(self._budget_plan_meta["effective_target"]))
            held_codes = {p.get("code") for p in existing_positions if p.get("code")}
            with self._stage_timer("entry.open_orders_lookup"):
                open_orders = self._safe_get_open_orders()
            open_buy_codes = {row.get("code") for row in open_orders if str(row.get("side") or "").upper() == "BUY"}
            try:
                with self._stage_timer("entry.today_buy_orders_lookup"):
                    today_orders = self._safe_list_today_orders(side="BUY")
            except Exception as e:
                logger.exception("[PB1][ORDERS_TODAY][FAIL] env=%s side=BUY err=%s", self.env, str(e))
                if engine_live_trading_enabled and not engine_dry_run and engine_run_mode == "LIVE":
                    raise
                today_orders = []
            logger.info(
                "[ENTRY][ENGINE_GATE][AUTHORITATIVE] setup_ok=%s risk_ok=%s sized_ok=%s buyable_ok=%s order_candidates_prebuild=%s scanner_passed=%s",
                len(setup_ok_codes),
                len(self._debug_risk_ok_codes),
                len(self._debug_sizing_ok_codes),
                len(buyable_ok_codes),
                len(orderable_candidates),
                int(locals().get("scanner_pass_count", 0) or 0),
            )
            logger.info(
                "[PB1][ORDER_LOOKUP][RESULT] open_orders=%s today_buy_orders=%s",
                len(open_orders or []),
                len(today_orders or []),
            )
            today_spent = 0.0
            for row in today_orders:
                qty = float(row.get("qty") or 0)
                limit_price = row.get("limit_price")
                if limit_price is None:
                    limit_price = (row.get("request_json") or {}).get("features", {}).get("close")
                today_spent += qty * float(limit_price or 0.0)
            planned_spent = today_spent
            position_by_code = {
                str(row.get("code") or "").zfill(6): dict(row)
                for row in positions
                if row.get("code")
            }
            ok_after_risk_codes = [str(cf.code or "").zfill(6) for cf in ok_after_risk if str(cf.code or "").strip()]
            today_start = self._now_kst.replace(hour=0, minute=0, second=0, microsecond=0)
            tomorrow_start = today_start + timedelta(days=1)
            blocking_statuses = {"SUBMITTED", "ACCEPTED", "FILLED", "PARTIAL_FILLED"}
            blocking_prior_by_code: dict[str, dict[str, Any]] = {}
            for row in today_orders:
                code_key = str((row or {}).get("code") or "").zfill(6)
                if code_key not in ok_after_risk_codes:
                    continue
                if str((row or {}).get("side") or "").upper() != "BUY":
                    continue
                if str((row or {}).get("stage") or "") != "PB1-CLOSE":
                    continue
                if str((row or {}).get("status") or "").upper() not in blocking_statuses:
                    continue
                existing_prior = blocking_prior_by_code.get(code_key)
                if existing_prior is None or str((row or {}).get("created_at") or "") > str(existing_prior.get("created_at") or ""):
                    blocking_prior_by_code[code_key] = dict(row)
            today_buy_fill_rows: list[dict[str, Any]] = []
            if ok_after_risk_codes:
                with self._stage_timer("entry.today_buy_fills_bulk_lookup"):
                    today_buy_fill_rows = self.fills_repo.list_fills_in_window(
                        self.env,
                        start_at=today_start,
                        end_at=tomorrow_start,
                        side="BUY",
                        codes=ok_after_risk_codes,
                    )
            today_buy_fill_by_code: dict[str, list[dict[str, Any]]] = {}
            for row in today_buy_fill_rows:
                code_key = str((row or {}).get("code") or "").zfill(6)
                today_buy_fill_by_code.setdefault(code_key, []).append(dict(row))
            for code_key in ok_after_risk_codes:
                prior = blocking_prior_by_code.get(code_key)
                self._tick_db_cache[("today_fills", code_key, "BUY")] = list(today_buy_fill_by_code.get(code_key, []))
                self._tick_db_cache[("blocking_order", code_key, "BUY", "PB1-CLOSE", self._today)] = (prior is not None, prior)
            buyable_gate_context: dict[str, dict[str, Any]] = {}
            self._buyable_gate_context = buyable_gate_context
            today_buy_codes: set[str] = set()
            buyable_gate_max_sec = self._buyable_gate_max_sec()
            buyable_gate_fail_open = self._buyable_gate_fail_open_enabled()
            logger.info("[PB1][STAGE][START] stage=buyable_gate")
            with self._stage_timer("entry.buyable_gate"):
                for cf in ok_after_risk:
                    self.current_code = cf.code
                    code_key = str(cf.code or "").zfill(6)
                    close_price = float(cf.features.get("close") or 0.0)
                    order_price = float(cf.features.get("order_price") or close_price or 0.0)
                    order_value = order_price * float(cf.planned_qty or 0)
                    candidate_gate_started = time.perf_counter()
                    gate_snapshot = self._build_candidate_buyable_gate_snapshot(
                        code=code_key,
                        position=position_by_code.get(code_key),
                        today_fills=[],
                        prior_order=None,
                    )
                    gate_snapshot["open_order_exists"] = bool(cf.code in open_buy_codes)
                    buyable_gate_context[code_key] = gate_snapshot

                    try:
                        is_blocked = False
                        prior = None
                        with self._stage_timer(f"entry.buyable_gate.blocking_order_lookup.{code_key}"):
                            if not allow_add_to_existing:
                                is_blocked, prior = self._should_block_order(
                                    cf.client_order_key or "",
                                    code=cf.code,
                                    side="BUY",
                                    stage="PB1-CLOSE",
                                )
                        logger.info(
                            "[PB1][BLOCKING_ORDER][RESULT] code=%s blocked=%s has_prior=%s",
                            code_key,
                            int(bool(is_blocked)),
                            int(prior is not None),
                        )
                        if is_blocked and prior:
                            logger.info(
                                "[BUYABLE_GATE][DUP] code=%s key=%s|BUY|PB1-CLOSE prior_status=%s prior_created=%s prior_run=%s",
                                code_key,
                                self._today,
                                prior.get("status"),
                                prior.get("created_at"),
                                prior.get("run_id"),
                            )
                        gate_snapshot = self._build_candidate_buyable_gate_snapshot(
                            code=code_key,
                            position=position_by_code.get(code_key),
                            today_fills=[],
                            prior_order=prior,
                        )
                        gate_snapshot["open_order_exists"] = bool(cf.code in open_buy_codes)
                        buyable_gate_context[code_key] = gate_snapshot
                        if self._buyable_gate_timeout_exceeded(code=code_key, started=candidate_gate_started, max_sec=buyable_gate_max_sec):
                            reject_reason = "BUYABLE_GATE_TIMEOUT"
                            self._record_drop(drop_reason_counter, drop_examples, reject_reason, cf.code)
                            buyable_stage_counter[reject_reason] += 1
                            self._log_buyable_gate(code=cf.code, ok=False, reasons=[reject_reason])
                            self._log_order_skip(cf, [reject_reason], "PB1-CLOSE")
                            self._emit_buy_decision(
                                cf,
                                order_value=order_value,
                                reasons=[reject_reason],
                                entry_allowed=entry_allowed,
                                entry_reason=entry_reason,
                            )
                            continue

                        with self._stage_timer(f"entry.buyable_gate.today_fills_lookup.{code_key}"):
                            today_fills = self._safe_list_today_fills(side="BUY", code=cf.code)
                        logger.info(
                            "[PB1][TODAY_FILLS][RESULT] code=%s fills=%s",
                            code_key,
                            len(today_fills or []),
                        )
                        gate_snapshot = self._build_candidate_buyable_gate_snapshot(
                            code=code_key,
                            position=position_by_code.get(code_key),
                            today_fills=today_fills,
                            prior_order=prior,
                        )
                        gate_snapshot["open_order_exists"] = bool(cf.code in open_buy_codes)
                        buyable_gate_context[code_key] = gate_snapshot
                        if gate_snapshot.get("today_buy_exists"):
                            today_buy_codes.add(code_key)
                        else:
                            today_buy_codes.discard(code_key)
                        if self._buyable_gate_timeout_exceeded(code=code_key, started=candidate_gate_started, max_sec=buyable_gate_max_sec):
                            reject_reason = "BUYABLE_GATE_TIMEOUT"
                            self._record_drop(drop_reason_counter, drop_examples, reject_reason, cf.code)
                            buyable_stage_counter[reject_reason] += 1
                            self._log_buyable_gate(code=cf.code, ok=False, reasons=[reject_reason])
                            self._log_order_skip(cf, [reject_reason], "PB1-CLOSE")
                            self._emit_buy_decision(
                                cf,
                                order_value=order_value,
                                reasons=[reject_reason],
                                entry_allowed=entry_allowed,
                                entry_reason=entry_reason,
                            )
                            continue

                        with self._stage_timer(f"entry.buyable_gate.cooldown_check.{code_key}"):
                            gate_snapshot = self._build_candidate_buyable_gate_snapshot(
                                code=code_key,
                                position=position_by_code.get(code_key),
                                today_fills=today_fills,
                                prior_order=prior,
                            )
                            gate_snapshot["open_order_exists"] = bool(cf.code in open_buy_codes)
                            buyable_gate_context[code_key] = gate_snapshot
                        if self._buyable_gate_timeout_exceeded(code=code_key, started=candidate_gate_started, max_sec=buyable_gate_max_sec):
                            reject_reason = "BUYABLE_GATE_TIMEOUT"
                            self._record_drop(drop_reason_counter, drop_examples, reject_reason, cf.code)
                            buyable_stage_counter[reject_reason] += 1
                            self._log_buyable_gate(code=cf.code, ok=False, reasons=[reject_reason])
                            self._log_order_skip(cf, [reject_reason], "PB1-CLOSE")
                            self._emit_buy_decision(
                                cf,
                                order_value=order_value,
                                reasons=[reject_reason],
                                entry_allowed=entry_allowed,
                                entry_reason=entry_reason,
                            )
                            continue

                        with self._stage_timer(f"entry.buyable_gate.pivot_check.{code_key}"):
                            if self.phase in {"entry", "pm_entry"} and cf.code in buyable_codes:
                                pivot_val = self._to_float(cf.features.get("pivot"))
                                if pivot_val and order_price > 0:
                                    # Fix: PULLBACK 계열은 pivot hard gate 미적용
                                    _pv_entry_style = str(
                                        cf.features.get("entry_style_selected")
                                        or cf.features.get("entry_component")
                                        or cf.features.get("family")
                                        or ""
                                    ).upper()
                                    _is_pullback_style = "PULLBACK" in _pv_entry_style
                                    _require_pivot_for_pullback = (
                                        os.getenv("PB1_REQUIRE_PIVOT_FOR_PULLBACK", "0") == "1"
                                    )
                                    _apply_pivot_hard_gate = (
                                        not _is_pullback_style or _require_pivot_for_pullback
                                    )
                                    if not _apply_pivot_hard_gate:
                                        logger.info(
                                            "[PB1][PIVOT_GATE][POLICY] code=%s style=%s apply=0 reason=pullback_style_no_pivot_required",
                                            cf.code,
                                            _pv_entry_style,
                                        )
                                    else:
                                        logger.info(
                                            "[PB1][PIVOT_GATE][POLICY] code=%s style=%s apply=1 reason=breakout_or_momentum",
                                            cf.code,
                                            _pv_entry_style,
                                        )
                                        if order_price <= pivot_val * 1.003:
                                            self._record_drop(drop_reason_counter, drop_examples, "pivot_not_broken", cf.code)
                                            buyable_stage_counter["pivot_not_broken"] += 1
                                            self._log_buyable_gate(code=cf.code, ok=False, reasons=["pivot_not_broken"])
                                            self._log_order_skip(cf, ["pivot_not_broken"], "PB1-CLOSE")
                                            self._emit_buy_decision(
                                                cf,
                                                order_value=order_value,
                                                reasons=["pivot_not_broken"],
                                                entry_allowed=entry_allowed,
                                                entry_reason=entry_reason,
                                            )
                                            continue
                                        if order_price > pivot_val * 1.03:
                                            self._record_drop(drop_reason_counter, drop_examples, "pivot_overshoot", cf.code)
                                            buyable_stage_counter["pivot_overshoot"] += 1
                                            self._log_buyable_gate(code=cf.code, ok=False, reasons=["pivot_overshoot"])
                                            self._log_order_skip(cf, ["pivot_overshoot"], "PB1-CLOSE")
                                            self._emit_buy_decision(
                                                cf,
                                                order_value=order_value,
                                                reasons=["pivot_overshoot"],
                                                entry_allowed=entry_allowed,
                                                entry_reason=entry_reason,
                                            )
                                            continue
                        if self._buyable_gate_timeout_exceeded(code=code_key, started=candidate_gate_started, max_sec=buyable_gate_max_sec):
                            reject_reason = "BUYABLE_GATE_TIMEOUT"
                            self._record_drop(drop_reason_counter, drop_examples, reject_reason, cf.code)
                            buyable_stage_counter[reject_reason] += 1
                            self._log_buyable_gate(code=cf.code, ok=False, reasons=[reject_reason])
                            self._log_order_skip(cf, [reject_reason], "PB1-CLOSE")
                            self._emit_buy_decision(
                                cf,
                                order_value=order_value,
                                reasons=[reject_reason],
                                entry_allowed=entry_allowed,
                                entry_reason=entry_reason,
                            )
                            continue

                        with self._stage_timer(f"entry.buyable_gate.finalize.{code_key}"):
                            if cf.planned_qty <= 0:
                                sizing_reason = _normalize_sizing_failure_reason(getattr(cf, "sizing_reason", None))
                                self._record_drop(drop_reason_counter, drop_examples, sizing_reason, cf.code)
                                order_stage_counter[sizing_reason] += 1
                                self._log_order_skip(cf, [sizing_reason], "PB1-CLOSE")
                                self._emit_buy_decision(
                                    cf,
                                    order_value=0.0,
                                    reasons=[sizing_reason],
                                    entry_allowed=entry_allowed,
                                    entry_reason=entry_reason,
                                )
                                self._store_entry_evaluation(
                                    cf,
                                    setup_ok=bool(cf.setup_ok),
                                    score_ok=cf.code in set(self._debug_score_cut_codes),
                                    risk_ok=cf.code in set(self._debug_risk_ok_codes),
                                    sizing_ok=False,
                                    buyable_ok=False,
                                    trigger_ok=False,
                                    order_ready=False,
                                    reasons=[sizing_reason],
                                )
                                continue
                            self._log_buyable_gate_trace(code=cf.code, entry_allowed=entry_allowed, snapshot=gate_snapshot)
                            duplicate_intent_exists = False
                            duplicate_intent_status = ""
                            if hasattr(self.orders_repo, "get_order_by_client_order_key") and cf.client_order_key:
                                existing_order = self.orders_repo.get_order_by_client_order_key(self.env, cf.client_order_key)
                                duplicate_intent_exists = bool(existing_order)
                                duplicate_intent_status = str((existing_order or {}).get("status") or "")
                            unified_context = self._build_unified_gate_context(
                                code=cf.code,
                                qty=int(cf.planned_qty or 0),
                                gate_snapshot=gate_snapshot,
                                open_order_exists=bool(cf.code in open_buy_codes),
                                duplicate_intent_exists=duplicate_intent_exists,
                                duplicate_intent_status=duplicate_intent_status,
                                blocking_duplicate_exists=bool(is_blocked),
                            )
                            unified_decision = self._evaluate_unified_buyable_gate(
                                code=cf.code,
                                gate_context=unified_context,
                                allow_add_to_existing=allow_add_to_existing,
                            )
                            self._log_buyable_gate_unified(code=cf.code, decision=unified_decision)
                            if not unified_decision.ok:
                                final_reasons = [reason for reason in unified_decision.reason_codes if reason != "ok"] or ["BUYABLE_GATE_BLOCKED"]
                                for reason in final_reasons:
                                    self._record_drop(drop_reason_counter, drop_examples, reason, cf.code)
                                    buyable_stage_counter[reason] += 1
                                self._log_buyable_gate(code=cf.code, ok=False, reasons=final_reasons)
                                self._log_order_skip(cf, final_reasons, "PB1-CLOSE")
                                self._emit_buy_decision(
                                    cf,
                                    order_value=order_value,
                                    reasons=final_reasons,
                                    entry_allowed=entry_allowed,
                                    entry_reason=entry_reason,
                                )
                                self._store_entry_evaluation(
                                    cf,
                                    setup_ok=bool(cf.setup_ok),
                                    score_ok=cf.code in set(self._debug_score_cut_codes),
                                    risk_ok=cf.code in set(self._debug_risk_ok_codes),
                                    sizing_ok=cf.code in set(self._debug_sizing_ok_codes),
                                    buyable_ok=False,
                                    trigger_ok=False,
                                    order_ready=False,
                                    reasons=final_reasons,
                                )
                                continue
                            if ENTRY_MODE == "CLOSE" and self.window_internal != "close":
                                self._record_drop(drop_reason_counter, drop_examples, "BUYABLE_WINDOW_BLOCK", cf.code)
                                buyable_stage_counter["BUYABLE_WINDOW_BLOCK"] += 1
                                self._log_buyable_gate(code=cf.code, ok=False, reasons=["BUYABLE_WINDOW_BLOCK"])
                                self._log_order_skip(cf, ["BUYABLE_WINDOW_BLOCK"], "PB1-CLOSE")
                                self._emit_buy_decision(
                                    cf,
                                    order_value=order_value,
                                    reasons=["BUYABLE_WINDOW_BLOCK"],
                                    entry_allowed=entry_allowed,
                                    entry_reason=entry_reason,
                                )
                                continue
                            if ENTRY_MODE == "INTRADAY" and self.window_internal == "close":
                                self._record_drop(drop_reason_counter, drop_examples, "BUYABLE_WINDOW_BLOCK", cf.code)
                                buyable_stage_counter["BUYABLE_WINDOW_BLOCK"] += 1
                                self._log_buyable_gate(code=cf.code, ok=False, reasons=["BUYABLE_WINDOW_BLOCK"])
                                self._log_order_skip(cf, ["BUYABLE_WINDOW_BLOCK"], "PB1-CLOSE")
                                self._emit_buy_decision(
                                    cf,
                                    order_value=order_value,
                                    reasons=["BUYABLE_WINDOW_BLOCK"],
                                    entry_allowed=entry_allowed,
                                    entry_reason=entry_reason,
                                )
                                continue
                            cf.features["buyable_ok"] = True
                            self._log_buyable_gate(code=cf.code, ok=True, reasons=[])
                            buyable_ok_codes.append(cf.code)
                        if self._buyable_gate_timeout_exceeded(code=code_key, started=candidate_gate_started, max_sec=buyable_gate_max_sec):
                            reject_reason = "BUYABLE_GATE_TIMEOUT"
                            self._record_drop(drop_reason_counter, drop_examples, reject_reason, cf.code)
                            buyable_stage_counter[reject_reason] += 1
                            self._log_buyable_gate(code=cf.code, ok=False, reasons=[reject_reason])
                            self._log_order_skip(cf, [reject_reason], "PB1-CLOSE")
                            self._emit_buy_decision(
                                cf,
                                order_value=order_value,
                                reasons=[reject_reason],
                                entry_allowed=entry_allowed,
                                entry_reason=entry_reason,
                            )
                            continue
                    except Exception as exc:
                        if buyable_gate_fail_open:
                            reject_reason = f"BUYABLE_GATE_EXCEPTION:{type(exc).__name__}"
                            logger.exception(
                                "[PB1][BUYABLE_GATE][FAIL_OPEN] code=%s err_type=%s err=%s",
                                code_key,
                                type(exc).__name__,
                                exc,
                            )
                            self._record_drop(drop_reason_counter, drop_examples, reject_reason, cf.code)
                            buyable_stage_counter[reject_reason] += 1
                            self._log_buyable_gate(code=cf.code, ok=False, reasons=[reject_reason])
                            self._log_order_skip(cf, [reject_reason], "PB1-CLOSE")
                            self._emit_buy_decision(
                                cf,
                                order_value=order_value,
                                reasons=[reject_reason],
                                entry_allowed=entry_allowed,
                                entry_reason=entry_reason,
                            )
                            continue
                        raise
                    last_price = float(cf.features.get("last_price") or order_price or close_price or 0.0)
                    last_volume = float(cf.features.get("last_volume") or 0.0)
                    trigger_ok, trigger_info = entry_trigger(
                        cf.features,
                        last_price=last_price,
                        last_volume=last_volume,
                        cfg=self.minervini_config,
                    )
                    setup_filters_ok = bool(cf.features.get("setup_loose_ok", cf.features.get("pullback_ok", cf.setup_ok)))
                    entry_ok, entry_reasons, entry_mode = self._entry_gate(
                        setup_filters_ok=setup_filters_ok,
                        breakout_trigger_ok=trigger_ok,
                    )
                    normalized_entry_reason = self._resolve_entry_setup_family(cf)
                    final_entry_reason_code = self._resolve_entry_decision_family(
                        entry_reason=normalized_entry_reason,
                        setup_filters_ok=setup_filters_ok,
                        breakout_trigger_ok=trigger_ok,
                        trigger_reason=(trigger_info or {}).get("reason") if isinstance(trigger_info, dict) else None,
                    )
                    trigger_policy = self._resolve_entry_trigger_policy(
                        trigger_ok=trigger_ok,
                        entry_ok=entry_ok,
                        setup_filters_ok=setup_filters_ok,
                        decision_family=final_entry_reason_code,
                    )
                    entry_ok, entry_reasons = self._enforce_explicit_trigger_bypass(
                        entry_ok=entry_ok,
                        trigger_ok=trigger_ok,
                        trigger_policy=trigger_policy,
                        reasons=entry_reasons,
                    )
                    override_ok = bool(entry_ok and setup_filters_ok and not trigger_ok)
                    cf.features["entry_reason"] = normalized_entry_reason
                    cf.features["entry_setup_family"] = normalized_entry_reason
                    cf.features["entry_trigger_policy"] = trigger_policy
                    cf.features["setup_ok"] = bool(setup_filters_ok)
                    cf.features["trigger_ok"] = bool(trigger_ok)
                    cf.features["override_ok"] = override_ok
                    cf.features["final_entry_reason_code"] = final_entry_reason_code
                    cf.features["entry_decision_family"] = final_entry_reason_code
                    cf.features["entry_rule_version"] = "pb1_entry_reason_v1"
                    cf.features["derived_as_of"] = engine_asof
                    cf.features["trace_id"] = f"{trace_id}:{cf.code}"
                    cf.features["setup_snapshot_json"] = {
                        "setup_filters_ok": setup_filters_ok,
                        "override_ok": override_ok,
                        "entry_cond_mode": entry_mode,
                        "close": cf.features.get("close"),
                        "ma50": cf.features.get("ma50"),
                        "ma150": cf.features.get("ma150"),
                        "ma200": cf.features.get("ma200"),
                        "ma200_slope": cf.features.get("ma200_slope"),
                        "rs_percentile": cf.features.get("rs_percentile"),
                        "vcp_ok": cf.features.get("vcp_ok"),
                        "score": cf.features.get("score"),
                    }
                    cf.features["trigger_snapshot_json"] = {
                        "breakout_trigger_ok": trigger_ok,
                        "trigger_ok": trigger_ok,
                        "last_price": last_price,
                        "pivot": trigger_info.get("pivot", cf.features.get("pivot")),
                        "trigger": trigger_info.get("trigger"),
                        "max_chase": trigger_info.get("max_chase"),
                        "last_volume": last_volume,
                        "vol20": trigger_info.get("vol20", cf.features.get("vol20")),
                        "vol_ok": trigger_info.get("vol_ok"),
                        "reason": trigger_info.get("reason"),
                    }
                    self._log_entry_gate(
                        code=cf.code,
                        setup_filters_ok=setup_filters_ok,
                        breakout_trigger_ok=trigger_ok,
                        entry_ok=entry_ok,
                        entry_mode=entry_mode,
                        setup_metrics={
                            "close": cf.features.get("close"),
                            "ma50": cf.features.get("ma50"),
                            "ma150": cf.features.get("ma150"),
                            "ma200": cf.features.get("ma200"),
                            "ma200_slope": cf.features.get("ma200_slope"),
                            "rs_percentile": cf.features.get("rs_percentile"),
                            "dollar_vol_50": cf.features.get("dollar_vol_50"),
                            "vcp_ok": cf.features.get("vcp_ok"),
                            "score": cf.features.get("score"),
                        },
                        trigger_metrics={
                            "last_price": last_price,
                            "pivot": trigger_info.get("pivot", cf.features.get("pivot")),
                            "trigger": trigger_info.get("trigger"),
                            "max_chase": trigger_info.get("max_chase"),
                            "last_volume": last_volume,
                            "vol20": trigger_info.get("vol20", cf.features.get("vol20")),
                            "vol_ok": trigger_info.get("vol_ok"),
                            "reason": trigger_info.get("reason"),
                        },
                    )
                    if not entry_ok:
                        if not trigger_ok:
                            cf.features["entry_trigger"] = trigger_info
                        for reason in entry_reasons:
                            self._record_drop(drop_reason_counter, drop_examples, reason, cf.code)
                            order_stage_counter[reason] += 1
                        self._log_order_skip(cf, entry_reasons or ["entry_gate_fail"], "PB1-CLOSE")
                        self._emit_buy_decision(
                            cf,
                            order_value=order_value,
                            reasons=entry_reasons or ["entry_gate_fail"],
                            entry_allowed=entry_allowed,
                            entry_reason=entry_reason,
                        )
                        self._store_entry_evaluation(
                            cf,
                            setup_ok=setup_filters_ok,
                            score_ok=cf.code in set(self._debug_score_cut_codes),
                            risk_ok=cf.code in set(self._debug_risk_ok_codes),
                            sizing_ok=cf.code in set(self._debug_sizing_ok_codes),
                            buyable_ok=True,
                            trigger_ok=trigger_ok,
                            order_ready=False,
                            reasons=entry_reasons or ["entry_gate_fail"],
                        )
                        continue
                    df, meta = self._fetch_daily(cf.code)
                    ohlcv_block_reason = self._entry_ohlcv_block_reason(
                        df=df, meta=meta, cf=cf, order_price=order_price
                    )
                    if ohlcv_block_reason:
                        self._record_drop(drop_reason_counter, drop_examples, ohlcv_block_reason, cf.code)
                        order_stage_counter[ohlcv_block_reason] += 1
                        self._log_order_skip(cf, [ohlcv_block_reason], "PB1-CLOSE")
                        self._emit_buy_decision(
                            cf,
                            order_value=order_value,
                            reasons=[ohlcv_block_reason],
                            entry_allowed=entry_allowed,
                            entry_reason=entry_reason,
                        )
                        self._store_entry_evaluation(
                            cf,
                            setup_ok=setup_filters_ok,
                            score_ok=cf.code in set(self._debug_score_cut_codes),
                            risk_ok=cf.code in set(self._debug_risk_ok_codes),
                            sizing_ok=cf.code in set(self._debug_sizing_ok_codes),
                            buyable_ok=True,
                            trigger_ok=trigger_ok,
                            order_ready=False,
                            reasons=[ohlcv_block_reason],
                        )
                        continue
                    if df.empty:
                        self._record_drop(drop_reason_counter, drop_examples, "stop_calc_fail", cf.code)
                        order_stage_counter["stop_calc_fail"] += 1
                        self._log_order_skip(cf, ["stop_calc_fail"], "PB1-CLOSE")
                        self._emit_buy_decision(
                            cf,
                            order_value=order_value,
                            reasons=["stop_calc_fail"],
                            entry_allowed=entry_allowed,
                            entry_reason=entry_reason,
                        )
                        continue
                    entry_price = last_price
                    pivot_val = cf.features.get("pivot")
                    tight_low = cf.features.get("tight_low")
                    atr_val = cf.features.get("atr14")
                    stop0 = calc_initial_stop(
                        pivot=float(pivot_val) if pivot_val is not None else float("nan"),
                        tight_low=float(tight_low) if tight_low is not None else None,
                        atr=float(atr_val) if atr_val is not None else None,
                        mode=INITIAL_STOP_MODE,
                        entry=entry_price,
                        atr_mult=ATR_MULT,
                    )
                    if stop0 >= entry_price:
                        self._record_drop(drop_reason_counter, drop_examples, "stop_above_entry", cf.code)
                        order_stage_counter["stop_above_entry"] += 1
                        self._log_order_skip(cf, ["stop_above_entry"], "PB1-CLOSE")
                        self._emit_buy_decision(
                            cf,
                            order_value=order_value,
                            reasons=["stop_above_entry"],
                            entry_allowed=entry_allowed,
                            entry_reason=entry_reason,
                        )
                        continue
                    cf.features["entry_price"] = float(entry_price)
                    cf.features["initial_stop"] = float(stop0)
                    cf.features["stop_price"] = float(stop0)
                    if isinstance(trigger_info, dict) and trigger_info.get("pivot") is not None:
                        cf.features["pivot_triggered"] = float(trigger_info.get("pivot"))
                    
                    # ========== 당일 매도 종목 재매수 금지 (종목별 필터링) ==========
                    reasons: list[str] = []
                    
                    # 1. 이번 tick에서 매도된 종목은 즉시 재매수 금지
                    if cf.code in sold_codes_this_tick:
                        reasons.append("SAME_TICK_SELL_REENTRY_BLOCK")
                        logger.info(
                            "[ENTRY][SKIP][CODE_LEVEL] code=%s reason=SAME_TICK_SELL_REENTRY_BLOCK",
                            cf.code,
                        )
                    
                    # 2. 오늘 매도된 종목은 당일 재매수 금지
                    elif cf.code in sold_codes_today:
                        reasons.append("SAME_DAY_SELL_REENTRY_BLOCK")
                        logger.info(
                            "[ENTRY][SKIP][CODE_LEVEL] code=%s reason=SAME_DAY_SELL_REENTRY_BLOCK",
                            cf.code,
                        )
                    
                    # 3. 동일 종목/동일 방향 open order 존재 시 재주문 금지
                    elif self.orders_repo.has_open_order_for_code(
                        env=self.env,
                        code=cf.code,
                        side="BUY",
                        trade_date=None,  # 오늘 기준
                    ):
                        reasons.append("OPEN_BUY_ORDER_SAME_CODE")
                        logger.info(
                            "[ENTRY][SKIP][CODE_LEVEL] code=%s reason=OPEN_BUY_ORDER_SAME_CODE",
                            cf.code,
                        )
                    
                    if reasons:
                        for reason in reasons:
                            self._record_drop(drop_reason_counter, drop_examples, reason, cf.code)
                            order_stage_counter[reason] += 1
                        self._log_order_skip(cf, reasons, "PB1-CLOSE")
                        self._emit_buy_decision(
                            cf,
                            order_value=order_value,
                            reasons=reasons,
                            entry_allowed=entry_allowed,
                            entry_reason=entry_reason,
                        )
                        continue
                    
                    # ========== 기존 capacity/budget 체크 ==========
                    if new_position_limit <= 0:
                        reasons.append("max_positions")
                    if target_new_positions <= 0:
                        reasons.append("target_new_positions_zero")
                    if tick_budget_krw <= 0:
                        reasons.append("tick_budget_zero")
                    if available_cash_krw <= 0:
                        reasons.append("available_cash_zero")
                    if min_order_krw > 0 and order_value < min_order_krw:
                        reasons.append("min_order_krw")
                    if order_value <= 0:
                        reasons.append("order_value_zero")
                    if order_value > available_cash_krw:
                        reasons.append("insufficient_cash")
                    if planned_spent + order_value > float(tick_budget_krw):
                        reasons.append("entry_cap_exceeded")
                    if not reasons and len(orderable_candidates) >= new_position_limit:
                        reasons.append("target_new_positions_limit")
                    if reasons:
                        if "entry_cap_exceeded" in reasons:
                            budget_meta = dict(getattr(self, "_budget_plan_meta", {}) or {})
                            logger.info(
                                "[PB1][ORDER][SKIP_DETAIL] code=%s reason=ENTRY_CAP_EXCEEDED base_cash=%.0f usable_cash=%.0f entry_capital=%.0f tick_budget=%.0f used_entry_budget=%.0f remaining_entry_budget=%.0f per_position_budget=%.0f planned_order_value=%.0f qty_candidate=%s price=%.0f",
                                cf.code,
                                float(getattr(self, "entry_base_cash_krw", 0.0) or 0.0),
                                float(self.entry_usable_krw or 0.0),
                                float(self.entry_capital_krw or 0.0),
                                float(tick_budget_krw),
                                float(planned_spent),
                                max(0.0, float(tick_budget_krw) - float(planned_spent)),
                                float(budget_meta.get("per_position_budget") or 0.0),
                                float(order_value),
                                int(cf.planned_qty or 0),
                                float(entry_price or 0.0),
                            )
                        for reason in reasons:
                            self._record_drop(drop_reason_counter, drop_examples, reason, cf.code)
                            order_stage_counter[reason] += 1
                        self._log_order_skip(cf, reasons, "PB1-CLOSE")
                        self._emit_buy_decision(
                            cf,
                            order_value=order_value,
                            reasons=reasons,
                            entry_allowed=entry_allowed,
                            entry_reason=entry_reason,
                        )
                        self._store_entry_evaluation(
                            cf,
                            setup_ok=setup_filters_ok,
                            score_ok=cf.code in set(self._debug_score_cut_codes),
                            risk_ok=cf.code in set(self._debug_risk_ok_codes),
                            sizing_ok=cf.code in set(self._debug_sizing_ok_codes),
                            buyable_ok=True,
                            trigger_ok=trigger_ok,
                            order_ready=False,
                            reasons=reasons,
                        )
                        continue
                    orderable_candidates.append(cf)
                    self._store_entry_evaluation(
                        cf,
                        setup_ok=setup_filters_ok,
                        score_ok=cf.code in set(self._debug_score_cut_codes),
                        risk_ok=cf.code in set(self._debug_risk_ok_codes),
                        sizing_ok=cf.code in set(self._debug_sizing_ok_codes),
                        buyable_ok=True,
                        trigger_ok=trigger_ok,
                        order_ready=True,
                        reasons=["ORDER_READY"],
                        decision_reason="ORDER_READY",
                    )
                    self._emit_buy_decision(
                        cf,
                        order_value=order_value,
                        reasons=[],
                        entry_allowed=entry_allowed,
                        entry_reason=entry_reason,
                    )
                    planned_spent += order_value
                    self._planned_entry_spent_krw = float(planned_spent)
                    if len(orderable_candidates) >= new_position_limit:
                        break
            # position limit으로 break한 경우 나머지 buyable 후보를 명시적으로 추적
            orderable_code_set_after_loop = {c.code for c in orderable_candidates}
            explicitly_dropped_codes = {
                code
                for code, reason in drop_reason_counter.items()
                if code  # drop_reason_counter key is reason not code, see _record_drop
            }
            # _record_drop(drop_reason_counter, drop_examples, reason, code) - reason이 key
            # 실제로 drop된 buyable codes = buyable_ok_set - orderable_code_set_after_loop
            # order_stage_counter에 없는 드랍 = limit break 로 처리
            limit_dropped_count = max(0, len(buyable_ok_codes) - len(orderable_code_set_after_loop) - sum(order_stage_counter.values()))
            if limit_dropped_count > 0:
                order_stage_counter["target_new_positions_limit"] += limit_dropped_count
                limit_dropped_examples = [
                    code for code in buyable_ok_codes
                    if code not in orderable_code_set_after_loop
                ]
                for code in limit_dropped_examples[:limit_dropped_count]:
                    self._record_drop(drop_reason_counter, drop_examples, "target_new_positions_limit", code)
                logger.info(
                    "[PB1][ORDER_CANDIDATES][LIMIT_DROP] reason=target_new_positions_limit count=%s limit=%s buyable=%s",
                    limit_dropped_count,
                    new_position_limit,
                    len(buyable_ok_codes),
                )
            logger.info(
                "[PB1][STAGE][END] stage=buyable_gate ok=%s blocked=%s",
                len(orderable_candidates),
                sum(buyable_stage_counter.values()),
            )

            if (
                self.bootstrap_enabled
                and self.phase in {"entry", "pm_entry"}
                and entry_allowed
                and bool(self.order_allowed)
                and existing_positions_count == 0
                and (self.env or "").lower() == "practice"
                and not orderable_candidates
            ):
                fallback_pool = [
                    c
                    for c in candidates
                    if bool(c.features.get("minervini_buyable"))
                    and "planned_qty_zero_or_min_order" in (c.reasons or [])
                ]
                if fallback_pool:
                    fallback_pool.sort(key=lambda c: float(c.features.get("score") or 0.0), reverse=True)
                    chosen = fallback_pool[0]
                    order_px = float(chosen.features.get("order_price") or chosen.features.get("close") or 0.0)
                    if order_px > 0:
                        chosen.planned_qty = 1
                        chosen.planned_value = float(order_px)
                        chosen.setup_ok = True
                        chosen.sizing_reason = "BOOTSTRAP_FALLBACK_FORCE_1"
                        chosen.sizing_details = {
                            "reason": "sizing_zero_but_buyable",
                            "forced_qty": 1,
                            "env": self.env,
                        }
                        orderable_candidates.append(chosen)
                        logger.warning(
                            "[PB1][BOOTSTRAP][FALLBACK_ORDER] code=%s reason=sizing_zero_but_buyable forcing_qty=1 env=%s",
                            chosen.code,
                            self.env,
                        )

            if self.bootstrap_enabled and len(orderable_candidates) > 3:
                for dropped_cf in orderable_candidates[3:]:
                    self._record_drop(drop_reason_counter, drop_examples, "score_keep_topn_limit", dropped_cf.code)
                    order_stage_counter["score_keep_topn_limit"] += 1
                orderable_candidates = orderable_candidates[:3]
                logger.warning("[PB1][BOOTSTRAP] orderable capped to top3")

            allow_emergency_candidate = bool(engine_dry_run and os.getenv("PB1_EMERGENCY_DEBUG", "0") == "1")
            has_buyable_block_reason = any(
                str(reason).upper().startswith("BUYABLE_")
                for reason in drop_reason_counter.keys()
            )
            has_buyable_gate_blocks = bool(today_buy_codes) or any(
                bool(snapshot.get("today_buy_exists")) or bool(snapshot.get("cooldown_active"))
                for snapshot in buyable_gate_context.values()
            )
            emergency_fallback_blocked = bool(
                engine_intended_live
                or engine_force_block_live
                or engine_compute_only
                or has_buyable_gate_blocks
                or has_buyable_block_reason
            )
            logger.info(
                "[ORDER_CANDIDATES][EMERGENCY][GATE] enabled=%s dry_run=%s intended_live=%s force_block_live=%s pb1_emergency_debug=%s",
                int(allow_emergency_candidate and not emergency_fallback_blocked),
                int(engine_dry_run),
                int(engine_intended_live),
                int(engine_force_block_live),
                os.getenv("PB1_EMERGENCY_DEBUG", "0"),
            )

            if not orderable_candidates and allow_emergency_candidate and not emergency_fallback_blocked:
                code_to_member = {
                    str(m.get("code") or "").zfill(6): dict(m)
                    for m in (scan_members or [])
                    if m.get("code")
                }
                emergency_ranked_codes = sorted(
                    code_to_member.keys(),
                    key=lambda code: (
                        float(code_to_member[code].get("score_final") or 0.0),
                        float(code_to_member[code].get("flow_score") or 0.0),
                        float(code_to_member[code].get("rs_percentile") or 0.0),
                    ),
                    reverse=True,
                )
                atr_max_ratio = float(PB1_MAX_ATR_PCT)
                if atr_max_ratio > 1.0:
                    atr_max_ratio = atr_max_ratio / 100.0
                for code in emergency_ranked_codes:
                    if code in held_codes or code in open_buy_codes or code in today_buy_codes:
                        continue
                    emergency_cf = next((c for c in candidates if c.code == code), None)
                    if emergency_cf is None:
                        computed = self._compute_candidates_from_codes([code])
                        emergency_cf = computed[0] if computed else None
                    if emergency_cf is None:
                        continue
                    order_px = float(
                        self._to_float(emergency_cf.features.get("order_price"))
                        or self._to_float(emergency_cf.features.get("close"))
                        or 0.0
                    )
                    if order_px <= 0:
                        continue
                    if min_order_krw > 0 and order_px < min_order_krw:
                        continue
                    if float(available_cash_krw) < order_px:
                        continue
                    atr_pct = self._to_float(emergency_cf.features.get("atr_pct"))
                    if atr_pct is None or float(atr_pct) > float(atr_max_ratio):
                        continue
                    emergency_cf.features["candidate_tier"] = "emergency_force1"
                    emergency_cf.sizing_reason = "EMERGENCY_CANDIDATE_FALLBACK"
                    emergency_cf.sizing_details = {"candidate_tier": "emergency_force1", "qty": 1, "price": order_px}
                    if emergency_order_enabled and not emergency_diag_only:
                        emergency_cf.setup_ok = True
                        emergency_cf.features["setup_loose_ok"] = True
                        emergency_cf.features["setup_strict_ok"] = False
                        emergency_cf.planned_qty = 1
                        emergency_cf.planned_value = float(order_px)
                        emergency_cf.client_order_key = self._client_order_key(emergency_cf.code, emergency_cf.mode, "BUY", "close", "PB1")
                        orderable_candidates.append(emergency_cf)
                        logger.warning(
                            "[ORDER_CANDIDATES][EMERGENCY] selected=%s qty=1 reason=EMERGENCY_CANDIDATE_FALLBACK",
                            emergency_cf.code,
                        )
                    else:
                        logger.warning(
                            "[ORDER_CANDIDATES][EMERGENCY][DIAG_ONLY] selected=%s qty=1 reason=EMERGENCY_CANDIDATE_FALLBACK order_enabled=%s diag_only=%s",
                            emergency_cf.code,
                            int(emergency_order_enabled),
                            int(emergency_diag_only),
                        )
                    break

            valid_orderable: list[CandidateFeature] = []
            for cf in orderable_candidates:
                loose_ok = bool(cf.features.get("setup_loose_ok", cf.setup_ok))
                strict_ok = bool(cf.features.get("setup_strict_ok", False))
                if loose_ok or strict_ok:
                    valid_orderable.append(cf)
                    continue
                logger.warning(
                    "[ORDER_CANDIDATES][DROP] code=%s reason=setup_not_ok loose_ok=%s strict_ok=%s",
                    cf.code,
                    int(loose_ok),
                    int(strict_ok),
                )
                self._record_drop(drop_reason_counter, drop_examples, "setup_not_ok", cf.code)
                order_stage_counter["setup_not_ok"] += 1
            orderable_candidates = valid_orderable

            orderable_codes = [c.code for c in orderable_candidates]
            logger.info("[ORDER_CANDIDATES] count=%s codes=%s", len(orderable_codes), orderable_codes)
            logger.info("[ORDER_CANDIDATES][READY] count=%s codes=%s", len(orderable_codes), orderable_codes)
            if (getattr(self, "_relax_bridge_summary", {}) or {}).get("activated"):
                bridge_orderable_codes = [c.code for c in orderable_candidates if "RELAX_BRIDGE" in set((c.features or {}).get("quality_flags") or [])]
                logger.info("[ENTRY][RELAX_BRIDGE][ORDERABLE] count=%s codes=%s", len(bridge_orderable_codes), bridge_orderable_codes)
            logger.info(
                "[TRADE][ORDERABLE][CODES] count=%s codes=%s",
                len(orderable_codes),
                orderable_codes,
            )
            logger.info(
                "[ENTRY][ENGINE_GATE][AUTHORITATIVE] setup_ok=%s risk_ok=%s sized_ok=%s buyable_ok=%s final_orders=%s",
                len(setup_ok_codes),
                len(self._debug_risk_ok_codes),
                len(self._debug_sizing_ok_codes),
                len(buyable_ok_codes),
                len(orderable_codes),
            )
            if int(locals().get("scanner_pass_count", 0) or 0) == 0 and len(orderable_codes) > 0:
                logger.info(
                    "[ENTRY][SCANNER][EXPLAIN] scanner_passed=0 engine_orders=%s reason=scanner_signals_are_diagnostic_but_engine_gate_is_authoritative",
                    len(orderable_codes),
                )
            relax_debug_payload["final_orderable_codes"] = orderable_codes

            after_buyable_check_count = len(buyable_ok_codes)
            after_dedup_count = len(orderable_candidates)
            drop_reasons_by_code: dict[str, list[str]] = {}
            drop_reason_examples_full: dict[str, list[str]] = {}
            for cf in candidates:
                if cf.setup_ok:
                    continue
                reasons = [str(r) for r in (cf.reasons or ["unspecified_fail"]) if r]
                if reasons:
                    drop_reasons_by_code[cf.code] = sorted(set(reasons))
                for reason in sorted(set(reasons)):
                    bucket = drop_reason_examples_full.setdefault(reason, [])
                    bucket.append(cf.code)
            logger.info(
                "[PB1][DROP_REASON][SAMPLES] reasons=%s",
                {k: v[:10] for k, v in drop_reason_examples_full.items()},
            )
            relax_debug_payload["drop_reasons_by_code"] = drop_reasons_by_code
            relax_debug_payload["drop_reason_examples"] = drop_reason_examples_full
            relax_debug_payload["score_cut_codes"] = list(self._debug_score_cut_codes)
            relax_debug_payload["risk_gate_codes"] = list(self._debug_risk_ok_codes)
            relax_debug_payload["sizing_ok_codes"] = list(self._debug_sizing_ok_codes)
            relax_debug_payload["sizing_fail_items"] = list(self._debug_sizing_fail_items)

            if not orderable_candidates and (os.getenv("TRADE_FORCE_MIN1_DIAG", "1") == "1"):
                near_miss = sorted(
                    [cf for cf in candidates if cf.code],
                    key=lambda x: (len(list(set(x.reasons or []))), -float(x.features.get("score") or 0.0), x.code),
                )[:3]
                near_miss_payload: list[dict[str, Any]] = []
                for cf in near_miss:
                    miss = sorted(set(cf.reasons or []))
                    near_miss_payload.append({"code": cf.code, "missing": miss})
                    logger.info("[ENTRY_SCAN][NEAR_MISS] code=%s missing=%s", cf.code, miss)
                relax_debug_payload["near_miss_candidates"] = near_miss_payload

            _write_relax_debug_report(relax_debug_payload)
            
            # [ENTRY][NO_BUY] 후보가 0일 때 이유 출력
            if not orderable_candidates:
                no_buy_reason = "UNKNOWN"
                if not candidates:
                    no_buy_reason = "EMPTY_AFTER_MINERVINI"
                elif not setup_ok_codes:
                    no_buy_reason = "EMPTY_AFTER_PB1_FILTER"
                elif data_ok_count == 0:
                    no_buy_reason = "OHLCV_INSUFFICIENT"
                elif not entry_allowed:
                    no_buy_reason = "ORDER_SUBMIT_BLOCKED"
                elif available_cash_krw <= 0:
                    no_buy_reason = "CASH_INSUFFICIENT"
                elif after_risk_check_count == 0:
                    no_buy_reason = "EMPTY_AFTER_RANK"
                elif drop_reason_counter:
                    top_reason = drop_reason_counter.most_common(1)[0][0] if drop_reason_counter else "unknown"
                    no_buy_reason = f"DROP:{top_reason}"
                logger.warning(
                    "[ENTRY][NO_BUY] trace=%s reason=%s candidates=%s setup_ok=%s after_risk=%s cash=%s drop_top3=%s",
                    trace_id,
                    no_buy_reason,
                    len(candidates),
                    len(setup_ok_codes),
                    after_risk_check_count,
                    available_cash_krw,
                    drop_reason_counter.most_common(3),
                )

            ok_count = len([c for c in candidates if c.setup_ok])
            logger.info(
                "[PB1][CANDIDATES][SUMMARY] universe=%s scanned=%s selected_tier=%s ok=%s total=%s relax_passes=%s min_score=%.1f require_both=%s thresholds={vol_max:%.2f volu_max:%.2f pullback_min:%.3f pullback_max:%.3f}",
                len(members),
                len(candidates),
                selected_tier,
                ok_count,
                len(candidates),
                relax_passes_used,
                applied_min_score,
                applied_require_both,
                selected_thresholds.vol_contraction_max,
                selected_thresholds.volu_contraction_max,
                selected_thresholds.pullback_min,
                selected_thresholds.pullback_max,
            )
            
            # ✅ 설계 1: 매매 없는 이유를 명확히 로깅
            logger.info(
                "[ENTRY][SUMMARY] scanned=%s score_pass=%s trigger_hit=%s blocked=%s final_orders=%s reasons_top=%s",
                len(candidates),
                len(setup_ok_codes),
                after_buyable_check_count,
                after_buyable_check_count - len(orderable_candidates),
                len(orderable_candidates),
                drop_reason_counter.most_common(10),
            )
            
            logger.info(
                "[PB1][CANDIDATES][SNAPSHOT] setup_ok_count=%s setup_ok_sample=%s after_risk_check_count=%s after_buyable_check_count=%s after_dedup_count=%s drop_reasons_topN=%s drop_examples=%s",
                len(setup_ok_codes),
                setup_ok_codes[:3],
                after_risk_check_count,
                after_buyable_check_count,
                after_dedup_count,
                drop_reason_counter.most_common(self.drop_reasons_topn),
                {k: v for k, v in drop_examples.items() if v},
            )
            summary_reason = "OK"
            if ok_count == 0:
                summary_reason = "NO_CANDIDATES_AFTER_RELAX"
            elif ok_count > 0 and not orderable_candidates:
                summary_reason = "NO_ORDERABLE_CANDIDATES"
            logger.info(
                "[PB1][RUN][SUMMARY] universe=%s candidates=%s relax_passes=%s selected=%s reason=%s cash_total=%s usable=%s tick_budget=%s",
                len(members),
                len(candidates),
                relax_passes_used,
                len(orderable_candidates),
                summary_reason,
                total_cash_krw,
                entry_usable_krw,
                tick_budget_krw,
            )
            risk_stage_counter: Counter[str] = Counter()
            sizing_stage_counter: Counter[str] = Counter()
            risk_ok_set = set(self._debug_risk_ok_codes)
            sizing_ok_set = set(self._debug_sizing_ok_codes)
            setup_ok_set = set(setup_ok_codes)
            for cf in candidates:
                if cf.code in setup_ok_set and cf.code not in risk_ok_set:
                    for reason in sorted(set(cf.reasons or ["unspecified_risk_fail"])):
                        risk_stage_counter[reason] += 1
                elif cf.code in risk_ok_set and cf.code not in sizing_ok_set:
                    sizing_stage_counter[_normalize_sizing_failure_reason(getattr(cf, "sizing_reason", None))] += 1
            logger.info(
                "[ENTRY][FUNNEL][PRE_SUBMIT] setup_ok=%s risk_ok=%s sized_ok=%s buyable_ok=%s order_candidates=%s api_submitted_pending=%s",
                len(setup_ok_codes),
                len(self._debug_risk_ok_codes),
                len(self._debug_sizing_ok_codes),
                len(buyable_ok_codes),
                len(orderable_candidates),
                submit_success_count,
            )
            for stage_name, stage_counter, before_count, after_count in (
                ("risk", risk_stage_counter, len(setup_ok_codes), len(self._debug_risk_ok_codes)),
                ("sizing", sizing_stage_counter, len(self._debug_risk_ok_codes), len(self._debug_sizing_ok_codes)),
                ("buyable", buyable_stage_counter, len(self._debug_sizing_ok_codes), len(buyable_ok_codes)),
                ("order_candidates", order_stage_counter, len(buyable_ok_codes), len(orderable_candidates)),
            ):
                drop_count = max(0, before_count - after_count)
                if drop_count > 0:
                    logger.info(
                        "[ENTRY][FUNNEL][DROP] stage=%s count=%s reasons=%s examples=%s",
                        stage_name,
                        drop_count,
                        [reason for reason, _count in stage_counter.most_common(self.drop_reasons_topn)],
                        {
                            codes[0]: reason
                            for reason, codes in drop_examples.items()
                            if codes and reason in {name for name, _count in stage_counter.most_common(self.drop_reasons_topn)}
                        },
                    )
            logger.info(
                "[RUN_SUMMARY][ENTRY] scanned=%s setup_ok=%s relax_ok=%s score_ok=%s risk_ok=%s sized_ok=%s order_candidates=%s api_submitted=%s accepted=%s filled=%s",
                len(scan_members),
                len(setup_ok_codes),
                len(setup_ok_codes),
                len(self._debug_score_cut_codes),
                len(self._debug_risk_ok_codes),
                len(self._debug_sizing_ok_codes),
                len(orderable_candidates),
                submit_success_count,
                int(accepted_count) if 'accepted_count' in locals() else 0,
                int(filled_count) if 'filled_count' in locals() else 0,
            )
            orderable_code_set = {candidate.code for candidate in orderable_candidates}
            buyable_ok_set = set(buyable_ok_codes)
            for cf in candidates:
                if cf.code in self._entry_eval_by_code:
                    continue
                self._store_entry_evaluation(
                    cf,
                    setup_ok=bool(cf.setup_ok),
                    score_ok=cf.code in set(self._debug_score_cut_codes),
                    risk_ok=cf.code in set(self._debug_risk_ok_codes),
                    sizing_ok=cf.code in set(self._debug_sizing_ok_codes),
                    buyable_ok=cf.code in buyable_ok_set,
                    trigger_ok=bool(cf.features.get("trigger_ok")),
                    order_ready=cf.code in orderable_code_set,
                    reasons=[str(reason) for reason in (cf.reasons or [cf.features.get("final_entry_reason_code") or "ENTRY_CONDITION_NOT_MET"]) if str(reason)],
                )
            self._log_fail_reason_breakdown(candidates, note="post_filter")
            _emit_entry_summary(setup_ok_codes, orderable_candidates, drop_reason_counter)
            if not candidates or ok_count == 0:
                top_reasons = all_reason_counts.most_common(3)
                final_status, final_reason = _classify_no_candidate_result()
                final_notes = f"no_candidates:{top_reasons or 'none'}"
                logger.info("[TRADE][NO_CANDIDATES][CLASSIFY] type=strategy_empty not_system_error=1")
                logger.info(
                    "[PB1][NO_TRADE] reason=no_candidates tiers_tried=%s tier=%s total=%s relax_passes=%s min_score=%.1f top_reasons=%s",
                    tiers_tried or ["none"],
                    selected_tier,
                    len(candidates),
                    relax_passes_used,
                    applied_min_score,
                    top_reasons or "none",
                )
                logger.info(
                    "[RUN_SUMMARY][RESULT] status=%s reason=%s",
                    final_status,
                    final_reason,
                )
                if self.phase in {"prep", "entry", "pm_entry"}:
                    logger.info(
                        "[TRADE][SKIP] reason=%s pool=%s final=%s report=%s",
                        ReasonCode.SKIP_NO_CANDIDATES,
                        len(members),
                        ok_count,
                        minervini_report_path or "none",
                    )
                    self._log_reason_summary(final_notes)
                    _emit_entry_summary(setup_ok_codes, orderable_candidates, drop_reason_counter)
                    _emit_entry_decision(
                        "SKIP",
                        reason="NO_FINAL_SETUPS",
                        ok_setups=ok_count,
                        blocked_by=_normalize_entry_block_counts(drop_reason_counter),
                    )
                    _set_run_summary_payload(
                        scanned=len(scan_members),
                        setup_ok=len(setup_ok_codes),
                        relax_ok=len(setup_ok_codes),
                        score_ok=len(self._debug_score_cut_codes),
                        risk_ok=len(self._debug_risk_ok_codes),
                        sized_ok=len(self._debug_sizing_ok_codes),
                        buyable_ok=len(buyable_ok_codes),
                        order_candidates=0,
                        submitted=0,
                        blocked_reasons_counter=drop_reason_counter,
                        no_trade_reason="NO_CANDIDATES_AFTER_RELAX",
                    )
                    return self._finalize_run_result(status=final_status, notes=final_notes)
            if self.phase in {"entry", "pm_entry"}:
                if not entry_allowed:
                    logger.info("[PB1][ENTRY][SKIP] entry_allowed=False")
                if ok_count > 0 and not orderable_candidates:
                    no_orders_reasons: list[str] = []
                    if not entry_allowed:
                        no_orders_reasons.append("entry_disabled")
                    if skip_entry_scan:
                        no_orders_reasons.append("entry_cutoff")
                    if max_positions - existing_positions_count <= 0:
                        no_orders_reasons.append("max_positions")
                        logger.warning(
                            "[ENTRY][BLOCKED_BY_PORTFOLIO_FULL] existing_positions=%s max_positions=%s target_new_positions=%s",
                            existing_positions_count,
                            max_positions,
                            target_new_positions,
                        )
                    if target_new_positions <= 0:
                        no_orders_reasons.append("target_new_positions_zero")
                        logger.warning(
                            "[ENTRY][BLOCKED_BY_TARGET_LIMIT] existing_positions=%s target_new_positions=%s max_positions=%s",
                            existing_positions_count,
                            target_new_positions,
                            max_positions,
                        )
                    if tick_budget_krw <= 0:
                        no_orders_reasons.append("tick_budget_zero")
                    if available_cash_krw <= 0:
                        no_orders_reasons.append("available_cash_zero")
                    if min_order_krw > 0 and after_buyable_check_count == 0:
                        no_orders_reasons.append("min_order_krw")
                    if not no_orders_reasons:
                        no_orders_reasons.append("exhausted_candidates")
                    blocked_by = _normalize_entry_block_counts(drop_reason_counter)
                    blocked_by.update(_normalize_entry_block_reasons(no_orders_reasons))
                    blocked_summary = _format_reason_counts(blocked_by)
                    primary_reason = _primary_no_trade_reason(
                        blocked_by,
                        ok_count=ok_count,
                        order_candidates=len(orderable_candidates),
                    )
                    final_status = "OK_NO_TRADE"
                    final_notes = "NO_ORDERABLE_CANDIDATES"
                    logger.info(
                        "[ENTRY][NO_ORDERABLE] setup_ok=%s risk_ok=%s sized_ok=%s buyable_ok=%s reason_top=%s",
                        len(setup_ok_codes),
                        after_risk_check_count,
                        len(self._debug_sizing_ok_codes),
                        after_buyable_check_count,
                        drop_reason_counter.most_common(self.drop_reasons_topn),
                    )
                    logger.info(
                        "[PB1][NO_TRADE] reason=no_orders primary_no_trade_reason=%s blocked_by=%s no_orders_reason=%s existing_positions=%s max_positions=%s target_new_positions=%s",
                        primary_reason,
                        blocked_summary,
                        no_orders_reasons or ["none"],
                        existing_positions_count,
                        max_positions,
                        target_new_positions,
                    )
                    if self.phase in {"prep", "entry", "pm_entry"}:
                        if ok_count == 0:
                            logger.info(
                                "[TRADE][SKIP] reason=%s pool=%s final=%s report=%s",
                                ReasonCode.SKIP_NO_CANDIDATES,
                                len(members),
                                ok_count,
                                minervini_report_path or "none",
                            )
                        self._log_reason_summary(final_notes)
                        _emit_entry_summary(setup_ok_codes, orderable_candidates, drop_reason_counter)
                        # ✅ FORCE_BUY 스모크 모드 (주문 endpoint 도달 검증)
                        if os.getenv("PB1_FORCE_BUY", "0") == "1":
                            qty = int(os.getenv("PB1_FORCE_BUY_QTY", "1"))
                            force_list = members if members else []
                            if force_list:
                                forced_code = str(force_list[0].get("code") or "").zfill(6)
                                logger.warning(
                                    "[PB1][FORCE_BUY][SMOKE] forcing buy code=%s qty=%d reason=verify_order_endpoint",
                                    forced_code, qty
                                )
                                self._submit_force_buy_order(code=forced_code, qty=qty)
                        
                        _emit_entry_decision(
                            "SKIP",
                            reason="NO_ORDER_INTENTS",
                            ok_setups=ok_count,
                            blocked_by=blocked_by,
                        )
                        _set_run_summary_payload(
                            scanned=len(scan_members),
                            setup_ok=len(setup_ok_codes),
                            relax_ok=len(setup_ok_codes),
                            score_ok=len(self._debug_score_cut_codes),
                            risk_ok=len(self._debug_risk_ok_codes),
                            sized_ok=len(self._debug_sizing_ok_codes),
                            buyable_ok=len(buyable_ok_codes),
                            order_candidates=0,
                            submitted=0,
                            blocked_reasons_counter=blocked_by,
                            no_trade_reason=primary_reason,
                        )
                        return self._finalize_run_result(status=final_status, notes=final_notes)
                if entry_allowed and orderable_candidates:
                    planned_total = sum(
                        float(cf.features.get("close") or 0.0) * float(cf.planned_qty or 0)
                        for cf in orderable_candidates
                    )
                    blocked_by = _normalize_entry_block_counts(drop_reason_counter)
                    _emit_entry_decision(
                        "PLACE",
                        reason=None,
                        ok_setups=ok_count,
                        blocked_by=blocked_by,
                        orders=len(orderable_candidates),
                        total_krw=planned_total,
                    )
                else:
                    blocked_by = _normalize_entry_block_counts(drop_reason_counter)
                    _emit_entry_decision(
                        "SKIP",
                        reason=entry_reason if entry_reason != "ok" else "NO_ORDER_INTENTS",
                        ok_setups=ok_count,
                        blocked_by=blocked_by,
                    )
                
                # [ORDER][BUILD] - 주문 생성 시작
                with self._stage_timer("entry.order_build"):
                    t_order_build = time.monotonic()
                    order_symbols = [cf.code for cf in orderable_candidates]
                    logger.info(
                        "[ORDER][BUILD] trace=%s count=%s symbols=%s",
                        trace_id,
                        len(orderable_candidates),
                        ",".join(order_symbols[:10]) + ("..." if len(order_symbols) > 10 else ""),
                    )
                    dt_order_build = time.monotonic() - t_order_build
                
                # [ORDER][SUBMIT] - 주문 제출
                with self._stage_timer("entry.order_submit"):
                    t_order_submit = time.monotonic()
                    attempted_count = 0
                    api_submitted_count = 0
                    accepted_count = 0
                    filled_count = 0
                    failed_count = 0
                    rejected_count = 0
                    skipped_count = 0
                    submit_attempt_count = len(orderable_candidates)
                    if not entry_allowed:
                        logger.info(
                            "[ORDER_SUBMIT][SKIP] reason=LIVE_GATE_BLOCKED count=%s",
                            len(orderable_candidates),
                        )
                        skipped_count = len(orderable_candidates)
                    else:
                        for cf in orderable_candidates:
                            try:
                                buy_allowed, buy_block_reason, runtime_cutoff_dt, _market_close_dt = self._is_buy_allowed_now(now_kst())
                                if not buy_allowed:
                                    logger.warning(
                                        "[ORDER][PRE_SUBMIT][BLOCK] side=BUY code=%s reason=%s now=%s cutoff=%s",
                                        cf.code,
                                        buy_block_reason,
                                        now_kst().isoformat(),
                                        runtime_cutoff_dt.isoformat(),
                                    )
                                    skipped_count += 1
                                    continue
                                logger.info("[ORDER_SUBMIT][ATTEMPT] code=%s qty=%s", cf.code, cf.planned_qty)
                                if self.window_internal == "close":
                                    order_status = self._place_entry_close(cf)
                                else:
                                    order_status = self._place_entry(cf)
                                terminal_event = str(order_status.get("terminal_event") or "")
                                if terminal_event not in {"API_RESULT", "FINAL_SKIP"}:
                                    raise RuntimeError(
                                        f"missing terminal submit event for code={cf.code} terminal_event={terminal_event or 'none'}"
                                    )
                                attempted_count += int(order_status.get("submit_attempted", 0) or 0)
                                api_submitted_count += int(order_status.get("api_submitted", 0) or 0)
                                accepted_count += int(order_status.get("accepted", 0) or 0)
                                filled_count += int(order_status.get("filled", 0) or 0)
                                rejected_count += int(order_status.get("rejected", 0) or 0)
                                skipped_count += int(order_status.get("skipped", 0) or 0)
                                failed_count += int(order_status.get("failed", 0) or 0)
                            except Exception as e:
                                failed_count += 1
                                logger.exception(
                                    "[ORDER][SUBMIT][ERROR] trace=%s code=%s error=%s",
                                    trace_id,
                                    cf.code,
                                    str(e),
                                )
                    dt_order_submit = time.monotonic() - t_order_submit
                submit_success_count = api_submitted_count
                logger.info(
                    "[ENTRY][FUNNEL][POST_SUBMIT] order_candidates=%s attempted=%s accepted=%s rejected=%s failed=%s filled_by_reconcile=%s",
                    len(orderable_candidates),
                    attempted_count,
                    accepted_count,
                    rejected_count,
                    failed_count,
                    filled_count,
                )
                
                logger.info(
                    "[ORDER][SUBMIT] trace=%s attempted=%s api_submitted=%s accepted=%s filled=%s rejected=%s skipped=%s failed=%s dt_build=%.2f dt_submit=%.2f",
                    trace_id,
                    attempted_count,
                    api_submitted_count,
                    accepted_count,
                    filled_count,
                    rejected_count,
                    skipped_count,
                    failed_count,
                    dt_order_build,
                    dt_order_submit,
                )
                logger.info(
                    "[RUN_SUMMARY][ORDER] candidates=%s attempted=%s api_submitted=%s accepted=%s filled=%s rejected=%s skipped=%s",
                    len(orderable_candidates),
                    attempted_count,
                    api_submitted_count,
                    accepted_count,
                    filled_count,
                    rejected_count,
                    skipped_count,
                )
                exit_summary_payload = dict(getattr(self, "_exit_summary_payload", {}) or {})
                sell_accepted = int(exit_summary_payload.get("accepted_sell_count") or 0)
                sell_filled = int(exit_summary_payload.get("fill_confirmed_sell_count") or 0)
                try:
                    open_orders_count = len(self.orders_repo.get_open_orders(self.env) or [])
                except Exception:
                    open_orders_count = 0
                logger.info(
                    "[RUN_SUMMARY][ORDERS] buy_accepted=%s buy_filled=%s sell_accepted=%s sell_filled=%s open_orders=%s",
                    accepted_count,
                    filled_count,
                    sell_accepted,
                    sell_filled,
                    open_orders_count,
                )
                if len(orderable_candidates) > 0 and self.order_allowed and not self.dry_run and self.intended_live and api_submitted_count == 0:
                    if skipped_count >= len(orderable_candidates) and attempted_count == 0:
                        # 모든 후보가 guard / cooldown / 중복 방지로 skip된 정상 케이스 → not-fatal
                        logger.warning(
                            "[ORDER][ALL_SKIPPED_BEFORE_SUBMIT] session=%s candidates=%s skipped=%s reasons=%s",
                            self.session_kind,
                            len(orderable_candidates),
                            skipped_count,
                            {},
                        )
                        logger.info(
                            "[RUN_SUMMARY][RESULT] session=%s status=OK_NO_TRADE reason=ALL_CANDIDATES_SKIPPED_BEFORE_API_SUBMIT",
                            self.session_kind,
                        )
                    else:
                        logger.error("[ORDER][ANOMALY][CANDIDATE_WITHOUT_API_SUBMIT] candidates=%s attempted=%s accepted=%s skipped=%s", len(orderable_candidates), attempted_count, accepted_count, skipped_count)
                        if os.getenv("PB1_HARD_FAIL_ON_CANDIDATE_WITHOUT_API_SUBMIT", "1") == "1":
                            raise RuntimeError(
                                f"[ORDER][ANOMALY][CANDIDATE_WITHOUT_API_SUBMIT] candidates={len(orderable_candidates)} attempted={attempted_count} api_submitted={api_submitted_count} skipped={skipped_count}"
                            )
                if entry_allowed and self.phase in {"entry", "pm_entry"} and allow_add_to_existing:
                    remaining_budget = max(0.0, float(tick_budget_krw) - planned_spent)
                    for pos in existing_positions:
                        code = pos.get("code")
                        if not code or remaining_budget <= 0:
                            continue
                        if code in open_buy_codes or code in today_buy_codes:
                            continue
                        pyramid_level = int(pos.get("pyramid_level") or 0)
                        if pyramid_level >= int(self.minervini_config.max_pyramid_levels):
                            continue
                        entry_price = float(pos.get("avg_buy_price") or 0.0)
                        initial_stop = pos.get("initial_stop")
                        if not entry_price or initial_stop is None:
                            continue
                        initial_stop = float(initial_stop)
                        r_value = entry_price - initial_stop
                        if r_value <= 0:
                            continue
                        mark, _source = self._resolve_price_with_fallback(code)
                        if mark is None:
                            continue
                        last_add_price = float(pos.get("last_add_price") or entry_price)
                        if mark < entry_price + float(self.minervini_config.add_on_R) * r_value:
                            continue
                        if mark > last_add_price * (1.0 + float(self.minervini_config.add_on_max_extension)):
                            continue
                        df, _ = self._fetch_daily(code)
                        if df.empty:
                            continue
                        feats = compute_features(df)
                        vol20 = feats.get("vol20")
                        last_volume = feats.get("last_volume")
                        if vol20 is None or last_volume is None or not np.isfinite(vol20) or not np.isfinite(last_volume):
                            continue
                        if last_volume < vol20 * self.minervini_config.breakout_vol_mult_20:
                            continue
                        stop_price = float(pos.get("stop_price") or initial_stop)
                        risk_krw = float(self._equity_krw or 0.0) * float(self.minervini_config.risk_pct_of_equity) * float(
                            self.minervini_config.add_on_size_frac
                        )
                        max_cap = min(remaining_budget, float(PB1_MAX_POS_PCT) * float(tick_budget_krw))
                        qty_risk = risk_position_size(
                            entry_price=float(mark),
                            stop_price=stop_price,
                            risk_krw=risk_krw,
                            max_capital_krw=max_cap,
                            min_order_krw=min_order_krw,
                        )
                        qty_base = int(float(pos.get("qty") or 0) * float(self.minervini_config.add_on_size_frac))
                        qty_add = max(1, min(qty_risk, qty_base))
                        order_value = qty_add * float(mark)
                        if qty_add <= 0 or (min_order_krw > 0 and order_value < min_order_krw):
                            continue
                        if order_value > available_cash_krw:
                            continue
                        self._place_add_on(pos, qty=qty_add, price=float(mark))
                        planned_spent += order_value
                        remaining_budget = max(0.0, float(tick_budget_krw) - planned_spent)
        _emit_entry_summary(setup_ok_codes, orderable_candidates, drop_reason_counter)
        if not entry_decision_emitted:
            ok_count = len(setup_ok_codes)
            blocked_by = _normalize_entry_block_counts(drop_reason_counter)
            if entry_reason and entry_reason != "ok":
                blocked_by.update(_normalize_entry_block_reasons([entry_reason]))
            if entry_allowed and orderable_candidates:
                planned_total = sum(
                    float(cf.features.get("close") or 0.0) * float(cf.planned_qty or 0)
                    for cf in orderable_candidates
                )
                _emit_entry_decision(
                    "PLACE",
                    reason=None,
                    ok_setups=ok_count,
                    blocked_by=blocked_by,
                    orders=len(orderable_candidates),
                    total_krw=planned_total,
                )
            else:
                reason = entry_reason if entry_reason != "ok" else "NO_ORDER_INTENTS"
                _emit_entry_decision(
                    "SKIP",
                    reason=reason,
                    ok_setups=ok_count,
                    blocked_by=blocked_by,
                )
        self._pnl_snapshot(self._positions_with_meta(positions_for_exit))
        final_notes = final_notes or self._universe_as_of or "ok"
        self._log_reason_summary(final_notes)
        
        # ENTRY PASS 종료 계측
        entry_pass_buys = len(orderable_candidates) if 'orderable_candidates' in locals() else 0
        dt_total_entry = time.monotonic() - t0_entry
        
        logger.info(
            "[ENTRY][PIPE][END] trace=%s total_dt=%.2f minervini_dt=%.2f pb1_dt=%.2f rank_dt=%.2f build_dt=%.2f submit_dt=%.2f",
            trace_id,
            dt_total_entry,
            dt_minervini,
            dt_pb1_filter,
            dt_rank_pick,
            dt_order_build,
            dt_order_submit,
        )
        logger.info("[PASS][ENTRY][END] buys=%s skipped_dup=%s candidates=%s", 
                   entry_pass_buys, entry_pass_skipped, len(candidates) if 'candidates' in locals() else 0)
        
        # [PATCH] 요약 로그 추가
        candidates_ok = len(orderable_candidates) if 'orderable_candidates' in locals() else 0
        priced_ok = len([c for c in (orderable_candidates if 'orderable_candidates' in locals() else []) if float(c.features.get('entry_price') or 0.0) > 0.0])
        intents_created = int(submit_attempt_count) if 'submit_attempt_count' in locals() else 0
        intents_skipped = self._setup_reason_counter.most_common(3)
        logger.info(
            "[PB1][TICK_SUMMARY] order_candidates=%d priced_ok=%d attempted=%d api_submitted=%d accepted=%d filled=%d skipped_reason_top3=%s",
            candidates_ok,
            priced_ok,
            int(submit_attempt_count) if 'submit_attempt_count' in locals() else 0,
            int(api_submitted_count) if 'api_submitted_count' in locals() else 0,
            int(submit_success_count) if 'submit_success_count' in locals() else 0,
            int(filled_count) if 'filled_count' in locals() else 0,
            intents_skipped,
        )
        logger.info(
            "[TRADE][DATA_SUMMARY] precomputed_hits=%s short_fetch=%s long_fetch_blocked=%s kis_trade_daily_fetch=%s kis_trade_daily_blocked=%s",
            self._data_metrics.get("precomputed_hits", 0),
            self._data_metrics.get("short_fetch_count", 0),
            self._data_metrics.get("long_fetch_blocked_count", 0),
            self._data_metrics.get("kis_trade_daily_fetch_count_trade", self._data_metrics.get("kis_daily_fetch_count_trade", 0)),
            self._data_metrics.get("kis_trade_daily_blocked_count_trade", 0),
        )
        _set_run_summary_payload(
            scanned=len(scan_members),
            setup_ok=len(setup_ok_codes),
            relax_ok=len(setup_ok_codes),
            score_ok=len(self._debug_score_cut_codes),
            risk_ok=len(self._debug_risk_ok_codes),
            sized_ok=len(self._debug_sizing_ok_codes),
            buyable_ok=len(buyable_ok_codes),
            order_candidates=len(orderable_candidates),
            submitted=int(api_submitted_count) if 'api_submitted_count' in locals() else int(submit_success_count),
            blocked_reasons_counter=drop_reason_counter,
            no_trade_reason=(
                _primary_no_trade_reason(
                    drop_reason_counter,
                    ok_count=len(setup_ok_codes),
                    order_candidates=len(orderable_candidates),
                )
                if not orderable_candidates
                else None
            ),
        )
        self._debug_summary.update(
            {
                "order_candidate_codes": [c.code for c in orderable_candidates],
                "submit_attempt_count": int(submit_attempt_count),
                "api_submitted_count": int(api_submitted_count) if 'api_submitted_count' in locals() else 0,
                "accepted_count": int(accepted_count) if 'accepted_count' in locals() else 0,
                "filled_count": int(filled_count) if 'filled_count' in locals() else 0,
                "rejected_count": int(rejected_count) if 'rejected_count' in locals() else 0,
                "skipped_count": int(skipped_count) if 'skipped_count' in locals() else 0,
            }
        )
        
        # ✅ 스코어카드 JSONL 작성 (선택사항: 성능 영향 최소화)
        # try:
        #     self._write_entry_scorecard_jsonl()
        # except Exception as e:
        #     logger.warning("[PB1][SCORECARD_WRITE_FAIL] %s", type(e).__name__)
        
        self._log_tick_price_cache_summary()
        return self._finalize_run_result(status=final_status, notes=final_notes)
