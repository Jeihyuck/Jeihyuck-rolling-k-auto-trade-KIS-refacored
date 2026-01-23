from __future__ import annotations

import json
import logging
import os
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Any, Dict, Iterable, List

import numpy as np
import pandas as pd

from trader.botstate_paths import close_entry_orders_path
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
    PB1_MAX_ATR_PCT,
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
    PAPER_MAX_CAPITAL_KRW,
    PB1_ENTRY_WINDOW_START,
    PB1_ENTRY_OPEN_END,
    PB1_ENTRY_WINDOW_END,
    PB1_EXIT_WINDOW_START,
    PB1_EXIT_WINDOW_END,
    PB1_ALLOW_ADD_TO_EXISTING,
    PB1_PREOPEN_ORDER_TYPE,
    PB1_PREOPEN_LIMIT_BUFFER_PCT,
    PB1_VOL_MAX,
    PB1_VOLU_MAX,
    PB1_VOLU_MAX_INTRADAY,
    PB1_PULLBACK_MIN,
    PB1_PULLBACK_MAX,
    PB1_REQUIRE_BOTH,
    MIN_ORDER_KRW,
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
    resolve_market_window,
)
from trader.db.repos import FillsRepo, LedgerEventsRepo, OrdersRepo, PositionsRepo, UniverseRepo
from trader.data.ohlcv_provider import ChainOHLCVProvider, KISOHLCVProvider, KRXOHLCVProvider
from trader.kis_wrapper import KisAPI
from trader.ledger.store import LedgerStore
from trader.factors.liquidity_risk import gap_filter, liquidity_filter, range_filter, spread_proxy_filter
from trader.factors.regime import get_regime, risk_multiplier
from trader.factors.rs_rank import rank_rs
from trader.positioning.minervini_risk import calc_initial_stop, calc_position_size, update_exits
from trader.setups.vcp_pro import PriceTightRules, VolContractRules, find_pivot, is_vcp_ready, score_vcp
from trader.strategies.pb1_minervini_v2 import (
    MinerviniConfig,
    compute_features,
    compute_pivot,
    detect_vcp,
    entry_trigger,
    evaluate_filters,
    initial_stop,
    risk_position_size,
    score_setup,
    update_trailing_stop,
)
from trader.time_utils import now_kst
from trader.core_utils import _round_to_tick
from trader.eventlog import emit_event
from trader.utils.env import env_bool
from trader.utils.json_sanitize import to_jsonable
from trader.window_router import WindowDecision

logger = logging.getLogger(__name__)

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
    "open_order": "RATE_LIMIT",
    "today_buy_exists": "RATE_LIMIT",
    "duplicate_order": "RATE_LIMIT",
    "holding_position": "EXISTING_POSITION",
    "order_value_zero": "ORDER_VALUE_ZERO",
    "qty_zero": "QTY_ZERO",
    "universe_empty": "UNIVERSE_EMPTY",
}

_ORDER_SKIP_REASON_MAP = {
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
    "open_order": "ORDER_SKIP_RATE_LIMIT",
    "today_buy_exists": "ORDER_SKIP_RATE_LIMIT",
    "duplicate_order": "ORDER_SKIP_RATE_LIMIT",
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


@dataclass
class RunResult:
    status: str
    notes: str | None = None
    balance_api_calls: int = 0
    balance_cache_hits: int = 0
    balance_tick_cache_hits: int = 0


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


def resolve_pb1_phase(
    now: datetime,
    trading_day: bool,
    force_phase_env: str | None = None,
) -> tuple[str, str, str]:
    force_raw = (force_phase_env or "").strip().lower()
    if force_raw:
        if force_raw in {"entry", "exit", "verify", "manage", "idle"}:
            window = resolve_market_window(now, trading_day)
            return force_raw, "force", window
        logger.warning("[PB1][PHASE] invalid force phase=%s -> auto", force_raw)
    window = resolve_market_window(now, trading_day)
    if not trading_day:
        return "idle", "auto_non_trading_day", window
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
        window: WindowDecision,
        window_label: str,
        phase: str,
        dry_run: bool,
        env: str,
        run_id: str,
        now_kst_value: datetime | None = None,
        balance_snapshot: dict | None = None,
        balance_source: str | None = None,
        entry_allowed_this_tick: bool = True,
        entry_block_reason: str | None = None,
        preopen_max_new_positions: int = 0,
        universe_context: UniverseContext | None = None,
    ) -> None:
        self.universe_repo = universe_repo
        self.orders_repo = orders_repo
        self.fills_repo = fills_repo
        self.positions_repo = positions_repo
        self.ledger_repo = ledger_repo
        self.kis = kis
        self.window = window
        self.window_label = window_label
        self.phase = phase
        self.dry_run = dry_run
        self.env = env
        self.run_id = run_id
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
        self._setup_reason_counter: Counter[str] = Counter()
        self._now_kst = now_kst_value or now_kst()
        self._today = self._now_kst.date().isoformat()
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
        self.current_code: str | None = None
        self.top_candidates: list[dict[str, Any]] = []
        self.entry_enabled = bool(entry_allowed_this_tick)
        self.entry_block_reason = entry_block_reason
        self.preopen_max_new_positions = int(preopen_max_new_positions or 0)
        self.window_internal = self._resolve_window_internal()
        self.filter_thresholds = self._resolve_filter_thresholds()
        self.entry_capital_krw: float | None = None
        self.entry_usable_krw: float | None = None
        self.entry_tick_budget_krw: float | None = None
        self.target_new_positions: int | None = None
        self._budget_plan_meta: dict[str, Any] | None = None

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

    def _warn_once(self, key: str, message: str, *args: object) -> None:
        if key in self._warned_keys:
            return
        self._warned_keys.add(key)
        logger.warning(message, *args)

    def _is_intraday_threshold_window(self) -> bool:
        if self.phase not in {"prep", "entry"}:
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
        volu_max = PB1_VOLU_MAX_INTRADAY if intraday else PB1_VOLU_MAX
        thresholds = FilterThresholds(
            vol_contraction_max=PB1_VOL_MAX,
            volu_contraction_max=volu_max,
            pullback_min=PB1_PULLBACK_MIN,
            pullback_max=PB1_PULLBACK_MAX,
            require_both_contractions=PB1_REQUIRE_BOTH,
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

    def _record_setup_reasons(self, reasons: Iterable[str]) -> None:
        for reason in reasons:
            if not reason:
                continue
            self._setup_reason_counter[reason] += 1

    def _log_reason_summary(self, note: str | None = None) -> None:
        if not self._setup_reason_counter:
            return
        top = self._setup_reason_counter.most_common(3)
        logger.info(
            "[PB1][SETUP-REASONS] total_bad=%s top3=%s%s",
            sum(self._setup_reason_counter.values()),
            top,
            f" note={note}" if note else "",
        )

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
        for key in ("dnca_tot_amt", "ord_psbl_cash"):
            if key in summary:
                selected_key = key
                cash_value = self._to_float(summary.get(key))
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
        base_cash_krw: int,
        override_capital: float | None,
        reserve_pct: float,
    ) -> tuple[int, int, dict]:
        use_override = override_capital is not None and int(override_capital) > 0
        usable = max(int(base_cash_krw * (1 - reserve_pct)), 0)
        entry_capital = usable
        if use_override:
            entry_capital = min(entry_capital, int(override_capital))
        cap_limit = None
        cap_applied = False
        if CAP_CAP and CAP_CAP > 0:
            cap_limit = int(base_cash_krw * CAP_CAP) if CAP_CAP <= 1 else int(CAP_CAP)
            if cap_limit > 0 and entry_capital > cap_limit:
                entry_capital = cap_limit
                cap_applied = True
        clamp_meta = {}
        if (self.env or "").lower() != "real":
            cap = min(int(base_cash_krw), int(PAPER_MAX_CAPITAL_KRW))
            if entry_capital > cap:
                before = entry_capital
                entry_capital = cap
                clamp_meta = {"before": before, "cap": cap, "after": entry_capital}
                logger.info(
                    "[PB1][CAPITAL][CLAMP] before=%s cap=%s after=%s reason=paper_limit",
                    before,
                    cap,
                    entry_capital,
                )
        meta = {
            "use_override": use_override,
            "reserve_pct": reserve_pct,
            "cap_limit": cap_limit,
            "cap_applied": cap_applied,
            "clamp": clamp_meta,
            "base_cash": base_cash_krw,
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
        positions_rows: Iterable[dict],
    ) -> list[dict]:
        kis_holdings = self._parse_kis_holdings(holdings_rows)
        positions_by_code = {
            str(row.get("code") or "").zfill(6): row
            for row in positions_rows or []
            if row.get("code")
        }
        ledger_store = LedgerStore(LEDGER_BASE_DIR, env=self.env, run_id=self.run_id)
        ledger_positions = ledger_store.rebuild_positions_average_cost(lookback_days=LEDGER_LOOKBACK_DAYS)
        if ledger_positions:
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
                self.ledger_repo.append_event(
                    env=self.env,
                    run_id=self.run_id,
                    event_type="POSITION_ORPHANED",
                    ts=now_kst(),
                    code=code,
                    market=state.get("market"),
                    sid=sid,
                    mode=mode,
                    qty=total_qty,
                    ok=True,
                    reasons=["ledger_only"],
                    payload_json={"ledger_total_qty": total_qty},
                )
        ledger_by_code: dict[str, dict] = {}
        for (code, sid, mode), state in ledger_positions.items():
            if sid != 1:
                continue
            existing = ledger_by_code.get(code)
            if not existing or int(state.get("total_qty") or 0) > int(existing.get("total_qty") or 0):
                ledger_by_code[code] = {**state, "sid": sid, "mode": mode, "code": code}

        positions: list[dict] = []
        for code, holding in kis_holdings.items():
            ledger_state = ledger_by_code.get(code, {})
            pos_state = positions_by_code.get(code, {})
            qty = holding.get("qty") or 0
            avg = holding.get("avg_buy_price") or ledger_state.get("avg_buy_price") or 0.0
            positions.append(
                {
                    "code": code,
                    "sid": 1,
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
                }
            )
        return positions

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
                "[PB1][BUY][SKIP] code=%s reasons=%s",
                self._display_code(cf.code),
                reasons_out,
            )

    def _fetch_holdings_snapshot(self) -> dict:
        if self._balance_snapshot is not None:
            self.balance_tick_cache_hits += 1
            source = self._balance_snapshot_source or "tick_cache"
            logger.info("[BALANCE][CACHE] hit=True source=%s", source)
            self._balance_snapshot_source = "tick_cache"
            return self._balance_snapshot
        if not self.kis:
            return {}
        snap, source = self.kis.get_balance_cached(return_source=True)
        if source == "api":
            self.balance_api_calls += 1
        else:
            self.balance_cache_hits += 1
        logger.info("[BALANCE][CACHE] hit=%s source=%s", source != "api", source)
        self._balance_snapshot = snap
        return self._balance_snapshot

    def _client_order_key(self, code: str, mode: int, side: str, window_tag: str, stage: str) -> str:
        return f"{self.env}:{self.STRATEGY_NAME}:{self._today}:{code}:{side.upper()}"

    def _name_for_code(self, code: str | None) -> str | None:
        if not code:
            return None
        return self._code_name_map.get(str(code).zfill(6))

    def _display_code(self, code: str | None) -> str:
        if not code:
            return ""
        name = self._name_for_code(code)
        return f"{name}({code})" if name else str(code)

    def _with_name_reason(self, reasons: list[str] | None, code: str | None) -> list[str]:
        enriched = list(reasons or [])
        name = self._name_for_code(code)
        if name:
            enriched.append(f"name:{name}")
        return enriched

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
        self.ledger_repo.append_event(
            env=self.env,
            run_id=self.run_id,
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
            payload_json=payload_json or {},
        )

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
        return f"ORDER_FAIL_API(rt_cd={rt_cd},msg_cd={msg_cd},msg1={msg1})"

    def _log_setup(self, cf: CandidateFeature) -> None:
        prefix = "[PB1][SETUP-OK]" if cf.setup_ok else "[PB1][SETUP-BAD]"
        if not cf.setup_ok:
            self._record_setup_reasons(cf.reasons or ["unspecified_fail"])
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

    def _fetch_daily(self, code: str, count: int = 260) -> tuple[pd.DataFrame, Dict]:
        try:
            result = self.ohlcv_provider.get_ohlcv(code, count)
        except Exception:
            logger.exception("[PB1][DATA][FAIL] code=%s", code)
            return pd.DataFrame(), {"volume_missing": True, "source": "error", "mapped": {}}
        if not result or result.df is None or result.df.empty:
            return pd.DataFrame(), (result.meta if result else {"volume_missing": True, "source": "none"})
        df_norm = result.df.sort_values("date").tail(count)
        meta = result.meta or {}
        meta.setdefault("volume_missing", df_norm["volume"].isna().all() if "volume" in df_norm.columns else True)
        return df_norm, meta

    def _compute_candidates(self, members: Iterable[dict]) -> List[CandidateFeature]:
        candidates: List[CandidateFeature] = []
        required_candles = max(self.min_candles, 252)
        bench_df, _ = self._fetch_daily(RS_BENCHMARK, count=260)
        bench_close = bench_df["close"] if not bench_df.empty else pd.Series(dtype=float)
        rs_prices: dict[str, pd.Series] = {}
        for m in members:
            code = str(m.get("code") or "").zfill(6)
            market = m.get("market") or ""
            try:
                df, meta = self._fetch_daily(code, count=260)
                if df.empty:
                    reasons = ["data_empty"]
                    cf = CandidateFeature(
                        code=code,
                        market=market,
                        features={"reasons": reasons, "data_ok": False},
                        setup_ok=False,
                        reasons=reasons,
                        mode=1,
                        mode_reasons=["default_day_mode"],
                    )
                    candidates.append(cf)
                    continue
                if len(df) < required_candles:
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
                rs_prices[code] = df["close"].reset_index(drop=True)
            except Exception:
                logger.exception("[PB1][DAILY] fetch/normalize failed code=%s", code)
                continue
        if not candidates:
            return candidates
        rs_rank = rank_rs(
            rs_prices,
            bench_close,
            lookback_days=RS_LOOKBACK_DAYS,
            lookback2_days=RS_LOOKBACK2_DAYS,
            w1=RS_COMPOSITE_W1,
            w2=RS_COMPOSITE_W2,
        )
        rs_map = {row["ticker"]: row for row in rs_rank.to_dict(orient="records")}
        for i, cf in enumerate(candidates):
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
            cf.score = score_setup(cf.features, rs_percentile=rs_p, vcp_info=vcp_info, cfg=self.minervini_config)
            cf.features["score"] = cf.score
        return candidates

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
        for cf in candidates:
            clone = self._clone_candidate(cf)
            data_ok = bool(clone.features.get("data_ok"))
            if not data_ok:
                if log_results:
                    self._log_setup(clone)
                evaluated.append(clone)
                continue
            ok, reasons = evaluate_filters(clone.features, cfg)
            hard_reasons: list[str] = []
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

            soft_reasons = [r for r in reasons if r not in hard_reason_set]

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

            must_fail = bool(hard_reasons) or not score_ok or (not soft_mode and not ok)
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
        strict_thresholds = self._resolve_strict_thresholds()
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
        applied_min_score = float(PB1_MIN_SCORE_BASE)
        applied_require_both = bool(PB1_REQUIRE_BOTH)

        min_score_values: list[float] = []
        current = float(PB1_MIN_SCORE_BASE)
        floor = float(PB1_MIN_SCORE_FLOOR)
        step = max(float(PB1_MIN_SCORE_STEP), 1.0)
        max_passes = max(1, int(PB1_RELAX_MAX_PASSES))
        while current >= floor and len(min_score_values) < max_passes:
            min_score_values.append(current)
            current -= step
        if not min_score_values:
            min_score_values = [float(PB1_MIN_SCORE_BASE)]

        relax_passes: list[tuple[float, bool, str]] = [(score, bool(PB1_REQUIRE_BOTH), "score_relax") for score in min_score_values]
        if PB1_REQUIRE_BOTH and len(relax_passes) < max_passes + 1:
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
        ok_setups = [c for c in candidates if c.setup_ok]
        if not ok_setups:
            return float(PB1_MIN_SCORE_BASE), set()
        target = min(max(1, target_new_positions), len(ok_setups)) if target_new_positions > 0 else min(1, len(ok_setups))
        base_cut = float(PB1_MIN_SCORE_BASE)
        floor_cut = float(PB1_MIN_SCORE_FLOOR)
        step = max(float(PB1_MIN_SCORE_STEP), 1.0)
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
        for cf in ok_list:
            score_fallback = bool(cf.features.get("score_fallback"))
            if cf.code not in selected_codes and not score_fallback:
                cf.reasons.append("score_below_cut")
                cf.features["score_below_cut"] = True
        filtered: List[CandidateFeature] = []
        for cf in ok_list:
            if not cf.setup_ok:
                continue
            atr_pct = cf.features.get("atr_pct")
            value20 = cf.features.get("value20")
            if atr_pct is None or (isinstance(atr_pct, float) and atr_pct != atr_pct):
                cf.setup_ok = False
                cf.reasons.append("atr_pct_missing")
                continue
            if float(atr_pct) > float(PB1_MAX_ATR_PCT):
                cf.setup_ok = False
                cf.reasons.append("atr_pct_too_high")
                continue
            if value20 is None or (isinstance(value20, float) and value20 != value20):
                cf.setup_ok = False
                cf.reasons.append("value20_missing")
                continue
            if float(value20) < float(PB1_MIN_VALUE20):
                cf.setup_ok = False
                cf.reasons.append("liquidity_too_low")
                continue

            filtered.append(cf)

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
            quote = self.kis.get_quote_safe(cf.code, diag_mode=True) if self.kis else {}
            if isinstance(quote, dict) and quote.get("fallback_used") not in {None, "none", "quote"}:
                logger.info(
                    "[PB1][PRICE][FALLBACK] code=%s used=%s px=%s",
                    cf.code,
                    quote.get("fallback_used"),
                    quote.get("prpr") or quote.get("last"),
                )
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
        self._budget_plan_meta = {"risk_krw": risk_krw, "per_position_budget": per_position_budget}
        for cf in ranked:
            if not cf.setup_ok:
                continue
            order_px = self._to_float(cf.features.get("order_price")) or 0.0
            if order_px <= 0:
                cf.setup_ok = False
                cf.reasons.append("order_price_missing")
                continue
            df, _ = self._fetch_daily(cf.code, count=260)
            if df.empty:
                cf.setup_ok = False
                cf.reasons.append("stop_calc_fail")
                continue
            pivot_val = cf.features.get("pivot")
            tight_low = cf.features.get("tight_low")
            atr_val = cf.features.get("atr14")
            stop0 = calc_initial_stop(
                pivot=float(pivot_val) if pivot_val is not None else float("nan"),
                tight_low=float(tight_low) if tight_low is not None else None,
                atr=float(atr_val) if atr_val is not None else None,
                mode=INITIAL_STOP_MODE,
                entry=order_px,
                atr_mult=ATR_MULT,
            )
            per_share_risk = order_px - stop0
            if per_share_risk <= 0:
                cf.setup_ok = False
                cf.reasons.append("risk_invalid")
                continue
            budget_cap = min(per_position_budget, max_pos_krw)
            qty = calc_position_size(
                equity=equity_krw,
                risk_pct=float(RISK_PER_TRADE_PCT),
                entry=order_px,
                stop=stop0,
                risk_mult=risk_mult,
            )
            if budget_cap > 0:
                qty = min(qty, int(budget_cap // order_px))
            if min_order_krw > 0 and qty * order_px < min_order_krw:
                qty = 0
            if qty <= 0:
                cf.setup_ok = False
                cf.reasons.append("planned_qty_zero_or_min_order")
                continue
            cf.planned_qty = qty
            cf.planned_value = float(qty * order_px)
            cf.features["planned_cap"] = float(budget_cap)
            cf.features["planned_value"] = cf.planned_value
            cf.features["initial_stop"] = float(stop0)
            cf.features["stop_price"] = float(stop0)
            cf.client_order_key = self._client_order_key(cf.code, cf.mode, "BUY", "close", "PB1")
            logger.info(
                "[PB1][RANK] code=%s score=%.1f cap=%.0f qty=%s value=%.0f atr_pct=%.2f value20=%s tick_budget=%.0f",
                cf.code,
                float(cf.features.get("score") or 0.0),
                float(cf.features.get("planned_cap") or 0.0),
                cf.planned_qty,
                cf.planned_value,
                float(cf.features.get("atr_pct") or 0.0),
                cf.features.get("value20"),
                tick_budget,
            )

        return candidates

    def _mark_price(self, code: str) -> float | None:
        if self.kis:
            try:
                diag_mode = self.dry_run or self.phase == "verify" or (self.window and self.window.name == "diagnostic")
                quote = self.kis.get_quote_safe(code, diag_mode=diag_mode)
                if not isinstance(quote, dict):
                    self._warn_once(f"quote_non_dict:{code}", "[PB1][PRICE][WARN] code=%s non-dict quote", code)
                    return None
                price = quote.get("last")
                if price is None:
                    price = quote.get("stck_prpr") or quote.get("prpr")
                    if price is None:
                        self._warn_once(
                            f"quote_missing_last:{code}", "[PB1][PRICE][WARN] code=%s missing_last keys=%s", code, list(quote.keys())
                        )
                        return None
                try:
                    price_val = float(price)
                except Exception:
                    self._warn_once(f"quote_invalid_price:{code}", "[PB1][PRICE][WARN] code=%s invalid price=%s", code, price)
                    return None
                if quote.get("ask") is None or quote.get("bid") is None:
                    self._warn_once(
                        f"quote_missing_book:{code}",
                        "[PB1][PRICE][WARN] code=%s ask=%s bid=%s",
                        code,
                        quote.get("ask"),
                        quote.get("bid"),
                    )
                return price_val
            except Exception:
                self._warn_once(f"quote_fail:{code}", "[PB1][PRICE][FAIL] code=%s", code)
        return None

    def _resolve_price_with_fallback(self, code: str, *, ohlcv_close: float | None = None) -> tuple[float | None, str | None]:
        price = self._mark_price(code)
        if price is not None:
            return price, "quote"
        if code in self._balance_price_map:
            fallback_price = self._balance_price_map.get(code)
            if fallback_price:
                logger.info("[PB1][PRICE][FALLBACK] code=%s source=balance_prpr", code)
                return float(fallback_price), "balance_prpr"
        if ohlcv_close is not None and ohlcv_close > 0:
            logger.info("[PB1][PRICE][FALLBACK] code=%s source=ohlcv_close", code)
            return float(ohlcv_close), "ohlcv_close"
        logger.info("[PB1][PRICE][UNAVAILABLE] code=%s", code)
        return None, None

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
            return prpr, "prpr"
        if close and close > 0:
            return close, "daily_close"
        logger.info("[PB1][PRICE][UNAVAILABLE] code=%s", code)
        return None, None

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

    def _should_block_order(self, client_order_key: str) -> bool:
        if not client_order_key:
            return True
        return self.orders_repo.has_client_order_key(self.env, client_order_key)

    def _place_entry(self, cf: CandidateFeature) -> None:
        display_code = self._display_code(cf.code)
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
            "[PB1][ENTRY][WHY] code=%s reason_codes=%s reason_text=%s features_snapshot=%s stage=%s",
            display_code,
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
                return
            buffer_pct = max(float(PB1_PREOPEN_LIMIT_BUFFER_PCT), 0.0)
            limit_price = round_to_tick(float(base_price) * (1 + buffer_pct / 100))
            order_type = "LIMIT"
        record_price = float(limit_price or entry_price or 0.0)
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
                client_order_key=cf.client_order_key or "",
                request_json={"features": cf.features, "reasons": cf.reasons},
                status="CREATED",
            )
        except Exception:
            logger.exception("[PB1][ENTRY][DB_FAIL] code=%s", display_code)
            if not self.dry_run:
                raise
            return
        if not created:
            try:
                self._append_ledger_event(
                    event_type="ORDER_SKIP",
                    code=cf.code,
                    market=cf.market,
                    mode=cf.mode,
                    side="BUY",
                    qty=cf.planned_qty,
                    price=record_price,
                    client_order_key=cf.client_order_key,
                    ok=False,
                    reasons=["duplicate_order"],
                    stage="PB1-CLOSE",
                )
            except Exception:
                logger.exception("[PB1][LEDGER][SKIP_FAIL] code=%s", display_code)
            return
        try:
            self._append_ledger_event(
                event_type="ORDER_INTENT",
                code=cf.code,
                market=cf.market,
                mode=cf.mode,
                side="BUY",
                qty=cf.planned_qty,
                price=record_price,
                client_order_key=cf.client_order_key,
                ok=True,
                reasons=["entry"] + (cf.reasons or []),
                stage="PB1-CLOSE",
                payload_json={"features": cf.features},
            )
        except Exception:
            logger.exception("[PB1][LEDGER][INTENT_FAIL] code=%s", display_code)
            if not self.dry_run:
                raise
            return
        if self.dry_run:
            logger.info("[PB1][ENTRY-DRY] code=%s qty=%s key=%s order_id=%s", display_code, cf.planned_qty, cf.client_order_key, order_id)
            return
        if not self.kis:
            logger.warning("[PB1][ENTRY][SKIP] KIS missing code=%s", display_code)
            return
        emit_event(
            as_of=self._today,
            event="ORDER_SUBMIT",
            side="BUY",
            code=cf.code,
            qty=cf.planned_qty,
            price=float(limit_price or 0.0),
            order_type=order_type,
            client_order_key=cf.client_order_key,
        )
        resp = None
        kis_odno = None
        try:
            if order_type == "LIMIT":
                resp = self.kis.buy_stock_limit(cf.code, cf.planned_qty, float(limit_price))
            else:
                resp = self.kis.buy_stock_market(cf.code, cf.planned_qty)
            kis_odno = (resp.get("output") or {}).get("ODNO") if isinstance(resp, dict) else None
        except Exception:
            logger.exception("[PB1][ENTRY][FAIL] code=%s", display_code)
        self.orders_repo.mark_submitted(self.env, cf.client_order_key or "", kis_odno, resp if isinstance(resp, dict) else {"resp": resp})
        ok = bool(resp and isinstance(resp, dict) and resp.get("rt_cd") == "0")
        rt_cd = resp.get("rt_cd") if isinstance(resp, dict) else None
        msg_cd = resp.get("msg_cd") if isinstance(resp, dict) else None
        msg1 = resp.get("msg1") if isinstance(resp, dict) else None
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
        logger.info(
            "[PB1][ORDER][RESULT] side=BUY code=%s ok=%s reason=%s rt_cd=%s msg_cd=%s msg1=%s",
            display_code,
            int(ok),
            reason_code,
            rt_cd,
            msg_cd,
            msg1,
        )
        if resp and isinstance(resp, dict) and resp.get("rt_cd") == "0":
            self.orders_repo.mark_acked(self.env, kis_odno, resp)
            filled_at = now_kst()
            self.fills_repo.upsert_fill(
                env=self.env,
                run_id=self.run_id,
                order_id=order_id,
                kis_odno=kis_odno,
                trade_id=None,
                code=cf.code,
                market=cf.market,
                side="BUY",
                qty=cf.planned_qty,
                price=record_price,
                fee=0.0,
                tax=0.0,
                filled_at=filled_at,
                raw_json=resp,
            )
            self.positions_repo.apply_fill(
                env=self.env,
                strategy=self.STRATEGY_NAME,
                sid=1,
                mode=cf.mode,
                code=cf.code,
                market=cf.market,
                side="BUY",
                qty=cf.planned_qty,
                price=record_price,
                fee=0.0,
                tax=0.0,
                filled_at=filled_at,
            )
            entry_price = float(record_price or 0.0)
            base_id = f"{cf.code}:{self._today}:{cf.features.get('pivot')}"
            r_value = entry_price - float(cf.features.get("stop_price") or 0.0)
            fields = {
                "entry_ts": filled_at.isoformat(),
                "initial_stop": cf.features.get("initial_stop"),
                "stop_price": cf.features.get("stop_price"),
                "max_price": entry_price if entry_price > 0 else None,
                "pyramid_level": 0,
                "pivot": cf.features.get("pivot"),
                "last_add_price": entry_price if entry_price > 0 else None,
                "partial_exit_level": 0,
                "base_id": base_id,
                "setup_id": base_id,
                "tight_low": cf.features.get("tight_low"),
                "base_high": cf.features.get("base_high"),
                "entry_price": entry_price if entry_price > 0 else None,
                "r_value": r_value if r_value > 0 else None,
                "tp1_done": 0,
                "tp2_done": 0,
                "trail_mode": TRAIL_MODE,
                "last_trail_stop": cf.features.get("stop_price"),
                "regime_at_entry": (self._regime or {}).get("regime"),
                "risk_mult_at_entry": getattr(self, "_regime_risk_mult", None),
            }
            self.positions_repo.update_position_fields(
                env=self.env,
                strategy=self.STRATEGY_NAME,
                sid=1,
                mode=cf.mode,
                code=cf.code,
                fields=fields,
            )
        else:
            self.orders_repo.mark_error(self.env, cf.client_order_key or "", resp if isinstance(resp, dict) else {"resp": resp})

    def _place_add_on(self, pos: dict, *, qty: int, price: float) -> None:
        code = pos.get("code")
        if not code or qty <= 0:
            return
        display_code = self._display_code(code)
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
            return
        if not self.kis:
            logger.warning("[PB1][ADD][SKIP] KIS missing code=%s", display_code)
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
        if ok:
            self.orders_repo.mark_acked(self.env, kis_odno, resp)
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
                raw_json=resp,
            )
            self.positions_repo.apply_fill(
                env=self.env,
                strategy=self.STRATEGY_NAME,
                sid=1,
                mode=mode,
                code=code,
                market=pos.get("market"),
                side="BUY",
                qty=qty,
                price=fill_price,
                fee=0.0,
                tax=0.0,
                filled_at=filled_at,
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
        else:
            self.orders_repo.mark_error(self.env, client_key, resp if isinstance(resp, dict) else {"resp": resp})

    def _append_close_entry_record(self, payload: dict) -> None:
        path = close_entry_orders_path(self._today)
        payload = to_jsonable(payload)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, ensure_ascii=False) + "\n")

    def _place_entry_close(self, cf: CandidateFeature) -> None:
        display_code = self._display_code(cf.code)
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
            return
        cap = round_to_tick(base * (1 + cap_buffer_pct / 100.0))
        logger.info(
            "[PB1][CLOSE_ENTRY][WHY] code=%s base_from=%s base=%.2f cap=%s cap_buffer_pct=%.2f ref_daily_close=%s reasons=%s",
            display_code,
            base_from,
            base,
            cap,
            cap_buffer_pct,
            ref_daily_close,
            reasons,
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
                client_order_key=cf.client_order_key or "",
                request_json={
                    "features": cf.features,
                    "reasons": reasons,
                    "base_from": base_from,
                    "base": base,
                    "cap_buffer_pct": cap_buffer_pct,
                },
                status="CREATED",
            )
        except Exception:
            logger.exception("[PB1][CLOSE_ENTRY][DB_FAIL] code=%s", display_code)
            if not self.dry_run:
                raise
            return
        if not created:
            logger.info("[PB1][CLOSE_ENTRY][SKIP] code=%s reason=duplicate_order", display_code)
            return
        if self.dry_run:
            logger.info(
                "[PB1][CLOSE_ENTRY-DRY] code=%s qty=%s cap=%s key=%s order_id=%s",
                display_code,
                cf.planned_qty,
                cap,
                cf.client_order_key,
                order_id,
            )
            return
        if not self.kis:
            logger.warning("[PB1][CLOSE_ENTRY][SKIP] KIS missing code=%s", display_code)
            return
        emit_event(
            as_of=self._today,
            event="ORDER_SUBMIT",
            side="BUY",
            code=cf.code,
            qty=cf.planned_qty,
            price=float(cap),
            order_type="LIMIT",
            client_order_key=cf.client_order_key,
        )
        resp = None
        kis_odno = None
        try:
            resp = self.kis.buy_stock_limit(cf.code, cf.planned_qty, cap)
            kis_odno = (resp.get("output") or {}).get("ODNO") if isinstance(resp, dict) else None
        except Exception:
            logger.exception("[PB1][CLOSE_ENTRY][FAIL] code=%s", display_code)
        self.orders_repo.mark_submitted(self.env, cf.client_order_key or "", kis_odno, resp if isinstance(resp, dict) else {"resp": resp})
        ok = bool(resp and isinstance(resp, dict) and resp.get("rt_cd") == "0")
        rt_cd = resp.get("rt_cd") if isinstance(resp, dict) else None
        msg_cd = resp.get("msg_cd") if isinstance(resp, dict) else None
        msg1 = resp.get("msg1") if isinstance(resp, dict) else None
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
        logger.info(
            "[PB1][ORDER][RESULT] side=BUY code=%s ok=%s reason=%s rt_cd=%s msg_cd=%s msg1=%s",
            display_code,
            int(ok),
            reason_code,
            rt_cd,
            msg_cd,
            msg1,
        )
        if resp and isinstance(resp, dict) and resp.get("rt_cd") == "0":
            self.orders_repo.mark_acked(self.env, kis_odno, resp)
            self._append_close_entry_record(
                {
                    "order_id": order_id,
                    "code": cf.code,
                    "qty": cf.planned_qty,
                    "cap_price": cap,
                    "client_order_key": cf.client_order_key,
                    "kis_odno": kis_odno,
                    "created_at": now_kst().isoformat(),
                }
            )
        else:
            self.orders_repo.mark_error(self.env, cf.client_order_key or "", resp if isinstance(resp, dict) else {"resp": resp})

    def _plan_exit_event(self, pos: Dict, features: Dict[str, float], df: pd.DataFrame, window_tag: str) -> None:
        avg = pos.get("avg_buy_price")
        if not avg:
            return
        code = pos.get("code")
        display_code = self._display_code(code)
        market = pos.get("market")
        mode = pos.get("mode")
        sid = int(pos.get("sid") or 0)
        if sid != 1:
            return
        qty = pos.get("qty") or 0
        if qty <= 0:
            return
        kis_qty = pos.get("kis_qty", qty) or 0
        if kis_qty <= 0:
            emit_event(
                as_of=self._today,
                event="PB1_SELL_DECISION",
                code=str(code),
                qty=int(qty),
                avg_price=float(avg),
                mark=float(avg),
                pnl_pct=0.0,
                should_sell=False,
                reasons=["no_kis_holding"],
            )
            logger.info(
                "[PB1][SELL][DECISION] code=%s should_sell=0 reasons=%s",
                display_code,
                ["no_kis_holding"],
            )
            return
        mark, _source = self._resolve_price_with_fallback(
            code,
            ohlcv_close=self._to_float(features.get("close") or avg),
        )
        if mark is None:
            mark = avg
        ret_pct = ((mark - avg) / avg) * 100 if avg else 0.0
        client_key = self._client_order_key(code, mode, "SELL", window_tag, "exit")
        stop_price = pos.get("stop_price")
        if stop_price is None:
            stop_price = pos.get("initial_stop")
        max_price = pos.get("max_price")
        new_max = max(float(max_price or 0.0), float(mark or 0.0))
        fields: dict[str, float | int | str | None] = {"max_price": new_max}
        if stop_price is not None and pos.get("stop_price") is None:
            fields["stop_price"] = stop_price
        if avg:
            feats_for_trail = {**features, "stop_price": stop_price, "initial_stop": pos.get("initial_stop")}
            trail_stop, trail_reasons = update_trailing_stop(float(avg), new_max, df, feats_for_trail, self.minervini_config)
            if trail_stop and trail_stop > float(stop_price or 0.0):
                fields["stop_price"] = trail_stop
                fields["last_stop_update_ts"] = now_kst().isoformat()
                stop_price = trail_stop
        if fields:
            self.positions_repo.update_position_fields(
                env=self.env,
                strategy=self.STRATEGY_NAME,
                sid=sid,
                mode=mode,
                code=code,
                fields=fields,
            )

        stage = "EXIT"
        decision_reasons: list[str] = []
        should_sell = False
        last_volume = features.get("last_volume")
        vol50 = features.get("vol50")
        heavy_volume = bool(
            last_volume is not None
            and vol50 is not None
            and np.isfinite(last_volume)
            and np.isfinite(vol50)
            and vol50 > 0
            and last_volume >= vol50 * self.minervini_config.heavy_volume_mult
        )
        partial_level = int(pos.get("partial_exit_level") or 0)
        if stop_price is not None and mark <= float(stop_price):
            decision_reasons = ["STOP_HIT"]
            should_sell = True
            stage = "STOP"
        elif window_tag != "close":
            decision_reasons = ["sell_disabled"]
        else:
            close_px = features.get("close")
            ma50 = features.get("ma50")
            ma20 = features.get("ma20")
            entry_price = float(pos.get("entry_price") or avg)
            r_value = float(pos.get("r_value") or (entry_price - float(stop_price or 0.0)))
            failed_breakout = False
            pivot = pos.get("pivot")
            entry_ts = pos.get("entry_ts")
            if pivot and close_px is not None and entry_ts:
                try:
                    entry_date = datetime.fromisoformat(str(entry_ts)).date()
                    if (self._now_kst.date() - entry_date).days <= FAILED_BREAKOUT_EXIT_DAYS and close_px < float(pivot):
                        failed_breakout = True
                except ValueError:
                    failed_breakout = False
            state = {
                "entry_price": entry_price,
                "stop_price": float(stop_price or 0.0),
                "r_value": r_value,
                "qty": qty,
                "tp1_done": bool(pos.get("tp1_done")),
                "tp2_done": bool(pos.get("tp2_done")),
                "last_trail_stop": pos.get("last_trail_stop"),
            }
            orders = update_exits(
                state,
                last_price=float(mark),
                ma20=float(ma20) if ma20 is not None else None,
                atr=float(features.get("atr14") or 0.0),
                take_profit_r1=TAKE_PROFIT_R1,
                take_profit_r2=TAKE_PROFIT_R2,
                tp1_pct=TP1_SELL_PCT,
                tp2_pct=TP2_SELL_PCT,
                trail_mode=TRAIL_MODE,
                trail_step_after_r=TRAIL_STEP_AFTER_R,
                failed_breakout_days=FAILED_BREAKOUT_EXIT_DAYS,
                failed_breakout=failed_breakout,
            )
            if close_px is not None and ma50 is not None and close_px < ma50 and heavy_volume:
                decision_reasons = ["ma50_break_heavy_volume"]
                should_sell = True
                stage = "MA50_BREAK"
            elif int(pos.get("holding_days") or 0) >= int(MINERVINI_TIME_STOP_DAYS) and ret_pct < 2.0:
                decision_reasons = ["time_stop"]
                should_sell = True
                stage = "TIME_STOP"
            elif heavy_volume and ret_pct >= 25.0 and partial_level < 1:
                decision_reasons = ["climax_partial"]
                should_sell = True
                stage = "PARTIAL1"
            elif orders:
                order = orders[0]
                decision_reasons = [order.reason]
                should_sell = True
                stage = order.reason
                qty = min(qty, order.qty)
                fields = {}
                if order.reason == "TP1":
                    fields["tp1_done"] = 1
                if order.reason == "TP2":
                    fields["tp2_done"] = 1
                if state.get("stop_price") and state.get("stop_price") != stop_price:
                    fields["stop_price"] = state.get("stop_price")
                    fields["last_trail_stop"] = state.get("last_trail_stop")
                if fields:
                    self.positions_repo.update_position_fields(
                        env=self.env,
                        strategy=self.STRATEGY_NAME,
                        sid=sid,
                        mode=mode,
                        code=code,
                        fields=fields,
                    )
            else:
                decision_reasons = ["ok_hold"]

        emit_event(
            as_of=self._today,
            event="PB1_SELL_DECISION",
            code=str(code),
            qty=int(qty),
            avg_price=float(avg),
            mark=float(mark),
            pnl_pct=float(ret_pct),
            should_sell=bool(should_sell),
            reasons=decision_reasons,
        )
        logger.info(
            "[PB1][SELL][DECISION] code=%s should_sell=%s reasons=%s",
            display_code,
            int(should_sell),
            decision_reasons,
        )

        if not should_sell:
            return

        if stage.startswith("PARTIAL"):
            sell_qty = max(1, int(qty * 0.5))
            qty = min(qty, sell_qty)
            fields = {"partial_exit_level": partial_level + 1}
            self.positions_repo.update_position_fields(
                env=self.env,
                strategy=self.STRATEGY_NAME,
                sid=sid,
                mode=mode,
                code=code,
                fields=fields,
            )

        if decision_reasons and decision_reasons[0] in {"STOP_HIT", "FAILED_BREAKOUT"}:
            cooldown_until = (self._now_kst.date().isoformat() if REENTRY_COOLDOWN_DAYS <= 0 else (self._now_kst.date() + pd.Timedelta(days=REENTRY_COOLDOWN_DAYS)).date().isoformat())
            self.positions_repo.update_position_fields(
                env=self.env,
                strategy=self.STRATEGY_NAME,
                sid=sid,
                mode=mode,
                code=code,
                fields={"cooldown_until": cooldown_until},
            )

        if self._should_block_order(client_key):
            logger.info("[PB1][EXIT-SKIP] code=%s mode=%s reason=dup key=%s", display_code, mode, client_key)
            try:
                self._append_ledger_event(
                    event_type="EXIT_SKIP",
                    code=code,
                    market=market,
                    mode=mode,
                    side="SELL",
                    qty=qty,
                    price=mark,
                    client_order_key=client_key,
                    ok=False,
                    reasons=["duplicate_order"],
                    stage="EXIT",
                )
            except Exception:
                logger.exception("[PB1][LEDGER][EXIT_SKIP_FAIL] code=%s", display_code)
            return

        reasons = decision_reasons

        try:
            order_id, created = self.orders_repo.create_intent_idempotent(
                env=self.env,
                run_id=self.run_id,
                strategy=self.STRATEGY_NAME,
                sid=1,
                mode=mode,
                code=code,
                market=market,
                side="SELL",
                ord_type="MARKET",
                qty=qty,
                limit_price=mark,
                stage=stage,
                client_order_key=client_key,
                request_json={"reasons": reasons, "ret_pct": ret_pct},
                status="CREATED",
            )
        except Exception:
            logger.exception("[PB1][EXIT][DB_FAIL] code=%s", display_code)
            if not self.dry_run:
                raise
            return
        if not created:
            try:
                self._append_ledger_event(
                    event_type="EXIT_SKIP",
                    code=code,
                    market=market,
                    mode=mode,
                    side="SELL",
                    qty=qty,
                    price=mark,
                    client_order_key=client_key,
                    ok=False,
                    reasons=["duplicate_order"],
                    stage=stage,
                )
            except Exception as e:
                logger.exception("[PB1][LEDGER][EXIT_SKIP_FAIL] code=%s", display_code)
                return
        try:
            self._append_ledger_event(
                event_type="EXIT_INTENT",
                code=code,
                market=market,
                mode=mode,
                side="SELL",
                qty=qty,
                price=mark,
                client_order_key=client_key,
                ok=True,
                reasons=reasons,
                stage=stage,
                payload_json={"ret_pct": ret_pct},
            )
        except Exception:
            logger.exception("[PB1][LEDGER][EXIT_INTENT_FAIL] code=%s", display_code)
            if not self.dry_run:
                raise
            return
        logger.info(
            "[PB1][EXIT][WHY] code=%s qty=%s mark=%s avg=%s ret_pct=%.2f exit_reason_codes=%s exit_reason_text=%s holding_days=%s stage=%s",
            display_code,
            qty,
            mark,
            avg,
            ret_pct,
            reasons,
            ", ".join(reasons),
            pos.get("holding_days") or 0,
            stage,
        )
        if self.dry_run:
            logger.info("[PB1][EXIT-DRY] code=%s qty=%s key=%s order_id=%s", display_code, qty, client_key, order_id)
            return
        if not self.kis:
            logger.warning("[PB1][EXIT][SKIP] kis missing code=%s", display_code)
            return
        emit_event(
            as_of=self._today,
            event="ORDER_SUBMIT",
            side="SELL",
            code=code,
            qty=qty,
            price=float(mark),
            order_type="MARKET",
            client_order_key=client_key,
        )
        resp = None
        kis_odno = None
        try:
            resp = self.kis.sell_stock_market(code, qty)
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
        reason_code = self._format_order_result_reason(resp if isinstance(resp, dict) else None)
        logger.info(
            "[PB1][ORDER][RESULT] side=SELL code=%s ok=%s reason=%s rt_cd=%s msg_cd=%s msg1=%s",
            display_code,
            int(ok),
            reason_code,
            rt_cd,
            msg_cd,
            msg1,
        )
        if resp and isinstance(resp, dict) and resp.get("rt_cd") == "0":
            self.orders_repo.mark_acked(self.env, kis_odno, resp)
            filled_at = now_kst()
            self.fills_repo.upsert_fill(
                env=self.env,
                run_id=self.run_id,
                order_id=order_id,
                kis_odno=kis_odno,
                trade_id=None,
                code=code,
                market=market,
                side="SELL",
                qty=qty,
                price=mark,
                fee=0.0,
                tax=0.0,
                filled_at=filled_at,
                raw_json=resp,
            )
            self.positions_repo.apply_fill(
                env=self.env,
                strategy=self.STRATEGY_NAME,
                sid=1,
                mode=mode,
                code=code,
                market=market,
                side="SELL",
                qty=qty,
                price=mark,
                fee=0.0,
                tax=0.0,
                filled_at=filled_at,
            )
        else:
            self.orders_repo.mark_error(self.env, client_key, resp if isinstance(resp, dict) else {"resp": resp})

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
                }
            )
        return enriched

    def _run_exit_always(
        self,
        *,
        positions: list[dict],
        holdings_rows: list[dict],
        marks_fallback: dict[str, float],
    ) -> list[dict]:
        holdings = list(holdings_rows or [])
        pos_list = self._positions_with_meta(positions)
        for pos in pos_list:
            df, _ = self._fetch_daily(pos["code"], count=260)
            if df.empty:
                continue
            try:
                features = compute_features(df)
            except ValueError:
                continue
            features["market"] = pos.get("market") or ""
            marks_fallback[pos["code"]] = features.get("close") or pos.get("avg_buy_price") or 0.0
            window_tag = "close" if self.window_internal == "close" else "manage"
            self._plan_exit_event(pos, features, df, window_tag)
        return positions

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
        open_orders = self.orders_repo.get_open_orders(self.env)
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
            today = self._now_kst.date().isoformat()
            members = self.universe_repo.get_universe_members(self.env, self.UNIVERSE_STRATEGY, today)
            self._universe_as_of = today if members else None
            if not members:
                members = self.universe_repo.get_latest_universe_members(self.env, self.UNIVERSE_STRATEGY)
                if members:
                    self._universe_as_of = members[0].get("as_of_date")
        self._code_name_map = {
            str(m.get("code") or "").zfill(6): (m.get("meta_json") or {}).get("name")
            for m in members or []
            if m.get("code")
        }
        return members

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

    def run(self) -> RunResult:
        self._warned_keys.clear()
        self._setup_reason_counter.clear()
        self.current_code = None
        self.top_candidates = []
        final_status = "OK"
        final_notes: str | None = None
        entry_allowed = self.entry_enabled
        entry_reason = self.entry_block_reason or ("entry_disabled" if not entry_allowed else "ok")
        entry_summary_emitted = False
        entry_decision_emitted = False
        entry_cutoff_dt, entry_cutoff_raw = self._resolve_entry_cutoff()
        entry_phase = self.phase in {"prep", "entry"}
        max_positions = int(PB1_MAX_POSITIONS)
        target_new_positions_raw = self._int_env("PB1_TARGET_NEW_POSITIONS", max_positions)
        min_order_krw = float(MIN_ORDER_KRW)
        entry_capital_krw = 0.0
        skip_entry_scan = False
        if self.preopen_max_new_positions > 0 and (self.window_label or "").lower() == "preopen":
            target_new_positions_raw = min(target_new_positions_raw, self.preopen_max_new_positions)
        if not entry_allowed:
            logger.warning("[PB1][ENTRY_DISABLED] entry_allowed=0 reason=%s -> skip new entries", entry_reason)
        if self.phase == "verify":
            entry_allowed = False
            entry_reason = "phase_verify"
        if self.phase in {"manage", "exit", "idle"}:
            entry_allowed = False
            entry_reason = f"phase_{self.phase}"
        logger.info(
            "[PB1][RUN] window=%s window_internal=%s phase=%s dry_run=%s env=%s",
            self.window_label,
            self.window_internal,
            self.phase,
            self.dry_run,
            self.env,
        )
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

        if self.phase in {"prep", "entry"} and self._now_kst > entry_cutoff_dt:
            skip_entry_scan = True
            entry_allowed = False
            entry_reason = "entry_cutoff"
            logger.info(
                "[PB1][SKIP_ENTRY] reason=entry_cutoff now=%s cutoff=%s",
                self._now_kst.isoformat(),
                entry_cutoff_dt.isoformat(),
            )
            if self.phase in {"prep", "entry"}:
                final_status = "SKIPPED"
                final_notes = "entry_cutoff"

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
                top_drop_reasons=drop_reason_counter.most_common(5),
            )
            entry_summary_emitted = True

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
        base_cash_krw = min(total_cash_krw, order_possible_cash_krw)
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
            budget_pct = min(max(float(PB1_ENTRY_BUDGET_PCT_PER_TICK), 0.0), 1.0)
            tick_budget_krw = int(entry_capital_krw * budget_pct)
            self.entry_tick_budget_krw = float(tick_budget_krw)
            logger.info(
                "[PB1][CAPITAL] mode=%s override=%s total_cash=%s order_possible_cash=%s base_cash=%s reserve=%.2f usable=%s use_override=%s entry_capital=%s cap_limit=%s tick_budget=%s tick_budget_pct=%.2f source=%s",
                PB1_CAPITAL_MODE,
                override_capital,
                total_cash_krw,
                order_possible_cash_krw,
                base_cash_krw,
                reserve_pct,
                entry_usable_krw,
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
        else:
            self.entry_capital_krw = 0.0
            self.entry_usable_krw = float(available_cash_krw)
            self.entry_tick_budget_krw = 0.0
            tick_budget_krw = 0.0
        positions = self.positions_repo.list_positions(self.env, self.STRATEGY_NAME)
        if not positions and holdings_rows:
            bootstrapped = self.positions_repo.bootstrap_from_kis_holdings(
                env=self.env,
                strategy=self.STRATEGY_NAME,
                sid=1,
                mode=1,
                holdings=holdings_rows,
            )
            logger.info("[PB1][BOOTSTRAP] holdings_count=%s inserted=%s", len(holdings_rows), bootstrapped)
            positions = self.positions_repo.list_positions(self.env, self.STRATEGY_NAME)
        cooldown_map: dict[str, str] = {
            str(p.get("code") or "").zfill(6): str(p.get("cooldown_until"))
            for p in positions
            if p.get("cooldown_until")
        }
        positions_for_exit = self._build_positions_from_kis(holdings_rows, positions)
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
            "[PB1][RUN-START] PB1_MAX_POSITIONS=%s PB1_TARGET_NEW_POSITIONS=%s PB1_ENTRY_CAPITAL_KRW=%.0f MIN_ORDER_KRW=%.0f ENTRY_CUTOFF_TIME=%s EXISTING_POSITIONS_COUNT=%s AVAILABLE_CASH_KRW=%s TICK_BUDGET_KRW=%s ADD_TO_EXISTING=%s",
            max_positions,
            target_new_positions,
            entry_capital_krw,
            min_order_krw,
            entry_cutoff_raw,
            existing_positions_count,
            available_cash_krw,
            tick_budget_krw,
            allow_add_to_existing,
        )
        holdings = list(holdings_rows or [])
        if not holdings and self.kis:
            logger.info("[PB1][HOLDINGS] empty_balance_snapshot -> skip extra fetch")
        marks_fallback: Dict[str, float] = {}
        positions_for_exit = self._run_exit_always(positions=positions_for_exit, holdings_rows=holdings, marks_fallback=marks_fallback)
        if self.phase == "exit":
            logger.info("[PB1][EXIT] entry_skipped=1")
            return RunResult(
                status=final_status,
                notes=final_notes or "exit_phase",
                balance_api_calls=self.balance_api_calls,
                balance_cache_hits=self.balance_cache_hits,
                balance_tick_cache_hits=self.balance_tick_cache_hits,
            )
        open_orders = self.orders_repo.get_open_orders(self.env)
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
            return RunResult(
                status=final_status,
                notes=final_notes,
                balance_api_calls=self.balance_api_calls,
                balance_cache_hits=self.balance_cache_hits,
                balance_tick_cache_hits=self.balance_tick_cache_hits,
            )

        members = self._load_universe()
        emit_event(
            as_of=self._today,
            event="PB1_UNIVERSE_STATUS",
            ok=bool(members),
            universe_members=len(members),
            universe_as_of=self._universe_as_of,
        )
        if not members:
            note = "universe_empty"
            logger.error(
                "[PB1][UNIVERSE][EMPTY] env=%s strategy=%s path=%s members_count=%s action_required=universe_build",
                self.env,
                self.UNIVERSE_STRATEGY,
                self._universe_path,
                len(members),
            )
            self._log_reason_summary("universe_empty")
            entry_reason = "universe_empty"
            entry_allowed = False
            logger.info("[PB1][ENTRY_BLOCKED] reason=%s entry_allowed=0", entry_reason)
            if self.phase in {"prep", "entry"}:
                _emit_entry_summary([], [], Counter())
                _emit_entry_decision(
                    "SKIP",
                    reason="UNIVERSE_EMPTY",
                    ok_setups=0,
                    blocked_by=_normalize_entry_block_reasons([entry_reason]),
                )
                return RunResult(
                    status="SKIPPED",
                    notes=note,
                    balance_api_calls=self.balance_api_calls,
                    balance_cache_hits=self.balance_cache_hits,
                    balance_tick_cache_hits=self.balance_tick_cache_hits,
                )
            skip_entry_scan = True

        code_market = {m.get("code"): m.get("market") for m in members}
        candidates: List[CandidateFeature] = []
        selected_tier = "tier1"
        selected_thresholds = self.filter_thresholds
        all_reason_counts: Counter[str] = Counter()
        tiers_tried: list[str] = []
        relax_passes_used = 0
        applied_min_score = float(PB1_MIN_SCORE_BASE)
        applied_require_both = bool(PB1_REQUIRE_BOTH)
        setup_ok_codes: list[str] = []
        drop_reason_counter: Counter[str] = Counter()
        drop_examples: Dict[str, list[str]] = {}
        after_risk_check_count = 0
        after_buyable_check_count = 0
        after_dedup_count = 0
        orderable_candidates: list[CandidateFeature] = []
        if self.phase in {"prep", "entry"} and not skip_entry_scan:
            candidates = self._compute_candidates(members)
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
            if not any(c.setup_ok for c in candidates):
                self._apply_score_fallback(candidates)
            setup_ok_codes = [c.code for c in candidates if c.setup_ok]
            candidates = self._size_positions(candidates)
            ok_after_risk = sorted(
                [c for c in candidates if c.setup_ok],
                key=lambda c: float(c.features.get("score") or 0.0),
                reverse=True,
            )
            after_risk_check_count = len(ok_after_risk)
            self.top_candidates = [
                {
                    "code": cf.code,
                    "cap": float(cf.features.get("planned_cap") or 0.0),
                    "qty": int(cf.planned_qty or 0),
                }
                for cf in ok_after_risk
            ]
            dropped_after_risk = {c.code for c in candidates if c.code in setup_ok_codes and not c.setup_ok}
            for cf in candidates:
                if cf.code in dropped_after_risk:
                    for reason in cf.reasons or ["unspecified_fail"]:
                        self._record_drop(drop_reason_counter, drop_examples, reason, cf.code)

            new_position_limit = min(target_new_positions, len(ok_after_risk)) if ok_after_risk else target_new_positions
            if isinstance(self._budget_plan_meta, dict) and self._budget_plan_meta.get("effective_target"):
                new_position_limit = min(new_position_limit, int(self._budget_plan_meta["effective_target"]))
            held_codes = {p.get("code") for p in existing_positions if p.get("code")}
            open_orders = self.orders_repo.get_open_orders(self.env)
            open_buy_codes = {row.get("code") for row in open_orders if str(row.get("side") or "").upper() == "BUY"}
            today_orders = self.orders_repo.list_today_orders(self.env, side="BUY")
            today_buy_codes = {row.get("code") for row in today_orders if row.get("code")}
            today_spent = 0.0
            for row in today_orders:
                qty = float(row.get("qty") or 0)
                limit_price = row.get("limit_price")
                if limit_price is None:
                    limit_price = (row.get("request_json") or {}).get("features", {}).get("close")
                today_spent += qty * float(limit_price or 0.0)
            planned_spent = today_spent
            for cf in ok_after_risk:
                self.current_code = cf.code
                close_price = float(cf.features.get("close") or 0.0)
                order_price = float(cf.features.get("order_price") or close_price or 0.0)
                planned_cap = float(cf.features.get("planned_cap") or (order_price * float(cf.planned_qty or 0)))
                if cf.planned_qty <= 0:
                    self._record_drop(drop_reason_counter, drop_examples, "qty_zero", cf.code)
                    logger.info(
                        "[PB1][SKIP] code=%s reason=qty_zero cap=%.0f close=%.0f min_order=%.0f",
                        cf.code,
                        planned_cap,
                        close_price,
                        min_order_krw,
                    )
                    self._log_order_skip(cf, ["qty_zero"], "PB1-CLOSE")
                    self._emit_buy_decision(
                        cf,
                        order_value=0.0,
                        reasons=["qty_zero"],
                        entry_allowed=entry_allowed,
                        entry_reason=entry_reason,
                    )
                    continue
                if min_order_krw > 0 and planned_cap < min_order_krw:
                    self._record_drop(drop_reason_counter, drop_examples, "cap_below_min_order", cf.code)
                    logger.info(
                        "[PB1][SKIP] code=%s reason=cap_below_min_order cap=%.0f close=%.0f min_order=%.0f",
                        cf.code,
                        planned_cap,
                        close_price,
                        min_order_krw,
                    )
                    self._log_order_skip(cf, ["cap_below_min_order"], "PB1-CLOSE")
                    self._emit_buy_decision(
                        cf,
                        order_value=order_price * float(cf.planned_qty or 0),
                        reasons=["cap_below_min_order"],
                        entry_allowed=entry_allowed,
                        entry_reason=entry_reason,
                    )
                    continue
                if order_price > 0 and planned_cap < order_price:
                    self._record_drop(drop_reason_counter, drop_examples, "cap_below_one_share", cf.code)
                    logger.info(
                        "[PB1][SKIP] code=%s reason=cap_below_one_share cap=%.0f close=%.0f min_order=%.0f",
                        cf.code,
                        planned_cap,
                        close_price,
                        min_order_krw,
                    )
                    self._log_order_skip(cf, ["cap_below_one_share"], "PB1-CLOSE")
                    self._emit_buy_decision(
                        cf,
                        order_value=order_price * float(cf.planned_qty or 0),
                        reasons=["cap_below_one_share"],
                        entry_allowed=entry_allowed,
                        entry_reason=entry_reason,
                    )
                    continue
                order_value = order_price * float(cf.planned_qty or 0)
                if not allow_add_to_existing and cf.code in held_codes:
                    self._record_drop(drop_reason_counter, drop_examples, "holding_position", cf.code)
                    self._log_order_skip(cf, ["holding_position"], "PB1-CLOSE")
                    self._emit_buy_decision(
                        cf,
                        order_value=order_value,
                        reasons=["holding_position"],
                        entry_allowed=entry_allowed,
                        entry_reason=entry_reason,
                    )
                    continue
                if cf.code in open_buy_codes:
                    self._record_drop(drop_reason_counter, drop_examples, "open_order", cf.code)
                    self._log_order_skip(cf, ["open_order"], "PB1-CLOSE")
                    self._emit_buy_decision(
                        cf,
                        order_value=order_value,
                        reasons=["open_order"],
                        entry_allowed=entry_allowed,
                        entry_reason=entry_reason,
                    )
                    continue
                if cf.code in today_buy_codes:
                    self._record_drop(drop_reason_counter, drop_examples, "today_buy_exists", cf.code)
                    self._log_order_skip(cf, ["today_buy_exists"], "PB1-CLOSE")
                    self._emit_buy_decision(
                        cf,
                        order_value=order_value,
                        reasons=["today_buy_exists"],
                        entry_allowed=entry_allowed,
                        entry_reason=entry_reason,
                    )
                    continue
                cooldown_until = cooldown_map.get(cf.code)
                if cooldown_until and str(cooldown_until) >= self._today:
                    self._record_drop(drop_reason_counter, drop_examples, "cooldown_active", cf.code)
                    self._log_order_skip(cf, ["cooldown_active"], "PB1-CLOSE")
                    self._emit_buy_decision(
                        cf,
                        order_value=order_value,
                        reasons=["cooldown_active"],
                        entry_allowed=entry_allowed,
                        entry_reason=entry_reason,
                    )
                    continue
                if not allow_add_to_existing and self._should_block_order(cf.client_order_key or ""):
                    self._record_drop(drop_reason_counter, drop_examples, "duplicate_order", cf.code)
                    self._log_order_skip(cf, ["duplicate_order"], "PB1-CLOSE")
                    self._emit_buy_decision(
                        cf,
                        order_value=order_value,
                        reasons=["duplicate_order"],
                        entry_allowed=entry_allowed,
                        entry_reason=entry_reason,
                    )
                    continue
                if ENTRY_MODE == "CLOSE" and self.window_internal != "close":
                    self._record_drop(drop_reason_counter, drop_examples, "entry_mode_close_only", cf.code)
                    self._log_order_skip(cf, ["entry_mode_close_only"], "PB1-CLOSE")
                    self._emit_buy_decision(
                        cf,
                        order_value=order_value,
                        reasons=["entry_mode_close_only"],
                        entry_allowed=entry_allowed,
                        entry_reason=entry_reason,
                    )
                    continue
                if ENTRY_MODE == "INTRADAY" and self.window_internal == "close":
                    self._record_drop(drop_reason_counter, drop_examples, "entry_mode_intraday_only", cf.code)
                    self._log_order_skip(cf, ["entry_mode_intraday_only"], "PB1-CLOSE")
                    self._emit_buy_decision(
                        cf,
                        order_value=order_value,
                        reasons=["entry_mode_intraday_only"],
                        entry_allowed=entry_allowed,
                        entry_reason=entry_reason,
                    )
                    continue
                last_price = float(cf.features.get("last_price") or order_price or close_price or 0.0)
                last_volume = float(cf.features.get("last_volume") or 0.0)
                trigger_ok, trigger_info = entry_trigger(
                    cf.features,
                    last_price=last_price,
                    last_volume=last_volume,
                    cfg=self.minervini_config,
                )
                if not trigger_ok:
                    cf.features["entry_trigger"] = trigger_info
                    self._record_drop(drop_reason_counter, drop_examples, "pivot_breakout_fail", cf.code)
                    self._log_order_skip(cf, ["pivot_breakout_fail"], "PB1-CLOSE")
                    self._emit_buy_decision(
                        cf,
                        order_value=order_value,
                        reasons=["pivot_breakout_fail"],
                        entry_allowed=entry_allowed,
                        entry_reason=entry_reason,
                    )
                    continue
                df, _ = self._fetch_daily(cf.code, count=260)
                if df.empty:
                    self._record_drop(drop_reason_counter, drop_examples, "stop_calc_fail", cf.code)
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
                reasons: list[str] = []
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
                    for reason in reasons:
                        self._record_drop(drop_reason_counter, drop_examples, reason, cf.code)
                    self._log_order_skip(cf, reasons, "PB1-CLOSE")
                    self._emit_buy_decision(
                        cf,
                        order_value=order_value,
                        reasons=reasons,
                        entry_allowed=entry_allowed,
                        entry_reason=entry_reason,
                    )
                    continue
                orderable_candidates.append(cf)
                self._emit_buy_decision(
                    cf,
                    order_value=order_value,
                    reasons=[],
                    entry_allowed=entry_allowed,
                    entry_reason=entry_reason,
                )
                planned_spent += order_value
                if len(orderable_candidates) >= new_position_limit:
                    break

            after_buyable_check_count = len(orderable_candidates)
            after_dedup_count = len(orderable_candidates)

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
            logger.info(
                "[PB1][CANDIDATES][SNAPSHOT] setup_ok_count=%s setup_ok_sample=%s after_risk_check_count=%s after_buyable_check_count=%s after_dedup_count=%s drop_reasons_topN=%s drop_examples=%s",
                len(setup_ok_codes),
                setup_ok_codes[:3],
                after_risk_check_count,
                after_buyable_check_count,
                after_dedup_count,
                drop_reason_counter.most_common(3),
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
            self._log_fail_reason_breakdown(candidates, note="post_filter")
            _emit_entry_summary(setup_ok_codes, orderable_candidates, drop_reason_counter)
            if not candidates or ok_count == 0:
                top_reasons = all_reason_counts.most_common(3)
                final_status = "NO_TRADE"
                final_notes = f"no_candidates:{top_reasons or 'none'}"
                logger.info(
                    "[PB1][NO_TRADE] reason=no_candidates tiers_tried=%s tier=%s total=%s relax_passes=%s min_score=%.1f top_reasons=%s",
                    tiers_tried or ["none"],
                    selected_tier,
                    len(candidates),
                    relax_passes_used,
                    applied_min_score,
                    top_reasons or "none",
                )
                if self.phase in {"prep", "entry"}:
                    self._log_reason_summary(final_notes)
                    _emit_entry_summary(setup_ok_codes, orderable_candidates, drop_reason_counter)
                    _emit_entry_decision(
                        "SKIP",
                        reason="NO_FINAL_SETUPS",
                        ok_setups=ok_count,
                        blocked_by=_normalize_entry_block_counts(drop_reason_counter),
                    )
                    return RunResult(
                        status=final_status,
                        notes=final_notes,
                        balance_api_calls=self.balance_api_calls,
                        balance_cache_hits=self.balance_cache_hits,
                        balance_tick_cache_hits=self.balance_tick_cache_hits,
                    )
            if self.phase in {"entry"}:
                if not entry_allowed:
                    logger.info("[PB1][ENTRY][SKIP] entry_allowed=False")
                orderable_candidates = orderable_candidates if entry_allowed else []
                if ok_count > 0 and not orderable_candidates:
                    no_orders_reasons: list[str] = []
                    if not entry_allowed:
                        no_orders_reasons.append("entry_disabled")
                    if skip_entry_scan:
                        no_orders_reasons.append("entry_cutoff")
                    if max_positions - existing_positions_count <= 0:
                        no_orders_reasons.append("max_positions")
                    if target_new_positions <= 0:
                        no_orders_reasons.append("target_new_positions_zero")
                    if tick_budget_krw <= 0:
                        no_orders_reasons.append("tick_budget_zero")
                    if available_cash_krw <= 0:
                        no_orders_reasons.append("available_cash_zero")
                    if min_order_krw > 0 and after_buyable_check_count == 0:
                        no_orders_reasons.append("min_order_krw")
                    if not no_orders_reasons:
                        no_orders_reasons.append("exhausted_candidates")
                    final_status = "NO_TRADE"
                    final_notes = f"no_orders:{no_orders_reasons or 'none'}"
                    logger.info(
                        "[PB1][NO_TRADE] reason=no_orders no_orders_reason=%s",
                        no_orders_reasons or ["none"],
                    )
                    if self.phase in {"prep", "entry"}:
                        self._log_reason_summary(final_notes)
                        _emit_entry_summary(setup_ok_codes, orderable_candidates, drop_reason_counter)
                        blocked_by = _normalize_entry_block_counts(drop_reason_counter)
                        blocked_by.update(_normalize_entry_block_reasons(no_orders_reasons))
                        _emit_entry_decision(
                            "SKIP",
                            reason="NO_ORDER_INTENTS",
                            ok_setups=ok_count,
                            blocked_by=blocked_by,
                        )
                        return RunResult(
                            status=final_status,
                            notes=final_notes,
                            balance_api_calls=self.balance_api_calls,
                            balance_cache_hits=self.balance_cache_hits,
                            balance_tick_cache_hits=self.balance_tick_cache_hits,
                        )
                if orderable_candidates:
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
                for cf in orderable_candidates:
                    if self.window_internal == "close":
                        self._place_entry_close(cf)
                    else:
                        self._place_entry(cf)
                if entry_allowed and self.phase == "entry" and allow_add_to_existing:
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
                        df, _ = self._fetch_daily(code, count=260)
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
        return RunResult(
            status=final_status,
            notes=final_notes,
            balance_api_calls=self.balance_api_calls,
            balance_cache_hits=self.balance_cache_hits,
            balance_tick_cache_hits=self.balance_tick_cache_hits,
        )
