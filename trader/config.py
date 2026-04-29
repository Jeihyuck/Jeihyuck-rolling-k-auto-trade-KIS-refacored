# -*- coding: utf-8 -*-
"""공용 설정 및 환경 파싱 모듈.

trader.py가 분리되어도 모든 전략/유틸이 동일한 설정을 참조할 수 있도록
CONFIG와 파생 상수를 한 곳에 모았다.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, time as dtime
from pathlib import Path
from typing import Dict
from zoneinfo import ZoneInfo

from trader.runtime_paths import ensure_not_repo_tracked_path, get_cache_root, runtime_path
from trader.utils.env import env_bool, resolve_mode, TRUE_VALUES, FALSE_VALUES

# =========================
# [CONFIG] .env 없이도 동작
# - 아래 값을 기본으로 사용
# - (선택) 동일 키를 환경변수로 넘기면 override
# =========================
CONFIG = {
    "SELL_FORCE_TIME": "14:40",
    "SELL_ALL_BALANCES_AT_CUTOFF": "false",  # "true"면 커트오프에 전체 잔고 포함 강제매도 루틴 사용
    "API_RATE_SLEEP_SEC": "0.5",
    "FORCE_SELL_PASSES_CUTOFF": "2",
    "FORCE_SELL_PASSES_CLOSE": "4",
    "FORCE_SELL_BLOCKED_LOTS": "0",
    "PARTIAL1": "0.5",
    "PARTIAL2": "0.3",
    "TRAIL_PCT": "0.02",
    "FAST_STOP": "0.01",
    "ATR_STOP": "1.5",
    "TIME_STOP_HHMM": "13:00",
    "DEFAULT_PROFIT_PCT": "3.0",
    "DEFAULT_LOSS_PCT": "-5.0",
    "DAILY_CAPITAL": "250000000",
    "CAP_CAP": "0.90",
    "SLIPPAGE_LIMIT_PCT": "0.25",
    "SLIPPAGE_ENTER_GUARD_PCT": "2.5",
    "VWAP_TOL": "0.003",  # 🔸 VWAP 허용 오차(기본 0.3%)
    "W_MAX_ONE": "0.25",
    "W_MIN_ONE": "0.03",
    "REBALANCE_ANCHOR": "weekly",             # weekly | today | monthly
    "WEEKLY_ANCHOR_REF": "last",              # NEW: 'last'(직전 일요일) | 'next'(다음 일요일)
    "MOMENTUM_OVERRIDES_FORCE_SELL": "true",
    # 레짐(코스닥) 파라미터
    "KOSDAQ_INDEX_CODE": "KOSDAQ",
    "KOSDAQ_ETF_FALLBACK": "229200",
    "REG_BULL_MIN_UP_PCT": "0.5",
    "REG_BULL_MIN_MINUTES": "10",
    "REG_BEAR_VWAP_MINUTES": "10",
    "REG_BEAR_DROP_FROM_HIGH": "0.7",
    "REG_BEAR_STAGE1_MINUTES": "20",
    "REG_BEAR_STAGE2_ADD_DROP": "0.5",
    "REG_PARTIAL_S1": "0.30",
    "REG_PARTIAL_S2": "0.30",
    "BASE_QTY_MODE": "initial",  # initial | current
    "TRAIL_PCT_BULL": "0.025",
    "TRAIL_PCT_BEAR": "0.012",
    "TP_PROFIT_PCT_BULL": "3.5",
    # 신고가 돌파 후 3일 눌림 + 반등 매수용 파라미터
    "USE_PULLBACK_ENTRY": "true",          # true면 '신고가 → 3일 연속 하락 → 반등' 패턴 충족 시에만 눌림목 진입 허용
    "PULLBACK_LOOKBACK": "60",             # 신고가 탐색 범위(거래일 기준)
    "PULLBACK_DAYS": "3",                  # 연속 하락 일수
    "PULLBACK_REVERSAL_BUFFER_PCT": "0.2", # 되돌림 확인 여유(%): 직전 하락일 고가 대비 여유율
    "PULLBACK_TOPN": "50",                 # 눌림목 스캔용 코스닥 시총 상위 종목 수
    "PULLBACK_UNIT_WEIGHT": "0.03",        # 눌림목 매수 1건당 자본 배분(활성 자본 비율)
    "PULLBACK_MAX_BUYS_PER_DAY": "5",      # 눌림목 하루 최대 신규 매수 건수
    # 챔피언 후보 필터
    "CHAMPION_MIN_TRADES": "5",            # 최소 거래수
    "CHAMPION_MIN_WINRATE": "45.0",        # 최소 승률(%)
    "CHAMPION_MAX_MDD": "30.0",            # 최대 허용 MDD(%)
    "CHAMPION_MIN_SHARPE": "0.0",          # 최소 샤프 비율
    "NEUTRAL_ENTRY_SCALE": "0.6",          # 중립 레짐 신규/재진입 스케일링 비율
    # 기타
    "MARKET_DATA_WHEN_CLOSED": "false",
    "FORCE_WEEKLY_REBALANCE": "0",
    # NEW: 1분봉 VWAP 모멘텀 파라미터
    "MOM_FAST": "5",        # 1분봉 fast MA 길이
    "MOM_SLOW": "20",       # 1분봉 slow MA 길이
    "MOM_TH_PCT": "0.5",    # fast/slow 괴리 임계값(%) – 0.5% 이상이면 강세로 본다
    # Subject flow gate 기본값
    "MIN_SMART_MONEY_RATIO_KOSPI": "0.02",
    "MIN_SMART_MONEY_RATIO_KOSDAQ": "0.03",
    "SUBJECT_FLOW_TIMEOUT_SEC": "1.2",
    "SUBJECT_FLOW_RETRY": "1",
    "SUBJECT_FLOW_CACHE_TTL_SEC": "60",
    "SUBJECT_FLOW_FAIL_POLICY": "CACHE",
    "SUBJECT_FLOW_EMPTY_POLICY": "TREAT_AS_FAIL",
    "SUBJECT_FLOW_DEGRADED_TURNOVER_MULT": "1.5",
    "SUBJECT_FLOW_DEGRADED_OB_ADD": "10",
    "SUBJECT_FLOW_MAX_CALLS_PER_RUN": "200",
    # PREP flow policy (전일 확정 수급만 사용, 누락은 non-blocking)
    "FLOW_MODE": "PREV_CLOSE_ONLY",            # PREV_CLOSE_ONLY | PREV_DAY_ONLY
    "FLOW_STRICT": "0",                        # 0이면 누락/실패 시 warning only
    "DEGRADED_EXCLUDE_FLOW": "1",              # 1이면 PREP degraded 판정에서 flow 지표 제외
    "EMERGENCY_GLOBAL_SELL": "false",
    "STRATEGY_REDUCTION_PRIORITY": "5,4,3,2,1",
    # Diagnostics
    "DIAGNOSTIC_MODE": "false",
    "DIAGNOSTIC_ONLY": "false",
    "DIAGNOSTIC_FORCE_RUN": "false",
    "DIAGNOSTIC_DUMP_PATH": "",
    "DIAGNOSTIC_TARGET_MARKETS": "",
    "DIAGNOSTIC_MAX_SYMBOLS": "200",
    "FORCE_NONTRADING_UNIVERSE_SMOKE": "0",
    "NONTRADING_SMOKE_FORCE": "0",
    "NONTRADING_SMOKE_DB_STORE": "1",
    "NONTRADING_SMOKE_FORCE_REBUILD": "0",
    "NONTRADING_SMOKE_TIMEOUT_SEC": "180",
    "EMERGENCY_UNIVERSE_BUILD": "0",
    "FORCE_UNIVERSE_REBUILD": "0",
    "UNIVERSE_NAMESPACE_MODE": "ACCOUNT_ENV",
    # === Strategy intent/exec defaults ===
    "ENABLED_STRATEGIES": "",
    "STRATEGY_MODE": "INTENT_ONLY",  # INTENT_ONLY | LIVE
    "STRATEGY_DRY_RUN": "true",
    "STRATEGY_INTENTS_PATH": "runtime/strategy_intents.jsonl",
    "STRATEGY_INTENTS_STATE_PATH": "runtime/strategy_intents_state.json",
    "STRATEGY_MAX_OPEN_INTENTS": "20",
    "STRATEGY_MAX_POSITION_PCT": "0.10",
    "STRATEGY_ALLOW_SELL_ONLY": "false",
    "STRATEGY_WEIGHTS": "",
    "DISABLE_KOSDAQ_LOOP": "false",
    "DISABLE_KOSPI_ENGINE": "false",
    "ACTIVE_STRATEGIES": "1",  # CSV of strategy IDs eligible for managed exits/entries
    "ALLOW_ADOPT_UNMANAGED": "false",
    "STATE_PATH": "runtime/state.json",
    # PB1 close-pullback defaults
    "ENABLE_BREAKOUT": "false",
    "LEDGER_LOOKBACK_DAYS": "120",
    "LEDGER_BASE_DIR": "trader_ledger",
    "PAPER_RESET_AUTO_PURGE": "0",
    "PAPER_RESET_EVENT_ONLY_IN_PRACTICE": "1",
    "PB1_CAPITAL_MODE": "CASH",
    "PB1_ENTRY_CAPITAL_KRW": "0",
    "PB1_CASH_RESERVE_PCT": "0.10",
    "PB1_ALLOW_PREOPEN_ENTRY": "0",
    "PB1_PREOPEN_START": "08:45",
    "PB1_PREOPEN_END": "09:00",
    "PB1_PREOPEN_REQUIRE_BALANCE": "1",
    "PB1_PREOPEN_MAX_NEW_POSITIONS": "0",
    "PB1_PREOPEN_ORDER_TYPE": "LIMIT",
    "PB1_PREOPEN_LIMIT_BUFFER_PCT": "0.3",
    "PB1_REQUIRE_BALANCE_FOR_ENTRY": "1",
    "PB1_ENTRY_ENABLED": "true",
    "PB1_ENTRY_MODE": "BOTH",
    "PB1_REQUIRE_BOTH": "1",
    "ENTRY_COND_MODE": "OR",
    "PB1_LOG_ENTRY_GATE": "1",
    "PB1_LOG_DROP_REASONS_TOPN": "10",
    "PB1_ENTRY_WINDOW_START": "09:00",
    "PB1_ENTRY_OPEN_END": "09:05",
    "PB1_ENTRY_WINDOW_END": "15:15",
    "PB1_EXIT_WINDOW_START": "15:15",
    "PB1_EXIT_WINDOW_END": "15:30",
    "PB1_MORNING_WINDOW_END": "10:30",
    "MORNING_WINDOW_START": "09:00",
    "MORNING_WINDOW_END": "10:30",
    "MORNING_EXIT_START": "09:00",
    "MORNING_EXIT_END": "09:20",
    "AFTERNOON_WINDOW_START": "10:30",
    "AFTERNOON_WINDOW_END": "15:15",
    "CLOSE_AUCTION_START": "15:15",
    "CLOSE_AUCTION_END": "15:30",
    "PB1_REQUIRE_VOLUME": "1",
    "PB1_FORCE_ENTRY_ON_PUSH": "1",
    "PB1_WAIT_FOR_WINDOW": "1",
    "PB1_MAX_WAIT_FOR_WINDOW_MIN": "240",
    "PB1_ALLOW_ADD_TO_EXISTING": "0",
    "MIN_ORDER_KRW": "0",
    "MARKET_OPEN_HHMM": "09:00",
    "MARKET_CLOSE_HHMM": "15:30",
    "PB1_PULLBACK_BAND_RELAXED": "1",
    "PB1_PULLBACK_BAND_KOSPI": "2,25",
    "PB1_PULLBACK_BAND_KOSDAQ": "2,28",
    "PB1_PULLBACK_BAND_KOSPI_STRICT": "3,8",
    "PB1_PULLBACK_BAND_KOSDAQ_STRICT": "4,10",
    "PB1_RELAX_MA_FILTER": "1",
    "PB1_RELAX_MA20_SLOPE": "1",
    "PB1_MA20_SLOPE_HARD_FAIL_MIN": "-0.05",
    "PB1_VOL_CONTRACTION_MAX": "1.15",
    "PB1_VOLU_CONTRACTION_MAX": "1.20",
    "PB1_VOL_CONTRACTION_MAX_STRICT": "1.00",
    "PB1_VOLU_CONTRACTION_MAX_STRICT": "0.98",
    "PB1_PULLBACK_MIN": "0.03",
    "PB1_PULLBACK_MAX": "0.18",
    "PB1_REQUIRE_BOTH_CONTRACTIONS": "1",
    "PB1_SWING_TREND_MIN": "1.05",
    "PB1_SWING_VOL_CONTRACTION_MAX": "0.80",
    "PB1_SWING_VOLU_CONTRACTION_MAX": "0.75",
    "PB1_R_FLOOR_PCT": "2.0",
    "PB1_DAY_TP_R": "0.8",
    "PB1_DAY_SL_R": "0.6",
    "KOSPI_HARD_STOP_PCT": "7.0",
    "KOSDAQ_HARD_STOP_PCT": "8.0",
    "PB1_SWING_TRAIL_MA": "20",
    "PB1_TIME_STOP_DAYS": "10",
    "PB1_MIN_CANDLES": "60",
    # OHLCV 로딩 기본 윈도우 (200일로 안정화)
    "PB1_OHLCV_DAYS_BASE": "200",
    # PB1 "최고 눌림목" 랭킹/사이징 튜닝
    "PB1_MAX_POSITIONS": "30",         # 계좌 전체 최대 보유 종목 수. trade-am/trade-afternoon 공통. 2026-04-28부터 8 -> 30 확대
    "PB1_MIN_SCORE": "70",             # 점수 컷(0~100)
    "PB1_MIN_SCORE_BASE": "70",        # Adaptive score cut 시작값
    "PB1_MIN_SCORE_FLOOR": "55",       # Adaptive score cut 하한
    "PB1_MIN_SCORE_STEP": "5",         # Adaptive score cut 단계
    "PB1_BOOTSTRAP_ENABLE": "1",
    "BOOTSTRAP_MINERVINI_RS_MIN_PCTILE": "60",
    "BOOTSTRAP_MINERVINI_VCP_MIN_SCORE": "45",
    "BOOTSTRAP_RELAX_PASSES": "5",
    "BOOTSTRAP_KEEP_TREND_TEMPLATE_ALWAYS": "0",
    "BOOTSTRAP_PB1_VOL_MAX": "1.15",
    "BOOTSTRAP_PB1_VOLU_MAX": "1.15",
    "BOOTSTRAP_PB1_PULLBACK_MIN": "0.01",
    "BOOTSTRAP_PB1_PULLBACK_MAX": "0.25",
    "BOOTSTRAP_PB1_REQUIRE_BOTH_CONTRACTIONS": "0",
    "BOOTSTRAP_PB1_MIN_SCORE_BASE": "55",
    "BOOTSTRAP_PB1_MIN_SCORE_FLOOR": "45",
    "BOOTSTRAP_PB1_MIN_SCORE_STEP": "5",
    "BOOTSTRAP_SCORE_CUT_KEEP_TOPN": "3",
    "BOOTSTRAP_FORCE_MIN_1_SHARE": "1",
    "BOOTSTRAP_MIN1_TOPN": "3",
    "PB1_FAILMODE_SOFT": "1",
    "PB1_MIN_CANDIDATES": "3",
    "PB1_RELAX_MAX_PASSES": "3",
    "PB1_MIN_BUYABLE": "1",
    "PB1_USE_MINERVINI_AS_RANK_ONLY": "1",
    "PB1_MINERVINI_HARD_GATE": "0",
    "PB1_EMERGENCY_ORDER_ENABLED": "0",
    "PB1_EMERGENCY_DIAG_ONLY": "1",
    "PB1_SPREAD_HARD_MAX_PCT": "0",
    "PB1_GAP_HARD_MAX_PCT": "0",
    "PB1_ENTRY_BUDGET_PCT_PER_TICK": "0.60",
    "PB1_MAX_POS_PCT": "0.35",
    "PB1_TARGET_NEW_POSITIONS": "30",
    "PB1_USE_RISK_PARITY": "1",         # 1이면 ATR 기반 리스크패리티 사이징
    "PB1_MAX_ATR_PCT": "8.0",           # ATR% 상한 (과변동 종목 제외)
    "PB1_ATR_PCT_MAX": "8.0",
    "ATR_MAX_PCT": "0.10",             # ATR% 상한 공통값 (ratio 기준)
    "PB1_MIN_VALUE20": "3000000000",    # 20일 평균 거래대금(원) 하한 (유동성 컷)
    "PB1_VOL_MAX": "1.00",
    "PB1_VOLU_MAX": "0.98",
    "PB1_VOLU_MAX_INTRADAY": "1.05",
    # 추가 환경변수
    "ALLOW_KIS_DAILY_FALLBACK": "0",
    "PB1_MAX_DAILY_FETCH_PER_TICK": "20",
    "PB1_MAX_PRICE_FETCH_PER_TICK": "30",
    "KIS_RATE_LIMIT_COOLDOWN_SEC": "8",
    "PRICE_SNAPSHOT_TTL_SEC": "2",
    "DAILY_BAR_TTL_SEC": "1800",
    "TRADE_REQUIRE_PREP_FINAL30_SCORED": "1",
    "TRADE_ALLOW_PLAIN_WATCHLIST_FALLBACK": "0",
    "TRADE_SKIP_KIS_DAILY_REFRESH": "1",
    "ALLOW_TRADE_WITH_MISSING_SCORED_COLUMNS": "0",
    "ENTRY_SCAN_SAVE_DEBUG": "1",
    "ENTRY_SCAN_LOG_TOP_REJECTS": "10",
    "TRADE_FORCE_MIN1_DIAG": "1",
    "FORCE_MIN1_OVERRIDE_POSITION_CAP": "1",
    "FORCE_MIN1_OVERRIDE_TOPN": "3",
    "ALLOW_SINGLE_SHARE_OVERRIDE": "1",
    "MIN_REMAINING_CASH_KRW": "10000",
    "BUY_PRICE_BUFFER_PCT": "0.002",
    "BUDGET_FLEX_PCT": "1.10",
    "MIN_TRAIL_BARS": "2",
    "MIN_EXIT_BARS": "1",
    # Candidate Pool (주말 후보군 생성/주중 후보군 기반 진입)
    "CANDIDATE_POOL_ENABLED": "1",                    # 후보군 시스템 활성화
    "CANDIDATE_POOL_TTL_DAYS": "7",                   # 후보군 유효기간(일)
    "CANDIDATE_POOL_SIZE": "120",                     # 후보군 목표 수
    "CANDIDATE_POOL_MIN_SIZE": "40",                  # 최소 후보군 수 (이보다 작으면 fallback)
    "CANDIDATE_POOL_STRATEGY_KEY": "pb1_candidate_pool",  # DB 저장 전략키
    "CANDIDATE_POOL_FORCE_REBUILD": "0",              # 강제 재생성 (DIAG/수동)
    "CANDIDATE_POOL_MIN_PRICE": "2000.0",             # 후보군 최소 주가
    "CANDIDATE_POOL_LIQ_DAYS": "20",                  # 유동성 계산 일수
    "CANDIDATE_POOL_MIN_ROWS": "30",                  # OHLCV 최소 행수
    # Derived Fallback (전일 데이터 없을 때 최근 영업일 최신 as_of로 fallback)
    "DERIVED_FALLBACK_MAX_DAYS": "3",                 # 최근 N 영업일 우선 탐색 (기본 3)
    "DERIVED_FALLBACK_ENABLED": "1",                  # fallback 활성화 (0=비활성화)
    "DERIVED_FALLBACK_WARN_AGE_DAYS": "2",            # fallback age가 이 값 이상이면 경고 (기본 2)
    # Minervini v2 tuning
    "MINERVINI_RS_MIN": "0.70",
    "MINERVINI_MAX_PYRAMID": "3",
    "MINERVINI_ADD_ON_R": "1.5",
    "MINERVINI_BREAKOUT_VOL_MULT": "1.5",
    "MINERVINI_MAX_EXTENSION_PIVOT": "0.05",
    "MINERVINI_INITIAL_STOP_PCT": "0.075",
    "MINERVINI_TIME_STOP_DAYS": "20",
    "MINERVINI_HEAVY_VOL_MULT": "1.5",
    # Minervini Pro universe/RS
    "UNIVERSE_POOL_SIZE": "200",
    "RS_BENCHMARK": "229200",
    "RS_LOOKBACK_DAYS": "63",
    "RS_LOOKBACK2_DAYS": "126",
    "RS_MIN_PCTILE": "60",
    "RS_COMPOSITE_W1": "0.6",
    "RS_COMPOSITE_W2": "0.4",
    # Market regime
    "REGIME_INDEX": "229200",
    "REGIME_MODE": "RELAXED",
    "REGIME_MA_FAST": "50",
    "REGIME_MA_SLOW": "200",
    "REGIME_BREADTH_WINDOW": "20",
    "REGIME_MAX_RISK": "1.0",
    "REGIME_MID_RISK": "0.6",
    "REGIME_MIN_RISK": "0.0",
    # VCP/trigger
    "VCP_LOOKBACK": "120",
    "VCP_MIN_SCORE": "45",
    "PIVOT_BUFFER_PCT": "0.30",
    "BREAKOUT_VOL_MULT": "1.5",
    "ENTRY_MODE": "BOTH",
    # Liquidity/gap/slippage
    "MIN_AVG_VALUE_KRW": "300000000",
    "MAX_INTRADAY_RANGE_PCT": "12",
    "MAX_GAP_UP_PCT": "6",
    "MAX_SPREAD_PROXY_BPS": "80",
    # Risk/exit/step-up stop
    "RISK_PER_TRADE_PCT": "0.5",
    "INITIAL_STOP_MODE": "TIGHTLOW",
    "ATR_WINDOW": "14",
    "ATR_MULT": "2.5",
    "TAKE_PROFIT_R1": "2.0",
    "TP1_SELL_PCT": "0.33",
    "TAKE_PROFIT_R2": "3.0",
    "TP2_SELL_PCT": "0.33",
    "TRAIL_MODE": "MA20",
    "TRAIL_STEP_AFTER_R": "1.5",
    "FAILED_BREAKOUT_EXIT_DAYS": "2",
    "REENTRY_COOLDOWN_DAYS": "10",
    # Minervini-only mode (analytics mode without trading)
    "MINERVINI_ONLY": "0",
    # === [2026-04-29] Effective Exit Policy (기존 보유분 새 기준 exit) ===
    "PB1_EXISTING_POSITION_EFFECTIVE_EXIT_ENABLED": "1",
    "PB1_EFFECTIVE_STOP_CAP_ENABLED": "1",
    "PB1_EFFECTIVE_STOP_CAP_KOSPI_PCT": "7.0",
    "PB1_EFFECTIVE_STOP_CAP_KOSDAQ_PCT": "8.0",
    "PB1_PROFIT_PROTECT_ENABLED": "1",
    "PB1_PROFIT_PROTECT_PCT": "8.0",
    "PB1_PROFIT_PROTECT_SELL_PCT": "0.33",
    "PB1_ABS_TP1_ENABLED": "1",
    "PB1_ABS_TP1_PROFIT_PCT": "10.0",
    "PB1_ABS_TP1_SELL_PCT": "0.33",
    # Paper cap: 기본 5천만원 (환경변수 PAPER_MAX_CAPITAL_KRW로 override 가능)
    "PAPER_MAX_CAPITAL_KRW": "50000000",
    # === [2026-04-29] Multi-Layer Exit Router ===
    "PB1_EXIT_ROUTER_ENABLED": "1",
    "PB1_PERCENT_EXIT_ENABLED": "1",
    "PB1_EXIT_POLICY_MODE": "HYBRID_BY_ENTRY_STYLE",
    "PB1_EXIT_POLICY_PATH": "trader/exit_policy/optimized_exit_router_policy.json",
    # SWING 수익률 기준 TP
    "PB1_SWING_PROFIT_ACTIVATE_PCT": "8.0",
    "PB1_SWING_PROFIT_GIVEBACK_PCT": "3.0",
    "PB1_SWING_PROFIT_FLOOR_PCT": "5.0",
    "PB1_SWING_TP1_PROFIT_PCT": "12.0",
    "PB1_SWING_TP2_PROFIT_PCT": "18.0",
    # MOMENTUM 전용 파라미터
    "PB1_MOMENTUM_PROFIT_ACTIVATE_PCT": "5.0",
    "PB1_MOMENTUM_PROFIT_GIVEBACK_PCT": "2.0",
    "PB1_MOMENTUM_TP1_R": "1.5",
    "PB1_MOMENTUM_TP2_R": "2.5",
    # CORE 수익률 기준 TP
    "PB1_CORE_TP1_PROFIT_PCT": "20.0",
    "PB1_CORE_TP1_SELL_PCT": "0.25",
    "PB1_CORE_TP2_PROFIT_PCT": "30.0",
    "PB1_CORE_TP2_SELL_PCT": "0.25",
}


def _cfg(key: str) -> str:
    """환경변수 > CONFIG 기본값"""
    return os.getenv(key, CONFIG.get(key, ""))


def _cfg_with_alias(primary: str, alias: str) -> str:
    if os.getenv(primary) is not None:
        return os.getenv(primary, "")
    if os.getenv(alias) is not None:
        return os.getenv(alias, "")
    if primary in CONFIG:
        return CONFIG.get(primary, "")
    return CONFIG.get(alias, "")


def _cfg_first(*keys: str) -> str:
    for key in keys:
        if os.getenv(key) is not None:
            return os.getenv(key, "")
    for key in keys:
        if key in CONFIG:
            return CONFIG.get(key, "")
    return ""


def _cfg_first_with_source(*keys: str) -> tuple[str | None, str | None]:
    for key in keys:
        if os.getenv(key) is not None:
            return os.getenv(key, ""), key
    for key in keys:
        if key in CONFIG:
            return CONFIG.get(key, ""), key
    return None, None


def _resolve_atr_max_pct(*keys: str, default_ratio: float = 0.10) -> tuple[float, str, str]:
    raw, source = _cfg_first_with_source(*keys)
    if raw is None or raw == "":
        raw = str(default_ratio)
        source = "default"
    try:
        value = float(raw)
    except (TypeError, ValueError):
        value = float(default_ratio)
        source = "default"
    if value > 1.0:
        value = value / 100.0
    return value, source or "default", str(raw)


def _default_bool(key: str, fallback: bool = False) -> bool:
    raw_default = str(CONFIG.get(key, "")).strip().lower()
    if raw_default in TRUE_VALUES:
        return True
    if raw_default in FALSE_VALUES:
        return False
    return fallback


def _cfg_bool(key: str, fallback: bool | None = None) -> bool:
    default_value = _default_bool(key, fallback if fallback is not None else False)
    return env_bool(key, default=default_value)


def _env_bool(keys: list[str], default: bool = False) -> tuple[bool, str]:
    """
    Read boolean env var from a list of keys.
    Accepts: 1/0, true/false, yes/no, on/off (case-insensitive).
    Returns: (value: bool, source_key: str)
    """
    for k in keys:
        v = os.getenv(k)
        if v is None:
            continue
        s = v.strip().lower()
        if s in ("1", "true", "yes", "y", "on"):
            return True, k
        if s in ("0", "false", "no", "n", "off", ""):
            return False, k
        # Unknown value -> treat non-empty as True
        return True, k
    return default, "default"


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _normalize_env_lower(key: str, default: str | None = None) -> str | None:
    raw = os.getenv(key)
    if raw is None and default is not None:
        raw = default
        os.environ[key] = raw
    if raw is None:
        return None
    normalized = raw.strip().lower()
    if raw != normalized:
        os.environ[key] = normalized
    return normalized


STRATEGY_ENV = _normalize_env_lower("STRATEGY_ENV")
KIS_ENV = _normalize_env_lower("KIS_ENV")
if STRATEGY_ENV:
    logger.info("[VERIFY] env=%s (STRATEGY_ENV)", STRATEGY_ENV)
if KIS_ENV:
    logger.info("[VERIFY] kis_env=%s", KIS_ENV)

LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)
STATE_FILE = Path(__file__).parent / "trade_state.json"  # legacy; position state uses STATE_PATH
STATE_DIR_RAW = _cfg("STATE_DIR")
STATE_DIR = Path(STATE_DIR_RAW) if STATE_DIR_RAW else runtime_path("runtime")
STATE_PATH = Path(_cfg("STATE_PATH") or STATE_DIR / "state.json")
ensure_not_repo_tracked_path(STATE_PATH)
STATE_DIR.mkdir(parents=True, exist_ok=True)
STATE_PATH.parent.mkdir(parents=True, exist_ok=True)

# 종목별 시장코드 고정 맵 (실전에서는 마스터테이블 로드로 대체 권장)
MARKET_MAP: Dict[str, str] = {
    # 예시: '145020': 'J', '347850': 'J', '257720': 'U', '178320': 'J', '348370': 'U'
}

# 데이터 없음 1차 감지 상태 저장(연속 DATA_EMPTY 확인용)
EXCLUDE_STATE: Dict[str, Dict[str, bool]] = {}

KST = ZoneInfo("Asia/Seoul")

SELL_FORCE_TIME_STR = _cfg("SELL_FORCE_TIME").strip()
SELL_ALL_BALANCES_AT_CUTOFF = _cfg_bool("SELL_ALL_BALANCES_AT_CUTOFF")
EMERGENCY_GLOBAL_SELL = _cfg_bool("EMERGENCY_GLOBAL_SELL")
RATE_SLEEP_SEC = float(_cfg("API_RATE_SLEEP_SEC"))
FORCE_SELL_PASSES_CUTOFF = int(_cfg("FORCE_SELL_PASSES_CUTOFF"))
FORCE_SELL_PASSES_CLOSE = int(_cfg("FORCE_SELL_PASSES_CLOSE"))
FORCE_SELL_BLOCKED_LOTS = _cfg_bool("FORCE_SELL_BLOCKED_LOTS")
PARTIAL1 = float(_cfg("PARTIAL1"))
PARTIAL2 = float(_cfg("PARTIAL2"))
TRAIL_PCT = float(_cfg("TRAIL_PCT"))
FAST_STOP = float(_cfg("FAST_STOP"))
ATR_STOP = float(_cfg("ATR_STOP"))
TIME_STOP_HHMM = _cfg("TIME_STOP_HHMM")
DEFAULT_PROFIT_PCT = float(_cfg("DEFAULT_PROFIT_PCT"))
DEFAULT_LOSS_PCT = float(_cfg("DEFAULT_LOSS_PCT"))
DAILY_CAPITAL = int(_cfg("DAILY_CAPITAL"))
CAP_CAP = float(_cfg("CAP_CAP"))
SLIPPAGE_LIMIT_PCT = float(_cfg("SLIPPAGE_LIMIT_PCT"))
SLIPPAGE_ENTER_GUARD_PCT = float(_cfg("SLIPPAGE_ENTER_GUARD_PCT"))
VWAP_TOL = float(_cfg("VWAP_TOL"))  # 🔸 VWAP 허용 오차(예: 0.003 = -0.3%까지 허용)
W_MAX_ONE = float(_cfg("W_MAX_ONE"))
W_MIN_ONE = float(_cfg("W_MIN_ONE"))
ALLOW_PYRAMID = _cfg_bool("ALLOW_PYRAMID")
REBALANCE_ANCHOR = _cfg("REBALANCE_ANCHOR")
WEEKLY_ANCHOR_REF = _cfg("WEEKLY_ANCHOR_REF").lower()
MOMENTUM_OVERRIDES_FORCE_SELL = _cfg_bool("MOMENTUM_OVERRIDES_FORCE_SELL")
BASE_QTY_MODE = (_cfg("BASE_QTY_MODE") or "initial").lower()
if BASE_QTY_MODE not in {"initial", "current"}:
    logging.getLogger(__name__).warning(
        f"[CONFIG] BASE_QTY_MODE={BASE_QTY_MODE} 지원 안 함 → initial로 대체"
    )
    BASE_QTY_MODE = "initial"

# NEW: 1분봉 모멘텀 파라미터
MOM_FAST = int(_cfg("MOM_FAST") or "5")
MOM_SLOW = int(_cfg("MOM_SLOW") or "20")
MOM_TH_PCT = float(_cfg("MOM_TH_PCT") or "0.5")
# subject flow
MIN_SMART_MONEY_RATIO_KOSPI = float(_cfg("MIN_SMART_MONEY_RATIO_KOSPI") or "0.02")
MIN_SMART_MONEY_RATIO_KOSDAQ = float(_cfg("MIN_SMART_MONEY_RATIO_KOSDAQ") or "0.03")
SUBJECT_FLOW_TIMEOUT_SEC = float(_cfg("SUBJECT_FLOW_TIMEOUT_SEC") or "1.2")
SUBJECT_FLOW_RETRY = int(_cfg("SUBJECT_FLOW_RETRY") or "1")
SUBJECT_FLOW_CACHE_TTL_SEC = float(_cfg("SUBJECT_FLOW_CACHE_TTL_SEC") or "60")
SUBJECT_FLOW_FAIL_POLICY = (_cfg("SUBJECT_FLOW_FAIL_POLICY") or "CACHE").upper()
SUBJECT_FLOW_EMPTY_POLICY = (_cfg("SUBJECT_FLOW_EMPTY_POLICY") or "TREAT_AS_FAIL").upper()
SUBJECT_FLOW_DEGRADED_TURNOVER_MULT = float(_cfg("SUBJECT_FLOW_DEGRADED_TURNOVER_MULT") or "1.5")
SUBJECT_FLOW_DEGRADED_OB_ADD = float(_cfg("SUBJECT_FLOW_DEGRADED_OB_ADD") or "10")
SUBJECT_FLOW_MAX_CALLS_PER_RUN = int(_cfg("SUBJECT_FLOW_MAX_CALLS_PER_RUN") or "200")
FLOW_MODE = (_cfg("FLOW_MODE") or "PREV_CLOSE_ONLY").upper()
FLOW_STRICT = _cfg_bool("FLOW_STRICT", fallback=False)
DEGRADED_EXCLUDE_FLOW = _cfg_bool("DEGRADED_EXCLUDE_FLOW", fallback=True)
# 전략별 활성/가중치 파싱
def parse_enabled_strategies(raw: str) -> set[str]:
    strategies: set[str] = set()
    for name in (raw or "").split(","):
        cleaned = name.strip().lower()
        if cleaned:
            strategies.add(cleaned)
    return strategies


def _parse_strategy_weights(raw: str) -> Dict[str, float]:
    weights: Dict[str, float] = {}
    for item in (raw or "").split(","):
        if not item.strip():
            continue
        if "=" in item:
            key, value = item.split("=", 1)
        elif ":" in item:
            key, value = item.split(":", 1)
        else:
            key, value = item, "0"
        key = key.strip().lower()
        try:
            weight = float(value)
        except ValueError:
            weight = 0.0
        if key:
            weights[key] = weight
    return weights


ENABLED_STRATEGIES_SET = parse_enabled_strategies(_cfg("ENABLED_STRATEGIES"))
RAW_STRATEGY_WEIGHTS = _parse_strategy_weights(_cfg("STRATEGY_WEIGHTS"))

if ENABLED_STRATEGIES_SET:
    STRATEGY_WEIGHTS = {
        name: (RAW_STRATEGY_WEIGHTS.get(name, 0.0) if name in ENABLED_STRATEGIES_SET else 0.0)
        for name in ENABLED_STRATEGIES_SET.union(RAW_STRATEGY_WEIGHTS.keys())
    }
else:
    STRATEGY_WEIGHTS = {name: 0.0 for name in RAW_STRATEGY_WEIGHTS.keys()}

STRATEGY_MODE = resolve_mode(_cfg("STRATEGY_MODE") or "INTENT_ONLY")
_STRATEGY_DRY_RUN_DEFAULT = _cfg_bool("STRATEGY_DRY_RUN", fallback=True)
STRATEGY_DRY_RUN = env_bool("DRY_RUN", default=_STRATEGY_DRY_RUN_DEFAULT)


def _resolve_min_order_krw() -> float:
    raw = _cfg("MIN_ORDER_KRW") or "0"
    try:
        value = float(raw)
    except ValueError:
        logger.warning("[CONFIG] MIN_ORDER_KRW invalid=%s -> fallback=0", raw)
        value = 0.0
    if value < 0:
        logger.warning("[CONFIG] MIN_ORDER_KRW=%s -> clamp=0", raw)
        value = 0.0
    logger.info("[CONFIG] MIN_ORDER_KRW=%s (sizing_floor)", value)
    return value


MIN_ORDER_KRW = _resolve_min_order_krw()

# ================================================================
# [NEW] Sizing 최소 1주 보장 옵션 (단계 B 완화: 기본 활성화)
# ================================================================
SIZING_ALLOW_MIN_1_SHARE = _cfg_bool("SIZING_ALLOW_MIN_1_SHARE", fallback=True)
SIZING_MIN_1_SHARE_TOPN = int(_cfg("SIZING_MIN_1_SHARE_TOPN") or "0")

# ================================================================
# [NEW] 주문 가격 slippage 설정 (단계 B 완화: ask 대신 현재가 사용)
# ================================================================
PRICE_SLIPPAGE_PCT_BUY = float(_cfg("PRICE_SLIPPAGE_PCT_BUY") or "0.005")  # 0.5% 기본
PRICE_USE_ASK_IF_AVAILABLE = _cfg_bool("PRICE_USE_ASK_IF_AVAILABLE", fallback=False)
ALLOW_SINGLE_SHARE_OVERRIDE = _cfg_bool("ALLOW_SINGLE_SHARE_OVERRIDE", fallback=True)
MIN_REMAINING_CASH_KRW = float(_cfg("MIN_REMAINING_CASH_KRW") or "10000")
BUY_PRICE_BUFFER_PCT = float(_cfg("BUY_PRICE_BUFFER_PCT") or "0.002")
BUDGET_FLEX_PCT = float(_cfg("BUDGET_FLEX_PCT") or "1.10")
MIN_TRAIL_BARS = max(1, int(float(_cfg("MIN_TRAIL_BARS") or "2")))
MIN_EXIT_BARS = max(1, int(float(_cfg("MIN_EXIT_BARS") or "1")))

logger.info(
    "[CONFIG][SIZING] allow_min_1_share=%s topn=%s slippage_buy=%.3f%% use_ask=%s single_share_override=%s budget_flex=%.3f min_remaining_cash=%.0f",
    int(SIZING_ALLOW_MIN_1_SHARE),
    SIZING_MIN_1_SHARE_TOPN,
    PRICE_SLIPPAGE_PCT_BUY * 100,
    int(PRICE_USE_ASK_IF_AVAILABLE),
    int(ALLOW_SINGLE_SHARE_OVERRIDE),
    BUDGET_FLEX_PCT,
    MIN_REMAINING_CASH_KRW,
)

STRATEGY_INTENTS_PATH = Path(
    _cfg("STRATEGY_INTENTS_PATH") or runtime_path(*Path(CONFIG["STRATEGY_INTENTS_PATH"]).parts)
)
STRATEGY_INTENTS_STATE_PATH = Path(
    _cfg("STRATEGY_INTENTS_STATE_PATH") or runtime_path(*Path(CONFIG["STRATEGY_INTENTS_STATE_PATH"]).parts)
)
STRATEGY_MAX_OPEN_INTENTS = int(_cfg("STRATEGY_MAX_OPEN_INTENTS") or "20")
STRATEGY_MAX_POSITION_PCT = float(_cfg("STRATEGY_MAX_POSITION_PCT") or "0.10")
STRATEGY_ALLOW_SELL_ONLY = _cfg_bool("STRATEGY_ALLOW_SELL_ONLY")

DIAGNOSTIC_MODE = _cfg_bool("DIAGNOSTIC_MODE")
DIAGNOSTIC_ONLY = _cfg_bool("DIAGNOSTIC_ONLY")
DIAGNOSTIC_FORCE_RUN = _cfg_bool("DIAGNOSTIC_FORCE_RUN")
_diag_dump_env = os.getenv("DIAGNOSTIC_DUMP_DIR") or os.getenv("DIAGNOSTIC_DUMP_PATH")
DIAGNOSTIC_DUMP_DIR = Path(_diag_dump_env) if _diag_dump_env else get_cache_root() / "diagnostics"
ensure_not_repo_tracked_path(DIAGNOSTIC_DUMP_DIR)
DIAGNOSTIC_DUMP_DIR.mkdir(parents=True, exist_ok=True)
DIAGNOSTIC_MAX_SYMBOLS = int(_cfg("DIAGNOSTIC_MAX_SYMBOLS") or CONFIG["DIAGNOSTIC_MAX_SYMBOLS"])
DIAGNOSTIC_TARGET_MARKETS = (_cfg("DIAGNOSTIC_TARGET_MARKETS") or "").strip()
DIAG_ENABLED = DIAGNOSTIC_MODE or DIAGNOSTIC_ONLY

if DIAGNOSTIC_MODE:
    STRATEGY_MODE = "INTENT_ONLY"
    STRATEGY_DRY_RUN = True
    STRATEGY_ALLOW_SELL_ONLY = True

logger.info(
    "[DIAG][CONFIG] mode=%s only=%s force_run=%s dump_dir=%s enabled=%s",
    DIAGNOSTIC_MODE,
    DIAGNOSTIC_ONLY,
    DIAGNOSTIC_FORCE_RUN,
    str(DIAGNOSTIC_DUMP_DIR),
    DIAG_ENABLED,
)


# ====================================================================
# [NEW] DIAG 모드 판정 헬퍼 (재사용 가능)
# ====================================================================
def is_diag_mode() -> bool:
    """
    DIAG 모드 여부를 반환.
    
    STRATEGY_MODE가 "DIAG" 또는 "INTENT_ONLY"이거나,
    DIAGNOSTIC_MODE 또는 DIAGNOSTIC_ONLY가 True이면 True 반환.
    
    이 함수는 KIS HTTP 호출 차단 로직에서 사용됨.
    LIVE 모드가 아닌 모든 경우를 DIAG로 간주한다.
    """
    return (
        STRATEGY_MODE in ("DIAG", "INTENT_ONLY") 
        or DIAGNOSTIC_MODE 
        or DIAGNOSTIC_ONLY
    )


FORCE_NONTRADING_UNIVERSE_SMOKE = _cfg_bool("FORCE_NONTRADING_UNIVERSE_SMOKE")
NONTRADING_SMOKE_FORCE = _cfg_bool("NONTRADING_SMOKE_FORCE") or FORCE_NONTRADING_UNIVERSE_SMOKE
NONTRADING_SMOKE_DB_STORE = _cfg_bool("NONTRADING_SMOKE_DB_STORE")
NONTRADING_SMOKE_FORCE_REBUILD = _cfg_bool("NONTRADING_SMOKE_FORCE_REBUILD")
NONTRADING_SMOKE_TIMEOUT_SEC = int(_cfg("NONTRADING_SMOKE_TIMEOUT_SEC") or "180")

logger.info(
    "[CONFIG][NONTRADING_SMOKE] force=%s db_store=%s force_rebuild=%s timeout_sec=%s",
    NONTRADING_SMOKE_FORCE,
    NONTRADING_SMOKE_DB_STORE,
    NONTRADING_SMOKE_FORCE_REBUILD,
    NONTRADING_SMOKE_TIMEOUT_SEC,
)

# Universe build flags (support new + legacy env keys)
emergency_build, emergency_src = _env_bool(
    ["UNIVERSE_EMERGENCY_BUILD", "EMERGENCY_BUILD"],
    default=False,
)
force_rebuild, rebuild_src = _env_bool(
    ["UNIVERSE_FORCE_REBUILD", "FORCE_REBUILD_UNIVERSE", "FORCE_REBUILD"],
    default=False,
)
UNIVERSE_NAMESPACE_MODE = (_cfg("UNIVERSE_NAMESPACE_MODE") or "ACCOUNT_ENV").strip().upper()

EMERGENCY_UNIVERSE_BUILD = emergency_build
FORCE_UNIVERSE_REBUILD = force_rebuild

logger.info(
    "[CONFIG][UNIVERSE] emergency_build=%s(src=%s) force_rebuild=%s(src=%s) namespace_mode=%s",
    emergency_build, emergency_src, force_rebuild, rebuild_src, UNIVERSE_NAMESPACE_MODE
)

# 전략별 레짐 축소 우선순위
def _parse_strategy_priority(raw: str) -> list[int]:
    priorities: list[int] = []
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        try:
            value = int(item)
        except ValueError:
            continue
        if 1 <= value <= 5 and value not in priorities:
            priorities.append(value)
    return priorities or [5, 4, 3, 2, 1]


STRATEGY_REDUCTION_PRIORITY = _parse_strategy_priority(
    _cfg("STRATEGY_REDUCTION_PRIORITY")
)


def _parse_active_strategies(raw: str) -> set[int]:
    strategies: set[int] = set()
    for item in (raw or "").split(","):
        item = item.strip()
        if not item:
            continue
        try:
            sid = int(item)
        except ValueError:
            continue
        if sid >= 0:
            strategies.add(sid)
    if not strategies:
        return {1}
    return strategies


ACTIVE_STRATEGIES = _parse_active_strategies(_cfg("ACTIVE_STRATEGIES"))
UNMANAGED_STRATEGY_ID = 0
ALLOW_ADOPT_UNMANAGED = _cfg_bool("ALLOW_ADOPT_UNMANAGED")
# 신고가 → 3일 눌림 → 반등 확인 후 매수 파라미터
USE_PULLBACK_ENTRY = _cfg_bool("USE_PULLBACK_ENTRY", fallback=True)
PULLBACK_LOOKBACK = int(_cfg("PULLBACK_LOOKBACK") or "60")
PULLBACK_DAYS = int(_cfg("PULLBACK_DAYS") or "3")
PULLBACK_REVERSAL_BUFFER_PCT = float(_cfg("PULLBACK_REVERSAL_BUFFER_PCT") or "0.2")
PULLBACK_TOPN = int(_cfg("PULLBACK_TOPN") or "50")
PULLBACK_UNIT_WEIGHT = float(_cfg("PULLBACK_UNIT_WEIGHT") or "0.03")
PULLBACK_MAX_BUYS_PER_DAY = int(_cfg("PULLBACK_MAX_BUYS_PER_DAY") or "5")
CHAMPION_MIN_TRADES = int(_cfg("CHAMPION_MIN_TRADES") or "5")
CHAMPION_MIN_WINRATE = float(_cfg("CHAMPION_MIN_WINRATE") or "45.0")
CHAMPION_MAX_MDD = float(_cfg("CHAMPION_MAX_MDD") or "30.0")
CHAMPION_MIN_SHARPE = float(_cfg("CHAMPION_MIN_SHARPE") or "0.0")

# 챔피언 등급 & GOOD/BAD 타점 판별 파라미터
CHAMPION_A_RULES = {
    "min_trades": 30,
    "min_cumret_pct": 40.0,
    "max_mdd_pct": 25.0,
    "min_win_pct": 50.0,
    "min_sharpe": 1.2,
    "min_turnover": 3_000_000_000,  # 30억
}

GOOD_ENTRY_PULLBACK_RANGE = (5.0, 15.0)  # 신고가 대비 눌림폭(%): 최소~최대
GOOD_ENTRY_MA20_RANGE = (1.0, 1.15)  # 현재가/20MA 허용 구간
GOOD_ENTRY_MAX_FROM_PEAK = 0.97  # 현재가/최근고점 최대치(≤0.97)
GOOD_ENTRY_MIN_RR = 2.0  # 기대수익/리스크 최소 비율
GOOD_ENTRY_MIN_INTRADAY_SIG = 2  # GOOD 타점으로 인정하기 위한 최소 intraday 시그널 개수


def resolve_active_strategies(raw: str | None = None) -> set[int]:
    """환경변수 ACTIVE_STRATEGIES를 우선 적용하여 활성 전략 집합을 반환한다."""

    raw_env = os.getenv("ACTIVE_STRATEGIES") if raw is None else raw
    parsed = _parse_active_strategies(raw_env or "")
    if parsed:
        return parsed
    return ACTIVE_STRATEGIES

BAD_ENTRY_MAX_MA20_DIST = 1.25  # 현재가/20MA 상한(추격매수 방지)
BAD_ENTRY_MAX_PULLBACK = 20.0  # 신고가 대비 눌림폭 상한(과도한 붕괴 방지)
BAD_ENTRY_MAX_BELOW_VWAP_RATIO = 0.7  # 분봉에서 VWAP 아래 체류 비중이 이 이상이면 BAD
NEUTRAL_ENTRY_SCALE = float(_cfg("NEUTRAL_ENTRY_SCALE") or "0.6")


def _parse_hhmm(hhmm: str) -> dtime:
    try:
        hh, mm = hhmm.split(":")
        return dtime(hour=int(hh), minute=int(mm))
    except Exception:
        logger.warning(f"[설정경고] SELL_FORCE_TIME 형식 오류 → 기본값 14:40 적용: {hhmm}")
        return dtime(hour=14, minute=40)


SELL_FORCE_TIME = _parse_hhmm(SELL_FORCE_TIME_STR)
TIME_STOP_TIME = _parse_hhmm(TIME_STOP_HHMM)
ALLOW_WHEN_CLOSED = _cfg_bool("MARKET_DATA_WHEN_CLOSED")
DISABLE_KOSDAQ_LOOP = _cfg_bool("DISABLE_KOSDAQ_LOOP")
DISABLE_KOSPI_ENGINE = _cfg_bool("DISABLE_KOSPI_ENGINE")
ENABLE_BREAKOUT = _cfg_bool("ENABLE_BREAKOUT")
PB1_ENTRY_ENABLED = _cfg_bool("PB1_ENTRY_ENABLED", fallback=True)
LEDGER_LOOKBACK_DAYS = int(_cfg("LEDGER_LOOKBACK_DAYS") or "120")
LEDGER_BASE_DIR = Path(_cfg("LEDGER_BASE_DIR") or runtime_path(*Path(CONFIG["LEDGER_BASE_DIR"]).parts))
ensure_not_repo_tracked_path(LEDGER_BASE_DIR)
PAPER_MAX_CAPITAL_KRW = int(_cfg("PAPER_MAX_CAPITAL_KRW") or "50000000")
PAPER_RESET_AUTO_PURGE = _cfg_bool("PAPER_RESET_AUTO_PURGE", fallback=False)
PAPER_RESET_EVENT_ONLY_IN_PRACTICE = _cfg_bool("PAPER_RESET_EVENT_ONLY_IN_PRACTICE", fallback=True)
PB1_CAPITAL_MODE = (_cfg("PB1_CAPITAL_MODE") or "CASH").strip().upper()
_PB1_ENTRY_CAPITAL_RAW = float(_cfg("PB1_ENTRY_CAPITAL_KRW") or "0")
PB1_ENTRY_CAPITAL_KRW = _PB1_ENTRY_CAPITAL_RAW if _PB1_ENTRY_CAPITAL_RAW > 0 else None
PB1_CASH_RESERVE_PCT = float(_cfg("PB1_CASH_RESERVE_PCT") or "0.10")
PB1_BLOCK_PREOPEN = _cfg_bool("PB1_BLOCK_PREOPEN", fallback=False)
PB1_ENTRY_ALLOW_PREOPEN = _cfg_bool("PB1_ENTRY_ALLOW_PREOPEN", fallback=False)
PB1_ALLOW_PREOPEN_ENTRY = _cfg_bool(
    "PB1_ALLOW_PREOPEN_ENTRY",
    fallback=PB1_ENTRY_ALLOW_PREOPEN,
)
PB1_PREOPEN_START = _cfg("PB1_PREOPEN_START") or "08:45"
PB1_PREOPEN_END = _cfg("PB1_PREOPEN_END") or "09:00"
PB1_PREOPEN_REQUIRE_BALANCE = _cfg_bool("PB1_PREOPEN_REQUIRE_BALANCE", fallback=True)
PB1_PREOPEN_MAX_NEW_POSITIONS = int(_cfg("PB1_PREOPEN_MAX_NEW_POSITIONS") or "0")
PB1_PREOPEN_ORDER_TYPE = (_cfg("PB1_PREOPEN_ORDER_TYPE") or "LIMIT").strip().upper()
PB1_PREOPEN_LIMIT_BUFFER_PCT = float(_cfg("PB1_PREOPEN_LIMIT_BUFFER_PCT") or "0.3")
PB1_REQUIRE_BALANCE_FOR_ENTRY = _cfg_bool("PB1_REQUIRE_BALANCE_FOR_ENTRY", fallback=True)

logger.info("[CONFIG] PAPER_MAX_CAPITAL_KRW=%s", PAPER_MAX_CAPITAL_KRW)
logger.info(
    "[CONFIG][CAPITAL] mode=%s entry_capital=%s cash_reserve_pct=%.2f",
    PB1_CAPITAL_MODE,
    int(_PB1_ENTRY_CAPITAL_RAW),
    PB1_CASH_RESERVE_PCT,
)
MORNING_WINDOW_START = _cfg("MORNING_WINDOW_START") or "09:00"
MORNING_WINDOW_END = _cfg("MORNING_WINDOW_END") or "10:30"
MORNING_EXIT_START = _cfg("MORNING_EXIT_START") or "09:00"
MORNING_EXIT_END = _cfg("MORNING_EXIT_END") or "09:20"
AFTERNOON_WINDOW_START = _cfg("AFTERNOON_WINDOW_START") or "10:30"
AFTERNOON_WINDOW_END = _cfg("AFTERNOON_WINDOW_END") or "15:15"
CLOSE_AUCTION_START = _cfg("CLOSE_AUCTION_START") or "15:15"
CLOSE_AUCTION_END = _cfg("CLOSE_AUCTION_END") or "15:30"
PB1_REQUIRE_VOLUME = _cfg_bool("PB1_REQUIRE_VOLUME", fallback=False)
PB1_FORCE_ENTRY_ON_PUSH = _cfg_bool("PB1_FORCE_ENTRY_ON_PUSH", fallback=True)
PB1_WAIT_FOR_WINDOW = _cfg_bool("PB1_WAIT_FOR_WINDOW", fallback=True)
PB1_MAX_WAIT_FOR_WINDOW_MIN = int(_cfg("PB1_MAX_WAIT_FOR_WINDOW_MIN") or "240")
MARKET_OPEN_HHMM = _cfg("MARKET_OPEN_HHMM") or "09:00"
MARKET_CLOSE_HHMM = _cfg("MARKET_CLOSE_HHMM") or "15:30"
PB1_ENTRY_WINDOW_START = _cfg("PB1_ENTRY_WINDOW_START") or "09:00"
PB1_ENTRY_OPEN_END = _cfg("PB1_ENTRY_OPEN_END") or "09:05"
PB1_ENTRY_WINDOW_END = _cfg("PB1_ENTRY_WINDOW_END") or "15:15"
PB1_EXIT_WINDOW_START = _cfg("PB1_EXIT_WINDOW_START") or "15:15"
PB1_EXIT_WINDOW_END = _cfg("PB1_EXIT_WINDOW_END") or "15:30"
PB1_MORNING_WINDOW_END = _cfg("PB1_MORNING_WINDOW_END") or "10:30"
PB1_PULLBACK_BAND_RELAXED = _cfg_bool("PB1_PULLBACK_BAND_RELAXED", fallback=True)
PB1_PULLBACK_BAND_KOSPI = tuple(float(x.strip()) for x in (_cfg("PB1_PULLBACK_BAND_KOSPI") or "2,25").split(","))
PB1_PULLBACK_BAND_KOSDAQ = tuple(float(x.strip()) for x in (_cfg("PB1_PULLBACK_BAND_KOSDAQ") or "2,28").split(","))
PB1_PULLBACK_BAND_KOSPI_STRICT = tuple(float(x.strip()) for x in (_cfg("PB1_PULLBACK_BAND_KOSPI_STRICT") or "3,8").split(","))
PB1_PULLBACK_BAND_KOSDAQ_STRICT = tuple(float(x.strip()) for x in (_cfg("PB1_PULLBACK_BAND_KOSDAQ_STRICT") or "4,10").split(","))
PB1_RELAX_MA_FILTER = _cfg_bool("PB1_RELAX_MA_FILTER", fallback=True)
PB1_RELAX_MA20_SLOPE = _cfg_bool("PB1_RELAX_MA20_SLOPE", fallback=True)
PB1_MA20_SLOPE_HARD_FAIL_MIN = float(_cfg("PB1_MA20_SLOPE_HARD_FAIL_MIN") or "-0.05")
PB1_VOL_MAX = float(_cfg_with_alias("PB1_VOL_MAX", "PB1_VOL_CONTRACTION_MAX") or "1.00")
PB1_VOLU_MAX = float(_cfg_with_alias("PB1_VOLU_MAX", "PB1_VOLU_CONTRACTION_MAX") or "0.98")
PB1_VOL_CONTRACTION_MAX_STRICT = float(_cfg("PB1_VOL_CONTRACTION_MAX_STRICT") or "1.00")
PB1_VOLU_CONTRACTION_MAX_STRICT = float(_cfg("PB1_VOLU_CONTRACTION_MAX_STRICT") or "0.98")
PB1_VOLU_MAX_INTRADAY = float(_cfg("PB1_VOLU_MAX_INTRADAY") or "1.05")
PB1_PULLBACK_MIN = float(_cfg("PB1_PULLBACK_MIN") or "0.03")
PB1_PULLBACK_MAX = float(_cfg("PB1_PULLBACK_MAX") or "0.18")
PB1_ENTRY_MODE = (_cfg("PB1_ENTRY_MODE") or "BOTH").strip().upper()
if PB1_ENTRY_MODE not in {"PULLBACK", "BREAKOUT", "BOTH"}:
    logger.warning("[CONFIG] PB1_ENTRY_MODE invalid=%s -> fallback=BOTH", PB1_ENTRY_MODE)
    PB1_ENTRY_MODE = "BOTH"
PB1_REQUIRE_BOTH = env_bool("PB1_REQUIRE_BOTH", default=_cfg_bool("PB1_REQUIRE_BOTH", fallback=True))
PB1_REQUIRE_BOTH_CONTRACTIONS = _cfg_bool("PB1_REQUIRE_BOTH_CONTRACTIONS", fallback=PB1_REQUIRE_BOTH)
ENTRY_COND_MODE = (_cfg("ENTRY_COND_MODE") or "OR").strip().upper()
if ENTRY_COND_MODE not in {"OR", "AND", "SETUP_ONLY", "TRIGGER_ONLY"}:
    logger.warning("[CONFIG] ENTRY_COND_MODE invalid=%s -> fallback=OR", ENTRY_COND_MODE)
    ENTRY_COND_MODE = "OR"
PB1_VOL_CONTRACTION_MAX = PB1_VOL_MAX
PB1_VOLU_CONTRACTION_MAX = PB1_VOLU_MAX
PB1_SWING_TREND_MIN = float(_cfg("PB1_SWING_TREND_MIN") or "1.05")
PB1_SWING_VOL_CONTRACTION_MAX = float(_cfg("PB1_SWING_VOL_CONTRACTION_MAX") or "0.80")
PB1_SWING_VOLU_CONTRACTION_MAX = float(_cfg("PB1_SWING_VOLU_CONTRACTION_MAX") or "0.75")
PB1_R_FLOOR_PCT = float(_cfg("PB1_R_FLOOR_PCT") or "2.0")
PB1_DAY_TP_R = float(_cfg("PB1_DAY_TP_R") or "0.8")
PB1_DAY_SL_R = float(_cfg("PB1_DAY_SL_R") or "0.6")
KOSPI_HARD_STOP_PCT = float(_cfg("KOSPI_HARD_STOP_PCT") or "7.0")
KOSDAQ_HARD_STOP_PCT = float(_cfg("KOSDAQ_HARD_STOP_PCT") or "8.0")
PB1_SWING_TRAIL_MA = int(_cfg("PB1_SWING_TRAIL_MA") or "20")
PB1_TIME_STOP_DAYS = int(_cfg("PB1_TIME_STOP_DAYS") or "10")
PB1_MIN_CANDLES = int(_cfg("PB1_MIN_CANDLES") or "60")
MINERVINI_RS_MIN = float(_cfg("MINERVINI_RS_MIN") or "0.80")
MINERVINI_MAX_PYRAMID = int(_cfg("MINERVINI_MAX_PYRAMID") or "3")
MINERVINI_ADD_ON_R = float(_cfg("MINERVINI_ADD_ON_R") or "1.5")
MINERVINI_BREAKOUT_VOL_MULT = float(_cfg("MINERVINI_BREAKOUT_VOL_MULT") or "1.5")
MINERVINI_MAX_EXTENSION_PIVOT = float(_cfg("MINERVINI_MAX_EXTENSION_PIVOT") or "0.05")
MINERVINI_INITIAL_STOP_PCT = float(_cfg("MINERVINI_INITIAL_STOP_PCT") or "0.075")
MINERVINI_TIME_STOP_DAYS = int(_cfg("MINERVINI_TIME_STOP_DAYS") or "20")
MINERVINI_HEAVY_VOL_MULT = float(_cfg("MINERVINI_HEAVY_VOL_MULT") or "1.5")
UNIVERSE_POOL_SIZE = int(_cfg("UNIVERSE_POOL_SIZE") or "200")


def _normalize_index_code(raw: str | None, fallback: str) -> str:
    value = (raw or "").strip()
    if value.isdigit() and len(value) == 6:
        return value
    return fallback


RS_BENCHMARK = _normalize_index_code(_cfg("RS_BENCHMARK"), _cfg("KOSDAQ_ETF_FALLBACK") or "229200")
RS_LOOKBACK_DAYS = int(_cfg("RS_LOOKBACK_DAYS") or "63")
RS_LOOKBACK2_DAYS = int(_cfg("RS_LOOKBACK2_DAYS") or "126")
RS_MIN_PCTILE = float(_cfg("RS_MIN_PCTILE") or "60")
RS_COMPOSITE_W1 = float(_cfg("RS_COMPOSITE_W1") or "0.6")
RS_COMPOSITE_W2 = float(_cfg("RS_COMPOSITE_W2") or "0.4")
REGIME_INDEX = _normalize_index_code(_cfg("REGIME_INDEX"), _cfg("KOSDAQ_ETF_FALLBACK") or "229200")
REGIME_MODE = _cfg("REGIME_MODE") or "STRICT"
REGIME_MA_FAST = int(_cfg("REGIME_MA_FAST") or "50")
REGIME_MA_SLOW = int(_cfg("REGIME_MA_SLOW") or "200")
REGIME_BREADTH_WINDOW = int(_cfg("REGIME_BREADTH_WINDOW") or "20")
REGIME_MAX_RISK = float(_cfg("REGIME_MAX_RISK") or "1.0")
REGIME_MID_RISK = float(_cfg("REGIME_MID_RISK") or "0.6")
REGIME_MIN_RISK = float(_cfg("REGIME_MIN_RISK") or "0.0")
VCP_LOOKBACK = int(_cfg("VCP_LOOKBACK") or "120")
VCP_MIN_SCORE = int(_cfg("VCP_MIN_SCORE") or "40")
PIVOT_BUFFER_PCT = float(_cfg("PIVOT_BUFFER_PCT") or "0.15")
BREAKOUT_VOL_MULT = float(_cfg("BREAKOUT_VOL_MULT") or "1.5")
ENTRY_MODE = (_cfg("ENTRY_MODE") or "BOTH").strip().upper()
MIN_AVG_VALUE_KRW = float(_cfg("MIN_AVG_VALUE_KRW") or "300000000")
MAX_INTRADAY_RANGE_PCT = float(_cfg("MAX_INTRADAY_RANGE_PCT") or "12")
MAX_GAP_UP_PCT = float(_cfg("MAX_GAP_UP_PCT") or "6")
MAX_SPREAD_PROXY_BPS = float(_cfg("MAX_SPREAD_PROXY_BPS") or "80")
RISK_PER_TRADE_PCT = float(_cfg_with_alias("RISK_PER_TRADE_PCT", "MINERVINI_RISK_PCT") or "0.5")
INITIAL_STOP_MODE = (_cfg("INITIAL_STOP_MODE") or "TIGHTLOW").strip().upper()
ATR_WINDOW = int(_cfg("ATR_WINDOW") or "14")
ATR_MULT = float(_cfg("ATR_MULT") or "2.5")
TAKE_PROFIT_R1 = float(_cfg("TAKE_PROFIT_R1") or "2.0")
TP1_SELL_PCT = float(_cfg("TP1_SELL_PCT") or "0.33")
TAKE_PROFIT_R2 = float(_cfg("TAKE_PROFIT_R2") or "3.0")
TP2_SELL_PCT = float(_cfg("TP2_SELL_PCT") or "0.33")
TRAIL_MODE = (_cfg("TRAIL_MODE") or "MA20").strip().upper()
TRAIL_STEP_AFTER_R = float(_cfg("TRAIL_STEP_AFTER_R") or "1.5")
FAILED_BREAKOUT_EXIT_DAYS = int(_cfg("FAILED_BREAKOUT_EXIT_DAYS") or "2")
REENTRY_COOLDOWN_DAYS = int(_cfg("REENTRY_COOLDOWN_DAYS") or "10")


PB1_MAX_POSITIONS = int(_cfg("PB1_MAX_POSITIONS") or "30")
PB1_TARGET_NEW_POSITIONS = max(1, int(_cfg("PB1_TARGET_NEW_POSITIONS") or "30"))
PB1_MIN_SCORE_BASE = float(_cfg("PB1_MIN_SCORE_BASE") or "70")
PB1_MIN_SCORE_FLOOR = float(_cfg("PB1_MIN_SCORE_FLOOR") or "55")
PB1_MIN_SCORE_STEP = float(_cfg("PB1_MIN_SCORE_STEP") or "5")
PB1_MIN_BUYABLE = max(1, int(_cfg("PB1_MIN_BUYABLE") or "1"))
PB1_USE_MINERVINI_AS_RANK_ONLY = _cfg_bool("PB1_USE_MINERVINI_AS_RANK_ONLY", fallback=True)
PB1_MINERVINI_HARD_GATE = _cfg_bool("PB1_MINERVINI_HARD_GATE", fallback=False)
PB1_EMERGENCY_ORDER_ENABLED = _cfg_bool("PB1_EMERGENCY_ORDER_ENABLED", fallback=False)
PB1_EMERGENCY_DIAG_ONLY = _cfg_bool("PB1_EMERGENCY_DIAG_ONLY", fallback=True)
PB1_MIN_SCORE = float(_cfg("PB1_MIN_SCORE") or str(PB1_MIN_SCORE_BASE))
PB1_BOOTSTRAP_ENABLE = _cfg_bool("PB1_BOOTSTRAP_ENABLE", fallback=True)
BOOTSTRAP_MINERVINI_RS_MIN_PCTILE = int(_cfg("BOOTSTRAP_MINERVINI_RS_MIN_PCTILE") or "60")
BOOTSTRAP_MINERVINI_VCP_MIN_SCORE = int(_cfg("BOOTSTRAP_MINERVINI_VCP_MIN_SCORE") or "45")
BOOTSTRAP_RELAX_PASSES = int(_cfg("BOOTSTRAP_RELAX_PASSES") or "5")
BOOTSTRAP_KEEP_TREND_TEMPLATE_ALWAYS = _cfg_bool("BOOTSTRAP_KEEP_TREND_TEMPLATE_ALWAYS", fallback=False)
BOOTSTRAP_PB1_VOL_MAX = float(_cfg("BOOTSTRAP_PB1_VOL_MAX") or "1.15")
BOOTSTRAP_PB1_VOLU_MAX = float(_cfg("BOOTSTRAP_PB1_VOLU_MAX") or "1.15")
BOOTSTRAP_PB1_PULLBACK_MIN = float(_cfg("BOOTSTRAP_PB1_PULLBACK_MIN") or "0.01")
BOOTSTRAP_PB1_PULLBACK_MAX = float(_cfg("BOOTSTRAP_PB1_PULLBACK_MAX") or "0.25")
BOOTSTRAP_PB1_REQUIRE_BOTH_CONTRACTIONS = _cfg_bool("BOOTSTRAP_PB1_REQUIRE_BOTH_CONTRACTIONS", fallback=False)
BOOTSTRAP_PB1_MIN_SCORE_BASE = float(_cfg("BOOTSTRAP_PB1_MIN_SCORE_BASE") or "55")
BOOTSTRAP_PB1_MIN_SCORE_FLOOR = float(_cfg("BOOTSTRAP_PB1_MIN_SCORE_FLOOR") or "45")
BOOTSTRAP_PB1_MIN_SCORE_STEP = float(_cfg("BOOTSTRAP_PB1_MIN_SCORE_STEP") or "5")
BOOTSTRAP_SCORE_CUT_KEEP_TOPN = int(_cfg("BOOTSTRAP_SCORE_CUT_KEEP_TOPN") or "3")
BOOTSTRAP_FORCE_MIN_1_SHARE = _cfg_bool("BOOTSTRAP_FORCE_MIN_1_SHARE", fallback=True)
BOOTSTRAP_MIN1_TOPN = int(_cfg("BOOTSTRAP_MIN1_TOPN") or "3")
FORCE_MIN1_OVERRIDE_POSITION_CAP = _cfg_bool("FORCE_MIN1_OVERRIDE_POSITION_CAP", fallback=True)
FORCE_MIN1_OVERRIDE_TOPN = int(_cfg("FORCE_MIN1_OVERRIDE_TOPN") or "3")
PB1_FAILMODE_SOFT = env_bool("PB1_FAILMODE_SOFT", default=True)
PB1_MIN_CANDIDATES = int(_cfg("PB1_MIN_CANDIDATES") or "3")
PB1_RELAX_MAX_PASSES = int(_cfg("PB1_RELAX_MAX_PASSES") or "3")
PB1_SPREAD_HARD_MAX_PCT = float(_cfg("PB1_SPREAD_HARD_MAX_PCT") or "0")
PB1_GAP_HARD_MAX_PCT = float(_cfg("PB1_GAP_HARD_MAX_PCT") or "0")
PB1_ENTRY_BUDGET_PCT_PER_TICK = float(_cfg("PB1_ENTRY_BUDGET_PCT_PER_TICK") or "0.60")
PB1_MAX_POS_PCT = float(_cfg("PB1_MAX_POS_PCT") or "0.35")
PB1_USE_RISK_PARITY = _cfg_bool("PB1_USE_RISK_PARITY", fallback=True)
ATR_MAX_PCT, ATR_MAX_PCT_SOURCE, ATR_MAX_PCT_RAW = _resolve_atr_max_pct(
    "ATR_MAX_PCT",
    "ATR_MAX",
    default_ratio=0.10,
)
PB1_MAX_ATR_PCT = ATR_MAX_PCT
PB1_MAX_ATR_PCT_SOURCE = ATR_MAX_PCT_SOURCE
PB1_MAX_ATR_PCT_RAW = ATR_MAX_PCT_RAW
MINERVINI_MAX_ATR_PCT = ATR_MAX_PCT
MINERVINI_MAX_ATR_PCT_SOURCE = ATR_MAX_PCT_SOURCE
MINERVINI_MAX_ATR_PCT_RAW = ATR_MAX_PCT_RAW
PB1_MIN_VALUE20 = float(_cfg("PB1_MIN_VALUE20") or "3000000000")
PB1_ALLOW_ADD_TO_EXISTING = _cfg_bool("PB1_ALLOW_ADD_TO_EXISTING")
PB1_LOG_ENTRY_GATE = _cfg_bool("PB1_LOG_ENTRY_GATE", fallback=True)
PB1_LOG_DROP_REASONS_TOPN = int(_cfg("PB1_LOG_DROP_REASONS_TOPN") or "10")
PB1_OHLCV_DAYS_BASE = int(_cfg("PB1_OHLCV_DAYS_BASE") or "200")

# === [NEW] PB1 Watchlist 환경변수 ===
PB1_WATCHLIST_ENABLED = _cfg_bool("PB1_WATCHLIST_ENABLED", fallback=True)
PB1_WATCHLIST_TOPK = int(_cfg("PB1_WATCHLIST_TOPK") or "50")
PB1_WATCHLIST_FINALN = int(_cfg("PB1_WATCHLIST_FINALN") or "30")
PB1_WATCHLIST_MIN_PRICE = float(_cfg("PB1_WATCHLIST_MIN_PRICE") or "2000")
PB1_WATCHLIST_LIQ_DAYS = int(_cfg("PB1_WATCHLIST_LIQ_DAYS") or "20")
PB1_WATCHLIST_MIN_ROWS = int(_cfg("PB1_WATCHLIST_MIN_ROWS") or "30")
PB1_WATCHLIST_FORCE_REBUILD = _cfg_bool("PB1_WATCHLIST_FORCE_REBUILD", fallback=False)

# -----------------------------
# PB1 Candidate Prefilter / Early Stop Controls
# -----------------------------
# Prefilter: reduce 120 scan to N for heavy computations (default 50 as current log)
PB1_PREFILTER_LIMIT = int(_cfg("PB1_PREFILTER_LIMIT") or "50")
PB1_PREFILTER_LOOKBACK_DAYS = int(_cfg("PB1_PREFILTER_LOOKBACK_DAYS") or "30")

# Early stop master switch
PB1_EARLY_STOP_ENABLED = _cfg_bool("PB1_EARLY_STOP_ENABLED", fallback=True)

# Old behavior: stop when we "collected candidates >= N"
# New behavior options: stop when we have enough "GOOD" candidates.
# Modes:
#  - "raw": old behavior (count any candidates produced by pre-stage)
#  - "minervini_pass": stop only after Minervini pass count reaches N
#  - "setup_ok": stop only after PB1 setup_ok count reaches N
#  - "buyable": stop only after buyable (qty>0 & min_order) reaches N  (best)
PB1_EARLY_STOP_MODE = str(_cfg("PB1_EARLY_STOP_MODE") or "buyable").strip().lower()

# Stop threshold (how many "good candidates" are enough)
PB1_EARLY_STOP_N = int(_cfg("PB1_EARLY_STOP_N") or "50")

# Safety: always evaluate at least this many symbols (even if early stop condition met early)
PB1_EARLY_STOP_MIN_EVAL = int(_cfg("PB1_EARLY_STOP_MIN_EVAL") or "50")

# For DIAG runs: ignore entry cutoff but still do full scan
PB1_DIAG_IGNORE_ENTRY_CUTOFF = _cfg_bool("PB1_DIAG_IGNORE_ENTRY_CUTOFF", fallback=True)

# === [NEW] Candidate Pool 환경변수 ===
CANDIDATE_POOL_ENABLED = _cfg_bool("CANDIDATE_POOL_ENABLED", fallback=True)
CANDIDATE_POOL_TTL_DAYS = int(_cfg("CANDIDATE_POOL_TTL_DAYS") or "7")
CANDIDATE_POOL_SIZE = int(_cfg("CANDIDATE_POOL_SIZE") or "120")
CANDIDATE_POOL_MIN_SIZE = int(_cfg("CANDIDATE_POOL_MIN_SIZE") or "40")
CANDIDATE_POOL_STRATEGY_KEY = _cfg("CANDIDATE_POOL_STRATEGY_KEY") or "pb1_candidate_pool"
CANDIDATE_POOL_FORCE_REBUILD = _cfg_bool("CANDIDATE_POOL_FORCE_REBUILD", fallback=False)
CANDIDATE_POOL_MIN_PRICE = float(_cfg("CANDIDATE_POOL_MIN_PRICE") or "2000.0")
CANDIDATE_POOL_LIQ_DAYS = int(_cfg("CANDIDATE_POOL_LIQ_DAYS") or "20")
CANDIDATE_POOL_MIN_ROWS = int(_cfg("CANDIDATE_POOL_MIN_ROWS") or "30")

# === [NEW] Derived Fallback 환경변수 ===
DERIVED_FALLBACK_ENABLED = _cfg_bool("DERIVED_FALLBACK_ENABLED", fallback=True)
DERIVED_FALLBACK_MAX_DAYS = int(_cfg("DERIVED_FALLBACK_MAX_DAYS") or "3")
DERIVED_FALLBACK_WARN_AGE_DAYS = int(_cfg("DERIVED_FALLBACK_WARN_AGE_DAYS") or "2")

# === [2026-04-29] Effective Exit Policy 상수 ===
PB1_EXISTING_POSITION_EFFECTIVE_EXIT_ENABLED = _cfg_bool("PB1_EXISTING_POSITION_EFFECTIVE_EXIT_ENABLED", fallback=True)
PB1_EFFECTIVE_STOP_CAP_ENABLED = _cfg_bool("PB1_EFFECTIVE_STOP_CAP_ENABLED", fallback=True)
PB1_EFFECTIVE_STOP_CAP_KOSPI_PCT = float(_cfg("PB1_EFFECTIVE_STOP_CAP_KOSPI_PCT") or "7.0")
PB1_EFFECTIVE_STOP_CAP_KOSDAQ_PCT = float(_cfg("PB1_EFFECTIVE_STOP_CAP_KOSDAQ_PCT") or "8.0")
PB1_PROFIT_PROTECT_ENABLED = _cfg_bool("PB1_PROFIT_PROTECT_ENABLED", fallback=True)
PB1_PROFIT_PROTECT_PCT = float(_cfg("PB1_PROFIT_PROTECT_PCT") or "8.0")
PB1_PROFIT_PROTECT_SELL_PCT = float(_cfg("PB1_PROFIT_PROTECT_SELL_PCT") or "0.33")
PB1_ABS_TP1_ENABLED = _cfg_bool("PB1_ABS_TP1_ENABLED", fallback=True)
PB1_ABS_TP1_PROFIT_PCT = float(_cfg("PB1_ABS_TP1_PROFIT_PCT") or "10.0")
PB1_ABS_TP1_SELL_PCT = float(_cfg("PB1_ABS_TP1_SELL_PCT") or "0.33")

# === [2026-04-29] Multi-Layer Exit Router 상수 ===
PB1_EXIT_ROUTER_ENABLED = _cfg_bool("PB1_EXIT_ROUTER_ENABLED", fallback=True)
PB1_PERCENT_EXIT_ENABLED = _cfg_bool("PB1_PERCENT_EXIT_ENABLED", fallback=True)
PB1_EXIT_POLICY_MODE = _cfg("PB1_EXIT_POLICY_MODE") or "HYBRID_BY_ENTRY_STYLE"
# SWING 수익률 기준
PB1_SWING_PROFIT_ACTIVATE_PCT = float(_cfg("PB1_SWING_PROFIT_ACTIVATE_PCT") or "8.0")
PB1_SWING_PROFIT_GIVEBACK_PCT = float(_cfg("PB1_SWING_PROFIT_GIVEBACK_PCT") or "3.0")
PB1_SWING_PROFIT_FLOOR_PCT = float(_cfg("PB1_SWING_PROFIT_FLOOR_PCT") or "5.0")
PB1_SWING_TP1_PROFIT_PCT = float(_cfg("PB1_SWING_TP1_PROFIT_PCT") or "12.0")
PB1_SWING_TP2_PROFIT_PCT = float(_cfg("PB1_SWING_TP2_PROFIT_PCT") or "18.0")
# MOMENTUM 전용
PB1_MOMENTUM_PROFIT_ACTIVATE_PCT = float(_cfg("PB1_MOMENTUM_PROFIT_ACTIVATE_PCT") or "5.0")
PB1_MOMENTUM_PROFIT_GIVEBACK_PCT = float(_cfg("PB1_MOMENTUM_PROFIT_GIVEBACK_PCT") or "2.0")
PB1_MOMENTUM_TP1_R = float(_cfg("PB1_MOMENTUM_TP1_R") or "1.5")
PB1_MOMENTUM_TP2_R = float(_cfg("PB1_MOMENTUM_TP2_R") or "2.5")
# CORE 전용
PB1_CORE_TP1_PROFIT_PCT = float(_cfg("PB1_CORE_TP1_PROFIT_PCT") or "20.0")
PB1_CORE_TP1_SELL_PCT = float(_cfg("PB1_CORE_TP1_SELL_PCT") or "0.25")
PB1_CORE_TP2_PROFIT_PCT = float(_cfg("PB1_CORE_TP2_PROFIT_PCT") or "30.0")
PB1_CORE_TP2_SELL_PCT = float(_cfg("PB1_CORE_TP2_SELL_PCT") or "0.25")

logger.info(
    "[CONFIG][EFFECTIVE_EXIT] existing_pos_eff=%s stop_cap=%s kospi_pct=%.1f kosdaq_pct=%.1f "
    "profit_protect=%s pct=%.1f abs_tp1=%s pct=%.1f",
    int(PB1_EXISTING_POSITION_EFFECTIVE_EXIT_ENABLED),
    int(PB1_EFFECTIVE_STOP_CAP_ENABLED),
    PB1_EFFECTIVE_STOP_CAP_KOSPI_PCT,
    PB1_EFFECTIVE_STOP_CAP_KOSDAQ_PCT,
    int(PB1_PROFIT_PROTECT_ENABLED),
    PB1_PROFIT_PROTECT_PCT,
    int(PB1_ABS_TP1_ENABLED),
    PB1_ABS_TP1_PROFIT_PCT,
)
# ttl_days는 CANDIDATE_POOL_TTL_DAYS를 재사용

# Minervini-only mode (analytics without trading)
MINERVINI_ONLY = _cfg_bool("MINERVINI_ONLY", fallback=False)

# 추가 상수
ALLOW_KIS_DAILY_FALLBACK = _cfg_bool("ALLOW_KIS_DAILY_FALLBACK", fallback=False)
PB1_MAX_DAILY_FETCH_PER_TICK = int(_cfg("PB1_MAX_DAILY_FETCH_PER_TICK") or "20")
PB1_MAX_PRICE_FETCH_PER_TICK = int(_cfg("PB1_MAX_PRICE_FETCH_PER_TICK") or "30")
KIS_RATE_LIMIT_COOLDOWN_SEC = float(_cfg("KIS_RATE_LIMIT_COOLDOWN_SEC") or "8")
PRICE_SNAPSHOT_TTL_SEC = float(_cfg("PRICE_SNAPSHOT_TTL_SEC") or "2")
DAILY_BAR_TTL_SEC = float(_cfg("DAILY_BAR_TTL_SEC") or "1800")

logger.info(
    "[CONFIG][PB1] entry_mode=%s require_both=%s entry_cond_mode=%s entry_budget_pct=%.2f max_pos_pct=%.2f vol_max=%.2f volu_max=%.2f volu_max_intraday=%.2f pullback_min=%.3f pullback_max=%.3f require_both_contractions=%s min_score_base=%.1f min_score_floor=%.1f min_score_step=%.1f failmode_soft=%s relax_passes=%s min_candidates=%s watchlist_enabled=%s watchlist_topk=%s watchlist_finaln=%s prefilter_limit=%s early_stop_enabled=%s early_stop_mode=%s early_stop_n=%s early_stop_min_eval=%s",
    PB1_ENTRY_MODE,
    int(PB1_REQUIRE_BOTH),
    ENTRY_COND_MODE,
    PB1_ENTRY_BUDGET_PCT_PER_TICK,
    PB1_MAX_POS_PCT,
    PB1_VOL_MAX,
    PB1_VOLU_MAX,
    PB1_VOLU_MAX_INTRADAY,
    PB1_PULLBACK_MIN,
    PB1_PULLBACK_MAX,
    int(PB1_REQUIRE_BOTH_CONTRACTIONS),
    PB1_MIN_SCORE_BASE,
    PB1_MIN_SCORE_FLOOR,
    PB1_MIN_SCORE_STEP,
    int(PB1_FAILMODE_SOFT),
    PB1_RELAX_MAX_PASSES,
    PB1_MIN_CANDIDATES,
    int(PB1_WATCHLIST_ENABLED),
    PB1_WATCHLIST_TOPK,
    PB1_WATCHLIST_FINALN,
    PB1_PREFILTER_LIMIT,
    int(PB1_EARLY_STOP_ENABLED),
    PB1_EARLY_STOP_MODE,
    PB1_EARLY_STOP_N,
    PB1_EARLY_STOP_MIN_EVAL,
)
logger.info(
    "[CONFIG][PB1][BOOTSTRAP] enabled=%s minervini_rs=%s minervini_vcp=%s relax_passes=%s keep_trend=%s pb1={vol_max:%.2f volu_max:%.2f pullback_min:%.3f pullback_max:%.3f require_both:%s min_score_base:%.1f min_score_floor:%.1f min_score_step:%.1f} score_keep_topn=%s force_min1=%s min1_topn=%s",
    int(PB1_BOOTSTRAP_ENABLE),
    BOOTSTRAP_MINERVINI_RS_MIN_PCTILE,
    BOOTSTRAP_MINERVINI_VCP_MIN_SCORE,
    BOOTSTRAP_RELAX_PASSES,
    int(BOOTSTRAP_KEEP_TREND_TEMPLATE_ALWAYS),
    BOOTSTRAP_PB1_VOL_MAX,
    BOOTSTRAP_PB1_VOLU_MAX,
    BOOTSTRAP_PB1_PULLBACK_MIN,
    BOOTSTRAP_PB1_PULLBACK_MAX,
    int(BOOTSTRAP_PB1_REQUIRE_BOTH_CONTRACTIONS),
    BOOTSTRAP_PB1_MIN_SCORE_BASE,
    BOOTSTRAP_PB1_MIN_SCORE_FLOOR,
    BOOTSTRAP_PB1_MIN_SCORE_STEP,
    BOOTSTRAP_SCORE_CUT_KEEP_TOPN,
    int(BOOTSTRAP_FORCE_MIN_1_SHARE),
    BOOTSTRAP_MIN1_TOPN,
)
logger.info(
    "[CONFIG][RISK] ATR_MAX_PCT=%.2f source=%s",
    PB1_MAX_ATR_PCT,
    PB1_MAX_ATR_PCT_SOURCE,
)
logger.info(
    "[CONFIG][RISK] ATR_MAX_PCT=%.2f source=%s",
    MINERVINI_MAX_ATR_PCT,
    MINERVINI_MAX_ATR_PCT_SOURCE,
)
logger.info(
    "[CONFIG][CANDIDATE_POOL] enabled=%s ttl_days=%s size=%s min_size=%s strategy_key=%s force_rebuild=%s",
    int(CANDIDATE_POOL_ENABLED),
    CANDIDATE_POOL_TTL_DAYS,
    CANDIDATE_POOL_SIZE,
    CANDIDATE_POOL_MIN_SIZE,
    CANDIDATE_POOL_STRATEGY_KEY,
    int(CANDIDATE_POOL_FORCE_REBUILD),
)
logger.info(
    "[CONFIG][DERIVED_FALLBACK] enabled=%s max_back_days=%s warn_age_days=%s ttl_days=%s",
    int(DERIVED_FALLBACK_ENABLED),
    DERIVED_FALLBACK_MAX_DAYS,
    DERIVED_FALLBACK_WARN_AGE_DAYS,
    CANDIDATE_POOL_TTL_DAYS,
)
logger.info(
    "[CONFIG][MINERVINI] universe_pool=%s rs_benchmark=%s rs_lookbacks=%s/%s rs_min_pctile=%s regime_index=%s regime_mode=%s vcp_lookback=%s vcp_min_score=%s entry_mode=%s risk_pct=%s",
    UNIVERSE_POOL_SIZE,
    RS_BENCHMARK,
    RS_LOOKBACK_DAYS,
    RS_LOOKBACK2_DAYS,
    RS_MIN_PCTILE,
    REGIME_INDEX,
    REGIME_MODE,
    VCP_LOOKBACK,
    VCP_MIN_SCORE,
    ENTRY_MODE,
    RISK_PER_TRADE_PCT,
)
logger.info("[ENV] MINERVINI_ONLY=%s", int(MINERVINI_ONLY))
ALLOW_KIS_DATA_HTTP_IN_DIAG = env_bool("ALLOW_KIS_DATA_HTTP_IN_DIAG", default=False)
_force_run_enabled = env_bool("FORCE_RUN", default=False)
_force_block_live_env = env_bool("FORCE_BLOCK_LIVE", default=False)
_disable_live_trading_env = env_bool("DISABLE_LIVE_TRADING", default=False)
_dry_run_env = env_bool("DRY_RUN", default=False) or env_bool("DRYRUN", default=False)
_live_trading_enabled_raw = os.getenv("LIVE_TRADING_ENABLED")
_live_trading_disabled_env = _live_trading_enabled_raw is not None and not env_bool("LIVE_TRADING_ENABLED", default=True)
if _force_run_enabled:
    if _dry_run_env or _disable_live_trading_env or _force_block_live_env or _live_trading_disabled_env:
        FORCE_RUN_ROUTE = "force_run_dry"
    else:
        FORCE_RUN_ROUTE = "force_run_live"
else:
    FORCE_RUN_ROUTE = "none"
_force_market_window_raw = (os.getenv("FORCE_MARKET_WINDOW") or "").strip().lower()
_pb1_window_override_raw = (os.getenv("PB1_WINDOW_OVERRIDE") or "").strip().lower()
logger.info(
    "[CONFIG][KIS_HTTP] strategy_mode=%s allow_kis_data_http_in_diag=%s",
    (os.getenv("STRATEGY_MODE", "").strip().upper() or "UNKNOWN"),
    int(ALLOW_KIS_DATA_HTTP_IN_DIAG),
)
logger.info(
    "[CONFIG][HTTP_POLICY] diag_blocks_order=%s diag_allows_data_http=%s token_allowed_in_diag=%s",
    1,
    int(ALLOW_KIS_DATA_HTTP_IN_DIAG),
    int(ALLOW_KIS_DATA_HTTP_IN_DIAG),
)
logger.info(
    "[CONFIG][FORCE_RUN] force_run=%s route=%s force_market_window=%s pb1_window_override=%s allow_kis_data_http_in_diag=%s",
    int(_force_run_enabled),
    FORCE_RUN_ROUTE,
    _force_market_window_raw or "none",
    _pb1_window_override_raw or "none",
    int(ALLOW_KIS_DATA_HTTP_IN_DIAG),
)
# === [NEW] 주간 리밸런싱 강제 트리거 상태 파일 ===
STATE_WEEKLY_PATH = Path(__file__).parent / "state_weekly.json"

def _this_iso_week_key(now=None):
    now = now or datetime.now(KST)
    return f"{now.year}-W{now.isocalendar().week:02d}"


def _normalize_strategy_mode(raw: str | None) -> str | None:
    normalized = (raw or "").strip().upper()
    if normalized in {"LIVE", "EXECUTE", "EXECUTION"}:
        return "LIVE"
    if normalized in {"DIAG", "DIAGNOSTIC", "INTENT_ONLY", "INTENT"}:
        return "DIAG"
    return None


def resolve_market_window(now: datetime, trading_day: bool) -> str:
    from trader.time_utils import calc_market_window_kst

    if now.tzinfo is None:
        now = now.replace(tzinfo=KST)
    if not trading_day:
        return "after"
    return calc_market_window_kst(now)


def resolve_trade_flags(
    *,
    strategy_mode: str,
    live_trading_enabled: bool,
    disable_live_trading: bool,
    kis_http_enabled: bool,
    requested_dry_run: bool | None,
) -> dict:
    """
    Single source of truth for trade flags.
    Rule: If LIVE is intended (strategy_mode == 'LIVE' and live_trading_enabled and not disable_live_trading and kis_http_enabled),
    then dry_run MUST be False regardless of any other heuristics.
    """
    intended_live = (
        (strategy_mode or "").upper() == "LIVE"
        and bool(live_trading_enabled)
        and not bool(disable_live_trading)
        and bool(kis_http_enabled)
    )

    if intended_live:
        # absolute lock: never allow dry_run in live intent
        dry_run = False
        reasons = ["intended_live_lock"]
    else:
        # follow requested dry_run if explicitly given, else default True
        dry_run = True if requested_dry_run is None else bool(requested_dry_run)
        reasons = ["requested_or_default"]

    return {"intended_live": intended_live, "dry_run": dry_run, "reasons": reasons}


def resolve_strategy_mode(
    now_kst: datetime | None = None,
    force_mode_env: str | None = None,
) -> tuple[str, bool, str, str]:
    now_kst = now_kst or datetime.now(KST)
    if now_kst.tzinfo is None:
        now_kst = now_kst.replace(tzinfo=KST)

    force_run = env_bool("FORCE_RUN", default=False)
    force_window = (os.getenv("FORCE_MARKET_WINDOW") or os.getenv("PB1_WINDOW_OVERRIDE") or "").strip().lower()
    force_window = force_window if force_window in {"preopen", "morning", "day", "close", "after"} else "day"
    if force_run:
        live_blocked = any(
            (
                env_bool("DRY_RUN", default=False),
                env_bool("DRYRUN", default=False),
                env_bool("DISABLE_LIVE_TRADING", default=False),
                env_bool("FORCE_BLOCK_LIVE", default=False),
                os.getenv("LIVE_TRADING_ENABLED") is not None and not env_bool("LIVE_TRADING_ENABLED", default=True),
            )
        )
        if live_blocked:
            return "DIAG", True, force_window, "force_run_dry"
        return "LIVE", True, force_window, "force_run_live"

    trading_day = now_kst.weekday() < 5
    window = resolve_market_window(now_kst, trading_day)
    forced = _normalize_strategy_mode(force_mode_env)
    if forced:
        return forced, trading_day, window, "force"
    market_open = _parse_hhmm(MARKET_OPEN_HHMM)
    market_close = _parse_hhmm(MARKET_CLOSE_HHMM)
    in_market = trading_day and (market_open <= now_kst.time() <= market_close)
    mode = "LIVE" if in_market else "DIAG"
    return mode, trading_day, window, "auto"


# ================================================================
# Live Gate - 시간 기반 자동 Live Trading 허용/차단
# ================================================================
from trader.live_gate import compute_live_gate, LiveGateStatus

# 현재 시각 기준으로 live gate 상태 계산
_now_kst_init = datetime.now(KST)

# KIS_ENV 가져오기 (settings.py에서 import)
from settings import KIS_ENV as _KIS_ENV_FROM_SETTINGS

# Strategy mode 결정
_strategy_mode_raw = os.getenv("STRATEGY_MODE", "").strip().upper()
if not _strategy_mode_raw:
    # 자동 결정: 거래 시간대이면 LIVE, 아니면 DIAG
    _auto_mode, _, _, _ = resolve_strategy_mode(_now_kst_init)
    _strategy_mode_raw = _auto_mode

# Dry-run / Analysis-only 플래그
_dryrun = env_bool("DRY_RUN", default=False)
_analysis_only = MINERVINI_ONLY  # 이미 위에서 정의됨

# Live Gate 상태 계산
LIVE_GATE_STATUS: LiveGateStatus = compute_live_gate(
    _now_kst_init,
    kis_env=_KIS_ENV_FROM_SETTINGS,
    strategy_mode=_strategy_mode_raw,
    dryrun=_dryrun,
    analysis_only=_analysis_only,
)

# 전역 변수로 공개
ALLOW_LIVE_GATE: bool = LIVE_GATE_STATUS.allow_live_gate
FORCE_BLOCK_LIVE: bool = LIVE_GATE_STATUS.force_block_live

# 로깅
logger.info(
    "[LIVE_GATE] allow_live_gate=%s force_block_live=%s reason=%s trading_day=%s window=%s now_kst=%s strategy_mode=%s dryrun=%s analysis_only=%s",
    int(ALLOW_LIVE_GATE),
    int(FORCE_BLOCK_LIVE),
    LIVE_GATE_STATUS.reason,
    int(LIVE_GATE_STATUS.trading_day),
    LIVE_GATE_STATUS.window,
    LIVE_GATE_STATUS.now_kst.isoformat(),
    _strategy_mode_raw,
    int(_dryrun),
    int(_analysis_only),
)
