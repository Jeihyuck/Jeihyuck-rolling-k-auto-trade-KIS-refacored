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
    "CAP_CAP": "0.8",
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
    "EMERGENCY_GLOBAL_SELL": "false",
    "STRATEGY_REDUCTION_PRIORITY": "5,4,3,2,1",
    # Diagnostics
    "DIAGNOSTIC_MODE": "false",
    "DIAGNOSTIC_ONLY": "false",
    "DIAGNOSTIC_FORCE_RUN": "false",
    "DIAGNOSTIC_DUMP_PATH": "trader/state/diagnostics",
    "DIAGNOSTIC_TARGET_MARKETS": "",
    "DIAGNOSTIC_MAX_SYMBOLS": "200",
    # === Strategy intent/exec defaults ===
    "ENABLED_STRATEGIES": "",
    "STRATEGY_MODE": "INTENT_ONLY",  # INTENT_ONLY | LIVE
    "STRATEGY_DRY_RUN": "true",
    "STRATEGY_INTENTS_PATH": "trader/state/strategy_intents.jsonl",
    "STRATEGY_INTENTS_STATE_PATH": "trader/state/strategy_intents_state.json",
    "STRATEGY_MAX_OPEN_INTENTS": "20",
    "STRATEGY_MAX_POSITION_PCT": "0.10",
    "STRATEGY_ALLOW_SELL_ONLY": "false",
    "STRATEGY_WEIGHTS": "",
    "DISABLE_KOSDAQ_LOOP": "false",
    "DISABLE_KOSPI_ENGINE": "false",
    "ACTIVE_STRATEGIES": "1",  # CSV of strategy IDs eligible for managed exits/entries
    "ALLOW_ADOPT_UNMANAGED": "false",
    "STATE_PATH": "trader/state/state.json",
    # PB1 close-pullback defaults
    "ENABLE_BREAKOUT": "false",
    "LEDGER_LOOKBACK_DAYS": "120",
    "BOTSTATE_LOCK_TTL_SEC": "240",
    "LEDGER_BASE_DIR": "bot_state/trader_ledger",
    "PB1_ENTRY_ENABLED": "true",
    "MORNING_WINDOW_START": "08:50",
    "MORNING_WINDOW_END": "15:20",
    "MORNING_EXIT_START": "09:00",
    "MORNING_EXIT_END": "09:20",
    "AFTERNOON_WINDOW_START": "15:20",
    "AFTERNOON_WINDOW_END": "15:30",
    "CLOSE_AUCTION_START": "15:20",
    "CLOSE_AUCTION_END": "15:30",
    "PB1_REQUIRE_VOLUME": "1",
    "PB1_FORCE_ENTRY_ON_PUSH": "1",
    "PB1_WAIT_FOR_WINDOW": "1",
    "PB1_MAX_WAIT_FOR_WINDOW_MIN": "240",
    "MARKET_OPEN_HHMM": "08:50",
    "MARKET_CLOSE_HHMM": "15:30",
    "PB1_PULLBACK_BAND_KOSPI": "3,8",
    "PB1_PULLBACK_BAND_KOSDAQ": "4,10",
    "PB1_VOL_CONTRACTION_MAX": "0.80",
    "PB1_VOLU_CONTRACTION_MAX": "0.75",
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
    # PB1 "최고 눌림목" 랭킹/사이징 튜닝
    "PB1_MAX_POSITIONS": "8",          # 통과 종목 중 상위 N개만 매수
    "PB1_MIN_SCORE": "60",             # 점수 컷(0~100)
    "PB1_USE_RISK_PARITY": "1",         # 1이면 ATR 기반 리스크패리티 사이징
    "PB1_MAX_ATR_PCT": "6.0",           # ATR% 상한 (과변동 종목 제외)
    "PB1_MIN_VALUE20": "3000000000",    # 20일 평균 거래대금(원) 하한 (유동성 컷)
}


def _cfg(key: str) -> str:
    """환경변수 > CONFIG 기본값"""
    return os.getenv(key, CONFIG.get(key, ""))


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


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)
STATE_FILE = Path(__file__).parent / "trade_state.json"  # legacy; position state uses STATE_PATH
STATE_DIR_RAW = _cfg("STATE_DIR")
STATE_DIR = Path(STATE_DIR_RAW) if STATE_DIR_RAW else Path(__file__).parent / "state"
STATE_PATH = Path(_cfg("STATE_PATH") or STATE_DIR / "state.json")
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
STRATEGY_INTENTS_PATH = Path(_cfg("STRATEGY_INTENTS_PATH") or CONFIG["STRATEGY_INTENTS_PATH"])
STRATEGY_INTENTS_STATE_PATH = Path(
    _cfg("STRATEGY_INTENTS_STATE_PATH") or CONFIG["STRATEGY_INTENTS_STATE_PATH"]
)
STRATEGY_MAX_OPEN_INTENTS = int(_cfg("STRATEGY_MAX_OPEN_INTENTS") or "20")
STRATEGY_MAX_POSITION_PCT = float(_cfg("STRATEGY_MAX_POSITION_PCT") or "0.10")
STRATEGY_ALLOW_SELL_ONLY = _cfg_bool("STRATEGY_ALLOW_SELL_ONLY")

DIAGNOSTIC_MODE = _cfg_bool("DIAGNOSTIC_MODE")
DIAGNOSTIC_ONLY = _cfg_bool("DIAGNOSTIC_ONLY")
DIAGNOSTIC_FORCE_RUN = _cfg_bool("DIAGNOSTIC_FORCE_RUN")
DIAGNOSTIC_DUMP_DIR = Path(
    _cfg("DIAGNOSTIC_DUMP_DIR") or _cfg("DIAGNOSTIC_DUMP_PATH") or CONFIG["DIAGNOSTIC_DUMP_PATH"]
)
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
BOTSTATE_LOCK_TTL_SEC = int(_cfg("BOTSTATE_LOCK_TTL_SEC") or "240")
LEDGER_BASE_DIR = Path(_cfg("LEDGER_BASE_DIR") or "bot_state/trader_ledger")
MORNING_WINDOW_START = _cfg("MORNING_WINDOW_START") or "08:50"
MORNING_WINDOW_END = _cfg("MORNING_WINDOW_END") or "15:20"
MORNING_EXIT_START = _cfg("MORNING_EXIT_START") or "09:00"
MORNING_EXIT_END = _cfg("MORNING_EXIT_END") or "09:20"
AFTERNOON_WINDOW_START = _cfg("AFTERNOON_WINDOW_START") or "15:20"
AFTERNOON_WINDOW_END = _cfg("AFTERNOON_WINDOW_END") or "15:30"
CLOSE_AUCTION_START = _cfg("CLOSE_AUCTION_START") or "15:20"
CLOSE_AUCTION_END = _cfg("CLOSE_AUCTION_END") or "15:30"
PB1_REQUIRE_VOLUME = _cfg_bool("PB1_REQUIRE_VOLUME", fallback=False)
PB1_FORCE_ENTRY_ON_PUSH = _cfg_bool("PB1_FORCE_ENTRY_ON_PUSH", fallback=True)
PB1_WAIT_FOR_WINDOW = _cfg_bool("PB1_WAIT_FOR_WINDOW", fallback=True)
PB1_MAX_WAIT_FOR_WINDOW_MIN = int(_cfg("PB1_MAX_WAIT_FOR_WINDOW_MIN") or "240")
MARKET_OPEN_HHMM = _cfg("MARKET_OPEN_HHMM") or "08:50"
MARKET_CLOSE_HHMM = _cfg("MARKET_CLOSE_HHMM") or "15:30"
PB1_PULLBACK_BAND_KOSPI = tuple(float(x.strip()) for x in (_cfg("PB1_PULLBACK_BAND_KOSPI") or "3,8").split(","))
PB1_PULLBACK_BAND_KOSDAQ = tuple(float(x.strip()) for x in (_cfg("PB1_PULLBACK_BAND_KOSDAQ") or "4,10").split(","))
PB1_VOL_CONTRACTION_MAX = float(_cfg("PB1_VOL_CONTRACTION_MAX") or "0.80")
PB1_VOLU_CONTRACTION_MAX = float(_cfg("PB1_VOLU_CONTRACTION_MAX") or "0.75")
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


PB1_MAX_POSITIONS = int(_cfg("PB1_MAX_POSITIONS") or "8")
PB1_MIN_SCORE = float(_cfg("PB1_MIN_SCORE") or "60")
PB1_USE_RISK_PARITY = _cfg_bool("PB1_USE_RISK_PARITY", fallback=True)
PB1_MAX_ATR_PCT = float(_cfg("PB1_MAX_ATR_PCT") or "6.0")
PB1_MIN_VALUE20 = float(_cfg("PB1_MIN_VALUE20") or "3000000000")
# === [NEW] 주간 리밸런싱 강제 트리거 상태 파일 ===
STATE_WEEKLY_PATH = Path(__file__).parent / "state_weekly.json"

def _this_iso_week_key(now=None):
    now = now or datetime.now(KST)
    return f"{now.year}-W{now.isocalendar().week:02d}"
