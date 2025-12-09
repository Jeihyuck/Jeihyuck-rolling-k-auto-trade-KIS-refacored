# -*- coding: utf-8 -*-
# trader.py — KOSDAQ 레짐(강세/약세) 기반 모멘텀 강화 + 단계적 축소, 14:40 리포트 생성 후 종료(전량매도 없음)

import logging
import requests
from .kis_wrapper import KisAPI, append_fill
from datetime import datetime, time as dtime, timedelta
from zoneinfo import ZoneInfo
import json
from pathlib import Path
import time
import os
import random
from typing import Optional, Dict, Any, Tuple, List
import csv
from .report_ceo import ceo_report
from .metrics import vwap_guard   # 🔸 VWAP 가드 함수
from rolling_k_auto_trade_api.best_k_meta_strategy import get_kosdaq_top_n

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
    # 챔피언 후보 필터
    "CHAMPION_MIN_TRADES": "5",            # 최소 거래수
    "CHAMPION_MIN_WINRATE": "45.0",        # 최소 승률(%)
    "CHAMPION_MAX_MDD": "30.0",            # 최대 허용 MDD(%)
    "CHAMPION_MIN_SHARPE": "0.0",          # 최소 샤프 비율
    # 기타
    "MARKET_DATA_WHEN_CLOSED": "false",
    "FORCE_WEEKLY_REBALANCE": "0",
    # NEW: 1분봉 VWAP 모멘텀 파라미터
    "MOM_FAST": "5",        # 1분봉 fast MA 길이
    "MOM_SLOW": "20",       # 1분봉 slow MA 길이
    "MOM_TH_PCT": "0.5",    # fast/slow 괴리 임계값(%) – 0.5% 이상이면 강세로 본다
}

def _cfg(key: str) -> str:
    """환경변수 > CONFIG 기본값"""
    return os.getenv(key, CONFIG.get(key, ""))

# RK-Max 유틸(가능하면 사용, 없으면 graceful fallback)
try:
    from .rkmax_utils import blend_k, recent_features
except Exception:
    def blend_k(k_month: float, day: int, atr20: Optional[float], atr60: Optional[float]) -> float:
        return float(k_month) if k_month is not None else 0.5

    def recent_features(kis, code: str) -> Dict[str, Optional[float]]:
        return {"atr20": None, "atr60": None}

# === [ANCHOR: TICK_UTILS] KRX 호가단위 & 라운딩 ===
def _krx_tick(price: float) -> int:
    p = float(price or 0)
    if p >= 500_000:
        return 1_000
    if p >= 100_000:
        return 500
    if p >= 50_000:
        return 100
    if p >= 10_000:
        return 50
    if p >= 5_000:
        return 10
    if p >= 1_000:
        return 5
    return 1

def _round_to_tick(price: float, mode: str = "nearest") -> int:
    """mode: 'down' | 'up' | 'nearest'"""
    if price is None or price <= 0:
        return 0
    tick = _krx_tick(price)
    q = price / tick
    if mode == "down":
        q = int(q)
    elif mode == "up":
        q = int(q) if q == int(q) else int(q) + 1
    else:
        q = int(q + 0.5)
    return int(q * tick)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)
STATE_FILE = Path(__file__).parent / "trade_state.json"

# 종목별 시장코드 고정 맵 (실전에서는 마스터테이블 로드로 대체 권장)
MARKET_MAP: Dict[str, str] = {
    # 예시: '145020': 'J', '347850': 'J', '257720': 'U', '178320': 'J', '348370': 'U'
}
def get_market(code: str) -> str:
    return MARKET_MAP.get(code, "J")  # 데이터 없음

# 데이터 없음 1차 감지 상태 저장(연속 DATA_EMPTY 확인용)
EXCLUDE_STATE: Dict[str, Dict[str, bool]] = {}

KST = ZoneInfo("Asia/Seoul")

# ===== 매개변수(.env 없이도 CONFIG 기본을 사용) =====
SELL_FORCE_TIME_STR = _cfg("SELL_FORCE_TIME").strip()
SELL_ALL_BALANCES_AT_CUTOFF = _cfg("SELL_ALL_BALANCES_AT_CUTOFF").lower() == "true"
RATE_SLEEP_SEC = float(_cfg("API_RATE_SLEEP_SEC"))
FORCE_SELL_PASSES_CUTOFF = int(_cfg("FORCE_SELL_PASSES_CUTOFF"))
FORCE_SELL_PASSES_CLOSE = int(_cfg("FORCE_SELL_PASSES_CLOSE"))
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
REBALANCE_ANCHOR = _cfg("REBALANCE_ANCHOR")
WEEKLY_ANCHOR_REF = _cfg("WEEKLY_ANCHOR_REF").lower()
MOMENTUM_OVERRIDES_FORCE_SELL = _cfg("MOMENTUM_OVERRIDES_FORCE_SELL").lower() == "true"

# NEW: 1분봉 모멘텀 파라미터
MOM_FAST = int(_cfg("MOM_FAST") or "5")
MOM_SLOW = int(_cfg("MOM_SLOW") or "20")
MOM_TH_PCT = float(_cfg("MOM_TH_PCT") or "0.5")
# 신고가 → 3일 눌림 → 반등 확인 후 매수 파라미터
USE_PULLBACK_ENTRY = _cfg("USE_PULLBACK_ENTRY").lower() != "false"
PULLBACK_LOOKBACK = int(_cfg("PULLBACK_LOOKBACK") or "60")
PULLBACK_DAYS = int(_cfg("PULLBACK_DAYS") or "3")
PULLBACK_REVERSAL_BUFFER_PCT = float(_cfg("PULLBACK_REVERSAL_BUFFER_PCT") or "0.2")
PULLBACK_TOPN = int(_cfg("PULLBACK_TOPN") or "50")
PULLBACK_UNIT_WEIGHT = float(_cfg("PULLBACK_UNIT_WEIGHT") or "0.03")
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

BAD_ENTRY_MAX_MA20_DIST = 1.25  # 현재가/20MA 상한(추격매수 방지)
BAD_ENTRY_MAX_PULLBACK = 20.0  # 신고가 대비 눌림폭 상한(과도한 붕괴 방지)
BAD_ENTRY_MAX_BELOW_VWAP_RATIO = 0.7  # 분봉에서 VWAP 아래 체류 비중이 이 이상이면 BAD

def _parse_hhmm(hhmm: str) -> dtime:
    try:
        hh, mm = hhmm.split(":")
        return dtime(hour=int(hh), minute=int(mm))
    except Exception:
        logger.warning(f"[설정경고] SELL_FORCE_TIME 형식 오류 → 기본값 14:40 적용: {hhmm}")
        return dtime(hour=14, minute=40)

SELL_FORCE_TIME = _parse_hhmm(SELL_FORCE_TIME_STR)
TIME_STOP_TIME = _parse_hhmm(TIME_STOP_HHMM)
ALLOW_WHEN_CLOSED = _cfg("MARKET_DATA_WHEN_CLOSED").lower() == "true"

# === [NEW] 주간 리밸런싱 강제 트리거 상태 파일 ===
STATE_WEEKLY_PATH = Path(__file__).parent / "state_weekly.json"

def _this_iso_week_key(now=None):
    now = now or datetime.now(KST)
    return f"{now.year}-W{now.isocalendar().week:02d}"

def _read_last_weekly():
    if not STATE_WEEKLY_PATH.exists():
        return None
    try:
        return (json.loads(STATE_WEEKLY_PATH.read_text(encoding="utf-8"))).get("weekly_rebalanced_at")
    except Exception:
        return None

def _write_last_weekly(now=None):
    now = now or datetime.now(KST)
    try:
        STATE_WEEKLY_PATH.write_text(
            json.dumps({"weekly_rebalanced_at": _this_iso_week_key(now)}, ensure_ascii=False),
            encoding="utf-8",
        )
    except Exception as e:
        logger.warning(f"[STATE_WRITE_FAIL] weekly: {e}")

def should_weekly_rebalance_now(now=None):
    """
    규칙:
      - 이번 주에 아직 리밸런싱 기록이 없으면 True
      - FORCE_WEEKLY_REBALANCE=1 이면 시간/요일 무시하고 True (단 1회)
    """
    now = now or datetime.now(KST)
    force = _cfg("FORCE_WEEKLY_REBALANCE") == "1"
    last = _read_last_weekly()
    cur = _this_iso_week_key(now)
    if force:
        logger.info("[REBALANCE] FORCE_WEEKLY_REBALANCE=1 → 주간 리밸런싱 강제 트리거")
        return True
    if last != cur:
        return True
    return False

def stamp_weekly_done(now=None):
    _write_last_weekly(now)

def get_rebalance_anchor_date(now: Optional[datetime] = None) -> str:
    """
    weekly 모드에서 기준일 산정:
      - WEEKLY_ANCHOR_REF='last'  → 직전 일요일(기본)
      - WEEKLY_ANCHOR_REF='next'  → 다음 일요일
    """
    now = now or datetime.now(KST)
    today = now.date()

    if REBALANCE_ANCHOR == "weekly":
        ref = WEEKLY_ANCHOR_REF if WEEKLY_ANCHOR_REF in ("last", "next", "prev", "previous") else "last"
        if ref in ("last", "prev", "previous"):
            # 월(0)~일(6). '일요일로부터 지난 일수' = (weekday+1) % 7
            days_since_sun = (today.weekday() + 1) % 7
            anchor_date = today - timedelta(days=days_since_sun)
        else:
            # 다음 일요일까지 남은 일수
            days_to_sun = (6 - today.weekday()) % 7
            anchor_date = today + timedelta(days=days_to_sun)
        return anchor_date.strftime("%Y-%m-%d")

    if REBALANCE_ANCHOR == "today":
        return today.strftime("%Y-%m-%d")

    # monthly
    return today.replace(day=1).strftime("%Y-%m-%d")

def fetch_rebalancing_targets(date: str) -> List[Dict[str, Any]]:
    REBALANCE_API_URL = f"http://localhost:8000/rebalance/run/{date}?force_order=true"
    response = requests.post(REBALANCE_API_URL)
    logger.info(f"[🛰️ 리밸런싱 API 전체 응답]: {response.text}")
    if response.status_code == 200:
        data = response.json()
        selected = data.get("selected") or data.get("selected_stocks") or []
        logger.info(f"[🎯 리밸런싱 종목]: {selected}")
        # 챔피언 & 레짐 상세 로그
        try:
            champion = selected[0] if selected else None
            log_champion_and_regime(logger, champion, REGIME_STATE, context="rebalance_api")
        except Exception as e:
            logger.exception(f"[VWAP_CHAMPION_LOG_ERROR] {e}")
        return selected
    else:
        raise Exception(f"리밸런싱 API 호출 실패: {response.text}")

def log_trade(trade: dict) -> None:
    today = datetime.now(KST).strftime("%Y-%m-%d")
    logfile = LOG_DIR / f"trades_{today}.json"
    with open(logfile, "a", encoding="utf-8") as f:
        f.write(json.dumps(trade, ensure_ascii=False) + "\n")

def save_state(holding: Dict[str, Any], traded: Dict[str, Any]) -> None:
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump({"holding": holding, "traded": traded}, f, ensure_ascii=False, indent=2)

def load_state() -> Tuple[Dict[str, Any], Dict[str, Any]]:
    if STATE_FILE.exists():
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            state = json.load(f)
        return state.get("holding", {}), state.get("traded", {})
    return {}, {}

def _with_retry(func, *args, max_retries=5, base_delay=0.6, **kwargs):
    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            last_err = e
            sleep_sec = base_delay * (1.6 ** (attempt - 1)) + random.uniform(0, 0.25)
            logger.error(f"[재시도 {attempt}/{max_retries}] {func.__name__} 실패: {e} → {sleep_sec:.2f}s 대기 후 재시도")
            time.sleep(sleep_sec)
    raise last_err

def _to_int(val, default=0) -> int:
    try:
        return int(float(val))
    except Exception:
        return default

def _to_float(val, default=None) -> Optional[float]:
    try:
        return float(val)
    except Exception:
        return default

def _log_realized_pnl(
    code: str,
    exec_px: Optional[float],
    sell_qty: int,
    buy_price: Optional[float],
    reason: str = ""
) -> None:
    try:
        if exec_px is None or sell_qty <= 0 or not buy_price or buy_price <= 0:
            return
        pnl_pct = ((float(exec_px) - float(buy_price)) / float(buy_price)) * 100.0
        profit  = (float(exec_px) - float(buy_price)) * int(sell_qty)
        msg = (
            f"[P&L] {code} SELL {int(sell_qty)}@{float(exec_px):.2f} / BUY={float(buy_price):.2f} "
            f"→ PnL={pnl_pct:.2f}% (₩{int(round(profit)):,.0f})"
        )
        if reason:
            msg += f" / REASON={reason}"
        logger.info(msg)
    except Exception as e:
        logger.warning(f"[P&L_LOG_FAIL] {code} err={e}")

# === [ANCHOR: PRICE_CACHE] 현재가 캐시 & 서킷브레이커 ===
_LAST_PRICE_CACHE: Dict[str, Dict[str, float]] = {}  # code -> {"px": float, "ts": epoch}
_PRICE_CB: Dict[str, Dict[str, float]] = {}          # code -> {"fail": int, "until": epoch}

# === [ANCHOR: BALANCE_CACHE] 잔고 캐싱 (루프 15초 단일 호출) ===
_BALANCE_CACHE: Dict[str, Any] = {"ts": 0.0, "balances": []}

def _safe_get_price(kis: KisAPI, code: str, ttl_sec: int = 5, stale_ok_sec: int = 30) -> Optional[float]:
    import time as _t
    now = _t.time()

    # 0) 서킷브레이커: 최근 실패 누적이면 잠시 건너뛴다
    cb = _PRICE_CB.get(code, {"fail": 0, "until": 0})
    primary_allowed = now >= cb.get("until", 0)

    # 장마감이면 캐시/종가로 대체
    try:
        if not kis.is_market_open() and not ALLOW_WHEN_CLOSED:
            ent = _LAST_PRICE_CACHE.get(code)
            if ent:
                return float(ent["px"])
            if hasattr(kis, "get_close_price"):
                try:
                    close_px = kis.get_close_price(code)
                    if close_px and float(close_px) > 0:
                        val = float(close_px)
                        _LAST_PRICE_CACHE[code] = {"px": val, "ts": now}
                        return val
                except Exception:
                    pass
            return None
    except Exception:
        pass

    # 1) 캐시 최신이면 반환
    ent = _LAST_PRICE_CACHE.get(code)
    if ent and (now - ent["ts"] <= ttl_sec):
        return float(ent["px"])

    # 2) 1차 소스
    if primary_allowed:
        try:
            px = _with_retry(kis.get_current_price, code)
            if px is not None and float(px) > 0:
                val = float(px)
                _LAST_PRICE_CACHE[code] = {"px": val, "ts": now}
                _PRICE_CB[code] = {"fail": 0, "until": 0}
                return val
            else:
                logger.warning(f"[PRICE_GUARD] {code} 현재가 무효값({px})")
        except Exception as e:
            fail = int(cb.get("fail", 0)) + 1
            cool = min(60, 3 * fail)
            _PRICE_CB[code] = {"fail": fail, "until": now + cool}
            logger.error(f"[NET/API 장애] {code} 현재가 1차조회 실패({e}) → cool {cool}s")

    # 3) 보조 소스
    try:
        if hasattr(kis, "get_quote_snapshot"):
            q = kis.get_quote_snapshot(code)
            cand = None
            if isinstance(q, dict):
                for k in ("tp", "trade_price", "prpr", "close", "price"):
                    v = q.get(k)
                    if v and float(v) > 0:
                        cand = float(v); break
            if cand and cand > 0:
                _LAST_PRICE_CACHE[code] = {"px": cand, "ts": now}
                return cand

        if hasattr(kis, "get_best_ask") and hasattr(kis, "get_best_bid"):
            ask = kis.get_best_ask(code)
            bid = kis.get_best_bid(code)
            if ask and bid and float(ask) > 0 and float(bid) > 0:
                mid = (float(ask) + float(bid)) / 2.0
                _LAST_PRICE_CACHE[code] = {"px": mid, "ts": now}
                return mid
    except Exception as e:
        logger.warning(f"[PRICE_FALLBACK_FAIL] {code} 보조소스 실패: {e}")

    # 4) 최후: 캐시가 있으면 stale_ok_sec 내 제공  (BUGFIX: px 반환)
    ent = _LAST_PRICE_CACHE.get(code)
    if ent and (now - ent["ts"] <= stale_ok_sec):
        return float(ent["px"])
    return None

def _fetch_balances(kis: KisAPI, ttl_sec: int = 15) -> List[Dict[str, Any]]:
    """
    get_balance / get_balance_all 호출을 15초 캐시.
    초당 루프를 돌려도 실제 API는 15초에 1번만 두드리도록 한다.
    """
    now = time.time()
    try:
        if _BALANCE_CACHE["balances"] and (now - float(_BALANCE_CACHE["ts"])) <= ttl_sec:
            return list(_BALANCE_CACHE["balances"])
    except Exception:
        pass

    if hasattr(kis, "get_balance_all"):
        res = _with_retry(kis.get_balance_all)
    else:
        res = _with_retry(kis.get_balance)

    if isinstance(res, dict):
        positions = res.get("positions") or res.get("output1") or []
        if not isinstance(positions, list):
            logger.error(f"[BAL_STD_FAIL] positions 타입 이상: {type(positions)}")
            positions = []
    elif isinstance(res, list):
        positions = res
    else:
        logger.error(f"[BAL_STD_FAIL] 지원하지 않는 반환 타입: {type(res)}")
        positions = []

    _BALANCE_CACHE["ts"] = now
    _BALANCE_CACHE["balances"] = list(positions)
    return positions

# === [ANCHOR: DAILY_CANDLE_CACHE] 일봉 완전 캐싱 ===
_DAILY_CANDLE_CACHE: Dict[str, Dict[str, Any]] = {}

def _get_daily_candles_cached(kis: KisAPI, code: str, count: int) -> List[Dict[str, Any]]:
    """
    코드별 일봉을 당일 기준으로 캐싱.
    - 동일 코드/거래일에서는 최초 요청 시에만 API 호출
    - 이후 더 긴 count가 들어오면 한 번 더 호출해서 캐시 갱신
    """
    today = datetime.now(KST).date()
    entry = _DAILY_CANDLE_CACHE.get(code)
    if entry and entry.get("date") == today and len(entry.get("candles") or []) >= count:
        return entry["candles"]

    candles = kis.get_daily_candles(code, count=count)
    if candles:
        _DAILY_CANDLE_CACHE[code] = {"date": today, "candles": candles}
    return candles or []


def _detect_pullback_reversal(
    kis: KisAPI,
    code: str,
    current_price: Optional[float] = None,
    lookback: int = PULLBACK_LOOKBACK,
    pullback_days: int = PULLBACK_DAYS,
    buffer_pct: float = PULLBACK_REVERSAL_BUFFER_PCT,
) -> Dict[str, Any]:
    """
    신고가 달성 이후 3일 연속 하락 후 반등 여부를 판정한다.

    반환 예시
    {
        "setup": True/False,        # 신고가 이후 3일 연속 하락 패턴 충족 여부
        "reversing": True/False,    # 현재가가 되돌림 확인선 위로 돌아섰는지
        "reversal_price": float,    # 되돌림 확인선(직전 하락일 고가 × (1+buffer))
        "peak_price": float,        # 신고가(lookback 내 최고가)
        "peak_date": "YYYYMMDD",  # 신고가 발생일
        "last_down_date": "YYYYMMDD",  # 3번째 하락일
        "reason": str               # setup=False일 때 스킵 사유
    }
    """
    try:
        candles = _get_daily_candles_cached(
            kis, code, count=max(lookback, pullback_days + 5)
        )
    except Exception as e:
        return {"setup": False, "reason": f"daily_fetch_fail:{e}"}

    if len(candles) < pullback_days + 2:
        return {"setup": False, "reason": "not_enough_candles"}

    today = datetime.now(KST).strftime("%Y%m%d")
    completed = list(candles)
    if completed and str(completed[-1].get("date")) == today:
        completed = completed[:-1]
    if len(completed) < pullback_days + 2:
        return {"setup": False, "reason": "insufficient_history_after_trim"}

    window = completed[-lookback:]
    highs = [float(c.get("high") or 0.0) for c in window]
    if not highs:
        return {"setup": False, "reason": "no_high_data"}

    peak_price = max(highs)
    try:
        peak_idx = max(
            i for i, c in enumerate(window) if float(c.get("high") or 0.0) == peak_price
        )
    except Exception:
        return {"setup": False, "reason": "peak_index_error"}

    # 직전 일자까지 연속 하락 구간 길이를 계산(어제까지 n일 연속 하락인지)
    down_streak_len = 0
    last_idx = len(window) - 1
    while last_idx > peak_idx:
        try:
            cur_close = float(window[last_idx].get("close") or 0.0)
            prev_close = float(window[last_idx - 1].get("close") or 0.0)
        except Exception:
            break
        if cur_close <= 0 or prev_close <= 0:
            break
        if cur_close < prev_close:
            down_streak_len += 1
            last_idx -= 1
            continue
        break

    if down_streak_len < pullback_days:
        return {
            "setup": False,
            "peak_price": peak_price,
            "reason": "not_enough_consecutive_down",
        }

    if last_idx < peak_idx:
        return {
            "setup": False,
            "peak_price": peak_price,
            "reason": "down_streak_not_after_peak",
        }

    last_down = window[len(window) - 1]
    try:
        reversal_line = max(
            float(last_down.get("high") or 0.0), float(last_down.get("close") or 0.0)
        )
    except Exception:
        reversal_line = 0.0

    reversal_price = reversal_line * (1.0 + buffer_pct / 100.0)
    reversing = (
        current_price is not None
        and reversal_price > 0
        and float(current_price) >= float(reversal_price)
    )

    return {
        "setup": True,
        "reversing": bool(reversing),
        "reversal_price": float(reversal_price) if reversal_price > 0 else None,
        "peak_price": float(peak_price),
        "peak_date": window[peak_idx].get("date"),
        "last_down_date": last_down.get("date"),
    }


def _classify_champion_grade(info: Dict[str, Any]) -> str:
    trades = _to_int(info.get("trades"), 0)
    win = _to_float(info.get("win_rate_pct"), 0.0)
    mdd = abs(_to_float(info.get("mdd_pct"), 0.0) or 0.0)
    sharpe = _to_float(info.get("sharpe_m") or info.get("sharpe"), 0.0)
    cumret = _to_float(
        info.get("cumulative_return_pct") or info.get("avg_return_pct"), 0.0
    )
    turnover = _to_float(
        info.get("prev_turnover") or info.get("avg_turnover") or info.get("turnover"),
        0.0,
    )

    turnover_ok = turnover <= 0 or turnover >= CHAMPION_A_RULES["min_turnover"]
    if (
        trades >= CHAMPION_A_RULES["min_trades"]
        and cumret >= CHAMPION_A_RULES["min_cumret_pct"]
        and mdd <= CHAMPION_A_RULES["max_mdd_pct"]
        and win >= CHAMPION_A_RULES["min_win_pct"]
        and sharpe >= CHAMPION_A_RULES["min_sharpe"]
        and turnover_ok
    ):
        return "A"

    if (
        trades >= CHAMPION_MIN_TRADES
        and win >= CHAMPION_MIN_WINRATE
        and mdd <= CHAMPION_MAX_MDD
        and sharpe >= CHAMPION_MIN_SHARPE
    ):
        return "B"

    return "C"


def _compute_daily_entry_context(
    kis: KisAPI, code: str, current_price: Optional[float]
) -> Dict[str, Any]:
    ctx: Dict[str, Any] = {"current_price": current_price}
    try:
        candles = _get_daily_candles_cached(kis, code, count=max(PULLBACK_LOOKBACK, 60))
    except Exception:
        return ctx

    today = datetime.now(KST).strftime("%Y%m%d")
    completed = list(candles)
    if completed and str(completed[-1].get("date")) == today:
        completed = completed[:-1]

    if not completed:
        return ctx

    closes = [float(c.get("close") or 0.0) for c in completed if c.get("close")]
    highs = [float(c.get("high") or 0.0) for c in completed if c.get("high")]
    lows = [float(c.get("low") or 0.0) for c in completed if c.get("low")]

    if len(closes) >= 20:
        ma20 = sum(closes[-20:]) / 20.0
        ctx["ma20"] = ma20
        if current_price:
            ctx["ma20_ratio"] = current_price / ma20
            ctx["ma20_risk"] = max(0.0, current_price - ma20)

    if highs:
        window_60 = highs[-60:] if len(highs) >= 60 else highs
        peak_price = max(window_60)
        ctx["peak_price"] = peak_price
        if current_price and peak_price > 0:
            ctx["distance_to_peak"] = current_price / peak_price
            ctx["pullback_depth_pct"] = (peak_price - current_price) / peak_price * 100.0

    # 연속 하락 일수 체크 (신고가 이후 눌림 판단)
    down_streak = 0
    for idx in range(len(completed) - 1, 0, -1):
        cur = float(completed[idx].get("close") or 0.0)
        prev = float(completed[idx - 1].get("close") or 0.0)
        if cur <= 0 or prev <= 0:
            break
        if cur < prev:
            down_streak += 1
        else:
            break
    ctx["down_streak"] = down_streak

    try:
        atr = _get_atr(kis, code)
        if atr:
            ctx["atr"] = float(atr)
    except Exception:
        pass

    if closes and highs:
        recent_high = max(highs[-20:])
        ctx["recent_high_20"] = recent_high
        ctx["setup_ok"] = bool(
            down_streak >= 2
            and ctx.get("pullback_depth_pct") is not None
            and ctx.get("pullback_depth_pct") >= GOOD_ENTRY_PULLBACK_RANGE[0]
            and (ctx.get("ma20_ratio") or 0) >= GOOD_ENTRY_MA20_RANGE[0]
            and recent_high >= max(highs[-60:]) * 0.95
        )

    return ctx


def _compute_intraday_entry_context(
    kis: KisAPI, code: str, prev_high: Optional[float] = None
) -> Dict[str, Any]:
    ctx: Dict[str, Any] = {}
    candles = _get_intraday_1min(kis, code, count=120)
    if not candles:
        return ctx

    vwap_val = _compute_vwap_from_1min(candles)
    ctx["vwap"] = vwap_val
    last = candles[-1]
    last_close = _to_float(last.get("close"), None)
    last_high = _to_float(last.get("high") or last.get("close"), None)
    last_low = _to_float(last.get("low") or last.get("close"), None)
    ctx["last_close"] = last_close
    ctx["last_high"] = last_high
    ctx["last_low"] = last_low

    if vwap_val and last_close:
        ctx["vwap_reclaim"] = last_close >= vwap_val

    highs = [
        float(c.get("high") or c.get("close") or 0.0)
        for c in candles
        if c.get("high") or c.get("close")
    ]
    lows = [
        float(c.get("low") or c.get("close") or 0.0)
        for c in candles
        if c.get("low") or c.get("close")
    ]
    vols = [float(c.get("volume") or 0.0) for c in candles]

    if highs:
        box_high = max(highs[-20:])
        box_low = min(lows[-20:]) if lows else None
        if last_high is not None and box_high:
            ctx["range_break"] = last_high >= box_high * 0.999
        if last_low is not None and box_low:
            ctx["box_floor"] = box_low

    if vols and len(vols) >= 10:
        recent_vol = sum(vols[-5:]) / 5.0
        base_vol = sum(vols[:-5]) / max(1, len(vols) - 5)
        if base_vol > 0:
            ctx["volume_spike"] = recent_vol >= base_vol * 1.5

    if vwap_val:
        below = sum(1 for c in candles if _to_float(c.get("close"), 0.0) < vwap_val)
        ctx["below_vwap_ratio"] = below / len(candles)

    if prev_high and last_high:
        ctx["prev_high_retest"] = last_high >= float(prev_high) * 0.999

    return ctx


def is_bad_entry(
    code: str,
    daily_ctx: Dict[str, Any],
    intraday_ctx: Dict[str, Any],
    regime_state: Optional[Dict[str, Any]] = None,
) -> bool:
    ma20_ratio = daily_ctx.get("ma20_ratio")
    if ma20_ratio and ma20_ratio > BAD_ENTRY_MAX_MA20_DIST:
        return True

    pullback = daily_ctx.get("pullback_depth_pct")
    if pullback and pullback > BAD_ENTRY_MAX_PULLBACK:
        return True

    if regime_state:
        try:
            kosdaq_drop = _to_float(regime_state.get("pct_change"), None)
            if kosdaq_drop is not None and kosdaq_drop <= -2.5:
                return True
        except Exception:
            pass

    below_vwap_ratio = intraday_ctx.get("below_vwap_ratio")
    if below_vwap_ratio is not None and below_vwap_ratio >= BAD_ENTRY_MAX_BELOW_VWAP_RATIO:
        return True

    return False


def is_good_entry(
    code: str,
    daily_ctx: Dict[str, Any],
    intraday_ctx: Dict[str, Any],
    prev_high: Optional[float] = None,
) -> bool:
    if not daily_ctx.get("setup_ok"):
        return False

    pullback = daily_ctx.get("pullback_depth_pct")
    if pullback is None or not (
        GOOD_ENTRY_PULLBACK_RANGE[0] <= pullback <= GOOD_ENTRY_PULLBACK_RANGE[1]
    ):
        return False

    ma20_ratio = daily_ctx.get("ma20_ratio")
    if ma20_ratio is None or not (
        GOOD_ENTRY_MA20_RANGE[0] <= ma20_ratio <= GOOD_ENTRY_MA20_RANGE[1]
    ):
        return False

    dist_peak = daily_ctx.get("distance_to_peak")
    if dist_peak is None or dist_peak > GOOD_ENTRY_MAX_FROM_PEAK:
        return False

    cur_px = daily_ctx.get("current_price")
    atr = daily_ctx.get("atr") or 0.0
    ma_risk = daily_ctx.get("ma20_risk") or 0.0
    risk = max(atr, ma_risk, (cur_px or 0) * 0.03)
    reward = max(0.0, (daily_ctx.get("peak_price") or 0) - (cur_px or 0)) + atr
    if risk <= 0 or reward / risk < GOOD_ENTRY_MIN_RR:
        return False

    signals = []
    if intraday_ctx.get("vwap_reclaim"):
        signals.append("vwap")
    if intraday_ctx.get("range_break"):
        signals.append("range")
    if intraday_ctx.get("volume_spike"):
        signals.append("volume")
    if prev_high and intraday_ctx.get("prev_high_retest"):
        signals.append("prev_high")

    return len(signals) >= GOOD_ENTRY_MIN_INTRADAY_SIG

from .kis_wrapper import NetTemporaryError, DataEmptyError, DataShortError

# === [ANCHOR: INTRADAY_MOMENTUM] 1분봉 VWAP + 단기 모멘텀 ===
def _get_intraday_1min(kis: KisAPI, code: str, count: int = 60) -> List[Dict[str, Any]]:
    """
    KisAPI에 1분봉 메서드가 있으면 사용하고, 없으면 호환 메서드로 fallback.
    반환은 최소한 'close'와 'volume' 정보를 가진 dict 리스트라고 가정한다.
    """
    try:
        if hasattr(kis, "get_intraday_1min"):
            return kis.get_intraday_1min(code, count=count)
        if hasattr(kis, "get_minute_candles"):
            return kis.get_minute_candles(code, unit=1, count=count)
        if hasattr(kis, "get_intraday_candles"):
            return kis.get_intraday_candles(code, unit="1", count=count)
    except Exception as e:
        logger.warning(f"[INTRADAY_1M_FAIL] {code}: {e}")
    return []

def _compute_vwap_from_1min(candles: List[Dict[str, Any]]) -> Optional[float]:
    if not candles:
        return None
    pv = 0.0
    vol_sum = 0.0
    for c in candles:
        try:
            price = float(c.get("close") or c.get("trade_price") or c.get("price") or 0.0)
            vol = float(c.get("volume") or c.get("trade_volume") or 0.0)
        except Exception:
            continue
        if price <= 0 or vol <= 0:
            continue
        pv += price * vol
        vol_sum += vol
    if vol_sum <= 0:
        return None
    return pv / vol_sum

def _compute_intraday_momentum(candles: List[Dict[str, Any]], fast: int = MOM_FAST, slow: int = MOM_SLOW) -> float:
    closes: List[float] = []
    for c in candles:
        try:
            px = float(c.get("close") or c.get("trade_price") or c.get("price") or 0.0)
        except Exception:
            continue
        if px > 0:
            closes.append(px)
    if len(closes) < max(fast, slow):
        return 0.0
    fast_ma = sum(closes[-fast:]) / float(fast)
    slow_ma = sum(closes[-slow:]) / float(slow)
    if slow_ma <= 0:
        return 0.0
    return (fast_ma - slow_ma) / slow_ma * 100.0

def is_strong_momentum_vwap(kis: KisAPI, code: str) -> bool:
    """
    1분봉 VWAP + 단기 모멘텀 기반 모멘텀 강세 판정.
    - 최근 가격이 VWAP 위
    - fast/slow 모멘텀 >= MOM_TH_PCT
    """
    try:
        if hasattr(kis, "is_market_open") and not kis.is_market_open() and not ALLOW_WHEN_CLOSED:
            return False
    except Exception:
        pass

    candles = _get_intraday_1min(kis, code, count=max(MOM_SLOW * 3, 60))
    if not candles:
        return False

    try:
        last_candle = candles[-1]
        last_price = float(last_candle.get("close") or last_candle.get("trade_price") or last_candle.get("price") or 0.0)
    except Exception:
        return False
    if last_price <= 0:
        return False

    vwap_val = _compute_vwap_from_1min(candles)
    if vwap_val is None or vwap_val <= 0:
        return False

    mom = _compute_intraday_momentum(candles)
    strong = (last_price > vwap_val) and (mom >= MOM_TH_PCT)
    if strong:
        logger.info(
            f"[모멘텀 강세] {code}: 강한 상승추세, 능동관리 매도 보류 "
            f"(VWAP/1분봉 기준, last={last_price:.2f}, vwap={vwap_val:.2f}, mom={mom:.2f}%)"
        )
    return strong

# === 20D 수익률 ===
def get_20d_return_pct(kis: KisAPI, code: str) -> Optional[float]:
    try:
        if not kis.is_market_open() and not ALLOW_WHEN_CLOSED:
            raise NetTemporaryError("market closed skip")
    except Exception:
        pass

    MAX_RETRY = 3
    last_err: Optional[Exception] = None

    for attempt in range(1, MAX_RETRY + 1):
        try:
            candles = _get_daily_candles_cached(kis, code, count=21)
            if not candles or len(candles) < 21:
                raise DataShortError("need at least 21 candles")

            if any(('close' not in c) or (c['close'] is None) for c in candles):
                logger.error("[20D_RETURN_FAIL] %s: 캔들 close 결측", code)
                raise DataEmptyError("close missing")

            old = float(candles[-21]['close'])
            nowp = float(candles[-1]['close'])
            return ((nowp - old) / old) * 100.0

        except NetTemporaryError as e:
            last_err = e
            logger.warning("[CANDLE_TEMP_SKIP] %s 20D 계산 네트워크 실패 (재시도 %d/%d)", code, attempt, MAX_RETRY)
            time.sleep(1.0 * attempt)
            continue
        except DataEmptyError:
            logger.warning("[DATA_EMPTY] %s 0캔들(20D 계산 불가) - 상위에서 재확인/제외 판단", code)
            raise
        except DataShortError:
            logger.warning("[DATA_SHORT] %s 21개 미만(20D 계산 불가) - 상위에서 제외 판단", code)
            raise
        except Exception as e:
            last_err = e
            logger.warning("[20D_RETURN_FAIL] %s: 예외 %s (재시도 %d/%d)", code, e, attempt, MAX_RETRY)
            time.sleep(1.0 * attempt)
            continue

    if last_err:
        logger.warning("[20D_RETURN_FAIL] %s 최종 실패: %s", code, last_err)
    raise NetTemporaryError("20D return calc failed")

def is_strong_momentum(kis: KisAPI, code: str) -> bool:
    """
    기존 일봉 기반 모멘텀 대신,
    1분봉 VWAP + 단기 모멘텀 기준으로 강세를 판별한다.
    """
    return is_strong_momentum_vwap(kis, code)

def _percentile_rank(values: List[float], value: float, higher_is_better: bool = True) -> float:
    if not values:
        return 0.0
    vals = [float(v) for v in values if v is not None]
    if not vals:
        return 0.0

    if higher_is_better:
        count = sum(1 for v in vals if v <= value)
    else:
        count = sum(1 for v in vals if v >= value)
    return (count / len(vals)) * 100.0

def _has_bullish_trend_structure(kis: KisAPI, code: str) -> Tuple[bool, Dict[str, float]]:
    """
    보유 지속 여부 판단용: 5/10/20일선 정배열 + 20일선 상승 + 종가>20일선 체크.
    """
    candles = _get_daily_candles_cached(kis, code, count=25)
    if not candles or len(candles) < 21:
        raise DataShortError("not enough candles")

    today = datetime.now(KST).strftime("%Y%m%d")
    completed = list(candles)
    if completed and str(completed[-1].get("date")) == today:
        completed = completed[:-1]

    if len(completed) < 21:
        raise DataShortError("insufficient completed candles")

    closes: List[float] = []
    for c in completed:
        close = c.get("close")
        if close is None:
            raise DataEmptyError("close missing")
        closes.append(float(close))

    if len(closes) < 21:
        raise DataShortError("need at least 21 closes")

    ma5 = sum(closes[-5:]) / 5.0
    ma10 = sum(closes[-10:]) / 10.0
    ma20 = sum(closes[-20:]) / 20.0
    prev_ma20 = sum(closes[-21:-1]) / 20.0
    last_close = closes[-1]

    aligned = last_close > ma20 and ma5 > ma10 > ma20 and ma20 > prev_ma20
    return aligned, {
        "ma5": ma5,
        "ma10": ma10,
        "ma20": ma20,
        "ma20_prev": prev_ma20,
        "last_close": last_close,
    }

def _weight_to_qty(
    kis: KisAPI,
    code: str,
    weight: float,
    daily_capital: int,
    ref_price: Optional[float] = None
) -> int:
    weight = max(W_MIN_ONE, min(max(0.0, float(weight)), W_MAX_ONE))
    alloc = int(round(daily_capital * weight))

    price = None
    if ref_price is not None and float(ref_price) > 0:
        price = float(ref_price)

    if price is None:
        try:
            if kis.is_market_open():
                price = _safe_get_price(kis, code)
            else:
                if hasattr(kis, "get_close_price"):
                    try:
                        price = float(kis.get_close_price(code))
                    except Exception:
                        price = None
        except Exception:
            price = None

    if price is None or price <= 0:
        return 0

    return max(0, int(alloc // int(price)))



def _notional_to_qty(
    kis: KisAPI,
    code: str,
    notional: int,
    ref_price: Optional[float] = None
) -> int:
    """Target Notional(원)을 기준으로 수량 계산 (weight 클램프 없이 직접 계산)."""
    try:
        notional = int(notional)
    except Exception:
        return 0
    if notional <= 0:
        return 0

    price = None
    if ref_price is not None:
        try:
            if float(ref_price) > 0:
                price = float(ref_price)
        except Exception:
            price = None

    if price is None:
        try:
            if kis.is_market_open():
                price = _safe_get_price(kis, code)
            else:
                if hasattr(kis, "get_close_price"):
                    try:
                        price = float(kis.get_close_price(code))
                    except Exception:
                        price = None
        except Exception:
            price = None

    if price is None or price <= 0:
        return 0

    return max(0, int(notional // int(price)))
# === ATR, 상태 초기화 ===
def _get_atr(kis: KisAPI, code: str, window: int = 14) -> Optional[float]:
    if hasattr(kis, "get_atr"):
        try:
            return kis.get_atr(code, window=window)
        except Exception as e:
            logger.warning(f"[ATR_FAIL] {code}: {e}")
            return None
    return None

def _init_position_state(kis: KisAPI, holding: Dict[str, Any], code: str, entry_price: float, qty: int, k_value: Any, target_price: Optional[float]) -> None:
    try:
        _ = kis.is_market_open()
    except Exception:
        pass
    atr = _get_atr(kis, code)
    rng_eff = (atr * 1.5) if (atr and atr > 0) else max(1.0, entry_price * 0.01)
    t1 = entry_price + 0.5 * rng_eff
    t2 = entry_price + 1.0 * rng_eff
    holding[code] = {
        'qty': int(qty),
        'buy_price': float(entry_price),
        'entry_time': datetime.now(KST).isoformat(),
        'high': float(entry_price),
        'tp1': float(t1),
        'tp2': float(t2),
        'sold_p1': False,
        'sold_p2': False,
        'trail_pct': TRAIL_PCT,
        'atr': float(atr) if atr else None,
        'stop_abs': float(entry_price - ATR_STOP * atr) if atr else float(entry_price * (1 - FAST_STOP)),
        'k_value': k_value,
        'target_price_src': float(target_price) if target_price is not None else None,
        'bear_s1_done': False,
        'bear_s2_done': False,
        # 눌림목 3단계 진입 관련 기본값 (신규 매수 직후 overwrite 가능)
        'entry_stage': 1,
        'max_price_after_entry': float(entry_price),
        'planned_total_qty': int(qty),
        'stage1_qty': int(qty),
        'stage2_qty': 0,
        'stage3_qty': 0,
    }

def _init_position_state_from_balance(kis: KisAPI, holding: Dict[str, Any], code: str, avg_price: float, qty: int) -> None:
    if qty <= 0 or code in holding:
        return
    try:
        _ = kis.is_market_open()
    except Exception:
        pass
    atr = _get_atr(kis, code)
    rng_eff = (atr * 1.5) if (atr and atr > 0) else max(1.0, avg_price * 0.01)
    t1 = avg_price + 0.5 * rng_eff
    t2 = avg_price + 1.0 * rng_eff
    holding[code] = {
        'qty': int(qty),
        'buy_price': float(avg_price),
        'entry_time': (datetime.now(KST) - timedelta(minutes=10)).isoformat(),
        'high': float(avg_price),
        'tp1': float(t1),
        'tp2': float(t2),
        'sold_p1': False,
        'sold_p2': False,
        'trail_pct': TRAIL_PCT,
        'atr': float(atr) if atr else None,
        'stop_abs': float(avg_price - ATR_STOP * atr) if atr else float(avg_price * (1 - FAST_STOP)),
        'k_value': None,
        'target_price_src': None,
        'bear_s1_done': False,
        'bear_s2_done': False,
        # 기존 보유분은 추가 진입(stage 3 완료 상태)으로 간주
        'entry_stage': 3,
        'max_price_after_entry': float(avg_price),
        'planned_total_qty': int(qty),
        'stage1_qty': int(qty),
        'stage2_qty': 0,
        'stage3_qty': 0,
    }


def _maybe_scale_in_dips(
    kis: KisAPI,
    holding: Dict[str, Any],
    code: str,
    target: Dict[str, Any],
    now_str: str,
    regime_mode: str,
) -> None:
    """
    신고가 → 3일 연속 하락 → 반등 확인 시 단계적 추가 매수 로직.
    - entry_stage: 1 → 2차 진입 후보(반등 확인선 돌파), 2 → 3차 진입 후보(신고가 회복)
    - bull / neutral 모드에서만 동작, bear 모드에서는 추가 진입 금지
    """
    pos = holding.get(code)
    if not pos:
        return

    # 약세 레짐에서는 추가 진입 금지
    if regime_mode not in ("bull", "neutral"):
        return

    entry_stage = int(pos.get("entry_stage") or 1)
    if entry_stage >= 3:
        return

    # 현재가 조회
    try:
        cur_price = _safe_get_price(kis, code)
    except Exception:
        cur_price = None
    if cur_price is None or cur_price <= 0:
        return

    # 손절선 이하면 추가 진입 금지
    try:
        stop_abs = pos.get("stop_abs")
        if stop_abs is not None and cur_price <= float(stop_abs):
            logger.info(
                f"[SCALE-IN-GUARD] {code}: 현재가({cur_price}) <= stop_abs({stop_abs}) → 추가 진입 금지"
            )
            return
    except Exception:
        pass

    # VWAP 가드: 과도한 추세 붕괴 구간에서는 추가 진입하지 않음
    try:
        vwap_val = kis.get_vwap_today(code)
    except Exception:
        vwap_val = None
    if vwap_val is None or vwap_val <= 0:
        logger.debug(f"[SCALE-IN-VWAP-SKIP] {code}: VWAP 데이터 없음 → VWAP 가드 생략")
    else:
        if not vwap_guard(float(cur_price), float(vwap_val), VWAP_TOL):
            logger.info(
                f"[SCALE-IN-VWAP-GUARD] {code}: 현재가({cur_price}) < VWAP*(1 - {VWAP_TOL:.4f}) "
                f"→ 눌림목 추가 진입 스킵 (VWAP={vwap_val:.2f})"
            )
            return

    # 계획 수량 계산
    planned_total_qty = int(
        pos.get("planned_total_qty")
        or _to_int(target.get("매수수량") or target.get("qty"), 0)
    )
    if planned_total_qty <= 0:
        return

    # 스테이지별 목표 수량(부족 시 재계산)
    s1 = int(pos.get("stage1_qty") or max(1, int(planned_total_qty * ENTRY_LADDERS[0])))
    s2 = int(pos.get("stage2_qty") or max(0, int(planned_total_qty * ENTRY_LADDERS[1])))
    s3 = int(pos.get("stage3_qty") or max(0, planned_total_qty - s1 - s2))

    pos["planned_total_qty"] = int(planned_total_qty)
    pos["stage1_qty"] = int(s1)
    pos["stage2_qty"] = int(s2)
    pos["stage3_qty"] = int(s3)

    current_qty = int(pos.get("qty") or 0)
    if current_qty <= 0:
        return

    # 신고가 → 3일 눌림 → 반등 여부 확인
    pullback = _detect_pullback_reversal(
        kis=kis,
        code=code,
        current_price=float(cur_price),
    )
    if USE_PULLBACK_ENTRY and not pullback.get("setup"):
        logger.info(
            f"[PULLBACK-SKIP] {code}: 신고가 눌림 패턴 미충족 → reason={pullback.get('reason')}"
        )
        return

    if USE_PULLBACK_ENTRY and not pullback.get("reversing"):
        rev_px = pullback.get("reversal_price")
        logger.info(
            f"[PULLBACK-WAIT] {code}: 현재가({cur_price}) < 반등확인선({rev_px}) → 대기"
        )
        return

    reversal_price = pullback.get("reversal_price") or float(cur_price)
    peak_price = pullback.get("peak_price") or reversal_price

    # 참고용 상태 업데이트
    pos["pullback_peak_price"] = float(peak_price)
    pos["pullback_reversal_price"] = float(reversal_price)

    add_qty = 0
    next_stage = entry_stage

    if entry_stage == 1:
        # 2차 진입: 3일 눌림 후 반등 확인선 돌파 → s1+s2까지 확대
        if cur_price >= reversal_price and current_qty < (s1 + s2):
            add_qty = max(0, (s1 + s2) - current_qty)
            next_stage = 2
    elif entry_stage == 2:
        # 3차 진입: 신고가 회복(peak_price 돌파) 시 전체 planned_total_qty까지 확대
        if cur_price >= peak_price and current_qty < planned_total_qty:
            add_qty = max(0, planned_total_qty - current_qty)
            next_stage = 3
    else:
        return

    if add_qty <= 0:
        return

    logger.info(
        f"[SCALE-IN] {code} stage={entry_stage}->{next_stage} "
        f"reversal_line={reversal_price:.2f} peak={peak_price:.2f} cur={cur_price} add_qty={add_qty}"
    )

    # 추가 매수 실행 (현재가 기준 가드형 지정가/시장가)
    try:
        result = place_buy_with_fallback(
            kis, code, int(add_qty), limit_price=int(cur_price)
        )
    except Exception as e:
        logger.error(f"[SCALE-IN-ORDER-FAIL] {code}: {e}")
        return

    # fills CSV 보강
    try:
        odno = ""
        if isinstance(result, dict):
            out = result.get("output") or {}
            odno = (
                out.get("ODNO")
                or out.get("ord_no")
                or out.get("order_no")
                or ""
            )
        ensure_fill_has_name(
            odno=odno,
            code=code,
            name=str(target.get("name") or target.get("종목명") or ""),
            qty=int(add_qty),
            price=float(cur_price),
        )
    except Exception as e:
        logger.warning(f"[SCALE-IN-FILL-NAME-FAIL] code={code} ex={e}")

    # 상태 업데이트
    pos["qty"] = int(current_qty + add_qty)
    pos["entry_stage"] = int(next_stage)
    holding[code] = pos

    # 매수 로그 기록
    try:
        log_trade(
            {
                "datetime": now_str,
                "code": code,
                "name": target.get("name") or target.get("종목명"),
                "qty": int(add_qty),
                "K": pos.get("k_value"),
                "target_price": pos.get("target_price_src"),
                "strategy": "눌림목 3단계 진입",
                "side": "BUY",
                "price": float(cur_price),
                "amount": int(float(cur_price)) * int(add_qty),
                "result": result,
                "reason": f"scale_in_stage_{entry_stage}_to_{next_stage}",
            }
        )
    except Exception as e:
        logger.warning(f"[SCALE-IN-LOG-FAIL] {code}: {e}")


def _sell_once(kis: KisAPI, code: str, qty: int, prefer_market=True) -> Tuple[Optional[float], Any]:
    cur_price = _safe_get_price(kis, code)
    try:
        if prefer_market and hasattr(kis, "sell_stock_market"):
            result = _with_retry(kis.sell_stock_market, code, qty)
        else:
            result = _with_retry(kis.sell_stock, code, qty)
    except Exception as e:
        logger.warning(f"[매도 재시도: 토큰 갱신 후 1회] {code} qty={qty} err={e}")
        try:
            if hasattr(kis, "refresh_token"):
                kis.refresh_token()
        except Exception:
            pass
        if prefer_market and hasattr(kis, "sell_stock_market"):
            result = _with_retry(kis.sell_stock_market, code, qty)
        else:
            result = _with_retry(kis.sell_stock, code, qty)
    logger.info(f"[매도호출] {code}, qty={qty}, price(log)={cur_price}, result={result}")
    return cur_price, result

def ensure_fill_has_name(odno: str, code: str, name: str, qty: int = 0, price: float = 0.0) -> None:
    try:
        fills_dir = Path("fills")
        fills_dir.mkdir(exist_ok=True)
        today_path = fills_dir / f"fills_{datetime.now().strftime('%Y%m%d')}.csv"
        updated = False
        if today_path.exists():
            with open(today_path, "r", encoding="utf-8", newline="") as f:
                reader = list(csv.reader(f))
            if reader:
                header = reader[0]
                try:
                    idx_odno = header.index("ODNO")
                    idx_code = header.index("code")
                    idx_name = header.index("name")
                except ValueError:
                    idx_odno = None
                    idx_code = None
                    idx_name = None
                if idx_odno is not None and idx_name is not None and idx_code is not None:
                    for i in range(1, len(reader)):
                        row = reader[i]
                        if len(row) <= max(idx_odno, idx_code, idx_name):
                            continue
                        if (row[idx_odno] == str(odno) or (not row[idx_odno] and str(odno) == "")) and row[idx_code] == str(code):
                            if not row[idx_name]:
                                row[idx_name] = name or ""
                                reader[i] = row
                                updated = True
                                logger.info(f"[FILL_NAME_UPDATE] ODNO={odno} code={code} name={name}")
                                break
        if updated:
            with open(today_path, "w", encoding="utf-8", newline="") as f:
                writer = csv.writer(f)
                writer.writerows(reader)
            return
        append_fill("BUY", code, name or "", qty, price or 0.0, odno or "", note="ensure_fill_added_by_trader")
    except Exception as e:
        logger.warning(f"[ENSURE_FILL_FAIL] odno={odno} code={code} ex={e}")

# === 앵커: 목표가 계산 함수 ===
def compute_entry_target(kis: KisAPI, stk: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    code = str(stk.get("code") or stk.get("stock_code") or stk.get("pdno") or "")
    if not code:
        return None, None

    try:
        market_open = kis.is_market_open()
    except Exception:
        market_open = True

    # 1) 오늘 시초가
    today_open = None
    try:
        today_open = kis.get_today_open(code)
    except Exception:
        pass
    if not today_open or today_open <= 0:
        try:
            snap = kis.get_current_price(code)
            if snap and snap > 0:
                today_open = float(snap)
        except Exception:
            pass
    if not today_open or today_open <= 0:
        logger.info(f"[TARGET/wait_open] {code} 오늘 시초가 미확정 → 목표가 계산 보류")
        return None, None

    # 2) 전일 범위
    prev_high = prev_low = None
    try:
        if market_open:
            prev_candles = _get_daily_candles_cached(kis, code, count=2)
            if prev_candles and len(prev_candles) >= 2:
                prev = prev_candles[-2]
                prev_high = _to_float(prev.get("high"))
                prev_low  = _to_float(prev.get("low"))
    except Exception:
        pass

    if prev_high is None or prev_low is None:
        try:
            prev_candles = _get_daily_candles_cached(kis, code, count=2)
            if prev_candles and len(prev_candles) >= 2:
                prev = prev_candles[-2]
                prev_high = _to_float(prev.get("high"))
                prev_low  = _to_float(prev.get("low"))
        except Exception:
            pass

    if prev_high is None or prev_low is None:
        prev_high = _to_float(stk.get("prev_high"))
        prev_low  = _to_float(stk.get("prev_low"))
        if prev_high is None or prev_low is None:
            logger.warning(f"[TARGET/prev_candle_fail] {code} 전일 캔들/백업 모두 부재")
            return None, None

    rng = max(0.0, float(prev_high) - float(prev_low))
    k_used = float(stk.get("best_k") or stk.get("K") or stk.get("k") or 0.5)
    raw_target = float(today_open) + rng * k_used

    eff_target_price = float(_round_to_tick(raw_target, mode="up"))
    return float(eff_target_price), float(k_used)

def place_buy_with_fallback(kis: KisAPI, code: str, qty: int, limit_price: int) -> Dict[str, Any]:
    """
    매수 주문(지정가 우선, 실패시 시장가 Fallback) + 체결가/슬리피지/네트워크 장애/실패 상세 로깅
    """
    result_limit: Optional[Dict[str, Any]] = None
    order_price = _round_to_tick(limit_price, mode="up") if (limit_price and limit_price > 0) else 0
    fill_price = None
    trade_logged = False

    try:
        # [PATCH] 예수금/과매수 방지: 가드형 지정가 사용
        if hasattr(kis, "buy_stock_limit_guarded") and order_price and order_price > 0:  # [PATCH]
            result_limit = _with_retry(kis.buy_stock_limit_guarded, code, qty, int(order_price))  # [PATCH]
            logger.info("[BUY-LIMIT] %s qty=%s limit=%s -> %s", code, qty, order_price, result_limit)
            time.sleep(2.0)
            filled = False
            if hasattr(kis, "check_filled"):
                try:
                    filled = bool(_with_retry(kis.check_filled, result_limit))
                except Exception:
                    filled = False
            if filled:
                try:
                    fill_price = float(result_limit.get("output", {}).get("prdt_price", 0)) or None
                except Exception:
                    fill_price = None
                if fill_price is None:
                    try:
                        fill_price = kis.get_current_price(code)
                    except Exception:
                        fill_price = None
                slippage = ((fill_price - order_price) / order_price * 100.0) if (fill_price and order_price) else None
                log_trade({
                    "datetime": datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S"),
                    "code": code,
                    "side": "BUY",
                    "order_price": order_price,
                    "fill_price": fill_price,
                    "slippage_pct": round(slippage, 2) if slippage is not None else None,
                    "qty": qty,
                    "result": result_limit,
                    "status": "filled",
                    "fail_reason": None
                })
                trade_logged = True
                if slippage is not None and abs(slippage) > SLIPPAGE_LIMIT_PCT:
                    logger.warning(f"[슬리피지 경고] {code} slippage {slippage:.2f}% > 임계값({SLIPPAGE_LIMIT_PCT}%)")
                return result_limit
        else:
            logger.info("[BUY-LIMIT] API 미지원 또는 limit_price 무효 → 시장가로 진행")
    except Exception as e:
        logger.error("[BUY-LIMIT-FAIL] %s qty=%s limit=%s err=%s", code, qty, order_price, e)
        log_trade({
            "datetime": datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S"),
            "code": code,
            "side": "BUY",
            "order_price": order_price,
            "fill_price": None,
            "slippage_pct": None,
            "qty": qty,
            "result": None,
            "status": "failed",
            "fail_reason": str(e)
        })
        trade_logged = True

    # --- 시장가 Fallback ---
    try:
        # [PATCH] 예수금/과매수 방지: 가드형 시장가 사용
        if hasattr(kis, "buy_stock_market_guarded"):  # [PATCH]
            result_mkt = _with_retry(kis.buy_stock_market_guarded, code, qty)  # [PATCH]
        elif hasattr(kis, "buy_stock_market"):
            result_mkt = _with_retry(kis.buy_stock_market, code, qty)
        else:
            result_mkt = _with_retry(kis.buy_stock, code, qty)
        logger.info("[BUY-MKT] %s qty=%s (from limit=%s) -> %s", code, qty, order_price, result_mkt)
        try:
            fill_price = float(result_mkt.get("output", {}).get("prdt_price", 0)) or None
        except Exception:
            fill_price = None
        if fill_price is None:
            try:
                fill_price = kis.get_current_price(code)
            except Exception:
                fill_price = None
        slippage = ((fill_price - order_price) / order_price * 100.0) if (fill_price and order_price) else None
        log_trade({
            "datetime": datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S"),
            "code": code,
            "side": "BUY",
            "order_price": order_price or None,
            "fill_price": fill_price,
            "slippage_pct": round(slippage, 2) if slippage is not None else None,
            "qty": qty,
            "result": result_mkt,
            "status": "filled" if result_mkt and result_mkt.get("rt_cd") == "0" else "failed",
            "fail_reason": None if result_mkt and result_mkt.get("rt_cd") == "0" else "체결실패"
        })
        trade_logged = True
        if slippage is not None and abs(slippage) > SLIPPAGE_LIMIT_PCT:
            logger.warning(f"[슬리피지 경고] {code} slippage {slippage:.2f}% > 임계값({SLIPPAGE_LIMIT_PCT}%)")
        return result_mkt
    except Exception as e:
        logger.error("[BUY-MKT-FAIL] %s qty=%s err=%s", code, qty, e)
        if not trade_logged:
            log_trade({
                "datetime": datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S"),
                "code": code,
                "side": "BUY",
                "order_price": order_price or None,
                "fill_price": None,
                "slippage_pct": None,
                "qty": qty,
                "result": None,
                "status": "failed",
                "fail_reason": str(e)
            })
        raise

# === [ANCHOR: REGIME PARAMS] 코스닥 레짐 파라미터 ===
REGIME_ENABLED = True
KOSDAQ_CODE = _cfg("KOSDAQ_INDEX_CODE")
KOSDAQ_ETF_FALLBACK = _cfg("KOSDAQ_ETF_FALLBACK")  # KODEX 코스닥150

REG_BULL_MIN_UP_PCT = float(_cfg("REG_BULL_MIN_UP_PCT"))
REG_BULL_MIN_MINUTES = int(_cfg("REG_BULL_MIN_MINUTES"))
REG_BEAR_VWAP_MINUTES = int(_cfg("REG_BEAR_VWAP_MINUTES"))
REG_BEAR_DROP_FROM_HIGH = float(_cfg("REG_BEAR_DROP_FROM_HIGH"))

REG_BEAR_STAGE1_MINUTES = int(_cfg("REG_BEAR_STAGE1_MINUTES"))
REG_BEAR_STAGE2_ADD_DROP = float(_cfg("REG_BEAR_STAGE2_ADD_DROP"))
REG_PARTIAL_S1 = float(_cfg("REG_PARTIAL_S1"))
REG_PARTIAL_S2 = float(_cfg("REG_PARTIAL_S2"))

TRAIL_PCT_BULL = float(_cfg("TRAIL_PCT_BULL"))
TRAIL_PCT_BEAR = float(_cfg("TRAIL_PCT_BEAR"))
TP_PROFIT_PCT_BASE = DEFAULT_PROFIT_PCT
TP_PROFIT_PCT_BULL = float(_cfg("TP_PROFIT_PCT_BULL"))

# === [ANCHOR: REGIME STATE] 코스닥 레짐 상태 ===
REGIME_STATE: Dict[str, Any] = {
    "mode": "neutral",          # 'bull' | 'bear' | 'neutral'
    "since": None,              # regime 시작 시각(datetime)
    "bear_stage": 0,            # 0/1/2
    "session_high": None,       # 당일 코스닥 고점
    "last_above_vwap_ts": None, # 최근 VWAP 상방 유지 시작시각
    "last_below_vwap_ts": None, # 최근 VWAP 하방 유지 시작시각
    "last_snapshot_ts": None,   # 최근 스냅샷 시간
    "vwap": None,               # 가능하면 채움
    "prev_close": None,         # 전일 종가
    "pct_change": None,          # 등락률(%)
    "stage": 0,
    "R20": None,
    "D1": None
}

# === [ANCHOR: REGIME TABLES] 레짐별 자본 스케일 / 최대 보유 종목 수 / 챔피언 비중 ===
# mode ∈ {'bull','bear','neutral'}, stage ∈ {0,1,2}
REGIME_CAPITAL_SCALE: Dict[Tuple[str, int], float] = {
    ("bull", 2): 1.00,
    ("bull", 1): 0.75,
    ("neutral", 0): 0.50,
    ("bear", 1): 0.30,
    ("bear", 2): 0.15,
}

REGIME_MAX_ACTIVE: Dict[Tuple[str, int], int] = {
    ("bull", 2): 7,
    ("bull", 1): 5,
    ("neutral", 0): 3,
    ("bear", 1): 2,
    ("bear", 2): 1,
}

# 순위별 비중 (합계 1.0 기준)
REGIME_WEIGHTS: Dict[Tuple[str, int], List[float]] = {
    ("bull", 2): [0.25, 0.18, 0.15, 0.13, 0.11, 0.09, 0.09],
    ("bull", 1): [0.28, 0.22, 0.18, 0.17, 0.15],
    ("neutral", 0): [0.40, 0.35, 0.25],
    ("bear", 1): [0.60, 0.40],
    ("bear", 2): [1.00],
}

# 각 종목 Target Notional 내에서 3단계 눌림목 진입 비중
ENTRY_LADDERS: List[float] = [0.40, 0.35, 0.25]

def _get_kosdaq_snapshot(kis: KisAPI) -> Dict[str, Optional[float]]:
    """
    코스닥 지수 스냅샷. 래퍼에 인덱스 조회가 없으면 ETF(229200)로 근사.
    반환: {'price', 'prev_close', 'pct_change', 'vwap', 'above_vwap'}
    """
    price = prev_close = vwap = None

    # 1) 인덱스 시도
    try:
        if hasattr(kis, "get_index_quote"):
            q = kis.get_index_quote(KOSDAQ_CODE)
            if isinstance(q, dict):
                price = _to_float(q.get("price"))
                prev_close = _to_float(q.get("prev_close"))
                vwap = _to_float(q.get("vwap"))
    except Exception:
        pass

    # 2) 폴백: ETF로 근사
    if price is None or prev_close is None:
        try:
            etf = KOSDAQ_ETF_FALLBACK
            last = _to_float(kis.get_current_price(etf))
            cs = kis.get_daily_candles(etf, count=2)
            pc = _to_float(cs[-2]['close']) if cs and len(cs) >= 2 and 'close' in cs[-2] else None
            if last and pc:
                price, prev_close = last, pc
                vwap = None
        except Exception:
            pass

    pct_change = None
    try:
        if price and prev_close and prev_close > 0:
            pct_change = (price - prev_close) / prev_close * 100.0
    except Exception:
        pct_change = None

    above_vwap = None
    try:
        if price is not None and vwap:
            above_vwap = bool(price >= vwap)
    except Exception:
        above_vwap = None

    return {"price": price, "prev_close": prev_close, "pct_change": pct_change, "vwap": vwap, "above_vwap": above_vwap}


def _update_market_regime(kis: KisAPI) -> Dict[str, Any]:
    """코스닥 지수 20일 수익률(R20) + 당일 수익률(D1) 기반 레짐 판정.

    - R20, D1은 KOSDAQ 지수 또는 ETF(KOSDAQ_ETF_FALLBACK)의 일봉으로 계산
    - 레짐(mode, stage) 규칙

      * bull-2:  R20 ≥ +6%  AND D1 ≥ +2.5%
      * bull-1:  R20 ≥ +3%  AND D1 ≥ +0.5%  (단, bull-2는 제외)
      * bear-2:  R20 ≤ -6%  AND D1 ≤ -2.5%
      * bear-1:  R20 ≤ -3%  AND D1 ≤ -0.5%  (단, bear-2는 제외)
      * neutral: -3% < R20 < +3%
                 또는 (|R20| ≥ 3% 이지만 D1이 -0.5% ~ +0.5% 사이인 흔들리는 날)

    stage:
      * bull: 1/2
      * bear: 1/2
      * neutral: 0
    """
    if not REGIME_ENABLED:
        return REGIME_STATE

    now = datetime.now(KST)

    # 스냅샷(전일 종가, 일중 등락률) 업데이트
    snap = _get_kosdaq_snapshot(kis)
    REGIME_STATE["last_snapshot_ts"] = now
    REGIME_STATE["prev_close"] = snap.get("prev_close")
    REGIME_STATE["pct_change"] = snap.get("pct_change")

    # R20 / D1 계산 (기본: KOSDAQ ETF 일봉)
    R20 = None
    D1 = None
    try:
        etf = KOSDAQ_ETF_FALLBACK
        candles = kis.get_daily_candles(etf, count=21)
        if candles and len(candles) >= 21:
            # candles는 과거→현재 순서로 정렬되어 있음
            close_20ago = float(candles[0]["close"])
            close_yday = float(candles[-2]["close"])
            close_today = float(candles[-1]["close"])
            if close_20ago > 0 and close_yday > 0:
                R20 = (close_today / close_20ago - 1.0) * 100.0
                D1 = (close_today / close_yday - 1.0) * 100.0
    except Exception as e:
        logger.warning(f"[REGIME] R20/D1 계산 실패: {e}")

    REGIME_STATE["R20"] = R20
    REGIME_STATE["D1"] = D1

    mode = REGIME_STATE.get("mode") or "neutral"
    stage = int(REGIME_STATE.get("stage") or 0)

    if R20 is None or D1 is None:
        # 데이터가 없으면 보수적으로 neutral-0
        mode, stage = "neutral", 0
    else:
        # 우선순위: 강한 강세/약세 → 일반 강세/약세 → 중립
        if R20 >= 6.0 and D1 >= 2.5:
            mode, stage = "bull", 2
        elif R20 >= 3.0 and D1 >= 0.5:
            mode, stage = "bull", 1
        elif R20 <= -6.0 and D1 <= -2.5:
            mode, stage = "bear", 2
        elif R20 <= -3.0 and D1 <= -0.5:
            mode, stage = "bear", 1
        elif (-3.0 < R20 < 3.0) or (abs(R20) >= 3.0 and -0.5 <= D1 <= 0.5):
            mode, stage = "neutral", 0
        else:
            # 나머지 애매한 케이스는 보수적으로 neutral-0 처리
            mode, stage = "neutral", 0

    REGIME_STATE["mode"] = mode
    REGIME_STATE["stage"] = stage
    # 기존 bear_stage는 약세일 때만 stage를 반영(하위 로직 호환용)
    REGIME_STATE["bear_stage"] = stage if mode == "bear" else 0

    return REGIME_STATE
def log_champion_and_regime(
    logger: logging.Logger,
    champion,
    regime_state: Dict[str, Any],
    context: str,
) -> None:
    """VWAP 챔피언 종목 및 현재 레짐 상태를 상세하게 남기는 공용 로그 함수.

    - champion: 리밸런싱 API나 내부 스코어링에서 1순위로 선택된 종목(없으면 None)
    - regime_state: REGIME_STATE 전역값을 그대로 전달
    - context: 'rebalance_api', 'intra_day' 등 호출 위치 태그
    """
    try:
        now_kst = datetime.now(KST)
        now_str = now_kst.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # 1) 챔피언 종목 선정 사유(최소한 코드/이름/스코어 등 기본 정보 위주)
    if champion is None:
        logger.info(
            "[VWAP_CHAMPION] %s | %s | champion=None (선택된 종목이 없습니다.)",
            now_str,
            context,
        )
    else:
        # champion 형식이 문자열(종목코드)인지, dict인지 모두 처리
        if isinstance(champion, str):
            code = champion
            name = "-"
            detail = "rebalance_api selected[0] 기준 챔피언"
        elif isinstance(champion, dict):
            code = champion.get("code") or champion.get("symbol") or champion.get("stock_code") or "?"
            name = champion.get("name") or champion.get("stock_name") or champion.get("nm") or "?"

            # 메타-K 리밸런싱 결과에 실제로 존재하는 필드들 위주로 사유 구성
            best_k = champion.get("best_k")
            avg_ret = champion.get("avg_return_pct")
            win = champion.get("win_rate_pct")
            mdd = champion.get("mdd_pct")
            cumret = champion.get("cumulative_return_pct")
            trades = champion.get("trades")
            sharpe_m = champion.get("sharpe_m")
            tgt = champion.get("target_price") or champion.get("목표가")
            close = champion.get("close")
            turnover = champion.get("prev_turnover")

            detail_parts = []

            if best_k is not None:
                detail_parts.append(f"best_k={best_k}")
            if avg_ret is not None:
                detail_parts.append(f"avg_ret={avg_ret}%")
            if win is not None:
                detail_parts.append(f"winrate={win}%")
            if mdd is not None:
                detail_parts.append(f"mdd={mdd}%")
            if cumret is not None:
                detail_parts.append(f"cumret={cumret}%")
            if trades is not None:
                detail_parts.append(f"trades={trades}")
            if sharpe_m is not None:
                detail_parts.append(f"sharpe_m={sharpe_m}")
            if tgt is not None and close is not None:
                # 목표가/현재가 차이도 한 줄로 요약
                try:
                    gap_pct = (tgt - close) / close * 100.0
                    detail_parts.append(f"target={tgt}, close={close}, gap={gap_pct:.2f}%")
                except Exception:
                    detail_parts.append(f"target={tgt}, close={close}")
            if turnover is not None:
                detail_parts.append(f"prev_turnover={turnover}")

            detail = ", ".join(detail_parts) if detail_parts else "meta-K 백테스트 기반 정보 없음"

        else:
            code = str(champion)
            name = "-"
            detail = "알 수 없는 champion 타입"

        logger.info(
            "[VWAP_CHAMPION] %s | %s | code=%s, name=%s, detail=%s",
            now_str,
            context,
            code,
            name,
            detail,
        )

    # 2) 레짐 상태 상세 로그
    if regime_state:
        logger.info(
            "[VWAP_REGIME] %s | %s | mode=%s, score=%s, kosdaq_ret5=%s, drop_stage=%s, since=%s, comment=%s",
            now_str,
            context,
            regime_state.get("mode"),
            regime_state.get("score"),
            regime_state.get("kosdaq_ret5"),
            regime_state.get("bear_stage"),
            regime_state.get("since"),
            regime_state.get("comment"),
        )

def _adaptive_exit(
    kis: KisAPI,
    code: str,
    pos: Dict[str, Any],
    regime_mode: str = "neutral",
) -> Tuple[Optional[str], Optional[float], Optional[Any], Optional[int]]:
    """
    레짐(강세/약세/중립) + 1분봉 모멘텀 기반
    - 부분 익절(1차/2차)
    - 트레일링 스탑
    - 손절
    을 동적으로 적용하는 매도 엔진.
    한 번 호출에서 "한 번의 매도"만 실행하고, 그 결과만 반환한다.
    """
    now = datetime.now(KST)
    reason: Optional[str] = None

    # 현재가 조회
    try:
        cur = _safe_get_price(kis, code)
        if cur is None or cur <= 0:
            logger.warning(f"[EXIT-FAIL] {code} 현재가 조회 실패")
            return None, None, None, None
    except Exception as e:
        logger.error(f"[EXIT-FAIL] {code} 현재가 조회 예외: {e}")
        return None, None, None, None

    # === 상태/기초 값 ===
    qty = _to_int(pos.get("qty"), 0)
    if qty <= 0:
        logger.warning(f"[EXIT-FAIL] {code} qty<=0")
        return None, None, None, None

    buy_price = float(pos.get("buy_price", 0.0)) or 0.0
    if buy_price <= 0:
        logger.warning(f"[EXIT-FAIL] {code} buy_price<=0")
        return None, None, None, None

    # 최고가(high) 갱신
    pos["high"] = max(float(pos.get("high", cur)), float(cur))
    max_price = float(pos["high"])

    # 현재 누적 수익률
    pnl_pct = (cur - buy_price) / buy_price * 100.0

    # 부분 익절 플래그 & 비율
    sold_p1 = bool(pos.get("sold_p1", False))
    sold_p2 = bool(pos.get("sold_p2", False))
    qty_p1 = max(1, int(qty * PARTIAL1))
    qty_p2 = max(1, int(qty * PARTIAL2))

    # === 레짐 기반 TP/트레일링 설정 ===
    base_tp1 = DEFAULT_PROFIT_PCT        # 보통 3.0
    base_tp2 = DEFAULT_PROFIT_PCT * 2    # 6.0
    trail_down_frac = 0.018              # 기본: 고점대비 1.8% 되돌리면 컷

    # (선택) 모멘텀 정보를 쓰고 싶으면 여기서 strong_mom 계산
    strong_mom = False
    try:
        # metrics에 is_strong_momentum이 있다면 사용, 없으면 False 유지
        strong_mom = bool(is_strong_momentum(kis, code))
    except Exception:
        strong_mom = False

    if regime_mode == "bull":
        # 좋은 장: 기본 목표 상향
        tp1 = base_tp1 + 1.0      # 4%
        tp2 = base_tp2 + 2.0      # 8%
        trail_down_frac = 0.025   # 2.5%

        if strong_mom:
            # 장도 좋고 모멘텀도 강하면 한 번 더 상향
            tp1 += 1.0            # 5%
            tp2 += 2.0            # 10%
            trail_down_frac = 0.03

    elif regime_mode == "neutral":
        tp1 = base_tp1            # 3%
        tp2 = base_tp2            # 6%
        trail_down_frac = 0.018

        if strong_mom:
            tp1 = base_tp1 + 1.0  # 4%
            tp2 = base_tp2 + 2.0  # 8%
            trail_down_frac = 0.02

    elif regime_mode == "bear":
        # 약세장: 보수적으로
        tp1 = 2.0
        tp2 = 4.0
        trail_down_frac = 0.01
    else:
        tp1 = base_tp1
        tp2 = base_tp2
        trail_down_frac = 0.018

    # 손절 기준
    hard_stop_pct = DEFAULT_LOSS_PCT

    sell_size: int = 0

    # === 1) 손절 ===
    if pnl_pct <= -hard_stop_pct:
        reason = f"손절 {hard_stop_pct:.1f}%"
        sell_size = qty

    # === 2) 2차 TP (더 높은 수익 구간) ===
    elif (pnl_pct >= tp2) and (not sold_p2) and qty > 1:
        reason = f"2차 익절 {tp2:.1f}%"
        sell_size = min(qty, qty_p2)
        pos["sold_p2"] = True

    # === 3) 1차 TP ===
    elif (pnl_pct >= tp1) and (not sold_p1) and qty > 1:
        reason = f"1차 익절 {tp1:.1f}%"
        sell_size = min(qty, qty_p1)
        pos["sold_p1"] = True

    else:
        # === 4) 트레일링 스탑 ===
        if max_price >= buy_price * (1 + tp1 / 100.0) and cur <= max_price * (1 - trail_down_frac):
            reason = f"트레일링스톱({trail_down_frac*100:.1f}%)"
            sell_size = qty
        else:
            # 청산 조건 없음 → 보유 유지
            return None, None, None, None

    # === 실제 매도 실행 ===
    try:
        exec_px, result = _sell_once(kis, code, sell_size, prefer_market=True)
        sold_qty = sell_size

        # 보유 수량 감소
        pos["qty"] = max(0, qty - sell_size)

        # 실현손익 로그
        try:
            log_trade(
                {
                    "datetime": now.strftime("%Y-%m-%d %H:%M:%S"),
                    "code": code,
                    "name": pos.get("name"),
                    "side": "SELL",
                    "qty": int(sold_qty),
                    "price": float(exec_px) if exec_px is not None else float(cur),
                    "amount": int(sold_qty) * int(exec_px or cur),
                    "reason": reason,
                    "regime_mode": regime_mode,
                }
            )
        except Exception as e:
            logger.warning(f"[EXIT-LOG-FAIL] {code}: {e}")

    except Exception as e:
        logger.error(f"[SELL-FAIL] {code} qty={sell_size} err={e}")
        # 매도 실패 시에는 상태 원복하지 않고, 다음 루프에서 다시 판단
        return None, None, None, None

    return reason, exec_px, result, sold_qty


def main():
    kis = KisAPI()

    rebalance_date = get_rebalance_anchor_date()
    logger.info(f"[ℹ️ 리밸런싱 기준일(KST)]: {rebalance_date} (anchor={REBALANCE_ANCHOR}, ref={WEEKLY_ANCHOR_REF})")
    logger.info(
        f"[⏱️ 커트오프(KST)] SELL_FORCE_TIME={SELL_FORCE_TIME.strftime('%H:%M')} / 전체잔고매도={SELL_ALL_BALANCES_AT_CUTOFF} / "
        f"패스(커트오프/마감)={FORCE_SELL_PASSES_CUTOFF}/{FORCE_SELL_PASSES_CLOSE}"
    )
    logger.info(f"[💰 DAILY_CAPITAL] {DAILY_CAPITAL:,}원")
    logger.info(f"[🛡️ SLIPPAGE_ENTER_GUARD_PCT] {SLIPPAGE_ENTER_GUARD_PCT:.2f}%")

    # 상태 복구
    holding, traded = load_state()
    logger.info(f"[상태복구] holding: {list(holding.keys())}, traded: {list(traded.keys())}")

    # === [NEW] 주간 리밸런싱 강제/중복 방지 ===
    targets: List[Dict[str, Any]] = []
    if REBALANCE_ANCHOR == "weekly":
        if should_weekly_rebalance_now():
            targets = fetch_rebalancing_targets(rebalance_date)
            # 중복 실행 방지를 위해 즉시 스탬프(필요 시 FORCE로 재실행 가능)
            stamp_weekly_done()
            logger.info(f"[REBALANCE] 이번 주 리밸런싱 실행 기록 저장({_this_iso_week_key()})")
        else:
            logger.info("[REBALANCE] 이번 주 이미 실행됨 → 신규 리밸런싱 생략 (보유 관리만)")
    else:
        # today/monthly 등 다른 앵커 모드는 기존 방식으로 바로 호출
        targets = fetch_rebalancing_targets(rebalance_date)

    # === [NEW] 예산 가드: 예수금이 0/부족이면 신규 매수만 스킵 ===
    can_buy = True
    try:
        cash = kis.get_cash_available_today()
        logger.info(f"[BUDGET] today cash available = {cash:,} KRW")
        if cash <= 0:
            can_buy = False
            logger.warning("[BUDGET] 가용현금 0 → 신규 매수 스킵(보유 관리만 수행)")
    except Exception as e:
        logger.error(f"[BUDGET_FAIL] 예수금 조회 실패: {e}")
        # 실패 시에는 일단 보수적으로 신규매수 스킵
        can_buy = False

    # 리밸런싱 대상 후처리: qty 없고 weight만 있으면 DAILY_CAPITAL로 수량 계산
    processed_targets: Dict[str, Any] = {}
    for t in targets:
        code = t.get("stock_code") or t.get("code")
        if not code:
            continue
        name = t.get("name") or t.get("종목명")
        k_best = t.get("best_k") or t.get("K") or t.get("k")
        target_price = _to_float(t.get("목표가") or t.get("target_price"))
        qty = _to_int(t.get("매수수량") or t.get("qty"), 0)
        weight = t.get("weight")
        strategy = t.get("strategy") or "전월 rolling K 최적화"
        avg_return_pct = _to_float(t.get("avg_return_pct") or t.get("수익률(%)"), 0.0)
        win_rate_pct = _to_float(t.get("win_rate_pct") or t.get("승률(%)"), 0.0)
        mdd_pct = _to_float(t.get("mdd_pct") or t.get("MDD(%)"), 0.0)
        trades = _to_int(t.get("trades"), 0)
        sharpe_m = _to_float(t.get("sharpe_m"), 0.0)
        cumret_pct = _to_float(t.get("cumulative_return_pct") or t.get("수익률(%)"), 0.0)

        if qty <= 0 and weight is not None:
            ref_px = _to_float(t.get("close")) or _to_float(t.get("prev_close"))
            try:
                qty = _weight_to_qty(kis, code, float(weight), DAILY_CAPITAL, ref_price=ref_px)
            except Exception as e:
                logger.warning("[REBALANCE] weight→qty 변환 실패 %s: %s", code, e)
                qty = 0

        processed_targets[code] = {
            "code": code,
            "name": name,
            "best_k": k_best,
            "target_price": target_price,
            "qty": qty,
            "strategy": strategy,
            "avg_return_pct": avg_return_pct,
            "win_rate_pct": win_rate_pct,
            "mdd_pct": mdd_pct,
            "trades": trades,
            "sharpe_m": sharpe_m,
            "cumulative_return_pct": cumret_pct,
            "prev_open": t.get("prev_open"),
            "prev_high": t.get("prev_high"),
            "prev_low": t.get("prev_low"),
            "prev_close": t.get("prev_close"),
            "prev_volume": t.get("prev_volume"),
        }

    filtered_targets: Dict[str, Any] = {}
    for code, info in processed_targets.items():
        trades = _to_int(info.get("trades"), 0)
        win_rate = _to_float(info.get("win_rate_pct"), 0.0)
        mdd = abs(_to_float(info.get("mdd_pct"), 0.0) or 0.0)
        sharpe = _to_float(info.get("sharpe_m"), 0.0)

        if (
            trades < CHAMPION_MIN_TRADES
            or win_rate < CHAMPION_MIN_WINRATE
            or mdd > CHAMPION_MAX_MDD
            or sharpe < CHAMPION_MIN_SHARPE
        ):
            logger.info(
                f"[CHAMPION_FILTER_SKIP] {code}: trades={trades}, win={win_rate:.1f}%, mdd={mdd:.1f}%, sharpe={sharpe:.2f}"
            )
            continue

        filtered_targets[code] = info

    processed_targets = filtered_targets

    # 챔피언 등급화 (A/B/C) → 실제 매수 후보는 A급만 사용
    graded_targets: Dict[str, Any] = {}
    grade_counts = {"A": 0, "B": 0, "C": 0}
    for code, info in processed_targets.items():
        grade = _classify_champion_grade(info)
        info["champion_grade"] = grade
        graded_targets[code] = info
        grade_counts[grade] = grade_counts.get(grade, 0) + 1

    logger.info(
        "[CHAMPION-GRADE] A:%d / B:%d / C:%d (A급만 실제 매수)",
        grade_counts.get("A", 0),
        grade_counts.get("B", 0),
        grade_counts.get("C", 0),
    )

    processed_targets = {code: info for code, info in graded_targets.items() if info.get("champion_grade") == "A"}
    non_a = [code for code, info in graded_targets.items() if info.get("champion_grade") != "A"]
    if non_a:
        logger.info(
            "[CHAMPION-HOLD] B/C급 %d종목은 관찰만 하고 매수 제외: %s",
            len(non_a),
            ",".join(non_a),
        )

    if processed_targets:
        cumrets = [
            _to_float(info.get("cumulative_return_pct"), 0.0) or 0.0 for info in processed_targets.values()
        ]
        win_rates = [_to_float(info.get("win_rate_pct"), 0.0) or 0.0 for info in processed_targets.values()]
        sharpes = [_to_float(info.get("sharpe_m"), 0.0) or 0.0 for info in processed_targets.values()]
        mdds = [abs(_to_float(info.get("mdd_pct"), 0.0) or 0.0) for info in processed_targets.values()]

        for code, info in processed_targets.items():
            cum = _to_float(info.get("cumulative_return_pct"), 0.0) or 0.0
            win = _to_float(info.get("win_rate_pct"), 0.0) or 0.0
            sharpe = _to_float(info.get("sharpe_m"), 0.0) or 0.0
            mdd_val = abs(_to_float(info.get("mdd_pct"), 0.0) or 0.0)

            score = (
                _percentile_rank(cumrets, cum) * 0.35
                + _percentile_rank(win_rates, win) * 0.25
                + _percentile_rank(sharpes, sharpe) * 0.25
                + _percentile_rank(mdds, mdd_val, higher_is_better=False) * 0.15
            )

            info["composite_score"] = round(score, 4)
            processed_targets[code] = info
    else:
        logger.warning("[CHAMPION_FILTER] 조건 충족 종목 없음 → 챔피언 루프 스킵")

    # === [NEW] Regime + 모멘텀 기반 상위 1~2종목 자동 선택 ===
    # - rolling K 리밸런싱 결과 중에서 최근 모멘텀/수익률이 가장 강한 소수 종목만 실매매 대상으로 사용
    # - 레짐(mode)에 따라 신규 편입 허용 종목 수를 1~2개로 자동 조절
    #   * bull / neutral: 최대 2개
    #   * bear: 최대 1개 (방어적 운용)
    # - intraday 진입은 기존 VWAP 가드(is_vwap_ok_for_entry)로 필터링됨
    selected_targets: Dict[str, Any] = {}


    try:
        # 가능하면 당일 레짐을 한번 계산해서 사용
        regime_snapshot = _update_market_regime(kis)
        mode = (regime_snapshot or {}).get("mode") or "neutral"
        stage = int((regime_snapshot or {}).get("stage") or 0)
        R20 = regime_snapshot.get("R20")
        D1 = regime_snapshot.get("D1")

        # 🔹 로그/조건식에서 쓰는 등락률(%): 새 레짐에서는 D1을 그대로 사용
        pct_change = float(D1 or 0.0)
    except Exception as e:
        logger.warning("[REBALANCE] 레짐 스냅샷 계산 실패, neutral-0로 대체: %s", e)
        mode = "neutral"
        stage = 0
        R20 = None
        D1 = None
        pct_change = 0.0

    regime_key = (mode, stage)
    cap_scale = REGIME_CAPITAL_SCALE.get(regime_key, REGIME_CAPITAL_SCALE.get(("neutral", 0), 0.5))

    # 레짐 + 예수금 기반 실제 사용 자본 계산
    try:
        ord_cash = kis.get_cash_available_today()
    except Exception as e:
        logger.error("[BUDGET_FAIL] 예수금 조회 실패(regime-capital): %s", e)
        ord_cash = 0

    capital_base = int(max(0, int(ord_cash * CAP_CAP)))
    capital_active = int(min(capital_base * cap_scale, DAILY_CAPITAL))
    logger.info(
        f"[REGIME-CAP] mode={mode} stage={stage} R20={R20 if R20 is not None else 'N/A'} D1={D1 if D1 is not None else 'N/A'} "
        f"ord_cash={ord_cash:,} base={capital_base:,} active={capital_active:,} scale={cap_scale:.2f}"
    )


    # 레짐별 최대 보유 종목 수
    n_active = REGIME_MAX_ACTIVE.get(regime_key, REGIME_MAX_ACTIVE.get(("neutral", 0), 3))

    scored: List[Tuple[str, float, bool]] = []

    for code, info in processed_targets.items():
        score = _to_float(info.get("composite_score"), 0.0) or 0.0

        # 단기 모멘텀 강세 여부 (is_strong_momentum)로 버킷 구분
        try:
            strong = is_strong_momentum(kis, code)
        except Exception as e:
            logger.warning("[REBALANCE] 모멘텀 판별 실패 %s: %s", code, e)
            strong = False

        scored.append((code, score, strong))

    # 모멘텀 strong 버킷 우선, 그 다음 나머지 중에서 점수 순으로 채우기
    strong_bucket = [x for x in scored if x[2]]
    weak_bucket = [x for x in scored if not x[2]]

    strong_bucket.sort(key=lambda x: x[1], reverse=True)
    weak_bucket.sort(key=lambda x: x[1], reverse=True)

    picked: List[str] = []

    # 모멘텀 강한 버킷을 우선 사용하되, 전체 보유 종목 수는 레짐별 n_active로 제한
    for code, score, _ in strong_bucket:
        if len(picked) >= n_active:
            break
        picked.append(code)

    for code, score, _ in weak_bucket:
        if len(picked) >= n_active:
            break
        picked.append(code)

    # === [NEW] 레짐별 챔피언 비중 & Target Notional 계산 ===
    regime_weights = REGIME_WEIGHTS.get(regime_key, REGIME_WEIGHTS.get(("neutral", 0), [1.0]))
    # 선택된 종목 수만큼 비중 슬라이스
    weights_for_picked: List[float] = list(regime_weights[: len(picked)])

    for idx, code in enumerate(picked):
        if code not in processed_targets:
            continue
        w = weights_for_picked[idx] if idx < len(weights_for_picked) else 0.0
        t = processed_targets[code]
        t["regime_weight"] = float(w)
        t["capital_active"] = int(capital_active)
        target_notional = int(round(capital_active * w))
        t["target_notional"] = target_notional

        ref_px = _to_float(t.get("close")) or _to_float(t.get("prev_close"))
        planned_qty = _notional_to_qty(kis, code, target_notional, ref_price=ref_px)
        t["qty"] = int(planned_qty)
        t["매수수량"] = int(planned_qty)
        processed_targets[code] = t

    for code in picked:
        if code in processed_targets:
            selected_targets[code] = processed_targets[code]

    logger.info(
        "[REGIME-CHAMPIONS] mode=%s stage=%s n_active=%s picked=%s capital_active=%s",
        mode,
        stage,
        n_active,
        picked,
        f"{capital_active:,}",
    )

    logger.info(
        "[REBALANCE] 레짐=%s pct=%.2f%%, 후보 %d개 중 상위 %d종목만 선택: %s",
        mode,
        pct_change,
        len(processed_targets),
        len(selected_targets),
        ",".join(selected_targets.keys()),
    )

    code_to_target: Dict[str, Any] = selected_targets

    # 눌림목 스캔용 코스닥 시총 상위 리스트 (챔피언과 별도로 관리)
    pullback_watch: Dict[str, Dict[str, Any]] = {}
    if USE_PULLBACK_ENTRY:
        try:
            pb_weight = max(0.0, min(PULLBACK_UNIT_WEIGHT, 1.0))
            base_notional = int(round(capital_active * pb_weight))
            pb_df = get_kosdaq_top_n(date_str=rebalance_date, n=PULLBACK_TOPN)
            for _, row in pb_df.iterrows():
                code = str(row.get("Code") or row.get("code") or "").zfill(6)
                if not code:
                    continue
                pullback_watch[code] = {
                    "code": code,
                    "name": row.get("Name") or row.get("name"),
                    "notional": base_notional,
                }
            logger.info(
                f"[PULLBACK-WATCH] 코스닥 시총 Top{PULLBACK_TOPN} {len(pullback_watch)}종목 스캔 준비"
            )
        except Exception as e:
            logger.warning(f"[PULLBACK-WATCH-FAIL] 시총 상위 로드 실패: {e}")

    loop_sleep_sec = 2.5  # 메인 루프 대기 시간(초)

    try:
        while True:
            # === 코스닥 레짐 업데이트 ===
            regime = _update_market_regime(kis)
            pct_txt = f"{regime.get('pct_change'):.2f}%" if regime.get("pct_change") is not None else "N/A"
            logger.info(f"[REGIME] mode={regime['mode']} stage={regime['bear_stage']} pct={pct_txt}")

            # 장 상태
            try:
                is_open = kis.is_market_open()
            except Exception:
                is_open = True
            now_dt_kst = datetime.now(KST)
            now_str = now_dt_kst.strftime("%Y-%m-%d %H:%M:%S")
            logger.info(f"[⏰ 장상태] {'OPEN' if is_open else 'CLOSED'} / KST={now_str}")

            # 잔고 동기화 & 보유분 능동관리 부트스트랩
            ord_psbl_map: Dict[str, int] = {}
            name_map: Dict[str, str] = {}
            try:
                balances = _fetch_balances(kis)
                logger.info(f"[보유잔고 API 결과 종목수] {len(balances)}개")
                for stock in balances:
                    code_b = stock.get("pdno")
                    name_b = stock.get("prdt_name")
                    name_map[code_b] = name_b
                    logger.debug(
                        " [잔고] 종목:%s, 코드:%s, 보유:%s, 매도가능:%s",
                        name_b,
                        code_b,
                        stock.get("hldg_qty"),
                        stock.get("ord_psbl_qty"),
                    )

                current_holding = {
                    b["pdno"]: _to_int(b.get("hldg_qty", 0))
                    for b in balances
                    if _to_int(b.get("hldg_qty", 0)) > 0
                }
                ord_psbl_map = {
                    b["pdno"]: _to_int(b.get("ord_psbl_qty", 0))
                    for b in balances
                }

                # 신규 보유분을 능동관리 대상으로 자동 초기화
                for b in balances:
                    code_b = str(b.get("pdno", "")).strip()
                    qty_b = _to_int(b.get("hldg_qty", 0))
                    avg_b = _to_float(
                        b.get("pchs_avg_pric") or b.get("avg_price") or 0.0,
                        0.0,
                    )

                    if (
                        qty_b > 0
                        and code_b
                        and code_b not in holding
                        and (avg_b is not None)
                        and avg_b > 0
                    ):
                        _init_position_state_from_balance(
                            kis, holding, code_b, float(avg_b), int(qty_b)
                        )
                        logger.info(
                            f"[잔고초기화] code={code_b} qty={qty_b} avg={avg_b}"
                        )

                # 실제 잔고에서 사라진 보유항목은 정리
                for code in list(holding.keys()):
                    if code not in current_holding or current_holding[code] == 0:
                        logger.info(
                            f"[보유종목 해제] {code} : 실제잔고 없음 → holding 제거"
                        )
                        holding.pop(code, None)

            except Exception as e:
                logger.error(f"[잔고조회 오류]{e}")

            # 장 마감 시: 캔들/ATR/모멘텀/매매 로직 스킵
            if not is_open:
                logger.info(
                    "[마감상태] 캔들/ATR/모멘텀/매매 로직 스킵 → 잔고만 동기화 후 대기"
                )
                save_state(holding, traded)
                time.sleep(60.0)
                continue

            # ====== 매수/매도(전략) LOOP — 오늘의 타겟 ======
            for code, target in code_to_target.items():
                prev_volume = _to_float(target.get("prev_volume"))
                prev_open = _to_float(target.get("prev_open"))
                prev_close = _to_float(target.get("prev_close"))
                logger.debug(
                    f"[prev_volume 체크] {code} 거래량:{prev_volume}, 전일시가:{prev_open}, 전일종가:{prev_close}"
                )

                planned_total_qty = _to_int(target.get("매수수량") or target.get("qty"), 0)
                if planned_total_qty <= 0:
                    logger.info(f"[SKIP] {code}: 매수수량 없음/0")
                    continue

                # 눌림목 3단계 진입(40/35/25%)을 위한 스테이지별 목표 수량
                stage1_qty = max(1, int(planned_total_qty * ENTRY_LADDERS[0]))
                stage2_qty = max(0, int(planned_total_qty * ENTRY_LADDERS[1]))
                stage3_qty = max(0, int(planned_total_qty - stage1_qty - stage2_qty))

                # 1차 진입 시 실제 매수 수량은 stage1(40%)만 사용
                qty = stage1_qty

                grade = target.get("champion_grade") or "C"
                if grade != "A":
                    logger.info(
                        f"[CHAMPION-SKIP] {code}: grade={grade} → 매수 루프에서 제외"
                    )
                    continue

                k_value = target.get("best_k") or target.get("K") or target.get("k")
                _ = None if k_value is None else _to_float(k_value)

                eff_target_price, k_used = compute_entry_target(kis, target)
                strategy = target.get("strategy") or "전월 rolling K 최적화"
                name = target.get("name") or target.get("종목명") or name_map.get(code)

                try:
                    current_price = _safe_get_price(kis, code)
                    logger.info(f"[📈 현재가] {code}: {current_price}")

                    pullback_info: Dict[str, Any] = {}
                    try:
                        pullback_info = _detect_pullback_reversal(
                            kis=kis,
                            code=code,
                            current_price=float(current_price) if current_price else None,
                        )
                    except Exception:
                        pullback_info = {}

                    trade_common_buy = {
                        "datetime": now_str,
                        "code": code,
                        "name": name,
                        "qty": qty,
                        "K": k_value if k_value is not None else k_used,
                        "target_price": eff_target_price,
                        "strategy": strategy,
                    }

                    daily_ctx = _compute_daily_entry_context(
                        kis, code, float(current_price) if current_price else None
                    )
                    intraday_ctx = _compute_intraday_entry_context(
                        kis, code, prev_high=target.get("prev_high")
                    )

                    if is_bad_entry(code, daily_ctx, intraday_ctx, REGIME_STATE):
                        logger.info(
                            f"[CHAMPION-HOLD] {code}: A급이지만 BAD 타점 → 오늘은 매수 보류"
                        )
                        continue

                    if not is_good_entry(
                        code,
                        daily_ctx,
                        intraday_ctx,
                        prev_high=target.get("prev_high"),
                    ):
                        logger.info(
                            f"[WAIT] {code}: A급이나 GOOD 타점 미충족 → 눌림 대기"
                        )
                        continue

                    # --- 매수 --- (돌파 진입 + 슬리피지 가드 + 예산 가드)
                    if is_open and code not in holding and code not in traded:
                        if not can_buy:
                            logger.info(
                                f"[BUDGET_SKIP] {code}: 예산 없음 → 신규 매수 스킵"
                            )
                            continue

                        trigger_price = eff_target_price
                        if pullback_info.get("reversal_price"):
                            if trigger_price is None:
                                trigger_price = float(pullback_info.get("reversal_price"))
                            else:
                                trigger_price = max(
                                    float(trigger_price),
                                    float(pullback_info.get("reversal_price")),
                                )

                        enter_cond = (
                            current_price is not None
                            and trigger_price is not None
                            and int(current_price) >= int(trigger_price)
                        )

                        if enter_cond:
                            guard_ok = True

                            # 1) 진입 슬리피지 가드
                            if (
                                eff_target_price
                                and eff_target_price > 0
                                and current_price is not None
                            ):
                                slip_pct = (
                                    (
                                        float(current_price)
                                        - float(eff_target_price)
                                    )
                                    / float(eff_target_price)
                                ) * 100.0
                                if slip_pct > SLIPPAGE_ENTER_GUARD_PCT:
                                    guard_ok = False
                                    logger.info(
                                        f"[ENTER-GUARD] {code} 진입슬리피지 {slip_pct:.2f}% > "
                                        f"{SLIPPAGE_ENTER_GUARD_PCT:.2f}% → 진입 스킵"
                                    )

                            # 2) VWAP 가드
                            if guard_ok and current_price is not None:
                                vwap_val = kis.get_vwap_today(code)
                                if vwap_val is None:
                                    logger.info(
                                        f"[VWAP-SKIP] {code}: VWAP 데이터 없음 → VWAP 가드 생략"
                                    )
                                else:
                                    if not vwap_guard(
                                        float(current_price),
                                        float(vwap_val),
                                        VWAP_TOL,
                                    ):
                                        guard_ok = False
                                        logger.info(
                                            f"[VWAP-GUARD] {code}: 현재가({current_price}) < VWAP*(1 - {VWAP_TOL:.4f}) "
                                            f"→ 진입 스킵 (VWAP={vwap_val:.2f})"
                                        )
                            if not guard_ok:
                                continue

                            result = place_buy_with_fallback(
                                kis, code, qty, limit_price=int(eff_target_price)
                            )
                            try:
                                if isinstance(result, dict) and result.get("rt_cd") == "0":
                                    out = result.get("output") or {}
                                    odno = (
                                        out.get("ODNO")
                                        or out.get("ord_no")
                                        or out.get("order_no")
                                        or ""
                                    )
                                    ensure_fill_has_name(
                                        odno=odno,
                                        code=code,
                                        name=name or "",
                                        qty=qty,
                                        price=current_price or 0.0,
                                    )
                            except Exception as e:
                                logger.warning(
                                    f"[BUY_FILL_NAME_FAIL] code={code} ex={e}"
                                )

                            _init_position_state(
                                kis,
                                holding,
                                code,
                                float(current_price),
                                int(qty),
                                (k_value if k_value is not None else k_used),
                                eff_target_price,
                            )

                            # 눌림목 3단계 진입용 상태값 세팅
                            try:
                                pos = holding.get(code, {})
                                pos["entry_stage"] = 1
                                pos["max_price_after_entry"] = float(current_price)
                                pos["planned_total_qty"] = int(planned_total_qty)
                                pos["stage1_qty"] = int(stage1_qty)
                                pos["stage2_qty"] = int(stage2_qty)
                                pos["stage3_qty"] = int(stage3_qty)
                                if pullback_info.get("peak_price"):
                                    pos["pullback_peak_price"] = float(pullback_info.get("peak_price"))
                                if pullback_info.get("reversal_price"):
                                    pos["pullback_reversal_price"] = float(pullback_info.get("reversal_price"))
                                holding[code] = pos
                            except Exception as e:
                                logger.warning(f"[INIT-SCALEIN-STATE-FAIL] {code}: {e}")

                            traded[code] = {
                                "buy_time": now_str,
                                "qty": int(qty),
                                "price": float(current_price),
                            }
                            logger.info(
                                f"[✅ 매수주문] {code}, qty={qty}, price={current_price}, result={result}"
                            )

                            log_trade(
                                {
                                    **trade_common_buy,
                                    "side": "BUY",
                                    "price": current_price,
                                    "amount": int(current_price) * int(qty),
                                    "result": result,
                                }
                            )
                            save_state(holding, traded)
                            time.sleep(RATE_SLEEP_SEC)
                        else:
                            logger.info(
                                f"[SKIP] {code}: 현재가({current_price}) < 목표가({eff_target_price}), 미매수"
                            )
                            continue

                    # --- 실전형 청산 (타겟 보유포지션) ---
                    if is_open and code in holding:
                        # (눌림목 3단계 진입) 추가 매수 평가
                        try:
                            _maybe_scale_in_dips(
                                kis=kis,
                                holding=holding,
                                code=code,
                                target=target,
                                now_str=now_str,
                                regime_mode=regime["mode"],
                            )
                        except Exception as e:
                            logger.warning(f"[SCALE-IN-EVAL-FAIL] {code}: {e}")

                        # (약세 레짐) 단계적 축소
                        if regime["mode"] == "bear":
                            sellable_here = ord_psbl_map.get(code, 0)
                            if sellable_here > 0:
                                if (
                                    regime["bear_stage"] >= 1
                                    and not holding[code].get("bear_s1_done")
                                ):
                                    cut_qty = max(
                                        1, int(holding[code]["qty"] * REG_PARTIAL_S1)
                                    )
                                    logger.info(
                                        f"[REGIME-REDUCE-S1] {code} 약세1단계 {REG_PARTIAL_S1 * 100:.0f}% 축소 → {cut_qty}"
                                    )
                                    exec_px, result = _sell_once(
                                        kis, code, cut_qty, prefer_market=True
                                    )
                                    holding[code]["qty"] -= int(cut_qty)
                                    holding[code]["bear_s1_done"] = True
                                    log_trade(
                                        {
                                            "datetime": now_str,
                                            "code": code,
                                            "name": name,
                                            "qty": int(cut_qty),
                                            "K": k_value
                                            if k_value is not None
                                            else k_used,
                                            "target_price": eff_target_price,
                                            "strategy": strategy,
                                            "side": "SELL",
                                            "price": exec_px,
                                            "amount": int((exec_px or 0))
                                            * int(cut_qty),
                                            "result": result,
                                            "reason": "시장약세 1단계 축소",
                                        }
                                    )
                                    save_state(holding, traded)
                                    time.sleep(RATE_SLEEP_SEC)

                                if (
                                    regime["bear_stage"] >= 2
                                    and not holding[code].get("bear_s2_done")
                                ):
                                    cut_qty = max(
                                        1, int(holding[code]["qty"] * REG_PARTIAL_S2)
                                    )
                                    logger.info(
                                        f"[REGIME-REDUCE-S2] {code} 약세2단계 {REG_PARTIAL_S2 * 100:.0f}% 축소 → {cut_qty}"
                                    )
                                    exec_px, result = _sell_once(
                                        kis, code, cut_qty, prefer_market=True
                                    )
                                    holding[code]["qty"] -= int(cut_qty)
                                    holding[code]["bear_s2_done"] = True
                                    log_trade(
                                        {
                                            "datetime": now_str,
                                            "code": code,
                                            "name": name,
                                            "qty": int(cut_qty),
                                            "K": k_value
                                            if k_value is not None
                                            else k_used,
                                            "target_price": eff_target_price,
                                            "strategy": strategy,
                                            "side": "SELL",
                                            "price": exec_px,
                                            "amount": int((exec_px or 0))
                                            * int(cut_qty),
                                            "result": result,
                                            "reason": "시장약세 2단계 축소",
                                        }
                                    )
                                    save_state(holding, traded)
                                    time.sleep(RATE_SLEEP_SEC)

                        # 먼저 트리거 기반 청산 평가/집행
                        sellable_here = ord_psbl_map.get(code, 0)
                        if sellable_here <= 0:
                            logger.info(
                                f"[SKIP] {code}: 매도가능수량=0 (대기/체결중/락) → 매도 보류"
                            )
                        else:
                            reason, exec_price, result, sold_qty = _adaptive_exit(
                                kis, code, holding[code], regime_mode=regime["mode"]
                            )
                            if reason:
                                trade_common_sell = {
                                    "datetime": now_str,
                                    "code": code,
                                    "name": name,
                                    "qty": int(sold_qty or 0),
                                    "K": k_value
                                    if k_value is not None
                                    else k_used,
                                    "target_price": eff_target_price,
                                    "strategy": strategy,
                                }
                                _bp = (
                                    float(holding[code].get("buy_price", 0.0))
                                    if code in holding
                                    else 0.0
                                )
                                _pnl_pct = (
                                    (
                                        (float(exec_price) - _bp)
                                        / _bp
                                    )
                                    * 100.0
                                    if (exec_price and _bp > 0)
                                    else None
                                )
                                _profit = (
                                    (
                                        (float(exec_price) - _bp)
                                        * int(sold_qty)
                                    )
                                    if (exec_price and _bp > 0 and sold_qty)
                                    else None
                                )
                                log_trade(
                                    {
                                        **trade_common_sell,
                                        "side": "SELL",
                                        "price": exec_price,
                                        "amount": int((exec_price or 0))
                                        * int(sold_qty or 0),
                                        "result": result,
                                        "pnl_pct": (
                                            _pnl_pct if _pnl_pct is not None else None
                                        ),
                                        "profit": (
                                            int(round(_profit))
                                            if _profit is not None
                                            else None
                                        ),
                                        "reason": reason,
                                    }
                                )
                                save_state(holding, traded)
                                time.sleep(RATE_SLEEP_SEC)
                            else:
                                try:
                                    if is_strong_momentum(kis, code):
                                        logger.info(
                                            f"[SELL_GUARD] {code} 모멘텀 강세 → 트리거 부재, 매도 보류"
                                        )
                                except Exception as e:
                                    logger.warning(
                                        f"[SELL_GUARD_FAIL] {code} 모멘텀 평가 실패: {e}"
                                    )

                except Exception as e:
                    logger.error(f"[❌ 주문/조회 실패] {code} : {e}")
                    continue

            # ====== 눌림목 전용 매수 (챔피언과 독립적으로 Top-N 시총 리스트 스캔) ======
            if USE_PULLBACK_ENTRY and is_open and pullback_watch:
                for code, info in pullback_watch.items():
                    if code in code_to_target:
                        continue  # 챔피언 루프와 별도로만 처리
                    if code in holding or code in traded:
                        continue
                    if not can_buy:
                        logger.info(
                            f"[PULLBACK-BUDGET-SKIP] {code}: 예산 없음 → 눌림목 신규 매수 스킵"
                        )
                        continue

                    try:
                        current_price = _safe_get_price(kis, code)
                    except Exception:
                        current_price = None
                    if current_price is None or current_price <= 0:
                        continue

                    try:
                        pullback_info = _detect_pullback_reversal(
                            kis=kis,
                            code=code,
                            current_price=float(current_price),
                        )
                    except Exception as e:
                        logger.warning(f"[PULLBACK-DETECT-FAIL] {code}: {e}")
                        continue
                    if not pullback_info.get("setup"):
                        logger.info(
                            f"[PULLBACK-SKIP] {code}: 신고가 눌림 패턴 미충족 → reason={pullback_info.get('reason')}"
                        )
                        continue
                    if not pullback_info.get("reversing"):
                        rev_px = pullback_info.get("reversal_price")
                        logger.info(
                            f"[PULLBACK-WAIT] {code}: 현재가({current_price}) < 반등확인선({rev_px}) → 눌림목 대기"
                        )
                        continue

                    trigger_price = float(pullback_info.get("reversal_price") or current_price)
                    notional = int(info.get("notional") or 0)
                    if notional <= 0:
                        logger.info(
                            f"[PULLBACK-SKIP] {code}: notional=0 → 매수 스킵"
                        )
                        continue

                    qty = _notional_to_qty(kis, code, notional, ref_price=current_price)
                    if qty <= 0:
                        logger.info(f"[PULLBACK-SKIP] {code}: 수량 0 → 매수 스킵")
                        continue

                    vwap_val = kis.get_vwap_today(code)
                    if vwap_val is not None and vwap_val > 0:
                        if not vwap_guard(float(current_price), float(vwap_val), VWAP_TOL):
                            logger.info(
                                f"[PULLBACK-VWAP-GUARD] {code}: 현재가({current_price}) < VWAP*(1 - {VWAP_TOL:.4f}) "
                                f"→ 눌림목 진입 스킵 (VWAP={vwap_val:.2f})"
                            )
                            continue

                    if int(current_price) >= int(trigger_price):
                        result = place_buy_with_fallback(
                            kis, code, int(qty), limit_price=int(trigger_price)
                        )
                        try:
                            _init_position_state(
                                kis,
                                holding,
                                code,
                                float(current_price),
                                int(qty),
                                None,
                                trigger_price,
                            )
                        except Exception as e:
                            logger.warning(f"[PULLBACK-INIT-FAIL] {code}: {e}")

                        traded[code] = {
                            "buy_time": now_str,
                            "qty": int(qty),
                            "price": float(current_price),
                        }
                        logger.info(
                            f"[✅ 눌림목 매수] {code}, qty={qty}, price={current_price}, trigger={trigger_price}, result={result}"
                        )

                        log_trade(
                            {
                                "datetime": now_str,
                                "code": code,
                                "name": info.get("name"),
                                "qty": int(qty),
                                "K": None,
                                "target_price": trigger_price,
                                "strategy": f"코스닥 Top{PULLBACK_TOPN} 눌림목",
                                "side": "BUY",
                                "price": float(current_price),
                                "amount": int(float(current_price) * int(qty)),
                                "result": result,
                            }
                        )
                        save_state(holding, traded)
                        time.sleep(RATE_SLEEP_SEC)

            # ====== (A) 비타겟 보유분도 장중 능동관리 ======
            if is_open:
                for code in list(holding.keys()):
                    if code in code_to_target:
                        continue  # 위 루프에서 이미 처리

                    # 약세 단계 축소(비타겟)
                    if regime["mode"] == "bear":
                        sellable_here = ord_psbl_map.get(code, 0)
                        if sellable_here > 0:
                            if (
                                regime["bear_stage"] >= 1
                                and not holding[code].get("bear_s1_done")
                            ):
                                cut_qty = max(
                                    1, int(holding[code]["qty"] * REG_PARTIAL_S1)
                                )
                                logger.info(
                                    f"[REGIME-REDUCE-S1/비타겟] {code} 약세1단계 {REG_PARTIAL_S1 * 100:.0f}% 축소 → {cut_qty}"
                                )
                                exec_px, result = _sell_once(
                                    kis, code, cut_qty, prefer_market=True
                                )
                                holding[code]["qty"] -= int(cut_qty)
                                holding[code]["bear_s1_done"] = True
                                log_trade(
                                    {
                                        "datetime": now_str,
                                        "code": code,
                                        "name": None,
                                        "qty": int(cut_qty),
                                        "K": holding[code].get("k_value"),
                                        "target_price": holding[code].get(
                                            "target_price_src"
                                        ),
                                        "strategy": "기존보유 능동관리",
                                        "side": "SELL",
                                        "price": exec_px,
                                        "amount": int((exec_px or 0))
                                        * int(cut_qty),
                                        "result": result,
                                        "reason": "시장약세 1단계 축소(비타겟)",
                                    }
                                )
                                save_state(holding, traded)
                                time.sleep(RATE_SLEEP_SEC)

                            if (
                                regime["bear_stage"] >= 2
                                and not holding[code].get("bear_s2_done")
                            ):
                                cut_qty = max(
                                    1, int(holding[code]["qty"] * REG_PARTIAL_S2)
                                )
                                logger.info(
                                    f"[REGIME-REDUCE-S2/비타겟] {code} 약세2단계 {REG_PARTIAL_S2 * 100:.0f}% 축소 → {cut_qty}"
                                )
                                exec_px, result = _sell_once(
                                    kis, code, cut_qty, prefer_market=True
                                )
                                holding[code]["qty"] -= int(cut_qty)
                                holding[code]["bear_s2_done"] = True
                                log_trade(
                                    {
                                        "datetime": now_str,
                                        "code": code,
                                        "name": None,
                                        "qty": int(cut_qty),
                                        "K": holding[code].get("k_value"),
                                        "target_price": holding[code].get(
                                            "target_price_src"
                                        ),
                                        "strategy": "기존보유 능동관리",
                                        "side": "SELL",
                                        "price": exec_px,
                                        "amount": int((exec_px or 0))
                                        * int(cut_qty),
                                        "result": result,
                                        "reason": "시장약세 2단계 축소(비타겟)",
                                    }
                                )
                                save_state(holding, traded)
                                time.sleep(RATE_SLEEP_SEC)

                    # 트리거 기반 청산 평가/집행
                    sellable_here = ord_psbl_map.get(code, 0)
                    if sellable_here <= 0:
                        logger.info(
                            f"[SKIP-기존보유] {code}: 매도가능수량=0 (대기/체결중/락)"
                        )
                        continue

                    reason, exec_price, result, sold_qty = _adaptive_exit(
                        kis, code, holding[code], regime_mode=regime["mode"]
                    )
                    if reason:
                        trade_common = {
                            "datetime": now_str,
                            "code": code,
                            "name": None,
                            "qty": int(sold_qty or 0),
                            "K": holding[code].get("k_value"),
                            "target_price": holding[code].get("target_price_src"),
                            "strategy": "기존보유 능동관리",
                        }
                        _bp = (
                            float(holding[code].get("buy_price", 0.0))
                            if code in holding
                            else 0.0
                        )
                        _pnl_pct = (
                            (
                                (float(exec_price) - _bp)
                                / _bp
                            )
                            * 100.0
                            if (exec_price and _bp > 0)
                            else None
                        )
                        _profit = (
                            (
                                (float(exec_price) - _bp)
                                * int(sold_qty)
                            )
                            if (exec_price and _bp > 0 and sold_qty)
                            else None
                        )

                        log_trade(
                            {
                                **trade_common,
                                "side": "SELL",
                                "price": exec_price,
                                "amount": int((exec_price or 0))
                                * int(sold_qty or 0),
                                "result": result,
                                "reason": reason,
                                "pnl_pct": (
                                    _pnl_pct if _pnl_pct is not None else None
                                ),
                                "profit": (
                                    int(round(_profit))
                                    if _profit is not None
                                    else None
                                ),
                            }
                        )

                        save_state(holding, traded)
                        time.sleep(RATE_SLEEP_SEC)
                    else:
                        try:
                            if is_strong_momentum(kis, code):
                                logger.info(
                                    f"[모멘텀 강세] {code}: 강한 상승추세, 능동관리 매도 보류"
                                )
                                continue
                        except Exception as e:
                            logger.warning(
                                f"[SELL_GUARD_FAIL] {code} 모멘텀 평가 실패: {e}"
                            )

                    try:
                        momentum_intact, trend_ctx = _has_bullish_trend_structure(kis, code)
                    except NetTemporaryError:
                        logger.warning(
                            f"[20D_TREND_TEMP_SKIP] {code}: 네트워크 일시 실패 → 이번 루프 스킵"
                        )
                        continue
                    except DataEmptyError:
                        logger.warning(
                            f"[DATA_EMPTY] {code}: 0캔들 → 다음 루프에서 재확인"
                        )
                        continue
                    except DataShortError:
                        logger.error(
                            f"[DATA_SHORT] {code}: 21개 미만 → 이번 루프 판단 스킵"
                        )
                        continue

                    if momentum_intact:
                        logger.info(
                            (
                                f"[모멘텀 보유] {code}: 5/10/20 정배열 & 20일선 상승 & 종가>20일선 유지 "
                                f"(close={trend_ctx.get('last_close'):.2f}, ma5={trend_ctx.get('ma5'):.2f}, "
                                f"ma10={trend_ctx.get('ma10'):.2f}, ma20={trend_ctx.get('ma20'):.2f}→{trend_ctx.get('ma20_prev'):.2f})"
                            )
                        )
                        continue

            # --- 장중 커트오프(KST): 14:40 도달 시 "전량매도 없이" 리포트 생성 후 정상 종료 ---
            if is_open and now_dt_kst.time() >= SELL_FORCE_TIME:
                logger.info(
                    f"[⏰ 커트오프] {SELL_FORCE_TIME.strftime('%H:%M')} 도달: 전량 매도 없이 리포트 생성 후 종료"
                )

                save_state(holding, traded)

                try:
                    _report = ceo_report(datetime.now(KST), period="daily")
                    logger.info(
                        f"[📄 CEO Report 생성 완료] title={_report.get('title')}"
                    )
                except Exception as e:
                    logger.error(f"[CEO Report 생성 실패] {e}")

                logger.info("[✅ 커트오프 완료: 루프 정상 종료]")
                break

            save_state(holding, traded)
            time.sleep(loop_sleep_sec)

    except KeyboardInterrupt:
        logger.info("[🛑 수동 종료]")
    except Exception as e:
        logger.exception(f"[FATAL] 메인 루프 예외 발생: {e}")

# 실행부
if __name__ == "__main__":
    main()
