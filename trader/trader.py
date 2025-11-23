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
    "DAILY_CAPITAL": "50000000",
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
    # 기타
    "MARKET_DATA_WHEN_CLOSED": "false",
    "FORCE_WEEKLY_REBALANCE": "0",
}

def _cfg(key: str) -> str:
    """환경변수 > CONFIG 기본값"""
    return os.getenv(key, CONFIG.get(key, ""))

# RK-Max 유틸(가능하면 사용, 없으면 graceful fallback)
try:
    from .rkmax_utils import blend_k, recent_features, decide_position_limit, select_champions
except Exception:
    # rkmax_utils 임포트 실패 시, 보수적인 기본값 사용
    def blend_k(k_month: float, day: int, atr20: Optional[float], atr60: Optional[float]) -> float:
        return float(k_month) if k_month is not None else 0.5

    def recent_features(kis, code: str) -> Dict[str, Optional[float]]:
        return {"atr20": None, "atr60": None}

    def decide_position_limit(candidates):
        # 정보가 없을 때는 종목 1개만 가져가도록 안전하게 조정
        try:
            n = len(list(candidates or []))
        except Exception:
            n = 0
        if n <= 0:
            return 0
        return 1

    def select_champions(candidates, position_limit):
        # 임포트 실패 시에는 상위 N개를 그대로 사용 (스코어링 없음)
        arr = list(candidates or [])
        return arr[: max(0, position_limit or 0)]

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
SLIPPAGE_LIMIT_PCT = float(_cfg("SLIPPAGE_LIMIT_PCT"))
SLIPPAGE_ENTER_GUARD_PCT = float(_cfg("SLIPPAGE_ENTER_GUARD_PCT"))
VWAP_TOL = float(_cfg("VWAP_TOL"))  # 🔸 VWAP 허용 오차(예: 0.003 = -0.3%까지 허용)
W_MAX_ONE = float(_cfg("W_MAX_ONE"))
W_MIN_ONE = float(_cfg("W_MIN_ONE"))
REBALANCE_ANCHOR = _cfg("REBALANCE_ANCHOR")
WEEKLY_ANCHOR_REF = _cfg("WEEKLY_ANCHOR_REF").lower()
MOMENTUM_OVERRIDES_FORCE_SELL = _cfg("MOMENTUM_OVERRIDES_FORCE_SELL").lower() == "true"

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
        logger.info(f"[🎯 리밸런싱 종목]: {data.get('selected') or data.get('selected_stocks')}")
        return data.get("selected") or data.get("selected_stocks") or []
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

def _fetch_balances(kis: KisAPI) -> List[Dict[str, Any]]:
    if hasattr(kis, "get_balance_all"):
        res = _with_retry(kis.get_balance_all)
    else:
        res = _with_retry(kis.get_balance)
    if isinstance(res, dict):
        positions = res.get("positions") or res.get("output1") or []
        if not isinstance(positions, list):
            logger.error(f"[BAL_STD_FAIL] positions 타입 이상: {type(positions)}")
            return []
        return positions
    elif isinstance(res, list):
        return res
    else:
        logger.error(f"[BAL_STD_FAIL] 지원하지 않는 반환 타입: {type(res)}")
        return []

from .kis_wrapper import NetTemporaryError, DataEmptyError, DataShortError

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
            candles = kis.get_daily_candles(code, count=21)
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
    try:
        if not kis.is_market_open() and not ALLOW_WHEN_CLOSED:
            return False
    except Exception:
        pass

    try:
        candles = kis.get_daily_candles(code, count=121)
        closes = [float(x['close']) for x in candles if 'close' in x and x['close'] is not None and float(x['close']) > 0]
        if len(closes) < 61:
            return False
        today = closes[-1]
        ma20 = sum(closes[-20:]) / 20
        ma60 = sum(closes[-60:]) / 60
        ma120 = sum(closes[-120:]) / 120 if len(closes) >= 120 else ma60
        r20 = (today - closes[-21]) / closes[-21] * 100 if len(closes) > 21 else 0
        r60 = (today - closes[-61]) / closes[-61] * 100 if len(closes) > 61 else 0
        r120 = (today - closes[0]) / closes[0] * 100 if len(closes) >= 120 else r60
        if r20 > 10 or r60 > 10 or r120 > 10:
            return True
        if today > ma20 or today > ma60 or today > ma120:
            return True
        return False
    except Exception as e:
        logger.warning(f"[모멘텀 판별 실패] {code}: {e}")
        return False

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
    }

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
            prev_candles = kis.get_daily_candles(code, count=2)
            if prev_candles and len(prev_candles) >= 2:
                prev = prev_candles[-2]
                prev_high = _to_float(prev.get("high"))
                prev_low  = _to_float(prev.get("low"))
    except Exception:
        pass

    if prev_high is None or prev_low is None:
        try:
            prev_candles = kis.get_daily_candles(code, count=2)
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
    "pct_change": None          # 등락률(%)
}

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
    """
    코스닥 지수 기반 레짐 판정 및 상태 업데이트.
    """
    if not REGIME_ENABLED:
        return REGIME_STATE

    snap = _get_kosdaq_snapshot(kis)
    now = datetime.now(KST)
    REGIME_STATE["last_snapshot_ts"] = now
    REGIME_STATE["prev_close"] = snap.get("prev_close")
    REGIME_STATE["pct_change"] = snap.get("pct_change")

    px = snap.get("price")
    if px is not None:
        if REGIME_STATE["session_high"] is None:
            REGIME_STATE["session_high"] = px
        else:
            REGIME_STATE["session_high"] = max(REGIME_STATE["session_high"], px)

    if snap.get("above_vwap") is True:
        if REGIME_STATE["last_above_vwap_ts"] is None:
            REGIME_STATE["last_above_vwap_ts"] = now
        REGIME_STATE["last_below_vwap_ts"] = None
    elif snap.get("above_vwap") is False:
        if REGIME_STATE["last_below_vwap_ts"] is None:
            REGIME_STATE["last_below_vwap_ts"] = now
        REGIME_STATE["last_above_vwap_ts"] = None

    # 강세 조건: +0.5% 이상 & VWAP 상방 10분 이상
    bull_ok = False
    try:
        if (snap.get("pct_change") is not None and snap["pct_change"] >= REG_BULL_MIN_UP_PCT):
            if REGIME_STATE["last_above_vwap_ts"]:
                mins = (now - REGIME_STATE["last_above_vwap_ts"]).total_seconds() / 60.0
                bull_ok = mins >= REG_BULL_MIN_MINUTES
    except Exception:
        bull_ok = False

    # 약세 조건: VWAP 하방 10분 이상 or 당일고점 대비 -0.7% 이상
    bear_ok = False
    drop_ok = False
    try:
        below_ok = False
        if REGIME_STATE["last_below_vwap_ts"]:
            mins_below = (now - REGIME_STATE["last_below_vwap_ts"]).total_seconds() / 60.0
            below_ok = mins_below >= REG_BEAR_VWAP_MINUTES

        if px is not None and REGIME_STATE["session_high"]:
            drop_ok = (REGIME_STATE["session_high"] - px) / REGIME_STATE["session_high"] * 100.0 >= REG_BEAR_DROP_FROM_HIGH

        bear_ok = below_ok or drop_ok
    except Exception:
        bear_ok = False

    new_mode = REGIME_STATE["mode"]
    if bear_ok:
        if new_mode != "bear":
            REGIME_STATE["mode"] = "bear"
            REGIME_STATE["since"] = now
            REGIME_STATE["bear_stage"] = 0
        else:
            mins_bear = (now - (REGIME_STATE["since"] or now)).total_seconds() / 60.0
            if REGIME_STATE["bear_stage"] < 1 and mins_bear >= REG_BEAR_STAGE1_MINUTES:
                REGIME_STATE["bear_stage"] = 1
            if REGIME_STATE["bear_stage"] >= 1 and drop_ok:
                REGIME_STATE["bear_stage"] = 2
    elif bull_ok:
        REGIME_STATE["mode"] = "bull"
        if new_mode != "bull":
            REGIME_STATE["since"] = now
            REGIME_STATE["bear_stage"] = 0
    else:
        REGIME_STATE["mode"] = "neutral"
        if new_mode != "neutral":
            REGIME_STATE["since"] = now
            REGIME_STATE["bear_stage"] = 0

    return REGIME_STATE

# === 매도 로직 ===
def _force_sell_pass(kis: KisAPI, targets_codes: set, reason: str, prefer_market=True) -> set:
    if not targets_codes:
        return set()
    targets_codes = {c for c in targets_codes if c}
    balances = _fetch_balances(kis)
    qty_map = {b.get("pdno"): _to_int(b.get("hldg_qty", 0)) for b in balances}
    sellable_map = {b.get("pdno"): _to_int(b.get("ord_psbl_qty", 0)) for b in balances}
    avg_price_map = {b.get("pdno"): _to_float(b.get("pchs_avg_pric") or b.get("avg_price") or 0.0, 0.0) for b in balances}

    remaining = set()
    for code in list(targets_codes):
        qty = qty_map.get(code, 0)
        sellable = sellable_map.get(code, 0)
        if qty <= 0:
            logger.info(f"[스킵] {code}: 실제 잔고 수량 0")
            continue
        if sellable <= 0:
            logger.info(f"[스킵] {code}: 매도가능수량=0 (대기/체결중/락) → 이번 패스 보류")
            remaining.add(code)
            continue

        if MOMENTUM_OVERRIDES_FORCE_SELL and is_strong_momentum(kis, code):
            logger.info(f"[모멘텀 강세] {code}: 강한 상승추세, 강제매도 제외 (policy=true)")
            continue

        try:
            return_pct = get_20d_return_pct(kis, code)
            logger.info(f"[모멘텀 수익률 체크] {code}: 최근 20일 수익률 {return_pct if return_pct is not None else 'N/A'}%")
        except NetTemporaryError:
            logger.warning(f"[20D_RETURN_TEMP_SKIP] {code}: 네트워크 일시 실패 → 이번 패스 스킵")
            remaining.add(code)
            continue
        except DataEmptyError:
            logger.warning(f"[DATA_EMPTY] {code}: 0캔들 감지 → 다음 루프에서 재확인")
            remaining.add(code)
            continue
        except DataShortError:
            logger.error(f"[DATA_SHORT] {code}: 21개 미만 → 강제매도 판단 스킵")
            remaining.add(code)
            continue

        if return_pct is not None and return_pct >= 3.0:
            logger.info(f"[모멘텀 보유 유지] {code}: 최근 20일 수익률 {return_pct:.2f}% >= 3% → 강제매도 제외")
            continue
        else:
            logger.info(f"[매도진행] {code}: 최근 20일 수익률 {return_pct if return_pct is not None else 'N/A'}% < 3% → 강제매도")

        try:
            sell_qty = min(qty, sellable) if sellable > 0 else qty
            cur_price, result = _sell_once(kis, code, sell_qty, prefer_market=prefer_market)
            buy_px_for_pnl = avg_price_map.get(code) or None
            if buy_px_for_pnl:
                _log_realized_pnl(code, cur_price, sell_qty, buy_px_for_pnl)

            log_trade({
                "datetime": datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S"),
                "code": code, "name": None, "qty": sell_qty, "K": None,
                "target_price": None, "strategy": "강제전량매도",
                "side": "SELL", "price": cur_price if cur_price is not None else 0,
                "amount": (_to_int(cur_price, 0) * int(sell_qty)) if cur_price is not None else 0,
                "result": result,
                "pnl_pct": (((float(cur_price) - float(buy_px_for_pnl)) / float(buy_px_for_pnl) * 100.0) if (cur_price is not None and buy_px_for_pnl) else None),
                "profit": (int(round((float(cur_price) - float(buy_px_for_pnl)) * int(sell_qty))) if (cur_price is not None and buy_px_for_pnl) else None),
                "reason": reason
            })
        finally:
            time.sleep(RATE_SLEEP_SEC)

    balances_after = _fetch_balances(kis)
    after_qty_map = {b.get("pdno"): _to_int(b.get("hldg_qty", 0)) for b in balances_after}
    for code in targets_codes:
        if after_qty_map.get(code, 0) > 0:
            remaining.add(code)
    return remaining

def _force_sell_all(kis: KisAPI, holding: dict, reason: str, passes: int, include_all_balances: bool, prefer_market=True) -> None:
    target_codes = set([c for c in holding.keys() if c])
    if include_all_balances:
        try:
            balances = _fetch_balances(kis)
            for b in balances:
                code = b.get("pdno")
                if code and _to_int(b.get("hldg_qty", 0)) > 0:
                    target_codes.add(code)
        except Exception as e:
            logger.error(f"[잔고조회 오류: 전체포함 불가] {e}")
    if not target_codes:
        logger.info("[강제전량매도] 대상 종목 없음")
        return
    logger.info(f"[⚠️ 강제전량매도] 사유: {reason} / 대상 종목수: {len(target_codes)} / 전체잔고포함={include_all_balances}")
    remaining = target_codes
    for p in range(1, max(1, passes) + 1):
        logger.info(f"[강제전량매도 PASS {p}/{passes}] 대상 {len(remaining)}종목 시도")
        remaining = _force_sell_pass(kis, remaining, reason=reason, prefer_market=prefer_market)
        if not remaining:
            logger.info("[강제전량매도] 모든 종목 매도 완료")
            break
    if remaining:
        logger.error(f"[강제전량매도] 미매도 잔여 {len(remaining)}종목: {sorted(list(remaining))}")
    for code in list(holding.keys()):
        holding.pop(code, None)
    save_state(holding, {})

# === [ANCHOR: EXIT] 동적 트레일링/TP + 손절 ===
def _adaptive_exit(
    kis: KisAPI, code: str, pos: Dict[str, Any], regime_mode: str = "neutral"
) -> Tuple[Optional[str], Optional[float], Optional[Any], Optional[int]]:
    """
    레짐(강세/약세/중립)에 따라 TP/트레일링을 동적으로 적용하고, 체결/로그를 남김
    """
    now = datetime.now(KST)
    reason = None
    exec_px, result, sold_qty = None, None, None
    trade_logged = False
    try:
        cur = _safe_get_price(kis, code)
        if cur is None:
            logger.warning(f"[EXIT-FAIL] {code} 현재가 조회 실패")
            return None, None, None, None
    except Exception as e:
        logger.error(f"[EXIT-FAIL] {code} 현재가 조회 예외: {e}")
        return None, None, None, None

    # 최고가(high) 갱신
    pos['high'] = max(float(pos.get('high', cur)), float(cur))
    qty = _to_int(pos.get('qty'), 0)
    if qty <= 0:
        logger.warning(f"[EXIT-FAIL] {code} qty<=0")
        return None, None, None, None

    buy_price = float(pos.get('buy_price', 0.0))
    max_price = pos.get('high', buy_price)
    slippage = None

    # === 레짐 기반 임계값 ===
    tp_profit_pct = TP_PROFIT_PCT_BASE               # 기본 3.0%
    trail_down_frac = 0.015                          # 기본 1.5% 되돌림
    if regime_mode == "bull":
        tp_profit_pct = TP_PROFIT_PCT_BULL           # 예: 3.5%
        trail_down_frac = TRAIL_PCT_BULL             # 예: 2.5%
    elif regime_mode == "bear":
        trail_down_frac = TRAIL_PCT_BEAR             # 예: 1.2%

    # === 익절(동적) ===
    if cur >= buy_price * (1 + tp_profit_pct / 100.0):
        reason = f"익절 {tp_profit_pct:.1f}%"
    # === 트레일링스톱(최고가 4% 돌파 후 동적 되돌림) ===
    elif max_price >= buy_price * 1.04 and cur <= max_price * (1 - trail_down_frac):
        reason = f"트레일링스톱({int(trail_down_frac*100)}%)"
    # === 손절(-5%) ===
    elif cur <= float(pos['buy_price']) * (1 + DEFAULT_LOSS_PCT / 100.0):
        reason = "손절 -5%"

    if reason:
        try:
            exec_px, result = _sell_once(kis, code, qty, prefer_market=True)
            sold_qty = qty
            if exec_px and buy_price > 0:
                slippage = (exec_px - buy_price) / buy_price * 100.0
            else:
                slippage = None

            _log_realized_pnl(code, exec_px, qty, buy_price, reason=reason)
            logger.info(f"[SELL-TRIGGER] {code} REASON={reason} qty={qty} price={exec_px}")

            log_trade({
                "datetime": now.strftime("%Y-%m-%d %H:%M:%S"),
                "code": code,
                "side": "SELL",
                "reason": reason,
                "order_price": buy_price,
                "fill_price": exec_px,
                "slippage_pct": round(slippage, 2) if slippage is not None else None,
                "qty": sold_qty,
                "result": result,
                "status": "filled" if result and result.get("rt_cd") == "0" else "failed",
                "fail_reason": None if result and result.get("rt_cd") == "0" else "체결실패"
            })
            trade_logged = True
        except Exception as e:
            logger.error(f"[SELL-FAIL] {code} qty={qty} err={e}")
            if not trade_logged:
                log_trade({
                    "datetime": now.strftime("%Y-%m-%d %H:%M:%S"),
                    "code": code,
                    "side": "SELL",
                    "reason": reason,
                    "order_price": buy_price,
                    "fill_price": None,
                    "slippage_pct": None,
                    "qty": qty,
                    "result": None,
                    "status": "failed",
                    "fail_reason": str(e)
                })
            return None, None, None, None

        return reason, exec_px, result, sold_qty

    return None, None, None, None

# ====== 메인 진입부 및 실전 rolling_k 루프 ======
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


    # [CHAMPION MODE] 오늘 가져갈 종목 수 결정 + 챔피언 종목만 선별
    try:
        position_limit = decide_position_limit(targets)
    except Exception:
        logger.exception("[CHAMPION] decide_position_limit 실패 → 기본값 2개 사용")
        position_limit = 2

    if position_limit <= 0:
        logger.info("[CHAMPION] position_limit<=0 → 오늘은 신규 매수 없음 (targets=%s)", len(targets))
        targets = []
    else:
        if targets:
            logger.info(
                "[CHAMPION] candidates=%s → position_limit=%s, 챔피언 선별 시작",
                len(targets),
                position_limit,
            )
            targets = select_champions(targets, position_limit)
            logger.info(
                "[CHAMPION] 최종 챔피언 종목: %s",
                [
                    (t.get("stock_code") or t.get("code"), t.get("champ_score"), t.get("champ_rank"))
                    for t in targets
                ],
            )

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
            "prev_open": t.get("prev_open"),
            "prev_high": t.get("prev_high"),
            "prev_low": t.get("prev_low"),
            "prev_close": t.get("prev_close"),
            "prev_volume": t.get("prev_volume"),
        }

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
        pct_change = float((regime_snapshot or {}).get("pct_change") or 0.0)
    except Exception as e:
        logger.warning("[REBALANCE] 레짐 스냅샷 계산 실패, neutral로 대체: %s", e)
        mode = "neutral"
        pct_change = 0.0

    # 레짐 기반 신규 편입 상한
    if mode == "bear":
        max_new = 1
    else:
        # neutral / bull 모두 2개까지 허용 (향후 pct_change 구간별로 더 쪼갤 수 있음)
        max_new = 2

    scored: List[Tuple[str, float, bool]] = []

    for code, info in processed_targets.items():
        # 20일 수익률을 기본 점수로 사용 (rolling K 백테스트 결과와 결을 맞추기 위함)
        try:
            ret_20d = _get_20d_return(kis, code)
        except Exception:
            ret_20d = 0.0

        # 단기 모멘텀 강세 여부 (is_strong_momentum)로 버킷 구분
        try:
            strong = is_strong_momentum(kis, code)
        except Exception as e:
            logger.warning("[REBALANCE] 모멘텀 판별 실패 %s: %s", code, e)
            strong = False

        scored.append((code, ret_20d, strong))

    # 모멘텀 strong 버킷 우선, 그 다음 나머지 중에서 점수 순으로 채우기
    strong_bucket = [x for x in scored if x[2]]
    weak_bucket = [x for x in scored if not x[2]]

    strong_bucket.sort(key=lambda x: x[1], reverse=True)
    weak_bucket.sort(key=lambda x: x[1], reverse=True)

    picked: List[str] = []

    for code, score, _ in strong_bucket:
        if len(picked) >= max_new:
            break
        picked.append(code)

    for code, score, _ in weak_bucket:
        if len(picked) >= max_new:
            break
        picked.append(code)

    for code in picked:
        selected_targets[code] = processed_targets[code]

    logger.info(
        "[REBALANCE] 레짐=%s pct=%.2f%%, 후보 %d개 중 상위 %d종목만 선택: %s",
        mode,
        pct_change,
        len(processed_targets),
        len(selected_targets),
        ",".join(selected_targets.keys()),
    )

    code_to_target: Dict[str, Any] = selected_targets


    loop_sleep_sec = 2.5

    try:
        while True:
            # === 코스닥 레짐 업데이트 ===
            regime = _update_market_regime(kis)
            pct_txt = f"{regime.get('pct_change'):.2f}%" if regime.get('pct_change') is not None else "N/A"
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
                    code_b = stock.get('pdno')
                    name_b = stock.get('prdt_name')
                    name_map[code_b] = name_b
                    logger.debug(" [잔고] 종목:%s, 코드:%s, 보유:%s, 매도가능:%s",
                                 name_b, code_b, stock.get('hldg_qty'), stock.get('ord_psbl_qty'))

                current_holding = {b['pdno']: _to_int(b.get('hldg_qty', 0)) for b in balances if _to_int(b.get('hldg_qty', 0)) > 0}
                ord_psbl_map = {b['pdno']: _to_int(b.get('ord_psbl_qty', 0)) for b in balances}

                # 신규 보유분을 능동관리 대상으로 자동 초기화
                for b in balances:
                    code_b = str(b.get('pdno', '')).strip()
                    qty_b  = _to_int(b.get('hldg_qty', 0))
                    avg_b  = _to_float(b.get('pchs_avg_pric') or b.get('avg_price') or 0.0, 0.0)

                    if qty_b > 0 and code_b and code_b not in holding and (avg_b is not None) and avg_b > 0:
                        _init_position_state_from_balance(kis, holding, code_b, float(avg_b), int(qty_b))
                        logger.info(f"[잔고초기화] code={code_b} qty={qty_b} avg={avg_b}")

                # 실제 잔고에서 사라진 보유항목은 정리
                for code in list(holding.keys()):
                    if code not in current_holding or current_holding[code] == 0:
                        logger.info(f"[보유종목 해제] {code} : 실제잔고 없음 → holding 제거")
                        holding.pop(code, None)

            except Exception as e:
                logger.error(f"[잔고조회 오류]{e}")

            # 마감 상태: 캔들/ATR/모멘텀/매매 로직 스킵
            if not is_open:
                logger.info("[마감상태] 캔들/ATR/모멘텀/매매 로직 스킵 → 잔고만 동기화 후 대기")
                save_state(holding, traded)
                time.sleep(60.0)
                continue

            # ====== 매수/매도(전략) LOOP — 오늘의 타겟 ======
            for code, target in code_to_target.items():
                prev_volume = _to_float(target.get("prev_volume"))
                prev_open   = _to_float(target.get("prev_open"))
                prev_close  = _to_float(target.get("prev_close"))
                logger.debug(f"[prev_volume 체크] {code} 거래량:{prev_volume}, 전일시가:{prev_open}, 전일종가:{prev_close}")

                qty = _to_int(target.get("매수수량") or target.get("qty"), 0)
                if qty <= 0:
                    logger.info(f"[SKIP] {code}: 매수수량 없음/0")
                    continue

                k_value = (target.get("best_k") or target.get("K") or target.get("k"))
                _ = None if k_value is None else _to_float(k_value)

                eff_target_price, k_used = compute_entry_target(kis, target)
                strategy = target.get("strategy") or "전월 rolling K 최적화"
                name = target.get("name") or target.get("종목명") or name_map.get(code)

                try:
                    current_price = _safe_get_price(kis, code)
                    logger.info(f"[📈 현재가] {code}: {current_price}")

                    trade_common_buy = {
                        "datetime": now_str,
                        "code": code,
                        "name": name,
                        "qty": qty,
                        "K": k_value if k_value is not None else k_used,
                        "target_price": eff_target_price,
                        "strategy": strategy,
                    }

                    # --- 매수 --- (돌파 진입 + 슬리피지 가드 + 예산 가드)
                    if is_open and code not in holding and code not in traded:
                        if not can_buy:
                            logger.info(f"[BUDGET_SKIP] {code}: 예산 없음 → 신규 매수 스킵")
                            continue

                        enter_cond = (
                            current_price is not None and
                            eff_target_price is not None and
                            int(current_price) >= int(eff_target_price)
                        )

                        if enter_cond:
                            guard_ok = True

                            # 1) 진입 슬리피지 가드 (기존)
                            if eff_target_price and eff_target_price > 0 and current_price is not None:
                                slip_pct = ((float(current_price) - float(eff_target_price)) / float(eff_target_price)) * 100.0
                                if slip_pct > SLIPPAGE_ENTER_GUARD_PCT:
                                    guard_ok = False
                                    logger.info(
                                        f"[ENTER-GUARD] {code} 진입슬리피지 {slip_pct:.2f}% > "
                                        f"{SLIPPAGE_ENTER_GUARD_PCT:.2f}% → 진입 스킵"
                                    )

                            # 2) VWAP 가드: 현재가가 VWAP*(1 - tol) 이상인지 체크
                            if guard_ok and current_price is not None:
                                vwap_val = kis.get_vwap_today(code)
                                if vwap_val is None:
                                    logger.info(f"[VWAP-SKIP] {code}: VWAP 데이터 없음 → VWAP 가드 생략")
                                else:
                                    if not vwap_guard(float(current_price), float(vwap_val), VWAP_TOL):
                                        guard_ok = False
                                        logger.info(
                                            f"[VWAP-GUARD] {code}: 현재가({current_price}) < VWAP*(1 - {VWAP_TOL:.4f}) "
                                            f"→ 진입 스킵 (VWAP={vwap_val:.2f})"
                                        )

                            if not guard_ok:
                                continue

                            result = place_buy_with_fallback(kis, code, qty, limit_price=int(eff_target_price))

                            # fills에 name 채우기 시도
                            try:
                                if isinstance(result, dict) and result.get("rt_cd") == "0":
                                    out = result.get("output") or {}
                                    odno = out.get("ODNO") or out.get("ord_no") or out.get("order_no") or ""
                                    ensure_fill_has_name(odno=odno, code=code, name=name or "", qty=qty, price=current_price or 0.0)
                            except Exception as e:
                                logger.warning(f"[BUY_FILL_NAME_FAIL] code={code} ex={e}")

                            _init_position_state(kis, holding, code, float(current_price), int(qty),
                                                 (k_value if k_value is not None else k_used), eff_target_price)

                            traded[code] = {"buy_time": now_str, "qty": int(qty), "price": float(current_price)}
                            logger.info(f"[✅ 매수주문] {code}, qty={qty}, price={current_price}, result={result}")

                            log_trade({
                                **trade_common_buy,
                                "side": "BUY",
                                "price": current_price,
                                "amount": int(current_price) * int(qty),
                                "result": result
                            })
                            save_state(holding, traded)
                            time.sleep(RATE_SLEEP_SEC)
                        else:
                            logger.info(f"[SKIP] {code}: 현재가({current_price}) < 목표가({eff_target_price}), 미매수")
                            continue

                    # --- 실전형 청산 (타겟 보유포지션) ---
                    if is_open and code in holding:
                        # (약세 레짐) 단계적 축소
                        if regime["mode"] == "bear":
                            sellable_here = ord_psbl_map.get(code, 0)
                            if sellable_here > 0:
                                if regime["bear_stage"] >= 1 and not holding[code].get("bear_s1_done"):
                                    cut_qty = max(1, int(holding[code]['qty'] * REG_PARTIAL_S1))
                                    logger.info(f"[REGIME-REDUCE-S1] {code} 약세1단계 {REG_PARTIAL_S1*100:.0f}% 축소 → {cut_qty}")
                                    exec_px, result = _sell_once(kis, code, cut_qty, prefer_market=True)
                                    holding[code]['qty'] -= int(cut_qty)
                                    holding[code]['bear_s1_done'] = True
                                    log_trade({
                                        "datetime": now_str, "code": code, "name": name, "qty": int(cut_qty),
                                        "K": k_value if k_value is not None else k_used, "target_price": eff_target_price,
                                        "strategy": strategy, "side": "SELL", "price": exec_px,
                                        "amount": int((exec_px or 0)) * int(cut_qty),
                                        "result": result, "reason": "시장약세 1단계 축소"
                                    })
                                    save_state(holding, traded)
                                    time.sleep(RATE_SLEEP_SEC)

                                if regime["bear_stage"] >= 2 and not holding[code].get("bear_s2_done"):
                                    cut_qty = max(1, int(holding[code]['qty'] * REG_PARTIAL_S2))
                                    logger.info(f"[REGIME-REDUCE-S2] {code} 약세2단계 {REG_PARTIAL_S2*100:.0f}% 축소 → {cut_qty}")
                                    exec_px, result = _sell_once(kis, code, cut_qty, prefer_market=True)
                                    holding[code]['qty'] -= int(cut_qty)
                                    holding[code]['bear_s2_done'] = True
                                    log_trade({
                                        "datetime": now_str, "code": code, "name": name, "qty": int(cut_qty),
                                        "K": k_value if k_value is not None else k_used, "target_price": eff_target_price,
                                        "strategy": strategy, "side": "SELL", "price": exec_px,
                                        "amount": int((exec_px or 0)) * int(cut_qty),
                                        "result": result, "reason": "시장약세 2단계 축소"
                                    })
                                    save_state(holding, traded)
                                    time.sleep(RATE_SLEEP_SEC)

                        # 먼저 트리거 기반 청산 평가/집행
                        sellable_here = ord_psbl_map.get(code, 0)
                        if sellable_here <= 0:
                            logger.info(f"[SKIP] {code}: 매도가능수량=0 (대기/체결중/락) → 매도 보류")
                        else:
                            reason, exec_price, result, sold_qty = _adaptive_exit(kis, code, holding[code], regime_mode=regime["mode"])
                            if reason:
                                trade_common_sell = {
                                    "datetime": now_str,
                                    "code": code,
                                    "name": name,
                                    "qty": int(sold_qty or 0),
                                    "K": k_value if k_value is not None else k_used,
                                    "target_price": eff_target_price,
                                    "strategy": strategy,
                                }
                                _bp = float(holding[code].get("buy_price", 0.0)) if code in holding else 0.0
                                _pnl_pct = (((float(exec_price) - _bp) / _bp) * 100.0) if (exec_price and _bp > 0) else None
                                _profit  = (((float(exec_price) - _bp) * int(sold_qty)) if (exec_price and _bp > 0 and sold_qty) else None)
                                log_trade({
                                    **trade_common_sell,
                                    "side": "SELL",
                                    "price": exec_price,
                                    "amount": int((exec_price or 0)) * int(sold_qty or 0),
                                    "result": result,
                                    "pnl_pct": (_pnl_pct if _pnl_pct is not None else None),
                                    "profit": (int(round(_profit)) if _profit is not None else None),
                                    "reason": reason
                                })
                                save_state(holding, traded)
                                time.sleep(RATE_SLEEP_SEC)
                            else:
                                try:
                                    if is_strong_momentum(kis, code):
                                        logger.info(f"[SELL_GUARD] {code} 모멘텀 강세 → 트리거 부재, 매도 보류")
                                except Exception as e:
                                    logger.warning(f"[SELL_GUARD_FAIL] {code} 모멘텀 평가 실패: {e}")

                except Exception as e:
                    logger.error(f"[❌ 주문/조회 실패] {code} : {e}")
                    continue

            # ====== (A) 비타겟 보유분도 장중 능동관리 ======
            if is_open:
                for code in list(holding.keys()):
                    if code in code_to_target:
                        continue  # 위 루프에서 이미 처리

                    # 약세 단계 축소(비타겟)
                    if regime["mode"] == "bear":
                        sellable_here = ord_psbl_map.get(code, 0)
                        if sellable_here > 0:
                            if regime["bear_stage"] >= 1 and not holding[code].get("bear_s1_done"):
                                cut_qty = max(1, int(holding[code]['qty'] * REG_PARTIAL_S1))
                                logger.info(f"[REGIME-REDUCE-S1/비타겟] {code} 약세1단계 {REG_PARTIAL_S1*100:.0f}% 축소 → {cut_qty}")
                                exec_px, result = _sell_once(kis, code, cut_qty, prefer_market=True)
                                holding[code]['qty'] -= int(cut_qty)
                                holding[code]['bear_s1_done'] = True
                                log_trade({
                                    "datetime": now_str, "code": code, "name": None, "qty": int(cut_qty),
                                    "K": holding[code].get("k_value"), "target_price": holding[code].get("target_price_src"),
                                    "strategy": "기존보유 능동관리", "side": "SELL", "price": exec_px,
                                    "amount": int((exec_px or 0)) * int(cut_qty),
                                    "result": result, "reason": "시장약세 1단계 축소(비타겟)"
                                })
                                save_state(holding, traded)
                                time.sleep(RATE_SLEEP_SEC)

                            if regime["bear_stage"] >= 2 and not holding[code].get("bear_s2_done"):
                                cut_qty = max(1, int(holding[code]['qty'] * REG_PARTIAL_S2))
                                logger.info(f"[REGIME-REDUCE-S2/비타겟] {code} 약세2단계 {REG_PARTIAL_S2*100:.0f}% 축소 → {cut_qty}")
                                exec_px, result = _sell_once(kis, code, cut_qty, prefer_market=True)
                                holding[code]['qty'] -= int(cut_qty)
                                holding[code]['bear_s2_done'] = True
                                log_trade({
                                    "datetime": now_str, "code": code, "name": None, "qty": int(cut_qty),
                                    "K": holding[code].get("k_value"), "target_price": holding[code].get("target_price_src"),
                                    "strategy": "기존보유 능동관리", "side": "SELL", "price": exec_px,
                                    "amount": int((exec_px or 0)) * int(cut_qty),
                                    "result": result, "reason": "시장약세 2단계 축소(비타겟)"
                                })
                                save_state(holding, traded)
                                time.sleep(RATE_SLEEP_SEC)

                    # 트리거 기반 청산 평가/집행
                    sellable_here = ord_psbl_map.get(code, 0)
                    if sellable_here <= 0:
                        logger.info(f"[SKIP-기존보유] {code}: 매도가능수량=0 (대기/체결중/락)")
                        continue

                    reason, exec_price, result, sold_qty = _adaptive_exit(kis, code, holding[code], regime_mode=regime["mode"])
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
                        _bp = float(holding[code].get("buy_price", 0.0)) if code in holding else 0.0
                        _pnl_pct = (((float(exec_price) - _bp) / _bp) * 100.0) if (exec_price and _bp > 0) else None
                        _profit  = (((float(exec_price) - _bp) * int(sold_qty)) if (exec_price and _bp > 0 and sold_qty) else None)

                        log_trade({
                            **trade_common,
                            "side": "SELL",
                            "price": exec_price,
                            "amount": int((exec_price or 0)) * int(sold_qty or 0),
                            "result": result,
                            "reason": reason,
                            "pnl_pct": (_pnl_pct if _pnl_pct is not None else None),
                            "profit": (int(round(_profit)) if _profit is not None else None)
                        })

                        save_state(holding, traded)
                        time.sleep(RATE_SLEEP_SEC)
                    else:
                        try:
                            if is_strong_momentum(kis, code):
                                logger.info(f"[모멘텀 강세] {code}: 강한 상승추세, 능동관리 매도 보류")
                                continue
                        except Exception as e:
                            logger.warning(f"[SELL_GUARD_FAIL] {code} 모멘텀 평가 실패: {e}")

                        try:
                            return_pct = get_20d_return_pct(kis, code)
                        except NetTemporaryError:
                            logger.warning(f"[20D_RETURN_TEMP_SKIP] {code}: 네트워크 일시 실패 → 이번 루프 스킵")
                            continue
                        except DataEmptyError:
                            logger.warning(f"[DATA_EMPTY] {code}: 0캔들 → 다음 루프에서 재확인")
                            continue
                        except DataShortError:
                            logger.error(f"[DATA_SHORT] {code}: 21개 미만 → 이번 루프 판단 스킵")
                            continue

                        if return_pct is not None and return_pct >= 3.0:
                            logger.info(f"[모멘텀 보유] {code}: 최근 20일 수익률 {return_pct:.2f}% >= 3% → 보유 지속")
                            continue

            # --- 장중 커트오프(KST): 14:40 도달 시 "전량매도 없이" 리포트 생성 후 정상 종료 ---
            if is_open and now_dt_kst.time() >= SELL_FORCE_TIME:
                logger.info(f"[⏰ 커트오프] {SELL_FORCE_TIME.strftime('%H:%M')} 도달: 전량 매도 없이 리포트 생성 후 종료")

                save_state(holding, traded)

                try:
                    _report = ceo_report(datetime.now(KST), period="daily")
                    logger.info(f"[📄 CEO Report 생성 완료] title={_report.get('title')}")
                except Exception as e:
                    logger.error(f"[CEO Report 생성 실패] {e}")

                logger.info("[✅ 커트오프 완료: 루프 정상 종료]")
                break

            save_state(holding, traded)
            time.sleep(loop_sleep_sec)

    except KeyboardInterrupt:
        logger.info("[🛑 수동 종료]")

# 실행부
if __name__ == "__main__":
    main()
