# trader.py [1/3] (라인 1~약 350)
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
from typing import Optional, Dict, Any, Tuple
import csv

# RK-Max 유틸(가능하면 사용, 없으면 graceful fallback)
try:
    from .rkmax_utils import blend_k, recent_features
except Exception:
    def blend_k(k_month: float, day: int, atr20: Optional[float], atr60: Optional[float]) -> float:
        return float(k_month) if k_month is not None else 0.5
    def recent_features(kis, code: str) -> Dict[str, Optional[float]]:
        return {"atr20": None, "atr60": None}

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)
STATE_FILE = Path(__file__).parent / "trade_state.json"

KST = ZoneInfo("Asia/Seoul")
SELL_FORCE_TIME_STR = os.getenv("SELL_FORCE_TIME", "14:40").strip()
SELL_ALL_BALANCES_AT_CUTOFF = os.getenv("SELL_ALL_BALANCES_AT_CUTOFF", "false").lower() == "true"
RATE_SLEEP_SEC = float(os.getenv("API_RATE_SLEEP_SEC", "0.5"))
FORCE_SELL_PASSES_CUTOFF = int(os.getenv("FORCE_SELL_PASSES_CUTOFF", "2"))
FORCE_SELL_PASSES_CLOSE = int(os.getenv("FORCE_SELL_PASSES_CLOSE", "4"))
PARTIAL1 = float(os.getenv("PARTIAL1", "0.5"))
PARTIAL2 = float(os.getenv("PARTIAL2", "0.3"))
TRAIL_PCT = float(os.getenv("TRAIL_PCT", "0.02"))
FAST_STOP = float(os.getenv("FAST_STOP", "0.01"))
ATR_STOP = float(os.getenv("ATR_STOP", "1.5"))
TIME_STOP_HHMM = os.getenv("TIME_STOP_HHMM", "13:00")
DEFAULT_PROFIT_PCT = float(os.getenv("DEFAULT_PROFIT_PCT", "3.0"))
DEFAULT_LOSS_PCT = float(os.getenv("DEFAULT_LOSS_PCT", "-2.0"))
DAILY_CAPITAL = int(os.getenv("DAILY_CAPITAL", "3000000"))
SLIPPAGE_LIMIT_PCT = float(os.getenv("SLIPPAGE_LIMIT_PCT", "0.15"))
SLIPPAGE_ENTER_GUARD_PCT = float(os.getenv("SLIPPAGE_ENTER_GUARD_PCT", "1.5"))
W_MAX_ONE = float(os.getenv("W_MAX_ONE", "0.25"))
W_MIN_ONE = float(os.getenv("W_MIN_ONE", "0.03"))
REBALANCE_ANCHOR = os.getenv("REBALANCE_ANCHOR", "first").lower().strip()
MOMENTUM_OVERRIDES_FORCE_SELL = os.getenv("MOMENTUM_OVERRIDES_FORCE_SELL", "true").lower() == "true"


def _parse_hhmm(hhmm: str) -> dtime:
    try:
        hh, mm = hhmm.split(":")
        return dtime(hour=int(hh), minute=int(mm))
    except Exception:
        logger.warning(f"[설정경고] SELL_FORCE_TIME 형식 오류 → 기본값 14:40 적용: {hhmm}")
        return dtime(hour=14, minute=40)

SELL_FORCE_TIME = _parse_hhmm(SELL_FORCE_TIME_STR)
TIME_STOP_TIME = _parse_hhmm(TIME_STOP_HHMM)

def get_rebalance_anchor_date():
    today = datetime.now(KST).date()
    if REBALANCE_ANCHOR == "today":
        return today.strftime("%Y-%m-%d")
    return today.replace(day=1).strftime("%Y-%m-%d")

def fetch_rebalancing_targets(date):
    REBALANCE_API_URL = f"http://localhost:8000/rebalance/run/{date}?force_order=true"
    response = requests.post(REBALANCE_API_URL)
    logger.info(f"[🛰️ 리밸런싱 API 전체 응답]: {response.text}")
    if response.status_code == 200:
        data = response.json()
        logger.info(f"[🎯 리밸런싱 종목]: {data.get('selected') or data.get('selected_stocks')}")
        return data.get("selected") or data.get("selected_stocks") or []
    else:
        raise Exception(f"리밸런싱 API 호출 실패: {response.text}")

def log_trade(trade: dict):
    today = datetime.now(KST).strftime("%Y-%m-%d")
    logfile = LOG_DIR / f"trades_{today}.json"
    with open(logfile, "a", encoding="utf-8") as f:
        f.write(json.dumps(trade, ensure_ascii=False) + "\n")

def save_state(holding, traded):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump({"holding": holding, "traded": traded}, f, ensure_ascii=False, indent=2)

def load_state():
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

def _to_int(val, default=0):
    try:
        return int(float(val))
    except Exception:
        return default

def _to_float(val, default=None):
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
):
    """
    매도 체결 후 실현손익 로그 출력 + 매도 사유도 함께 남김
    """
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



def _safe_get_price(kis: KisAPI, code: str):
    try:
        price = _with_retry(kis.get_current_price, code)
        if price is None or (isinstance(price, (int, float)) and price <= 0):
            logger.warning(f"[PRICE_GUARD] {code} 현재가 무효값({price})")
            return None
        return price
    except Exception as e:
        logger.warning(f"[현재가 조회 실패: 계속 진행] {code} err={e}")
        return None

def _fetch_balances(kis: KisAPI):
    if hasattr(kis, "get_balance_all"):
        res = _with_retry(kis.get_balance_all)
    else:
        res = _with_retry(kis.get_balance)
    if isinstance(res, dict):
        positions = res.get("positions") or []
        if not isinstance(positions, list):
            logger.error(f"[BAL_STD_FAIL] positions 타입 이상: {type(positions)}")
            return []
        return positions
    elif isinstance(res, list):
        return res
    else:
        logger.error(f"[BAL_STD_FAIL] 지원하지 않는 반환 타입: {type(res)}")
        return []

# === [여기 아래에 추가!] ===
def get_20d_return_pct(kis: KisAPI, code: str) -> Optional[float]:
    MAX_RETRY = 3
    for attempt in range(1, MAX_RETRY+1):
        try:
            candles = kis.get_daily_candles(code, count=21)
            if not candles or len(candles) < 20:
                logger.warning(f"[20D_RETURN_FAIL] {code}: 캔들 {len(candles) if candles else 0}개 (21개 미만) (재시도 {attempt})")
                time.sleep(1)
                continue
            # 모든 캔들에 close값 존재 확인
            if any('close' not in c or c['close'] is None for c in candles):
                logger.error(f"[20D_RETURN_FAIL] {code}: 캔들 내 close 결측 {candles}")
                return None
            old = float(candles[-21]['close']) if len(candles) > 20 else float(candles[0]['close'])
            now = float(candles[-1]['close'])
            return ((now - old) / old) * 100.0
        except Exception as e:
            logger.warning(f"[20D_RETURN_FAIL] {code}: {e} (재시도 {attempt})")
            time.sleep(1)
            continue
    logger.error(f"[20D_RETURN_FAIL] {code}: 21개 일봉 불러오기 최종실패 - 종목제외 필요")
    return None

def is_strong_momentum(kis, code):
    """
    강한 상향 추세 모멘텀 여부: 20, 60, 120일 수익률과 MA20/MA60/MA120 위치 기준(자유 조합 가능)
    """
    try:
        candles = kis.get_daily_candles(code, count=121)
        closes = [float(x['close']) for x in candles if float(x['close']) > 0]
        if len(closes) < 61:
            return False  # 데이터 부족
        today = closes[-1]
        ma20 = sum(closes[-20:]) / 20
        ma60 = sum(closes[-60:]) / 60
        ma120 = sum(closes[-120:]) / 120
        # 수익률
        r20 = (today - closes[-21]) / closes[-21] * 100 if len(closes) > 21 else 0
        r60 = (today - closes[-61]) / closes[-61] * 100 if len(closes) > 61 else 0
        r120 = (today - closes[0]) / closes[0] * 100
        # 기준: 최근 20/60/120일 중 하나라도 +10% 이상, or 단기이평(20, 60, 120) 위에 있으면 강세로 간주
        if r20 > 10 or r60 > 10 or r120 > 10:
            return True
        if today > ma20 or today > ma60 or today > ma120:
            return True
        return False
    except Exception as e:
        logger.warning(f"[모멘텀 판별 실패] {code}: {e}")
        return False



def _weight_to_qty(kis: KisAPI, code: str, weight: float, daily_capital: int) -> int:
    weight = max(0.0, float(weight))
    alloc = int(round(daily_capital * weight))
    price = _safe_get_price(kis, code) or 0
    if price <= 0:
        return 0
    return max(0, int(alloc // int(price)))

# ... 이하 2/3에서 계속 ...
# trader.py [2/5] (158~약 330)
def _get_atr(kis: KisAPI, code: str, window: int = 14) -> Optional[float]:
    if hasattr(kis, "get_atr"):
        try:
            return kis.get_atr(code, window=window)
        except Exception as e:
            logger.warning(f"[ATR_FAIL] {code}: {e}")
            return None
    return None

def _init_position_state(holding: Dict[str, Any], code: str, entry_price: float, qty: int, k_value: Any, target_price: Optional[float]):
    atr = _get_atr(KisAPI(), code)
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
    }

def _init_position_state_from_balance(holding: Dict[str, Any], code: str, avg_price: float, qty: int):
    if qty <= 0 or code in holding:
        return
    atr = _get_atr(KisAPI(), code)
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

def ensure_fill_has_name(odno: str, code: str, name: str, qty: int = 0, price: float = 0.0):
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

# ... 이하 [3/5]에서 계속 ...
# trader.py [3/??] (약 331~500)
# === 앵커: 목표가 계산 함수 정의부 ===
def compute_entry_target(kis: KisAPI, stk: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    code = str(stk.get("code") or stk.get("stock_code") or stk.get("pdno") or "")
    if not code:
        return None, None

    # 1) 오늘 시초가
    today_open = None
    try:
        today_open = kis.get_today_open(code)
    except Exception:
        pass
    if not today_open or today_open <= 0:
        # 스냅샷 현재가를 임시 대체(야간 대비)
        try:
            snap = kis.get_current_price(code)
            if snap and snap > 0:
                today_open = float(snap)
        except Exception:
            pass
    if not today_open or today_open <= 0:
        logger.info(f"[TARGET/wait_open] {code} 오늘 시초가 미확정 → 목표가 계산 보류")
        return None, None

    # 2) 전일 범위: 일봉 → 실패 시 prev_* 백업
    prev_high = prev_low = None
    try:
        prev_candles = kis.get_daily_candles(code, count=2)
        if prev_candles and len(prev_candles) >= 2:
            prev = prev_candles[-2]
            prev_high = _to_float(prev.get("high"))
            prev_low  = _to_float(prev.get("low"))
    except Exception:
        pass
    if prev_high is None or prev_low is None:
        # 백업: 리밸런싱 응답 필드 사용
        prev_high = _to_float(stk.get("prev_high"))
        prev_low  = _to_float(stk.get("prev_low"))
        if prev_high is None or prev_low is None:
            logger.warning(f"[TARGET/prev_candle_fail] {code} 전일 캔들/백업 모두 부재")
            return None, None

    rng = max(0.0, float(prev_high) - float(prev_low))
    k_used = float(stk.get("best_k") or stk.get("K") or stk.get("k") or 0.5)
    eff_target_price = float(today_open) + rng * k_used
    return float(eff_target_price), float(k_used)




def place_buy_with_fallback(kis: KisAPI, code: str, qty: int, limit_price: int) -> Dict[str, Any]:
    result_limit = None
    try:
        if hasattr(kis, "buy_stock_limit") and limit_price and limit_price > 0:
            result_limit = _with_retry(kis.buy_stock_limit, code, qty, int(limit_price))
            logger.info("[BUY-LIMIT] %s qty=%s limit=%s -> %s", code, qty, limit_price, result_limit)
            time.sleep(3.0)
            if hasattr(kis, "check_filled"):
                try:
                    filled = bool(_with_retry(kis.check_filled, result_limit))
                except Exception:
                    filled = False
                if filled:
                    return result_limit
        else:
            logger.info("[BUY-LIMIT] API 미지원 또는 limit_price 무효 → 시장가로 진행")
    except Exception as e:
        logger.warning("[BUY-LIMIT-FAIL] %s qty=%s limit=%s err=%s", code, qty, limit_price, e)
    try:
        if hasattr(kis, "buy_stock_market"):
            result_mkt = _with_retry(kis.buy_stock_market, code, qty)
        else:
            result_mkt = _with_retry(kis.buy_stock, code, qty)
        logger.info("[BUY-MKT] %s qty=%s (from limit=%s) -> %s", code, qty, limit_price, result_mkt)
        return result_mkt
    except Exception as e:
        logger.error("[BUY-MKT-FAIL] %s qty=%s err=%s", code, qty, e)
        raise

def _force_sell_pass(kis: KisAPI, targets_codes: set, reason: str, prefer_market=True):
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

        # === [모멘텀 강세: 매도 제외] ===
        # === [모멘텀 강세: 매도 제외] (정책 토글) ===
        if MOMENTUM_OVERRIDES_FORCE_SELL and is_strong_momentum(kis, code):
            logger.info(f"[모멘텀 강세] {code}: 강한 상승추세, 강제매도 제외 (policy=MOMENTUM_OVERRIDES_FORCE_SELL=true)")
            continue


        # 기존 수익률 기반 매도 예외 로직(원하는 경우 병행 가능)
        return_pct = get_20d_return_pct(kis, code)
        logger.info(f"[모멘텀 수익률 체크] {code}: 최근 20일 수익률 {return_pct if return_pct is not None else 'N/A'}%")

        if return_pct is not None and return_pct >= 3.0:
            logger.info(
                f"[모멘텀 보유 유지] {code}: 최근 20일 수익률 {return_pct:.2f}% >= 3% → 강제매도에서 제외"
            )
            continue
        else:
            logger.info(
                f"[매도진행] {code}: 최근 20일 수익률 {return_pct if return_pct is not None else 'N/A'}% < 3% → 강제매도"
            )
        
        # 이하 기존 매도 로직
        try:
            sell_qty = min(qty, sellable) if sellable > 0 else qty
            cur_price, result = _sell_once(kis, code, sell_qty, prefer_market=prefer_market)
            # 실현손익 로그
            buy_px_for_pnl = avg_price_map.get(code) or None
            if buy_px_for_pnl:
                _log_realized_pnl(code, cur_price, sell_qty, buy_px_for_pnl)

            log_trade({
                "datetime": datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S"),
                "code": code,
                "name": None,
                "qty": sell_qty,
                "K": None,
                "target_price": None,
                "strategy": "강제전량매도",
                "side": "SELL",
                "price": cur_price if cur_price is not None else 0,
                "amount": (_to_int(cur_price, 0) * int(sell_qty)) if cur_price is not None else 0,
                "result": result,
                "pnl_pct": ( ( (float(cur_price) - float(buy_px_for_pnl)) / float(buy_px_for_pnl) * 100.0) if (cur_price is not None and buy_px_for_pnl) else None ),
                "profit": ( int(round( (float(cur_price) - float(buy_px_for_pnl)) * int(sell_qty) )) if (cur_price is not None and buy_px_for_pnl) else None ),
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


def _force_sell_all(kis: KisAPI, holding: dict, reason: str, passes: int, include_all_balances: bool, prefer_market=True):
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

# ... 이하 [4/??]에서 계속 ...
# trader.py [4/??]
def _adaptive_exit(kis: KisAPI, code: str, pos: Dict[str, Any]) -> Tuple[Optional[str], Optional[float], Optional[Any], Optional[int]]:
    now = datetime.now(KST)
    try:
        cur = _safe_get_price(kis, code)
        if cur is None:
            return None, None, None, None
        # 강한 모멘텀이면 청산 보류 (원래대로 유지)
        if is_strong_momentum(kis, code):
            logger.info(f"[SELL_GUARD] {code} 모멘텀 강세 → _adaptive_exit 스킵")
            return None, None, None, None
    except Exception:
        return None, None, None, None

    # 최고가(high) 갱신
    pos['high'] = max(float(pos.get('high', cur)), float(cur))
    qty = _to_int(pos.get('qty'), 0)
    if qty <= 0:
        return None, None, None, None

    # === 익절(3%) ===
    if cur >= float(pos['buy_price']) * 1.03:
        exec_px, result = _sell_once(kis, code, qty, prefer_market=True)
        _log_realized_pnl(code, exec_px, qty, float(pos.get('buy_price', 0.0)), reason="익절 3%")
        logger.info(f"[SELL-TRIGGER] {code} REASON=익절 3% qty={qty} price={exec_px}")
        return "익절 3%", exec_px, result, qty

    # === 트레일링스톱(최고가 4% 돌파후 1.5% 이상 하락) ===
    max_price = pos.get('high', float(pos['buy_price']))
    if max_price >= float(pos['buy_price']) * 1.04 and cur <= max_price * 0.985:
        exec_px, result = _sell_once(kis, code, qty, prefer_market=True)
        _log_realized_pnl(code, exec_px, qty, float(pos.get('buy_price', 0.0)), reason="트레일링스톱")
        logger.info(f"[SELL-TRIGGER] {code} REASON=트레일링스톱 qty={qty} price={exec_px}")
        return "트레일링스톱", exec_px, result, qty

    # === 손절(-3%) ===
    if cur <= float(pos['buy_price']) * 0.97:
        exec_px, result = _sell_once(kis, code, qty, prefer_market=True)
        _log_realized_pnl(code, exec_px, qty, float(pos.get('buy_price', 0.0)), reason="손절 -3%")
        logger.info(f"[SELL-TRIGGER] {code} REASON=손절 -3% qty={qty} price={exec_px}")
        return "손절 -3%", exec_px, result, qty

    # 더 이상 당일청산(TIME_STOP), ATR_STOP 등 없음
    return None, None, None, None


# ====== 메인 진입부 및 실전 rolling_k 루프 ======
def main():
    kis = KisAPI()

    rebalance_date = get_rebalance_anchor_date()
    logger.info(f"[ℹ️ 리밸런싱 기준일(KST)]: {rebalance_date} (anchor={REBALANCE_ANCHOR})")
    logger.info(
        f"[⏱️ 커트오프(KST)] SELL_FORCE_TIME={SELL_FORCE_TIME.strftime('%H:%M')} / 전체잔고매도={SELL_ALL_BALANCES_AT_CUTOFF} / "
        f"패스(커트오프/마감)={FORCE_SELL_PASSES_CUTOFF}/{FORCE_SELL_PASSES_CLOSE}"
    )
    logger.info(f"[💰 DAILY_CAPITAL] {DAILY_CAPITAL:,}원")
    logger.info(f"[🛡️ SLIPPAGE_ENTER_GUARD_PCT] {SLIPPAGE_ENTER_GUARD_PCT:.2f}%")

    # 상태 복구
    holding, traded = load_state()
    logger.info(f"[상태복구] holding: {list(holding.keys())}, traded: {list(traded.keys())}")

    # 리밸런싱 대상 종목 추출
    targets = fetch_rebalancing_targets(rebalance_date)  # API 반환 dict 목록

    # 후처리: qty 없고 weight만 있으면 DAILY_CAPITAL로 수량 계산
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
            try:
                qty = _weight_to_qty(kis, code, float(weight), DAILY_CAPITAL)
                logger.info(f"[ALLOC->QTY] {code} weight={weight} → qty={qty}")
            except Exception:
                qty = 0

        processed_targets[code] = {
            "code": code,
            "name": name,
            "best_k": k_best,
            "target_price": target_price,
            "qty": qty,
            "strategy": strategy,
        # === 여기 추가 ===
            "prev_open": t.get("prev_open"),
            "prev_high": t.get("prev_high"),
            "prev_low": t.get("prev_low"),
            "prev_close": t.get("prev_close"),
        }
    code_to_target: Dict[str, Any] = processed_targets

    loop_sleep_sec = 2.5

    try:
        while True:
            is_open = kis.is_market_open()
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
                    logger.info(
                        f" [잔고] 종목: {name_b}, 코드: {code_b}, "
                        f"보유수량: {stock.get('hldg_qty')}, 매도가능: {stock.get('ord_psbl_qty')}"
                    )

                current_holding = {b['pdno']: _to_int(b.get('hldg_qty', 0)) for b in balances if _to_int(b.get('hldg_qty', 0)) > 0}
                ord_psbl_map = {b['pdno']: _to_int(b.get('ord_psbl_qty', 0)) for b in balances}

                # 신규 보유분을 능동관리 대상으로 자동 초기화 (A)
                for b in balances:
                    code_b = b.get('pdno')
                    qty_b = _to_int(b.get('hldg_qty', 0))
                    if qty_b > 0 and code_b and code_b not in holding:
                        avg_b = _to_float(b.get('pchs_avg_pric') or b.get('avg_price') or 0.0, 0.0)
                        if avg_b and avg_b > 0:
                            _init_position_state_from_balance(holding, code_b, avg_b, qty_b)

                # 실제 잔고에서 사라진 보유항목은 정리
                for code in list(holding.keys()):
                    if code not in current_holding or current_holding[code] == 0:
                        logger.info(f"[보유종목 해제] {code} : 실제잔고 없음 → holding 제거")
                        holding.pop(code, None)

            except Exception as e:
                logger.error(f"[잔고조회 오류]{e}")

# ... 이하 [5/??]에서 계속 ...
# trader.py [5/??]

            # ====== 매수/매도(전략) LOOP — 오늘의 타겟 ======
            for code, target in code_to_target.items():
                qty = _to_int(target.get("매수수량") or target.get("qty"), 0)
                if qty <= 0:
                    logger.info(f"[SKIP] {code}: 매수수량 없음/0")
                    continue

                k_value = (target.get("best_k") or target.get("K") or target.get("k"))
                k_value_float = None if k_value is None else _to_float(k_value)

                # 리밸런싱이 제공한 목표가/오픈값은 절대 사용하지 않기 위해 None 고정
                # 리밸런싱이 제공한 목표가/오픈값은 절대 사용하지 않기 위해 None 고정
                raw_target_price = None
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

                    # --- 매수 --- (돌파 진입 + 슬리피지 가드)
                    if is_open and code not in holding and code not in traded:
                        enter_cond = (
                            current_price is not None and
                            eff_target_price is not None and
                            int(current_price) >= int(eff_target_price)
                        )

                        if enter_cond:
                            # 진입 슬리피지 가드
                            guard_ok = True
                            if eff_target_price and eff_target_price > 0 and current_price is not None:
                                slip_pct = ((float(current_price) - float(eff_target_price)) / float(eff_target_price)) * 100.0
                                if slip_pct > SLIPPAGE_ENTER_GUARD_PCT:
                                    guard_ok = False
                                    logger.info(
                                        f"[ENTER-GUARD] {code} 진입슬리피지 {slip_pct:.2f}% > "
                                        f"{SLIPPAGE_ENTER_GUARD_PCT:.2f}% → 진입 스킵"
                                    )

                            if not guard_ok:
                                continue

                            result = place_buy_with_fallback(kis, code, qty, limit_price=int(eff_target_price))

                            # 성공 여부 판별 후 fills에 name 채우기 시도
                            try:
                                if isinstance(result, dict) and result.get("rt_cd") == "0":
                                    out = result.get("output") or {}
                                    odno = out.get("ODNO") or out.get("ord_no") or out.get("order_no") or ""
                                    ensure_fill_has_name(odno=odno, code=code, name=name or "", qty=qty, price=current_price or 0.0)
                            except Exception as e:
                                logger.warning(f"[BUY_FILL_NAME_FAIL] code={code} ex={e}")

                            _init_position_state(holding, code, float(current_price), int(qty),
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
                        # === [SELL GUARD ②] 모멘텀 강세면 이 루프에서도 즉시 스킵 ===
                        try:
                            if is_strong_momentum(kis, code):
                                logger.info(f"[SELL_GUARD] {code} 모멘텀 강세 → 타겟 보유 매도 스킵")
                                continue
                        except Exception as e:
                            logger.warning(f"[SELL_GUARD_FAIL] {code} 모멘텀 평가 실패: {e}")

                        sellable_here = ord_psbl_map.get(code, 0)
                        if sellable_here <= 0:
                            logger.info(f"[SKIP] {code}: 매도가능수량=0 (대기/체결중/락) → 매도 보류")
                        else:
                            reason, exec_price, result, sold_qty = _adaptive_exit(kis, code, holding[code])
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
                                _pnl_pct = ( ((float(exec_price) - _bp) / _bp) * 100.0 ) if (exec_price and _bp > 0) else None
                                _profit  = ( (float(exec_price) - _bp) * int(sold_qty) ) if (exec_price and _bp > 0 and sold_qty) else None
                                log_trade({
                                    **trade_common_sell,
                                    "side": "SELL",
                                    "price": exec_price,
                                    "amount": int(exec_price or 0) * int(sold_qty or 0),
                                    "result": result,
                                    "pnl_pct": (_pnl_pct if _pnl_pct is not None else None),
                                    "profit": ( int(round(_profit)) if _profit is not None else None ),
                                    "reason": reason
                                })
                                save_state(holding, traded)
                                time.sleep(RATE_SLEEP_SEC)

                except Exception as e:
                    logger.error(f"[❌ 주문/조회 실패] {code} : {e}")
                    continue

            # ====== (A) 비타겟 보유분도 장중 능동관리 ======
            if is_open:
                for code in list(holding.keys()):
                    if code in code_to_target:
                        continue  # 위 루프에서 이미 처리

                    # === [추가: 모멘텀 강세시 매도 스킵] ===
                    if is_strong_momentum(kis, code):
                        logger.info(f"[모멘텀 강세] {code}: 강한 상승추세, 능동관리 매도 보류")
                        continue

                    # === [모멘텀: 최근 20일 수익률 +3% 이상이면 보유 지속] ===
                    return_pct = get_20d_return_pct(kis, code)
                    if return_pct is not None and return_pct >= 3.0:
                        logger.info(
                            f"[모멘텀 보유] {code}: 최근 20일 수익률 {return_pct:.2f}% >= 3% → 보유 지속"
                        )
                        continue  # 매도하지 않고 보유

                    sellable_here = ord_psbl_map.get(code, 0)
                    if sellable_here <= 0:
                        logger.info(f"[SKIP-기존보유] {code}: 매도가능수량=0 (대기/체결중/락)")
                        continue
                    name = name_map.get(code)
                    reason, exec_price, result, sold_qty = _adaptive_exit(kis, code, holding[code])
                    if reason:
                        trade_common = {
                            "datetime": now_str,
                            "code": code,
                            "name": name,
                            "qty": int(sold_qty or 0),
                            "K": holding[code].get("k_value"),
                            "target_price": holding[code].get("target_price_src"),
                            "strategy": "기존보유 능동관리",
                        }
                        _bp = float(holding[code].get("buy_price", 0.0)) if code in holding else 0.0
                        _pnl_pct = ( ((float(exec_price) - _bp) / _bp) * 100.0 ) if (exec_price and _bp > 0) else None
                        _profit  = ( (float(exec_price) - _bp) * int(sold_qty) ) if (exec_price and _bp > 0 and sold_qty) else None

                        log_trade({
                            **trade_common,
                            "side": "SELL",
                            "price": exec_price,
                            "amount": int(exec_price or 0) * int(sold_qty or 0),
                            "result": result,
                            "reason": reason,
                            "pnl_pct": (_pnl_pct if _pnl_pct is not None else None),
                            "profit": ( int(round(_profit)) if _profit is not None else None )
                        })

                        save_state(holding, traded)
                        time.sleep(RATE_SLEEP_SEC)


            # --- 장중 커트오프(KST) 강제 전량매도 (마지막 안전장치) ---
            #if is_open and now_dt_kst.time() >= SELL_FORCE_TIME:
            #    _force_sell_all(
            #        kis=kis,
            #        holding=holding,
            #        reason=f"장중 강제전량매도(커트오프 {SELL_FORCE_TIME.strftime('%H:%M')} KST)",
            #        passes=FORCE_SELL_PASSES_CUTOFF,
            #        include_all_balances=SELL_ALL_BALANCES_AT_CUTOFF,
            #        prefer_market=True
            #    )

            # --- 장마감 전량매도(더블 세이프) ---
            #if not is_open:
            #    _force_sell_all(
            #        kis=kis,
            #        holding=holding,
            #        reason="장마감 전 강제전량매도",
            #        passes=FORCE_SELL_PASSES_CLOSE,
            #        include_all_balances=True,
            #        prefer_market=True
            #    )
            #    logger.info("[✅ 장마감, 루프 종료]")
            ""    break

            save_state(holding, traded)
            time.sleep(loop_sleep_sec)

    except KeyboardInterrupt:
        logger.info("[🛑 수동 종료]")

# 실행부
if __name__ == "__main__":
    main()
