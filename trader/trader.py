# trader.py
import logging
import requests
# 패키지/스크립트 양쪽 실행 호환을 위한 import fallback
try:
    from .kis_wrapper import KisAPI
except ImportError:
    from kis_wrapper import KisAPI

from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo
import json
from pathlib import Path
import time
import os
import random
from collections import deque

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)
STATE_FILE = Path(__file__).parent / "trade_state.json"

# ====== 시간대(KST) 및 설정 ======
KST = ZoneInfo("Asia/Seoul")

# 장중 강제 전량매도 커트오프 (KST 기준)
SELL_FORCE_TIME_STR = os.getenv("SELL_FORCE_TIME", "15:15").strip()

# 커트오프/장마감 시 보유 전 종목(계좌 잔고 전체) 포함 여부 (기본 True)
SELL_ALL_BALANCES_AT_CUTOFF = os.getenv("SELL_ALL_BALANCES_AT_CUTOFF", "true").lower() == "true"

# API 호출 간 최소 휴지시간(초)
RATE_SLEEP_SEC = float(os.getenv("API_RATE_SLEEP_SEC", "0.5"))

# 커트오프/장마감 매도 시 패스(회차) 수
FORCE_SELL_PASSES_CUTOFF = int(os.getenv("FORCE_SELL_PASSES_CUTOFF", "3"))
FORCE_SELL_PASSES_CLOSE  = int(os.getenv("FORCE_SELL_PASSES_CLOSE",  "5"))

# ====== ATR(근사) & 매도전략 파라미터 ======
# ATR 근사 파라미터(EMA)
ATR_N = int(os.getenv("ATR_N", "14"))
ATR_ALPHA = 2 / (ATR_N + 1)

# 변동성 경계: ATR%가 이 값 이상이면 '고변동성'
ATR_HIGH_TH_PCT = float(os.getenv("ATR_HIGH_TH_PCT", "2.2"))  # %
ATR_LOW_TH_PCT  = float(os.getenv("ATR_LOW_TH_PCT", "1.2"))   # (정보용 하한)

# 초기 손절(저변동성/고변동성)
INIT_SL_PCT_LOWVOL  = float(os.getenv("INIT_SL_PCT_LOWVOL",  "3.5"))  # %
INIT_SL_PCT_HIGHVOL = float(os.getenv("INIT_SL_PCT_HIGHVOL", "4.5"))  # %

# TP1/TP2 및 부분청산 비율
TP1_PCT = float(os.getenv("TP1_PCT", "2.8"))
TP2_PCT = float(os.getenv("TP2_PCT", "5.5"))
TP1_SELL_RATIO = float(os.getenv("TP1_SELL_RATIO", "0.35"))  # 30~40% → 기본 35%
TP2_SELL_RATIO = float(os.getenv("TP2_SELL_RATIO", "0.35"))

# 트레일링 스탑(피크 대비 하락폭)
TRAIL_PCT_DEFAULT = float(os.getenv("TRAIL_PCT_DEFAULT", "6.0"))  # TP2 이전
TRAIL_PCT_TIGHT   = float(os.getenv("TRAIL_PCT_TIGHT",   "4.0"))  # TP2 이후

# 가격 히스토리 보관(ATR 계산용)
MAX_PRICE_SAMPLES = int(os.getenv("MAX_PRICE_SAMPLES", "120"))  # 최근 120틱(루프)만 유지

# 메인 루프 슬립
LOOP_SLEEP_SEC = float(os.getenv("LOOP_SLEEP_SEC", "3"))

def _parse_hhmm(hhmm: str) -> dtime:
    try:
        hh, mm = hhmm.split(":")
        return dtime(hour=int(hh), minute=int(mm))
    except Exception:
        logger.warning(f"[설정경고] SELL_FORCE_TIME 형식 오류 → 기본값 15:15 적용: {hhmm}")
        return dtime(hour=15, minute=15)

SELL_FORCE_TIME = _parse_hhmm(SELL_FORCE_TIME_STR)

def get_month_first_date():
    today = datetime.now(KST)
    month_first = today.replace(day=1)
    return month_first.strftime("%Y-%m-%d")

def fetch_rebalancing_targets(date):
    """
    /rebalance/run/{date}?force_order=true 호출 결과에서
    selected 또는 selected_stocks 키를 우선 사용.
    """
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

def _compress_price_hist(hist_deque: deque):
    """상태 저장 시 기록 폭을 제한."""
    if hist_deque is None:
        return []
    return list(hist_deque)[-min(len(hist_deque), MAX_PRICE_SAMPLES):]

def save_state(holding, traded):
    # deque는 직렬화가 안되므로 list로 변환
    serializable = {}
    for code, st in holding.items():
        st_copy = dict(st)
        if isinstance(st_copy.get("price_hist"), deque):
            st_copy["price_hist"] = _compress_price_hist(st_copy["price_hist"])
        serializable[code] = st_copy
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump({"holding": serializable, "traded": traded}, f, ensure_ascii=False, indent=2)

def load_state():
    if STATE_FILE.exists():
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            state = json.load(f)
            holding = state.get("holding", {})
            # price_hist를 deque로 복구
            for code, st in holding.items():
                hist = st.get("price_hist") or []
                st["price_hist"] = deque(hist, maxlen=MAX_PRICE_SAMPLES)
            return holding, state.get("traded", {})
    return {}, {}

# ----- 공용 재시도 래퍼 -----
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

def _safe_get_price(kis: KisAPI, code: str):
    """현재가 조회 실패해도 매도는 진행할 수 있도록 None을 허용."""
    try:
        price = _with_retry(kis.get_current_price, code)
        if price is None or (isinstance(price, (int, float)) and price <= 0):
            logger.warning(f"[PRICE_GUARD] {code} 현재가 무효값({price})")
            return None
        return float(price)
    except Exception as e:
        logger.warning(f"[현재가 조회 실패: 계속 진행] {code} err={e}")
        return None

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

# ===== 정규화: 항상 '포지션 리스트'만 반환 =====
def _fetch_positions(kis: KisAPI):
    """
    항상 [포지션 dict, ...] 리스트를 반환.
    - KisAPI.get_positions()가 있으면 그걸 사용
    - 없으면 get_balance()의 'positions' 또는 'output1' 키를 사용
    """
    if hasattr(kis, "get_positions"):
        return _with_retry(kis.get_positions)

    b = _with_retry(kis.get_balance)
    if isinstance(b, dict):
        return b.get("positions") or b.get("output1") or []
    return b if isinstance(b, list) else []

# ===== ATR(근사) & 포지션 상태 보조 =====
def _update_price_hist_and_atr(state: dict, price: float):
    """
    분봉 API 없이 루프에서 수집한 틱 단위 가격으로 TR≈|Close_t - Close_{t-1}|를 사용,
    EMA로 근사 ATR을 갱신한다. (엄밀 ATR과 차이 존재)
    """
    if "price_hist" not in state or state["price_hist"] is None:
        state["price_hist"] = deque(maxlen=MAX_PRICE_SAMPLES)

    hist = state["price_hist"]
    prev = hist[-1] if len(hist) > 0 else None
    hist.append(price)

    # TR 근사
    tr = abs(price - prev) if prev is not None else 0.0

    # EMA ATR
    prev_atr = _to_float(state.get("atr"), 0.0)
    atr = (ATR_ALPHA * tr) + ((1 - ATR_ALPHA) * prev_atr)
    state["atr"] = atr

    # ATR%
    buy_price = _to_float(state.get("buy_price"))
    atr_pct = (atr / buy_price * 100.0) if (buy_price and buy_price > 0) else None
    state["atr_pct"] = atr_pct
    return atr, atr_pct

def _init_or_adjust_stops(state: dict):
    """
    매수 직후 또는 ATR 업데이트 후 초기 손절/트레일링 폭을 변동성에 맞춰 설정/조정.
    """
    buy_price = _to_float(state.get("buy_price"))
    atr_pct = _to_float(state.get("atr_pct"))
    if not buy_price:
        return

    # 변동성 레짐 판정
    high_vol = (atr_pct is not None) and (atr_pct >= ATR_HIGH_TH_PCT)

    init_sl_pct = INIT_SL_PCT_HIGHVOL if high_vol else INIT_SL_PCT_LOWVOL
    state.setdefault("stop_price", round(buy_price * (1 - init_sl_pct / 100.0), 2))
    state.setdefault("trail_pct", TRAIL_PCT_DEFAULT)
    state.setdefault("tp1_done", False)
    state.setdefault("tp2_done", False)
    state.setdefault("tight_after_tp2", False)

def _maybe_take_profits_and_move_stops(kis: KisAPI, code: str, pos: dict, cur_price: float, sellable_here: int):
    """
    TP1/TP2 조건, BE 이동, 트레일링 타이트닝을 처리.
    일부/전량 매도 시도 후 상태 갱신 및 로깅.
    """
    buy_price = _to_float(pos.get("buy_price"))
    if not buy_price or cur_price is None:
        return

    qty_total = _to_int(pos.get("qty"), 0)
    qty_left = _to_int(pos.get("qty_left"), qty_total)
    if qty_left <= 0:
        return

    # 피크/트레일 갱신
    pos["peak_price"] = max(_to_float(pos.get("peak_price"), buy_price), cur_price)

    # 익절% 계산
    profit_pct = ((cur_price - buy_price) / buy_price) * 100.0

    # --- TP1: +2.8% ---
    if not pos.get("tp1_done") and profit_pct >= TP1_PCT:
        sell_ratio = TP1_SELL_RATIO
        sell_qty = max(1, int(qty_total * sell_ratio))
        sell_qty = min(sell_qty, qty_left, sellable_here)
        if sell_qty > 0:
            _do_sell(kis, code, sell_qty, reason=f"TP1(+{TP1_PCT}%) 부분청산 {int(sell_ratio*100)}% & BE 이동")
            pos["qty_left"] = qty_left - sell_qty
            pos["tp1_done"] = True
            # BE로 스탑 올림
            pos["stop_price"] = max(_to_float(pos.get("stop_price"), 0.0), buy_price)

    # --- TP2: +5.5% ---
    qty_left = _to_int(pos.get("qty_left"), qty_total)
    if not pos.get("tp2_done") and profit_pct >= TP2_PCT and qty_left > 0:
        sell_ratio = TP2_SELL_RATIO
        sell_qty = max(1, int(qty_total * sell_ratio))
        sell_qty = min(sell_qty, qty_left, sellable_here)
        if sell_qty > 0:
            _do_sell(kis, code, sell_qty, reason=f"TP2(+{TP2_PCT}%) 부분청산 {int(sell_ratio*100)}% & 트레일 축소")
            pos["qty_left"] = qty_left - sell_qty
            pos["tp2_done"] = True
            # 트레일 타이트닝
            pos["trail_pct"] = min(_to_float(pos.get("trail_pct"), TRAIL_PCT_DEFAULT), TRAIL_PCT_TIGHT)
            pos["tight_after_tp2"] = True

def _enforce_trailing_or_stop(kis: KisAPI, code: str, pos: dict, cur_price: float, sellable_here: int):
    """
    트레일링 스탑(피크 대비) 또는 고정 스탑가격 도달 시 잔여 전량 매도.
    """
    buy_price = _to_float(pos.get("buy_price"))
    qty_left = _to_int(pos.get("qty_left"), _to_int(pos.get("qty"), 0))
    if qty_left <= 0 or cur_price is None or not buy_price:
        return

    peak = _to_float(pos.get("peak_price"), buy_price)
    trail_pct = _to_float(pos.get("trail_pct"), TRAIL_PCT_DEFAULT)
    stop_price = _to_float(pos.get("stop_price"), buy_price * (1 - INIT_SL_PCT_LOWVOL / 100.0))

    # 트레일 조건
    trigger_trail = (cur_price <= peak * (1 - trail_pct / 100.0))
    # 고정 스탑
    trigger_stop = (cur_price <= stop_price)

    if trigger_trail or trigger_stop:
        sell_qty = min(qty_left, sellable_here) if sellable_here > 0 else qty_left
        if sell_qty > 0:
            reason = "트레일링 스탑 발동" if trigger_trail else "스탑로스 발동"
            _do_sell(kis, code, sell_qty, reason=reason)
            pos["qty_left"] = qty_left - sell_qty
            if pos["qty_left"] <= 0:
                # 포지션 종료
                pos["closed"] = True

def _do_sell(kis: KisAPI, code: str, qty: int, reason: str):
    now_str = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
    cur_price = _safe_get_price(kis, code)
    try:
        result = _with_retry(kis.sell_stock_market, code, qty)
    except Exception as e:
        logger.warning(f"[매도 재시도: 토큰 갱신 후 1회] {code} qty={qty} err={e}")
        try:
            if hasattr(kis, "refresh_token"):
                kis.refresh_token()
        except Exception:
            pass
        result = _with_retry(kis.sell_stock_market, code, qty)

    logger.info(f"[✅ SELL] {code} qty={qty} price(log)={cur_price} reason={reason} result={result}")
    log_trade({
        "datetime": now_str,
        "code": code,
        "name": None,
        "qty": qty,
        "K": None,
        "target_price": None,
        "strategy": "ATR_Partial_Trail",
        "side": "SELL",
        "price": cur_price if cur_price is not None else 0,
        "amount": (int(cur_price) * int(qty)) if cur_price else 0,
        "result": result,
        "reason": reason
    })
    time.sleep(RATE_SLEEP_SEC)

def _force_sell_pass(kis: KisAPI, targets_codes: set, reason: str, prefer_market=True):
    """
    강제 전량매도 1패스: 포지션 리스트를 기준으로 매도 시도 후 잔존 파악
    """
    if not targets_codes:
        return set()

    targets_codes = {c for c in targets_codes if c}
    positions = _fetch_positions(kis)
    qty_map      = {b.get("pdno"): _to_int(b.get("hldg_qty", 0)) for b in positions}
    sellable_map = {b.get("pdno"): _to_int(b.get("ord_psbl_qty", 0)) for b in positions}

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
        try:
            sell_qty = min(qty, sellable)
            _do_sell(kis, code, sell_qty, reason=reason)
        finally:
            time.sleep(RATE_SLEEP_SEC)

    positions_after = _fetch_positions(kis)
    after_qty_map = {b.get("pdno"): _to_int(b.get("hldg_qty", 0)) for b in positions_after}
    for code in targets_codes:
        if after_qty_map.get(code, 0) > 0:
            remaining.add(code)
    return remaining

def _force_sell_all(kis: KisAPI, holding: dict, reason: str, passes: int, include_all_balances: bool, prefer_market=True):
    target_codes = set([c for c in holding.keys() if c])

    if include_all_balances:
        try:
            positions = _fetch_positions(kis)
            for b in positions:
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

    # 상태 정리
    for code in list(holding.keys()):
        holding.pop(code, None)
    save_state(holding, {})  # traded는 의미 없으므로 비움

def main():
    kis = KisAPI()
    rebalance_date = get_month_first_date()
    logger.info(f"[ℹ️ 리밸런싱 기준일(KST)]: {rebalance_date}")
    logger.info(f"[⏱️ 커트오프(KST)] SELL_FORCE_TIME={SELL_FORCE_TIME.strftime('%H:%M')} / 전체잔고매도={SELL_ALL_BALANCES_AT_CUTOFF} / "
                f"패스(커트오프/마감)={FORCE_SELL_PASSES_CUTOFF}/{FORCE_SELL_PASSES_CLOSE}")

    # ======== 상태 복구 ========
    holding, traded = load_state()
    logger.info(f"[상태복구] holding: {list(holding.keys())}, traded: {list(traded.keys())}")

    # deque 복구 누락 대비
    for code, st in holding.items():
        if not isinstance(st.get("price_hist"), deque):
            st["price_hist"] = deque(st.get("price_hist", []), maxlen=MAX_PRICE_SAMPLES)

    # ======== 리밸런싱 대상 종목 추출 ========
    targets = fetch_rebalancing_targets(rebalance_date)
    code_to_target = {}
    for target in targets:
        code = target.get("stock_code") or target.get("code")
        if code:
            code_to_target[code] = target

    try:
        while True:
            is_open = kis.is_market_open()
            now_dt_kst = datetime.now(KST)
            now_str = now_dt_kst.strftime("%Y-%m-%d %H:%M:%S")
            logger.info(f"[⏰ 장상태] {'OPEN' if is_open else 'CLOSED'} / KST={now_str}")

            # ====== 잔고 동기화 ======
            ord_psbl_map = {}
            try:
                positions = _fetch_positions(kis)
                logger.info(f"[보유잔고 API 결과 종목수] {len(positions)}개")
                for stock in positions:
                    logger.info(
                        f"  [잔고] 종목: {stock.get('prdt_name')}, 코드: {stock.get('pdno')}, "
                        f"보유수량: {stock.get('hldg_qty')}, 매도가능: {stock.get('ord_psbl_qty')}"
                    )
                current_holding = {b['pdno']: _to_int(b.get('hldg_qty', 0)) for b in positions if _to_int(b.get('hldg_qty', 0)) > 0}
                ord_psbl_map    = {b['pdno']: _to_int(b.get('ord_psbl_qty', 0)) for b in positions}
                # 보유 해제
                for code in list(holding.keys()):
                    if code not in current_holding or current_holding[code] == 0:
                        logger.info(f"[보유종목 해제] {code} : 실제잔고 없음 → holding 제거")
                        holding.pop(code, None)
            except Exception as e:
                logger.error(f"[잔고조회 오류]{e}")

            # ====== 매수/매도(전략) LOOP ======
            for code, target in code_to_target.items():
                qty_target = _to_int(target.get("매수수량") or target.get("qty"), 0)
                if qty_target <= 0:
                    logger.info(f"[SKIP] {code}: 매수수량 없음/0")
                    continue

                k_value = (target.get("best_k") or target.get("K") or target.get("k"))
                target_price = _to_float(target.get("목표가") or target.get("target_price"))
                strategy = target.get("strategy") or "전월 rolling K 최적화"
                name = target.get("name") or target.get("종목명")

                if target_price is None:
                    logger.warning(f"[SKIP] {code}: target_price 누락")
                    continue

                try:
                    current_price = _safe_get_price(kis, code)
                    logger.info(f"[📈 현재가] {code}: {current_price}")

                    # --- 매수 ---
                    if is_open and code not in holding and code not in traded:
                        if current_price is not None and current_price >= float(target_price):
                            result = _with_retry(kis.buy_stock, code, qty_target)
                            # 포지션 상태 초기화
                            holding[code] = {
                                "qty": int(qty_target),
                                "qty_left": int(qty_target),
                                "buy_price": float(current_price),
                                "peak_price": float(current_price),
                                "stop_price": None,           # ATR 갱신 후 설정
                                "trail_pct": TRAIL_PCT_DEFAULT,
                                "tp1_done": False,
                                "tp2_done": False,
                                "tight_after_tp2": False,
                                "atr": 0.0,
                                "atr_pct": None,
                                "price_hist": deque([float(current_price)], maxlen=MAX_PRICE_SAMPLES),
                                "trade_common": {
                                    "datetime": now_str,
                                    "code": code,
                                    "name": name,
                                    "qty": qty_target,
                                    "K": k_value,
                                    "target_price": target_price,
                                    "strategy": strategy,
                                }
                            }
                            traded[code] = {"buy_time": now_str, "qty": int(qty_target), "price": float(current_price)}
                            logger.info(f"[✅ 매수주문] {code}, qty={qty_target}, price={current_price}, result={result}")
                            log_trade({**holding[code]["trade_common"], "side": "BUY", "price": current_price,
                                       "amount": int(current_price) * int(qty_target), "result": result})
                            # 초기 스탑 설정(첫 루프에 ATR 근사 반영)
                            _init_or_adjust_stops(holding[code])
                            save_state(holding, traded)
                            time.sleep(RATE_SLEEP_SEC)
                        else:
                            logger.info(f"[SKIP] {code}: 현재가({current_price}) < 목표가({target_price}), 미매수")
                            continue

                    # --- 보유 중 매도 로직(부분익절/트레일/스탑) ---
                    if is_open and code in holding:
                        sellable_here = ord_psbl_map.get(code, 0)
                        if sellable_here <= 0:
                            logger.info(f"[SKIP] {code}: 매도가능수량=0 (대기/체결중/락) → 매도 보류")
                        else:
                            pos = holding[code]
                            if current_price is None:
                                logger.warning(f"[매도조건 판정불가] {code} cur=None")
                            else:
                                # ATR(근사) 업데이트 및 초기/조정 스탑 적용
                                _update_price_hist_and_atr(pos, float(current_price))
                                _init_or_adjust_stops(pos)

                                # 부분익절 & 스탑 이동/트레일 타이트닝
                                _maybe_take_profits_and_move_stops(kis, code, pos, float(current_price), sellable_here)

                                # 트레일/스탑 강제
                                _enforce_trailing_or_stop(kis, code, pos, float(current_price), sellable_here)

                                # 포지션 종료 정리
                                if pos.get("closed") or _to_int(pos.get("qty_left"), 0) <= 0:
                                    logger.info(f"[포지션 종료] {code}")
                                    holding.pop(code, None)
                                    traded.pop(code, None)

                except Exception as e:
                    logger.error(f"[❌ 주문/조회 실패] {code} : {e}")
                    continue

            # --- 장중 커트오프(KST) 강제 전량매도 ---
            if is_open and now_dt_kst.time() >= SELL_FORCE_TIME:
                _force_sell_all(
                    kis=kis,
                    holding=holding,
                    reason=f"장중 강제전량매도(커트오프 {SELL_FORCE_TIME.strftime('%H:%M')} KST)",
                    passes=FORCE_SELL_PASSES_CUTOFF,
                    include_all_balances=SELL_ALL_BALANCES_AT_CUTOFF,
                    prefer_market=True
                )
                # 이후에도 루프는 유지(남은 상태는 다음 루프에서 다시 동기화)

            # --- 장마감 전량매도(더블 세이프) ---
            if not is_open:
                _force_sell_all(
                    kis=kis,
                    holding=holding,
                    reason="장마감 전 강제전량매도",
                    passes=FORCE_SELL_PASSES_CLOSE,
                    include_all_balances=True,   # 장마감 시에는 무조건 전체 잔고 대상
                    prefer_market=True
                )
                logger.info("[✅ 장마감, 루프 종료]")
                break

            save_state(holding, traded)
            time.sleep(LOOP_SLEEP_SEC)

    except KeyboardInterrupt:
        logger.info("[🛑 수동 종료]")

if __name__ == "__main__":
    main()
