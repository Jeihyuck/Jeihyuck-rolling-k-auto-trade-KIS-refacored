# trader.py — fixed version
# 안전 로직 강화, 리밸런서 장애 시 즉시 전량매도 금지, 리밸런서 페일오버(캐시/selected/latest) 우선 사용,
# 그리고 각 종목의 목표가(target_price)를 트레이더 측에서 재계산하도록 보완.

import logging
import requests
from .kis_wrapper import KisAPI
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo
import json
from pathlib import Path
import time
import os
import random
import argparse

# ------------------------------------------------------------------
# 기본 로깅/경로
# ------------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)
STATE_FILE = Path(__file__).parent / "trade_state.json"

# ====== 시간대(KST) 및 설정 ======
KST = ZoneInfo("Asia/Seoul")

# 장중 강제 전량매도 커트오프 (KST 기준)
SELL_FORCE_TIME_STR = os.getenv("SELL_FORCE_TIME", "11:15").strip()

# 커트오프/장마감 시 보유 전 종목(계좌 잔고 전체) 포함 여부 (기본 True)
SELL_ALL_BALANCES_AT_CUTOFF = os.getenv("SELL_ALL_BALANCES_AT_CUTOFF", "true").lower() == "true"

# API 호출 간 최소 휴지시간(초)
RATE_SLEEP_SEC = float(os.getenv("API_RATE_SLEEP_SEC", "0.5"))

# 커트오프/장마감 매도 시 패스(회차) 수
FORCE_SELL_PASSES_CUTOFF = int(os.getenv("FORCE_SELL_PASSES_CUTOFF", "3"))
FORCE_SELL_PASSES_CLOSE = int(os.getenv("FORCE_SELL_PASSES_CLOSE", "5"))

# 안전 관련 envs
ALLOW_FORCE_SELL_ON_REBALANCE_FAIL = os.getenv("ALLOW_FORCE_SELL_ON_REBALANCE_FAIL", "false").lower() in ("1", "true", "yes")
ALLOW_REBALANCE_RUN_CALL = os.getenv("ALLOW_REBALANCE_RUN_CALL", "0") in ("1", "true", "True")
REBALANCE_API_BASE = os.getenv("REBALANCE_API_BASE", "http://localhost:8000")
REBALANCE_OUT_DIR = Path(os.getenv("REBALANCE_OUT_DIR", "rebalance_results"))
REBALANCE_OUT_DIR.mkdir(exist_ok=True)

# 기본 트레이딩 파라미터 (환경변수로 재정의 가능)
TOTAL_CAPITAL = int(os.getenv("DAILY_CAPITAL", os.getenv("TOTAL_CAPITAL", "10000000")))
MIN_QTY_PER_TICKET = int(os.getenv("MIN_QTY_PER_TICKET", "1"))
ORDER_THROTTLE_SEC = float(os.getenv("ORDER_THROTTLE_SEC", "0.3"))

# ------------------------------------------------------------------
# 시간 헬퍼
# ------------------------------------------------------------------

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

# ------------------------------------------------------------------
# 상태 저장/로딩
# ------------------------------------------------------------------

def log_trade(trade: dict):
    today = datetime.now(KST).strftime("%Y-%m-%d")
    logfile = LOG_DIR / f"trades_{today}.json"
    with open(logfile, "a", encoding="utf-8") as f:
        f.write(json.dumps(trade, ensure_ascii=False) + "\n")


def save_state(holding, traded):
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump({"holding": holding, "traded": traded}, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.warning(f"[STATE_SAVE_FAIL] {e}")


def load_state():
    if STATE_FILE.exists():
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                state = json.load(f)
                return state.get("holding", {}), state.get("traded", {})
        except Exception as e:
            logger.warning(f"[STATE_LOAD_FAIL] {e}")
    return {}, {}

# ------------------------------------------------------------------
# 재시도 래퍼
# ------------------------------------------------------------------

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

# ------------------------------------------------------------------
# 가격/매도 래퍼
# ------------------------------------------------------------------

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


def _sell_once(kis: KisAPI, code: str, qty: int, prefer_market=True):
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

# ------------------------------------------------------------------
# 잔고 응답 정규화
# ------------------------------------------------------------------

def normalize_balances(resp):
    """
    KisAPI에서 반환되는 잔고 응답은 환경/버전마다 다를 수 있음.
    - 이미 list 형식이면 그대로 반환
    - dict 형태면서 'output1' 키가 있으면 그 리스트를 반환
    - dict 형태지만 바로 항목들을 담고 있는 경우도 처리
    - 기타: 빈 리스트 반환
    """
    if resp is None:
        return []
    if isinstance(resp, list):
        return resp
    if isinstance(resp, dict):
        # common pattern: {'output1': [..], 'other': ...}
        if "output1" in resp and isinstance(resp["output1"], list):
            return resp["output1"]
        # some wrappers may return {'balances': [...]}
        for k in ("balances", "items", "output"):
            if k in resp and isinstance(resp[k], list):
                return resp[k]
        # sometimes the wrapper already returned a dict that *is* the item
        # but not list — try to detect numeric-keyed dict
        # fall back: attempt to find any list-valued key
        for v in resp.values():
            if isinstance(v, list):
                return v
    # unknown format
    logger.warning(f"[BALANCE_NORMALIZE_WARN] unexpected balance format: {type(resp)}")
    return []

# ------------------------------------------------------------------
# 잔고 조회 통합
# ------------------------------------------------------------------

def _fetch_balances(kis: KisAPI):
    if hasattr(kis, "get_balance_all"):
        resp = _with_retry(kis.get_balance_all)
    else:
        resp = _with_retry(kis.get_balance)
    return normalize_balances(resp)

# ------------------------------------------------------------------
# 강제 전량매도 (기존 로직 유지) — 내부에서 holding을 반드시 비움
# ------------------------------------------------------------------

def _force_sell_pass(kis: KisAPI, targets_codes: set, reason: str, prefer_market=True):
    if not targets_codes:
        return set()

    targets_codes = {c for c in targets_codes if c}
    balances = _fetch_balances(kis)
    qty_map = {b.get("pdno"): _to_int(b.get("hldg_qty", 0)) for b in balances}
    sellable_map = {b.get("pdno"): _to_int(b.get("ord_psbl_qty", 0)) for b in balances}

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
            sell_qty = min(qty, sellable) if sellable > 0 else qty
            cur_price, result = _sell_once(kis, code, sell_qty, prefer_market=prefer_market)
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

    # 상태 정리
    for code in list(holding.keys()):
        holding.pop(code, None)
    save_state(holding, {})

# ------------------------------------------------------------------
# 리밸런스 시그널 페치(안전한 로직)
# ------------------------------------------------------------------

def _read_local_rebalance_cache(date: str):
    fp = REBALANCE_OUT_DIR / f"rebalance_{date}.json"
    if fp.exists():
        try:
            with open(fp, "r", encoding="utf-8") as f:
                data = json.load(f)
            logger.info(f"[CACHE] local rebalance cache loaded: {fp}")
            return data
        except Exception as e:
            logger.warning(f"[CACHE_ERR] local cache load failed: {fp} err={e}")
    return None


def fetch_rebalancing_targets(date, timeout=12, max_retries=3):
    selected = None
    last_err = None

    # 1) GET /rebalance/selected/{date}
    try:
        url = f"{REBALANCE_API_BASE}/rebalance/selected/{date}"
        logger.info(f"[REB_FETCH] GET {url}")
        r = requests.get(url, timeout=timeout)
        if r.status_code == 200:
            payload = r.json()
            if payload.get("status") == "ready":
                selected = payload.get("selected", [])
                logger.info(f"[REB_FETCH] selected/{date} returned {len(selected)} items")
                return selected
            else:
                logger.info(f"[REB_FETCH] selected/{date} status not ready: {payload.get('status')}")
    except Exception as e:
        last_err = e
        logger.warning(f"[REBALANCE_FETCH_FAIL] GET /selected/{date} failed: {e}")

    # 2) GET /rebalance/latest
    try:
        url = f"{REBALANCE_API_BASE}/rebalance/latest"
        logger.info(f"[REB_FETCH] GET {url}")
        r = requests.get(url, timeout=timeout)
        if r.status_code == 200:
            payload = r.json()
            selected = payload.get("selected_stocks") or payload.get("selected") or []
            if selected:
                logger.info(f"[REB_FETCH] latest returned {len(selected)} items (date={payload.get('date')})")
                return selected
            logger.info("[REB_FETCH] latest returned empty selected_stocks")
    except Exception as e:
        last_err = e
        logger.warning(f"[REBALANCE_FETCH_FAIL] GET /latest failed: {e}")

    # 3) local cache file
    local = _read_local_rebalance_cache(date)
    if local:
        if isinstance(local, list):
            logger.info(f"[REB_FETCH] using local file list (count={len(local)})")
            return local
        elif isinstance(local, dict):
            if "selected" in local:
                return local["selected"]
            if "selected_stocks" in local:
                return local["selected_stocks"]
            for k in ("signals", "results"):
                if k in local and isinstance(local[k], list):
                    return local[k]

    # 4) POST /rebalance/run/{date} — 최후의 수단, 기본적으로 비활성
    if ALLOW_REBALANCE_RUN_CALL:
        try:
            url = f"{REBALANCE_API_BASE}/rebalance/run/{date}?force_order=false"
            logger.info(f"[REB_FETCH] POST {url} (last-resort)")
            r = requests.post(url, timeout=timeout * 2)
            if r.status_code == 200:
                payload = r.json()
                selected = payload.get("selected") or payload.get("selected_stocks") or payload.get("signals") or []
                logger.info(f"[REB_FETCH] run returned {len(selected)} items")
                return selected
        except Exception as e:
            last_err = e
            logger.warning(f"[REBALANCE_FETCH_FAIL] POST /run failed: {e}")

    logger.error(f"[REBALANCE_FETCH_FAIL] all methods failed. last_err={last_err}")
    return []

# ------------------------------------------------------------------
# 목표가 계산: 트레이더가 직접 재계산 (우선순위: Kis prev OHLC > signal base_ohlc)
# ------------------------------------------------------------------

def _fetch_prev_day_ohlc(kis: KisAPI, code: str):
    """
    시도적으로 이전 거래일 OHLC를 KisAPI에서 가져오는 함수.
    KisAPI 구현체마다 메서드명이 다를 수 있으므로 안전하게 확인.
    반환: dict with keys 'open','high','low','close' (numbers) or None
    """
    try:
        if hasattr(kis, "get_prev_day_ohlc"):
            return _with_retry(kis.get_prev_day_ohlc, code)
        # fallback: get_ohlc or get_candle
        if hasattr(kis, "get_ohlc"):
            return _with_retry(kis.get_ohlc, code)
        if hasattr(kis, "get_daily_candle"):
            return _with_retry(kis.get_daily_candle, code)
    except Exception as e:
        logger.debug(f"[OHLC_FETCH_FAIL] {code} err={e}")
    return None


def compute_target_price(kis: KisAPI, target: dict):
    """
    트레이더 측에서 목표가를 재계산.
    > 우선적으로 KisAPI에서 이전 거래일 OHLC를 시도 요청
    > 실패시 리밸런서가 제공한 base_close/base_high/base_low 사용
    > 공식: target = prev_close + best_k * (prev_high - prev_low)
    """
    code = target.get("stock_code") or target.get("code")
    best_k = _to_float(target.get("best_k") or target.get("K") or target.get("k"), None)

    # 1) Kis prev OHLC
    ohlc = None
    try:
        ohlc = _fetch_prev_day_ohlc(kis, code)
    except Exception:
        ohlc = None

    # 2) fallback to signal's base_* if present
    if not ohlc:
        if all(k in target for k in ("base_close", "base_high", "base_low")):
            ohlc = {
                "close": _to_float(target.get("base_close")),
                "high": _to_float(target.get("base_high")),
                "low": _to_float(target.get("base_low")),
            }

    if not ohlc or best_k is None:
        logger.warning(f"[TARGET_CALC_FAIL] {code}: insufficient data to compute target (best_k={best_k}, ohlc_exists={bool(ohlc)})")
        return None

    prev_close = _to_float(ohlc.get("close"))
    prev_high = _to_float(ohlc.get("high"))
    prev_low = _to_float(ohlc.get("low"))

    if prev_close is None or prev_high is None or prev_low is None:
        logger.warning(f"[TARGET_CALC_FAIL] {code}: ohlc missing fields: {ohlc}")
        return None

    range_ = prev_high - prev_low
    target_price = prev_close + best_k * range_

    # defensive: round and ensure at least prev_close
    try:
        t_int = int(round(target_price))
    except Exception:
        t_int = int(prev_close)

    if t_int <= 0:
        logger.warning(f"[TARGET_GUARD] {code}: computed non-positive target {t_int} -> skip")
        return None

    # if computed target is less than prev_close, bump it to prev_close+1 to avoid stale low targets
    if t_int <= int(prev_close):
        t_int = int(prev_close) + 1

    return t_int

# ------------------------------------------------------------------
# 메인 루프
# ------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--force-sell", action="store_true", help="강제전량매도 모드")
    args = parser.parse_args()

    kis = KisAPI()
    rebalance_date = get_month_first_date()
    logger.info(f"[ℹ️ 리밸런싱 기준일(KST)]: {rebalance_date}")
    logger.info(f"[⏱️ 커트오프(KST)] SELL_FORCE_TIME={SELL_FORCE_TIME.strftime('%H:%M')} / 전체잔고매도={SELL_ALL_BALANCES_AT_CUTOFF} / "
                f"패스(커트오프/마감)={FORCE_SELL_PASSES_CUTOFF}/{FORCE_SELL_PASSES_CLOSE}")

    holding, traded = load_state()
    logger.info(f"[상태복구] holding: {list(holding.keys())}, traded: {list(traded.keys())}")

    # 리밸런스 대상 추출 (안전 방식)
    targets = fetch_rebalancing_targets(rebalance_date)
    code_to_target = {}
    if not targets:
        logger.warning("[PREPARE_TARGETS] 리밸런싱 시그널 없음 — 거래 스킵")
    else:
        for target in targets:
            code = target.get("stock_code") or target.get("code")
            if not code:
                continue
            # 재계산된 목표가를 trader가 새로 산정
            try:
                tprice = compute_target_price(kis, target)
                # 수량 계산: 자금/포지션제약 또는 target에서 파생
                # simple fallback:  TOTAL_CAPITAL / min(len(targets), 1)
                # 여기서는 기존 리밸런서가 매수수량을 제공할 수 있으므로 우선 사용
                qty = _to_int(target.get("매수수량") or target.get("qty") or 0)
                if qty <= 0:
                    # 기본 자본 분배
                    npos = max(1, len(targets))
                    each = max(int(TOTAL_CAPITAL // npos), 1)
                    if tprice and tprice > 0:
                        qty = max(each // tprice, MIN_QTY_PER_TICKET)
                    else:
                        qty = MIN_QTY_PER_TICKET

                # attach new computed fields
                target_copy = dict(target)
                target_copy["computed_target_price"] = tprice
                target_copy["매수수량"] = qty
                code_to_target[code] = target_copy
            except Exception as e:
                logger.warning(f"[TARGET_PREP_FAIL] {code} err={e}")

    # 기본 매도조건(익절/손절) — 환경변수로 조정 가능
    sell_conditions = {
        'profit_pct': float(os.getenv('SELL_PROFIT_PCT', '3.0')),   # +3% 익절
        'loss_pct':  float(os.getenv('SELL_LOSS_PCT', '-2.0'))      # -2% 손절
    }

    loop_sleep_sec = int(os.getenv('LOOP_SLEEP_SEC', '3'))

    try:
        while True:
            is_open = kis.is_market_open()
            now_dt_kst = datetime.now(KST)
            now_str = now_dt_kst.strftime("%Y-%m-%d %H:%M:%S")
            logger.info(f"[⏰ 장상태] {'OPEN' if is_open else 'CLOSED'} / KST={now_str}")

            # 잔고 동기화
            ord_psbl_map = {}
            try:
                balances = _fetch_balances(kis)
                logger.info(f"[보유잔고 API 결과 종목수] {len(balances)}개")
                for stock in balances:
                    logger.info(
                        f"  [잔고] 종목: {stock.get('prdt_name')}, 코드: {stock.get('pdno')}, "
                        f"보유수량: {stock.get('hldg_qty')}, 매도가능: {stock.get('ord_psbl_qty')}"
                    )
                current_holding = {b.get('pdno'): _to_int(b.get('hldg_qty', 0)) for b in balances if _to_int(b.get('hldg_qty', 0)) > 0}
                ord_psbl_map = {b.get('pdno'): _to_int(b.get('ord_psbl_qty', 0)) for b in balances}
                for code in list(holding.keys()):
                    if code not in current_holding or current_holding[code] == 0:
                        logger.info(f"[보유종목 해제] {code} : 실제잔고 없음 → holding 제거")
                        holding.pop(code, None)
            except Exception as e:
                logger.error(f"[잔고조회 오류]{e}")

            # 매수/매도 루프 — 안전 조건: code_to_target may be empty
            for code, target in list(code_to_target.items()):
                qty = _to_int(target.get("매수수량") or target.get("qty"), 0)
                if qty <= 0:
                    logger.info(f"[SKIP] {code}: 매수수량 없음/0")
                    continue

                k_value = (target.get("best_k") or target.get("K") or target.get("k"))
                target_price = target.get("computed_target_price") or _to_float(target.get("목표가") or target.get("target_price"))
                strategy = target.get("strategy") or "전월 rolling K 최적화"
                name = target.get("name") or target.get("종목명")

                if target_price is None:
                    logger.warning(f"[SKIP] {code}: target_price 누락")
                    continue

                try:
                    current_price = _safe_get_price(kis, code)
                    logger.info(f"[📈 현재가] {code}: {current_price}")

                    trade_common = {
                        "datetime": now_str,
                        "code": code,
                        "name": name,
                        "qty": qty,
                        "K": k_value,
                        "target_price": target_price,
                        "strategy": strategy,
                    }

                    # --- 매수 ---
                    if is_open and code not in holding and code not in traded:
                        if current_price is not None and current_price >= float(target_price):
                            result = _with_retry(kis.buy_stock, code, qty)
                            holding[code] = {
                                'qty': int(qty),
                                'buy_price': float(current_price),
                                'trade_common': trade_common
                            }
                            traded[code] = {"buy_time": now_str, "qty": int(qty), "price": float(current_price)}
                            logger.info(f"[✅ 매수주문] {code}, qty={qty}, price={current_price}, result={result}")
                            log_trade({**trade_common, "side": "BUY", "price": current_price,
                                       "amount": int(current_price) * int(qty), "result": result})
                            save_state(holding, traded)
                            time.sleep(RATE_SLEEP_SEC)
                        else:
                            logger.info(f"[SKIP] {code}: 현재가({current_price}) < 목표가({target_price}), 미매수")
                            continue

                    # --- 익절/손절 매도 ---
                    if is_open and code in holding:
                        sellable_here = ord_psbl_map.get(code, 0)
                        if sellable_here <= 0:
                            logger.info(f"[SKIP] {code}: 매도가능수량=0 (대기/체결중/락) → 매도 보류")
                        else:
                            buy_info = holding[code]
                            buy_price = _to_float(buy_info.get('buy_price'))
                            bqty = _to_int(buy_info.get('qty'), 0)

                            if bqty <= 0 or buy_price is None or current_price is None:
                                logger.warning(f"[매도조건 판정불가] {code} qty={bqty}, buy_price={buy_price}, cur={current_price}")
                            else:
                                profit_pct = ((current_price - buy_price) / buy_price) * 100
                                if profit_pct >= sell_conditions['profit_pct'] or profit_pct <= sell_conditions['loss_pct']:
                                    sell_qty = min(bqty, sellable_here)
                                    cur_price, result = _sell_once(kis, code, sell_qty, prefer_market=True)
                                    logger.info(f"[✅ 매도주문] {code}, qty={sell_qty}, result={result}, 수익률: {profit_pct:.2f}%")
                                    log_trade({**trade_common, "side": "SELL", "price": cur_price,
                                               "amount": (int(cur_price) * int(sell_qty)) if cur_price else 0,
                                               "result": result,
                                               "reason": f"매도조건 (수익률: {profit_pct:.2f}%)"})
                                    holding.pop(code, None)
                                    traded.pop(code, None)
                                    save_state(holding, traded)
                                    time.sleep(RATE_SLEEP_SEC)

                except Exception as e:
                    logger.error(f"[❌ 주문/조회 실패] {code} : {e}")
                    continue

            # 장중 커트오프(KST) 강제 전량매도 — 안전: 운영자가 허용한 경우만 수행
            if is_open and now_dt_kst.time() >= SELL_FORCE_TIME:
                logger.info("[INFO] SELL_FORCE_TIME 도달 — 강제전량매도 체크")
                # 강제매도는 CLI --force-sell로만 실행하거나 env 허용시에만 자동 실행
                if args.force_sell or ALLOW_FORCE_SELL_ON_REBALANCE_FAIL:
                    _force_sell_all(
                        kis=kis,
                        holding=holding,
                        reason=f"장중 강제전량매도(커트오프 {SELL_FORCE_TIME.strftime('%H:%M')} KST)",
                        passes=FORCE_SELL_PASSES_CUTOFF,
                        include_all_balances=SELL_ALL_BALANCES_AT_CUTOFF,
                        prefer_market=True
                    )
                else:
                    logger.info("[INFO] 강제전량매도 비활성 (CLI/ENV 미허용)")

            # 장마감 전량매도 — 운영 정책에 따라 자동 수행 (여기선 기본 수행)
            if not is_open:
                logger.info("[INFO] 장마감 감지 — 전량매도 실행")
                _force_sell_all(
                    kis=kis,
                    holding=holding,
                    reason="장마감 전 강제전량매도",
                    passes=FORCE_SELL_PASSES_CLOSE,
                    include_all_balances=True,
                    prefer_market=True
                )
                logger.info("[✅ 장마감, 루프 종료]")
                break

            save_state(holding, traded)
            time.sleep(loop_sleep_sec)

    except KeyboardInterrupt:
        logger.info("[🛑 수동 종료]")


if __name__ == "__main__":
    main()

