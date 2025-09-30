# trader.py
# Signals-only 변경: rebalance_api로부터 'signals' (stock_code, best_k, base_close/base_high/base_low 등)
# 를 받아오고, trader가 매일(장 개시 직전) 최신 전일 OHLC로 목표가를 계산하여 매수/수량 결정을 수행합니다.

import logging
import requests
from .kis_wrapper import KisAPI
from datetime import datetime, time as dtime, timedelta
from zoneinfo import ZoneInfo
import json
from pathlib import Path
import time
import os
import random

# (외부 의존) FinanceDataReader는 폴백으로 사용
try:
    from FinanceDataReader import DataReader
except Exception:
    DataReader = None

# 기본 로깅
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)

# 파일/상태
LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)
STATE_FILE = Path(__file__).parent / "trade_state.json"

# 시간대(KST)
KST = ZoneInfo("Asia/Seoul")

# 환경변수 / 운영 파라미터
SELL_FORCE_TIME_STR = os.getenv("SELL_FORCE_TIME", "11:15").strip()
SELL_ALL_BALANCES_AT_CUTOFF = os.getenv("SELL_ALL_BALANCES_AT_CUTOFF", "true").lower() == "true"
RATE_SLEEP_SEC = float(os.getenv("API_RATE_SLEEP_SEC", "0.5"))
FORCE_SELL_PASSES_CUTOFF = int(os.getenv("FORCE_SELL_PASSES_CUTOFF", "3"))
FORCE_SELL_PASSES_CLOSE = int(os.getenv("FORCE_SELL_PASSES_CLOSE", "5"))

# 자금/포지션 관련
TOTAL_CAPITAL = int(os.getenv("TOTAL_CAPITAL", "10000000"))
DAILY_CAPITAL = int(os.getenv("DAILY_CAPITAL", str(TOTAL_CAPITAL)))
MAX_POSITIONS = int(os.getenv("MAX_POSITIONS", "8"))
CAPITAL_PER_SYMBOL = int(os.getenv("CAPITAL_PER_SYMBOL", str(max(1, DAILY_CAPITAL // MAX_POSITIONS))))
MIN_QTY_PER_TICKET = int(os.getenv("MIN_QTY_PER_TICKET", "1"))
ORDER_THROTTLE_SEC = float(os.getenv("ORDER_THROTTLE_SEC", "0.3"))

# 목표가 산정 공식 기본값
TARGET_FORMULA = os.getenv("TARGET_FORMULA", "prev_close + best_k*(prev_high-prev_low)")

# 헬퍼: hh:mm 파싱
def _parse_hhmm(hhmm: str) -> dtime:
    try:
        hh, mm = hhmm.split(":")
        return dtime(hour=int(hh), minute=int(mm))
    except Exception:
        logger.warning(f"[설정경고] SELL_FORCE_TIME 형식 오류 → 기본값 15:15 적용: {hhmm}")
        return dtime(hour=15, minute=15)

SELL_FORCE_TIME = _parse_hhmm(SELL_FORCE_TIME_STR)

# 상태 저장/복구
def save_state(holding, traded):
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump({"holding": holding, "traded": traded}, f, ensure_ascii=False, indent=2)
    except Exception:
        logger.exception("[STATE_SAVE_FAIL]")


def load_state():
    if STATE_FILE.exists():
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                state = json.load(f)
                return state.get("holding", {}), state.get("traded", {})
        except Exception:
            logger.exception("[STATE_LOAD_FAIL]")
    return {}, {}


def log_trade(trade: dict):
    today = datetime.now(KST).strftime("%Y-%m-%d")
    logfile = LOG_DIR / f"trades_{today}.json"
    try:
        with open(logfile, "a", encoding="utf-8") as f:
            f.write(json.dumps(trade, ensure_ascii=False) + "\n")
    except Exception:
        logger.exception("[LOG_TRADE_FAIL]")

# 재시도 래퍼
def _with_retry(func, *args, max_retries=5, base_delay=0.6, **kwargs):
    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            last_err = e
            sleep_sec = base_delay * (1.6 ** (attempt - 1)) + random.uniform(0, 0.25)
            logger.error(f"[재시도 {attempt}/{max_retries}] {func.__name__} 실패: {e} → {sleep_sec:.2f}s 후 재시도")
            time.sleep(sleep_sec)
    raise last_err

# 안전한 현재가 조회
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

# 유틸: 안전한 정수/실수 변환
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

# 잔고 정규화: KisAPI 응답을 항상 List[dict]로
def _normalize_balances(raw):
    # raw may be list, dict with 'output1', or other shapes
    if raw is None:
        return []
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict):
        # common wrapper: {'output1': [ ... ]} or {'output': {'output1': [...]}}
        if "output1" in raw and isinstance(raw["output1"], list):
            return raw["output1"]
        # sometimes it's nested under 'output'
        if raw.get("output") and isinstance(raw["output"], dict) and isinstance(raw["output"].get("output1"), list):
            return raw["output"]["output1"]
        # legacy: some wrappers return list under 'output'
        if raw.get("output") and isinstance(raw.get("output"), list):
            return raw.get("output")
    # unknown -> return empty but log
    logger.warning(f"[BALANCE_NORMALIZE_WARN] unexpected balance format: {type(raw)}")
    return []

# 잔고 조회 통합
def _fetch_balances(kis: KisAPI):
    if hasattr(kis, "get_balance_all"):
        raw = _with_retry(kis.get_balance_all)
    else:
        raw = _with_retry(kis.get_balance)
    return _normalize_balances(raw)

# --- 목표가 계산 관련 ---
# signal: dict with at least stock_code, best_k; optionally base_close/base_high/base_low or base_close_date
# kis: KisAPI instance - for OHLC 폴백

def fetch_rebalancing_targets(date: str, kis: KisAPI):
    """
    rebalance API 호출: signals 우선 파싱.
    signals 항목을 받아와서, 각 종목에 대해 trader에서 목표가/수량을 계산하도록 정보를 보강해서 반환.
    """
    REBALANCE_API_URL = f"http://localhost:8000/rebalance/run/{date}?force_order=true"
    try:
        resp = requests.post(REBALANCE_API_URL, timeout=20)
    except Exception as e:
        raise Exception(f"리밸런싱 API 호출 실패: {e}")

    logger.info(f"[🛰️ 리밸런싱 API 전체 응답]: {resp.text}")

    if resp.status_code != 200:
        raise Exception(f"리밸런싱 API 호출 실패: {resp.status_code} {resp.text}")

    data = resp.json()
    # 표준: signals 우선, 그다음 selected/selected_stocks
    raw_signals = data.get("signals") or data.get("selected") or data.get("selected_stocks") or []

    logger.info(f"[🎯 리밸런싱 시그널 수]: {len(raw_signals)}")

    enriched = []
    for sig in raw_signals:
        # 표준화
        code = sig.get("stock_code") or sig.get("code") or sig.get("pdno")
        if not code:
            logger.warning(f"[SKIP_SIG] 코드 누락: {sig}")
            continue
        best_k = _to_float(sig.get("best_k") or sig.get("K") or sig.get("k"), None)
        base_close = _to_float(sig.get("base_close") or sig.get("base_close_price") or sig.get("종가"), None)
        base_high = _to_float(sig.get("base_high") or sig.get("base_high_price") or sig.get("고가"), None)
        base_low = _to_float(sig.get("base_low") or sig.get("base_low_price") or sig.get("저가"), None)
        base_date = sig.get("base_close_date") or sig.get("base_date")

        info = {
            "stock_code": code,
            "name": sig.get("name") or sig.get("종목명"),
            "best_k": best_k,
            "base_close": base_close,
            "base_high": base_high,
            "base_low": base_low,
            "base_date": base_date,
            "meta": sig.get("meta") or sig.get("메타"),
        }

        # 폴백: 필요한 OHLC가 빠지면 kis 또는 DataReader로 전일 OHLC 조회
        if base_close is None or base_high is None or base_low is None:
            try:
                ohlc = _get_prev_ohlc_for_code(kis, code, base_date)
                if ohlc:
                    info["base_close"] = info["base_close"] or ohlc.get("close")
                    info["base_high"] = info["base_high"] or ohlc.get("high")
                    info["base_low"] = info["base_low"] or ohlc.get("low")
                    info["base_date"] = info["base_date"] or ohlc.get("date")
                    logger.info(f"[OHLC_FALLBACK] {code} <- {ohlc}")
            except Exception as e:
                logger.warning(f"[OHLC_FALLBACK_FAIL] {code} : {e}")

        # 목표가 계산
        target_price = _compute_target_price(info)
        if target_price is None:
            logger.warning(f"[SKIP] {code}: 목표가 산정 불가 (필요 값 부족)")
            continue
        info["target_price"] = target_price

        # 매수수량 계산: 우선 CAPITAL_PER_SYMBOL 기준
        qty = max(int(CAPITAL_PER_SYMBOL // target_price), MIN_QTY_PER_TICKET)
        info["qty"] = qty
        # debug
        logger.info(f"[TARGET_CALC] {code} base_close={info.get('base_close')} base_high={info.get('base_high')} base_low={info.get('base_low')} best_k={best_k} -> target={target_price}, qty={qty}")

        enriched.append(info)

    return enriched


def _get_prev_ohlc_for_code(kis: KisAPI, code: str, base_date: str | None = None):
    """
    전일 OHLC를 얻는 폴백 함수
    우선 kis_wrapper의 helper 사용을 시도하고, 없으면 FinanceDataReader로 시도
    반환: {date: YYYY-MM-DD, open:..., high:..., low:..., close:...} 또는 None
    """
    # 1) KisAPI에 helper가 있으면 사용
    try:
        if hasattr(kis, "get_prev_day_ohlc"):
            res = _with_retry(kis.get_prev_day_ohlc, code, base_date)
            if res:
                # 예상 포맷을 표준화
                return {"date": res.get("date") or res.get("base_date"),
                        "open": _to_float(res.get("open")),
                        "high": _to_float(res.get("high")),
                        "low": _to_float(res.get("low")),
                        "close": _to_float(res.get("close"))}
    except Exception:
        logger.debug(f"[KIS_OHLC_FAIL] {code}")

    # 2) FinanceDataReader 폴백
    if DataReader is None:
        logger.debug("[DATAFALLBACK] FinanceDataReader 미사용 가능")
        return None

    try:
        # base_date가 주어지면 그 날짜의 종가, 고가, 저가를 사용하고
        # 없으면 최근 2 거래일 데이터를 가져와 전일 값 사용
        end_date = base_date or datetime.now(KST).strftime("%Y-%m-%d")
        start_date = (datetime.strptime(end_date, "%Y-%m-%d") - timedelta(days=7)).strftime("%Y-%m-%d")
        df = DataReader(code, start_date, end_date)
        if df is None or df.empty:
            return None
        df.index = df.index.astype("datetime64[ns]")
        # 최신 거래일을 골라 전일(가장 최근 인덱스 - 1) 값 사용
        df_sorted = df.sort_index()
        # 최근 행
        last = df_sorted.iloc[-1]
        # 만약 마지막 날짜가 오늘 장중이라면 전일값을 가져오기 위해 -1
        # 안전하게: 인덱스를 -1이 아닌 -2로 시도
        if len(df_sorted) >= 2:
            prev = df_sorted.iloc[-2]
            return {"date": str(df_sorted.index[-2].date()),
                    "open": _to_float(prev.get("Open") or prev.get("open")),
                    "high": _to_float(prev.get("High") or prev.get("high")),
                    "low": _to_float(prev.get("Low") or prev.get("low")),
                    "close": _to_float(prev.get("Close") or prev.get("close"))}
        else:
            # 데이터가 하나뿐이면 그 값을 사용
            return {"date": str(df_sorted.index[-1].date()),
                    "open": _to_float(last.get("Open") or last.get("open")),
                    "high": _to_float(last.get("High") or last.get("high")),
                    "low": _to_float(last.get("Low") or last.get("low")),
                    "close": _to_float(last.get("Close") or last.get("close"))}
    except Exception as e:
        logger.exception(f"[DATA_READER_FAIL] {code} : {e}")
        return None


def _compute_target_price(info: dict):
    """
    기본 전략: target = prev_close + best_k * (prev_high - prev_low)
    info는 base_close/base_high/base_low와 best_k를 포함해야 함
    반환: int(rounded) 또는 None
    """
    best_k = _to_float(info.get("best_k"), None)
    prev_close = _to_float(info.get("base_close"), None)
    prev_high = _to_float(info.get("base_high"), None)
    prev_low = _to_float(info.get("base_low"), None)

    if best_k is None or prev_close is None or prev_high is None or prev_low is None:
        return None

    try:
        target = prev_close + best_k * (prev_high - prev_low)
        # 전략적으로 반올림: 소수 없애고 정수로
        tp = int(round(target))
        if tp <= 0:
            return None
        return tp
    except Exception:
        return None

# --- 강제 전량매도 로직 (기존 유지) ---

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

    for code in list(holding.keys()):
        holding.pop(code, None)
    save_state(holding, {})

# ------------------ 메인 루프 ------------------

def main(force_sell=False):
    kis = KisAPI()
    rebalance_date = datetime.now(KST).replace(day=1).strftime("%Y-%m-%d")
    logger.info(f"[ℹ️ 리밸런싱 기준일(KST)]: {rebalance_date}")
    logger.info(f"[⏱️ 커트오프(KST)] SELL_FORCE_TIME={SELL_FORCE_TIME.strftime('%H:%M')} / 전체잔고매도={SELL_ALL_BALANCES_AT_CUTOFF} / 패스(커트오프/마감)={FORCE_SELL_PASSES_CUTOFF}/{FORCE_SELL_PASSES_CLOSE}")

    holding, traded = load_state()
    logger.info(f"[상태복구] holding: {list(holding.keys())}, traded: {list(traded.keys())}")

    # 리밸런싱 신호 가져오기 & 트레이더 내부 목표가/수량 산정
    try:
        targets = fetch_rebalancing_targets(rebalance_date, kis)
    except Exception as e:
        logger.error(f"[REBALANCE_FETCH_FAIL] {e}")
        targets = []

    # code -> target map
    code_to_target = {t['stock_code']: t for t in targets}

    # 매수/매도 조건
    sell_conditions = {
        'profit_pct': float(os.getenv('PROFIT_PCT', '3.0')),
        'loss_pct': float(os.getenv('LOSS_PCT', '-2.0'))
    }

    loop_sleep_sec = 3

    # FORCE_SELL option: 즉시 강제 매도 후 종료
    if force_sell:
        logger.info("[FORCE_SELL_MODE] 즉시 강제전량매도 실행")
        _force_sell_all(kis=kis, holding=holding, reason="수동 강제매도", passes=FORCE_SELL_PASSES_CLOSE, include_all_balances=True)
        return

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
                for b in balances:
                    logger.info(f"  [잔고] 종목: {b.get('prdt_name')}, 코드: {b.get('pdno')}, 보유수량: {b.get('hldg_qty')}, 매도가능: {b.get('ord_psbl_qty')}")
                current_holding = {b.get('pdno'): _to_int(b.get('hldg_qty', 0)) for b in balances if _to_int(b.get('hldg_qty', 0)) > 0}
                ord_psbl_map = {b.get('pdno'): _to_int(b.get('ord_psbl_qty', 0)) for b in balances}
                for code in list(holding.keys()):
                    if code not in current_holding or current_holding[code] == 0:
                        logger.info(f"[보유종목 해제] {code} : 실제잔고 없음 → holding 제거")
                        holding.pop(code, None)
            except Exception as e:
                logger.error(f"[잔고조회 오류]{e}")

            # 매수/매도 전략 루프
            for code, target in code_to_target.items():
                try:
                    qty = _to_int(target.get('qty', 0))
                    if qty <= 0:
                        logger.info(f"[SKIP] {code}: 매수수량 없음/0")
                        continue

                    k_value = target.get('best_k')
                    target_price = _to_float(target.get('target_price'))
                    strategy = target.get('strategy') or "전월 rolling K 최적화"
                    name = target.get('name')

                    if target_price is None:
                        logger.warning(f"[SKIP] {code}: target_price 누락")
                        continue

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

                    # 매수: 장중이고 미보유/미거래
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
                            time.sleep(ORDER_THROTTLE_SEC)
                        else:
                            logger.info(f"[SKIP] {code}: 현재가({current_price}) < 목표가({target_price}), 미매수")

                    # 익절/손절 매도
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

            # 장중 커트오프 강제 전량매도
            if is_open and now_dt_kst.time() >= SELL_FORCE_TIME:
                _force_sell_all(
                    kis=kis,
                    holding=holding,
                    reason=f"장중 강제전량매도(커트오프 {SELL_FORCE_TIME.strftime('%H:%M')} KST)",
                    passes=FORCE_SELL_PASSES_CUTOFF,
                    include_all_balances=SELL_ALL_BALANCES_AT_CUTOFF,
                    prefer_market=True
                )

            # 장마감 전량매도
            if not is_open:
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
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--force-sell", action="store_true", help="즉시 강제전량매도 후 종료")
    args = p.parse_args()
    main(force_sell=args.force_sell)
