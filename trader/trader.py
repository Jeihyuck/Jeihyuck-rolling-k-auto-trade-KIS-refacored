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
import pandas as pd

# 외부 데이터 소스 폴백
from FinanceDataReader import DataReader

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

# 기본 전체 포트폴리오 투자금 (rebalancer와 동일한 상수 사용)
TOTAL_CAPITAL = 10_000_000


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
    /rebalance/run/{date} 호출하여 시그널(목록)을 가져온다.
    시그널은 목표가를 포함하지 않으므로 trader가 매일 목표가를 계산해야 한다.
    """
    REBALANCE_API_URL = f"http://localhost:8000/rebalance/run/{date}?force_order=true"
    response = requests.post(REBALANCE_API_URL)
    logger.info(f"[🛰️ 리밸런싱 API 전체 응답]: {response.text}")
    if response.status_code == 200:
        data = response.json()
        signals = data.get('signals') or data.get('selected') or data.get('selected_stocks') or []
        logger.info(f"[🎯 리밸런싱 시그널]: {signals}")
        return signals
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
        # 가격가드: 0.0 / 음수 / 비정상은 None 처리
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


def _fetch_balances(kis: KisAPI):
    if hasattr(kis, "get_balance_all"):
        return _with_retry(kis.get_balance_all)
    return _with_retry(kis.get_balance)


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


# ---- 추가된 부분: trader가 매일 목표가를 계산하기 위한 헬퍼들 ----

def _get_prev_day_ohlc(kis: KisAPI, code: str, ref_date: str | None = None):
    """
    가능한 경우 먼저 KisAPI 제공 메서드로 전일 OHLC를 얻어오고, 없으면 FinanceDataReader(DataReader)로 폴백한다.
    ref_date: 기준일(YYYY-MM-DD). None이면 오늘 기준으로 가장 최근 영업일 전일을 조회.
    반환: dict with keys: date, open, high, low, close
    """
    # 1) kis에 관련 메서드가 있으면 시도
    candidates = [
        "get_prev_ohlc",
        "get_previous_day_ohlc",
        "get_ohlc",
        "get_daily_ohlc",
        "get_price_history",
    ]
    for m in candidates:
        try:
            if hasattr(kis, m):
                func = getattr(kis, m)
                # try with/without date param
                try:
                    res = func(code, ref_date) if ref_date is not None else func(code)
                except TypeError:
                    res = func(code)
                if res and isinstance(res, dict):
                    return {
                        "date": res.get("date") or res.get("base_close_date") or None,
                        "open": _to_float(res.get("open") or res.get("Open")),
                        "high": _to_float(res.get("high") or res.get("High")),
                        "low": _to_float(res.get("low") or res.get("Low")),
                        "close": _to_float(res.get("close") or res.get("Close")),
                    }
        except Exception:
            continue

    # 2) FinanceDataReader로 폴백
    try:
        # ref_date 이전 7일치 데이터 확보
        if ref_date is None:
            end = datetime.now().strftime("%Y-%m-%d")
        else:
            end = ref_date
        start = (datetime.strptime(end, "%Y-%m-%d") - timedelta(days=7)).strftime("%Y-%m-%d")
        df = DataReader(code, start, end)
        if df is None or df.empty:
            return {"date": None, "open": None, "high": None, "low": None, "close": None}
        df.index = pd.to_datetime(df.index)
        df_filtered = df[df.index <= pd.to_datetime(end)]
        if df_filtered.empty:
            return {"date": None, "open": None, "high": None, "low": None, "close": None}
        row = df_filtered.iloc[-1]
        return {
            "date": str(row.name.date()),
            "open": _to_float(row.get("Open")),
            "high": _to_float(row.get("High")),
            "low": _to_float(row.get("Low")),
            "close": _to_float(row.get("Close") or row.get("Adj Close")),
        }
    except Exception as e:
        logger.warning(f"[WARN] prev day OHLC 조회 실패(DataReader 폴백): {code} {e}")
        return {"date": None, "open": None, "high": None, "low": None, "close": None}


# ----- main loop -----

def main():
    kis = KisAPI()
    rebalance_date = get_month_first_date()
    logger.info(f"[ℹ️ 리밸런싱 기준일(KST)]: {rebalance_date}")
    logger.info(f"[⏱️ 커트오프(KST)] SELL_FORCE_TIME={SELL_FORCE_TIME.strftime('%H:%M')} / 전체잔고매도={SELL_ALL_BALANCES_AT_CUTOFF} / "
                f"패스(커트오프/마감)={FORCE_SELL_PASSES_CUTOFF}/{FORCE_SELL_PASSES_CLOSE}")

    # ======== 상태 복구 ========
    holding, traded = load_state()
    logger.info(f"[상태복구] holding: {list(holding.keys())}, traded: {list(traded.keys())}")

    # ======== 리밸런싱 대상 종목 추출 (시그널 수신) ========
    signals = fetch_rebalancing_targets(rebalance_date)

    # 시그널 형태 검사 및 code->signal 매핑
    code_to_signal = {}
    for s in signals:
        code = s.get("stock_code") or s.get("code")
        if not code:
            continue
        code_to_signal[code] = s

    # ======== 트레이더 측에서 매일 목표가(타깃) 계산 및 매수수량 산정 ========
    # - 공식 예시: target_price = prev_close + best_k * (prev_high - prev_low)
    # - per-stock allocation: TOTAL_CAPITAL / n_signals
    n_signals = len(code_to_signal)
    per_invest = TOTAL_CAPITAL // n_signals if n_signals > 0 else 0

    for code, sig in list(code_to_signal.items()):
        best_k = sig.get("best_k") or sig.get("K") or sig.get("k")
        try:
            best_k = float(best_k) if best_k is not None else None
        except Exception:
            best_k = None

        # 우선 제공된 base OHLC(리밸런서가 포함해주었을 경우)를 사용
        base_close = sig.get("base_close")
        base_high = sig.get("base_high")
        base_low = sig.get("base_low")
        base_date = sig.get("base_close_date")

        # 부족하면 KisAPI/DataReader로 최신 전일 OHLC를 다시 조회
        if base_close is None or base_high is None or base_low is None:
            ohlc = _get_prev_day_ohlc(kis, code, ref_date=base_date or rebalance_date)
            base_close = base_close or ohlc.get("close")
            base_high = base_high or ohlc.get("high")
            base_low = base_low or ohlc.get("low")
            base_date = base_date or ohlc.get("date")

        # 목표가 계산
        target_price = None
        if base_close is not None and base_high is not None and base_low is not None and best_k is not None:
            try:
                target_price = float(base_close) + float(best_k) * (float(base_high) - float(base_low))
                # 소수 반올림 (int 단위)
                target_price = int(round(target_price))
            except Exception as e:
                logger.warning(f"[WARN] 목표가 계산 실패: {code} base_close={base_close} high={base_high} low={base_low} best_k={best_k} err={e}")
                target_price = None

        # 매수수량 산정 (보수적으로 최소 1주)
        qty = 0
        if target_price is not None and per_invest > 0:
            qty = max(per_invest // target_price, 1)

        # trader 루프에서 기존 코드들과 호환되도록 필드명 맞춤
        sig["target_price"] = target_price
        sig["목표가"] = target_price
        sig["매수수량"] = qty

    # 변환 완료된 code_to_target 자료구조 생성
    code_to_target = {code: sig for code, sig in code_to_signal.items()}

    # 기본 매도조건(익절/손절)
    sell_conditions = {
        'profit_pct': 3.0,   # +3% 이상 익절
        'loss_pct':  -2.0    # -2% 이하 손절
    }

    loop_sleep_sec = 3

    try:
        while True:
            is_open = kis.is_market_open()
            now_dt_kst = datetime.now(KST)
            now_str = now_dt_kst.strftime("%Y-%m-%d %H:%M:%S")
            logger.info(f"[⏰ 장상태] {'OPEN' if is_open else 'CLOSED'} / KST={now_str}")

            # ====== 잔고 동기화 ======
            ord_psbl_map = {}
            try:
                balances = _fetch_balances(kis)
                logger.info(f"[보유잔고 API 결과 종목수] {len(balances)}개")
                for stock in balances:
                    logger.info(
                        f"  [잔고] 종목: {stock.get('prdt_name')}, 코드: {stock.get('pdno')}, "
                        f"보유수량: {stock.get('hldg_qty')}, 매도가능: {stock.get('ord_psbl_qty')}"
                    )
                current_holding = {b['pdno']: _to_int(b.get('hldg_qty', 0)) for b in balances if _to_int(b.get('hldg_qty', 0)) > 0}
                ord_psbl_map = {b['pdno']: _to_int(b.get('ord_psbl_qty', 0)) for b in balances}
                for code in list(holding.keys()):
                    if code not in current_holding or current_holding[code] == 0:
                        logger.info(f"[보유종목 해제] {code} : 실제잔고 없음 → holding 제거")
                        holding.pop(code, None)
            except Exception as e:
                logger.error(f"[잔고조회 오류]{e}")

            # ====== 매수/매도(전략) LOOP ======
            for code, target in code_to_target.items():
                # 입력 방어
                qty = _to_int(target.get("매수수량") or target.get("qty"), 0)
                if qty <= 0:
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
            time.sleep(loop_sleep_sec)

    except KeyboardInterrupt:
        logger.info("[🛑 수동 종료]")


if __name__ == "__main__":
    main()
   