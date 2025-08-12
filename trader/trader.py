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
FORCE_SELL_PASSES_CLOSE  = int(os.getenv("FORCE_SELL_PASSES_CLOSE",  "5"))

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
    """현재가 조회 실패/무효(<=0) 시 None 반환하여 의사결정에서 제외."""
    try:
        price = _with_retry(kis.get_current_price, code)
        if price is None or price <= 0:
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
    """
    1회 매도 시도(시장가 우선). 현재가는 로깅용이며 실패해도 매도 시도는 진행.
    실패 시 토큰 갱신 후 1회 추가 재시도.
    - KisAPI에 sell_stock_market이 있으면 우선 사용
    - 없으면 sell_stock(지정가)로 폴백
    """
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
    """
    잔고 조회 통합: list[dict]를 항상 반환하도록 호환 처리.
    - KisAPI.get_balance_all()가 있으면 그대로 사용(positions list를 돌려준다고 가정)
    - 없으면 KisAPI.get_balance() 호출 후 dict면 ['positions']를, list면 그대로 반환
    - 최후 폴백: KisAPI.get_positions()
    """
    if hasattr(kis, "get_balance_all"):
        bal = _with_retry(kis.get_balance_all)
        return bal if isinstance(bal, list) else (bal.get("positions", []) if isinstance(bal, dict) else [])

    try:
        bal = _with_retry(kis.get_balance)
        if isinstance(bal, list):
            return bal
        if isinstance(bal, dict):
            return bal.get("positions", [])
    except Exception as e:
        logger.warning(f"[get_balance 실패, get_positions 폴백] {e}")

    # 최후 폴백
    if hasattr(kis, "get_positions"):
        return _with_retry(kis.get_positions)
    return []


def _force_sell_pass(kis: KisAPI, targets_codes: set, reason: str, prefer_market=True):
    """
    주어진 코드 집합에 대해 1 패스 매도 시도.
    실제 잔고 수량 0이거나 매도 성공 시 집합에서 제거.
    실패/잔존은 다음 패스에서 재시도.
    """
    if not targets_codes:
        return set()

    # 대상 집합 방어: None/공백 제거
    targets_codes = {c for c in targets_codes if c}

    balances = _fetch_balances(kis)
    qty_map = {b.get("pdno"): _to_int(b.get("hldg_qty", 0)) for b in balances}
    remaining = set()

    for code in list(targets_codes):
        qty = qty_map.get(code, 0)
        if qty <= 0:
            logger.info(f"[스킵] {code}: 실제 잔고 수량 0")
            continue

        cur_price, result = _sell_once(kis, code, qty, prefer_market=prefer_market)
        log_trade({
            "datetime": datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S"),
            "code": code,
            "name": None,
            "qty": qty,
            "K": None,
            "target_price": None,
            "strategy": "강제전량매도",
            "side": "SELL",
            "price": cur_price if cur_price is not None else 0,
            "amount": (_to_int(cur_price, 0) * int(qty)) if cur_price is not None else 0,
            "result": result,
            "reason": reason
        })
        time.sleep(RATE_SLEEP_SEC)

    # 1 패스 후 재조회로 잔존 파악
    balances_after = _fetch_balances(kis)
    after_qty_map = {b.get("pdno"): _to_int(b.get("hldg_qty", 0)) for b in balances_after}

    for code in targets_codes:
        if after_qty_map.get(code, 0) > 0:
            remaining.add(code)

    return remaining


def _force_sell_all(kis: KisAPI, holding: dict, reason: str, passes: int, include_all_balances: bool, prefer_market=True):
    """
    강제 전량 매도(여러 패스로 견고하게).
    - include_all_balances=True 이면 계좌 잔고 전체 대상
    - False 이면 holding에 등록된 종목만 대상
    """
    # 초기 대상 집합 구성
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

    # 상태 정리: holding에서 제거(혹시 남아있더라도 이후 루프에서 잔고동기화로 해제됨)
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

    # ======== 리밸런싱 대상 종목 추출 ========
    targets = fetch_rebalancing_targets(rebalance_date)
    code_to_target = {}
    for target in targets:
        code = target.get("stock_code") or target.get("code")
        if code:
            code_to_target[code] = target

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
            try:
                balances = _fetch_balances(kis)
                logger.info(f"[보유잔고 API 결과 종목수] {len(balances)}개")
                for stock in balances:
                    logger.info(f"  [잔고] 종목: {stock.get('prdt_name')}, 코드: {stock.get('pdno')}, 보유수량: {stock.get('hldg_qty')}")
                current_holding = {b['pdno']: _to_int(b.get('hldg_qty', 0)) for b in balances if _to_int(b.get('hldg_qty', 0)) > 0}
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
                    if current_price is None:
                        logger.info(f"[SKIP] {code}: 현재가 무효(NaN/<=0)")
                        continue

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
                        if current_price >= float(target_price):
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
                        buy_info = holding[code]
                        buy_price = _to_float(buy_info.get('buy_price'))
                        bqty = _to_int(buy_info.get('qty'), 0)

                        if bqty <= 0 or buy_price is None or current_price is None:
                            logger.warning(f"[매도조건 판정불가] {code} qty={bqty}, buy_price={buy_price}, cur={current_price}")
                        else:
                            profit_pct = ((current_price - buy_price) / buy_price) * 100
                            if profit_pct >= sell_conditions['profit_pct'] or profit_pct <= sell_conditions['loss_pct']:
                                cur_price, result = _sell_once(kis, code, bqty, prefer_market=True)
                                logger.info(f"[✅ 매도주문] {code}, qty={bqty}, result={result}, 수익률: {profit_pct:.2f}%")
                                log_trade({**trade_common, "side": "SELL", "price": cur_price,
                                           "amount": (int(cur_price) * int(bqty)) if cur_price else 0,
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
            time.sleep(loop_sleep_sec)

    except KeyboardInterrupt:
        logger.info("[🛑 수동 종료]")


if __name__ == "__main__":
    main()


