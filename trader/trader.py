import logging
import requests
from .kis_wrapper import KisAPI
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo  # ✅ KST 비교를 위해 추가
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

# ====== 장중 강제 전량매도 커트오프 시각 설정 (KST 기준) ======
SELL_FORCE_TIME_STR = os.getenv("SELL_FORCE_TIME", "15:15").strip()
SELL_ALL_BALANCES_AT_CUTOFF = os.getenv("SELL_ALL_BALANCES_AT_CUTOFF", "false").lower() == "true"
RATE_SLEEP_SEC = float(os.getenv("API_RATE_SLEEP_SEC", "0.5"))  # 요청 간격

KST = ZoneInfo("Asia/Seoul")

def _parse_hhmm(hhmm: str) -> dtime:
    try:
        hh, mm = hhmm.split(":")
        return dtime(hour=int(hh), minute=int(mm))
    except Exception:
        logger.warning(f"[설정경고] SELL_FORCE_TIME 형식 오류 → 기본값 15:15 적용: {hhmm}")
        return dtime(hour=15, minute=15)

SELL_FORCE_TIME = _parse_hhmm(SELL_FORCE_TIME_STR)

def get_month_first_date():
    today = datetime.now(KST)  # ✅ 리밸런싱 기준일도 KST 기준
    month_first = today.replace(day=1)
    return month_first.strftime("%Y-%m-%d")

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

# ----- 네트워크/게이트웨이 오류 보강: 재시도 공용 함수 -----
def _with_retry(func, *args, max_retries=5, base_delay=0.6, **kwargs):
    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            last_err = e
            # IGW00008, SSLEOF, RemoteDisconnected 등은 일시 오류 가능성이 높음
            sleep_sec = base_delay * (1.6 ** (attempt - 1)) + random.uniform(0, 0.2)
            logger.error(f"[재시도 {attempt}/{max_retries}] {func.__name__} 실패: {e} → {sleep_sec:.2f}s 대기 후 재시도")
            time.sleep(sleep_sec)
    # 최종 실패
    raise last_err

def _sell_once(kis: KisAPI, code: str, qty: int, prefer_market=True):
    """
    시장가 매도를 선호. 래퍼가 market 옵션을 지원하면 사용,
    없으면 기존 sell_stock으로 폴백.
    """
    # 현재가 1회 조회(로깅·금액 계산용)
    cur_price = _with_retry(kis.get_current_price, code)

    try:
        if prefer_market and hasattr(kis, "sell_stock_market"):
            result = _with_retry(kis.sell_stock_market, code, qty)
        else:
            # 래퍼가 시장가 옵션을 지원하지 않는 경우 폴백
            result = _with_retry(kis.sell_stock, code, qty)
    except Exception as e:
        # 토큰 만료/세션 문제 가능성 → 토큰 갱신 후 1회 재도전
        logger.warning(f"[매도 재시도:토큰갱신] {code} qty={qty} err={e}")
        try:
            kis.refresh_token() if hasattr(kis, "refresh_token") else None
        except Exception:
            pass
        if prefer_market and hasattr(kis, "sell_stock_market"):
            result = _with_retry(kis.sell_stock_market, code, qty)
        else:
            result = _with_retry(kis.sell_stock, code, qty)

    logger.info(f"[매도호출] {code}, qty={qty}, price(log)={cur_price}, result={result}")
    return cur_price, result

def _force_sell_all(kis: KisAPI, holding: dict, traded: dict, balances: list, reason: str, prefer_market=True):
    """
    보유 전량 강제 매도.
    - 기본은 프로그램이 매수해 추적 중인 holding 대상
    - SELL_ALL_BALANCES_AT_CUTOFF=true 이면 계좌 잔고 전체 포함
    """
    # 매도 대상 집합 구성
    codes_to_sell = set(holding.keys())
    if SELL_ALL_BALANCES_AT_CUTOFF:
        for b in balances:
            code = b.get("pdno")
            if code and int(float(b.get("hldg_qty", 0))) > 0:
                codes_to_sell.add(code)

    if not codes_to_sell:
        return

    logger.info(f"[⚠️ 강제전량매도] 사유: {reason} / 대상 종목수: {len(codes_to_sell)} / 전체잔고포함={SELL_ALL_BALANCES_AT_CUTOFF}")

    for code in list(codes_to_sell):
        try:
            qty = None
            # 우선 balances에서 실제 수량 확인
            for b in balances:
                if b.get("pdno") == code:
                    qty = int(float(b.get("hldg_qty", 0)))
                    break
            if not qty or qty <= 0:
                logger.info(f"[스킵] {code}: 실제 잔고 수량 0")
                continue

            # trade_common 정보 확보
            tc = holding.get(code, {}).get("trade_common", {
                "datetime": datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S"),
                "code": code,
                "name": None,
                "qty": qty,
                "K": None,
                "target_price": None,
                "strategy": "강제전량매도"
            })

            cur_price, result = _sell_once(kis, code, qty, prefer_market=prefer_market)

            trade = {
                **tc,
                "side": "SELL",
                "price": cur_price,
                "amount": int(cur_price) * int(qty),
                "result": result,
                "reason": reason
            }
            log_trade(trade)

            # 상태 정리
            if code in holding:
                holding.pop(code, None)
            traded.pop(code, None)

            save_state(holding, traded)
            time.sleep(RATE_SLEEP_SEC)
        except Exception as e:
            logger.error(f"[❌ 강제매도 실패] {code} : {e}")

def main():
    kis = KisAPI()
    rebalance_date = get_month_first_date()
    logger.info(f"[ℹ️ 리밸런싱 기준일(KST)]: {rebalance_date}")
    logger.info(f"[⏱️ 커트오프(KST)] SELL_FORCE_TIME={SELL_FORCE_TIME.strftime('%H:%M')} / 전체잔고매도옵션={SELL_ALL_BALANCES_AT_CUTOFF}")

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

    # 기본 매도조건
    sell_conditions = {
        'profit_pct': 3.0,
        'loss_pct': -2.0
    }

    loop_sleep_sec = 3  # 루프 주기(초)

    try:
        while True:
            # 장 상태 및 현재시각(KST)
            is_open = kis.is_market_open()
            now_dt_kst = datetime.now(KST)
            now_str = now_dt_kst.strftime("%Y-%m-%d %H:%M:%S")
            logger.info(f"[⏰ 장상태] {'OPEN' if is_open else 'CLOSED'} / KST={now_str}")

            # ====== 현재 보유 현황 동기화 ======
            balances = []
            try:
                balances = _with_retry(kis.get_balance)  # output1만 사용
                logger.info(f"[보유잔고 API 결과 종목수] {len(balances)}개")
                for stock in balances:
                    logger.info(f"  [잔고] 종목: {stock.get('prdt_name')}, 코드: {stock.get('pdno')}, 보유수량: {stock.get('hldg_qty')}")
                # 보유 수량 0이면 holding에서 제거
                current_holding = {b['pdno']: int(float(b['hldg_qty'])) for b in balances if int(float(b.get('hldg_qty', 0))) > 0}
                for code in list(holding.keys()):
                    if code not in current_holding or current_holding[code] == 0:
                        logger.info(f"[보유종목 해제] {code} : 실제잔고 없음 → holding 제거")
                        holding.pop(code, None)
            except Exception as e:
                logger.error(f"[잔고조회 오류]{e}")

            # ====== 매수/매도 LOOP ======
            for code, target in code_to_target.items():
                qty = target.get("매수수량") or target.get("qty")
                k_value = target.get("best_k") or target.get("K") or target.get("k")
                target_price = target.get("목표가") or target.get("target_price")
                strategy = target.get("strategy") or "전월 rolling K 최적화"
                name = target.get("name") or target.get("종목명")

                try:
                    current_price = _with_retry(kis.get_current_price, code)
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

                    # --- 매수 시도 ---
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
                            trade = {
                                **trade_common,
                                "side": "BUY",
                                "price": current_price,
                                "amount": int(current_price) * int(qty),
                                "result": result
                            }
                            log_trade(trade)
                            save_state(holding, traded)
                            time.sleep(RATE_SLEEP_SEC)
                        else:
                            logger.info(f"[SKIP] {code}: 현재가({current_price}) < 목표가({target_price}), 미매수")
                            continue

                    # --- 매도 조건(익절/손절) ---
                    if is_open and code in holding:
                        buy_info = holding[code]
                        buy_price = buy_info['buy_price']
                        bqty = buy_info['qty']
                        profit_pct = ((current_price - buy_price) / buy_price) * 100
                        if profit_pct >= sell_conditions['profit_pct'] or profit_pct <= sell_conditions['loss_pct']:
                            cur_price, result = _sell_once(kis, code, bqty, prefer_market=True)
                            logger.info(f"[✅ 매도주문] {code}, qty={bqty}, result={result}, 수익률: {profit_pct:.2f}%")
                            trade = {
                                **trade_common,
                                "side": "SELL",
                                "price": cur_price,
                                "amount": int(cur_price) * int(bqty),
                                "result": result,
                                "reason": f"매도조건 (수익률: {profit_pct:.2f}%)"
                            }
                            log_trade(trade)
                            holding.pop(code, None)
                            traded.pop(code, None)
                            save_state(holding, traded)
                            time.sleep(RATE_SLEEP_SEC)

                except Exception as e:
                    logger.error(f"[❌ 주문/조회 실패] {code} : {e}")
                    continue

            # --- (신규) 장중 커트오프 시각(KST) 강제 전량매도 ---
            if is_open and holding and now_dt_kst.time() >= SELL_FORCE_TIME:
                _force_sell_all(
                    kis, holding, traded, balances,
                    reason=f"장중 강제전량매도(커트오프 {SELL_FORCE_TIME.strftime('%H:%M')} KST)",
                    prefer_market=True
                )
                # 이후에도 루프는 유지(상태 저장은 내부에서 수행)

            # --- 장마감시 전량매도 (기존 구조 유지, 더블세이프) ---
            if not is_open and holding:
                logger.info("[🏁 장마감, 전량 시장가 매도]")
                _force_sell_all(
                    kis, holding, traded, balances,
                    reason="장마감 전 강제전량매도",
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
