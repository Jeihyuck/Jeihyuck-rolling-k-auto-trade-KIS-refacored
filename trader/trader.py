import logging
import requests
from .kis_wrapper import KisAPI
from datetime import datetime, time as dtime
import json
from pathlib import Path
import time
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)
STATE_FILE = Path(__file__).parent / "trade_state.json"

# ====== 장중 강제 전량매도 커트오프 시각 설정 ======
# 예) "14:30"으로 설정하면 14:30 이후 장중에 보유 종목 전량 매도
SELL_FORCE_TIME_STR = os.getenv("SELL_FORCE_TIME", "14:30").strip()

def _parse_hhmm(hhmm: str) -> dtime:
    try:
        hh, mm = hhmm.split(":")
        return dtime(hour=int(hh), minute=int(mm))
    except Exception:
        logger.warning(f"[설정경고] SELL_FORCE_TIME 형식이 잘못되어 기본값 14:30 사용: {hhmm}")
        return dtime(hour=14, minute=30)

SELL_FORCE_TIME = _parse_hhmm(SELL_FORCE_TIME_STR)

def get_month_first_date():
    today = datetime.today()
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
    today = datetime.now().strftime("%Y-%m-%d")
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

def _force_sell_all(kis: KisAPI, holding: dict, traded: dict, reason: str):
    """보유 전량 강제 매도(공용 함수)"""
    if not holding:
        return
    logger.info(f"[⚠️ 강제전량매도] 사유: {reason} / 대상 종목수: {len(holding)}")
    for code in list(holding.keys()):
        try:
            info = holding[code]
            qty = info['qty']
            # 현재가 1회 조회해 기록 일관성 확보
            cur_price = kis.get_current_price(code)
            result = kis.sell_stock(code, qty)
            logger.info(f"[강제매도] {code}, qty={qty}, price={cur_price}, result={result}")
            trade = {
                **info['trade_common'],
                "side": "SELL",
                "price": cur_price,
                "amount": int(cur_price) * int(qty),
                "result": result,
                "reason": reason
            }
            log_trade(trade)
            holding.pop(code)
            traded.pop(code, None)
            save_state(holding, traded)
            time.sleep(0.25)
        except Exception as e:
            logger.error(f"[❌ 강제매도 실패] {code} : {e}")

def main():
    kis = KisAPI()
    rebalance_date = get_month_first_date()
    logger.info(f"[ℹ️ 리밸런싱 기준일]: {rebalance_date}")
    logger.info(f"[⏱️ 커트오프 시각] SELL_FORCE_TIME={SELL_FORCE_TIME.strftime('%H:%M')}")

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

    is_open = kis.is_market_open()
    if is_open:
        logger.info("[⏰ 장 OPEN] 실시간 매수/매도 주문 실행")
    else:
        logger.info("[⏰ 장 종료] 실매수/매도 주문 생략, 현재가만 조회")

    sell_conditions = {
        'profit_pct': 3.0,
        'loss_pct': -2.0
    }

    loop_sleep_sec = 3  # 루프 주기(초)

    try:
        while True:
            is_open = kis.is_market_open()
            logger.info(f"[⏰ 장상태] {'OPEN' if is_open else 'CLOSED'}")
            now_dt = datetime.now()
            now_str = now_dt.strftime("%Y-%m-%d %H:%M:%S")

            # ====== 현재 보유 현황 API로 동기화 ======
            try:
                balances = kis.get_balance()  # output1만 사용
                logger.info(f"[보유잔고 API 결과 종목수] {len(balances)}개")
                for stock in balances:
                    logger.info(f"  [잔고] 종목: {stock.get('prdt_name')}, 코드: {stock.get('pdno')}, 보유수량: {stock.get('hldg_qty')}")
                current_holding = {b['pdno']: int(float(b['hldg_qty'])) for b in balances if int(float(b.get('hldg_qty', 0))) > 0}
                # 보유 수량 0이면 holding에서 제거
                for code in list(holding.keys()):
                    if code not in current_holding or current_holding[code] == 0:
                        logger.info(f"[보유종목 해제] {code} : 실제잔고 없음, holding에서 제거")
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
                    current_price = kis.get_current_price(code)
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
                            result = kis.buy_stock(code, qty)
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
                            time.sleep(0.25)
                        else:
                            logger.info(f"[SKIP] {code}: 현재가({current_price}) < 목표가({target_price}), 미매수")
                            continue

                    # --- 매도 조건 확인 및 실행(익절/손절) ---
                    if is_open and code in holding:
                        buy_info = holding[code]
                        buy_price = buy_info['buy_price']
                        bqty = buy_info['qty']
                        profit_pct = ((current_price - buy_price) / buy_price) * 100
                        if profit_pct >= sell_conditions['profit_pct'] or profit_pct <= sell_conditions['loss_pct']:
                            result = kis.sell_stock(code, bqty)
                            logger.info(f"[✅ 매도주문] {code}, qty={bqty}, result={result}, 수익률: {profit_pct:.2f}%")
                            trade = {
                                **trade_common,
                                "side": "SELL",
                                "price": current_price,
                                "amount": int(current_price) * int(bqty),
                                "result": result,
                                "reason": f"매도조건 (수익률: {profit_pct:.2f}%)"
                            }
                            log_trade(trade)
                            holding.pop(code)
                            traded.pop(code, None)
                            save_state(holding, traded)
                            time.sleep(0.25)

                except Exception as e:
                    logger.error(f"[❌ 주문/조회 실패] {code} : {e}")
                    continue

            # --- (신규) 장중 커트오프 시각 강제 전량매도 ---
            # 장이 열려 있고, 현재 시간이 SELL_FORCE_TIME 이후이면 강제 전량 매도
            if is_open and holding and now_dt.time() >= SELL_FORCE_TIME:
                _force_sell_all(
                    kis,
                    holding,
                    traded,
                    reason=f"장중 강제전량매도(커트오프 {SELL_FORCE_TIME.strftime('%H:%M')})"
                )
                # 커트오프 매도 후에도 루프는 계속 돌아 다음 날/상태를 관리

            # --- 장마감시 전량매도 (기존 구조 유지) ---
            if not is_open and holding:
                logger.info("[🏁 장마감, 전량 시장가 매도]")
                _force_sell_all(kis, holding, traded, reason="장마감 전 강제전량매도")
                logger.info("[✅ 장마감, 루프 종료]")
                break

            save_state(holding, traded)
            time.sleep(loop_sleep_sec)

    except KeyboardInterrupt:
        logger.info("[🛑 수동 종료]")

if __name__ == "__main__":
    main()
