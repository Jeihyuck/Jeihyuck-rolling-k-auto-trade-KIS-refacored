import logging
import requests
from trader.kis_wrapper import KisAPI
from datetime import datetime
import json
from pathlib import Path
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

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

def main():
    kis = KisAPI()
    rebalance_date = get_month_first_date()
    logger.info(f"[ℹ️ 리밸런싱 기준일]: {rebalance_date}")
    targets = fetch_rebalancing_targets(rebalance_date)
    is_open = kis.is_market_open()
    if is_open:
        logger.info("[⏰ 장 OPEN] 실시간 매수/매도 주문 실행")
    else:
        logger.info("[⏰ 장 종료] 실매수/매도 주문 생략, 현재가만 조회")

    holding = {}  # {code: {'qty': int, 'buy_price': float, ...}}
    sell_conditions = {  # 매도조건을 예시로 세팅 (목표수익률 3%, 손절 -2%)
        'profit_pct': 3.0,
        'loss_pct': -2.0
    }

    for target in targets:
        code = target.get("stock_code") or target.get("code")
        qty = target.get("매수수량") or target.get("qty")
        k_value = target.get("best_k") or target.get("K") or target.get("k")
        target_price = target.get("목표가") or target.get("target_price")
        strategy = target.get("strategy") or "전월 rolling K 최적화"
        name = target.get("name") or target.get("종목명")
        if not code or not qty:
            logger.error(f"[❌ 필수 값 없음] target={target}")
            continue
        time.sleep(0.3)
        try:
            current_price = kis.get_current_price(code)
            logger.info(f"[📈 현재가 조회] {code}: {current_price}원")

            trade_common = {
                "datetime": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "code": code,
                "name": name,
                "qty": qty,
                "K": k_value,
                "target_price": target_price,
                "strategy": strategy,
            }

            if is_open:
                # 실시간 매수: 목표가 돌파시 진입, holding dict 반영
                if current_price >= float(target_price) and code not in holding:
                    result = kis.buy_stock(code, qty)
                    holding[code] = {
                        'qty': int(qty),
                        'buy_price': float(current_price),
                        'trade_common': trade_common
                    }
                    logger.info(f"[✅ 매수주문 성공] 종목: {code}, 수량: {qty}, 응답: {result}")
                    trade = {
                        **trade_common,
                        "side": "BUY",
                        "price": current_price,
                        "amount": int(current_price) * int(qty),
                        "result": result
                    }
                    log_trade(trade)
                    time.sleep(0.3)
                # 실시간 매도: 보유 중, 매도조건 충족시 바로 매도
                elif code in holding:
                    buy_info = holding[code]
                    buy_price = buy_info['buy_price']
                    qty = buy_info['qty']
                    profit_pct = ((current_price - buy_price) / buy_price) * 100
                    # 매도 조건: 익절/손절
                    if profit_pct >= sell_conditions['profit_pct'] or profit_pct <= sell_conditions['loss_pct']:
                        result = kis.sell_stock(code, qty)
                        logger.info(f"[✅ 매도주문 성공] 종목: {code}, 수량: {qty}, 응답: {result}")
                        trade = {
                            **trade_common,
                            "side": "SELL",
                            "price": current_price,
                            "amount": int(current_price) * int(qty),
                            "result": result,
                            "reason": f"매도조건 충족 (수익률: {profit_pct:.2f}%)"
                        }
                        log_trade(trade)
                        del holding[code]
                        time.sleep(0.3)
                else:
                    logger.info(f"[SKIP] {code}: 현재가({current_price}) < 목표가({target_price}), 매수 미실행 & 미보유")
                    trade = {
                        **trade_common,
                        "side": "SKIP",
                        "price": current_price,
                        "amount": int(current_price) * int(qty),
                        "reason": "현재가 < 목표가, 매수 미실행"
                    }
                    log_trade(trade)
            else:
                logger.info(f"[🔔 장종료, 주문 SKIP] 종목: {code}, 목표가(매수수량): {target_price}({qty})")
                trade = {
                    **trade_common,
                    "side": "INFO",
                    "price": current_price,
                    "amount": int(current_price) * int(qty)
                }
                log_trade(trade)
        except Exception as e:
            logger.error(f"[❌ 주문/조회 실패] 종목: {code}, 오류: {e}")

    # 4. 장마감 시 미매도 종목 전량 시장가 매도 (실전 리스크 방지)
    if is_open:
        for code, info in holding.items():
            try:
                qty = info['qty']
                result = kis.sell_stock(code, qty)
                logger.info(f"[🏁 장마감 전량매도] {code}, 수량: {qty}, 응답: {result}")
                trade = {
                    **info['trade_common'],
                    "side": "SELL",
                    "price": kis.get_current_price(code),
                    "amount": kis.get_current_price(code) * qty,
                    "result": result,
                    "reason": "장마감 전 강제전량매도"
                }
                log_trade(trade)
            except Exception as e:
                logger.error(f"[❌ 장마감 전량매도 실패] 종목: {code}, 오류: {e}")

if __name__ == "__main__":
    main()
