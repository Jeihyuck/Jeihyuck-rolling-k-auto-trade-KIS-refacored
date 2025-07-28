import logging
import requests
from kis_wrapper import KisAPI
from datetime import datetime
import json
from pathlib import Path

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
    # 일자별 jsonl(한줄에 한 dict)로 기록
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
        logger.info("[⏰ 장 OPEN] 실매수 주문 실행")
    else:
        logger.info("[⏰ 장 종료] 실매수 주문 생략, 현재가만 조회")

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
                # 🔥 목표가 도달 조건 추가!
                if current_price >= float(target_price):
                    result = kis.buy_stock(code, qty)
                    logger.info(f"[✅ 매수주문 성공] 종목: {code}, 수량: {qty}, 응답: {result}")
                    # 매수 로그 기록
                    trade = {
                        **trade_common,
                        "side": "BUY",
                        "price": current_price,
                        "amount": int(current_price) * int(qty),
                        "result": result
                    }
                    log_trade(trade)
                else:
                    logger.info(f"[SKIP] {code}: 현재가({current_price}) < 목표가({target_price}), 매수 미실행")
                    # 기록도 남길 수 있음 (원하면 아래 코드 주석 해제)
                    trade = {
                        **trade_common,
                        "side": "SKIP",
                        "price": current_price,
                        "amount": int(current_price) * int(qty),
                        "reason": f"현재가 < 목표가, 매수 미실행"
                    }
                    log_trade(trade)
            else:
                logger.info(f"[🔔 장종료, 주문 SKIP] 종목: {code}, 목표가(매수수량): {target_price}({qty})")
                # 장종료에도 조회/기록 가능
                trade = {
                    **trade_common,
                    "side": "INFO",
                    "price": current_price,
                    "amount": int(current_price) * int(qty)
                }
                log_trade(trade)
        except Exception as e:
            logger.error(f"[❌ 주문/조회 실패] 종목: {code}, 오류: {e}")

if __name__ == "__main__":
    main()
