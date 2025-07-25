import logging
import requests
from kis_wrapper import KisAPI
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_month_first_date():
    today = datetime.today()
    month_first = today.replace(day=1)
    return month_first.strftime("%Y-%m-%d")

def fetch_rebalancing_targets(date):
    REBALANCE_API_URL = f"http://localhost:8000/rebalance/run/{date}?force_order=false"
    response = requests.post(REBALANCE_API_URL)
    logger.info(f"[🛰️ 리밸런싱 API 전체 응답]: {response.text}")
    if response.status_code == 200:
        data = response.json()
        logger.info(f"[🎯 리밸런싱 종목]: {data.get('selected') or data.get('selected_stocks')}")
        return data.get("selected") or data.get("selected_stocks") or []
    else:
        raise Exception(f"리밸런싱 API 호출 실패: {response.text}")

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
        if not code or not qty:
            logger.error(f"[❌ 필수 값 없음] target={target}")
            continue
        try:
            current_price = kis.get_current_price(code)
            logger.info(f"[📈 현재가 조회] {code}: {current_price}원")
            if is_open:
                result = kis.buy_stock(code, qty)
                logger.info(f"[✅ 매수주문 성공] 종목: {code}, 수량: {qty}, 응답: {result}")
            else:
                logger.info(f"[🔔 장종료, 주문 SKIP] 종목: {code}, 목표가(매수수량): {target.get('목표가')}({qty})")
        except Exception as e:
            logger.error(f"[❌ 주문/조회 실패] 종목: {code}, 오류: {e}")

if __name__ == "__main__":
    main()
