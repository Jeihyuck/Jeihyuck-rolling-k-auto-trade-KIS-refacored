import logging, requests
from kis_wrapper import KisAPI
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fetch_rebalancing_targets(date):
    REBALANCE_API_URL = f"http://localhost:8000/rebalance/run/{date}"
    # ⚠️ [GET → POST로 변경] ⚠️
    response = requests.post(REBALANCE_API_URL)
    if response.status_code == 200:
        data = response.json()
        logger.info(f"[🎯 리밸런싱 종목]: {data['selected']}")
        return data["selected"]
    else:
        raise Exception(f"리밸런싱 API 호출 실패: {response.text}")

def main():
    kis = KisAPI()
    today = datetime.today().strftime("%Y%m%d")
    targets = fetch_rebalancing_targets(today)

    for target in targets:
        code = target["종목코드"]
        qty = target["매수수량"]
        try:
            current_price = kis.get_current_price(code)
            logger.info(f"[📈 현재가 조회] {code}: {current_price}원")

            result = kis.buy_stock(code, qty)
            logger.info(f"[✅ 매수주문 성공] 종목: {code}, 수량: {qty}, 응답: {result}")

        except Exception as e:
            logger.error(f"[❌ 주문 실패] 종목: {code}, 오류: {e}")

if __name__ == "__main__":
    main()
