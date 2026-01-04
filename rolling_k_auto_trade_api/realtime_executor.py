import time
from datetime import datetime
from .kis_api import get_price_data, send_order
import json
import os

PORTFOLIO_STATE_FILE = "rolling_k_auto_trade_api/portfolio_state.json"


def load_portfolio_state():
    if os.path.exists(PORTFOLIO_STATE_FILE):
        with open(PORTFOLIO_STATE_FILE, "r") as f:
            return json.load(f)
    return {}


def save_portfolio_state(state):
    with open(PORTFOLIO_STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def calculate_target_price(prev_high, prev_low, today_open, k):
    return today_open + (prev_high - prev_low) * k


def monitor_and_trade_all(stocks):
    """
    리밸런싱된 종목 전체에 대해 실시간으로 K값 조건 감시 후 매수/매도 실행
    stocks: [{stock_code, best_k}]
    """
    state = load_portfolio_state()

    while datetime.now().hour < 15:
        for stock in stocks:
            code = stock["stock_code"]
            k = stock["best_k"]
            data = get_price_data(code)
            try:
                today_open = int(data["output"][0]["stck_oprc"])
                prev_high = int(data["output"][1]["stck_hgpr"])
                prev_low = int(data["output"][1]["stck_lwpr"])
                current_price = int(data["output"][0]["stck_prpr"])
            except:
                continue

            target_price = calculate_target_price(prev_high, prev_low, today_open, k)

            # 아직 매수하지 않은 경우 → 조건 만족 시 매수
            if code not in state and current_price > target_price:
                send_order(code, qty=1, side="buy")
                state[code] = {
                    "buy_price": current_price,
                    "buy_time": datetime.now().isoformat(),
                    "target_price": target_price,
                }

            # 보유 중인 경우 → 종가 근접 시 자동 매도 (단순 종료 조건 예시)
            elif code in state:
                if datetime.now().hour == 14 and datetime.now().minute > 50:
                    # TODO: send_sell(code, qty=1)
                    state[code]["sell_price"] = current_price
                    state[code]["sell_time"] = datetime.now().isoformat()

        save_portfolio_state(state)
        time.sleep(30)
