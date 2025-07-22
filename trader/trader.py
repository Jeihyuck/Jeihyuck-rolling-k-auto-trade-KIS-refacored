import time
from settings import TARGETS, POLL_INTERVAL
from kis_wrapper import KisAPI
from utils import log, send_slack

def main():
    kis = KisAPI()
    kis.authenticate()
    for code, target in TARGETS.items():
        price = kis.get_current_price(code)
        log(f"{code} 현재가 {price}, 목표가 {target}")
        if price >= target:
            resp = kis.order_cash(code, qty=1, side="1")
            log(f"BUY {code}@{price}: {resp}")
            send_slack(f"📈 매수: {code} @ {price}")

    for o in kis.get_open_orders():
        log(f"Order {o['ord_no']} 상태 조회: {kis.inquire_order(o['ord_no'])}")

    for bal in kis.get_balance():
        qty = int(bal.get("hldg_qty", 0))
        if qty > 0:
            resp = kis.order_cash(bal["pdno"], qty=qty, side="2")
            log(f"SELL {bal['pdno']} qty={qty}: {resp}")
            send_slack(f"📉 매도: {bal['pdno']} qty={qty}")

if __name__ == "__main__":
    main()
