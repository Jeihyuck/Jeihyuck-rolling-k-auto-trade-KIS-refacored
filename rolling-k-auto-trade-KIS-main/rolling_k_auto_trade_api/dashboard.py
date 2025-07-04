# rolling_k_auto_trade_api/dashboard.py
import os
import json
import pandas as pd

LOG_DIR = "./rolling_k_auto_trade_api/logs"

def dashboard_summary():
    try:
        buy_path = os.path.join(LOG_DIR, "buy_orders.log")
        sell_path = os.path.join(LOG_DIR, "sell_orders.log")
        with open(buy_path) as f:
            buys = [json.loads(line) for line in f]
        with open(sell_path) as f:
            sells = [json.loads(line) for line in f]

        buy_df = pd.DataFrame(buys)
        sell_df = pd.DataFrame(sells)

        summary = {
            "매수건수": len(buy_df),
            "매도건수": len(sell_df),
            "보유종목수": len(set(buy_df['code']) - set(sell_df['code']))
        }
        return summary
    except Exception as e:
        return {"error": str(e)}
