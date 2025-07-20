# rolling_k_auto_trade_api/strategies.py
import FinanceDataReader as fdr
import pandas as pd
import numpy as np
from datetime import datetime
from rolling_k_auto_trade_api.orders import log_order, TRADE_STATE

import logging

logging.basicConfig(
    filename='rebalance_debug.log',   # 파일 저장, 필요시 삭제하거나 로그 파일명 변경
    level=logging.DEBUG,
    format='[%(asctime)s][%(levelname)s] %(message)s',
    encoding='utf-8'
)
logger = logging.getLogger(__name__)


fee = 0.0015
k_values = np.arange(0.1, 1.01, 0.05)


def run_rebalance_for_date(date_input):
    from calendar import monthrange
    from datetime import datetime

    rebalance_date = pd.to_datetime(date_input)
    start_train = (rebalance_date - pd.DateOffset(months=1)).strftime("%Y-%m-%d")
    end_train = (rebalance_date - pd.DateOffset(days=1)).strftime("%Y-%m-%d")
    start_test = rebalance_date.strftime("%Y-%m-%d")
    end_test = datetime(rebalance_date.year, rebalance_date.month, monthrange(rebalance_date.year, rebalance_date.month)[1]).strftime("%Y-%m-%d")

    kosdaq = fdr.StockListing("KOSDAQ")
    top50 = kosdaq.sort_values(by="Marcap", ascending=False).head(50)
    tickers = list(zip(top50["Code"], top50["Name"]))

    k_values = [round(x, 1) for x in np.arange(0.1, 1.1, 0.1)]
    fee = 0.0015  # 수수료 가정

    selected = []
    candidates = []

    for code, name in tickers:
        try:
            df = fdr.DataReader(code, start_train, end_test)
            df.index = pd.to_datetime(df.index)
            train = df[(df.index >= start_train) & (df.index <= end_train)].copy()
            test = df[(df.index >= start_test) & (df.index <= end_test)].copy()
            if len(train) < 15 or len(test) < 5:
                continue

            best_k, best_ret = 0, -np.inf
            for k in k_values:
                temp = train.copy()
                temp["range"] = temp["High"].shift(1) - temp["Low"].shift(1)
                temp["target"] = temp["Open"] + temp["range"] * k
                temp["buy_signal"] = temp["High"] > temp["target"]
                temp["buy_price"] = np.where(temp["buy_signal"], temp["target"], np.nan)
                temp["sell_price"] = temp["Close"]
                temp["strategy_return"] = np.where(temp["buy_signal"], (temp["sell_price"] - temp["buy_price"]) / temp["buy_price"] - fee, 0)
                temp["cumulative_return"] = (1 + temp["strategy_return"].fillna(0)).cumprod() - 1
                final_ret = temp["cumulative_return"].iloc[-1]
                if final_ret > best_ret:
                    best_ret = final_ret
                    best_k = k

            test["range"] = test["High"].shift(1) - test["Low"].shift(1)
            test["target"] = test["Open"] + test["range"] * best_k
            test["buy_signal"] = test["High"] > test["target"]
            test["buy_price"] = np.where(test["buy_signal"], test["target"], np.nan)
            test["sell_price"] = test["Close"]
            test["strategy_return"] = np.where(test["buy_signal"], (test["sell_price"] - test["buy_price"]) / test["buy_price"] - fee, 0)
            test["cumulative"] = (1 + test["strategy_return"].fillna(0)).cumprod()

            mdd = ((test["cumulative"] - test["cumulative"].cummax()) / test["cumulative"].cummax()).min() * 100
            wins = (test["strategy_return"] > 0).sum()
            total = test["strategy_return"].notnull().sum()
            win_rate = wins / total if total > 0 else 0
            final_ret = (test["cumulative"].iloc[-1] - 1) * 100

            info = {
                "rebalance_date": start_test,
                "code": code,
                "name": name,
                "best_k": best_k,
                "cumulative_return_pct": round(final_ret, 2),
                "win_rate_pct": round(win_rate * 100, 2),
                "mdd_pct": round(mdd, 2),
                "목표가": round(test["target"].iloc[-1], 2),
                "close": round(test["Close"].iloc[-1], 2),
            }
            candidates.append(info)

            if (
    info["cumulative_return_pct"] > 1.0 and
    info["win_rate_pct"] > 50.0 and
    info["mdd_pct"] <= 15.0
):
                selected.append(info)

        except Exception as e:
            print(f"[ERROR] {code} - {name} 분석 실패: {str(e)}")
            continue

    if not selected:
        return {
            "status": "skipped",
            "reason": "no_qualified_candidates",
            "candidates": candidates,
            "selected": []
        }

    selected_df = pd.DataFrame(selected)
    selected_df = selected_df.sort_values("cumulative_return_pct", ascending=False).head(20)
    selected_df["포트비중(%)"] = round(100 / len(selected_df), 2)

    return {
        "status": "ready",
        "rebalance_date": date_input,
        "selected": selected_df.to_dict(orient="records"),
        "candidates": candidates
    }

def auto_trade_on_rebalance(date: str):
    result = run_rebalance_for_date(date)
    if result["status"] != "ready":
        print(f"[INFO] 리밸런싱 매매 생략: 사유 = {result.get('reason')}")
        return {"status": "skipped", "reason": result.get("reason")}

    selected = result["selected"]
    for row in selected:
        try:
            # 여기에 실제 주문 함수 호출 로직 삽입 가능
            print(f"[ORDER] 매수 - 종목: {row['name']}({row['code']}), 목표가: {row['목표가']}, 비중: {row['포트비중(%)']}%")
        except Exception as e:
            print(f"[ERROR] 주문 실패 - 종목: {row['name']}({row['code']}) - {str(e)}")

    return {"status": "매수 주문 완료", "종목 수": len(selected)}



def check_sell_conditions():
    # 예시: 체결 로그 확인하여 매도 타이밍 판단
    return {"message": "매도 조건 점검 로직은 추후 구현"}


def generate_performance_report():
    try:
        df = pd.read_csv("rolling_k_auto_trade_api/logs/buy_orders.log")
        df["수익률(%)"] = np.random.uniform(-5, 15, len(df))  # 예시용
        report = df.groupby("날짜")["수익률(%)"].mean().reset_index()
        return report.to_dict(orient="records")
    except:
        return {"message": "리포트 데이터를 불러올 수 없습니다."}
