# rolling_k_auto_trade_api/strategies.py
import FinanceDataReader as fdr
import pandas as pd
import numpy as np
from datetime import datetime
from rolling_k_auto_trade_api.orders import log_order, TRADE_STATE

fee = 0.0015
k_values = np.arange(0.1, 1.01, 0.05)


def run_rebalance_for_date(date_input):
    if date_input == "latest":
        raise ValueError(
            "Invalid string literal 'latest' passed. Use actual date string like '2024-04-01'."
        )

    # rebalance_date = pd.to_datetime(date_input)
    # start_train = (rebalance_date - pd.DateOffset(months=1)).strftime('%Y-%m-%d')
    # end_train = (rebalance_date - pd.DateOffset(days=1)).strftime('%Y-%m-%d')
    # start_test = rebalance_date.strftime('%Y-%m-%d')
    # end_test = (rebalance_date + pd.DateOffset(months=1) - pd.DateOffset(days=1)).strftime('%Y-%m-%d')

    from calendar import monthrange
    from datetime import datetime

    rebalance_date = pd.to_datetime(date_input)

    start_train = (rebalance_date - pd.DateOffset(months=1)).strftime("%Y-%m-%d")
    end_train = (rebalance_date - pd.DateOffset(days=1)).strftime("%Y-%m-%d")
    start_test = rebalance_date.strftime("%Y-%m-%d")

    # ✅ 수정: 해당 월의 마지막 날짜를 정확하게 계산
    year = rebalance_date.year
    month = rebalance_date.month
    last_day = monthrange(year, month)[1]  # 월말 계산 (예: 2024-04 → 30)
    end_test = datetime(year, month, last_day).strftime("%Y-%m-%d")

    kosdaq = fdr.StockListing("KOSDAQ")
    top50 = kosdaq.sort_values(by="Marcap", ascending=False).head(50)
    tickers = list(zip(top50["Code"], top50["Name"]))

    pool = []

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
                temp["strategy_return"] = np.where(
                    temp["buy_signal"],
                    (temp["sell_price"] - temp["buy_price"]) / temp["buy_price"] - fee,
                    0,
                )
                temp["cumulative_return"] = (
                    1 + temp["strategy_return"].fillna(0)
                ).cumprod() - 1
                final_ret = temp["cumulative_return"].iloc[-1]
                if final_ret > best_ret:
                    best_ret = final_ret
                    best_k = k

            test["range"] = test["High"].shift(1) - test["Low"].shift(1)
            test["target"] = test["Open"] + test["range"] * best_k
            test["buy_signal"] = test["High"] > test["target"]
            test["buy_price"] = np.where(test["buy_signal"], test["target"], np.nan)
            test["sell_price"] = test["Close"]
            test["strategy_return"] = np.where(
                test["buy_signal"],
                (test["sell_price"] - test["buy_price"]) / test["buy_price"] - fee,
                0,
            )
            test["cumulative"] = (1 + test["strategy_return"].fillna(0)).cumprod()

            if len(test["cumulative"]) == 0:
                continue

            mdd = (
                (test["cumulative"] - test["cumulative"].cummax())
                / test["cumulative"].cummax()
            ).min() * 100
            wins = (test["strategy_return"] > 0).sum()
            total = test["strategy_return"].notnull().sum()
            win_rate = wins / total if total > 0 else 0
            final_ret = (test["cumulative"].iloc[-1] - 1) * 100

            if (final_ret > 0) and (win_rate > 0.2) and (mdd > -30):
                pool.append(
                    {
                        "리밸런싱시점": start_test,
                        "티커": code,
                        "종목명": name,
                        "최적k": best_k,
                        "수익률(%)": round(final_ret, 2),
                        "MDD(%)": round(mdd, 2),
                        "승률(%)": round(win_rate * 100, 2),
                    }
                )
        except:
            continue

    pool_df = pd.DataFrame(pool)

    if pool_df.empty:
        return {
            "message": f"{rebalance_date.strftime('%Y-%m-%d')}에 해당하는 리밸런싱 종목이 없습니다."
        }

    pool_df = pool_df.sort_values("수익률(%)", ascending=False).head(20)
    pool_df["포트비중(%)"] = round(100 / len(pool_df), 2)

    return pool_df.to_dict(orient="records")


def auto_trade_on_rebalance(date: str):
    result = run_rebalance_for_date(date)
    for row in result:
        log_order("buy", row["티커"], row["포트비중(%)"])
    return {"status": "매수 주문 완료", "종목 수": len(result)}


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
