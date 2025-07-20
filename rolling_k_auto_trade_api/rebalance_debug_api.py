from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse
from FinanceDataReader import StockListing, DataReader
import pandas as pd
import numpy as np
from datetime import datetime

rebalance_debug_router = APIRouter()


@rebalance_debug_router.get(
    "/rebalance/debug-backtest",
    tags=["Rebalance-Debug"],
    responses={400: {"description": "조건에 맞는 종목이 없습니다. (빈 결과)"}},
)
def debug_backtest_monthly(
    start_date: str = Query("2020-01-01", description="시작일 (YYYY-MM-DD)"),
    end_date: str = Query("2021-04-01", description="종료일 (YYYY-MM-DD)"),
):
    try:
        top50_df = (
            StockListing("KOSDAQ").sort_values("Marcap", ascending=False).head(50)
        )
        tickers = list(zip(top50_df["Code"], top50_df["Name"]))
        fee = 0.0015
        k_values = np.arange(0.1, 1.01, 0.05)
        periods = pd.date_range(start=start_date, end=end_date, freq="MS")
        all_results = []
        debug_logs = []

        for i in range(len(periods) - 1):
            rebalance_date = periods[i + 1]
            start_train = (rebalance_date - pd.DateOffset(months=1)).strftime(
                "%Y-%m-%d"
            )
            end_train = (rebalance_date - pd.DateOffset(days=1)).strftime("%Y-%m-%d")
            start_test = rebalance_date.strftime("%Y-%m-%d")
            end_test = (
                rebalance_date + pd.DateOffset(months=1) - pd.DateOffset(days=1)
            ).strftime("%Y-%m-%d")

            selected = []
            for code, name in tickers:
                try:
                    df = DataReader(code, start_train, end_test)
                    df.index = pd.to_datetime(df.index)
                    train = df[
                        (df.index >= start_train) & (df.index <= end_train)
                    ].copy()
                    test = df[(df.index >= start_test) & (df.index <= end_test)].copy()
                    if len(train) < 15 or len(test) < 5:
                        debug_logs.append(f"{name}({code}): insufficient data")
                        continue

                    best_k, best_ret = 0, -np.inf
                    for k in k_values:
                        temp = train.copy()
                        temp["range"] = temp["High"].shift(1) - temp["Low"].shift(1)
                        temp["target"] = temp["Open"] + temp["range"] * k
                        temp["buy_signal"] = temp["High"] > temp["target"]
                        temp["buy_price"] = np.where(
                            temp["buy_signal"], temp["target"], np.nan
                        )
                        temp["sell_price"] = temp["Close"]
                        temp["strategy_return"] = np.where(
                            temp["buy_signal"],
                            (temp["sell_price"] - temp["buy_price"]) / temp["buy_price"]
                            - fee,
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
                    test["buy_price"] = np.where(
                        test["buy_signal"], test["target"], np.nan
                    )
                    test["sell_price"] = test["Close"]
                    test["strategy_return"] = np.where(
                        test["buy_signal"],
                        (test["sell_price"] - test["buy_price"]) / test["buy_price"]
                        - fee,
                        0,
                    )
                    test["cumulative"] = (
                        1 + test["strategy_return"].fillna(0)
                    ).cumprod()
                    if len(test["cumulative"]) == 0:
                        debug_logs.append(f"{name}({code}): empty cumulative")
                        continue
                    mdd = (
                        (test["cumulative"] - test["cumulative"].cummax())
                        / test["cumulative"].cummax()
                    ).min() * 100
                    wins = (test["strategy_return"] > 0).sum()
                    total = test["strategy_return"].notnull().sum()
                    win_rate = wins / total if total > 0 else 0
                    final_ret = (test["cumulative"].iloc[-1] - 1) * 100

                    if (final_ret > 0.02) and (win_rate > 0.5) and (mdd <= 0.1):
                        selected.append(
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
                    else:
                        debug_logs.append(
                            f"{name}({code}): 필터 탈락 (수익률 {final_ret:.2f}%, 승률 {win_rate*100:.1f}%, MDD {mdd:.1f}%)"
                        )
                except Exception as e:
                    debug_logs.append(f"{name}({code}): ERROR {str(e)}")
                    continue

            monthly_df = (
                pd.DataFrame(selected)
                .sort_values("수익률(%)", ascending=False)
                .head(20)
            )
            if not monthly_df.empty:
                monthly_df["포트비중(%)"] = round(100 / len(monthly_df), 2)
                all_results.append(monthly_df)
            else:
                debug_logs.append(f"{rebalance_date.date()}: 선택된 종목 없음")

        return {
            "결과": (
                pd.concat(all_results, ignore_index=True).to_dict(orient="records")
                if all_results
                else []
            ),
            "로그": debug_logs,
        }

    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})
