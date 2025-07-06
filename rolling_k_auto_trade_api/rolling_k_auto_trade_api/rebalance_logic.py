import FinanceDataReader as fdr
import random


def get_all_kosdaq_top50():
    df = fdr.StockListing("KOSDAQ")
    df = df[df["Marcap"] > 0]
    top_50 = df.sort_values("Marcap", ascending=False).head(50)
    return top_50


def simulate_backtest_k(stock_code, k):
    return {
        "stock_code": stock_code,
        "best_k": round(random.uniform(0.3, 0.7), 2),
        "avg_return_pct": f"{round(random.uniform(8, 25), 2)}%",
        "mdd_pct": f"{round(random.uniform(3, 12), 2)}%",
        "win_rate_pct": f"{round(random.uniform(70, 95), 2)}%",
    }


def run_rebalance_strategy():
    df = get_all_kosdaq_top50()
    results = []
    for _, row in df.iterrows():
        stock_code = row["Code"]
        stock_name = row["Name"]

        result = simulate_backtest_k(stock_code, k=0.5)
        result["name"] = stock_name

        if (
            float(result["mdd_pct"].replace("%", "")) <= 10
            and float(result["win_rate_pct"].replace("%", "")) >= 70
        ):
            results.append(result)

    return sorted(
        results, key=lambda x: float(x["avg_return_pct"].replace("%", "")), reverse=True
    )[:5]
