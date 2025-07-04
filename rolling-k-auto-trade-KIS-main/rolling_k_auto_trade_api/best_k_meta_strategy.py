import numpy as np
from rolling_k_auto_trade_api.simulate_with_k_and_get_metrics import simulate_with_k_and_get_metrics
from rolling_k_auto_trade_api.get_best_k_meta import get_best_k_meta
from FinanceDataReader import StockListing, DataReader
from datetime import datetime, timedelta

def get_kosdaq_top_50():
    df = StockListing("KOSDAQ")
    df = df[df["Marcap"] > 0]
    top_50 = df.sort_values("Marcap", ascending=False).head(50)
    return top_50[["Code", "Name"]].to_dict(orient="records")

def simulate_k_range_for(stock_code, price_data, k_range=np.arange(0.1, 1.0, 0.1)):
    results = []
    for k in k_range:
        metrics = simulate_with_k_and_get_metrics(stock_code, k, price_data)
        metrics["k"] = k
        metrics["sharpe"] = round((metrics["avg_return_pct"] / 100) / (0.01 + metrics["mdd_pct"] / 100), 2)
        results.append(metrics)
    return results


def get_price_data_segments(code, base_date):
    from FinanceDataReader import DataReader
    from datetime import timedelta

    price_data = {}
    try:
        print(f"[DEBUG] 📦 Requesting DataReader for {code} from {base_date - timedelta(days=400)} to {base_date + timedelta(days=1)}")
        df = DataReader(code, start=base_date - timedelta(days=400), end=base_date + timedelta(days=1))
        print(f"[DEBUG] 📊 DataReader response: {df.shape}")
        df = df.dropna()
        df = df.rename(columns={"Open": "open", "High": "high", "Low": "low", "Close": "close"})
        df = df.reset_index()
        df["date"] = df["Date"].dt.date  # 날짜 비교 위해 date 객체로 변환
        df = df[["date", "open", "high", "low", "close"]]
        df = df.sort_values("date")

        base = base_date  # base_date는 이미 date 객체로 가정
        price_data["year"] = df[df["date"] >= base - timedelta(days=365)].to_dict(orient="records")
        price_data["quarter"] = df[df["date"] >= base - timedelta(days=90)].to_dict(orient="records")
        price_data["month"] = df[df["date"] >= base - timedelta(days=30)].to_dict(orient="records")

        print(f"[DEBUG] ✅ Segments for {code} — year: {len(price_data['year'])}, quarter: {len(price_data['quarter'])}, month: {len(price_data['month'])}")

    except Exception as e:
        print(f"[ERROR] ❌ Failed to fetch data for {code}: {e}")
        price_data = {"year": [], "quarter": [], "month": []}

    return price_data




def get_best_k_for_kosdaq_50(rebalance_date_str):
    rebalance_date = datetime.strptime(rebalance_date_str, "%Y-%m-%d").date()
    today = datetime.today().date()

    top50 = get_kosdaq_top_50()
    result_map = {}

    for stock in top50:
        code = stock["Code"]
        try:
            price_data_map = get_price_data_segments(code, rebalance_date)
            if not price_data_map['month']:
                print(f"[WARN] {stock['Name']} ({code}) 전월 데이터 없음 → 시뮬레이션 생략")
                continue
            y_metrics = simulate_k_range_for(code, price_data_map["year"])
            q_metrics = simulate_k_range_for(code, price_data_map["quarter"])
            m_metrics = simulate_k_range_for(code, price_data_map["month"])
            best_k = get_best_k_meta(y_metrics, q_metrics, m_metrics)

            if rebalance_date < today:
                monthly_metrics = simulate_with_k_and_get_metrics(code, best_k, price_data_map["month"])
                avg_return = monthly_metrics["avg_return_pct"]
                win_rate = monthly_metrics["win_rate_pct"]
                mdd = monthly_metrics["mdd_pct"]
                trades = monthly_metrics.get("trades", 0)
                cumulative_return = monthly_metrics.get("cumulative_return_pct", avg_return)
                avg_holding_days = monthly_metrics.get("avg_holding_days", 1)

                print(f"[LOG] {stock['Name']} ({code}) - Return: {avg_return}%, Win Rate: {win_rate}%, MDD: {mdd}%")
            if avg_return > 5 and win_rate > 60 and mdd < 10:
                    result_map[code] = {
                        "name": stock["Name"],
                        "best_k": best_k,
                        "avg_return_pct": avg_return,
                        "win_rate_pct": win_rate,
                        "mdd_pct": mdd,
                        "trades": trades,
                        "cumulative_return_pct": cumulative_return,
                        "avg_holding_days": avg_holding_days,
                        "sharpe_y": max([x["sharpe"] for x in y_metrics]) if y_metrics else 0,
                        "sharpe_q": max([x["sharpe"] for x in q_metrics]) if q_metrics else 0,
                        "sharpe_m": max([x["sharpe"] for x in m_metrics]) if m_metrics else 0
                    }
            else:
                result_map[code] = {
                    "name": stock["Name"],
                    "best_k": best_k
                }
        except Exception as e:
            result_map[code] = {"error": str(e)}

    return result_map
