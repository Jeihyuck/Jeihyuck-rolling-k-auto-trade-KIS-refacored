from fastapi import APIRouter
import json
import os
import calendar
from datetime import datetime
from rolling_k_auto_trade_api.simulate_with_k_and_get_metrics import simulate_with_k_and_get_metrics
from FinanceDataReader import DataReader

report_router = APIRouter()

@report_router.get("/report/monthly/{month}", tags=["Report"])
def run_monthly_report(month: str):
    # month 예: "2024-04"
    rebalance_path = f"rebalance_results/rebalance_{month}-01.json"
    if not os.path.exists(rebalance_path):
        return {"error": "리밸런싱 결과 파일이 존재하지 않습니다."}

    with open(rebalance_path, "r", encoding="utf-8") as f:
        rebalance_data = json.load(f)

    report = []

    year, mon = map(int, month.split("-"))
    last_day = calendar.monthrange(year, mon)[1]  # 실제 마지막 일자 구함
    end_date = f"{month}-{last_day:02d}"          # 예: "2024-04-30"

    for stock in rebalance_data:
        code = stock["stock_code"]
        name = stock["name"]
        k = stock["best_k"]
        try:
            df = DataReader(code, start=f"{month}-01", end=end_date).dropna()
            df = df.rename(columns={"Open": "open", "High": "high", "Low": "low", "Close": "close"})
            df = df.reset_index()
            df["date"] = df["Date"]
            price_data = df[["date", "open", "high", "low", "close"]].to_dict(orient="records")
            metrics = simulate_with_k_and_get_metrics(code, k, price_data)
            metrics.update({"stock_code": code, "name": name})
            report.append(metrics)
        except Exception as e:
            report.append({"stock_code": code, "name": name, "error": str(e)})

    return {
        "month": month,
        "total_count": len(report),
        "report": report
    }
