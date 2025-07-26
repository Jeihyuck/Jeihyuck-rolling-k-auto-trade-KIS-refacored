import json
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

LOG_DIR = Path(__file__).parent / "logs"
REPORT_DIR = Path(__file__).parent / "reports"
REPORT_DIR.mkdir(exist_ok=True)

def load_trades(date):
    log_file = LOG_DIR / f"trades_{date.strftime('%Y-%m-%d')}.json"
    trades = []
    if log_file.exists():
        with open(log_file, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    trades.append(json.loads(line))
                except Exception:
                    continue
    return trades

def ceo_report(date=None, period="daily"):
    # period: daily, weekly, monthly 지원
    if not date:
        date = datetime.now()
    if period == "daily":
        trade_days = [date]
        title = f"{date.strftime('%Y-%m-%d')} Rolling K 실전 리포트 (일간)"
    elif period == "weekly":
        week_start = date - timedelta(days=date.weekday())
        trade_days = [week_start + timedelta(days=i) for i in range(7)]
        title = f"{week_start.strftime('%Y-%m-%d')}~{(week_start+timedelta(days=6)).strftime('%Y-%m-%d')} Rolling K 리포트 (주간)"
    elif period == "monthly":
        month_start = date.replace(day=1)
        next_month = (month_start + timedelta(days=32)).replace(day=1)
        trade_days = [month_start + timedelta(days=i) for i in range((next_month - month_start).days)]
        title = f"{month_start.strftime('%Y-%m')} Rolling K 리포트 (월간)"
    else:
        raise ValueError("period must be daily, weekly, monthly")
    trades = []
    for d in trade_days:
        trades.extend(load_trades(d))

    if not trades:
        return {"title": title, "msg": "해당 기간 거래 내역 없음"}

    df = pd.DataFrame(trades)
    df["수익"] = 0
    df["수익률"] = 0.0
    df["매도단가"] = None
    df["매도시간"] = None

    # 매수/매도 매칭 (code별 매수/매도 페어링)
    buy_trades = df[df["side"]=="BUY"].copy()
    sell_trades = df[df["side"]=="SELL"].copy()
    sell_trades = sell_trades.set_index(["code","name","qty"])
    for idx, row in buy_trades.iterrows():
        try:
            sell_row = sell_trades.loc[(row["code"], row["name"], row["qty"])]
            buy_trades.at[idx, "매도단가"] = sell_row["price"]
            buy_trades.at[idx, "매도시간"] = sell_row["datetime"]
            buy_trades.at[idx, "수익"] = int(sell_row["price"] - row["price"]) * row["qty"]
            buy_trades.at[idx, "수익률"] = round((sell_row["price"] - row["price"]) / row["price"] * 100, 2)
        except Exception:
            continue

    # 요약
    total_invest = int(buy_trades["amount"].sum())
    total_pnl = int(buy_trades["수익"].sum())
    win_rate = (buy_trades["수익"] > 0).mean() * 100 if len(buy_trades) > 0 else 0
    mdd = "-"
    total_trade_count = len(df)
    symbol_count = buy_trades["code"].nunique()

    top_win = buy_trades.sort_values("수익", ascending=False).head(5)
    top_lose = buy_trades.sort_values("수익").head(3)
    종목별상세 = []
    for idx, row in buy_trades.iterrows():
        종목별상세.append({
            "code": row["code"],
            "name": row["name"],
            "K": row.get("K", "-"),
            "목표가": row.get("target_price", "-"),
            "매수수량": row["qty"],
            "매수단가": row["price"],
            "매수시간": row["datetime"],
            "매도단가": row["매도단가"],
            "매도시간": row["매도시간"],
            "수익률": f'{row["수익률"]}%',
            "실현손익": int(row["수익"]),
            "전략설명": row.get("strategy", "N/A")
        })

    전략설명 = {
        "운영K": "종목별 전월 rolling K, 백테스트 기반 자동적용",
        "리밸런싱방식": "월초 리밸런싱 + 실시간 매수, 당일 장마감 익일 매도",
        "매수기준": "목표가 돌파시 실시간 시장가/지정가 매수",
        "매도기준": "당일 장마감 전 전량 매도",
    }

    리포트 = {
        "title": title,
        "전략설정": 전략설명,
        "요약": {
            "총투자금액": f"{total_invest:,}원",
            "실현수익": f"{total_pnl:,}원 ({round(total_pnl/total_invest*100,2) if total_invest else 0}%)",
            "MDD": mdd,
            "승률": f"{round(win_rate,1)}%",
            "체결종목수": int(symbol_count),
            "매매회수": int(total_trade_count)
        },
        "수익TOP": [
            {
                "종목명": r["name"], "K": r.get("K","-"),
                "매수": r["price"], "매도": r["매도단가"], "수익률": f'{r["수익률"]}%',
                "매수시간": r["datetime"], "매도시간": r["매도시간"]
            } for _, r in top_win.iterrows()
        ],
        "손실TOP": [
            {
                "종목명": r["name"], "K": r.get("K","-"),
                "매수": r["price"], "매도": r["매도단가"], "수익률": f'{r["수익률"]}%',
                "매수시간": r["datetime"], "매도시간": r["매도시간"]
            } for _, r in top_lose.iterrows()
        ],
        "종목별 상세": 종목별상세,
        "거래세부내역": df.to_dict(orient="records")
    }
    # 저장
    filename = f"ceo_report_{period}_{date.strftime('%Y-%m-%d')}.json"
    with open(REPORT_DIR / filename, "w", encoding="utf-8") as f:
        json.dump(리포트, f, indent=2, ensure_ascii=False)
    return 리포트

if __name__ == "__main__":
    today = datetime.now()
    print(ceo_report(today, period="daily"))  # 일간
    print(ceo_report(today, period="weekly")) # 주간
    print(ceo_report(today, period="monthly"))# 월간
