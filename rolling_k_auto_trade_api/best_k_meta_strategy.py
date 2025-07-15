from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import FinanceDataReader as fdr
from pykrx.stock import get_market_cap_by_ticker, get_nearest_business_day_in_a_week
import logging

from .simulate_with_k_and_get_metrics import simulate_with_k_and_get_metrics
from rolling_k_auto_trade_api.adjust_price_to_tick import adjust_price_to_tick

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────
# 1. 시가총액 기준 KOSDAQ‑50 추출
# ──────────────────────────────────────────────────────────────

def get_kosdaq_top_50(date_str: str | None = None) -> pd.DataFrame:
    try:
        target_dt = datetime.today() if date_str is None else datetime.strptime(date_str, "%Y-%m-%d")
        from_date = get_nearest_business_day_in_a_week(target_dt.strftime("%Y%m%d"))
        logger.info(f"📅 pykrx 시총 조회일 → {from_date}")

        mktcap_df = get_market_cap_by_ticker(from_date, market="KOSDAQ")
        if mktcap_df.empty:
            logger.warning("⚠️  pykrx 시총 DF가 비었습니다 → 종료")
            return pd.DataFrame()
        mktcap_df = mktcap_df.reset_index()

        cols = mktcap_df.columns.tolist()
        capcol = next((c for c in cols if "시가총액" in c.replace(" ", "")), None)
        ticcol = next((c for c in cols if "티커" in c.replace(" ", "")), None)
        if capcol is None or ticcol is None:
            logger.error(f"❌  필수컬럼 누락 cap={capcol}, tic={ticcol}")
            return pd.DataFrame()

        mktcap_df = mktcap_df.rename(columns={capcol: "Marcap", ticcol: "Code"})
        mktcap_df["Code"] = mktcap_df["Code"].astype(str).str.zfill(6)

        fdr_df = fdr.StockListing("KOSDAQ").rename(columns={"Symbol": "Code", "Name": "Name"})
        fdr_df["Code"] = fdr_df["Code"].astype(str).str.zfill(6)

        merged = pd.merge(fdr_df, mktcap_df[["Code", "Marcap"]], on="Code", how="inner")

        if "Marcap" not in merged.columns:
            for c in ("Marcap_x", "Marcap_y"):
                if c in merged.columns:
                    merged = merged.rename(columns={c: "Marcap"})
                    break
        if "Marcap" not in merged.columns:
            logger.error("❌  병합 후에도 'Marcap' 컬럼 없음 → 종료")
            return pd.DataFrame()

        merged = merged.dropna(subset=["Marcap"])
        top50 = merged.sort_values("Marcap", ascending=False).head(50)
        logger.info(f"✅  시총 Top50 추출 완료 → {len(top50)} 종목")
        return top50[["Code", "Name", "Marcap"]]
    except Exception:
        logger.exception("❌  get_kosdaq_top_50 예외:")
        return pd.DataFrame()


def simulate_k_range_for(code: str, price_data: list[dict], k_range=np.arange(0.1, 1.0, 0.1)):
    results = []
    for k in k_range:
        metrics = simulate_with_k_and_get_metrics(code, k, price_data)
        metrics["k"] = k
        metrics["sharpe"] = round((metrics["avg_return_pct"] / 100) / (0.01 + metrics["mdd_pct"] / 100), 2)
        results.append(metrics)
    return results


def get_price_data_segments(code: str, base_date: datetime.date) -> dict[str, list[dict]]:
    try:
        start_date = base_date - timedelta(days=400)
        end_date = base_date - timedelta(days=1)
        df = fdr.DataReader(code, start=start_date, end=end_date)
        df = df.dropna(subset=["Open", "High", "Low", "Close"]).rename(columns={
            "Open": "open", "High": "high", "Low": "low", "Close": "close"})
        df = df.reset_index()
        df["date"] = df["Date"].dt.date
        df = df[["date", "open", "high", "low", "close"]].sort_values("date")

        return {
            "year":    df[df["date"] >= base_date - timedelta(days=365)].to_dict("records"),
            "quarter": df[df["date"] >= base_date - timedelta(days=90)].to_dict("records"),
            "month":   df[df["date"] >= base_date - timedelta(days=30)].to_dict("records"),
        }
    except Exception as e:
        logger.exception(f"[ERROR] ❌ Failed to fetch data for {code}: {e}")
        return {"year": [], "quarter": [], "month": []}


def get_best_k_for_kosdaq_50(rebalance_date_str: str) -> list[dict]:
    rebalance_date = datetime.strptime(rebalance_date_str, "%Y-%m-%d").date()
    today = datetime.today().date()
    top50_df = get_kosdaq_top_50(rebalance_date_str)
    if top50_df.empty:
        logger.warning("[WARN] get_kosdaq_top_50 결과 없음 → 빈 리스트 반환")
        return []

    result_map: dict[str, dict] = {}

    for _, stock in top50_df.iterrows():
        code, name = stock["Code"], stock["Name"]
        try:
            segments = get_price_data_segments(code, rebalance_date)
            if not segments["month"]:
                logger.warning(f"[SKIP] {name}({code}) 전월 데이터 없음")
                continue

            close_price = segments["month"][-1]["close"]
            y_metrics = simulate_k_range_for(code, segments["year"])
            q_metrics = simulate_k_range_for(code, segments["quarter"])
            m_metrics = simulate_k_range_for(code, segments["month"])

            best_k = get_best_k_meta(y_metrics, q_metrics, m_metrics)

            target_price = None
            if len(segments["month"]) >= 2:
                today_open = segments["month"][-1]["open"]
                y_high, y_low = segments["month"][-2]["high"], segments["month"][-2]["low"]
                target_price = round(today_open + (y_high - y_low) * best_k, 2)
                target_price = adjust_price_to_tick(target_price)

            avg_return = win_rate = mdd = trades = cum_ret = hold_days = 0
            if rebalance_date < today:
                month_perf = simulate_with_k_and_get_metrics(code, best_k, segments["month"])
                avg_return = month_perf["avg_return_pct"]
                win_rate = month_perf["win_rate_pct"]
                mdd = month_perf["mdd_pct"]
                trades = month_perf.get("trades", 0)
                cum_ret = month_perf.get("cumulative_return_pct", avg_return)
                hold_days = month_perf.get("avg_holding_days", 1)

            logger.info(f"[SIM] {name}({code}) R={avg_return:.1f}%  W={win_rate:.1f}%  MDD={mdd:.1f}%  K={best_k}")

            if avg_return > 1 and win_rate > 20 and mdd < 30:
                result_map[code] = {
                    "code": code,
                    "name": name,
                    "best_k": best_k,
                    "avg_return_pct": round(avg_return, 2),
                    "win_rate_pct": round(win_rate, 1),
                    "mdd_pct": round(mdd, 1),
                    "trades": trades,
                    "cumulative_return_pct": round(cum_ret, 2),
                    "avg_holding_days": round(hold_days, 1),
                    "sharpe_y": max((m["sharpe"] for m in y_metrics), default=0),
                    "sharpe_q": max((m["sharpe"] for m in q_metrics), default=0),
                    "sharpe_m": max((m["sharpe"] for m in m_metrics), default=0),
                    "목표가": target_price,
                    "close": close_price,
                }

        except Exception as e:
            logger.exception(f"[ERR] {name}({code}) 시뮬 실패: {e}")

    logger.info(f"📊 필터 통과 종목 = {len(result_map)}개")
    return list(result_map.values())


def get_best_k_meta(year_metrics: list[dict], quarter_metrics: list[dict], month_metrics: list[dict]) -> float:
    scores: dict[float, float] = {}

    def update_scores(metrics: list[dict], weight: float):
        for m in metrics:
            k = round(m["k"], 2)
            scores.setdefault(k, 0)
            scores[k] += m.get("sharpe", 0) * weight

    update_scores(year_metrics, 1.0)
    update_scores(quarter_metrics, 1.5)
    update_scores(month_metrics, 2.0)

    if not scores:
        return 0.5
    best_k = max(scores.items(), key=lambda x: x[1])[0]
    return round(best_k, 2)
