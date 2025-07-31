from datetime import datetime, timedelta
import logging
from typing import Dict, List

import numpy as np
import pandas as pd
import FinanceDataReader as fdr
from pykrx.stock import get_market_cap_by_ticker, get_nearest_business_day_in_a_week

from .simulate_with_k_and_get_metrics import simulate_with_k_and_get_metrics
from rolling_k_auto_trade_api.adjust_price_to_tick import adjust_price_to_tick

logger = logging.getLogger(__name__)

# 1. 시가총액 기준 KOSDAQ‑50 추출
def _find_column(df: pd.DataFrame, keyword: str) -> str | None:
    kw = keyword.replace(" ", "")
    for c in df.columns:
        if kw in c.replace(" ", ""):
            return c
    return None

def get_kosdaq_top_50(date_str: str | None = None) -> pd.DataFrame:
    """시가총액 상위 50개 KOSDAQ 종목 반환 (Code, Name, Marcap)."""
    try:
        target_dt = datetime.today() if date_str is None else datetime.strptime(date_str, "%Y-%m-%d")
        from_date = get_nearest_business_day_in_a_week(target_dt.strftime("%Y%m%d"))
        logger.info(f"📅 pykrx 시총 조회일 → {from_date}")

        mktcap_df = get_market_cap_by_ticker(from_date, market="KOSDAQ")
        if mktcap_df.empty:
            logger.warning("⚠️  pykrx 시총 DF가 비었습니다 → 종료")
            return pd.DataFrame()

        mktcap_df = mktcap_df.reset_index()
        capcol = _find_column(mktcap_df, "시가총액")
        ticcol = _find_column(mktcap_df, "티커") or _find_column(mktcap_df, "코드")
        if capcol is None or ticcol is None:
            logger.error("❌  시총/티커 컬럼 탐색 실패 → 종료")
            return pd.DataFrame()

        mktcap_df = mktcap_df.rename(columns={capcol: "Marcap", ticcol: "Code"})
        mktcap_df["Code"] = mktcap_df["Code"].astype(str).zfill(6)
        fdr_df = fdr.StockListing("KOSDAQ").rename(columns={"Symbol": "Code", "Name": "Name"})
        fdr_df["Code"] = fdr_df["Code"].astype(str).zfill(6)
        merged = pd.merge(fdr_df[["Code", "Name"]], mktcap_df[["Code", "Marcap"]], on="Code", how="inner")
        if "Marcap" not in merged.columns:
            for cand in ("Marcap_x", "Marcap_y", "MarketCap", "MarketCap_x", "MarketCap_y"):
                if cand in merged.columns:
                    merged = merged.rename(columns={cand: "Marcap"})
                    break
        if "Marcap" not in merged.columns:
            logger.error("❌  병합 후에도 'Marcap' 없음 → 종료")
            return pd.DataFrame()

        top50 = merged.dropna(subset=["Marcap"]).sort_values("Marcap", ascending=False).head(50)
        logger.info(f"✅  시총 Top50 추출 완료 → {len(top50)} 종목")
        return top50[["Code", "Name", "Marcap"]]
    except Exception:
        logger.exception("❌  get_kosdaq_top_50 예외:")
        return pd.DataFrame()

# 2. K 시뮬레이션 (최근 1달 데이터만)
def simulate_k_range_for(
    code: str,
    price_data: List[Dict],
    k_range=np.arange(0.1, 1.0, 0.1),
) -> List[Dict]:
    results: List[Dict] = []
    if not price_data:
        return results
    for k in k_range:
        metrics = simulate_with_k_and_get_metrics(code, k, price_data)
        metrics["k"] = k
        # 간이 Sharpe = (평균수익)/(0.01+MDD)
        metrics["sharpe"] = round((metrics["avg_return_pct"] / 100) / (0.01 + metrics["mdd_pct"] / 100), 2)
        results.append(metrics)
    return results

# 3. 가격 데이터 수집 (1년·1분기·1개월)
def get_price_data_segments(code: str, base_date: datetime.date) -> Dict[str, List[Dict]]:
    try:
        start_date = base_date - timedelta(days=400)
        end_date = base_date - timedelta(days=1)
        df = fdr.DataReader(code, start=start_date, end=end_date)
        df = (
            df.dropna(subset=["Open", "High", "Low", "Close"])
            .rename(columns={"Open": "open", "High": "high", "Low": "low", "Close": "close"})
            .reset_index()
        )
        df["date"] = df["Date"].dt.date
        df = df[["date", "open", "high", "low", "close"]].sort_values("date")
        return {
            "year": df[df["date"] >= base_date - timedelta(days=365)].to_dict("records"),
            "quarter": df[df["date"] >= base_date - timedelta(days=90)].to_dict("records"),
            "month": df[df["date"] >= base_date - timedelta(days=30)].to_dict("records"),
        }
    except Exception as e:
        logger.exception(f"[ERROR] ❌ Failed to fetch data for {code}: {e}")
        return {"year": [], "quarter": [], "month": []}

# 4. K 최적화 & 필터링 (음수 수익률 제외)
def get_best_k_for_kosdaq_50(rebalance_date_str: str) -> List[Dict]:
    rebalance_date = datetime.strptime(rebalance_date_str, "%Y-%m-%d").date()
    top50_df = get_kosdaq_top_50(rebalance_date_str)
    if top50_df.empty:
        logger.warning("[WARN] get_kosdaq_top_50 결과 없음 → 빈 리스트 반환")
        return []

    result_map: Dict[str, Dict] = {}
    for _, stock in top50_df.iterrows():
        code, name = stock["Code"], stock["Name"]
        try:
            segments = get_price_data_segments(code, rebalance_date)
            month_data = segments["month"]
            if not month_data:
                logger.debug(f"[SKIP] {name}({code}) 전월 데이터 없음")
                continue

            m_metrics = simulate_k_range_for(code, month_data)
            best_k = get_best_k_meta([], [], m_metrics)
            month_perf = simulate_with_k_and_get_metrics(code, best_k, month_data)
            avg_return = month_perf["avg_return_pct"]
            if avg_return <= 0:
                logger.debug(f"[FILTER] {name}({code}) 수익률 {avg_return:.2f}% ≤ 0 → 제외")
                continue

            win_rate = month_perf["win_rate_pct"]
            mdd = month_perf["mdd_pct"]
            trades = month_perf.get("trades", 0)
            cum_ret = month_perf.get("cumulative_return_pct", avg_return)
            hold_days = month_perf.get("avg_holding_days", 1)

            # 목표가 (전일 변동폭)
            target_price = None
            if len(month_data) >= 2:
                today_open = month_data[-1]["open"]
                y_high, y_low = month_data[-2]["high"], month_data[-2]["low"]
                target_price = adjust_price_to_tick(round(today_open + (y_high - y_low) * best_k, 2))

            close_price = month_data[-1]["close"]

            logger.info(
                f"[SIM] {name}({code}) R={avg_return:.1f} W={win_rate:.1f} MDD={mdd:.1f} K={best_k}"
            )

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
                "sharpe_m": max((m["sharpe"] for m in m_metrics), default=0),
                "목표가": target_price,
                "close": close_price,
            }

        except Exception as e:
            logger.exception(f"[ERR] {name}({code}) 시뮬 실패: {e}")

    logger.info(f"📊 필터 통과 종목 = {len(result_map)}개")
    return list(result_map.values())

# 5. 메타 점수 집계 함수 (가중합)
def get_best_k_meta(year_metrics: List[Dict], quarter_metrics: List[Dict], month_metrics: List[Dict]) -> float:
    scores: Dict[float, float] = {}
    def _update(metrics: List[Dict], weight: float):
        for m in metrics:
            k = round(m["k"], 2)
            scores.setdefault(k, 0)
            scores[k] += m.get("sharpe", 0) * weight

    _update(year_metrics, 1.0)
    _update(quarter_metrics, 1.5)
    _update(month_metrics, 2.0)

    if not scores:
        return 0.5
    best_k, _ = max(scores.items(), key=lambda x: x[1])
    return round(best_k, 2)

