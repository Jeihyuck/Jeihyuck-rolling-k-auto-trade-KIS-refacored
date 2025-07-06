from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import FinanceDataReader as fdr
from pykrx.stock import get_market_cap_by_ticker, get_nearest_business_day_in_a_week
import logging

from .simulate_with_k_and_get_metrics import simulate_with_k_and_get_metrics

logger = logging.getLogger(__name__)


def get_kosdaq_top_50(date_str: str | None = None):
    """
    pykrx 시가총액 + FDR 종목명을 병합해 KOSDAQ 시총 상위 50개를 반환
    ─ 모든 단계에서 컬럼·행 존재 여부를 검증하여 KeyError 재발을 막는다.
    """
    try:
        # ── 0. 날짜 보정 ──────────────────────────────────────────────
        target = (
            datetime.today()
            if date_str is None
            else datetime.strptime(date_str, "%Y-%m-%d")
        )
        from_date = get_nearest_business_day_in_a_week(target.strftime("%Y%m%d"))
        logger.info(f"📅 pykrx 시총 조회일 → {from_date}")

        # ── 1. pykrx 시가총액 조회 ────────────────────────────────────
        mktcap_df = get_market_cap_by_ticker(from_date, market="KOSDAQ")
        if mktcap_df.empty:
            logger.warning("⚠️  pykrx 시총 DF가 비었습니다 → 종료")
            return pd.DataFrame()

        # 인덱스(티커) → 컬럼 전환
        mktcap_df = mktcap_df.reset_index()

        # 시가총액·티커 컬럼명 탐색 (공백·괄호 대비)
        cols = mktcap_df.columns.tolist()
        capcol = next((c for c in cols if "시가총액" in c.replace(" ", "")), None)
        ticcol = next((c for c in cols if "티커" in c.replace(" ", "")), None)
        if capcol is None or ticcol is None:
            logger.error(f"❌  필수컬럼 누락 cap={capcol}, tic={ticcol}")
            return pd.DataFrame()

        # 표준 컬럼명으로 리네임 + 코드 6자리 맞춤
        mktcap_df = mktcap_df.rename(columns={capcol: "Marcap", ticcol: "Code"})
        mktcap_df["Code"] = mktcap_df["Code"].astype(str).str.zfill(6)

        logger.info(
            f"[DEBUG] pykrx after rename → {mktcap_df[['Code', 'Marcap']].head()}"
        )

        # ── 2. FDR 종목 기본정보 ────────────────────────────────────
        fdr_df = fdr.StockListing("KOSDAQ").rename(
            columns={"Symbol": "Code", "Name": "Name"}
        )
        fdr_df["Code"] = fdr_df["Code"].astype(str).str.zfill(6)

        # ── 3. 병합 ────────────────────────────────────────────────
        merged = pd.merge(fdr_df, mktcap_df[["Code", "Marcap"]], on="Code", how="inner")
        logger.info(f"[DEBUG] merged.columns → {merged.columns.tolist()}")
        logger.info(f"[DEBUG] merged.shape   → {merged.shape}")

        # 병합 후 컬럼명이 Marcap_x / Marcap_y 일 수도 있으므로 표준화
        if "Marcap" not in merged.columns:
            if "Marcap_x" in merged.columns:
                merged = merged.rename(columns={"Marcap_x": "Marcap"})
            elif "Marcap_y" in merged.columns:
                merged = merged.rename(columns={"Marcap_y": "Marcap"})

        if "Marcap" not in merged.columns:
            logger.error("❌  병합 후에도 'Marcap' 컬럼 없음 → 종료")
            return pd.DataFrame()

        # ── 4. NaN 제거·상위 50 추출 ───────────────────────────────
        merged = merged.dropna(subset=["Marcap"])
        top50 = merged.sort_values("Marcap", ascending=False).head(50)

        if top50.empty:
            logger.warning("⚠️  Top50 결과가 비어 있음 → 종료")
            return pd.DataFrame()

        logger.info(f"✅  시총 Top50 추출 완료 → {len(top50)} 종목")
        logger.info(f"[샘플]\n{top50[['Code', 'Name', 'Marcap']].head()}")

        return top50[["Code", "Name", "Marcap"]]

    except Exception:
        logger.exception("❌  get_kosdaq_top_50 예외:")
        return pd.DataFrame()


def simulate_k_range_for(stock_code, price_data, k_range=np.arange(0.1, 1.0, 0.1)):
    results = []
    for k in k_range:
        metrics = simulate_with_k_and_get_metrics(stock_code, k, price_data)
        metrics["k"] = k
        metrics["sharpe"] = round(
            (metrics["avg_return_pct"] / 100) / (0.01 + metrics["mdd_pct"] / 100), 2
        )
        results.append(metrics)
    return results


import logging
from datetime import timedelta
import FinanceDataReader as fdr

logger = logging.getLogger(__name__)

def get_price_data_segments(code: str, base_date: datetime.date) -> dict[str, list[dict]]:
    """
    종목 코드(code)와 기준일(base_date)에 대해
    과거 1년, 90일, 30일 가격 데이터를 조회하여
    'year', 'quarter', 'month'로 구분해 반환합니다.
    """
    try:
        # 조회 기간 설정: 과거 400일치부터 리밸런스 전날까지
        start_date = base_date - timedelta(days=400)
        end_date = base_date - timedelta(days=1)
        logger.info(f"[DEBUG] 📦 Fetching {code} from {start_date} to {end_date}")

        df = fdr.DataReader(code, start=start_date, end=end_date)
        logger.info(f"[DEBUG] 📊 DataReader returned {df.shape} rows for {code}")

        # 필수 컬럼만 남기고 결측치 제거
        df = df.dropna(subset=["Open", "High", "Low", "Close"])
        df = df.rename(columns={
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close"
        })

        # 날짜 컬럼 생성 및 정리
        df = df.reset_index()
        df["date"] = df["Date"].dt.date
        df = df[["date", "open", "high", "low", "close"]]
        df = df.sort_values("date")

        # 구간별로 분리
        price_data = {
            "year": df[df["date"] >= base_date - timedelta(days=365)].to_dict(orient="records"),
            "quarter": df[df["date"] >= base_date - timedelta(days=90)].to_dict(orient="records"),
            "month": df[df["date"] >= base_date - timedelta(days=30)].to_dict(orient="records"),
        }
        logger.info(
            f"[DEBUG] ✅ Segments for {code}: "
            f"year={len(price_data['year'])}, "
            f"quarter={len(price_data['quarter'])}, "
            f"month={len(price_data['month'])}"
        )
    except Exception as e:
        logger.exception(f"[ERROR] ❌ Failed to fetch data for {code}: {e}")
        price_data = {"year": [], "quarter": [], "month": []}

    return price_data



# --------------------------------------------------------------------
# Rolling-K 변동성-돌파 : 코스닥 Top50 종목별 Best-K 선정 + 필터링
# --------------------------------------------------------------------
def get_best_k_for_kosdaq_50(rebalance_date_str: str) -> list[dict]:
    """
    ● 입력  : 리밸런스 기준일(YYYY-MM-DD)
    ● 출력  : 조건 통과 종목 리스트   list[dict]
              └ dict 예시
                 {
                     "code"            : "091990",
                     "name"            : "셀트리온헬스케어",
                     "best_k"          : 0.4,
                     "avg_return_pct"  : 8.7,
                     "win_rate_pct"    : 65.0,
                     "mdd_pct"         : 7.2,
                     "trades"          : 12,
                     "cumulative_return_pct": 29.5,
                     "avg_holding_days": 4.3,
                     "sharpe_y"        : 1.12,
                     "sharpe_q"        : 1.35,
                     "sharpe_m"        : 1.77
                 }
    """
    rebalance_date = datetime.strptime(rebalance_date_str, "%Y-%m-%d").date()
    today = datetime.today().date()

    # 1) 시가총액 Top50 확보
    top50_df = get_kosdaq_top_50(rebalance_date_str)
    if top50_df.empty:
        logger.warning("[WARN] get_kosdaq_top_50 결과 없음 → 빈 리스트 반환")
        return []

    # 2) 종목별 시뮬레이션
    result_map: dict[str, dict] = {}

    for _, stock in top50_df.iterrows():
        code = stock["Code"]
        name = stock["Name"]

        try:
            # 가격 데이터 1년치 다운로드 & 세그먼트 분할
            price_segments = get_price_data_segments(code, rebalance_date)
            if not price_segments["month"]:
                logger.warning(f"[SKIP] {name}({code}) 전월 데이터 없음")
                continue

            # K값 범위 시뮬
            y_metrics = simulate_k_range_for(code, price_segments["year"])
            q_metrics = simulate_k_range_for(code, price_segments["quarter"])
            m_metrics = simulate_k_range_for(code, price_segments["month"])

            best_k = get_best_k_meta(y_metrics, q_metrics, m_metrics)

            # 리밸런스 기준일이 과거면 다시 전월 실적 검증
            avg_return = win_rate = mdd = trades = cum_ret = hold_days = 0
            if rebalance_date < today:
                month_perf = simulate_with_k_and_get_metrics(
                    code, best_k, price_segments["month"]
                )
                avg_return = month_perf["avg_return_pct"]
                win_rate = month_perf["win_rate_pct"]
                mdd = month_perf["mdd_pct"]
                trades = month_perf.get("trades", 0)
                cum_ret = month_perf.get("cumulative_return_pct", avg_return)
                hold_days = month_perf.get("avg_holding_days", 1)

            logger.info(
                f"[SIM] {name}({code}) R={avg_return:.1f}%  "
                f"W={win_rate:.1f}%  MDD={mdd:.1f}%  K={best_k}"
            )

            # 3) 필터링
            #if avg_return > 5 and win_rate > 60 and mdd < 10:
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
                }

        except Exception as e:
            logger.exception(f"[ERR] {name}({code}) 시뮬 실패: {e}")

    # 4) list[dict] 로 반환  (rebalance_watchlist.py 가 dict.get() 사용 가능)
    logger.info(f"📊 필터 통과 종목 = {len(result_map)}개")
    return list(result_map.values())


def get_best_k_meta(year_metrics, quarter_metrics, month_metrics):
    """
    Sharpe 점수 기반 K값 선택
    - 연: 1.0 가중치
    - 분기: 1.5
    - 월: 2.0
    """
    scores = {}

    def update_scores(metrics, weight):
        for m in metrics:
            k = round(m["k"], 2)
            scores.setdefault(k, 0)
            scores[k] += m.get("sharpe", 0) * weight

    update_scores(year_metrics, 1.0)
    update_scores(quarter_metrics, 1.5)
    update_scores(month_metrics, 2.0)

    if not scores:
        return 0.5  # fallback

    best_k = max(scores.items(), key=lambda x: x[1])[0]
    return round(best_k, 2)
