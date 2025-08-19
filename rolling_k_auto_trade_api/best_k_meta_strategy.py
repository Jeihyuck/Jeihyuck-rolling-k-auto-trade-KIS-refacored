from __future__ import annotations

"""
best_k_meta_strategy.py — 원본 유지 + 실전 최적화 확장 (전체 파일)

주요 변경점
- (유지) KOSDAQ 시총 Top50 추출 로직 보강: 컬럼 탐색/병합 안정화
- (유지) 구간별 가격 세그먼트(1년/분기/월) 수집 + 월간 K 시뮬레이션
- (개선) 시뮬레이션 스코어링: 간이 Sharpe(수정) + 필터(음수수익/최소거래수/최대MDD)
- (개선) 목표가 산출: 전일 고저폭×K + 틱 규격 보정
- (추가) 종목 가중치 계산(assign_weights): 승률/수익률 우대, MDD 패널티
- (추가) 최종 selection 빌더: get_best_k_for_kosdaq_50 → 가중치 포함 리스트

본 모듈은 리밸런싱 API에서 import 하여 사용하거나, 단독 실행 시에도
선정 결과를 출력하도록 구성할 수 있습니다.
"""

from datetime import datetime, timedelta, date
import logging
from typing import Dict, List, Any, Optional

import numpy as np
import pandas as pd
import FinanceDataReader as fdr
from pykrx.stock import (
    get_market_cap_by_ticker,
    get_nearest_business_day_in_a_week,
)

from .simulate_with_k_and_get_metrics import simulate_with_k_and_get_metrics
from rolling_k_auto_trade_api.adjust_price_to_tick import adjust_price_to_tick

logger = logging.getLogger(__name__)

# -----------------------------
# 환경 파라미터 (튜닝 가능)
# -----------------------------
import os
K_MIN = float(os.getenv("K_MIN", "0.1"))
K_MAX = float(os.getenv("K_MAX", "0.9"))
K_STEP = float(os.getenv("K_STEP", "0.1"))

MIN_TRADES = int(os.getenv("MIN_TRADES", "5"))       # 월 구간 최소 거래수
MAX_MDD_PCT = float(os.getenv("MAX_MDD_PCT", "30"))   # 월 구간 최대 허용 MDD(%)
REQUIRE_POS_RET = os.getenv("REQUIRE_POS_RET", "true").lower() == "true"  # 월 평균수익 > 0 필터

TOP_N = int(os.getenv("TOP_N", "50"))                 # 시총 상위 추출 개수

# -----------------------------
# 1) 시가총액 기준 KOSDAQ Top-N
# -----------------------------

def _find_column(df: pd.DataFrame, keyword: str) -> Optional[str]:
    kw = keyword.replace(" ", "")
    for c in df.columns:
        if kw in str(c).replace(" ", ""):
            return c
    return None


def get_kosdaq_top_n(date_str: Optional[str] = None, n: int = TOP_N) -> pd.DataFrame:
    """시가총액 상위 n개 KOSDAQ 종목 반환 (Code, Name, Marcap)."""
    try:
        target_dt = datetime.today() if date_str is None else datetime.strptime(date_str, "%Y-%m-%d")
        from_date = get_nearest_business_day_in_a_week(target_dt.strftime("%Y%m%d"))
        logger.info(f"📅 pykrx 시총 조회일 → {from_date}")

        mktcap_df = get_market_cap_by_ticker(from_date, market="KOSDAQ")
        if mktcap_df is None or len(mktcap_df) == 0:
            logger.warning("⚠️  pykrx 시총 DF가 비었습니다 → 빈 DF 반환")
            return pd.DataFrame(columns=["Code", "Name", "Marcap"])    

        mktcap_df = mktcap_df.reset_index()
        capcol = _find_column(mktcap_df, "시가총액")
        ticcol = _find_column(mktcap_df, "티커") or _find_column(mktcap_df, "코드")
        if capcol is None or ticcol is None:
            logger.error("❌  시총/티커 컬럼 탐색 실패 → 빈 DF 반환")
            return pd.DataFrame(columns=["Code", "Name", "Marcap"])    

        mktcap_df = mktcap_df.rename(columns={capcol: "Marcap", ticcol: "Code"})
        mktcap_df["Code"] = mktcap_df["Code"].astype(str).str.zfill(6)

        fdr_df = fdr.StockListing("KOSDAQ").rename(columns={"Symbol": "Code", "Name": "Name"})
        fdr_df["Code"] = fdr_df["Code"].astype(str).str.zfill(6)

        merged = pd.merge(
            fdr_df[["Code", "Name"]],
            mktcap_df[["Code", "Marcap"]],
            on="Code",
            how="inner",
        )
        if "Marcap" not in merged.columns:
            for cand in ("Marcap_x", "Marcap_y", "MarketCap", "MarketCap_x", "MarketCap_y"):
                if cand in merged.columns:
                    merged = merged.rename(columns={cand: "Marcap"})
                    break
        if "Marcap" not in merged.columns:
            logger.error("❌  병합 후에도 'Marcap' 없음 → 빈 DF 반환")
            return pd.DataFrame(columns=["Code", "Name", "Marcap"])    

        topn = merged.dropna(subset=["Marcap"]).sort_values("Marcap", ascending=False).head(n)
        logger.info(f"✅  시총 Top{n} 추출 완료 → {len(topn)} 종목")
        return topn[["Code", "Name", "Marcap"]]
    except Exception:
        logger.exception("❌  get_kosdaq_top_n 예외:")
        return pd.DataFrame(columns=["Code", "Name", "Marcap"])    


# -----------------------------
# 2) K 시뮬레이션 (월 구간)
# -----------------------------

def simulate_k_range_for(
    code: str,
    price_data: List[Dict[str, Any]],
    k_range: Optional[np.ndarray] = None,
) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    if not price_data:
        return results

    if k_range is None:
        # np.arange의 부동소수 오차 방지 → 사전 반올림
        steps = int(round((K_MAX - K_MIN) / K_STEP)) + 1
        k_range = np.round(np.linspace(K_MIN, K_MAX, steps), 2)

    for k in k_range:
        metrics = simulate_with_k_and_get_metrics(code, float(k), price_data)
        metrics["k"] = float(k)
        # 간이 Sharpe: (평균수익률)/(0.01 + MDD)  — 둘 다 % 입력 가정
        try:
            mu = float(metrics.get("avg_return_pct", 0)) / 100.0
            mdd = abs(float(metrics.get("mdd_pct", 0))) / 100.0
            sharpe = (mu) / (0.01 + mdd)
            metrics["sharpe"] = round(sharpe, 4)
        except Exception:
            metrics["sharpe"] = 0.0
        results.append(metrics)
    return results


# -----------------------------
# 3) 가격 데이터 수집 (1년·1분기·1개월)
# -----------------------------

def get_price_data_segments(code: str, base_date: date) -> Dict[str, List[Dict[str, Any]]]:
    try:
        start_date = base_date - timedelta(days=400)
        end_date = base_date - timedelta(days=1)
        df = fdr.DataReader(code, start=start_date, end=end_date)
        df = (
            df.dropna(subset=["Open", "High", "Low", "Close"])
            .rename(columns={"Open": "open", "High": "high", "Low": "low", "Close": "close"})
            .reset_index()
        )
        if "Date" not in df.columns:
            # FDR 버전/마켓별로 Index가 DatetimeIndex인 경우 대비
            if df.index.name is not None:
                df = df.rename_axis("Date").reset_index()
            else:
                raise ValueError("DataReader 결과에 Date 컬럼이 없음")
        df["date"] = pd.to_datetime(df["Date"]).dt.date
        df = df[["date", "open", "high", "low", "close"]].sort_values("date")
        return {
            "year": df[df["date"] >= base_date - timedelta(days=365)].to_dict("records"),
            "quarter": df[df["date"] >= base_date - timedelta(days=90)].to_dict("records"),
            "month": df[df["date"] >= base_date - timedelta(days=30)].to_dict("records"),
        }
    except Exception as e:
        logger.exception(f"[ERROR] ❌ Failed to fetch data for {code}: {e}")
        return {"year": [], "quarter": [], "month": []}


# -----------------------------
# 4) K 최적화 & 필터링 (월 수익률>0, 거래수/MDD 조건)
# -----------------------------

def get_best_k_for_kosdaq_50(rebalance_date_str: str) -> List[Dict[str, Any]]:
    rebalance_date = datetime.strptime(rebalance_date_str, "%Y-%m-%d").date()
    top_df = get_kosdaq_top_n(rebalance_date_str, n=TOP_N)
    if top_df.empty:
        logger.warning("[WARN] get_kosdaq_top_n 결과 없음 → 빈 리스트 반환")
        return []

    results: Dict[str, Dict[str, Any]] = {}

    for _, stock in top_df.iterrows():
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

            avg_return = float(month_perf.get("avg_return_pct", 0.0))
            win_rate = float(month_perf.get("win_rate_pct", 0.0))
            mdd = float(month_perf.get("mdd_pct", 0.0))
            trades = int(month_perf.get("trades", 0))
            cum_ret = float(month_perf.get("cumulative_return_pct", avg_return))
            hold_days = float(month_perf.get("avg_holding_days", 1))

            # 필터링 규칙: 음수수익 제외, 거래수/최대낙폭 한도
            if REQUIRE_POS_RET and avg_return <= 0:
                logger.debug(f"[FILTER] {name}({code}) 수익률 {avg_return:.2f}% ≤ 0 → 제외")
                continue
            if trades < MIN_TRADES:
                logger.debug(f"[FILTER] {name}({code}) 거래수 {trades} < {MIN_TRADES} → 제외")
                continue
            if abs(mdd) > MAX_MDD_PCT:
                logger.debug(f"[FILTER] {name}({code}) MDD {mdd:.1f}% > {MAX_MDD_PCT}% → 제외")
                continue

            # 목표가 (전일 변동폭 기준)
            target_price = None
            if len(month_data) >= 2:
                today_open = float(month_data[-1]["open"])  # 당일 시가
                y_high, y_low = float(month_data[-2]["high"]), float(month_data[-2]["low"])  # 전일 고저
                target_price = adjust_price_to_tick(round(today_open + (y_high - y_low) * best_k, 2))

            close_price = float(month_data[-1]["close"]) if month_data else None

            # k-range 내 최고 sharpe
            try:
                max_sharpe = max((float(m.get("sharpe", 0)) for m in m_metrics), default=0.0)
            except Exception:
                max_sharpe = 0.0

            results[code] = {
                "code": code,
                "name": name,
                "best_k": float(best_k),
                "avg_return_pct": round(avg_return, 2),
                "win_rate_pct": round(win_rate, 1),
                "mdd_pct": round(mdd, 1),
                "trades": trades,
                "cumulative_return_pct": round(cum_ret, 2),
                "avg_holding_days": round(hold_days, 1),
                "sharpe_m": round(max_sharpe, 4),
                "목표가": target_price,
                "close": close_price,
            }

            logger.info(
                f"[SIM] {name}({code}) R={avg_return:.1f}% W={win_rate:.1f}% MDD={mdd:.1f}% K={best_k} trades={trades}"
            )

        except Exception as e:
            logger.exception(f"[ERR] {name}({code}) 시뮬 실패: {e}")

    logger.info(f"📊 필터 통과 종목 = {len(results)}개")

    # 가중치 부여
    out = list(results.values())
    out = assign_weights(out)
    return out


# -----------------------------
# 5) 메타 점수 집계 (가중합)
# -----------------------------

def get_best_k_meta(year_metrics: List[Dict[str, Any]],
                    quarter_metrics: List[Dict[str, Any]],
                    month_metrics: List[Dict[str, Any]]) -> float:
    scores: Dict[float, float] = {}

    def _update(metrics: List[Dict[str, Any]], weight: float):
        for m in metrics:
            k = round(float(m["k"]), 2)
            scores[k] = scores.get(k, 0.0) + float(m.get("sharpe", 0.0)) * weight

    _update(year_metrics, 1.0)
    _update(quarter_metrics, 1.5)
    _update(month_metrics, 2.0)

    if not scores:
        return 0.5
    best_k, _ = max(scores.items(), key=lambda x: x[1])
    return round(float(best_k), 2)


# -----------------------------
# 6) 비중 산출 (로컬 버전)
# -----------------------------

def assign_weights(selected: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """승률/수익률 우대, MDD 패널티 반영. 합은 1.0으로 정규화.
    입력 항목 예: {code, win_rate_pct, avg_return_pct, mdd_pct, ...}
    """
    if not selected:
        return []

    raw = []
    for it in selected:
        try:
            win = float(it.get("win_rate_pct", 0.0)) / 100.0
            ret = float(it.get("avg_return_pct", 0.0)) / 100.0
            mdd = abs(float(it.get("mdd_pct", 0.0))) / 100.0
        except Exception:
            win, ret, mdd = 0.5, 0.1, 0.1
        # 위험조정 기대값 느낌의 스코어
        score = (0.6 * win + 0.6 * ret) / max(0.05, (0.4 * mdd))
        raw.append(max(0.0, score))

    s = sum(raw) or 1.0
    ws = [r / s for r in raw]

    out: List[Dict[str, Any]] = []
    for it, w in zip(selected, ws):
        obj = dict(it)
        obj["weight"] = round(float(w), 6)
        out.append(obj)
    return out


# -----------------------------
# 7) (선택) 단독 실행용 헬퍼
# -----------------------------
if __name__ == "__main__":
    # 예: python -m rolling_k_auto_trade_api.best_k_meta_strategy 2025-08-01
    import sys
    if len(sys.argv) >= 2:
        dt = sys.argv[1]
    else:
        dt = datetime.today().strftime("%Y-%m-%d")
    sel = get_best_k_for_kosdaq_50(dt)
    # 요약 표 형태로 출력
    df = pd.DataFrame(sel)
    cols = [c for c in ["code", "name", "best_k", "avg_return_pct", "win_rate_pct", "mdd_pct", "weight", "목표가", "close"] if c in df.columns]
    print(df[cols].to_string(index=False))
