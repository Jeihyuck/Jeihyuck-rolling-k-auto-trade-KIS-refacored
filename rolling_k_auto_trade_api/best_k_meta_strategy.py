# best_k_meta_strategy.py (실전 rolling_k, 최적화 전체본)
"""
실전형 rolling_k 변동성돌파 + 월초/rolling/Top5/보유분/동적K/가중치 최적화 전략 (FastAPI/trader.py 바로 연동)
- KOSDAQ TopN pykrx+fdr 유니버스/시총 동적
- 월/분기/연간 K-grid(고정/ATR동적)
- 목표가: 전일 변동폭*K + 틱보정
- best_k/Sharpe/승률/수익률/MDD/거래수 필터 + assign_weights
- 보유종목 강제포함/비중하한/rolling top5 통합
"""

from __future__ import annotations

from trader.rkmax_utils import get_best_k_meta, assign_weights, _enforce_min_weight_for_forced
from datetime import datetime, timedelta, date
import logging
from typing import Dict, List, Any, Optional, Iterable
import os
import math
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
K_MIN = float(os.getenv("K_MIN", "0.1"))
K_MAX = float(os.getenv("K_MAX", "1.0"))
K_STEP = float(os.getenv("K_STEP", "0.1"))
K_GRID_MODE = os.getenv("K_GRID_MODE", "fixed").lower()
K_STEP_FINE = float(os.getenv("K_STEP_FINE", "0.05"))
K_DYNAMIC_STEP_MIN = float(os.getenv("K_DYNAMIC_STEP_MIN", "0.03"))
K_DYNAMIC_STEP_MAX = float(os.getenv("K_DYNAMIC_STEP_MAX", "0.10"))
K_DYNAMIC_STEP_MULT = float(os.getenv("K_DYNAMIC_STEP_MULT", "1.5"))
MIN_TRADES = int(os.getenv("MIN_TRADES", "5"))
MAX_MDD_PCT = float(os.getenv("MAX_MDD_PCT", "30"))
REQUIRE_POS_RET = os.getenv("REQUIRE_POS_RET", "true").lower() == "true"
TOP_N = int(os.getenv("TOP_N", "50"))
ALWAYS_INCLUDE_CODES = {
    c.strip() for c in os.getenv("ALWAYS_INCLUDE_CODES", "").replace(" ", "").split(",") if c.strip()
}
KEEP_HELD_BYPASS_FILTERS = os.getenv("KEEP_HELD_BYPASS_FILTERS", "true").lower() == "true"
HELD_MIN_WEIGHT = float(os.getenv("HELD_MIN_WEIGHT", "0.01"))

# -----------------------------
# 유틸
# -----------------------------
def _clip(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))

def _round2(x: float) -> float:
    return float(np.round(x, 2))

def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default

def _find_column(df: pd.DataFrame, keyword: str) -> Optional[str]:
    kw = keyword.replace(" ", "")
    for c in df.columns:
        if kw in str(c).replace(" ", ""):
            return c
    return None

# -----------------------------
# 1) 시가총액 기준 KOSDAQ Top-N
# -----------------------------
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
# ATR 계산(월 데이터 레코드에서)
# -----------------------------
def _compute_atr_from_records(records: List[Dict[str, Any]], window: int = 14) -> Optional[float]:
    """월 구간 레코드([{open,high,low,close}...])에서 ATR 계산."""
    if not records or len(records) < window + 1:
        return None
    df = pd.DataFrame(records).copy()
    # 컬럼 보정
    need = {"open", "high", "low", "close"}
    if not need.issubset(set(df.columns)):
        return None
    df = df[["open", "high", "low", "close"]].astype(float)
    prev_close = df["close"].shift(1)
    tr = pd.concat(
        [
            (df["high"] - df["low"]).abs(),
            (df["high"] - prev_close).abs(),
            (df["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    atr = tr.rolling(window=window, min_periods=window).mean().iloc[-1]
    try:
        return float(atr) if atr and not math.isnan(atr) else None
    except Exception:
        return None

# -----------------------------
# K 그리드 생성
# -----------------------------
def _build_k_range(code: str, month_data: List[Dict[str, Any]]) -> np.ndarray:
    kmin, kmax = float(K_MIN), float(K_MAX)
    kmin = _clip(kmin, 0.01, 1.50)
    kmax = _clip(kmax, 0.05, 1.50)
    if kmax <= kmin:
        kmax = kmin + 0.05

    mode = K_GRID_MODE
    step = float(K_STEP)
    if mode == "fine":
        step = float(K_STEP_FINE)
    elif mode == "atr":
        atr = _compute_atr_from_records(month_data, window=14)
        close = _safe_float(month_data[-1].get("close")) if month_data else None
        if atr and close and close > 0:
            step_est = K_DYNAMIC_STEP_MULT * (atr / close)
            step = _clip(_round2(step_est), K_DYNAMIC_STEP_MIN, K_DYNAMIC_STEP_MAX)
        else:
            step = float(K_STEP_FINE)
    steps = int(round((kmax - kmin) / max(1e-6, step))) + 1
    steps = int(_clip(steps, 3, 100))
    k_range = np.round(np.linspace(kmin, kmax, steps), 2)
    k_range = np.unique(np.clip(k_range, 0.01, 1.50))
    logger.debug(f"[KGRID] {code} mode={mode} range=[{kmin:.2f},{kmax:.2f}] step≈{step:.2f} → {len(k_range)} pts")
    return k_range

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
        k_range = _build_k_range(code, price_data)
    for k in k_range:
        metrics = simulate_with_k_and_get_metrics(code, float(k), price_data)
        metrics["k"] = float(k)
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
# 4) K 최적화 & 필터링 (+ 보유분 강제 포함)
# -----------------------------
def _parse_force_include_codes(env_codes: Iterable[str]) -> List[str]:
    out = []
    for c in env_codes:
        c = str(c).strip()
        if not c:
            continue
        out.append(c.zfill(6))
    return sorted(set(out))

def _inject_forced_codes(universe_df: pd.DataFrame, forced_codes: List[str]) -> pd.DataFrame:
    if not forced_codes:
        return universe_df
    fdr_df = fdr.StockListing("KOSDAQ").rename(columns={"Symbol": "Code", "Name": "Name"})
    fdr_df["Code"] = fdr_df["Code"].astype(str).str.zfill(6)
    force_df = fdr_df[fdr_df["Code"].isin(forced_codes)][["Code", "Name"]].copy()
    missing = [c for c in forced_codes if c not in set(force_df["Code"])]
    if missing:
        force_df = pd.concat(
            [force_df, pd.DataFrame({"Code": missing, "Name": [None] * len(missing)})],
            ignore_index=True,
        )
    uni = universe_df.copy()
    uni = pd.concat([uni[["Code", "Name", "Marcap"], force_df.assign(Marcap=np.nan)]], ignore_index=True)
    uni = uni.drop_duplicates(subset=["Code"], keep="first")
    return uni


def get_best_k_for_kosdaq_50(rebalance_date_str: str) -> List[Dict[str, Any]]:
    rebalance_date = datetime.strptime(rebalance_date_str, "%Y-%m-%d").date()
    top_df = get_kosdaq_top_n(rebalance_date_str, n=TOP_N)
    forced_codes = _parse_force_include_codes(ALWAYS_INCLUDE_CODES)
    if forced_codes:
        top_df = _inject_forced_codes(top_df, forced_codes)
    if top_df.empty:
        logger.warning("[WARN] get_kosdaq_top_n 결과 없음 → 빈 리스트 반환")
        return []
    results: Dict[str, Dict[str, Any]] = {}
    for _, stock in top_df.iterrows():
        code, name = str(stock["Code"]).zfill(6), stock.get("Name")
        try:
            segments = get_price_data_segments(code, rebalance_date)
            month_data = segments["month"]
            if not month_data:
                logger.debug(f"[SKIP] {name}({code}) 전월 데이터 없음")
                if code in forced_codes and KEEP_HELD_BYPASS_FILTERS:
                    results[code] = {
                        "code": code, "name": name, "best_k": 0.5,
                        "avg_return_pct": 0.0, "win_rate_pct": 0.0,
                        "mdd_pct": 0.0, "trades": 0, "cumulative_return_pct": 0.0,
                        "avg_holding_days": 0.0, "sharpe_m": 0.0,
                        "목표가": None, "close": None,
                        "forced_include": True, "filtered_reason": "NO_DATA"
                    }
                continue
            k_range = _build_k_range(code, month_data)
            m_metrics = simulate_k_range_for(code, month_data, k_range=k_range)
            best_k = get_best_k_meta([], [], m_metrics)
            month_perf = simulate_with_k_and_get_metrics(code, best_k, month_data)
            avg_return = float(month_perf.get("avg_return_pct", 0.0))
            win_rate = float(month_perf.get("win_rate_pct", 0.0))
            mdd = float(month_perf.get("mdd_pct", 0.0))
            trades = int(month_perf.get("trades", 0))
            cum_ret = float(month_perf.get("cumulative_return_pct", avg_return))
            hold_days = float(month_perf.get("avg_holding_days", 1))
            filtered_out = False
            reason = []
            if REQUIRE_POS_RET and avg_return <= 0:
                filtered_out = True; reason.append("NEG_RET")
            if trades < MIN_TRADES:
                filtered_out = True; reason.append("LOW_TRADES")
            if abs(mdd) > MAX_MDD_PCT:
                filtered_out = True; reason.append("HIGH_MDD")
            # 목표가 계산
            target_price = None
            if len(month_data) >= 2:
                today_open = float(month_data[-1]["open"])
                y_high, y_low = float(month_data[-2]["high"]), float(month_data[-2]["low"])
                target_price = adjust_price_to_tick(round(today_open + (y_high - y_low) * best_k, 2))
            close_price = float(month_data[-1]["close"]) if month_data else None
            # k-range 내 최고 sharpe
            try:
                max_sharpe = max((float(m.get("sharpe", 0)) for m in m_metrics), default=0.0)
            except Exception:
                max_sharpe = 0.0
            # 강제 포함 예외 처리
            if filtered_out and code in forced_codes and KEEP_HELD_BYPASS_FILTERS:
                logger.info(f"[FORCE-KEEP] {name}({code}) 필터탈락({','.join(reason)})이지만 보유분 포함")
                filtered_out = False
            if filtered_out:
                logger.debug(f"[FILTER] {name}({code}) 제외: {','.join(reason)}")
                continue
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
                "forced_include": code in forced_codes,
                "k_grid_mode": K_GRID_MODE,
            }
            logger.info(
                f"[SIM] {name}({code}) R={avg_return:.1f}% W={win_rate:.1f}% MDD={mdd:.1f}% "
                f"K={best_k} trades={trades} forced={code in forced_codes}"
            )
        except Exception as e:
            logger.exception(f"[ERR] {name}({code}) 시뮬 실패: {e}")
            continue
    logger.info(f"📊 필터/강제포함 반영 종목 = {len(results)}개")
    # 가중치 부여
    selected = list(results.values())
    selected = assign_weights(selected)
    # 보유분 최소 비중 하한 보정 (합계 1 유지)
    if selected and HELD_MIN_WEIGHT > 0:
        selected = _enforce_min_weight_for_forced(selected, min_w=HELD_MIN_WEIGHT)
    return selected
