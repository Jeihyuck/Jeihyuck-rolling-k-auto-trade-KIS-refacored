# -*- coding: utf-8 -*-
# best_k_meta_strategy.py (실전 rolling_k, 최적화 전체본)
"""
실전형 rolling_k 변동성돌파 + 월초/rolling/TopN/보유분/동적K/가중치 최적화 전략
- KOSDAQ TopN(pykrx+fdr) 유니버스/시총 동적
- 월/분기/연간 K-grid(고정/ATR동적)
- 목표가: 전일 변동폭*K + 틱보정
- best_k/Sharpe/승률/수익률/MDD/거래수 필터 + assign_weights
- 보유종목 강제포함/비중하한/rolling 통합
- FastAPI(trader.py/main.py)에서 /rebalance/run/{date}가 호출할 run_rebalance() 제공
"""

from __future__ import annotations

import logging
import math
import os
from datetime import datetime, timedelta, date
from functools import lru_cache
from typing import Any, Dict, Iterable, List, Optional

import numpy as np
import pandas as pd
import FinanceDataReader as fdr
from pykrx.stock import get_nearest_business_day_in_a_week

from trader.rkmax_utils import get_best_k_meta, assign_weights, _enforce_min_weight_for_forced
from .simulate_with_k_and_get_metrics import simulate_with_k_and_get_metrics
from rolling_k_auto_trade_api.adjust_price_to_tick import adjust_price_to_tick
from trader.universe.providers.krx_provider import safe_get_market_cap_by_ticker

logger = logging.getLogger(__name__)

# -----------------------------
# 환경 파라미터 (튜닝 가능)
# -----------------------------
K_MIN = float(os.getenv("K_MIN", "0.1"))
K_MAX = float(os.getenv("K_MAX", "1.0"))
K_STEP = float(os.getenv("K_STEP", "0.1"))
K_GRID_MODE = os.getenv("K_GRID_MODE", "fixed").lower()  # fixed|fine|atr
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

def _safe_float(x: Any, default: float | None = 0.0) -> float | None:
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
# 1) 시가총액 기준 Top-N (KOSDAQ only for rolling-k universe)
# -----------------------------
@lru_cache(maxsize=None)
def _get_listing_df_cached(markets: tuple[str, ...]) -> pd.DataFrame:
    frames: List[pd.DataFrame] = []
    for m in markets:
        try:
            df = fdr.StockListing(m).rename(columns={"Symbol": "Code", "Name": "Name"})
            df["Code"] = df["Code"].astype(str).str.zfill(6)
            frames.append(df[["Code", "Name"]])
        except Exception:
            logger.exception("❌  StockListing(%s) 실패", m)
    if not frames:
        return pd.DataFrame(columns=["Code", "Name"])
    merged = pd.concat(frames, ignore_index=True)
    merged = merged.drop_duplicates(subset=["Code"], keep="first")
    return merged


def _get_listing_df(markets: Iterable[str]) -> pd.DataFrame:
    """주어진 시장 리스트에 대한 종목명 DF 합친 후 Code 포맷을 정규화한다."""
    normalized_markets = tuple(dict.fromkeys(markets))
    return _get_listing_df_cached(normalized_markets).copy()


def _resolve_kr_market_date(date_str: Optional[str]) -> str:
    requested_dt = datetime.today() if date_str is None else datetime.strptime(date_str, "%Y-%m-%d")
    requested = requested_dt.strftime("%Y%m%d")
    req_iso = requested_dt.strftime("%Y-%m-%d")
    try:
        adjusted = get_nearest_business_day_in_a_week(requested)
    except Exception as exc:
        logger.warning("[KRX][INDEX_OHLCV][INVALID_JSON] ticker=1001 date_from=%s date_to=%s err=%s", requested, requested, exc)
        adjusted_dt = requested_dt
        while adjusted_dt.weekday() >= 5:
            adjusted_dt -= timedelta(days=1)
        adjusted = adjusted_dt.strftime("%Y%m%d")
    adj_iso = datetime.strptime(adjusted, "%Y%m%d").strftime("%Y-%m-%d")
    if adjusted != requested:
        logger.info("[MARKET_DATE][ADJUST] market=KR requested=%s adjusted=%s reason=non_trading_day", req_iso, adj_iso)
    else:
        logger.info("[MARKET_DATE][USE] market=KR trade_date=%s reason=trading_day_intraday", adj_iso)
    return adjusted

def _get_top_n_for_market(date_str: Optional[str], n: int, market: str) -> pd.DataFrame:
    """주어진 시장의 시가총액 상위 n개 종목 반환."""
    try:
        from_date = _resolve_kr_market_date(date_str)
        logger.info("📅 pykrx 시총 조회일(%s) → %s", market, from_date)

        mktcap_df = safe_get_market_cap_by_ticker(from_date, market=market)
        if mktcap_df is None or len(mktcap_df) == 0:
            logger.warning("[UNIVERSE][EMPTY] market=%s reason=KRX_DATA_UNAVAILABLE", market)
            return pd.DataFrame(columns=["Code", "Name", "Marcap"])

        mktcap_df = mktcap_df.reset_index()
        capcol = _find_column(mktcap_df, "시가총액")
        ticcol = _find_column(mktcap_df, "티커") or _find_column(mktcap_df, "코드")
        if capcol is None or ticcol is None:
            logger.error("❌  %s 시총/티커 컬럼 탐색 실패 → 빈 DF 반환", market)
            return pd.DataFrame(columns=["Code", "Name", "Marcap"])

        mktcap_df = mktcap_df.rename(columns={capcol: "Marcap", ticcol: "Code"})
        mktcap_df["Code"] = mktcap_df["Code"].astype(str).str.zfill(6)

        fdr_df = _get_listing_df([market])
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
            logger.error("❌  병합 후에도 'Marcap' 없음(%s) → 빈 DF 반환", market)
            return pd.DataFrame(columns=["Code", "Name", "Marcap"])

        topn = merged.dropna(subset=["Marcap"])
        # 6자리 숫자 코드만 사용 (우선주/ETN 등 특수코드, 0009K0 같은 것 제거)
        topn = topn[topn["Code"].astype(str).str.match(r"^\d{6}$")]
        topn = topn.sort_values("Marcap", ascending=False).head(n)
        logger.info(f"✅  {market} 시총 Top{n} 추출 완료 → {len(topn)} 종목")
        return topn[["Code", "Name", "Marcap"]]

    except Exception as exc:
        if type(exc).__name__ == "JSONDecodeError":
            logger.warning("[KRX][INDEX_OHLCV][INVALID_JSON] ticker=%s date_from=%s date_to=%s", "market_cap", date_str, date_str)
        logger.warning("[UNIVERSE][EMPTY] market=%s reason=KRX_DATA_UNAVAILABLE err=%s", market, repr(exc))
        return pd.DataFrame(columns=["Code", "Name", "Marcap"])

def get_kosdaq_top_n(date_str: Optional[str] = None, n: int = TOP_N) -> pd.DataFrame:
    return _get_top_n_for_market(date_str, n, market="KOSDAQ")

def get_kospi_top_n(date_str: Optional[str] = None, n: int = TOP_N) -> pd.DataFrame:
    return _get_top_n_for_market(date_str, n, market="KOSPI")

# -----------------------------
# ATR 계산(월 데이터 레코드에서)
# -----------------------------
def _compute_atr_from_records(records: List[Dict[str, Any]], window: int = 14) -> Optional[float]:
    """월 구간 레코드([{open,high,low,close}...])에서 ATR 계산."""
    if not records or len(records) < window + 1:
        return None
    df = pd.DataFrame(records).copy()
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
    """
    base_date를 기준으로 이전 거래일까지의 데이터를 수집하여
    year/quarter/month 세그먼트로 반환.
    """
    try:
        start_date = base_date - timedelta(days=400)
        end_date = base_date - timedelta(days=1)
        df = fdr.DataReader(code, start=start_date, end=end_date)
        df = (
            df.dropna(subset=["Open", "High", "Low", "Close", "Volume"])
            .rename(columns={
                "Open": "open", "High": "high", "Low": "low",
                "Close": "close", "Volume": "volume"
            })
            .reset_index()
        )
        if "Date" not in df.columns:
            if df.index.name is not None:
                df = df.rename_axis("Date").reset_index()
            else:
                raise ValueError("DataReader 결과에 Date 컬럼이 없음")
        df["date"] = pd.to_datetime(df["Date"]).dt.date
        df = df[["date", "open", "high", "low", "close", "volume"]].sort_values("date")
        prev_records = df[df["date"] < base_date].tail(1).to_dict("records")
        return {
            "year": df[df["date"] >= base_date - timedelta(days=365)].to_dict("records"),
            "quarter": df[df["date"] >= base_date - timedelta(days=90)].to_dict("records"),
            "month": df[df["date"] >= base_date - timedelta(days=30)].to_dict("records"),
            "prev": prev_records,
        }
    except Exception as e:
        logger.exception(f"[ERROR] ❌ Failed to fetch data for {code}: {e}")
        return {"year": [], "quarter": [], "month": [], "prev": []}

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

def _inject_forced_codes(universe_df: pd.DataFrame, forced_codes: List[str], markets: Iterable[str]) -> pd.DataFrame:
    if not forced_codes:
        return universe_df
    fdr_df = _get_listing_df(list(markets))
    force_df = fdr_df[fdr_df["Code"].isin(forced_codes)][["Code", "Name"]].copy()
    missing = [c for c in forced_codes if c not in set(force_df["Code"])]
    if missing:
        force_df = pd.concat(
            [force_df, pd.DataFrame({"Code": missing, "Name": [None] * len(missing)})],
            ignore_index=True,
        )
    uni = universe_df.copy()
    uni = pd.concat([uni[["Code", "Name", "Marcap"]], force_df.assign(Marcap=np.nan)], ignore_index=True)
    uni = uni.drop_duplicates(subset=["Code"], keep="first")
    return uni

def _calc_best_k_for_universe(
    universe_df: pd.DataFrame,
    rebalance_date: date,
    forced_codes: List[str],
    market: str,
) -> List[Dict[str, Any]]:
    results: Dict[str, Dict[str, Any]] = {}

    for _, stock in universe_df.iterrows():
        code, name = str(stock["Code"]).zfill(6), stock.get("Name")
        try:
            segments = get_price_data_segments(code, rebalance_date)
            month_data = segments["month"]

            if not month_data:
                logger.debug(f"[SKIP] {name}({code}) 전월 데이터 없음")
                if code in forced_codes and KEEP_HELD_BYPASS_FILTERS:
                    results[code] = {
                        "code": code, "name": name, "market": market, "best_k": 0.5,
                        "avg_return_pct": 0.0, "win_rate_pct": 0.0,
                        "mdd_pct": 0.0, "trades": 0, "cumulative_return_pct": 0.0,
                        "avg_holding_days": 0.0, "sharpe_m": 0.0,
                        "목표가": None, "close": None,
                        "prev_open": None, "prev_high": None, "prev_low": None, "prev_close": None, "prev_volume": None, "prev_turnover": None,
                        "forced_include": True, "filtered_reason": "NO_DATA",
                        "qty": None, "weight": None, "k_grid_mode": K_GRID_MODE,
                    }
                continue

            # K grid → best_k 선택
            k_range = _build_k_range(code, month_data)
            m_metrics = simulate_k_range_for(code, month_data, k_range=k_range)
            best_k = get_best_k_meta([], [], m_metrics)

            # 성능 지표(월)
            month_perf = simulate_with_k_and_get_metrics(code, best_k, month_data)
            avg_return = float(month_perf.get("avg_return_pct", 0.0))
            win_rate = float(month_perf.get("win_rate_pct", 0.0))
            mdd = float(abs(month_perf.get("mdd_pct", 0.0)))
            trades = int(month_perf.get("trades", 0))
            cumret = float(month_perf.get("cumulative_return_pct", 0.0))
            sharpe_m = float(month_perf.get("sharpe_m", 0.0))
            avg_hold = float(month_perf.get("avg_holding_days", 0.0))

            # 데이터 부족 or 필터링
            if trades < MIN_TRADES:
                logger.debug(f"[SKIP] {name}({code}) trades<{MIN_TRADES}")
                if code in forced_codes and KEEP_HELD_BYPASS_FILTERS:
                    results[code] = {
                        "code": code, "name": name, "market": market, "best_k": best_k,
                        "avg_return_pct": avg_return, "win_rate_pct": win_rate,
                        "mdd_pct": mdd, "trades": trades, "cumulative_return_pct": cumret,
                        "avg_holding_days": avg_hold, "sharpe_m": sharpe_m,
                        "목표가": None, "close": None,
                        "prev_open": None, "prev_high": None, "prev_low": None, "prev_close": None, "prev_volume": None, "prev_turnover": None,
                        "forced_include": True, "filtered_reason": "LOW_TRADES",
                        "qty": None, "weight": None, "k_grid_mode": K_GRID_MODE,
                    }
                continue

            if mdd > MAX_MDD_PCT:
                logger.debug(f"[SKIP] {name}({code}) mdd>{MAX_MDD_PCT}")
                if code in forced_codes and KEEP_HELD_BYPASS_FILTERS:
                    results[code] = {
                        "code": code, "name": name, "market": market, "best_k": best_k,
                        "avg_return_pct": avg_return, "win_rate_pct": win_rate,
                        "mdd_pct": mdd, "trades": trades, "cumulative_return_pct": cumret,
                        "avg_holding_days": avg_hold, "sharpe_m": sharpe_m,
                        "목표가": None, "close": None,
                        "prev_open": None, "prev_high": None, "prev_low": None, "prev_close": None, "prev_volume": None, "prev_turnover": None,
                        "forced_include": True, "filtered_reason": "HIGH_MDD",
                        "qty": None, "weight": None, "k_grid_mode": K_GRID_MODE,
                    }
                continue

            if REQUIRE_POS_RET and avg_return <= 0:
                logger.debug(f"[SKIP] {name}({code}) avg_return<=0")
                if code in forced_codes and KEEP_HELD_BYPASS_FILTERS:
                    results[code] = {
                        "code": code, "name": name, "market": market, "best_k": best_k,
                        "avg_return_pct": avg_return, "win_rate_pct": win_rate,
                        "mdd_pct": mdd, "trades": trades, "cumulative_return_pct": cumret,
                        "avg_holding_days": avg_hold, "sharpe_m": sharpe_m,
                        "목표가": None, "close": None,
                        "prev_open": None, "prev_high": None, "prev_low": None, "prev_close": None, "prev_volume": None, "prev_turnover": None,
                        "forced_include": True, "filtered_reason": "NEG_RETURN",
                        "qty": None, "weight": None, "k_grid_mode": K_GRID_MODE,
                    }
                continue

            # 전일 OHLCV 로드: 1) month_data 마지막 캔들, 2) segs["prev"]
            prev_candle = None
            if month_data:
                prev_candle = month_data[-1]
            elif segments.get("prev"):
                prev_candle = segments["prev"][-1]

            prev_open = _safe_float(prev_candle.get("open") if prev_candle else None, None)
            prev_high = _safe_float(prev_candle.get("high") if prev_candle else None, None)
            prev_low = _safe_float(prev_candle.get("low") if prev_candle else None, None)
            prev_close = _safe_float(prev_candle.get("close") if prev_candle else None, None)
            prev_volume = _safe_float(prev_candle.get("volume") if prev_candle else None, None)
            prev_turnover = None
            try:
                if prev_close is not None and prev_volume is not None:
                    prev_turnover = float(prev_close) * float(prev_volume)
            except Exception:
                prev_turnover = None

            # 최종 출력
            target_price = adjust_price_to_tick(
                prev_close + (prev_high - prev_low) * best_k,
                code,
            ) if prev_close is not None and prev_high is not None and prev_low is not None else None

            close_price = float(prev_close) if prev_close is not None else None

            results[code] = {
                "code": code,
                "name": name,
                "market": market,
                "best_k": best_k,
                "avg_return_pct": avg_return,
                "win_rate_pct": win_rate,
                "mdd_pct": mdd,
                "trades": trades,
                "cumulative_return_pct": cumret,
                "avg_holding_days": avg_hold,
                "sharpe_m": sharpe_m,
                # trader.py가 읽는 필드들
                "목표가": target_price,                # (동일 키 유지)
                "target_price": target_price,         # 호환 키 추가
                "close": close_price,
                "prev_open": prev_open,
                "prev_high": prev_high,
                "prev_low": prev_low,
                "prev_close": prev_close,
                "prev_volume": prev_volume,
                "prev_turnover": prev_turnover,
                # 메타
                "forced_include": code in forced_codes,
                "k_grid_mode": K_GRID_MODE,
                # 수량은 trader.py가 weight→qty로 변환하므로 기본 None
                "qty": None,
                "weight": None,  # assign_weights 후 채워짐
            }

            logger.info(
                f"[SIM] {name}({code})[{market}] R={avg_return:.1f}% W={win_rate:.1f}% MDD={mdd:.1f}% "
                f"K={best_k} trades={trades} forced={code in forced_codes}"
            )

        except Exception as e:
            logger.exception(f"[ERR] {name}({code})[{market}] 시뮬 실패: {e}")
            continue

    logger.info(f"📊 [{market}] 필터/강제포함 반영 종목 = {len(results)}개")
    return list(results.values())


def _normalize_weights(selected: List[Dict[str, Any]], forced_codes: List[str]) -> List[Dict[str, Any]]:
    if not selected:
        return []

    selected = assign_weights(selected)  # 내부에서 'weight' 채워짐

    # 보유분 최소 비중 하한 보정 (합계 1 유지)
    if HELD_MIN_WEIGHT > 0:
        selected = _enforce_min_weight_for_forced(selected, forced_codes, min_weight=HELD_MIN_WEIGHT)

    # 사후 정규화로 weight 합계를 1.0으로 유지
    total_weight = sum(float(it.get("weight") or 0) for it in selected)
    if total_weight > 0:
        for it in selected:
            it["weight"] = float(it.get("weight") or 0) / total_weight
    return selected


def _normalize_weights_by_market(
    selected_all: List[Dict[str, Any]], forced_codes: List[str]
) -> Dict[str, List[Dict[str, Any]]]:
    """Normalize weights per market without cross-market renormalization."""

    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in selected_all:
        market = (row.get("market") or "UNKNOWN").upper()
        grouped.setdefault(market, []).append(row)

    for market, rows in grouped.items():
        grouped[market] = _normalize_weights(rows, forced_codes)
        weight_sum = sum(float(r.get("weight") or 0.0) for r in grouped[market])
        logger.info("[WEIGHT] %s weight_sum=%.6f count=%d", market, weight_sum, len(rows))

    return grouped


def get_best_k_for_kosdaq_topn(rebalance_date_str: str) -> List[Dict[str, Any]]:
    """
    리밸런싱 대상 리스트 작성:
    - code/name/best_k/weight(+qty=None) + prev_* + 목표가(close 포함)까지 채움
    - KOSDAQ TopN만 포함 (KOSPI는 별도 코어 엔진에서 처리)
    """
    rebalance_date = datetime.strptime(rebalance_date_str, "%Y-%m-%d").date()

    kosdaq_df = get_kosdaq_top_n(rebalance_date_str, n=TOP_N)
    logger.info("📈 유니버스 수집: KOSDAQ=%d (Top%d)", len(kosdaq_df), TOP_N)
    top_df = kosdaq_df.copy()
    forced_codes = _parse_force_include_codes(ALWAYS_INCLUDE_CODES)
    if forced_codes:
        top_df = _inject_forced_codes(top_df, forced_codes, ["KOSDAQ"])

    if top_df.empty:
        logger.warning("[WARN] KOSDAQ TopN 결과 없음 → 빈 리스트 반환")
        return []

    logger.info("📊 KOSDAQ 시총 TopN 유니버스 수량: %d개 (고유)", len(top_df))

    selected = _calc_best_k_for_universe(top_df, rebalance_date, forced_codes, market="KOSDAQ")

    return _normalize_weights(selected, forced_codes)


def get_best_k_for_krx_topn(
    rebalance_date_str: str,
    markets: list[str] | tuple[str, ...] = ("KOSDAQ", "KOSPI"),
    topn_map: dict[str, int] | None = None,
    return_by_market: bool = False,
) -> List[Dict[str, Any]] | Dict[str, Any]:
    """시장별 Top-N을 합쳐 K 최적화 리스트를 생성한다."""
    rebalance_date = datetime.strptime(rebalance_date_str, "%Y-%m-%d").date()
    markets_seq = list(dict.fromkeys(markets))
    if not markets_seq:
        markets_seq = ["KOSDAQ", "KOSPI"]
    topn_map = topn_map or {"KOSDAQ": TOP_N, "KOSPI": TOP_N}
    forced_codes = _parse_force_include_codes(ALWAYS_INCLUDE_CODES)

    all_selected: List[Dict[str, Any]] = []
    for market in markets_seq:
        n = int(topn_map.get(market, TOP_N))
        uni_df = _get_top_n_for_market(rebalance_date_str, n=n, market=market)
        logger.info("📈 유니버스 수집: %s=%d (Top%d)", market, len(uni_df), n)
        if forced_codes:
            uni_df = _inject_forced_codes(uni_df, forced_codes, [market])
        if uni_df.empty:
            logger.warning("[WARN] %s TopN 결과 없음 → 건너뜀", market)
            continue
        logger.info("📊 %s 시총 TopN 유니버스 수량: %d개 (고유)", market, len(uni_df))
        selected = _calc_best_k_for_universe(uni_df, rebalance_date, forced_codes, market=market)
        logger.info("[SELECT] %s 최종 선정 %d개", market, len(selected))
        all_selected.extend(selected)

    if not all_selected:
        return {"selected": [], "selected_by_market": {}} if return_by_market else []

    by_market = _normalize_weights_by_market(all_selected, forced_codes)
    merged_per_market: List[Dict[str, Any]] = []
    for rows in by_market.values():
        merged_per_market.extend(rows)

    merged_global: List[Dict[str, Any]] = [dict(r) for r in merged_per_market]
    total_weight = sum(float(it.get("weight") or 0.0) for it in merged_global)
    if total_weight > 0:
        for it in merged_global:
            it["weight"] = float(it.get("weight") or 0.0) / total_weight

    counts: Dict[str, int] = {}
    for row in merged_per_market:
        mkt = row.get("market") or "UNKNOWN"
        counts[mkt] = counts.get(mkt, 0) + 1
    for mkt, cnt in counts.items():
        logger.info("[COUNT] %s selected_count=%d", mkt, cnt)

    if return_by_market:
        return {
            "selected": merged_global,
            "selected_by_market": by_market,
            "weight_scope": {"selected": "global", "selected_by_market": "per_market"},
        }
    return merged_global


# Backward compatibility alias for callers that still want the KOSDAQ-only variant
get_best_k_for_kosdaq_only = get_best_k_for_kosdaq_topn

# -----------------------------
# 5) API 진입점: /rebalance/run/{date} 에서 호출
# -----------------------------
def run_rebalance(
    date: str, force_order: bool = False, return_by_market: bool = False
) -> Dict[str, Any]:
    """
    /rebalance/run/{date} 엔드포인트에서 직접 호출되는 진입점.
    반환 스키마는 trader.py/main.py가 기대하는 형태로 보장한다.

    Returns:
        {
          "selected": [ ... ],
          "selected_stocks": [ ... ]  # 동일 배열(호환성)
        }
    """
    try:
        results = get_best_k_for_krx_topn(date, return_by_market=return_by_market)
        if isinstance(results, dict):
            selected = results.get("selected", [])
            selected_by_market = results.get("selected_by_market", {})
            weight_scope = results.get("weight_scope")
        else:
            selected = results
            selected_by_market = {}
            weight_scope = None
        # force_order가 True라고 해서 여기서 실주문을 내지 않음.
        # (주문은 trader.py가 관리) — 필요 시 'strategy'에 플래그만 남김.
        for it in selected:
            it.setdefault("strategy", "전월 rolling K 최적화")
    except Exception as e:
        logger.exception("[run_rebalance] failed: %s", e)
        selected = []
        selected_by_market = {}
        weight_scope = None

    payload: Dict[str, Any] = {
        "selected": selected,
        "selected_stocks": selected,
        "selected_by_market": selected_by_market,
    }
    if weight_scope:
        payload["weight_scope"] = weight_scope
    return payload
