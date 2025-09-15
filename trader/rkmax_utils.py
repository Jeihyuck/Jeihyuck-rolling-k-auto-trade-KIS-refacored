# -*- coding: utf-8 -*-
"""
RK-Max utilities
- 환경변수 헬퍼
- 랭크 지수 가중치
- Jaccard 유사도
- 시장 브레드스(20일 상승 비율)
- 최근 특성(mom5, 거래대금 스파이크, ATR20/60)
- K 블렌딩(월간 K + 최근 변동성)
- 점착도(Sticky) 교체 기준

[업데이트 요약]
- kis.get_daily_ohlc()가 없어도 동작하도록 **KisAPI.get_daily_candles() 폴백** 추가
- 일봉 DataFrame 어댑터 강화: KIS 원본 JSON/가공 list-of-dict 모두 수용
- 거래대금(trdval) 부재 시 spike=NaN 처리(정상동작)
"""
from __future__ import annotations

import os
import math
import logging
from typing import Dict, Iterable, Optional, Tuple

import numpy as np
import pandas as pd

# 로거 (외부에서 핸들러 설정)
LOG = logging.getLogger("rkmax")
if not LOG.handlers:
    LOG.addHandler(logging.NullHandler())


# -------- env helper --------
def _env(key: str, default=None, cast=str):
    """환경변수 안전 읽기 (형변환 실패 시 default 반환)"""
    v = os.getenv(key)
    if v is None:
        return default
    try:
        return cast(v)
    except Exception:
        return default


# -------- weights / set similarity --------
def rank_weights_exp(n: int, alpha: float = 0.35) -> np.ndarray:
    """
    순위 1..n (1=최상위)에 대해 지수 가중치 부여.
    w_r ∝ exp(alpha * (n+1 - r))
    """
    if n <= 0:
        return np.array([])
    r = np.arange(1, n + 1)
    w = np.exp(alpha * (n + 1 - r))
    return w / w.sum()


def jaccard(a: Iterable, b: Iterable) -> float:
    """집합 유사도 지표: |A∩B| / |A∪B|"""
    sa, sb = set(a or []), set(b or [])
    if not sa and not sb:
        return 1.0
    return len(sa & sb) / float(max(1, len(sa | sb)))


# -------- data adapters --------
def _kis_ohlc_to_df(js: Dict) -> pd.DataFrame:
    """
    한국투자증권 **원본 일봉 응답(dict)** → pandas DataFrame.
    - 'output1' 또는 'output' 키를 수용
    - 최신→과거 순으로 들어오는 경우가 있어, 항상 '과거→현재'로 정렬
    - 컬럼 표준화: stck_oprc, stck_hgpr, stck_lwpr, stck_clpr, stck_trdval
    """
    out = js.get("output1") or js.get("output") or []
    df = pd.DataFrame(out).copy()
    if df.empty:
        return df

    # 숫자 컬럼 캐스팅
    for c in ("stck_clpr", "stck_hgpr", "stck_lwpr", "stck_oprc", "stck_trdval"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # 날짜 정렬: 과거→현재
    date_col = None
    for cand in ("basDt", "stck_bsop_date", "date"):
        if cand in df.columns:
            date_col = cand
            break

    if date_col:
        try:
            df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
            df = df.sort_values(date_col).reset_index(drop=True)
        except Exception:
            df = df.reset_index(drop=True)
    else:
        df = df.iloc[::-1].reset_index(drop=True)

    # 필요한 컬럼 없는 경우 NaN 추가
    for c in ("stck_oprc", "stck_hgpr", "stck_lwpr", "stck_clpr", "stck_trdval"):
        if c not in df.columns:
            df[c] = np.nan
    return df


def _candles_list_to_df(rows: Iterable[Dict]) -> pd.DataFrame:
    """KisAPI.get_daily_candles() 결과(list of dict) → 표준 컬럼 DF로 변환."""
    df = pd.DataFrame(list(rows) if rows is not None else [])
    if df.empty:
        return df
    # 기대 컬럼: date, open, high, low, close
    # 표준 컬럼명으로 매핑
    rename = {
        "open": "stck_oprc",
        "high": "stck_hgpr",
        "low": "stck_lwpr",
        "close": "stck_clpr",
    }
    df = df.rename(columns=rename)

    # 숫자 캐스팅
    for c in ("stck_oprc", "stck_hgpr", "stck_lwpr", "stck_clpr"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    # 거래대금은 제공되지 않을 수 있음
    if "stck_trdval" not in df.columns:
        df["stck_trdval"] = np.nan

    # 날짜 처리
    if "date" in df.columns:
        try:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            df = df.sort_values("date").reset_index(drop=True)
        except Exception:
            df = df.reset_index(drop=True)
    return df


def _fetch_daily_df(kis, code: str, need_count: int = 260) -> pd.DataFrame:
    """일봉 DF를 얻는 통합 헬퍼.
    - 선호: kis.get_daily_ohlc(code, start, end)  (KIS 원본 JSON)
    - 폴백: kis.get_daily_candles(code, count=need_count)  (가공 rows)
    """
    # 1) 원본 API 경로 지원 시도
    if hasattr(kis, "get_daily_ohlc"):
        try:
            end = pd.Timestamp.now(tz="Asia/Seoul").strftime("%Y%m%d")
            start = (pd.Timestamp.now(tz="Asia/Seoul") - pd.Timedelta(days=need_count * 2)).strftime("%Y%m%d")
            js = kis.get_daily_ohlc(str(code), start, end)
            df = _kis_ohlc_to_df(js)
            if not df.empty:
                return df
        except Exception as e:
            LOG.warning("[rkmax] get_daily_ohlc 실패, candles 폴백 사용: %s", e)

    # 2) 가공 candles 폴백
    if hasattr(kis, "get_daily_candles"):
        try:
            rows = kis.get_daily_candles(str(code), count=need_count)
            return _candles_list_to_df(rows)
        except Exception as e:
            LOG.warning("[rkmax] get_daily_candles 실패: %s", e)

    return pd.DataFrame()


# -------- market breadth --------
def breadth_pos_ratio(kis, codes: Iterable[str], lookback: int = 20) -> int:
    """
    유니버스에서 'lookback일 수익률 > 0' 비율(%)
    kis: 한국투자 API 래퍼 인스턴스
    """
    pos = 0
    cnt = 0
    for code in set(codes or []):
        try:
            df = _fetch_daily_df(kis, code, need_count=max(lookback + 10, 60))
            cl = df.get("stck_clpr")
            if cl is None or cl.dropna().shape[0] < (lookback + 1):
                continue
            clv = cl.astype(float).to_numpy()
            ret = clv[-1] / clv[-1 - lookback] - 1.0
            pos += 1 if ret > 0 else 0
            cnt += 1
        except Exception as e:
            LOG.warning("breadth fail %s: %s", code, e)
    return int(round(100 * (pos / cnt))) if cnt > 0 else 0


# -------- recent features / ATR --------
def _atr_from_hl(df: pd.DataFrame, window: int) -> float:
    """고가-저가 단순 범위 평균으로 ATR 근사(거래량 계산부하 최소화용)"""
    if df is None or df.empty or window <= 0:
        return float("nan")
    if ("stck_hgpr" not in df.columns) or ("stck_lwpr" not in df.columns):
        return float("nan")
    rng = (df["stck_hgpr"] - df["stck_lwpr"]).astype(float)
    if rng.shape[0] < window:
        return float("nan")
    return float(rng.tail(window).mean())


def recent_features(kis, code: str) -> Dict[str, float]:
    """
    최근 특성치:
      - mom5: 5일 수익률(%)
      - spike: ADTV5 / ADTV20 (거래대금 스파이크)
      - atr20, atr60: 고저 범위 기반 ATR 근사
    """
    df = _fetch_daily_df(kis, code, need_count=260)
    if df.empty or ("stck_clpr" not in df.columns):
        return dict(mom5=np.nan, spike=np.nan, atr20=np.nan, atr60=np.nan)

    cl = df["stck_clpr"].astype(float)
    mom5 = (cl.iloc[-1] / cl.iloc[-6] - 1.0) * 100.0 if cl.shape[0] >= 6 else float("nan")

    tv = df.get("stck_trdval")
    adtv5 = float(tv.tail(5).mean()) if (tv is not None and tv.shape[0] >= 5) else np.nan
    adtv20 = float(tv.tail(20).mean()) if (tv is not None and tv.shape[0] >= 20) else np.nan
    spike = (adtv5 / adtv20) if (adtv5 and adtv20 and adtv20 > 0) else np.nan

    atr20 = _atr_from_hl(df, 20)
    atr60 = _atr_from_hl(df, 60)

    return dict(mom5=float(mom5), spike=float(spike), atr20=float(atr20), atr60=float(atr60))


# -------- K blending --------
def blend_k(k_month: float, day_of_month: int, atr20: float, atr60: float) -> float:
    """
    월간 K와 최근 변동성 비율(ATR20/ATR60)을 섞는 블렌딩.
    K_use = w * K_month + (1-w) * K_recent
      where w = exp(- day_of_month / HALF_LIFE_DAYS)
            K_recent = clip(K_month * (ATR20/ATR60), KREC_MIN..KREC_MAX)
    """
    if not _env("K_BLEND", 1, int):
        return float(k_month)

    half = _env("HALF_LIFE_DAYS", 10.0, float)
    w = math.exp(-float(day_of_month) / max(1.0, float(half)))

    krec_min = _env("KREC_MIN", 0.1, float)
    krec_max = _env("KREC_MAX", 0.7, float)

    if not atr20 or not atr60 or atr60 <= 0:
        k_use = float(k_month)
        LOG.info(
            "[K-blend] day=%d w=%.2f Km=%.2f (no recent) -> Ku=%.2f",
            day_of_month, w, k_month, k_use,
        )
        return k_use

    k_recent = float(k_month) * float(atr20) / float(atr60)
    k_recent = max(krec_min, min(krec_max, k_recent))
    k_use = w * float(k_month) + (1.0 - w) * k_recent

    LOG.info(
        "[K-blend] day=%d w=%.2f Km=%.2f Kr=%.2f -> Ku=%.2f",
        day_of_month,
        w,
        float(k_month),
        float(k_recent),
        float(k_use),
    )
    return float(k_use)


# -------- sticky replace --------
def sticky_replace(old_min_rar: float, new_rar: float, delta: float = 0.10) -> bool:
    """
    점착도 규칙: 신규 편입은 '기존 최하위 RAR × (1+δ)' 보다 커야 함.
    """
    try:
        if old_min_rar is None or np.isnan(old_min_rar):
            return True
        return float(new_rar) > float(old_min_rar) * (1.0 + float(delta))
    except Exception:
        return True


# ---- (옵션) rolling ret/mdd 근사 ----
def rolling_ret_mdd_from_close(closes: pd.Series, window: int = 20) -> Tuple[float, float]:
    """
    종가 시리즈로부터 window 구간 수익률(%), MDD(%) 근사 계산.
    """
    if closes is None or closes.dropna().shape[0] < (window + 1):
        return (float("nan"), float("nan"))
    x = closes.dropna().astype(float).to_numpy()
    seg = x[-(window + 1):]
    ret = (seg[-1] / seg[0] - 1.0) * 100.0
    runmax = np.maximum.accumulate(seg)
    dd = (seg / runmax - 1.0) * 100.0
    mdd = abs(dd.min())
    return float(ret), float(mdd)
