# -*- coding: utf-8 -*-
""" RK-Max utilities
- 환경변수 헬퍼
- 랭크 지수 가중치
- Jaccard 유사도
- 시장 브레드스(20일 상승 비율)
- 최근 특성(mom5, 거래대금 스파이크, ATR20/60)
- K 블렌딩(월간 K + 최근 변동성)
- 점착도(Sticky) 교체 기준
"""
from __future__ import annotations

import os
import math
import logging
from typing import Dict, Iterable, Optional, Tuple, Any, List

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

# -------- data helpers --------
def _kis_ohlc_to_df(js: Dict) -> pd.DataFrame:
    """
    한국투자증권 일봉 응답(dict)을 pandas DataFrame으로 변환.
    - 신/구 사양을 모두 견딜 수 있게 'output1' 또는 'output' 키를 수용
    - 최신 → 과거 순으로 들어오는 경우가 있어, 항상 '과거→최신'으로 정렬
    필드(일반적): stck_clpr(종가), stck_hgpr(고가), stck_lwpr(저가), stck_trdval(거래대금)
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
    # API별로 'basDt' 또는 'stck_bsop_date' 등의 필드가 있을 수 있음
    date_col = None
    for cand in ("basDt", "stck_bsop_date", "date"):
        if cand in df.columns:
            date_col = cand
            break

    if date_col:
        # 날짜 파싱 실패 시 원본 순서 유지
        try:
            df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
            df = df.sort_values(date_col).reset_index(drop=True)
        except Exception:
            df = df.reset_index(drop=True)
    else:
        # 날짜 필드가 없으면 그냥 역순(최신이 위)일 가능성을 고려해 뒤집기
        df = df.iloc[::-1].reset_index(drop=True)

    return df

# -------- market breadth --------
def breadth_pos_ratio(kis, codes: Iterable[str], lookback: int = 20) -> int:
    """
    유니버스에서 '20일 수익률 > 0' 인 종목 비율(%)
    kis: 한국투자 API 래퍼 인스턴스 (get_daily_ohlc(code, start, end))
    """
    pos = 0
    cnt = 0
    # 조회 기간 넉넉히
    end = pd.Timestamp.now(tz="Asia/Seoul").strftime("%Y%m%d")
    start = (pd.Timestamp.now(tz="Asia/Seoul") - pd.Timedelta(days=160)).strftime(
        "%Y%m%d"
    )

    for code in set(codes or []):
        try:
            js = kis.get_daily_ohlc(str(code), start, end)
            df = _kis_ohlc_to_df(js)
            cl = df.get("stck_clpr")
            if cl is None or cl.dropna().shape[0] < (lookback + 1):
                continue
            cl = cl.astype(float).to_numpy()
            ret = cl[-1] / cl[-1 - lookback] - 1.0
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
    end = pd.Timestamp.now(tz="Asia/Seoul").strftime("%Y%m%d")
    start = (pd.Timestamp.now(tz="Asia/Seoul") - pd.Timedelta(days=260)).strftime(
        "%Y%m%d"
    )

    js = kis.get_daily_ohlc(str(code), start, end)
    df = _kis_ohlc_to_df(js)

    if df.empty or ("stck_clpr" not in df.columns):
        return dict(mom5=np.nan, spike=np.nan, atr20=np.nan, atr60=np.nan)

    cl = df["stck_clpr"].astype(float)
    mom5 = (
        (cl.iloc[-1] / cl.iloc[-6] - 1.0) * 100.0
        if cl.shape[0] >= 6
        else float("nan")
    )

    tv = df.get("stck_trdval")
    adtv5 = float(tv.tail(5).mean()) if (tv is not None and tv.shape[0] >= 5) else np.nan
    adtv20 = (
        float(tv.tail(20).mean()) if (tv is not None and tv.shape[0] >= 20) else np.nan
    )
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
        LOG.info("[K-blend] day=%d w=%.2f Km=%.2f (no recent) -> Ku=%.2f",
                 day_of_month, w, k_month, k_use)
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
def get_best_k_meta(_, __, k_metrics):
    # 가장 단순하게 avg_return_pct 최대 K 선택
    if not k_metrics:
        return 0.5
    best = max(k_metrics, key=lambda x: x.get('avg_return_pct', -999))
    return best.get('k', 0.5)

def assign_weights(selected):
    # 동등가중치 할당 예시
    if not selected:
        return []
    w = 1.0 / len(selected)
    for s in selected:
        s['weight'] = w
    return selected

def _enforce_min_weight_for_forced(selected, forced_codes, min_weight=0.08):
    # 강제포함 종목의 최소 weight 보장 (예시)
    total = sum(s['weight'] for s in selected)
    n_forced = sum(1 for s in selected if s.get('forced_include'))
    if n_forced == 0:
        return selected
    for s in selected:
        if s.get('forced_include') and s['weight'] < min_weight:
            s['weight'] = min_weight
    # 나머지 종목들의 weight는 비율에 맞게 scale down
    forced_weight_sum = sum(s['weight'] for s in selected if s.get('forced_include'))
    left = 1.0 - forced_weight_sum
    others = [s for s in selected if not s.get('forced_include')]
    if others and left > 0:
        w = left / len(others)
        for s in others:
            s['weight'] = w
    return selected

# --- (끝) ---

# === Champion Mode: dynamic position sizing & top-N selection ===

def _safe_float(val, default: float = 0.0) -> float:
    try:
        if val is None or (isinstance(val, str) and not str(val).strip()):
            return default
        return float(val)
    except Exception:
        return default


def compute_champion_score(candidate: Dict[str, Any]) -> float:
    """RK-Max용 챔피언 종목 스코어링 함수.

    기대되는 필드 (없으면 0으로 처리):
      - meta_score: Meta-K 통합 스코어
      - momentum_5d or mom5: 단기 모멘텀
      - win_rate: 백테스트 승률 (%)
      - mdd: 백테스트 MDD (% / -값일 수 있음)
      - vwap_premium: (종가 - VWAP)/VWAP * 100 (%)
    """
    base = _safe_float(
        candidate.get("meta_score")
        or candidate.get("score")
        or candidate.get("rk_score")
    )
    momo = _safe_float(
        candidate.get("momentum_5d")
        or candidate.get("mom5")
        or candidate.get("momentum")
    )
    win = _safe_float(candidate.get("win_rate"))
    mdd = _safe_float(candidate.get("mdd"))
    vwap = _safe_float(candidate.get("vwap_premium"))

    # 가중합 기반 스코어
    score = (
        base * 0.4 +
        momo * 0.3 +
        win * 0.2 -
        abs(mdd) * 0.05 +
        vwap * 0.1
    )
    return score


def decide_position_limit(candidates: Iterable[Dict[str, Any]]) -> int:
    """오늘 가져갈 포지션 개수(0~4개)를 결정.

    - 후보 수가 적으면(<=2) → 그 수만큼만
    - 평균/상위 모멘텀이 강하면 포지션 수를 늘림
    - 전체적으로 약하면 1~2개로 축소
    """
    candidates = list(candidates or [])
    n = len(candidates)
    if n == 0:
        return 0
    if n <= 2:
        return n

    # 모멘텀 기준으로 강도 측정
    momos: List[float] = []
    for c in candidates:
        mom = _safe_float(
            c.get("momentum_5d")
            or c.get("mom5")
            or c.get("momentum")
        )
        if mom != 0:
            momos.append(mom)

    if not momos:
        # 정보 없으면 기본 2개
        return min(2, n)

    momos.sort(reverse=True)
    top_k = momos[: min(5, len(momos))]
    avg_top = sum(top_k) / len(top_k)

    # 임계값은 경험적 설정 (단기 모멘텀 % 기준 가정)
    if avg_top >= 10:
        # 아주 강한 장 → 최대 4개
        return min(4, n)
    elif avg_top >= 5:
        # 적당히 강한 장 → 3개
        return min(3, n)
    elif avg_top >= 2:
        # 애매한 장 → 2개
        return min(2, n)
    else:
        # 약한 장 → 1개만
        return 1


from typing import Iterable, Dict, Any, Iterable

def select_champions(
    candidates: Iterable[Dict[str, Any]],
    position_limit: int,
    allowed_grades: tuple[str, ...] = ("A", "B"),
) -> list[Dict[str, Any]]:
    """후보군 중에서 챔피언 종목만 선별.

    - compute_champion_score() 기준으로 내림차순 정렬
    - allowed_grades 에 포함된 champion_grade 만 사용 (기본: A/B)
    - 상위 position_limit개 반환
    - 각 종목에 champ_score / champ_rank 필드 부여
    """
    if position_limit <= 0:
        return []

    # 🔵 등급 필터 적용 (기본: A/B급 모두 허용)
    raw = list(candidates or [])
    if not raw:
        return []

    if allowed_grades:
        arr = [
            c for c in raw
            if c.get("champion_grade") in allowed_grades
        ]
    else:
        # allowed_grades=None 또는 () 면 필터 없이 전체 사용
        arr = raw

    if not arr:
        return []

    scored: list[tuple[float, Dict[str, Any]]] = []
    for c in arr:
        s = compute_champion_score(c)
        scored.append((s, c))

    scored.sort(key=lambda x: x[0], reverse=True)
    top = scored[: position_limit]

    champions: list[Dict[str, Any]] = []
    for rank, (s, c) in enumerate(top, start=1):
        c = dict(c)  # 복사해서 부가 정보 부여
        c["champ_score"] = s
        c["champ_rank"] = rank
        champions.append(c)

    return champions
