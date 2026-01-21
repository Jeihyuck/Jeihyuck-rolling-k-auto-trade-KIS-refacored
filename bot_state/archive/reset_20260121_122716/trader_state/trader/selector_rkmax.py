# -*- coding: utf-8 -*-
"""
RK-Max selector
- 전월 스냅샷 기반 1차 필터(MIN_RET_PCT, MAX_MDD_PCT, RAR>1)
- AUTO 전환: 시장 브레드스(20D>0비율)가 임계 이상이면 ROLLING, 아니면 SNAPSHOT
- SNAPSHOT 모드: 신선도 게이트(mom5, 거래대금 스파이크)
- ROLLING 모드: 간단 롤링 보정( mom5 를 RAR에 가중 )  *부하 최소화
- 점착도(Sticky) 규칙: 기존 최하위 RAR 대비 10% 이상 우수할 때만 신규 교체
- 랭크 지수 가중 배분
반환: pandas.DataFrame [rank, code, name, RAR, weight, (mom5, spike)]
    attrs: {"mode": "...", "breadth": int or None}
"""
from __future__ import annotations

import os
import logging
from typing import Iterable, Optional, Set, Tuple

import numpy as np
import pandas as pd

from .rkmax_utils import (
    _env,
    rank_weights_exp,
    breadth_pos_ratio,
    recent_features,
    sticky_replace,
)

LOG = logging.getLogger("selector")
if not LOG.handlers:
    LOG.addHandler(logging.NullHandler())


# ---------- loaders ----------
def _load_last_month(backtest_csv: str) -> pd.DataFrame:
    """
    전월 백테스트 요약 CSV 로드
    필수 컬럼: code, name, ret_m, mdd
    선택 컬럼: K, sector 등
    RAR = ret_m / max(mdd,1)
    """
    df = pd.read_csv(backtest_csv)
    need = {"code", "name", "ret_m", "mdd"}
    miss = need - set(df.columns)
    if miss:
        raise ValueError(f"CSV columns missing: {miss}")

    # 타입 정리
    df["code"] = df["code"].astype(str).str.zfill(6)
    df["name"] = df["name"].astype(str)
    df["ret_m"] = pd.to_numeric(df["ret_m"], errors="coerce")
    df["mdd"] = pd.to_numeric(df["mdd"], errors="coerce")

    df["RAR"] = df["ret_m"] / df["mdd"].clip(lower=1.0)
    df = df.sort_values(["RAR", "ret_m"], ascending=[False, False]).reset_index(drop=True)
    return df


# ---------- main ----------
def select_and_allocate(
    backtest_csv: str,
    kis,  # 한국투자 API 래퍼
    codes_universe: Iterable[str],
    yesterday_watch: Optional[Set[str]] = None,
) -> pd.DataFrame:
    """
    RK-Max 선정/배분
    - 반환 DF: [rank, code, name, RAR, weight, mom5?, spike?]
    - DF.attrs["mode"] = "SNAPSHOT"|"ROLLING"
    - DF.attrs["breadth"] = int or None
    """
    # ---- 파라미터 ----
    mode = os.getenv("SELECT_MODE", "AUTO").upper()  # AUTO | SNAPSHOT | ROLLING
    nmax = _env("NMAX", 10, int)
    min_ret = _env("MIN_RET_PCT", 2.0, float)
    max_mdd = _env("MAX_MDD_PCT", 10.0, float)
    breadth_thr = _env("BREADTH_THRESHOLD", 60, int)
    use_recency_gate = _env("RECENCY_GATE", 1, int) == 1
    mom5_min = _env("MOM5_MIN", 1.0, float)
    spike_min = _env("VOL_SPIKE_MIN", 1.2, float)
    alpha = _env("RANK_ALPHA", 0.35, float)

    # ---- 전월 스냅샷 로드 & 1차 필터 ----
    base = _load_last_month(backtest_csv)
    snap = base[(base["ret_m"] >= min_ret) & (base["mdd"] <= max_mdd) & (base["RAR"] > 1)].copy()

    # ---- AUTO: 시장 브레드스로 모드 결정 ----
    breadth = None
    if mode == "AUTO":
        breadth = breadth_pos_ratio(kis, codes_universe, lookback=20)
        mode = "ROLLING" if breadth >= int(breadth_thr) else "SNAPSHOT"
        LOG.info("AUTO -> %s (breadth=%d%%, thr=%d%%)", mode, breadth, int(breadth_thr))
    else:
        LOG.info("SELECT_MODE=%s", mode)

    # ---- 모드별 랭킹 소스 ----
    if mode == "ROLLING":
        # 부하를 최소화하기 위해 mom5로 RAR을 보정(양수 모멘텀에 가중)
        rows = []
        for _, r in snap.iterrows():
            code = str(r["code"]).zfill(6)
            try:
                f = recent_features(kis, code)
                mom_adj = max(0.0, float(f.get("mom5", 0.0))) / 100.0
                rar_adj = float(r["RAR"]) * (1.0 + mom_adj)
                rows.append((code, r["name"], rar_adj, f["mom5"], f["spike"]))
            except Exception as e:
                LOG.warning("rolling feat fail %s: %s", code, e)
        sel = pd.DataFrame(rows, columns=["code", "name", "RAR", "mom5", "spike"]).sort_values(
            "RAR", ascending=False
        )
    else:
        # SNAPSHOT: 전월 스냅샷 그대로, 단 신선도 게이트를 통과한 종목만
        sel = snap.copy()
        if use_recency_gate:
            feats = []
            # 우선 상위 후보 몇 개만 신선도 체크하여 부하 감소
            for _, r in sel.head(int(nmax * 2)).iterrows():
                code = str(r["code"]).zfill(6)
                f = recent_features(kis, code)
                feats.append(dict(code=code, mom5=f["mom5"], spike=f["spike"]))
            feat = pd.DataFrame(feats)
            before = len(sel)
            sel = sel.merge(feat, on="code", how="left")
            sel = sel[(sel["mom5"] >= mom5_min) & (sel["spike"] >= spike_min)]
            LOG.info("Recency gate: %d -> %d (mom5>=%.1f%%, spike>=%.2f)", before, len(sel), mom5_min, spike_min)

    # ---- 상위 nmax & 점착도(Sticky) ----
    sel = sel.sort_values("RAR", ascending=False).head(nmax).reset_index(drop=True)

    if yesterday_watch:
        yesterday_watch = set(str(c).zfill(6) for c in yesterday_watch)
        # 기존 편입 중 최하위 RAR
        base_rar = base[["code", "RAR"]].copy()
        base_rar["code"] = base_rar["code"].astype(str).str.zfill(6)
        old_min_rar = (
            pd.merge(
                pd.DataFrame({"code": list(yesterday_watch)}),
                base_rar,
                on="code",
                how="left",
            )["RAR"]
            .astype(float)
            .min()
        )

        confirmed_codes = []
        for _, r in sel.iterrows():
            code = str(r["code"]).zfill(6)
            if code in yesterday_watch:
                confirmed_codes.append(code)
                continue
            # 신규 후보는 점착도 기준으로 교체 허용
            try:
                new_rar = float(base_rar.loc[base_rar["code"] == code, "RAR"].iloc[0])
            except Exception:
                new_rar = float(r.get("RAR", np.nan))
            if sticky_replace(old_min_rar, new_rar, delta=0.10):
                confirmed_codes.append(code)

        sel = sel[sel["code"].isin(confirmed_codes)].reset_index(drop=True)

    # ---- 랭크 지수 가중 ----
    weights = rank_weights_exp(len(sel), alpha=alpha) if len(sel) > 0 else np.array([])
    sel.insert(0, "rank", sel.index + 1)
    sel["weight"] = np.round(weights, 6)

    # ---- 메타 (리포트용) ----
    if breadth is not None:
        sel.attrs["breadth"] = int(breadth)
    sel.attrs["mode"] = mode

    # 반환 컬럼 정리
    keep_cols = [c for c in ["rank", "code", "name", "RAR", "mom5", "spike", "weight"] if c in sel.columns]
    return sel[keep_cols].reset_index(drop=True)
