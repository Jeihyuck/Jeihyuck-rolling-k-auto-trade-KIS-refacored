"""
final30 rank_final30 repair 테스트
"""
from __future__ import annotations
import pandas as pd
import pytest


def _apply_rank_repair(df: pd.DataFrame) -> pd.DataFrame:
    """repos.py의 rank_final30 복구 로직을 그대로 재현"""
    if df.empty:
        return df

    rank_col = "rank_final30"
    if rank_col not in df.columns:
        df[rank_col] = 0

    rank_invalid = (
        df[rank_col].isna().any()
        or df[rank_col].nunique(dropna=True) != len(df)
        or float(df[rank_col].astype(float).min()) <= 0
    )
    if rank_invalid:
        if "rank" in df.columns:
            df = df.sort_values("rank", ascending=True)
        elif "score_final" in df.columns:
            df = df.sort_values("score_final", ascending=False)
        elif "score" in df.columns:
            df = df.sort_values("score", ascending=False)
        df = df.reset_index(drop=True)
        df["rank_final30"] = range(1, len(df) + 1)
    return df


def test_final30_rank_repair_assigns_unique_1_to_30():
    """rank_final30이 전부 0인 경우 repair 후 1~30 unique 보장"""
    n = 30
    df = pd.DataFrame({
        "code": [str(i).zfill(6) for i in range(1, n + 1)],
        "rank_final30": [0.0] * n,
        "score_final": [float(n - i) for i in range(n)],
    })
    df = _apply_rank_repair(df)

    assert df["rank_final30"].nunique() == n
    assert int(df["rank_final30"].min()) == 1
    assert int(df["rank_final30"].max()) == n


def test_final30_rank_repair_when_all_same():
    """rank_final30이 모두 동일값인 경우 repair"""
    n = 10
    df = pd.DataFrame({
        "code": [str(i).zfill(6) for i in range(1, n + 1)],
        "rank_final30": [5] * n,
        "score_final": [float(n - i) for i in range(n)],
    })
    df = _apply_rank_repair(df)

    assert df["rank_final30"].nunique() == n


def test_final30_rank_valid_not_changed():
    """rank_final30이 이미 유효하면 변경 없음"""
    n = 5
    df = pd.DataFrame({
        "code": [str(i).zfill(6) for i in range(1, n + 1)],
        "rank_final30": list(range(1, n + 1)),
        "score_final": [float(n - i) for i in range(n)],
    })
    original = df["rank_final30"].tolist()
    df = _apply_rank_repair(df)

    assert df["rank_final30"].tolist() == original
