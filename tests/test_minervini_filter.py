"""
Minervini 필터 스모크 테스트

간단한 테스트로 Minervini 필터가 후보를 줄이는지 확인합니다.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from trader.strategies.pb1_minervini_v2 import (
    MinerviniConfig,
    compute_features,
    detect_vcp,
    evaluate_filters,
)


def _make_df(days: int = 252, uptrend: bool = True) -> pd.DataFrame:
    """테스트용 OHLCV 데이터 생성"""
    dates = pd.date_range(end="2024-01-30", periods=days, freq="D")
    base = 10000.0
    
    if uptrend:
        # 상승 추세: MA50 > MA150 > MA200
        close = base + np.linspace(0, base * 0.5, days)
    else:
        # 하락 추세
        close = base - np.linspace(0, base * 0.3, days)
    
    high = close * 1.02
    low = close * 0.98
    open_price = close * 1.00
    volume = np.random.randint(100000, 1000000, days)
    
    return pd.DataFrame({
        "date": dates,
        "open": open_price,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
    })


def test_compute_features_basic():
    """compute_features가 기본 지표를 계산하는지 확인"""
    df = _make_df(days=252, uptrend=True)
    features = compute_features(df)
    
    assert "close" in features
    assert "ma50" in features
    assert "ma150" in features
    assert "ma200" in features
    assert "atr14" in features
    assert "dollar_vol_50" in features
    
    # 상승 추세면 MA50 > MA150 > MA200
    assert features["ma50"] > features["ma150"]
    assert features["ma150"] > features["ma200"]


def test_detect_vcp_pass():
    """VCP 패턴 탐지가 동작하는지 확인"""
    df = _make_df(days=252, uptrend=True)
    cfg = MinerviniConfig()
    
    vcp_info = detect_vcp(df, cfg)
    
    assert "vcp_ok" in vcp_info
    assert "contractions" in vcp_info
    assert "score" in vcp_info
    assert isinstance(vcp_info["vcp_ok"], bool)


def test_evaluate_filters_uptrend_high_rs():
    """상승 추세 + 높은 RS인 경우 필터 통과 확인"""
    df = _make_df(days=252, uptrend=True)
    features = compute_features(df)
    features["rs_percentile"] = 0.85  # 85 percentile
    features["vcp_ok"] = True
    features["dollar_vol_50"] = 3.0e9  # 30억
    
    cfg = MinerviniConfig(rs_min_percentile=0.80)
    ok, reasons = evaluate_filters(features, cfg)
    
    # 모든 조건 충족 시 통과
    assert ok is True
    assert len(reasons) == 0


def test_evaluate_filters_low_rs():
    """낮은 RS인 경우 필터 탈락 확인"""
    df = _make_df(days=252, uptrend=True)
    features = compute_features(df)
    features["rs_percentile"] = 0.50  # 50 percentile (낮음)
    features["vcp_ok"] = True
    features["dollar_vol_50"] = 3.0e9
    
    cfg = MinerviniConfig(rs_min_percentile=0.80)
    ok, reasons = evaluate_filters(features, cfg)
    
    # RS 낮아서 탈락
    assert ok is False
    assert "rs_below_min" in reasons


def test_evaluate_filters_no_vcp():
    """VCP 패턴이 없는 경우 필터 탈락 확인"""
    df = _make_df(days=252, uptrend=True)
    features = compute_features(df)
    features["rs_percentile"] = 0.90
    features["vcp_ok"] = False  # VCP 패턴 없음
    features["dollar_vol_50"] = 3.0e9
    
    cfg = MinerviniConfig(rs_min_percentile=0.80)
    ok, reasons = evaluate_filters(features, cfg)
    
    # VCP 없어서 탈락
    assert ok is False
    assert "vcp_fail" in reasons


def test_evaluate_filters_downtrend():
    """하락 추세인 경우 필터 탈락 확인"""
    df = _make_df(days=252, uptrend=False)
    features = compute_features(df)
    features["rs_percentile"] = 0.90
    features["vcp_ok"] = True
    features["dollar_vol_50"] = 3.0e9
    
    cfg = MinerviniConfig(rs_min_percentile=0.80)
    ok, reasons = evaluate_filters(features, cfg)
    
    # 하락 추세로 MA 조건 불충족
    assert ok is False
    assert any(r in reasons for r in ["trend_template_fail", "ma200_not_rising"])


def test_evaluate_filters_illiquid():
    """유동성 부족 시 필터 탈락 확인"""
    df = _make_df(days=252, uptrend=True)
    features = compute_features(df)
    features["rs_percentile"] = 0.90
    features["vcp_ok"] = True
    features["dollar_vol_50"] = 1.0e9  # 10억 (기준 20억 미만)
    
    cfg = MinerviniConfig(
        rs_min_percentile=0.80,
        min_dollar_vol_50d=2.0e9,
    )
    ok, reasons = evaluate_filters(features, cfg)
    
    # 유동성 부족으로 탈락
    assert ok is False
    assert "illiquid" in reasons


def test_minervini_filter_reduces_candidates():
    """
    Minervini 필터가 실제로 후보를 줄이는지 통합 테스트
    
    100개 종목 중:
    - 50개: 상승 추세 + 높은 RS + VCP
    - 50개: 하락 추세 또는 낮은 RS
    
    필터 적용 후 약 50개 이하로 줄어야 함
    """
    cfg = MinerviniConfig(rs_min_percentile=0.80)
    
    pass_count = 0
    fail_count = 0
    
    # 통과 예상 종목 (50개)
    for i in range(50):
        df = _make_df(days=252, uptrend=True)
        features = compute_features(df)
        features["rs_percentile"] = 0.85 + i * 0.001  # 85-90 percentile
        features["vcp_ok"] = True
        features["dollar_vol_50"] = 3.0e9
        
        ok, _ = evaluate_filters(features, cfg)
        if ok:
            pass_count += 1
        else:
            fail_count += 1
    
    # 탈락 예상 종목 (50개)
    for i in range(50):
        df = _make_df(days=252, uptrend=(i % 2 == 0))  # 절반만 상승 추세
        features = compute_features(df)
        features["rs_percentile"] = 0.40 + i * 0.005  # 40-65 percentile (낮음)
        features["vcp_ok"] = (i % 3 == 0)  # 일부만 VCP
        features["dollar_vol_50"] = 3.0e9
        
        ok, _ = evaluate_filters(features, cfg)
        if ok:
            pass_count += 1
        else:
            fail_count += 1
    
    # 필터가 동작하면 통과 종목이 전체(100개)보다 적어야 함
    assert pass_count < 100, f"필터가 후보를 줄이지 못함: pass={pass_count}, fail={fail_count}"
    
    # 통과 예상 50개 중 최소 30개는 통과해야 함 (필터 정상 작동)
    assert pass_count >= 30, f"필터가 너무 엄격함: pass={pass_count}, fail={fail_count}"
    
    print(f"[MINERVINI_TEST] before=100 pass={pass_count} fail={fail_count}")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
