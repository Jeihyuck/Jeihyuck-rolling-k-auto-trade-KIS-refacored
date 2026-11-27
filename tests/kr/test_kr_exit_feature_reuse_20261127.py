import pandas as pd

from trader.kr.exit_feature_reuse import cached_exit_features, high_water_update_fields
from trader.strategies.pb1_minervini_v2 import compute_features


def test_exit_features_reuse_matches_original_and_invalidates(monkeypatch):
    monkeypatch.setenv("MA200_SLOPE_LOOKBACK", "20")
    dates = pd.date_range("2025-01-01", periods=280)
    df = pd.DataFrame({"date": dates, "open": range(100,380),
                       "high": range(102,382), "low": range(99,379),
                       "close": range(101,381), "volume": [1000]*280})
    calls = []
    def calc(frame):
        calls.append(1)
        return compute_features(frame)
    args = dict(code="005930", trade_date="2026-11-27", compute=calc)
    a, hit = cached_exit_features(df=df, **args)
    assert not hit
    b, hit = cached_exit_features(df=df.copy(), **args)
    assert hit and len(calls) == 1
    assert a == b == compute_features(df)
    b["close"] = -1
    assert cached_exit_features(df=df, **args)[0]["close"] > 0
    changed = df.copy()
    changed.loc[279, "volume"] = 1200
    assert not cached_exit_features(df=changed, **args)[1]
    monkeypatch.setenv("MA200_SLOPE_LOOKBACK", "15")
    assert not cached_exit_features(df=df, **args)[1]
    assert not cached_exit_features(df=df, **{**args, "trade_date":"2026-11-30"})[1]


def test_noop_highwater_only_positive_exact_equality():
    assert high_water_update_fields(persisted_max=100, computed_max=100, enabled=True) == {}
    assert high_water_update_fields(persisted_max=100, computed_max=105, enabled=True) == {"max_price":105}
    assert high_water_update_fields(persisted_max=None, computed_max=100, enabled=True) == {"max_price":100}
    assert high_water_update_fields(persisted_max=0, computed_max=0, enabled=True) == {"max_price":0}
    assert high_water_update_fields(persisted_max=100, computed_max=100, enabled=False) == {"max_price":100}
