import json
from pathlib import Path

import pandas as pd
import pytest
import requests

from rolling_k_auto_trade_api import best_k_meta_strategy as strat


@pytest.fixture
def cache_dir(tmp_path, monkeypatch):
    path = tmp_path / "universe_cache"
    monkeypatch.setenv("UNIVERSE_CACHE_DIR", str(path))
    strat._get_listing_df_cached.cache_clear()
    return path


def test_pykrx_failure_uses_cached_universe(cache_dir, monkeypatch):
    cached_df = pd.DataFrame([{"Code": "123456", "Name": "Alpha", "Marcap": 1000}])
    strat._save_cached_universe(cached_df, "KOSPI")

    monkeypatch.setattr(strat, "_get_listing_df", lambda markets: cached_df[["Code", "Name"]])
    monkeypatch.setattr(strat, "get_nearest_business_day_in_a_week", lambda *_args, **_kwargs: "20240102")

    def raise_decode(*_args, **_kwargs):
        raise json.JSONDecodeError("bad json", doc="", pos=0)

    monkeypatch.setattr(strat, "get_market_cap_by_ticker", raise_decode)

    result = strat._get_top_n_for_market("2024-01-02", n=1, market="KOSPI")

    assert not result.empty
    assert result.iloc[0]["Code"] == "123456"


def test_pykrx_failure_without_cache_returns_empty(cache_dir, monkeypatch):
    monkeypatch.setattr(strat, "_get_listing_df", lambda markets: pd.DataFrame(columns=["Code", "Name"]))
    monkeypatch.setattr(strat, "get_nearest_business_day_in_a_week", lambda *_args, **_kwargs: "20240102")

    def raise_network(*_args, **_kwargs):
        raise requests.exceptions.JSONDecodeError("bad json", response=None)

    monkeypatch.setattr(strat, "get_market_cap_by_ticker", raise_network)

    result = strat._get_top_n_for_market("2024-01-02", n=5, market="KOSDAQ")

    assert result.empty
