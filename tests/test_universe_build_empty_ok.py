import os
from importlib import reload

import pytest

from trader.universe import build


@pytest.fixture(autouse=True)
def reset_env(tmp_path, monkeypatch):
    db_url = f"sqlite:///{tmp_path}/test.db"
    monkeypatch.setenv("DATABASE_URL", db_url)
    monkeypatch.setenv("ALLOW_UNIVERSE_DB_FAIL", "1")
    yield


def test_build_universe_succeeds_on_empty_payload(monkeypatch):
    monkeypatch.setattr(
        build,
        "run_rebalance",
        lambda as_of_date, return_by_market=True: {"selected": [], "selected_stocks": [], "selected_by_market": {}},
    )
    result = build.build_universe(as_of_date="2026-01-05", env="practice", strategy="best_k_meta")
    assert result is None or isinstance(result, str)
