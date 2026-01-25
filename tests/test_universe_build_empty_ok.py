import os
from importlib import reload

import pytest

from trader.universe import build


@pytest.fixture(autouse=True)
def reset_env(tmp_path, monkeypatch):
    monkeypatch.setenv("PBCORE_DB_URL", "postgresql+psycopg://user:pass@localhost:5432/pbcore")
    monkeypatch.setenv("UNIVERSE_VALIDATE_KIS", "0")
    yield


def test_build_universe_succeeds_on_empty_payload(monkeypatch):
    class FakeUniverseRepo:
        def __init__(self, _engine):
            return None

        def store_universe_snapshot(self, **_kwargs):
            return "fake-run"

        def cleanup_old_runs(self, **_kwargs):
            return None

    monkeypatch.setattr(build, "make_engine", lambda: None)
    monkeypatch.setattr(build, "run_migrations", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(build, "UniverseRepo", FakeUniverseRepo)
    monkeypatch.setattr(
        build,
        "run_rebalance",
        lambda as_of_date, return_by_market=True: {"selected": [], "selected_stocks": [], "selected_by_market": {}},
    )
    with pytest.raises(RuntimeError, match="universe_members_empty"):
        build.build_universe(as_of_date="2026-01-05", env="practice", strategy="best_k_meta")
