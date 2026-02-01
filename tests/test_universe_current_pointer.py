import os

import pytest

from trader.db.engine import make_engine
from trader.db.migrate import run_migrations
from trader.db.repos import UniverseRepo


def test_current_universe_excludes_legacy_codes(tmp_path, monkeypatch):
    db_url = os.getenv("PBCORE_DB_URL")
    if not db_url:
        pytest.skip("PBCORE_DB_URL not set for Postgres-backed test")

    engine = make_engine()
    run_migrations(engine)
    repo = UniverseRepo(engine)

    run_id1 = repo.start_universe_run(
        env="practice",
        strategy="best_k_meta",
        as_of_date="2025-01-01",
        provider="seed_static",
    )
    repo.store_universe_snapshot(
        run_id=run_id1,
        env="practice",
        strategy="best_k_meta",
        as_of_date="2025-01-01",
        provider="seed_static",
        members=[{"code": "218410", "market": "KOSDAQ"}],
        reason="legacy",
    )
    
    run_id2 = repo.start_universe_run(
        env="practice",
        strategy="best_k_meta",
        as_of_date="2025-01-02",
        provider="seed_static",
    )
    repo.store_universe_snapshot(
        run_id=run_id2,
        env="practice",
        strategy="best_k_meta",
        as_of_date="2025-01-02",
        provider="seed_static",
        members=[{"code": "005930", "market": "KOSPI"}],
        reason="current",
    )

    members = repo.get_current_universe_members("practice", "best_k_meta")
    codes = [m.get("code") for m in members]

    assert "005930" in codes
    assert "218410" not in codes
