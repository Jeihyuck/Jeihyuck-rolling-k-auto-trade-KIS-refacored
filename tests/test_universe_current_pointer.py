from trader.db.engine import make_engine
from trader.db.migrate import run_migrations
from trader.db.repos import UniverseRepo


def test_current_universe_excludes_legacy_codes(tmp_path, monkeypatch):
    db_url = f"sqlite:///{tmp_path}/test.db"
    monkeypatch.setenv("DATABASE_URL", db_url)

    engine = make_engine()
    run_migrations(engine)
    repo = UniverseRepo(engine)

    repo.store_universe_snapshot(
        env="practice",
        strategy="best_k_meta",
        as_of_date="2025-01-01",
        provider="seed_static",
        members=[{"code": "218410", "market": "KOSDAQ"}],
        reason="legacy",
    )
    repo.store_universe_snapshot(
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
