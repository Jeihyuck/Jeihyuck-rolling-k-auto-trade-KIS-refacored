from pathlib import Path

from trader.universe import build


def test_universe_build_writes_runtime_files(tmp_path, monkeypatch):
    bot_state_dir = tmp_path / "bot_state"
    bot_state_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("BOTSTATE_ROOT", str(bot_state_dir))
    monkeypatch.setenv("PBCORE_DB_URL", "postgresql+psycopg://user:pass@localhost:5432/pbcore")
    monkeypatch.setenv("ALLOW_UNIVERSE_DB_FAIL", "1")
    monkeypatch.setenv("UNIVERSE_VALIDATE_OHLCV", "0")
    monkeypatch.setenv("UNIVERSE_VALIDATE_KIS", "0")

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

    seed_payload = {
        "payload": {
            "selected": ["000001"],
            "selected_by_market": {"KOSPI": [{"code": "000001", "rank": 1}]},
        },
        "members": [{"code": "000001", "market": "KOSPI"}],
        "source": "seed_static",
        "params": {"as_of": "2025-01-01"},
    }
    monkeypatch.setattr(build, "_load_static_seed", lambda: seed_payload)

    build.build_universe(as_of_date="2025-01-01", env="practice", strategy="best_k_meta", provider_override="seed_static")

    runtime_universe = bot_state_dir / "runtime" / "universe" / "2025-01-01.json"
    runtime_sanitize = bot_state_dir / "runtime" / "universe_sanitize_2025-01-01.json"
    runtime_flag = bot_state_dir / "runtime" / "universe_build_done_2025-01-01.flag"

    assert runtime_universe.exists()
    assert runtime_sanitize.exists()
    assert runtime_flag.exists()
