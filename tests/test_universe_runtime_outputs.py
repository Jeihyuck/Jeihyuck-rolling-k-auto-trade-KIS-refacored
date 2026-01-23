from pathlib import Path

from trader.universe import build


def test_universe_build_writes_runtime_files(tmp_path, monkeypatch):
    bot_state_dir = tmp_path / "bot_state"
    bot_state_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("BOTSTATE_ROOT", str(bot_state_dir))
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{tmp_path}/test.db")
    monkeypatch.setenv("ALLOW_UNIVERSE_DB_FAIL", "1")
    monkeypatch.setenv("UNIVERSE_VALIDATE_OHLCV", "0")

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
