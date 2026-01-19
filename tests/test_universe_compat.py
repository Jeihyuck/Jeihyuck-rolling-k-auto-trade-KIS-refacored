def test_ensure_universe_built_once_with_runtime_store(monkeypatch, tmp_path):
    bot_state_dir = tmp_path / "bot_state"
    bot_state_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("BOT_STATE_DIR", str(bot_state_dir))

    from trader.pb1_runner import RuntimeStore, ensure_universe_built_once

    rs = RuntimeStore(base_dir=bot_state_dir)
    ensure_universe_built_once(rs)
