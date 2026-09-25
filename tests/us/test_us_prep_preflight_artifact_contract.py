from pathlib import Path


def test_preflight_uses_final30_scored_not_locked_watchlist():
    script = Path('scripts/wsl/check-us-prep-before-am.sh').read_text()
    assert 'final30_scored.json' in script
    assert 'locked_watchlist.json' not in script


def test_us_force_now_passes_cli_and_exports_force_now_input():
    script = Path('scripts/wsl/run-us-prep.sh').read_text()
    assert 'export FORCE_NOW_INPUT="${US_FORCE_NOW}"' in script
    assert 'cmd+=(--force-now "${US_FORCE_NOW}")' in script


def test_preflight_db_validation_uses_project_python_and_clears_marker_on_success():
    script = Path("scripts/wsl/check-us-prep-before-am.sh").read_text(encoding="utf-8")
    assert 'source "$SCRIPT_DIR/resolve-nullim-python.sh"' in script
    assert 'PYTHON_BIN="$(nullim_resolve_python "$APP_DIR")"' in script
    assert '"$PYTHON_BIN" - "$trade_date"' in script
    assert 'rm -f "$marker"' in script


def test_runtime_guard_consumes_durable_preflight_exit_only(tmp_path, monkeypatch):
    import json
    from trader.us import prep_contract

    monkeypatch.chdir(tmp_path)
    marker_dir = tmp_path / "runtime" / "health"
    marker_dir.mkdir(parents=True)
    trade_date = "2026-09-24"
    (marker_dir / f"us-prep-missing-{trade_date}.json").write_text(
        json.dumps({
            "trade_date": trade_date,
            "status": "EXIT_ONLY",
            "reason": "prep_missing_after_preflight_recovery",
            "entry_can_proceed": 0,
            "exit_can_proceed": 1,
            "close_can_proceed": 1,
        }),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "trader.us.path_contract.load_us_prep_contract",
        lambda _td: {
            "trade_date": trade_date,
            "status": "OK_WITH_WARNINGS",
            "trade_can_proceed": 1,
            "entry_can_proceed": 1,
            "exit_can_proceed": 1,
            "close_can_proceed": 1,
            "final30_scored_count": 30,
            "score_nonzero_count": 30,
            "final30_trade_ready": True,
        },
    )

    guard = prep_contract.check_us_prep_guard(trade_date, session="am")
    assert guard["ok"] is True
    assert guard["guard_state"] == "PREFLIGHT_EXIT_ONLY"
    assert guard["entry_can_proceed"] is False
    assert guard["exit_can_proceed"] is True
    assert guard["close_can_proceed"] is True
    assert guard["source"] == "am_preflight_health"
