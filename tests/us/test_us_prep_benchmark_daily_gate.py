from trader.us.runner.prep_runner import _evaluate_benchmark_daily_gate


def _sync_quality_ok():
    return {
        "SPY": {"symbol": "SPY", "quality": "OK", "db_latest": "2026-08-04", "expected_latest": "2026-08-04"},
        "QQQ": {"symbol": "QQQ", "quality": "OK", "db_latest": "2026-08-04", "expected_latest": "2026-08-04"},
        "SMH": {"symbol": "SMH", "quality": "OK", "db_latest": "2026-08-04", "expected_latest": "2026-08-04"},
    }


def test_stale_sync_failed_symbols_are_ignored_after_backfill_ok():
    gate = _evaluate_benchmark_daily_gate(
        daily_sync_summary={
            "benchmark_sync_failed_symbols": ["SPY"],
            "daily_sync_quality_by_symbol": _sync_quality_ok(),
        },
        rotation_context={"benchmark_data_quality": "ok", "missing_symbols": []},
        market_state_overlay={"allow_new_buy": True},
    )

    assert gate["benchmark_daily_failed"] is False
    assert gate["missing_symbols"] == []
    assert gate["symbol_status"]["SPY"]["final_ok"] is True


def test_ok_quality_plus_allow_new_buy_must_not_emit_unavailable_state():
    gate = _evaluate_benchmark_daily_gate(
        daily_sync_summary={"daily_sync_quality_by_symbol": _sync_quality_ok()},
        rotation_context={"benchmark_data_quality": "ok", "missing_symbols": []},
        market_state_overlay={"allow_new_buy": True},
    )

    assert gate["benchmark_data_quality"] == "ok"
    assert gate["benchmark_daily_failed"] is False
    assert gate["missing_symbols"] == []


def test_bad_or_missing_quality_keeps_entry_block_precondition():
    broken = _sync_quality_ok()
    broken["SPY"] = {
        "symbol": "SPY",
        "quality": "OK",
        "db_latest": "2026-08-01",
        "expected_latest": "2026-08-04",
    }
    gate = _evaluate_benchmark_daily_gate(
        daily_sync_summary={"daily_sync_quality_by_symbol": broken},
        rotation_context={"benchmark_data_quality": "degraded", "missing_symbols": ["SPY"]},
        market_state_overlay={"allow_new_buy": True},
    )

    assert gate["benchmark_daily_failed"] is True
    assert "SPY" in gate["missing_symbols"]
    assert gate["symbol_status"]["SPY"]["latest_date"] == "2026-08-01"
    assert gate["symbol_status"]["SPY"]["expected_date"] == "2026-08-04"
