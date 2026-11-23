"""Benchmark final gate proof: post-sync dates, stale vs healed context.

Offline unit checks only. No PREP artifact overwrite or live BUY.
"""
import inspect

from trader.us.runner.prep_runner import (
    _evaluate_benchmark_daily_gate,
    run_prep,
)


def _quality(spy_latest="2026-11-19", quality="OK"):
    return {
        symbol: {
            "symbol": symbol, "quality": quality,
            "db_latest": spy_latest if symbol == "SPY" else "2026-11-19",
            "expected_latest": "2026-11-19",
        }
        for symbol in ("SPY", "QQQ", "SMH")
    }


def _gate(quality, context):
    return _evaluate_benchmark_daily_gate(
        daily_sync_summary={"daily_sync_quality_by_symbol": quality},
        rotation_context=context,
        market_state_overlay={"allow_new_buy": True},
    )


def test_stale_spy_day_blocks_entry_without_fabricating_freshness():
    result = _gate(_quality("2026-11-18"),
                   {"benchmark_data_quality": "stale", "missing_symbols": ["SPY"]})
    assert result["benchmark_daily_failed"] is True
    assert result["symbol_status"]["SPY"]["final_ok"] is False
    assert "SPY" in result["missing_symbols"]


def test_verified_newly_completed_day_can_heal_specific_stale_sync_failure():
    result = _gate(_quality("2026-11-19"),
                   {"benchmark_data_quality": "ok", "missing_symbols": []})
    assert result["benchmark_daily_failed"] is False
    assert result["symbol_status"]["SPY"]["latest_date"] == "2026-11-19"
    assert result["symbol_status"]["SPY"]["final_ok"] is True


def test_context_still_stale_with_symbol_ok_does_not_authorize_buy():
    result = _gate(_quality("2026-11-19"),
                   {"benchmark_data_quality": "stale", "missing_symbols": []})
    assert result["benchmark_daily_failed"] is True
    assert result["benchmark_data_quality"] == "stale"


def test_missing_context_quality_is_fail_closed():
    result = _gate(_quality(), {"missing_symbols": []})
    assert result["benchmark_daily_failed"] is True


def test_prep_successful_sync_preserves_actual_provider_asof_dates():
    # Source-level invariant for the full PREP function: the successful
    # post-sync result must be stored, not overwritten with bare quality=OK.
    source = inspect.getsource(run_prep)
    assert 'sync_ok_evidence[sym]' in source
    assert '"db_latest": result.get("db_latest")' in source
    assert '"expected_latest": result.get("expected_latest")' in source
    assert '{x["symbol"]: x for x in sync_failed} | sync_ok_evidence' in source
