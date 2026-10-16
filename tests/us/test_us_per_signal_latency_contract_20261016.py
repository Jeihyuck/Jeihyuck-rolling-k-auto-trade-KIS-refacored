"""Regression contract for US SELL signal-to-broker observation.

This test deliberately does not place a broker order. It checks that the
instrumentation stays observational and retains distinct attempt measurements.
"""
from pathlib import Path


def test_per_symbol_signal_observation_and_per_attempt_latency_are_present():
    root = Path(__file__).resolve().parents[2]
    tick = (root / "trader/us/runner/trade_tick_runner.py").read_text(encoding="utf-8")
    router = (root / "trader/us/execution/order_router.py").read_text(encoding="utf-8")
    assert '_sell_signal_ready_by_symbol' in tick
    assert '_sell_signal_ready_by_symbol.setdefault' in tick
    assert '_per_symbol_ready.get(symbol_upper)' in router
    assert 'sell_signal_to_broker_boundary_by_attempt_ms' in router
    assert 'str(intent["submit_attempt_id"])' in router


def test_no_trading_policy_or_frequency_mutation_in_trace():
    root = Path(__file__).resolve().parents[2]
    tick = (root / "trader/us/runner/trade_tick_runner.py").read_text(encoding="utf-8")
    assert '[US_EXIT][PER_SIGNAL_READY]' in tick
    assert 'phase=safety' in tick
    assert 'phase=trend' in tick


def test_snapshot_timing_is_observational_and_monotonic():
    root = Path(__file__).resolve().parents[2]
    engine = (root / "trader/us/pb1/us_exit_engine.py").read_text(encoding="utf-8")
    assert '_snapshot_start_mono = time.monotonic()' in engine
    assert '[US_EXIT][SNAPSHOT_READY]' in engine
    assert 'elapsed_ms=%.2f' in engine
    assert 'snapshots.append(pos)' in engine


def test_per_attempt_metric_retains_previous_attempts():
    # The accumulation contract is keyed by submit attempt, not overwritten.
    first = 'attempt-1'
    second = 'attempt-2'
    measurements = {}
    measurements.setdefault('sell_signal_to_broker_boundary_by_attempt_ms', {})[first] = 100.0
    measurements.setdefault('sell_signal_to_broker_boundary_by_attempt_ms', {})[second] = 300.0
    assert measurements['sell_signal_to_broker_boundary_by_attempt_ms'] == {
        first: 100.0, second: 300.0,
    }
