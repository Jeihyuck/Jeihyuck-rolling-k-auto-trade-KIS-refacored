"""Regression tests for trader.entry_engine backward-compatible public API."""

from __future__ import annotations

from types import SimpleNamespace


def test_entry_engine_public_api_import_and_callable():
    """Legacy public API names should remain importable and callable."""
    from trader.entry_engine import calculate_position_size, scan_all_strategies

    assert callable(scan_all_strategies)
    assert callable(calculate_position_size)


def test_scan_all_strategies_wrapper_delegates_to_legacy(monkeypatch):
    """Compat wrapper should delegate calls to the legacy implementation."""
    import trader.entry_engine as entry_engine

    calls = {"count": 0, "args": None}

    def fake_scan_all_strategies(**kwargs):
        calls["count"] += 1
        calls["args"] = kwargs
        return {"all": []}

    fake_legacy_module = SimpleNamespace(
        scan_all_strategies=fake_scan_all_strategies,
        calculate_position_size=lambda **_: {"shares": 0},
    )

    monkeypatch.setattr(
        entry_engine,
        "_load_legacy_entry_engine_module",
        lambda: fake_legacy_module,
    )

    result = entry_engine.scan_all_strategies(
        watchlist=[{"code": "005930", "name": "Samsung"}],
        ohlcv_provider=lambda *_: None,
    )

    assert result == {"all": []}
    assert calls["count"] == 1
    assert "watchlist" in calls["args"]
    assert "ohlcv_provider" in calls["args"]


def test_pb1_runner_main_import_regression():
    """pb1_runner main import should remain stable."""
    from trader.pb1_runner import main

    assert callable(main)
