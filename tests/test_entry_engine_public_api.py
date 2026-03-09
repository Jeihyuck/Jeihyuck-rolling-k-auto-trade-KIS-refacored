"""Regression tests for trader.entry_engine public API and static compat path."""

from __future__ import annotations
import inspect


def test_entry_engine_public_api_import_and_callable():
    """Legacy public API names should remain importable and callable."""
    from trader.entry_engine import calculate_position_size, scan_all_strategies

    assert callable(scan_all_strategies)
    assert callable(calculate_position_size)


def test_scan_all_strategies_wrapper_uses_package_scanner():
    """Compat wrapper should route through package scanner adapter."""
    from trader.entry_engine import scan_all_strategies

    result = scan_all_strategies(
        watchlist=[{"code": "005930", "name": "Samsung"}],
        ohlcv_provider=lambda *_: None,
    )

    assert isinstance(result, dict)
    assert set(result.keys()) == {"breakout", "pullback", "momentum", "all"}


def test_pb1_runner_main_import_regression():
    """pb1_runner main import should remain stable."""
    from trader.pb1_runner import main

    assert callable(main)


def test_entry_engine_no_dynamic_legacy_loader_symbols():
    import trader.entry_engine as entry_engine

    assert not hasattr(entry_engine, "_load_legacy_entry_engine_module")
    module_source = inspect.getsource(entry_engine)
    assert "spec_from_file_location" not in module_source
    assert "exec_module" not in module_source
