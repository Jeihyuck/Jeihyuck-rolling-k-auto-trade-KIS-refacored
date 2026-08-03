from datetime import date
import logging

import pytest

from trader import prep_runner


class _ReachedDerivedStage(RuntimeError):
    """Stop the test after the post-universe OHLCV path has completed."""


@pytest.fixture
def run_prep_through_ohlcv(monkeypatch):
    captured_symbols = []

    monkeypatch.setattr(prep_runner, "assert_db_ready", lambda: None)
    monkeypatch.setattr(prep_runner, "get_engine", lambda: object())
    monkeypatch.setattr(prep_runner, "run_migrations", lambda engine: None)
    monkeypatch.setattr(prep_runner, "quarantine_stale_kr_artifacts", lambda **kwargs: None)
    monkeypatch.setattr(prep_runner, "_write_prep_last_stage", lambda **kwargs: None)
    monkeypatch.setattr(
        prep_runner,
        "resolve_prep_date_context",
        lambda run_ts: {
            "kr_market": True,
            "run_date": date(2026, 8, 3),
            "as_of": date(2026, 7, 31),
        },
    )
    monkeypatch.setattr(prep_runner, "calc_market_window_kst", lambda run_ts: "pre_market")
    monkeypatch.setattr(
        prep_runner,
        "_ensure_universe",
        lambda **kwargs: [{"code": "005930", "market": "KOSPI"}],
    )

    def fake_upsert(*, symbols, as_of, days):
        captured_symbols.extend(symbols)
        return {"symbols": len(symbols), "inserted": 0, "updated": 0, "failed": 0}

    monkeypatch.setattr(prep_runner, "upsert_ohlcv_delta", fake_upsert)
    monkeypatch.setattr(prep_runner, "get_required_history_days", lambda strategy: 120)
    monkeypatch.setattr(
        prep_runner,
        "find_symbols_with_insufficient_history",
        lambda **kwargs: ([], {}),
    )
    monkeypatch.setattr(
        prep_runner,
        "compute_and_store_derived_minervini",
        lambda **kwargs: (_ for _ in ()).throw(_ReachedDerivedStage()),
    )

    def run():
        with pytest.raises(_ReachedDerivedStage):
            prep_runner.main()
        return captured_symbols

    return run


def test_kr_prep_does_not_reference_removed_single_benchmark(run_prep_through_ohlcv):
    run_prep_through_ohlcv()


def test_kr_prep_logs_kospi_and_kosdaq_benchmarks(run_prep_through_ohlcv, caplog):
    with caplog.at_level(logging.INFO, logger=prep_runner.__name__):
        run_prep_through_ohlcv()

    messages = [record.getMessage() for record in caplog.records]
    benchmark_logs = [message for message in messages if "ohlcv_prefetch_start" in message]
    assert benchmark_logs
    assert "kospi_benchmark=069500" in benchmark_logs[0]
    assert "kosdaq_benchmark=229200" in benchmark_logs[0]


def test_kr_prep_reaches_post_universe_stage_without_nameerror(run_prep_through_ohlcv):
    symbols = run_prep_through_ohlcv()

    assert prep_runner.RS_BENCHMARK_KOSPI in symbols
    assert prep_runner.RS_BENCHMARK_KOSDAQ in symbols
