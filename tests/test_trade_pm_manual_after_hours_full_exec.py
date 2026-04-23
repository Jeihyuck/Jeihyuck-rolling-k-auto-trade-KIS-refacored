from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pandas as pd

from trader import pb1_runner
from trader.pb1_engine import RunResult, UniverseContext
from trader.run_context import RunContext


def _final30_rows() -> list[dict]:
    return [
        {
            "as_of": "2026-04-03",
            "code": f"{900000 + idx:06d}",
            "rank_final30": idx,
            "score_final": 100.0 + idx,
            "tech_score": 80.0 + idx,
            "breakout_score": 70.0 + idx,
            "pullback_score": 60.0 + idx,
            "momentum_score": 50.0 + idx,
            "rs_percentile": 95.0,
            "vcp_score": 75.0,
            "entry_style_selected": "BREAKOUT",
            "ma20": 100.0 + idx,
            "ma50": 90.0 + idx,
            "ma150": 80.0 + idx,
            "atr_pct": 0.03,
            "close": 110.0 + idx,
            "reasons": ["db_only"],
            "filters_passed": ["scored"],
            "filters_failed": [],
        }
        for idx in range(1, 31)
    ]


def test_trade_pm_manual_after_hours_full_exec(monkeypatch, tmp_path: Path, caplog) -> None:
    monkeypatch.setenv("MODE", "trade")
    monkeypatch.setenv("STRATEGY_MODE", "DIAG")
    monkeypatch.setenv("DRY_RUN", "1")
    monkeypatch.setenv("FORCE_BLOCK_LIVE", "1")
    monkeypatch.setenv("ALLOW_KIS_DATA_HTTP_IN_DIAG", "1")
    monkeypatch.setenv("FORCE_MARKET_WINDOW", "after")
    monkeypatch.setenv("FORCE_PB1_PHASE", "entry")
    monkeypatch.setenv("PB1_DIAG_FULL_EXEC", "1")
    monkeypatch.setenv("PB1_SESSION_KIND", "pm")
    monkeypatch.setenv("WATCHLIST_MODE", "1")
    monkeypatch.setenv("WATCHLIST", "005930,000660,035420")
    monkeypatch.setenv("PB1_UNIVERSE_STRATEGY", "pb1_watchlist_final_scored")
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setenv("STRATEGY_ENV", "practice")
    monkeypatch.setenv("LIVE_TRADING_ENABLED", "0")
    monkeypatch.setenv("DISABLE_LIVE_TRADING", "1")
    monkeypatch.setenv("PB1_CLOSE_MANUAL_REPLAY_ACTIVE", "0")
    monkeypatch.setenv("PB1_CLOSE_MANUAL_REPLAY_MODE", "live_close")
    monkeypatch.setenv("CLOSE_MANUAL_MODE", "live_close")

    class DummyRepo:
        def __init__(self, *_args, **_kwargs):
            pass

        def upsert_run(self, **_kwargs):
            return None

        def start_run(self, **_kwargs):
            return "test-run"

        def __getattr__(self, _name):
            return lambda *_args, **_kwargs: None

    class DummyKis:
        env = "practice"

        def __init__(self):
            self.caller_route = (pb1_runner.os.getenv("KIS_HTTP_CALLER_ROUTE") or "")

        def get_balance_cached(self, **_kwargs):
            return {"output1": [], "output2": {"ord_psbl_cash": "10000000", "dnca_tot_amt": "10000000"}}

        def get_price_quote(self, *_args, **_kwargs):
            return {"last": 100000}

    engine_calls: dict[str, object] = {"run_called": False, "order_submit_called": False}

    class DummyEngine:
        def __init__(self, **kwargs):
            engine_calls["kwargs"] = kwargs
            self._trade_date = kwargs.get("trade_date")
            self._as_of_source = "test"
            self._data_metrics = {}

        def get_as_of(self):
            return engine_calls["kwargs"].get("as_of")

        def run(self):
            engine_calls["run_called"] = True
            return RunResult(status="OK_NO_TRADE", notes="manual_test", balance_api_calls=0, balance_cache_hits=0, balance_tick_cache_hits=0)

        def run_close_cancel(self):
            raise AssertionError("close path should not run")

    monkeypatch.setattr(pb1_runner, "UniverseRepo", DummyRepo)
    monkeypatch.setattr(pb1_runner, "OrdersRepo", DummyRepo)
    monkeypatch.setattr(pb1_runner, "FillsRepo", DummyRepo)
    monkeypatch.setattr(pb1_runner, "PositionsRepo", DummyRepo)
    monkeypatch.setattr(pb1_runner, "LedgerEventsRepo", DummyRepo)
    monkeypatch.setattr(pb1_runner, "RunsRepo", DummyRepo)
    monkeypatch.setattr(pb1_runner, "ReconcileLogRepo", DummyRepo)
    monkeypatch.setattr(pb1_runner, "PB1Engine", DummyEngine)
    monkeypatch.setattr(pb1_runner, "KisAPI", DummyKis)
    monkeypatch.setattr(pb1_runner, "get_balance_state", lambda **_kwargs: (pb1_runner.BALANCE_STATE_OK, {"cash": 10000000}, "stub"))
    monkeypatch.setattr(pb1_runner, "scan_all_strategies", lambda **_kwargs: {"summary": {}, "evaluations": [], "all": [], "breakout": [], "pullback": [], "momentum": []})
    monkeypatch.setattr(pb1_runner, "resolve_trade_context", lambda **_kwargs: {"trade_date": "2026-04-04", "as_of": "2026-04-03", "reason": "test"})
    monkeypatch.setattr(pb1_runner, "resolve_strategy_mode", lambda **_kwargs: ("DIAG", False, "after", "test"))
    monkeypatch.setattr(pb1_runner, "_decide_action", lambda *_args, **_kwargs: ("smoke", None))
    monkeypatch.setattr(pb1_runner, "_resolve_market_context", lambda **_kwargs: (None, "after", "entry", "test", "after", []))
    monkeypatch.setattr(pb1_runner, "ensure_universe_built_once", lambda **_kwargs: [{"code": "005930"}])
    monkeypatch.setattr(pb1_runner, "run_nontrading_smoke_once", lambda **_kwargs: None)
    monkeypatch.setattr(pb1_runner, "write_nontrading_smoke_flag", lambda *_args, **_kwargs: None)

    class DummyDerivedRepo:
        def __init__(self, *_args, **_kwargs):
            pass

        def load_for_as_of_with_fallback(self, **_kwargs):
            return [], None

    class DummyWatchlistRepo:
        def __init__(self, *_args, **_kwargs):
            pass

        def load_watchlist_scored(self, **_kwargs):
            return [], None

    monkeypatch.setattr(pb1_runner, "DerivedMinerviniRepo", DummyDerivedRepo)
    monkeypatch.setattr(pb1_runner, "WatchlistRepo", DummyWatchlistRepo)
    monkeypatch.setattr(
        pb1_runner,
        "_load_universe_context",
        lambda **_kwargs: UniverseContext(
            as_of_date="2026-04-03",
            members=_final30_rows(),
            selected_path=None,
            meta={
                "source": "db_pb1_watchlist_final_scored",
                "is_scored": True,
                "locked_final30_rows": _final30_rows(),
            },
            is_empty=False,
        ),
    )

    caplog.set_level(logging.INFO)
    args = SimpleNamespace(window="auto", phase="entry", target_branch="bot-state")
    ctx = RunContext.new(account_env="practice", exec_mode="DIAG", strategy="pb1", dry_run=True)

    touched, did_work, _metrics, _phase, status = pb1_runner.run_once(
        args=args,
        engine=SimpleNamespace(url="postgresql+psycopg://localhost/postgres"),
        ctx=ctx,
        loop_mode=False,
        window=None,
        runtime_dir=tmp_path,
    )

    assert engine_calls["run_called"] is True
    assert did_work is True
    assert status.startswith("OK")
    assert touched is not None
    assert engine_calls["kwargs"]["dry_run"] is True
    assert engine_calls["kwargs"]["intended_live"] is False
    assert engine_calls["kwargs"]["final30_source"] == "db_pb1_watchlist_final_scored"
    assert engine_calls["kwargs"]["diag_full_exec"] is True
    assert caplog.text.count("[PB1][MANUAL_TEST_ROUTE] reason=diag_full_exec -> force action=run") == 1
    assert "[TRADE][ENGINE_BOOT][OK] engine=PB1Engine" in caplog.text
    assert "[SMOKE][FAIL]" not in caplog.text


def test_trade_pm_uses_locked_final30_rows_not_members(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("MODE", "trade")
    monkeypatch.setenv("STRATEGY_MODE", "DIAG")
    monkeypatch.setenv("DRY_RUN", "1")
    monkeypatch.setenv("FORCE_BLOCK_LIVE", "1")
    monkeypatch.setenv("ALLOW_KIS_DATA_HTTP_IN_DIAG", "1")
    monkeypatch.setenv("FORCE_MARKET_WINDOW", "after")
    monkeypatch.setenv("FORCE_PB1_PHASE", "entry")
    monkeypatch.setenv("PB1_DIAG_FULL_EXEC", "1")
    monkeypatch.setenv("PB1_SESSION_KIND", "pm")
    monkeypatch.setenv("WATCHLIST_MODE", "1")
    monkeypatch.setenv("WATCHLIST", "005930,000660,035420")
    monkeypatch.setenv("PB1_UNIVERSE_STRATEGY", "pb1_watchlist_final_scored")
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setenv("STRATEGY_ENV", "practice")
    monkeypatch.setenv("LIVE_TRADING_ENABLED", "0")
    monkeypatch.setenv("DISABLE_LIVE_TRADING", "1")
    monkeypatch.setenv("PB1_CLOSE_MANUAL_REPLAY_ACTIVE", "0")
    monkeypatch.setenv("PB1_CLOSE_MANUAL_REPLAY_MODE", "live_close")
    monkeypatch.setenv("CLOSE_MANUAL_MODE", "live_close")

    class DummyRepo:
        def __init__(self, *_args, **_kwargs):
            pass

        def upsert_run(self, **_kwargs):
            return None

        def start_run(self, **_kwargs):
            return "test-run"

        def __getattr__(self, _name):
            return lambda *_args, **_kwargs: None

    class DummyKis:
        env = "practice"

        def __init__(self):
            self.caller_route = (pb1_runner.os.getenv("KIS_HTTP_CALLER_ROUTE") or "")

        def get_balance_cached(self, **_kwargs):
            return {"output1": [], "output2": {"ord_psbl_cash": "10000000", "dnca_tot_amt": "10000000"}}

        def get_price_quote(self, *_args, **_kwargs):
            return {"last": 100000}

    engine_calls: dict[str, object] = {"run_called": False}

    class DummyEngine:
        def __init__(self, **kwargs):
            engine_calls["kwargs"] = kwargs

        def get_as_of(self):
            return engine_calls["kwargs"].get("as_of")

        def run(self):
            engine_calls["run_called"] = True
            return RunResult(status="OK_NO_TRADE", notes="locked_rows", balance_api_calls=0, balance_cache_hits=0, balance_tick_cache_hits=0)

        def run_close_cancel(self):
            raise AssertionError("close path should not run")

    monkeypatch.setattr(pb1_runner, "UniverseRepo", DummyRepo)
    monkeypatch.setattr(pb1_runner, "OrdersRepo", DummyRepo)
    monkeypatch.setattr(pb1_runner, "FillsRepo", DummyRepo)
    monkeypatch.setattr(pb1_runner, "PositionsRepo", DummyRepo)
    monkeypatch.setattr(pb1_runner, "LedgerEventsRepo", DummyRepo)
    monkeypatch.setattr(pb1_runner, "RunsRepo", DummyRepo)
    monkeypatch.setattr(pb1_runner, "ReconcileLogRepo", DummyRepo)
    monkeypatch.setattr(pb1_runner, "PB1Engine", DummyEngine)
    monkeypatch.setattr(pb1_runner, "KisAPI", DummyKis)
    monkeypatch.setattr(pb1_runner, "get_balance_state", lambda **_kwargs: (pb1_runner.BALANCE_STATE_OK, {"cash": 10000000}, "stub"))
    monkeypatch.setattr(pb1_runner, "scan_all_strategies", lambda **_kwargs: {"summary": {}, "evaluations": [], "all": [], "breakout": [], "pullback": [], "momentum": []})
    monkeypatch.setattr(pb1_runner, "resolve_trade_context", lambda **_kwargs: {"trade_date": "2026-04-04", "as_of": "2026-04-03", "reason": "test"})
    monkeypatch.setattr(pb1_runner, "resolve_strategy_mode", lambda **_kwargs: ("DIAG", False, "after", "test"))
    monkeypatch.setattr(pb1_runner, "_decide_action", lambda *_args, **_kwargs: ("smoke", None))
    monkeypatch.setattr(pb1_runner, "_resolve_market_context", lambda **_kwargs: (None, "after", "entry", "test", "after", []))
    monkeypatch.setattr(pb1_runner, "ensure_universe_built_once", lambda **_kwargs: [{"code": "005930"}])
    monkeypatch.setattr(pb1_runner, "run_nontrading_smoke_once", lambda **_kwargs: None)
    monkeypatch.setattr(pb1_runner, "write_nontrading_smoke_flag", lambda *_args, **_kwargs: None)

    class DummyDerivedRepo:
        def __init__(self, *_args, **_kwargs):
            pass

        def load_for_as_of_with_fallback(self, **_kwargs):
            return [], None

    class DummyWatchlistRepo:
        def __init__(self, *_args, **_kwargs):
            pass

        def load_watchlist_scored(self, **_kwargs):
            return [], None

    monkeypatch.setattr(pb1_runner, "DerivedMinerviniRepo", DummyDerivedRepo)
    monkeypatch.setattr(pb1_runner, "WatchlistRepo", DummyWatchlistRepo)

    locked_rows = _final30_rows()
    monkeypatch.setattr(
        pb1_runner,
        "_load_universe_context",
        lambda **_kwargs: UniverseContext(
            as_of_date="2026-04-03",
            members=[{"code": row["code"]} for row in locked_rows],
            selected_path=None,
            meta={
                "source": "db_pb1_watchlist_final_scored",
                "is_scored": True,
                "locked_final30_rows": locked_rows,
            },
            is_empty=False,
        ),
    )

    args = SimpleNamespace(window="auto", phase="entry", target_branch="bot-state")
    ctx = RunContext.new(account_env="practice", exec_mode="DIAG", strategy="pb1", dry_run=True)

    _touched, did_work, _metrics, _phase, status = pb1_runner.run_once(
        args=args,
        engine=SimpleNamespace(url="postgresql+psycopg://localhost/postgres"),
        ctx=ctx,
        loop_mode=False,
        window=None,
        runtime_dir=tmp_path,
    )

    precomputed = engine_calls["kwargs"]["precomputed_final30_df"]

    assert did_work is True
    assert status.startswith("OK")
    assert engine_calls["run_called"] is True
    assert len(precomputed) == 30
    assert "rank_final30" in precomputed.columns


def test_run_once_logs_fail_open_flag_and_uses_warn_status(monkeypatch, tmp_path: Path, caplog) -> None:
    monkeypatch.setenv("MODE", "trade")
    monkeypatch.setenv("STRATEGY_MODE", "DIAG")
    monkeypatch.setenv("DRY_RUN", "1")
    monkeypatch.setenv("FORCE_BLOCK_LIVE", "1")
    monkeypatch.setenv("ALLOW_KIS_DATA_HTTP_IN_DIAG", "1")
    monkeypatch.setenv("FORCE_MARKET_WINDOW", "after")
    monkeypatch.setenv("FORCE_PB1_PHASE", "entry")
    monkeypatch.setenv("PB1_DIAG_FULL_EXEC", "1")
    monkeypatch.setenv("PB1_SESSION_KIND", "pm")
    monkeypatch.setenv("WATCHLIST_MODE", "1")
    monkeypatch.setenv("WATCHLIST", "005930,000660,035420")
    monkeypatch.setenv("PB1_UNIVERSE_STRATEGY", "pb1_watchlist_final_scored")
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setenv("STRATEGY_ENV", "practice")
    monkeypatch.setenv("LIVE_TRADING_ENABLED", "0")
    monkeypatch.setenv("DISABLE_LIVE_TRADING", "1")
    monkeypatch.setenv("PB1_CLOSE_MANUAL_REPLAY_ACTIVE", "0")
    monkeypatch.setenv("PB1_CLOSE_MANUAL_REPLAY_MODE", "live_close")
    monkeypatch.setenv("CLOSE_MANUAL_MODE", "live_close")

    class DummyRepo:
        def __init__(self, *_args, **_kwargs):
            pass

        def upsert_run(self, **_kwargs):
            return None

        def start_run(self, **_kwargs):
            return "test-run"

        def __getattr__(self, _name):
            return lambda *_args, **_kwargs: None

    class DummyKis:
        env = "practice"

        def get_balance_cached(self, **_kwargs):
            return {"output1": [], "output2": {"ord_psbl_cash": "10000000", "dnca_tot_amt": "10000000"}}

        def get_price_quote(self, *_args, **_kwargs):
            return {"last": 100000}

    class DummyEngine:
        def __init__(self, **kwargs):
            self._trade_date = kwargs.get("trade_date")
            self._as_of_source = "test"
            self._data_metrics = {}

        def get_as_of(self):
            return "2026-04-03"

        def run(self):
            return RunResult(status="OK_NO_TRADE", notes="manual_test", balance_api_calls=0, balance_cache_hits=0, balance_tick_cache_hits=0)

        def run_close_cancel(self):
            raise AssertionError("close path should not run")

    monkeypatch.setattr(pb1_runner, "UniverseRepo", DummyRepo)
    monkeypatch.setattr(pb1_runner, "OrdersRepo", DummyRepo)
    monkeypatch.setattr(pb1_runner, "FillsRepo", DummyRepo)
    monkeypatch.setattr(pb1_runner, "PositionsRepo", DummyRepo)
    monkeypatch.setattr(pb1_runner, "LedgerEventsRepo", DummyRepo)
    monkeypatch.setattr(pb1_runner, "RunsRepo", DummyRepo)
    monkeypatch.setattr(pb1_runner, "ReconcileLogRepo", DummyRepo)
    monkeypatch.setattr(pb1_runner, "PB1Engine", DummyEngine)
    monkeypatch.setattr(pb1_runner, "KisAPI", lambda *_, **__: DummyKis())
    monkeypatch.setattr(pb1_runner, "get_balance_state", lambda **_kwargs: (pb1_runner.BALANCE_STATE_OK, {"cash": 10000000}, "stub"))
    monkeypatch.setattr(pb1_runner, "scan_all_strategies", lambda **_kwargs: {"summary": {}, "evaluations": [], "all": [], "breakout": [], "pullback": [], "momentum": []})
    monkeypatch.setattr(pb1_runner, "resolve_trade_context", lambda **_kwargs: {"trade_date": "2026-04-04", "as_of": "2026-04-03", "reason": "test"})
    monkeypatch.setattr(pb1_runner, "resolve_strategy_mode", lambda **_kwargs: ("DIAG", False, "after", "test"))
    monkeypatch.setattr(pb1_runner, "_decide_action", lambda *_args, **_kwargs: ("run", None))
    monkeypatch.setattr(pb1_runner, "_resolve_market_context", lambda **_kwargs: (None, "after", "entry", "test", "after", []))
    monkeypatch.setattr(pb1_runner, "ensure_universe_built_once", lambda **_kwargs: [{"code": "005930"}])
    monkeypatch.setattr(pb1_runner, "run_nontrading_smoke_once", lambda **_kwargs: None)
    monkeypatch.setattr(pb1_runner, "write_nontrading_smoke_flag", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        pb1_runner,
        "_load_universe_context",
        lambda **_kwargs: UniverseContext(
            as_of_date="2026-04-03",
            members=_final30_rows(),
            selected_path=None,
            meta={
                "source": "db_pb1_watchlist_final_scored",
                "is_scored": True,
                "locked_final30_rows": _final30_rows(),
            },
            is_empty=False,
        ),
    )

    caplog.set_level(logging.INFO)
    args = SimpleNamespace(window="auto", phase="entry", target_branch="bot-state")
    ctx = RunContext.new(account_env="practice", exec_mode="DIAG", strategy="pb1", dry_run=True)

    _touched, did_work, _metrics, _phase, status = pb1_runner.run_once(
        args=args,
        engine=SimpleNamespace(url="postgresql+psycopg://localhost/postgres"),
        ctx=ctx,
        loop_mode=False,
        window=None,
        runtime_dir=tmp_path,
        runs_ledger_fail_open=True,
    )

    assert did_work is True
    assert status == "OK_WITH_WARNINGS"
    assert "[PB1][RUN_ONCE][FAIL_OPEN_FLAG] runs_ledger_fail_open=1 loop_mode=0" in caplog.text