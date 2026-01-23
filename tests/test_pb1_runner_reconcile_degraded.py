from types import SimpleNamespace
import os
import sys
from pathlib import Path

import sqlalchemy as sa
import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

import trader.pb1_runner as pb1_runner
from trader.db.migrate import run_migrations
from trader.kis_wrapper import KisTemporaryError
from trader.pb1_engine import RunResult
from trader.window_router import WindowDecision


def test_run_once_continues_after_reconcile_temp_error(tmp_path, monkeypatch):
    db_url = os.getenv("PBCORE_DB_URL")
    if not db_url:
        pytest.skip("PBCORE_DB_URL not set for Postgres-backed test")
    engine = sa.create_engine(db_url)
    run_migrations(engine)

    class DummyKis:
        env = "practice"
        CANO = "12345678"
        ACNT_PRDT_CD = "01"

        def __init__(self):
            self.inquire_called = False

        def inquire_daily_ccld(self, **_kwargs):
            self.inquire_called = True
            raise KisTemporaryError("HTTP 500")

        def get_balance_cached(self, **_kwargs):
            return {"output1": [], "output2": {"dnca_tot_amt": "10000000", "ord_psbl_cash": "10000000"}}

        def get_price_quote(self, *_args, **_kwargs):
            return {}

    dummy_kis = DummyKis()

    def fake_kis_factory():
        return dummy_kis

    ran = {"called": False}

    class DummyEngine:
        def __init__(self, **_kwargs):
            self.current_code = None
            self.top_candidates = []

        def run(self):
            ran["called"] = True
            return RunResult(status="OK", notes="test", balance_api_calls=0, balance_cache_hits=0, balance_tick_cache_hits=0)

        def run_close_cancel(self):
            return self.run()

    monkeypatch.setattr(pb1_runner, "KisAPI", fake_kis_factory)
    monkeypatch.setattr(pb1_runner, "PB1Engine", DummyEngine)
    monkeypatch.setattr(pb1_runner, "ensure_universe_built_once", lambda **_kwargs: None)
    monkeypatch.setattr(pb1_runner, "_load_universe_context", lambda **_kwargs: None)
    monkeypatch.setattr(pb1_runner, "try_acquire_lock", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(pb1_runner, "release_lock", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(pb1_runner, "detect_window", lambda *_args, **_kwargs: "day")
    monkeypatch.setattr(pb1_runner, "resolve_strategy_mode", lambda **_kwargs: ("LIVE", True, "day", "test"))
    monkeypatch.setattr(
        pb1_runner,
        "_resolve_market_context",
        lambda **_kwargs: (WindowDecision(name="day", phase="trade"), "day", "entry", "test", "day", []),
    )
    monkeypatch.setattr(pb1_runner, "_decide_action", lambda *_args, **_kwargs: ("run", None))
    monkeypatch.setattr(
        pb1_runner,
        "get_balance_state",
        lambda **_kwargs: (pb1_runner.BALANCE_STATE_OK, dummy_kis.get_balance_cached(), "stub"),
    )
    monkeypatch.setattr(pb1_runner, "get_botstate_root", lambda: tmp_path)

    args = SimpleNamespace(window="auto", phase="entry", target_branch="bot-state")

    touched, did_work, _metrics, _phase, status = pb1_runner.run_once(
        args=args,
        engine=engine,
        loop_mode=False,
        window=WindowDecision(name="day", phase="trade"),
        bot_state_dir=tmp_path,
    )

    assert dummy_kis.inquire_called is True
    assert ran["called"] is True
    assert did_work is True
    assert status == "OK"
    assert touched is not None
