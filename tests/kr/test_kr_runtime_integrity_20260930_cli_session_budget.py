from __future__ import annotations

import os

from trader.kr import runtime_integrity_20260930 as fix


def _clear_session_aliases(monkeypatch):
    for name in ("PB1_SESSION_KIND", "PB1_SESSION", "WSL_RUN_SESSION"):
        monkeypatch.delenv(name, raising=False)


def test_direct_cli_am_refreshes_budget_after_guard_was_installed_without_session(monkeypatch):
    _clear_session_aliases(monkeypatch)
    monkeypatch.setenv("PB1_TICK_HARD_TIMEOUT_SEC", "90")
    monkeypatch.setenv("PB1_MIN_TICK_BUDGET_SEC", "105")
    monkeypatch.delenv("KR_POST_ENGINE_RESERVE_SEC", raising=False)
    monkeypatch.delenv("KR_ORDER_SUBMIT_MIN_REMAINING_SEC", raising=False)

    # Reproduce trade_session_runner import ordering: the one-shot guard is
    # installed before argparse --session has become PB1_SESSION.
    fix._apply_live_session_budget_contract()
    assert os.environ["PB1_TICK_HARD_TIMEOUT_SEC"] == "90"
    assert os.environ["PB1_MIN_TICK_BUDGET_SEC"] == "105"

    calls = []

    def original_init(self, *args, **kwargs):
        calls.append((args, kwargs))

    guarded_init = fix._build_session_aware_pb1_engine_init(original_init)

    # Direct CLI --session am is copied to PB1_SESSION by the KR runner before
    # PB1Engine is constructed. The late hook must now apply the expanded budget.
    monkeypatch.setenv("PB1_SESSION", "am")
    guarded_init(object())

    assert len(calls) == 1
    assert os.environ["PB1_TICK_HARD_TIMEOUT_SEC"] == "180"
    assert os.environ["PB1_MIN_TICK_BUDGET_SEC"] == "200"
    assert os.environ["KR_POST_ENGINE_RESERVE_SEC"] == "20"
    assert os.environ["KR_ORDER_SUBMIT_MIN_REMAINING_SEC"] == "20"


def test_direct_cli_afternoon_refreshes_budget_and_preserves_explicit_override(monkeypatch):
    _clear_session_aliases(monkeypatch)
    monkeypatch.setenv("PB1_TICK_HARD_TIMEOUT_SEC", "210")
    monkeypatch.setenv("PB1_MIN_TICK_BUDGET_SEC", "230")
    monkeypatch.setenv("KR_POST_ENGINE_RESERVE_SEC", "25")
    monkeypatch.setenv("KR_ORDER_SUBMIT_MIN_REMAINING_SEC", "22")

    guarded_init = fix._build_session_aware_pb1_engine_init(lambda self: None)
    monkeypatch.setenv("PB1_SESSION", "afternoon")
    guarded_init(object())

    assert os.environ["PB1_TICK_HARD_TIMEOUT_SEC"] == "210"
    assert os.environ["PB1_MIN_TICK_BUDGET_SEC"] == "230"
    assert os.environ["KR_POST_ENGINE_RESERVE_SEC"] == "25"
    assert os.environ["KR_ORDER_SUBMIT_MIN_REMAINING_SEC"] == "22"


def test_direct_cli_close_keeps_short_budget(monkeypatch):
    _clear_session_aliases(monkeypatch)
    monkeypatch.setenv("PB1_TICK_HARD_TIMEOUT_SEC", "45")
    monkeypatch.setenv("PB1_MIN_TICK_BUDGET_SEC", "30")

    guarded_init = fix._build_session_aware_pb1_engine_init(lambda self: None)
    monkeypatch.setenv("PB1_SESSION", "close")
    guarded_init(object())

    assert os.environ["PB1_TICK_HARD_TIMEOUT_SEC"] == "45"
    assert os.environ["PB1_MIN_TICK_BUDGET_SEC"] == "30"
