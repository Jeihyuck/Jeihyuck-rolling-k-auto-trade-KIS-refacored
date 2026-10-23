"""Confirmed stop debt is created before the legacy pending-SELL fence."""
import pytest
from trader.us.protective_stop_obligation import record_confirmed_stop, StopObligationUnavailable


def test_off_mode_never_writes(monkeypatch):
    monkeypatch.setenv("US_PROTECTIVE_STOP_LEDGER_MODE","OFF")
    assert not record_confirmed_stop({"symbol":"AMD"},exit_type="hard_stop_loss",
        target_qty=4,pnl_pct=-0.1,signal_price=90,policy={"hard_stop":.06})


def test_enforce_mode_rejects_missing_lifecycle_without_broker_order(monkeypatch):
    monkeypatch.setenv("US_PROTECTIVE_STOP_LEDGER_MODE","ENFORCE")
    monkeypatch.setenv("KIS_HTTP_BLOCK","0")
    monkeypatch.setenv("OFFLINE_MODE","0")
    with pytest.raises(StopObligationUnavailable,match="SCOPE_UNVERIFIED"):
        record_confirmed_stop({"symbol":"AMD","strategy_owner":"US_STANDARD"},
            exit_type="hard_stop_loss",target_qty=4,pnl_pct=-0.10,
            signal_price=90,policy={"hard_stop":.06})


def test_shadow_mode_does_not_forge_a_missing_account(monkeypatch):
    monkeypatch.setenv("US_PROTECTIVE_STOP_LEDGER_MODE","SHADOW")
    monkeypatch.setenv("KIS_HTTP_BLOCK","0")
    monkeypatch.setenv("OFFLINE_MODE","0")
    assert not record_confirmed_stop({"symbol":"AMD","strategy_owner":"US_STANDARD"},
        exit_type="hard_stop_loss",target_qty=4,pnl_pct=-0.1,
        signal_price=90,policy={"hard_stop":.06})


def test_profit_exit_never_creates_pb1_stop_debt(monkeypatch):
    monkeypatch.setenv("US_PROTECTIVE_STOP_LEDGER_MODE","ENFORCE")
    monkeypatch.setenv("KIS_HTTP_BLOCK","0")
    assert not record_confirmed_stop({"symbol":"AMD"},exit_type="giveback",
        target_qty=4,pnl_pct=.1,signal_price=110,policy={})
