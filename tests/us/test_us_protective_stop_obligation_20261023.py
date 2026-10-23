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


def test_confirmed_stop_is_recorded_before_pending_tp_suppression(monkeypatch):
    """An existing TP fence must not erase a confirmed hard-stop debt."""
    from datetime import datetime, timezone
    from trader.us.pb1 import us_exit_engine
    records=[]
    monkeypatch.setattr(
        "trader.us.protective_stop_obligation.record_confirmed_stop",
        lambda position, **kwargs: records.append((position, kwargs)) or True,
    )
    monkeypatch.setattr(
        us_exit_engine, "should_skip_exit_due_to_pending_sell",
        lambda symbol,trade_date: (True,"stale_tp_fence",{"order_no":"old-tp"}),
    )
    position={
        "symbol":"AMD", "exchange":"NASDAQ", "qty":4,"holding_qty":4,
        "orderable_qty":4, "entry_price":100.0, "max_price":100.0,
        "strategy_owner":"US_STANDARD", "account_id":"practice-account",
        "position_lifecycle_id":"amd-life",
    }
    intent=us_exit_engine.evaluate_exit(
        position, 93.0, now=datetime(2026,10,22,17,0,tzinfo=timezone.utc),
        include_trend_time=False,
    )
    assert intent is None  # no duplicate broker SELL while old order unresolved
    assert len(records)==1
    assert records[0][1]["exit_type"]=="hard_stop_loss"
    assert records[0][1]["target_qty"]==4
