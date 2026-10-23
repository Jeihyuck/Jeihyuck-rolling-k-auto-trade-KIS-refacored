"""Authoritative broker terminality is mandatory for closing durable stop debt."""
from datetime import datetime, timezone
import pytest
from trader.us.protective_stop_obligation import (
    validate_terminal_broker_proof, satisfy_stop_obligation_from_broker_proof,
    StopObligationUnavailable,
)


def _debt():
    return {"trading_epoch_id":"epoch-1","env":"practice",
            "account_id":"acct-1","strategy_owner":"US_STANDARD",
            "symbol":"AMD","lifecycle_id":"life-1"}


def _proof():
    return {**_debt(),"position_source":"KIS_AUTHORITATIVE",
            "open_orders_source":"KIS_AUTHORITATIVE",
            "fills_reconciled":True,"claims_reconciled":True,
            "broker_terminal_order_states":True,
            "broker_holding_qty":0,"broker_open_sell_qty":0,
            "broker_evidence_ref":"KIS-verified-evidence-id",
            "observed_at":datetime(2026,10,23,tzinfo=timezone.utc).isoformat()}


def test_only_full_broker_terminal_proof_can_satisfy_stop():
    assert validate_terminal_broker_proof(_debt(),_proof())
    for patch in (
        {"account_id":"other"},{"lifecycle_id":"other"},
        {"trading_epoch_id":"other"},{"symbol":"MRVL"},
        {"strategy_owner":"TQQQ_INFINITE"},{"position_source":"LOCAL_DB"},
        {"open_orders_source":"LOCAL_DB"},{"fills_reconciled":False},
        {"claims_reconciled":False},{"broker_terminal_order_states":False},
        {"broker_holding_qty":1},{"broker_open_sell_qty":2},
        {"broker_evidence_ref":""},{"observed_at":"2026-10-23"},
    ):
        assert not validate_terminal_broker_proof(_debt(),{**_proof(),**patch}), patch


def test_cancel_ack_or_old_open_order_is_not_terminal():
    assert not validate_terminal_broker_proof(_debt(),
        {**_proof(), "broker_terminal_order_states":False, "broker_evidence_ref":"CANCEL_ACK"})


def test_unknown_proof_rejected_before_any_db_mutation(monkeypatch):
    monkeypatch.setattr("trader.us.db.repos._get_engine_or_none",
                        lambda: (_ for _ in ()).throw(AssertionError("no DB writes")))
    with pytest.raises(StopObligationUnavailable,match="TERMINAL_PROOF_UNVERIFIED"):
        satisfy_stop_obligation_from_broker_proof(
            debt=_debt(),proof={**_proof(),"broker_holding_qty":1})
