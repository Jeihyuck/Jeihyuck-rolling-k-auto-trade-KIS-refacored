"""Account-fenced, cross-epoch stop debt and exact approval validation."""
from datetime import datetime, timezone

import pytest


def test_pending_stop_recovery_reads_other_epochs_for_same_broker_account(monkeypatch):
    import trader.us.protective_stop_obligation as m
    import trader.us.db.repos as repo
    import trader.account_state as account_state

    calls=[]
    class Result:
        def mappings(self):
            return self
        def all(self):
            return [{"trading_epoch_id":"previous-epoch","account_id":"practice:abc:01",
                     "symbol":"AMD","lifecycle_id":"same-life","severity":3,
                     "obligation_state":"PENDING"}]
    class Conn:
        def __enter__(self):return self
        def __exit__(self,*_):pass
        def execute(self, statement, params):
            calls.append((str(statement),params))
            return Result()
    class Engine:
        def connect(self):return Conn()
    monkeypatch.setattr(repo,"_get_engine_or_none",lambda:Engine())
    monkeypatch.setattr(repo,"_active_us_epoch",lambda conn:"current-epoch")
    monkeypatch.setattr(account_state,"resolve_env_name",lambda:"practice")
    monkeypatch.setattr(account_state,"get_account_key",lambda **kwargs:"practice:abc:01")
    debt=m.load_pending_stop_obligations()
    assert debt[0]["trading_epoch_id"]=="previous-epoch"
    assert calls[0][1]["account"]=="practice:abc:01"
    assert "trading_epoch_id=:epoch" not in calls[0][0]
    assert "LIMIT 501" in calls[0][0]


def test_cross_epoch_stop_debt_scan_never_truncates_silently(monkeypatch):
    import trader.us.protective_stop_obligation as m
    import trader.us.db.repos as repo
    import trader.account_state as account_state
    class Result:
        def mappings(self):return self
        def all(self):return [{"symbol":"AMD"}]*501
    class Conn:
        def __enter__(self):return self
        def __exit__(self,*_):pass
        def execute(self,*_):return Result()
    class Engine:
        def connect(self):return Conn()
    monkeypatch.setattr(repo,"_get_engine_or_none",lambda:Engine())
    monkeypatch.setattr(repo,"_active_us_epoch",lambda conn:"epoch")
    monkeypatch.setattr(account_state,"resolve_env_name",lambda:"practice")
    monkeypatch.setattr(account_state,"get_account_key",lambda **kwargs:"practice:abc:01")
    with pytest.raises(m.StopObligationUnavailable,match="INCOMPLETE_EPOCH_SCAN"):
        m.load_pending_stop_obligations()


def test_cross_epoch_terminal_proof_requires_exact_original_epoch(monkeypatch):
    import trader.us.protective_stop_obligation as m
    debt={"trading_epoch_id":"previous-epoch","env":"practice","account_id":"practice:abc:01",
          "strategy_owner":"US_STANDARD","symbol":"AMD","lifecycle_id":"same-life"}
    proof={**debt,"position_source":"KIS_AUTHORITATIVE",
           "open_orders_source":"KIS_AUTHORITATIVE",
           "fills_reconciled":True,"claims_reconciled":True,
           "broker_terminal_order_states":True,
           "broker_holding_qty":0,"broker_open_sell_qty":0,
           "broker_evidence_ref":"authoritative-original-order-proof",
           "observed_at":datetime.now(timezone.utc).isoformat()}
    assert m.validate_terminal_broker_proof(debt,proof)
    assert not m.validate_terminal_broker_proof(
        debt,{**proof,"trading_epoch_id":"current-epoch"})
