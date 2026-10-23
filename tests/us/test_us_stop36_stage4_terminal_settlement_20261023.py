"""Stage4 stop terminal proof never settles debt on broker uncertainty."""
import pytest
from trader.us.execution import protective_stop_settlement_worker as settlement


def test_settlement_rejects_non_authoritative_balance(monkeypatch):
    import trader.account_state as state
    import trader.us.execution.reconcile as reconcile
    import trader.us.protective_stop_obligation as stop
    monkeypatch.setattr(state,"resolve_env_name",lambda:"practice")
    monkeypatch.setattr(state,"get_account_key",lambda **kw:"practice:123:01")
    # The real balance fail-close is required when outstanding debt exists.
    # No-debt sessions rightly skip unrelated broker balance I/O.
    monkeypatch.setattr(stop,"load_pending_stop_obligations",lambda **kw:[{
        "account_id":"practice:123:01","trading_epoch_id":"epoch",
        "symbol":"AMD","lifecycle_id":"life-1","severity":3,
    }])
    monkeypatch.setattr(reconcile,"reconcile_positions",
                        lambda **kw:{"status":"TEMP_ERROR","authoritative_positions":False})
    with pytest.raises(RuntimeError,match="BALANCE_UNVERIFIED"):
        settlement.settle_verified_stop_debts_once(provider=object(),trade_date="2026-10-22")


def test_settlement_requires_original_date_terminal_order_for_zero_balance(monkeypatch):
    import trader.account_state as state
    import trader.us.execution.reconcile as reconcile
    import trader.us.db.repos as repo
    import trader.us.protective_stop_obligation as stop
    monkeypatch.setattr(state,"resolve_env_name",lambda:"practice")
    monkeypatch.setattr(state,"get_account_key",lambda **kw:"practice:123:01")
    monkeypatch.setattr(reconcile,"reconcile_positions",
                        lambda **kw:{"status":"OK","balance_fetch_status":"OK",
                        "authoritative_positions":True,"preserve_previous_positions":False,
                        "positions":[]})
    monkeypatch.setattr(stop,"load_pending_stop_obligations",lambda **kw:[{
        "account_id":"practice:123:01","trading_epoch_id":"epoch",
        "symbol":"AMD","lifecycle_id":"life-1","severity":3,
    }])
    class Row:
        def __init__(self,result):self.result=result
        def scalar_one(self):return self.result
        def mappings(self):return self
        def all(self):return []
    class Conn:
        def __enter__(self):return self
        def __exit__(self,*a):pass
        def execute(self,stmt,kw):return Row(0 if "COUNT(*)" in str(stmt) else None)
    class Engine:
        def connect(self):return Conn()
    monkeypatch.setattr(repo,"_get_engine_or_none",lambda:Engine())
    monkeypatch.setattr(stop,"satisfy_stop_obligation_from_broker_proof",
                        lambda **kw:(_ for _ in ()).throw(AssertionError("no proof")))
    result=settlement.settle_verified_stop_debts_once(provider=object(),trade_date="2026-10-22")
    assert result["settled"]==0
    assert result["blocked"]==1
