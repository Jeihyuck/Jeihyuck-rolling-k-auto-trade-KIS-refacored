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
    # Settlement now checks durable claim eligibility before broker I/O.
    # A clean DB claim means a non-authoritative broker balance must still
    # fail closed and never settle.
    import trader.us.db.repos as repos
    class ReadOnlyConn:
        def __enter__(self): return self
        def __exit__(self, *_): pass
        def execute(self, *_args, **_kwargs):
            class Result:
                def scalar_one(self): return (1 if "exact_original_tp" in str(_args[0]) else 0)
            return Result()
    class Engine:
        def connect(self): return ReadOnlyConn()
    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: Engine())
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
                        lambda **kw: pytest.fail("without original TP proof, must skip balance"))
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


def test_settlement_rotates_bounded_candidates_and_fenced_debts_skip_kis(monkeypatch):
    """A stuck first TP cannot starve another debt or trigger KIS balance."""
    import trader.account_state as state
    import trader.us.db.repos as repo
    import trader.us.execution.reconcile as reconcile
    import trader.us.protective_stop_obligation as stop
    monkeypatch.setattr(settlement, "_DEFERRED_SETTLEMENT_CURSOR", 0)
    monkeypatch.setattr(state, "resolve_env_name", lambda: "practice")
    monkeypatch.setattr(state, "get_account_key", lambda **kw: "practice:123:01")
    monkeypatch.setattr(stop, "load_pending_stop_obligations", lambda **kw: [
        {"account_id": "practice:123:01", "trading_epoch_id": "epoch",
         "symbol": "KO", "lifecycle_id": "life-ko"},
        {"account_id": "practice:123:01", "trading_epoch_id": "epoch",
         "symbol": "PFE", "lifecycle_id": "life-pfe"},
    ])
    observed = []
    class Row:
        def scalar_one(self): return 1
    class Conn:
        def __enter__(self): return self
        def __exit__(self, *_): pass
        def execute(self, sql, params):
            assert "us_execution_claims" in str(sql)
            observed.append(params["life"])
            return Row()
    class Engine:
        def connect(self): return Conn()
    monkeypatch.setattr(repo, "_get_engine_or_none", lambda: Engine())
    monkeypatch.setattr(reconcile, "reconcile_positions",
                        lambda **kw: pytest.fail("fenced debt must skip costly KIS balance"))
    for _ in range(2):
        result = settlement.settle_verified_stop_debts_once(
            provider=object(), trade_date="2026-11-02", limit=1
        )
        assert result == {"checked":0, "settled":0, "blocked":1, "total_pending":2}
    assert observed == ["life-ko", "life-pfe"]


def test_settlement_rechecks_durable_claim_after_kis_balance_race(monkeypatch):
    """A newly acquired claim must still fence settlement after a slow GET."""
    import trader.account_state as state
    import trader.us.db.repos as repo
    import trader.us.execution.reconcile as reconcile
    import trader.us.protective_stop_obligation as stop
    monkeypatch.setattr(settlement, "_DEFERRED_SETTLEMENT_CURSOR", 0)
    monkeypatch.setattr(state, "resolve_env_name", lambda: "practice")
    monkeypatch.setattr(state, "get_account_key", lambda **kw: "practice:123:01")
    monkeypatch.setattr(stop, "load_pending_stop_obligations", lambda **kw: [{
        "account_id":"practice:123:01", "trading_epoch_id":"epoch",
        "symbol":"PFE", "lifecycle_id":"life-pfe"
    }])
    count = []
    class Row:
        def __init__(self, value): self.value = value
        def scalar_one(self): return self.value
    class Conn:
        def __enter__(self): return self
        def __exit__(self, *_): pass
        def execute(self, sql, params):
            assert "COUNT(*)" in str(sql)
            count.append(("original" if "exact_original_tp" in str(sql) else "claim",
                          params["life"]))
            if "exact_original_tp" in str(sql):
                return Row(1)  # original TP exists; balance GET necessary
            return Row(0 if len(count) == 1 else 1)
    class Engine:
        def connect(self): return Conn()
    monkeypatch.setattr(repo, "_get_engine_or_none", lambda: Engine())
    monkeypatch.setattr(reconcile, "reconcile_positions", lambda **kw: {
        "status":"OK", "balance_fetch_status":"OK",
        "authoritative_positions":True, "preserve_previous_positions":False,
        "positions":[],
    })
    monkeypatch.setattr(
        stop, "satisfy_stop_obligation_from_broker_proof",
        lambda **kw: pytest.fail("new active claim cannot settle protective debt"),
    )
    result = settlement.settle_verified_stop_debts_once(
        provider=object(), trade_date="2026-11-02", limit=1
    )
    assert count == [("claim","life-pfe"), ("original","life-pfe"),
                     ("claim","life-pfe")]
    assert result == {"checked":1, "settled":0, "blocked":1, "total_pending":1}
