"""Practice-only independent recovery worker never uses naked stale OPEN as submit authority."""
import pytest
from trader.us.execution.protective_stop_recovery_worker import recovery_candidates, recover_once


def test_recovery_candidates_only_confirmed_full_standard_debt():
    debts=[
        {"symbol":"AMD","account_id":"practice-a","lifecycle_id":"amd-1","severity":3},
        {"symbol":"AMD","account_id":"practice-a","lifecycle_id":"amd-1","severity":3},
        {"symbol":"MSFT","account_id":"practice-a","lifecycle_id":"msft-1","severity":1},
        {"symbol":"TQQQ","account_id":"practice-a","lifecycle_id":"tqqq-1","severity":3},
        {"symbol":"MRVL","account_id":"","lifecycle_id":"mrvl-1","severity":3},
    ]
    result=recovery_candidates(debts)
    assert len(result)==1
    assert result[0]["symbol"]=="AMD"
    assert result[0]["strategy_owner"]=="US_STANDARD"
    assert result[0]["account_id"]=="practice-a"
    assert result[0]["exit_type"]=="hard_stop_loss"


def test_worker_dry_run_never_queries_or_cancels(monkeypatch):
    monkeypatch.setenv("KIS_ENV","practice")
    class Forbidden:
        def __getattr__(self,_):
            raise AssertionError("dry run must have zero broker operations")
    report=recover_once(provider=Forbidden(),kis_client=Forbidden(),
        trade_date="2026-10-22",
        pending=[{"symbol":"AMD","account_id":"a","lifecycle_id":"l","severity":3}])
    assert report["status"]=="DRY_RUN"
    assert report["broker_cancels"]==0


def test_worker_requires_operator_execution_authorization(monkeypatch):
    monkeypatch.setenv("KIS_ENV","practice")
    monkeypatch.setenv("US_STOP_RECOVERY_AUTHORIZED","0")
    with pytest.raises(RuntimeError,match="NOT_AUTHORIZED"):
        recover_once(trade_date="2026-10-22",execute_practice_cancel=True,
            pending=[{"symbol":"AMD","account_id":"a","lifecycle_id":"l","severity":3}])


def test_worker_can_inject_recovery_but_does_not_directly_submit_sells(monkeypatch):
    monkeypatch.setenv("KIS_ENV","practice")
    monkeypatch.setenv("US_STOP_RECOVERY_AUTHORIZED","1")
    monkeypatch.setenv("US_PROTECTIVE_TP_CANCEL_RECOVERY_ENABLED","1")
    monkeypatch.setattr(
        "trader.us.execution.protective_stop_recovery_worker.request_protective_tp_cancel",
        lambda **kwargs:[{"symbol":"AMD","status":"FENCED","reason":"terminal_proof_missing"}],
    )
    report=recover_once(provider=object(),kis_client=object(),
        trade_date="2026-10-22",execute_practice_cancel=True,
        pending=[{"symbol":"AMD","account_id":"a","lifecycle_id":"l","severity":3}])
    assert report["results"]==[{"symbol":"AMD","status":"FENCED","reason":"terminal_proof_missing"}]


def test_account_mismatch_between_stop_debt_and_broker_order_claim_retains_fence(monkeypatch):
    """Even a terminal-looking broker order cannot settle a different account's stop."""
    from trader.us.protective_tp_recovery import request_protective_tp_cancel
    monkeypatch.setenv("US_PROTECTIVE_TP_CANCEL_RECOVERY_ENABLED","1")
    intents=recovery_candidates([
        {"symbol":"AMD","account_id":"practice-a","lifecycle_id":"amd-life","severity":3},
    ])
    order={
        "id":"order-id", "symbol":"AMD","trade_date":"2026-10-06",
        "side":"SELL","order_no":"0000035281","qty_requested":1,
        "qty_filled":0,"client_order_key":"old-tp",
        "claim_state":"IN_FLIGHT","claim_account_id":"wrong-account-hash",
        "meta":{"strategy_owner":"US_STANDARD",
                "position_lifecycle_id":"amd-life","profit_capture_stage":"tp1"},
    }
    class NoBroker:
        def __getattr__(self, _):
            raise AssertionError("account mismatch must block KIS operation")
    result=request_protective_tp_cancel(
        intents=intents,provider=NoBroker(),kis_client=NoBroker(),
        trade_date="2026-10-22",env="practice",
        find_open=lambda *a,**kw:[order],
    )
    assert result==[{"symbol":"AMD","status":"FENCED","reason":"account_identity_unverified"}]


def test_hashed_claim_account_matches_raw_lifecycle_identity(monkeypatch):
    import hashlib
    from trader.us.protective_tp_recovery import request_protective_tp_cancel
    monkeypatch.setenv("US_PROTECTIVE_TP_CANCEL_RECOVERY_ENABLED","1")
    intents=recovery_candidates([
        {"symbol":"AMD","account_id":"practice-a","lifecycle_id":"amd-life","severity":3}
    ])
    order={
        "id":"order-id","symbol":"AMD","trade_date":"2026-10-06",
        "side":"SELL","order_no":"0000035281","qty_requested":1,
        "qty_filled":0,"client_order_key":"old-tp",
        "claim_state":"IN_FLIGHT",
        "claim_account_id":hashlib.sha256(b"practice-a").hexdigest(),
        "meta":{"strategy_owner":"US_STANDARD",
                "position_lifecycle_id":"amd-life","profit_capture_stage":"tp1"},
    }
    class Provider:
        def get_fills_by_order_no(self,**kwargs):
            return {"order_no":"35281","status":"OPEN","requested_qty":1,
                    "filled_qty":0,"remaining_qty":1,"filled_qty_present":True,
                    "normalization_result":"normalized"}
    class NoCancel:
        def __getattr__(self,_):
            raise AssertionError("Historical OPEN is not current cancel proof")
    results=request_protective_tp_cancel(
        intents=intents,provider=Provider(),kis_client=NoCancel(),
        trade_date="2026-10-22",env="practice",
        find_open=lambda *args,**kwargs:[order],
    )
    assert len(results)==1
    assert results[0]["reason"]!="account_identity_unverified"
    assert results[0]["status"]=="FENCED"


def test_recovery_candidates_preserve_original_trading_epoch_and_do_not_collapse_generations():
    debts = [
        {"symbol": "AMD", "account_id": "practice-a", "trading_epoch_id": "old",
         "lifecycle_id": "cycle-1", "severity": 3},
        {"symbol": "AMD", "account_id": "practice-a", "trading_epoch_id": "current",
         "lifecycle_id": "cycle-1", "severity": 3},
    ]
    candidates = recovery_candidates(debts)
    assert len(candidates) == 2
    assert {c["trading_epoch_id"] for c in candidates} == {"old", "current"}
    assert all(c["meta"]["trading_epoch_id"] == c["trading_epoch_id"] for c in candidates)


def test_tp_recovery_old_epoch_is_fenced_before_any_broker_query(monkeypatch):
    from trader.us.protective_tp_recovery import request_protective_tp_cancel
    monkeypatch.setenv("US_PROTECTIVE_TP_CANCEL_RECOVERY_ENABLED", "1")
    intent = recovery_candidates([{
        "symbol": "AMD", "account_id": "practice-a",
        "trading_epoch_id": "old-epoch", "lifecycle_id": "cycle-1",
        "severity": 3,
    }])[0]
    order = {
        "id": "oid", "symbol": "AMD", "trading_epoch_id": "current-epoch",
        "trade_date": "2026-11-03", "side": "SELL", "order_no": "0000093456",
        "qty_requested": 1, "qty_filled": 0, "client_order_key": "client-1",
        "claim_state": "IN_FLIGHT",
        "meta": {"strategy_owner": "US_STANDARD", "position_lifecycle_id": "cycle-1",
                 "profit_capture_stage": "tp1"},
    }
    class NoBroker:
        def __getattr__(self, name):
            raise AssertionError("epoch mismatch must block broker I/O")
    got = request_protective_tp_cancel(
        intents=[intent], provider=NoBroker(), kis_client=NoBroker(),
        trade_date="2026-11-04", env="practice",
        find_open=lambda *args, **kwargs: [order],
    )
    assert got == [{"symbol": "AMD", "status": "FENCED",
                    "reason": "original_trading_epoch_mismatch"}]


def test_tp_recovery_ambiguous_same_symbol_cycles_never_broker_query(monkeypatch):
    from trader.us.protective_tp_recovery import request_protective_tp_cancel
    monkeypatch.setenv("US_PROTECTIVE_TP_CANCEL_RECOVERY_ENABLED", "1")
    intents = recovery_candidates([
        {"symbol": "AMD", "account_id": "practice-a", "trading_epoch_id": "epoch-one",
         "lifecycle_id": "cycle-one", "severity": 3},
        {"symbol": "AMD", "account_id": "practice-a", "trading_epoch_id": "epoch-two",
         "lifecycle_id": "cycle-two", "severity": 3},
    ])
    def no_order_query(*args, **kwargs):
        raise AssertionError("ambiguous cycle must not query for cancel")
    got = request_protective_tp_cancel(
        intents=intents, provider=object(), kis_client=object(),
        trade_date="2026-11-04", env="practice", find_open=no_order_query,
    )
    assert got == [{"symbol": "AMD", "status": "FENCED",
                    "reason": "multiple_recovery_cycles_same_symbol"}]


def test_tp_claim_lookup_binds_current_account_epoch_and_action_key(monkeypatch):
    import hashlib
    import trader.account_state as account_state
    import trader.us.db.repos as repos
    from trader.us.protective_tp_recovery import load_conflicting_open_tp_orders
    calls = []

    class Rows:
        def mappings(self):
            return self
        def __iter__(self):
            return iter(())

    class Conn:
        def __enter__(self):
            return self
        def __exit__(self, *_args):
            return None
        def execute(self, query, params):
            calls.append((str(query), params))
            return Rows()

    class Engine:
        def connect(self):
            return Conn()

    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: Engine())
    monkeypatch.setattr(repos, "_active_us_epoch", lambda conn: "current-epoch")
    monkeypatch.setattr(account_state, "resolve_env_name", lambda: "practice")
    monkeypatch.setattr(account_state, "get_account_key", lambda **kw: "practice:acct:01")

    assert load_conflicting_open_tp_orders("AMD", "2026-11-04", env="practice") == []
    sql, args = calls[0]
    assert "c.action_key=o.meta->>'execution_action_key'" in sql
    assert "c.account_id=:account_hash" in sql
    assert "o.trading_epoch_id=:epoch" in sql
    assert "c.lifecycle_id=o.meta->>'position_lifecycle_id'" in sql
    assert args["account_hash"] == hashlib.sha256(b"practice:acct:01").hexdigest()
    assert args["epoch"] == "current-epoch"


def test_tp_claim_lookup_rejects_account_environment_mismatch(monkeypatch):
    import trader.account_state as account_state
    from trader.us.protective_tp_recovery import load_conflicting_open_tp_orders
    monkeypatch.setattr(account_state, "resolve_env_name", lambda: "live")
    with pytest.raises(RuntimeError, match="environment mismatch"):
        load_conflicting_open_tp_orders("AMD", "2026-11-04", env="practice")
