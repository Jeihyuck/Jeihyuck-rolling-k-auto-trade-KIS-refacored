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
