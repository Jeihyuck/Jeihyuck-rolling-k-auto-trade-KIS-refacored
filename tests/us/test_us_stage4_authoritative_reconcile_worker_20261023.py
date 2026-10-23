"""Stage4 writer: account-wide singleton lock and strict readiness fencing."""
from datetime import datetime,timedelta,timezone
import pytest
from trader.us.execution import authoritative_reconcile_worker as worker


def test_worker_advisory_lock_key_is_stable_account_scoped():
    first={"env":"practice","account_id":"same-account"}
    second={"env":"practice","account_id":"other-account"}
    assert worker.advisory_key(first)==worker.advisory_key(first)
    assert worker.advisory_key(first)!=worker.advisory_key(second)


def test_readiness_requires_actual_writer_and_fresh_timestamp(monkeypatch):
    import trader.us.db.repos as repos
    row={"evidence_at":datetime.now(timezone.utc),"worker_state":"READY",
         "reconcile_status":"OK","replay_status":"OK",
         "pending_count":0,"unresolved_count":0,"filled_rows":5,
         "writer_generation":"gen"}
    class Rows:
        def __init__(self,row):self.row=row
        def mappings(self):return self
        def first(self):return self.row
    class Conn:
        def __init__(self,row):self.row=row
        def __enter__(self):return self
        def __exit__(self,*a):pass
        def execute(self,stmt,params):
            assert params["account"]=="hash-account"
            return Rows(self.row)
    class Engine:
        def __init__(self,row):self.row=row
        def connect(self):return Conn(self.row)
    monkeypatch.setattr(repos,"_us_execution_claim_scope",
                        lambda:{"env":"practice","account_id":"hash-account",
                                "trading_epoch_id":"epoch"})
    monkeypatch.setattr(repos,"_get_engine_or_none",lambda:Engine(row))
    assert worker.read_worker_health()["reconcile_status"]=="OK"
    monkeypatch.setattr(repos,"_get_engine_or_none",
                        lambda:Engine({**row,"worker_state":"FAILED"}))
    with pytest.raises(RuntimeError,match="NOT_READY"):
        worker.read_worker_health()
    monkeypatch.setattr(repos,"_get_engine_or_none",
                        lambda:Engine({**row,"evidence_at":datetime.now(timezone.utc)-timedelta(seconds=99)}))
    with pytest.raises(RuntimeError,match="STALE"):
        worker.read_worker_health()


def test_failed_broker_fills_never_publish_authoritative_ready(monkeypatch):
    from trader.us.execution import fills
    monkeypatch.setattr(fills,"get_fills_today",
                        lambda **kwargs:{"status":"TEMP_ERROR","fills":[]})
    with pytest.raises(RuntimeError,match="KIS_FILLS_UNVERIFIED"):
        worker.run_reconcile_once(provider=object(),trade_date="2026-10-23",
            session_run_id="test",writer_generation="g",conn=None,scope={})
