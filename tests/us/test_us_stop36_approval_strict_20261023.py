"""Stop36 approval must not accept a stale conflicting row with a matching count."""
import pytest

from scripts.us import approve_pb1_stop36_existing as approval


def _eligible():
    return [{"symbol":"AMD","account":"practice:account:product",
             "lifecycle":"life-amd","source_sha":"a"*64,"qty":4}]


class _Rows:
    def __init__(self, row):
        self.row=row
    def mappings(self):
        return self
    def one(self):
        return self.row


class _Connection:
    def __init__(self, *, mismatch=False):
        self.executions=[]
        self.mismatch=mismatch
    def execute(self, sql, params):
        src=str(sql)
        self.executions.append((src,dict(params)))
        if "SELECT source_contract_sha256,soft_stop,hard_stop,active,approval_ref" in src:
            return _Rows({"source_contract_sha256":"b"*64 if self.mismatch else "a"*64,
                          "soft_stop":0.03,"hard_stop":0.06,
                          "active":True,"approval_ref":"approved-revision"})
        return _Rows({})
    def __enter__(self):
        return self
    def __exit__(self,*args):
        return False


class _Engine:
    def __init__(self, *, mismatch=False):
        self.conn=_Connection(mismatch=mismatch)
    def begin(self):
        return self.conn


def test_existing_revision_requires_exact_sha_even_if_upsert_did_nothing(monkeypatch):
    import trader.us.db.repos as repo
    engine=_Engine(mismatch=True)
    monkeypatch.setattr(approval,"_get_engine_or_none",lambda:engine)
    monkeypatch.setattr(approval,"_active_us_epoch",lambda conn:"epoch")
    monkeypatch.setattr(approval,"eligible_rows",lambda conn,epoch:_eligible())
    with pytest.raises(RuntimeError,match="STOP_REVISION_CONFLICTING_APPROVAL:AMD"):
        approval.run(apply=True,approval_ref="approved-revision",expected_count=1)


def test_approval_fails_on_position_count_change_before_any_insert(monkeypatch):
    import trader.us.db.repos as repo
    engine=_Engine()
    monkeypatch.setattr(approval,"_get_engine_or_none",lambda:engine)
    monkeypatch.setattr(approval,"_active_us_epoch",lambda conn:"epoch")
    monkeypatch.setattr(approval,"eligible_rows",lambda conn,epoch:_eligible())
    with pytest.raises(RuntimeError,match="STOP_REVISION_COUNT_CHANGED"):
        approval.run(apply=True,approval_ref="approved-revision",expected_count=2)
    assert not engine.conn.executions


def test_approved_exact_revision_has_individual_result_proof(monkeypatch):
    import trader.us.db.repos as repo
    engine=_Engine()
    monkeypatch.setattr(approval,"_get_engine_or_none",lambda:engine)
    monkeypatch.setattr(approval,"_active_us_epoch",lambda conn:"epoch")
    monkeypatch.setattr(approval,"eligible_rows",lambda conn,epoch:_eligible())
    result=approval.run(apply=True,approval_ref="approved-revision",expected_count=1)
    assert result["mode"]=="APPLIED"
    assert result["symbols"]==["AMD"]
    assert any("SELECT source_contract_sha256" in src for src,_ in engine.conn.executions)
