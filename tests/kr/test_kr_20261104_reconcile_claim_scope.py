"""KR practice account/epoch claim health must be scoped at reconciliation."""
import hashlib
from datetime import datetime

import sqlalchemy as sa

from trader import reconcile_kis
from trader.run_context import RunContext


def test_health_wrapper_passes_account_and_epoch_filters():
    recorded = {}

    class Orders:
        def execution_claim_health(self, **filters):
            recorded.update(filters)
            return {"unresolved_execution_actions": 0, "execution_claim_conflicts": 0}

    result = reconcile_kis._execution_claim_health(
        Orders(), market="KR", env="practice",
        account_id="hashed-account", trading_epoch_id="current-epoch",
    )
    assert result["available"] is True
    assert recorded == {
        "market": "KR", "env": "practice",
        "account_id": "hashed-account", "trading_epoch_id": "current-epoch",
    }


def _mock_empty_reconcile(monkeypatch, *, epoch="active-epoch", fail=False):
    seen = []
    account = "practice:12345678:01"

    class Orders:
        def execution_claim_health(self, **filters):
            seen.append(filters)
            return {"unresolved_execution_actions": 0, "execution_claim_conflicts": 0}

    class NoopRepo:
        def __init__(self, *_args, **_kwargs):
            pass

        def __getattr__(self, _name):
            return lambda **_kwargs: None

    def active_epoch(_engine, *, env, account_id, required):
        assert env == "practice"
        assert account_id == account
        assert required is True
        if fail:
            raise RuntimeError("multiple active epochs")
        return epoch

    class FakeKis:
        CANO = "12345678"
        ACNT_PRDT_CD = "01"

        def inquire_daily_ccld(self, **_kwargs):
            return {"output1": [], "output2": []}

    monkeypatch.setattr(reconcile_kis, "_resolve_reconcile_run_id", lambda *_args: "run")
    monkeypatch.setattr(reconcile_kis, "OrdersRepo", lambda _engine: Orders())
    monkeypatch.setattr(reconcile_kis, "FillsRepo", NoopRepo)
    monkeypatch.setattr(reconcile_kis, "PositionsRepo", NoopRepo)
    monkeypatch.setattr(reconcile_kis, "LedgerEventsRepo", NoopRepo)
    monkeypatch.setattr(reconcile_kis, "ReconcileLogRepo", NoopRepo)
    monkeypatch.setattr(reconcile_kis, "active_trading_epoch_id", active_epoch)
    monkeypatch.setattr(reconcile_kis, "trading_epoch_enforced", lambda: True)
    monkeypatch.setattr(reconcile_kis, "get_account_key", lambda **_kwargs: account)

    result = reconcile_kis.reconcile_today(
        engine=sa.create_engine("sqlite:///:memory:"),
        kis=FakeKis(),
        ctx=RunContext(
            run_id="run", env="practice", strategy="pb1_pullback_close",
            started_at=datetime(2026, 11, 4, 10), dry_run=True,
        ),
    )
    return result, seen, account


def test_current_account_does_not_count_prior_account_claims(monkeypatch):
    result, calls, account = _mock_empty_reconcile(monkeypatch)
    assert result["ok"] is True
    assert result["execution_claim_health"]["unresolved_execution_actions"] == 0
    assert calls == [{
        "market": "KR", "env": "practice",
        "account_id": hashlib.sha256(account.encode("utf-8")).hexdigest(),
        "trading_epoch_id": "active-epoch",
    }]


def test_unavailable_epoch_keeps_reconcile_degraded(monkeypatch):
    result, calls, _account = _mock_empty_reconcile(monkeypatch, fail=True)
    assert result["ok"] is False
    assert result["degraded"] == "execution_claim_scope_unavailable"
    assert result["execution_claim_health"]["available"] is False
    assert calls == []
