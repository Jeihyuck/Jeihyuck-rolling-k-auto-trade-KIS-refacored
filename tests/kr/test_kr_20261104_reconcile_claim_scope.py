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


def test_reconciled_order_without_broker_filled_qty_is_not_a_fill():
    # 2026-10-27 293490 is labelled RECONCILED in orders, but the
    # corresponding execution claim is UNCERTAIN and KIS fill proof is absent.
    recorded = []

    class Orders:
        def record_execution_claim_for_order(self, key, **observation):
            recorded.append((key, observation))

    error = reconcile_kis._record_execution_claim_observation(
        orders_repo=Orders(),
        source_order={"client_order_key": "practice:2026-10-27:293490:BUY"},
        broker_status="RECONCILED",
        cumulative_filled_qty=None,
        requested_qty=174,
    )
    assert error is None
    assert recorded == [
        (
            "practice:2026-10-27:293490:BUY",
            {
                "state": "UNRESOLVED",
                "cumulative_filled_qty": None,
                "authoritative": False,
            },
        )
    ]


def test_real_execution_claim_repo_isolates_unresolved_accounts_and_epochs():
    from datetime import date

    from trader.db.schema import schema_for_engine
    from trader.execution_claims import DurableExecutionClaimRepo
    from trader.execution_state import SemanticActionIdentity

    engine = sa.create_engine("sqlite:///:memory:")
    schema = schema_for_engine(engine)
    schema.metadata.create_all(
        engine, tables=[schema.execution_claims, schema.execution_attempts],
    )
    claims = DurableExecutionClaimRepo(
        engine, schema.execution_claims, schema.execution_attempts,
    )
    for account, epoch in (("current-account-hash", "epoch-current"),
                           ("old-account-hash", "epoch-old")):
        identity = SemanticActionIdentity(
            env="practice", account_id=account, market="KR",
            trading_epoch_id=epoch, strategy_owner="PB1",
            lifecycle_id=f"293490:{account}", action="BUY_ENTRY:PB1-AM",
            trade_date=date(2026, 10, 27),
        )
        assert claims.acquire(
            identity, attempt_id=f"attempt:{account}", requested_qty=174,
        ).acquired
    assert claims.health(market="KR")["unresolved_execution_actions"] == 2
    assert claims.health(
        market="KR", env="practice", account_id="current-account-hash",
        trading_epoch_id="epoch-current",
    )["unresolved_execution_actions"] == 1
    assert claims.health(
        market="KR", env="practice", account_id="current-account-hash",
        trading_epoch_id="epoch-old",
    )["unresolved_execution_actions"] == 0
