"""Protective SELL shortcuts must not be blocked by old practice accounts."""
import hashlib
from types import SimpleNamespace

from trader.kr import protective_sell_handoff as gate


def test_protective_sell_health_reads_only_authenticated_account(monkeypatch):
    account = "practice:22222222:01"
    monkeypatch.setattr(
        gate, "resolve_kr_recovery_scope",
        lambda **kwargs: SimpleNamespace(
            account_key=account, trading_epoch_id="current-epoch",
        ),
    )
    calls = []

    class Repo:
        def execution_claim_health(self, **kwargs):
            calls.append(kwargs)
            return {"available": True, "in_flight": 0, "uncertain": 0, "unresolved": 0}

    result = gate._current_account_claim_health(
        engine=object(), kis=object(), env="practice", orders=Repo(),
    )
    assert result["available"] is True
    assert calls == [{
        "market": "KR", "env": "practice",
        "account_id": hashlib.sha256(account.encode("utf-8")).hexdigest(),
        "trading_epoch_id": "current-epoch",
    }]


def test_missing_protective_account_scope_fails_closed(monkeypatch):
    def missing(**_kwargs):
        raise RuntimeError("KR_RECOVERY_TRADING_EPOCH_UNPROVEN")
    monkeypatch.setattr(gate, "resolve_kr_recovery_scope", missing)

    class Repo:
        def execution_claim_health(self, **_kwargs):
            raise AssertionError("must not perform global health read")
    import pytest
    with pytest.raises(RuntimeError, match="TRADING_EPOCH_UNPROVEN"):
        gate._current_account_claim_health(
            engine=object(), kis=object(), env="practice", orders=Repo(),
        )
