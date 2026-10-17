"""US_STANDARD broker-proof-only pilot tests; no KIS order or production DB I/O."""
import inspect

import pytest

from trader.us.execution.evidence_policy import require_broker_proof_for_balance_estimate

CASES = [
    ("2026-10-13", "MCK", 1),
    ("2026-10-14", "NET", 1),
    ("2026-10-14", "COST", 1),
    ("2026-10-14", "ABBV", 2),
    ("2026-10-14", "PG", 4),
    ("2026-10-14", "GILD", 4),
    ("2026-10-15", "DHR", 2),
]


@pytest.mark.parametrize("date,symbol,qty", CASES)
def test_seven_cases_proof_only_gate_ignores_balance_estimate(monkeypatch, date, symbol, qty):
    from trader.us.execution import reconcile
    order = {
        "symbol": symbol, "side": "SELL", "order_no": "mock-order-"+symbol,
        "client_order_key": "mock-key-"+symbol,
        "qty_requested": qty, "pre_order_position_qty": 2 * qty,
        "limit_price": 100.0,
        "meta": {"strategy_owner": "US_STANDARD",
                 "exit_reason": "TP1", "position_lifecycle_id": "mock-life"},
    }
    monkeypatch.setenv("US_STANDARD_BROKER_PROOF_ONLY", "1")
    monkeypatch.setattr("trader.us.db.repos.load_pending_ack_orders",
                        lambda trade_date, env="practice": [order])

    def _cannot_promote(**_kwargs):
        pytest.fail("balance-only observation must not promote order/claim to filled")

    monkeypatch.setattr("trader.us.db.repos.mark_order_filled_by_reconcile", _cannot_promote)

    class Provider:
        def get_balance(self, force_refresh=False):
            return {"positions": [
                {"symbol": symbol, "qty": qty, "orderable_qty": qty, "avg_price": 100.0}
            ]}
        def get_fills_by_order_no(self, **kwargs):
            return {"filled_qty": 0}

    result = reconcile.reconcile_ack_orders_with_balance(
        provider=Provider(), trade_date=date, env="practice"
    )
    assert result["confirmed_count"] == 0
    assert result["balance_reconcile_count"] == 0
    assert result["unresolved_count"] >= 1
    assert symbol in result["symbols_by_status"]["unresolved"]
    assert result["manual_reconcile_required"] == 1


def test_off_by_default_and_named_owner_required():
    row = {"meta": {"strategy_owner": "US_STANDARD"}}
    assert not require_broker_proof_for_balance_estimate(row, {})
    assert require_broker_proof_for_balance_estimate(
        row, {"US_STANDARD_BROKER_PROOF_ONLY": "1"}
    )
    assert not require_broker_proof_for_balance_estimate(
        {"meta": {"strategy_owner": "TQQQ_INFINITE"}},
        {"US_STANDARD_BROKER_PROOF_ONLY": "1"},
    )
    assert not require_broker_proof_for_balance_estimate(
        {"meta": {}}, {"US_STANDARD_BROKER_PROOF_ONLY": "1"},
    )


def test_broker_proof_path_is_not_suppressed():
    from trader.us.execution.order_journal import replay_order_journal
    src = inspect.getsource(replay_order_journal)
    # The gate lives only in the no-broker-evidence fallback.
    assert "require_broker_proof_for_balance_estimate(latest)" in src
    assert "apply_broker_order_observation" in src
    assert "BALANCE_ESTIMATE_PENDING_BROKER" in src
