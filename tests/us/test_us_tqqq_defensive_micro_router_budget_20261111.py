"""Only fake/no-broker tests for the new TQQQ micro budget boundary."""
import pytest

from trader.us.execution.order_router import defensive_micro_first_buy_budget_error


def intent(*, max_usd=125, cost_buffer=0.01, owner="TQQQ_INFINITE", proof=True):
    meta = {
        "tqqq_defensive_micro_first_buy_evidence": {
            "defensive_micro_first_buy_max_usd": max_usd,
            "defensive_micro_first_buy_cost_buffer_pct": cost_buffer,
        }
    } if proof else {}
    return {
        "symbol": "TQQQ", "strategy_owner": owner, "sleeve_id": owner,
        "side": "BUY", "reason": "DEFENSIVE_MICRO_FIRST_BUY", "meta": meta,
    }


def test_native_boundary_checks_all_in_cash_requirement(monkeypatch):
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.delenv("US_BROKER_ORDERABLE_CASH_SAFETY_BUFFER", raising=False)
    assert defensive_micro_first_buy_budget_error(intent(), qty=1, price=123) is None
    assert defensive_micro_first_buy_budget_error(intent(), qty=1, price=124) == (
        "TQQQ_DEFENSIVE_MICRO_BUDGET_EXCEEDED"
    )
    assert defensive_micro_first_buy_budget_error(intent(), qty=2, price=100) == (
        "TQQQ_DEFENSIVE_MICRO_BUDGET_EXCEEDED"
    )
    monkeypatch.setenv("US_BROKER_ORDERABLE_CASH_SAFETY_BUFFER", "1.05")
    assert defensive_micro_first_buy_budget_error(intent(), qty=1, price=120) == (
        "TQQQ_DEFENSIVE_MICRO_BUDGET_EXCEEDED"
    )


@pytest.mark.parametrize("cap", [0, -1, 125.0001, float("nan")])
def test_invalid_or_missing_proof_fails_closed(monkeypatch, cap):
    monkeypatch.setenv("KIS_ENV", "practice")
    assert defensive_micro_first_buy_budget_error(intent(max_usd=cap), qty=1, price=50) == (
        "TQQQ_DEFENSIVE_MICRO_BUDGET_INVALID"
    )
    assert defensive_micro_first_buy_budget_error(intent(proof=False), qty=1, price=50) == (
        "TQQQ_DEFENSIVE_MICRO_PROOF_MISSING"
    )


def test_practice_only_and_other_owner_is_not_touched(monkeypatch):
    monkeypatch.setenv("KIS_ENV", "real")
    assert defensive_micro_first_buy_budget_error(intent(), qty=1, price=50) == (
        "TQQQ_DEFENSIVE_MICRO_PRACTICE_ONLY"
    )
    assert defensive_micro_first_buy_budget_error(intent(owner="US_STANDARD"), qty=1, price=500) is None
