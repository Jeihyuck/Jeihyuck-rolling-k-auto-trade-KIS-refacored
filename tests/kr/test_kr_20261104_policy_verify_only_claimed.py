"""Normal PB1 entry contracts must not be treated as failed legacy adoption."""
from trader.kr import market_state_overlay as overlay


def _native_position():
    return {
        "code": "059090", "owner_strategy": "PB1", "qty": 91,
        "unrealized_pnl_pct": 0.05,
        "entry_style_selected": "ENTRY_PULLBACK",
        "exit_policy_family": "SWING_STAGED_EXIT",
        "entry_exit_plan_json": {
            "profit_plan": {"tp1_sell_pct": 0.25},
            "exit_policy_family": "SWING_STAGED_EXIT",
        },
        "position_meta": {},
    }


def test_native_pb1_contract_does_not_run_legacy_adoption_verifier(monkeypatch):
    monkeypatch.setattr(overlay, "has_kr_policy_missing_adoption_claim", lambda _: False)

    def must_not_verify(_position):
        raise AssertionError("unclaimed legacy adoption must not be verified")

    monkeypatch.setattr(overlay, "is_verified_kr_policy_missing_adoption", must_not_verify)
    assert overlay.generate_kr_profit_capture_intents(
        [_native_position()], {"market_state": "KR_NORMAL"},
    ) == []


def test_invalid_claimed_legacy_adoption_remains_blocked(monkeypatch):
    monkeypatch.setattr(overlay, "has_kr_policy_missing_adoption_claim", lambda _: True)
    monkeypatch.setattr(overlay, "is_verified_kr_policy_missing_adoption", lambda _: False)
    assert overlay.generate_kr_profit_capture_intents(
        [_native_position()], {"market_state": "KR_NORMAL"},
    ) == []
