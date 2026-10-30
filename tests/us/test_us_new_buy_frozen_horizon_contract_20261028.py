"""US PB1 frozen SWING policy must not be overwritten by contradictory BUY metadata."""
from trader.us.entry_exit_contract import (
    build_us_entry_exit_contract,
    validate_us_new_buy_management_contract,
)
from trader.us.execution.order_router import resolve_entry_metadata_contract_reason


def _new_buy(style="ENTRY_MOMENTUM"):
    return {"symbol": "KR", "side": "BUY", "strategy_owner": "US_STANDARD",
            "position_action": "NEW_POSITION_BUY", "entry_style_selected": style,
            "book": "SWING_BOOK", "horizon": "SWING_CARRY",
            "exit_policy": "US_SWING_DEFAULT",
            "meta": {"strategy_owner": "US_STANDARD",
                     "position_action": "NEW_POSITION_BUY",
                     "book": "SWING_BOOK", "horizon": "SWING_CARRY",
                     "exit_policy": "US_SWING_DEFAULT"}}


def test_us_momentum_uses_existing_swing_contract_without_day_remap():
    intent = _new_buy()
    assert validate_us_new_buy_management_contract(intent) is None
    contract = build_us_entry_exit_contract(intent)
    assert contract["management"]["book"] == "SWING_BOOK"
    assert contract["management"]["horizon"] == "SWING_CARRY"
    assert contract["management"]["exit_policy"] == "US_SWING_DEFAULT"


def test_stale_meta_day_policy_fails_new_buy_preflight():
    intent = _new_buy()
    intent["meta"]["horizon"] = "DAY_TRADE"
    assert validate_us_new_buy_management_contract(intent) == "US_ENTRY_EXIT_POLICY_CONFLICT"
    assert resolve_entry_metadata_contract_reason(intent) == "US_ENTRY_EXIT_POLICY_CONFLICT"


def test_inherited_frozen_policy_conflict_is_rejected():
    base = _new_buy()
    frozen = build_us_entry_exit_contract(base)
    intent = _new_buy()
    intent["meta"]["entry_exit_contract"] = frozen
    assert validate_us_new_buy_management_contract(intent) is None
    intent["horizon"] = "DAY_TRADE"
    intent["meta"]["horizon"] = "DAY_TRADE"
    assert validate_us_new_buy_management_contract(intent) == "US_ENTRY_EXIT_POLICY_CONFLICT"


def test_tqqq_and_sells_are_not_reclassified():
    intent = _new_buy()
    intent["symbol"] = "TQQQ"
    intent["meta"]["horizon"] = "DAY_TRADE"
    assert validate_us_new_buy_management_contract(intent) is None
    intent = _new_buy()
    intent["side"] = "SELL"
    intent["meta"]["horizon"] = "DAY_TRADE"
    assert validate_us_new_buy_management_contract(intent) is None


def test_existing_position_add_not_mutated():
    intent = _new_buy()
    intent["position_action"] = "ADD_TO_EXISTING_BUY"
    intent["meta"]["position_action"] = "ADD_TO_EXISTING_BUY"
    intent["meta"]["horizon"] = "DAY_TRADE"
    assert validate_us_new_buy_management_contract(intent) is None
