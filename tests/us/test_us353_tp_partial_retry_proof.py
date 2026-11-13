from copy import deepcopy

import pytest

from trader.us.tp_partial_retry import proven_partial_tp_remainder


def proof():
    meta = {
        "source_entry_contract_sha256": "signed-buy-contract",
        "portfolio_epoch_id": "portfolio", "position_cycle_id": "cycle-id",
    }
    order_meta = {
        **meta, "broker_account_hash": "account", "position_lifecycle_id": "cycle",
        "strategy_owner": "US_STANDARD", "profit_capture_stage": "tp1",
        "submit_attempt_id": "old-attempt", "execution_action_key": "action",
        "fill_evidence_type": "KIS_ORDER_CUMULATIVE_ACTUAL", "broker_open_qty": 0,
        "broker_raw_row": {"requested_qty": 5, "filled_qty": 2,
                           "remaining_qty": 0, "filled_qty_present": True,
                           "remaining_qty_present": True},
    }
    return dict(
        claim={"action_key": "action", "action": "TP1", "target_qty": 5,
               "filled_qty_before_attempt": 2, "lifecycle_id": "cycle"},
        current_attempt={"requested_qty": 3},
        prior_attempts=[{"client_order_key": "old-order", "attempt_id": "old-attempt",
                         "requested_qty": 5, "cumulative_filled_qty": 2,
                         "attempt_state": "CANCELLED_PARTIAL_FILL", "authoritative": True}],
        prior_orders=[{"client_order_key": "old-order", "symbol": "GILD", "side": "SELL",
                       "status": "CANCELLED", "order_no": "123", "qty_requested": 5,
                       "qty_filled": 2, "meta": order_meta}],
        stage={"requested_qty": 5, "cumulative_filled_qty": 2},
        intent_meta=meta, account="account", cycle="cycle", symbol="GILD",
        stage_name="tp1", qty=3,
    )


def test_cancelled_five_share_order_authorizes_only_three_remaining():
    evidence = proof()
    assert proven_partial_tp_remainder(**evidence)
    evidence["qty"] = 4
    assert not proven_partial_tp_remainder(**evidence)


@pytest.mark.parametrize("change", [
    lambda p: p["prior_attempts"][0].update(authoritative=False),
    lambda p: p["prior_attempts"][0].update(attempt_state="UNRESOLVED"),
    lambda p: p["prior_orders"][0].update(status="OPEN"),
    lambda p: p["prior_orders"][0]["meta"].update(broker_open_qty=1),
    lambda p: p["prior_orders"][0]["meta"].update(source_entry_contract_sha256="other"),
    lambda p: p["prior_orders"][0]["meta"].update(position_cycle_id="other"),
    lambda p: p["prior_orders"][0]["meta"].update(broker_account_hash="other"),
    lambda p: p["prior_orders"][0]["meta"]["broker_raw_row"].update(filled_qty_present=False),
    lambda p: p["prior_orders"][0].update(qty_filled=3),
    lambda p: p["claim"].update(filled_qty_before_attempt=3),
    lambda p: p["stage"].update(cumulative_filled_qty=3),
    lambda p: p["prior_orders"].clear(),
])
def test_incomplete_or_conflicting_evidence_fences_retry(change):
    evidence = deepcopy(proof())
    change(evidence)
    assert not proven_partial_tp_remainder(**evidence)
