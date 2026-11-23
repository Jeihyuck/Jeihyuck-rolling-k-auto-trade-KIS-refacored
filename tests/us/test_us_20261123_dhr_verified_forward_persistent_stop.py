"""DHR 14→7 partial → persistent full exit pre-authorization proof.

All original/forward relationships are injected native-ledger *fixtures*.
No live KIS HTTP, DB writes, re-sign, state mutation, or real orders.
"""
from copy import deepcopy

import pytest

from trader.us.pb1.us_exit_engine import (
    _confirmed_soft_stop_partial_fill_for_current_lifecycle,
    _verified_legacy_forward_soft_stop_partial,
)

ORIGINAL = "04248c0ae98ba36b7db83055"
FORWARD = "3a19b908f6cc4954a9dafeb8b05d3e51"
EPOCH = "fixture-us-trading-epoch"
SHA = "verified-original-buy-sha"


def _pos(qty=7):
    return {
        "symbol": "DHR", "exchange": "NYSE", "qty": qty,
        "orderable_qty": qty,
        "strategy_owner": "US_STANDARD",
        "position_lifecycle_id": ORIGINAL,
        "legacy_nine_decision_only_provisional": True,
        "_legacy_nine_db_original_provenance": {
            "original_cycle": ORIGINAL, "epoch": EPOCH,
            "v2_sha": SHA, "source_buy_day": "2026-10-27",
        },
        "meta": {
            "position_lifecycle_id": ORIGINAL,
            "strategy_owner": "US_STANDARD",
        },
        "risk_state": {
            "trade_date": "2026-11-20",
            "soft_stop_breach_count": 60,
            "state": {
                "soft_stop_execution": {
                    "order_no": "32972",
                    "client_order_key": "first-native-stop",
                    "first_soft_stop_at": "2026-11-20T15:04:34+00:00",
                    "first_soft_stop_price": 225.84,
                    "position_lifecycle_id": FORWARD,
                    "soft_stop_partial_done": True,
                    "soft_stop_triggered_today": True,
                    "first_soft_stop_filled_qty": 7,
                },
            },
        },
    }


def _order():
    return {
        "symbol": "DHR", "trade_date": "2026-11-20",
        "side": "SELL", "status": "FILLED",
        "qty_requested": 7, "qty_filled": 7,
        "order_no": "0000032972",
        "client_order_key": "first-native-stop",
        "trading_epoch_id": EPOCH,
        "meta": {
            "stop_type": "soft_stop_loss",
            "strategy_owner": "US_STANDARD",
            "position_lifecycle_id": FORWARD,
            "source_original_runtime_lifecycle_id": ORIGINAL,
            "source_original_entry_exit_contract_sha256": SHA,
            "forward_management_not_original_buy": True,
        },
    }


@pytest.fixture()
def native_proof(monkeypatch):
    from trader.us.execution import us_legacy_nine_active_sell as legacy
    from trader.us.db import repos

    def scoped(position, trade_date):
        assert trade_date == "2026-11-20"
        return {
            **position,
            "trading_epoch_id": EPOCH,
            "position_cycle_id": FORWARD,
            "position_lifecycle_id": FORWARD,
            "_legacy_stop_proof_scoped": True,
        }

    current = {"order": _order()}
    monkeypatch.setattr(legacy, "build_verified_legacy_stop_obligation_position", scoped)
    monkeypatch.setattr(repos, "load_us_order_for_fill", lambda **_: current["order"])
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setenv("WSL_RUN_MARKET", "US")
    return current


def test_first_partial_7_of_14_can_escalate_with_proven_forward_and_no_state_lifecycle(native_proof):
    before = _pos()
    original = deepcopy(before)
    result, reason = _confirmed_soft_stop_partial_fill_for_current_lifecycle(before)
    assert (result, reason) == (
        True, "PERSISTENT_SOFT_STOP_VERIFIED_LEGACY_FORWARD_PARTIAL",
    )
    assert before == original  # no fake state.lifecycle or rewritten original BUY


@pytest.mark.parametrize("mutation", [
    lambda p, o: p["risk_state"]["state"]["soft_stop_execution"].update(
        {"position_lifecycle_id": "wrong-forward-cycle"}),
    lambda p, o: p["_legacy_nine_db_original_provenance"].update(
        {"original_cycle": "other-buy"}),
    lambda p, o: p["risk_state"]["state"]["soft_stop_execution"].update(
        {"first_soft_stop_filled_qty": 8}),
    lambda p, o: o.update({"status": "ACK"}),
    lambda p, o: o["meta"].update({"stop_type": "hard_stop_loss"}),
    lambda p, o: o["meta"].update({"source_original_runtime_lifecycle_id": "other-buy"}),
    lambda p, o: o["meta"].update({"source_original_entry_exit_contract_sha256": "tampered"}),
    lambda p, o: o.update({"client_order_key": "other-order"}),
    lambda p, o: o.update({"trading_epoch_id": "other-epoch"}),
])
def test_cross_scope_unconfirmed_or_mismatched_first_fill_cannot_escalate(native_proof, mutation):
    pos = _pos()
    mutation(pos, native_proof["order"])
    allowed, reason = _confirmed_soft_stop_partial_fill_for_current_lifecycle(pos)
    assert allowed is False, reason


def test_no_legacy_proof_or_wrong_env_cannot_escalate(native_proof, monkeypatch):
    pos = _pos()
    pos.pop("legacy_nine_decision_only_provisional")
    assert _confirmed_soft_stop_partial_fill_for_current_lifecycle(pos)[0] is False
    pos = _pos()
    monkeypatch.setenv("KIS_ENV", "live")
    assert _confirmed_soft_stop_partial_fill_for_current_lifecycle(pos)[0] is False
    monkeypatch.setenv("KIS_ENV", "practice")
    pos = _pos()
    pos["strategy_owner"] = "TQQQ_INFINITE"
    assert _confirmed_soft_stop_partial_fill_for_current_lifecycle(pos)[0] is False


def test_unavailable_original_account_or_forward_proof_fails_closed(monkeypatch, native_proof):
    from trader.us.execution import us_legacy_nine_active_sell as legacy
    monkeypatch.setattr(
        legacy, "build_verified_legacy_stop_obligation_position",
        lambda *_a, **_kw: (_ for _ in ()).throw(ValueError("wrong_account")),
    )
    assert _verified_legacy_forward_soft_stop_partial(
        _pos(), _pos()["risk_state"]["state"]["soft_stop_execution"],
    )[0] is False


def test_same_lifecycle_confirmed_path_preserved_without_legacy_bridge(native_proof):
    pos = _pos()
    pos.pop("legacy_nine_decision_only_provisional")
    pos["position_lifecycle_id"] = "current-cycle"
    pos["risk_state"]["state"]["lifecycle"] = {
        "lifecycle_id": "current-cycle", "is_open": True,
    }
    pos["risk_state"]["state"]["soft_stop_execution"]["position_lifecycle_id"] = "current-cycle"
    assert _confirmed_soft_stop_partial_fill_for_current_lifecycle(pos) == (
        True, "PERSISTENT_SOFT_STOP_ESCALATION",
    )
