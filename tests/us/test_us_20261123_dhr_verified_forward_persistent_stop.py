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


def test_real_exit_evaluator_chooses_remaining_full_qty_after_first_7(native_proof, monkeypatch):
    """STOP-01 positive decision: 14 BUY→7 filled→7 remaining, no new KIS."""
    from datetime import datetime, timezone
    import trader.us.pb1.us_exit_engine as engine
    from trader.us.protective_stop_obligation import record_confirmed_stop

    pos = _pos(qty=7)
    pos.update({"entry_price": 100.0, "max_price": 100.0})
    recorded = []
    # This acceptance stops at the *decision boundary*. The separate native
    # router must still prove holdings / UNKNOWN fences before any POST.
    monkeypatch.setattr(engine, "_make_exit_intent", lambda **kw: dict(kw))
    monkeypatch.setattr(
        "trader.us.protective_stop_obligation.record_confirmed_stop",
        lambda *args, **kwargs: recorded.append(kwargs) or True,
    )
    intent = engine.evaluate_exit(
        pos, current_price=95.0,
        now=datetime(2026, 11, 20, 16, 0, tzinfo=timezone.utc),
    )
    assert intent is not None
    assert intent["exit_type"] == "persistent_soft_stop_full_exit"
    assert intent["qty"] == 7
    assert len(recorded) == 1


def test_not_proven_first_fill_reverts_to_existing_partial_policy(native_proof, monkeypatch):
    """Missing broker confirmation cannot upgrade to a full persistent exit."""
    from datetime import datetime, timezone
    import trader.us.pb1.us_exit_engine as engine
    pos = _pos(qty=7)
    pos.update({"entry_price": 100.0, "max_price": 100.0})
    native_proof["order"]["status"] = "ACK"
    monkeypatch.setattr(engine, "_make_exit_intent", lambda **kw: dict(kw))
    monkeypatch.setattr(
        "trader.us.protective_stop_obligation.record_confirmed_stop",
        lambda *_a, **_kw: True,
    )
    intent = engine.evaluate_exit(
        pos, current_price=95.0,
        now=datetime(2026, 11, 20, 16, 0, tzinfo=timezone.utc),
    )
    assert not intent or intent.get("exit_type") != "persistent_soft_stop_full_exit"


def test_previous_day_first_partial_is_reverified_under_todays_cycle(native_proof):
    current = _pos(qty=7)
    prior = deepcopy(current["risk_state"]["state"]["soft_stop_execution"])
    current["risk_state"] = {
        "trade_date": "2026-11-23",
        "soft_stop_breach_count": 4,
        "state": {},
    }
    current["_legacy_prior_soft_stop_execution_candidate"] = prior
    assert _confirmed_soft_stop_partial_fill_for_current_lifecycle(current) == (
        True, "PERSISTENT_SOFT_STOP_VERIFIED_LEGACY_FORWARD_PARTIAL",
    )
    assert current["risk_state"]["state"] == {}  # no historical state migration


def test_legacy_original_order_day_cannot_be_forged_by_todays_risk_row(native_proof):
    pos = _pos(qty=7)
    pos["risk_state"]["trade_date"] = "2026-11-23"
    pos["risk_state"]["state"]["soft_stop_execution"]["first_soft_stop_at"] = "2026-11-23T15:04:34+00:00"
    assert _confirmed_soft_stop_partial_fill_for_current_lifecycle(pos)[0] is False


def test_stable_legacy_stop_action_overrides_dynamic_explanation(native_proof, monkeypatch):
    from datetime import datetime, timezone
    import trader.us.pb1.us_exit_engine as engine
    import trader.account_state as account_state
    from trader.us.execution.order_router import _semantic_action_identity
    pos = _pos(qty=7)
    pos.update({"entry_price": 100.0, "max_price": 100.0})
    monkeypatch.setattr(engine, "_make_exit_intent", lambda **kw: dict(kw))
    monkeypatch.setattr(
        "trader.us.protective_stop_obligation.record_confirmed_stop",
        lambda *_a, **_kw: True,
    )
    monkeypatch.setattr(account_state, "get_account_key", lambda **_: "fixture-authenticated-account")
    def decision(price, ticks):
        pos["risk_state"]["soft_stop_breach_count"] = ticks
        result = engine.evaluate_exit(
            pos, current_price=price,
            now=datetime(2026, 11, 20, 16, 0, tzinfo=timezone.utc),
        )
        assert result is not None
        assert result["semantic_action"] == "US_LEGACY_PERSISTENT_SOFT_STOP_FINAL"
        return {
            "symbol": "DHR", "side": "SELL", "qty": result["qty"],
            "reason": result["reason"], "trade_date": "2026-11-20",
            "strategy_owner": "US_STANDARD", "position_lifecycle_id": FORWARD,
            "meta": {"semantic_action": result["semantic_action"],
                     "trading_epoch_id": EPOCH,
                     "strategy_owner": "US_STANDARD"},
        }
    one = decision(95.0, 4)
    two = decision(94.5, 60)
    assert one["reason"] != two["reason"]
    assert _semantic_action_identity(one, account_env="practice").action_key == (
        _semantic_action_identity(two, account_env="practice").action_key
    )


def test_daily_first_partial_same_day_stable_and_next_day_distinct(monkeypatch):
    import trader.account_state as account_state
    from trader.us.execution.order_router import _semantic_action_identity
    monkeypatch.setattr(account_state, "get_account_key", lambda **_: "fixture-authenticated-account")
    def i(date, reason, qty):
        return {
            "side": "SELL", "qty": qty, "trade_date": date,
            "reason": reason, "strategy_owner": "US_STANDARD",
            "position_lifecycle_id": FORWARD,
            "meta": {"semantic_action": "US_LEGACY_SOFT_STOP_FIRST_PARTIAL",
                     "trading_epoch_id": EPOCH,
                     "strategy_owner": "US_STANDARD"},
        }
    a = _semantic_action_identity(i("2026-11-20", "pct=-.031 ticks=2", 7), account_env="practice")
    b = _semantic_action_identity(i("2026-11-20", "pct=-.039 ticks=3", 3), account_env="practice")
    c = _semantic_action_identity(i("2026-11-23", "pct=-.041 ticks=5", 1), account_env="practice")
    assert a.action_key == b.action_key
    assert c.action_key != a.action_key


def test_prior_uncertain_dhr_claim_blocks_new_economic_stop_action():
    from trader.us.execution.recovery_identity import (
        USRecoveryScope, USRecoveryIdentityError,
        assert_no_conflicting_us_legacy_stop_claim,
    )
    class _Records:
        def __init__(self, rows):
            self.rows = rows
        def mappings(self):
            return self
        def all(self):
            return self.rows
    class _Conn:
        def __init__(self, rows):
            self.rows = rows
            self.params = None
        def execute(self, stmt, params):
            assert "position_cycle_id=:cycle" in str(stmt)
            assert "action_state IN" in str(stmt)
            self.params = params
            return _Records(self.rows)
    scope = USRecoveryScope(
        env="practice", account_id="f" * 64, trading_epoch_id=EPOCH,
        portfolio_epoch_id="forward-portfolio", position_cycle_id=FORWARD,
        strategy_owner="US_STANDARD",
    )
    old_uncertain = _Conn([{"action_key": "old-volatile-action", "action_state": "UNCERTAIN"}])
    with pytest.raises(USRecoveryIdentityError, match="OTHER_ACTION_UNRESOLVED"):
        assert_no_conflicting_us_legacy_stop_claim(
            old_uncertain, scope=scope, prospective_action_key="new-stable-stop-action",
        )
    assert old_uncertain.params["cycle"] == FORWARD
    assert old_uncertain.params["account"] == "f" * 64
    assert_no_conflicting_us_legacy_stop_claim(
        _Conn([]), scope=scope, prospective_action_key="new-stable-stop-action",
    )
