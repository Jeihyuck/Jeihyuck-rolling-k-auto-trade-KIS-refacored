"""KR -3/-6 proposal: no live decision, frozen stop and owner isolation."""
import pytest
from trader.kr.swing_stop_shadow import evaluate_kr_pb1_swing_stop_shadow


def _pos(**kwargs):
    base = {
        "code": "053800", "exit_policy_family": "SWING_STAGED_EXIT",
        "entry_price": 100, "stop_price_at_entry": 92,
    }
    base.update(kwargs)
    return base


@pytest.mark.parametrize("mark,soft,hard", [
    (100, False, False), (97, True, False),
    (95, True, False), (94, True, True), (90, True, True),
])
def test_shadow_thresholds_and_touches_do_not_generate_orders(mark, soft, hard):
    r = evaluate_kr_pb1_swing_stop_shadow(
        _pos(), mark, strategy_owner="pb1_pullback_close",
    )
    assert r.eligible
    assert r.soft_threshold == pytest.approx(97)
    assert r.hard_threshold == pytest.approx(94)
    assert (r.soft_touched, r.hard_touched) == (soft, hard)
    assert not r.order_authorized


def test_existing_tighter_frozen_stop_is_never_widened():
    r = evaluate_kr_pb1_swing_stop_shadow(
        _pos(stop_price_at_entry=96), 95, strategy_owner="pb1_pullback_close",
    )
    assert r.hard_threshold == 96
    assert r.hard_touched
    assert r.raw_stop_preserved == 96


@pytest.mark.parametrize("owner,family", [
    ("KR_INFINITE", "SWING_STAGED_EXIT"),
    ("pb1_pullback_close", "INTRADAY_PROFIT_PROTECT"),
    ("pb1_pullback_close", "CORE_TREND_FOLLOW"),
])
def test_other_strategy_owners_and_exit_families_unchanged(owner, family):
    assert not evaluate_kr_pb1_swing_stop_shadow(
        _pos(exit_policy_family=family), 89, strategy_owner=owner,
    ).eligible


@pytest.mark.parametrize("mark", [None, float("nan"), -1, 0])
def test_missing_price_never_authorizes_routing(mark):
    r = evaluate_kr_pb1_swing_stop_shadow(
        _pos(), mark, strategy_owner="pb1_pullback_close",
    )
    assert not r.eligible and not r.order_authorized
