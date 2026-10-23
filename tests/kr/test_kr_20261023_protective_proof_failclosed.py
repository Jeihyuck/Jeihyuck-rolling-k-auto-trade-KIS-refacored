"""KR protective SELL proof: missing evidence always retains full pre-exit reconcile.

These tests never make KIS requests or write operational DB rows.
"""
from datetime import date

import pytest

from trader.kr.protective_sell_handoff import prove_safe_protective_sell_handoff


@pytest.mark.parametrize("value,reason", [
    (None, "KIS_BALANCE_FRESHNESS_NOT_PROVEN"),
    ("", "KIS_BALANCE_FRESHNESS_NOT_PROVEN"),
    ("broken timestamp", "KIS_BALANCE_FRESHNESS_MALFORMED"),
    ("2026-10-23T09:00:00", "KIS_BALANCE_FRESHNESS_UNZONED"),
])
def test_invalid_freshness_never_treated_as_order_authorization(value, reason):
    snapshot = {"output1": []}
    if value is not None:
        snapshot["_kr_verified_fresh_at_kst"] = value
    deferred, observed = prove_safe_protective_sell_handoff(
        engine=object(), kis=object(), env="practice",
        strategy="pb1_pullback_close",
        trade_date=date(2026, 10, 23),
        balance_snapshot=snapshot,
    )
    assert deferred is False
    assert observed == reason


@pytest.mark.parametrize("env,owner", [
    ("real", "pb1_pullback_close"),
    ("practice", "KR_INFINITE"),
    ("practice", "other_strategy"),
])
def test_fast_path_scope_stays_owner_and_practice_only(env, owner):
    deferred, reason = prove_safe_protective_sell_handoff(
        engine=object(), kis=object(), env=env, strategy=owner,
        trade_date=date(2026, 10, 23),
        balance_snapshot={"output1": []},
    )
    assert deferred is False
    assert reason == "SCOPE_NOT_VALIDATED"
