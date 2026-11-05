from datetime import date, timedelta
from trader.us.pb1.swing_runner_exit_v3 import (
    V3_RUNNER_POLICY, chandelier_from_verified_completed_bars, evaluate_v3_runner,
)


def sig(**k):
    args = dict(entry_price=100, verified_high=110, current_price=108,
                holding_qty=10, orderable_qty=10, trend_state="TRIM",
                trend_verified=True, high_verified=True, atr_evidence=None,
                partial_stage_status="NOT_TRIGGERED",
                final_stage_status="NOT_TRIGGERED", policy=V3_RUNNER_POLICY)
    args.update(k)
    return evaluate_v3_runner(**args)


def test_v3_20_percent_of_profit_not_high_price():
    got = sig()
    assert got["stage"] == "PROFIT_DEFENSE_PARTIAL"
    assert got["qty"] == 5
    assert abs(got["giveback"] - .2) < 1e-10


def test_unverified_trend_and_atr_never_confirm_gb():
    assert sig(trend_state="UNKNOWN", trend_verified=False)["action"] == "HOLD"
    assert sig(trend_state="WARNING")["action"] == "HOLD"


def test_r1_r2_r4_preempt_partial():
    assert sig(trend_state="EXIT")["stage"] == "RUNNER_FINAL"
    assert sig(current_price=105, trend_state="TRIM")["reason"] == "R2_GIVEBACK_TREND"
    assert sig(current_price=103, trend_state="UNKNOWN")["reason"] == "R4_PROFIT_GIVEBACK_CAP"


def test_v3_minimum_runner_and_single_partial_budget():
    assert sig(holding_qty=1, orderable_qty=1)["action"] == "HOLD"
    assert sig(holding_qty=3, orderable_qty=3)["qty"] == 1
    for state in ("DONE", "ACKED", "BROKER_UNKNOWN", "BLOCKED", "PENDING"):
        assert sig(partial_stage_status=state)["action"] == "HOLD"
    assert sig(final_stage_status="ACKED")["action"] == "HOLD"


def _bars(n=44):
    start = date(2026, 7, 1)
    return [
        {"trade_date": (start + timedelta(days=i)).isoformat(),
         "high": 125 + 0.1 * i, "low": 120 + 0.1 * i,
         "close": 123 + 0.1 * i, "complete": True,
         "source": "KIS_DAILY_VERIFIED",
         "adjustment_basis": "RAW"}
        for i in range(n)
    ]


def test_atr_completed_bars_no_future_or_unverified_price_daily():
    bars = _bars()
    e = chandelier_from_verified_completed_bars(bars, decision_date="2026-10-01")
    assert e["valid"] and e["atr14"] > 0
    assert not chandelier_from_verified_completed_bars(
        bars, decision_date=bars[-1]["trade_date"])["valid"]
    bars[0]["source"] = "PRICE_DAILY_UNVERIFIED"
    assert chandelier_from_verified_completed_bars(bars, decision_date="2026-10-01")["reason"] == "BAR_SOURCE_UNVERIFIED"


def test_ratchet_stop_never_looser_when_highs_decline():
    bars = _bars()
    for i in range(38, len(bars)):
        bars[i]["high"] = 131
        bars[i]["low"] = 125
        bars[i]["close"] = 127
    e = chandelier_from_verified_completed_bars(bars, decision_date="2026-10-01")
    assert e["valid"]
    assert e["stop"] >= e["previous_stop"]

def test_outstanding_partial_blocks_discretionary_final_until_reconciled():
    for active in ("ACKED", "BROKER_UNKNOWN", "BLOCKED"):
        result = sig(current_price=103, trend_state="EXIT", partial_stage_status=active)
        assert result["action"] == "HOLD"
        assert "RECONCILE" in result["reason"]
    assert sig(current_price=103, trend_state="EXIT", partial_stage_status="DONE")["stage"] == "RUNNER_FINAL"


def test_v3_position_adapter_preserves_explicit_zero_broker_orderable(monkeypatch):
    # A reservation can reduce orderable to zero even when holding is 10.
    from datetime import datetime, timezone
    from trader.us.pb1 import swing_runner_exit_v3 as module
    stages = {"PROFIT_DEFENSE_PARTIAL": "NOT_TRIGGERED",
              "RUNNER_FINAL": "NOT_TRIGGERED"}
    monkeypatch.setattr(
        "trader.us.execution.v3_stage_truth.read_v3_lifecycle_stage_states",
        lambda **kwargs: stages,
    )
    seen = {}
    def capture(**kwargs):
        seen.update(kwargs)
        return {"action": "HOLD", "reason": "NO_VERIFIED_ORDERABLE_QTY"}
    monkeypatch.setattr(module, "evaluate_v3_runner", capture)
    got = module.evaluate_v3_from_position(
        position={"symbol": "SPY", "qty": 10, "holding_qty": 10,
                  "orderable_qty": 0, "entry_price": 100},
        current_price=110,
        now=datetime(2026, 11, 5, 15, tzinfo=timezone.utc),
        contract={"sha256": "a" * 64,
                  "management": {"runner_exit": V3_RUNNER_POLICY}},
    )
    assert got["action"] == "HOLD"
    assert seen["holding_qty"] == 10
    assert seen["orderable_qty"] == 0


def test_v3_position_adapter_blocks_missing_or_conflicting_broker_quantity():
    from datetime import datetime, timezone
    from trader.us.pb1.swing_runner_exit_v3 import evaluate_v3_from_position
    now = datetime(2026, 11, 5, 15, tzinfo=timezone.utc)
    contract = {"sha256": "a" * 64,
                "management": {"runner_exit": V3_RUNNER_POLICY}}
    for position in (
        {"qty": 10, "entry_price": 100},
        {"qty": 10, "orderable_qty": -1, "entry_price": 100},
        {"qty": 10, "orderable_qty": 11, "entry_price": 100},
    ):
        got = evaluate_v3_from_position(
            position=position, current_price=110, now=now,
            contract=contract,
        )
        assert got["action"] == "HOLD"
        assert got["reason"] in {
            "V3_BROKER_QUANTITY_UNVERIFIED", "V3_BROKER_QUANTITY_CONFLICT",
        }


def test_v3_runner_final_requires_full_orderable_inventory():
    # Other live SELL reservations must reconcile before a one-time FINAL.
    for trend in ("EXIT", "TRIM"):
        decision = sig(
            current_price=103, trend_state=trend,
            holding_qty=10, orderable_qty=7,
        )
        assert decision["action"] == "HOLD"
        assert decision["reason"] == "RUNNER_FINAL_RESERVED_INVENTORY_RECONCILE_REQUIRED"
    # GB1 partial still fits within confirmed orderable inventory.
    partial = sig(holding_qty=10, orderable_qty=3)
    assert partial["stage"] == "PROFIT_DEFENSE_PARTIAL"
    assert partial["qty"] == 3


def test_v3_runner_rejects_nonintegral_or_boolean_broker_inventory():
    # Never silently truncate a fractional quantity or treat True as one share.
    for invalid in (2.5, True, False, "2.0", "-1", " 2", 2 + 0j):
        assert sig(holding_qty=invalid)["action"] == "HOLD"
        assert sig(orderable_qty=invalid)["action"] == "HOLD"
    assert sig(holding_qty="10", orderable_qty="10")["qty"] == 5
    assert sig(holding_qty=10, orderable_qty=11)["action"] == "HOLD"


def test_v3_position_adapter_refuses_fractional_or_boolean_broker_inventory():
    from datetime import datetime, timezone
    from trader.us.pb1.swing_runner_exit_v3 import evaluate_v3_from_position
    now = datetime(2026, 11, 5, 15, tzinfo=timezone.utc)
    contract = {"sha256": "a" * 64, "management": {"runner_exit": V3_RUNNER_POLICY}}
    for invalid in (3.7, True, False, "3.0", " 3", "-1"):
        for field in ("qty", "orderable_qty"):
            position = {"symbol": "SPY", "qty": 10, "orderable_qty": 10, "entry_price": 100}
            position[field] = invalid
            result = evaluate_v3_from_position(
                position=position, current_price=108, now=now, contract=contract,
            )
            assert result["action"] == "HOLD"
            assert result["reason"] == "V3_BROKER_QUANTITY_UNVERIFIED"
