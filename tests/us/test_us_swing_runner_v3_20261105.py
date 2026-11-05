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
