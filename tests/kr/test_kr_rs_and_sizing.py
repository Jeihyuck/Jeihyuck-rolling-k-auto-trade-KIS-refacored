import pandas as pd
from trader.factors.rs_rank import rank_rs
from trader.pb1_engine import _compute_kr_per_position_budget


def test_market_specific_rs_benchmarks_are_used():
    prices = {"A": pd.Series(range(100, 230)), "B": pd.Series(range(100, 230))}
    benches = {"KOSPI": pd.Series(range(100, 230)), "KOSDAQ": pd.Series(range(200, 330))}
    out = rank_rs(prices, None, benchmark_prices_by_market=benches,
                  ticker_markets={"A": "KOSPI", "B": "KOSDAQ"})
    rows = out.set_index("ticker")
    assert rows.loc["A", "benchmark"] == "069500"
    assert rows.loc["B", "benchmark"] == "229200"


def test_two_orderable_candidates_size_against_two_not_remaining_slots(monkeypatch):
    monkeypatch.setenv("PB1_KR_MAX_POSITION_KRW", "20000000")
    budget, meta = _compute_kr_per_position_budget(
        tick_budget=27_000_000, orderable_count=2, slots_remaining=26,
        policy_max_new_positions=3,
    )
    assert meta["actual_target"] == 2
    assert budget == 13_500_000


def test_final_quantity_is_rebuilt_and_can_increase_from_old_small_plan():
    from trader.pb1_engine import CandidateFeature, _recalculate_kr_orderable_quantities
    candidate = CandidateFeature("005930", "KOSPI", {
        "order_price": 100_000, "stop_price": 95_000, "vol20": 100_000,
    }, True, [], 1, [], planned_qty=1, planned_value=100_000)
    result = _recalculate_kr_orderable_quantities(
        [candidate], per_position_budget=13_500_000, account_equity=1_000_000_000, risk_pct=.5)
    assert result[0].planned_qty == 135
    assert result[0].planned_qty > 1
