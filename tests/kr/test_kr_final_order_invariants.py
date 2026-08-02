from trader.kr.regime import build_kr_regime_snapshot
from trader.pb1_engine import (
    CandidateFeature,
    _compute_kr_per_position_budget,
    _enforce_kr_final_order_invariants,
)


def _obs():
    return {
        "close": 120, "ma20": 110, "ma50": 105, "ma200": 100,
        "ma20_slope_5d": 1, "breadth_ma20": .7, "breadth_ma50": .65,
        "advance_ratio": .7, "median_return_5d": .02,
        "return_5d": .03, "return_20d": .08,
    }


def _candidate(code: str, market: str, qty: int = 200, price: float = 100_000):
    return CandidateFeature(
        code, market,
        {"order_price": price, "stop_price": price * .95, "vol20": 1_000_000,
         "market": market, "above_vwap": True, "last_price": price,
         "ma20": price * .9, "ma20_slope_5d": 1, "turnover_expansion": 2,
         "rs_percentile": .99},
        True, [], 1, [], planned_qty=qty, planned_value=qty * price,
    )


def _apply(candidates, *, slots=10, existing_count=0, budgets=None, cap=40_000_000, overlay=None):
    snapshot = build_kr_regime_snapshot({"KOSPI": _obs(), "KOSDAQ": _obs()})
    return _enforce_kr_final_order_invariants(
        candidates, snapshot=snapshot,
        base_overlay=overlay or {"portfolio_equity_krw": 100_000_000,
                                 "gross_exposure_pct": 0, "sector_exposure_pct": {},
                                 "high_beta_exposure_pct": 0},
        existing_positions=[], market_budgets=budgets or {"KOSPI": 20_000_000, "KOSDAQ": 20_000_000},
        total_tick_cap=cap, slots_remaining_at_tick_start=slots,
        max_positions=10, existing_positions_count=existing_count,
        available_cash=100_000_000,
    )


def test_zero_remaining_slots_produces_zero_kr_orders():
    budget, meta = _compute_kr_per_position_budget(
        tick_budget=10_000_000, orderable_count=2, slots_remaining=0,
    )
    assert budget == 0 and meta["actual_target"] == 0


def test_second_market_cannot_exceed_global_remaining_slots():
    out, meta = _apply([_candidate("005930", "KOSPI"), _candidate("247540", "KOSDAQ")], slots=1)
    assert len(out) == meta["final_new_orders"] == 1


def test_cross_market_orders_never_exceed_pb1_max_positions():
    out, _ = _apply([_candidate("005930", "KOSPI"), _candidate("247540", "KOSDAQ")], slots=2, existing_count=9)
    assert len(out) == 1 and 9 + len(out) <= 10


def test_planned_value_by_market_respects_market_budget():
    out, meta = _apply(
        [_candidate("005930", "KOSPI"), _candidate("000660", "KOSPI"), _candidate("247540", "KOSDAQ")],
        budgets={"KOSPI": 5_000_000, "KOSDAQ": 8_000_000}, cap=13_000_000,
    )
    assert out
    assert meta["planned_value_by_market"]["KOSPI"] <= 5_000_000
    assert meta["planned_value_by_market"]["KOSDAQ"] <= 8_000_000


def test_kospi_orders_cannot_consume_kosdaq_allocation():
    _, meta = _apply(
        [_candidate("005930", "KOSPI"), _candidate("000660", "KOSPI"), _candidate("247540", "KOSDAQ")],
        budgets={"KOSPI": 4_000_000, "KOSDAQ": 9_000_000}, cap=13_000_000,
    )
    assert meta["planned_value_by_market"]["KOSPI"] <= 4_000_000
    assert meta["planned_value_by_market"]["KOSDAQ"] <= 9_000_000


def test_final_planned_value_sum_respects_total_tick_cap():
    _, meta = _apply(
        [_candidate("005930", "KOSPI"), _candidate("000660", "KOSPI"), _candidate("247540", "KOSDAQ")],
        cap=7_000_000,
    )
    assert meta["planned_value_sum"] <= 7_000_000


def test_min_position_floor_never_exceeds_market_allocation(monkeypatch):
    monkeypatch.setenv("PB1_KR_MIN_POSITION_KRW", "2000000")
    budget, _ = _compute_kr_per_position_budget(tick_budget=500_000, orderable_count=1, slots_remaining=1)
    assert budget == 500_000


def test_final_resized_value_rechecks_single_position_cap():
    out, _ = _apply([_candidate("005930", "KOSPI")])
    assert sum(c.planned_value for c in out) <= 10_000_000


def test_final_resized_value_rechecks_gross_exposure_cap():
    out, meta = _apply([_candidate("005930", "KOSPI")], overlay={
        "portfolio_equity_krw": 100_000_000, "gross_exposure_pct": .94,
        "sector_exposure_pct": {}, "high_beta_exposure_pct": 0,
    })
    assert meta["projected_gross_exposure_pct"] < .95
    assert sum(c.planned_value for c in out) < 1_000_000


def test_multiple_new_orders_use_cumulative_projected_exposure():
    out, meta = _apply([_candidate("005930", "KOSPI"), _candidate("000660", "KOSPI")], overlay={
        "portfolio_equity_krw": 100_000_000, "gross_exposure_pct": .89,
        "sector_exposure_pct": {}, "high_beta_exposure_pct": 0,
    })
    assert meta["projected_gross_exposure_pct"] < .95
    assert sum(c.planned_value for c in out) < 6_000_000


def test_final_resized_value_rechecks_sector_cap(monkeypatch):
    monkeypatch.setenv("KR_MAX_SECTOR_EXPOSURE_RISK_ON", "0.05")
    out, _ = _apply([_candidate("005930", "KOSPI"), _candidate("000660", "KOSPI")])
    assert sum(c.planned_value for c in out) <= 5_000_000


def test_final_gate_uses_mark_to_market_portfolio_equity():
    out, meta = _apply([_candidate("005930", "KOSPI")], overlay={
        "portfolio_equity_krw": 50_000_000, "gross_exposure_pct": .80,
        "sector_exposure_pct": {}, "high_beta_exposure_pct": 0,
    })
    assert meta["projected_gross_exposure_pct"] < .95
    assert sum(c.planned_value for c in out) < 7_500_000


def test_final_gate_does_not_overwrite_overlay_equity_with_cost_basis():
    # The helper has no cost-basis equity argument: the overlay is authoritative.
    out, meta = _apply([_candidate("005930", "KOSPI")], overlay={
        "portfolio_equity_krw": 40_000_000, "gross_exposure_pct": .90,
        "sector_exposure_pct": {}, "high_beta_exposure_pct": 0,
    })
    assert meta["projected_gross_exposure_pct"] < .95
    assert sum(c.planned_value for c in out) < 2_000_000


def test_unrealized_loss_account_does_not_overallocate_from_cost_basis_equity():
    out, _ = _apply([_candidate("005930", "KOSPI")], overlay={
        "portfolio_equity_krw": 50_000_000, "gross_exposure_pct": 0,
        "sector_exposure_pct": {}, "high_beta_exposure_pct": 0,
    })
    assert sum(c.planned_value for c in out) <= 5_000_000  # 10% of MTM equity


def test_unrealized_gain_account_uses_same_equity_for_base_and_incremental_exposure():
    out, meta = _apply([_candidate("005930", "KOSPI")], overlay={
        "portfolio_equity_krw": 200_000_000, "gross_exposure_pct": .90,
        "sector_exposure_pct": {}, "high_beta_exposure_pct": 0,
    })
    expected = .90 + sum(c.planned_value for c in out) / 200_000_000
    assert meta["projected_gross_exposure_pct"] == expected


def test_invalid_portfolio_equity_fails_closed():
    out, meta = _apply([_candidate("005930", "KOSPI")], overlay={
        "portfolio_equity_krw": 0, "gross_exposure_pct": 0,
        "sector_exposure_pct": {}, "high_beta_exposure_pct": 0,
    })
    assert out == [] and not meta["invariant_valid"]
    assert meta["reason"] == "INVALID_PORTFOLIO_EQUITY"


def test_nan_or_nonfinite_final_notional_fails_closed():
    candidate = _candidate("005930", "KOSPI")
    candidate.planned_value = float("nan")
    out, meta = _apply([candidate])
    assert out == [] and not meta["invariant_valid"]
    assert meta["reason"] == "NONFINITE_FINAL_ORDER"


def test_market_budget_invariant_failure_blocks_order_submission():
    out, meta = _apply([_candidate("005930", "KOSPI")], budgets={
        "KOSPI": float("nan"), "KOSDAQ": 0,
    })
    assert out == [] and not meta["invariant_valid"]


def test_invariant_false_clears_all_orderable_candidates(monkeypatch):
    monkeypatch.setenv("KR_MAX_GROSS_EXPOSURE_PCT", "0.50")
    out, meta = _apply([_candidate("005930", "KOSPI")], overlay={
        "portfolio_equity_krw": 100_000_000, "gross_exposure_pct": .60,
        "sector_exposure_pct": {}, "high_beta_exposure_pct": 0,
    })
    assert out == [] and not meta["invariant_valid"]


def test_production_sequence_does_not_submit_when_final_invariant_is_false():
    submitted = []
    candidate = _candidate("005930", "KOSPI")
    candidate.planned_value = float("inf")
    orderable, meta = _apply([candidate])
    for order in orderable:  # production submits only this returned collection
        submitted.append(order)
    assert not meta["invariant_valid"] and orderable == [] and submitted == []
