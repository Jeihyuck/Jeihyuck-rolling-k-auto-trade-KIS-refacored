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
        equity_krw=100_000_000,
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
