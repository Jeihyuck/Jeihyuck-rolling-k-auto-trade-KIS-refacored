from trader.pb1_engine import CandidateFeature
from tests.test_pb1_entry_plan import make_engine


def make_candidate(code="000660", order_price=2560000, planned_qty=1, entry_style_selected="PULLBACK", atr_pct=10.5):
    features = {
        "order_price": order_price,
        "close": order_price,
        "entry_price": order_price,
        "stop_price": order_price * 0.9,
        "initial_stop": order_price * 0.9,
        "entry_style_selected": entry_style_selected,
        "atr_pct": atr_pct,
        "setup_loose_ok": True,
    }
    if atr_pct is None:
        features.pop("atr_pct", None)
    return CandidateFeature(
        code=code,
        market="J",
        features=features,
        setup_ok=True,
        reasons=[],
        mode=1,
        mode_reasons=[],
        planned_qty=planned_qty,
    )


def test_high_price_single_candidate_triggers_concentration_guard(monkeypatch):
    monkeypatch.setenv("PB1_ADAPTIVE_ATR_BACKFILL_MIN_BUYABLE", "5")
    engine = make_engine()
    orderable_candidates = [make_candidate()]
    result = engine._apply_candidate_width_backfill_and_concentration_guard(
        orderable_candidates=orderable_candidates,
        candidates=[],
        budget_meta={"per_position_budget": 900000},
    )
    assert result.concentration_guard_triggered is True
    assert result.backfill_attempted is True


def test_backfill_does_not_select_missing_atr_candidate(monkeypatch):
    monkeypatch.setenv("PB1_ADAPTIVE_ATR_BACKFILL_MIN_BUYABLE", "1")
    engine = make_engine()
    candidate = make_candidate(code="123456", order_price=10000, atr_pct=None)

    result = engine._apply_candidate_width_backfill_and_concentration_guard(
        orderable_candidates=[],
        candidates=[candidate],
        budget_meta={"per_position_budget": 100000},
    )

    assert candidate not in result.orderable_candidates


def test_backfill_candidate_requires_valid_entry_order_plan(monkeypatch):
    monkeypatch.setenv("PB1_ADAPTIVE_ATR_BACKFILL_MIN_BUYABLE", "1")
    engine = make_engine()
    candidate = make_candidate(code="123456", order_price=10000, atr_pct=10.5)

    result = engine._apply_candidate_width_backfill_and_concentration_guard(
        orderable_candidates=[],
        candidates=[candidate],
        budget_meta={"per_position_budget": 100000, "available_cash": 100000},
    )

    assert candidate in result.orderable_candidates
    for cf in result.orderable_candidates:
        ok, reasons = engine._validate_entry_plan(cf.entry_plan)
        assert ok, reasons
        assert cf.client_order_key
        assert int(cf.planned_qty or 0) > 0


def test_backfill_respects_new_position_limit(monkeypatch):
    monkeypatch.setenv("PB1_ADAPTIVE_ATR_BACKFILL_MIN_BUYABLE", "5")
    engine = make_engine()
    candidate1 = make_candidate(code="111111", order_price=500000, atr_pct=10.5)
    candidate2 = make_candidate(code="222222", order_price=10000, atr_pct=10.5)
    candidate3 = make_candidate(code="333333", order_price=10000, atr_pct=11.0)
    candidate4 = make_candidate(code="444444", order_price=10000, atr_pct=11.5)

    result = engine._apply_candidate_width_backfill_and_concentration_guard(
        orderable_candidates=[candidate1],
        candidates=[candidate2, candidate3, candidate4],
        new_position_limit=1,
        target_new_positions=1,
        tick_budget_krw=1_000_000,
        planned_spent=500_000,
        available_cash_krw=1_000_000,
        min_order_krw=0,
    )

    assert len(result.orderable_candidates) == 1
    assert result.backfill_added_count == 0
    assert result.planned_spent_after_backfill == 500_000


def test_backfill_respects_tick_budget(monkeypatch):
    monkeypatch.setenv("PB1_ADAPTIVE_ATR_BACKFILL_MIN_BUYABLE", "5")
    engine = make_engine()
    candidate_price_900k = make_candidate(code="555555", order_price=900_000, atr_pct=10.5)
    candidate_price_900k_2 = make_candidate(code="666666", order_price=900_000, atr_pct=11.0)

    result = engine._apply_candidate_width_backfill_and_concentration_guard(
        orderable_candidates=[],
        candidates=[candidate_price_900k, candidate_price_900k_2],
        new_position_limit=5,
        target_new_positions=5,
        tick_budget_krw=1_000_000,
        planned_spent=0,
        available_cash_krw=5_000_000,
        min_order_krw=0,
    )

    assert len(result.orderable_candidates) == 1
    assert result.backfill_added_count == 1
    assert result.planned_spent_after_backfill == 900_000
