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
