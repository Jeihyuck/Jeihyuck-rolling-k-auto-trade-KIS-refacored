from trader.pb1_engine import CandidateFeature
from tests.test_pb1_entry_plan import make_engine


def make_candidate(code="000660", order_price=2560000, planned_qty=1, entry_style_selected="PULLBACK"):
    return CandidateFeature(
        code=code,
        market="J",
        features={"order_price": order_price, "close": order_price, "entry_style_selected": entry_style_selected},
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
