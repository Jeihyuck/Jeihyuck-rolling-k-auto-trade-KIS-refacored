from __future__ import annotations

from types import SimpleNamespace

from trader.pb1_runtime_guards import _apply_buyable_candidate_backfill


class DummyEngine:
    env = "practice"
    window_label = "day"
    _existing_holding_codes = {"207940"}
    _open_buy_codes = set()
    _today_buy_codes = set()
    _cooldown_codes = set()
    _buyable_gate_context = {
        "207940": {"kis_holding_qty": 1, "holding_qty": 1},
        "005930": {"kis_holding_qty": 0, "holding_qty": 0},
    }

    def _entry_stage_name(self):
        return "PB1-CLOSE"

    def _client_order_key(self, code, mode, side, window_tag, stage):
        return f"practice:pb1:{code}:{side}:{stage}"

    def _build_entry_plan(
        self,
        cf,
        *,
        entry_price,
        order_price,
        stop_price,
        trigger_ok,
        trigger_info,
        entry_mode,
        stage,
        price_source,
    ):
        return {
            "qty": int(cf.planned_qty or 1),
            "entry_price": float(entry_price),
            "order_price": float(order_price),
            "limit_price": float(order_price),
            "stop_price": float(stop_price),
            "entry_mode": entry_mode,
            "stage": stage,
            "price_source": price_source,
        }

    def _validate_entry_plan(self, plan):
        return (bool(plan.get("qty") and plan.get("order_price") and plan.get("stop_price")), [])


def _candidate(code: str, *, held: bool = False, score: float = 40.0):
    return SimpleNamespace(
        code=code,
        market="KRX",
        mode=1,
        setup_ok=not held,
        planned_qty=1,
        client_order_key="",
        reasons=[],
        features={
            "data_ok": True,
            "score_final": score,
            "close": 50000.0,
            "order_price": 50100.0,
            "ma20": 49500.0,
            "ma50": 48000.0,
            "ma150": 43000.0,
            "atr_pct": 0.04,
            "entry_style_selected": "PULLBACK",
            "stop_price": 48500.0,
        },
    )


def test_buyable_backfill_skips_existing_holding_and_promotes_next_candidate():
    engine = DummyEngine()
    result = SimpleNamespace(
        orderable_candidates=[],
        backfill_attempted=False,
        backfill_added_count=0,
        planned_spent_after_backfill=0.0,
    )
    held = _candidate("207940", held=True, score=99.0)
    nxt = _candidate("005930", score=41.0)

    added = _apply_buyable_candidate_backfill(
        engine,
        result=result,
        orderable_candidates=result.orderable_candidates,
        candidates=[held, nxt],
        new_position_limit=1,
        target_new_positions=1,
        tick_budget_krw=1_000_000,
        planned_spent=0,
        available_cash_krw=1_000_000,
        min_order_krw=0,
    )

    assert added == 1
    assert [cf.code for cf in result.orderable_candidates] == ["005930"]
    assert result.backfill_attempted is True
    assert result.backfill_added_count == 1
    assert result.orderable_candidates[0].features["buyable_backfill"] is True
    assert result.orderable_candidates[0].features["entry_plan"]["qty"] == 1


def test_buyable_backfill_does_not_force_trade_when_all_candidates_blocked():
    engine = DummyEngine()
    engine._buyable_gate_context = {
        "207940": {"kis_holding_qty": 1},
        "005930": {"today_buy_exists": True},
    }
    result = SimpleNamespace(
        orderable_candidates=[],
        backfill_attempted=False,
        backfill_added_count=0,
        planned_spent_after_backfill=0.0,
    )

    added = _apply_buyable_candidate_backfill(
        engine,
        result=result,
        orderable_candidates=result.orderable_candidates,
        candidates=[_candidate("207940", score=99.0), _candidate("005930", score=41.0)],
        new_position_limit=1,
        target_new_positions=1,
        tick_budget_krw=1_000_000,
        planned_spent=0,
        available_cash_krw=1_000_000,
        min_order_krw=0,
    )

    assert added == 0
    assert result.orderable_candidates == []
    assert result.backfill_added_count == 0
