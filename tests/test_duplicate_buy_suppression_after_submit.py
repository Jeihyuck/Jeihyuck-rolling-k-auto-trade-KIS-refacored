from trader.pb1_engine import PB1Engine


def test_submitted_or_filled_buy_is_blocked_by_unified_gate():
    engine = object.__new__(PB1Engine)
    engine.phase_name = "am"
    engine.window_internal = "morning"
    engine._today = "2026-07-20"
    engine._display_code = lambda code: code
    decision = engine._evaluate_unified_buyable_gate(
        code="090430", allow_add_to_existing=False,
        gate_context={"today_submit_exists": True, "today_fill_exists": True},
    )
    assert not decision.ok
    assert "BUYABLE_TODAY_SUBMIT" in decision.reason_codes
    assert "BUYABLE_TODAY_FILL" in decision.reason_codes


def test_submitted_code_is_excluded_before_order_candidate_list_is_built():
    """Regression for 090430 repeatedly reaching FINAL_SKIP on later ticks."""
    engine = object.__new__(PB1Engine)
    engine.phase_name = "am"
    engine.window_internal = "morning"
    engine._today = "2026-07-20"
    engine._display_code = lambda code: code
    candidates = ["090430", "005930"]
    contexts = {"090430": {"today_submit_exists": True}, "005930": {}}
    order_candidates = [
        code for code in candidates
        if engine._evaluate_unified_buyable_gate(code=code, gate_context=contexts[code], allow_add_to_existing=False).ok
    ]
    assert order_candidates == ["005930"]
    assert "090430" not in order_candidates
