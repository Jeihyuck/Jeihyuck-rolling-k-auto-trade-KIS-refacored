from trader.kr.pb1.reconcile_utils import extract_cooldown_source_details, fill_reconcile_warn_needed
from trader.pb1_engine import _extract_cooldown_source_details, _fill_reconcile_warn_needed


def test_reconcile_helpers_match_wrappers() -> None:
    rows = [
        {"event_type": "EXIT_DONE", "side": "SELL", "payload_json": {"exit_reason": "EXIT_RISK_OFF"}},
        {"event_type": "EXIT_DONE", "side": "SELL", "payload_json": {"exit_reason": "SOME_REASON"}},
    ]
    assert extract_cooldown_source_details(rows) == _extract_cooldown_source_details(rows)
    assert extract_cooldown_source_details([]) == _extract_cooldown_source_details([])

    assert fill_reconcile_warn_needed(100.0, 101.5) == _fill_reconcile_warn_needed(100.0, 101.5)
    assert fill_reconcile_warn_needed(100.0, 100.5) == _fill_reconcile_warn_needed(100.0, 100.5)
    assert fill_reconcile_warn_needed(0.0, 100.0) == _fill_reconcile_warn_needed(0.0, 100.0)
