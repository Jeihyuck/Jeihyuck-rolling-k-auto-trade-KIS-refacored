from __future__ import annotations

from datetime import date

from trader.pb1_engine import (
    _calendar_days_held,
    _calculate_exit_qty,
    _classify_trade_horizon,
    _horizon_to_exit_family,
    _resolve_position_book,
    _resolve_position_horizon,
)
from trader.kr.pb1.horizon_utils import (
    calendar_days_held,
    calculate_exit_qty,
    classify_trade_horizon,
    horizon_to_exit_family,
    resolve_position_book,
    resolve_position_horizon,
)


def test_horizon_helpers_match_wrappers() -> None:
    trade_date = date(2026, 9, 6)
    assert calendar_days_held("2026-09-03", trade_date) == _calendar_days_held("2026-09-03", trade_date)
    assert calendar_days_held(None, trade_date) == _calendar_days_held(None, trade_date)

    cases = [
        {"entry_style_selected": "ENTRY_BREAKOUT"},
        {"entry_style_selected": "ENTRY_PULLBACK"},
        {"trend_template_ok": True, "vcp_score": 50, "score_final": 70},
        {"score_final": 90, "trend_template_ok": True, "atr_pct": 3.0},
        {},
    ]
    for features in cases:
        assert classify_trade_horizon(features) == _classify_trade_horizon(features)

    for horizon in ["DAY_PROTECT", "SWING_CARRY", "CORE_CARRY", "UNKNOWN"]:
        assert horizon_to_exit_family(horizon) == _horizon_to_exit_family(horizon)

    positions = [
        {"position_meta": {"trade_horizon": "CORE_CARRY"}},
        {"entry_meta_json": {"trade_horizon": "DAY_PROTECT"}},
        {"entry_date": trade_date.isoformat()},
        {},
        {"position_meta": "bad_string"},
    ]
    for pos in positions:
        assert resolve_position_horizon(pos, now_kst_date=trade_date) == _resolve_position_horizon(pos, now_kst_date=trade_date)
        assert resolve_position_book(pos) == _resolve_position_book(pos)

    qty_cases = [
        (10, 8, None),
        (10, 10, 0.5),
        (10, 9, 0.33),
        (1, 1, 0.5),
        (5, 0, 0.5),
        (100, 5, 1.0),
    ]
    for holding_qty, orderable_qty, sell_pct in qty_cases:
        assert calculate_exit_qty(holding_qty, orderable_qty, sell_pct) == _calculate_exit_qty(holding_qty, orderable_qty, sell_pct)
