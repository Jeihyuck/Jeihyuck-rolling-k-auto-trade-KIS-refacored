from trader.kr.pb1.exit_planning import (
    resolve_core_trend_follow_exit,
    resolve_day_protect_exit,
    resolve_exit_policy_wrapper,
    resolve_swing_staged_exit,
)
from trader.pb1_engine import (
    _resolve_core_trend_follow_exit,
    _resolve_day_protect_exit,
    _resolve_exit_policy,
    _resolve_swing_staged_exit,
)


def test_exit_planning_wrappers_match_module():
    pos_day = {"qty": 10, "orderable_qty": 10, "position_meta": {}}
    assert _resolve_day_protect_exit(pos_day, 101.0, 1520, ret_pct=4.5, max_pnl_pct=5.0, stop_hit=False) == resolve_day_protect_exit(pos_day, 101.0, 1520, ret_pct=4.5, max_pnl_pct=5.0, stop_hit=False)

    pos_swing = {
        "code": "000001",
        "qty": 10,
        "orderable_qty": 10,
        "avg_buy_price": 100.0,
        "position_meta": {"initial_stop_price": 95.0},
    }
    assert _resolve_swing_staged_exit(pos_swing, 110.0, 105.0, ret_pct=10.0, days_held=2, stop_hit=False) == resolve_swing_staged_exit(pos_swing, 110.0, 105.0, ret_pct=10.0, days_held=2, stop_hit=False)

    pos_core = {
        "qty": 10,
        "orderable_qty": 10,
        "avg_buy_price": 100.0,
        "position_meta": {"initial_stop_price": 95.0},
    }
    assert _resolve_core_trend_follow_exit(pos_core, 92.0, 90.0, 89.0, ret_pct=-9.0, days_held=3, regime="BEAR") == resolve_core_trend_follow_exit(pos_core, 92.0, 90.0, 89.0, ret_pct=-9.0, days_held=3, regime="BEAR")

    assert _resolve_exit_policy(days_held=2, holding_bars=2, stop_hit=True, trail_stop_price=None, mark=100.0, ma20=99.0, ma50=98.0, time_stop_hit=False, risk_off_signal=False) == resolve_exit_policy_wrapper(days_held=2, holding_bars=2, stop_hit=True, trail_stop_price=None, mark=100.0, ma20=99.0, ma50=98.0, time_stop_hit=False, risk_off_signal=False)
