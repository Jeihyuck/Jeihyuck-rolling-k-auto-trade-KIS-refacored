from datetime import datetime, timedelta, timezone

from trader.kr.infinite.integration import exclude_owned, global_order_gate
from trader.kr.infinite.replay import ReplayCosts, replay


def test_ownership_filter_only_removes_122630():
    rows = [{"code": "005930"}, {"symbol": "122630"}, {"code": "000660"}]
    assert exclude_owned(rows) == [rows[0], rows[2]]


def test_global_live_gate_never_bypassed():
    live = {"STRATEGY_MODE": "LIVE", "KIS_ENV": "real", "LIVE_TRADING_ENABLED": "1",
            "DRY_RUN": "0", "DISABLE_LIVE_TRADING": "0", "FORCE_BLOCK_LIVE": "0"}
    assert global_order_gate(live)[0]
    for key, value in (("DRY_RUN", "1"), ("DISABLE_LIVE_TRADING", "1"),
                       ("FORCE_BLOCK_LIVE", "1"), ("LIVE_TRADING_ENABLED", "0")):
        blocked = dict(live); blocked[key] = value
        assert not global_order_gate(blocked)[0]


def test_replay_is_next_day_and_cost_aware():
    start = datetime(2020, 1, 1)
    rows = []
    for i in range(180):
        px = 10_000 + i * 20
        rows.append({"date": (start + timedelta(days=i)).date().isoformat(), "open": px, "close": px + 10})
    free = replay(rows, costs=ReplayCosts(0, 0, 0, 0))
    costly = replay(rows)
    assert costly["costs_krw"] > 0
    assert costly["total_return"] <= free["total_return"]
    assert costly["max_used_units"] <= 40 and costly["max_invested_notional"] <= 15_000_000
