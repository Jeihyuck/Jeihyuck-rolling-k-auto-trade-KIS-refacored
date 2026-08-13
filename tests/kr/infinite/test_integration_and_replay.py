from datetime import datetime, timedelta, timezone

from trader.kr.forbidden_products import block_buy_if_forbidden
from trader.kr.infinite.integration import exclude_owned, global_order_gate
from trader.kr.infinite.replay import ReplayCosts, replay


def test_ownership_filter_only_removes_122630():
    rows = [{"code": "005930"}, {"symbol": "122630"}, {"code": "000660"}]
    assert exclude_owned(rows) == [rows[0], rows[2]]


def test_common_router_ownership_contract_allows_only_valid_sleeve_buy():
    valid = {"side": "BUY", "symbol": "122630",
             "strategy_id": "KR_KODEX_LEVERAGE_INFINITE_V1", "book": "KR_INFINITE_BOOK"}
    assert block_buy_if_forbidden(valid).get("status") != "BLOCKED"
    assert block_buy_if_forbidden({**valid, "strategy_id": "PB1"})["status"] == "BLOCKED"
    assert block_buy_if_forbidden({"side": "BUY", "symbol": "122630"})["status"] == "BLOCKED"
    assert block_buy_if_forbidden({"side": "SELL", "symbol": "122630"}).get("status") != "BLOCKED"
    normal = {"side": "BUY", "symbol": "005930", "strategy_id": "PB1"}
    assert block_buy_if_forbidden(normal) == normal


def test_global_live_gate_never_bypassed():
    live = {"STRATEGY_MODE": "LIVE", "KIS_ENV": "real", "LIVE_TRADING_ENABLED": "1",
            "DRY_RUN": "0", "DISABLE_LIVE_TRADING": "0", "FORCE_BLOCK_LIVE": "0"}
    assert global_order_gate(live)[0]
    for key, value in (("DRY_RUN", "1"), ("DISABLE_LIVE_TRADING", "1"),
                       ("FORCE_BLOCK_LIVE", "1"), ("LIVE_TRADING_ENABLED", "0")):
        blocked = dict(live); blocked[key] = value
        assert not global_order_gate(blocked)[0]


def test_practice_is_an_ordering_mode_not_a_live_account_failure():
    env = {"STRATEGY_MODE": "LIVE", "KIS_ENV": "practice", "LIVE_TRADING_ENABLED": "1",
           "DRY_RUN": "0", "DISABLE_LIVE_TRADING": "0", "FORCE_BLOCK_LIVE": "0"}
    assert global_order_gate(env) == (True, "", "PRACTICE")


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
