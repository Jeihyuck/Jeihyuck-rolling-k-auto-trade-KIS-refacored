from datetime import datetime, timezone
from trader.us.db import repos
from trader.us.position_trend_state import update_us_position_trend_state, filter_add_to_existing_by_trend_state


def setup_function(): repos.reset_memory_stores()

def test_daily_streak_once_and_stale_final30_no_absent_increment():
    now=datetime(2026,7,10,tzinfo=timezone.utc)
    f={"trade_date":"2026-07-10","available":True,"score_contract_ok":True,"in_final30_today":False}
    d={"ma20":100,"ma50":90,"rs_20d":.01}
    for _ in range(10):
        st=update_us_position_trend_state(symbol="AMD", trade_date="2026-07-10", now=now, current_price=99, holding_trade_days=1, final30=f, daily=d)
    assert st["final30_absent_streak"] == 1
    assert st["below_ma20_streak"] == 1
    stale={**f,"trade_date":"2026-07-09"}
    st2=update_us_position_trend_state(symbol="AMD", trade_date="2026-07-11", now=datetime(2026,7,11,tzinfo=timezone.utc), current_price=101, holding_trade_days=2, final30=stale, daily=d)
    assert st2["trend_data_quality"] == "missing_or_stale"
    assert st2["final30_absent_streak"] == 1

def test_trend_filter_blocks_only_existing_non_healthy():
    kept, blocked = filter_add_to_existing_by_trend_state([{"symbol":"AMD","side":"BUY"},{"symbol":"NEW","side":"BUY"}], [{"symbol":"AMD","qty":1,"trend_state":"WARNING"}])
    assert [b["symbol"] for b in blocked] == ["AMD"]
    assert [k["symbol"] for k in kept] == ["NEW"]
