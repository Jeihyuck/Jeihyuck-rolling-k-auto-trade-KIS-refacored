from datetime import datetime, timezone
from trader.us.db import repos
from trader.us.position_lifecycle_state import reconcile_us_position_lifecycles


def setup_function(): repos.reset_memory_stores()

def test_lifecycle_carries_forward_and_closes_only_authoritative():
    now=datetime(2026,7,10,tzinfo=timezone.utc)
    pos=[{"symbol":"AMD","qty":10,"entry_price":100,"current_price_usd":105}]
    first=reconcile_us_position_lifecycles(positions=pos, trade_date="2026-07-10", now=now, authoritative=True)["AMD"]
    lid=first["lifecycle_id"]
    pos2=[{"symbol":"AMD","qty":12,"entry_price":101,"current_price_usd":104}]
    second=reconcile_us_position_lifecycles(positions=pos2, trade_date="2026-07-11", now=datetime(2026,7,11,tzinfo=timezone.utc), authoritative=False)["AMD"]
    assert second["lifecycle_id"] == lid
    assert second["holding_trade_days"] == 2
    assert reconcile_us_position_lifecycles(positions=[], trade_date="2026-07-12", now=datetime(2026,7,12,tzinfo=timezone.utc), authoritative=False) == {}
    assert repos.load_latest_us_position_risk_state("AMD","2026-07-12")["state"]["lifecycle"]["is_open"] is True
    closed=reconcile_us_position_lifecycles(positions=[], trade_date="2026-07-12", now=datetime(2026,7,12,tzinfo=timezone.utc), authoritative=True)["AMD"]
    assert closed["is_open"] is False
    new=reconcile_us_position_lifecycles(positions=[{"symbol":"AMD","qty":1,"entry_price":90}], trade_date="2026-07-13", now=datetime(2026,7,13,tzinfo=timezone.utc), authoritative=True)["AMD"]
    assert new["lifecycle_id"] != lid
