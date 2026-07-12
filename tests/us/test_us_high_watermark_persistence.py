from datetime import datetime, timezone
from trader.us.db import repos
from trader.us.position_lifecycle_state import reconcile_us_position_lifecycles, update_us_position_high_watermark


def setup_function(): repos.reset_memory_stores()

def test_high_watermark_persists_across_ticks_days_and_resets_new_lifecycle():
    now=datetime(2026,7,10,tzinfo=timezone.utc)
    lc=reconcile_us_position_lifecycles(positions=[{"symbol":"NVDA","qty":2,"entry_price":100}], trade_date="2026-07-10", now=now, authoritative=True)["NVDA"]
    lid=lc["lifecycle_id"]
    assert update_us_position_high_watermark(symbol="NVDA", trade_date="2026-07-10", lifecycle_id=lid, current_price=105, entry_price=100, now=now)["high_watermark"] == 105
    assert update_us_position_high_watermark(symbol="NVDA", trade_date="2026-07-10", lifecycle_id=lid, current_price=103, entry_price=100, now=now)["high_watermark"] == 105
    assert update_us_position_high_watermark(symbol="NVDA", trade_date="2026-07-11", lifecycle_id=lid, current_price=104, entry_price=99, now=datetime(2026,7,11,tzinfo=timezone.utc))["high_watermark"] == 105
    assert update_us_position_high_watermark(symbol="NVDA", trade_date="2026-07-11", lifecycle_id=lid, current_price=108, entry_price=99, now=datetime(2026,7,11,tzinfo=timezone.utc))["high_watermark"] == 108
