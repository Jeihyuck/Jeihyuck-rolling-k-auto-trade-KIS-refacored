from trader.kr.pb1.sell_fence import NoSellableStickyFence
from trader.kr.pb1_stability import NO_SELLABLE_STICKY


def test_sticky_holds_for_same_snapshot_and_releases_on_new_sellable_snapshot():
    fence = NoSellableStickyFence()
    fence.mark(trade_date="2026-08-21", symbol="000240", snapshot_version="v1")
    assert fence.blocked(trade_date="2026-08-21", symbol="000240", snapshot_version="v1", orderable_qty=0)
    assert not fence.blocked(trade_date="2026-08-21", symbol="000240", snapshot_version="v2", orderable_qty=3)


def test_module_reexport_matches_facade_instance_type():
    assert isinstance(NO_SELLABLE_STICKY, NoSellableStickyFence)
