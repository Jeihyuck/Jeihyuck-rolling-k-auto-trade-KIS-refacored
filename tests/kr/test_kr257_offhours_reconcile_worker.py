"""KR257 independent historical worker is practice-only and off-hours only."""
from datetime import datetime
from zoneinfo import ZoneInfo
from scripts.kr257_offhours_reconcile_worker import offhours_allowed, worker_permitted

KST = ZoneInfo("Asia/Seoul")


def test_worker_blocks_every_trading_session_and_prep():
    for hour, minute in [(5, 30), (6, 30), (8, 55), (9, 5), (15, 30), (16, 29)]:
        assert not offhours_allowed(datetime(2026, 10, 26, hour, minute, tzinfo=KST))
    assert offhours_allowed(datetime(2026, 10, 26, 17, 0, tzinfo=KST))
    assert offhours_allowed(datetime(2026, 10, 25, 13, 0, tzinfo=KST))


def test_worker_requires_explicit_approved_scope():
    kw=dict(env="practice", strategy="pb1_pullback_close",
            endpoint="https://openapivts.koreainvestment.com:29443",
            explicit=True, approved=True,
            now=datetime(2026, 10, 26, 17, 0, tzinfo=KST))
    assert worker_permitted(**kw)[0] is True
    for change in (
        {"env": "real"},
        {"strategy": "KR_INFINITE"},
        {"endpoint": "https://openapi.koreainvestment.com:9443"},
        {"explicit": False},
        {"approved": False},
        {"now": datetime(2026, 10, 26, 9, 0, tzinfo=KST)},
    ):
        assert not worker_permitted(**{**kw, **change})[0]


def test_worker_does_not_spawn_trade_order_threads_or_change_windows_scheduler():
    import inspect
    import scripts.kr257_offhours_reconcile_worker as worker
    source=inspect.getsource(worker)
    assert "pg_try_advisory_lock" in source
    assert "reconcile_kis(" in source
    assert "KR257_OFFHOURS_RECONCILE_APPROVED" in source
    assert "buy_stock" not in source and "sell_stock" not in source
