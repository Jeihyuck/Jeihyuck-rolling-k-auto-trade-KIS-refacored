from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from trader.kr.exit_guards import same_day_soft_exit_block


def test_same_day_trailing_exit_is_blocked_before_minimum(monkeypatch):
    monkeypatch.setenv("KR_SWING_MIN_HOLD_MINUTES", "240")
    now = datetime(2026, 8, 24, 13, 0, tzinfo=ZoneInfo("Asia/Seoul"))
    assert same_day_soft_exit_block(now_kst=now, bought_at=now - timedelta(minutes=120), exit_reason="TRAIL_STOP_HIT")[0]


def test_same_day_hard_stop_is_never_blocked():
    now = datetime(2026, 8, 24, 10, 0, tzinfo=ZoneInfo("Asia/Seoul"))
    assert not same_day_soft_exit_block(now_kst=now, bought_at=now - timedelta(minutes=5), exit_reason="STOP_HIT_EFFECTIVE")[0]
