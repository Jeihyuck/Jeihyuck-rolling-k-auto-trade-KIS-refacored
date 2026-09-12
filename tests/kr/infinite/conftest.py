from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest

from trader.kr.infinite.config import InfiniteConfig
from trader.kr.infinite.models import BrokerPosition, State, Status


@pytest.fixture(autouse=True)
def stable_kr_infinite_runner_clock(monkeypatch):
    """Keep non-time-focused KR Infinite integration tests independent of wall clock.

    The production runner deliberately uses the live KST clock for opening-entry
    stabilization.  Tests that do not inject ``now_kst_value`` must therefore
    not change behaviour merely because CI happens to run before 09:10 KST.
    Dedicated opening-overlay tests call ``evaluate`` with explicit
    ``minutes_since_open`` and are unaffected by this runner-only clock patch.
    """
    import trader.kr.infinite.runner as runner

    fixed = datetime(2026, 8, 14, 10, 0, 0, tzinfo=ZoneInfo("Asia/Seoul"))

    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            if tz is None:
                return fixed.replace(tzinfo=None)
            return fixed.astimezone(tz)

    monkeypatch.setattr(runner, "datetime", FixedDateTime)


@pytest.fixture
def default_config():
    return InfiniteConfig(enabled=True)


def active(**kw):
    values = dict(
        cycle_id="KRINF-20260101-test",
        cycle_start_date=date(2026, 1, 1),
        allocated_capital_krw=4_000_000,
        unit_krw=100_000,
        core_filled_notional=500_000,
        units_used=5,
        core_units_used=5,
        last_buy_date=date(2026, 8, 1),
        last_buy_price=100,
        status=Status.ACTIVE,
    )
    values.update(kw)
    return State(**values)


def pos(price=100, avg=100, qty=100, orderable=None):
    return BrokerPosition(qty, qty if orderable is None else orderable, avg, price)
