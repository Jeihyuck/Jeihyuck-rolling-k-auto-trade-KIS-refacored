from datetime import date
import pytest
from trader.kr.infinite.config import InfiniteConfig
from trader.kr.infinite.models import BrokerPosition,State,Status
@pytest.fixture
def default_config(): return InfiniteConfig(enabled=True,live=True)
def active(**kw):
 values=dict(cycle_id="KRINF-20260101-test",cycle_start_date=date(2026,1,1),allocated_capital_krw=4_000_000,unit_krw=100_000,core_filled_notional=500_000,units_used=5,core_units_used=5,last_buy_date=date(2026,8,1),last_buy_price=100,status=Status.ACTIVE);values.update(kw);return State(**values)
def pos(price=100,avg=100,qty=100,orderable=None):return BrokerPosition(qty,qty if orderable is None else orderable,avg,price)
