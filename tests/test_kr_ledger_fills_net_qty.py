"""
한국장 ledger/fills net qty 테스트
"""
import pytest
import os
import tempfile
from pathlib import Path
from datetime import datetime

from trader.ledger.store import LedgerStore
from trader.db.repos import FillsRepo
from trader.db.schema import PBCoreSchema


def test_ledger_rebuild_positions_removes_net_zero():
    """ledger rebuild_positions_average_cost는 net qty 0 종목을 제거"""
    with tempfile.TemporaryDirectory() as tmpdir:
        store = LedgerStore(base_dir=Path(tmpdir))
        
        # BUY 58주
        store.append_fill({
            "code": "028050",
            "sid": 1,
            "mode": 1,
            "side": "BUY",
            "qty": 58,
            "price": 52200,
            "ts": "2026-05-01T09:05:00+09:00",
            "market": "KOSPI",
        })
        
        # SELL 58주 (전량 매도)
        store.append_fill({
            "code": "028050",
            "sid": 1,
            "mode": 1,
            "side": "SELL",
            "qty": 58,
            "price": 53000,
            "ts": "2026-05-13T10:00:00+09:00",
            "market": "KOSPI",
        })
        
        positions = store.rebuild_positions_average_cost(lookback_days=30)
        
        # net qty 0이므로 positions에 없어야 함
        assert ("028050", 1, 1) not in positions


def test_ledger_rebuild_positions_partial_sell():
    """ledger rebuild_positions_average_cost는 부분 매도만 반영"""
    with tempfile.TemporaryDirectory() as tmpdir:
        store = LedgerStore(base_dir=Path(tmpdir))
        
        # BUY 58주
        store.append_fill({
            "code": "028050",
            "sid": 1,
            "mode": 1,
            "side": "BUY",
            "qty": 58,
            "price": 52200,
            "ts": "2026-05-01T09:05:00+09:00",
            "market": "KOSPI",
        })
        
        # SELL 40주 (부분 매도)
        store.append_fill({
            "code": "028050",
            "sid": 1,
            "mode": 1,
            "side": "SELL",
            "qty": 40,
            "price": 53000,
            "ts": "2026-05-13T10:00:00+09:00",
            "market": "KOSPI",
        })
        
        positions = store.rebuild_positions_average_cost(lookback_days=30)
        
        # net qty 18주가 남아야 함
        key = ("028050", 1, 1)
        assert key in positions
        assert positions[key]["total_qty"] == 18


def test_ledger_rebuild_positions_sell_without_buy():
    """ledger rebuild_positions_average_cost는 SELL without BUY를 방어"""
    with tempfile.TemporaryDirectory() as tmpdir:
        store = LedgerStore(base_dir=Path(tmpdir))
        
        # BUY 없이 SELL만 있음
        store.append_fill({
            "code": "028050",
            "sid": 1,
            "mode": 1,
            "side": "SELL",
            "qty": 58,
            "price": 53000,
            "ts": "2026-05-13T10:00:00+09:00",
            "market": "KOSPI",
        })
        
        positions = store.rebuild_positions_average_cost(lookback_days=30)
        
        # SELL without BUY는 무시되므로 positions에 없어야 함
        assert ("028050", 1, 1) not in positions


def test_fills_net_positions_from_fills_kr_only(test_db_engine):
    """FillsRepo.list_net_positions_from_fills는 한국장 6자리만 반환"""
    from sqlalchemy import MetaData
    
    schema = PBCoreSchema.for_engine(test_db_engine)
    metadata = MetaData()
    metadata.reflect(bind=test_db_engine)
    
    repo = FillsRepo(test_db_engine)
    
    # 한국장 종목 BUY
    with test_db_engine.begin() as conn:
        conn.execute(
            schema.fills.insert().values(
                env="practice",
                strategy="test",
                code="028050",
                side="BUY",
                qty=58,
                price=52200,
                filled_at=datetime(2026, 5, 1, 9, 5, 0),
            )
        )
        
        # 한국장 종목 전량 매도
        conn.execute(
            schema.fills.insert().values(
                env="practice",
                strategy="test",
                code="028050",
                side="SELL",
                qty=58,
                price=53000,
                filled_at=datetime(2026, 5, 13, 10, 0, 0),
            )
        )
        
        # 한국장 종목 부분 보유
        conn.execute(
            schema.fills.insert().values(
                env="practice",
                strategy="test",
                code="066970",
                side="BUY",
                qty=100,
                price=50000,
                filled_at=datetime(2026, 5, 1, 9, 5, 0),
            )
        )
        conn.execute(
            schema.fills.insert().values(
                env="practice",
                strategy="test",
                code="066970",
                side="SELL",
                qty=40,
                price=51000,
                filled_at=datetime(2026, 5, 13, 10, 0, 0),
            )
        )
    
    result = repo.list_net_positions_from_fills("practice", kr_only=True)
    
    # 028050은 net qty 0이므로 없어야 함
    assert "028050" not in [r["code"] for r in result]
    
    # 066970은 net qty 60이어야 함
    row_066970 = next((r for r in result if r["code"] == "066970"), None)
    assert row_066970 is not None
    assert row_066970["qty"] == 60


@pytest.fixture
def test_db_engine():
    """테스트용 in-memory SQLite DB"""
    import sqlalchemy as sa
    from trader.db.schema import PBCoreSchema
    
    engine = sa.create_engine("sqlite:///:memory:")
    schema = PBCoreSchema.for_engine(engine)
    
    # 테이블 생성
    with engine.begin() as conn:
        schema.metadata.create_all(conn)
    
    yield engine
    
    engine.dispose()
