"""PR354: a native fill write may share a transaction with position accounting."""
from datetime import datetime, timezone

import pytest
import sqlalchemy as sa

from trader.db.repos import FillsRepo
from tests.kr.execution_claim_fixtures import create_schema_with_active_test_epoch


def test_fill_upsert_rolls_back_with_enclosing_position_failure():
    engine = sa.create_engine("sqlite:///:memory:")
    schema = create_schema_with_active_test_epoch(engine)
    fills = FillsRepo(engine)
    kwargs = {
        "env": "practice", "run_id": None, "order_id": None,
        "kis_odno": "0000007777", "trade_id": "KR_354_NATIVE_FILL:7777",
        "code": "035760", "market": "KOSDAQ", "side": "BUY",
        "qty": 2, "price": 2500.0, "fee": 0.0, "tax": 0.0,
        "filled_at": datetime(2026, 11, 13, 1, 0, tzinfo=timezone.utc),
        "raw_json": {"kis_response": {"tot_ccld_qty": "2", "tot_ccld_amt": "5000"}},
    }
    with pytest.raises(RuntimeError, match="INJECT_POSITION_FAILURE"):
        with engine.begin() as conn:
            fills.upsert_fill(**kwargs, _conn=conn)
            # Simulates position persistence failing AFTER a native fill UPSERT.
            raise RuntimeError("INJECT_POSITION_FAILURE")
    with engine.connect() as conn:
        assert conn.execute(sa.select(sa.func.count()).select_from(schema.fills)).scalar_one() == 0


def test_fill_upsert_commits_when_enclosing_transaction_commits():
    engine = sa.create_engine("sqlite:///:memory:")
    schema = create_schema_with_active_test_epoch(engine)
    fills = FillsRepo(engine)
    kwargs = {
        "env": "practice", "run_id": None, "order_id": None,
        "kis_odno": "0000007778", "trade_id": "KR_354_NATIVE_FILL:7778",
        "code": "035760", "market": "KOSDAQ", "side": "BUY",
        "qty": 2, "price": 2500.0, "fee": 0.0, "tax": 0.0,
        "filled_at": datetime(2026, 11, 13, 1, 0, tzinfo=timezone.utc),
        "raw_json": {"kis_response": {"tot_ccld_qty": "2", "tot_ccld_amt": "5000"}},
    }
    with engine.begin() as conn:
        fills.upsert_fill(**kwargs, _conn=conn)
    with engine.connect() as conn:
        assert conn.execute(sa.select(sa.func.count()).select_from(schema.fills)).scalar_one() == 1
