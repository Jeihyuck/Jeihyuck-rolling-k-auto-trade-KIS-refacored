import json
import sys
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import numpy as np
import pandas as pd
import sqlalchemy as sa

sys.path.append(str(Path(__file__).resolve().parents[1]))

from trader.db.json_safe import json_sanitize
from trader.db.migrate import run_migrations
from trader.db.repos import OrdersRepo
from trader.db.schema import ORDERS


def test_json_sanitize_handles_complex_types() -> None:
    payload = {
        "ts": pd.Timestamp("2023-01-01"),
        "dt": datetime(2023, 1, 1, 12, 30, 45),
        "d": date(2023, 1, 1),
        "dec": Decimal("12.34"),
        "np_int": np.int64(7),
        "np_float": np.float64(3.14),
        "bad": float("nan"),
    }
    sanitized = json_sanitize(payload)
    assert isinstance(sanitized["ts"], str)
    assert isinstance(sanitized["dt"], str)
    assert isinstance(sanitized["d"], str)
    assert sanitized["dec"] == 12.34
    assert sanitized["np_int"] == 7
    assert sanitized["np_float"] == 3.14
    assert sanitized["bad"] is None
    json.dumps(sanitized)


def test_create_intent_sanitizes_request_json(tmp_path) -> None:
    db_url = f"sqlite:///{tmp_path}/test.db"
    engine = sa.create_engine(db_url)
    run_migrations(engine)

    repo = OrdersRepo(engine)
    order_id, created = repo.create_intent_idempotent(
        env="practice",
        run_id=None,
        strategy="pb1_pullback_close",
        sid=1,
        mode=1,
        code="005930",
        market="KOSPI",
        side="BUY",
        ord_type="LIMIT",
        qty=1,
        limit_price=100.0,
        stage="PB1-CLOSE",
        client_order_key="practice:pb1:test",
        request_json={"ts": pd.Timestamp("2023-01-01")},
    )
    assert created is True

    with engine.begin() as conn:
        count = conn.execute(sa.select(sa.func.count()).select_from(ORDERS)).scalar()
    assert count == 1
    assert order_id
