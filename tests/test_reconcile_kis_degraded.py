import os
import sys
from pathlib import Path

import sqlalchemy as sa
import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from trader.db.schema import METADATA
from trader.kis_wrapper import KisTemporaryError
from trader.reconcile_kis import reconcile_today


def test_reconcile_today_returns_ok_on_temp_error(tmp_path):
    db_url = os.getenv("PBCORE_DB_URL")
    if not db_url:
        pytest.skip("PBCORE_DB_URL not set for Postgres-backed test")
    engine = sa.create_engine(db_url)
    METADATA.create_all(engine)

    class DummyKis:
        def inquire_daily_ccld(self, **_kwargs):
            raise KisTemporaryError("HTTP 500")

    result = reconcile_today(
        engine=engine,
        kis=DummyKis(),
        env="practice",
        run_id=None,
        strategy="pb1_pullback_close",
    )

    assert result["ok"] is True
    assert result["orders"] == 0
    assert result["fills"] == 0
    assert result["degraded"] == "temporary"
