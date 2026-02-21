from __future__ import annotations

import pytest
import sqlalchemy as sa

from trader.db.schema import schema_for_engine
from trader import minervini_v2_runner


def test_runner_raises_when_final30_missing(monkeypatch) -> None:
    engine = sa.create_engine("sqlite:///:memory:")
    schema = schema_for_engine(engine)
    schema.metadata.create_all(engine)

    monkeypatch.setenv("STRATEGY_ENV", "practice")
    monkeypatch.setenv("AS_OF_OVERRIDE", "2026-02-20")
    monkeypatch.setattr(minervini_v2_runner, "get_engine", lambda: engine)

    with pytest.raises(ValueError, match="final30 watchlist is missing/empty"):
        minervini_v2_runner.main()
