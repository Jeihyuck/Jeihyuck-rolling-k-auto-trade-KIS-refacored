from __future__ import annotations

from datetime import datetime

import sqlalchemy as sa

from trader.db.repos import LedgerEventsRepo
from trader.db.schema import schema_for_engine


def test_prep_done_status_uses_payload_as_of() -> None:
    engine = sa.create_engine("sqlite:///:memory:")
    schema = schema_for_engine(engine)
    schema.metadata.create_all(engine)

    repo = LedgerEventsRepo(engine)
    repo.append_event(
        env="practice",
        run_id=None,
        strategy=None,
        run_window="prep",
        event_type="PREP_DONE",
        ts=datetime(2026, 2, 24, 9, 0, 0),
        payload_json={"as_of": "2026-02-23"},
    )

    ok, count = repo.prep_done_status(env="practice", as_of="2026-02-23")
    assert ok is True
    assert count == 1

    ok_other, count_other = repo.prep_done_status(env="practice", as_of="2026-02-22")
    assert ok_other is False
    assert count_other == 0
