import os
from pathlib import Path

import pytest

from trader.db.engine import make_engine
from trader.db.migrate import run_migrations
from trader.db.repos import LedgerEventsRepo, PositionsRepo
from trader.pb1_runner import _handle_missing_positions_reset
from trader.time_utils import now_kst


def test_practice_reset_closes_positions_and_writes_event(tmp_path, monkeypatch):
    db_url = os.getenv("PBCORE_DB_URL")
    if not db_url:
        pytest.skip("PBCORE_DB_URL not set for Postgres-backed test")
    engine = make_engine()
    run_migrations(engine)

    positions_repo = PositionsRepo(engine)
    ledger_repo = LedgerEventsRepo(engine)

    positions_repo.apply_fill(
        env="practice",
        strategy="pb1_pullback_close",
        sid=1,
        mode=1,
        code="000001",
        market="KOSPI",
        side="BUY",
        qty=5,
        price=1000.0,
        fee=0.0,
        tax=0.0,
        filled_at=now_kst(),
    )
    positions_repo.apply_fill(
        env="practice",
        strategy="pb1_pullback_close",
        sid=1,
        mode=1,
        code="000002",
        market="KOSDAQ",
        side="BUY",
        qty=7,
        price=2000.0,
        fee=0.0,
        tax=0.0,
        filled_at=now_kst(),
    )

    bot_state_dir = Path(tmp_path) / "bot_state"
    bot_state_dir.mkdir(parents=True, exist_ok=True)

    handled = _handle_missing_positions_reset(
        balance_snapshot={"output1": [], "output2": []},
        env="practice",
        strategy="pb1_pullback_close",
        run_id="run-1",
        positions_repo=positions_repo,
        ledger_repo=ledger_repo,
        bot_state_dir=bot_state_dir,
    )

    assert handled is True
    positions = positions_repo.list_positions("practice", "pb1_pullback_close")
    assert all(int(p.get("qty") or 0) == 0 for p in positions)

    event_path = bot_state_dir / "runtime" / "events" / f"account_reset_{now_kst().date().isoformat()}.jsonl"
    assert event_path.exists()
