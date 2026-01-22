import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from trader.reconcile_db import evaluate_stale_db_guard


def test_kis_holdings_empty_requires_consecutive_ticks(tmp_path):
    now = datetime.utcnow()
    allow, reason, guard = evaluate_stale_db_guard(
        bot_state_dir=tmp_path,
        tick_ts=now,
        kis_holdings_empty=True,
        orders_count=0,
        fills_count=0,
        had_kis_error=False,
    )
    assert allow is False
    assert reason == "empty_streak_insufficient"
    assert guard["empty_streak"] == 1

    allow, reason, guard = evaluate_stale_db_guard(
        bot_state_dir=tmp_path,
        tick_ts=now + timedelta(seconds=30),
        kis_holdings_empty=True,
        orders_count=0,
        fills_count=0,
        had_kis_error=False,
    )
    assert allow is True
    assert reason == "empty_streak_confirmed"
    assert guard["empty_streak"] >= 2


def test_kis_error_blocks_purge(tmp_path):
    now = datetime.utcnow()
    allow, reason, guard = evaluate_stale_db_guard(
        bot_state_dir=tmp_path,
        tick_ts=now,
        kis_holdings_empty=True,
        orders_count=0,
        fills_count=0,
        had_kis_error=True,
    )
    assert allow is False
    assert reason == "kis_error"
    assert guard["last_kis_error_ts"]
