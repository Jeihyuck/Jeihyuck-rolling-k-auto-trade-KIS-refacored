"""PR-A durable lifecycle leaf merge tests against real PostgreSQL."""
from __future__ import annotations

import os
import pytest
from sqlalchemy import text
from tests.us.test_us_postgres_integration_real import pg_engine

pytestmark = pytest.mark.skipif(
    not os.getenv("PBCORE_TEST_POSTGRES_URL"),
    reason="requires CI's real PostgreSQL service",
)


def _lifecycle(*, high=105.0, life="life-one", open=True, owner="US_STANDARD"):
    return {
        "lifecycle_id": life, "strategy_owner": owner,
        "trading_epoch_id": "atomic-epoch",
        "account_id": "practice:test-account",
        "env": "practice", "is_open": open,
        "opened_at": "2026-10-12T14:00:00+00:00",
        "high_watermark": high,
        "entry_policy": {"book": "PB1", "exit_policy": "FROZEN"},
    }


def test_atomic_lifecycle_save_cannot_erase_tp_or_reduce_watermark(pg_engine, monkeypatch):
    from trader.us.db import repos
    monkeypatch.setattr(repos, "_us_state_epoch_id", lambda *_a, **_k: "atomic-epoch")
    repos.save_us_position_risk_state(
        "AAPL", "2026-10-12", {
            "soft_stop_breach_count": 3,
            "state": {"profit_capture": {"tp1_done": True}, "trend": "TRIM"},
        }
    )
    first = repos.save_us_position_lifecycle_atomic(
        "AAPL", "2026-10-12", _lifecycle(high=125)
    )
    assert first["high_watermark"] == 125
    stale = _lifecycle(high=102)
    stale["opened_at"] = "2026-10-12T21:00:00+00:00"
    stale["entry_policy"] = {"book": "WRONG", "exit_policy": "WRONG"}
    again = repos.save_us_position_lifecycle_atomic(
        "AAPL", "2026-10-12", stale
    )
    assert float(again["high_watermark"]) == 125
    assert again["opened_at"] == first["opened_at"]
    assert again["entry_policy"] == first["entry_policy"]

    # A stale stop/risk writer must not overwrite this lifecycle, even if
    # it carries an old snapshot. All unrelated JSONB leaves must survive.
    assert repos.save_us_position_risk_state("AAPL", "2026-10-12", {
        "soft_stop_breach_count": 4,
        "state": {
            "profit_capture": {"tp1_done": True},
            "defense": {"active": True},
            "lifecycle": _lifecycle(high=99),
        },
    })
    with pg_engine.begin() as conn:
        row = conn.execute(text("""
            SELECT state,soft_stop_breach_count FROM us_position_risk_state
            WHERE trading_epoch_id='atomic-epoch'
              AND trade_date='2026-10-12' AND symbol='AAPL'
        """)).mappings().one()
    assert int(row["state"]["lifecycle"]["high_watermark"]) == 125
    assert row["state"]["lifecycle"]["entry_policy"]["book"] == "PB1"
    assert row["state"]["profit_capture"]["tp1_done"] is True
    assert row["state"]["defense"]["active"] is True
    assert row["soft_stop_breach_count"] == 4


def test_closed_lifecycle_cannot_be_reopened_or_cross_ever_written(pg_engine, monkeypatch):
    from trader.us.db import repos
    monkeypatch.setattr(repos, "_us_state_epoch_id", lambda *_a, **_k: "atomic-epoch")
    saved = repos.save_us_position_lifecycle_atomic(
        "MSFT", "2026-10-12", _lifecycle(high=202, open=True)
    )
    assert saved["is_open"] is True
    closed = repos.save_us_position_lifecycle_atomic(
        "MSFT", "2026-10-12", _lifecycle(high=208, open=False)
    )
    assert closed["is_open"] is False
    stale = repos.save_us_position_lifecycle_atomic(
        "MSFT", "2026-10-12", _lifecycle(high=201, open=True)
    )
    assert stale["is_open"] is False
    assert float(stale["high_watermark"]) == 208
    with pytest.raises(RuntimeError, match="US_LIFECYCLE_IDENTITY_CONFLICT"):
        repos.save_us_position_lifecycle_atomic(
            "MSFT", "2026-10-12", _lifecycle(life="other-lifecycle")
        )
    with pytest.raises(RuntimeError, match="US_LIFECYCLE_EPOCH_MISMATCH"):
        repos.save_us_position_lifecycle_atomic(
            "MSFT", "2026-10-12",
            {**_lifecycle(), "trading_epoch_id": "foreign-epoch"},
        )
