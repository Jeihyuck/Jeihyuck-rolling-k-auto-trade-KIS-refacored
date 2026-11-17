"""US v3 hot-path risk-state schema ensure caching.

No remote SQL or broker calls: the fake connection counts actual DDL requests.
"""
from __future__ import annotations

import pytest

from trader.us.db import repos


class SchemaConnection:
    def __init__(self, *, fail_first=False):
        self.info = {}
        self.ddls = 0
        self.fail_first = fail_first

    def execute(self, stmt):
        assert "CREATE TABLE IF NOT EXISTS us_position_risk_state" in str(stmt)
        self.ddls += 1
        if self.fail_first:
            self.fail_first = False
            raise RuntimeError("schema unavailable")


def test_per_connection_schema_ddl_is_not_repeated_in_live_tick(monkeypatch):
    monkeypatch.delenv("PBCORE_TEST_POSTGRES_URL", raising=False)
    monkeypatch.delenv("US_RISK_STATE_FORCE_SCHEMA_DDL", raising=False)
    a = SchemaConnection()
    b = SchemaConnection()
    for _ in range(25):
        repos._ensure_us_position_risk_state_table(a)
    assert a.ddls == 1
    assert a.info["us_position_risk_state_schema_ready_v3"] is True
    repos._ensure_us_position_risk_state_table(b)
    assert b.ddls == 1  # never shared across independent physical connections


def test_schema_failure_is_not_cached_and_retries_durable_check(monkeypatch):
    monkeypatch.delenv("PBCORE_TEST_POSTGRES_URL", raising=False)
    monkeypatch.delenv("US_RISK_STATE_FORCE_SCHEMA_DDL", raising=False)
    connection = SchemaConnection(fail_first=True)
    with pytest.raises(RuntimeError, match="schema unavailable"):
        repos._ensure_us_position_risk_state_table(connection)
    assert not connection.info.get("us_position_risk_state_schema_ready_v3")
    repos._ensure_us_position_risk_state_table(connection)
    assert connection.ddls == 2


@pytest.mark.parametrize("setting,value", [
    ("PBCORE_TEST_POSTGRES_URL", "postgresql://localhost/us_ci"),
    ("US_RISK_STATE_FORCE_SCHEMA_DDL", "1"),
])
def test_schema_cache_explicit_bypass_for_ci_and_operator(monkeypatch, setting, value):
    monkeypatch.delenv("PBCORE_TEST_POSTGRES_URL", raising=False)
    monkeypatch.delenv("US_RISK_STATE_FORCE_SCHEMA_DDL", raising=False)
    monkeypatch.setenv(setting, value)
    conn = SchemaConnection()
    repos._ensure_us_position_risk_state_table(conn)
    repos._ensure_us_position_risk_state_table(conn)
    assert conn.ddls == 2


def test_schema_connection_without_info_preserves_legacy_behavior(monkeypatch):
    monkeypatch.delenv("PBCORE_TEST_POSTGRES_URL", raising=False)
    monkeypatch.delenv("US_RISK_STATE_FORCE_SCHEMA_DDL", raising=False)

    class NoInfo:
        calls = 0
        def execute(self, stmt):
            self.calls += 1

    conn = NoInfo()
    repos._ensure_us_position_risk_state_table(conn)
    repos._ensure_us_position_risk_state_table(conn)
    assert conn.calls == 2


@pytest.mark.parametrize("pnl", [-0.04, 0.01])
def test_armed_us_stop_state_save_failure_fences_confirmation(monkeypatch, pnl):
    from datetime import datetime, timezone

    monkeypatch.setenv("WSL_RUN_MARKET", "US")
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setenv("US_ORDER_ARMED", "1")
    monkeypatch.setattr(repos, "load_us_position_risk_state", lambda *args: {})
    durable = []
    def unavailable(*args, **kwargs):
        durable.append(kwargs.get("require_durable"))
        return False
    monkeypatch.setattr(repos, "save_us_position_risk_state", unavailable)

    with pytest.raises(RuntimeError, match="US_SOFT_STOP_DURABLE_STATE_WRITE_FAILED"):
        repos.update_us_soft_stop_risk_state(
            symbol="ANY_US_SYMBOL", trade_date="2026-11-16",
            pnl_pct=pnl, current_price=96.0 if pnl < 0 else 101.0,
            now=datetime(2026, 11, 16, 16, 30, tzinfo=timezone.utc),
            soft_stop_pct=0.03,
        )
    assert durable == [True]


def test_offline_stop_state_replay_preserves_memory_mode(monkeypatch):
    from datetime import datetime, timezone
    monkeypatch.setenv("WSL_RUN_MARKET", "US")
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setenv("US_ORDER_ARMED", "0")
    monkeypatch.setattr(repos, "load_us_position_risk_state", lambda *args: {})
    seen = []
    monkeypatch.setattr(
        repos, "save_us_position_risk_state",
        lambda *args, **kwargs: (seen.append(kwargs) or False)
    )
    result = repos.update_us_soft_stop_risk_state(
        symbol="ANY_US_SYMBOL", trade_date="2026-11-16",
        pnl_pct=-0.04, current_price=96.0,
        now=datetime(2026, 11, 16, 16, 30, tzinfo=timezone.utc),
        soft_stop_pct=0.03,
    )
    assert result["soft_stop_breach_count"] == 1
    assert seen == [{"require_durable": False}]
