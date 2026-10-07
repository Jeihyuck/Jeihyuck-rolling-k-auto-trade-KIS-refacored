"""Controlled before/after PB1 repeated-I/O benchmark, run with real PostgreSQL.

Run as: python -m pytest -q -s tests/us/test_us_20261007_performance_acceptance.py

The artificial provider latency is intentionally modest for CI.  Results are
comparable *within this fixture only*.  They do not establish a production
300-second slot or pre-entry budget SLO: that requires a representative
latency replay or an observed deployment after merge.
"""
from __future__ import annotations

import json
import os
import statistics
import time
from datetime import date, timedelta

import pytest


@pytest.mark.skipif(not os.getenv("PBCORE_TEST_POSTGRES_URL"), reason="PostgreSQL acceptance URL unavailable")
def test_comparable_completed_market_io_twelve_ticks_before_after(pg_engine):
    from sqlalchemy import event, text
    from trader.us import market_state_overlay as market

    symbols = len(market._MARKET_RETURN_SYMBOLS)
    daily_rows = [
        {"date": (date(2026, 10, 5) - timedelta(days=259-i)).isoformat(),
         "close": 100 + i}
        for i in range(260)
    ]

    def run_scenario(*, optimized: bool):
        counts = {"db_select": 0, "kis_quote": 0, "completed_calls": 0}
        times_ms = []
        hit_count = 0
        context = None

        def track_sql(_conn, _cursor, statement, _parameters, _ctx, _many):
            if str(statement).lstrip().upper().startswith("SELECT"):
                counts["db_select"] += 1
        event.listen(pg_engine, "before_cursor_execute", track_sql)
        class Provider:
            def get_completed_daily_prices(
                self, symbol, exchange, trade_date, required_bars,
                allow_http_sync=False,
            ):
                counts["completed_calls"] += 1
                with pg_engine.begin() as conn:
                    # A real SQL statement is executed once per completed-bar
                    # request. This mocks the provider's DB read without
                    # touching or rewiring any operational market-data table.
                    conn.execute(text("SELECT :symbol AS requested_symbol"), {"symbol": symbol})
                time.sleep(0.004)  # same artificial DB latency in both scenarios
                return daily_rows

            def get_current_price(self, symbol, exchange):
                counts["kis_quote"] += 1
                time.sleep(0.001)  # must remain fresh on every single tick
                return {"last": 100.0, "source": "fresh-mock-kis"}
        provider = Provider()
        try:
            for tick in range(12):
                started = time.perf_counter()
                provider.get_current_price("SPY", "NYSE")
                ret = market._market_returns(
                    provider, "2026-10-06", [],
                    completed_market_context=context if optimized else None,
                    data_version="same-day-version-1",
                )
                assert ret["_completed_market_context"]["quality"] == "OK"
                hit_count += int(ret["_completed_market_cache_hit"])
                context = ret["_completed_market_context"]
                times_ms.append(round(1000 * (time.perf_counter() - started), 3))
        finally:
            event.remove(pg_engine, "before_cursor_execute", track_sql)
        return {
            "tick_count": 12,
            "completed_calls": counts["completed_calls"],
            "db_select": counts["db_select"],
            "kis_quote": counts["kis_quote"],
            "cache_hits": hit_count,
            "mean_wall_ms": round(statistics.mean(times_ms), 3),
            "p95_wall_ms": round(sorted(times_ms)[11], 3),
            "sum_wall_ms": round(sum(times_ms), 3),
            "slot_overrun_300s": sum(ms > 300000 for ms in times_ms),
            "entry_defer": None,  # not modeled by this IO-only fixture
        }

    baseline = run_scenario(optimized=False)
    optimized = run_scenario(optimized=True)
    assert baseline["completed_calls"] == symbols * 12
    assert optimized["completed_calls"] == symbols
    assert baseline["db_select"] == symbols * 12
    assert optimized["db_select"] == symbols
    assert baseline["kis_quote"] == optimized["kis_quote"] == 12
    assert optimized["cache_hits"] == 11
    assert optimized["sum_wall_ms"] < baseline["sum_wall_ms"]
    report = {
        "measurement": "controlled_io_only_not_live_slot_proof",
        "before": baseline,
        "after": optimized,
        "db_select_saved": baseline["db_select"] - optimized["db_select"],
        "mean_wall_ms_reduction": round(baseline["mean_wall_ms"] - optimized["mean_wall_ms"], 3),
        "measured_live_slot_after": False,
    }
    print("US_PB1_CONTROLLED_IO_BENCHMARK=" + json.dumps(report, sort_keys=True))


# Reuse the project's already migrated PostgreSQL schema fixture.
from tests.us.test_us_postgres_integration_real import pg_engine  # noqa: E402,F401
