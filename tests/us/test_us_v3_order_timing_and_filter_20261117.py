"""V3: timing and risk-filter regressions on generic US_STANDARD symbols.

No real broker / POST.  Historic ticker strings are fixtures, not code gates.
"""
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import patch

import pytest

from trader.us.market_state_overlay import (
    build_profit_capture_intents,
    filter_entry_intents_for_market_state,
)
from trader.us.execution.order_router import _tp_decision_fresh_for_native_http


TICK_STARTED = datetime(2026, 11, 16, 14, 31, 1, tzinfo=timezone.utc)
DECIDED = TICK_STARTED + timedelta(seconds=133)
TRADE_DATE = "2026-11-16"


@pytest.mark.parametrize("symbol", ["PG", "XYZ", "AAA"])
def test_live_tp_uses_actual_decision_clock_not_stale_tick_start(symbol):
    position = {
        "symbol": symbol, "strategy_owner": "US_STANDARD",
        "qty": 20, "current_price": 110.0, "entry_price": 100.0,
        "position_lifecycle_id": f"cycle-{symbol}",
        "resolved_quote_asof": DECIDED.isoformat(),
        "resolved_quote_source": "KIS_LIVE",
    }
    with patch("trader.us.profit_capture.authoritative_broker_avg",
               return_value=(Decimal("100"), {"broker_avg_price_source": "KIS"})):
        intents = build_profit_capture_intents(
            [position], {"profit_capture_enabled": True},
            now=TICK_STARTED, trade_date=TRADE_DATE,
            profit_capture_state={symbol: {}},
            defer_pending_until_claim=True,
            decision_clock=lambda: DECIDED,
        )
    assert len(intents) == 1
    intent = intents[0]
    assert intent["meta"]["last_profit_capture_at"] == DECIDED.isoformat()
    assert intent["meta"]["decision_quote_source_at"] == DECIDED.isoformat()
    assert intent["meta"]["decision_quote_price"] == "110.0"
    assert _tp_decision_fresh_for_native_http(
        intent, now_utc=DECIDED + timedelta(seconds=28),
    ) == (True, "US_TP_DECISION_FRESH")
    assert _tp_decision_fresh_for_native_http(
        intent, now_utc=DECIDED + timedelta(seconds=31),
    ) == (False, "US_TP_DECISION_EXPIRED_BEFORE_HTTP")


def test_unverified_or_naive_decision_clock_cannot_authorize_tp():
    pos = {"symbol": "XYZ", "qty": 10, "entry_price": 100.0,
           "current_price": 110.0, "position_lifecycle_id": "cycle-xyz"}
    with patch("trader.us.profit_capture.authoritative_broker_avg",
               return_value=(Decimal("100"), {"broker_avg_price_source": "KIS"})):
        for clock in [lambda: None, lambda: DECIDED.replace(tzinfo=None)]:
            assert build_profit_capture_intents(
                [dict(pos)], {"profit_capture_enabled": True},
                now=TICK_STARTED, trade_date=TRADE_DATE,
                profit_capture_state={"XYZ": {}},
                defer_pending_until_claim=True, decision_clock=clock,
            ) == []


def test_source_quote_time_is_unknown_not_fabricated_from_decision():
    pos = {"symbol": "XYZ", "qty": 10, "entry_price": 100.0,
           "current_price": 110.0, "position_lifecycle_id": "cycle-xyz"}
    with patch("trader.us.profit_capture.authoritative_broker_avg",
               return_value=(Decimal("100"), {"broker_avg_price_source": "KIS"})):
        intents = build_profit_capture_intents(
            [pos], {"profit_capture_enabled": True},
            now=TICK_STARTED, trade_date=TRADE_DATE,
            profit_capture_state={"XYZ": {}},
            defer_pending_until_claim=True,
            decision_clock=lambda: DECIDED,
        )
    assert len(intents) == 1
    assert intents[0]["meta"]["decision_quote_source_at"] is None
    assert intents[0]["meta"]["decision_quote_source"] == "UNKNOWN"


def test_filter_isolates_candidate_veto_and_preserves_independent_peer():
    overlay = {
        "market_state": "NORMAL", "market_regime": "RISK_ON",
        "allow_new_buy": True, "allow_ai_tech_buy": True,
        "intraday_market_overlay": "NORMAL",
        "intraday_rotation_overlay": "INTRADAY_SEMI_CRASH",
    }
    intents = [
        {"symbol": "AAA", "side": "BUY", "strategy_owner": "US_STANDARD",
         "qty": 4, "theme_cluster": "AI_SEMI"},
        {"symbol": "BBB", "side": "BUY", "strategy_owner": "US_STANDARD",
         "qty": 4, "theme_cluster": "DEFENSIVE"},
    ]
    kept, blocked = filter_entry_intents_for_market_state(intents, overlay, [])
    assert [i["symbol"] for i in kept] == ["BBB"]
    assert [i["symbol"] for i in blocked] == ["AAA"]
    assert blocked[0]["reason"] == "INTRADAY_SEMI_CRASH_AI_SEMI_BLOCK"


def test_global_market_crash_remains_closed_for_all_standard_candidates():
    overlay = {
        "market_state": "NORMAL", "market_regime": "RISK_ON",
        "allow_new_buy": True, "allow_ai_tech_buy": True,
        "intraday_market_overlay": "INTRADAY_MARKET_CRASH",
    }
    intents = [
        {"symbol": s, "side": "BUY", "strategy_owner": "US_STANDARD",
         "qty": 4, "theme_cluster": c}
        for s, c in [("AAA", "AI_SEMI"), ("BBB", "DEFENSIVE")]
    ]
    kept, blocked = filter_entry_intents_for_market_state(intents, overlay, [])
    assert kept == []
    assert len(blocked) == 2
    assert all(row["reason"] == "INTRADAY_MARKET_CRASH_ENTRY_BLOCK"
               for row in blocked)
