"""2026-11-16 US incidents: all-symbol TP clock and candidate-isolation regression.

Read-only unit tests: no KIS POST, no physical DB writes.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import patch

import pytest

from trader.us.execution.order_router import (
    _tp_decision_fresh_for_native_http,
    _tp_live_quote_threshold_proven,
)
from trader.us.market_state_overlay import (
    build_profit_capture_intents,
    filter_entry_intents_for_market_state,
)
from trader.us.runner.trade_tick_runner import (
    is_us_global_postfilter_contract_unverified,
)


TICK_START = datetime(2026, 11, 16, 14, 31, 1, tzinfo=timezone.utc)


def _make_tp(*, symbol="NOVA", live=False, proof=None):
    position = {
        "symbol": symbol, "qty": 20, "current_price": 110.0,
        "entry_price": 100.0, "position_lifecycle_id": "cycle-test-001",
        "exit_quote_evidence": proof or {},
    }
    with (
        patch("trader.us.profit_capture.authoritative_broker_avg",
              return_value=(Decimal("100"), {"broker_avg_price_source": "kis_pchs_avg_pric"})),
        patch("trader.us.execution.us_legacy_nine_active_sell.enrich_legacy_nine_v2_decision_position",
              side_effect=lambda p, **_: p),
    ):
        return build_profit_capture_intents(
            [position], {"profit_capture_enabled": True},
            now=TICK_START, trade_date="2026-11-16",
            profit_capture_state={symbol: {}},
            defer_pending_until_claim=True,
            decision_clock_real_time=live,
        )


@pytest.mark.parametrize("symbol", ["NOVA", "AXYZ", "BETA"])
def test_live_tp_decision_time_is_not_stale_tick_start(symbol):
    before = datetime.now(timezone.utc)
    intents = _make_tp(symbol=symbol, live=True)
    after = datetime.now(timezone.utc)
    assert len(intents) == 1
    assert intents[0]["strategy_owner"] == "US_STANDARD"
    decision = datetime.fromisoformat(intents[0]["meta"]["last_profit_capture_at"])
    assert before <= decision <= after
    assert decision > TICK_START
    # Nov16 incident: an actual 28s routing delay must not look like 161s.
    assert _tp_decision_fresh_for_native_http(
        intents[0], now_utc=decision + timedelta(seconds=28), max_age_sec=30
    ) == (True, "US_TP_DECISION_FRESH")
    assert _tp_decision_fresh_for_native_http(
        intents[0], now_utc=decision + timedelta(seconds=31), max_age_sec=30
    ) == (False, "US_TP_DECISION_EXPIRED_BEFORE_HTTP")


def test_replay_keeps_injected_clock_separate_from_live_wall_clock():
    intents = _make_tp(live=False)
    assert len(intents) == 1
    assert intents[0]["meta"]["last_profit_capture_at"] == TICK_START.isoformat()


def test_quote_source_and_asof_are_not_fabricated_by_new_signal_clock():
    proof = {
        "source": "KIS_LIVE", "asof": "2026-11-16T14:33:14+00:00",
        "asof_epoch": 1794839594.0,
        "received_at_utc": "2026-11-16T14:33:15+00:00",
        "quality": "FRESH",
    }
    i = _make_tp(live=True, proof=proof)[0]
    meta = i["meta"]
    assert meta["decision_quote_evidence"] == {"price": 110.0, **proof}
    unknown = _make_tp(live=True)[0]["meta"]["decision_quote_evidence"]
    assert unknown["asof"] is None
    assert unknown["asof_epoch"] is None
    assert unknown["source"] == "UNVERIFIED"
    # A new decision time does NOT bypass the final original KIS quote fence.
    live_quote = {
        "_quote_quality": "FRESH", "_quote_source": "KIS_LIVE",
        "_quote_asof_epoch": 100.0, "output": {"last": "109.00"},
    }
    assert _tp_live_quote_threshold_proven(
        i, live_quote, now_epoch=107.0
    ) == (False, "US_TP_LIVE_QUOTE_EXPIRED")
    live_quote["_quote_asof_epoch"] = 104.0
    live_quote["output"]["last"] = "102.00"
    assert _tp_live_quote_threshold_proven(
        i, live_quote, now_epoch=107.0
    ) == (False, "US_TP_LIVE_THRESHOLD_LOST")


def test_per_symbol_intraday_rotation_block_preserves_other_eligible_symbol():
    overlay = {
        "market_state": "NORMAL", "intraday_market_overlay": "NORMAL",
        "intraday_rotation_overlay": "INTRADAY_SEMI_CRASH",
        "intraday_overlay_available": True,
    }
    candidates = [
        {"symbol": "AXYZ", "side": "BUY", "qty": 10, "theme_cluster": "AI_SEMI",
         "score_final": 80, "trend_score": 90, "rank_final30": 1},
        {"symbol": "BETA", "side": "BUY", "qty": 10, "theme_cluster": "HEALTHCARE",
         "score_final": 80, "trend_score": 90, "rank_final30": 2},
    ]
    kept, blocked = filter_entry_intents_for_market_state(candidates, overlay)
    assert [row["symbol"] for row in kept] == ["BETA"]
    assert [row["symbol"] for row in blocked] == ["AXYZ"]
    assert not is_us_global_postfilter_contract_unverified(overlay)


@pytest.mark.parametrize("overlay", [
    None,
    {"intraday_market_overlay": "INTRADAY_DATA_UNKNOWN"},
    {"intraday_overlay_available": False},
    {"intraday_error_reason": "LIVE_RISK_EVALUATION_FAILED"},
    {"global_risk_integrity_failed": True},
])
def test_unverified_global_risk_still_fences_all_standard_buys(overlay):
    assert is_us_global_postfilter_contract_unverified(overlay)


def test_per_symbol_metadata_mismatch_does_not_change_other_candidates():
    overlay = {"market_state": "NORMAL", "intraday_market_overlay": "NORMAL",
               "intraday_overlay_available": True}
    data = [
        {"symbol": "AXYZ", "side": "BUY", "qty": 5, "theme_cluster": "HEALTHCARE",
         "meta": {"theme_cluster": "AI_SEMI"}},
        {"symbol": "BETA", "side": "BUY", "qty": 5, "theme_cluster": "HEALTHCARE"},
    ]
    kept, blocked = filter_entry_intents_for_market_state(data, overlay)
    assert [row["symbol"] for row in kept] == ["BETA"]
    assert blocked[0]["reason"] == "ENTRY_METADATA_INVARIANT_FAIL"
    assert not is_us_global_postfilter_contract_unverified(overlay)
