"""P0 TP decision source freshness: offline only, no KIS HTTP/order/DB."""
from __future__ import annotations

import time
from copy import deepcopy

import pytest

from trader.us.execution.tp_signal_quote_refresh import refresh_tp_source_quotes


def _position(symbol="KO", *, quote_age=40, owner="US_STANDARD"):
    return {
        "symbol": symbol, "exchange": "NYSE", "strategy_owner": owner,
        "qty": 27, "current_price": 109.0, "entry_price": 100.0,
        "resolved_quote_source": "KIS_LIVE",
        "resolved_quote_asof": time.time() - quote_age,
        "exit_quote_evidence": {
            "source": "KIS_LIVE", "price": 109.0, "asof_epoch": time.time() - quote_age,
        },
        "entry_exit_contract": {"marker": "ORIGINAL_UNCHANGED"},
        "risk_state": {"soft_stop_breach_count": 1},
        "max_price": 120.0,
    }


class _SafeProvider:
    def __init__(self, quote=None, failure=False):
        self.quote = quote
        self.failure = failure
        self.calls = []

    def get_isolated_exit_quote(self, symbol, exchange, *, stage_deadline):
        self.calls.append((symbol, exchange, stage_deadline))
        assert stage_deadline >= time.monotonic()
        if self.failure:
            raise RuntimeError("test-only KIS quote failure")
        return self.quote or {
            "last": "110.00", "quality": "fresh", "source": "KIS_LIVE",
            "asof_epoch": time.time(),
        }


@pytest.fixture(autouse=True)
def _practice_limiter(monkeypatch):
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setenv("US_SHARED_KIS_LIMITER_ENABLED", "1")


def test_stale_source_refreshes_only_tp_copy_without_rewriting_stop_or_contract():
    row = _position()
    original = deepcopy(row)
    provider = _SafeProvider()
    refreshed = refresh_tp_source_quotes([row], provider)
    assert len(provider.calls) == 1
    assert refreshed[0] is not row
    assert refreshed[0]["current_price"] == 110.0
    assert refreshed[0]["exit_quote_evidence"]["source"] == "KIS_LIVE"
    assert refreshed[0]["resolved_quote_asof"] > original["resolved_quote_asof"]
    assert refreshed[0]["risk_state"] == original["risk_state"]
    assert refreshed[0]["max_price"] == original["max_price"]
    assert refreshed[0]["entry_exit_contract"] == original["entry_exit_contract"]
    assert row == original


def test_fresh_source_is_reused_with_no_extra_kis_read():
    row = _position(quote_age=1)
    provider = _SafeProvider()
    result = refresh_tp_source_quotes([row], provider)
    assert provider.calls == []
    assert result[0] is row


@pytest.mark.parametrize("bad", [
    {"last": "110", "quality": "stale", "source": "KIS_LIVE", "asof_epoch": lambda: time.time()},
    {"last": "110", "quality": "fresh", "source": "KIS_LIVE", "asof_epoch": lambda: time.time() - 40},
    {"last": "110", "quality": "fresh", "source": "KIS_LIVE"},
    {"last": "110", "quality": "fresh", "source": "UNKNOWN", "asof_epoch": lambda: time.time()},
    {"last": "nan", "quality": "fresh", "source": "KIS_LIVE", "asof_epoch": lambda: time.time()},
    {"last": "110", "quality": "fresh", "source": "KIS_LIVE", "asof_epoch": lambda: time.time() + 15},
])
def test_unverified_source_never_becomes_fresh(bad):
    quote = {k: (v() if callable(v) else v) for k, v in bad.items()}
    row = _position()
    result = refresh_tp_source_quotes([row], _SafeProvider(quote))
    assert result[0] is row
    assert result[0]["resolved_quote_asof"] == row["resolved_quote_asof"]


def test_failing_reader_preserves_original_stale_block_and_other_symbols():
    first, second = _position("KO"), _position("PG")
    result = refresh_tp_source_quotes([first, second], _SafeProvider(failure=True))
    assert result == [first, second]


def test_budget_and_owner_fences_do_not_touch_infinite_or_oversubscribe():
    rows = [
        _position("TQQQ", owner="TQQQ_INFINITE"),
        _position("KO"),
        _position("PG"),
        _position("DHR"),
    ]
    provider = _SafeProvider()
    result = refresh_tp_source_quotes(rows, provider, max_reads=1)
    assert [x[0] for x in provider.calls] == ["KO"]
    assert result[0] is rows[0]
    assert result[1] is not rows[1]
    assert result[2] is rows[2]
    assert result[3] is rows[3]


@pytest.mark.parametrize("env,limiter", [("live", "1"), ("practice", "0")])
def test_unsupported_env_or_missing_shared_limiter_disables_isolated_read(monkeypatch, env, limiter):
    monkeypatch.setenv("KIS_ENV", env)
    monkeypatch.setenv("US_SHARED_KIS_LIMITER_ENABLED", limiter)
    provider = _SafeProvider()
    row = _position()
    assert refresh_tp_source_quotes([row], provider)[0] is row
    assert provider.calls == []


def test_no_reader_or_zero_budget_keeps_original():
    row = _position()
    assert refresh_tp_source_quotes([row], object())[0] is row
    assert refresh_tp_source_quotes([row], _SafeProvider(), budget_sec=0)[0] is row


def test_fresh_source_allows_original_tp1_decision_but_stale_source_does_not():
    """Positive TP decision path, not broker-authorized POST."""
    from decimal import Decimal
    from datetime import datetime, timezone
    from unittest.mock import patch

    from trader.us.entry_exit_contract import build_us_entry_exit_contract
    from trader.us.market_state_overlay import build_profit_capture_intents

    row = _position(quote_age=40)
    contract = build_us_entry_exit_contract({
        "symbol": "KO", "strategy_owner": "US_STANDARD",
        "entry_style_selected": "pb1_pullback",
        "position_action": "NEW_POSITION_BUY",
    })
    row["meta"] = {
        "entry_exit_contract": contract,
        "entry_exit_contract_sha256": contract["sha256"],
        "entry_exit_contract_version": contract["version"],
        "strategy_owner": "US_STANDARD",
    }
    row["position_lifecycle_id"] = "KO:original-v2-cycle"
    now = datetime.now(timezone.utc)
    overlay = {"profit_capture_enabled": True}

    def decision(positions):
        with (
            patch("trader.us.profit_capture.authoritative_broker_avg",
                  return_value=(Decimal("100"), {"broker_avg_price_source": "KIS_BROKER"})),
            patch("trader.us.execution.us_legacy_nine_active_sell.enrich_legacy_nine_v2_decision_position",
                  side_effect=lambda pos, **_: pos),
        ):
            return build_profit_capture_intents(
                positions, overlay, now=now, trade_date="2026-11-20",
                profit_capture_state={"KO": {}}, defer_pending_until_claim=True,
                decision_clock_real_time=True,
            )

    assert decision([row]) == [], "Known 40s-old quote must block TP decision"
    refreshed = refresh_tp_source_quotes([row], _SafeProvider())
    intents = decision(refreshed)
    assert len(intents) == 1
    assert intents[0]["symbol"] == "KO"
    assert intents[0]["side"] == "SELL"
    assert intents[0]["strategy_owner"] == "US_STANDARD"
    assert intents[0]["meta"]["profit_capture_stage"] == "tp1"
    assert row["resolved_quote_asof"] < refreshed[0]["resolved_quote_asof"]


def test_protective_sell_symbols_are_never_refetched_by_tp_path():
    a, b = _position("DHR"), _position("KO")
    provider = _SafeProvider()
    refreshed = refresh_tp_source_quotes(
        [a, b], provider, max_reads=1, exclude_symbols={"DHR"},
    )
    assert [call[0] for call in provider.calls] == ["KO"]
    assert refreshed[0] is a
    assert refreshed[1] is not b
