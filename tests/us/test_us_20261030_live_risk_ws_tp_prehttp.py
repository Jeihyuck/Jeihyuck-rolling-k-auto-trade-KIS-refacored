"""2026-10-29 US incident: long WS socket, stale intraday input, stranded TP1.

No native KIS HTTP/order, migration, or broker state mutation in these tests.
"""
from __future__ import annotations

import os
import socket
import stat
import time

import pytest

from trader.marketdata import us_ws_collector
from trader.us import market_state_overlay as overlay
from trader.us.db import repos


def _completed_prices():
    return {
        "spy_previous_close": 100.0,
        "qqq_previous_close": 100.0,
        "smh_previous_close": 100.0,
        "spy_previous_close_date": "20261028",
        "qqq_previous_close_date": "20261028",
        "smh_previous_close_date": "20261028",
    }


def _live_quote(last, at=None):
    return {
        "last": str(last),
        "asof_epoch": time.time() if at is None else at,
        "stale": False,
        "source": "KIS_LIVE",
        "quality": "fresh",
    }


def test_ws_socket_path_is_short_private_and_bindable(monkeypatch):
    monkeypatch.setattr(us_ws_collector, "_account_identity",
                        lambda env: "f" * 24 if env == "practice" else "e" * 24)
    a = us_ws_collector.socket_path("practice")
    b = us_ws_collector.socket_path("real")
    assert a != b
    assert len(os.fsencode(str(a))) < 108
    assert stat.S_IMODE(a.parent.lstat().st_mode) == 0o700
    assert a.parent.lstat().st_uid == os.getuid()
    assert "Jeihyuck-rolling-k" not in str(a)
    # Prove an actual Linux AF_UNIX bind, not only a string-length assertion.
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
        try:
            sock.bind(str(a))
        finally:
            a.unlink(missing_ok=True)


def test_live_intraday_risk_overrides_positive_completed_day():
    now = time.time()
    provider = {"intraday_quotes": {
        "SPY": _live_quote(99.7, now),
        "QQQ": _live_quote(98.0, now),
        "SMH": _live_quote(98.5, now),
    }}
    daily = _completed_prices()
    daily.update({"spy_1d_return": +0.0073, "qqq_1d_return": +0.00417,
                  "smh_1d_return": +0.01117})
    warnings = []
    actual = overlay.read_us_live_intraday_returns(provider, daily, warnings)
    assert actual["quality"] == "OK"
    assert actual["returns"]["qqq"] == pytest.approx(-0.02)
    assert daily["qqq_1d_return"] > 0
    intraday = overlay.compute_us_intraday_overlay(
        actual["returns"]["spy"], actual["returns"]["qqq"],
        actual["returns"]["smh"],
    )
    assert intraday["intraday_market_overlay"] == "INTRADAY_RISK_OFF"
    qty, blocked, reason = overlay.apply_intraday_overlay_to_qty(
        10, "HEALTHCARE", intraday, owner_strategy="US_STANDARD",
    )
    assert (qty, blocked, reason) == (5, False, None)
    assert overlay.apply_intraday_overlay_to_qty(
        10, "AI_SEMI", intraday, owner_strategy="TQQQ_INFINITE",
    ) == (10, False, None)


def test_stale_or_missing_live_quotes_never_make_risk_look_normal():
    stale = time.time() - 120.0
    provider = {"intraday_quotes": {
        "SPY": _live_quote(100.1, stale),
        "QQQ": _live_quote(97.0, stale),
        "SMH": _live_quote(100, stale),
    }}
    warnings = []
    actual = overlay.read_us_live_intraday_returns(
        provider, _completed_prices(), warnings, max_age_sec=30.0,
    )
    assert actual["quality"] == "UNKNOWN"
    assert all(v is None for v in actual["returns"].values())
    qty, blocked, reason = overlay.apply_intraday_overlay_to_qty(
        10, "HEALTHCARE",
        {"intraday_market_overlay": "INTRADAY_DATA_UNKNOWN"},
        owner_strategy="US_STANDARD",
    )
    assert (qty, blocked, reason) == (
        0, True, "INTRADAY_MARKET_DATA_UNVERIFIED_ENTRY_BLOCK",
    )


def test_tp_pre_http_abort_releases_only_own_pending_intent(monkeypatch):
    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)
    repos.reset_memory_stores()
    args = {
        "trade_date": "2026-10-29", "symbol": "GILD",
        "stage": "tp1", "position_lifecycle_id": "life-gild",
    }
    repos.mark_us_profit_capture_stage(
        "2026-10-29", "GILD", "tp1", position_lifecycle_id="life-gild",
        order_key="gild-tp1-exact", qty=5, status="PENDING",
    )
    state = repos.load_us_profit_capture_state(
        "2026-10-29", ["GILD"], {"GILD": "life-gild"},
    )["GILD"]
    assert state["tp1_pending"] and not state["tp1_done"]
    assert not repos.release_us_profit_capture_pre_http_abort(
        **args, client_order_key="wrong-attempt",
    )
    assert repos.release_us_profit_capture_pre_http_abort(
        **args, client_order_key="gild-tp1-exact",
    )
    current = repos.load_us_profit_capture_state(
        "2026-10-29", ["GILD"], {"GILD": "life-gild"},
    )["GILD"]
    assert current["tp1_pending"] is False
    assert current["tp1_done"] is False
    assert current["meta"]["last_evidence_type"] == "BROKER_SUBMIT_ABORTED_PRE_IO"


def test_tp_pre_http_abort_cannot_clear_acked_order(monkeypatch):
    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)
    repos.reset_memory_stores()
    for status in ("ACK", "PARTIALLY_FILLED"):
        repos.mark_us_profit_capture_stage(
            "2026-10-29", "GILD", "tp1", position_lifecycle_id="life-gild",
            order_key="acked-order", qty=5, status=status,
        )
        assert not repos.release_us_profit_capture_pre_http_abort(
            trade_date="2026-10-29", symbol="GILD", stage="tp1",
            position_lifecycle_id="life-gild", client_order_key="acked-order",
        )
        state = repos.load_us_profit_capture_state(
            "2026-10-29", ["GILD"], {"GILD": "life-gild"},
        )["GILD"]
        assert state["tp1_pending"] is True


def test_real_tick_market_overlay_uses_today_not_daily(monkeypatch):
    # Completed index candles are green; only today's QQQ quote is negative.
    def completed(_provider, _date, _warnings, **_kw):
        return {
            "spy_1d_return": .0073,
            "qqq_1d_return": .00417,
            "smh_1d_return": .01117,
            "spy_3d_return": .0073,
            "qqq_3d_return": .00417,
            "smh_3d_return": .01117,
            **_completed_prices(),
        }

    monkeypatch.setattr(overlay, "_market_returns", completed)
    provider = {"intraday_quotes": {
        "SPY": _live_quote(99.7),
        "QQQ": _live_quote(98.0),
        "SMH": _live_quote(98.5),
    }}
    result = overlay.evaluate_us_market_state(
        trade_date="2026-10-29", provider=provider, positions=[],
        account_snapshot={}, rotation_context={"rotation_regime": "NORMAL"},
        prep_result={}, intraday_live_required=True,
    )
    assert result["qqq_1d_return"] == .00417
    assert result["intraday_live_returns"]["qqq"] == pytest.approx(-.02)
    assert result["intraday_market_overlay"] == "INTRADAY_RISK_OFF"
    assert result["intraday_overlay_available"] is True


def test_real_tick_missing_quote_is_not_faked_as_no_risk(monkeypatch):
    monkeypatch.setattr(
        overlay, "_market_returns",
        lambda *_a, **_kw: {
            "spy_1d_return": .01, "qqq_1d_return": .01,
            "smh_1d_return": .01, **_completed_prices(),
        },
    )
    result = overlay.evaluate_us_market_state(
        trade_date="2026-10-29",
        provider={"intraday_quotes": {"SPY": _live_quote(100)}},
        positions=[], account_snapshot={},
        rotation_context={"rotation_regime": "NORMAL"},
        prep_result={}, intraday_live_required=True,
    )
    assert result["intraday_live_quote_quality"] == "UNKNOWN"
    assert result["intraday_market_overlay"] == "INTRADAY_DATA_UNKNOWN"
    assert result["intraday_overlay_available"] is False


@pytest.mark.parametrize("claim_release_ok", [True, False])
def test_gild_tp1_route_pre_http_fence_recovers_only_after_claim_release(
    monkeypatch, claim_release_ok,
):
    """Exercise real order_router callback + TP lifecycle; no KIS HTTP."""
    from types import SimpleNamespace

    from trader.us.execution import order_router, order_journal
    from trader.us.execution.tick_context import TickExecutionContext

    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)
    repos.reset_memory_stores()
    td, lifecycle, order_key = "2026-10-29", "gild-lifecycle", "gild-tp1-key"
    repos.mark_us_profit_capture_stage(
        td, "GILD", "tp1", order_key=order_key, qty=5,
        position_lifecycle_id=lifecycle, status="PENDING",
    )
    events = []

    class NoPostClient:
        def __init__(self):
            self.before_http = None
            self.http_calls = 0

        def bind_tick_context(self, _context):
            return self

        def set_before_order_http(self, callback):
            self.before_http = callback

        def clear_before_order_http(self):
            self.before_http = None

        def place_us_sell_order(self, *_args):
            # The Stage4 health recheck fails within the before-HTTP callback.
            assert self.before_http is not None
            self.before_http()
            self.http_calls += 1
            pytest.fail("broker POST must remain unreachable")

    client = NoPostClient()
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setenv("DRY_RUN", "0")
    monkeypatch.setenv("US_RECONCILE_WORKER_MODE", "WORKER")
    monkeypatch.setenv("US_ACCOUNT_RESERVATION_MODE", "OFF")
    monkeypatch.setenv("US_ATOMIC_CLAIM_OUTBOX_ENABLED", "0")
    monkeypatch.setattr("trader.account_state.get_account_key",
                        lambda **_kw: "offline-practice-account")
    monkeypatch.setattr(order_router, "canonical_order_risk_check",
                        lambda *_a, **_kw: None, raising=False)
    monkeypatch.setattr(order_router, "same_day_semantic_sell_exists",
                        lambda *_a, **_kw: False)
    monkeypatch.setattr(order_router, "enrich_sell_exchange",
                        lambda *_a, **_kw: "NYSE")
    monkeypatch.setattr(order_router, "resolve_dry_run_for_us_order",
                        lambda: False)
    monkeypatch.setattr(repos, "load_today_order_keys", lambda **_kw: set())
    monkeypatch.setattr(repos, "save_order_intent", lambda *_a, **_kw: True)
    monkeypatch.setattr(repos, "claim_execution_action",
                        lambda *_a, **_kw: SimpleNamespace(
                            acquired=True, action_key="claim-gild", reason=""))
    monkeypatch.setattr(repos, "record_execution_action_observation",
                        lambda *_a, **_kw: None)

    def claim_release(*_a, **_kw):
        events.append("CLAIM_RELEASE")
        if not claim_release_ok:
            raise RuntimeError("injected_claim_release_failure")
        return True

    monkeypatch.setattr(
        repos, "release_execution_action_before_submit", claim_release,
    )
    monkeypatch.setattr(
        order_journal, "append_order_event",
        lambda event_type, *_a, **_kw: events.append(event_type),
    )
    from trader.us.execution import authoritative_reconcile_worker as worker
    monkeypatch.setattr(
        worker, "read_worker_health",
        lambda **_kw: (_ for _ in ()).throw(RuntimeError("writer-fenced")),
    )
    context = TickExecutionContext(
        td, "am", "run-gild", 1, "tick-gild",
        deadline=time.monotonic() + 30,
    )
    result = order_router.route_order({
        "symbol": "GILD", "exchange": "NYSE", "side": "SELL",
        "qty": 5, "limit_price": 162.695,
        "notional_usd": 813.475, "client_order_key": order_key,
        "trade_date": td, "trading_epoch_id": "offline-epoch",
        "strategy_owner": "US_STANDARD",
        "position_lifecycle_id": lifecycle,
        "position_action": "TAKE_PROFIT_SELL",
        "reason": "TAKE_PROFIT_TP1",
        "meta": {
            "profit_capture_stage": "tp1",
            "position_lifecycle_id": lifecycle,
            "strategy_owner": "US_STANDARD",
            "reason": "TAKE_PROFIT_TP1",
        },
    }, kis_client=client, context=context)
    state = repos.load_us_profit_capture_state(
        td, ["GILD"], {"GILD": lifecycle},
    )["GILD"]
    assert result["broker_submit"] is False
    assert client.http_calls == 0
    assert "BROKER_SUBMIT_ABORTED_PRE_IO" in events
    assert "CLAIM_RELEASE" in events
    assert state["tp1_pending"] is (not claim_release_ok)
    assert result["requires_reconcile"] is (not claim_release_ok)
