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
    # The Stage4 fault occurs only after the independent fresh balance and
    # frozen TP checks have passed; isolate those checks with known evidence.
    monkeypatch.setattr(
        order_router, "_get_broker_position",
        lambda *_a, **_kw: {
            "symbol": "GILD", "qty": 22, "orderable_qty": 22,
            "currency": "USD", "avg_price": 157.43,
        },
    )
    monkeypatch.setattr(
        order_router, "_validate_take_profit_with_fresh_broker_position",
        lambda *_a, **_kw: {
            "ok": True, "broker_avg_price": 157.43, "return_rate": .03344,
        },
    )
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


# This production-migration fixture uses a disposable CI PostgreSQL database,
# never a connected live / practice Supabase trading database.
from tests.us.test_us_postgres_integration_real import pg_engine  # noqa: E402,F401


@pytest.mark.skipif(
    not os.getenv("PBCORE_TEST_POSTGRES_URL"),
    reason="disposable PostgreSQL acceptance URL unavailable",
)
def test_pg_tp1_pre_http_abort_without_journal_or_claim_stays_fenced(pg_engine):
    from sqlalchemy import text

    repos.reset_memory_stores()
    td, lifecycle, key = "2026-10-29", "GILD_PG_L1", "GILD_PG_TP1_K1"
    repos.mark_us_profit_capture_stage(
        td, "GILD", "tp1", position_lifecycle_id=lifecycle,
        order_key=key, qty=5, status="PENDING",
    )
    with pg_engine.begin() as conn:
        before = conn.execute(text("""
            SELECT stage_status, client_order_key, cumulative_filled_qty
            FROM us_profit_capture_lifecycle
            WHERE trade_date=:td AND symbol='GILD'
            AND stage='tp1' AND position_lifecycle_id=:lifecycle
        """), {"td": td, "lifecycle": lifecycle}).mappings().one()
    assert before["stage_status"] == "PENDING"
    assert before["client_order_key"] == key
    assert int(before["cumulative_filled_qty"]) == 0

    assert not repos.release_us_profit_capture_pre_http_abort(
        trade_date=td, symbol="GILD", stage="tp1",
        position_lifecycle_id=lifecycle, client_order_key="WRONG_KEY",
    )
    # DB absence is not proof of an authorized ABORT_PRE_IO.
    assert not repos.release_us_profit_capture_pre_http_abort(
        trade_date=td, symbol="GILD", stage="tp1",
        position_lifecycle_id=lifecycle, client_order_key=key,
    )
    assert not repos.release_us_profit_capture_pre_http_abort(
        trade_date=td, symbol="GILD", stage="tp1",
        position_lifecycle_id=lifecycle, client_order_key=key,
    )
    with pg_engine.begin() as conn:
        after = conn.execute(text("""
            SELECT stage_status, cumulative_filled_qty, state
            FROM us_profit_capture_lifecycle
            WHERE trade_date=:td AND symbol='GILD'
            AND stage='tp1' AND position_lifecycle_id=:lifecycle
        """), {"td": td, "lifecycle": lifecycle}).mappings().one()
    assert after["stage_status"] == "PENDING"
    assert int(after["cumulative_filled_qty"]) == 0
    assert after["state"]["tp1_pending"] is True
    assert after["state"]["tp1_done"] is False



@pytest.mark.skipif(
    not os.getenv("PBCORE_TEST_POSTGRES_URL"),
    reason="disposable PostgreSQL acceptance URL unavailable",
)
def test_pg_proven_same_attempt_pre_io_abort_releases_only_locked_tp(
    pg_engine, monkeypatch,
):
    """Real migrated PG: journal + Claim release + exact TP row, no broker."""
    from datetime import date, datetime, timezone
    from uuid import uuid4
    from trader.execution_claims import DurableExecutionClaimRepo
    from trader.execution_state import SemanticActionIdentity
    from trader.us.db.execution_claim_schema import (
        us_execution_attempts, us_execution_claims,
    )

    repos.reset_memory_stores()
    td, epoch = "2026-10-29", "pg-us353-test-epoch"
    lifecycle, key = "GILD_PG_PROVEN_CYCLE", "GILD_PG_PROVEN_TP1"
    attempt_id = "GILD_PG_PROVEN_ATTEMPT"
    monkeypatch.setattr(repos, "_active_us_epoch", lambda _bind=None, **_kw: epoch)

    repos.mark_us_profit_capture_stage(
        td, "GILD", "tp1", position_lifecycle_id=lifecycle,
        order_key=key, qty=5, status="PENDING",
    )
    identity = SemanticActionIdentity(
        env="practice", account_id="pg-us353-test-account",
        market="US", trading_epoch_id=epoch, strategy_owner="US_STANDARD",
        lifecycle_id=lifecycle, action="TAKE_PROFIT_TP1",
        trade_date=date.fromisoformat(td),
    )
    claim_repo = DurableExecutionClaimRepo(
        pg_engine, us_execution_claims, us_execution_attempts,
    )
    assert claim_repo.acquire(
        identity, attempt_id=attempt_id, requested_qty=5, client_order_key=key,
    ).acquired
    assert not repos.release_us_profit_capture_pre_http_abort(
        trade_date=td, symbol="GILD", stage="tp1",
        position_lifecycle_id=lifecycle, client_order_key=key,
    ), "active claim and absent abort are NOT proof"
    claim_repo.release_before_submit(identity, attempt_id=attempt_id)
    # A released claim alone is insufficient without durable explicit abort.
    assert not repos.release_us_profit_capture_pre_http_abort(
        trade_date=td, symbol="GILD", stage="tp1",
        position_lifecycle_id=lifecycle, client_order_key=key,
    )
    assert repos.append_us_order_event({
        "event_id": str(uuid4()), "trade_date": td,
        "event_type": "BROKER_SUBMIT_ABORTED_PRE_IO",
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "session": "am", "session_run_id": "pg-us353-proven",
        "tick_id": "pg-us353-tick",
        "trading_epoch_id": epoch,
        "client_order_key": key, "submit_attempt_id": attempt_id,
        "symbol": "GILD", "side": "SELL", "requested_qty": 5,
        "raw_broker_order_no": None, "canonical_broker_order_no": None,
        "position_lifecycle_id": lifecycle,
        "profit_capture_stage": "tp1", "cumulative_filled_qty": 0,
        "payload": {"strategy_owner": "US_STANDARD"},
        "idempotency_key": "pg-us353-proven-abort",
    })
    assert repos.release_us_profit_capture_pre_http_abort(
        trade_date=td, symbol="GILD", stage="tp1",
        position_lifecycle_id=lifecycle, client_order_key=key,
    )
    assert not repos.release_us_profit_capture_pre_http_abort(
        trade_date=td, symbol="GILD", stage="tp1",
        position_lifecycle_id=lifecycle, client_order_key=key,
    ), "duplicate abort must not release again"
    state = repos.load_us_profit_capture_state(
        td, ["GILD"], {"GILD": lifecycle},
    )["GILD"]
    assert state["tp1_pending"] is False
    assert state["tp1_done"] is False
    assert state["meta"]["last_evidence_type"] == "BROKER_SUBMIT_ABORTED_PRE_IO"



@pytest.mark.skipif(
    not os.getenv("PBCORE_TEST_POSTGRES_URL"),
    reason="disposable PostgreSQL acceptance URL unavailable",
)
def test_pg_us353_native_tp_claim_reserve_http_gate_and_exact_abort(
    pg_engine, monkeypatch,
):
    """Migrated PostgreSQL: a specific US_STANDARD Claim alone can reserve TP.

    Deliberately uses no network or native KIS client. Proves NOT_TRIGGERED
    before Claim, one-stage CAS, missing STARTED fail-closed, exact STARTED
    authority, no release without Claim+durable abort, and idempotent release.
    """
    from datetime import date, datetime, timezone
    from uuid import uuid4
    from trader.execution_claims import DurableExecutionClaimRepo
    from trader.execution_state import SemanticActionIdentity
    from trader.us.db.execution_claim_schema import (
        us_execution_attempts, us_execution_claims,
    )
    from sqlalchemy import text

    repos.reset_memory_stores()
    td, epoch = "2026-10-29", "pg-us353-native-tp-epoch"
    account = "pg-us353-native-tp-account"
    cycle = "GILD_PG_US353_CLAIMED_NATIVE"
    key = "GILD_PG_US353_CLAIMED_TP1"
    attempt = "GILD_PG_US353_CLAIMED_ATTEMPT"
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setattr(repos, "_active_us_epoch", lambda _bind=None, **_kw: epoch)
    monkeypatch.setattr(repos, "_us_execution_claim_scope", lambda: {
        "env": "practice", "account_id": account, "market": "US",
        "trading_epoch_id": epoch,
    })
    assert repos.save_order_intent({
        "trade_date": td, "symbol": "GILD", "side": "SELL",
        "exchange": "NYSE", "qty": 5, "limit_price": 100,
        "notional_usd": 500, "client_order_key": key,
        "position_lifecycle_id": cycle, "strategy_owner": "US_STANDARD",
        "submit_attempt_id": attempt,
        "meta": {"submit_attempt_id": attempt, "profit_capture_stage": "tp1",
                 "position_lifecycle_id": cycle, "strategy_owner": "US_STANDARD",
                 "tp_pending_deferred_to_claim": True},
    }, trade_date=td)

    identity = SemanticActionIdentity(
        env="practice", account_id=account, market="US", trading_epoch_id=epoch,
        strategy_owner="US_STANDARD", lifecycle_id=cycle, action="TP1",
        trade_date=date.fromisoformat(td),
    )
    repo = DurableExecutionClaimRepo(
        pg_engine, us_execution_claims, us_execution_attempts,
    )
    assert repo.acquire(identity, attempt_id=attempt, requested_qty=5,
                        client_order_key=key).acquired
    args = dict(
        trade_date=td, symbol="GILD", stage="tp1", position_lifecycle_id=cycle,
        client_order_key=key, attempt_id=attempt, action_key=identity.action_key,
    )
    claim_meta = {
        "submit_attempt_id": attempt, "profit_capture_stage": "tp1",
        "position_lifecycle_id": cycle, "strategy_owner": "US_STANDARD",
        "tp_pending_deferred_to_claim": True,
    }
    assert repos.reserve_us_profit_capture_stage_after_claim(
        **args, qty=5, notional_usd=500, limit_price_usd=100,
        intent_meta=claim_meta,
    )
    assert not repos.reserve_us_profit_capture_stage_after_claim(
        **args, qty=5, notional_usd=500, limit_price_usd=100,
        intent_meta=claim_meta,
    ), "the same TP stage cannot be reserved twice"
    assert not repos.verify_us_tp_claim_stage_at_http_boundary(**args)

    def event(kind, suffix):
        return repos.append_us_order_event({
            "event_id": str(uuid4()), "trade_date": td,
            "event_type": kind, "event_timestamp": datetime.now(timezone.utc).isoformat(),
            "session": "am", "session_run_id": "pg-us353-native",
            "tick_id": "pg-us353-native-tick", "trading_epoch_id": epoch,
            "client_order_key": key, "submit_attempt_id": attempt,
            "symbol": "GILD", "side": "SELL", "requested_qty": 5,
            "raw_broker_order_no": None, "canonical_broker_order_no": None,
            "position_lifecycle_id": cycle, "profit_capture_stage": "tp1",
            "cumulative_filled_qty": 0,
            "payload": {"strategy_owner": "US_STANDARD"},
            "idempotency_key": f"pg-us353-native-{suffix}",
        })

    assert event("BROKER_SUBMIT_STARTED", "started")
    assert repos.verify_us_tp_claim_stage_at_http_boundary(**args)
    assert not repos.release_us_profit_capture_pre_http_abort(
        trade_date=td, symbol="GILD", stage="tp1",
        position_lifecycle_id=cycle, client_order_key=key,
    ), "the live claim/started event cannot be mistaken for a safe abort"

    assert event("BROKER_SUBMIT_ABORTED_PRE_IO", "abort")
    repo.release_before_submit(identity, attempt_id=attempt)
    assert not repos.verify_us_tp_claim_stage_at_http_boundary(**args)
    assert repos.release_us_profit_capture_pre_http_abort(
        trade_date=td, symbol="GILD", stage="tp1",
        position_lifecycle_id=cycle, client_order_key=key,
    )
    assert not repos.release_us_profit_capture_pre_http_abort(
        trade_date=td, symbol="GILD", stage="tp1",
        position_lifecycle_id=cycle, client_order_key=key,
    )
    with pg_engine.connect() as conn:
        row = conn.execute(text("""
            SELECT stage_status,state FROM us_profit_capture_lifecycle
             WHERE trade_date=:td AND symbol='GILD' AND stage='tp1'
               AND position_lifecycle_id=:cycle
        """), {"td": td, "cycle": cycle}).mappings().one()
    assert row["stage_status"] == "NOT_TRIGGERED"
    assert row["state"]["meta"]["last_evidence_type"] == "BROKER_SUBMIT_ABORTED_PRE_IO"


@pytest.mark.skipif(
    not os.getenv("PBCORE_TEST_POSTGRES_URL"),
    reason="disposable PostgreSQL acceptance URL unavailable",
)
def test_pg_tp1_ack_is_immune_to_late_pre_http_abort(pg_engine):
    repos.reset_memory_stores()
    td, lifecycle, key = "2026-10-29", "GILD_PG_L2", "GILD_PG_TP1_K2"
    repos.mark_us_profit_capture_stage(
        td, "GILD", "tp1", position_lifecycle_id=lifecycle,
        order_key=key, qty=5, status="PENDING",
    )
    repos.mark_us_profit_capture_stage(
        td, "GILD", "tp1", position_lifecycle_id=lifecycle,
        order_key=key, qty=5, status="ACK",
        broker_order_no="KIS_ORDER_123",
    )
    assert not repos.release_us_profit_capture_pre_http_abort(
        trade_date=td, symbol="GILD", stage="tp1",
        position_lifecycle_id=lifecycle, client_order_key=key,
    )
    state = repos.load_us_profit_capture_state(
        td, ["GILD"], {"GILD": lifecycle},
    )["GILD"]
    assert state["tp1_pending"] is True
    assert state["tp1_done"] is False
    assert state.get("tp1_broker_order_no")


@pytest.mark.parametrize(
    ("router", "expected"),
    [
        ({"status": "BROKER_SUBMIT_PRE_IO_FAILED", "requires_reconcile": False}, "FAILED"),
        ({"status": "BROKER_SUBMIT_PRE_IO_FAILED", "requires_reconcile": True}, "AMBIGUOUS_ACK"),
        ({"status": "ORDER_DISABLED_CLAIM_OBSERVATION_FAILED",
          "requires_reconcile": True}, "AMBIGUOUS_ACK"),
        ({"status": "BROKER_SUBMIT_RESULT_UNKNOWN"}, "AMBIGUOUS_ACK"),
        ({"status": "ACK"}, "ACK"),
        ({"status": "BLOCKED", "pre_submit_tp_block": True,
          "safe_to_release_tp_pending": False}, "AMBIGUOUS_ACK"),
        ({"status": "BLOCKED", "pre_submit_tp_block": True,
          "safe_to_release_tp_pending": True}, "FAILED"),
    ],
)
def test_tp_router_status_requires_complete_pre_http_proof(router, expected):
    from trader.us.runner.trade_tick_runner import _profit_capture_router_status
    assert _profit_capture_router_status(router) == expected


@pytest.mark.skipif(
    not os.getenv("PBCORE_TEST_POSTGRES_URL"),
    reason="disposable PostgreSQL acceptance URL unavailable",
)
def test_pg_tp_claim_reservation_rearms_only_unsent_intent(pg_engine, monkeypatch):
    """Real migrated PG: old risk BLOCKED intent -> fresh Claim -> TP -> HTTP gate."""
    from datetime import date, datetime, timezone
    from uuid import uuid4
    from sqlalchemy import text
    from trader.execution_claims import DurableExecutionClaimRepo
    from trader.execution_state import SemanticActionIdentity
    from trader.us.db.execution_claim_schema import (
        us_execution_attempts, us_execution_claims,
    )
    td, epoch, account = "2026-10-29", "pg-us353-rearm-epoch", "pg-us353-rearm-account"
    sym, cycle, key, prior, attempt = (
        "REGTP", "rearm-buy-cycle", "US_PC_2026-10-29_REGTP_rearm-buy-cycle_TAKE_PROFIT_TP1",
        "preclaim-blocked", "claimed-now",
    )
    monkeypatch.setattr(repos, "_active_us_epoch", lambda _bind=None, **_kw: epoch)
    monkeypatch.setattr(repos, "_us_execution_claim_scope", lambda: {
        "env": "practice", "account_id": account, "market": "US",
        "trading_epoch_id": epoch,
    })
    old_meta = {
        "strategy_owner": "US_STANDARD", "position_lifecycle_id": cycle,
        "profit_capture_stage": "tp1", "submit_attempt_id": prior,
        "tp_pending_deferred_to_claim": True, "source_entry_contract_sha256": "signed-v2",
    }
    assert repos.save_order_intent({
        "trade_date": td, "symbol": sym, "side": "SELL", "qty": 3,
        "limit_price": 101, "notional_usd": 303, "client_order_key": key,
        "meta": old_meta, "strategy_owner": "US_STANDARD",
    }, trade_date=td)
    repos.mark_order_intent_blocked(key, reason="risk_gate_preclaim_blocked")
    with pg_engine.connect() as conn:
        assert conn.execute(text(
            "SELECT status FROM us_order_intents WHERE client_order_key=:key"
        ), {"key": key}).scalar_one() == "BLOCKED"
        assert conn.execute(text(
            "SELECT COUNT(*) FROM us_profit_capture_lifecycle WHERE symbol=:symbol"
        ), {"symbol": sym}).scalar_one() == 0

    claim_repo = DurableExecutionClaimRepo(
        pg_engine, us_execution_claims, us_execution_attempts,
    )
    identity = SemanticActionIdentity(
        env="practice", account_id=account, market="US",
        trading_epoch_id=epoch, strategy_owner="US_STANDARD",
        lifecycle_id=cycle, action="TAKE_PROFIT_TP1",
        trade_date=date.fromisoformat(td),
    )
    assert claim_repo.acquire(
        identity, attempt_id=attempt, requested_qty=3,
        client_order_key=key,
    ).acquired
    new_meta = {
        **old_meta, "submit_attempt_id": attempt,
        "last_profit_capture_at": datetime.now(timezone.utc).isoformat(),
    }
    args = {
        "trade_date": td, "symbol": sym, "stage": "tp1",
        "position_lifecycle_id": cycle, "client_order_key": key,
        "attempt_id": attempt, "action_key": identity.action_key,
    }
    assert repos.reserve_us_profit_capture_stage_after_claim(
        **args, qty=3, notional_usd=315, limit_price_usd=105,
        intent_meta=new_meta,
    )
    assert not repos.reserve_us_profit_capture_stage_after_claim(
        **args, qty=3, notional_usd=315, limit_price_usd=105,
        intent_meta=new_meta,
    ), "second concurrent Claim must never reserve an existing TP"
    with pg_engine.connect() as conn:
        intent = conn.execute(text(
            "SELECT status,qty,limit_price_usd,notional_usd,meta "
            "FROM us_order_intents WHERE client_order_key=:key"
        ), {"key": key}).mappings().one()
        assert intent["status"] == "PENDING"
        assert intent["qty"] == 3
        assert float(intent["limit_price_usd"]) == 105
        assert intent["meta"]["submit_attempt_id"] == attempt
        assert intent["meta"]["source_entry_contract_sha256"] == "signed-v2"
    assert not repos.verify_us_tp_claim_stage_at_http_boundary(**args), (
        "a Claim without a durable STARTED event cannot cross HTTP"
    )

    def event(kind, suffix):
        return {
            "event_id": str(uuid4()), "trade_date": td, "event_type": kind,
            "event_timestamp": datetime.now(timezone.utc).isoformat(),
            "session": "am", "session_run_id": "pg-us353-native",
            "tick_id": "pg-us353-native-tick", "trading_epoch_id": epoch,
            "client_order_key": key, "submit_attempt_id": attempt,
            "symbol": sym, "side": "SELL", "requested_qty": 3,
            "raw_broker_order_no": None, "canonical_broker_order_no": None,
            "position_lifecycle_id": cycle, "profit_capture_stage": "tp1",
            "cumulative_filled_qty": 0,
            "payload": {"strategy_owner": "US_STANDARD"},
            "idempotency_key": "pg-us353-"+suffix,
        }
    assert repos.append_us_order_event(event("BROKER_SUBMIT_STARTED", "started"))
    assert repos.verify_us_tp_claim_stage_at_http_boundary(**args)
    assert not repos.verify_us_tp_claim_stage_at_http_boundary({
        **args, "attempt_id": "different_attempt",
    }), "another writer cannot submit against our stage/Claim"

    # Actual broker-possible evidence must never authorize a second POST.
    assert repos.append_us_order_event(event("BROKER_SUBMIT_RESULT_UNKNOWN", "unknown"))
    assert not repos.verify_us_tp_claim_stage_at_http_boundary(**args)
    assert not repos.release_us_profit_capture_pre_http_abort(
        trade_date=td, symbol=sym, stage="tp1",
        position_lifecycle_id=cycle, client_order_key=key,
    )


@pytest.mark.skipif(
    not os.getenv("PBCORE_TEST_POSTGRES_URL"),
    reason="disposable PostgreSQL acceptance URL unavailable",
)
def test_pg_tp_claim_reservation_rejects_wrong_scope_or_contract(pg_engine, monkeypatch):
    from datetime import date
    from trader.execution_claims import DurableExecutionClaimRepo
    from trader.execution_state import SemanticActionIdentity
    from trader.us.db.execution_claim_schema import (
        us_execution_attempts, us_execution_claims,
    )
    td, epoch, account = "2026-10-29", "pg-us353-foreign-epoch", "pg-us353-account"
    sym, cycle, key, attempt = "SCOPE", "cycle-scope", "US_PC_scope_TP1", "scope-attempt"
    monkeypatch.setattr(repos, "_active_us_epoch", lambda _bind=None, **_kw: epoch)
    monkeypatch.setattr(repos, "_us_execution_claim_scope", lambda: {
        "env": "practice", "account_id": account, "market": "US",
        "trading_epoch_id": epoch,
    })
    old_meta = {
        "strategy_owner": "US_STANDARD", "position_lifecycle_id": cycle,
        "source_entry_contract_sha256": "signed-old",
        "profit_capture_stage": "tp1", "submit_attempt_id": "old-preclaim",
        "tp_pending_deferred_to_claim": True,
    }
    assert repos.save_order_intent({
        "trade_date": td, "symbol": sym, "side": "SELL", "qty": 2,
        "limit_price": 100, "notional_usd": 200,
        "client_order_key": key, "meta": old_meta,
    }, trade_date=td)
    repos.mark_order_intent_blocked(key, reason="no_broker_submission")
    claim_repo = DurableExecutionClaimRepo(
        pg_engine, us_execution_claims, us_execution_attempts,
    )
    identity = SemanticActionIdentity(
        env="practice", account_id=account, market="US",
        trading_epoch_id=epoch, strategy_owner="US_STANDARD",
        lifecycle_id=cycle, action="TAKE_PROFIT_TP1",
        trade_date=date.fromisoformat(td),
    )
    assert claim_repo.acquire(
        identity, attempt_id=attempt, requested_qty=2, client_order_key=key,
    ).acquired
    args = {
        "trade_date": td, "symbol": sym, "stage": "tp1",
        "position_lifecycle_id": cycle, "client_order_key": key,
        "attempt_id": attempt, "action_key": identity.action_key,
        "qty": 2, "notional_usd": 204, "limit_price_usd": 102,
    }
    assert not repos.reserve_us_profit_capture_stage_after_claim(
        **args, intent_meta={**old_meta, "submit_attempt_id": attempt,
                             "source_entry_contract_sha256": "signed-different"},
    )
    monkeypatch.setattr(repos, "_us_execution_claim_scope", lambda: {
        "env": "practice", "account_id": "other-account",
        "market": "US", "trading_epoch_id": epoch,
    })
    assert not repos.reserve_us_profit_capture_stage_after_claim(
        **args, intent_meta={**old_meta, "submit_attempt_id": attempt},
    )
