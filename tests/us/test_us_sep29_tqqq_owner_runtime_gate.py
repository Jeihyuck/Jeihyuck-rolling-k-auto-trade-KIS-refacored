from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from trader.us.infinite.integration import run_sleeve
from trader.us.infinite.models import InfiniteState, Status
from trader.us.infinite.risk_adapter import (
    apply_tqqq_runtime_entry_override,
    effective_regime,
)
from trader.us.runner.tick_entry_permission import (
    resolve_shared_tick_entry_evaluation_permission,
    resolve_tqqq_policy_entry_override_permission,
)


def _overlay(**overrides):
    base = {
        "market_state": "DEFENSE_RISK_OFF",
        "market_regime": "DEFENSIVE",
        "entry_can_proceed": False,
        "exit_can_proceed": True,
        "tqqq_policy_override_allowed": True,
        "reconcile_entry_block": False,
        "hard_system_failure": False,
        "trade_block_reason": "sector_cap_violation_block",
        "tqqq_context_quality": "ok",
        "tqqq_quote_stale": False,
        "opening_buy_blocked": False,
        "qqq_completed_close": 100.0,
        "qqq_ma50": 99.0,
        "qqq_ma200": 98.0,
        "qqq_ma200_slope": 0.1,
        "qqq_20d_return": 0.02,
        "qqq_drawdown_252": -0.05,
        "qqq_realized_vol_20d": 0.2,
        "qqq_trend_efficiency_20d": 0.5,
    }
    base.update(overrides)
    base["tqqq_runtime_gate"] = {
        "entry_block_reason": base["trade_block_reason"],
        "entry_block_source": "authoritative_session_prep_guard",
        "prep_run_id": "fixture-prep-run",
        "prep_recovery_verified": True,
        "override_authorized": bool(base["tqqq_policy_override_allowed"]),
        "context_quality": base["tqqq_context_quality"],
        "quote_stale": bool(base["tqqq_quote_stale"]),
        "exit_can_proceed": bool(base["exit_can_proceed"]),
        "reconcile_entry_block": bool(base.get("reconcile_entry_block", False)),
        "hard_system_failure": bool(base.get("hard_system_failure", False)),
    }
    if isinstance(overrides.get("tqqq_runtime_gate"), dict):
        base["tqqq_runtime_gate"].update(overrides["tqqq_runtime_gate"])
    return base


def _guard(**overrides):
    base = {
        "guard_state": "PREP_DEGRADED_ENTRY_BLOCKED",
        "entry_can_proceed": False,
        "exit_can_proceed": True,
        "close_can_proceed": True,
        "trade_block_reason": "sector_cap_violation_block",
        "run_revision_mismatch": False,
        "ok": True,
        "prep_run_id": "fixture-prep-run",
    }
    base.update(overrides)
    return base


class _FakeRepository:
    def __init__(self, state: InfiniteState):
        self.state = state

    def ensure_schema(self):
        return None

    def load_state(self, **_):
        return self.state

    def save_state(self, state):
        self.state = state

    def pending_sides(self, *_):
        return False, False

    def has_pending_infinite_order(self, *_):
        return False

    def fill_accounting(self, *_):
        return 0, 0, 0, None, None

    def reconcile_metadata(self, state, **_):
        return state


def _sep29_state() -> InfiniteState:
    return InfiniteState(
        cycle_id="sep29-cycle",
        cycle_start_date=date(2026, 9, 1),
        core_filled_notional=477.9879,
        last_buy_date=date(2026, 9, 23),
        status=Status.ACTIVE,
        metadata={
            "last_buy_fill_price": 79.0793,
            "long_trend": "BULL",
            "strategy_owner": "TQQQ_INFINITE",
        },
    )


def test_pb1_sector_cap_does_not_reblock_healthy_tqqq_owner():
    overlay = _overlay()

    regime, multiplier, _reserve, entry_allowed, _reason = effective_regime(overlay)

    assert regime in {"RISK_OFF", "DEFENSIVE"}
    assert multiplier == 0.5
    assert entry_allowed is True
    assert overlay["entry_can_proceed"] is True
    assert overlay["tqqq_runtime_entry_override"] is True
    assert overlay["tqqq_runtime_entry_override_reason"] == "sector_cap_violation_block"


def test_unknown_operational_block_stays_fail_closed_for_tqqq():
    overlay = _overlay(trade_block_reason="prep_missing_after_preflight_recovery")

    changed, reason = apply_tqqq_runtime_entry_override(overlay)

    assert changed is False
    assert reason == "prep_missing_after_preflight_recovery"
    assert overlay["entry_can_proceed"] is False


def test_pb1_override_requires_authoritative_session_authorization():
    overlay = _overlay(tqqq_policy_override_allowed=False)

    changed, reason = apply_tqqq_runtime_entry_override(overlay)

    assert changed is False
    assert reason == "tqqq_pb1_policy_override_not_authorized"
    assert overlay["entry_can_proceed"] is False


def test_stale_tqqq_quote_cannot_bypass_pb1_gate():
    overlay = _overlay(tqqq_quote_stale=True)

    changed, reason = apply_tqqq_runtime_entry_override(overlay)

    assert changed is False
    assert reason == "tqqq_operational_safety_not_proven"
    assert overlay["entry_can_proceed"] is False


def test_tqqq_override_requires_authoritative_prep_reason_provenance():
    overlay = _overlay(tqqq_runtime_gate={
        "entry_block_source": "prep_overlay_cache",
    })

    changed, reason = apply_tqqq_runtime_entry_override(overlay)

    assert changed is False
    assert reason == "authoritative_prep_recovery_not_proven"
    assert overlay["entry_can_proceed"] is False


@pytest.mark.parametrize(
    "safety_override",
    [
        {"reconcile_entry_block": True},
        {"hard_system_failure": True},
        {"exit_can_proceed": False},
        {"tqqq_runtime_gate": {"prep_recovery_verified": False}},
    ],
)
def test_operational_safety_blocks_tqqq_policy_override(safety_override):
    overlay = _overlay(**safety_override)

    changed, reason = apply_tqqq_runtime_entry_override(overlay)

    assert changed is False
    assert reason in {
        "authoritative_prep_recovery_not_proven",
        "tqqq_operational_safety_not_proven",
    }
    assert overlay["entry_can_proceed"] is False


def test_session_policy_only_block_does_not_promote_shared_pb1_permission():
    assert resolve_shared_tick_entry_evaluation_permission(
        _guard(), timeout_entry_block=False, session_execution_mode="NORMAL"
    ) is False


def test_tqqq_owner_override_authorized_only_for_verified_policy_block():
    guard = _guard()
    assert resolve_tqqq_policy_entry_override_permission(
        guard, timeout_entry_block=False, session_execution_mode="NORMAL",
    )
    assert not resolve_tqqq_policy_entry_override_permission(
        guard, timeout_entry_block=True, session_execution_mode="NORMAL",
    )
    assert not resolve_tqqq_policy_entry_override_permission(
        guard, timeout_entry_block=False, session_execution_mode="SAFE_DEGRADED",
    )
    assert not resolve_tqqq_policy_entry_override_permission(
        _guard(prep_run_id=""), timeout_entry_block=False, session_execution_mode="NORMAL",
    )
    assert not resolve_tqqq_policy_entry_override_permission(
        _guard(trade_block_reason="prep_missing_after_preflight_recovery"),
        timeout_entry_block=False, session_execution_mode="NORMAL",
    )


def test_authoritative_guard_entry_allowed_keeps_shared_permission_true():
    assert resolve_shared_tick_entry_evaluation_permission(
        _guard(
            guard_state="OK",
            entry_can_proceed=True,
            trade_block_reason="",
        ),
        timeout_entry_block=False,
        session_execution_mode="NORMAL",
    ) is True


def test_preflight_exit_only_never_promotes_shared_entry():
    assert resolve_shared_tick_entry_evaluation_permission(
        _guard(
            guard_state="PREFLIGHT_EXIT_ONLY",
            trade_block_reason="prep_missing_after_preflight_recovery",
        ),
        timeout_entry_block=False,
        session_execution_mode="NORMAL",
    ) is False


def test_revision_mismatch_never_promotes_blocked_shared_permission():
    assert resolve_shared_tick_entry_evaluation_permission(
        _guard(run_revision_mismatch=True),
        timeout_entry_block=False,
        session_execution_mode="NORMAL",
    ) is False
    assert resolve_shared_tick_entry_evaluation_permission(
        _guard(run_revision_mismatch=True, revision_mismatch_recovery_allowed=True),
        timeout_entry_block=False,
        session_execution_mode="NORMAL",
    ) is False
    assert resolve_shared_tick_entry_evaluation_permission(
        _guard(
            guard_state="OK",
            entry_can_proceed=True,
            trade_block_reason="",
            run_revision_mismatch=True,
            revision_mismatch_recovery_allowed=True,
        ),
        timeout_entry_block=False,
        session_execution_mode="NORMAL",
    ) is True


def test_safe_degraded_and_timeout_remain_global_buy_fences():
    allowed_guard = _guard(guard_state="OK", entry_can_proceed=True, trade_block_reason="")
    assert resolve_shared_tick_entry_evaluation_permission(
        allowed_guard, timeout_entry_block=False, session_execution_mode="SAFE_DEGRADED"
    ) is False
    assert resolve_shared_tick_entry_evaluation_permission(
        allowed_guard, timeout_entry_block=True, session_execution_mode="NORMAL"
    ) is False


def test_unknown_or_noncanonical_degraded_reason_remains_fail_closed():
    assert resolve_shared_tick_entry_evaluation_permission(
        _guard(trade_block_reason="data_sync_quality_entry_block"),
        timeout_entry_block=False,
        session_execution_mode="NORMAL",
    ) is False
    assert resolve_shared_tick_entry_evaluation_permission(
        _guard(guard_state="PREP_MISSING_EXIT_ONLY"),
        timeout_entry_block=False,
        session_execution_mode="NORMAL",
    ) is False


def test_live_session_runner_keeps_pb1_blocked_when_db_cache_is_stale_allowed(monkeypatch, tmp_path):
    """Latest artifact guard block must win over an older DB entry-allowed cache."""
    from trader.us.runner.trade_session_runner import run_trade_session

    seen: dict = {}
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("GITHUB_RUN_ID", "sep29-test")
    monkeypatch.setenv("GITHUB_SHA", "current-sha")
    monkeypatch.setenv("US_DISABLE_TICK_PROCESS_ISOLATION", "1")
    monkeypatch.setenv("US_KIS_ORDER_ALLOWED", "0")
    monkeypatch.setenv("DRY_RUN", "1")

    monkeypatch.setattr(
        "trader.us.market_calendar.now_ny",
        lambda: datetime(2026, 9, 29, 10, 6, tzinfo=ZoneInfo("America/New_York")),
    )
    monkeypatch.setattr("trader.us.market_calendar.is_us_trading_day", lambda _d: True)
    monkeypatch.setattr(
        "trader.us.utils.session_guard.acquire_us_session_running_lock",
        lambda *a, **k: {"acquired": True},
    )
    monkeypatch.setattr("trader.us.utils.session_guard.release_us_session_running_lock", lambda *a, **k: None)
    monkeypatch.setattr(
        "trader.us.utils.session_guard.check_us_session_file_guard",
        lambda trade_date, session: {"already_ran": False, "guard_status": "OK"},
    )
    monkeypatch.setattr("trader.us.utils.session_guard.now_et_iso", lambda: "2026-09-29T10:06:00-04:00")
    monkeypatch.setattr("trader.us.utils.session_guard.write_us_session_done_file", lambda **_k: None)
    monkeypatch.setattr("trader.us.budget.resolve_us_order_budget", lambda _cash: {"capital_usd_cap": 10_000.0})
    monkeypatch.setattr(
        "trader.us.prep_contract.check_us_prep_guard",
        lambda trade_date, session=None: {
            "ok": True,
            **_guard(),
            "status": "OK_WITH_WARNINGS_ENTRY_BLOCKED_CLUSTER_CAP",
            "prep_status": "OK_WITH_WARNINGS_ENTRY_BLOCKED_CLUSTER_CAP",
            "final30_scored_count": 28,
            "score_nonzero_count": 28,
            "final30_trade_ready": True,
            "contract": {"entry_can_proceed": 0, "exit_can_proceed": 1},
        },
    )
    monkeypatch.setattr(
        "trader.us.run_manifest.verify_run_revision",
        lambda *a, **k: {"ok": True, "entry_can_proceed": True, "expected_revision": "current-sha"},
    )
    # Simulate the P1 review case: DB still exposes an older entry-allowed PREP.
    monkeypatch.setattr(
        "trader.us.db.repos.load_latest_us_prep_status",
        lambda *a, **k: {
            "status": "OK",
            "run_id": "older-db-prep",
            "result": {
                "entry_can_proceed": 1,
                "trade_can_proceed": 1,
                "final30_trade_ready": True,
                "cluster_contract_ok": True,
                "cap_violations": [],
                "trade_block_reason": "",
            },
        },
    )
    monkeypatch.setattr(
        "trader.us.db.repos.load_locked_us_watchlist",
        lambda *a, **k: [{"symbol": f"S{i}"} for i in range(28)],
    )
    monkeypatch.setattr("trader.us.db.repos.load_us_daily_orders_for_report", lambda *a, **k: [])
    monkeypatch.setattr("trader.us.db.repos.load_today_fills", lambda *a, **k: [])
    monkeypatch.setattr("trader.us.execution.order_journal.aggregate_order_events", lambda *a, **k: {})
    monkeypatch.setattr("trader.us.execution.order_journal.load_order_events", lambda *a, **k: [])
    monkeypatch.setattr(
        "trader.us.runner.trade_tick_runner.evaluate_balance_error_circuit",
        lambda *a, **k: {
            "balance_reconcile_degraded": False,
            "entry_blocked_by_balance_degraded": False,
            "balance_consecutive_failed_ticks": 0,
            "entry_block_reasons": [],
        },
    )
    monkeypatch.setattr("trader.us.runner.trade_session_runner._write_us_session_report", lambda *a, **k: None)
    monkeypatch.setattr("trader.us.runner.trade_session_runner._write_us_schedule_health", lambda *a, **k: None)

    def _run_tick(**kwargs):
        seen.update(kwargs)
        return {
            "status": "OK",
            "last_stage": "done",
            "prep_status": "OK",
            "locked_watchlist_count": 28,
            "entry_eval_status": "BLOCKED_BY_AUTHORITATIVE_GUARD",
            "entry_intents": 0,
            "exit_intents": 0,
            "orders_sent": 0,
            "orders_ack": 0,
            "orders_rejected": 0,
            "orders_error": 0,
            "fills": 0,
            "positions": 1,
            "trade_block_reason": "sector_cap_violation_block",
        }

    monkeypatch.setattr("trader.us.runner.trade_tick_runner.run_trade_tick", _run_tick)

    run_trade_session(
        session="am",
        env="practice",
        offline=False,
        max_minutes=1,
        interval_sec=1,
        max_ticks=1,
        force_now="2026-09-29T10:06:00-04:00",
    )

    assert seen["entry_can_proceed"] is False
    assert seen["exit_can_proceed"] is True
    assert seen["entry_block_reason"] == "sector_cap_violation_block"
    assert seen["tqqq_policy_override_allowed"] is True


def test_sep29_fast_dip_reaches_router_via_tqqq_owner_override(monkeypatch):
    monkeypatch.setenv("US_TQQQ_INFINITE_ENABLED", "1")
    monkeypatch.setenv("US_TQQQ_INFINITE_REAL_ORDER", "1")

    routed: list[dict] = []
    # Shared PB1 permission stays False. The dedicated Infinite sleeve releases
    # only the canonical PB1 policy block in its own overlay.
    overlay = _overlay(entry_can_proceed=False, trade_block_reason="sector_cap_violation_block")
    result = run_sleeve(
        positions=[{
            "symbol": "TQQQ",
            "qty": 6,
            "orderable_qty": 6,
            "avg_price": 79.664,
            "strategy_owner": "TQQQ_INFINITE",
        }],
        price=77.49,
        trading_date=date(2026, 9, 29),
        overlay=overlay,
        repository=_FakeRepository(_sep29_state()),
        route=lambda intent: (routed.append(intent) or {"status": "ACK"}),
    )

    assert result["decision"].action.value == "BUY"
    assert result["decision"].reason == "FAST_DIP_ADD_BUY"
    assert result["status"] == "ACK"
    assert len(routed) == 1
    assert routed[0]["side"] == "BUY"
    assert routed[0]["qty"] == 3
    assert routed[0]["reason"] == "FAST_DIP_ADD_BUY"


def test_sep29_fast_dip_operational_block_never_reaches_router(monkeypatch):
    monkeypatch.setenv("US_TQQQ_INFINITE_ENABLED", "1")
    monkeypatch.setenv("US_TQQQ_INFINITE_REAL_ORDER", "1")

    routed: list[dict] = []
    result = run_sleeve(
        positions=[{
            "symbol": "TQQQ",
            "qty": 6,
            "orderable_qty": 6,
            "avg_price": 79.664,
            "strategy_owner": "TQQQ_INFINITE",
        }],
        price=77.49,
        trading_date=date(2026, 9, 29),
        overlay=_overlay(
            entry_can_proceed=False,
            trade_block_reason="prep_missing_after_preflight_recovery",
        ),
        repository=_FakeRepository(_sep29_state()),
        route=lambda intent: (routed.append(intent) or {"status": "ACK"}),
    )

    assert result["decision"].action.value == "BUY"
    assert result["decision"].reason == "FAST_DIP_ADD_BUY"
    assert result["status"] == "WAIT"
    assert result["reason"] == "SESSION_SAFE_DEGRADED"
    assert routed == []


def _replay_session_tick_tqqq(
    monkeypatch, tmp_path, fixture_name, operational_block, *, include_pb1_position=False,
    use_session_lock=False,
):
    from types import SimpleNamespace

    from trader.us.execution import order_router
    from trader.us.infinite import integration
    from trader.us.infinite.models import PositionSnapshot
    from trader.us.infinite.repository import InfiniteRepository
    from trader.us.runner import trade_tick_runner
    from trader.us.runner.trade_session_runner import run_trade_session
    from trader.us.runner.trade_tick_runner import run_trade_tick as production_tick

    fixture = json.loads(
        (Path(__file__).parent / "fixtures/runtime_integrity" / fixture_name).read_text()
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("GITHUB_RUN_ID", fixture["session"]["run_id"])
    monkeypatch.setenv("GITHUB_SHA", fixture["code_sha"])
    for name, value in fixture["settings"].items():
        monkeypatch.setenv(name, value)

    incident_now = datetime.fromisoformat(fixture["clock"]["timestamp"])
    if use_session_lock:
        import trader.us.utils.session_guard as session_guard

        real_datetime = session_guard.datetime

        class ReplayDateTime(real_datetime):
            @classmethod
            def now(cls, tz=None):
                return incident_now.astimezone(tz) if tz else incident_now.replace(tzinfo=None)

        monkeypatch.setattr(session_guard, "datetime", ReplayDateTime)
    monkeypatch.setattr(
        "trader.us.market_calendar.now_ny",
        lambda: incident_now,
    )
    monkeypatch.setattr("trader.us.market_calendar.is_us_trading_day", lambda _d: True)
    if not use_session_lock:
        monkeypatch.setattr(
            "trader.us.utils.session_guard.acquire_us_session_running_lock",
            lambda *a, **k: {"acquired": True},
        )
        monkeypatch.setattr("trader.us.utils.session_guard.release_us_session_running_lock", lambda *a, **k: None)
    monkeypatch.setattr(
        "trader.us.utils.session_guard.check_us_session_file_guard",
        lambda trade_date, session: {"already_ran": False, "guard_status": "OK"},
    )
    monkeypatch.setattr("trader.us.utils.session_guard.now_et_iso", lambda: fixture["clock"]["timestamp"])
    monkeypatch.setattr("trader.us.utils.session_guard.write_us_session_done_file", lambda **_k: None)
    monkeypatch.setattr("trader.us.budget.resolve_us_order_budget", lambda _cash: {
        "effective_order_budget_usd": 10_000.0, "capital_usd_cap": 10_000.0,
    })
    guard = _guard(
        ok=True,
        prep_run_id=fixture["prep"]["run_id"],
        trade_block_reason=fixture["prep"]["trade_block_reason"],
    )
    monkeypatch.setattr("trader.us.prep_contract.check_us_prep_guard", lambda *a, **k: {
        **guard, "status": "OK_WITH_WARNINGS_ENTRY_BLOCKED_CLUSTER_CAP",
        "prep_status": "OK_WITH_WARNINGS_ENTRY_BLOCKED_CLUSTER_CAP",
        "final30_scored_count": 28, "score_nonzero_count": 28,
        "final30_trade_ready": True, "contract": {"entry_can_proceed": 0, "exit_can_proceed": 1},
    })
    monkeypatch.setattr(
        "trader.us.run_manifest.verify_run_revision",
        lambda *a, **k: {"ok": True, "entry_can_proceed": True, "expected_revision": fixture["code_sha"]},
    )
    monkeypatch.setattr("trader.us.db.repos.load_latest_us_prep_status", lambda *a, **k: {
        "status": fixture["prep"]["status"], "run_id": fixture["prep"]["run_id"],
        "result": {
            "entry_can_proceed": int(fixture["prep"]["entry_can_proceed"]),
            "trade_block_reason": fixture["prep"]["trade_block_reason"],
        },
    })
    monkeypatch.setattr(
        "trader.us.db.repos.load_locked_us_watchlist",
        lambda *a, **k: [{"symbol": f"FIX{i}"} for i in range(28)],
    )
    monkeypatch.setattr("trader.us.db.repos.load_us_daily_orders_for_report", lambda *a, **k: [])
    monkeypatch.setattr("trader.us.db.repos.load_today_fills", lambda *a, **k: [])
    monkeypatch.setattr("trader.us.execution.order_journal.aggregate_order_events", lambda *a, **k: {})
    monkeypatch.setattr("trader.us.execution.order_journal.load_order_events", lambda *a, **k: [])
    monkeypatch.setattr("trader.us.runner.trade_session_runner._write_us_session_report", lambda *a, **k: None)
    monkeypatch.setattr("trader.us.runner.trade_session_runner._write_us_schedule_health", lambda *a, **k: None)

    position = {
        **fixture["initial_positions"][0],
        "current_px": fixture["prices"]["TQQQ"],
        "authoritative_positions": True,
    }
    fixture_positions = [position]
    if include_pb1_position:
        fixture_positions.append({
            "symbol": "HELD_FIXTURE",
            "exchange": "NASDAQ",
            "qty": 2,
            "orderable_qty": 2,
            "avg_price": 50.0,
            "current_px": 51.0,
            "strategy_owner": "US_STANDARD",
            "owner": "US_STANDARD",
            "authoritative_positions": True,
        })
    monkeypatch.setattr("trader.us.data_provider.USDataProvider", lambda **_k: SimpleNamespace(
        stats={}, _get_client=lambda: SimpleNamespace(stats={}),
        get_current_price=lambda symbol, exchange: {
            "last": fixture["prices"]["TQQQ"] if symbol == "TQQQ" else fixture["prices"]["QQQ_completed_close"],
            "source": "fixture", "stale": False,
        },
        get_orderable_cash=lambda **_k: 10_000.0,
    ))
    fixture_reconcile = {
        "status": "OK", "positions": [dict(row) for row in fixture_positions],
        "position_count": len(fixture_positions),
        "position_symbols": [row["symbol"] for row in fixture_positions],
        "balance_fetch_status": "OK",
        "balance_parse_status": "OK", "authoritative_positions": True,
        "preserve_previous_positions": False, "total_pvs": 10_000.0,
        "total_pvs_semantics": "holdings_market_value_usd",
        "account_equity_usd": 10_000.0, "account_equity_source": "fixture",
    }
    monkeypatch.setattr(
        "trader.us.execution.reconcile.reconcile_positions",
        lambda **_k: dict(fixture_reconcile),
    )
    monkeypatch.setattr("trader.us.db.repos.save_position_snapshot", lambda *a, **k: True)
    monkeypatch.setattr("trader.us.db.repos.save_reconcile_log", lambda *a, **k: True)
    monkeypatch.setattr("trader.us.db.repos.save_fills", lambda *a, **k: 0)
    monkeypatch.setattr("trader.us.db.repos.load_pending_ack_orders", lambda *a, **k: [])
    monkeypatch.setattr("trader.us.db.repos.load_today_order_keys", lambda *a, **k: set())
    monkeypatch.setattr("trader.us.db.repos.load_today_symbols_sold", lambda *a, **k: set())
    monkeypatch.setattr("trader.us.db.repos.load_today_committed_buy_notional", lambda *a, **k: 0.0)
    monkeypatch.setattr("trader.us.db.repos.get_today_buy_orders_count", lambda *a, **k: 0)
    monkeypatch.setattr("trader.us.runner.trade_tick_runner.evaluate_balance_error_circuit", lambda *a, **k: {
        "entry_can_proceed": True, "balance_reconcile_degraded": False,
        "entry_blocked_by_balance_degraded": False, "balance_consecutive_failed_ticks": 0,
        "entry_block_reasons": [],
    })
    monkeypatch.setattr("trader.us.execution.fills.get_fills_today", lambda **_k: {"status": "OK", "fills": []})
    monkeypatch.setattr("trader.us.execution.reconcile.reconcile_ack_orders_with_balance", lambda **_k: {
        "status": "OK", "pending_count": 0, "confirmed_count": 0,
        "balance_reconcile_count": 0, "unresolved_count": 0,
    }    )
    if operational_block == "stale_quote":
        monkeypatch.setattr(
            trade_tick_runner,
            "_get_tqqq_tick_quote",
            lambda *_a, **_k: (fixture["prices"]["TQQQ"], "fixture", True),
        )
    elif operational_block == "reconcile":
        monkeypatch.setattr(
            "trader.us.execution.reconcile.reconcile_ack_orders_with_balance",
            lambda **_k: {
                "status": "WARN", "pending_count": 1, "confirmed_count": 0,
                "balance_reconcile_count": 0, "unresolved_count": 1,
            },
        )
    elif operational_block == "hard_failure":
        monkeypatch.setattr(
            "trader.us.execution.reconcile.reconcile_positions",
            lambda **_k: {**fixture_reconcile, "block_new_entry": True},
        )
    monkeypatch.setattr(trade_tick_runner, "_build_tqqq_ttl_callbacks", lambda *_a, **_k: (None, None))
    monkeypatch.setattr("trader.us.position_lifecycle_state.reconcile_us_position_lifecycles", lambda **_k: {})
    monkeypatch.setattr("trader.us.pb1.us_exit_position_resolver.enrich_us_positions_for_exit", lambda positions, **_k: (positions, {"total": len(positions), "ok": len(positions), "missing": 0, "sources": {}}))
    positions_seen_by_tick = []

    def capture_tick_positions(**kwargs):
        positions_seen_by_tick.extend(kwargs["positions"])
        return (
            kwargs["positions"],
            {},
            kwargs["locked_watchlist_cache"],
            kwargs["watchlist_cache_source"],
        )

    monkeypatch.setattr(trade_tick_runner, "_update_position_trends_for_tick", capture_tick_positions)
    monkeypatch.setattr(
        "trader.us.market_state_overlay.evaluate_us_market_state",
        lambda **_k: _overlay(
            qqq_completed_close=fixture["prices"]["QQQ_completed_close"],
            qqq_ma50=fixture["prices"]["QQQ_ma50"],
            qqq_ma200=fixture["prices"]["QQQ_ma200"],
        ),
    )
    monkeypatch.setattr("trader.us.market_state_overlay.build_profit_capture_intents", lambda *_a, **_k: [])
    monkeypatch.setattr("trader.us.portfolio_cluster_guard.evaluate_portfolio_cluster_guard", lambda *a, **k: {
        "portfolio_cluster_guard_status": "OK", "portfolio_ai_tech_weight": 0.0,
        "portfolio_cluster_cap_violations": [], "cluster_guard_trim_intents": [],
        "cluster_guard_trim_notional": 0.0,
    })
    monkeypatch.setattr("trader.us.market_state_overlay.build_defense_trim_intents", lambda *a, **k: [])

    class Engine:
        def evaluate_exits(self, **_k):
            return []

    monkeypatch.setattr(trade_tick_runner, "_get_strategy_engine", lambda **_k: Engine())

    class ReplayRepository(_FakeRepository, InfiniteRepository):
        def __init__(self):
            _FakeRepository.__init__(self, _sep29_state())

        def backfill_tqqq_attribution(self, *_a, **_k):
            return 0

        def load_open_orders(self, **_k):
            return []

        def pending_buy_notional(self, *_a):
            return 0.0

    monkeypatch.setattr(integration, "InfiniteRepository", ReplayRepository)
    actual_run_sleeve = integration.run_sleeve
    captured_runtime_overlay = {}

    def capture_run_sleeve(**kwargs):
        captured_runtime_overlay.update(kwargs["overlay"])
        return actual_run_sleeve(**kwargs)

    monkeypatch.setattr(integration, "run_sleeve", capture_run_sleeve)
    monkeypatch.setattr(integration, "fetch_fresh_tqqq_preorder_position", lambda: {
        "authoritative": True,
        "position": PositionSnapshot(qty=6, orderable_qty=6, average_price=79.664, price=77.49),
    })
    repos = __import__("trader.us.db.repos", fromlist=["reset_memory_stores"])
    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)
    repos.reset_memory_stores()
    monkeypatch.setattr(repos, "save_order_intent", lambda *a, **k: True)
    monkeypatch.setattr(repos, "save_dry_run_order", lambda *a, **k: True)
    monkeypatch.setattr(repos, "mark_order_intent_dry_run", lambda *a, **k: None)
    monkeypatch.setattr(repos, "load_us_positions_by_symbols", lambda *_a, **_k: {"TQQQ": position})
    monkeypatch.setattr(order_router, "canonical_order_risk_check", lambda *a, **k: None)
    monkeypatch.setattr(order_router, "assert_order_allowed", lambda *a, **k: None)
    monkeypatch.setattr(order_router, "assert_tqqq_infinite_order_allowed", lambda *a, **k: None)

    routed = []
    actual_route_order = order_router.route_order

    def capture_route_order(intent, **kwargs):
        route_result = actual_route_order(intent, **kwargs)
        routed.append((dict(intent), route_result))
        return route_result

    monkeypatch.setattr(order_router, "route_order", capture_route_order)
    seen = {}

    def session_tick(**kwargs):
        seen.update(kwargs)
        if use_session_lock:
            from trader.us.utils.session_guard import acquire_us_session_running_lock

            overlap = acquire_us_session_running_lock(
                fixture["incident_date"], "am", "overlapping-replay-worker",
            )
            seen["overlapping_session_lock_acquired"] = overlap["acquired"]
            if overlap["acquired"]:
                from trader.us.utils.session_guard import release_us_session_running_lock

                release_us_session_running_lock(
                    fixture["incident_date"], "am", "overlapping-replay-worker",
                )
        return production_tick(**kwargs)

    monkeypatch.setattr(trade_tick_runner, "run_trade_tick", session_tick)
    result = run_trade_session(
        session="am", env="practice", offline=False, max_minutes=1, interval_sec=1,
        max_ticks=1, force_now=fixture["clock"]["timestamp"],
    )

    assert seen["entry_can_proceed"] is False
    assert seen["entry_block_reason"] == fixture["prep"]["trade_block_reason"]
    assert seen["tqqq_policy_override_allowed"] is True
    if use_session_lock:
        assert seen["overlapping_session_lock_acquired"] is False
    assert seen["prep_run_id"] == fixture["prep"]["run_id"]
    runtime_gate = captured_runtime_overlay["tqqq_runtime_gate"]
    assert runtime_gate["entry_block_reason"] == fixture["prep"]["trade_block_reason"]
    assert runtime_gate["entry_block_source"] == "authoritative_session_prep_guard"
    assert runtime_gate["prep_run_id"] == fixture["prep"]["run_id"]
    assert runtime_gate["prep_recovery_verified"] is True
    assert runtime_gate["override_authorized"] is True
    if operational_block is None:
        assert runtime_gate == {
            "entry_block_reason": fixture["prep"]["trade_block_reason"],
            "entry_block_source": "authoritative_session_prep_guard",
            "prep_run_id": fixture["prep"]["run_id"],
            "prep_recovery_verified": True,
            "override_authorized": True,
            "context_quality": "ok",
            "quote_stale": False,
            "exit_can_proceed": True,
            "reconcile_entry_block": False,
            "hard_system_failure": False,
        }
        assert [
            (i["symbol"], i["side"], i["reason"], route["status"])
            for i, route in routed
        ] == [
            ("TQQQ", "BUY", "FAST_DIP_ADD_BUY", "DRY_RUN"),
        ]
    else:
        assert routed == []
        if operational_block == "stale_quote":
            assert runtime_gate["quote_stale"] is True
        elif operational_block == "reconcile":
            assert runtime_gate["reconcile_entry_block"] is True
        else:
            assert runtime_gate["hard_system_failure"] is True
    assert result["tick_count"] == 1
    assert result["tick_latency"]["measured_tick_count"] == 1
    assert result["tick_latency"]["max_tick_ms"] > 0
    assert result["tick_latency"]["stage_ms"]["order_reconcile_ms"] >= 0
    if include_pb1_position:
        # PB1 trend/exit must only see its own managed holdings; Infinite
        # still sees TQQQ through the dedicated run_sleeve path above.
        assert {
            (row["symbol"], row.get("strategy_owner"))
            for row in positions_seen_by_tick
        } == {("HELD_FIXTURE", "US_STANDARD")}
    if operational_block is None:
        assert result["runtime_integrity_status"] == "POLICY_ENTRY_BLOCKED"
    elif operational_block == "reconcile":
        assert result["runtime_integrity_status"] == "RECONCILE_REQUIRED"
    else:
        assert result["runtime_integrity_status"] != "OK"
    assert result["sell_liveness_status"] == "OK"


def test_oct1_simultaneous_pb1_and_infinite_positions_keep_owner_gates_isolated(
    monkeypatch, tmp_path,
):
    # The existing session->tick->sleeve->router replay now receives two held
    # owners from the same authoritative balance snapshot.
    _replay_session_tick_tqqq(
        monkeypatch,
        tmp_path,
        "incident_20261001_tqqq_sector_cap.json.fixture",
        None,
        include_pb1_position=True,
    )


@pytest.mark.parametrize("operational_block", [None, "stale_quote", "reconcile", "hard_failure"])
def test_oct1_session_tick_overlay_routes_only_verified_tqqq_policy_override(
    monkeypatch, tmp_path, operational_block,
):
    _replay_session_tick_tqqq(
        monkeypatch,
        tmp_path,
        "incident_20261001_tqqq_sector_cap.json.fixture",
        operational_block,
    )


def test_sep29_executable_session_tick_prep_and_tqqq_replay(monkeypatch, tmp_path):
    from trader.marketdata.kis_ws_price import get_kis_ws_price_service
    from trader.us.utils.session_guard import (
        acquire_us_session_running_lock,
        release_us_session_running_lock,
    )

    _replay_session_tick_tqqq(
        monkeypatch,
        tmp_path,
        "incident_20260929_prep_tqqq.json.fixture",
        None,
        use_session_lock=True,
    )
    lock = acquire_us_session_running_lock("2026-09-29", "am", "replay-lock-check")
    assert lock["acquired"] is True
    assert release_us_session_running_lock(
        "2026-09-29", "am", "replay-lock-check",
    )["released"] is True

    repository_root = Path(__file__).resolve().parents[2]
    prep_wrapper = (repository_root / "scripts/wsl/run-us-prep.sh").read_text(encoding="utf-8")
    trade_wrapper = (repository_root / "scripts/wsl/run-us-am.sh").read_text(encoding="utf-8")
    assert 'export KIS_WS_PRICE_ENABLED="0"' in prep_wrapper
    assert 'export KIS_WS_PRICE_ENABLED="0"' not in trade_wrapper
    assert "KIS_WS_PRICE_ENABLED" not in prep_wrapper.split("source .env", 1)[0]

    ws_service = get_kis_ws_price_service()
    monkeypatch.setattr(ws_service, "_desired", {})
    monkeypatch.setenv("KIS_WS_PRICE_ENABLED", "0")
    monkeypatch.setenv("KIS_WS_PRICE_FORCE_ENABLE", "1")
    assert ws_service.network_allowed("US") is False
    monkeypatch.setenv("KIS_WS_PRICE_ENABLED", "1")
    monkeypatch.setenv("KIS_WS_PRICE_TEST_ENABLE", "1")
    monkeypatch.setenv("US_KIS_HTTP_ENABLED", "1")
    assert ws_service.network_allowed("US") is True
    subscriptions = []
    monkeypatch.setattr(ws_service, "_ensure_started", lambda: subscriptions.append("start"))
    ws_service.subscribe_us("FIXTURE", "NASDAQ")
    ws_service.subscribe_us("FIXTURE", "NASDAQ")
    assert get_kis_ws_price_service() is ws_service
    assert ws_service.stats()["subscriptions"] == 1
    assert subscriptions == ["start", "start"]


def test_session_runner_source_uses_authoritative_shared_permission_helper():
    src = Path("trader/us/runner/trade_session_runner.py").read_text(encoding="utf-8")
    assert "resolve_shared_tick_entry_evaluation_permission" in src
    assert "entry_can_proceed=resolve_shared_tick_entry_evaluation_permission" in src
