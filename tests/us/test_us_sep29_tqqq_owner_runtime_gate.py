from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from trader.us.infinite.integration import run_sleeve
from trader.us.infinite.models import InfiniteState, Status
from trader.us.infinite.risk_adapter import (
    apply_tqqq_runtime_entry_override,
    effective_regime,
)
from trader.us.runner.tick_entry_permission import (
    resolve_shared_tick_entry_evaluation_permission,
)


def _overlay(**overrides):
    base = {
        "market_state": "DEFENSE_RISK_OFF",
        "market_regime": "DEFENSIVE",
        "entry_can_proceed": False,
        "exit_can_proceed": True,
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
    return base


def _guard(**overrides):
    base = {
        "guard_state": "PREP_DEGRADED_ENTRY_BLOCKED",
        "entry_can_proceed": False,
        "exit_can_proceed": True,
        "close_can_proceed": True,
        "trade_block_reason": "sector_cap_violation_block",
        "run_revision_mismatch": False,
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


def test_stale_tqqq_quote_cannot_bypass_pb1_gate():
    overlay = _overlay(tqqq_quote_stale=True)

    changed, reason = apply_tqqq_runtime_entry_override(overlay)

    assert changed is False
    assert reason == "tqqq_operational_safety_not_proven"
    assert overlay["entry_can_proceed"] is False


def test_session_policy_only_block_allows_owner_specific_tick_evaluation():
    assert resolve_shared_tick_entry_evaluation_permission(
        _guard(), timeout_entry_block=False, session_execution_mode="NORMAL"
    ) is True


def test_preflight_exit_only_never_promotes_tqqq_evaluation():
    assert resolve_shared_tick_entry_evaluation_permission(
        _guard(
            guard_state="PREFLIGHT_EXIT_ONLY",
            trade_block_reason="prep_missing_after_preflight_recovery",
        ),
        timeout_entry_block=False,
        session_execution_mode="NORMAL",
    ) is False


def test_revision_mismatch_never_promotes_tqqq_without_validated_recovery():
    assert resolve_shared_tick_entry_evaluation_permission(
        _guard(run_revision_mismatch=True),
        timeout_entry_block=False,
        session_execution_mode="NORMAL",
    ) is False
    assert resolve_shared_tick_entry_evaluation_permission(
        _guard(run_revision_mismatch=True, revision_mismatch_recovery_allowed=True),
        timeout_entry_block=False,
        session_execution_mode="NORMAL",
    ) is True


def test_safe_degraded_and_timeout_remain_global_buy_fences():
    assert resolve_shared_tick_entry_evaluation_permission(
        _guard(), timeout_entry_block=False, session_execution_mode="SAFE_DEGRADED"
    ) is False
    assert resolve_shared_tick_entry_evaluation_permission(
        _guard(), timeout_entry_block=True, session_execution_mode="NORMAL"
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


def test_live_session_runner_wires_policy_only_guard_as_tick_evaluation_permission(monkeypatch, tmp_path):
    """The production session caller must no longer collapse PB1 policy into global safety."""
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
    monkeypatch.setattr(
        "trader.us.db.repos.load_latest_us_prep_status",
        lambda *a, **k: {
            "status": "OK_WITH_WARNINGS_ENTRY_BLOCKED_CLUSTER_CAP",
            "run_id": "prep-sep29",
            "result": {"trade_block_reason": "sector_cap_violation_block"},
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
            "prep_status": "OK_WITH_WARNINGS_ENTRY_BLOCKED_CLUSTER_CAP",
            "locked_watchlist_count": 28,
            "entry_eval_status": "BLOCKED_BY_PB1_POLICY",
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

    assert seen["entry_can_proceed"] is True
    assert seen["exit_can_proceed"] is True


def test_sep29_fast_dip_reaches_router_after_owner_permission_split(monkeypatch):
    monkeypatch.setenv("US_TQQQ_INFINITE_ENABLED", "1")
    monkeypatch.setenv("US_TQQQ_INFINITE_REAL_ORDER", "1")

    routed: list[dict] = []
    overlay = _overlay(entry_can_proceed=True)
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
    assert result["status"] == "BLOCK"
    assert result["reason"] == "SESSION_SAFE_DEGRADED"
    assert routed == []


def test_session_runner_source_uses_split_permission_helper():
    src = Path("trader/us/runner/trade_session_runner.py").read_text(encoding="utf-8")
    assert "resolve_shared_tick_entry_evaluation_permission" in src
    assert (
        'entry_can_proceed=bool(prep_guard_result.get("entry_can_proceed", False)) '
        'and not timeout_entry_block and session_execution_mode == "NORMAL"'
    ) not in src
