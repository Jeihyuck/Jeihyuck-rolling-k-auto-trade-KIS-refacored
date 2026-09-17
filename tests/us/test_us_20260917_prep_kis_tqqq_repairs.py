from __future__ import annotations

import time
from datetime import date
from pathlib import Path


def test_tqqq_held_fast_dip_buys_even_when_defensive_regime_blocks_routine_entries():
    from trader.us.infinite.config import InfiniteConfig
    from trader.us.infinite.models import Action, InfiniteState, PositionSnapshot, Status
    from trader.us.infinite.strategy import evaluate

    state = InfiniteState(
        cycle_id="cycle-20260915",
        status=Status.ACTIVE,
        core_filled_notional=2000.0,
        last_buy_date=date(2026, 9, 15),
        metadata={"last_buy_fill_price": 69.41, "long_trend": "BULL"},
    )
    position = PositionSnapshot(
        qty=27, orderable_qty=27, average_price=71.973, price=66.60, exchange="NASDAQ"
    )

    decision = evaluate(
        config=InfiniteConfig(),
        state=state,
        position=position,
        trading_date=date(2026, 9, 16),
        overlay={"market_state": "DEFENSE_RISK_OFF", "market_regime": "DEFENSIVE"},
        entry_allowed=False,
        buy_multiplier=0.0,
        effective_regime_name="DEFENSIVE",
    )

    assert decision.action == Action.BUY
    assert decision.reason == "FAST_DIP_ADD_BUY"
    assert decision.qty == 3
    assert decision.metadata["fast_dip_policy"] == "HELD_TQQQ_LAST_FILL_MINUS_1PCT"
    assert decision.metadata["fast_dip_threshold_price"] == 69.41 * 0.99


def test_tqqq_fast_dip_still_respects_pending_and_daily_safety_fences():
    from trader.us.infinite.config import InfiniteConfig
    from trader.us.infinite.models import Action, InfiniteState, PositionSnapshot, Status
    from trader.us.infinite.strategy import evaluate

    state = InfiniteState(
        cycle_id="cycle",
        status=Status.ACTIVE,
        core_filled_notional=2000.0,
        last_buy_date=date(2026, 9, 15),
        metadata={"last_buy_fill_price": 69.41, "long_trend": "BULL"},
    )
    position = PositionSnapshot(qty=27, orderable_qty=27, average_price=71.973, price=66.60)

    pending = evaluate(
        config=InfiniteConfig(), state=state, position=position,
        trading_date=date(2026, 9, 16), pending_buy=True,
        overlay={"market_state": "DEFENSE_RISK_OFF", "market_regime": "DEFENSIVE"},
        entry_allowed=False, effective_regime_name="DEFENSIVE",
    )
    assert pending.action == Action.BLOCK
    assert pending.reason == "tqqq_pending_order_exists"

    capped = evaluate(
        config=InfiniteConfig(), state=state, position=position,
        trading_date=date(2026, 9, 16), daily_filled_buy_notional=250.0,
        overlay={"market_state": "DEFENSE_RISK_OFF", "market_regime": "DEFENSIVE"},
        entry_allowed=False, effective_regime_name="DEFENSIVE",
    )
    assert capped.action == Action.BLOCK
    assert capped.reason == "daily_buy_limit"


def test_authoritative_tick_balance_primes_sell_snapshot_without_second_exchange_sweep():
    from trader.us.execution.tick_context import TickExecutionContext

    ctx = TickExecutionContext("2026-09-16", "am", "run", 1, "tick")
    snapshot = {
        "balance_parse_status": "OK",
        "balance_complete": True,
        "balance_authoritative": True,
        "positions": [{"symbol": "AMZN", "qty": 1, "exchange": "NASDAQ"}],
    }
    ctx.balance_snapshot = snapshot

    assert ctx.sell_balance_snapshot is snapshot


def test_kis_governor_is_vts_only_and_enforces_account_wide_interval(monkeypatch, tmp_path):
    from trader.us import kis_http_governor as governor

    monkeypatch.setenv("US_KIS_GLOBAL_RATE_GOVERNOR", "1")
    monkeypatch.setenv("US_KIS_GLOBAL_RATE_GOVERNOR_TEST", "1")
    monkeypatch.setenv("US_KIS_PRACTICE_GLOBAL_MIN_INTERVAL_SEC", "1.05")
    lock_path = tmp_path / "kis.lock"
    state_path = tmp_path / "kis.last"
    monkeypatch.setattr(governor, "_state_paths", lambda: (lock_path, state_path))

    state_path.write_text(str(time.time()), encoding="utf-8")
    sleeps: list[float] = []
    monkeypatch.setattr(governor.time, "sleep", lambda value: sleeps.append(value))

    waited = governor._wait_for_global_slot(
        "GET", "https://openapivts.koreainvestment.com:29443/uapi/overseas-stock/v1/trading/inquire-balance"
    )
    assert waited >= 0.95
    assert sleeps and sleeps[0] >= 0.95

    assert governor._wait_for_global_slot("GET", "https://example.com/test") == 0.0


def test_preflight_uses_canonical_final30_loader_and_recovery_protects_completed_prep():
    preflight = Path("scripts/wsl/check-us-prep-before-am.sh").read_text(encoding="utf-8")
    recovery = Path("scripts/wsl/run-us-prep-recovery.sh").read_text(encoding="utf-8")

    assert "load_us_final30_scored" in preflight
    assert "load_us_prep_contract" in preflight
    assert "final30_scored.json" in preflight
    assert "data.get('rows', data)" not in preflight
    assert "SKIP_EFFECTIVE_PREP" in recovery
    assert "exec bash scripts/wsl/run-us-prep.sh" in recovery
