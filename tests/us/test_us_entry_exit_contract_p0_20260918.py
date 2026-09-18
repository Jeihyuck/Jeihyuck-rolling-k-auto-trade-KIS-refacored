from datetime import datetime, timezone
import copy

import pytest

from trader.us.entry_exit_contract import (
    build_us_entry_exit_contract,
    extract_us_entry_exit_contract,
)
from trader.us.market_state_overlay import build_profit_capture_intents
from trader.us.pb1.us_exit_engine import evaluate_exit
from trader.us.pb1.us_exit_router import evaluate_day_exit


def _contract(*, symbol="AAPL", book="SWING_BOOK", horizon="SWING_CARRY",
              exit_policy="US_SWING_DEFAULT", signal="pullback"):
    return build_us_entry_exit_contract({
        "symbol": symbol, "strategy_owner": "US_STANDARD", "sleeve_id": "US_STANDARD",
        "book": book, "horizon": horizon, "exit_policy": exit_policy,
        "entry_strategy": "us_pb1", "entry_signal_type": signal,
        "entry_style_selected": "ENTRY_" + signal.upper(),
        "reasons": ["ENTRY_" + signal.upper()],
        "score_breakdown": {signal: 0.81},
        "filters_passed": ["score", "risk"],
    })





def test_us_new_buy_never_fabricates_momentum_when_style_is_missing():
    from trader.us.pb1.us_entry_engine import _validate_new_buy_explain_contract

    ok, reason = _validate_new_buy_explain_contract(
        "AAPL",
        {"score_final": 0.9, "breakout_score": 0.2, "pullback_score": 0.3, "momentum_score": 0.4},
        "",
        signal_score=0.9,
    )
    assert ok is False
    assert reason == "ENTRY_EXPLAIN_CONTRACT_ERROR"


def test_us_explanation_accepts_entry_style_as_authoritative_source():
    from trader.us.pb1.us_explain import build_us_entry_explanation

    explanation = build_us_entry_explanation(
        "AAPL",
        {
            "entry_style": "pullback",
            "pullback_score": 0.8,
            "score_final": 0.9,
            "close": 100.0,
            "ma20": 95.0,
            "ma50": 90.0,
        },
        decision="BUY",
    )
    assert explanation["entry_style_selected"] == "ENTRY_PULLBACK"
    assert "entry_condition_met" not in explanation["reasons"]
    assert explanation["reasons"]


def test_us_contract_freezes_swing_exit_after_env_change(monkeypatch):
    monkeypatch.setenv("US_HARD_STOP_PCT", "0.08")
    contract = _contract()
    monkeypatch.setenv("US_HARD_STOP_PCT", "0.20")
    position = {
        "symbol": "AAPL", "exchange": "NASDAQ", "qty": 10, "orderable_qty": 10,
        "entry_price": 100.0, "max_price": 100.0,
        "meta": {"entry_exit_contract": contract},
    }
    intent = evaluate_exit(position, 91.0, now=datetime.now(timezone.utc), include_trend_time=False)
    assert intent is not None
    assert intent["exit_type"] in {"hard_stop", "hard_stop_loss", "hard_stop_full_exit"}
    assert intent["meta"]["hard_stop_threshold_pct"] == pytest.approx(0.08)


def test_us_day_exit_uses_buy_time_contract_not_current_env(monkeypatch):
    monkeypatch.setenv("US_DAY_PROFIT_TAKE_PCT", "0.025")
    contract = _contract(
        symbol="MSFT", book="DAY_BOOK", horizon="DAY_TRADE",
        exit_policy="DAY_BOOK", signal="breakout",
    )
    monkeypatch.setenv("US_DAY_PROFIT_TAKE_PCT", "0.50")
    position = {
        "symbol": "MSFT", "exchange": "NASDAQ", "qty": 5, "holding_qty": 5,
        "entry_price": 100.0, "meta": {"entry_exit_contract": contract},
    }
    intent = evaluate_day_exit(position, 103.0, include_trend_time=False)
    assert intent is not None
    assert intent["exit_type"] == "day_profit_take"


def test_us_contract_tamper_is_rejected():
    contract = _contract()
    tampered = copy.deepcopy(contract)
    tampered["management"]["swing"]["hard_stop"] = 0.50
    assert extract_us_entry_exit_contract({"entry_exit_contract": tampered}) == {}


def test_us_profit_capture_uses_frozen_buy_contract_after_env_change(monkeypatch):
    monkeypatch.setenv("US_TP1_PCT", "0.03")
    contract = _contract()
    monkeypatch.setenv("US_TP1_PCT", "0.50")
    now = datetime.now(timezone.utc)
    position = {
        "symbol": "AAPL", "qty": 20, "orderable_qty": 20, "current_price_usd": 104.0,
        "position_lifecycle_id": "life-contract",
        "broker_avg_price": 100.0, "broker_avg_price_source": "kis_pchs_avg_pric",
        "broker_avg_price_asof": now.isoformat(), "broker_avg_price_currency": "USD",
        "balance_source": "kis_balance_authoritative", "authoritative_positions": True,
        "meta": {"entry_exit_contract": contract},
    }
    intents = build_profit_capture_intents(
        [position], {"profit_capture_enabled": True, "market_state": "NORMAL"},
        now=now, trade_date="2026-09-18", profit_capture_state={},
    )
    assert intents and intents[0]["reason"] == "TAKE_PROFIT_TP1"
    assert intents[0]["meta"]["source_entry_contract_sha256"] == contract["sha256"]
    assert intents[0]["meta"]["tp_threshold_fraction"] == "0.03"





def test_us_soft_stop_sell_ratio_is_frozen_at_buy(monkeypatch):
    monkeypatch.setenv("US_SOFT_STOP_SELL_RATIO", "0.40")
    contract = _contract()
    monkeypatch.setenv("US_SOFT_STOP_SELL_RATIO", "0.90")
    position = {
        "symbol": "AAPL", "exchange": "NASDAQ", "qty": 10, "orderable_qty": 10,
        "entry_price": 100.0, "max_price": 100.0,
        "soft_stop_breach_count": 2, "soft_stop_confirm_ticks": 2,
        "meta": {"entry_exit_contract": contract},
    }
    intent = evaluate_exit(
        position, 94.0, now=datetime(2026, 9, 18, 18, 0, tzinfo=timezone.utc),
        include_trend_time=False,
    )
    assert intent is not None
    assert intent["exit_type"] in {"soft_stop_loss", "soft_stop"}
    assert intent["qty"] == 4


def test_us_profit_trailing_sell_ratio_is_frozen_at_buy(monkeypatch):
    monkeypatch.setenv("US_PROFIT_TRAILING_SELL_RATIO", "0.40")
    contract = _contract()
    monkeypatch.setenv("US_PROFIT_TRAILING_SELL_RATIO", "0.90")
    position = {
        "symbol": "AAPL", "exchange": "NASDAQ", "qty": 10, "orderable_qty": 10,
        "entry_price": 100.0, "max_price": 110.0,
        "meta": {"entry_exit_contract": contract},
    }
    intent = evaluate_exit(
        position, 104.0, now=datetime(2026, 9, 18, 18, 0, tzinfo=timezone.utc),
        include_trend_time=False,
    )
    assert intent is not None
    assert intent["exit_type"] in {"profit_trailing_stop", "trailing_stop"}
    assert intent["qty"] == 4


def test_us_trend_time_exit_uses_buy_time_days_and_ratio(monkeypatch):
    from trader.us.position_trend_state import choose_trend_time_exit

    monkeypatch.setenv("US_TIME_STOP_DAYS", "2")
    monkeypatch.setenv("US_TIME_STOP_FIRST_SELL_RATIO", "0.25")
    monkeypatch.setenv("US_TREND_EXIT_MIN_HOLD_DAYS", "1")
    contract = _contract()
    monkeypatch.setenv("US_TIME_STOP_DAYS", "99")
    monkeypatch.setenv("US_TIME_STOP_FIRST_SELL_RATIO", "0.90")

    position = {
        "symbol": "AAPL", "qty": 20, "orderable_qty": 20,
        "holding_trade_days": 2,
        "meta": {"entry_exit_contract": contract},
    }
    trend = {
        "trend_state": "WARNING", "holding_trade_days": 2,
        "final30_absent_streak": 2, "current_price": 99.0, "ma20": 100.0,
        "trend_trim_done": False, "time_stop_trim_done": False,
        "time_stop_trim_pending": False,
    }
    choice = choose_trend_time_exit(position, trend, pnl_pct=0.0, orderable_qty=20)
    assert choice is not None
    exit_type, qty, stage = choice
    assert exit_type == "time_stop_trim"
    assert stage == "time_stop_trim"
    assert qty == 5


def test_us_add_to_existing_blocks_without_parent_contract(monkeypatch):
    from trader.us.execution.order_router import route_order
    import trader.us.db.repos as repos

    monkeypatch.setattr(repos, "load_us_positions_by_symbols", lambda *a, **k: {})
    intent = {
        "trade_date": "2026-09-18", "client_order_key": "AAPL-ADD-NO-PARENT",
        "symbol": "AAPL", "exchange": "NASDAQ", "side": "BUY",
        "qty": 1, "limit_price": 100.0, "notional_usd": 100.0,
        "strategy": "us_pb1", "position_action": "ADD_TO_EXISTING_BUY",
    }
    result = route_order(
        intent, signal_only=True, current_position_symbols={"AAPL"},
        allowed_symbols={"AAPL"},
    )
    assert result["status"] == "BLOCKED"
    assert result["reason"] == "US_ADD_PARENT_CONTRACT_MISSING"


def test_us_add_to_existing_inherits_exact_parent_contract(monkeypatch):
    from trader.us.execution.order_router import route_order
    import trader.us.db.repos as repos

    monkeypatch.setenv("US_HARD_STOP_PCT", "0.08")
    parent = _contract()
    parent_position = {
        "symbol": "AAPL", "qty": 10, "position_lifecycle_id": "life-parent",
        "meta": {
            "position_lifecycle_id": "life-parent",
            "entry_exit_contract": parent,
            "entry_exit_contract_sha256": parent["sha256"],
        },
    }
    monkeypatch.setattr(
        repos, "load_us_positions_by_symbols",
        lambda *a, **k: {"AAPL": parent_position},
    )
    monkeypatch.setenv("US_HARD_STOP_PCT", "0.20")
    intent = {
        "trade_date": "2026-09-18", "client_order_key": "AAPL-ADD-PARENT",
        "symbol": "AAPL", "exchange": "NASDAQ", "side": "BUY",
        "qty": 1, "limit_price": 100.0, "notional_usd": 100.0,
        "strategy": "us_pb1", "position_action": "ADD_TO_EXISTING_BUY",
        "reasons": ["pyramid_add"],
    }
    result = route_order(
        intent, signal_only=True, current_position_symbols={"AAPL"},
        allowed_symbols={"AAPL"},
    )
    assert result["status"] == "SIGNAL_ONLY"
    routed = result["intent"]
    assert routed["meta"]["parent_entry_contract_sha256"] == parent["sha256"]
    assert routed["meta"]["entry_exit_contract"]["sha256"] == parent["sha256"]
    assert routed["meta"]["position_lifecycle_id"] == "life-parent"
    assert routed["meta"]["entry_exit_contract"]["management"]["swing"]["hard_stop"] == pytest.approx(0.08)


def test_us_locked_watchlist_meta_keeps_entry_provenance():
    from trader.us.db.repos import _merge_us_daily_metrics_meta
    row = {
        "symbol": "AAPL", "meta": {}, "entry_style_selected": "ENTRY_PULLBACK",
        "pullback_score": 0.81, "breakout_score": 0.12, "momentum_score": 0.43,
        "reasons": ["ENTRY_PULLBACK"], "score_breakdown": {"pullback": 0.81},
    }
    meta = _merge_us_daily_metrics_meta(row)
    assert meta["entry_style_selected"] == "ENTRY_PULLBACK"
    assert meta["pullback_score"] == pytest.approx(0.81)
    assert meta["reasons"] == ["ENTRY_PULLBACK"]


def test_us_order_policy_contract_contains_full_v2_provenance():
    from trader.us.db.repos import _ensure_us_entry_policy_contract
    policy = _ensure_us_entry_policy_contract({
        "symbol": "AAPL", "strategy_owner": "US_STANDARD", "sleeve_id": "US_STANDARD",
        "book": "SWING_BOOK", "horizon": "SWING_CARRY", "exit_policy": "US_SWING_DEFAULT",
        "entry_strategy": "us_pb1", "entry_signal_type": "pullback",
        "entry_style_selected": "ENTRY_PULLBACK", "entry_reason": "ENTRY_PULLBACK",
        "reasons": ["ENTRY_PULLBACK"], "score_breakdown": {"pullback": 0.81},
        "filters_passed": ["score", "risk"],
    })
    contract = policy["entry_exit_contract"]
    assert policy["entry_exit_contract_sha256"] == contract["sha256"]
    assert contract["entry_provenance"]["entry_reason"] == "ENTRY_PULLBACK"
    assert contract["entry_provenance"]["score_breakdown"]["pullback"] == pytest.approx(0.81)


def test_tqqq_infinite_never_receives_standard_contract():
    assert build_us_entry_exit_contract({
        "symbol": "TQQQ", "strategy_owner": "TQQQ_INFINITE", "sleeve_id": "TQQQ_INFINITE",
    }) == {}



def test_us_corrupted_v2_contract_blocks_normal_profit_trend_and_global_tp(monkeypatch):
    from trader.us.pb1.us_exit_engine import evaluate_exit
    from trader.us.market_state_overlay import build_profit_capture_intents
    from trader.us.position_trend_state import choose_trend_time_exit

    contract = _contract()
    tampered = copy.deepcopy(contract)
    tampered["management"]["swing"]["trailing_stop"] = 0.99
    position = {
        "symbol": "AAPL", "exchange": "NASDAQ", "qty": 20, "orderable_qty": 20,
        "entry_price": 100.0, "current_price_usd": 104.0, "max_price": 110.0,
        "position_lifecycle_id": "life-bad-contract",
        "holding_trade_days": 30,
        "broker_avg_price": 100.0,
        "broker_avg_price_source": "kis_pchs_avg_pric",
        "broker_avg_price_asof": datetime.now(timezone.utc).isoformat(),
        "broker_avg_price_currency": "USD",
        "balance_source": "kis_balance_authoritative",
        "authoritative_positions": True,
        "meta": {
            "entry_exit_contract": tampered,
            "entry_exit_contract_sha256": contract["sha256"],
            "entry_exit_contract_version": contract["version"],
        },
    }

    # +4% / large giveback would normally be eligible for profit logic, but a
    # claimed-and-corrupted v2 contract must not fall back to today's ENV.
    assert evaluate_exit(
        position, 104.0, now=datetime.now(timezone.utc), include_trend_time=False
    ) is None
    assert build_profit_capture_intents(
        [position], {"profit_capture_enabled": True, "market_state": "NORMAL"},
        now=datetime.now(timezone.utc), trade_date="2026-09-18",
        profit_capture_state={},
    ) == []
    assert choose_trend_time_exit(
        position,
        {"trend_state": "EXIT", "holding_trade_days": 30},
        pnl_pct=0.04,
        orderable_qty=20,
    ) is None


@pytest.mark.skipif(not __import__("os").getenv("PBCORE_TEST_POSTGRES_URL"), reason="real PostgreSQL integration URL not configured")
def test_us_postgres_buy_contract_survives_position_restart_and_drives_sell(monkeypatch):
    import os
    from sqlalchemy import create_engine, text
    import trader.us.db.repos as repos
    from trader.us.pb1.us_exit_engine import generate_exit_intents

    engine = create_engine(os.environ["PBCORE_TEST_POSTGRES_URL"], future=True)
    try:
        with engine.begin() as conn:
            for table in (
                "us_fills", "us_orders", "us_order_intents", "us_order_events",
                "us_profit_capture_lifecycle", "us_positions", "us_watchlist",
            ):
                conn.execute(text(f"DROP TABLE IF EXISTS {table} CASCADE"))
            conn.exec_driver_sql(open("migrations/0038_us_agent_tables.sql", encoding="utf-8").read())
            conn.exec_driver_sql(open("migrations/0043_us_fills_idempotency_and_order_reconcile_fix.sql", encoding="utf-8").read())
            conn.exec_driver_sql(open("migrations/0046_us_orders_committed_notional.sql", encoding="utf-8").read())
            conn.exec_driver_sql(open("migrations/0047_us_order_events_profit_lifecycle.sql", encoding="utf-8").read())

        monkeypatch.setattr(repos, "_get_engine_or_none", lambda: engine)
        monkeypatch.setenv("KIS_ENV", "practice")
        monkeypatch.setenv("US_HARD_STOP_PCT", "0.08")

        intent = {
            "trade_date": "2026-09-18",
            "client_order_key": "US-20260918-AAPL-BUY-CONTRACT-E2E",
            "symbol": "AAPL", "exchange": "NASDAQ", "side": "BUY",
            "qty": 10, "limit_price": 100.0, "notional_usd": 1000.0,
            "strategy": "us_pb1", "strategy_owner": "US_STANDARD",
            "sleeve_id": "US_STANDARD",
            "meta": {
                "book": "SWING_BOOK", "horizon": "SWING_CARRY",
                "exit_policy": "US_SWING_DEFAULT", "entry_strategy": "us_pb1",
                "entry_signal_type": "pullback",
                "entry_style_selected": "ENTRY_PULLBACK",
                "entry_reason": "ENTRY_PULLBACK",
                "reasons": ["ENTRY_PULLBACK"],
                "score_breakdown": {"pullback": 0.81},
                "filters_passed": ["score", "risk"],
            },
        }
        assert repos.save_order_intent(intent, trade_date="2026-09-18")
        # Verify the real PostgreSQL persistence boundary instead of relying on
        # caller-dict mutation.
        with engine.connect() as conn:
            persisted_intent_meta = conn.execute(
                text("SELECT meta FROM us_order_intents WHERE client_order_key=:key"),
                {"key": intent["client_order_key"]},
            ).scalar_one()
        contract = persisted_intent_meta["entry_exit_contract"]
        root_sha = contract["sha256"]
        assert persisted_intent_meta["entry_exit_contract_sha256"] == root_sha

        assert repos.save_order_ack({
            **intent,
            "qty_requested": 10,
            "qty_filled": 0,
            "order_no": "AAPL-CONTRACT-E2E",
            "status": "ACK",
            "env": "practice",
            "meta": persisted_intent_meta,
        }, trade_date="2026-09-18")

        actual_fill = {
            "symbol": "AAPL", "exchange": "NASDAQ", "side": "BUY",
            "qty": 10, "price_usd": 100.0,
            "order_no": "AAPL-CONTRACT-E2E",
            "client_order_key": intent["client_order_key"],
            "filled_at": "2026-09-18T13:35:00+00:00",
            "cumulative_filled_qty": 10,
            "requested_qty": 10,
            "fill_evidence_type": "KIS_ORDER_CUMULATIVE_ACTUAL",
            "meta": {
                "fill_evidence_type": "KIS_ORDER_CUMULATIVE_ACTUAL",
                "cumulative_filled_qty": 10,
                "requested_qty": 10,
                "entry_exit_contract": contract,
                "entry_exit_contract_sha256": root_sha,
                "entry_exit_contract_version": contract["version"],
            },
        }
        fill_result = repos.save_fills_with_result([actual_fill], trade_date="2026-09-18")
        assert fill_result["status"] == "OK"
        with engine.connect() as conn:
            fill_meta = conn.execute(
                text("SELECT meta FROM us_fills WHERE client_order_key=:key"),
                {"key": intent["client_order_key"]},
            ).scalar_one()
        assert fill_meta["entry_exit_contract_sha256"] == root_sha
        assert fill_meta["entry_exit_contract"]["sha256"] == root_sha

        balance_row = {
            "symbol": "AAPL", "exchange": "NASDAQ", "qty": 10,
            "avg_price_usd": 100.0, "current_price_usd": 91.0,
            "balance_source": "kis_balance_authoritative",
            "meta": {"balance_source": "kis_balance_authoritative"},
        }
        assert repos.save_position_snapshot([balance_row], trade_date="2026-09-18") == 1

        with engine.connect() as conn:
            stored_meta = conn.execute(
                text("SELECT meta FROM us_positions WHERE symbol='AAPL' AND as_of='2026-09-18'")
            ).scalar_one()
        assert stored_meta["entry_exit_contract_sha256"] == root_sha
        assert stored_meta["entry_exit_contract"]["sha256"] == root_sha

        # Fresh DB load simulates the next process/tick.
        reloaded = repos.load_us_positions_by_symbols(["AAPL"], as_of="2026-09-18")["AAPL"]
        assert reloaded["meta"]["entry_exit_contract_sha256"] == root_sha

        # Mutating today's ENV must not alter the already-filled lifecycle.
        monkeypatch.setenv("US_HARD_STOP_PCT", "0.20")
        reloaded["resolved_current_price"] = 91.0
        reloaded["entry_price"] = 100.0
        reloaded["max_price"] = 100.0
        sells = generate_exit_intents(
            [reloaded], prepared_snapshots=[reloaded],
            now=datetime.now(timezone.utc), include_trend_time=False,
        )
        assert sells and sells[0]["side"] == "SELL"
        assert sells[0]["meta"]["hard_stop_threshold_pct"] == pytest.approx(0.08)
        assert sells[0]["source_entry_contract_sha256"] == root_sha
        assert sells[0]["meta"]["source_entry_contract_sha256"] == root_sha
        assert sells[0]["source_entry_reason"] == "ENTRY_PULLBACK"
    finally:
        engine.dispose()



def test_us_lifecycle_uses_confirmed_buy_fill_time_and_preserves_it_after_restart(monkeypatch):
    from datetime import datetime, timezone
    import trader.us.position_lifecycle_state as lifecycle
    from trader.us.pb1.us_exit_router import _min_hold_elapsed

    state_by_symbol = {}

    def _latest(symbol, trade_date):
        return state_by_symbol.get(str(symbol).upper(), {})

    def _load(symbol, trade_date):
        return state_by_symbol.get(str(symbol).upper(), {})

    def _save(symbol, trade_date, risk):
        state_by_symbol[str(symbol).upper()] = copy.deepcopy(risk)
        return True

    monkeypatch.setattr(lifecycle, "load_latest_open_us_position_lifecycles", lambda trade_date: {})
    monkeypatch.setattr(lifecycle, "load_latest_us_position_risk_state", _latest)
    monkeypatch.setattr(lifecycle, "load_us_position_risk_state", _load)
    monkeypatch.setattr(lifecycle, "save_us_position_risk_state", _save)

    contract = _contract()
    fill_time = "2026-09-18T13:35:00+00:00"
    now = datetime(2026, 9, 18, 14, 0, tzinfo=timezone.utc)
    positions = [{
        "symbol": "AAPL", "qty": 10, "entry_price": 100.0, "current_price_usd": 101.0,
        "meta": {
            "entry_exit_contract": contract,
            "entry_exit_contract_sha256": contract["sha256"],
            "entry_exit_contract_version": contract["version"],
        },
    }]
    out = lifecycle.reconcile_us_position_lifecycles(
        positions=positions, trade_date="2026-09-18", now=now, authoritative=True,
        fills=[{
            "symbol": "AAPL", "side": "BUY", "qty": 10, "price_usd": 100.0,
            "filled_at": fill_time,
        }],
    )
    assert out["AAPL"]["opened_at"] == fill_time
    assert out["AAPL"]["opened_at_source"] == "confirmed_buy_fill"
    assert positions[0]["opened_at"] == fill_time
    assert positions[0]["meta"]["entry_exit_contract_sha256"] == contract["sha256"]

    elapsed, held_minutes, required = _min_hold_elapsed(positions[0], now)
    assert elapsed is False
    assert held_minutes == 25
    assert required == 390

    # A later process/tick with no fills must carry exactly the original open time.
    later = datetime(2026, 9, 18, 20, 10, tzinfo=timezone.utc)
    restarted_positions = [{
        "symbol": "AAPL", "qty": 10, "entry_price": 100.0, "current_price_usd": 101.0,
        "meta": {},
    }]
    out2 = lifecycle.reconcile_us_position_lifecycles(
        positions=restarted_positions, trade_date="2026-09-18", now=later,
        authoritative=True, fills=[],
    )
    assert out2["AAPL"]["opened_at"] == fill_time
    assert restarted_positions[0]["opened_at"] == fill_time
    elapsed2, held_minutes2, required2 = _min_hold_elapsed(restarted_positions[0], later)
    assert elapsed2 is True
    assert held_minutes2 == 395
    assert required2 == 390



def test_us_profit_capture_carries_frozen_partial_exit_allowed_into_sell_guard(monkeypatch):
    from trader.us.execution.us_sell_qty_guard import resolve_sell_qty

    monkeypatch.setenv("US_SELL_PARTIAL_ALLOWED", "1")
    monkeypatch.setenv("US_TP1_PCT", "0.03")
    contract = _contract()
    monkeypatch.setenv("US_SELL_PARTIAL_ALLOWED", "0")

    now = datetime.now(timezone.utc)
    position = {
        "symbol": "AAPL", "qty": 20, "orderable_qty": 20, "current_price_usd": 104.0,
        "position_lifecycle_id": "life-partial-contract",
        "broker_avg_price": 100.0,
        "broker_avg_price_source": "kis_pchs_avg_pric",
        "broker_avg_price_asof": now.isoformat(),
        "broker_avg_price_currency": "USD",
        "balance_source": "kis_balance_authoritative",
        "authoritative_positions": True,
        "meta": {"entry_exit_contract": contract},
    }
    intents = build_profit_capture_intents(
        [position], {"profit_capture_enabled": True, "market_state": "NORMAL"},
        now=now, trade_date="2026-09-18", profit_capture_state={},
    )
    assert intents and intents[0]["qty"] == 5
    assert intents[0]["partial_exit_allowed"] is True
    assert intents[0]["meta"]["partial_exit_allowed"] is True

    sell_qty, guard = resolve_sell_qty(
        intents[0], {"holding_qty": 20, "orderable_qty": 20}
    )
    assert sell_qty == 5
    assert guard["strategic_partial"] is True


def test_us_profit_capture_respects_frozen_partial_exit_disallow_after_env_change(monkeypatch):
    from trader.us.execution.us_sell_qty_guard import resolve_sell_qty

    monkeypatch.setenv("US_SELL_PARTIAL_ALLOWED", "0")
    monkeypatch.setenv("US_TP1_PCT", "0.03")
    contract = _contract()
    monkeypatch.setenv("US_SELL_PARTIAL_ALLOWED", "1")

    now = datetime.now(timezone.utc)
    position = {
        "symbol": "AAPL", "qty": 20, "orderable_qty": 20, "current_price_usd": 104.0,
        "position_lifecycle_id": "life-no-partial-contract",
        "broker_avg_price": 100.0,
        "broker_avg_price_source": "kis_pchs_avg_pric",
        "broker_avg_price_asof": now.isoformat(),
        "broker_avg_price_currency": "USD",
        "balance_source": "kis_balance_authoritative",
        "authoritative_positions": True,
        "meta": {"entry_exit_contract": contract},
    }
    intents = build_profit_capture_intents(
        [position], {"profit_capture_enabled": True, "market_state": "NORMAL"},
        now=now, trade_date="2026-09-18", profit_capture_state={},
    )
    assert intents and intents[0]["partial_exit_allowed"] is False
    sell_qty, guard = resolve_sell_qty(
        intents[0], {"holding_qty": 20, "orderable_qty": 20}
    )
    assert sell_qty == 20
    assert guard["strategic_partial"] is False


def test_us_corrupted_day_contract_still_honors_day_hard_stop(monkeypatch):
    monkeypatch.setenv("US_DAY_HARD_STOP_PCT", "0.03")
    monkeypatch.setenv("US_HARD_STOP_PCT", "0.08")
    contract = _contract(
        symbol="MSFT",
        book="DAY_BOOK",
        horizon="DAY_TRADE",
        exit_policy="DAY_BOOK",
        signal="breakout",
    )
    tampered = copy.deepcopy(contract)
    tampered["management"]["day"]["trailing_stop"] = 0.99

    position = {
        "symbol": "MSFT", "exchange": "NASDAQ", "qty": 5, "holding_qty": 5,
        "entry_price": 100.0,
        "meta": {
            "entry_exit_contract": tampered,
            "entry_exit_contract_sha256": contract["sha256"],
            "entry_exit_contract_version": contract["version"],
        },
    }
    intent = evaluate_day_exit(
        position, 96.0,
        now=datetime(2026, 9, 18, 18, 0, tzinfo=timezone.utc),
        include_trend_time=False,
    )
    assert intent is not None
    assert intent["exit_type"] == "day_hard_stop"
    assert intent["qty"] == 5


def test_us_soft_stop_confirmation_ticks_are_frozen_at_buy(monkeypatch):
    monkeypatch.setenv("US_SOFT_STOP_CONFIRM_TICKS", "5")
    monkeypatch.setenv("US_SOFT_STOP_LOSS_PCT", "0.05")
    contract = _contract()
    monkeypatch.setenv("US_SOFT_STOP_CONFIRM_TICKS", "1")

    position = {
        "symbol": "AAPL", "exchange": "NASDAQ",
        "qty": 10, "orderable_qty": 10,
        "entry_price": 100.0, "max_price": 100.0,
        "soft_stop_breach_count": 1,
        "meta": {"entry_exit_contract": contract},
    }
    hold = evaluate_exit(
        position, 94.0,
        now=datetime(2026, 9, 18, 18, 0, tzinfo=timezone.utc),
        include_trend_time=False,
    )
    assert hold is not None
    assert hold["side"] == "HOLD"
    assert hold["reason"] == "soft_stop_wait_confirm"
    assert hold["meta"]["soft_stop_required_ticks"] == 5

    position["soft_stop_breach_count"] = 5
    sell = evaluate_exit(
        position, 94.0,
        now=datetime(2026, 9, 18, 18, 0, tzinfo=timezone.utc),
        include_trend_time=False,
    )
    assert sell is not None
    assert sell["side"] == "SELL"
    assert sell["exit_type"] in {"soft_stop_loss", "soft_stop"}
