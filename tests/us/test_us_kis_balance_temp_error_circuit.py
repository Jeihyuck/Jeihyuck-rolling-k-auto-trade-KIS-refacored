from unittest.mock import MagicMock

from trader.us.runner.trade_tick_runner import evaluate_balance_error_circuit


def test_balance_thresholds():
    assert not evaluate_balance_error_circuit(4)["balance_warning"]
    assert evaluate_balance_error_circuit(5)["balance_warning"]
    assert evaluate_balance_error_circuit(10)["balance_reconcile_degraded"]
    blocked = evaluate_balance_error_circuit(20)
    assert blocked["entry_can_proceed"] is False and blocked["exit_can_proceed"] is True


def test_three_consecutive_failures_block_entry():
    result = evaluate_balance_error_circuit(0, consecutive_failed_ticks=3)
    assert result["entry_blocked_by_balance_degraded"] is True
    assert result["balance_consecutive_failed_ticks"] == 3
    assert result["entry_block_reasons"] == [
        "balance_reconcile_degraded", "balance_entry_blocked"
    ]
    assert result["exit_can_proceed"] is True
    assert result["close_can_proceed"] is True


def test_risk_off_reason_is_preserved_with_balance_degradation():
    result = evaluate_balance_error_circuit(20, entry_block_reasons=["risk_off_entry_block"])
    assert result["entry_block_reasons"] == [
        "risk_off_entry_block", "balance_reconcile_degraded", "balance_entry_blocked"
    ]


def test_current_reconcile_failure_blocks_buy_on_third_consecutive_tick(monkeypatch):
    """The current recon result must participate in the circuit before BUY routing."""
    import trader.us.runner.trade_tick_runner as mod

    provider = MagicMock()
    provider.is_available.return_value = True
    provider.get_orderable_cash.return_value = 10_000.0
    provider.get_balance.return_value = {}
    provider._get_client.return_value = MagicMock(stats={})
    monkeypatch.setattr("trader.us.data_provider.USDataProvider", lambda offline=True: provider)
    monkeypatch.setattr(
        "trader.us.execution.reconcile.reconcile_positions",
        lambda *args, **kwargs: {
            "status": "WARN",
            "balance_fetch_status": "TEMP_ERROR",
            "preserve_previous_positions": True,
            "authoritative_positions": False,
            "positions": [],
            "position_count": 0,
            "position_symbols": [],
            "block_new_entry": False,
        },
    )

    result = mod.run_trade_tick(
        session="am",
        env="practice",
        offline=True,
        run_mode="TRADE",
        signal_only=False,
        force_now="2026-06-02T10:00:00-04:00",
        balance_consecutive_failed_ticks=2,
    )

    circuit = result["balance_circuit"]
    assert result["balance_fetch_failed"] is True
    assert result["skip_zero_snapshot_count"] == 1
    assert circuit["balance_consecutive_failed_ticks"] == 3
    assert circuit["entry_can_proceed"] is False
    assert circuit["exit_can_proceed"] is True
    assert circuit["close_can_proceed"] is True
    assert result["entry_skipped"] is True
    assert result["buy_orders"] == 0


def test_us_balance_incomplete_continues_exit_monitoring(monkeypatch):
    """A non-authoritative balance fences BUYs but still routes a held SELL."""
    import trader.us.runner.trade_tick_runner as mod

    held = {"symbol": "HELD", "exchange": "NASDAQ", "qty": 1, "orderable_qty": 1, "entry_price": 100.0}
    provider = MagicMock()
    provider._get_client.return_value = MagicMock(stats={})
    provider.get_orderable_cash.return_value = 10_000.0
    monkeypatch.setattr("trader.us.data_provider.USDataProvider", lambda offline=False: provider)
    monkeypatch.setattr("trader.us.market_calendar.is_us_trading_day", lambda _d: True)
    monkeypatch.setattr("trader.us.market_calendar.market_phase", lambda _now: "REGULAR_MID")
    monkeypatch.setattr("trader.us.budget.resolve_us_order_budget", lambda cash: {"effective_order_budget_usd": cash})
    monkeypatch.setattr("trader.us.execution.reconcile.reconcile_positions", lambda **_kwargs: {
        "status": "WARN", "reason": "balance_incomplete", "balance_fetch_status": "TEMP_ERROR",
        "preserve_previous_positions": True, "authoritative_positions": False, "positions": [],
        "position_count": 0, "position_symbols": [], "block_new_entry": True,
    })
    monkeypatch.setattr("trader.us.execution.fills.get_fills_today", lambda **_kwargs: {"status": "OK", "fills": []})
    monkeypatch.setattr("trader.us.execution.reconcile.reconcile_ack_orders_with_balance", lambda **_kwargs: {
        "status": "OK", "pending_count": 0, "unresolved_count": 0, "symbols_by_status": {},
    })
    monkeypatch.setattr("trader.us.db.repos.load_positions", lambda *_args, **_kwargs: [held])
    monkeypatch.setattr("trader.us.db.repos.load_today_symbols_sold", lambda **_kwargs: set())
    monkeypatch.setattr("trader.us.db.repos.load_today_committed_buy_notional", lambda *_args, **_kwargs: 0.0)
    monkeypatch.setattr("trader.us.db.repos.save_reconcile_log", lambda *_args, **_kwargs: True)
    monkeypatch.setattr("trader.us.db.repos.has_pending_order_for_symbol_side", lambda **_kwargs: False)
    monkeypatch.setattr("trader.us.pb1.us_exit_position_resolver.enrich_us_positions_for_exit", lambda positions, **_kwargs: (positions, {"total": 1, "ok": 1, "missing": 0}))

    class Engine:
        def evaluate_exits(self, positions, provider, now):
            assert positions == [held]
            return [{"symbol": "HELD", "exchange": "NASDAQ", "side": "SELL", "qty": 1,
                     "notional_usd": 100.0, "client_order_key": "held-exit"}]
        def evaluate_entries(self, *_args, **_kwargs):
            raise AssertionError("incomplete balance must fence entry evaluation")
    monkeypatch.setattr("trader.us.runner.trade_tick_runner._get_strategy_engine", lambda **_kwargs: Engine())
    routed = []
    monkeypatch.setattr("trader.us.execution.order_router.route_order", lambda intent, **kwargs: (
        routed.append((intent, kwargs)) or {"status": "ACK", "side": "SELL", "symbol": intent["symbol"], "intent": intent}
    ))

    result = mod.run_trade_tick(session="am", env="practice", offline=False,
                                force_now="2026-06-02T10:00:00-04:00")

    assert result["balance_fetch_failed"] is True
    assert result["entry_skipped"] is True
    assert result["entry_intents"] == 0
    assert [intent["symbol"] for intent, _kwargs in routed] == ["HELD"]
    assert routed[0][1]["allowed_symbols"] is None
