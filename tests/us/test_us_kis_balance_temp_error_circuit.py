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
