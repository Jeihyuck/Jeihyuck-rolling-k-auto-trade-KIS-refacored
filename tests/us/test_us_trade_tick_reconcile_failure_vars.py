# -*- coding: utf-8 -*-
"""reconcile 실패 시 UnboundLocalError 없이 정상 실패 payload를 반환해야 한다."""
from __future__ import annotations

import pytest


def _patch_provider(monkeypatch):
    """USDataProvider를 MagicMock으로 교체."""
    from unittest.mock import MagicMock
    provider = MagicMock()
    provider.is_available.return_value = True
    provider.get_orderable_cash.return_value = 10000.0
    provider.get_balance.return_value = {}
    provider._get_client.return_value = MagicMock(stats={})
    import trader.us.data_provider as dp_mod
    monkeypatch.setattr(dp_mod, "USDataProvider", lambda offline=True: provider)
    return provider


def test_reconcile_contract_error_no_unbound(monkeypatch):
    """reconcile CONTRACT_ERROR → FAILED 반환, UnboundLocalError 없음."""
    import trader.us.runner.trade_tick_runner as mod

    # reconcile_positions → CONTRACT_ERROR
    monkeypatch.setattr(
        "trader.us.execution.reconcile.reconcile_positions",
        lambda provider: {
            "status": "CONTRACT_ERROR",
            "block_new_entry": True,
            "reason": "balance_position_parse_error",
            "balance_parse_error": "balance_position_parse_error",
            "position_count": 0,
            "position_symbols": [],
        },
    )

    _patch_provider(monkeypatch)

    result = mod.run_trade_tick(
        session="am",
        env="practice",
        offline=True,  # offline=True로 provider 초기화 단순화
        run_mode="TRADE",
        signal_only=False,
        force_now="2026-06-02T10:00:00-04:00",
    )

    assert result["status"] == "FAILED", f"status={result['status']}"
    assert result.get("reason") == "reconcile_internal_type_error"
    assert result.get("fills") == 0
    assert result.get("temp_error_count") == 0
    assert result.get("temp_recovered_count") == 0
    assert result.get("entry_eval_status") == "BLOCKED"


def test_reconcile_failure_vars_always_defined(monkeypatch):
    """reconcile 실패 경로에서 fills_today, temp_error_count 등이 항상 정의됨."""
    import trader.us.runner.trade_tick_runner as mod

    monkeypatch.setattr(
        "trader.us.execution.reconcile.reconcile_positions",
        lambda provider: {
            "status": "CONTRACT_ERROR",
            "block_new_entry": True,
            "reason": "balance_position_parse_error",
            "balance_parse_error": "balance_position_parse_error",
            "position_count": 0,
            "position_symbols": [],
        },
    )

    _patch_provider(monkeypatch)

    # UnboundLocalError 없이 실행되어야 한다
    try:
        result = mod.run_trade_tick(
            session="am", env="practice", offline=True,
            run_mode="TRADE", signal_only=False,
            force_now="2026-06-02T10:00:00-04:00",
        )
    except UnboundLocalError as e:
        pytest.fail(f"UnboundLocalError 발생: {e}")

    # payload 키 확인
    for key in ("fills", "temp_error_count", "temp_recovered_count"):
        assert key in result, f"payload에 {key}가 없다"
