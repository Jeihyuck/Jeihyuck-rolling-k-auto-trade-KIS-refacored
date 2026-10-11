"""Four US_STANDARD strategies share risk-safe execution but retain BUY provenance.

No brokerage, production database, or KIS action. The canonical frozen policy,
TP1/2/3 completion evidence and owner boundary are exercised together.
"""
from datetime import datetime, timezone

import pytest

from trader.us.entry_exit_contract import (
    build_us_entry_exit_contract,
    contract_profit_capture,
    verify_us_entry_exit_contract,
)
from trader.us.pb1.us_exit_engine import evaluate_exit
from trader.us.profit_capture import sync_profit_capture_stage_from_order


@pytest.mark.parametrize("style", ["ENTRY_PULLBACK", "ENTRY_MOMENTUM", "ENTRY_BREAKOUT", "ENTRY_VCP"])
def test_each_independent_family_keeps_three_stages_and_owner_after_restart(monkeypatch, style):
    monkeypatch.setenv("US_HARD_STOP_PCT", "0.08")
    monkeypatch.setenv("US_TP1_PCT", "0.03")
    monkeypatch.setenv("US_TP2_PCT", "0.05")
    monkeypatch.setenv("US_TP3_PCT", "0.08")
    source = {
        "symbol": "AAPL", "strategy_owner": "US_STANDARD",
        "entry_style_selected": style, "entry_reason": style,
        "entry_style_raw": style.replace("ENTRY_", "").lower(),
    }
    frozen = build_us_entry_exit_contract(source)
    assert verify_us_entry_exit_contract(frozen)
    assert frozen["strategy_owner"] == "US_STANDARD"
    assert frozen["entry_provenance"]["entry_style_selected"] == style
    sha = frozen["sha256"]
    snapshot = contract_profit_capture({"meta": {"entry_exit_contract": frozen}})
    assert snapshot["enabled"] is True
    assert snapshot["partial_exit_allowed"] is True
    stages = snapshot["stages"]
    assert [s["reason"] for s in stages] == [
        "TAKE_PROFIT_TP1", "TAKE_PROFIT_TP2", "TAKE_PROFIT_TP3",
    ]
    assert [round(s["threshold_fraction"], 4) for s in stages] == [.03, .05, .08]

    # A later PREP or deploy must NOT rewrite an already-owned BUY exit contract.
    monkeypatch.setenv("US_TP1_PCT", "0.20")
    monkeypatch.setenv("US_TP2_PCT", "0.30")
    monkeypatch.setenv("US_TP3_PCT", "0.40")
    monkeypatch.setenv("US_HARD_STOP_PCT", "0.30")
    position = {
        "symbol": "AAPL", "exchange": "NASDAQ", "qty": 20,
        "orderable_qty": 20, "entry_price": 100., "max_price": 100.,
        "meta": {"entry_exit_contract": frozen, "entry_exit_contract_sha256": sha},
    }
    assert contract_profit_capture(position) == snapshot
    assert frozen["sha256"] == sha
    signal = evaluate_exit(position, current_price=91.)
    assert signal is not None
    assert signal["exit_type"] == "hard_stop_loss"
    assert signal["qty"] == 20

    # A TQQQ Infinite BUY cannot adopt the US_STANDARD frozen strategy policy.
    infinite = build_us_entry_exit_contract({
        "symbol": "TQQQ", "strategy_owner": "TQQQ_INFINITE",
        "entry_style_selected": style, "entry_exit_contract": frozen,
    })
    assert infinite == {}


@pytest.mark.parametrize("stage", ["tp1", "tp2", "tp3"])
def test_tp_stages_require_broker_terminal_fill_not_ack(monkeypatch, stage):
    """A PENDING or unknown filled quantity must never be treated as DONE."""
    calls = []
    monkeypatch.setattr(
        "trader.us.db.repos.mark_us_profit_capture_stage",
        lambda *args, **kwargs: calls.append((args, kwargs)),
    )
    common = {
        "trade_date": "2026-09-24", "symbol": "INTC",
        "profit_capture_stage": stage,
        "position_lifecycle_id": "historical-momentum-verified",
        "client_order_key": f"mock-{stage}",
        "requested_qty": 5,
    }
    sync_profit_capture_stage_from_order(
        **common, order_status="FILLED", evidence_type="ACK_ONLY", filled_qty=0,
    )
    assert calls[-1][1]["status"] == "PARTIALLY_FILLED"
    sync_profit_capture_stage_from_order(
        **common, order_status="FILLED", evidence_type="KIS_EXECUTION_ACTUAL",
        filled_qty=4,
    )
    assert calls[-1][1]["status"] == "PARTIALLY_FILLED"
    sync_profit_capture_stage_from_order(
        **common, order_status="FILLED", evidence_type="KIS_EXECUTION_ACTUAL",
        filled_qty=5,
    )
    assert calls[-1][1]["status"] == "FILLED"
    assert calls[-1][1]["filled_qty"] == 5
    count = len(calls)
    sync_profit_capture_stage_from_order(
        **common, order_status="SIGNAL_ONLY", evidence_type="KIS_EXECUTION_ACTUAL",
        filled_qty=5,
    )
    assert len(calls) == count
