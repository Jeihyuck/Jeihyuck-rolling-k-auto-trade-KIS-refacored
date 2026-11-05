"""Combined A+B+C behavioral acceptance (NO KIS HTTP, NO DB writes).

Each isolated PR passes on its own; this suite proves the merged source
retains all three safety boundaries simultaneously.
"""
from datetime import datetime, timedelta, timezone

from trader.us.execution.order_router import (
    _semantic_action_instance, _tp_decision_fresh_for_native_http,
)
from trader.us.entry_exit_contract import build_us_entry_exit_contract
from trader.us.runner.trade_tick_runner import merge_exit_intents_by_symbol
from trader.us.pb1.swing_runner_exit_v3 import evaluate_v3_runner, V3_RUNNER_POLICY
from trader.us.execution.stage4_code_integrity import assert_stage4_worker_code_pin


def test_tp_pre_http_and_signed_v3_semantic_coexist(monkeypatch):
    now = datetime(2026, 11, 5, 15, tzinfo=timezone.utc)
    old_tp = {
        "side": "SELL",
        "client_order_key": "US_PC_2026-11-05_SPY_life_TAKE_PROFIT_TP2",
        "meta": {
            "profit_capture_stage": "tp2",
            "last_profit_capture_at": (now - timedelta(minutes=276)).isoformat(),
        },
    }
    assert _tp_decision_fresh_for_native_http(old_tp, now_utc=now)[0] is False

    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setenv("US_SWING_EXIT_MODE", "V3_ACTIVE_PRACTICE")
    monkeypatch.setenv("US_SWING_V3_DEPLOY_APPROVED", "1")
    signed = build_us_entry_exit_contract({
        "symbol": "SPY", "strategy_owner": "US_STANDARD",
        "book": "SWING_BOOK", "horizon": "SWING_CARRY",
        "position_action": "NEW_POSITION_BUY",
    })
    assert signed["version"] == "us_entry_exit_contract_v3"
    meta = {
        "source_entry_contract_version": signed["version"],
        "source_entry_contract_sha256": signed["sha256"],
        "v3_signed_entry_contract": signed,
    }
    action = "PROFIT_DEFENSE_PARTIAL"
    assert _semantic_action_instance({
        "side": "SELL", "strategy_owner": "US_STANDARD",
        "semantic_action": action,
    }, meta, action, None) == signed["sha256"]


def test_tp_dedupe_and_v3_runner_preserve_protective_priority():
    tp = {"symbol": "SPY", "side": "SELL", "qty": 2, "reason": "TAKE_PROFIT_TP2"}
    runner = {"symbol": "SPY", "side": "SELL", "qty": 10,
              "exit_type": "swing_runner_final_v3", "reason": "R4"}
    hard = {"symbol": "SPY", "side": "SELL", "qty": 10,
            "exit_type": "hard_stop_loss", "reason": "hard_stop_loss"}
    assert merge_exit_intents_by_symbol([tp, runner])[0]["exit_type"] == "swing_runner_final_v3"
    assert merge_exit_intents_by_symbol([tp, runner, hard])[0]["exit_type"] == "hard_stop_loss"


def test_unsettled_broker_partial_blocks_v3_final():
    result = evaluate_v3_runner(
        entry_price=100, verified_high=110, current_price=103,
        holding_qty=10, orderable_qty=10, trend_state="EXIT",
        trend_verified=True, high_verified=True,
        atr_evidence=None,
        partial_stage_status="ACTIVE_ORDER", final_stage_status="NOT_TRIGGERED",
        policy=V3_RUNNER_POLICY,
    )
    assert result["action"] == "HOLD"
    assert result["reason"] == "PARTIAL_ORDER_UNSETTLED_FINAL_REQUIRES_RECONCILE"


def test_stage4_worker_ignores_inherited_pin_without_matching_real_head(tmp_path):
    expected = "a" * 40
    other = "b" * 40
    folder = tmp_path / "runtime" / "code-pins"
    folder.mkdir(parents=True)
    (folder / "US-2026-11-05.sha").write_text(expected, encoding="ascii")
    try:
        assert_stage4_worker_code_pin(
            trade_date="2026-11-05", expected_sha=expected,
            repo_root=tmp_path,
            git_read=lambda *args: (
                "dual-agent" if args == ("branch", "--show-current") else other
            ),
        )
    except RuntimeError as exc:
        assert "HEAD_MISMATCH" in str(exc)
    else:
        raise AssertionError("mixed Stage4 code must never publish READY")
